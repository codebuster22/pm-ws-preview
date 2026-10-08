use core::fmt;
use core::time::Duration;
use pm_ws::limitless::connection::DEFAULT_ENDPOINT;
use pm_ws::limitless::supervisor::{
    BookSegment, MAX_BOOK_LEVELS, MAX_REPLICAS, Supervisor, SupervisorConfig, SupervisorError,
    SupervisorNotice, SupervisorStats, VENUE,
};
use pm_ws::limitless::{LimitlessEvent, ORDERBOOK_UPDATE_DEDUP_KEY};
use pm_ws::{
    BookMutation, BookObserver, ConnectionIdentity, DEFAULT_DIRTY_CAPACITY, DEFAULT_EVENT_CAPACITY,
    DedupKey, DedupKeyError, IdentityError, Level, MAX_POOL_SOCKETS, MIN_POOL_SOCKETS, MarketRef,
    MarketResolution, MutationCursor, NativeIdentifierKind, NativeMarketKey, ObserverEvent,
    ObserverRecvError, PoolViolation, Price, PublishedBook, Quantity, RegionError, SegmentConfig,
    SegmentLayout, SegmentRegion, SegmentWriter, Side, SourceState, Venue, WriterError,
};
use std::path::PathBuf;
use tokio::sync::mpsc;
use tokio::time::Instant;

const DIAGNOSTIC_CHANNEL_CAPACITY: usize = 1024;
const MAX_SECONDS: u64 = 86_400;
const TOP_LEVELS: usize = 3;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = match parse_args(std::env::args()) {
        Ok(args) => args,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    };
    if let Err(error) = run(args).await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

struct Args {
    market: String,
    endpoint: String,
    seconds: u64,
    replicas: usize,
    pool: Option<usize>,
    record: Option<PathBuf>,
    shm: Option<PathBuf>,
    print_book: bool,
    log_versions: bool,
    kill_primary_after: Option<Duration>,
}

#[derive(Debug)]
enum ArgsError {
    MissingValue(&'static str),
    InvalidValue(&'static str),
    MissingRequired(&'static str),
    ConflictingFlags(&'static str, &'static str),
    UnknownFlag(String),
}

impl fmt::Display for ArgsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingValue(flag) => write!(f, "{flag} requires a value"),
            Self::InvalidValue(flag) => write!(f, "{flag} has an invalid value"),
            Self::MissingRequired(flag) => write!(f, "{flag} is required"),
            Self::ConflictingFlags(left, right) => {
                write!(f, "{left} and {right} both set this run's connection count")
            }
            Self::UnknownFlag(flag) => write!(f, "unknown flag {flag}"),
        }
    }
}

/// Parses CLI arguments. `--seconds` is capped at [`MAX_SECONDS`] (24h) so
/// `Instant::now() + Duration::from_secs(seconds)` cannot overflow. `--endpoint` defaults
/// to the venue's public market-data socket and exists so the binary can be pointed at a
/// controlled peer. `--replicas` defaults to one publishing connection and accepts at most
/// [`MAX_REPLICAS`], this tool's own supported primary-and-standby ladder depth — not a
/// figure any venue places.
///
/// `--log-versions` is diagnostic-only conformance recording, off by default: every
/// accepted book update prints one `dedup` line naming the venue's dedup key (Limitless:
/// the `orderbookUpdate` top-level `version`) and the content digest of the book the
/// accepting role then held, which is the evidence a `version` conformance session
/// analyses offline. Absent, the supervisor parses no key and computes no digest.
///
/// `--pool <n>` opts this run into pooled publishing over `n` connections, in
/// [`MIN_POOL_SOCKETS`]..=[`MAX_POOL_SOCKETS`], the venue-agnostic structural ceiling on how
/// many sockets one pool may hold. Absent — the default — the run holds one publishing
/// primary and, with `--replicas 2`, one shadowing hot standby. Present, every connection's
/// arrivals are judged by the venue key gate and the first arrival past the last published
/// key becomes the book's next state, whichever connection carried it; the first observation
/// contradicting the recorded conformance basis hands the book back to primary-and-standby
/// for the rest of the process. It sets the run's connection count itself, so it and
/// `--replicas` are mutually exclusive.
///
/// `--kill-primary-after <secs>` is diagnostic-only fault injection, off by default: that
/// many seconds after the first accepted subscription, the publishing connection's task is
/// aborted so its socket dies mid-run and the ordinary loss and failover path runs against
/// a real end of connection. Zero is rejected, because a kill armed for the instant of the
/// subscription proves nothing about a book that has not been served yet; the value shares
/// `--seconds`'s [`MAX_SECONDS`] cap so the armed instant cannot overflow. It has no place
/// in a run serving consumers.
fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, ArgsError> {
    args.next();
    let mut market = None;
    let mut endpoint = DEFAULT_ENDPOINT.to_owned();
    let mut seconds = 60u64;
    let mut replicas = None;
    let mut pool = None;
    let mut record = None;
    let mut print_book = false;
    let mut log_versions = false;
    let mut shm = None;
    let mut kill_primary_after = None;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--market" => market = Some(args.next().ok_or(ArgsError::MissingValue("--market"))?),
            "--endpoint" => {
                endpoint = args.next().ok_or(ArgsError::MissingValue("--endpoint"))?;
            }
            "--seconds" => {
                let value = args.next().ok_or(ArgsError::MissingValue("--seconds"))?;
                seconds = value
                    .parse()
                    .map_err(|_| ArgsError::InvalidValue("--seconds"))?;
                if seconds > MAX_SECONDS {
                    return Err(ArgsError::InvalidValue("--seconds"));
                }
            }
            "--replicas" => {
                let value = args.next().ok_or(ArgsError::MissingValue("--replicas"))?;
                let count: usize = value
                    .parse()
                    .map_err(|_| ArgsError::InvalidValue("--replicas"))?;
                if count == 0 || count > MAX_REPLICAS {
                    return Err(ArgsError::InvalidValue("--replicas"));
                }
                replicas = Some(count);
            }
            "--pool" => {
                let value = args.next().ok_or(ArgsError::MissingValue("--pool"))?;
                let count: usize = value
                    .parse()
                    .map_err(|_| ArgsError::InvalidValue("--pool"))?;
                if !(MIN_POOL_SOCKETS..=MAX_POOL_SOCKETS).contains(&count) {
                    return Err(ArgsError::InvalidValue("--pool"));
                }
                pool = Some(count);
            }
            "--shm" => {
                shm = Some(PathBuf::from(
                    args.next().ok_or(ArgsError::MissingValue("--shm"))?,
                ));
            }
            "--record" => {
                record = Some(PathBuf::from(
                    args.next().ok_or(ArgsError::MissingValue("--record"))?,
                ));
            }
            "--kill-primary-after" => {
                let value = args
                    .next()
                    .ok_or(ArgsError::MissingValue("--kill-primary-after"))?;
                let seconds: u64 = value
                    .parse()
                    .map_err(|_| ArgsError::InvalidValue("--kill-primary-after"))?;
                if seconds == 0 || seconds > MAX_SECONDS {
                    return Err(ArgsError::InvalidValue("--kill-primary-after"));
                }
                kill_primary_after = Some(Duration::from_secs(seconds));
            }
            "--print-book" => print_book = true,
            "--log-versions" => log_versions = true,
            other => return Err(ArgsError::UnknownFlag(other.to_owned())),
        }
    }
    if replicas.is_some() && pool.is_some() {
        return Err(ArgsError::ConflictingFlags("--replicas", "--pool"));
    }
    Ok(Args {
        market: market.ok_or(ArgsError::MissingRequired("--market"))?,
        endpoint,
        seconds,
        replicas: replicas.unwrap_or(1),
        pool,
        record,
        shm,
        print_book,
        log_versions,
        kill_primary_after,
    })
}

/// A local misconfiguration this process cannot run under. Connection failure is not one of
/// them: the supervisor recovers from those and the run continues to its `--seconds` cap.
#[derive(Debug)]
enum RunError {
    Supervisor(SupervisorError),
    Market(IdentityError),
    Region(RegionError),
    AliasedOutputs,
    Layout(pm_ws::LayoutError),
    Segment(WriterError),
}

impl fmt::Display for RunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Supervisor(error) => write!(f, "invalid configuration: {error:?}"),
            Self::Market(error) => write!(f, "invalid market identity: {error}"),
            Self::Region(error) => write!(f, "shared-memory segment unavailable: {error}"),
            Self::AliasedOutputs => f.write_str(
                "--shm and --record name the same file; the capture would grow the segment \
                 out of its mapped size and every later reader would fail validation",
            ),
            Self::Layout(error) => write!(f, "shared-memory layout rejected: {error}"),
            Self::Segment(error) => write!(f, "shared-memory segment refused: {error:?}"),
        }
    }
}

impl std::error::Error for RunError {}

async fn run(args: Args) -> Result<(), RunError> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let print_book = args.print_book;
    let seconds = args.seconds;
    if let (Some(segment), Some(capture)) = (args.shm.as_deref(), args.record.as_deref())
        && names_the_same_file(segment, capture)
    {
        return Err(RunError::AliasedOutputs);
    }
    let shm = args
        .shm
        .as_deref()
        .map(|path| open_segment(path, &args.market))
        .transpose()?;
    let (notice_tx, notice_rx) = mpsc::channel::<SupervisorNotice>(DIAGNOSTIC_CHANNEL_CAPACITY);
    let config = SupervisorConfig {
        endpoint: args.endpoint,
        market: args.market,
        capture_path: args.record,
        kill_primary_after: args.kill_primary_after,
        log_dedup_keys: args.log_versions,
        replicas: args.pool.unwrap_or(args.replicas),
        pooled: args.pool.is_some(),
        ..SupervisorConfig::default()
    };
    let mut supervisor = Supervisor::new(config)
        .map_err(RunError::Supervisor)?
        .with_diagnostics(notice_tx);
    if let Some(segment) = shm {
        supervisor
            .publish_into(segment)
            .map_err(RunError::Segment)?;
    }
    let observer = print_book.then(|| supervisor.attach());
    let printer = tokio::spawn(printer_task(notice_rx, observer, print_book));

    let deadline = Instant::now() + Duration::from_secs(seconds);
    let stats = supervisor.run_until(deadline).await;
    supervisor.close_diagnostics();
    let observer_continuity_losses = printer.await.unwrap_or(0);
    let segment_failure = supervisor.segment_failure().cloned();
    drop(supervisor);
    if let Some(error) = segment_failure {
        return Err(RunError::Segment(error));
    }

    print_summary(
        &RunSummary {
            stats,
            observer_continuity_losses,
        },
        print_book,
    );
    Ok(())
}

/// The supervisor's stats plus what the attached observer saw, which only this process
/// knows.
struct RunSummary {
    stats: SupervisorStats,
    observer_continuity_losses: u64,
}

/// Consumes supervisor notices and, when `observer` is attached, that book's published
/// revisions and derived mutations, printing all of them without ever blocking the
/// supervisor that feeds them. Mutations print before the revision they produced, because
/// [`BookObserver::next_event`] drains pending history first.
///
/// This is a diagnostic reader and nothing else. It delivers no data to anyone: shared
/// memory is published by the supervisor on the book's own commit path, so an overrun here
/// costs this process a few printed lines and can never cost a consumer its history.
///
/// Returns the [`ObserverRecvError::ContinuityLost`] count for the end-of-run summary. Exits
/// once `notices` drains; a final [`drain_observer`] catches anything still buffered then.
async fn printer_task(
    mut notices: mpsc::Receiver<SupervisorNotice>,
    mut observer: Option<BookObserver>,
    print: bool,
) -> u64 {
    let mut state = BookPrinterState {
        print,
        ..BookPrinterState::default()
    };
    if let Some(active) = observer.as_ref() {
        print_book_revision(&active.latest(), &mut state);
    }
    loop {
        let mut delivery = None;
        tokio::select! {
            notice = notices.recv() => match notice {
                Some(notice) => println!("{}", format_notice(&notice)),
                None => break,
            },
            result = observer_next(&mut observer) => delivery = Some(result),
        }
        if let Some(result) = delivery {
            record_observer_event(&mut observer, result, &mut state);
        }
    }
    drain_observer(&mut observer, &mut state);
    state.continuity_losses
}

/// Awaits the next observer event on either surface, or never resolves when no observer is
/// attached, so [`printer_task`] can select over this unconditionally.
async fn observer_next(
    observer: &mut Option<BookObserver>,
) -> Result<ObserverEvent, ObserverRecvError> {
    match observer {
        Some(observer) => observer.next_event().await,
        None => std::future::pending().await,
    }
}

/// Reads every mutation [`BookObserver::try_recv`] currently holds without waiting, then
/// prints the final published revision.
fn drain_observer(observer: &mut Option<BookObserver>, state: &mut BookPrinterState) {
    while let Some(active) = observer.as_mut() {
        let outcome = active.try_recv();
        match outcome {
            Ok(Some(delivery)) => {
                record_observer_event(observer, Ok(ObserverEvent::from(delivery)), state)
            }
            Ok(None) => break,
            Err(error) => record_observer_event(observer, Err(error), state),
        }
    }
    if let Some(active) = observer.as_ref() {
        print_book_revision(&active.latest(), state);
    }
}

/// The continuity-loss count for the summary, plus the last revision printed so deliveries
/// that coalesce onto the same [`BookObserver::latest`] read print one line per revision.
#[derive(Default)]
struct BookPrinterState {
    continuity_losses: u64,
    last_printed_revision: Option<u64>,
    print: bool,
}

/// `path` with its directory resolved, so two spellings of one location compare equal even
/// when the file itself does not exist yet.
fn resolved_target(path: &std::path::Path) -> PathBuf {
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => std::path::Path::new("."),
    };
    match parent.canonicalize() {
        Ok(directory) => match path.file_name() {
            Some(name) => directory.join(name),
            None => directory,
        },
        Err(_) => path.to_path_buf(),
    }
}

/// Whether two output paths name one file.
///
/// The shared-memory segment is a fixed-size mapping and the frame capture is append-only,
/// so letting them collide would grow the file past the size its header declares and make
/// every later reader fail validation while the daemon kept publishing into its original
/// mapping. Directory-resolved paths catch the ordinary spellings; where both files already
/// exist the device and inode catch a hard link or a symlink that no path comparison can.
/// The inode half is Unix-only, and a platform without it keeps the path comparison.
fn names_the_same_file(left: &std::path::Path, right: &std::path::Path) -> bool {
    if resolved_target(left) == resolved_target(right) {
        return true;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if let (Ok(left), Ok(right)) = (left.metadata(), right.metadata()) {
            return left.dev() == right.dev() && left.ino() == right.ino();
        }
    }
    false
}

/// Creates and formats the segment file for one market, sized for a single book.
///
/// The segment is sized from the supervisor's own [`MAX_BOOK_LEVELS`], so every book this
/// run can accept fits it and a publication into it cannot be refused for depth.
fn open_segment(path: &std::path::Path, slug: &str) -> Result<BookSegment, RunError> {
    let market = MarketRef::new(
        Venue::new(VENUE).map_err(RunError::Market)?,
        NativeMarketKey::new(NativeIdentifierKind::slug(), slug).map_err(RunError::Market)?,
    );
    let levels = u32::try_from(MAX_BOOK_LEVELS).map_err(|_| {
        RunError::Segment(WriterError::LevelCapacityExceeded {
            levels: MAX_BOOK_LEVELS,
            capacity: u32::MAX,
        })
    })?;
    let layout = SegmentLayout::new(1, 1, levels, DEFAULT_EVENT_CAPACITY, DEFAULT_DIRTY_CAPACITY)
        .map_err(RunError::Layout)?;
    let region =
        SegmentRegion::create_file(path, layout.region_size()).map_err(RunError::Region)?;
    let mut writer = SegmentWriter::create(
        std::sync::Arc::new(region),
        SegmentConfig::new(layout, segment_instance_id(), 1),
    )
    .map_err(RunError::Segment)?;
    let handle = writer.install(&market).map_err(RunError::Segment)?;
    Ok(BookSegment::new(writer, handle))
}

/// A 128-bit instance identity for this run, from the process id and the wall clock.
///
/// Distinguishes one daemon run's segment from another's; it is not a secret and carries no
/// venue material.
fn segment_instance_id() -> u128 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos());
    nanos ^ (u128::from(std::process::id()) << 96)
}

/// Prints one observer event: the published book for [`ObserverEvent::Published`], a compact
/// line for [`ObserverEvent::Mutation`] and [`ObserverEvent::Resolution`], and an explicit
/// `reattach` on [`ObserverRecvError::ContinuityLost`] before printing the state it resumed
/// from.
fn record_observer_event(
    observer: &mut Option<BookObserver>,
    result: Result<ObserverEvent, ObserverRecvError>,
    state: &mut BookPrinterState,
) {
    let Some(active) = observer.as_mut() else {
        return;
    };
    match result {
        Ok(ObserverEvent::Published(published)) => print_book_revision(&published, state),
        Ok(ObserverEvent::Mutation(delivery)) => {
            if state.print {
                println!(
                    "{}",
                    format_mutation(delivery.revision(), delivery.cursor(), delivery.mutation())
                );
            }
        }
        Ok(ObserverEvent::Resolution(delivery)) => {
            if state.print {
                println!(
                    "{}",
                    format_resolution(
                        delivery.revision(),
                        delivery.cursor(),
                        delivery.resolution()
                    )
                );
            }
        }
        Err(ObserverRecvError::ContinuityLost { reason, missed }) => {
            state.continuity_losses += 1;
            if state.print {
                println!("book continuity_lost reason={reason:?} missed={missed}");
            }
            state.last_printed_revision = None;
            print_book_revision(&active.reattach(), state);
        }
        Err(ObserverRecvError::Closed) => *observer = None,
    }
}

fn print_book_revision(published: &PublishedBook, state: &mut BookPrinterState) {
    if state.last_printed_revision == Some(published.revision()) {
        return;
    }
    state.last_printed_revision = Some(published.revision());
    if !state.print {
        return;
    }
    let canonical = published.canonical_levels();
    let complement_summary = match published.derived_complement_levels() {
        Ok(levels) => format!(
            "derived_complement_bids=[{}] derived_complement_asks=[{}]",
            format_levels(&top_n_by_side(&levels, Side::Bid, TOP_LEVELS)),
            format_levels(&top_n_by_side(&levels, Side::Ask, TOP_LEVELS)),
        ),
        Err(_) => "derived_complement=unavailable".to_owned(),
    };
    println!(
        "book revision={} authority={:?} continuity_epoch={} canonical_bids=[{}] canonical_asks=[{}] {complement_summary}",
        published.revision(),
        published.authority(),
        published.continuity().epoch(),
        format_levels(&top_n_by_side(canonical, Side::Bid, TOP_LEVELS)),
        format_levels(&top_n_by_side(canonical, Side::Ask, TOP_LEVELS)),
    );
}

/// The best `n` levels of `side` from `levels`, best first: highest price for a bid,
/// lowest price for an ask.
fn top_n_by_side(levels: &[Level], side: Side, n: usize) -> Vec<&Level> {
    let mut matching: Vec<&Level> = levels.iter().filter(|level| level.side() == side).collect();
    if side == Side::Bid {
        matching.reverse();
    }
    matching.truncate(n);
    matching
}

fn format_levels(levels: &[&Level]) -> String {
    levels
        .iter()
        .map(|level| {
            format!(
                "{}@{}",
                level.price().value().canonical(),
                level.quantity().value().canonical()
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn format_mutation(revision: u64, cursor: &MutationCursor, mutation: &BookMutation) -> String {
    let quantity = |level: Option<&Level>| {
        level
            .map(|level| level.quantity().value().canonical())
            .unwrap_or_else(|| "none".to_owned())
    };
    let anchor = mutation
        .replacement()
        .or(mutation.old())
        .expect("BookMutation always carries at least one side");
    format!(
        "mutation revision={revision} epoch={} position={} side={:?} price={} qty={}->{}",
        cursor.epoch(),
        cursor.position(),
        anchor.side(),
        anchor.price().value().canonical(),
        quantity(mutation.old()),
        quantity(mutation.replacement()),
    )
}

/// One venue-reported resolution, at the stream position it was ordered at.
///
/// `revision` is the book revision it was ordered after, not one it produced: a resolution
/// leaves the book exactly where it was.
fn format_resolution(
    revision: u64,
    cursor: &MutationCursor,
    resolution: &MarketResolution,
) -> String {
    format!(
        "resolution revision={revision} epoch={} position={} winner={} index={} type={} date={}",
        cursor.epoch(),
        cursor.position(),
        resolution.winner().text_value().unwrap_or(""),
        resolution.winning_index(),
        resolution.native_label().as_str(),
        resolution.resolution_date().as_lexeme(),
    )
}

fn format_notice(notice: &SupervisorNotice) -> String {
    match notice {
        SupervisorNotice::Connected {
            generation,
            replica,
            sid,
            ping_interval_ms,
            ping_timeout_ms,
            max_payload_bytes,
            subscription_generation,
        } => format!(
            "connected generation={generation} replica={replica:?} sid={sid} ping_interval_ms={ping_interval_ms} ping_timeout_ms={ping_timeout_ms} max_payload_bytes={max_payload_bytes} subscription_generation={subscription_generation}"
        ),
        SupervisorNotice::Event(event) => format_event(event),
        SupervisorNotice::ContinuityLoss {
            continuity,
            authority,
        } => format!("continuity_loss continuity={continuity:?} authority={authority:?}"),
        SupervisorNotice::SourceTransition { source } => format_source(source),
        SupervisorNotice::Fenced { generation } => format!("fenced generation={generation}"),
        SupervisorNotice::Reconnecting {
            generation,
            replica,
            delay_ms,
        } => {
            format!("reconnecting generation={generation} replica={replica:?} delay_ms={delay_ms}")
        }
        SupervisorNotice::Resubscribing {
            generation,
            replica,
        } => {
            format!("resubscribing generation={generation} replica={replica:?}")
        }
        SupervisorNotice::RecoveryBaseUnavailable { attempts } => {
            format!("recovery_base_unavailable attempts={attempts}")
        }
        SupervisorNotice::PoolDegraded {
            violation,
            generation,
        } => format_pool_degraded(violation, *generation),
        SupervisorNotice::AcceptedUpdate {
            market,
            position,
            generation,
            replica,
            key,
            digest,
        } => format!(
            "dedup market={market} position={position} generation={generation} replica={replica:?} semantics={} key={} digest={digest}",
            ORDERBOOK_UPDATE_DEDUP_KEY.semantics().as_label(),
            format_dedup_key(key),
        ),
    }
}

/// Renders a frame's dedup key as one whitespace-free token: the venue's own value, `none`
/// when the frame carried no key at all, and `invalid:<reason>` when the venue reported a
/// value this daemon refuses to represent inexactly. The three cases stay distinguishable,
/// because a venue that stopped sending the field and one that sent an unusable value are
/// different conformance facts.
fn format_dedup_key(key: &Result<Option<DedupKey>, DedupKeyError>) -> String {
    match key {
        Ok(Some(key)) => key.to_string(),
        Ok(None) => "none".to_owned(),
        Err(reason) => format!("invalid:{reason:?}"),
    }
}

/// Renders the one arrival that withdrew a pool's licence to publish across sockets: what
/// was observed, on which socket and connection, and against what.
///
/// The keys and digests are printed rather than summarized, because a degrade is the
/// evidence a conformance basis has to be re-examined against; `none` stands for a value
/// the arrival did not carry.
fn format_pool_degraded(violation: &PoolViolation, generation: u64) -> String {
    let key = |value: &Option<DedupKey>| {
        value
            .as_ref()
            .map_or_else(|| "none".to_owned(), DedupKey::to_string)
    };
    let digest = |value: &Option<pm_ws::ContentDigest>| {
        value.map_or_else(|| "none".to_owned(), |digest| digest.to_string())
    };
    format!(
        "pool_degraded reason={:?} socket={} generation={generation} observed_key={} \
previous_key={} published_key={} observed_digest={} previous_digest={}",
        violation.reason,
        violation.socket,
        key(&violation.observed_key),
        key(&violation.previous_key),
        key(&violation.published_key),
        violation.observed_digest,
        digest(&violation.previous_digest),
    )
}

/// Renders one source-topology transition: which connection publishes, which shadow it,
/// what each standby comparison says, and how much standby coverage was requested — or,
/// for a pool, which connections hold its sockets, how many can currently deliver, and the
/// key of the arrival it last published.
fn format_source(source: &SourceState) -> String {
    if source.pool_capacity() > 0 {
        let sockets = source
            .pool_sockets()
            .map(|(socket, state)| format!("{}={state:?}", format_connection(socket.connection())))
            .collect::<Vec<_>>()
            .join(",");
        let last = source
            .last_published_key()
            .map_or_else(|| "none".to_owned(), DedupKey::to_string);
        let degraded = source
            .pool_degraded()
            .map_or_else(|| "none".to_owned(), |reason| format!("{reason:?}"));
        return format!(
            "source pooled sockets=[{sockets}] covering={}/{} last_published_key={last} \
degraded={degraded}",
            source.pool_covering(),
            source.pool_capacity(),
        );
    }
    if let Some(primary) = source.publishing_primary() {
        let standbys = source
            .standbys()
            .map(|(standby, state)| {
                format!("{}={state:?}", format_connection(standby.connection()))
            })
            .collect::<Vec<_>>()
            .join(",");
        let capacity = source.standby_capacity();
        return format!(
            "source publishing primary={} standbys=[{standbys}] standby_capacity={capacity}",
            format_connection(primary.connection())
        );
    }
    match source.recovery() {
        Some(recovery) => format!(
            "source recovering replica={}",
            format_connection(recovery.connection())
        ),
        None => "source none".to_owned(),
    }
}

fn format_connection(connection: &ConnectionIdentity) -> String {
    format!("{}#{}", connection.value(), connection.generation())
}

fn format_event(event: &LimitlessEvent) -> String {
    match event {
        LimitlessEvent::OrderbookUpdate(update) => format!(
            "orderbookUpdate slug={} bids={} asks={} best_bid={} best_ask={} ts={}",
            update.market_slug(),
            update.bids().len(),
            update.asks().len(),
            format_level(update.bids().first()),
            format_level(update.asks().first()),
            update.timestamp(),
        ),
        LimitlessEvent::MarketResolved(resolved) => format!(
            "marketResolved slug={} type={} winning_outcome={} winning_index={} resolution_date={}",
            resolved.slug(),
            resolved.market_type(),
            resolved.winning_outcome(),
            resolved.winning_index(),
            resolved.resolution_date(),
        ),
        LimitlessEvent::Unknown { name } => format!("unknown event name={name}"),
    }
}

fn format_level(level: Option<&(Price, Quantity)>) -> String {
    match level {
        Some((price, quantity)) => {
            format!(
                "{}@{}",
                price.value().canonical(),
                quantity.value().canonical()
            )
        }
        None => "-".to_owned(),
    }
}

/// The pool's end-of-run coverage line, or `None` for a run that held no pool.
///
/// One line per run, with the fields in a pinned order and every value a single
/// whitespace-free token: how many arrivals the gate published, which socket carried each
/// of them, how many arrivals it dropped as duplicates and how many as skew, the venue key
/// of the arrival it published last, and whether the tripwire still stands or which
/// observation withdrew the pool's licence.
fn pool_summary_line(stats: &SupervisorStats) -> Option<String> {
    if stats.pool_published_by_socket.is_empty() {
        return None;
    }
    let by_socket = stats
        .pool_published_by_socket
        .iter()
        .enumerate()
        .map(|(socket, published)| format!("{socket}:{published}"))
        .collect::<Vec<_>>()
        .join(",");
    Some(format!(
        "summary pool published={} by_socket=[{by_socket}] dedup_drops={} stale_drops={} \
last_published_key={} tripwire={}",
        stats.pool_published,
        stats.pool_dedup_drops,
        stats.pool_stale_drops,
        stats
            .pool_last_published_key
            .as_ref()
            .map_or_else(|| "none".to_owned(), DedupKey::to_string),
        stats
            .pool_degraded
            .map_or_else(|| "armed".to_owned(), |reason| format!("{reason:?}")),
    ))
}

/// Prints the end-of-run summary. `print_book` gates the book-stats line so a run without
/// `--print-book` prints no book state it never observed.
fn print_summary(summary: &RunSummary, print_book: bool) {
    let stats = &summary.stats;
    println!(
        "summary frames_seen={} connection_attempts={} fenced_generations={} fenced_events={}",
        stats.frames_seen, stats.connection_attempts, stats.fenced_generations, stats.fenced_events
    );
    println!(
        "summary events orderbookUpdate={} marketResolved={} unknown={}",
        stats.events_orderbook, stats.events_resolved, stats.events_unknown
    );
    println!(
        "summary diagnostics_dropped={} overload_drops={}",
        stats.diagnostics_dropped, stats.overload_drops
    );
    if let Some(line) = pool_summary_line(stats) {
        println!("{line}");
    }
    if stats.decode_failures.is_empty() {
        println!("summary decode_failures=none");
    } else {
        for (reason, count) in &stats.decode_failures {
            println!("summary decode_failure reason={reason} count={count}");
        }
    }
    if print_book {
        println!(
            "summary book snapshots_applied={} mutations_derived={} continuity_losses={} recovery_base_unavailable={} observer_continuity_losses={}",
            stats.snapshots_applied,
            stats.mutations_derived,
            stats.continuity_losses,
            stats.recovery_base_unavailable,
            summary.observer_continuity_losses
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cli(args: &[&str]) -> impl Iterator<Item = String> {
        std::iter::once("pm-ws")
            .chain(args.iter().copied())
            .map(str::to_owned)
    }

    #[test]
    fn parse_args_defaults_print_book_to_false() {
        let args = parse_args(cli(&["--market", "abc"])).unwrap();
        assert!(!args.print_book);
    }

    #[test]
    fn parse_args_sets_print_book_when_flag_present() {
        let args = parse_args(cli(&["--market", "abc", "--print-book"])).unwrap();
        assert!(args.print_book);
    }

    #[test]
    fn parse_args_print_book_does_not_consume_a_value() {
        let args =
            parse_args(cli(&["--market", "abc", "--print-book", "--seconds", "30"])).unwrap();
        assert!(args.print_book);
        assert_eq!(args.seconds, 30);
    }

    #[test]
    fn parse_args_defaults_log_versions_to_false() {
        let args = parse_args(cli(&["--market", "abc"])).unwrap();
        assert!(!args.log_versions);
    }

    #[test]
    fn parse_args_log_versions_does_not_consume_a_value() {
        let args = parse_args(cli(&[
            "--market",
            "abc",
            "--log-versions",
            "--seconds",
            "30",
        ]))
        .unwrap();
        assert!(args.log_versions);
        assert_eq!(args.seconds, 30);
    }

    /// The conformance line an offline analysis parses: one line per accepted update, its
    /// fields in a pinned order and every value a single whitespace-free token. A change
    /// here breaks every recorded session, so it is pinned rather than merely exercised.
    #[test]
    fn accepted_update_renders_one_machine_parsable_conformance_line() {
        let market = MarketRef::new(
            Venue::new("limitless").unwrap(),
            NativeMarketKey::new(
                NativeIdentifierKind::slug(),
                "btc-up-or-down-5-min-1788188100",
            )
            .unwrap(),
        );
        let digest = pm_ws::content_digest(&pm_ws::OrderBook::new(market).publish());
        let render = |key: Result<Option<DedupKey>, DedupKeyError>| {
            format_notice(&SupervisorNotice::AcceptedUpdate {
                market: "btc-up-or-down-5-min-1788188100".to_owned(),
                position: 12,
                generation: 3,
                replica: pm_ws::ReplicaRole::HotStandby,
                key,
                digest,
            })
        };
        let line = |key: &str| {
            format!(
                "dedup market=btc-up-or-down-5-min-1788188100 position=12 generation=3 \
replica=HotStandby semantics=session-monotone key={key} digest={digest}"
            )
        };
        assert_eq!(render(Ok(Some(DedupKey::integer(526_135)))), line("526135"));
        assert_eq!(render(Ok(None)), line("none"));
        assert_eq!(
            render(Err(DedupKeyError::NotAnExactInteger)),
            line("invalid:NotAnExactInteger")
        );
    }

    #[test]
    fn parse_args_defaults_endpoint_to_the_venue() {
        let args = parse_args(cli(&["--market", "abc"])).unwrap();
        assert_eq!(args.endpoint, DEFAULT_ENDPOINT);
    }

    #[test]
    fn parse_args_overrides_endpoint() {
        let args = parse_args(cli(&[
            "--market",
            "abc",
            "--endpoint",
            "ws://127.0.0.1:1/x",
        ]))
        .unwrap();
        assert_eq!(args.endpoint, "ws://127.0.0.1:1/x");
    }

    #[test]
    fn parse_args_defaults_to_a_single_publishing_connection() {
        let args = parse_args(cli(&["--market", "abc"])).unwrap();
        assert_eq!(args.replicas, 1);
    }

    #[test]
    fn parse_args_accepts_a_hot_standby() {
        let args = parse_args(cli(&["--market", "abc", "--replicas", "2"])).unwrap();
        assert_eq!(args.replicas, 2);
    }

    #[test]
    fn parse_args_rejects_replica_counts_outside_the_socket_ceiling() {
        for value in [0, MAX_REPLICAS + 1] {
            let value = value.to_string();
            let result = parse_args(cli(&["--market", "abc", "--replicas", &value]));
            assert!(
                matches!(result, Err(ArgsError::InvalidValue("--replicas"))),
                "--replicas {value} was accepted"
            );
        }
        let missing = parse_args(cli(&["--market", "abc", "--replicas"]));
        assert!(matches!(
            missing,
            Err(ArgsError::MissingValue("--replicas"))
        ));
    }

    #[test]
    fn parse_args_defaults_the_pool_to_absent() {
        let args = parse_args(cli(&["--market", "abc"])).unwrap();
        assert!(
            args.pool.is_none(),
            "a run that did not ask for a pool keeps one publishing primary"
        );
        assert_eq!(args.replicas, 1);
    }

    #[test]
    fn parse_args_accepts_a_pool_and_sets_the_connection_count_from_it() {
        let args = parse_args(cli(&["--market", "abc", "--pool", "2"])).unwrap();
        assert_eq!(args.pool, Some(2));
    }

    /// Sizes outside [`MIN_POOL_SOCKETS`]..=[`MAX_POOL_SOCKETS`] are refused here: below the
    /// floor is not a pool at all, and above [`MAX_POOL_SOCKETS`] is past the pool gate's
    /// own structural ceiling and is refused there too, so this test proves `--pool` never
    /// accepts a size the gate would refuse a moment later.
    #[test]
    fn parse_args_rejects_pool_sizes_outside_the_gate() {
        let parse = |size: usize| {
            parse_args(cli(&["--market", "abc", "--pool", &size.to_string()])).map(|args| args.pool)
        };
        for size in 0..MIN_POOL_SOCKETS {
            assert!(
                matches!(parse(size), Err(ArgsError::InvalidValue("--pool"))),
                "--pool {size} is not a pool at all and was accepted"
            );
        }
        for size in MIN_POOL_SOCKETS..=MAX_POOL_SOCKETS {
            assert_eq!(parse(size).ok().flatten(), Some(size));
        }
        for size in MAX_POOL_SOCKETS + 1..=MAX_POOL_SOCKETS + 2 {
            assert!(
                matches!(parse(size), Err(ArgsError::InvalidValue("--pool"))),
                "--pool {size} exceeds the pool gate's own socket ceiling and was accepted"
            );
        }
        let missing = parse_args(cli(&["--market", "abc", "--pool"]));
        assert!(matches!(missing, Err(ArgsError::MissingValue("--pool"))));
    }

    #[test]
    fn parse_args_refuses_pool_and_replicas_together() {
        let result = parse_args(cli(&["--market", "abc", "--replicas", "2", "--pool", "2"]));
        assert!(
            matches!(
                result,
                Err(ArgsError::ConflictingFlags("--replicas", "--pool"))
            ),
            "both flags set the run's connection count, so one run cannot carry both"
        );
    }

    /// The summary line an operator reads a pool's coverage from: how much it published,
    /// which socket carried what, what the gate dropped and why, and whether the tripwire
    /// still stands.
    #[test]
    fn a_pooled_run_reports_socket_coverage_and_gate_drops() {
        let summary = RunSummary {
            stats: SupervisorStats {
                pool_published: 6,
                pool_published_by_socket: vec![4, 2],
                pool_dedup_drops: 5,
                pool_stale_drops: 1,
                pool_last_published_key: Some(DedupKey::integer(2_002_185)),
                pool_degraded: Some(pm_ws::PoolDegradeReason::ConnectionInversion),
                ..SupervisorStats::default()
            },
            observer_continuity_losses: 0,
        };
        let rendered = pool_summary_line(&summary.stats).expect("a pooled run reports a pool line");
        assert_eq!(
            rendered,
            "summary pool published=6 by_socket=[0:4,1:2] dedup_drops=5 stale_drops=1 \
last_published_key=2002185 tripwire=ConnectionInversion"
        );
        assert_eq!(
            pool_summary_line(&SupervisorStats::default()),
            None,
            "a run with no pool reports no pool line"
        );
    }

    #[test]
    fn a_pooled_source_transition_names_every_socket_and_the_key_it_last_published() {
        let connection = |generation| {
            ConnectionIdentity::new("limitless-markets", generation).expect("a valid connection")
        };
        let source = SourceState::pooled(
            [
                (
                    pm_ws::PoolSocket::new(connection(1)),
                    pm_ws::PoolSocketState::Covering,
                ),
                (
                    pm_ws::PoolSocket::new(connection(2)),
                    pm_ws::PoolSocketState::Failed(pm_ws::ReplicaFailureReason::Disconnect),
                ),
            ],
            2,
            Some(DedupKey::integer(2_002_185)),
            None,
        )
        .expect("a valid pooled topology");
        assert_eq!(
            format_source(&source),
            "source pooled sockets=[limitless-markets#1=Covering,\
limitless-markets#2=Failed(Disconnect)] covering=1/2 last_published_key=2002185 degraded=none"
        );
    }

    #[test]
    fn parse_args_defaults_kill_primary_after_to_off() {
        let args = parse_args(cli(&["--market", "abc"])).unwrap();
        assert!(args.kill_primary_after.is_none());
    }

    #[test]
    fn parse_args_accepts_a_diagnostic_kill_delay() {
        let args = parse_args(cli(&["--market", "abc", "--kill-primary-after", "5"])).unwrap();
        assert_eq!(args.kill_primary_after, Some(Duration::from_secs(5)));
    }

    #[test]
    fn parse_args_rejects_a_kill_delay_of_zero_or_without_a_value() {
        let zero = parse_args(cli(&["--market", "abc", "--kill-primary-after", "0"]));
        assert!(matches!(
            zero,
            Err(ArgsError::InvalidValue("--kill-primary-after"))
        ));
        let missing = parse_args(cli(&["--market", "abc", "--kill-primary-after"]));
        assert!(matches!(
            missing,
            Err(ArgsError::MissingValue("--kill-primary-after"))
        ));
    }

    #[test]
    fn shm_and_record_paths_that_name_one_file_are_detected() {
        use std::path::Path;
        let directory = std::env::temp_dir();
        let plain = directory.join("pm-ws-alias-probe.seg");
        assert!(names_the_same_file(&plain, &plain));
        assert!(names_the_same_file(
            &plain,
            &directory.join(".").join("pm-ws-alias-probe.seg")
        ));
        assert!(!names_the_same_file(
            &plain,
            &directory.join("pm-ws-alias-other.seg")
        ));
        assert!(!names_the_same_file(
            Path::new("book.seg"),
            Path::new("frames.jsonl")
        ));

        let target = directory.join(format!("pm-ws-alias-{}.jsonl", std::process::id()));
        let link = directory.join(format!("pm-ws-alias-{}.link", std::process::id()));
        std::fs::write(&target, b"x").expect("capture file");
        let _ = std::fs::remove_file(&link);
        #[cfg(unix)]
        {
            std::fs::hard_link(&target, &link).expect("hard link");
            assert!(names_the_same_file(&link, &target));
        }
        let _ = std::fs::remove_file(&link);
        let _ = std::fs::remove_file(&target);
    }

    #[test]
    fn parse_args_reads_the_shared_memory_segment_path() {
        let args = parse_args(cli(&["--market", "abc", "--shm", "/tmp/book.seg"])).unwrap();
        assert_eq!(
            args.shm.as_deref(),
            Some(std::path::Path::new("/tmp/book.seg"))
        );
        assert!(parse_args(cli(&["--market", "abc"])).unwrap().shm.is_none());
        assert!(matches!(
            parse_args(cli(&["--market", "abc", "--shm"])),
            Err(ArgsError::MissingValue("--shm"))
        ));
    }

    #[test]
    fn parse_args_rejects_endpoint_without_a_value() {
        let result = parse_args(cli(&["--market", "abc", "--endpoint"]));
        assert!(matches!(result, Err(ArgsError::MissingValue("--endpoint"))));
    }
}

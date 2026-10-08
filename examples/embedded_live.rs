//! The `rust-embedded` leg of the S8c comparative harness (`bench/sdk-harness/README.md`):
//! the pm-ws engine in one process, with no daemon and no shared memory, riding the venue
//! rails the daemon itself rides.
//!
//! `docs/design.md` "Standalone and embedded boundaries" is the contract this leg exists to
//! measure: embedded mode reuses the same venue rails, validation, book ownership, and
//! provenance, and replaces only the cross-process delivery boundary with in-process access.
//! Nothing here re-implements the venue protocol. The market set is sorted and chunked by
//! the daemon's own arithmetic (`DaemonConfig::validate` sorts then partitions into
//! fixed-size runs), each chunk is carried by one
//! [`run_connection`](pm_ws::limitless::connection::run_connection) — the same task a shard
//! spawns, which dials, completes the Engine.IO and namespace handshake, emits one
//! `subscribe_market_prices` command through the process-wide per-endpoint pacer, answers
//! server pings and sends none of its own, and forwards decoded events — and every
//! `orderbookUpdate` becomes a snapshot [`Candidate`](pm_ws::Candidate) applied to that
//! market's own [`BookWriter`].
//!
//! The engine's two threads are the point of the leg. Ingestion runs on this process's own
//! runtime thread: sockets, decode, provenance, and the one writer per book. Strategy runs on
//! a thread of its own, holding one [`BookObserver`] per market and nothing else. `t_obs` is
//! stamped there, the instant the observer's latest-state wait returns the published book,
//! before anything else — the harness's boundary. The content digest, the revision dedup and
//! the log append all happen after the stamp: harness cost, the same work in every leg.
//!
//! Two behaviours are deliberate and named in the report rather than papered over. A
//! connection that ends is not redialled: reconnect policy belongs to
//! `crate::limitless::supervisor`, and a leg that grew its own would be measuring a second
//! implementation. A `marketResolved` closes that market's recording by dropping its writer,
//! which is what ends its strategy task; publishing an `Unsubscribed` state instead would put
//! a row in the `.obs` log that no venue book event produced and that no other leg observed.
//! New listings are ignored: the market set is pinned for the run, as the harness requires.
//!
//! **This example dials the live venue.** Every run is live traffic under
//! `docs/limitless.md` etiquette; there is no synthetic mode.
//!
//! ```text
//! cargo run --release --example embedded_live -- \
//!     --slugs /tmp/all-active.txt --seconds 900 \
//!     --obs-out /tmp/s8c/rust-embedded.obs --label "<machine>, all-active"
//! ```

use pm_ws::limitless::connection::{
    ConnectionConfig, ConnectionEndReason, ConnectionNote, ConnectionNotice, DEFAULT_ENDPOINT,
    run_connection,
};
use pm_ws::limitless::shard::ShardConfig;
use pm_ws::limitless::supervisor::{MAX_BOOK_LEVELS, VENUE};
use pm_ws::limitless::{LimitlessEvent, OrderbookUpdate};
use pm_ws::{
    BookObserver, BookWriter, BoundedSourceEvidence, ConnectionIdentity, DEFAULT_MARKETS_PER_SHARD,
    DeliveryProfile, LevelCapacity, LocalMonotonicTimestamp, MarketRef, NativeIdentifierKind,
    NativeMarketKey, NativeOutcome, ObserverCapacity, OrderBook, Origin, Provenance,
    ProvenanceInput, ReplicaRole, Representation, SourceEvidenceCapacity, SourceTimestamp, Venue,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};

#[path = "common/matched_obs.rs"]
mod matched_obs;

use matched_obs::{
    IMPLAUSIBLE_NANOS, OBS_ROW_CAP, ObsLog, SlugId, git_pin, level_digest, now_epoch_nanos,
    parse_seconds, print_distribution, read_slug_file,
};

/// The leg identifier this process writes into its `.obs` header.
const LEG: &str = "rust-embedded";

/// The connection identity every book's provenance names, mirroring the shard's own.
const CONNECTION_NAME: &str = "limitless-markets";

/// The venue event name a book snapshot's provenance is labelled with, mirroring the shard's.
const ORDERBOOK_UPDATE_EVENT: &str = "orderbookUpdate";

/// The daemon generation this leg's provenance declares. One process, one generation.
const DAEMON_GENERATION: u64 = 1;

/// How many notices one connection may have outstanding toward its ingest task, matching the
/// daemon's own `ingest_capacity` default. Bounded, drop-newest with ordered reporting — the
/// overflow behaviour `run_connection` documents and reports as
/// [`ConnectionNote::Overload`].
const INGEST_CAPACITY: usize = 1_024;

/// How many control commands one connection may have outstanding. Nothing here ever sends
/// one; the channel exists because the connection task takes one.
const CONTROL_CAPACITY: usize = 8;

const USAGE: &str = "usage: embedded_live --slugs <file> --obs-out <path> \
                     --label \"<machine, workload>\" [--seconds <n>] [--endpoint <url>] \
                     [--markets-per-connection <n>]";

#[derive(Clone, Debug, Eq, PartialEq)]
struct Args {
    slugs: PathBuf,
    seconds: u64,
    obs_out: PathBuf,
    label: String,
    endpoint: String,
    markets_per_connection: usize,
}

fn main() {
    let args = match parse_args(std::env::args()) {
        Ok(args) => args,
        Err(message) => {
            eprintln!("embedded_live: {message}");
            eprintln!("{USAGE}");
            std::process::exit(2);
        }
    };
    let slugs = match read_slug_file(args.slugs.as_path()) {
        Ok(slugs) => slugs,
        Err(message) => {
            eprintln!("embedded_live: {message}");
            std::process::exit(2);
        }
    };
    if let Err(message) = run(&args, slugs) {
        eprintln!("embedded_live: {message}");
        std::process::exit(1);
    }
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let _binary = args.next();
    let mut slugs = None;
    let mut seconds = 600_u64;
    let mut obs_out = None;
    let mut label = None;
    let mut endpoint = DEFAULT_ENDPOINT.to_owned();
    let mut markets_per_connection = DEFAULT_MARKETS_PER_SHARD;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--slugs" => {
                slugs = Some(PathBuf::from(
                    args.next().ok_or("--slugs requires a value")?,
                ))
            }
            "--seconds" => {
                seconds = parse_seconds(&args.next().ok_or("--seconds requires a value")?)?;
            }
            "--obs-out" => {
                obs_out = Some(PathBuf::from(
                    args.next().ok_or("--obs-out requires a value")?,
                ));
            }
            "--label" => label = Some(args.next().ok_or("--label requires a value")?),
            "--endpoint" => {
                let value = args.next().ok_or("--endpoint requires a value")?;
                if !(value.starts_with("ws://") || value.starts_with("wss://")) {
                    return Err("--endpoint must be a WebSocket URL".to_owned());
                }
                endpoint = value;
            }
            "--markets-per-connection" => {
                let value = args
                    .next()
                    .ok_or("--markets-per-connection requires a value")?;
                markets_per_connection = value
                    .parse()
                    .map_err(|_| "--markets-per-connection takes a positive integer".to_owned())?;
                if markets_per_connection == 0 {
                    return Err("--markets-per-connection must be at least 1".to_owned());
                }
            }
            other => return Err(format!("unknown flag {other}")),
        }
    }
    Ok(Args {
        slugs: slugs.ok_or("--slugs is required (one market slug per line)")?,
        seconds,
        obs_out: obs_out.ok_or("--obs-out is required (where the observation log is written)")?,
        label: label.ok_or("--label is required (the machine and the workload)")?,
        endpoint,
        markets_per_connection,
    })
}

/// The daemon's own sort-then-chunk sharding arithmetic, applied to this leg's connections.
///
/// `DaemonConfig::validate` sorts the configured market set and partitions it into
/// fixed-size runs, so which shard holds a market is a property of the set rather than of the
/// order it was typed in. This leg partitions identically, so a size-N rung run against a
/// daemon and run embedded subscribes the same markets on the same connection boundaries.
fn connection_chunks(slugs: &[String], markets_per_connection: usize) -> Vec<Vec<String>> {
    let mut sorted = slugs.to_vec();
    sorted.sort();
    let chunks: Vec<Vec<String>> = sorted
        .chunks(markets_per_connection)
        .map(<[String]>::to_vec)
        .collect();
    if chunks.is_empty() {
        vec![Vec::new()]
    } else {
        chunks
    }
}

fn market_ref(slug: &str) -> Result<MarketRef, String> {
    let venue = Venue::new(VENUE).map_err(|error| format!("{VENUE} is not a venue: {error:?}"))?;
    let key = NativeMarketKey::new(NativeIdentifierKind::slug(), slug)
        .map_err(|error| format!("{slug} is not a market slug: {error:?}"))?;
    Ok(MarketRef::new(venue, key))
}

/// Everything the strategy thread hands back when the run ends.
struct Strategy {
    log: ObsLog,
    /// `observation - venue socket read`, both taken on this process's monotonic clock, in
    /// this process. The engine's whole in-process cost for one venue frame.
    kept: Vec<u64>,
    discarded: u64,
    /// Observations of a state no venue frame drove, which carry no receive stamp to
    /// difference against.
    absent: u64,
}

/// Builds every book, runs the connections, and writes the observation log.
fn run(args: &Args, slugs: Vec<String>) -> Result<(), String> {
    let chunks = connection_chunks(&slugs, args.markets_per_connection);
    let observer_capacity = ObserverCapacity::new(DeliveryProfile::Common.observer_capacity())
        .map_err(|error| format!("the observer capacity is invalid: {error:?}"))?;
    let level_capacity = LevelCapacity::new(MAX_BOOK_LEVELS)
        .map_err(|error| format!("the level capacity is invalid: {error:?}"))?;

    let mut log = ObsLog::new(
        LEG,
        args.label.as_str(),
        git_pin().as_str(),
        slugs.len(),
        OBS_ROW_CAP,
    );
    let mut observers: Vec<(SlugId, BookObserver)> = Vec::with_capacity(slugs.len());
    let mut books: Vec<HashMap<String, BookWriter>> = Vec::with_capacity(chunks.len());
    for chunk in &chunks {
        let mut writers = HashMap::with_capacity(chunk.len());
        for slug in chunk {
            let writer = BookWriter::new(OrderBook::new(market_ref(slug)?), observer_capacity);
            observers.push((log.intern(slug.as_str()), writer.attach()));
            let _replaced = writers.insert(slug.clone(), writer);
        }
        books.push(writers);
    }

    let run_start = Instant::now();
    let (finished, _unused) = watch::channel(false);
    let shared = Arc::new(Mutex::new(Strategy {
        log,
        kept: Vec::new(),
        discarded: 0,
        absent: 0,
    }));
    let strategy = spawn_strategy(
        observers,
        Arc::clone(&shared),
        finished.subscribe(),
        run_start,
    )?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("cannot build the ingest runtime: {error}"))?;
    let outcome = runtime.block_on(ingest_run(
        args,
        chunks,
        books,
        level_capacity,
        run_start,
        &finished,
    ));

    let joined = strategy.join();
    drop(runtime);
    joined.map_err(|_| "the strategy thread panicked".to_owned())?;

    let guard = shared
        .lock()
        .map_err(|_| "the observation log was poisoned by a panicked strategy task".to_owned())?;
    let written = guard.log.write(args.obs_out.as_path());
    print_report(args, &slugs, &outcome, &guard, run_start.elapsed());
    written.map_err(|error| format!("cannot write {}: {error}", args.obs_out.display()))
}

/// What one run's ingest side produced.
#[derive(Default)]
struct Outcome {
    stats: IngestStats,
    frames: u64,
    connections: usize,
    ends: Vec<(usize, ConnectionEndReason)>,
    terminated: bool,
}

/// Dials one connection per chunk, feeds each chunk's books from it, and returns when the
/// run's own duration elapses or this process is asked to terminate.
async fn ingest_run(
    args: &Args,
    chunks: Vec<Vec<String>>,
    books: Vec<HashMap<String, BookWriter>>,
    level_capacity: LevelCapacity,
    run_start: Instant,
    finished: &watch::Sender<bool>,
) -> Outcome {
    let frames = Arc::new(AtomicU64::new(0));
    let ends = Arc::new(Mutex::new(Vec::new()));
    let mut connections = Vec::new();
    let mut ingests = Vec::new();
    let mut controls = Vec::new();
    let template = ShardConfig::default();
    for (index, (markets, writers)) in chunks.into_iter().zip(books).enumerate() {
        if markets.is_empty() {
            continue;
        }
        let (notices, inbox) = mpsc::channel(INGEST_CAPACITY);
        let (control, commands) = mpsc::channel(CONTROL_CAPACITY);
        controls.push(control);
        let config = ConnectionConfig {
            endpoint: args.endpoint.clone(),
            markets,
            setup_timeout: template.setup_timeout,
            capture_path: None,
            min_command_interval: Duration::from_millis(template.min_command_interval_ms),
        };
        let generation = u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1);
        let recorded = Arc::clone(&ends);
        let counted = Arc::clone(&frames);
        connections.push(tokio::spawn(async move {
            let reason = run_connection(generation, config, notices, counted, commands).await;
            if let Ok(mut ends) = recorded.lock() {
                ends.push((index, reason));
            }
        }));
        ingests.push(tokio::spawn(ingest_loop(
            Ingest {
                writers,
                level_capacity,
                run_start,
                generation,
                position: 0,
                stats: IngestStats::default(),
            },
            inbox,
            finished.subscribe(),
        )));
    }
    let established = connections.len();
    let terminated =
        wait_for_end(tokio::time::Instant::now() + Duration::from_secs(args.seconds)).await;
    let _previous = finished.send_replace(true);
    let mut stats = IngestStats::default();
    for ingest in ingests {
        if let Ok(returned) = ingest.await {
            stats.absorb(&returned);
        }
    }
    for connection in &connections {
        connection.abort();
    }
    let ends = ends.lock().map(|ends| ends.clone()).unwrap_or_default();
    Outcome {
        stats,
        frames: frames.load(Ordering::Relaxed),
        connections: established,
        ends,
        terminated,
    }
}

/// Resolves when the run's duration elapses or a termination signal arrives, answering
/// whether a signal ended it.
///
/// Purely reactive: nothing polls a flag. A process whose signal registration fails ends on
/// its duration alone.
async fn wait_for_end(deadline: tokio::time::Instant) -> bool {
    use tokio::signal::unix::{SignalKind, signal};
    let (Ok(mut terminate), Ok(mut interrupt)) = (
        signal(SignalKind::terminate()),
        signal(SignalKind::interrupt()),
    ) else {
        eprintln!("embedded_live: note: no signal handler; the run ends on --seconds alone");
        tokio::time::sleep_until(deadline).await;
        return false;
    };
    tokio::select! {
        _ = tokio::time::sleep_until(deadline) => false,
        _ = terminate.recv() => true,
        _ = interrupt.recv() => true,
    }
}

/// Counters one connection's ingest task keeps, summed across connections for the report.
#[derive(Clone, Copy, Debug, Default)]
struct IngestStats {
    orderbook_updates: u64,
    applied: u64,
    apply_failures: u64,
    candidate_failures: u64,
    provenance_failures: u64,
    unrouted: u64,
    resolutions_closed: u64,
    resolutions_unrouted: u64,
    unknown_events: u64,
    decode_failures: u64,
    decode_failures_book_relevant: u64,
    overload_dropped: u64,
    heartbeats: u64,
    ready: u64,
    resubscribed: u64,
}

impl IngestStats {
    fn absorb(&mut self, other: &Self) {
        self.orderbook_updates += other.orderbook_updates;
        self.applied += other.applied;
        self.apply_failures += other.apply_failures;
        self.candidate_failures += other.candidate_failures;
        self.provenance_failures += other.provenance_failures;
        self.unrouted += other.unrouted;
        self.resolutions_closed += other.resolutions_closed;
        self.resolutions_unrouted += other.resolutions_unrouted;
        self.unknown_events += other.unknown_events;
        self.decode_failures += other.decode_failures;
        self.decode_failures_book_relevant += other.decode_failures_book_relevant;
        self.overload_dropped += other.overload_dropped;
        self.heartbeats += other.heartbeats;
        self.ready += other.ready;
        self.resubscribed += other.resubscribed;
    }
}

/// One connection's books and the counters its ingest keeps.
struct Ingest {
    writers: HashMap<String, BookWriter>,
    level_capacity: LevelCapacity,
    run_start: Instant,
    generation: u64,
    position: u64,
    stats: IngestStats,
}

/// Applies one connection's decoded events to its books until the run ends or the connection
/// stops producing.
async fn ingest_loop(
    mut ingest: Ingest,
    mut inbox: mpsc::Receiver<ConnectionNotice>,
    mut finished: watch::Receiver<bool>,
) -> IngestStats {
    loop {
        tokio::select! {
            _changed = finished.changed() => break,
            notice = inbox.recv() => match notice {
                Some(notice) => handle_notice(&mut ingest, notice),
                None => break,
            },
        }
    }
    ingest.stats
}

fn handle_notice(ingest: &mut Ingest, notice: ConnectionNotice) {
    match notice.note {
        ConnectionNote::Ready { .. } => ingest.stats.ready += 1,
        ConnectionNote::Heartbeat { .. } => ingest.stats.heartbeats += 1,
        ConnectionNote::Resubscribed { .. } => ingest.stats.resubscribed += 1,
        ConnectionNote::Overload { dropped } => ingest.stats.overload_dropped += dropped,
        ConnectionNote::DecodeFailure { book_relevant, .. } => {
            ingest.stats.decode_failures += 1;
            if book_relevant {
                ingest.stats.decode_failures_book_relevant += 1;
            }
        }
        ConnectionNote::Event {
            event,
            received_at,
            subscription_generation,
            ..
        } => match event {
            LimitlessEvent::OrderbookUpdate(update) => {
                apply_update(ingest, &update, received_at, subscription_generation);
            }
            LimitlessEvent::MarketResolved(resolved) => close_market(ingest, resolved.slug()),
            LimitlessEvent::Unknown { .. } => ingest.stats.unknown_events += 1,
        },
    }
}

/// Normalizes one venue book into a snapshot candidate and applies it to that market's own
/// writer, which publishes the revision its observer then reads.
///
/// A market this connection holds no book for is counted and dropped, never a reason to
/// create one. A rejected candidate or apply costs that one market its revision and nothing
/// else.
fn apply_update(
    ingest: &mut Ingest,
    update: &OrderbookUpdate,
    received_at: tokio::time::Instant,
    subscription: u64,
) {
    ingest.stats.orderbook_updates += 1;
    let Some(writer) = ingest.writers.get_mut(update.market_slug()) else {
        ingest.stats.unrouted += 1;
        return;
    };
    ingest.position = ingest.position.saturating_add(1);
    let market = writer.book().market().clone();
    let provenance = match build_provenance(
        market,
        update,
        ingest.position,
        received_at
            .into_std()
            .saturating_duration_since(ingest.run_start),
        ingest.run_start.elapsed(),
        ingest.generation,
        subscription,
    ) {
        Ok(provenance) => provenance,
        Err(()) => {
            ingest.stats.provenance_failures += 1;
            return;
        }
    };
    let candidate = match update.snapshot_candidate(provenance, ingest.level_capacity) {
        Ok(candidate) => candidate,
        Err(_) => {
            ingest.stats.candidate_failures += 1;
            return;
        }
    };
    match writer.apply_snapshot(&candidate) {
        Ok(_commit) => ingest.stats.applied += 1,
        Err(_) => ingest.stats.apply_failures += 1,
    }
}

/// Ends one market's recording on the venue's own resolution report.
///
/// Dropping the writer closes the latest-state surface, which is what ends that market's
/// strategy task. The book is not published as `Unsubscribed` first: that publication is a
/// state change no venue book event produced, and it would appear in this leg's `.obs` log as
/// an observation no other leg made.
fn close_market(ingest: &mut Ingest, slug: &str) {
    match ingest.writers.remove(slug) {
        Some(writer) => {
            drop(writer);
            ingest.stats.resolutions_closed += 1;
        }
        None => ingest.stats.resolutions_unrouted += 1,
    }
}

/// The provenance one venue frame arrived under, built exactly as
/// `crate::limitless::shard`'s own `build_provenance` builds it: the venue's outcome token
/// and `version` lexeme reproduced, the connection and subscription generations the frame was
/// read under, and both local stamps on this process's monotonic clock.
#[allow(clippy::too_many_arguments)]
fn build_provenance(
    market: MarketRef,
    update: &OrderbookUpdate,
    position: u64,
    since_start_at_receive: Duration,
    since_start_at_commit: Duration,
    generation: u64,
    subscription: u64,
) -> Result<Provenance, ()> {
    let outcome = update
        .token_id()
        .map(NativeOutcome::token)
        .transpose()
        .map_err(|_| ())?;
    let source_evidence = match update.version_evidence().map_err(|_| ())? {
        Some(evidence) => {
            BoundedSourceEvidence::new([evidence], SourceEvidenceCapacity::new(1).map_err(|_| ())?)
        }
        None => {
            BoundedSourceEvidence::new(Vec::new(), SourceEvidenceCapacity::new(0).map_err(|_| ())?)
        }
    }
    .map_err(|_| ())?;
    let connection = ConnectionIdentity::new(CONNECTION_NAME, generation).map_err(|_| ())?;
    Provenance::new(ProvenanceInput {
        market,
        outcome,
        native_family: ORDERBOOK_UPDATE_EVENT.to_owned(),
        source_timestamp: Some(SourceTimestamp::new(update.timestamp()).map_err(|_| ())?),
        source_evidence,
        daemon_generation: DAEMON_GENERATION,
        connection,
        subscription_generation: subscription,
        receive_position: position,
        commit_position: position,
        local_receive_time: LocalMonotonicTimestamp::new(duration_nanos(since_start_at_receive)),
        local_commit_time: LocalMonotonicTimestamp::new(duration_nanos(since_start_at_commit)),
        replica: ReplicaRole::PublishingPrimary,
        representation: Representation::VenueNative,
        origin: Origin::SourceReported,
        local_revision: 0,
        continuity_epoch: 0,
    })
    .map_err(|_| ())
}

fn duration_nanos(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
}

/// Starts the strategy thread and hands it every market's observer.
///
/// A thread of its own, with a runtime of its own: the engine's contract is that a consumer
/// reads published state without touching the writer, and a leg that observed on the
/// ingestion thread would be measuring an interleaving no consumer has.
fn spawn_strategy(
    observers: Vec<(SlugId, BookObserver)>,
    shared: Arc<Mutex<Strategy>>,
    finished: watch::Receiver<bool>,
    run_start: Instant,
) -> Result<std::thread::JoinHandle<()>, String> {
    std::thread::Builder::new()
        .name("pmws-strategy".to_owned())
        .spawn(move || {
            let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                eprintln!("embedded_live: the strategy thread has no runtime; nothing is observed");
                return;
            };
            runtime.block_on(async move {
                let mut tasks = Vec::with_capacity(observers.len());
                for (slug, observer) in observers {
                    tasks.push(tokio::spawn(observe_market(
                        observer,
                        slug,
                        Arc::clone(&shared),
                        finished.clone(),
                        run_start,
                    )));
                }
                for task in tasks {
                    let _joined = task.await;
                }
            });
        })
        .map_err(|error| format!("cannot start the strategy thread: {error}"))
}

/// One market's strategy task: wait for a published revision, stamp, then record it.
///
/// The wait is [`BookObserver::state_changed`], the coalescing latest-state surface a
/// strategy consumer actually uses, and `t_obs` is stamped the instant it returns. Everything
/// after the stamp is harness cost. A revision this task already recorded is skipped, so a
/// publication that carries no new book content — a state change the venue did not drive —
/// never becomes a row.
///
/// Ends when the writer goes away, which is what a `marketResolved` produces, or when the run
/// is finished. Nothing polls: both are awaited.
async fn observe_market(
    mut observer: BookObserver,
    slug: SlugId,
    shared: Arc<Mutex<Strategy>>,
    mut finished: watch::Receiver<bool>,
    run_start: Instant,
) {
    let mut last_revision = None;
    loop {
        let published = tokio::select! {
            _changed = finished.changed() => return,
            state = observer.state_changed() => match state {
                Ok(published) => published,
                Err(_gone) => return,
            },
        };
        let observed = now_epoch_nanos();
        let since_start = duration_nanos(run_start.elapsed());
        let revision = published.revision();
        if last_revision == Some(revision) {
            continue;
        }
        last_revision = Some(revision);
        let digest = level_digest(published.canonical_levels());
        let received = published
            .provenance()
            .map(|provenance| provenance.local_receive_time().value());
        let Ok(mut strategy) = shared.lock() else {
            return;
        };
        let _seq = strategy.log.record(slug, observed, digest, revision);
        match received {
            None => strategy.absent += 1,
            Some(received)
                if since_start >= received && since_start - received < IMPLAUSIBLE_NANOS =>
            {
                strategy.kept.push(since_start - received);
            }
            Some(_) => strategy.discarded += 1,
        }
    }
}

fn print_report(
    args: &Args,
    slugs: &[String],
    outcome: &Outcome,
    strategy: &Strategy,
    elapsed: Duration,
) {
    let stats = &outcome.stats;
    println!("label: {}", args.label);
    println!("leg: {LEG}");
    println!("endpoint: {}", args.endpoint);
    println!("duration_seconds: {:.3}", elapsed.as_secs_f64());
    println!("terminated_by_signal: {}", outcome.terminated);
    println!("markets_requested: {}", slugs.len());
    println!("markets_per_connection: {}", args.markets_per_connection);
    println!("connections: {}", outcome.connections);
    println!(
        "observer_capacity: {}",
        DeliveryProfile::Common.observer_capacity()
    );
    println!("level_capacity: {MAX_BOOK_LEVELS}");
    println!("frames_read: {}", outcome.frames);
    println!("connections_ready: {}", stats.ready);
    println!("heartbeats: {}", stats.heartbeats);
    println!("resubscribes: {}", stats.resubscribed);
    println!("orderbook_updates: {}", stats.orderbook_updates);
    println!("applied: {}", stats.applied);
    println!("apply_failures: {}", stats.apply_failures);
    println!("candidate_failures: {}", stats.candidate_failures);
    println!("provenance_failures: {}", stats.provenance_failures);
    println!("frames_unrouted: {}", stats.unrouted);
    println!("unknown_events: {}", stats.unknown_events);
    println!("decode_failures: {}", stats.decode_failures);
    println!(
        "decode_failures_book_relevant: {}",
        stats.decode_failures_book_relevant
    );
    println!("notice_overload_dropped: {}", stats.overload_dropped);
    println!("resolutions_closed: {}", stats.resolutions_closed);
    println!("resolutions_unrouted: {}", stats.resolutions_unrouted);
    println!("connections_ended: {}", outcome.ends.len());
    for (index, reason) in &outcome.ends {
        println!("connection_end: {index} {reason:?}");
    }
    println!(
        "reconnect_policy: none; a connection that ends is not redialled, because reconnect \
         policy belongs to the supervisor and this leg rides one connection generation"
    );
    println!("markets_observed: {}", strategy.log.markets_observed());
    println!("obs_out: {}", args.obs_out.display());
    println!("obs_rows: {}", strategy.log.rows());
    println!("obs_events_total: {}", strategy.log.events_total());
    println!("obs_dropped: {}", strategy.log.dropped());
    let seconds = elapsed.as_secs_f64();
    let rate = if seconds > 0.0 {
        strategy.log.events_total() as f64 / seconds
    } else {
        0.0
    };
    println!("observations_per_second: {rate:.3}");
    println!(
        "engine_latency: observation - venue socket read, both on this process's monotonic \
         clock; the in-process analogue of the shm leg's end_to_end"
    );
    print_distribution("engine", &strategy.kept, strategy.discarded);
    println!("engine_samples_absent: {}", strategy.absent);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cli(args: &[&str]) -> impl Iterator<Item = String> {
        std::iter::once("embedded_live".to_owned()).chain(args.iter().map(|arg| (*arg).to_owned()))
    }

    fn complete() -> Vec<&'static str> {
        vec![
            "--slugs",
            "/tmp/all-active.txt",
            "--obs-out",
            "/tmp/rust-embedded.obs",
            "--label",
            "m1, all-active",
        ]
    }

    #[test]
    fn parse_args_defaults_the_endpoint_and_the_daemons_own_shard_width() {
        let args = parse_args(cli(&complete())).expect("the contract's flags parse");
        assert_eq!(args.slugs, PathBuf::from("/tmp/all-active.txt"));
        assert_eq!(args.obs_out, PathBuf::from("/tmp/rust-embedded.obs"));
        assert_eq!(args.label, "m1, all-active");
        assert_eq!(args.seconds, 600);
        assert_eq!(args.endpoint, DEFAULT_ENDPOINT);
        assert_eq!(args.markets_per_connection, DEFAULT_MARKETS_PER_SHARD);
    }

    #[test]
    fn parse_args_accepts_the_optional_flags_and_refuses_a_non_websocket_endpoint() {
        let mut flags = complete();
        flags.extend([
            "--seconds",
            "240",
            "--endpoint",
            "ws://127.0.0.1:9/socket.io/",
            "--markets-per-connection",
            "20",
        ]);
        let args = parse_args(cli(&flags)).expect("the optional flags parse");
        assert_eq!(args.seconds, 240);
        assert_eq!(args.endpoint, "ws://127.0.0.1:9/socket.io/");
        assert_eq!(args.markets_per_connection, 20);

        let mut wrong = complete();
        wrong.extend(["--endpoint", "https://ws.limitless.exchange/"]);
        assert!(
            parse_args(cli(&wrong))
                .expect_err("a non-WebSocket endpoint is refused")
                .contains("--endpoint")
        );

        let mut zero = complete();
        zero.extend(["--markets-per-connection", "0"]);
        assert!(
            parse_args(cli(&zero))
                .expect_err("a zero width is refused")
                .contains("--markets-per-connection")
        );
    }

    #[test]
    fn parse_args_requires_every_mandatory_flag() {
        for missing in ["--slugs", "--obs-out", "--label"] {
            let flags: Vec<&str> = complete()
                .chunks(2)
                .filter(|pair| pair[0] != missing)
                .flatten()
                .copied()
                .collect();
            let error = parse_args(cli(&flags)).expect_err("a missing flag is refused");
            assert!(error.contains(missing), "{missing}: {error}");
        }
    }

    /// The requirement `bench/sdk-harness/README.md` states — this leg "mirrors the daemon's
    /// own sort-then-chunk sharding arithmetic" — asserted against the daemon itself rather
    /// than against a restatement of it.
    #[test]
    fn the_chunking_is_the_daemons_own_partition_of_the_same_set() {
        let unsorted = ["d-market", "a-market", "c-market", "b-market", "e-market"];
        let quoted: Vec<String> = unsorted.iter().map(|slug| format!("\"{slug}\"")).collect();
        let document = format!(
            "control_socket = \"/tmp/pmwsd-embedded-live-test.sock\"\n\
             markets = [{}]\nmarkets_per_shard = 2\n",
            quoted.join(", ")
        );
        let plan = pm_ws::DaemonConfig::parse(&document).expect("the document is valid");
        let daemon: Vec<Vec<String>> = plan
            .shards
            .iter()
            .map(|shard| shard.markets.clone())
            .collect();
        let slugs: Vec<String> = unsorted.iter().map(|slug| (*slug).to_owned()).collect();
        assert_eq!(connection_chunks(&slugs, 2), daemon);
        assert_eq!(
            daemon[0],
            vec!["a-market".to_owned(), "b-market".to_owned()],
            "the partition is by sorted slug, so the order the set was given in steers nothing"
        );
    }

    /// The daemon's own default width is what this leg partitions at when none is given, so
    /// an embedded run and a daemon run of one set subscribe the same connection boundaries.
    #[test]
    fn the_default_width_chunks_a_set_the_way_a_default_daemon_shards_it() {
        let slugs: Vec<String> = (0..300).map(|index| format!("market-{index:04}")).collect();
        let chunks = connection_chunks(&slugs, DEFAULT_MARKETS_PER_SHARD);
        assert_eq!(chunks.len(), 300_usize.div_ceil(DEFAULT_MARKETS_PER_SHARD));
        assert_eq!(chunks[0].len(), DEFAULT_MARKETS_PER_SHARD);
        assert_eq!(chunks[0][0], "market-0000");
        let carried: usize = chunks.iter().map(Vec::len).sum();
        assert_eq!(carried, slugs.len(), "every market is carried exactly once");
    }

    /// An empty set still produces one connection slot, exactly as a configuration with no
    /// market still produces one shard; that slot is skipped rather than dialled.
    #[test]
    fn an_empty_set_produces_one_empty_chunk() {
        let empty: Vec<String> = Vec::new();
        assert_eq!(
            connection_chunks(&empty, DEFAULT_MARKETS_PER_SHARD),
            vec![Vec::<String>::new()]
        );
    }

    #[test]
    fn a_market_reference_is_venue_scoped_and_venue_native() {
        let market = market_ref("btc-up-or-down-5-min-1").expect("a slug resolves");
        assert_eq!(market.venue().as_str(), VENUE);
        assert_eq!(market.key().value(), "btc-up-or-down-5-min-1");
        assert!(market_ref("").is_err(), "an empty slug is not a market");
    }
}

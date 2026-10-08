//! Passive latency probe: attaches read-only to an EXISTING shared-memory segment, either
//! directly by path or by descriptor transfer through a daemon's control socket, and reports
//! the socket-arrival -> consumer-observable latency distribution, in both consumer modes
//! below.
//!
//! This process never creates a segment and never publishes into one — it is instrumentation
//! for a live daemon someone else already started (the single-market `pmws-run` binary, or a
//! `pmwsd` shard), never a load generator. Every kept sample differs this process's own wall
//! clock against the arrival stamp `BookSnapshot::arrival_time_nanos` carries — the
//! wall-clock instant the venue's socket frame was read, threaded through the writer exactly
//! as `commit_time` is — under the same discard-not-clamp rule `examples/shm_latency.rs` uses
//! for its own commit-to-reader deltas: a backwards or absurd delta is what a clock adjustment
//! between the stamp and the read produces, and is discarded and counted rather than clamped.
//!
//! Two mutually exclusive attachment modes, chosen by which flags are given:
//! - `--shm <path>` opens an existing segment file directly and attaches read-only
//!   (`SegmentReader::attach`) — the shape a `pmws-run` single-market process is watched
//!   with, and a path this probe never creates.
//! - `--control <socket-path> --market <slug>` attaches the way a production consumer does:
//!   connect to a `pmwsd` control socket, send one `{"command":"attach","market":...}` line,
//!   receive the answer and its one transferred descriptor
//!   (`pm_ws::recv_with_fds`), and validate the descriptor's segment header against what the
//!   answer promised — daemon instance, segment generation, and doorbell placement — before
//!   trusting a byte of it. That connection is then held open for the whole run by a keeper
//!   thread of its own, which renews it inside the lease TTL the same answer declared,
//!   because the connection *is* the lease: the daemon subscribes a market nothing else wants
//!   in order to serve the attach and unsubscribes it when the session closes or its TTL
//!   expires, so a probe that dropped the socket after reading its descriptor — or renewed it
//!   more slowly than the daemon's own deadline — would spend its run measuring a market it
//!   had just had removed, a segment that stays mapped and readable and simply stops
//!   changing. The keeper is a thread rather than a step in the sampling loop so that no
//!   control syscall is ever charged to a measured observation. This is the same conversation
//!   `src/ffi/mod.rs`'s `pmws_connect`
//!   drives for every non-Rust consumer, reimplemented here against the same public
//!   `pm_ws` surfaces because that module's own `connect_session`/`read_attachment` are
//!   private to it. A segment whose header declares a page-placement doorbell can never be
//!   parked on this way — the control channel never transfers that page's descriptor, only
//!   the segment's own — so a `parked`-mode run attached this way falls back to `spin` and
//!   says so with a `note:` line and a `mode_effective:` line in the report, rather than
//!   silently measuring a mode it did not actually run.
//!
//! Two consumer modes share one drain loop, whichever attachment mode produced the reader:
//! - `parked` spins for `--spin-micros` then parks on the segment's doorbell
//!   (`SegmentReader::wait_for_publication`), waking on a real publish.
//! - `spin` never parks: a tight acquire-load loop on
//!   `SegmentReader::publication_generation` alone, reporting the pure busy-poll floor with
//!   no syscall anywhere in its path.
//!
//! Either way, a woken or generation-changed round drains the segment's dirty-index ring
//! (`SegmentReader::next_dirty`): a `Delivered` entry names a changed market by
//! `directory_index`, which this probe turns into a readable handle with
//! `SegmentReader::resolve_directory_index`, needed because `SegmentReader::resolve` can only
//! look a market up by a venue-native identity the caller already holds, and a dirty entry
//! names none — only the segment's own directory record ties a bare `directory_index` back
//! to a slot. A `Rescan`
//! means the writer lapped the ring past this probe's cursor; the recovery taken here is to
//! re-read every published entry the segment's directory holds, walked by index. Re-reading
//! only the markets this probe had already been delivered would be the wrong recovery: an
//! overrun is exactly the case where a market's one and only dirty entry was overwritten
//! before this probe ever saw it, and a market whose entry is lost that way would then be
//! excluded from the sampled workload for as long as it kept changing without ever being the
//! entry a poll happened to land on. The directory is bounded and enumerable, so the rescan
//! enumerates it.
//!
//! Every state read is deduplicated by `(directory_index, book_revision)` against the last
//! revision this probe actually sampled for that market, so a rescan pass can never
//! double-count a revision a delivered entry already sampled — a resolution republish
//! advertises the same revision on purpose, and must never look like a second, faster event.
//!
//! Live invocation (no synthetic load; this binary only ever attaches, never publishes) —
//! path mode, against a real single-market `pmws-run` process:
//!
//! ```text
//! cargo run --release --bin pmws-run -- --market btc-up-or-down-5-min-<epoch> \
//!     --shm /tmp/pmws-live.seg
//! cargo run --release --example latency_probe -- \
//!     --shm /tmp/pmws-live.seg --mode parked --seconds 60 \
//!     --label "<machine>, pmws-run single-market, live limitless traffic"
//! ```
//!
//! Connect mode, against a real `pmwsd` shard (`control_socket` is whatever the daemon's own
//! config names):
//!
//! ```text
//! cargo run --release --bin pmwsd -- --config /tmp/pmwsd.toml
//! cargo run --release --example latency_probe -- \
//!     --control /tmp/pmwsd.sock --market btc-up-or-down-5-min-<epoch> \
//!     --mode parked --seconds 60 \
//!     --label "<machine>, pmwsd shard, live limitless traffic"
//! ```
//!
//! Swap `--mode parked` for `--mode spin` to report the spin floor on the same segment, and
//! pass `--spin-micros <n>` to measure parked mode's spin-then-park budget instead of its
//! pure-parked (`0`) default.

#[path = "common/matched_obs.rs"]
mod matched_obs;

use matched_obs::{OBS_ROW_CAP, ObsLog, git_pin, level_digest, now_epoch_nanos};
use pm_ws::{
    Attachment, ControlRequest, ControlResponse, DirtyCursor, DirtyPoll, DoorbellLocation,
    FEATURE_DOORBELL_IN_HEADER, FEATURE_DOORBELL_PAGE, MAX_CONTROL_LINE_BYTES,
    MAX_TRANSFERRED_DESCRIPTORS, SegmentReader, SegmentRegion, WaitOutcome, recv_with_fds,
};
use std::collections::{BTreeSet, HashMap};
use std::io::{Read, Write};
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// The `leg` name this probe's own `.obs` rows carry, when `--obs-out` is given.
const OBS_LEG: &str = "latency-probe";

/// A delta at or beyond this is a clock artifact, not a latency: discarded, never clamped,
/// exactly as `examples/shm_latency.rs` treats its own commit-to-reader deltas.
const IMPLAUSIBLE_NANOS: u64 = 1_000_000_000;

/// The fewest kept samples a reported distribution may rest on — mirrors
/// `examples/shm_latency.rs`'s own floor. A run below it withholds percentiles rather than
/// print a p99.9 that is one or two outliers dressed up as a distribution.
const MIN_REPORTABLE_SAMPLES: usize = 1_000;

/// The fewest kept samples a p99.99 may be printed from. Nearest-rank at this depth reads
/// the worst two of twenty thousand; below the floor the cell is omitted rather than a
/// couple of outliers dressed up as a distribution, the same reasoning as
/// [`MIN_REPORTABLE_SAMPLES`] applied one order deeper.
const MIN_P9999_SAMPLES: usize = 20_000;

/// How long one parked-mode wait blocks before this probe re-checks its own `--seconds`
/// deadline. Bounds how late the process notices the run is over; never a measurement. A
/// quiet segment therefore accrues roughly one reported `timeouts` count per this interval —
/// that counter measures quiet time in parked mode, not faults; spin mode's own `timeouts`
/// means something structurally different (see `print_report`) and the two are never
/// comparable numbers.
const PARK_POLL_TIMEOUT: Duration = Duration::from_millis(100);

/// The most seconds a single run may be asked to last.
const MAX_SECONDS: u64 = 86_400;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Parked,
    Spin,
}

fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::Parked => "parked",
        Mode::Spin => "spin",
    }
}

/// Which segment this run attaches to, and how: exactly one of a direct path or a control
/// socket plus the market to ask it for — never both, never neither.
#[derive(Debug)]
enum Source {
    Shm(PathBuf),
    Control { control: PathBuf, market: String },
}

#[derive(Debug)]
struct Args {
    source: Source,
    mode: Mode,
    spin_micros: u64,
    seconds: u64,
    label: String,
    /// Additive and optional: when given, one `.obs` row
    /// (`examples/common/matched_obs.rs`) is appended per distinct revision this probe
    /// samples, alongside the latency percentiles this probe has always reported. Absent —
    /// the default — this probe's behavior and output are byte-identical to before this flag
    /// existed.
    obs_out: Option<PathBuf>,
}

const USAGE: &str = "usage: latency_probe (--shm <path> | --control <socket-path> --market <slug>) \
                      [--mode parked|spin] [--spin-micros <n>] [--seconds <n>] \
                      --label \"<machine, workload>\" [--obs-out <path>]";

fn main() {
    let args = match parse_args(std::env::args()) {
        Ok(args) => args,
        Err(message) => {
            eprintln!("latency_probe: {message}");
            eprintln!("{USAGE}");
            std::process::exit(2);
        }
    };
    let (reader, mode_effective, note, mut lease) = match &args.source {
        Source::Shm(path) => match attach(path) {
            Ok(reader) => (reader, args.mode, None, None),
            Err(message) => {
                eprintln!("latency_probe: {message}");
                std::process::exit(1);
            }
        },
        Source::Control { control, market } => match attach_via_control(control, market) {
            Ok((reader, doorbell, lease)) => {
                let parking_unavailable =
                    args.mode == Mode::Parked && doorbell == DoorbellLocation::Page;
                let note = parking_unavailable.then(|| {
                    "parking is unavailable over this attachment: its doorbell is in a sibling \
                     page, and the control channel never transfers that page's descriptor \
                     (only the segment's own); falling back to spin measurement"
                        .to_owned()
                });
                let effective = if parking_unavailable {
                    Mode::Spin
                } else {
                    args.mode
                };
                (reader, effective, note, Some(lease))
            }
            Err(message) => {
                eprintln!("latency_probe: {message}");
                std::process::exit(1);
            }
        },
    };
    if let Some(note) = note.as_deref() {
        eprintln!("latency_probe: note: {note}");
    }
    let show_mode_effective = matches!(args.source, Source::Control { .. });
    let mut obs_log = args.obs_out.as_ref().map(|_| {
        ObsLog::new(
            OBS_LEG,
            args.label.as_str(),
            git_pin().as_str(),
            1,
            OBS_ROW_CAP,
        )
    });
    let mut report = run(&reader, &args, mode_effective, &mut obs_log);
    if let Some(lease) = lease.as_mut() {
        let (renewed, failed) = lease.finish();
        report.renewals = renewed;
        report.renew_failures = failed;
    }
    let mut obs_write_failed = false;
    if let Some(path) = args.obs_out.as_ref()
        && let Some(log) = obs_log.as_ref()
        && let Err(error) = log.write(path)
    {
        eprintln!("latency_probe: cannot write {}: {error}", path.display());
        obs_write_failed = true;
    }
    print_report(
        &args,
        &report,
        mode_effective,
        show_mode_effective,
        note.as_deref(),
        obs_log.as_ref(),
    );
    if obs_write_failed {
        std::process::exit(1);
    }
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let _binary = args.next();
    let mut shm = None;
    let mut control = None;
    let mut market = None;
    let mut mode = Mode::Parked;
    let mut spin_micros = 0_u64;
    let mut seconds = 60_u64;
    let mut label = None;
    let mut obs_out = None;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--shm" => shm = Some(PathBuf::from(args.next().ok_or("--shm requires a value")?)),
            "--control" => {
                control = Some(PathBuf::from(
                    args.next().ok_or("--control requires a value")?,
                ));
            }
            "--market" => market = Some(args.next().ok_or("--market requires a value")?),
            "--mode" => {
                let value = args.next().ok_or("--mode requires a value")?;
                mode = match value.as_str() {
                    "parked" => Mode::Parked,
                    "spin" => Mode::Spin,
                    other => return Err(format!("--mode must be parked or spin, got {other}")),
                };
            }
            "--spin-micros" => {
                let value = args.next().ok_or("--spin-micros requires a value")?;
                spin_micros = value
                    .parse()
                    .map_err(|_| "--spin-micros takes a non-negative integer".to_owned())?;
            }
            "--seconds" => {
                let value = args.next().ok_or("--seconds requires a value")?;
                seconds = value
                    .parse()
                    .map_err(|_| "--seconds takes a positive integer".to_owned())?;
                if seconds == 0 || seconds > MAX_SECONDS {
                    return Err(format!("--seconds must be between 1 and {MAX_SECONDS}"));
                }
            }
            "--label" => label = Some(args.next().ok_or("--label requires a value")?),
            "--obs-out" => {
                obs_out = Some(PathBuf::from(
                    args.next().ok_or("--obs-out requires a value")?,
                ));
            }
            other => return Err(format!("unknown flag {other}")),
        }
    }
    let source = match (shm, control, market) {
        (Some(path), None, None) => Source::Shm(path),
        (None, Some(control), Some(market)) => Source::Control { control, market },
        (Some(_), _, _) => {
            return Err("--shm is mutually exclusive with --control and --market".to_owned());
        }
        (None, Some(_), None) => return Err("--control requires --market".to_owned()),
        (None, None, Some(_)) => return Err("--market requires --control".to_owned()),
        (None, None, None) => {
            return Err("either --shm or --control together with --market is required".to_owned());
        }
    };
    Ok(Args {
        source,
        mode,
        spin_micros,
        seconds,
        label: label.ok_or("--label is required (the machine and the workload)")?,
        obs_out,
    })
}

/// Opens and validates the segment at `path`. Never creates one: a missing or malformed
/// segment is this probe's caller's problem to fix, not this probe's to paper over.
fn attach(path: &Path) -> Result<SegmentReader, String> {
    let region = SegmentRegion::open_file(path)
        .map_err(|error| format!("cannot open {}: {error}", path.display()))?;
    SegmentReader::attach(Arc::new(region))
        .map_err(|error| format!("{} is not a readable segment: {error}", path.display()))
}

/// How long the whole control-socket attach conversation — send the request, receive the
/// answer and its descriptor — is allowed to run for, starting once the connection is
/// established. Mirrors `src/ffi/mod.rs`'s own `ATTACH_TIMEOUT` for `pmws_connect`, which
/// this function otherwise reimplements against the same public `pm_ws` surfaces: that
/// module's `connect_session` and `read_attachment` are private to it, so this probe cannot
/// call them directly.
const CONTROL_ATTACH_TIMEOUT: Duration = Duration::from_secs(5);

/// Attaches to the segment carrying `market` by asking the daemon listening on `control`'s
/// Unix socket for it: one request line out, one answer line and its one descriptor back,
/// the descriptor's segment header validated against what the answer promised before this
/// process trusts a byte read through it. Exactly the production consumer path
/// `src/ffi/mod.rs`'s `pmws_connect` drives.
///
/// Never opens a segment by name and never creates one: the transferred descriptor is the
/// only thing this process ever reads through. Returns the segment's own reader, the doorbell
/// placement the answer promised — so the caller can decide whether a parked-mode request is
/// actually satisfiable over this attachment — and the [`ControlLease`] the attach was served
/// on, already renewing itself on the cadence the answer's own declared TTL implies, which
/// the caller must hold for as long as it means to measure this market.
fn attach_via_control(
    control: &Path,
    market: &str,
) -> Result<(SegmentReader, DoorbellLocation, ControlLease), String> {
    let request = pm_ws::encode_line(&ControlRequest::Attach {
        market: market.to_owned(),
    })
    .map_err(|error| format!("cannot encode an attach request: {error}"))?;
    if request.len() > MAX_CONTROL_LINE_BYTES {
        return Err(format!(
            "market identifier does not fit one control line (bounded at \
             {MAX_CONTROL_LINE_BYTES} bytes)"
        ));
    }
    let mut socket = UnixStream::connect(control).map_err(|error| {
        format!(
            "cannot connect to control socket {}: {error}",
            control.display()
        )
    })?;
    let deadline = Instant::now() + CONTROL_ATTACH_TIMEOUT;
    write_attach_request(&mut socket, request.as_bytes(), deadline)?;
    let (line, descriptors) = read_attach_answer(&mut socket, deadline)?;
    let attachment = match serde_json::from_str::<ControlResponse>(line.trim_end()) {
        Ok(ControlResponse::Attached { attachment }) => attachment,
        Ok(ControlResponse::Markets { markets }) => {
            return Err(format!("daemon refused market {market}: {markets:?}"));
        }
        Ok(ControlResponse::Error { message }) => {
            return Err(format!("daemon refused the attach: {message}"));
        }
        Ok(ControlResponse::Busy { message }) => {
            return Err(format!("daemon's control queue was busy: {message}"));
        }
        Ok(
            ControlResponse::Status { .. }
            | ControlResponse::Renewed { .. }
            | ControlResponse::Released { .. },
        ) => {
            return Err("daemon answered a status line to an attach request".to_owned());
        }
        Err(error) => {
            return Err(format!(
                "attach answer is not a control response: {error}: {line}"
            ));
        }
    };
    let (reader, doorbell) = build_reader(&attachment, descriptors)?;
    let lease = ControlLease::start(socket, attachment.lease_ttl_ms)?;
    Ok((reader, doorbell, lease))
}

/// The longest this probe ever leaves a control session silent, whatever the daemon's TTL.
///
/// A daemon that expires nothing needs no renewal at all; one line every five seconds is what
/// makes a session that *would* be expired by a daemon reconfigured mid-run — or by a daemon
/// this probe misread — fail in seconds rather than at the end of a long measurement.
const MAX_RENEW_INTERVAL: Duration = Duration::from_secs(5);

/// The shortest renewal cadence this probe will drive itself at.
///
/// `pm_ws::daemon::MIN_LEASE_TTL_MS` keeps a conforming daemon's declared TTL far above this,
/// so what this bounds is a daemon that declared something smaller than its own configuration
/// accepts: the renewals stay a bounded trickle of one line rather than becoming a write loop
/// beside a measurement.
const MIN_RENEW_INTERVAL: Duration = Duration::from_millis(100);

/// What fraction of the declared TTL a renewal is sent inside.
///
/// The daemon measures silence from the request it last answered, so a renewal sent *at* the
/// deadline has already lost to a sweep that is due; a third leaves room for two consecutive
/// renewals to be late and the lease still to hold.
const RENEW_TTL_DIVISOR: u32 = 3;

/// How long one renewal exchange — the line out and the acknowledgement back — may take.
///
/// Never longer than the cadence itself: an exchange that outlived the interval it was sent
/// on would push the next renewal past the deadline it exists to stay inside.
fn renew_exchange_budget(interval: Duration) -> Duration {
    interval.min(Duration::from_secs(1))
}

/// How often the keeper thread looks at its stop flag while it waits for the next renewal.
/// Bounds how long a finished run waits for the keeper to notice; never a measurement.
const KEEPER_STOP_POLL: Duration = Duration::from_millis(50);

/// The cadence a lease declared as `lease_ttl_ms` is renewed on: a fraction of the TTL, inside
/// this probe's own bounds, and the ceiling for a daemon that expires nothing (`0`).
fn renew_interval(lease_ttl_ms: u64) -> Duration {
    if lease_ttl_ms == 0 {
        return MAX_RENEW_INTERVAL;
    }
    (Duration::from_millis(lease_ttl_ms) / RENEW_TTL_DIVISOR)
        .clamp(MIN_RENEW_INTERVAL, MAX_RENEW_INTERVAL)
}

/// Acknowledged renewals and failed exchanges, as the keeper thread reports them to the run
/// that ends it.
#[derive(Default)]
struct LeaseCounters {
    renewed: AtomicU64,
    failed: AtomicU64,
}

/// The control session an attachment was served on, held open for the whole run by a keeper
/// thread of its own.
///
/// The connection is the lease. `pmwsd` subscribes a market nothing else wants in order to
/// serve the attach and releases it when the session closes, so this guard is the difference
/// between measuring a market and measuring a segment nobody is publishing into any more.
///
/// The keeper is a separate thread, and that placement is the measurement's: renewal syscalls
/// on the sampling thread would land inside the socket-arrival-to-observation interval this
/// probe exists to report, and an update arriving during one would be charged the control
/// exchange's time. Nothing on the measured path touches this session — the counters cross
/// back as atomics, read once when the run is over.
struct ControlLease {
    stop: Arc<AtomicBool>,
    counters: Arc<LeaseCounters>,
    keeper: Option<std::thread::JoinHandle<()>>,
}

impl ControlLease {
    /// Takes the attach conversation's socket and starts renewing it on the cadence the
    /// daemon's own declared TTL implies.
    ///
    /// The socket is left blocking with an explicit timeout on both directions: the keeper is
    /// off the measured path, so bounded blocking I/O is what it wants, and the attach
    /// exchange's own leftover deadlines are replaced rather than inherited.
    fn start(socket: UnixStream, lease_ttl_ms: u64) -> Result<Self, String> {
        let interval = renew_interval(lease_ttl_ms);
        let budget = renew_exchange_budget(interval);
        socket
            .set_nonblocking(false)
            .map_err(|error| format!("cannot make the control session blocking: {error}"))?;
        socket
            .set_write_timeout(Some(budget))
            .map_err(|error| format!("cannot bound the control session's writes: {error}"))?;
        socket
            .set_read_timeout(Some(budget))
            .map_err(|error| format!("cannot bound the control session's reads: {error}"))?;
        let stop = Arc::new(AtomicBool::new(false));
        let counters = Arc::new(LeaseCounters::default());
        let keeper = std::thread::Builder::new()
            .name("pmws-lease-keeper".to_owned())
            .spawn({
                let stop = Arc::clone(&stop);
                let counters = Arc::clone(&counters);
                move || {
                    Keeper {
                        socket,
                        pending: Vec::new(),
                        budget,
                        counters,
                        noted: false,
                    }
                    .run(&stop, interval);
                }
            })
            .map_err(|error| format!("cannot start the lease keeper: {error}"))?;
        Ok(Self {
            stop,
            counters,
            keeper: Some(keeper),
        })
    }

    /// Ends the keeper and reports the acknowledged renewals and the failed exchanges.
    ///
    /// The session closes with the keeper thread, which is what releases the lease, so this is
    /// called when the measurement is over and never before it.
    fn finish(&mut self) -> (u64, u64) {
        self.stop_keeper();
        (
            self.counters.renewed.load(Ordering::Relaxed),
            self.counters.failed.load(Ordering::Relaxed),
        )
    }

    fn stop_keeper(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(keeper) = self.keeper.take() {
            let _joined = keeper.join();
        }
    }
}

impl Drop for ControlLease {
    fn drop(&mut self) {
        self.stop_keeper();
    }
}

/// Why one renewal exchange did not end in an acknowledgement.
enum RenewFault {
    /// The daemon closed the session. Every lease it held went with it and no later exchange
    /// on this socket can succeed, so the keeper stops rather than counting the same loss
    /// once per cadence.
    SessionLost(String),
    /// This exchange failed and the session may well still be there: counted, said once, and
    /// tried again on the next cadence.
    Exchange(String),
}

/// The keeper thread's own state: the leased session, whatever bytes of an answer have
/// arrived, and the counters the run reads when it ends.
struct Keeper {
    socket: UnixStream,
    /// Bytes read that are not yet a complete answer line. A conforming daemon answers one
    /// line per request, so this is normally empty between renewals; it exists so a partial
    /// read is resumed rather than mistaken for a malformed answer.
    pending: Vec<u8>,
    budget: Duration,
    counters: Arc<LeaseCounters>,
    noted: bool,
}

impl Keeper {
    /// Renews on `interval` until the run stops it or the session is lost.
    ///
    /// Exactly one renewal is outstanding at a time — one line out, one acknowledgement back —
    /// so nothing here can queue work on the daemon faster than it answers, and a stopped run
    /// waits at most [`KEEPER_STOP_POLL`] plus one exchange budget for this thread to end.
    fn run(mut self, stop: &AtomicBool, interval: Duration) {
        let mut due = Instant::now() + interval;
        while !stop.load(Ordering::Relaxed) {
            let now = Instant::now();
            let Some(left) = due.checked_duration_since(now) else {
                due = now + interval;
                if self.renew_once() == Keeping::Stopped {
                    return;
                }
                continue;
            };
            std::thread::sleep(left.min(KEEPER_STOP_POLL));
        }
    }

    fn renew_once(&mut self) -> Keeping {
        match self.exchange() {
            Ok(()) => {
                let _renewed = self.counters.renewed.fetch_add(1, Ordering::Relaxed);
                Keeping::Renewing
            }
            Err(RenewFault::Exchange(message)) => {
                let _failed = self.counters.failed.fetch_add(1, Ordering::Relaxed);
                self.note(format!(
                    "renewal failed: {message}; the measurement continues, and the attach \
                     socket still holds the lease"
                ));
                Keeping::Renewing
            }
            Err(RenewFault::SessionLost(message)) => {
                let _failed = self.counters.failed.fetch_add(1, Ordering::Relaxed);
                self.note(message);
                Keeping::Stopped
            }
        }
    }

    /// One renewal: the request line out, then exactly one answer line back, parsed. Only
    /// [`ControlResponse::Renewed`] is an acknowledgement — a write that the kernel accepted
    /// says nothing about a daemon that had already decided to expire this session.
    fn exchange(&mut self) -> Result<(), RenewFault> {
        let line = pm_ws::encode_line(&ControlRequest::Renew)
            .map_err(|error| RenewFault::Exchange(format!("cannot encode a renewal: {error}")))?;
        let deadline = Instant::now() + self.budget;
        self.socket
            .write_all(line.as_bytes())
            .map_err(|error| RenewFault::Exchange(format!("cannot write the renewal: {error}")))?;
        let answer = self.read_answer(deadline)?;
        match serde_json::from_str::<ControlResponse>(answer.trim_end()) {
            Ok(ControlResponse::Renewed { .. }) => Ok(()),
            Ok(other) => Err(RenewFault::Exchange(format!(
                "the daemon answered {other:?} to a renewal"
            ))),
            Err(error) => Err(RenewFault::Exchange(format!(
                "the daemon's renewal answer is not a control response: {error}: {answer}"
            ))),
        }
    }

    /// Reads exactly one answer line, bounded twice over: by `deadline`, and by the protocol's
    /// own line cap, past which a peer sending bytes with no newline in them is not answering
    /// this request at all.
    fn read_answer(&mut self, deadline: Instant) -> Result<String, RenewFault> {
        loop {
            if let Some(end) = self.pending.iter().position(|byte| *byte == b'\n') {
                let line: Vec<u8> = self.pending.drain(..=end).collect();
                return String::from_utf8(line)
                    .map(|text| text.trim_end().to_owned())
                    .map_err(|error| {
                        RenewFault::Exchange(format!("the renewal answer is not utf-8: {error}"))
                    });
            }
            if self.pending.len() >= MAX_CONTROL_LINE_BYTES {
                return Err(RenewFault::Exchange(format!(
                    "no renewal answer in {MAX_CONTROL_LINE_BYTES} bytes"
                )));
            }
            let left = deadline
                .checked_duration_since(Instant::now())
                .filter(|left| !left.is_zero())
                .ok_or_else(|| {
                    RenewFault::Exchange("the renewal exchange exceeded its bound".to_owned())
                })?;
            self.socket.set_read_timeout(Some(left)).map_err(|error| {
                RenewFault::Exchange(format!("cannot bound the renewal read: {error}"))
            })?;
            let mut chunk = [0_u8; 512];
            match self.socket.read(&mut chunk) {
                Ok(0) => {
                    return Err(RenewFault::SessionLost(
                        "the daemon closed the control session; the lease it held is gone"
                            .to_owned(),
                    ));
                }
                Ok(read) => self.pending.extend_from_slice(&chunk[..read]),
                Err(error) => {
                    return Err(RenewFault::Exchange(format!(
                        "cannot read the renewal answer: {error}"
                    )));
                }
            }
        }
    }

    /// Says something once. A steady cadence would otherwise repeat one broken session's note
    /// for every renewal left in a long run.
    fn note(&mut self, message: String) {
        if self.noted {
            return;
        }
        self.noted = true;
        eprintln!("latency_probe: note: {message}");
    }
}

/// Whether the keeper goes on renewing after one exchange.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Keeping {
    Renewing,
    Stopped,
}

/// What is left of `deadline`, or a typed error once nothing is: an expired deadline reads
/// the same as an expired socket timeout to this exchange's caller, because to it they are
/// the same event — the conversation did not finish inside its bound.
fn remaining(deadline: Instant) -> Result<Duration, String> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|left| !left.is_zero())
        .ok_or_else(|| "the control exchange exceeded its 5-second deadline".to_owned())
}

/// Sends the request line under `deadline`, a chunk at a time, exactly as
/// `src/ffi/mod.rs`'s `write_request` does: a send timeout is installed per syscall, not
/// once around the whole write, so a peer that reads one byte at a time cannot extend the
/// bound past `deadline`.
fn write_attach_request(
    socket: &mut UnixStream,
    request: &[u8],
    deadline: Instant,
) -> Result<(), String> {
    let mut sent = 0;
    while sent < request.len() {
        socket
            .set_write_timeout(Some(remaining(deadline)?))
            .map_err(|error| format!("cannot set the control socket's write timeout: {error}"))?;
        match socket.write(&request[sent..]) {
            Ok(0) => {
                return Err("control socket closed before the attach request was sent".to_owned());
            }
            Err(error) => return Err(format!("cannot write the attach request: {error}")),
            Ok(written) => sent += written,
        }
    }
    Ok(())
}

/// Reads the answer line, taking the descriptors that ride its first message, exactly as
/// `src/ffi/mod.rs`'s `read_attachment` does: the daemon sends the line and its descriptor
/// as one `sendmsg`, so the first receive is the one that carries the transfer.
fn read_attach_answer(
    socket: &mut UnixStream,
    deadline: Instant,
) -> Result<(String, Vec<OwnedFd>), String> {
    let mut buffer = vec![0_u8; MAX_CONTROL_LINE_BYTES];
    socket
        .set_read_timeout(Some(remaining(deadline)?))
        .map_err(|error| format!("cannot set the control socket's read timeout: {error}"))?;
    let (mut filled, descriptors) = recv_with_fds(
        socket.as_fd(),
        buffer.as_mut_slice(),
        MAX_TRANSFERRED_DESCRIPTORS,
    )
    .map_err(|error| format!("cannot receive the attach answer: {error}"))?;
    while filled < buffer.len() && !buffer[..filled].contains(&b'\n') {
        socket
            .set_read_timeout(Some(remaining(deadline)?))
            .map_err(|error| format!("cannot set the control socket's read timeout: {error}"))?;
        match socket.read(&mut buffer[filled..]) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(error) => return Err(format!("cannot read the attach answer: {error}")),
        }
    }
    let end = buffer[..filled]
        .iter()
        .position(|byte| *byte == b'\n')
        .ok_or_else(|| "the attach answer never terminated with a newline".to_owned())?;
    let line = core::str::from_utf8(&buffer[..end])
        .map_err(|error| format!("the attach answer is not utf-8: {error}"))?
        .to_owned();
    Ok((line, descriptors))
}

/// Validates a received transfer against what its answer promised, and builds a reader over
/// it — the same validation order `src/ffi/mod.rs`'s `session_from_attachment` performs: the
/// descriptor count first, then the mapping itself (read-only, header and trailer checked by
/// [`SegmentReader::attach_with_doorbell`]), and only then the header's declared identity
/// against the promise.
fn build_reader(
    attachment: &Attachment,
    descriptors: Vec<OwnedFd>,
) -> Result<(SegmentReader, DoorbellLocation), String> {
    if attachment.descriptors != 1 || descriptors.len() != 1 {
        return Err(format!(
            "the attach answer promised {} descriptor(s) but {} arrived",
            attachment.descriptors,
            descriptors.len()
        ));
    }
    let promised_instance =
        u128::from_str_radix(attachment.instance_id.as_str(), 16).map_err(|error| {
            format!(
                "the attach answer's instance id {:?} is not hexadecimal: {error}",
                attachment.instance_id
            )
        })?;
    let mut descriptors = descriptors.into_iter();
    let segment = descriptors
        .next()
        .ok_or_else(|| "the attach answer carried no descriptor".to_owned())?;
    let region = SegmentRegion::open_read_only_from_fd(segment)
        .map_err(|error| format!("the transferred descriptor does not map read-only: {error}"))?;
    let reader = SegmentReader::attach_with_doorbell(Arc::new(region), None)
        .map_err(|error| format!("the transferred segment does not validate: {error}"))?;
    let geometry = reader.geometry();
    if geometry.daemon_instance_id() != promised_instance {
        return Err(format!(
            "the attach answer promised daemon instance {} but the segment header declares \
             {:032x}",
            attachment.instance_id,
            geometry.daemon_instance_id()
        ));
    }
    if geometry.segment_generation() != attachment.segment_generation {
        return Err(format!(
            "the attach answer promised segment generation {} but the header declares {}",
            attachment.segment_generation,
            geometry.segment_generation()
        ));
    }
    let declared_bit = match attachment.doorbell {
        DoorbellLocation::InHeader => FEATURE_DOORBELL_IN_HEADER,
        DoorbellLocation::Page => FEATURE_DOORBELL_PAGE,
    };
    if reader.doorbell_feature_bit() != declared_bit {
        return Err(format!(
            "the attach answer promised doorbell placement {:?} but the header declares a \
             different one",
            attachment.doorbell
        ));
    }
    Ok((reader, attachment.doorbell))
}

/// One half of a split latency: measured, or absent for want of a stamp, or rejected as
/// implausible.
///
/// Kept distinct so a report can say which of the three it was. `Absent` is a slot that never
/// carried the stamp this difference needs; `Implausible` is one that carried it and produced
/// a difference no delivery path can have taken, which is what a clock adjustment between two
/// stamps looks like.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Sample {
    Absent,
    Implausible,
    Measured(u64),
}

/// The three latencies one observation carries, in nanoseconds.
///
/// [`Latencies::daemon`] is `commit - arrival`: how long this daemon took to make a venue
/// frame's state consumer-readable. Both of its stamps are written by the daemon, on one
/// clock, in one process, so no consumer clock and no clock-domain correction appears in it at
/// all. [`Latencies::consumer`] is `observed - commit` — how long this process took to make
/// use of a state the daemon had already published — and [`Latencies::end_to_end`] is
/// `observed - arrival`, the number this probe has always reported. Both of those difference
/// this process's clock against a daemon stamp, and both carry the discard-not-clamp rule.
///
/// Per observation the three satisfy `daemon + consumer == end_to_end` exactly whenever all
/// three are `Measured`, because they are three differences of the same three instants. Their
/// *percentiles* do not add: each distribution is ordered independently.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Latencies {
    daemon: Sample,
    consumer: Sample,
    end_to_end: Sample,
}

/// `to - from` when both stamps are present and the difference is plausible.
fn difference(from: Option<u64>, to: Option<u64>) -> Sample {
    match (from, to) {
        (Some(from), Some(to)) => {
            if to >= from && to - from < IMPLAUSIBLE_NANOS {
                Sample::Measured(to - from)
            } else {
                Sample::Implausible
            }
        }
        _ => Sample::Absent,
    }
}

/// Splits one observation into the three latencies above.
fn latencies(arrival: Option<u64>, commit: Option<u64>, observed: u64) -> Latencies {
    Latencies {
        daemon: difference(arrival, commit),
        consumer: difference(commit, Some(observed)),
        end_to_end: difference(arrival, Some(observed)),
    }
}

#[derive(Default)]
struct Report {
    elapsed: Duration,
    markets_seen: usize,
    kept: Vec<u64>,
    skipped: u64,
    discarded: u64,
    /// `commit_time - arrival_time`, the daemon's own half. No consumer clock is involved.
    daemon_kept: Vec<u64>,
    daemon_discarded: u64,
    /// `observation - commit_time`, this process's half.
    consumer_kept: Vec<u64>,
    consumer_discarded: u64,
    wakes: u64,
    timeouts: u64,
    delivered: u64,
    rescans: u64,
    /// Renewals the daemon acknowledged on the control session an attachment was served on,
    /// and the exchanges that failed instead. Both are counted by the keeper thread and read
    /// once the run is over, and both are zero for a `--shm` attachment, which holds no
    /// session at all.
    renewals: u64,
    renew_failures: u64,
}

/// Drains the segment for `--seconds`, sampling one arrival-to-observation delta per state
/// read.
///
/// Nothing in this loop touches the control session: a lease is kept by its own keeper thread
/// ([`ControlLease`]), so no renewal syscall can land between an update's arrival and this
/// probe's observation of it and be reported as latency.
fn run(reader: &SegmentReader, args: &Args, mode: Mode, obs: &mut Option<ObsLog>) -> Report {
    let start = Instant::now();
    let deadline = start + Duration::from_secs(args.seconds);
    let spin_budget = Duration::from_micros(args.spin_micros);

    let mut last_seen = reader.publication_generation();
    let mut cursor = reader.dirty_cursor();
    let mut seen: BTreeSet<u32> = BTreeSet::new();
    let mut last_sampled_revision: HashMap<u32, u64> = HashMap::new();
    let mut report = Report::default();

    while Instant::now() < deadline {
        let outcome = match mode {
            Mode::Parked => {
                match reader.wait_for_publication(last_seen, spin_budget, Some(PARK_POLL_TIMEOUT)) {
                    Ok(outcome) => outcome,
                    Err(fault) => {
                        eprintln!("latency_probe: wait_for_publication failed: {fault}");
                        break;
                    }
                }
            }
            Mode::Spin => spin_poll(reader, last_seen, deadline),
        };
        match outcome {
            WaitOutcome::Changed(generation) => {
                report.wakes += 1;
                last_seen = generation;
                drain_dirty(
                    reader,
                    &mut cursor,
                    &mut seen,
                    &mut last_sampled_revision,
                    &mut report,
                    obs,
                );
            }
            WaitOutcome::TimedOut(generation) => {
                report.timeouts += 1;
                last_seen = generation;
            }
        }
    }

    report.elapsed = start.elapsed();
    report.markets_seen = seen.len();
    report
}

/// The pure spin-mode wait: no doorbell, no syscall, just repeated acquire-loads of
/// `SegmentReader::publication_generation` until it moves or the run's own deadline passes.
///
/// The only reason this returns without a publication is the run's own `--seconds` deadline:
/// a control-attached run's lease is kept by a thread of its own, so nothing else needs the
/// measurement loop to hand control back.
fn spin_poll(reader: &SegmentReader, last_seen: u64, deadline: Instant) -> WaitOutcome {
    loop {
        let current = reader.publication_generation();
        if current != last_seen {
            return WaitOutcome::Changed(current);
        }
        if Instant::now() >= deadline {
            return WaitOutcome::TimedOut(current);
        }
        core::hint::spin_loop();
    }
}

/// Drains every entry the dirty-index ring currently has for `cursor`, sampling one state
/// read per `Delivered` entry and, on a `Rescan`, one per published entry in the segment's
/// whole directory — then returns, even though more may already be dirty.
///
/// The rescan walks the directory rather than the set of markets already delivered, because
/// an overrun is precisely the case where a market's only dirty entry was overwritten before
/// this probe ever saw it: recovering over the already-seen set alone would leave such a
/// market out of the sampled workload indefinitely. Every index the directory can hold is
/// tried; `resolve_directory_index` answers `None` for one no market is installed at, and
/// that entry is simply not counted as seen.
///
/// A `Rescan` ends this call rather than looping back into `next_dirty`: a directory walk
/// takes longer than an empty poll, and a writer publishing fast enough to lap the ring
/// within that walk would otherwise hand back another `Rescan` on the very next poll, and
/// the one after that, forever — this call would never return, and the caller's own
/// `--seconds` deadline would never be rechecked. Counting the rescan and returning instead
/// costs nothing: `next_dirty` already rebased the cursor past the lap it observed, so the
/// next call to this function resumes from there, and whatever the writer published during
/// this walk is exactly what makes the caller's next wait return immediately rather than
/// park.
fn drain_dirty(
    reader: &SegmentReader,
    cursor: &mut DirtyCursor,
    seen: &mut BTreeSet<u32>,
    last_sampled_revision: &mut HashMap<u32, u64>,
    report: &mut Report,
    obs: &mut Option<ObsLog>,
) {
    loop {
        match reader.next_dirty(cursor) {
            DirtyPoll::Delivered {
                directory_index, ..
            } => {
                report.delivered += 1;
                let _ = seen.insert(directory_index);
                observe(reader, directory_index, last_sampled_revision, report, obs);
            }
            DirtyPoll::Rescan => {
                report.rescans += 1;
                for directory_index in 0..reader.geometry().layout().directory_capacity() {
                    if reader.resolve_directory_index(directory_index).is_none() {
                        continue;
                    }
                    let _ = seen.insert(directory_index);
                    observe(reader, directory_index, last_sampled_revision, report, obs);
                }
                return;
            }
            DirtyPoll::Idle => break,
        }
    }
}

/// Reads one market's current state and, unless its revision was already sampled, differences
/// this process's wall clock against the venue socket-arrival stamp the writer carried
/// through, under the same discard-not-clamp rule as `examples/shm_latency.rs`.
///
/// A resolve or read failure — an uninstalled entry, a torn read, a writer mid-publish — is
/// silently skipped, exactly as `examples/shm_latency.rs`'s own busy-poll loop treats one: the
/// next dirty entry or rescan for the same market tries again, so nothing here is counted as
/// a discard or a skip, both of which name an observation that was taken and rejected.
fn observe(
    reader: &SegmentReader,
    directory_index: u32,
    last_sampled_revision: &mut HashMap<u32, u64>,
    report: &mut Report,
    obs: &mut Option<ObsLog>,
) {
    let Some(handle) = reader.resolve_directory_index(directory_index) else {
        return;
    };
    let Ok(snapshot) = reader.read(handle) else {
        return;
    };
    let revision = snapshot.revision();
    if last_sampled_revision.get(&directory_index) == Some(&revision) {
        return;
    }
    last_sampled_revision.insert(directory_index, revision);
    if let Some(log) = obs.as_mut() {
        let slug = log.intern(snapshot.market().key().value());
        let digest = level_digest(snapshot.levels());
        let _seq = log.record(slug, now_epoch_nanos(), digest, revision);
    }
    let split = latencies(
        snapshot.arrival_time_nanos(),
        snapshot.commit_time_nanos(),
        now_nanos(),
    );
    match split.end_to_end {
        Sample::Measured(delta) => report.kept.push(delta),
        Sample::Implausible => report.discarded += 1,
        Sample::Absent => report.skipped += 1,
    }
    match split.daemon {
        Sample::Measured(delta) => report.daemon_kept.push(delta),
        Sample::Implausible => report.daemon_discarded += 1,
        Sample::Absent => {}
    }
    match split.consumer {
        Sample::Measured(delta) => report.consumer_kept.push(delta),
        Sample::Implausible => report.consumer_discarded += 1,
        Sample::Absent => {}
    }
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_nanos()).unwrap_or(u64::MAX)
        })
}

/// Prints the run's report. `mode` is the effective mode the measurement loop actually ran
/// under, which differs from `args.mode` exactly when a control-socket attachment forced a
/// `parked` request down to `spin` — see [`attach_via_control`]. `show_mode_effective`
/// prints that fact as its own `mode_effective:` line; a `--shm` attachment never forces this
/// downgrade, so its report carries neither that line nor `note`, byte-identical to this
/// probe's original path-only report. `note`, when given, is the same reason a caller was
/// just told on stderr, repeated here so the report never claims a mode it did not run.
fn print_report(
    args: &Args,
    report: &Report,
    mode: Mode,
    show_mode_effective: bool,
    note: Option<&str>,
    obs_log: Option<&ObsLog>,
) {
    println!("label: {}", args.label);
    println!("mode: {}", mode_name(args.mode));
    if show_mode_effective {
        println!("mode_effective: {}", mode_name(mode));
    }
    if let Some(note) = note {
        println!("note: {note}");
    }
    match mode {
        Mode::Parked => println!("spin_budget_us: {}", args.spin_micros),
        Mode::Spin => println!("spin_budget_us: n/a (spin mode never parks)"),
    }
    println!("duration_seconds: {:.3}", report.elapsed.as_secs_f64());
    println!("markets_seen: {}", report.markets_seen);
    println!("samples_kept: {}", report.kept.len());
    if report.kept.len() < MIN_REPORTABLE_SAMPLES {
        println!(
            "percentiles: withheld, only {} kept samples (floor is {MIN_REPORTABLE_SAMPLES})",
            report.kept.len()
        );
    } else {
        let mut sorted = report.kept.clone();
        sorted.sort_unstable();
        println!("p50_us: {}", format_micros(quantile(&sorted, 500)));
        println!("p95_us: {}", format_micros(quantile(&sorted, 950)));
        println!("p99_us: {}", format_micros(quantile(&sorted, 990)));
        println!("p99.9_us: {}", format_micros(quantile(&sorted, 999)));
        if sorted.len() >= MIN_P9999_SAMPLES {
            println!(
                "p99.99_us: {}",
                format_micros(ten_thousandth_quantile(&sorted, 9999))
            );
        }
        println!(
            "max_us: {}",
            format_micros(sorted.last().copied().unwrap_or(0))
        );
    }
    println!("wakes: {}", report.wakes);
    let seconds = report.elapsed.as_secs_f64();
    let rate = if seconds > 0.0 {
        report.wakes as f64 / seconds
    } else {
        0.0
    };
    println!("wakes_per_second: {rate:.3}");
    match mode {
        Mode::Parked => println!("timeouts: {}", report.timeouts),
        Mode::Spin => println!(
            "timeouts: {} (spin mode: at most one, marking the run's own deadline, not a fault count)",
            report.timeouts
        ),
    }
    println!("dirty_delivered: {}", report.delivered);
    println!("rescans: {}", report.rescans);
    println!("samples_skipped: {}", report.skipped);
    println!("samples_discarded: {}", report.discarded);
    if matches!(args.source, Source::Control { .. }) {
        println!("lease_renewals: {}", report.renewals);
        println!("lease_renew_failures: {}", report.renew_failures);
    }
    println!(
        "daemon_latency: commit_time - arrival_time, both stamped by this daemon on one clock \
         in one process (no consumer clock is involved)"
    );
    print_distribution("daemon", &report.daemon_kept, report.daemon_discarded);
    println!("consumer_latency: observation - commit_time");
    print_distribution("consumer", &report.consumer_kept, report.consumer_discarded);
    println!(
        "end_to_end: observation - arrival_time, reported above as samples_kept and p50_us \
         through max_us"
    );
    if let Some(path) = args.obs_out.as_ref() {
        println!("obs_out: {}", path.display());
        if let Some(log) = obs_log {
            println!("obs_rows: {}", log.rows());
            println!("obs_dropped: {}", log.dropped());
        }
    }
}

/// Prints one split distribution under `prefix`, in the shape the end-to-end block above
/// prints its own — the same nearest-rank quantiles, the same
/// [`MIN_REPORTABLE_SAMPLES`] floor, and percentiles withheld rather than printed below it.
///
/// The end-to-end block is written out longhand rather than routed through this, so that every
/// key this probe has ever printed keeps its exact text: the two new groups are additive, and
/// nothing that parses the old report has to change.
fn print_distribution(prefix: &str, kept: &[u64], discarded: u64) {
    println!("{prefix}_samples_kept: {}", kept.len());
    if kept.len() < MIN_REPORTABLE_SAMPLES {
        println!(
            "{prefix}_percentiles: withheld, only {} kept samples (floor is \
             {MIN_REPORTABLE_SAMPLES})",
            kept.len()
        );
    } else {
        let mut sorted = kept.to_vec();
        sorted.sort_unstable();
        println!("{prefix}_p50_us: {}", format_micros(quantile(&sorted, 500)));
        println!("{prefix}_p95_us: {}", format_micros(quantile(&sorted, 950)));
        println!("{prefix}_p99_us: {}", format_micros(quantile(&sorted, 990)));
        println!(
            "{prefix}_p99.9_us: {}",
            format_micros(quantile(&sorted, 999))
        );
        if sorted.len() >= MIN_P9999_SAMPLES {
            println!(
                "{prefix}_p99.99_us: {}",
                format_micros(ten_thousandth_quantile(&sorted, 9999))
            );
        }
        println!(
            "{prefix}_max_us: {}",
            format_micros(sorted.last().copied().unwrap_or(0))
        );
    }
    println!("{prefix}_samples_discarded: {discarded}");
}

/// The `permille`-th value of a sorted nanosecond sample set, by nearest-rank; 0 for an empty
/// set. Identical to `examples/shm_latency.rs`'s own `quantile`.
fn quantile(sorted: &[u64], permille: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (sorted.len() * permille).div_ceil(1000).max(1) - 1;
    sorted[rank.min(sorted.len() - 1)]
}

/// The `per_ten_thousand`-th value of a sorted nanosecond sample set, by the same
/// nearest-rank rule as [`quantile`] one basis deeper; 0 for an empty set. Exists for the
/// p99.99 cell, which permille cannot express.
fn ten_thousandth_quantile(sorted: &[u64], per_ten_thousand: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (sorted.len() * per_ten_thousand).div_ceil(10_000).max(1) - 1;
    sorted[rank.min(sorted.len() - 1)]
}

fn format_micros(nanos: u64) -> String {
    format!("{:.3}", nanos as f64 / 1000.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantile_matches_nearest_rank_on_a_small_sorted_set() {
        let sorted = [10_u64, 20, 30, 40, 50, 60, 70, 80, 90, 100];
        assert_eq!(quantile(&sorted, 500), 50);
        assert_eq!(quantile(&sorted, 990), 100);
        assert_eq!(quantile(&sorted, 999), 100);
    }

    #[test]
    fn ten_thousandth_quantile_matches_nearest_rank_at_its_own_basis() {
        let sorted: Vec<u64> = (1..=20_000).collect();
        assert_eq!(ten_thousandth_quantile(&sorted, 5_000), 10_000);
        assert_eq!(ten_thousandth_quantile(&sorted, 9_999), 19_998);
        assert_eq!(ten_thousandth_quantile(&sorted, 10_000), 20_000);
        assert_eq!(ten_thousandth_quantile(&[], 9_999), 0);
        let ties = [7_u64; 20_000];
        assert_eq!(ten_thousandth_quantile(&ties, 9_999), 7);
    }

    /// The two halves of a split observation account for the whole of it, sample by sample.
    ///
    /// The percentiles never add this way -- each distribution is ordered on its own -- which
    /// is why the identity is pinned here, on the arithmetic, and not on a report's output.
    #[test]
    fn the_daemon_and_consumer_halves_sum_to_the_end_to_end_latency() {
        for (arrival, commit, observed) in [
            (1_000_u64, 1_400_u64, 2_500_u64),
            (0, 0, 0),
            (10, 10, 10),
            (5, 999_999_998, 999_999_999),
        ] {
            let split = latencies(Some(arrival), Some(commit), observed);
            let (Sample::Measured(daemon), Sample::Measured(consumer), Sample::Measured(whole)) =
                (split.daemon, split.consumer, split.end_to_end)
            else {
                panic!("every stamp was present and plausible: {split:?}");
            };
            assert_eq!(daemon + consumer, whole);
        }
    }

    /// A slot with no commit stamp still yields the end-to-end latency, and neither half.
    #[test]
    fn a_missing_commit_stamp_leaves_only_the_end_to_end_latency() {
        let split = latencies(Some(1_000), None, 2_000);
        assert_eq!(split.daemon, Sample::Absent);
        assert_eq!(split.consumer, Sample::Absent);
        assert_eq!(split.end_to_end, Sample::Measured(1_000));
    }

    /// A slot no venue frame drove carries no arrival stamp, and only the consumer half of it
    /// can be measured.
    #[test]
    fn a_missing_arrival_stamp_leaves_only_the_consumer_latency() {
        let split = latencies(None, Some(1_000), 2_000);
        assert_eq!(split.daemon, Sample::Absent);
        assert_eq!(split.consumer, Sample::Measured(1_000));
        assert_eq!(split.end_to_end, Sample::Absent);
    }

    /// A difference that runs backwards, or runs longer than any delivery path takes, is
    /// rejected rather than clamped -- in the daemon's own half as much as in the two that
    /// cross processes, because a clock adjustment between two stamps is a clock adjustment
    /// wherever it happens.
    #[test]
    fn a_backwards_or_absurd_difference_is_discarded_rather_than_clamped() {
        let backwards = latencies(Some(2_000), Some(1_000), 3_000);
        assert_eq!(backwards.daemon, Sample::Implausible);
        assert_eq!(backwards.end_to_end, Sample::Measured(1_000));

        let absurd = latencies(Some(0), Some(IMPLAUSIBLE_NANOS), IMPLAUSIBLE_NANOS + 1);
        assert_eq!(absurd.daemon, Sample::Implausible);
        assert_eq!(absurd.end_to_end, Sample::Implausible);
        assert_eq!(absurd.consumer, Sample::Measured(1));

        let edge = latencies(Some(0), Some(IMPLAUSIBLE_NANOS - 1), IMPLAUSIBLE_NANOS - 1);
        assert_eq!(edge.daemon, Sample::Measured(IMPLAUSIBLE_NANOS - 1));
        assert_eq!(edge.consumer, Sample::Measured(0));
    }

    #[test]
    fn quantile_of_an_empty_set_is_zero() {
        assert_eq!(quantile(&[], 500), 0);
    }

    /// The renewal cadence comes from the deadline the daemon declared, so every TTL an
    /// operator may configure fits several renewals — a lease is never carried by one line
    /// arriving on time.
    #[test]
    fn a_declared_ttl_fits_several_renewals_inside_itself() {
        for ttl_ms in [
            pm_ws::daemon::MIN_LEASE_TTL_MS,
            3_000,
            60_000,
            pm_ws::daemon::MAX_LEASE_TTL_MS,
        ] {
            let interval = renew_interval(ttl_ms);
            assert!(
                interval * RENEW_TTL_DIVISOR <= Duration::from_millis(ttl_ms),
                "a {ttl_ms} ms TTL renewed every {interval:?} does not fit \
                 {RENEW_TTL_DIVISOR} renewals inside itself"
            );
        }
        assert_eq!(
            renew_interval(3_000),
            Duration::from_secs(1),
            "the cadence is a third of the declared TTL until a bound takes over"
        );
    }

    /// A daemon that expires nothing declares `0`, and gets the ceiling cadence; a TTL long
    /// enough that a third of it is longer than the ceiling gets the ceiling too.
    #[test]
    fn no_declared_ttl_renews_on_the_ceiling_cadence() {
        assert_eq!(renew_interval(0), MAX_RENEW_INTERVAL);
        assert_eq!(
            renew_interval(pm_ws::daemon::MAX_LEASE_TTL_MS),
            MAX_RENEW_INTERVAL
        );
    }

    /// A daemon declaring a TTL below what its own configuration accepts still gets a bounded
    /// trickle of renewals rather than a write loop.
    #[test]
    fn a_ttl_below_the_daemons_own_floor_is_renewed_no_faster_than_the_probe_floor() {
        assert_eq!(renew_interval(1), MIN_RENEW_INTERVAL);
        assert_eq!(
            renew_exchange_budget(MIN_RENEW_INTERVAL),
            MIN_RENEW_INTERVAL,
            "an exchange never outlives the cadence it was sent on"
        );
    }

    fn cli(args: &[&str]) -> impl Iterator<Item = String> {
        std::iter::once("latency_probe".to_owned()).chain(args.iter().map(|arg| (*arg).to_owned()))
    }

    #[test]
    fn parse_args_requires_shm() {
        let error = parse_args(cli(&["--label", "x"])).unwrap_err();
        assert!(error.contains("--shm"));
    }

    #[test]
    fn parse_args_requires_label() {
        let error = parse_args(cli(&["--shm", "/tmp/a.seg"])).unwrap_err();
        assert!(error.contains("--label"));
    }

    #[test]
    fn parse_args_defaults_mode_to_parked_with_zero_spin_and_sixty_seconds() {
        let args = parse_args(cli(&["--shm", "/tmp/a.seg", "--label", "x"])).unwrap();
        assert_eq!(args.mode, Mode::Parked);
        assert_eq!(args.spin_micros, 0);
        assert_eq!(args.seconds, 60);
    }

    #[test]
    fn parse_args_accepts_spin_mode_and_overrides() {
        let args = parse_args(cli(&[
            "--shm",
            "/tmp/a.seg",
            "--mode",
            "spin",
            "--spin-micros",
            "50",
            "--seconds",
            "10",
            "--label",
            "bench",
        ]))
        .unwrap();
        assert_eq!(args.mode, Mode::Spin);
        assert_eq!(args.spin_micros, 50);
        assert_eq!(args.seconds, 10);
        assert_eq!(args.label, "bench");
    }

    #[test]
    fn parse_args_rejects_an_unknown_mode() {
        let error = parse_args(cli(&[
            "--shm",
            "/tmp/a.seg",
            "--mode",
            "eager",
            "--label",
            "x",
        ]))
        .unwrap_err();
        assert!(error.contains("eager"));
    }

    #[test]
    fn parse_args_rejects_seconds_outside_the_ceiling() {
        let error = parse_args(cli(&[
            "--shm",
            "/tmp/a.seg",
            "--label",
            "x",
            "--seconds",
            "86401",
        ]))
        .unwrap_err();
        assert!(error.contains("--seconds"));
    }

    #[test]
    fn parse_args_rejects_zero_seconds() {
        let error = parse_args(cli(&[
            "--shm",
            "/tmp/a.seg",
            "--label",
            "x",
            "--seconds",
            "0",
        ]))
        .unwrap_err();
        assert!(error.contains("--seconds"));
    }

    #[test]
    fn parse_args_rejects_an_unknown_flag() {
        let error =
            parse_args(cli(&["--shm", "/tmp/a.seg", "--label", "x", "--bogus"])).unwrap_err();
        assert!(error.contains("--bogus"));
    }

    #[test]
    fn parse_args_accepts_control_and_market() {
        let args = parse_args(cli(&[
            "--control",
            "/tmp/pmwsd.sock",
            "--market",
            "btc-up-or-down-5-min-1",
            "--label",
            "x",
        ]))
        .unwrap();
        match args.source {
            Source::Control { control, market } => {
                assert_eq!(control, PathBuf::from("/tmp/pmwsd.sock"));
                assert_eq!(market, "btc-up-or-down-5-min-1");
            }
            Source::Shm(_) => panic!("expected a control source"),
        }
    }

    #[test]
    fn parse_args_rejects_shm_combined_with_control() {
        let error = parse_args(cli(&[
            "--shm",
            "/tmp/a.seg",
            "--control",
            "/tmp/pmwsd.sock",
            "--market",
            "x",
            "--label",
            "x",
        ]))
        .unwrap_err();
        assert!(error.contains("mutually exclusive"));
    }

    #[test]
    fn parse_args_rejects_control_without_market() {
        let error = parse_args(cli(&["--control", "/tmp/pmwsd.sock", "--label", "x"])).unwrap_err();
        assert!(error.contains("--market"));
    }

    #[test]
    fn parse_args_rejects_market_without_control() {
        let error = parse_args(cli(&["--market", "x", "--label", "x"])).unwrap_err();
        assert!(error.contains("--control"));
    }

    #[test]
    fn parse_args_defaults_obs_out_to_absent() {
        let args = parse_args(cli(&["--shm", "/tmp/a.seg", "--label", "x"])).unwrap();
        assert!(args.obs_out.is_none());
    }

    #[test]
    fn parse_args_reads_the_obs_out_path() {
        let args = parse_args(cli(&[
            "--shm",
            "/tmp/a.seg",
            "--label",
            "x",
            "--obs-out",
            "/tmp/probe.obs",
        ]))
        .unwrap();
        assert_eq!(
            args.obs_out.as_deref(),
            Some(std::path::Path::new("/tmp/probe.obs"))
        );
        assert!(matches!(
            parse_args(cli(&["--shm", "/tmp/a.seg", "--label", "x", "--obs-out"])),
            Err(ref message) if message.contains("--obs-out")
        ));
    }
}

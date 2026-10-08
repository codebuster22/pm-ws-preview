//! The `rust-shm` leg of the S8c comparative harness (`bench/sdk-harness/README.md`): a
//! separate-process consumer that leases a pinned market set from a running `pmwsd` over one
//! control session, observes every published revision through the segment it was handed, and
//! writes the `.obs` observation log the harness matches legs on.
//!
//! One process is one shard's segment. The daemon maps one shard's markets to one segment
//! and a control session maps one segment, so the runner launches one of these per shard with
//! that shard's slug file; a slug the daemon holds on another shard is refused here rather
//! than silently measured, exactly as `src/ffi/mod.rs`'s `pmws_lease` refuses it with
//! `PMWS_STATUS_FOREIGN_SEGMENT`.
//!
//! The lease conversation is the one `examples/latency_probe.rs` drives, extended from one
//! market to a set: the first slug's [`ControlRequest::Attach`] carries the segment's
//! descriptor back and is what this process reads through for the whole run, and every
//! further slug is one more attach on that same open session whose duplicate descriptor is
//! dropped. The session is then held by a keeper thread of its own, renewing inside the TTL
//! the daemon declared, because the connection is the lease: a consumer that dropped it would
//! spend its run reading a segment nothing publishes into any more. The keeper is a thread so
//! that no control syscall is ever charged to a measured observation.
//!
//! The measured boundary is the harness's, not this file's: `t_obs` is stamped the instant
//! [`SegmentReader::read`] returns the published state for a dirty market, before anything
//! else. Deduplication, the content digest, and the log append all happen after the stamp —
//! they are harness cost, the same work in every leg, and never boundary cost.
//!
//! Alongside the `.obs` file it prints the same three-way latency split
//! `examples/latency_probe.rs` reports — `daemon` is `commit - arrival`, `consumer` is
//! `observed - commit`, `end_to_end` is `observed - arrival` — under the same
//! discard-not-clamp rule, the same nearest-rank percentiles, and the same sample floor.
//!
//! Live invocation, against a `pmwsd` the runner already started:
//!
//! ```text
//! cargo run --release --example matched_consumer -- \
//!     --control /tmp/pmwsd.sock --slugs /tmp/shard-00.txt --seconds 600 \
//!     --obs-out /tmp/s8c/rust-shm-00.obs --label "<machine>, shard 0, all-active"
//! ```

use pm_ws::{
    Attachment, ControlRequest, ControlResponse, DirtyCursor, DirtyPoll, DoorbellLocation,
    FEATURE_DOORBELL_IN_HEADER, FEATURE_DOORBELL_PAGE, MAX_CONTROL_LINE_BYTES,
    MAX_TRANSFERRED_DESCRIPTORS, SegmentReader, SegmentRegion, WaitOutcome, recv_with_fds,
};
use std::collections::{BTreeSet, HashMap};
use std::io::{Read, Write};
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[path = "common/matched_obs.rs"]
mod matched_obs;

use matched_obs::{
    IMPLAUSIBLE_NANOS, MIN_REPORTABLE_SAMPLES, OBS_ROW_CAP, ObsLog, SlugId, format_micros, git_pin,
    level_digest, now_epoch_nanos, parse_seconds, print_distribution, quantile, read_slug_file,
    termination_flag,
};

/// The leg identifier this process writes into its `.obs` header.
const LEG: &str = "rust-shm";

/// How long one parked wait blocks before the loop rechecks its deadline and its stop flag.
/// Bounds how late a finished or terminated run notices; never a measurement.
const PARK_POLL_TIMEOUT: Duration = Duration::from_millis(100);

/// How long the whole control conversation for one market is allowed to run for, mirroring
/// `src/ffi/mod.rs`'s own `ATTACH_TIMEOUT`.
const CONTROL_ATTACH_TIMEOUT: Duration = Duration::from_secs(5);

/// How many times a [`ControlResponse::Busy`] answer is retried before the market is counted
/// as unleased.
///
/// `Busy` says a bounded control queue was full and *nothing was applied*, which is a state
/// the daemon leaves on its own; twenty consumer processes leasing a hundred markets each
/// against one daemon is exactly when it happens. Retrying it a bounded number of times is
/// the difference between a shard measuring its pinned set and a shard measuring a subset it
/// never reported.
const ATTACH_BUSY_RETRIES: u32 = 5;

/// How long a retried [`ControlResponse::Busy`] waits before asking again. Off the measured
/// path entirely: every lease is taken before the measurement loop starts.
const ATTACH_BUSY_PAUSE: Duration = Duration::from_millis(50);

/// The longest this process ever leaves its control session silent, whatever the daemon's
/// declared TTL. Mirrors `examples/latency_probe.rs`.
const MAX_RENEW_INTERVAL: Duration = Duration::from_secs(5);

/// The shortest renewal cadence this process will drive itself at.
const MIN_RENEW_INTERVAL: Duration = Duration::from_millis(100);

/// What fraction of the declared TTL a renewal is sent inside: the daemon measures silence
/// from the request it last answered, so a renewal sent *at* the deadline has already lost.
const RENEW_TTL_DIVISOR: u32 = 3;

/// How often the keeper thread looks at its stop flag while it waits for the next renewal.
const KEEPER_STOP_POLL: Duration = Duration::from_millis(50);

const USAGE: &str = "usage: matched_consumer --control <socket-path> --slugs <file> \
                     --obs-out <path> --label \"<machine, workload>\" [--seconds <n>] \
                     [--spin-micros <n>] [--lease-ttl-ms <n>]";

#[derive(Clone, Debug, Eq, PartialEq)]
struct Args {
    control: PathBuf,
    slugs: PathBuf,
    seconds: u64,
    obs_out: PathBuf,
    label: String,
    spin_micros: u64,
    lease_ttl_ms: Option<u64>,
}

fn main() {
    let args = match parse_args(std::env::args()) {
        Ok(args) => args,
        Err(message) => {
            eprintln!("matched_consumer: {message}");
            eprintln!("{USAGE}");
            std::process::exit(2);
        }
    };
    let slugs = match read_slug_file(args.slugs.as_path()) {
        Ok(slugs) => slugs,
        Err(message) => {
            eprintln!("matched_consumer: {message}");
            std::process::exit(2);
        }
    };
    let stop = termination_flag();
    let mut session = match lease_set(&args, &slugs) {
        Ok(session) => session,
        Err(message) => {
            eprintln!("matched_consumer: {message}");
            std::process::exit(1);
        }
    };
    let parking_unavailable = session.doorbell == DoorbellLocation::Page;
    let note = parking_unavailable.then(|| {
        "parking is unavailable over this attachment: its doorbell is in a sibling page, and \
         the control channel never transfers that page's descriptor (only the segment's own); \
         falling back to spin observation"
            .to_owned()
    });
    if let Some(note) = note.as_deref() {
        eprintln!("matched_consumer: note: {note}");
    }
    let mut log = ObsLog::new(
        LEG,
        args.label.as_str(),
        git_pin().as_str(),
        slugs.len(),
        OBS_ROW_CAP,
    );
    let wanted: HashMap<String, SlugId> = session
        .leased
        .iter()
        .map(|slug| {
            let id = log.intern(slug.as_str());
            (slug.clone(), id)
        })
        .collect();
    let mut report = observe_run(
        &session.reader,
        &args,
        parking_unavailable,
        &stop,
        &wanted,
        &mut log,
    );
    let (renewed, failed) = session.lease.finish();
    report.renewals = renewed;
    report.renew_failures = failed;
    let written = log.write(args.obs_out.as_path());
    if let Err(error) = &written {
        eprintln!(
            "matched_consumer: cannot write {}: {error}",
            args.obs_out.display()
        );
    }
    print_report(&args, &session, &report, &log, note.as_deref(), slugs.len());
    if written.is_err() {
        std::process::exit(1);
    }
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let _binary = args.next();
    let mut control = None;
    let mut slugs = None;
    let mut seconds = 600_u64;
    let mut obs_out = None;
    let mut label = None;
    let mut spin_micros = 0_u64;
    let mut lease_ttl_ms = None;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--control" => {
                control = Some(PathBuf::from(
                    args.next().ok_or("--control requires a value")?,
                ));
            }
            "--slugs" => {
                slugs = Some(PathBuf::from(
                    args.next().ok_or("--slugs requires a value")?,
                ))
            }
            "--seconds" => {
                seconds = parse_seconds(&args.next().ok_or("--seconds requires a value")?)?
            }
            "--obs-out" => {
                obs_out = Some(PathBuf::from(
                    args.next().ok_or("--obs-out requires a value")?,
                ));
            }
            "--label" => label = Some(args.next().ok_or("--label requires a value")?),
            "--spin-micros" => {
                spin_micros = args
                    .next()
                    .ok_or("--spin-micros requires a value")?
                    .parse()
                    .map_err(|_| "--spin-micros takes a non-negative integer".to_owned())?;
            }
            "--lease-ttl-ms" => {
                lease_ttl_ms = Some(
                    args.next()
                        .ok_or("--lease-ttl-ms requires a value")?
                        .parse()
                        .map_err(|_| "--lease-ttl-ms takes a non-negative integer".to_owned())?,
                );
            }
            other => return Err(format!("unknown flag {other}")),
        }
    }
    Ok(Args {
        control: control.ok_or("--control is required (the daemon's control socket)")?,
        slugs: slugs.ok_or("--slugs is required (one market slug per line)")?,
        seconds,
        obs_out: obs_out.ok_or("--obs-out is required (where the observation log is written)")?,
        label: label.ok_or("--label is required (the machine and the workload)")?,
        spin_micros,
        lease_ttl_ms,
    })
}

/// Everything one control session produced: the segment it handed over, the markets it took a
/// lease on, and the leases it could not take.
struct Session {
    reader: SegmentReader,
    doorbell: DoorbellLocation,
    lease: ControlLease,
    leased: Vec<String>,
    attach_failures: Vec<(String, String)>,
    foreign_segment: Vec<String>,
}

/// Takes one control session and leases every slug in the pinned set on it.
///
/// The first slug's attach is what produces the segment this process reads through; every
/// later one is one more attach on the same session whose duplicate descriptor is dropped and
/// whose answer is checked against the segment already mapped. A slug the daemon serves from
/// another shard's segment is given straight back with [`ControlRequest::Release`] and
/// recorded, because this session has no mapping for it — the same rollback
/// `src/ffi/mod.rs`'s `lease_market` performs.
///
/// Fails only when no market at all could be leased: a session with nothing to observe has
/// nothing to report, and a partly-leased set is reported rather than refused.
fn lease_set(args: &Args, slugs: &[String]) -> Result<Session, String> {
    let mut socket = UnixStream::connect(args.control.as_path()).map_err(|error| {
        format!(
            "cannot connect to control socket {}: {error}",
            args.control.display()
        )
    })?;
    let mut leased = Vec::new();
    let mut attach_failures = Vec::new();
    let mut foreign_segment = Vec::new();
    let mut anchor: Option<(SegmentReader, Attachment)> = None;
    for slug in slugs {
        match attach_market(&mut socket, slug.as_str()) {
            Err(message) => attach_failures.push((slug.clone(), message)),
            Ok((attachment, descriptors)) => match anchor.as_ref() {
                None => match build_reader(&attachment, descriptors) {
                    Ok((reader, _doorbell)) => {
                        leased.push(slug.clone());
                        anchor = Some((reader, attachment));
                    }
                    Err(message) => attach_failures.push((slug.clone(), message)),
                },
                Some((reader, first)) => {
                    drop(descriptors);
                    if names_same_segment(reader, first, &attachment) {
                        leased.push(slug.clone());
                    } else {
                        match release_market(&mut socket, slug.as_str()) {
                            Ok(()) => foreign_segment.push(slug.clone()),
                            Err(message) => {
                                return Err(format!(
                                    "{slug} is served from another shard's segment and the \
                                     lease could not be given back: {message}"
                                ));
                            }
                        }
                    }
                }
            },
        }
    }
    let (reader, attachment) = anchor.ok_or_else(|| {
        "no market could be leased on this session; every attach was refused".to_owned()
    })?;
    let ttl_ms = args.lease_ttl_ms.unwrap_or(attachment.lease_ttl_ms);
    let lease = ControlLease::start(socket, ttl_ms)?;
    Ok(Session {
        reader,
        doorbell: attachment.doorbell,
        lease,
        leased,
        attach_failures,
        foreign_segment,
    })
}

/// One attach conversation on an open session: the request line out, the answer line and
/// whatever descriptors rode it back.
///
/// [`ControlResponse::Busy`] is retried a bounded number of times because it means the
/// request was never applied; every other refusal is returned as it was given.
fn attach_market(
    socket: &mut UnixStream,
    market: &str,
) -> Result<(Attachment, Vec<OwnedFd>), String> {
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
    let mut attempt = 0;
    loop {
        let deadline = Instant::now() + CONTROL_ATTACH_TIMEOUT;
        write_line(socket, request.as_bytes(), deadline)?;
        let (line, descriptors) = read_answer_with_fds(socket, deadline)?;
        match serde_json::from_str::<ControlResponse>(line.trim_end()) {
            Ok(ControlResponse::Attached { attachment }) => return Ok((attachment, descriptors)),
            Ok(ControlResponse::Busy { message }) => {
                if attempt >= ATTACH_BUSY_RETRIES {
                    return Err(format!(
                        "the daemon's control queue stayed busy over {ATTACH_BUSY_RETRIES} \
                         retries: {message}"
                    ));
                }
                attempt += 1;
                std::thread::sleep(ATTACH_BUSY_PAUSE);
            }
            Ok(ControlResponse::Markets { markets }) => {
                return Err(format!("the daemon refused the market: {markets:?}"));
            }
            Ok(ControlResponse::Error { message }) => {
                return Err(format!("the daemon refused the attach: {message}"));
            }
            Ok(other) => return Err(format!("the daemon answered {other:?} to an attach")),
            Err(error) => {
                return Err(format!(
                    "the attach answer is not a control response: {error}: {line}"
                ));
            }
        }
    }
}

/// Gives one market's lease back on an open session, and reads the acknowledgement.
fn release_market(socket: &mut UnixStream, market: &str) -> Result<(), String> {
    let request = pm_ws::encode_line(&ControlRequest::Release {
        market: market.to_owned(),
    })
    .map_err(|error| format!("cannot encode a release request: {error}"))?;
    let deadline = Instant::now() + CONTROL_ATTACH_TIMEOUT;
    write_line(socket, request.as_bytes(), deadline)?;
    let (line, descriptors) = read_answer_with_fds(socket, deadline)?;
    drop(descriptors);
    match serde_json::from_str::<ControlResponse>(line.trim_end()) {
        Ok(ControlResponse::Released { .. }) => Ok(()),
        Ok(other) => Err(format!("the daemon answered {other:?} to a release")),
        Err(error) => Err(format!(
            "the release answer is not a control response: {error}: {line}"
        )),
    }
}

/// Whether `attachment` names the very segment this session already mapped.
///
/// The instance and generation are taken from the validated header rather than from what the
/// first answer claimed, and the segment name is the field that actually discriminates: two
/// shards of one daemon share an instance identity, and a freshly started pair share a
/// segment generation too. Exactly `src/ffi/mod.rs`'s `names_this_segment`.
fn names_same_segment(reader: &SegmentReader, first: &Attachment, attachment: &Attachment) -> bool {
    let geometry = reader.geometry();
    first.segment == attachment.segment
        && u128::from_str_radix(attachment.instance_id.as_str(), 16)
            .is_ok_and(|promised| promised == geometry.daemon_instance_id())
        && attachment.segment_generation == geometry.segment_generation()
}

/// What is left of `deadline`, or a typed error once nothing is.
fn remaining(deadline: Instant) -> Result<Duration, String> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|left| !left.is_zero())
        .ok_or_else(|| "the control exchange exceeded its deadline".to_owned())
}

/// Sends one request line under `deadline`, a chunk at a time, with the send timeout
/// installed per syscall so a peer that reads one byte at a time cannot extend the bound.
fn write_line(socket: &mut UnixStream, request: &[u8], deadline: Instant) -> Result<(), String> {
    let mut sent = 0;
    while sent < request.len() {
        socket
            .set_write_timeout(Some(remaining(deadline)?))
            .map_err(|error| format!("cannot set the control socket's write timeout: {error}"))?;
        match socket.write(&request[sent..]) {
            Ok(0) => return Err("the control socket closed before the request was sent".to_owned()),
            Err(error) => return Err(format!("cannot write the control request: {error}")),
            Ok(written) => sent += written,
        }
    }
    Ok(())
}

/// Reads one answer line, taking whatever descriptors ride its first message.
///
/// The daemon sends an attach answer and its descriptor as one `sendmsg`, so the first
/// receive is the one that carries the transfer — the same read `src/ffi/mod.rs`'s
/// `read_attachment` performs. An answer carrying no descriptor reads the same way and
/// simply returns none.
fn read_answer_with_fds(
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
    .map_err(|error| format!("cannot receive the control answer: {error}"))?;
    while filled < buffer.len() && !buffer[..filled].contains(&b'\n') {
        socket
            .set_read_timeout(Some(remaining(deadline)?))
            .map_err(|error| format!("cannot set the control socket's read timeout: {error}"))?;
        match socket.read(&mut buffer[filled..]) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(error) => return Err(format!("cannot read the control answer: {error}")),
        }
    }
    let end = buffer[..filled]
        .iter()
        .position(|byte| *byte == b'\n')
        .ok_or_else(|| "the control answer never terminated with a newline".to_owned())?;
    let line = core::str::from_utf8(&buffer[..end])
        .map_err(|error| format!("the control answer is not utf-8: {error}"))?
        .to_owned();
    Ok((line, descriptors))
}

/// Validates a received transfer against what its answer promised and builds a read-only
/// reader over it, in the order `src/ffi/mod.rs`'s `session_from_attachment` validates:
/// descriptor count, then the mapping, then the header's declared identity.
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

/// The cadence a lease declared as `lease_ttl_ms` is renewed on.
fn renew_interval(lease_ttl_ms: u64) -> Duration {
    if lease_ttl_ms == 0 {
        return MAX_RENEW_INTERVAL;
    }
    (Duration::from_millis(lease_ttl_ms) / RENEW_TTL_DIVISOR)
        .clamp(MIN_RENEW_INTERVAL, MAX_RENEW_INTERVAL)
}

/// How long one renewal exchange may take. Never longer than the cadence itself.
fn renew_exchange_budget(interval: Duration) -> Duration {
    interval.min(Duration::from_secs(1))
}

/// Acknowledged renewals and failed exchanges, as the keeper reports them to the run.
#[derive(Default)]
struct LeaseCounters {
    renewed: AtomicU64,
    failed: AtomicU64,
}

/// The control session every lease was taken on, held open for the whole run by a keeper
/// thread of its own.
///
/// The connection is the lease: `pmwsd` releases every market a session held when it closes,
/// so this guard is the difference between measuring a market set and measuring a segment
/// nobody publishes into. The keeper is a separate thread, and that placement is the
/// measurement's: a renewal syscall on the observing thread would land inside the interval
/// this leg reports.
struct ControlLease {
    stop: Arc<AtomicBool>,
    counters: Arc<LeaseCounters>,
    keeper: Option<std::thread::JoinHandle<()>>,
}

impl ControlLease {
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
    /// The daemon closed the session; every lease went with it and no later exchange can
    /// succeed, so the keeper stops rather than counting one loss per cadence.
    SessionLost(String),
    /// This exchange failed and the session may well still be there.
    Exchange(String),
}

/// The keeper thread's own state.
struct Keeper {
    socket: UnixStream,
    /// Bytes read that are not yet a complete answer line, so a partial read is resumed
    /// rather than mistaken for a malformed answer.
    pending: Vec<u8>,
    budget: Duration,
    counters: Arc<LeaseCounters>,
    noted: bool,
}

impl Keeper {
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
                    "renewal failed: {message}; the observation continues, and the session \
                     still holds the leases"
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
                        "the daemon closed the control session; the leases it held are gone"
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

    /// Says something once, so a steady cadence does not repeat one broken session's note for
    /// every renewal left in a long run.
    fn note(&mut self, message: String) {
        if self.noted {
            return;
        }
        self.noted = true;
        eprintln!("matched_consumer: note: {message}");
    }
}

/// Whether the keeper goes on renewing after one exchange.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Keeping {
    Renewing,
    Stopped,
}

/// One half of a split latency: measured, or absent for want of a stamp, or rejected as
/// implausible.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Sample {
    Absent,
    Implausible,
    Measured(u64),
}

/// The three latencies one observation carries, in nanoseconds, exactly as
/// `examples/latency_probe.rs` splits them: `daemon` is `commit - arrival`, both stamps
/// written by the daemon on one clock in one process; `consumer` is `observed - commit`; and
/// `end_to_end` is `observed - arrival`.
///
/// Per observation the three satisfy `daemon + consumer == end_to_end` whenever all three are
/// measured, because they are three differences of the same three instants. Their percentiles
/// do not add: each distribution is ordered independently.
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
    daemon_kept: Vec<u64>,
    daemon_discarded: u64,
    consumer_kept: Vec<u64>,
    consumer_discarded: u64,
    wakes: u64,
    timeouts: u64,
    delivered: u64,
    rescans: u64,
    /// States read for a market this session did not lease, which the segment carries because
    /// another session or an operator pin holds it. Counted and never logged: an `.obs` file
    /// carries this leg's pinned set and nothing else.
    unleased_states: u64,
    /// How many of the segment's directory entries this run was ever handed a dirty entry
    /// for, this session's leases and every other holder's markets together.
    directory_entries_seen: usize,
    renewals: u64,
    renew_failures: u64,
    terminated: bool,
}

/// What one drain round needs to observe a market, gathered so the recursion stays one
/// parameter per concern rather than a long positional list.
struct Observing<'a> {
    reader: &'a SegmentReader,
    wanted: &'a HashMap<String, SlugId>,
    log: &'a mut ObsLog,
    last_revision: HashMap<u32, u64>,
    report: Report,
}

/// Observes the segment until `--seconds` elapses or the process is asked to terminate,
/// recording one `.obs` row per market revision this session leased.
///
/// Nothing in this loop touches the control session: the leases are kept by their own keeper
/// thread, so no renewal syscall can land between a state's publication and this leg's
/// observation of it.
fn observe_run(
    reader: &SegmentReader,
    args: &Args,
    parking_unavailable: bool,
    stop: &AtomicBool,
    wanted: &HashMap<String, SlugId>,
    log: &mut ObsLog,
) -> Report {
    let start = Instant::now();
    let deadline = start + Duration::from_secs(args.seconds);
    let spin_budget = Duration::from_micros(args.spin_micros);
    let mut last_seen = reader.publication_generation();
    let mut cursor = reader.dirty_cursor();
    let mut seen: BTreeSet<u32> = BTreeSet::new();
    let mut state = Observing {
        reader,
        wanted,
        log,
        last_revision: HashMap::new(),
        report: Report::default(),
    };

    while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
        let outcome = if parking_unavailable {
            spin_poll(reader, last_seen, deadline, stop)
        } else {
            match reader.wait_for_publication(last_seen, spin_budget, Some(PARK_POLL_TIMEOUT)) {
                Ok(outcome) => outcome,
                Err(fault) => {
                    eprintln!("matched_consumer: wait_for_publication failed: {fault}");
                    break;
                }
            }
        };
        match outcome {
            WaitOutcome::Changed(generation) => {
                state.report.wakes += 1;
                last_seen = generation;
                drain_dirty(&mut state, &mut cursor, &mut seen);
            }
            WaitOutcome::TimedOut(generation) => {
                state.report.timeouts += 1;
                last_seen = generation;
            }
        }
    }

    state.report.terminated = stop.load(Ordering::Relaxed);
    state.report.elapsed = start.elapsed();
    state.report.markets_seen = state.log.markets_observed();
    state.report.directory_entries_seen = seen.len();
    state.report
}

/// The pure spin wait: repeated acquire-loads of the publication generation until it moves,
/// the run's deadline passes, or this process is asked to terminate.
///
/// The stop flag is part of the exit condition rather than a check the deadline eventually
/// reaches: a spinning run that ignored it would hold its buffered observations until
/// `--seconds` elapsed and lose them to whatever killed it first.
fn spin_poll(
    reader: &SegmentReader,
    last_seen: u64,
    deadline: Instant,
    stop: &AtomicBool,
) -> WaitOutcome {
    loop {
        let current = reader.publication_generation();
        if current != last_seen {
            return WaitOutcome::Changed(current);
        }
        if Instant::now() >= deadline || stop.load(Ordering::Relaxed) {
            return WaitOutcome::TimedOut(current);
        }
        core::hint::spin_loop();
    }
}

/// Drains every entry the dirty-index ring currently holds, observing one state per
/// `Delivered` entry and, on a `Rescan`, one per published entry in the whole directory.
///
/// The rescan walks the directory rather than the already-seen set, because an overrun is
/// precisely the case where a market's only dirty entry was overwritten before this process
/// saw it. It ends this call rather than looping back, so a writer fast enough to lap the
/// ring during the walk cannot keep this function from returning to its caller's deadline.
fn drain_dirty(state: &mut Observing<'_>, cursor: &mut DirtyCursor, seen: &mut BTreeSet<u32>) {
    loop {
        match state.reader.next_dirty(cursor) {
            DirtyPoll::Delivered {
                directory_index, ..
            } => {
                state.report.delivered += 1;
                let _inserted = seen.insert(directory_index);
                observe(state, directory_index);
            }
            DirtyPoll::Rescan => {
                state.report.rescans += 1;
                let capacity = state.reader.geometry().layout().directory_capacity();
                for directory_index in 0..capacity {
                    if state
                        .reader
                        .resolve_directory_index(directory_index)
                        .is_none()
                    {
                        continue;
                    }
                    let _inserted = seen.insert(directory_index);
                    observe(state, directory_index);
                }
                return;
            }
            DirtyPoll::Idle => break,
        }
    }
}

/// Reads one market's published state and, for a revision this leg has not already observed,
/// records the observation.
///
/// `t_obs` is stamped the instant [`SegmentReader::read`] returns, which is the harness's
/// boundary: the state is readable in this leg's own representation and strategy code could
/// act on it. Everything after — the revision dedup, the content digest, the log append, the
/// latency split — is harness cost, deliberately after the stamp.
///
/// A resolve or read failure is silently skipped, exactly as `examples/latency_probe.rs`
/// treats one: the next dirty entry or rescan for the same market tries again, so nothing
/// here is counted as a skip or a discard, both of which name an observation that was taken
/// and then rejected.
fn observe(state: &mut Observing<'_>, directory_index: u32) {
    let Some(handle) = state.reader.resolve_directory_index(directory_index) else {
        return;
    };
    let Ok(snapshot) = state.reader.read(handle) else {
        return;
    };
    let observed = now_epoch_nanos();
    let revision = snapshot.revision();
    if state.last_revision.get(&directory_index) == Some(&revision) {
        return;
    }
    state.last_revision.insert(directory_index, revision);
    let Some(slug) = state.wanted.get(snapshot.market().key().value()).copied() else {
        state.report.unleased_states += 1;
        return;
    };
    let digest = level_digest(snapshot.levels());
    let _seq = state.log.record(slug, observed, digest, revision);
    let split = latencies(
        snapshot.arrival_time_nanos(),
        snapshot.commit_time_nanos(),
        observed,
    );
    match split.end_to_end {
        Sample::Measured(delta) => state.report.kept.push(delta),
        Sample::Implausible => state.report.discarded += 1,
        Sample::Absent => state.report.skipped += 1,
    }
    match split.daemon {
        Sample::Measured(delta) => state.report.daemon_kept.push(delta),
        Sample::Implausible => state.report.daemon_discarded += 1,
        Sample::Absent => {}
    }
    match split.consumer {
        Sample::Measured(delta) => state.report.consumer_kept.push(delta),
        Sample::Implausible => state.report.consumer_discarded += 1,
        Sample::Absent => {}
    }
}

fn print_report(
    args: &Args,
    session: &Session,
    report: &Report,
    log: &ObsLog,
    note: Option<&str>,
    requested: usize,
) {
    println!("label: {}", args.label);
    println!("leg: {LEG}");
    println!("mode: parked");
    println!(
        "mode_effective: {}",
        if session.doorbell == DoorbellLocation::Page {
            "spin"
        } else {
            "parked"
        }
    );
    if let Some(note) = note {
        println!("note: {note}");
    }
    println!("spin_budget_us: {}", args.spin_micros);
    println!("duration_seconds: {:.3}", report.elapsed.as_secs_f64());
    println!("terminated_by_signal: {}", report.terminated);
    println!("markets_requested: {requested}");
    println!("markets_leased: {}", session.leased.len());
    println!("lease_attach_failures: {}", session.attach_failures.len());
    for (slug, message) in &session.attach_failures {
        println!("lease_attach_failure: {slug}: {message}");
    }
    println!("lease_foreign_segment: {}", session.foreign_segment.len());
    for slug in &session.foreign_segment {
        println!("foreign_segment_market: {slug}");
    }
    println!("lease_renewals: {}", report.renewals);
    println!("lease_renew_failures: {}", report.renew_failures);
    println!("markets_observed: {}", report.markets_seen);
    println!("obs_out: {}", args.obs_out.display());
    println!("obs_rows: {}", log.rows());
    println!("obs_events_total: {}", log.events_total());
    println!("obs_dropped: {}", log.dropped());
    println!("wakes: {}", report.wakes);
    let seconds = report.elapsed.as_secs_f64();
    let rate = if seconds > 0.0 {
        report.wakes as f64 / seconds
    } else {
        0.0
    };
    println!("wakes_per_second: {rate:.3}");
    println!("timeouts: {}", report.timeouts);
    println!("dirty_delivered: {}", report.delivered);
    println!("rescans: {}", report.rescans);
    println!("unleased_states: {}", report.unleased_states);
    println!("directory_entries_seen: {}", report.directory_entries_seen);
    println!("samples_skipped: {}", report.skipped);
    println!(
        "daemon_latency: commit_time - arrival_time, both stamped by this daemon on one clock \
         in one process (no consumer clock is involved)"
    );
    print_distribution("daemon", &report.daemon_kept, report.daemon_discarded);
    println!("consumer_latency: observation - commit_time");
    print_distribution("consumer", &report.consumer_kept, report.consumer_discarded);
    println!("end_to_end_latency: observation - arrival_time");
    print_distribution("end_to_end", &report.kept, report.discarded);
    if report.kept.len() < MIN_REPORTABLE_SAMPLES {
        return;
    }
    let mut sorted = report.kept.clone();
    sorted.sort_unstable();
    println!("p50_us: {}", format_micros(quantile(&sorted, 500)));
    println!("p99_us: {}", format_micros(quantile(&sorted, 990)));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cli(args: &[&str]) -> impl Iterator<Item = String> {
        std::iter::once("matched_consumer".to_owned())
            .chain(args.iter().map(|arg| (*arg).to_owned()))
    }

    fn complete() -> Vec<&'static str> {
        vec![
            "--control",
            "/tmp/pmwsd.sock",
            "--slugs",
            "/tmp/shard-00.txt",
            "--obs-out",
            "/tmp/rust-shm-00.obs",
            "--label",
            "m1, shard 0",
        ]
    }

    #[test]
    fn parse_args_accepts_the_harness_cli_contract_and_defaults_the_rest() {
        let args = parse_args(cli(&complete())).expect("the contract's flags parse");
        assert_eq!(args.control, PathBuf::from("/tmp/pmwsd.sock"));
        assert_eq!(args.slugs, PathBuf::from("/tmp/shard-00.txt"));
        assert_eq!(args.obs_out, PathBuf::from("/tmp/rust-shm-00.obs"));
        assert_eq!(args.label, "m1, shard 0");
        assert_eq!(args.seconds, 600);
        assert_eq!(args.spin_micros, 0);
        assert_eq!(args.lease_ttl_ms, None);
    }

    #[test]
    fn parse_args_accepts_the_optional_flags() {
        let mut flags = complete();
        flags.extend([
            "--seconds",
            "240",
            "--spin-micros",
            "50",
            "--lease-ttl-ms",
            "9000",
        ]);
        let args = parse_args(cli(&flags)).expect("the optional flags parse");
        assert_eq!(args.seconds, 240);
        assert_eq!(args.spin_micros, 50);
        assert_eq!(args.lease_ttl_ms, Some(9_000));
    }

    #[test]
    fn parse_args_requires_every_mandatory_flag() {
        for missing in ["--control", "--slugs", "--obs-out", "--label"] {
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

    #[test]
    fn parse_args_rejects_an_unknown_flag_and_an_out_of_range_duration() {
        let mut unknown = complete();
        unknown.push("--bogus");
        assert!(
            parse_args(cli(&unknown))
                .expect_err("an unknown flag is refused")
                .contains("--bogus")
        );
        let mut zero = complete();
        zero.extend(["--seconds", "0"]);
        assert!(
            parse_args(cli(&zero))
                .expect_err("a zero duration is refused")
                .contains("--seconds")
        );
    }

    /// The two halves of a split observation account for the whole of it, sample by sample.
    /// The percentiles never add this way, which is why the identity is pinned on the
    /// arithmetic rather than on a report's output.
    #[test]
    fn the_daemon_and_consumer_halves_sum_to_the_end_to_end_latency() {
        for (arrival, commit, observed) in [
            (1_000_u64, 1_400_u64, 2_500_u64),
            (0, 0, 0),
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

    /// A difference that runs backwards, or longer than any delivery path takes, is rejected
    /// rather than clamped.
    #[test]
    fn a_backwards_or_absurd_difference_is_discarded_rather_than_clamped() {
        let backwards = latencies(Some(2_000), Some(1_000), 3_000);
        assert_eq!(backwards.daemon, Sample::Implausible);
        assert_eq!(backwards.end_to_end, Sample::Measured(1_000));

        let absurd = latencies(Some(0), Some(IMPLAUSIBLE_NANOS), IMPLAUSIBLE_NANOS + 1);
        assert_eq!(absurd.daemon, Sample::Implausible);
        assert_eq!(absurd.end_to_end, Sample::Implausible);
        assert_eq!(absurd.consumer, Sample::Measured(1));
    }

    #[test]
    fn a_slot_with_no_venue_frame_behind_it_yields_only_the_consumer_half() {
        let split = latencies(None, Some(1_000), 2_000);
        assert_eq!(split.daemon, Sample::Absent);
        assert_eq!(split.consumer, Sample::Measured(1_000));
        assert_eq!(split.end_to_end, Sample::Absent);
    }

    /// The renewal cadence comes from the deadline the daemon declared, so every TTL an
    /// operator may configure fits several renewals inside itself.
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
        assert_eq!(renew_interval(0), MAX_RENEW_INTERVAL);
        assert_eq!(renew_interval(1), MIN_RENEW_INTERVAL);
        assert_eq!(
            renew_exchange_budget(MIN_RENEW_INTERVAL),
            MIN_RENEW_INTERVAL,
            "an exchange never outlives the cadence it was sent on"
        );
    }
}

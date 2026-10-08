//! `pmwsd` — the multi-market prediction-market data daemon.
//!
//! Reads one TOML configuration, partitions its market set into shards, runs each shard over
//! its own venue connection, and listens on a local Unix domain socket for control
//! conversations. Controllers submit desired market sets over that socket; they never inject
//! market data.
//!
//! Two kinds of caller share the socket. An operator sends one command and closes. A consumer
//! holds its connection open: attaching over it leases the market, and the daemon subscribes a
//! market nothing else holds to serve that lease and unsubscribes it when the last lease goes.
//! Aggregate demand — operator pins plus reference-counted leases — is combined here, and the
//! shards below see one desired set reconciled exactly as it always was.
//!
//! The control listener and the shards are separate tasks. A control command reaches a shard
//! through the same bounded, non-blocking handle a phase-1 consumer uses, so a control
//! client — fast, slow, or silent — can never delay ingestion. A shard whose control queue is
//! full answers `busy` and changes nothing.

use core::pin::Pin;
use core::task::Poll;
use core::time::Duration;
use pm_ws::daemon::{
    DaemonConfig, DaemonPlan, descriptor_envelope, random_instance_id, segment_file_name,
};
use pm_ws::limitless::shard::{
    MarketOutcome, MarketRejection, MarketStatus, PoolState, Shard, ShardControlError, ShardHandle,
    ShardMetrics, ShardSegment, ShardStats, ShardStopper, is_market_slug,
};
use pm_ws::{
    Attachment, ControlRequest, ControlResponse, DaemonStatus, DoorbellLocation,
    FEATURE_DOORBELL_PAGE, MAX_CONTROL_LINE_BYTES, MarketRow, PoolDegradeReason, ReplicaRole,
    STATUS_PAGE_MARKETS, SegmentConfig, SegmentRegion, SegmentWriter, ShardReport, own_euid,
    peer_euid, send_with_fds,
};
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, TryLockError};
use std::future::{Future, poll_fn};
use std::net::SocketAddr;
use std::os::fd::{AsFd, BorrowedFd};
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, Interest};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::{Semaphore, SemaphorePermit, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

/// How long one control client has to send its first request line before the daemon gives
/// up on it. A client that connects and says nothing costs one refused conversation.
///
/// It bounds the opening of a session and nothing after it: a consumer that has attached is
/// entitled to be silent for as long as it is reading its segment, and what bounds *that*
/// silence is the configured lease TTL, or nothing at all when none is configured.
const CONTROL_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// How much of one control connection's unterminated request line the daemon will hold.
///
/// One line's worth plus a read's worth: a read is taken only when no complete line is
/// already buffered, so what can accumulate is one partial request and whatever the last
/// read added, never a pipelining client's whole backlog.
const CONTROL_READ_CHUNK: usize = 8192;

/// How many requests one session is answered before the scan moves on to the next session.
///
/// Small enough that no session's backlog delays the rotation by more than this many answers
/// *per other ready session*: a client that pipelines a thousand requests is served like a
/// thousand clients that sent one each, rather than holding the control task until its buffer
/// runs out. What a dispatch does not reach is not lost — a session with a complete request
/// still in its buffer is readable by that alone, so it re-enters the rotation and is answered
/// on its next turn.
///
/// This is one session's turn, not the whole rotation: with `n` sessions ready at once, up to
/// `CONTROL_REQUESTS_PER_DISPATCH * (n - 1)` answers separate two turns of any one of them.
/// [`Sessions::serve`] states the bound that follows for time.
const CONTROL_REQUESTS_PER_DISPATCH: usize = 4;

/// How long a market nothing wants any more waits before its removal is tried again.
///
/// Demand is reconciled as desired state rather than as a sequence of transitions, so a
/// removal a shard could not take is not lost: the next reconciliation finds the same market
/// unwanted and tries again. This is how soon that happens when nothing else wakes the
/// control task.
const RECONCILE_RETRY: Duration = Duration::from_millis(250);

/// How long the daemon will spend writing one answer before abandoning the client.
///
/// A client that sends a request and then stops reading would otherwise hold the control
/// task on a socket whose buffer is full. Shards run as their own tasks, so ingestion is
/// never behind this either way; what this bounds is how long the *next* operator command
/// waits behind an inattentive one.
const CONTROL_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// The permissions the control socket is created with: the owner alone may command this
/// daemon.
const CONTROL_SOCKET_MODE: u32 = 0o600;

/// The exit status of a daemon whose shard stopped feeding.
const EXIT_SHARD_LOST: i32 = 3;

/// The generation every segment this daemon formats declares.
///
/// A segment is created once per run and never reformatted, so a run has exactly one
/// generation; the identity that separates two runs is the instance identifier in the header
/// and in the file name, not this. It is a named constant because an attaching consumer is
/// promised it in the answer line and must find the same number in the header it validates.
const SEGMENT_GENERATION: u64 = 1;

/// How many metrics connections this daemon serves at once.
///
/// A scrape is a monitoring server's periodic collection rather than a consumer surface, so
/// the bound is small on purpose. A connection past it is closed immediately rather than
/// queued: a queue of connections nothing is serving is a wait with no bound on it.
const MAX_METRICS_CONNECTIONS: usize = 4;

/// How much of one metrics request head this daemon will hold before refusing the request.
const METRICS_HEAD_LIMIT: usize = 8192;

/// How long one metrics connection has to send its request head, how long its collection may
/// take, and how long this daemon will spend writing the answer.
const METRICS_IO_TIMEOUT: Duration = Duration::from_secs(5);

/// How long one whole collection may spend inside the control loop before it is abandoned.
///
/// Shorter than [`METRICS_IO_TIMEOUT`] on purpose. The loop this bounds is the one that
/// accepts control connections, answers sessions, sweeps leases, and notices a shard that
/// stopped feeding; a scrape enters it once, and a shard that took the command and never
/// answered must cost it a bounded wait rather than the rest of the run. Answering inside
/// the scrape's own deadline is also what makes a failed collection this daemon's own `503`
/// rather than a scraper's timeout, which reads as a daemon that is not there at all.
const METRICS_COLLECTION_TIMEOUT: Duration = Duration::from_secs(1);

/// How long the metrics acceptor waits after an accept it could not complete before it
/// accepts again.
///
/// Descriptor or buffer exhaustion — the process's own or the host's — is transient and not
/// this endpoint's to resolve, so the listener is kept and the accept is retried. The pause
/// is what keeps a failing accept from spinning on the thread every shard also runs on.
const METRICS_ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// The only request target this endpoint answers with numbers.
const METRICS_TARGET: &str = "/metrics";

/// The exposition format the metrics body is written in.
const EXPOSITION_CONTENT_TYPE: &str = "text/plain; version=0.0.4";

/// The content type every answer that is not an exposition carries.
const PLAIN_CONTENT_TYPE: &str = "text/plain; charset=utf-8";

/// How many descriptors one accepted attach transfers: the segment's read-only descriptor,
/// and nothing else.
///
/// A constant rather than a count of what was collected, because it is a protocol promise the
/// consumer checks the arriving message against: a segment declaring a page doorbell is served
/// exactly as one declaring a header doorbell is, so the placement the answer names changes
/// what a consumer may *do* with the segment and never what rides the message. See [`attach`]
/// for why the doorbell page's own descriptor never crosses.
const TRANSFERRED_DESCRIPTORS: u8 = 1;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let invocation = match parse_args(std::env::args()) {
        Ok(invocation) => invocation,
        Err(message) => {
            eprintln!("pmwsd: {message}");
            eprintln!("usage: pmwsd --config <path>");
            std::process::exit(2);
        }
    };
    let plan = match DaemonConfig::load(invocation.config.as_path()) {
        Ok(plan) => plan,
        Err(error) => {
            eprintln!("pmwsd: {error}");
            std::process::exit(2);
        }
    };
    match run(plan, invocation.kill_shard_after).await {
        Ok(()) => {}
        Err(Failure { message, code }) => {
            eprintln!("pmwsd: {message}");
            std::process::exit(code);
        }
    }
}

/// Why the daemon stopped, and what it exits with.
struct Failure {
    message: String,
    code: i32,
}

impl From<String> for Failure {
    fn from(message: String) -> Self {
        Self { message, code: 1 }
    }
}

/// What the command line asked for: the configuration, and any diagnostic fault injection.
struct Invocation {
    config: PathBuf,
    /// Diagnostic fault injection: end the first shard's run after this long, so a daemon's
    /// supervision of a shard that stopped feeding is observable. `None` in any deployment.
    kill_shard_after: Option<Duration>,
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Invocation, String> {
    let _binary = args.next();
    let mut config = None;
    let mut kill_shard_after = None;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--config" => {
                config = Some(PathBuf::from(
                    args.next().ok_or("--config requires a value")?,
                ));
            }
            "--kill-shard-after" => {
                let value = args.next().ok_or("--kill-shard-after requires a value")?;
                let millis: u64 = value
                    .parse()
                    .map_err(|_| "--kill-shard-after takes milliseconds".to_owned())?;
                kill_shard_after = Some(Duration::from_millis(millis));
            }
            other => return Err(format!("unknown flag {other}")),
        }
    }
    Ok(Invocation {
        config: config.ok_or_else(|| "--config is required".to_owned())?,
        kill_shard_after,
    })
}

/// This process's own limit on open file descriptors, as `getrlimit` answers for it.
///
/// The soft limit is what the process is actually held to, so it is the one reported. The
/// three answers are kept apart because the preflight owes a different response to each: a
/// figure to check against, nothing to check against, and no answer at all.
enum DescriptorLimit {
    /// The soft limit, in descriptors.
    Bounded(usize),
    /// No soft limit. No configuration can exceed it and a preflight against it would be
    /// theatre.
    Unlimited,
    /// `getrlimit` failed, so this process does not know what it may open.
    Unreadable,
}

fn descriptor_limit() -> DescriptorLimit {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `getrlimit` writes one `rlimit` through the pointer it is given and reads
    // nothing else through it. The pointer is to a live local of exactly that type, valid
    // for the duration of the call, and the return value is checked before the value is read.
    let read = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &raw mut limit) };
    if read != 0 {
        return DescriptorLimit::Unreadable;
    }
    if limit.rlim_cur == libc::RLIM_INFINITY {
        return DescriptorLimit::Unlimited;
    }
    usize::try_from(limit.rlim_cur).map_or(DescriptorLimit::Unlimited, DescriptorLimit::Bounded)
}

/// Refuses a configuration whose peak descriptor envelope does not fit this process's limit.
///
/// `docs/design.md` requires the daemon to calculate its requested resource envelope before
/// accepting production traffic, and this is where that calculation is made binding:
/// [`descriptor_envelope`] names what the configured shard count peaks at — counting the
/// metrics listener when `metrics_listener` says one is configured — and a process that could
/// not hold it is told so by a sentence naming both figures rather than by a segment that
/// fails to open somewhere in the middle of a fleet.
///
/// A limit that cannot be read refuses startup. A preflight whose whole purpose is to fail
/// before the first venue dial cannot honestly be skipped on the grounds that it could not be
/// performed: the alternative is a fleet that opens descriptors until one of them does not,
/// which is exactly the failure this exists to move earlier.
fn check_descriptor_envelope(
    shards: usize,
    replicas: usize,
    metrics_listener: bool,
) -> Result<(), Failure> {
    let needed = descriptor_envelope(shards, replicas, metrics_listener);
    match descriptor_limit() {
        DescriptorLimit::Bounded(available) if needed > available => Err(Failure::from(format!(
            "this configuration peaks at {needed} open file descriptors across {shards} \
             shard(s) and this process may open {available}; raise the descriptor limit or \
             configure fewer shards"
        ))),
        DescriptorLimit::Unreadable => Err(Failure::from(format!(
            "this configuration peaks at {needed} open file descriptors across {shards} \
             shard(s) and this process's descriptor limit could not be read, so the envelope \
             cannot be checked; raise or expose the limit and start again"
        ))),
        DescriptorLimit::Bounded(_) | DescriptorLimit::Unlimited => Ok(()),
    }
}

/// Starts every shard, serves control commands until a signal, then shuts down.
///
/// Failure here is a startup failure — a resource envelope this process could not hold, a
/// socket that cannot be bound, or a shard whose configuration the shard itself refuses — or
/// a shard that stopped feeding. Nothing a control client sends can end the run.
///
/// Startup is stage-then-serve. The descriptor envelope is checked before anything is
/// opened, and the loop below that creates every segment and installs every shard contains
/// no `await`, so on this current-thread runtime not one spawned shard task is polled until
/// all of them exist. A configuration that cannot fit therefore fails before the first venue
/// dial rather than half-way through one: the process that reports the failure has sent the
/// venue nothing.
///
/// A shard task runs for as long as the daemon does. Its completion is therefore never
/// ordinary: a shard that returned has stopped keeping its markets' books, and a daemon that
/// went on serving `status` for them would be reporting state nothing is maintaining. So any
/// shard completing before shutdown ends the daemon, loudly and with a non-zero status, and
/// the socket goes with it through the same guard an orderly shutdown drops.
async fn run(plan: DaemonPlan, kill_shard_after: Option<Duration>) -> Result<(), Failure> {
    check_descriptor_envelope(
        plan.shards.len(),
        plan.replicas(),
        plan.metrics_listen.is_some(),
    )?;
    let capacity = plan.markets_per_shard();
    let socket = plan.control_socket.clone();
    let lease_ttl = plan.lease_ttl;
    let max_control_sessions = plan.max_control_sessions;
    let (listener, guard) = bind_control(socket.as_path())?;
    let metrics = match plan.metrics_listen {
        Some(address) => Some(bind_metrics(address).await?),
        None => None,
    };
    let instance = random_instance_id();
    let mut segments = SegmentFiles::default();

    let mut router = Router::new(
        capacity,
        instance,
        metrics.as_ref().map(|(_, bound)| bound.to_string()),
    );
    let mut tasks: Vec<JoinHandle<ShardStats>> = Vec::new();
    let mut stoppers: Vec<ShardStopper> = Vec::new();
    for (index, config) in plan.shards.into_iter().enumerate() {
        let markets = config.markets.clone();
        let mut shard = Shard::new(config).map_err(|error| format!("shard {index}: {error}"))?;
        let (segment, target) = open_segment(&plan.delivery, instance, index, &mut segments)
            .map_err(|message| format!("shard {index}: {message}"))?;
        shard
            .publish_into(segment)
            .map_err(|error| format!("shard {index}: segment refused this shard: {error:?}"))?;
        router.install(shard.handle(), &markets, target);
        stoppers.push(shard.stopper());
        let deadline = kill_shard_after
            .filter(|_| index == 0)
            .map(|after| Instant::now() + after);
        tasks.push(tokio::spawn(async move { shard.run_until(deadline).await }));
    }
    println!(
        "pmwsd pid={} shards={} markets={} socket={}{}",
        std::process::id(),
        router.shards(),
        router.assigned(),
        socket.display(),
        metrics
            .as_ref()
            .map_or_else(String::new, |(_, bound)| format!(" metrics={bound}"))
    );
    let (mut scrapes, metrics_acceptor) = match metrics {
        Some((bound_listener, _bound)) => {
            let (requests, scrapes) = mpsc::channel(1);
            (
                Some(scrapes),
                Some(tokio::spawn(serve_metrics(bound_listener, requests))),
            )
        }
        None => (None, None),
    };

    let mut interrupt =
        signal(SignalKind::interrupt()).map_err(|error| format!("SIGINT: {error}"))?;
    let mut terminate =
        signal(SignalKind::terminate()).map_err(|error| format!("SIGTERM: {error}"))?;
    let mut lost = None;
    let mut sessions = Sessions::new(max_control_sessions);
    loop {
        let deadline = sessions.next_deadline(lease_ttl, &router);
        let event = tokio::select! {
            _ = interrupt.recv() => ControlEvent::Shutdown,
            _ = terminate.recv() => ControlEvent::Shutdown,
            (index, outcome) = first_shard_to_finish(&mut tasks) => {
                ControlEvent::ShardFinished(index, Box::new(outcome))
            }
            accepted = listener.accept() => ControlEvent::Accepted(accepted.ok().map(|(stream, _address)| stream)),
            index = sessions.next_readable() => ControlEvent::Readable(index),
            scrape = next_scrape(scrapes.as_mut()) => ControlEvent::Scrape(scrape),
            () = until(deadline) => ControlEvent::Deadline,
        };
        match event {
            ControlEvent::Shutdown => break,
            ControlEvent::ShardFinished(index, outcome) => {
                let message = match *outcome {
                    Ok(_stats) => format!(
                        "shard {index} stopped feeding; its markets are no longer maintained"
                    ),
                    Err(error) => format!("shard {index} did not survive: {error}"),
                };
                lost = Some((index, message));
                break;
            }
            ControlEvent::Accepted(Some(stream)) => sessions.accept(stream, &mut router).await,
            ControlEvent::Accepted(None) => {}
            ControlEvent::Readable(index) => {
                sessions.serve(index, &mut router, lease_ttl).await;
            }
            ControlEvent::Scrape(scrape) => {
                let collected = router.metrics().await;
                let _answered = scrape.reply.send(collected);
            }
            ControlEvent::Deadline => sessions.sweep(lease_ttl, &mut router).await,
        }
    }

    drop(listener);
    if let Some(acceptor) = metrics_acceptor {
        acceptor.abort();
    }
    if let Some((index, _)) = lost.as_ref() {
        let _finished = tasks.remove(*index);
    }
    for stopper in &stoppers {
        stopper.stop();
    }
    for (index, task) in tasks.into_iter().enumerate() {
        match task.await {
            Ok(stats) => {
                println!(
                    "shard {index}: connections={} subscriptions={} snapshots={} resolutions={} losses={} unrouted={} queue_age_max_us={} queue_age_p99_us={}",
                    stats.connection_attempts,
                    stats.subscriptions_emitted,
                    stats.snapshots_applied,
                    stats.resolutions_forwarded,
                    stats.continuity_losses,
                    stats.frames_unrouted,
                    stats.queue_age.max_micros,
                    stats.queue_age.p99_micros
                );
                if let Some(failure) = stats.segment_failure {
                    eprintln!("shard {index}: segment publication refused: {failure}");
                }
            }
            Err(error) => eprintln!("shard {index}: did not finish: {error}"),
        }
    }
    drop(segments);
    drop(guard);
    match lost {
        Some((_, message)) => Err(Failure {
            message,
            code: EXIT_SHARD_LOST,
        }),
        None => Ok(()),
    }
}

/// What woke the daemon's control loop.
///
/// The `select!` arms produce this and nothing else, so every borrow they took is released
/// before anything acts on what happened: serving a session, accepting one, and sweeping all
/// of them each need the whole session set and the router, and none of them can have it while
/// a readiness future is still holding a piece.
enum ControlEvent {
    Shutdown,
    /// The shard's statistics are boxed because they are much the largest thing any wake
    /// carries and this one happens once, at the end of the run, while every other wake pays
    /// the enum's size on the ordinary path.
    ShardFinished(usize, Box<Result<ShardStats, tokio::task::JoinError>>),
    /// One accepted control connection, or `None` for an accept that failed.
    Accepted(Option<UnixStream>),
    Readable(usize),
    /// One metrics connection asking for the numbers only the control loop can collect.
    Scrape(MetricsScrape),
    Deadline,
}

/// Resolves when a metrics connection asks for this daemon's numbers, and never at all for a
/// daemon serving no metrics endpoint.
///
/// The pending arm is what lets one `select!` carry an optional listener: an absent endpoint
/// is a branch that never fires rather than a second loop, exactly as [`until`] carries an
/// absent deadline.
async fn next_scrape(scrapes: Option<&mut mpsc::Receiver<MetricsScrape>>) -> MetricsScrape {
    match scrapes {
        Some(scrapes) => match scrapes.recv().await {
            Some(scrape) => scrape,
            None => std::future::pending().await,
        },
        None => std::future::pending().await,
    }
}

/// Resolves when any shard task finishes, naming which one and what it returned.
///
/// Every handle is polled on each wake, so the waker of whichever finishes first is the one
/// that resolves this. Nothing is consumed: the caller keeps the whole set, and the handle
/// that resolved is the only one that must not be polled again.
async fn first_shard_to_finish(
    tasks: &mut [JoinHandle<ShardStats>],
) -> (usize, Result<ShardStats, tokio::task::JoinError>) {
    poll_fn(|context| {
        for (index, task) in tasks.iter_mut().enumerate() {
            if let Poll::Ready(outcome) = Pin::new(task).poll(context) {
                return Poll::Ready((index, outcome));
            }
        }
        Poll::Pending
    })
    .await
}

/// The bound control socket, the lock that makes this daemon its only owner, and the
/// identity that lets it be unlinked safely.
///
/// The guard owns the cleanup, so a panic anywhere in the run unlinks the same socket an
/// orderly shutdown would, and no other.
struct ControlSocket {
    path: PathBuf,
    /// Held for the daemon's lifetime. The operating system releases it when this file is
    /// closed, including on a crash, which is what makes an abandoned socket detectable
    /// rather than permanent.
    _lock: File,
    /// The device and inode of the socket this daemon created. Unlinking is conditional on
    /// the path still naming a socket with this identity, because a file system may hand a
    /// freed inode number to the next file created.
    identity: (u64, u64),
}

impl Drop for ControlSocket {
    fn drop(&mut self) {
        let same = std::fs::symlink_metadata(self.path.as_path()).is_ok_and(|metadata| {
            metadata.file_type().is_socket() && (metadata.dev(), metadata.ino()) == self.identity
        });
        if same {
            let _removed = std::fs::remove_file(self.path.as_path());
        }
    }
}

/// The path of the lock file guarding one control socket.
fn lock_path(socket: &Path) -> PathBuf {
    let mut name = socket.as_os_str().to_owned();
    name.push(".lock");
    PathBuf::from(name)
}

/// Creates the control socket under a lock that makes this daemon its only owner.
///
/// The lock comes first and is held for the whole run, because the checks that follow are
/// otherwise a race: two daemons starting together can both find the same stale socket, both
/// unlink it, and the second can unlink the first's freshly bound one. An exclusive lock on
/// an adjacent file makes "is this socket abandoned?" a question only one process asks at a
/// time, and the operating system releases it even when the holder crashes, so a lock left
/// behind by a dead daemon does not wedge its successor.
///
/// Under the lock, an existing path is inspected before anything is removed. Anything that is
/// not a socket — a regular file, a directory, a device — is refused outright: a configuration
/// file names arbitrary paths, and a daemon that unlinks whatever it is pointed at is a daemon
/// that can be told to delete something. A path that *is* a socket is probed with a connect:
/// one that answers belongs to something still serving and is refused, and one that refuses
/// the connection is stale and is replaced.
///
/// The bound socket's device and inode are recorded so shutdown unlinks the socket this
/// daemon created and never a successor's. The lock file itself is left in place: unlinking
/// it would reintroduce exactly the race it exists to close.
fn bind_control(path: &Path) -> Result<(UnixListener, ControlSocket), String> {
    let guard = lock_path(path);
    let lock = File::options()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(guard.as_path())
        .map_err(|error| format!("control lock {} not opened: {error}", guard.display()))?;
    match lock.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => {
            return Err(format!(
                "another daemon holds the control lock for {}",
                path.display()
            ));
        }
        Err(TryLockError::Error(error)) => {
            return Err(format!(
                "control lock for {} not taken: {error}",
                path.display()
            ));
        }
    }
    if let Ok(metadata) = std::fs::symlink_metadata(path) {
        if !metadata.file_type().is_socket() {
            return Err(format!(
                "control socket {} exists and is not a socket",
                path.display()
            ));
        }
        match std::os::unix::net::UnixStream::connect(path) {
            Ok(_live) => {
                return Err(format!("another daemon is listening on {}", path.display()));
            }
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                std::fs::remove_file(path).map_err(|error| {
                    format!(
                        "stale control socket {} not removed: {error}",
                        path.display()
                    )
                })?;
            }
            Err(error) => {
                return Err(format!(
                    "control socket {} could not be probed: {error}",
                    path.display()
                ));
            }
        }
    }
    let listener = UnixListener::bind(path)
        .map_err(|error| format!("control socket {}: {error}", path.display()))?;
    std::fs::set_permissions(path, PermissionsExt::from_mode(CONTROL_SOCKET_MODE))
        .map_err(|error| format!("control socket {} permissions: {error}", path.display()))?;
    let bound = std::fs::symlink_metadata(path)
        .map_err(|error| format!("control socket {} not stat'd: {error}", path.display()))?;
    Ok((
        listener,
        ControlSocket {
            path: path.to_path_buf(),
            _lock: lock,
            identity: (bound.dev(), bound.ino()),
        },
    ))
}

/// The segment files this daemon created, unlinked when the run ends.
///
/// The daemon owns the cleanup for the same reason it owns the control socket's: a panic
/// anywhere in the run must leave no segment behind that a later consumer could map and read
/// as live. Only files this daemon created exclusively are recorded here, and a doorbell page
/// is unlinked only when the writer reported putting one there, so nothing this daemon did
/// not create is ever removed.
///
/// Each unlink is conditional on the path still naming the object this run put there, exactly
/// as [`ControlSocket`]'s is: an operator or another process may replace a segment file mid
/// run, and a daemon that removed whatever the path resolved to at shutdown would delete a
/// successor's file. Both identities come from the `fstat` of the object each file was
/// created as — the segment's from the descriptor [`SegmentRegion`] retained, the page's from
/// the one [`SegmentWriter`] retained — never from a second lookup of a mutable name.
///
/// The condition narrows the window; it does not close it. `still_names` and `remove_file`
/// are two syscalls against a name, so a replacement landing between them is removed anyway:
/// the check proves what the name resolved to a moment ago, not what `remove_file` will act
/// on. Closing it needs an unlink relative to a directory descriptor with the object named,
/// which no portable API this daemon may use offers. It sits inside the declared trust
/// boundary — a same-user local peer, `docs/notes/shared-memory-model.md` §4.2 — where a
/// party able to win that race is already able to remove the file itself.
#[derive(Default)]
struct SegmentFiles {
    created: Vec<CreatedSegment>,
}

/// One created segment file and the sibling doorbell page beside it, each with the device and
/// inode that make its removal conditional.
struct CreatedSegment {
    path: PathBuf,
    identity: (u64, u64),
    /// The doorbell page's own creation identity, `None` for a segment whose doorbell lives
    /// in its header. A page this never identified is never unlinked.
    page: Option<(u64, u64)>,
}

/// Whether `path` still names the object `identity` was taken from.
fn still_names(path: &Path, identity: (u64, u64)) -> bool {
    std::fs::symlink_metadata(path)
        .is_ok_and(|metadata| (metadata.dev(), metadata.ino()) == identity)
}

impl Drop for SegmentFiles {
    fn drop(&mut self) {
        for created in &self.created {
            if still_names(created.path.as_path(), created.identity) {
                let _removed = std::fs::remove_file(created.path.as_path());
            }
            let page_path = doorbell_page_path(created.path.as_path());
            if created
                .page
                .is_some_and(|identity| still_names(page_path.as_path(), identity))
            {
                let _removed = std::fs::remove_file(page_path.as_path());
            }
        }
    }
}

/// The flags the retained read-only open adds to a plain read: `O_NOFOLLOW | O_NONBLOCK`.
///
/// `O_NOFOLLOW` refuses a symlink planted at the final component outright rather than
/// resolving it and leaving [`names_the_created_object`] to notice afterwards. `O_NONBLOCK`
/// bounds the `open` syscall itself: opening a FIFO for reading blocks until a writer arrives,
/// so without it a named pipe left at the path would hang startup instead of failing it. It
/// applies to the open only — the descriptor is mapped, never read — and neither flag replaces
/// the identity check, which is what actually decides that this is the created object.
///
/// The values are the platform's own, spelled out here because this crate binds the few
/// syscall constants it needs directly rather than taking a dependency for them, exactly as
/// `src/shm/channel.rs` does. Each was read from that platform's own `fcntl.h` rather than
/// recalled, and only targets checked that way are listed: a wrong value here is silent —
/// the `open` would carry some other flag and this guard would evaporate with nothing
/// failing — so a target not on this list fails to compile instead.
#[cfg(target_os = "macos")]
const RETAINED_OPEN_FLAGS: i32 = 0x0000_0100 | 0x0000_0004;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
const RETAINED_OPEN_FLAGS: i32 = 0o400_000 | 0o4_000;

/// How the segment's retained read-only descriptor is opened.
fn retained_open_options() -> std::fs::OpenOptions {
    use std::os::unix::fs::OpenOptionsExt;
    let mut options = std::fs::OpenOptions::new();
    let _ = options.read(true).custom_flags(RETAINED_OPEN_FLAGS);
    options
}

/// Whether the object `opened` describes is the one `created` was `fstat`ed from, and has the
/// shape a segment of `bytes` bytes has.
///
/// `created` is the identity of the creation object itself, so this compares a descriptor
/// against the file this daemon made rather than against a second resolution of its name. The
/// two refusals are kept apart because they mean different things: a different device and
/// inode is another object at the name, while the created object in an unexpected shape is
/// the file this daemon made, changed under it.
fn names_the_created_object(
    path: &Path,
    created: (u64, u64),
    opened: &std::fs::Metadata,
    bytes: u64,
) -> Result<(), String> {
    if (opened.dev(), opened.ino()) != created {
        return Err(format!(
            "segment {}: the read-only descriptor names a different object than the one \
             created there",
            path.display()
        ));
    }
    if !opened.file_type().is_file() || opened.len() != bytes {
        return Err(format!(
            "segment {}: the created object is no longer a regular file of {bytes} bytes",
            path.display()
        ));
    }
    Ok(())
}

/// `<segment path>.doorbell`, exactly as the segment writer's own placement spells it.
fn doorbell_page_path(segment: &Path) -> PathBuf {
    let mut name = segment.as_os_str().to_owned();
    name.push(".doorbell");
    PathBuf::from(name)
}

/// Creates and formats one shard's segment, retains the read-only descriptor every attach is
/// served from, and records the file for cleanup.
///
/// The file is created exclusively under a name carrying this daemon instance's random
/// identity, so it can collide with nothing — not another instance's segment, and not a name
/// a client could have guessed and planted (`docs/notes/shared-memory-model.md` §4.2). It is
/// created before the shard task starts, because formatting a region is blocking I/O and
/// nothing on the update path may do any.
///
/// The read-only descriptor is opened here, once, and held for the daemon's lifetime. Every
/// later attach transfers this same descriptor rather than re-opening a path that anything
/// running as this user could have replaced in the meantime.
///
/// Its identity is anchored to the creation object, not to the name: what it is compared
/// against is the `fstat` [`SegmentRegion::create_file`] took of the descriptor the exclusive
/// create returned, so the check is "is this the object this daemon made" rather than two
/// resolutions of a mutable name compared to each other — which agree with each other exactly
/// as well when both resolve to a replacement. The open adds `O_NOFOLLOW` and `O_NONBLOCK`
/// ([`RETAINED_OPEN_FLAGS`]) so a symlink is refused outright and a FIFO cannot hold startup
/// in the `open` itself, and the object it did reach must be the created one, a regular file,
/// of exactly the region's length.
///
/// What remains outside the guarantee is the exclusive-create syscall itself: `create_new`
/// proves nothing existed at the name and that this daemon made what does, and everything
/// after it is anchored to that object — but the object is still *reached* the second time by
/// resolving the name, so a same-user party that swaps the file in that window makes this a
/// named startup refusal rather than an undetected substitution. Refusing is the outcome; the
/// swap itself is not preventable without a portable reopen-by-descriptor this daemon has.
fn open_segment(
    delivery: &pm_ws::daemon::DeliveryPlan,
    instance: u128,
    shard: usize,
    created: &mut SegmentFiles,
) -> Result<(ShardSegment, SegmentTarget), String> {
    let name = segment_file_name(instance, shard);
    let path = delivery.directory.join(name.as_str());
    let bytes = delivery.layout.region_size();
    let region = SegmentRegion::create_file(path.as_path(), bytes)
        .map_err(|error| format!("segment {}: {error}", path.display()))?;
    let identity = region.creation_identity().ok_or_else(|| {
        format!(
            "segment {} was created without a retained creation object",
            path.display()
        )
    })?;
    created.created.push(CreatedSegment {
        path: path.clone(),
        identity,
        page: None,
    });
    let file = retained_open_options()
        .open(path.as_path())
        .map_err(|error| format!("segment {} not reopened read-only: {error}", path.display()))?;
    let opened = file
        .metadata()
        .map_err(|error| format!("segment {} descriptor not stat'd: {error}", path.display()))?;
    names_the_created_object(path.as_path(), identity, &opened, bytes as u64)?;
    let writer = SegmentWriter::create(
        std::sync::Arc::new(region),
        SegmentConfig::new(delivery.layout, instance, SEGMENT_GENERATION),
    )
    .map_err(|error| format!("segment {}: {error:?}", path.display()))?;
    let page = writer.doorbell_feature_bit() == FEATURE_DOORBELL_PAGE;
    if page && let Some(last) = created.created.last_mut() {
        last.page = writer.doorbell_page_identity();
    }
    let target = SegmentTarget {
        name: name.clone(),
        file,
        doorbell: if page {
            DoorbellLocation::Page
        } else {
            DoorbellLocation::InHeader
        },
        attachments: 0,
    };
    Ok((ShardSegment::new(name, writer), target))
}

/// One shard's segment as the attachment path needs it: the descriptor every attach hands
/// out, and what an attaching consumer is promised about it.
///
/// The daemon keeps this beside the routing table rather than asking the shard, for the
/// reason the routing table itself exists: an attach is answered on the control task, and a
/// shard's answer would arrive after the descriptor had to be in hand. Nothing here changes
/// after startup — a shard installs at most one segment for its whole life — so a stale copy
/// is not a thing this can hold.
struct SegmentTarget {
    name: String,
    /// The segment opened read-only at creation, held for the daemon's lifetime. It is
    /// borrowed for each transfer and never consumed: `SCM_RIGHTS` duplicates it into the
    /// receiver, so serving an attach costs no `open` and no `dup`.
    file: File,
    doorbell: DoorbellLocation,
    /// How many consumers this daemon has transferred this segment's descriptor to.
    attachments: u64,
}

/// One control connection, from its accept to its close.
///
/// A session is the unit a lease belongs to. An operator's session lives for one command; a
/// consumer's lives for as long as it wants the markets it attached to, and can hold many
/// leases at once — a [`pm_ws::ControlRequest::Release`] on it drops one without ending the
/// session or its other leases. Closing it — deliberately, or by exiting, or by dying — is
/// what releases whatever it still holds. That is why the socket is the outer bound on every
/// lease's lifetime: the operating system reports a closed one whatever happened to the
/// process behind it, which no keepalive protocol can promise.
struct Session {
    id: SessionId,
    stream: UnixStream,
    /// Bytes read that are not yet a complete request line, and any complete one a
    /// dispatch's own budget did not reach. Nothing is read into this while a complete line
    /// is already in it, so it holds at most one partial request and the last read's
    /// remainder however much a peer pipelines.
    pending: Vec<u8>,
    /// When this session must have completed its first request by, cleared once it has. A
    /// client that connects and says nothing is dropped at it.
    opening_deadline: Option<Instant>,
    /// When this session last completed a request, which is what a configured lease TTL
    /// measures silence from.
    last_activity: Instant,
    /// How many bytes of an over-long request line are still being discarded, or `None` when
    /// this session is reading requests normally.
    ///
    /// A framing error belongs to the request that carried it, not to the connection: the
    /// refusal is answered, the rest of that line is thrown away, and the session goes on at
    /// the next newline. It matters more now than it did when every connection carried one
    /// command, because a session carries leases, and dropping a consumer's markets over one
    /// malformed line would be a far larger answer than the mistake.
    discarding: Option<usize>,
}

/// A process-local name for one control connection. Never external, never reused within a
/// run: it exists so a market can say which sessions hold it.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct SessionId(u64);

/// Every open control connection, and the identity the next one takes.
struct Sessions {
    open: Vec<Session>,
    next_id: u64,
    /// Where the next scan for a session with work starts, advanced past every session a
    /// dispatch touches. A position in the vector rather than a session's identity: what it
    /// has to promise is that no index is structurally favoured — the scan wraps, so every
    /// open session is reached within one rotation — and [`Sessions::close`] moves it with the
    /// sessions a removal shifts, so a close costs no session its turn.
    scan_start: usize,
    /// How many control connections may be open at once, from
    /// [`pm_ws::daemon::DaemonConfig::max_control_sessions`].
    ///
    /// A session is one open connection held for as long as its consumer wants its leases, so
    /// this is what bounds the daemon's control-side memory and the work one sweep does. A
    /// connection past it is answered with a typed refusal and closed rather than queued: a
    /// queue of connections nothing is serving is a wait with no bound on it.
    max_sessions: usize,
}

/// What one framing attempt found at the front of a session's buffer.
enum Framed {
    /// A complete request line, newline stripped.
    Line(String),
    /// Nothing complete yet, and room to keep reading.
    Incomplete,
    /// The line at the front is not one this daemon will read. It is answered with this, the
    /// rest of that line is discarded, and the session goes on at the next newline.
    Refused(String),
    /// The peer has sent more than one over-long line's worth of bytes without a newline
    /// anywhere in them. It is answered with this and the session is closed: there is no next
    /// request to go on to, and nothing else this daemon can tell it.
    Abandoned(String),
}

/// The most bytes of an unterminated request this daemon will throw away before it stops
/// looking for the end of it.
///
/// A refused line is discarded up to its newline so the session can resume, which is a peer's
/// own byte count to spend. This bounds it: a peer that sends this much more with no newline
/// in it is not framing requests at all, and the session ends rather than reading whatever it
/// sends forever.
const MAX_DISCARDED_BYTES: usize = MAX_CONTROL_LINE_BYTES;

impl Session {
    /// Takes the next complete request line out of this session's buffer.
    ///
    /// The cap counts the newline, so a line that carries one and still occupies more than
    /// the cap is over it, and a buffer that has reached the cap with no newline in it is a
    /// request that ran out of budget mid-line. Both are refused rather than truncated into
    /// something that happens to parse, and both leave the session reading for the newline
    /// that ends the offending line.
    fn next_request(&mut self) -> Framed {
        if let Some(discarded) = self.discarding {
            match self.pending.iter().position(|byte| *byte == b'\n') {
                Some(end) => {
                    let _discarded: Vec<u8> = self.pending.drain(..=end).collect();
                    self.discarding = None;
                }
                None => {
                    let discarded = discarded.saturating_add(self.pending.len());
                    self.pending.clear();
                    self.discarding = Some(discarded);
                    return if discarded > MAX_DISCARDED_BYTES {
                        Framed::Abandoned(format!(
                            "no request line in {discarded} bytes; the connection is not \
                             speaking this protocol"
                        ))
                    } else {
                        Framed::Incomplete
                    };
                }
            }
        }
        match self.pending.iter().position(|byte| *byte == b'\n') {
            Some(end) if end < MAX_CONTROL_LINE_BYTES => {
                let line: Vec<u8> = self.pending.drain(..=end).collect();
                match String::from_utf8(line) {
                    Ok(text) => Framed::Line(text.trim_end().to_owned()),
                    Err(_) => Framed::Refused("request line is not utf-8".to_owned()),
                }
            }
            Some(end) => {
                let _discarded: Vec<u8> = self.pending.drain(..=end).collect();
                Framed::Refused(format!(
                    "request line exceeds {MAX_CONTROL_LINE_BYTES} bytes"
                ))
            }
            None if self.pending.len() >= MAX_CONTROL_LINE_BYTES => {
                self.discarding = Some(self.pending.len());
                self.pending.clear();
                Framed::Refused(format!(
                    "request line exceeds {MAX_CONTROL_LINE_BYTES} bytes or carries no newline"
                ))
            }
            None => Framed::Incomplete,
        }
    }

    /// Whether this session's own buffer already decides something, with no further read.
    ///
    /// A newline is one such decision; so is a buffer that reached the line cap without one,
    /// which is refused rather than waited on. Either way [`Session::next_request`] answers a
    /// request or consumes bytes, so a dispatch taken on this readiness always makes
    /// progress — which is what lets a session left holding a budgeted-out backlog re-enter
    /// the rotation without new bytes arriving to wake it.
    fn has_buffered_request(&self) -> bool {
        self.pending.contains(&b'\n') || self.pending.len() >= MAX_CONTROL_LINE_BYTES
    }
}

impl Sessions {
    fn new(max_sessions: usize) -> Self {
        Self {
            open: Vec::new(),
            next_id: 0,
            scan_start: 0,
            max_sessions,
        }
    }

    /// Registers one accepted connection, or refuses it because too many are already open.
    ///
    /// The refusal is written and the connection dropped, so a client learns why rather than
    /// waiting on a daemon that silently never answers.
    async fn accept(&mut self, stream: UnixStream, router: &mut Router) {
        if self.open.len() >= self.max_sessions {
            let max_sessions = self.max_sessions;
            let refusal = ControlResponse::Busy {
                message: format!(
                    "this daemon holds {max_sessions} control sessions; nothing was applied"
                ),
            };
            let mut stream = stream;
            let _answered = answer(&mut stream, &refusal, router).await;
            return;
        }
        self.next_id = self.next_id.saturating_add(1);
        self.open.push(Session {
            id: SessionId(self.next_id),
            stream,
            pending: Vec::new(),
            opening_deadline: Some(Instant::now() + CONTROL_READ_TIMEOUT),
            last_activity: Instant::now(),
            discarding: None,
        });
    }

    /// Resolves when any open session has work to take, naming which.
    ///
    /// Work is bytes on the socket or a complete request already in the session's own buffer,
    /// which is what a per-dispatch budget leaves behind: a session whose backlog was not
    /// finished is ready by that fact alone and never waits for new bytes to be noticed
    /// again.
    ///
    /// The scan starts at [`Sessions::scan_start`] and wraps, so the session served last is
    /// the last one looked at next: no index is structurally favoured, and a session that is
    /// always ready cannot keep the scan from reaching a session that is closed, silent, or
    /// waiting on one operator command. Readiness rather than a stored read future: a
    /// session's buffer is plain bytes this task owns, so nothing has to be held across a
    /// wake, and a session that says nothing for hours costs one registered waker. Pending
    /// forever when no session is open, which is what the caller's `select!` wants of a
    /// branch with nothing to do.
    async fn next_readable(&self) -> usize {
        poll_fn(|context| {
            if self.open.is_empty() {
                return Poll::Pending;
            }
            let start = self.scan_start % self.open.len();
            for offset in 0..self.open.len() {
                let index = (start + offset) % self.open.len();
                let session = &self.open[index];
                if session.has_buffered_request()
                    || session.stream.poll_read_ready(context).is_ready()
                {
                    return Poll::Ready(index);
                }
            }
            Poll::Pending
        })
        .await
    }

    /// The soonest instant at which this daemon has session work to do on its own: an
    /// opening deadline, a lease TTL expiry, or a reconciliation to retry.
    ///
    /// A TTL whose deadline this platform's clock cannot represent contributes no deadline
    /// rather than a panic. `pm_ws::daemon::MAX_LEASE_TTL_MS` refuses such a value before a
    /// daemon runs under it, so this is the arithmetic saying the same thing.
    fn next_deadline(&self, ttl: Option<Duration>, router: &Router) -> Option<Instant> {
        let sessions = self.open.iter().filter_map(|session| match session {
            Session {
                opening_deadline: Some(deadline),
                ..
            } => Some(*deadline),
            Session { last_activity, .. } => ttl.and_then(|ttl| last_activity.checked_add(ttl)),
        });
        let retry = router
            .has_undesired()
            .then(|| Instant::now() + RECONCILE_RETRY);
        sessions.chain(retry).min()
    }

    /// Reads whatever the session at `index` has to say and answers up to
    /// [`CONTROL_REQUESTS_PER_DISPATCH`] of the complete requests in it, in order.
    ///
    /// Requests are served one at a time: one is answered before the next is taken out of any
    /// session's buffer, and no two are ever in flight together. One session's own requests
    /// keep the order it sent them. What a dispatch does not take is exclusivity — every other
    /// ready session gets at most [`CONTROL_REQUESTS_PER_DISPATCH`] answers before the scan
    /// wraps back, so with `n` sessions ready at once at most
    /// `CONTROL_REQUESTS_PER_DISPATCH * (n - 1)` answers separate two turns of any one of
    /// them. Sessions' requests therefore interleave in units of that budget, and a session
    /// pipelining thousands of them cannot hold the control task until its buffer runs out.
    /// A backlog left behind is answered on this session's next turn, because
    /// [`Sessions::next_readable`] counts a buffered complete request as readiness.
    ///
    /// The budget bounds a turn in answers, and each answer is separately bounded by
    /// [`CONTROL_WRITE_TIMEOUT`], so the worst one turn costs in time is the budget times that
    /// bound, and the worst a session waits for its next turn is that again for every other
    /// ready session — a peer reading just slowly enough that every write still completes.
    /// That product is the price of the write bound and the session cap
    /// ([`Sessions::max_sessions`], from
    /// [`pm_ws::daemon::DaemonConfig::max_control_sessions`]) together, and it is a fairness
    /// bound rather than an operator latency guarantee: a deployment that needs a bounded
    /// command latency bounds the sessions it opens. A peer that stops reading altogether
    /// costs one such bound and then loses its session.
    ///
    /// Nothing is read while a complete request is already buffered, which is what keeps a
    /// pipelining client's backlog in its own socket rather than in this daemon's memory.
    ///
    /// The work behind one request is a bounded `try_send` to a shard plus a reply, so a
    /// queued client waits on the daemon's control task and never on ingestion — shards are
    /// their own tasks and no control path touches theirs.
    ///
    /// The session is closed when its peer closes, when its framing gave up, or when an
    /// answer could not be written. That last one is why a client that stopped reading costs
    /// one [`CONTROL_WRITE_TIMEOUT`] and not one per request behind it: leaving it open would
    /// spend that bound again on every request it had already queued, and hold its leases
    /// while doing it.
    async fn serve(&mut self, index: usize, router: &mut Router, lease_ttl: Option<Duration>) {
        self.scan_start = index.saturating_add(1);
        let Some(session) = self.open.get_mut(index) else {
            return;
        };
        if !session.has_buffered_request() {
            let mut chunk = [0_u8; CONTROL_READ_CHUNK];
            let read = match session.stream.try_read(&mut chunk) {
                Ok(0) => None,
                Ok(read) => Some(read),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return,
                Err(_) => None,
            };
            let Some(read) = read else {
                self.close(index, router).await;
                return;
            };
            session.pending.extend_from_slice(&chunk[..read]);
        }
        let mut served = 0;
        while served < CONTROL_REQUESTS_PER_DISPATCH {
            let Some(session) = self.open.get_mut(index) else {
                return;
            };
            served += 1;
            match session.next_request() {
                Framed::Incomplete => return,
                Framed::Refused(message) => {
                    let refusal = ControlResponse::Error { message };
                    if answer(&mut session.stream, &refusal, router).await == Answered::Abandoned {
                        self.close(index, router).await;
                        return;
                    }
                }
                Framed::Abandoned(message) => {
                    let refusal = ControlResponse::Error { message };
                    let _answered = answer(&mut session.stream, &refusal, router).await;
                    self.close(index, router).await;
                    return;
                }
                Framed::Line(line) => {
                    session.opening_deadline = None;
                    session.last_activity = Instant::now();
                    let id = session.id;
                    let answered = match serde_json::from_str::<ControlRequest>(line.as_str()) {
                        Err(error) => {
                            let refusal = ControlResponse::Error {
                                message: format!("unreadable request: {error}"),
                            };
                            answer(&mut session.stream, &refusal, router).await
                        }
                        Ok(ControlRequest::Attach { market }) => {
                            attach(&mut session.stream, router, id, market.as_str(), lease_ttl)
                                .await
                        }
                        Ok(ControlRequest::Release { market }) => {
                            release(&mut session.stream, router, id, market.as_str()).await
                        }
                        Ok(ControlRequest::Renew) => {
                            let renewed = ControlResponse::Renewed {
                                leases: router.leases_held_by(id),
                            };
                            answer(&mut session.stream, &renewed, router).await
                        }
                        Ok(request) => {
                            let response = router.apply(request).await;
                            let Some(session) = self.open.get_mut(index) else {
                                return;
                            };
                            answer(&mut session.stream, &response, router).await
                        }
                    };
                    if answered == Answered::Abandoned {
                        self.close(index, router).await;
                        return;
                    }
                }
            }
        }
    }

    /// Drops the session at `index` and releases everything it held.
    ///
    /// Removing from the middle of the vector shifts every later session down one place, so
    /// the scan cursor moves with them: a cursor past the removed index names one session
    /// before the removal and a different one after it, and the session that inherited the
    /// index would be skipped for a rotation. Adjusting it keeps the cursor on the position it
    /// named — the session after the one that closed — whichever session that now is.
    async fn close(&mut self, index: usize, router: &mut Router) {
        if index >= self.open.len() {
            return;
        }
        let session = self.open.remove(index);
        if self.scan_start > index {
            self.scan_start = self.scan_start.saturating_sub(1);
        }
        router.release_session(session.id).await;
    }

    /// Drops every session whose time is up, and retries any reconciliation that is owed.
    ///
    /// Two deadlines, for two different failures. A session that has never completed a
    /// request is dropped at [`CONTROL_READ_TIMEOUT`]: it is a connection that has said
    /// nothing at all. A session that has completed one is dropped only under a configured
    /// lease TTL, because silence after that point is what a consumer reading its segment
    /// looks like, and unsubscribing a market underneath a live reader is the more expensive
    /// mistake. Expiry closes the stream as well as releasing the leases, so the peer learns
    /// its leases are gone rather than holding a socket that no longer means anything.
    ///
    /// A TTL whose deadline this platform's clock cannot represent expires nothing.
    /// `pm_ws::daemon::MAX_LEASE_TTL_MS` refuses such a value before a daemon can run under
    /// one, and this is the arithmetic that makes that a configuration error rather than a
    /// panic here.
    async fn sweep(&mut self, ttl: Option<Duration>, router: &mut Router) {
        let now = Instant::now();
        let expired: Vec<usize> = self
            .open
            .iter()
            .enumerate()
            .filter(|(_, session)| match session.opening_deadline {
                Some(deadline) => deadline <= now,
                None => ttl
                    .and_then(|ttl| session.last_activity.checked_add(ttl))
                    .is_some_and(|deadline| deadline <= now),
            })
            .map(|(index, _)| index)
            .collect();
        for index in expired.into_iter().rev() {
            self.close(index, router).await;
        }
        router.reconcile().await;
    }
}

/// Sleeps until `deadline`, or forever when there is none.
async fn until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// Whether one answer reached the client it was written to.
///
/// The session's fate, not a diagnostic: an answer this daemon could not write inside
/// [`CONTROL_WRITE_TIMEOUT`] is a peer that is not reading, and a peer that is not reading
/// has nothing more to be told.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Answered {
    Delivered,
    Abandoned,
}

/// Writes one answer line, bounded by [`CONTROL_WRITE_TIMEOUT`], counting a client that
/// stopped reading and telling the caller to drop it.
async fn answer(
    stream: &mut UnixStream,
    response: &ControlResponse,
    router: &mut Router,
) -> Answered {
    let encoded = encoded_line(response);
    let written = tokio::time::timeout(CONTROL_WRITE_TIMEOUT, async {
        stream.write_all(encoded.as_bytes()).await?;
        stream.flush().await
    })
    .await;
    if matches!(written, Ok(Ok(()))) {
        return Answered::Delivered;
    }
    router.answers_abandoned = router.answers_abandoned.saturating_add(1);
    Answered::Abandoned
}

/// One protocol value as the line that carries it, falling back to an error line for a value
/// that cannot be represented — which, for these types, cannot happen.
fn encoded_line(response: &ControlResponse) -> String {
    pm_ws::encode_line(response)
        .unwrap_or_else(|error| format!("{{\"result\":\"error\",\"message\":\"{error}\"}}\n"))
}

/// Serves one attach: peer credentials, then routing, then the segment's descriptor itself.
///
/// The credential check comes before everything else — before the market is looked up —
/// because it decides whether this peer may be told anything at all. A peer running as
/// another user is answered with a typed error, is never told whether the market exists, and
/// receives no descriptor. It is the check on top of the segment's own 0600 permissions and
/// the control socket's, never a replacement for either
/// (`docs/notes/shared-memory-model.md` §4.2).
///
/// **This path opens nothing.** The descriptor it transfers is the read-only one
/// [`open_segment`] retained when it created the file, borrowed for the send and duplicated
/// into the receiver by `SCM_RIGHTS`. Resolving a *name* while serving would be two separate
/// defects: an `open` of a path anything running as this user may have replaced is a blocking
/// syscall on the runtime that also drives ingestion — a planted FIFO holds it before any
/// timeout begins — and a replacement that still validates is a consumer permanently reading
/// a book nothing writes, which a quiet market gives no signal of.
///
/// **Only the segment's descriptor crosses**, whatever the doorbell placement. A sibling
/// doorbell page has to be mapped read-write — the platform's wait primitive will not park on
/// a read-only mapping — and a writable mapping carries a writable *length*: any holder of
/// that descriptor can truncate the page and the writer's next mirrored store takes `SIGBUS`.
/// The answer still names the placement, so a consumer on a [`DoorbellLocation::Page`] segment
/// knows it may spin or poll but not park; its first park is the typed
/// [`pm_ws::WaitFault::DoorbellUnavailable`] a reader whose page failed to open already gets.
/// Same-user consumers that open the page by name through [`pm_ws::SegmentReader::attach`]
/// park exactly as before. Lifting this needs a sealed or otherwise non-resizable shared
/// object (Linux `memfd` with `F_SEAL_SHRINK`; no macOS equivalent yet).
///
/// **The attach takes a lease.** `session` is the connection asking, and it holds the market
/// until it closes. A market nothing already holds is added to the desired set first, through
/// the same routing an operator's `add` goes through, and a shard that refuses it is the
/// answer the caller gets — with the lease released again, so a refusal leaves demand exactly
/// as it found it.
///
/// **The answer carries the deadline the lease lives under.** `lease_ttl` is this daemon's
/// configured TTL, and it goes out as [`Attachment::lease_ttl_ms`] because the consumer is
/// the side that has to meet it and the daemon is the only side that knows it. A consumer
/// left to guess renews on a cadence of its own, which is wrong for every TTL shorter than
/// that cadence: it would lose the market it is reading while its socket is still open and
/// its next renewal still pending.
///
/// The descriptor is transferred as soon as the shard holds the market, which is before the
/// venue has necessarily said anything about it. That is not a shortcut: a shard that has
/// accepted a market has already given it its directory entry and published its first state
/// into the slot that entry binds, so the consumer's read is coherent from its first byte and
/// reports the book as synchronizing until a venue base lands. Waiting here instead would put
/// the control task on a venue's clock, and every attach to a quiet market — which no timer
/// may call stale — would have to end in a refusal or a lie.
async fn attach(
    stream: &mut UnixStream,
    router: &mut Router,
    session: SessionId,
    market: &str,
    lease_ttl: Option<Duration>,
) -> Answered {
    let served = own_euid();
    let peer = match peer_euid(stream.as_fd()) {
        Ok(peer) => peer,
        Err(error) => {
            let refusal = ControlResponse::Error {
                message: format!("attach refused: peer credentials unreadable: {error}"),
            };
            router.refuse_attachment();
            return answer(stream, &refusal, router).await;
        }
    };
    if let Some(refusal) = foreign_peer_refusal(served, peer) {
        router.refuse_attachment();
        return answer(stream, &refusal, router).await;
    }
    if !is_market_slug(market) {
        let refusal = ControlResponse::Markets {
            markets: vec![MarketOutcome {
                slug: market.to_owned(),
                status: MarketStatus::Rejected(MarketRejection::InvalidIdentifier),
            }],
        };
        router.refuse_attachment();
        return answer(stream, &refusal, router).await;
    }
    if let Some(refusal) = router.lease(session, market).await {
        router.refuse_attachment();
        return answer(stream, &refusal, router).await;
    }
    let plan = match router.attach_plan(market) {
        Ok(plan) => plan,
        Err(status) => {
            let refusal = ControlResponse::Markets {
                markets: vec![MarketOutcome {
                    slug: market.to_owned(),
                    status,
                }],
            };
            router.release(session, market).await;
            router.refuse_attachment();
            return answer(stream, &refusal, router).await;
        }
    };
    let attachment = Attachment {
        shard: plan.shard,
        segment: plan.name,
        instance_id: router.instance_id(),
        segment_generation: SEGMENT_GENERATION,
        doorbell: plan.doorbell,
        descriptors: TRANSFERRED_DESCRIPTORS,
        lease_ttl_ms: declared_ttl_ms(lease_ttl),
    };
    let encoded = encoded_line(&ControlResponse::Attached { attachment });
    if router.segment_descriptor(plan.shard).is_none() {
        let refusal = ControlResponse::Error {
            message: format!("attach refused: shard {} holds no segment", plan.shard),
        };
        router.release(session, market).await;
        router.refuse_attachment();
        return answer(stream, &refusal, router).await;
    }
    let Some(descriptor) = router.segment_descriptor(plan.shard) else {
        return Answered::Abandoned;
    };
    let transferred = tokio::time::timeout(
        CONTROL_WRITE_TIMEOUT,
        transfer(stream, encoded.as_bytes(), &[descriptor]),
    )
    .await;
    if matches!(transferred, Ok(Ok(()))) {
        router.record_attachment(plan.shard);
        return Answered::Delivered;
    }
    router.answers_abandoned = router.answers_abandoned.saturating_add(1);
    Answered::Abandoned
}

/// Serves one release: `session`'s lease on `market` is given up, and the answer names how
/// many leases the session holds afterward.
///
/// `market` is validated exactly as [`attach`] validates it — an identifier no shard would
/// accept is refused as [`ControlResponse::Markets`] before anything is touched, never
/// applied as a release of nothing. [`Router::release`] is already idempotent and already
/// leaves another session's lease and an operator's pin alone, so a market this session never
/// leased, or leases twice over, answers the same way a real release does: the session's
/// current count, no error.
///
/// Never counted against [`Router::attachments_refused`]: that counter is what an attach
/// failed at, and this path has attached nothing.
async fn release(
    stream: &mut UnixStream,
    router: &mut Router,
    session: SessionId,
    market: &str,
) -> Answered {
    if !is_market_slug(market) {
        let refusal = ControlResponse::Markets {
            markets: vec![MarketOutcome {
                slug: market.to_owned(),
                status: MarketStatus::Rejected(MarketRejection::InvalidIdentifier),
            }],
        };
        return answer(stream, &refusal, router).await;
    }
    router.release(session, market).await;
    let released = ControlResponse::Released {
        leases: router.leases_held_by(session),
    };
    answer(stream, &released, router).await
}

/// The configured lease TTL as the attach answer states it: milliseconds, or `0` for a daemon
/// that expires no session.
///
/// `pm_ws::daemon::MAX_LEASE_TTL_MS` bounds the accepted range well inside what a `u64` of
/// milliseconds carries, so the saturating conversion describes an impossible configuration
/// rather than a rounding rule: a TTL that did not fit would be reported as the longest one
/// that does, which is the conservative direction — a consumer renewing for a longer deadline
/// than the daemon keeps loses its leases, and one renewing for a shorter deadline than the
/// daemon keeps only sends lines nobody needed.
fn declared_ttl_ms(lease_ttl: Option<Duration>) -> u64 {
    lease_ttl.map_or(0, |ttl| {
        u64::try_from(ttl.as_millis()).unwrap_or(pm_ws::daemon::MAX_LEASE_TTL_MS)
    })
}

/// The attachment policy: which peers this daemon transfers a descriptor to.
///
/// Same effective user as the daemon, and nothing else. It is deliberately the whole rule —
/// a deployment that wants a wider audience widens the segment's own permissions and says so,
/// rather than having this quietly trust a group it was never told about — and it is a check
/// on top of those permissions, never a replacement for them
/// (`docs/notes/shared-memory-model.md` §4.2).
///
/// The refusal names both users because the caller cannot see either: a consumer told only
/// "refused" has no way to discover that it is running as the wrong user, and neither is a
/// secret. It says nothing about the market, which is not this decision's to disclose.
fn foreign_peer_refusal(served: u32, peer: u32) -> Option<ControlResponse> {
    (served != peer).then(|| ControlResponse::Error {
        message: format!(
            "attach refused: this daemon serves uid {served} and the peer is uid {peer}"
        ),
    })
}

/// Sends `payload` and `descriptors` as one message, then whatever of the payload the kernel
/// did not take.
///
/// The descriptors ride the first message, so a consumer that reads the answer line has them:
/// there is no window in which the line has arrived and the transfer has not. A stream socket
/// may accept fewer bytes than were offered, which for a line this short means a socket buffer
/// with almost nothing left in it; the remainder goes out as an ordinary write, because the
/// descriptors are already across.
async fn transfer(
    stream: &mut UnixStream,
    payload: &[u8],
    descriptors: &[BorrowedFd<'_>],
) -> std::io::Result<()> {
    let sent = send_descriptors(stream, payload, descriptors).await?;
    if sent < payload.len() {
        stream.write_all(&payload[sent..]).await?;
    }
    stream.flush().await
}

/// The one `sendmsg` that carries the descriptors, retried while the socket is not writable.
///
/// `writable()` is what keeps this off the blocking path: the send itself is attempted only
/// when the runtime says the socket will take it, and a `WouldBlock` answer clears the
/// readiness and waits again rather than spinning. The retry is unbounded here and bounded by
/// [`CONTROL_WRITE_TIMEOUT`] at the caller, which is where every other control write is
/// bounded too.
async fn send_descriptors(
    socket: &UnixStream,
    payload: &[u8],
    descriptors: &[BorrowedFd<'_>],
) -> std::io::Result<usize> {
    loop {
        socket.writable().await?;
        let attempt = socket.try_io(Interest::WRITABLE, || {
            send_with_fds(socket.as_fd(), payload, descriptors)
        });
        match attempt {
            Ok(0) => return Err(std::io::Error::from(std::io::ErrorKind::WriteZero)),
            Ok(sent) => return Ok(sent),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(error) => return Err(error),
        }
    }
}

/// One market's demand: why this daemon is holding it.
///
/// The two sources are kept apart rather than added into one count. An operator's pin is not
/// a lease that happens to be held by an operator — nothing releases it but another operator
/// command — and a lease is named by the session holding it rather than counted, so a client
/// that attaches to the same market ten times holds it once and its close ends it. A market
/// with neither is one nothing wants.
#[derive(Default)]
struct Demand {
    pinned: bool,
    lessees: std::collections::HashSet<SessionId>,
}

impl Demand {
    /// Whether anything wants this market: the shard-visible desired bit.
    fn wanted(&self) -> bool {
        self.pinned || !self.lessees.is_empty()
    }
}

/// Which shard holds which market, how much room each has left, and who wants what.
///
/// The daemon owns this rather than asking the shards, because a routing decision must be
/// made before a command is sent and a shard's own answer arrives after. Every entry is
/// recorded from a shard's reply, so the table never claims a market a shard refused.
///
/// It is also where the two sources of demand `docs/design.md` "Subscription control" names
/// are combined: operator pins and reference-counted client leases. Shards below this see
/// only the aggregate — one desired set, reconciled exactly as it always was — so nothing in
/// a shard knows what a lease is, and only a change in the aggregate reaches the venue. A
/// consumer attaching to a market another consumer already holds costs no venue traffic at
/// all, which is the whole point of counting.
struct Router {
    handles: Vec<ShardHandle>,
    /// Each shard's segment, in shard order: what an attaching consumer is handed.
    segments: Vec<SegmentTarget>,
    assignments: HashMap<String, usize>,
    /// Why each market this daemon holds is held. A market leaves this the moment its
    /// removal is accepted, so it never grows past what the daemon carries.
    demand: HashMap<String, Demand>,
    counts: Vec<usize>,
    capacity: usize,
    /// This daemon instance's 128-bit identity, which every segment header declares and every
    /// attachment answer promises.
    instance: u128,
    /// Answers this daemon gave up writing because the client stopped reading, each of which
    /// also ended the session it was written to. Reported through `status`; nothing about it
    /// is printed, because the control task shares its thread with everything else this
    /// runtime drives.
    answers_abandoned: u64,
    /// Attach requests refused, for every reason one is refused.
    attachments_refused: u64,
    /// The address this daemon's metrics endpoint bound to, reported through `status` so an
    /// operator who configured port `0` can find the port the operating system chose.
    metrics_listen: Option<String>,
}

impl Router {
    fn new(capacity: usize, instance: u128, metrics_listen: Option<String>) -> Self {
        Self {
            handles: Vec::new(),
            segments: Vec::new(),
            assignments: HashMap::new(),
            demand: HashMap::new(),
            counts: Vec::new(),
            capacity,
            instance,
            answers_abandoned: 0,
            attachments_refused: 0,
            metrics_listen,
        }
    }

    /// Records one started shard, the markets it was configured with, and its segment.
    ///
    /// A configured market is pinned: the configuration document is an operator command that
    /// happens to arrive at startup, and a market it names must not leave when the last
    /// consumer of it does.
    fn install(&mut self, handle: ShardHandle, markets: &[String], segment: SegmentTarget) {
        let index = self.handles.len();
        self.handles.push(handle);
        self.segments.push(segment);
        self.counts.push(markets.len());
        for slug in markets {
            let _previous = self.assignments.insert(slug.clone(), index);
            self.demand.entry(slug.clone()).or_default().pinned = true;
        }
    }

    /// This instance's identity as the 32 hexadecimal digits an attachment answer carries.
    fn instance_id(&self) -> String {
        format!("{:032x}", self.instance)
    }

    fn refuse_attachment(&mut self) {
        self.attachments_refused = self.attachments_refused.saturating_add(1);
    }

    fn record_attachment(&mut self, shard: usize) {
        if let Some(segment) = self.segments.get_mut(shard) {
            segment.attachments = segment.attachments.saturating_add(1);
        }
    }

    /// The read-only descriptor shard `shard`'s segment is served from, borrowed from the
    /// one this daemon opened when it created the file.
    ///
    /// `None` only for a shard with no segment installed, which [`Router::attach_plan`] has
    /// already refused by the time an attach reaches this.
    fn segment_descriptor(&self, shard: usize) -> Option<BorrowedFd<'_>> {
        self.segments.get(shard).map(|segment| segment.file.as_fd())
    }

    /// Which segment carries `market`, and what an attaching consumer is promised about it.
    ///
    /// Answered from the routing table rather than from the shard, because an attach is served
    /// on this task and a shard's answer would arrive after the answer had to be written.
    /// The table is exact for this purpose: a market only enters it when a shard *accepted*
    /// it, and a shard accepts nothing its segment refused an entry for — a market with no
    /// directory entry is rejected as [`MarketRejection::DeliveryUnavailable`] at add time and
    /// never recorded here.
    ///
    /// Refuses in the vocabulary the rest of this protocol already uses:
    /// [`MarketRejection::InvalidIdentifier`] for an identifier no shard would take,
    /// [`MarketStatus::Removed`] for a market this daemon does not hold — the same answer a
    /// `remove` for it earns — and [`MarketRejection::DeliveryUnavailable`] for a shard
    /// publishing into no segment.
    fn attach_plan(&self, market: &str) -> Result<AttachPlan, MarketStatus> {
        if !is_market_slug(market) {
            return Err(MarketStatus::Rejected(MarketRejection::InvalidIdentifier));
        }
        let Some(shard) = self.assignments.get(market).copied() else {
            return Err(MarketStatus::Removed);
        };
        let Some(segment) = self.segments.get(shard) else {
            return Err(MarketStatus::Rejected(MarketRejection::DeliveryUnavailable));
        };
        Ok(AttachPlan {
            shard,
            name: segment.name.clone(),
            doorbell: segment.doorbell,
        })
    }

    /// Takes `session`'s lease on `market`, adding the market to the desired set when it is
    /// the first thing to want it.
    ///
    /// Idempotent per session: a session that already holds the market holds it once, and one
    /// release ends it, so a client cannot wedge demand open by leaking counts it cannot name.
    ///
    /// Answers `None` when the lease is held and the market is this daemon's to serve, and
    /// the refusal to send otherwise — a shard's own rejection, or a full control queue —
    /// with the lease released again, so a refused attach leaves demand exactly as it was.
    async fn lease(&mut self, session: SessionId, market: &str) -> Option<ControlResponse> {
        let held = self.assignments.contains_key(market);
        let _existing = self
            .demand
            .entry(market.to_owned())
            .or_default()
            .lessees
            .insert(session);
        if held {
            return None;
        }
        match self.add(vec![market.to_owned()], false).await {
            Ok(outcomes)
                if outcomes
                    .iter()
                    .all(|outcome| !matches!(outcome.status, MarketStatus::Rejected(_))) =>
            {
                None
            }
            Ok(outcomes) => {
                self.release(session, market).await;
                Some(ControlResponse::Markets { markets: outcomes })
            }
            Err(error) => {
                self.release(session, market).await;
                Some(control_failure(error))
            }
        }
    }

    /// Gives up `session`'s lease on `market` and reconciles what that leaves.
    async fn release(&mut self, session: SessionId, market: &str) {
        if let Some(demand) = self.demand.get_mut(market) {
            let _held = demand.lessees.remove(&session);
        }
        self.forget_unheld();
        self.reconcile().await;
    }

    /// Gives up every lease `session` held, which is what a closed control connection means.
    async fn release_session(&mut self, session: SessionId) {
        for demand in self.demand.values_mut() {
            let _held = demand.lessees.remove(&session);
        }
        self.forget_unheld();
        self.reconcile().await;
    }

    /// Drops every demand record for a market this daemon does not hold.
    ///
    /// A lease taken for an attach the shard then refused leaves a record of demand for a
    /// market that was never installed, and nothing else would ever clear it: the
    /// reconciliation walks what the daemon *holds*, and this market is not among it.
    fn forget_unheld(&mut self) {
        self.demand
            .retain(|slug, demand| demand.wanted() || self.assignments.contains_key(slug));
    }

    /// How many markets `session` holds a lease on.
    fn leases_held_by(&self, session: SessionId) -> u32 {
        let held = self
            .demand
            .values()
            .filter(|demand| demand.lessees.contains(&session))
            .count();
        u32::try_from(held).unwrap_or(u32::MAX)
    }

    /// Whether anything holds `market` in the desired set.
    fn wanted(&self, market: &str) -> bool {
        self.demand.get(market).is_some_and(Demand::wanted)
    }

    /// Every market this daemon holds that nothing wants any more.
    fn undesired(&self) -> Vec<String> {
        let mut undesired: Vec<String> = self
            .assignments
            .keys()
            .filter(|slug| !self.wanted(slug.as_str()))
            .cloned()
            .collect();
        undesired.sort_unstable();
        undesired
    }

    /// Whether a reconciliation is owed, which is what makes the control task wake for one.
    fn has_undesired(&self) -> bool {
        self.assignments
            .keys()
            .any(|slug| !self.wanted(slug.as_str()))
    }

    /// Brings the venue's subscriptions back in line with aggregate demand.
    ///
    /// Desired state rather than a transition: what it acts on is every market this daemon
    /// holds that nothing wants, whatever made that true and however many attempts ago. So a
    /// removal a shard could not take — a full control queue — is not lost, and a market is
    /// never left subscribed because one `try_send` was refused; the next reconciliation
    /// finds the same market unwanted and asks again.
    async fn reconcile(&mut self) {
        let undesired = self.undesired();
        if undesired.is_empty() {
            return;
        }
        let _outcomes = self.release_markets(undesired).await;
    }

    fn shards(&self) -> usize {
        self.handles.len()
    }

    fn assigned(&self) -> usize {
        self.assignments.len()
    }

    /// Answers one request whose whole reply is a line.
    ///
    /// [`ControlRequest::Attach`], [`ControlRequest::Release`], and [`ControlRequest::Renew`]
    /// are not among those: `Attach`'s reply carries file descriptors and is written by
    /// [`attach`] on the connection that asked, which is the only place they can go, and
    /// `Release` and `Renew` each name a session's own lease count, which only the session
    /// that asked can be answered with. Reaching any of the three here would mean `serve`
    /// routed it to the wrong half, so each is answered as the refusal it is rather than
    /// silently ignored.
    async fn apply(&mut self, request: ControlRequest) -> ControlResponse {
        let answer = match request {
            ControlRequest::Add { markets } => self.add(markets, true).await,
            ControlRequest::Remove { markets } => self.unpin(markets).await,
            ControlRequest::Status { after } => return self.status(after.as_deref()).await,
            ControlRequest::Attach { market } => {
                return ControlResponse::Error {
                    message: format!("attach for {market} is answered on its own connection"),
                };
            }
            ControlRequest::Release { market } => {
                return ControlResponse::Error {
                    message: format!("release of {market} is answered on the session that sent it"),
                };
            }
            ControlRequest::Renew => {
                return ControlResponse::Error {
                    message: "renew is answered on the session that sent it".to_owned(),
                };
            }
        };
        match answer {
            Ok(markets) => ControlResponse::Markets { markets },
            Err(error) => control_failure(error),
        }
    }

    /// Chooses one shard per *unique valid* market named in a batch.
    ///
    /// Planning is per unique slug because a batch may name the same market twice, and a
    /// batch that planned each mention separately would give two shards one market: the
    /// assignments a dispatch produces are recorded after it, so the second mention would
    /// not yet see the first's. It is per *valid* slug because provisional room must not be
    /// spent on a market no shard will take — an invalid identifier that consumed capacity
    /// would push a valid market out of a shard that had room for it.
    ///
    /// Answers a target per unique slug, `None` when nothing is a valid target: an unusable
    /// identifier, or a set with no room left.
    fn plan_add(&self, slugs: &[String]) -> BTreeMap<String, Option<usize>> {
        let mut planned = self.counts.clone();
        let mut targets: BTreeMap<String, Option<usize>> = BTreeMap::new();
        for slug in slugs {
            if targets.contains_key(slug) {
                continue;
            }
            if !is_market_slug(slug.as_str()) {
                let _existing = targets.insert(slug.clone(), None);
                continue;
            }
            let target = self.assignments.get(slug).copied().or_else(|| {
                let free = planned.iter().position(|held| *held < self.capacity);
                if let Some(index) = free {
                    planned[index] += 1;
                }
                free
            });
            let _existing = targets.insert(slug.clone(), target);
        }
        targets
    }

    /// Routes an add batch: an already-held market to the shard that holds it, and a new one
    /// to the first shard with room.
    ///
    /// A market no shard has room for is rejected here rather than sent anywhere, because
    /// shard count is fixed by configuration at startup and growing it at runtime would open
    /// a connection the configuration never authorized. An identifier no shard would accept
    /// is rejected the same way, by the same predicate the shard uses.
    ///
    /// `pinned` says whether this add is an operator's, which sets the pin, or a lease's,
    /// which sets nothing — the lease itself is already recorded by then.
    ///
    /// Every named market is dispatched, including one the daemon already holds. That is not
    /// a venue transition: a shard answers an add for a market already in its desired set by
    /// reporting what that market is now, without touching its book, its evidence or its
    /// authority, and without a byte reaching the venue. It is what makes a repeated add
    /// answer `live` instead of a synthesized acceptance the daemon would have had to guess.
    /// The transition rule holds where it matters, on the way out: a `remove` is dispatched
    /// only for a market nothing wants any more.
    async fn add(
        &mut self,
        slugs: Vec<String>,
        pinned: bool,
    ) -> Result<Vec<MarketOutcome>, ShardControlError> {
        let targets = self.plan_add(&slugs);
        let answered = self.dispatch(&targets, true).await?;
        for (slug, outcome) in &answered {
            if matches!(outcome.status, MarketStatus::Rejected(_)) {
                continue;
            }
            if pinned {
                self.demand.entry(slug.clone()).or_default().pinned = true;
            }
            if outcome.status != MarketStatus::Accepted {
                continue;
            }
            let Some(Some(index)) = targets.get(slug.as_str()).copied() else {
                continue;
            };
            let _previous = self.assignments.insert(slug.clone(), index);
            self.counts[index] = self.counts[index].saturating_add(1);
        }
        Ok(reassemble(&slugs, &answered))
    }

    /// Clears the operator's pin on each named market and answers what that left.
    ///
    /// An operator's `remove` removes the operator's own ownership, which is what
    /// `docs/design.md` "Operator command families" asks of it, and never a consumer's. A
    /// market consumer sessions still lease therefore stays, and its answer is the state the
    /// shard reports it in — `live`, `reconciling`, whatever it is — rather than `removed`,
    /// which would tell the operator a market had gone while consumers went on reading it.
    /// Only the markets nothing wants any more are dispatched as removals.
    async fn unpin(&mut self, slugs: Vec<String>) -> Result<Vec<MarketOutcome>, ShardControlError> {
        for slug in &slugs {
            if let Some(demand) = self.demand.get_mut(slug.as_str()) {
                demand.pinned = false;
            }
        }
        let (retained, releasing): (Vec<String>, Vec<String>) = slugs
            .iter()
            .cloned()
            .partition(|slug| self.wanted(slug.as_str()));
        let mut answered = self.report(retained.as_slice()).await?;
        answered.extend(self.release_markets(releasing).await?);
        Ok(reassemble(&slugs, &answered))
    }

    /// What each named market's shard says about it right now.
    ///
    /// A read: the shard is asked for its status, never for a change. An add would have
    /// answered the same question, but an add is not a pure read for every market — one whose
    /// removal the venue has not reconciled yet would be closed and reinstalled by it — and
    /// answering a question must never rebuild a book.
    async fn report(
        &self,
        slugs: &[String],
    ) -> Result<BTreeMap<String, MarketOutcome>, ShardControlError> {
        let shards: std::collections::BTreeSet<usize> = slugs
            .iter()
            .filter_map(|slug| self.assignments.get(slug.as_str()).copied())
            .collect();
        let mut answered = BTreeMap::new();
        for shard in shards {
            let status = self.handles[shard].status().await?;
            for report in status.markets {
                if !slugs.contains(&report.slug) {
                    continue;
                }
                let _existing = answered.insert(
                    report.slug.clone(),
                    MarketOutcome {
                        slug: report.slug,
                        status: report.status,
                    },
                );
            }
        }
        Ok(answered)
    }

    /// Routes a remove batch to the shard holding each market, and an unheld market to the
    /// first shard, which validates the identifier and answers for it.
    ///
    /// The one place a removal reaches a shard. Every caller has already established that
    /// nothing wants the market: an operator's `remove` of the last thing holding it, the
    /// last lease going with its session, or a reconciliation retrying either.
    async fn release_markets(
        &mut self,
        slugs: Vec<String>,
    ) -> Result<BTreeMap<String, MarketOutcome>, ShardControlError> {
        if slugs.is_empty() {
            return Ok(BTreeMap::new());
        }
        let mut targets: BTreeMap<String, Option<usize>> = BTreeMap::new();
        for slug in &slugs {
            let target = self.assignments.get(slug).copied().or(Some(0));
            let _existing = targets.insert(slug.clone(), target);
        }
        let answered = self.dispatch(&targets, false).await?;
        for (slug, outcome) in &answered {
            if outcome.status != MarketStatus::Removed {
                continue;
            }
            if let Some(index) = self.assignments.remove(slug.as_str()) {
                self.counts[index] = self.counts[index].saturating_sub(1);
            }
            if !self.wanted(slug.as_str()) {
                let _forgotten = self.demand.remove(slug.as_str());
            }
        }
        Ok(answered)
    }

    /// Sends one batch per shard and answers per unique slug.
    ///
    /// A slug with no target shard is answered here as a capacity refusal — no shard had
    /// room, or the identifier was one no shard accepts, and both are answers this daemon
    /// owns. Every other slug is answered by the shard that owns the decision, which keeps
    /// identifier validation and desired-state semantics in one place.
    async fn dispatch(
        &self,
        targets: &BTreeMap<String, Option<usize>>,
        adding: bool,
    ) -> Result<BTreeMap<String, MarketOutcome>, ShardControlError> {
        let mut batches: Vec<Vec<String>> = vec![Vec::new(); self.handles.len()];
        let mut answered: BTreeMap<String, MarketOutcome> = BTreeMap::new();
        for (slug, target) in targets {
            match target {
                Some(index) => batches[*index].push(slug.clone()),
                None => {
                    let status = if is_market_slug(slug.as_str()) {
                        MarketStatus::Rejected(MarketRejection::CapacityExceeded)
                    } else {
                        MarketStatus::Rejected(MarketRejection::InvalidIdentifier)
                    };
                    let _existing = answered.insert(
                        slug.clone(),
                        MarketOutcome {
                            slug: slug.clone(),
                            status,
                        },
                    );
                }
            }
        }
        for (index, batch) in batches.into_iter().enumerate() {
            if batch.is_empty() {
                continue;
            }
            let handle = &self.handles[index];
            let outcomes = if adding {
                handle.add(batch).await?
            } else {
                handle.remove(batch).await?
            };
            for outcome in outcomes {
                let _existing = answered.insert(outcome.slug.clone(), outcome);
            }
        }
        Ok(answered)
    }

    /// Answers one page of the daemon's state, starting after the slug the caller last saw.
    ///
    /// Every shard's rows are merged into one slug-ordered sequence, so the cursor names a
    /// position in that sequence rather than in any one shard. A market added or removed
    /// between two pages therefore changes only whether it is itself reported: every market
    /// present throughout the walk still appears exactly once, which an offset cursor cannot
    /// promise, because a removal shifts every later row past the offset the caller is about
    /// to ask for.
    ///
    /// Shard summaries ride the first page — the one with no cursor. Every shard is asked
    /// once per page, which is what keeps a page's rows consistent with the summaries above
    /// them.
    async fn status(&self, after: Option<&str>) -> ControlResponse {
        let mut summaries = Vec::with_capacity(self.handles.len());
        let mut rows: Vec<MarketRow> = Vec::new();
        for (shard, handle) in self.handles.iter().enumerate() {
            match handle.status().await {
                Ok(status) => {
                    let redundant = status.replicas > 1;
                    summaries.push(ShardReport {
                        shard,
                        connected: status.subscribed,
                        reconciling: status.reconciling,
                        replicas: redundant.then_some(status.replicas),
                        standbys_established: redundant.then(|| {
                            status
                                .connections
                                .iter()
                                .filter(|row| {
                                    row.role == ReplicaRole::HotStandby && row.established
                                })
                                .count()
                        }),
                        standby_agreeing_markets: redundant
                            .then_some(status.standby_agreeing_markets),
                        pool: status.pool.map(|pool| pool.state),
                        desired: status.desired,
                        segment: status.segment,
                        segment_markets: status.segment_markets,
                        attachments: self
                            .segments
                            .get(shard)
                            .map_or(0, |segment| segment.attachments),
                        queue_age: status.queue_age,
                        publish_latency: status.publish_latency,
                    });
                    rows.extend(status.markets.into_iter().map(|market| {
                        let demand = self.demand.get(market.slug.as_str());
                        MarketRow {
                            shard,
                            pinned: demand.is_some_and(|demand| demand.pinned),
                            leases: demand.map_or(0, |demand| {
                                u32::try_from(demand.lessees.len()).unwrap_or(u32::MAX)
                            }),
                            market,
                        }
                    }));
                }
                Err(error) => return control_failure(error),
            }
        }
        rows.sort_by(|left, right| left.market.slug.cmp(&right.market.slug));
        let mut remaining = rows
            .into_iter()
            .skip_while(|row| after.is_some_and(|cursor| row.market.slug.as_str() <= cursor));
        let markets: Vec<MarketRow> = remaining.by_ref().take(STATUS_PAGE_MARKETS).collect();
        let more = markets.len() == STATUS_PAGE_MARKETS && remaining.next().is_some();
        ControlResponse::Status {
            status: DaemonStatus {
                pid: std::process::id(),
                more,
                shards: if after.is_none() {
                    summaries
                } else {
                    Vec::new()
                },
                markets,
                answers_abandoned: self.answers_abandoned,
                attachments_refused: self.attachments_refused,
                metrics_listen: self.metrics_listen.clone(),
            },
        }
    }

    /// Collects everything one metrics scrape reports, asking each shard exactly once.
    ///
    /// One bounded control command per shard, the same class `status` sends, and no
    /// per-market row: a collection costs the shard count and never the market count. A
    /// shard that cannot take the command fails the whole collection rather than leaving one
    /// shard silently missing from a document that looks complete — a scraper reads a failed
    /// collection as such, and a quietly short one as truth.
    ///
    /// The whole collection is bounded by [`METRICS_COLLECTION_TIMEOUT`], shared across the
    /// shards rather than granted to each, because this runs on the control loop: a shard
    /// that accepted the command and then stopped servicing its queue would otherwise
    /// suspend every control session, every lease sweep, and this daemon's own shutdown for
    /// as long as it stayed silent. Running out of time fails the collection naming the
    /// shard that did not answer, exactly as a shard that could not take the command does,
    /// and the scrape is refused rather than answered short.
    ///
    /// An abandoned command stays in that shard's bounded control queue, so a shard that is
    /// wedged and scraped indefinitely fills that queue and then answers `busy` to operator
    /// commands, as any other full control queue does.
    async fn metrics(&self) -> Result<MetricsSnapshot, String> {
        let deadline = Instant::now() + METRICS_COLLECTION_TIMEOUT;
        let mut shards = Vec::with_capacity(self.handles.len());
        for (shard, handle) in self.handles.iter().enumerate() {
            let answered = tokio::time::timeout_at(deadline, handle.metrics())
                .await
                .map_err(|_| {
                    format!(
                        "shard {shard} did not answer the collection in time; nothing was collected"
                    )
                })?;
            let metrics = answered.map_err(|error| match error {
                ShardControlError::Busy => {
                    format!("shard {shard}'s control queue is full; nothing was collected")
                }
                ShardControlError::Stopped => format!("shard {shard} is no longer running"),
            })?;
            shards.push(ShardSnapshot {
                shard,
                attachments: self
                    .segments
                    .get(shard)
                    .map_or(0, |segment| segment.attachments),
                metrics,
            });
        }
        Ok(MetricsSnapshot {
            pid: std::process::id(),
            answers_abandoned: self.answers_abandoned,
            attachments_refused: self.attachments_refused,
            markets: shards
                .iter()
                .map(|shard| widen(shard.metrics.markets))
                .sum(),
            pinned: widen(self.demand.values().filter(|demand| demand.pinned).count()),
            leases: self
                .demand
                .values()
                .map(|demand| widen(demand.lessees.len()))
                .sum(),
            shards,
        })
    }
}

/// One resolved attach: which shard's segment carries the market, and what its answer line
/// promises about it.
///
/// Owned rather than borrowed from the router, because serving the attach counts against the
/// router as it goes. It carries no path: the descriptor comes from
/// [`Router::segment_descriptor`], and nothing on this path resolves a name.
struct AttachPlan {
    shard: usize,
    name: String,
    doorbell: DoorbellLocation,
}

/// Puts one answer per requested slug back in request order, repeating the answer a slug
/// that was named twice earned once.
fn reassemble(slugs: &[String], answered: &BTreeMap<String, MarketOutcome>) -> Vec<MarketOutcome> {
    slugs
        .iter()
        .map(|slug| {
            answered.get(slug).cloned().unwrap_or(MarketOutcome {
                slug: slug.clone(),
                status: MarketStatus::Rejected(MarketRejection::CapacityExceeded),
            })
        })
        .collect()
}

fn control_failure(error: ShardControlError) -> ControlResponse {
    match error {
        ShardControlError::Busy => ControlResponse::Busy {
            message: "a shard's control queue is full; nothing was applied".to_owned(),
        },
        ShardControlError::Stopped => ControlResponse::Error {
            message: "a shard is no longer running".to_owned(),
        },
    }
}

/// One scrape waiting for the daemon's own numbers.
struct MetricsScrape {
    reply: oneshot::Sender<Result<MetricsSnapshot, String>>,
}

/// Everything one scrape reports, collected in a single pass over the shards.
///
/// Data rather than text: the control loop collects this and hands it back, and the
/// connection that asked renders the document, so no formatting happens on the thread that
/// also answers control sessions.
struct MetricsSnapshot {
    pid: u32,
    answers_abandoned: u64,
    attachments_refused: u64,
    /// Every market a shard holds a book for, including one whose removal is not reconciled.
    markets: u64,
    /// Markets an operator command holds in the desired set.
    pinned: u64,
    /// Market leases held by control sessions, counted once per session per market.
    leases: u64,
    shards: Vec<ShardSnapshot>,
}

/// One shard's contribution to a scrape: its own numbers, and what the daemon knows about
/// its segment.
struct ShardSnapshot {
    shard: usize,
    /// Consumers handed this shard's segment descriptor over the run.
    attachments: u64,
    metrics: ShardMetrics,
}

/// How one per-shard family reads its value out of a shard's snapshot.
type ShardSample = fn(&ShardSnapshot) -> u64;

/// The per-shard families carrying one value each, with the help text they are read by.
///
/// A table rather than a run of `writeln!`s because the exposition format requires each
/// family's `# HELP` and `# TYPE` to appear once, before its first sample: rendering shard
/// by shard would repeat them and produce a document a scraper refuses.
const SHARD_FAMILIES: &[(&str, &str, &str, ShardSample)] = &[
    (
        "pmws_shard_frames_seen",
        "counter",
        "Venue frames this shard has read from its connections.",
        |shard| shard.metrics.stats.frames_seen,
    ),
    (
        "pmws_shard_snapshots_applied",
        "counter",
        "Venue snapshots applied to a book this shard owns.",
        |shard| shard.metrics.stats.snapshots_applied,
    ),
    (
        "pmws_shard_mutations_derived",
        "counter",
        "Level mutations derived from applied venue state.",
        |shard| shard.metrics.stats.mutations_derived,
    ),
    (
        "pmws_shard_resolutions_forwarded",
        "counter",
        "Venue-reported resolutions forwarded onto consumer lanes.",
        |shard| shard.metrics.stats.resolutions_forwarded,
    ),
    (
        "pmws_shard_overload_drops",
        "counter",
        "Events dropped because a bounded queue was full.",
        |shard| shard.metrics.stats.overload_drops,
    ),
    (
        "pmws_shard_continuity_losses",
        "counter",
        "Continuity losses reported against a book this shard owns.",
        |shard| shard.metrics.stats.continuity_losses,
    ),
    (
        "pmws_shard_connection_attempts",
        "counter",
        "Venue connections this shard has dialled.",
        |shard| shard.metrics.stats.connection_attempts,
    ),
    (
        "pmws_shard_markets_dropped",
        "counter",
        "Books dropped because the connection carrying them stopped carrying them.",
        |shard| shard.metrics.stats.markets_dropped,
    ),
    (
        "pmws_shard_segment_attachments",
        "counter",
        "Consumers handed this shard's segment descriptor over the run.",
        |shard| shard.attachments,
    ),
    (
        "pmws_shard_queue_age_samples",
        "counter",
        "Ingest queue-age samples behind this shard's queue-age figures.",
        |shard| shard.metrics.stats.queue_age.samples,
    ),
    (
        "pmws_shard_queue_age_last_micros",
        "gauge",
        "Age of the event this shard dequeued most recently, in microseconds.",
        |shard| shard.metrics.stats.queue_age.last_micros,
    ),
    (
        "pmws_shard_queue_age_max_micros",
        "gauge",
        "Deepest ingest queue age this shard has sampled, in microseconds.",
        |shard| shard.metrics.stats.queue_age.max_micros,
    ),
    (
        "pmws_shard_queue_age_p50_micros",
        "gauge",
        "Median sampled ingest queue age, in microseconds, as a bucket upper bound.",
        |shard| shard.metrics.stats.queue_age.p50_micros,
    ),
    (
        "pmws_shard_queue_age_p99_micros",
        "gauge",
        "99th-percentile sampled ingest queue age, in microseconds, as a bucket upper bound.",
        |shard| shard.metrics.stats.queue_age.p99_micros,
    ),
    (
        "pmws_shard_publish_latency_samples",
        "counter",
        "State publications measured from socket arrival to publication complete; a \
         publication no venue frame drove is handed no arrival and is not one.",
        |shard| shard.metrics.stats.publish_latency.samples,
    ),
    (
        "pmws_shard_publish_latency_last_micros",
        "gauge",
        "Most recent socket-arrival to publication-complete interval, in microseconds, on \
         this daemon's monotonic clock at both ends.",
        |shard| shard.metrics.stats.publish_latency.last_micros,
    ),
    (
        "pmws_shard_publish_latency_max_micros",
        "gauge",
        "Longest socket-arrival to publication-complete interval observed, in microseconds.",
        |shard| shard.metrics.stats.publish_latency.max_micros,
    ),
    (
        "pmws_shard_publish_latency_p50_micros",
        "gauge",
        "Median socket-arrival to publication-complete interval, in microseconds, as a \
         bucket upper bound.",
        |shard| shard.metrics.stats.publish_latency.p50_micros,
    ),
    (
        "pmws_shard_publish_latency_p99_micros",
        "gauge",
        "99th-percentile socket-arrival to publication-complete interval, in microseconds, \
         as a bucket upper bound.",
        |shard| shard.metrics.stats.publish_latency.p99_micros,
    ),
    (
        "pmws_shard_publish_latency_p999_micros",
        "gauge",
        "99.9th-percentile socket-arrival to publication-complete interval, in microseconds, \
         as a bucket upper bound.",
        |shard| shard.metrics.stats.publish_latency.p999_micros,
    ),
    (
        "pmws_shard_queue_depth_max",
        "gauge",
        "Deepest this shard's bounded ingest queue has been observed while draining it.",
        |shard| widen(shard.metrics.stats.queue_depth_max),
    ),
    (
        "pmws_shard_connected",
        "gauge",
        "1 when a connection is established with this shard's subscription on the wire.",
        |shard| u64::from(shard.metrics.connected),
    ),
    (
        "pmws_shard_reconciling",
        "gauge",
        "1 when a whole-set reissue is in flight for this shard.",
        |shard| u64::from(shard.metrics.reconciling),
    ),
    (
        "pmws_shard_desired_markets",
        "gauge",
        "Markets in this shard's desired set.",
        |shard| widen(shard.metrics.desired),
    ),
    (
        "pmws_shard_markets",
        "gauge",
        "Markets this shard holds a book for, including ones whose removal is unreconciled.",
        |shard| widen(shard.metrics.markets),
    ),
    (
        "pmws_shard_segment_markets",
        "gauge",
        "Markets holding a live directory entry in this shard's segment.",
        |shard| widen(shard.metrics.segment_markets),
    ),
];

/// The per-shard families describing a shard's connection redundancy, rendered only by a
/// daemon that runs some.
///
/// Held apart from [`SHARD_FAMILIES`] rather than flagged inside it because the gate is on
/// the family and not on the sample: a `# HELP`/`# TYPE` pair with no series under it is
/// still a family on the scrape surface, and a daemon at the default `replicas = 1` exposes
/// the surface it exposed before redundancy was configurable. A deployment that turns
/// redundancy on gets them for every shard, since one daemon runs one replica count.
const REDUNDANCY_FAMILIES: &[(&str, &str, &str, ShardSample)] = &[
    (
        "pmws_shard_replicas",
        "gauge",
        "Venue connection roles this shard runs, publishing role included.",
        |shard| widen(shard.metrics.replicas),
    ),
    (
        "pmws_shard_standbys_established",
        "gauge",
        "Hot standby connections of this shard holding an established subscription.",
        |shard| widen(shard.metrics.standbys_established),
    ),
    (
        "pmws_shard_standby_agreeing_markets",
        "gauge",
        "Desired markets whose shadow on the standby best placed to take over agrees with \
         the published book, which is the promotion coverage a loss of the publishing \
         connection would find.",
        |shard| widen(shard.metrics.standby_agreeing_markets),
    ),
    (
        "pmws_shard_pool_armed",
        "gauge",
        "1 while this shard publishes across its connections through the venue-key gate, \
         0 while it publishes from one primary with hot standbys.",
        |shard| {
            u64::from(
                shard
                    .metrics
                    .pool
                    .as_ref()
                    .is_some_and(|pool| matches!(pool.state, PoolState::Armed)),
            )
        },
    ),
    (
        "pmws_shard_pool_covering",
        "gauge",
        "Connections of this shard holding an established subscription, which is the \
         coverage an arrival could reach the publish gate from.",
        |shard| {
            shard
                .metrics
                .pool
                .as_ref()
                .map_or(0, |pool| widen(pool.covering))
        },
    ),
    (
        "pmws_shard_pool_published",
        "counter",
        "Arrivals this shard's per-market publish gates applied to a published book, \
         whichever connection carried them.",
        |shard| shard.metrics.stats.pool_published,
    ),
    (
        "pmws_shard_pool_duplicate_drops",
        "counter",
        "Arrivals dropped because another connection had already published that same \
         frame, which is what a pool exists to absorb.",
        |shard| shard.metrics.stats.pool_duplicate_drops,
    ),
    (
        "pmws_shard_pool_stale_drops",
        "counter",
        "Arrivals dropped because the book already held newer state: cross-connection \
         skew, which is not a duplicate.",
        |shard| shard.metrics.stats.pool_stale_drops,
    ),
    (
        "pmws_shard_pool_handovers",
        "counter",
        "Times a surviving connection was moved into the publishing slot because the one \
         holding it went away while the pool was armed. Never a promotion.",
        |shard| shard.metrics.stats.pool_handovers,
    ),
];

/// The stable label one pool-degrade reason is exposed under.
///
/// Written here rather than derived from the type's `Debug`, because a scrape label is a
/// wire identity a dashboard keys on and a derived name would move with a rename.
const fn pool_degrade_label(reason: PoolDegradeReason) -> &'static str {
    match reason {
        PoolDegradeReason::ConnectionInversion => "connection_inversion",
        PoolDegradeReason::ReconnectRewind => "reconnect_rewind",
        PoolDegradeReason::EqualKeyContentMismatch => "equal_key_content_mismatch",
        PoolDegradeReason::KeyUnavailable => "key_unavailable",
    }
}

/// A count as the exposition format carries it, saturating rather than wrapping.
fn widen(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// Binds the metrics endpoint and answers the address it actually took.
///
/// The resolved address rather than the configured one, which is what makes a configuration
/// naming port `0` usable: the operating system chose the port and this is where it becomes
/// knowable. A bind that fails ends startup, because a metrics address an operator wrote and
/// a daemon silently did not serve looks exactly like a healthy daemon nothing is scraping.
async fn bind_metrics(address: SocketAddr) -> Result<(TcpListener, SocketAddr), String> {
    let listener = TcpListener::bind(address)
        .await
        .map_err(|error| format!("metrics_listen {address}: {error}"))?;
    let bound = listener.local_addr().map_err(|error| {
        format!("metrics_listen {address}: bound, and not readable back: {error}")
    })?;
    Ok((listener, bound))
}

/// Serves `GET /metrics` for the life of the run.
///
/// The contract, in full:
///
/// * **Nothing on a book's path runs here.** A scrape costs one bounded control command per
///   shard — the class `status` already uses, answered from the shard's own task between
///   frames — and touches no book, segment, or observer. It is the "metrics export outside
///   the market-data hot path" `docs/design.md` licenses, and its cost is the shard count,
///   never the market count.
/// * **No scraper's socket reaches the control loop.** Accepting, reading a request, and
///   writing an answer happen here and in the per-connection tasks this spawns. The control
///   loop is entered once per scrape, to collect numbers, and never waits on a scraper. A
///   scrape is not a control session: it is not accepted against
///   [`pm_ws::daemon::DaemonConfig::max_control_sessions`] and not counted against
///   [`CONTROL_REQUESTS_PER_DISPATCH`].
/// * **Every read and write is bounded.** A request head is capped at
///   [`METRICS_HEAD_LIMIT`] bytes, and each of the read, the collection, and the write is
///   bounded by [`METRICS_IO_TIMEOUT`], so a scraper that connects and says nothing, or that
///   stops reading its answer, costs one timed-out connection and nothing else.
/// * **Concurrency is capped** at [`MAX_METRICS_CONNECTIONS`]; a connection past it is
///   closed unread rather than queued. One collection is in flight at a time — a single
///   permit held from the request through the answer, which the request channel's own
///   capacity does not give, since it frees its slot when the control loop dequeues the
///   request rather than when the collection it asks for has run — and a scrape arriving
///   during another is answered `503` rather than made to wait.
/// * **Resident memory is Linux-only.** It is read from `/proc/self/status` once per scrape,
///   on the blocking pool rather than on the runtime thread: a kernel-generated file is
///   still a file, and a filesystem call that stalls must cost the scrape and never the
///   ingestion the one runtime thread also drives. macOS offers no dependency-free
///   equivalent, so the metric is absent there rather than guessed, and `pmwsctl status`
///   pairs RSS with the reported pid from outside the daemon.
///
/// The acceptor keeps its listener for the life of the run. A peer's own aborted connection
/// is retried at once; any other accept failure — the process's or the host's descriptors,
/// or buffer exhaustion — waits [`METRICS_ACCEPT_BACKOFF`] and accepts again, so it neither
/// spins on the thread every shard also runs on nor leaves a bound address `pmwsctl status`
/// still reports and nothing answers on.
async fn serve_metrics(listener: TcpListener, scrapes: mpsc::Sender<MetricsScrape>) {
    let permits = Arc::new(Semaphore::new(MAX_METRICS_CONNECTIONS));
    let inflight = Arc::new(Semaphore::new(1));
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _peer)) => stream,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionAborted | std::io::ErrorKind::Interrupted
                ) =>
            {
                continue;
            }
            Err(_) => {
                tokio::time::sleep(METRICS_ACCEPT_BACKOFF).await;
                continue;
            }
        };
        let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
            continue;
        };
        let scrapes = scrapes.clone();
        let inflight = Arc::clone(&inflight);
        let _serving = tokio::spawn(async move {
            let _held = permit;
            serve_scrape(stream, scrapes, inflight).await;
        });
    }
}

/// One metrics connection, from its request head to its closed socket.
///
/// One request, one answer, one close: the answer declares `Connection: close` and the
/// socket is shut down after it, so nothing here holds a connection open for a second
/// request.
async fn serve_scrape(
    mut stream: TcpStream,
    scrapes: mpsc::Sender<MetricsScrape>,
    inflight: Arc<Semaphore>,
) {
    let head = tokio::time::timeout(METRICS_IO_TIMEOUT, read_request_head(&mut stream))
        .await
        .ok()
        .flatten();
    let answer = match head {
        Some(head) if asks_for_metrics(head.as_str()) => scrape_answer(&scrapes, &inflight).await,
        Some(_) => http_answer(
            "404 Not Found",
            PLAIN_CONTENT_TYPE,
            "this daemon serves GET /metrics and nothing else\n",
        ),
        None => http_answer(
            "400 Bad Request",
            PLAIN_CONTENT_TYPE,
            "a request head, within 8192 bytes and the read deadline, is required\n",
        ),
    };
    let written = tokio::time::timeout(METRICS_IO_TIMEOUT, stream.write_all(answer.as_bytes()))
        .await
        .is_ok_and(|written| written.is_ok());
    if written {
        let _closed = tokio::time::timeout(METRICS_IO_TIMEOUT, stream.shutdown()).await;
    }
}

/// Reserves the one collection this endpoint runs at a time and asks the control loop for
/// it, or answers `None` when a collection is already queued or running, and when the
/// control loop is gone.
///
/// The permit is the reservation, not the request channel's capacity: that channel frees its
/// slot the moment the control loop dequeues the request, which is before the collection it
/// asks for has run, so a second scrape arriving in that window would be queued and made to
/// wait for an answer the endpoint promises to refuse instead. The permit is released when
/// the caller drops it, on every path a caller can leave by.
fn reserve_collection<'a>(
    inflight: &'a Semaphore,
    scrapes: &mpsc::Sender<MetricsScrape>,
) -> Option<(
    SemaphorePermit<'a>,
    oneshot::Receiver<Result<MetricsSnapshot, String>>,
)> {
    let permit = inflight.try_acquire().ok()?;
    let (reply, collected) = oneshot::channel();
    scrapes.try_send(MetricsScrape { reply }).ok()?;
    Some((permit, collected))
}

/// Collects one scrape through the control loop and renders it.
///
/// One collection at a time: a scrape arriving while another is queued or running is refused
/// immediately rather than queued, because a queue of collections is a queue of control-loop
/// work no scraper is bounded by. What holds a scrape's place is
/// [`reserve_collection`]'s permit, from the request through the rendered answer.
///
/// A scrape that gives up at [`METRICS_IO_TIMEOUT`] releases its permit while the collection
/// it abandoned may still be running, so the next scrape can overlap one already in flight
/// by at most [`METRICS_COLLECTION_TIMEOUT`], which is what bounds a collection inside the
/// loop.
///
/// Resident memory is read after the collection and off this thread, bounded by
/// [`METRICS_IO_TIMEOUT`]: a read that does not answer in time costs the document that one
/// metric rather than the scrape.
async fn scrape_answer(scrapes: &mpsc::Sender<MetricsScrape>, inflight: &Semaphore) -> String {
    let Some((_collecting, collected)) = reserve_collection(inflight, scrapes) else {
        return http_answer(
            "503 Service Unavailable",
            PLAIN_CONTENT_TYPE,
            "a collection is already in flight\n",
        );
    };
    match tokio::time::timeout(METRICS_IO_TIMEOUT, collected).await {
        Ok(Ok(Ok(snapshot))) => {
            let resident = tokio::time::timeout(METRICS_IO_TIMEOUT, resident_bytes())
                .await
                .ok()
                .flatten();
            http_answer(
                "200 OK",
                EXPOSITION_CONTENT_TYPE,
                render_metrics(&snapshot, resident).as_str(),
            )
        }
        Ok(Ok(Err(refusal))) => http_answer(
            "503 Service Unavailable",
            PLAIN_CONTENT_TYPE,
            format!("{refusal}\n").as_str(),
        ),
        Ok(Err(_)) | Err(_) => http_answer(
            "503 Service Unavailable",
            PLAIN_CONTENT_TYPE,
            "this daemon did not answer the collection\n",
        ),
    }
}

/// Reads one request head, or `None` for a peer that closed, overran
/// [`METRICS_HEAD_LIMIT`], or sent bytes that are not text.
///
/// The whole head is read rather than the request line alone, so the answer is written to a
/// peer with nothing left unread on the socket.
async fn read_request_head(stream: &mut TcpStream) -> Option<String> {
    let mut head = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        head.extend_from_slice(&chunk[..read]);
        if head.len() > METRICS_HEAD_LIMIT {
            return None;
        }
        if let Some(end) = head_end(head.as_slice()) {
            head.truncate(end);
            return String::from_utf8(head).ok();
        }
    }
}

/// Where a request head ends: the blank line after the last header.
fn head_end(head: &[u8]) -> Option<usize> {
    let crlf = head.windows(4).position(|window| window == b"\r\n\r\n");
    let bare = head.windows(2).position(|window| window == b"\n\n");
    match (crlf, bare) {
        (Some(crlf), Some(bare)) => Some(crlf.min(bare)),
        (Some(crlf), None) => Some(crlf),
        (None, bare) => bare,
    }
}

/// Whether a request head asks for this daemon's metrics: `GET /metrics`, with or without a
/// query the exposition ignores.
fn asks_for_metrics(head: &str) -> bool {
    let Some(line) = head.lines().next() else {
        return false;
    };
    let mut words = line.split(' ');
    let method = words.next().unwrap_or_default();
    let target = words.next().unwrap_or_default();
    let target = target.split('?').next().unwrap_or_default();
    method == "GET" && target == METRICS_TARGET
}

/// One HTTP/1.0 answer: a status line, the headers this endpoint always sends, and a body.
fn http_answer(status: &str, content_type: &str, body: &str) -> String {
    format!(
        "HTTP/1.0 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: \
         close\r\n\r\n{body}",
        body.len()
    )
}

/// Renders one snapshot as a Prometheus text exposition document.
///
/// One `# HELP` and one `# TYPE` per family, before that family's first sample, with every
/// shard's sample of it beneath: metadata repeated between samples is not a document a
/// scraper accepts. Every value is an integer, because every value behind it is one — the
/// counters are counts and the queue-age figures are whole microseconds — so nothing here
/// is rounded through a float.
///
/// A distribution is never a lone figure: the queue-age families report the sample count
/// beside the last, maximum, p50 and p99, so a percentile with nothing behind it reads as
/// unmeasured rather than as fast.
fn render_metrics(snapshot: &MetricsSnapshot, resident: Option<u64>) -> String {
    use core::fmt::Write as _;
    let mut body = String::new();
    let redundancy: &[(&str, &str, &str, ShardSample)] = if snapshot
        .shards
        .iter()
        .any(|shard| shard.metrics.replicas > 1)
    {
        REDUNDANCY_FAMILIES
    } else {
        &[]
    };
    for (name, kind, help, sample) in SHARD_FAMILIES.iter().chain(redundancy) {
        declare(&mut body, name, kind, help);
        for shard in &snapshot.shards {
            let _ = writeln!(
                body,
                "{name}{{shard=\"{}\"}} {}",
                shard.shard,
                sample(shard)
            );
        }
    }
    if !redundancy.is_empty() {
        declare(
            &mut body,
            "pmws_shard_standby_ends",
            "counter",
            "Standby connections of this shard that ended, by what ended them.",
        );
        for shard in &snapshot.shards {
            for (reason, count) in &shard.metrics.stats.standby_ends {
                let _ = writeln!(
                    body,
                    "pmws_shard_standby_ends{{shard=\"{}\",reason=\"{}\"}} {count}",
                    shard.shard,
                    escape_label(reason)
                );
            }
        }
        declare(
            &mut body,
            "pmws_shard_pool_degraded",
            "gauge",
            "1 against the observation that withdrew this shard's licence to publish \
             across its connections. Absent while the licence stands.",
        );
        for shard in &snapshot.shards {
            if let Some(reason) = shard.metrics.stats.pool_degraded {
                let _ = writeln!(
                    body,
                    "pmws_shard_pool_degraded{{shard=\"{}\",reason=\"{}\"}} 1",
                    shard.shard,
                    escape_label(pool_degrade_label(reason))
                );
            }
        }
    }
    declare(
        &mut body,
        "pmws_shard_events",
        "counter",
        "Decoded venue events, by kind.",
    );
    for shard in &snapshot.shards {
        let index = shard.shard;
        let stats = &shard.metrics.stats;
        for (kind, count) in [
            ("orderbook", stats.events_orderbook),
            ("resolved", stats.events_resolved),
            ("unknown", stats.events_unknown),
        ] {
            let _ = writeln!(
                body,
                "pmws_shard_events{{shard=\"{index}\",kind=\"{kind}\"}} {count}"
            );
        }
    }
    declare(
        &mut body,
        "pmws_shard_decode_failures",
        "counter",
        "Venue frames this shard could not decode, by reason.",
    );
    for shard in &snapshot.shards {
        for (reason, count) in &shard.metrics.stats.decode_failures {
            let _ = writeln!(
                body,
                "pmws_shard_decode_failures{{shard=\"{}\",reason=\"{}\"}} {count}",
                shard.shard,
                escape_label(reason)
            );
        }
    }
    for (name, kind, help, value) in [
        (
            "pmws_answers_abandoned",
            "counter",
            "Control answers this daemon gave up writing because the client stopped reading.",
            snapshot.answers_abandoned,
        ),
        (
            "pmws_attachments_refused",
            "counter",
            "Consumer attach requests this daemon refused, for every reason it refuses one.",
            snapshot.attachments_refused,
        ),
        (
            "pmws_markets",
            "gauge",
            "Markets this daemon holds a book for, across every shard.",
            snapshot.markets,
        ),
        (
            "pmws_markets_pinned",
            "gauge",
            "Markets an operator command holds in the desired set.",
            snapshot.pinned,
        ),
        (
            "pmws_market_leases",
            "gauge",
            "Market leases held by control sessions, counted once per session per market.",
            snapshot.leases,
        ),
        (
            "pmws_pid",
            "gauge",
            "This daemon's process identifier, for pairing external process accounting.",
            u64::from(snapshot.pid),
        ),
    ] {
        declare(&mut body, name, kind, help);
        let _ = writeln!(body, "{name} {value}");
    }
    if let Some(resident) = resident {
        declare(
            &mut body,
            "pmws_resident_bytes",
            "gauge",
            "This daemon's resident set size in bytes, where the operating system reports it.",
        );
        let _ = writeln!(body, "pmws_resident_bytes {resident}");
    }
    body
}

/// Writes one family's `# HELP` and `# TYPE` lines.
fn declare(body: &mut String, name: &str, kind: &str, help: &str) {
    use core::fmt::Write as _;
    let _ = writeln!(body, "# HELP {name} {help}");
    let _ = writeln!(body, "# TYPE {name} {kind}");
}

/// One label value with the three characters the exposition format reserves escaped.
///
/// Every reason this daemon counts is one of its own `&'static str` keys, so nothing here
/// carries venue text today; the escape is what keeps the document well formed if a key ever
/// stops being one of those.
fn escape_label(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            other => escaped.push(other),
        }
    }
    escaped
}

/// This process's resident set size in bytes, where the operating system reports it without
/// a dependency.
///
/// Linux answers from `/proc/self/status`, whose `VmRSS` line is already in kibibytes. macOS
/// has no equivalent a dependency-free daemon can read, so it answers `None` and the metric
/// is absent from the document rather than guessed; `pmwsctl status` reports resident memory
/// there by asking the operating system about the daemon's pid from outside it.
///
/// The read happens on the blocking pool, never on the runtime thread. A kernel-generated
/// file is still read through the filesystem, and this daemon drives every shard, every book
/// writer and the control loop on one thread: a read that stalls must cost this metric and
/// nothing else. Giving up on it does not cancel it — an abandoned read finishes on the pool
/// unattended, and its answer is dropped.
#[cfg(target_os = "linux")]
async fn resident_bytes() -> Option<u64> {
    let status = tokio::task::spawn_blocking(|| std::fs::read_to_string("/proc/self/status"))
        .await
        .ok()?
        .ok()?;
    let kibibytes = status
        .lines()
        .find(|line| line.starts_with("VmRSS:"))?
        .split_whitespace()
        .nth(1)?
        .parse::<u64>()
        .ok()?;
    kibibytes.checked_mul(1024)
}

#[cfg(not(target_os = "linux"))]
async fn resident_bytes() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::{
        METRICS_COLLECTION_TIMEOUT, MetricsSnapshot, Router, ShardSnapshot, doorbell_page_path,
        escape_label, foreign_peer_refusal, names_the_created_object, render_metrics,
        reserve_collection,
    };
    use pm_ws::ControlResponse;
    use pm_ws::limitless::shard::{
        PoolReport, PoolState, QueueAgeSummary, Shard, ShardConfig, ShardMetrics, ShardStats,
    };
    use std::os::unix::fs::MetadataExt;
    use std::path::{Path, PathBuf};
    use tokio::sync::{Semaphore, mpsc};

    fn rendered_snapshot() -> String {
        rendered_topology(2)
    }

    fn rendered_topology(replicas: usize) -> String {
        let shards = (0..2)
            .map(|shard| ShardSnapshot {
                shard,
                attachments: 3,
                metrics: ShardMetrics {
                    connected: true,
                    reconciling: false,
                    replicas,
                    standbys_established: replicas.saturating_sub(1),
                    standby_agreeing_markets: 6,
                    pool: (replicas > 1).then_some(PoolReport {
                        sockets: replicas,
                        covering: replicas,
                        state: PoolState::Armed,
                    }),
                    desired: 7,
                    markets: 9,
                    segment_markets: 9,
                    stats: ShardStats {
                        frames_seen: 11,
                        events_orderbook: 5,
                        queue_age: QueueAgeSummary {
                            samples: 4,
                            last_micros: 12,
                            max_micros: 40,
                            p50_micros: 16,
                            p99_micros: 32,
                        },
                        decode_failures: [("envelope\"\\", 2_u64)].into_iter().collect(),
                        standby_ends: [("disconnect", 1_u64)].into_iter().collect(),
                        ..ShardStats::default()
                    },
                },
            })
            .collect();
        render_metrics(
            &MetricsSnapshot {
                pid: 4242,
                answers_abandoned: 1,
                attachments_refused: 2,
                markets: 18,
                pinned: 6,
                leases: 4,
                shards,
            },
            Some(65_536),
        )
    }

    /// Every metric family a daemon named before redundancy was configurable, in the order
    /// it rendered them.
    ///
    /// Written out rather than derived: what it pins is the scrape surface itself, which a
    /// dashboard and an alert rule are both written against, so a family appearing under a
    /// configuration that did not ask for one is the regression this catches.
    const FAMILIES_BEFORE_REPLICAS: &[&str] = &[
        "pmws_shard_frames_seen",
        "pmws_shard_snapshots_applied",
        "pmws_shard_mutations_derived",
        "pmws_shard_resolutions_forwarded",
        "pmws_shard_overload_drops",
        "pmws_shard_continuity_losses",
        "pmws_shard_connection_attempts",
        "pmws_shard_markets_dropped",
        "pmws_shard_segment_attachments",
        "pmws_shard_queue_age_samples",
        "pmws_shard_queue_age_last_micros",
        "pmws_shard_queue_age_max_micros",
        "pmws_shard_queue_age_p50_micros",
        "pmws_shard_queue_age_p99_micros",
        "pmws_shard_publish_latency_samples",
        "pmws_shard_publish_latency_last_micros",
        "pmws_shard_publish_latency_max_micros",
        "pmws_shard_publish_latency_p50_micros",
        "pmws_shard_publish_latency_p99_micros",
        "pmws_shard_publish_latency_p999_micros",
        "pmws_shard_queue_depth_max",
        "pmws_shard_connected",
        "pmws_shard_reconciling",
        "pmws_shard_desired_markets",
        "pmws_shard_markets",
        "pmws_shard_segment_markets",
        "pmws_shard_events",
        "pmws_shard_decode_failures",
        "pmws_answers_abandoned",
        "pmws_attachments_refused",
        "pmws_markets",
        "pmws_markets_pinned",
        "pmws_market_leases",
        "pmws_pid",
        "pmws_resident_bytes",
    ];

    /// Every family the document declares, in render order.
    fn declared_families(document: &str) -> Vec<&str> {
        document
            .lines()
            .filter_map(|line| line.strip_prefix("# TYPE "))
            .filter_map(|rest| rest.split(' ').next())
            .collect()
    }

    /// A daemon nobody configured redundancy for exposes the scrape surface it always did.
    ///
    /// Family *presence*, not only sample presence: a `# HELP`/`# TYPE` pair with no series
    /// under it is still a new family on the surface, and a gate that only suppressed the
    /// samples would leave one behind.
    #[test]
    fn a_default_topology_daemon_declares_the_families_that_preceded_replicas() {
        assert_eq!(
            declared_families(&rendered_topology(1)),
            FAMILIES_BEFORE_REPLICAS
        );
    }

    /// A daemon that did configure redundancy gets the families that describe it.
    #[test]
    fn a_daemon_running_standbys_declares_its_redundancy_families() {
        let document = rendered_topology(2);
        let families = declared_families(&document);
        for name in [
            "pmws_shard_replicas",
            "pmws_shard_standbys_established",
            "pmws_shard_standby_agreeing_markets",
            "pmws_shard_standby_ends",
        ] {
            assert!(families.contains(&name), "{name} is declared at replicas 2");
        }
    }

    /// Every family declares itself once, before its own samples and nowhere between them.
    ///
    /// The failure this pins is the one a hand-rolled exporter makes: rendering shard by
    /// shard, which repeats a family's `# HELP` and `# TYPE` between its samples and produces
    /// a document a scraper refuses whole. Interleaving is checked as well as duplication,
    /// because a family split in two is refused for the same reason.
    #[test]
    fn every_metric_family_declares_itself_once_before_its_own_samples() {
        let document = rendered_snapshot();
        let mut declared: Vec<&str> = Vec::new();
        let mut sampled: Vec<&str> = Vec::new();
        for line in document.lines() {
            if let Some(rest) = line.strip_prefix("# TYPE ") {
                let name = rest.split(' ').next().expect("a TYPE line names a family");
                assert!(!declared.contains(&name), "{name} declares itself twice");
                declared.push(name);
                continue;
            }
            if line.starts_with('#') || line.is_empty() {
                continue;
            }
            let name = line
                .split(['{', ' '])
                .next()
                .expect("a sample line names its family");
            assert!(
                declared.contains(&name),
                "{name} carries a sample before it declares itself"
            );
            if sampled.last() != Some(&name) {
                assert!(
                    !sampled.contains(&name),
                    "{name} has its samples split by another family's"
                );
                sampled.push(name);
            }
        }
        for name in [
            "pmws_shard_frames_seen",
            "pmws_shard_queue_age_samples",
            "pmws_shard_queue_age_p99_micros",
            "pmws_shard_events",
            "pmws_shard_decode_failures",
            "pmws_markets",
            "pmws_pid",
            "pmws_resident_bytes",
        ] {
            assert!(
                sampled.contains(&name),
                "{name} is declared and never sampled"
            );
        }
    }

    /// A label value cannot end its own quoted string.
    ///
    /// The reasons counted today are this daemon's own `&'static str` keys, so the escape is
    /// what keeps the document well formed if one ever stops being one of those.
    #[test]
    fn a_label_value_cannot_break_out_of_its_quotes() {
        assert_eq!(escape_label("plain"), "plain");
        assert_eq!(escape_label("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
        let document = rendered_snapshot();
        assert!(
            document.contains("reason=\"envelope\\\"\\\\\""),
            "the escaped reason is what reaches the document: {document}"
        );
    }

    /// The credential policy, tested where the decision is made.
    ///
    /// A foreign effective user cannot be fabricated inside a test process — the peer of a
    /// socket this process connects is this process — so what the live attachment tests prove
    /// is the *accepting* branch, and this proves the branch that refuses: the same-user case
    /// passes, every other user is refused, and the refusal names both users and no market.
    #[test]
    fn only_a_peer_running_as_this_daemon_is_transferred_a_descriptor() {
        assert!(
            foreign_peer_refusal(501, 501).is_none(),
            "a peer running as this daemon is served"
        );
        let Some(ControlResponse::Error { message }) = foreign_peer_refusal(501, 502) else {
            panic!("a peer running as another user is refused with a typed error");
        };
        assert!(
            message.contains("501") && message.contains("502"),
            "the refusal names the user served and the user refused: {message}"
        );
        assert!(
            !message.contains("market"),
            "a credential refusal discloses nothing about the market asked for: {message}"
        );
        assert!(
            foreign_peer_refusal(0, 501).is_some(),
            "a daemon running as root serves no other user either"
        );
    }

    /// The sibling page the daemon unlinks at shutdown is the one the writer created.
    ///
    /// Tested here because the two are built in different modules from the same rule, and
    /// this side's failure is silent: the page is unlinked only when its recorded identity
    /// still matches, so a wrong name would look exactly like a page something else had
    /// already replaced. The name is asserted against the writer's own spelling of it,
    /// `<segment>.doorbell`, appended to the file name rather than replacing an extension.
    #[test]
    fn the_page_a_shutdown_unlinks_is_the_one_the_writer_created() {
        assert_eq!(
            doorbell_page_path(Path::new(
                "/tmp/pmws-0123456789abcdef0123456789abcdef-1.seg"
            )),
            PathBuf::from("/tmp/pmws-0123456789abcdef0123456789abcdef-1.seg.doorbell"),
            "the page name is the segment's own with `.doorbell` appended, never an extension \
             swapped for it"
        );
    }

    /// A read-only descriptor that does not name the created object is refused by name.
    ///
    /// The race this guards cannot be staged deterministically — winning it means swapping the
    /// file between an exclusive create and the very next `open`, which no test can schedule —
    /// so what is pinned here is the decision that race reaches: the checker is handed a
    /// creation identity deliberately unlike the descriptor's, and must refuse, naming the
    /// path an operator would have to look at. The matching case is asserted beside it so the
    /// refusal cannot be vacuous.
    #[test]
    fn a_descriptor_that_is_not_the_created_object_is_refused() {
        let path = std::path::PathBuf::from(format!(
            "/tmp/pmws-creation-identity-{}.seg",
            std::process::id()
        ));
        let _removed = std::fs::remove_file(path.as_path());
        std::fs::write(path.as_path(), [0_u8; 128]).expect("the fixture file is written");
        let opened = std::fs::File::open(path.as_path()).expect("the fixture file opens");
        let stat = opened.metadata().expect("the descriptor stats");
        let identity = (stat.dev(), stat.ino());

        assert_eq!(
            names_the_created_object(path.as_path(), identity, &stat, 128),
            Ok(()),
            "the object a descriptor was stat'd from is the object it names"
        );

        let refusal =
            names_the_created_object(path.as_path(), (identity.0, identity.1 ^ 1), &stat, 128)
                .expect_err("a descriptor naming another object is refused");
        assert!(
            refusal.contains(path.display().to_string().as_str()),
            "the refusal names the path an operator has to look at: {refusal}"
        );
        assert!(
            names_the_created_object(path.as_path(), identity, &stat, 256).is_err(),
            "the created object at the wrong length is refused too"
        );

        let _removed = std::fs::remove_file(path.as_path());
    }

    /// A scrape arriving while a collection is queued or still running is refused, not queued.
    ///
    /// The window this pins is the one the request channel's capacity alone does not close:
    /// the control loop has taken the request — freeing the channel's only slot — and the
    /// collection it asks for has not answered yet. A second scrape admitted there is made to
    /// wait for the answer the endpoint documents as an immediate refusal, and a flood of
    /// them keeps one extra collection permanently queued behind the loop.
    #[tokio::test]
    async fn a_scrape_is_refused_while_a_collection_is_queued_or_still_running() {
        let inflight = Semaphore::new(1);
        let (requests, mut queue) = mpsc::channel(1);

        let reserved =
            reserve_collection(&inflight, &requests).expect("the first scrape is admitted");
        let taken = queue
            .recv()
            .await
            .expect("the control loop takes the request, freeing the channel's slot");

        assert!(
            reserve_collection(&inflight, &requests).is_none(),
            "a scrape arriving between the request being dequeued and its collection answering \
             is refused"
        );

        drop(taken);
        drop(reserved);
        let (_permit, _collected) = reserve_collection(&inflight, &requests)
            .expect("the reservation is released with the scrape that held it");
    }

    /// A shard that took the collection command and never answers costs the control loop a
    /// bounded wait and the scrape its answer, not the run.
    ///
    /// The shard here is constructed and never run, so its command queue accepts the request
    /// and nothing ever services it — the state a wedged shard leaves the loop in. What is
    /// asserted is that the collection *returns*: `Router::metrics` runs on the one loop that
    /// also accepts control connections, sweeps leases and notices a lost shard, so a
    /// collection that never came back would suspend all of it for as long as the shard
    /// stayed silent. The outer deadline is four times the inner one, so a regression fails
    /// this test rather than hanging it.
    #[tokio::test]
    async fn a_shard_that_never_answers_fails_the_collection_within_its_deadline() {
        let shard = Shard::new(ShardConfig::default()).expect("a default shard is constructible");
        let mut router = Router::new(8, 0, None);
        router.handles.push(shard.handle());

        let collected = tokio::time::timeout(METRICS_COLLECTION_TIMEOUT * 4, router.metrics())
            .await
            .expect("the collection is bounded rather than suspended on a silent shard");

        let Err(refusal) = collected else {
            panic!("a shard that never answers fails the collection rather than reporting one");
        };
        assert!(
            refusal.contains("shard 0"),
            "the refusal names the shard that did not answer: {refusal}"
        );
        assert!(
            refusal.contains("nothing was collected"),
            "a failed collection is refused whole rather than answered short: {refusal}"
        );
    }
}

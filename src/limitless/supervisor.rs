//! The single owner of one Limitless market's authoritative book and of the connection
//! generations that feed it.
//!
//! Exactly one task runs this loop, and it holds the only [`BookWriter`]. Connections are
//! separate tasks that push generation-tagged notices into one bounded queue; the
//! supervisor publishes from exactly one generation at a time and discards everything
//! else, so a connection this supervisor has already declared dead can never mutate the
//! book with work that was still in flight.
//!
//! Every connection beyond the first runs in a [`Slot::Standby`] role. Its accepted
//! snapshots feed a shadow [`OrderBook`] this same task owns, which is never published to
//! consumers and never counted as the market's liquidity: consumers see one physical book.
//! Every role draws generations from one counter, so a generation number identifies a
//! connection across the whole supervisor and fencing stays a single comparison.
//!
//! With `pooled` set those connections are a publishing pool instead. Each still keeps its
//! own shadow, but the book's next state is whichever arrival first carries a venue key past
//! the last published one, on any socket, and there is no publishing primary while the
//! pool's licence stands. The licence is withdrawn by the first arrival contradicting the
//! conformance basis recorded in `docs/limitless.md`, which hands the same connections back
//! to the primary-and-standby topology above without touching the published book. Everything
//! about generations, fencing, heartbeats, reconnect ladders and the attempt ledger is the
//! same either way: a pool changes which arrival publishes, and nothing about how a
//! connection lives or dies.
//!
//! Liveness here is evidence-based and comes from one source only: Engine.IO ping notices.
//! Market data never refreshes the heartbeat deadline, and no timer anywhere in this module
//! observes market activity — a subscribed market that says nothing for an hour stays
//! [`crate::AuthorityState::Live`] as long as its connection's heartbeat holds. The backoff and
//! fenced-linger timers govern connection scheduling only; neither can change book
//! authority.

use crate::limitless::connection::{
    ConnectionConfig, ConnectionControl, ConnectionEndReason, ConnectionNote, ConnectionNotice,
    DEFAULT_ENDPOINT, run_connection,
};
use crate::limitless::{
    LimitlessEvent, MarketResolved, ORDERBOOK_UPDATE_DEDUP_KEY, OrderbookUpdate,
};
use crate::shm::{RES_DATE_CAPACITY, RES_OUTCOME_CAPACITY, RES_TYPE_CAPACITY};
use crate::wire::session::TerminalFrameReason;
use crate::{
    AuthorityReason, AuthorityState, BookCommit, BookError, BookObserver, BookWriter,
    BoundedSourceEvidence, Candidate, ConnectionIdentity, ContentDigest, ContinuityReason,
    DedupKey, DedupKeyError, DeliveryPath, DescriptorError, DivergenceReason, HotStandby,
    IdentityError, LevelCapacity, LocalMonotonicTimestamp, MarketHandle, MarketRef,
    MarketResolution, NativeIdentifierKind, NativeLabel, NativeMarketKey, NativeOutcome,
    ObservationError, ObserverCapacity, OrderBook, Origin, PoolDegradeReason, PoolError, PoolGate,
    PoolSocket, PoolSocketState, PoolVerdict, PoolViolation, Provenance, ProvenanceInput,
    PublishedBook, PublishingPrimary, RecoveryReplica, ReplicaFailureReason, ReplicaRole,
    Representation, ResolutionDelivery, ResolutionObservation, SegmentWriter,
    SourceEvidenceCapacity, SourceState, SourceTimestamp, StandbyState, Venue, WriterError,
    candidate_projection, content_digest,
};
use core::future::Future;
use core::pin::Pin;
use core::task::Poll;
use core::time::Duration;
use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, VecDeque};
use std::hash::BuildHasher;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tokio::time::Instant;

/// The venue this adapter speaks for, and the venue half of every market identity it
/// publishes.
pub const VENUE: &str = "limitless";

/// The deepest book this supervisor accepts from the venue, in levels per snapshot.
///
/// The single source of truth for book depth: every surface that must be able to carry an
/// accepted book — the shared-memory segment included — sizes itself from this, so a
/// snapshot the supervisor admits can never be one a downstream surface has to refuse.
pub const MAX_BOOK_LEVELS: usize = 4096;

const _: () = assert!(
    MAX_BOOK_LEVELS <= crate::MAX_LEVEL_CAPACITY as usize,
    "a book this supervisor accepts must fit one shared-memory state slot"
);
pub(crate) const CONNECTION_NAME: &str = "limitless-markets";
const ORDERBOOK_UPDATE_EVENT: &str = "orderbookUpdate";
pub(crate) const MARKET_RESOLVED_EVENT: &str = "marketResolved";
const JITTER_FLOOR_PER_MILLE: u64 = 750;
const JITTER_SPAN_PER_MILLE: u64 = 501;
const MAX_BACKOFF_DOUBLINGS: u32 = 16;
const ONE_DAY: Duration = Duration::from_secs(86_400);
/// The most connection attempts anything in this module will account for in one rolling day.
///
/// [`AttemptLedger`] stores one [`Instant`] per admitted attempt and admits up to whatever
/// budget it is given, so the configured budget is also the ledger's storage bound and must
/// itself be bounded — at this ceiling the ledger holds a million instants and no more. A
/// budget configured above it would be a budget the ledger is asked to store past the only
/// figure anything here sizes against. Both callers that accept a budget from configuration
/// — `daily_connection_attempt_budget` in `crate::daemon::DaemonConfig` and
/// [`SupervisorConfig::daily_attempt_budget`] — refuse a document above it.
pub const WORST_CASE_ATTEMPT_CEILING: u64 = 1_000_000;

/// The default shortest gap between two subscription-bearing commands this process puts on
/// the wire toward one endpoint, when a configuration does not name its own.
///
/// No retrieved Limitless documentation places a sustained-command ceiling; this is this
/// project's own conservative default for a polite wire citizen, carried in
/// [`SupervisorConfig::min_command_interval_ms`] and threaded from
/// `crate::daemon::DaemonConfig`'s equivalent key so an operator can widen or narrow it.
/// Every such command draws from one permit: every role's and every shard's establishing
/// subscription, and every later re-emit. Pacing is a property of one endpoint and the
/// source address dialling it, not of one socket, so pacing each connection separately
/// would let two connections exceed the configured floor while each stayed inside it —
/// which is why [`COMMAND_PACERS`] is a process-wide static keyed by endpoint.
///
/// The permit is taken inside the connection writer, immediately before the bytes of a
/// `subscribe_market_prices` command are written (`crate::limitless::connection`), rather
/// than where a supervisor or a shard decides to send one. The distinction matters because
/// a dial and a handshake stand between a decision and the write, and neither is timed by
/// the code that authorized the command: metering decision points leaves two connections
/// whose handshakes converge free to land their subscriptions inside the floor. Metering
/// the write makes the spacing on the wire the spacing the configured floor names.
///
/// A window that must cover a command still has to allow for that wait, which is why a
/// reissue's deadline is the configured window plus this floor.
pub(crate) const MIN_COMMAND_INTERVAL: Duration = Duration::from_millis(500);

/// The structural maximum a configured command floor may name, in milliseconds.
///
/// Every recovery and reissue deadline runs from the pacer slot its command reserved, which
/// stands one floor behind the command before it and further back again for every command
/// queued ahead of that. A floor above a minute therefore makes resubscribe-then-reconnect
/// recovery indistinguishable from an outage: the daemon would sit inside its own pacing for
/// longer than any venue would take to answer, and nothing downstream could tell a slow
/// queue from a dead rail. The bound also keeps the `Instant + Duration` arithmetic every one
/// of those deadlines is built from trivially far from saturation, fleet-sized queues
/// included.
pub const MAX_COMMAND_INTERVAL_MS: u64 = 60_000;

/// The default number of connection attempts this process will spend in a day, when a
/// configuration does not name its own.
///
/// No retrieved Limitless documentation places a daily connection-attempt ceiling; two
/// hundred eighty is this project's own conservative default, carried in
/// [`SupervisorConfig::daily_attempt_budget`] and `daily_connection_attempt_budget` in
/// `crate::daemon::DaemonConfig`. The ledger enforcing whatever budget is configured is
/// process-local and bounds one process lifetime: it re-arms empty on restart, and a second
/// process — a diagnostic run beside the daemon — keeps a ledger of its own while sharing
/// the source address a venue that does track attempts would count against. What this
/// ledger sees, it clamps — it delays a spawn rather than recording an overspend — and
/// cross-restart and cross-process enforcement is deferred to daemonization.
pub(crate) const DAILY_ATTEMPT_BUDGET: u64 = 280;

/// The most connections one supervisor may run for one market as a publishing primary with
/// hot standbys.
///
/// Aligned to [`crate::MAX_POOL_SOCKETS`], the venue-agnostic structural ceiling on how many
/// sockets one pool may hold: both topologies share the same `2n`-peak-socket arithmetic
/// (each role, like each pool socket, holds at most one live connection plus at most one
/// fenced connection still draining), and nothing about the primary-and-standby ladder
/// assumes a narrower depth — a supervisor keeps its standbys in a plain list and promotes
/// whichever one is viable, however many there are. This is this tool's own supported
/// ladder depth, not a figure any venue placed.
pub const MAX_REPLICAS: usize = 4;

/// The rolling day of connection attempts this process has spent, against whichever
/// caller's configured budget admitted it.
///
/// Process-wide because every role, every market, and every ladder in this process dials
/// from one source address, and a ledger enforced per ladder would not see what a venue
/// that does track attempts by source address would see.
static ATTEMPT_LEDGER: Mutex<AttemptLedger> = Mutex::new(AttemptLedger::new());

/// A rolling 24-hour record of connection attempts spent against a caller's configured
/// daily budget.
///
/// Storage is bounded by the budget it is asked to enforce: an attempt is recorded only
/// when it is admitted, admission stops at the budget, and entries older than a day are
/// discarded on every decision, so the window rolls rather than resetting on a calendar
/// boundary.
struct AttemptLedger {
    spent: VecDeque<Instant>,
}

impl AttemptLedger {
    const fn new() -> Self {
        Self {
            spent: VecDeque::new(),
        }
    }

    /// Records one connection attempt made at `now`, or refuses it and names the instant
    /// the oldest attempt still inside the rolling day ages out of it.
    ///
    /// A refusal is a clamp, not a counter that lies: the caller waits for the returned
    /// instant instead of spending an attempt past `budget`.
    fn admit(&mut self, now: Instant, budget: u64) -> Result<(), Instant> {
        while self
            .spent
            .front()
            .is_some_and(|at| now.saturating_duration_since(*at) >= ONE_DAY)
        {
            let _ = self.spent.pop_front();
        }
        let budget = usize::try_from(budget).unwrap_or(usize::MAX);
        if self.spent.len() >= budget {
            let oldest = self.spent.front().copied().unwrap_or(now);
            return Err(oldest + ONE_DAY);
        }
        self.spent.push_back(now);
        Ok(())
    }
}

/// Spends one attempt from the process-wide ledger against the caller's configured
/// `budget`, or names when the next one is free.
pub(crate) fn admit_attempt(now: Instant, budget: u64) -> Result<(), Instant> {
    ATTEMPT_LEDGER
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .admit(now, budget)
}

/// The instant a subscription-bearing command may follow the one reserved at `last`, spaced
/// at least `interval` after it.
pub(crate) fn command_grant_at(last: Option<Instant>, now: Instant, interval: Duration) -> Instant {
    last.map_or(now, |at| at + interval).max(now)
}

/// The furthest instant any subscription-bearing command has been reserved for against each
/// venue endpoint this process has dialled.
///
/// Process-wide for the same reason [`ATTEMPT_LEDGER`] is: every supervisor, shard, role
/// and socket in this process dials from one source address, so a pacer owned by one book
/// would let two books each stay inside their own configured floor while the address they
/// share crosses it — the residual the single-market daemon recorded and deferred to the
/// multi-market one.
///
/// The key is the endpoint because pacing is a property of traffic *to one address*.
/// Commands this process sends somewhere else — a second venue later, a controlled peer in
/// a fault test — dial a different address, and pacing them against this floor would
/// throttle work the configured floor was never meant to cover. Every shard of one daemon
/// reads one configured endpoint, so a daemon's shards share one pacer and jointly honor
/// one floor.
///
/// A slot spaces each caller by *that caller's own* configured floor against the shared
/// reservation, so a process running deliberately different floors toward one endpoint gets
/// each caller's own promise kept rather than the strictest of them imposed on all: a caller
/// configured at 500 ms never writes within 500 ms of the previous command, while a caller
/// configured at zero may write immediately after it. One daemon's shards all read one
/// configuration, which is what keeps a deployment coherent; a process that mixes floors —
/// a diagnostic run beside the daemon, say — is choosing that, and gets exactly what each of
/// its callers asked for.
///
/// The instant held is a *reservation* rather than a record of the last write: it is where
/// the schedule has been filled to, which is what lets a caller learn its own position in
/// the queue at the moment it joins one. See [`reserve_command_grant`].
///
/// Storage is bounded at [`MAX_PACED_ENDPOINTS`]. A process that dials more endpoints than
/// that paces every further one against a single shared slot, which is stricter than
/// per-endpoint pacing and never looser.
static COMMAND_PACERS: Mutex<BTreeMap<String, Instant>> = Mutex::new(BTreeMap::new());

/// The most venue endpoints [`COMMAND_PACERS`] tracks separately.
const MAX_PACED_ENDPOINTS: usize = 64;

/// The pacer slot an endpoint uses: its own while there is room, and one shared overflow
/// slot past that.
fn pacer_slot(pacers: &BTreeMap<String, Instant>, endpoint: &str) -> String {
    if pacers.contains_key(endpoint) || pacers.len() < MAX_PACED_ENDPOINTS {
        endpoint.to_owned()
    } else {
        String::new()
    }
}

/// Reserves the next subscription-bearing command slot against `endpoint` and names the
/// instant its bytes may be written.
///
/// The grant is `max(last_reserved + interval, now)`, and `last_reserved` advances to it, so
/// the caller after this one stands behind the instant this one *holds* rather than behind
/// the instant it asked. Reserving never refuses and never blocks: it hands back a place in
/// a queue, and waiting for that place is the caller's job.
///
/// This is the whole point of a reservation rather than a permit. The pacer is process-wide,
/// so a fleet whose connections all need a command at once forms one queue, and the `k`-th
/// command cannot reach the wire before `k` intervals have passed. A caller that only
/// learned "not yet, try again" could never tell how long its own wait was, and every
/// deadline written against it had to guess — invariably at one interval, which is right
/// only for the caller that happens to stand first. A caller that learns its granted instant
/// can budget for the queue it is actually in.
///
/// Reserving and spending are one locked operation, so two callers cannot be handed the same
/// place: whoever takes the lock second is spaced behind whoever took it first.
///
/// A configured interval of zero degenerates to `max(last_reserved, now)` — every caller is
/// granted the present moment and nothing waits. No special case.
///
/// A reservation whose command is never written leaves a hole in the schedule: the
/// connection's task ended before it could take the command, or the encoded command turned
/// out not to fit, and the slot passes unused while every later reservation still stands
/// behind it. That is accepted and never compensated for. A hole can only make this process
/// quieter toward the venue than its configured floor allows, which is the safe direction,
/// and any machinery that reclaimed one — a released reservation, a compacted schedule —
/// would have to be correct under exactly the races the reservation exists to remove.
///
/// Storage is bounded at [`MAX_PACED_ENDPOINTS`]; the overflow slot is shared, which is
/// stricter than per-endpoint pacing and never looser.
pub(crate) fn reserve_command_grant(endpoint: &str, now: Instant, interval: Duration) -> Instant {
    let mut pacers = COMMAND_PACERS
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let slot = pacer_slot(&pacers, endpoint);
    let granted = command_grant_at(pacers.get(&slot).copied(), now, interval);
    let _ = pacers.insert(slot, granted);
    granted
}

/// Everything the supervisor needs to keep one market's book alive across reconnects.
///
/// The reconnect timings are operator configuration, defaulted conservatively rather than
/// pinned by anything a venue documents. What one connection can actually spend in a day is
/// clamped by `daily_attempt_budget` through [`AttemptLedger`].
///
/// `market` has no default; an empty one is rejected by [`Supervisor::new`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupervisorConfig {
    pub endpoint: String,
    pub market: String,
    pub setup_timeout: Duration,
    /// Diagnostic raw-frame capture, appended by the connection task itself. Blocking file
    /// I/O; `None` in any configuration serving consumers.
    pub capture_path: Option<PathBuf>,
    /// Diagnostic fault injection: how long after this supervisor's first accepted
    /// subscription the connection then holding the publishing role has its task aborted,
    /// which drops its socket without a close frame. `None` — the default — in any
    /// configuration serving consumers.
    ///
    /// It arms once per run and fires once. The abort is a local task end, so the join
    /// reports [`ConnectionEndReason::TaskFailed`] and the book's loss reads
    /// [`AuthorityReason::LocalLoss`] rather than the [`AuthorityReason::Disconnect`] a
    /// venue-side socket death reports; everything after that is the ordinary
    /// end-of-connection path, and no loss is reported by any other route. Firing while no
    /// connection holds the publishing role does nothing.
    pub kill_primary_after: Option<Duration>,
    pub ingest_capacity: usize,
    pub observer_capacity: usize,
    pub level_capacity: usize,
    /// How many venue connections feed this market: 1 for a single publishing source, and
    /// one per replica or pool socket beyond that. Anything outside 1..=[`MAX_REPLICAS`] is
    /// rejected by [`Supervisor::new`] while `pooled` is unset, and anything outside
    /// [`crate::MIN_POOL_SOCKETS`]..=[`crate::MAX_POOL_SOCKETS`] while it is set.
    ///
    /// Each connection runs its own reconnect ladder, so the attempt budget is shared: see
    /// `max_backoff`.
    pub replicas: usize,
    /// Whether those connections publish as a pool rather than as one publishing primary
    /// with hot standbys.
    ///
    /// Unset — the default — every connection beyond the first shadows the primary and
    /// publishes nothing, which is the behavior of every configuration that predates the
    /// pool. Set, every connection's arrivals are judged by one venue-key gate and the
    /// first arrival past the last published key becomes the book's next state, whichever
    /// connection carried it; the first observation contradicting the recorded conformance
    /// basis withdraws that licence for the rest of the process and the supervisor falls
    /// back to the unset behavior. See [`crate::PoolGate`].
    ///
    /// Setting it requires the venue's declared key semantics to order frames within a
    /// connection-session; [`Supervisor::new`] refuses it otherwise.
    pub pooled: bool,
    pub initial_backoff: Duration,
    /// The ceiling of one ladder's exponential backoff, stated for a single-source
    /// supervisor. Every ladder paces at this value multiplied by `replicas`, so the whole
    /// supervisor's worst-case daily attempt total is the same whether it runs one
    /// connection or two, and one number governs the configured daily attempt budget
    /// however many roles are configured. The rule is uniform: it applies to whatever value
    /// is configured, defaults and explicit test timings alike.
    pub max_backoff: Duration,
    /// The retry cadence after recovery has been reported exhausted, scaled by `replicas`
    /// exactly as `max_backoff` is.
    pub exhausted_backoff: Duration,
    /// How long an established connection that has been asked to re-emit its subscription
    /// set has to produce an accepted recovery base before the supervisor stops waiting on
    /// it and replaces the connection. Bounds the venue's silence, never the book: a live
    /// book's quiet market is never touched, because a resubscription is only ever attempted
    /// on a book that has already lost authority.
    pub resubscribe_window: Duration,
    /// Consecutive connection generations that may end without producing an accepted
    /// recovery base before the book is reported [`AuthorityReason::RecoveryBaseUnavailable`].
    pub max_recovery_attempts: u32,
    /// How long a fenced connection may keep draining before its task is aborted. Its work
    /// is ineligible for publication from the instant it is fenced, whatever it produces
    /// during this window.
    pub fenced_linger: Duration,
    /// How long a connection generation must live before its end is treated as an ordinary
    /// disconnect rather than a flap. Only a generation that reached this age resets the
    /// backoff ladder, so a venue that accepts, serves, and drops in a loop keeps backing
    /// off instead of reconnecting at the venue's expense.
    pub stable_after: Duration,
    pub daemon_generation: u64,
    /// Diagnostic dedup-key conformance recording: every accepted book update is reported
    /// on the diagnostics tap as a [`SupervisorNotice::AcceptedUpdate`], which is the
    /// evidence a conformance session analyses offline. `false` — the default — in any
    /// configuration serving consumers.
    ///
    /// Off, nothing on the update path changes: neither the key parse nor the content
    /// digest runs. On, both run after the commit, on the same bounded tap every other
    /// notice uses, so a slow diagnostic reader costs dropped notices rather than ingestion.
    pub log_dedup_keys: bool,
    /// The most connection attempts this process will spend in a day, shared across every
    /// ladder and enforced by [`AttemptLedger`].
    ///
    /// Operator configuration, defaulted to [`DAILY_ATTEMPT_BUDGET`]: no retrieved venue
    /// documentation places a daily attempt ceiling, so this is a conservative default
    /// rather than a fact enforced on this process's behalf. The ledger every spawn passes
    /// through delays an attempt past this budget rather than making it.
    pub daily_attempt_budget: u64,
    /// The floor, in milliseconds, between two subscription-bearing commands this process
    /// puts on the wire toward `endpoint`.
    ///
    /// Operator configuration, defaulted to [`MIN_COMMAND_INTERVAL`]: no retrieved venue
    /// documentation places a sustained-command ceiling, so this is a conservative default
    /// for a polite wire citizen rather than a fact enforced on this process's behalf. The
    /// permit is spent at the connection writer, immediately before the command's bytes are
    /// written, so every emit this process sends `endpoint` is spaced at least this floor
    /// apart whatever ladder or role sent it.
    pub min_command_interval_ms: u64,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            endpoint: DEFAULT_ENDPOINT.to_owned(),
            market: String::new(),
            setup_timeout: Duration::from_secs(15),
            capture_path: None,
            kill_primary_after: None,
            ingest_capacity: 1024,
            observer_capacity: 1024,
            level_capacity: MAX_BOOK_LEVELS,
            replicas: 1,
            pooled: false,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(600),
            exhausted_backoff: Duration::from_secs(600),
            resubscribe_window: Duration::from_secs(5),
            max_recovery_attempts: 5,
            fenced_linger: Duration::from_secs(1),
            stable_after: Duration::from_secs(60),
            daemon_generation: 1,
            log_dedup_keys: false,
            daily_attempt_budget: DAILY_ATTEMPT_BUDGET,
            min_command_interval_ms: u64::try_from(MIN_COMMAND_INTERVAL.as_millis())
                .unwrap_or(u64::MAX),
        }
    }
}

impl SupervisorConfig {
    /// One ladder's share of a configured backoff: the configured value multiplied by the
    /// number of ladders this supervisor runs, which is its replica count.
    fn ladder_backoff(&self, configured: Duration) -> Duration {
        configured.saturating_mul(u32::try_from(self.replicas).unwrap_or(u32::MAX))
    }
}

/// A reason the supervisor could not be built.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SupervisorError {
    Market(IdentityError),
    LevelCapacity(ObservationError),
    ObserverCapacity(BookError),
    RecoveryAttemptsZero,
    IngestCapacityZero,
    ReplicasOutOfRange,
    /// `daily_attempt_budget` is above [`WORST_CASE_ATTEMPT_CEILING`], the ceiling the
    /// attempt ledger's storage is sized against.
    DailyAttemptBudgetTooLarge,
    /// `min_command_interval_ms` is above [`MAX_COMMAND_INTERVAL_MS`], past which this
    /// process's own pacing would be indistinguishable from an outage.
    CommandIntervalTooLarge,
    /// A pooled configuration this venue's key declaration or the pool gate's own
    /// structural socket ceiling refuses.
    Pool(PoolError),
}

impl std::fmt::Display for SupervisorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DailyAttemptBudgetTooLarge => write!(
                f,
                "daily_attempt_budget must be at most {WORST_CASE_ATTEMPT_CEILING}"
            ),
            Self::CommandIntervalTooLarge => write!(
                f,
                "min_command_interval_ms must be at most {MAX_COMMAND_INTERVAL_MS}"
            ),
            _ => f.write_str("invalid supervisor configuration"),
        }
    }
}
impl std::error::Error for SupervisorError {}

/// Run-level counters. Every field is evidence of something that happened, not a health
/// verdict; the book's own authority is the verdict.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SupervisorStats {
    pub connection_attempts: u64,
    /// Connection spawns the process-wide daily attempt ledger held back rather than let
    /// the configured daily attempt budget be crossed.
    pub attempts_clamped: u64,
    pub fenced_generations: u64,
    /// Venue events that arrived from a generation this supervisor no longer publishes
    /// from, and were therefore discarded without reaching the book.
    pub fenced_events: u64,
    pub frames_seen: u64,
    pub events_orderbook: u64,
    pub events_resolved: u64,
    pub events_unknown: u64,
    pub snapshots_applied: u64,
    /// Snapshots accepted into the standby's shadow book, which no consumer ever reads.
    pub shadow_snapshots_applied: u64,
    pub mutations_derived: u64,
    pub continuity_losses: u64,
    /// Seamless source switches: a standby took over the publishing role without the
    /// authoritative book being touched.
    pub promotions: u64,
    /// Primary losses where a live standby existed but was not eligible to take over the
    /// book's state, so authority was surrendered and a fresh venue base was awaited.
    pub promotions_refused: u64,
    pub overload_drops: u64,
    pub diagnostics_dropped: u64,
    /// Complete-set subscription re-emits a connection reported putting on the wire while
    /// it was still serving a book it had lost authority over — the resubscribe half of
    /// recovery. A command the control channel refused is not one of these.
    pub resubscribes_emitted: u64,
    /// Resubscription attempts whose window expired without an accepted base, each of which
    /// replaced its connection through the reconnect ladder.
    pub resubscribes_escalated: u64,
    pub recovery_base_unavailable: u64,
    /// Arrivals a pool's publish gate applied to the authoritative book, whichever socket
    /// carried them.
    pub pool_published: u64,
    /// How many of those each pool socket contributed, indexed by socket, with the
    /// publishing slot first. Empty in a configuration that runs no pool.
    pub pool_published_by_socket: Vec<u64>,
    /// Arrivals dropped because another socket had already published that same frame —
    /// what the pool exists to absorb.
    pub pool_dedup_drops: u64,
    /// Arrivals dropped because the book already held newer state: ordinary
    /// cross-connection skew, counted apart from duplicates because it is a different fact.
    pub pool_stale_drops: u64,
    /// The venue key of the arrival the pool published last, which is what ties the book a
    /// run ended on to the venue's own stream. `None` for a run that held no pool or
    /// published nothing through one.
    pub pool_last_published_key: Option<DedupKey>,
    /// Why the pool stopped publishing across its sockets, or `None` if it never did.
    pub pool_degraded: Option<PoolDegradeReason>,
    pub decode_failures: BTreeMap<&'static str, u64>,
}

impl SupervisorStats {
    fn record_failure(&mut self, key: &'static str) {
        *self.decode_failures.entry(key).or_insert(0) += 1;
    }
}

/// What the supervisor tells an attached diagnostic surface. Purely informational: nothing
/// here is required for the book to be correct, and a dropped notice costs only a printed
/// line.
#[derive(Clone, Debug)]
pub enum SupervisorNotice {
    Connected {
        generation: u64,
        replica: ReplicaRole,
        sid: String,
        ping_interval_ms: u64,
        ping_timeout_ms: u64,
        max_payload_bytes: u64,
        subscription_generation: u64,
    },
    Event(LimitlessEvent),
    ContinuityLoss {
        continuity: ContinuityReason,
        authority: AuthorityReason,
    },
    /// The tracked source topology changed: which connection publishes, which shadows it,
    /// and what the standby comparison currently says.
    SourceTransition {
        source: SourceState,
    },
    Fenced {
        generation: u64,
    },
    Reconnecting {
        generation: u64,
        replica: ReplicaRole,
        delay_ms: u64,
    },
    /// A connection still considered healthy was asked to re-emit its complete subscription
    /// set, because the book it serves needs a fresh recovery base.
    Resubscribing {
        generation: u64,
        replica: ReplicaRole,
    },
    RecoveryBaseUnavailable {
        attempts: u32,
    },
    /// One book update this supervisor accepted, reported only while
    /// [`SupervisorConfig::log_dedup_keys`] is set: the record a dedup-key conformance
    /// session reads.
    ///
    /// `position` is this supervisor's own receive counter, shared by both roles and
    /// monotonic across them, so an offline analysis can tell a contiguous record from one
    /// with holes. A hole is not by itself a dropped notice: a position is also spent by an
    /// update that failed provenance, candidate, or apply, which reports no record and is
    /// counted in [`SupervisorStats::decode_failures`]. Trusting the record as complete
    /// means checking [`SupervisorStats::diagnostics_dropped`] is zero.
    ///
    /// `key` is the venue's own key for the frame, absent when the frame carried none, and
    /// an error when the venue's value was not representable exactly. `digest` fingerprints
    /// the content of the book this role published or shadowed after the update; it is
    /// diagnostic evidence, never the replica agreement authority.
    AcceptedUpdate {
        market: String,
        position: u64,
        generation: u64,
        replica: ReplicaRole,
        key: Result<Option<DedupKey>, DedupKeyError>,
        digest: ContentDigest,
    },
    /// The pool withdrew its own licence to publish across sockets, naming the arrival that
    /// withdrew it rather than only the conclusion drawn from it.
    ///
    /// Reported once per run: the licence never comes back in this process. The topology
    /// transition that follows carries what replaced the pool.
    PoolDegraded {
        violation: PoolViolation,
        generation: u64,
    },
}

/// Ends a supervisor run at its next loop iteration, leaving the book at the revision it
/// had reached.
#[derive(Clone, Debug)]
pub struct Stopper {
    signal: Arc<Notify>,
}

impl Stopper {
    pub fn stop(&self) {
        self.signal.notify_one();
    }
}

/// Which connection role a notice, timer, or connection end belongs to.
///
/// [`Self::Standby`] carries the index of the role, because a supervisor may run several:
/// one for every connection beyond the first. The index names the role, not the connection
/// occupying it, so a replaced connection inherits its role's reconnect ladder and its
/// place in the socket order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Slot {
    Primary,
    Standby(usize),
}

impl Slot {
    /// The provenance role a snapshot accepted from this role carries. It follows the role
    /// the connection holds now, not the role it was spawned into: a promoted standby
    /// stamps [`ReplicaRole::PublishingPrimary`] from its first authoritative snapshot.
    ///
    /// This is the structural role. A pool has no standbys — every socket publishes
    /// through one gate — so a pooled supervisor stamps provenance with
    /// [`Supervisor::provenance_role`] instead, while connection announcements keep the
    /// structural role so one socket stays tellable from another.
    fn role(self) -> ReplicaRole {
        match self {
            Self::Primary => ReplicaRole::PublishingPrimary,
            Self::Standby(_) => ReplicaRole::HotStandby,
        }
    }

    /// This role's position in the supervisor's socket order: 0 for the publishing role and
    /// one past its own index for every other. Pool sockets are numbered by it.
    fn index(self) -> usize {
        match self {
            Self::Primary => 0,
            Self::Standby(index) => index + 1,
        }
    }

    /// The role holding socket `index` of the supervisor's socket order.
    fn from_index(index: usize) -> Self {
        match index.checked_sub(1) {
            Some(index) => Self::Standby(index),
            None => Self::Primary,
        }
    }
}

/// What one accepted notice says about where it came from and when: the connection
/// generation that produced it, the subscription generation that connection read it under,
/// and the wall-clock nanosecond its socket read returned (0 for a notice no venue frame
/// drove).
///
/// All three are carried by the notice rather than read from the connection's current
/// state, so a snapshot is stamped with the emit it actually arrived under even when a
/// later emit has since advanced the connection.
#[derive(Clone, Copy, Debug)]
struct ConnectionStamp {
    generation: u64,
    subscription_generation: u64,
    arrival_time_nanos: u64,
}

/// One in-flight attempt to recover a stale book from the connection already serving it,
/// before any connection is replaced.
///
/// The attempt exists only while the book needs a fresh base and this connection is still
/// considered healthy. It is held on the connection itself, so closing the connection
/// discards the attempt with it and no state can outlive the generation it belonged to.
#[derive(Clone, Copy, Debug)]
enum RecoveryAttempt {
    /// The re-emit is due at this instant. Nothing is reserved yet: the pacer slot is taken
    /// when the command is handed over.
    Pending(Instant),
    /// The re-emit has been handed to the connection, which has until this instant to put
    /// it on the wire and say so. Nothing is expected of the venue yet: a command the
    /// daemon has not written cannot be one the venue is declining to answer. The instant
    /// is the pacer slot the command reserved plus the configured window, because the
    /// connection may not write before that slot and escalating a command that was never
    /// allowed out would blame the venue for this daemon's own pacing. Deriving it from the
    /// reservation is what makes it honest at fleet scale: the slot already accounts for
    /// every command queued ahead of this one, where a fixed allowance of one floor assumed
    /// a queue of one.
    ///
    /// It is also the state a handover refused for want of room lands in. The command was
    /// not taken, but the deadline owed is the same one: a connection that has not drained
    /// what is already queued for it, like one that has not written what it took, is a
    /// connection this supervisor stops waiting on at this instant and replaces.
    Requested(Instant),
    /// The re-emit is on the wire, as the connection reported: the venue has until this
    /// instant to serve an accepted base before the connection is replaced.
    Awaiting(Instant),
    /// The connection this attempt was for could not be handed the command because its
    /// control channel is closed, which only a connection whose task is ending does.
    ///
    /// It carries no instant, and so arms no timer: recovery from here is the connection's
    /// own end, which the run loop is already waiting on and which runs the reconnect
    /// ladder. Retrying against a channel whose receiver is gone could only ever fail
    /// again, and a timer that scheduled those retries would be this supervisor polling for
    /// news the join is about to deliver. [`Supervisor::arm_recovery`] leaves an attempt
    /// that already exists alone, so this state is not re-armed while the connection it
    /// belongs to is still being closed, and the connection's end discards it with the
    /// connection.
    Unreachable,
}

impl RecoveryAttempt {
    /// When this attempt next needs the run loop's attention, or `None` for an attempt that
    /// waits on an event rather than a clock.
    fn wake_at(self) -> Option<Instant> {
        match self {
            Self::Pending(at) | Self::Requested(at) | Self::Awaiting(at) => Some(at),
            Self::Unreachable => None,
        }
    }
}

struct ActiveConnection {
    generation: u64,
    spawned_at: Instant,
    handle: JoinHandle<ConnectionEndReason>,
    control: mpsc::Sender<ConnectionControl>,
    heartbeat_deadline: Option<Duration>,
    last_heartbeat: Option<Instant>,
    subscription_generation: u64,
    produced_base: bool,
    recovery: Option<RecoveryAttempt>,
}

impl ActiveConnection {
    /// The instant this connection's liveness evidence expires, or `None` before the venue
    /// has announced a heartbeat cadence. Derived only from Engine.IO ping evidence.
    fn heartbeat_expiry(&self) -> Option<Instant> {
        match (self.last_heartbeat, self.heartbeat_deadline) {
            (Some(at), Some(deadline)) => Some(at + deadline),
            _ => None,
        }
    }

    /// Whether this connection has put its subscription set on the wire. Only an
    /// established connection can be asked to re-emit one, and only an established
    /// connection's silence is evidence about the venue rather than about setup still
    /// running.
    fn is_established(&self) -> bool {
        self.subscription_generation > 0
    }

    /// Whether this connection's transport is viable right now: its task is still running
    /// and its heartbeat evidence has not expired. A connection that has not yet announced
    /// a cadence has no expired evidence and is viable on its task alone.
    fn is_viable(&self, now: Instant) -> bool {
        !self.handle.is_finished() && self.heartbeat_expiry().is_none_or(|expiry| expiry > now)
    }
}

struct FencedConnection {
    handle: JoinHandle<ConnectionEndReason>,
    expires_at: Instant,
}

/// One role's own transport state: the generation currently feeding it, at most one fenced
/// generation still draining, and its own reconnect ladder.
#[derive(Default)]
struct ConnectionSlot {
    active: Option<ActiveConnection>,
    fenced: Option<FencedConnection>,
    reconnect_at: Option<Instant>,
    backoff_attempt: u32,
}

/// The hot standby: its own connection role, the shadow book its accepted snapshots feed,
/// the connection currently assigned to the role, and the last comparison verdict.
///
/// The shadow is a second book only in the bookkeeping sense. It is never published, never
/// read by a consumer, and never added to the authoritative book's depth: the market's
/// liquidity stays one physical book with two views.
struct Standby {
    slot: ConnectionSlot,
    shadow: OrderBook,
    assigned: Option<ConnectionIdentity>,
    state: StandbyState,
}

/// What happens to the authoritative book when the publishing connection is lost.
enum SourceSwitch {
    /// The standby at this index is eligible: it takes over the publishing role and the
    /// book is not touched — no epoch bump, no continuity loss, no staleness, and the
    /// revision simply continues.
    Promote(usize),
    /// A live standby exists but is not eligible. Authority is surrendered with this
    /// reason and a fresh venue base is awaited.
    Refuse(AuthorityReason),
    /// No standby connection to consider; recovery follows the single-source path.
    Absent,
}

enum Wake {
    Finished,
    HeartbeatMissed(Slot),
    FencedExpired(Slot),
    Reconnect,
    RecoveryDue,
    KillPrimary,
    Ended(Slot, ConnectionEndReason),
    Notice(ConnectionNotice),
}

/// The shared-memory segment one supervisor publishes its book into, as both latest state
/// and retained level mutations.
///
/// Publishing is a handful of plain memory stores into a region whose pages were made
/// resident at creation, so it performs no system call, no allocation and no blocking I/O:
/// the update path keeps the daemon's no-blocking-I/O rule, which is what lets the
/// publication sit on the book's own commit path rather than behind a queue.
///
/// A refused publication is latched: nothing more is published into the segment, the run
/// ends, and the error is reported through [`Supervisor::segment_failure`]. A consumer must
/// never be left reading a stale segment that looks live because the writer silently
/// stopped. A refused mutation is fatal for that reason and one more — it would leave a hole
/// in the ring that every attached consumer waits on forever. A ring that is full is not a
/// refusal: it wraps, and the consumer it outran is told so.
///
/// No venue value can produce that refusal. Book depth cannot: a segment sized from
/// [`MAX_BOOK_LEVELS`] carries every book its supervisor can accept. A resolution's
/// venue-native texts are recorded in a vocabulary wider than the fixed cells this ABI
/// stores them in, so they could — but [`resolution_fits_the_ring`] judges them before a
/// stream position is allocated, and a report the cells cannot carry reaches neither lane.
/// A truncated outcome would name a different winner and no consumer could tell, so it is
/// never written; what remains here is the writer's typed refusal as defence in depth
/// against a producer that skipped that judgement.
pub struct BookSegment {
    writer: SegmentWriter,
    handle: MarketHandle,
    failure: Option<WriterError>,
}

impl BookSegment {
    /// Pairs a formatted segment's writer with the handle of the market installed in it.
    ///
    /// `handle` must name the market whose book the supervisor keeps: the segment writer
    /// refuses a publication carrying another market's identity, and that refusal becomes
    /// the supervisor's fatal segment failure.
    pub fn new(writer: SegmentWriter, handle: MarketHandle) -> Self {
        Self {
            writer,
            handle,
            failure: None,
        }
    }

    /// Publishes one commit's mutations in the writer's own order, then the state that
    /// commit produced.
    ///
    /// Mutations go first so no published state ever advertises a position the ring was
    /// not given: a refused mutation leaves the state slot naming that same position as its
    /// next one, which is the honest report of a stream that stopped rather than a cursor
    /// pointing past a hole. Reading a state slot one position behind the ring costs a
    /// consumer nothing — [`crate::EventStream::poll`] compares epochs there, never
    /// positions.
    fn publish_commit(
        &mut self,
        commit: &BookCommit,
        published: &PublishedBook,
        arrival_time_nanos: u64,
    ) {
        if self.failure.is_some() {
            return;
        }
        for record in commit.mutations() {
            if let Err(error) = self.writer.publish_mutation(
                self.handle,
                commit.revision(),
                record.cursor(),
                record.mutation(),
                arrival_time_nanos,
            ) {
                self.failure = Some(error);
                return;
            }
        }
        self.publish_state(published, arrival_time_nanos);
    }

    /// Publishes one revision of latest state, for a change that derived no mutation.
    ///
    /// `arrival_time_nanos` is the wall-clock nanosecond at which the socket read that caused
    /// this change returned, or 0 for a change no venue frame drove.
    fn publish_state(&mut self, published: &PublishedBook, arrival_time_nanos: u64) {
        if self.failure.is_some() {
            return;
        }
        if let Err(error) = self
            .writer
            .publish(self.handle, published, arrival_time_nanos)
        {
            self.failure = Some(error);
        }
    }

    /// Publishes one venue-reported resolution into the ring, then republishes the state that
    /// names the position past it.
    ///
    /// The state slot advances with every delivery this stream carries — mutations and
    /// resolutions alike — because its `(epoch, next_position)` is the boundary every
    /// attachment starts from. A late attacher must start strictly after a delivery already
    /// made, which is exactly what the in-process lane guarantees by publishing state before
    /// it sends ([`BookWriter::publish_resolution`]); both lanes therefore keep one attachment
    /// contract rather than two. A boundary left behind a live ring is worse than untidy: a
    /// resolution-only burst deeper than the ring wraps past the slot every new attachment
    /// probes, and [`crate::SegmentReader::attach_stream`]'s coherence retry then fails
    /// [`crate::ReadFault::Contended`] forever — a permanent attach failure no consumer can be
    /// told about. With the republish that is impossible by construction; a consumer already
    /// attached and lapped by such a burst still gets the ordinary explicit
    /// [`crate::ContinuityReason::Overrun`].
    ///
    /// The republish carries the commit and arrival stamps the book's own last commit made,
    /// byte for byte, rather than fresh ones: a resolution commits no revision, and restamping
    /// would make a book that has not moved look freshly committed to a consumer keying
    /// freshness on those stamps. `SLOT_COMMIT_TIME` keeps meaning when the *revision* was
    /// stamped, and `SLOT_ARRIVAL_TIME` when the frame that produced it arrived.
    ///
    /// The ring is written first, for [`Self::publish_commit`]'s reason: state must never
    /// advertise a position the ring was not given. Either refusal is latched like any other —
    /// a hole in the ring is a position every attached consumer would wait on forever. No
    /// venue value can produce the ring's refusal: [`resolution_fits_the_ring`] judges
    /// representability before a position is allocated, leaving the writer's typed refusals as
    /// defence in depth.
    fn publish_resolution(
        &mut self,
        delivery: &ResolutionDelivery,
        published: &PublishedBook,
        arrival_time_nanos: u64,
    ) {
        if self.failure.is_some() {
            return;
        }
        if let Err(error) = self.writer.publish_resolution(
            self.handle,
            delivery.revision(),
            delivery.cursor(),
            delivery.resolution(),
            arrival_time_nanos,
        ) {
            self.failure = Some(error);
            return;
        }
        if let Err(error) = self
            .writer
            .republish_carrying_stamps(self.handle, published)
        {
            self.failure = Some(error);
        }
    }
}

/// The one writer for one market's book, and the owner of its connection generations.
pub struct Supervisor {
    config: SupervisorConfig,
    market: MarketRef,
    level_capacity: LevelCapacity,
    writer: BookWriter,
    segment: Option<BookSegment>,
    latest_resolution: Option<Arc<MarketResolution>>,
    notices_tx: mpsc::Sender<ConnectionNotice>,
    notices_rx: mpsc::Receiver<ConnectionNotice>,
    frames: Arc<AtomicU64>,
    stop: Arc<Notify>,
    tap: Option<mpsc::Sender<SupervisorNotice>>,
    primary: ConnectionSlot,
    standbys: Vec<Standby>,
    pool: Option<PoolGate>,
    source: SourceState,
    next_generation: u64,
    recovery_attempts: u32,
    base_accepted: bool,
    unavailable_reported: bool,
    position: u64,
    run_start: Instant,
    kill_primary_after: Option<Duration>,
    kill_primary_at: Option<Instant>,
    jitter: RandomState,
    stats: SupervisorStats,
}

impl Supervisor {
    /// Builds a supervisor for one market's book, with no connection yet running.
    ///
    /// Fails when the market slug is not a valid venue-native identifier, when a capacity
    /// is out of range, when `replicas` is outside the range its topology permits, when a
    /// pooled configuration asks to publish across sockets on a venue key that orders
    /// nothing within a connection-session, or when `max_recovery_attempts` is zero, which
    /// would report recovery exhaustion before any recovery was attempted.
    ///
    /// The two configured pacing figures are bounded above by structure rather than by
    /// anything a venue places: `daily_attempt_budget` at [`WORST_CASE_ATTEMPT_CEILING`],
    /// which is what the attempt ledger's storage is sized against, and
    /// `min_command_interval_ms` at [`MAX_COMMAND_INTERVAL_MS`], past which every recovery
    /// deadline built on top of the floor would outlast any answer a venue could give. An
    /// embedded caller reaches these refusals here; a daemon document reaches the same two
    /// bounds in `crate::daemon::DaemonConfig::parse`, which names the field and the bound
    /// in its message.
    pub fn new(config: SupervisorConfig) -> Result<Self, SupervisorError> {
        if config.max_recovery_attempts == 0 {
            return Err(SupervisorError::RecoveryAttemptsZero);
        }
        if config.daily_attempt_budget > WORST_CASE_ATTEMPT_CEILING {
            return Err(SupervisorError::DailyAttemptBudgetTooLarge);
        }
        if config.min_command_interval_ms > MAX_COMMAND_INTERVAL_MS {
            return Err(SupervisorError::CommandIntervalTooLarge);
        }
        if config.ingest_capacity == 0 {
            return Err(SupervisorError::IngestCapacityZero);
        }
        if config.pooled {
            if config.replicas > crate::MAX_POOL_SOCKETS {
                return Err(SupervisorError::Pool(PoolError::SocketCountOutOfRange));
            }
        } else if config.replicas == 0 || config.replicas > MAX_REPLICAS {
            return Err(SupervisorError::ReplicasOutOfRange);
        }
        let pool = config
            .pooled
            .then(|| PoolGate::new(ORDERBOOK_UPDATE_DEDUP_KEY, config.replicas))
            .transpose()
            .map_err(SupervisorError::Pool)?;
        let market = MarketRef::new(
            Venue::new(VENUE).map_err(SupervisorError::Market)?,
            NativeMarketKey::new(NativeIdentifierKind::slug(), config.market.as_str())
                .map_err(SupervisorError::Market)?,
        );
        let level_capacity =
            LevelCapacity::new(config.level_capacity).map_err(SupervisorError::LevelCapacity)?;
        let observer_capacity = ObserverCapacity::new(config.observer_capacity)
            .map_err(SupervisorError::ObserverCapacity)?;
        let writer = BookWriter::new(OrderBook::new(market.clone()), observer_capacity);
        let (notices_tx, notices_rx) = mpsc::channel(config.ingest_capacity);
        let config_kill_primary_after = config.kill_primary_after;
        let standbys = (1..config.replicas)
            .map(|_| Standby {
                slot: ConnectionSlot::default(),
                shadow: OrderBook::new(market.clone()),
                assigned: None,
                state: StandbyState::Divergent(DivergenceReason::ContinuityMismatch),
            })
            .collect();
        Ok(Self {
            config,
            market,
            level_capacity,
            writer,
            segment: None,
            latest_resolution: None,
            notices_tx,
            notices_rx,
            frames: Arc::new(AtomicU64::new(0)),
            stop: Arc::new(Notify::new()),
            tap: None,
            primary: ConnectionSlot::default(),
            standbys,
            pool,
            source: SourceState::no_source(),
            next_generation: 1,
            recovery_attempts: 0,
            base_accepted: false,
            unavailable_reported: false,
            position: 0,
            run_start: Instant::now(),
            kill_primary_after: config_kill_primary_after,
            kill_primary_at: None,
            jitter: RandomState::new(),
            stats: SupervisorStats::default(),
        })
    }

    /// Attaches a consumer to the book. Attaching before the run starts guarantees the
    /// consumer sees every revision this supervisor ever publishes.
    ///
    /// There is one book to attach to whatever `replicas` is: a standby's shadow state has
    /// no consumer surface and is never observable.
    pub fn attach(&self) -> BookObserver {
        self.writer.attach()
    }

    /// Publishes this supervisor's book into `segment` from now on, on the book's own
    /// ordered commit path.
    ///
    /// The current state is published immediately, so a market that never receives an
    /// update still exposes a readable revision rather than an unpublished slot. Every
    /// later change publishes synchronously with the commit that made it: a commit's
    /// mutations in the writer's own order and then the state they produced, and a change
    /// that derives no mutation — an evidence-based loss — as state alone.
    ///
    /// Publication is therefore not a consumer of the book and cannot be overtaken by one.
    /// The segment's ring receives exactly the book's mutation sequence, so the only
    /// continuity a shared-memory consumer can lose is its own — being lapped by the ring —
    /// or the book's own rebase onto a new epoch, both of which the segment reports
    /// explicitly. A silent gap is not a state this can reach.
    ///
    /// A supervisor installs at most one segment for its whole lifetime. A second call is
    /// refused with [`WriterError::SegmentAlreadyInstalled`] and never touches the segment
    /// already installed, whether that segment is still publishing or has latched a
    /// failure: installing a second segment over the first would strand every consumer
    /// attached to it, silently, on a ring that stopped advancing — the exact hang this
    /// method exists to prevent. A latched failure is preserved for the life of the
    /// supervisor, and [`Self::segment_failure`] keeps reporting it.
    ///
    /// Otherwise fails with whatever refused the initial publication, leaving no segment
    /// installed and this supervisor publishing to none. A refusal during the run is
    /// reported by [`Self::segment_failure`] instead, and ends the run.
    pub fn publish_into(&mut self, mut segment: BookSegment) -> Result<(), WriterError> {
        if self.segment.is_some() {
            return Err(WriterError::SegmentAlreadyInstalled);
        }
        let published = self.writer.published();
        segment.publish_state(&published, 0);
        if let Some(error) = segment.failure.take() {
            return Err(error);
        }
        self.segment = Some(segment);
        Ok(())
    }

    /// The shared-memory publication failure that ended this run, if one did.
    ///
    /// `None` for a run that published into no segment, and for one that published every
    /// commit into its segment. The failure is latched — nothing further is published and
    /// the run ends at the next turn of its loop — so the reported value is stable once
    /// set, and a caller that finds one must treat the run as failed rather than report a
    /// clean end over a segment that stopped advancing.
    pub fn segment_failure(&self) -> Option<&WriterError> {
        self.segment
            .as_ref()
            .and_then(|segment| segment.failure.as_ref())
    }

    /// A handle that ends a run early. Independent of the run deadline.
    pub fn stopper(&self) -> Stopper {
        Stopper {
            signal: Arc::clone(&self.stop),
        }
    }

    /// Routes accepted venue events, connection lifecycle notices, and source-topology
    /// transitions to a diagnostic surface.
    ///
    /// `tap` is bounded and drop-newest: a diagnostic consumer that cannot keep up loses
    /// lines, counted in [`SupervisorStats::diagnostics_dropped`], and never delays
    /// ingestion or the book.
    pub fn with_diagnostics(mut self, tap: mpsc::Sender<SupervisorNotice>) -> Self {
        self.tap = Some(tap);
        self
    }

    /// Detaches the diagnostic surface, ending its stream without releasing the book.
    ///
    /// A diagnostic consumer stops when every sender is gone, so this is what lets one
    /// drain and print a final book state while the writer is still alive to be read.
    pub fn close_diagnostics(&mut self) {
        self.tap = None;
    }

    /// Runs until `deadline` or until a [`Stopper`] fires, keeping the book alive across
    /// connection failures, and returns what the run observed.
    ///
    /// Losing the publishing connection with no eligible standby reports book continuity
    /// loss with its own reason, closes that generation so its late work is ineligible,
    /// backs off, and connects afresh; the first snapshot accepted after a loss becomes a
    /// recovery base in a new continuity epoch, and the book returns to
    /// [`crate::AuthorityState::Live`]. Losing it with an eligible standby switches source
    /// instead and never touches the book. Silence never does any of this.
    ///
    /// Recovery is resubscribe then reconnect. Whenever the book is stale and an established
    /// connection is still serving it — after a local loss, and after a refused promotion
    /// handed the surviving connection over as the recovery source — that connection is
    /// asked once to re-emit its complete subscription set, and only a window with no
    /// accepted base escalates to replacing it. A connection that is already gone skips
    /// straight to the reconnect, which carries its own subscription.
    ///
    /// The wait is ordered: deadlines and timers are polled before the notice queue, so a
    /// flood of market data can never starve liveness detection or a scheduled reconnect,
    /// and the notice queue is polled before a connection's own end, so everything a
    /// generation produced while it was still current is applied before it is closed. Both
    /// roles' timers keep that same precedence.
    ///
    /// A refused shared-memory publication is the one local failure that ends the run
    /// early: the loop stops at the end of the turn that hit it and
    /// [`Self::segment_failure`] names it, because a segment that stopped advancing while
    /// the daemon kept running would look live to every consumer attached to it.
    pub async fn run_until(&mut self, deadline: Instant) -> SupervisorStats {
        let stop = Arc::clone(&self.stop);
        loop {
            if Instant::now() >= deadline {
                break;
            }
            for slot in self.slots() {
                self.spawn_due(slot);
            }
            self.arm_recovery();
            let recovery = self
                .primary
                .active
                .as_ref()
                .and_then(|active| active.recovery)
                .and_then(RecoveryAttempt::wake_at);
            let primary_heartbeat = self
                .primary
                .active
                .as_ref()
                .and_then(ActiveConnection::heartbeat_expiry);
            let standby_heartbeat = self.earliest_standby(|slot| {
                slot.active
                    .as_ref()
                    .and_then(ActiveConnection::heartbeat_expiry)
            });
            let primary_fenced = self.primary.fenced.as_ref().map(|fenced| fenced.expires_at);
            let standby_fenced =
                self.earliest_standby(|slot| slot.fenced.as_ref().map(|fenced| fenced.expires_at));
            let kill_primary = self.kill_primary_at;
            let primary_reconnect = self.primary.reconnect_at;
            let standby_reconnect = self.earliest_standby(|slot| slot.reconnect_at);
            let wake = tokio::select! {
                biased;
                () = tokio::time::sleep_until(deadline) => Wake::Finished,
                () = stop.notified() => Wake::Finished,
                () = sleep_until_opt(primary_heartbeat) => Wake::HeartbeatMissed(Slot::Primary),
                () = sleep_until_opt(deadline_of(standby_heartbeat)) => {
                    Wake::HeartbeatMissed(standby_slot(standby_heartbeat))
                }
                () = sleep_until_opt(primary_fenced) => Wake::FencedExpired(Slot::Primary),
                () = sleep_until_opt(deadline_of(standby_fenced)) => {
                    Wake::FencedExpired(standby_slot(standby_fenced))
                }
                () = sleep_until_opt(primary_reconnect) => Wake::Reconnect,
                () = sleep_until_opt(deadline_of(standby_reconnect)) => Wake::Reconnect,
                () = sleep_until_opt(recovery) => Wake::RecoveryDue,
                () = sleep_until_opt(kill_primary) => Wake::KillPrimary,
                notice = self.notices_rx.recv() => match notice {
                    Some(notice) => Wake::Notice(notice),
                    None => Wake::Finished,
                },
                reason = join_slot(&mut self.primary.active) => Wake::Ended(Slot::Primary, reason),
                ended = join_any_standby(&mut self.standbys) => {
                    Wake::Ended(Slot::Standby(ended.0), ended.1)
                }
            };
            match wake {
                Wake::Finished => break,
                Wake::HeartbeatMissed(slot) => self.on_heartbeat_missed(slot),
                Wake::FencedExpired(slot) => self.release_fenced(slot),
                Wake::Reconnect => {}
                Wake::RecoveryDue => self.on_recovery_due(),
                Wake::KillPrimary => self.kill_primary(),
                Wake::Ended(slot, reason) => {
                    let ended = self.slot_mut(slot).and_then(|slot| slot.active.take());
                    let produced_base = ended.as_ref().is_some_and(|active| active.produced_base);
                    let established = ended.as_ref().is_some_and(ActiveConnection::is_established);
                    let lifetime = ended.map_or(Duration::ZERO, |active| {
                        Instant::now().saturating_duration_since(active.spawned_at)
                    });
                    self.on_slot_ended(slot, reason, produced_base, established, lifetime);
                }
                Wake::Notice(notice) => self.on_notice(notice),
            }
            if self.segment_failure().is_some() {
                break;
            }
        }
        self.finish()
    }

    /// The earliest deadline `deadline` reports across the standby roles, and which role
    /// it belongs to.
    ///
    /// Several roles hold the same kinds of timer while one wait arm can carry only one
    /// deadline. Waking on the earliest and re-deciding is exactly what one role per arm
    /// did: every judgement that follows a wake re-reads the role's own state rather than
    /// trusting the wake, so a role whose deadline was not the earliest is simply woken on
    /// a later iteration.
    fn earliest_standby(
        &self,
        deadline: impl Fn(&ConnectionSlot) -> Option<Instant>,
    ) -> Option<(Instant, usize)> {
        self.standbys
            .iter()
            .enumerate()
            .filter_map(|(index, standby)| deadline(&standby.slot).map(|at| (at, index)))
            .min_by_key(|(at, _)| *at)
    }

    fn slot(&self, slot: Slot) -> Option<&ConnectionSlot> {
        match slot {
            Slot::Primary => Some(&self.primary),
            Slot::Standby(index) => self.standbys.get(index).map(|standby| &standby.slot),
        }
    }

    fn slot_mut(&mut self, slot: Slot) -> Option<&mut ConnectionSlot> {
        match slot {
            Slot::Primary => Some(&mut self.primary),
            Slot::Standby(index) => self
                .standbys
                .get_mut(index)
                .map(|standby| &mut standby.slot),
        }
    }

    /// Every connection role this supervisor runs, publishing role first, which is also the
    /// socket order a pool numbers by.
    fn slots(&self) -> impl Iterator<Item = Slot> + use<> {
        let standbys = self.standbys.len();
        core::iter::once(Slot::Primary).chain((0..standbys).map(Slot::Standby))
    }

    fn slot_of(&self, generation: u64) -> Option<Slot> {
        self.slots().find(|slot| {
            self.slot(*slot)
                .and_then(|slot| slot.active.as_ref())
                .is_some_and(|active| active.generation == generation)
        })
    }

    /// Whether this supervisor is publishing across a socket pool right now, which is true
    /// only while a pool is configured and its licence still stands.
    fn pool_armed(&self) -> bool {
        self.pool.as_ref().is_some_and(PoolGate::is_armed)
    }

    /// The index of the first standby role holding a connection that can still deliver.
    fn first_viable_standby(&self) -> Option<usize> {
        let now = Instant::now();
        self.standbys.iter().position(|standby| {
            standby
                .slot
                .active
                .as_ref()
                .is_some_and(|active| active.is_viable(now))
        })
    }

    /// Starts a connection for `slot` when the role is empty, its backoff has elapsed, the
    /// shared command permit is free, and the configured daily attempt budget has room.
    ///
    /// Generations come from one counter shared by both roles, so no two live connections
    /// of one supervisor ever share a generation and a notice identifies its role by that
    /// number alone.
    ///
    /// A connection emits its subscription as part of establishing itself, and that command
    /// is paced where it is written rather than where it is authorized: the connection task
    /// waits at the configured command floor immediately before the bytes go out.
    /// Spawning is therefore free of pacing, and two connections dialling together produce
    /// two subscriptions the floor apart however their dials and handshakes converge. A
    /// spawn the daily attempt ledger holds back is rescheduled, never abandoned, and never
    /// counted as an attempt.
    fn spawn_due(&mut self, slot: Slot) {
        let due = match self.slot(slot) {
            Some(target) => {
                target.active.is_none() && target.reconnect_at.is_none_or(|at| at <= Instant::now())
            }
            None => false,
        };
        if !due {
            return;
        }
        let now = Instant::now();
        if let Err(free_at) = admit_attempt(now, self.config.daily_attempt_budget) {
            self.stats.attempts_clamped = self.stats.attempts_clamped.saturating_add(1);
            if let Some(target) = self.slot_mut(slot) {
                target.reconnect_at = Some(free_at);
            }
            return;
        }
        let generation = self.next_generation;
        self.next_generation = self.next_generation.saturating_add(1);
        self.stats.connection_attempts = self.stats.connection_attempts.saturating_add(1);
        let config = ConnectionConfig {
            endpoint: self.config.endpoint.clone(),
            markets: vec![self.config.market.clone()],
            setup_timeout: self.config.setup_timeout,
            capture_path: self.config.capture_path.clone(),
            min_command_interval: Duration::from_millis(self.config.min_command_interval_ms),
        };
        let (control, commands) = mpsc::channel(1);
        let handle = tokio::spawn(run_connection(
            generation,
            config,
            self.notices_tx.clone(),
            Arc::clone(&self.frames),
            commands,
        ));
        let active = ActiveConnection {
            generation,
            spawned_at: Instant::now(),
            handle,
            control,
            heartbeat_deadline: None,
            last_heartbeat: None,
            subscription_generation: 0,
            produced_base: false,
            recovery: None,
        };
        if let Some(target) = self.slot_mut(slot) {
            target.active = Some(active);
            target.reconnect_at = None;
        }
        if let Slot::Standby(index) = slot
            && let Some(standby) = self.standbys.get_mut(index)
        {
            standby.assigned = ConnectionIdentity::new(CONNECTION_NAME, generation).ok();
        }
        if let Some(gate) = self.pool.as_mut() {
            gate.reset_socket(slot.index());
        }
        self.resync();
    }

    /// Arms, holds, or discards the resubscription attempt for the publishing connection.
    ///
    /// The attempt exists exactly while the authoritative book is stale and an established
    /// primary connection is still serving it: that is the state in which the venue owes a
    /// fresh base and this daemon still holds a healthy rail to ask on. A live book, a book
    /// that has not yet held authority, and a role with no established connection all clear
    /// it, so a quiet live market is never asked for anything and no timer over market
    /// silence exists.
    ///
    /// An armed attempt is never re-armed, and one recovery attempt puts exactly one command
    /// on the wire. The configured command floor is enforced where that command is
    /// written, inside the connection task, so nothing here has to reason about how many
    /// other connections are emitting: the attempt is armed as soon as the book needs one,
    /// and the wire spaces it.
    ///
    /// A base is not gated on which subscription generation produced it. The venue's
    /// `orderbookUpdate` is a self-contained complete book, and `docs/design.md` requires a
    /// complete authoritative snapshot as the recovery base, not a post-re-emit one: an
    /// in-flight complete snapshot arriving on a healthy subscribed connection truthfully
    /// ends recovery, and spends no further venue command to obtain what the venue has
    /// already sent. What the emit is for is the case where nothing is in flight.
    fn arm_recovery(&mut self) {
        let needed = matches!(self.writer.book().authority(), AuthorityState::Stale(_));
        let due = Instant::now();
        let Some(active) = self.primary.active.as_mut() else {
            return;
        };
        if !needed || !active.is_established() {
            active.recovery = None;
            return;
        }
        if active.recovery.is_some() {
            return;
        }
        active.recovery = Some(RecoveryAttempt::Pending(due));
    }

    /// Acts on a due resubscription attempt, against everything already queued.
    ///
    /// The recovery timer is polled ahead of the notice queue, so a base that would end the
    /// attempt may still be waiting in it. This applies what the queue already holds —
    /// bounded by its capacity, which is all it can hold — and re-decides before acting, so
    /// a connection that has already served its base is neither commanded nor replaced.
    ///
    /// A due [`RecoveryAttempt::Pending`] hands one complete-set re-emit to the connection.
    /// A due [`RecoveryAttempt::Awaiting`] is the venue declining to serve a base on an
    /// otherwise healthy connection, and a due [`RecoveryAttempt::Requested`] is the
    /// connection failing to put the command on the wire at all; both escalate to the
    /// existing reconnect ladder, spending this generation as one ordinary failed recovery
    /// attempt rather than running a ladder of its own.
    fn on_recovery_due(&mut self) {
        for _ in 0..self.config.ingest_capacity {
            match self.notices_rx.try_recv() {
                Ok(notice) => self.on_notice(notice),
                Err(_) => break,
            }
        }
        self.arm_recovery();
        let now = Instant::now();
        let due = self
            .primary
            .active
            .as_ref()
            .and_then(|active| active.recovery)
            .filter(|attempt| attempt.wake_at().is_some_and(|at| at <= now));
        match due {
            Some(RecoveryAttempt::Pending(_)) => self.emit_resubscribe(),
            Some(RecoveryAttempt::Requested(_) | RecoveryAttempt::Awaiting(_)) => {
                self.escalate_recovery();
            }
            Some(RecoveryAttempt::Unreachable) | None => {}
        }
    }

    /// Hands the publishing connection a complete-set re-emit, and starts the bounded wait
    /// for it to reach the wire.
    ///
    /// The command is offered, never awaited: the connection owns the write half and a
    /// supervisor that blocked on it would stall every other book event. A handover that
    /// does not land is not an emit — nothing has been asked of the venue, so no response
    /// window starts and no attempt is spent — and what happens next is decided by *why* it
    /// did not land, because the two answers are not the same failure.
    ///
    /// A closed channel is a connection whose task is ending: its receiver lives for exactly
    /// as long as the task does. Nothing is scheduled — the attempt becomes
    /// [`RecoveryAttempt::Unreachable`], which carries no instant — because the connection's
    /// own end is already a wake this loop is waiting on, and it runs the reconnect ladder.
    /// A timer here would be this supervisor polling for news the join is about to deliver.
    ///
    /// A full channel is a connection that is alive but has not consumed a command already
    /// queued for it. It cannot arise from this state machine, which emits from
    /// [`RecoveryAttempt::Pending`] alone and leaves it on a landed handover, and whose only
    /// route back to `Pending` — [`Self::escalate_recovery`] — takes the connection and its
    /// channel with it; but if it ever did, the honest reading is a connection that cannot
    /// take work. The attempt is given the same absence deadline a landed handover gets, so
    /// a connection that never drains is replaced by the escalation path already built for
    /// one that never writes. No new timer either way.
    ///
    /// The venue's answering window opens only when the connection reports the command on
    /// the wire, so neither a delayed write nor the wait at the configured command floor
    /// can consume the window it was supposed to start.
    ///
    /// The command takes its place in this endpoint's process-wide command queue here, at
    /// the moment it is decided on, and carries the granted instant to the connection. The
    /// deadline for the write runs from that instant, because it is the earliest the bytes
    /// may leave: a fleet whose connections all recover at once forms one queue, and a
    /// deadline that allowed one interval regardless of position would expire on every
    /// connection standing further back than second while its command was still waiting for
    /// this daemon's own pacing — fencing a healthy connection, and sending its replacement
    /// to the back of the same queue.
    fn emit_resubscribe(&mut self) {
        let window = self.config.resubscribe_window;
        let interval = Duration::from_millis(self.config.min_command_interval_ms);
        let endpoint = self.config.endpoint.as_str();
        let now = Instant::now();
        let Some(active) = self.primary.active.as_mut() else {
            return;
        };
        let generation = active.generation;
        let granted_at = reserve_command_grant(endpoint, now, interval);
        let handover = active
            .control
            .try_send(ConnectionControl::Resubscribe { granted_at });
        let accepted = handover.is_ok();
        active.recovery = Some(match handover {
            Ok(()) => RecoveryAttempt::Requested(granted_at + window),
            Err(mpsc::error::TrySendError::Full(_)) => {
                RecoveryAttempt::Requested(granted_at + window)
            }
            Err(mpsc::error::TrySendError::Closed(_)) => RecoveryAttempt::Unreachable,
        });
        if !accepted {
            return;
        }
        self.tap(SupervisorNotice::Resubscribing {
            generation,
            replica: Slot::Primary.role(),
        });
    }

    /// Gives up on recovering through the connection already serving the book and replaces
    /// it, which is the reconnect half of resubscribe-then-reconnect.
    ///
    /// The generation is fenced first, so nothing it produces after this instant can reach
    /// the book, and is then closed through the ordinary end path holding no base: it
    /// advances the same backoff ladder and the same recovery-attempt count an ordinary
    /// failed generation does.
    fn escalate_recovery(&mut self) {
        let Some(active) = self.primary.active.take() else {
            return;
        };
        let lifetime = Instant::now().saturating_duration_since(active.spawned_at);
        let established = active.is_established();
        self.stats.resubscribes_escalated = self.stats.resubscribes_escalated.saturating_add(1);
        self.fence(Slot::Primary, active);
        self.on_slot_ended(
            Slot::Primary,
            ConnectionEndReason::ResubscribeTimedOut,
            false,
            established,
            lifetime,
        );
    }

    /// Accepts a notice only from a generation this supervisor currently runs.
    ///
    /// Anything else is late work from a closed generation: it is discarded without
    /// touching either book, and a discarded venue event is counted as evidence that
    /// fencing did its job.
    fn on_notice(&mut self, notice: ConnectionNotice) {
        let Some(slot) = self.slot_of(notice.generation) else {
            if matches!(notice.note, ConnectionNote::Event { .. }) {
                self.stats.fenced_events = self.stats.fenced_events.saturating_add(1);
            }
            return;
        };
        let generation = match self.slot(slot).and_then(|slot| slot.active.as_ref()) {
            Some(active) => active.generation,
            None => return,
        };
        match notice.note {
            ConnectionNote::Ready {
                open,
                subscription_generation,
                observed_at,
            } => {
                if let Some(active) = self.active_mut(slot) {
                    active.heartbeat_deadline = Some(open.heartbeat_deadline());
                    active.last_heartbeat = Some(observed_at);
                    active.subscription_generation = subscription_generation;
                }
                if let Some(after) = self.kill_primary_after.take() {
                    self.kill_primary_at = Some(Instant::now() + after);
                }
                self.tap(SupervisorNotice::Connected {
                    generation,
                    replica: slot.role(),
                    sid: open.sid().to_owned(),
                    ping_interval_ms: open.ping_interval_ms(),
                    ping_timeout_ms: open.ping_timeout_ms(),
                    max_payload_bytes: open.max_payload_bytes(),
                    subscription_generation,
                });
                self.resync();
            }
            ConnectionNote::Heartbeat { observed_at } => {
                if let Some(active) = self.active_mut(slot) {
                    active.last_heartbeat = Some(observed_at);
                }
            }
            ConnectionNote::Resubscribed {
                subscription_generation,
            } => {
                let window = self.config.resubscribe_window;
                let now = Instant::now();
                let mut on_wire = false;
                if let Some(active) = self.active_mut(slot) {
                    active.subscription_generation = subscription_generation;
                    if matches!(active.recovery, Some(RecoveryAttempt::Requested(_))) {
                        active.recovery = Some(RecoveryAttempt::Awaiting(now + window));
                        on_wire = true;
                    }
                }
                if on_wire {
                    self.stats.resubscribes_emitted =
                        self.stats.resubscribes_emitted.saturating_add(1);
                }
            }
            ConnectionNote::Event {
                event,
                received_at,
                arrival_time_nanos,
                subscription_generation,
            } => {
                let stamp = ConnectionStamp {
                    generation,
                    subscription_generation,
                    arrival_time_nanos,
                };
                self.on_event(slot, event, received_at, stamp);
            }
            ConnectionNote::DecodeFailure { key, book_relevant } => {
                self.stats.record_failure(key);
                if book_relevant {
                    self.report_slot_loss(
                        slot,
                        ContinuityReason::LocalLoss,
                        AuthorityReason::LocalLoss,
                    );
                }
            }
            ConnectionNote::Overload { dropped } => {
                self.stats.overload_drops = self.stats.overload_drops.saturating_add(dropped);
                self.report_slot_loss(slot, ContinuityReason::LocalLoss, AuthorityReason::Overload);
            }
        }
    }

    fn active_mut(&mut self, slot: Slot) -> Option<&mut ActiveConnection> {
        self.slot_mut(slot).and_then(|slot| slot.active.as_mut())
    }

    fn on_event(
        &mut self,
        slot: Slot,
        event: LimitlessEvent,
        received_at: Instant,
        stamp: ConnectionStamp,
    ) {
        match &event {
            LimitlessEvent::OrderbookUpdate(update) => {
                self.stats.events_orderbook = self.stats.events_orderbook.saturating_add(1);
                if update.market_slug() == self.config.market {
                    self.apply_update(slot, update, received_at, stamp);
                }
            }
            LimitlessEvent::MarketResolved(resolved) => {
                self.stats.events_resolved = self.stats.events_resolved.saturating_add(1);
                if slot == Slot::Primary && resolved.slug() == self.config.market {
                    self.forward_resolution(resolved, received_at, stamp);
                }
            }
            LimitlessEvent::Unknown { .. } => {
                self.stats.events_unknown = self.stats.events_unknown.saturating_add(1);
            }
        }
        self.tap(SupervisorNotice::Event(event));
    }

    /// Applies one venue book snapshot to the book its role feeds: the sole writer for
    /// [`Slot::Primary`], the shadow for [`Slot::Standby`], and whichever the pool's key
    /// gate selects while a pool publishes.
    ///
    /// The arrival that withdraws a pool's licence reaches no book. Two of the three ways
    /// that happens are the venue contradicting the basis the pool publishes on, and the
    /// frame carrying that contradiction is the last one to trust; the third is a frame
    /// whose key the gate cannot read, which the topology being handed over to has no
    /// established rule for either. One frame is lost at that instant and every later
    /// arrival is applied under the topology the degrade installed.
    ///
    /// A rejected candidate or apply is counted and reported as local loss on that role's
    /// own book, because a rejected update for the subscribed market is a book update this
    /// supervisor failed to apply; it never ends the run and never closes the connection. A
    /// shadow rejection therefore costs the standby its agreement, never the primary its
    /// authority.
    fn apply_update(
        &mut self,
        slot: Slot,
        update: &OrderbookUpdate,
        received_at: Instant,
        stamp: ConnectionStamp,
    ) {
        let Some(position) = self.position.checked_add(1) else {
            self.stats.record_failure("book:PositionCounterOverflow");
            self.report_slot_loss(
                slot,
                ContinuityReason::LocalLoss,
                AuthorityReason::LocalLoss,
            );
            return;
        };
        self.position = position;
        let replica = self.provenance_role(slot);
        let provenance = match self.build_provenance(update, position, received_at, stamp, replica)
        {
            Ok(provenance) => provenance,
            Err(_) => {
                self.stats.record_failure("book:InvalidProvenance");
                self.report_slot_loss(
                    slot,
                    ContinuityReason::LocalLoss,
                    AuthorityReason::LocalLoss,
                );
                return;
            }
        };
        let candidate = match update.snapshot_candidate(provenance, self.level_capacity) {
            Ok(candidate) => candidate,
            Err(_) => {
                self.stats.record_failure("book:InvalidCandidate");
                self.report_slot_loss(
                    slot,
                    ContinuityReason::LocalLoss,
                    AuthorityReason::LocalLoss,
                );
                return;
            }
        };
        if self.pool_armed() {
            let key = update.dedup_key().ok().flatten();
            let projection = candidate_projection(&candidate);
            let redelivered = match (self.pool.as_ref(), key.as_ref()) {
                (Some(gate), Some(key)) => gate.evidence_matches(key, &projection),
                _ => false,
            };
            let recovering = !self.authority_is_live();
            let verdict = self.pool.as_mut().map_or(PoolVerdict::Disarmed, |gate| {
                gate.admit(slot.index(), key.as_ref(), projection)
            });
            match verdict {
                PoolVerdict::Publish => {
                    self.publish_pooled(slot, &candidate, key, update, position, stamp);
                    return;
                }
                PoolVerdict::Duplicate if redelivered && recovering => {
                    self.recover_pooled(slot, &candidate, update, position, stamp);
                    return;
                }
                PoolVerdict::Duplicate | PoolVerdict::Stale => {
                    self.shadow_pooled(slot, &candidate, update, position, stamp);
                    return;
                }
                PoolVerdict::Degrade(violation) => {
                    self.degrade_pool(violation, stamp.generation);
                    return;
                }
                PoolVerdict::Disarmed => {}
            }
        }
        match slot {
            Slot::Primary => {
                if self.apply_authoritative(&candidate, stamp.arrival_time_nanos) {
                    self.on_base_accepted(slot, true);
                    self.resync();
                    self.report_accepted_update(slot, update, position, stamp, true);
                }
            }
            Slot::Standby(index) => {
                if self.apply_shadow(index, &candidate) {
                    self.on_base_accepted(slot, false);
                    self.resync();
                    self.report_accepted_update(slot, update, position, stamp, false);
                }
            }
        }
    }

    /// The replica role one accepted arrival's provenance carries.
    ///
    /// In an armed pool every socket is a publishing source — the key gate, not a role
    /// assignment, decides which arrival reaches the book — so a pooled arrival is stamped
    /// [`ReplicaRole::PublishingPrimary`] whichever socket carried it. Without a pool the
    /// structural role stands, and a promoted standby stamps the publishing role from its
    /// first authoritative snapshot exactly as it did before.
    fn provenance_role(&self, slot: Slot) -> ReplicaRole {
        if self.pool_armed() {
            ReplicaRole::PublishingPrimary
        } else {
            slot.role()
        }
    }

    /// Forwards one venue-reported resolution for this supervisor's market onto the
    /// consumer lanes, without touching the book.
    ///
    /// Only an arrival on [`Slot::Primary`] is forwarded. That is the same
    /// one-publishing-primary model the book itself follows: a standby holds a second copy
    /// of the venue's stream and publishes nothing from it, so its copy of the same
    /// resolution is counted and tapped like every other standby arrival and reaches no
    /// consumer. Nothing here deduplicates by content either — `docs/limitless.md` records
    /// this venue delivering one resolution as several byte-identical frames, and
    /// reproducing what the venue reported means forwarding each of them rather than
    /// deciding which was the real one.
    ///
    /// The resolution is retained on this supervisor whatever the book is doing and whatever
    /// the lanes can carry, so [`Self::latest_resolution`] answers even while consumers are
    /// under an explicit continuity loss: what the venue reported is true whether or not this
    /// daemon's surfaces can reproduce it. Delivery is what depends on the stream: a lost
    /// stream has no position to allocate, so no lane receives it and no consumer is told
    /// anything new — they already hold a typed loss and must reattach.
    ///
    /// While a segment is installed, delivery also depends on the shared-memory ring being
    /// able to carry the venue's own texts verbatim, judged by [`resolution_fits_the_ring`]
    /// before any position is allocated. A report the ring cannot carry is counted under
    /// `resolution:Unrepresentable` and reaches neither lane, so the two stay in step and no
    /// stream position is consumed: the next mutation takes the position this resolution would
    /// have had. Judging it after allocation would deliver it in process, leave the ring an
    /// unwritten slot every attached consumer polls forever, and end the run on a value the
    /// venue chose.
    ///
    /// A run publishing into no segment has no second lane to keep in step, and its
    /// deliveries carry the venue's text in owned strings no fixed cell bounds, so the same
    /// report is delivered in full and counted as no kind of failure. Narrowing it there would
    /// deny the run's only consumer a domain-valid venue report for a reason that does not
    /// apply to it. The condition is stable for a whole run: [`Self::publish_into`] installs
    /// at most one segment and refuses a second, and it takes `&mut self`, so no segment can
    /// appear or vanish while [`Self::run_until`] is running.
    ///
    /// Nothing here reaches `apply_update`, authority, recovery, or subscriptions. A
    /// resolution changes no level and no revision, and a book update arriving after one is
    /// applied exactly as one arriving before it.
    fn forward_resolution(
        &mut self,
        resolved: &MarketResolved,
        received_at: Instant,
        stamp: ConnectionStamp,
    ) {
        let Some(position) = self.position.checked_add(1) else {
            self.stats
                .record_failure("resolution:PositionCounterOverflow");
            return;
        };
        self.position = position;
        let Some(resolution) = self.build_resolution(resolved, position, received_at, stamp) else {
            self.stats.record_failure("resolution:InvalidObservation");
            return;
        };
        let resolution = Arc::new(resolution);
        self.latest_resolution = Some(Arc::clone(&resolution));
        if self.segment.is_some() && !resolution_fits_the_ring(&resolution) {
            self.stats.record_failure("resolution:Unrepresentable");
            return;
        }
        if let Ok(delivery) = self.writer.publish_resolution(resolution) {
            self.publish_segment_resolution(&delivery, stamp.arrival_time_nanos);
        }
    }

    /// Records one venue resolution as a venue-agnostic observation, or `None` when the
    /// venue's own values do not fit the vocabulary this daemon reproduces them in.
    ///
    /// Provenance mirrors [`Self::build_provenance`]: the same connection identity,
    /// generations, receive position and monotonic times, under this event's own venue
    /// family. `outcome` is deliberately absent — this venue's resolution frame carries no
    /// token, and the winner it does carry is the observation's own field rather than a
    /// second copy in provenance. `local_revision` and `continuity_epoch` are the book's
    /// current values, because that is the point in the stream the resolution is ordered at;
    /// no rebase touches them, since a resolution opens no epoch and commits no revision.
    fn build_resolution(
        &self,
        resolved: &MarketResolved,
        position: u64,
        received_at: Instant,
        stamp: ConnectionStamp,
    ) -> Option<MarketResolution> {
        let winner = NativeOutcome::venue_defined(resolved.winning_outcome()).ok()?;
        let native_label = NativeLabel::new(resolved.market_type()).ok()?;
        let resolution_date = SourceTimestamp::new(resolved.resolution_date()).ok()?;
        let connection = ConnectionIdentity::new(CONNECTION_NAME, stamp.generation).ok()?;
        let provenance = Provenance::new(ProvenanceInput {
            market: self.market.clone(),
            outcome: None,
            native_family: MARKET_RESOLVED_EVENT.to_owned(),
            source_timestamp: Some(resolution_date.clone()),
            source_evidence: BoundedSourceEvidence::new(
                Vec::new(),
                SourceEvidenceCapacity::new(0).expect("0 is within MAX_SOURCE_EVIDENCE"),
            )
            .ok()?,
            daemon_generation: self.config.daemon_generation,
            connection,
            subscription_generation: stamp.subscription_generation,
            receive_position: position,
            commit_position: position,
            local_receive_time: LocalMonotonicTimestamp::new(elapsed_nanos(
                self.run_start,
                received_at,
            )),
            local_commit_time: LocalMonotonicTimestamp::new(elapsed_nanos(
                self.run_start,
                Instant::now(),
            )),
            replica: self.provenance_role(Slot::Primary),
            representation: Representation::VenueNative,
            origin: Origin::SourceReported,
            local_revision: self.writer.book().revision(),
            continuity_epoch: self.writer.book().continuity().epoch(),
        })
        .ok()?;
        let observation =
            ResolutionObservation::new(provenance, winner, native_label, DeliveryPath::MarketFeed)
                .ok()?;
        Some(MarketResolution::new(
            observation,
            resolved.winning_index(),
            resolution_date,
        ))
    }

    /// The most recent resolution this supervisor forwarded, or `None` before the venue
    /// reported one.
    ///
    /// Independent of book state: it is retained across continuity losses, recovery bases
    /// and unsubscription, because the venue's report of how a market resolved does not stop
    /// being true when this daemon's stream breaks.
    pub fn latest_resolution(&self) -> Option<&Arc<MarketResolution>> {
        self.latest_resolution.as_ref()
    }

    /// Hands one forwarded resolution to the shared-memory segment, if this run publishes
    /// into one.
    ///
    /// The published state read here is the one [`BookWriter::publish_resolution`] has already
    /// replaced, so its stream boundary already names the position past this resolution.
    /// `arrival_time_nanos` is the resolution frame's own wall-clock arrival stamp; the state
    /// republish that follows it in the ring instead carries forward whatever the book's last
    /// commit stamped; see [`BookSegment::publish_resolution`].
    fn publish_segment_resolution(
        &mut self,
        delivery: &ResolutionDelivery,
        arrival_time_nanos: u64,
    ) {
        if self.segment.is_none() {
            return;
        }
        let published = self.writer.published();
        if let Some(segment) = &mut self.segment {
            segment.publish_resolution(delivery, &published, arrival_time_nanos);
        }
    }

    /// Hands one accepted commit to the shared-memory segment, if this run publishes into
    /// one.
    ///
    /// Called from the single place the authoritative book commits, so the segment sees
    /// every mutation the book derived, in the order it derived them, and no in-process
    /// consumer stands between the two. `arrival_time_nanos` is the wall-clock stamp of the
    /// venue frame that produced this commit.
    fn publish_commit(&mut self, commit: &BookCommit, arrival_time_nanos: u64) {
        if self.segment.is_none() {
            return;
        }
        let published = self.writer.published();
        if let Some(segment) = &mut self.segment {
            segment.publish_commit(commit, &published, arrival_time_nanos);
        }
    }

    /// Hands the current published state to the shared-memory segment, for a change that
    /// derived no mutation: an evidence-based loss, which consumers learn of by reading
    /// state rather than the ring.
    fn publish_segment_state(&mut self) {
        if self.segment.is_none() {
            return;
        }
        let published = self.writer.published();
        if let Some(segment) = &mut self.segment {
            segment.publish_state(&published, 0);
        }
    }

    /// Applies one arrival to the authoritative book, and reports whether it landed.
    ///
    /// A rejected apply is counted and reported as local loss, exactly as it is for a
    /// single publishing source: the arrival was for this market and this daemon failed to
    /// apply it. `arrival_time_nanos` is the frame's own wall-clock arrival stamp, carried
    /// into the segment publication an accepted apply produces.
    fn apply_authoritative(&mut self, candidate: &Candidate, arrival_time_nanos: u64) -> bool {
        match self.writer.apply_snapshot(candidate) {
            Ok(commit) => {
                self.stats.snapshots_applied = self.stats.snapshots_applied.saturating_add(1);
                self.stats.mutations_derived = self
                    .stats
                    .mutations_derived
                    .saturating_add(u64::try_from(commit.mutations().len()).unwrap_or(u64::MAX));
                self.publish_commit(&commit, arrival_time_nanos);
                true
            }
            Err(error) => {
                self.stats.record_failure(book_error_key(&error));
                self.report_loss(ContinuityReason::LocalLoss, AuthorityReason::LocalLoss);
                false
            }
        }
    }

    /// Applies one arrival to the shadow book of standby role `index`, and reports whether
    /// it landed. A shadow rejection costs that role its agreement and never the book its
    /// authority.
    fn apply_shadow(&mut self, index: usize, candidate: &Candidate) -> bool {
        let applied = match self.standbys.get_mut(index) {
            Some(standby) => standby.shadow.apply_snapshot(candidate),
            None => return false,
        };
        match applied {
            Ok(_) => {
                self.stats.shadow_snapshots_applied =
                    self.stats.shadow_snapshots_applied.saturating_add(1);
                true
            }
            Err(error) => {
                self.stats.record_failure(book_error_key(&error));
                self.report_shadow_loss(
                    index,
                    ContinuityReason::LocalLoss,
                    AuthorityReason::LocalLoss,
                );
                false
            }
        }
    }

    /// Commits an arrival the pool's gate chose to publish.
    ///
    /// A socket that is also a standby role keeps its own shadow current first, so the
    /// role it falls back to on a degrade holds the state it delivered rather than a gap
    /// where its own publications were. The gate learns the arrival reached the book only
    /// after it did: an apply that failed leaves that key free to publish when it next
    /// arrives on another socket.
    fn publish_pooled(
        &mut self,
        slot: Slot,
        candidate: &Candidate,
        key: Option<DedupKey>,
        update: &OrderbookUpdate,
        position: u64,
        stamp: ConnectionStamp,
    ) {
        if let Slot::Standby(index) = slot {
            let _ = self.apply_shadow(index, candidate);
        }
        if !self.apply_authoritative(candidate, stamp.arrival_time_nanos) {
            return;
        }
        if let Some(gate) = self.pool.as_mut()
            && let Some(key) = key
        {
            gate.committed(slot.index(), key);
        }
        self.on_base_accepted(slot, true);
        self.resync();
        self.report_accepted_update(slot, update, position, stamp, true);
    }

    /// Installs a redelivery of the published key as the stale book's recovery base.
    ///
    /// A pool that has lost the book's authority recovers the way any source does: from a
    /// complete authoritative snapshot the venue serves. The venue's observed answer to a
    /// resubscribe is the same frame again, carrying the key the book already holds, so
    /// treating that as nothing but a duplicate would leave a stale book waiting for a
    /// higher key that a quiet market need never produce. It is admitted only when the
    /// window holds exact evidence that this key carried exactly this content, so what is
    /// installed is the state the book was already meant to hold rather than an unverified
    /// claim about it; a redelivery whose content disagrees withdrew the licence before
    /// reaching here.
    ///
    /// It is a recovery base under the book's ordinary contract — a new continuity epoch,
    /// no diffs across the gap — and it does not move the published key: nothing newer than
    /// that key has been published, and later arrivals go on being judged against it.
    fn recover_pooled(
        &mut self,
        slot: Slot,
        candidate: &Candidate,
        update: &OrderbookUpdate,
        position: u64,
        stamp: ConnectionStamp,
    ) {
        if let Slot::Standby(index) = slot {
            let _ = self.apply_shadow(index, candidate);
        }
        if !self.apply_authoritative(candidate, stamp.arrival_time_nanos) {
            return;
        }
        if let Some(gate) = self.pool.as_mut() {
            gate.recovered(slot.index());
        }
        self.on_base_accepted(slot, true);
        self.resync();
        self.report_accepted_update(slot, update, position, stamp, true);
    }

    /// Records an arrival the pool's gate did not publish: a duplicate of a frame another
    /// socket already published, or state older than the book already holds.
    ///
    /// It still belongs in that socket's own shadow, which is what keeps a standby role's
    /// comparison meaningful and what makes it usable the instant the pool degrades. The
    /// publishing slot keeps no shadow, so for it there is nothing to record.
    fn shadow_pooled(
        &mut self,
        slot: Slot,
        candidate: &Candidate,
        update: &OrderbookUpdate,
        position: u64,
        stamp: ConnectionStamp,
    ) {
        let Slot::Standby(index) = slot else {
            return;
        };
        if self.apply_shadow(index, candidate) {
            self.on_base_accepted(slot, false);
            self.resync();
            self.report_accepted_update(slot, update, position, stamp, false);
        }
    }

    /// Reports one accepted update as a dedup-key conformance record, when the run asked
    /// for one.
    ///
    /// A no-op unless [`SupervisorConfig::log_dedup_keys`] is set: with it unset the venue
    /// key is never parsed and no digest is computed, so a run serving consumers carries
    /// none of this work. With it set, both happen after the commit, and the digest
    /// fingerprints the book the arrival reached — the published book when `authoritative`,
    /// the accepting role's shadow otherwise — so two roles' records are directly
    /// comparable.
    ///
    /// Reporting nothing here never affects the book: the notice is diagnostic, and a tap
    /// that cannot take it counts a dropped diagnostic.
    fn report_accepted_update(
        &mut self,
        slot: Slot,
        update: &OrderbookUpdate,
        position: u64,
        stamp: ConnectionStamp,
        authoritative: bool,
    ) {
        if !self.config.log_dedup_keys {
            return;
        }
        let digest = match (authoritative, slot) {
            (true, _) => content_digest(&self.writer.published()),
            (false, Slot::Standby(index)) => match self.standbys.get(index) {
                Some(standby) => content_digest(&standby.shadow.publish()),
                None => return,
            },
            (false, Slot::Primary) => return,
        };
        self.tap(SupervisorNotice::AcceptedUpdate {
            market: update.market_slug().to_owned(),
            position,
            generation: stamp.generation,
            replica: slot.role(),
            key: update.dedup_key(),
            digest,
        });
    }

    /// Records this generation's provenance truthfully.
    ///
    /// Venue-reported: market, outcome from `tokenId`, the event's `timestamp` lexeme, and
    /// its `version` lexeme as uninterpreted source evidence. Locally observed: the daemon
    /// generation, this connection's process-monotonic generation, the subscription
    /// generation the frame was read under as the connection reported it, rather than
    /// whichever emit is current when the notice is applied, the monotonic receive/commit
    /// position, monotonic local times taken when
    /// the frame was read and when it was committed, and the role the connection held when
    /// the snapshot was accepted.
    fn build_provenance(
        &self,
        update: &OrderbookUpdate,
        position: u64,
        received_at: Instant,
        stamp: ConnectionStamp,
        replica: ReplicaRole,
    ) -> Result<Provenance, DescriptorError> {
        let outcome = update
            .token_id()
            .map(|token| {
                NativeOutcome::token(token).map_err(|_| DescriptorError::InvalidSourceEvidence)
            })
            .transpose()?;
        let source_evidence = match update.version_evidence()? {
            Some(evidence) => BoundedSourceEvidence::new(
                [evidence],
                SourceEvidenceCapacity::new(1).expect("1 is within MAX_SOURCE_EVIDENCE"),
            )?,
            None => BoundedSourceEvidence::new(
                Vec::new(),
                SourceEvidenceCapacity::new(0).expect("0 is within MAX_SOURCE_EVIDENCE"),
            )?,
        };
        let connection = ConnectionIdentity::new(CONNECTION_NAME, stamp.generation)?;
        Provenance::new(ProvenanceInput {
            market: self.market.clone(),
            outcome,
            native_family: ORDERBOOK_UPDATE_EVENT.to_owned(),
            source_timestamp: Some(SourceTimestamp::new(update.timestamp())?),
            source_evidence,
            daemon_generation: self.config.daemon_generation,
            connection,
            subscription_generation: stamp.subscription_generation,
            receive_position: position,
            commit_position: position,
            local_receive_time: LocalMonotonicTimestamp::new(elapsed_nanos(
                self.run_start,
                received_at,
            )),
            local_commit_time: LocalMonotonicTimestamp::new(elapsed_nanos(
                self.run_start,
                Instant::now(),
            )),
            replica,
            representation: Representation::VenueNative,
            origin: Origin::SourceReported,
            local_revision: 0,
            continuity_epoch: 0,
        })
    }

    /// Records that one arrival was accepted, and by what.
    ///
    /// `authoritative` is whether the arrival reached the published book, which is what
    /// clears the recovery state — never which role delivered it. In a pool any socket's
    /// arrival can be the one that does.
    /// Whether the authoritative book holds authority right now.
    ///
    /// Deliberately not `base_accepted`, which is sticky and means only that a base was
    /// accepted at some point. A connection arriving in the publishing role holds a base
    /// only if the state it would continue is still authoritative: a book that held
    /// authority and lost it must recover from a fresh venue base, and deriving the arriving
    /// connection's `produced_base` from the sticky flag would reset the recovery-attempt
    /// count on every role change and postpone a truthful
    /// [`AuthorityReason::RecoveryBaseUnavailable`] indefinitely.
    fn authority_is_live(&self) -> bool {
        matches!(self.writer.book().authority(), AuthorityState::Live)
    }

    fn on_base_accepted(&mut self, slot: Slot, authoritative: bool) {
        if authoritative {
            self.base_accepted = true;
            self.recovery_attempts = 0;
            self.unavailable_reported = false;
        }
        if let Some(active) = self.active_mut(slot) {
            active.produced_base = true;
            active.recovery = None;
        }
    }

    /// Reports one role's known local loss to the book that loses by it.
    ///
    /// While a pool is armed every socket is an authoritative source, so a book-relevant
    /// decode failure or an overflowed notice queue on any of them is an authoritative
    /// continuity loss, not a shadow one. The venue's streams are near-identical but not
    /// provably identical, and its counter is non-contiguous per market, so a frame this
    /// daemon received and lost on one socket may carry a state no other socket's stream
    /// contains: publishing straight past it would present a hole as an intact history. A
    /// standby socket also loses its shadow, because it is equally true that its own stream
    /// broke. After a degrade the roles mean what they meant before, and a non-publishing
    /// socket's loss costs only its shadow.
    fn report_slot_loss(
        &mut self,
        slot: Slot,
        continuity: ContinuityReason,
        authority: AuthorityReason,
    ) {
        if self.pool_armed() {
            self.report_loss(continuity.clone(), authority.clone());
            if let Slot::Standby(index) = slot {
                self.report_shadow_loss(index, continuity, authority);
            }
            return;
        }
        match slot {
            Slot::Primary => self.report_loss(continuity, authority),
            Slot::Standby(index) => self.report_shadow_loss(index, continuity, authority),
        }
    }

    /// Reports evidence-based loss to the authoritative book, and records that the current
    /// publishing generation no longer holds a base.
    fn report_loss(&mut self, continuity: ContinuityReason, authority: AuthorityReason) {
        if let Some(active) = self.primary.active.as_mut() {
            active.produced_base = false;
        }
        match self
            .writer
            .report_continuity_loss(continuity.clone(), authority.clone())
        {
            Ok(true) => {
                self.stats.continuity_losses = self.stats.continuity_losses.saturating_add(1);
                self.publish_segment_state();
                self.tap(SupervisorNotice::ContinuityLoss {
                    continuity,
                    authority,
                });
            }
            Ok(false) => {}
            Err(error) => self.stats.record_failure(book_error_key(&error)),
        }
        self.resync();
    }

    /// Reports the same evidence to the shadow book. No consumer sees it: its only effect
    /// is that the standby stops agreeing until its stream is rebased by a fresh venue
    /// snapshot.
    fn report_shadow_loss(
        &mut self,
        index: usize,
        continuity: ContinuityReason,
        authority: AuthorityReason,
    ) {
        let mut failure = None;
        if let Some(standby) = self.standbys.get_mut(index) {
            if let Some(active) = standby.slot.active.as_mut() {
                active.produced_base = false;
            }
            if let Err(error) = standby.shadow.report_continuity_loss(continuity, authority) {
                failure = Some(book_error_key(&error));
            }
        }
        if let Some(key) = failure {
            self.stats.record_failure(key);
        }
        self.resync();
    }

    /// Recomputes the standby's comparison verdict and the tracked source topology, tapping
    /// any transition.
    ///
    /// The verdict stored here is continuous telemetry, refreshed after every commit on
    /// either book and after every change of role assignment. It is deliberately not what
    /// the promotion gate reads: [`Self::decide_switch`] recomputes the same predicate at
    /// the instant of the loss, so a verdict that predates the newest authoritative commit
    /// can never authorize a takeover.
    fn resync(&mut self) {
        let published = self.writer.published();
        for standby in &mut self.standbys {
            if standby.slot.active.is_some() {
                standby.state = agreement(&standby.shadow, &published);
            }
        }
        let next = self.topology();
        if topology_changed(&self.source, &next) {
            self.source = next.clone();
            self.tap(SupervisorNotice::SourceTransition { source: next });
        } else {
            self.source = next;
        }
    }

    /// The source topology as currently assigned.
    ///
    /// `Publishing` requires the authoritative book to be live: that is what "a source is
    /// publishing" means. A book that has held authority and lost it is `Recovering` behind
    /// whichever connection now occupies the publishing role, and everything else — no
    /// connection, or no base ever accepted — is `NoSource`.
    fn topology(&self) -> SourceState {
        if self.pool_armed() {
            return self.pooled_topology(None);
        }
        let Some(active) = self.primary.active.as_ref() else {
            return SourceState::no_source();
        };
        let Ok(identity) = ConnectionIdentity::new(CONNECTION_NAME, active.generation) else {
            return SourceState::no_source();
        };
        if !matches!(self.writer.book().authority(), AuthorityState::Live) {
            return if self.base_accepted {
                SourceState::recovering(RecoveryReplica::new(identity))
            } else {
                SourceState::no_source()
            };
        }
        let primary = PublishingPrimary::new(identity);
        let capacity = self.config.replicas.saturating_sub(1);
        if capacity == 0 {
            return SourceState::primary(primary);
        }
        let assigned: Vec<(HotStandby, StandbyState)> = self
            .standbys
            .iter()
            .filter_map(|standby| {
                standby
                    .assigned
                    .clone()
                    .map(|identity| (HotStandby::new(identity), standby.state.clone()))
            })
            .collect();
        let names: Vec<HotStandby> = assigned
            .iter()
            .map(|(standby, _)| standby.clone())
            .collect();
        let Ok(mut source) = SourceState::with_hot_standbys(primary.clone(), names, capacity)
        else {
            return SourceState::primary(primary);
        };
        for (standby, state) in assigned {
            source.report_standby(&standby, state);
        }
        source
    }

    /// The pool's coverage as consumers see it: which connections hold its sockets, what
    /// each can currently contribute, the key of the arrival last published, and — on the
    /// one transition that reports it — why the pool stopped publishing across them.
    ///
    /// A socket the pool is configured to hold but has no connection for is simply absent,
    /// so the shortfall against `capacity` is the coverage a consumer can see it has lost.
    fn pooled_topology(&self, degraded: Option<PoolDegradeReason>) -> SourceState {
        let now = Instant::now();
        let sockets: Vec<(PoolSocket, PoolSocketState)> = self
            .slots()
            .filter_map(|slot| {
                let Some(active) = self.slot(slot).and_then(|slot| slot.active.as_ref()) else {
                    return self.vacated_socket(slot);
                };
                let identity = ConnectionIdentity::new(CONNECTION_NAME, active.generation).ok()?;
                let state = if !active.is_viable(now) {
                    PoolSocketState::Failed(ReplicaFailureReason::Disconnect)
                } else if active.is_established() {
                    PoolSocketState::Covering
                } else {
                    PoolSocketState::Establishing
                };
                Some((PoolSocket::new(identity), state))
            })
            .collect();
        let last_published = self
            .pool
            .as_ref()
            .and_then(|gate| gate.last_published().cloned());
        SourceState::pooled(sockets, self.config.replicas, last_published, degraded)
            .unwrap_or_else(|_| SourceState::no_source())
    }

    /// The connection a socket held until it failed, and why, for as long as the socket has
    /// no replacement.
    ///
    /// A socket between connections is a coverage shortfall either way, but a shortfall with
    /// a named connection and a named reason is a different operator problem from an
    /// anonymous one: a namespace refusal, an overflowed notice queue and a dead socket all
    /// have to stay tellable apart while the replacement is backing off. The publishing slot
    /// keeps no such record, so a vacancy there is reported as the absence it is.
    fn vacated_socket(&self, slot: Slot) -> Option<(PoolSocket, PoolSocketState)> {
        let Slot::Standby(index) = slot else {
            return None;
        };
        let standby = self.standbys.get(index)?;
        let identity = standby.assigned.clone()?;
        let state = match &standby.state {
            StandbyState::Failed(reason) => PoolSocketState::Failed(reason.clone()),
            _ => PoolSocketState::Failed(ReplicaFailureReason::Disconnect),
        };
        Some((PoolSocket::new(identity), state))
    }

    /// Withdraws the pool's licence to publish across sockets, for the rest of the process,
    /// and hands the book back to one publishing primary with hot standbys.
    ///
    /// The published book is not touched: no state is replaced, no continuity epoch opens
    /// and no staleness is reported, because nothing about what was published has been
    /// called into question — only the licence to keep choosing arrivals by key. The socket
    /// that carried the last published arrival becomes the publishing primary, so the
    /// primary's own stream and the published book agree from the first instant; when that
    /// socket is gone, the publishing role stays where it is if it can still deliver, and
    /// otherwise moves to the first socket that can.
    ///
    /// The pool never re-arms here. The evidence licensing it was recorded before the run
    /// against the venue's contract document, and a run that has just contradicted it
    /// cannot re-record it; an operator restart is what re-arms a pool.
    fn degrade_pool(&mut self, violation: PoolViolation, generation: u64) {
        let reason = violation.reason;
        let Some(gate) = self.pool.as_mut() else {
            return;
        };
        let _ = gate.degrade(reason);
        self.tap(SupervisorNotice::PoolDegraded {
            violation,
            generation,
        });
        let degraded = self.pooled_topology(Some(reason));
        self.source = degraded.clone();
        self.tap(SupervisorNotice::SourceTransition { source: degraded });
        let preferred = self
            .pool
            .as_ref()
            .and_then(PoolGate::last_publisher)
            .map(Slot::from_index);
        let now = Instant::now();
        let viable = |slot: Slot| {
            self.slot(slot)
                .and_then(|slot| slot.active.as_ref())
                .is_some_and(|active| active.is_viable(now))
        };
        let chosen = preferred
            .filter(|slot| viable(*slot))
            .or_else(|| viable(Slot::Primary).then_some(Slot::Primary))
            .or_else(|| self.first_viable_standby().map(Slot::Standby));
        if let Some(Slot::Standby(index)) = chosen {
            self.exchange_with_primary(index);
        }
        self.resync();
    }

    /// Exchanges the connections held by the publishing role and standby role `index`,
    /// carrying each one's pool-socket history with it.
    ///
    /// Used only by the degrade, where the connection that produced the published state
    /// must become the one publishing it. The displaced connection keeps serving as a
    /// standby with a fresh shadow, so it holds no state it did not itself deliver under
    /// the role it now occupies; nothing about the published book changes either way.
    fn exchange_with_primary(&mut self, index: usize) {
        let holds_base = self.authority_is_live();
        let Some(standby) = self.standbys.get_mut(index) else {
            return;
        };
        let Some(mut taken) = standby.slot.active.take() else {
            return;
        };
        taken.produced_base = holds_base;
        standby.assigned = None;
        let displaced = self.primary.active.take();
        self.primary.active = Some(taken);
        self.primary.reconnect_at = None;
        let market = self.market.clone();
        if let Some(standby) = self.standbys.get_mut(index) {
            standby.shadow = OrderBook::new(market);
            standby.state = StandbyState::Divergent(DivergenceReason::ContinuityMismatch);
            if let Some(active) = displaced {
                standby.assigned = ConnectionIdentity::new(CONNECTION_NAME, active.generation).ok();
                standby.slot.active = Some(active);
            }
        }
        if let Some(gate) = self.pool.as_mut() {
            gate.exchange_sockets(0, index + 1);
        }
    }

    /// Whether the standby may take over the authoritative book without replacing its
    /// state.
    ///
    /// Every condition must hold at the instant the publishing connection is lost:
    ///
    /// 1. a standby role exists (`replicas = 2`) and its connection is assigned;
    /// 2. that connection is transport-viable — its task is still running and its own
    ///    heartbeat evidence has not expired, judged now rather than inherited from
    ///    whenever the slot was filled;
    /// 3. the authoritative book is [`AuthorityState::Live`] — a book that has already lost
    ///    authority must recover from a fresh venue base, never from a source switch;
    /// 4. the shadow is itself live, which holds exactly when it has accepted at least one
    ///    snapshot on its own stream and its mutation continuity is intact;
    /// 5. the shadow's canonical economic state equals the published authoritative state.
    ///
    /// 4 and 5 are [`agreement`], recomputed here rather than read from the tracked
    /// [`StandbyState`]. 2 is what keeps a correlated loss from being answered with a
    /// source transition onto a connection that is already dead: an agreeing shadow says
    /// what the standby knew, not that it can still produce. Availability pressure is not
    /// part of the predicate: a failed condition surrenders authority and waits for a venue
    /// base instead.
    fn decide_switch(&self, ended: ConnectionEndReason) -> SourceSwitch {
        let now = Instant::now();
        let published = self.writer.published();
        let mut refusal = None;
        for (index, standby) in self.standbys.iter().enumerate() {
            let Some(active) = standby.slot.active.as_ref() else {
                continue;
            };
            let verdict = agreement(&standby.shadow, &published);
            if verdict == StandbyState::Agreeing
                && matches!(published.authority(), AuthorityState::Live)
                && active.is_viable(now)
            {
                return SourceSwitch::Promote(index);
            }
            if refusal.is_none() {
                refusal = Some(refusal_reason(&verdict, ended));
            }
        }
        match refusal {
            Some(authority) => SourceSwitch::Refuse(authority),
            None => SourceSwitch::Absent,
        }
    }

    /// Closes an already-expired standby before a publishing loss is decided.
    ///
    /// Both roles' deadlines can lapse inside one wake, and the publishing role is polled
    /// first, so without this the switch decision would be taken against a standby whose
    /// own expiry had not been acted on yet. Settling it first makes the topology the
    /// decision reads the topology that actually holds.
    fn settle_expired_standby(&mut self) {
        for index in 0..self.standbys.len() {
            let _ = self.fence_if_expired(Slot::Standby(index));
        }
    }

    /// Closes one role's connection, whatever ended it.
    ///
    /// A generation that ended because its own notice queue overflowed past delivery is the
    /// one end reason that hides a loss rather than reporting one: the notice it could not
    /// hand over may have been the overload report itself, or a book update that never
    /// reached the book. While a pool is armed every socket is an authoritative source, so
    /// that is an authoritative continuity loss and it is reported before the coverage
    /// handover, on either kind of slot. Only an established session is treated this way: a
    /// connection that never reached its subscription had no book state to lose.
    fn on_slot_ended(
        &mut self,
        slot: Slot,
        reason: ConnectionEndReason,
        produced_base: bool,
        established: bool,
        lifetime: Duration,
    ) {
        if self.pool_armed()
            && established
            && matches!(reason, ConnectionEndReason::NoticeUndeliverable)
        {
            self.report_loss(ContinuityReason::LocalLoss, AuthorityReason::Overload);
        }
        match slot {
            Slot::Primary => self.on_primary_ended(reason, produced_base, lifetime),
            Slot::Standby(index) => self.on_standby_ended(index, reason, lifetime),
        }
    }

    /// Closes the publishing generation and decides what replaces it.
    ///
    /// A generation that never produced a base counts as one spent recovery attempt. Once
    /// `max_recovery_attempts` consecutive attempts have failed to produce one, the book is
    /// reported [`AuthorityReason::RecoveryBaseUnavailable`] exactly once and retries
    /// continue at the slower `exhausted_backoff` cadence until a base is accepted.
    ///
    /// Before any base has ever been accepted, a failed attempt reports no continuity loss:
    /// there is no authority to lose yet, so the book stays
    /// [`crate::AuthorityState::Synchronizing`] and only exhaustion makes it stale.
    ///
    /// The backoff ladder restarts only for a generation that lived at least `stable_after`.
    /// An accepted base does not restart it: a venue that accepts, serves one book, and
    /// drops the socket would otherwise reconnect at `initial_backoff` forever.
    ///
    /// Once recovery has spent its attempt budget, every further failed attempt reports
    /// [`AuthorityReason::RecoveryBaseUnavailable`] rather than the transport reason of
    /// whichever attempt failed last, which would present a standing terminal fact as a
    /// fresh, milder one.
    fn on_primary_ended(
        &mut self,
        reason: ConnectionEndReason,
        produced_base: bool,
        lifetime: Duration,
    ) {
        if self.pool_armed() {
            self.on_pool_slot_ended(reason, produced_base, lifetime);
            return;
        }
        self.settle_expired_standby();
        self.primary.backoff_attempt = if resets_backoff(lifetime, self.config.stable_after) {
            1
        } else {
            self.primary.backoff_attempt.saturating_add(1)
        };
        if produced_base {
            self.recovery_attempts = 0;
        } else {
            self.recovery_attempts = self.recovery_attempts.saturating_add(1);
        }
        let switch = self.decide_switch(reason);
        self.apply_switch(switch, reason);
        self.resync();
    }

    /// Carries out a switch decision, and is the only place a promotion is counted.
    ///
    /// The decision and the takeover are two instants, not one: between them the standby's
    /// task can end and its heartbeat evidence can expire, and [`Self::take_over`] refuses
    /// to hand the publishing role to a connection that is no longer viable. A promotion
    /// that could not be completed is answered exactly as a refusal is, because the book is
    /// in the same position either way — the publishing connection is gone and nothing
    /// eligible is left to continue it. Counting it as a promotion, or leaving a live book
    /// behind a publishing role nothing occupies, would both be false.
    ///
    /// Such a refusal reports the reason an agreeing standby's refusal reports, because
    /// agreement is exactly what the decision established: what failed between the two
    /// instants is the standby's transport, not the comparison.
    fn apply_switch(&mut self, switch: SourceSwitch, ended: ConnectionEndReason) {
        match switch {
            SourceSwitch::Promote(index) => {
                if self.take_over(index, true) {
                    self.stats.promotions = self.stats.promotions.saturating_add(1);
                } else {
                    self.refuse_switch(refusal_reason(&StandbyState::Agreeing, ended));
                }
            }
            SourceSwitch::Refuse(authority) => self.refuse_switch(authority),
            SourceSwitch::Absent => {
                if self.base_accepted {
                    let (continuity, authority) = end_reason_mapping(ended);
                    self.report_loss(continuity, self.terminal_authority(authority));
                }
                self.report_exhaustion();
                self.schedule_reconnect(Slot::Primary);
            }
        }
    }

    /// Surrenders the book's authority behind a standby that did not take over, and
    /// arranges what fills the publishing role.
    ///
    /// A standby connection that is still viable is handed over as the recovery source
    /// holding no base, so the book recovers from a fresh venue snapshot on it; when there
    /// is none to hand over, the role is refilled through the reconnect ladder.
    fn refuse_switch(&mut self, authority: AuthorityReason) {
        if self.base_accepted {
            self.report_loss(
                ContinuityReason::Reconnect,
                self.terminal_authority(authority),
            );
        }
        self.stats.promotions_refused = self.stats.promotions_refused.saturating_add(1);
        self.report_exhaustion();
        let handover = self.first_viable_standby();
        if !handover.is_some_and(|index| self.take_over(index, false)) {
            self.schedule_reconnect(Slot::Primary);
        }
    }

    /// Closes the publishing slot's connection while the pool still holds its licence.
    ///
    /// A pool has no publishing primary to lose. Every socket feeds the same published book
    /// through the same gate, so one socket going away costs coverage and nothing else: the
    /// promotion gate — which answers "which of two histories is authoritative" — is not
    /// consulted, because the key gate has already answered that for every arrival the book
    /// holds. No continuity loss is reported and no epoch opens while another socket can
    /// still deliver; the survivors keep publishing without a gap.
    ///
    /// The publishing slot is refilled from a surviving socket so every later decision
    /// still has one to read, and the socket that moved is replaced on its own ladder. With
    /// no surviving socket the pool is in the single-source position, and the ordinary
    /// single-source path reports the loss and reconnects.
    ///
    /// A recovery attempt is spent only when the book actually needs recovering as this
    /// socket ends. A pool's publishing slot is a structural role, not the source of the
    /// book's authority: a connection there that published nothing while other sockets kept
    /// the book live has failed at nothing, and counting its end as a failed recovery would
    /// let a healthy pool report a terminal
    /// [`AuthorityReason::RecoveryBaseUnavailable`] over a book that never stopped
    /// publishing.
    fn on_pool_slot_ended(
        &mut self,
        reason: ConnectionEndReason,
        produced_base: bool,
        lifetime: Duration,
    ) {
        self.primary.backoff_attempt = if resets_backoff(lifetime, self.config.stable_after) {
            1
        } else {
            self.primary.backoff_attempt.saturating_add(1)
        };
        let needs_recovery = !self.authority_is_live();
        if needs_recovery {
            if produced_base {
                self.recovery_attempts = 0;
            } else {
                self.recovery_attempts = self.recovery_attempts.saturating_add(1);
            }
        }
        if let Some(gate) = self.pool.as_mut() {
            gate.retire_socket(Slot::Primary.index());
        }
        let survivor = self.first_viable_standby();
        match survivor {
            Some(index) => {
                let holds_base = self.authority_is_live();
                let _ = self.take_over(index, holds_base);
            }
            None => {
                if self.base_accepted {
                    let (continuity, authority) = end_reason_mapping(reason);
                    self.report_loss(continuity, self.terminal_authority(authority));
                }
                self.schedule_reconnect(Slot::Primary);
            }
        }
        if needs_recovery {
            self.report_exhaustion();
        }
        self.resync();
    }

    /// Whether recovery has spent its attempt budget without producing a base.
    ///
    /// This is the book's terminal condition, separate from whether the one-time
    /// [`SupervisorNotice::RecoveryBaseUnavailable`] notification has been sent: the
    /// notification happens once, the condition holds until a base is accepted.
    fn recovery_exhausted(&self) -> bool {
        self.recovery_attempts >= self.config.max_recovery_attempts
    }

    /// The authority reason a failed attempt reports, which is its own until recovery is
    /// exhausted and [`AuthorityReason::RecoveryBaseUnavailable`] after that.
    fn terminal_authority(&self, reason: AuthorityReason) -> AuthorityReason {
        if self.recovery_exhausted() {
            AuthorityReason::RecoveryBaseUnavailable
        } else {
            reason
        }
    }

    /// Closes the standby generation. It cannot cost the book anything: the shadow is
    /// discarded, the role records why it failed, and a replacement is scheduled on the
    /// standby's own ladder.
    ///
    /// The failed connection keeps its assignment until the replacement generation spawns,
    /// so what consumers see is a named connection that failed for a named reason rather
    /// than an anonymous hole in the coverage. A namespace refusal, an overflowed notice
    /// queue and a dead socket are different operator problems and must not collapse into
    /// one another during the backoff window.
    fn on_standby_ended(&mut self, index: usize, reason: ConnectionEndReason, lifetime: Duration) {
        let market = self.market.clone();
        if let Some(standby) = self.standbys.get_mut(index) {
            standby.state = StandbyState::Failed(replica_failure(reason));
            standby.shadow = OrderBook::new(market);
        }
        if let Some(gate) = self.pool.as_mut() {
            gate.retire_socket(Slot::Standby(index).index());
        }
        self.schedule_slot_reconnect(Slot::Standby(index), lifetime);
        self.resync();
    }

    /// Moves the standby's live connection into the publishing role, and returns whether
    /// there was one to move.
    ///
    /// `promoted` records whether the connection arrives holding a proven base. A promotion
    /// carries the standby's agreement forward, so nothing about the book changes and the
    /// role is simply reassigned. A refused promotion hands the same connection over as the
    /// recovery source only: it arrives holding no base, so the book stays stale until that
    /// connection delivers a fresh venue snapshot, and the shadow state that was refused is
    /// discarded rather than published. Either way the standby role is vacated and a
    /// replacement is scheduled on its ladder, which does not restart unless the vacating
    /// connection had reached `stable_after`.
    ///
    /// Only a transport-viable connection is moved, promoted or not: handing the publishing
    /// role to a task that has already ended, or to one whose heartbeat evidence has
    /// expired, would name a dead connection as this market's source. There being nothing
    /// to move is reported to the caller, which reconnects instead.
    fn take_over(&mut self, index: usize, promoted: bool) -> bool {
        let market = self.market.clone();
        let now = Instant::now();
        let Some(standby) = self.standbys.get_mut(index) else {
            return false;
        };
        if !standby
            .slot
            .active
            .as_ref()
            .is_some_and(|active| active.is_viable(now))
        {
            return false;
        }
        let Some(mut active) = standby.slot.active.take() else {
            return false;
        };
        let lifetime = Instant::now().saturating_duration_since(active.spawned_at);
        active.produced_base = promoted;
        standby.assigned = None;
        standby.shadow = OrderBook::new(market);
        standby.state = StandbyState::Divergent(DivergenceReason::ContinuityMismatch);
        self.primary.active = Some(active);
        self.primary.reconnect_at = None;
        self.primary.backoff_attempt = 0;
        if let Some(gate) = self.pool.as_mut() {
            gate.reassign_socket(Slot::Standby(index).index(), Slot::Primary.index());
        }
        self.schedule_slot_reconnect(Slot::Standby(index), lifetime);
        true
    }

    /// Aborts the publishing connection's task, dropping its socket without a close frame.
    ///
    /// Diagnostic fault injection for [`SupervisorConfig::kill_primary_after`] and nothing
    /// else: it touches neither book, reports no loss of its own, and spends its single
    /// armed instant whether or not there was a connection to abort. What follows is what
    /// the join observes — [`ConnectionEndReason::TaskFailed`] through the same
    /// end-of-connection path every other generation ends on.
    fn kill_primary(&mut self) {
        self.kill_primary_at = None;
        if let Some(active) = self.primary.active.as_ref() {
            active.handle.abort();
        }
    }

    fn report_exhaustion(&mut self) {
        if self.recovery_attempts < self.config.max_recovery_attempts || self.unavailable_reported {
            return;
        }
        self.report_loss(
            ContinuityReason::Reconnect,
            AuthorityReason::RecoveryBaseUnavailable,
        );
        self.unavailable_reported = true;
        self.stats.recovery_base_unavailable =
            self.stats.recovery_base_unavailable.saturating_add(1);
        let attempts = self.recovery_attempts;
        self.tap(SupervisorNotice::RecoveryBaseUnavailable { attempts });
    }

    /// Confirms a missed heartbeat deadline against everything already queued, and closes
    /// the generation before any of that queue reaches the book.
    ///
    /// The deadline is polled ahead of the notice queue so market data cannot starve
    /// liveness detection; the cost is that ping evidence may still be queued when the
    /// deadline fires. This stages what the queue already holds — bounded by its capacity,
    /// which is all it can hold — and re-checks the deadline against the ping evidence
    /// among it, so a busy connection is never mistaken for a dead one.
    ///
    /// Only the expiring generation's own liveness evidence is applied before the verdict.
    /// Its remaining staged notices are dispatched after the verdict, so a frame that
    /// arrived at or after the deadline cannot reach the book on the strength of having
    /// been queued before the timer was polled: once the deadline stands, the generation is
    /// fenced first and everything it staged is discarded as the late work it is. Notices
    /// from every other generation are dispatched first and unaffected — nothing about one
    /// role's liveness makes another role's work late.
    fn on_heartbeat_missed(&mut self, slot: Slot) {
        let Some(generation) = self
            .slot(slot)
            .and_then(|slot| slot.active.as_ref())
            .map(|active| active.generation)
        else {
            return;
        };
        let mut staged: Vec<ConnectionNotice> = Vec::new();
        for _ in 0..self.config.ingest_capacity {
            match self.notices_rx.try_recv() {
                Ok(notice) => staged.push(notice),
                Err(_) => break,
            }
        }
        let (expiring, others): (Vec<ConnectionNotice>, Vec<ConnectionNotice>) = staged
            .into_iter()
            .partition(|notice| notice.generation == generation);
        for notice in others {
            self.on_notice(notice);
        }
        for notice in &expiring {
            if let ConnectionNote::Heartbeat { observed_at } = notice.note
                && let Some(active) = self.active_mut(slot)
            {
                active.last_heartbeat = Some(observed_at);
            }
        }
        self.fence_if_expired(slot);
        for notice in expiring {
            self.on_notice(notice);
        }
    }

    /// Closes a role's connection when its heartbeat deadline stands against the evidence
    /// applied so far, and reports whether it did.
    ///
    /// Judges only; it never waits for more evidence. A caller that can still drain queued
    /// ping evidence does so before asking.
    fn fence_if_expired(&mut self, slot: Slot) -> bool {
        let expired = self
            .slot(slot)
            .and_then(|slot| slot.active.as_ref())
            .and_then(ActiveConnection::heartbeat_expiry)
            .is_some_and(|expiry| expiry <= Instant::now());
        if !expired {
            return false;
        }
        let Some(active) = self.slot_mut(slot).and_then(|slot| slot.active.take()) else {
            return false;
        };
        let produced_base = active.produced_base;
        let established = active.is_established();
        let lifetime = Instant::now().saturating_duration_since(active.spawned_at);
        self.fence(slot, active);
        self.on_slot_ended(
            slot,
            ConnectionEndReason::HeartbeatTimeout,
            produced_base,
            established,
            lifetime,
        );
        true
    }

    /// Makes a generation's work ineligible immediately and lets its task drain for a
    /// bounded window before aborting it.
    ///
    /// Ineligibility is what protects the book, and it takes effect the instant the role
    /// stops naming this generation. The drain window exists because in-flight work cannot
    /// be made to vanish: everything the fenced task still produces is discarded. At most
    /// one fenced connection is held per role, so a flapping venue cannot accumulate
    /// sockets.
    fn fence(&mut self, slot: Slot, active: ActiveConnection) {
        let generation = active.generation;
        let expires_at = Instant::now() + self.config.fenced_linger;
        if let Some(target) = self.slot_mut(slot) {
            if let Some(previous) = target.fenced.take() {
                previous.handle.abort();
            }
            target.fenced = Some(FencedConnection {
                handle: active.handle,
                expires_at,
            });
        }
        self.stats.fenced_generations = self.stats.fenced_generations.saturating_add(1);
        self.tap(SupervisorNotice::Fenced { generation });
    }

    fn release_fenced(&mut self, slot: Slot) {
        if let Some(target) = self.slot_mut(slot)
            && let Some(fenced) = target.fenced.take()
        {
            fenced.handle.abort();
        }
    }

    /// Advances a role's backoff ladder for a connection that lived `lifetime`, then
    /// schedules its replacement.
    fn schedule_slot_reconnect(&mut self, slot: Slot, lifetime: Duration) {
        let reset = resets_backoff(lifetime, self.config.stable_after);
        if let Some(target) = self.slot_mut(slot) {
            target.backoff_attempt = if reset {
                1
            } else {
                target.backoff_attempt.saturating_add(1)
            };
        }
        self.schedule_reconnect(slot);
    }

    fn schedule_reconnect(&mut self, slot: Slot) {
        let attempt = self.slot(slot).map_or(1, |target| target.backoff_attempt);
        let base = if slot == Slot::Primary && self.recovery_exhausted() {
            self.config.ladder_backoff(self.config.exhausted_backoff)
        } else {
            exponential(
                self.config.initial_backoff,
                attempt,
                self.config.ladder_backoff(self.config.max_backoff),
            )
        };
        let delay = jittered(base, self.jitter.hash_one((slot.index(), attempt)));
        let at = Instant::now() + delay;
        if let Some(target) = self.slot_mut(slot) {
            target.reconnect_at = Some(at);
        } else {
            return;
        }
        let generation = self.next_generation;
        let delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);
        self.tap(SupervisorNotice::Reconnecting {
            generation,
            replica: slot.role(),
            delay_ms,
        });
    }

    fn tap(&mut self, notice: SupervisorNotice) {
        let dropped = match &self.tap {
            Some(sender) => sender.try_send(notice).is_err(),
            None => false,
        };
        if dropped {
            self.stats.diagnostics_dropped = self.stats.diagnostics_dropped.saturating_add(1);
        }
    }

    fn finish(&mut self) -> SupervisorStats {
        for slot in self.slots() {
            if let Some(target) = self.slot_mut(slot)
                && let Some(active) = target.active.take()
            {
                active.handle.abort();
            }
            self.release_fenced(slot);
        }
        if let Some(gate) = self.pool.as_ref() {
            self.stats.pool_published = gate.published();
            self.stats.pool_published_by_socket = gate.published_by_socket();
            self.stats.pool_dedup_drops = gate.duplicate_drops();
            self.stats.pool_stale_drops = gate.stale_drops();
            self.stats.pool_last_published_key = gate.last_published().cloned();
            self.stats.pool_degraded = gate.degraded();
        }
        self.stats.frames_seen = self.frames.load(Ordering::Relaxed);
        self.stats.clone()
    }
}

/// The comparison verdict for a standby whose connection is alive.
///
/// [`StandbyState::Agreeing`] needs the shadow to be [`AuthorityState::Live`] — which holds
/// exactly when it has accepted at least one snapshot on its own stream and its mutation
/// continuity is intact — and its canonical economic state to equal the published
/// authoritative state.
///
/// A shadow that has accepted nothing yet, or whose own stream broke and has not been
/// rebased by a fresh venue snapshot, is [`DivergenceReason::ContinuityMismatch`]: there is
/// no comparable history, which is a different fact from two comparable histories that
/// disagree. Two comparable histories that disagree are
/// [`DivergenceReason::ContentMismatch`]. Divergence in either form is feed-health evidence
/// about the standby; it never stales the primary by itself.
/// Whether the shared-memory ring's fixed cells can carry this venue report verbatim.
///
/// The vocabulary a resolution is recorded in is wider than the ABI stores it in — a winning
/// outcome up to 1024 bytes against [`RES_OUTCOME_CAPACITY`], a native label up to 1024
/// against [`RES_TYPE_CAPACITY`], a timestamp lexeme up to 256 against [`RES_DATE_CAPACITY`],
/// and an outcome the venue names by index alone carrying no text at all — so a report can
/// satisfy every domain type and still not fit a slot. Truncation is never an option: a
/// truncated outcome names a different winner and no consumer could tell. The widths come
/// from the layout the segment writer stores into, so the two can never drift apart.
pub(crate) fn resolution_fits_the_ring(resolution: &MarketResolution) -> bool {
    resolution
        .winner()
        .text_value()
        .is_some_and(|outcome| outcome.len() <= RES_OUTCOME_CAPACITY)
        && resolution.native_label().as_str().len() <= RES_TYPE_CAPACITY
        && resolution.resolution_date().as_lexeme().len() <= RES_DATE_CAPACITY
}

/// Shared with [`crate::limitless::shard`], which asks the same question once per market in
/// a set rather than once for a market. One definition of agreement serves both rails, so a
/// standby cannot be eligible under one and ineligible under the other.
pub(crate) fn agreement(shadow: &OrderBook, published: &PublishedBook) -> StandbyState {
    if !matches!(shadow.authority(), AuthorityState::Live) {
        return StandbyState::Divergent(DivergenceReason::ContinuityMismatch);
    }
    if economically_equal(&shadow.publish(), published) {
        StandbyState::Agreeing
    } else {
        StandbyState::Divergent(DivergenceReason::ContentMismatch)
    }
}

/// Whether two books describe the same economic state.
///
/// Compares the market and every canonical level's side, price, and quantity by exact
/// decimal value, so `0.50` and `0.5` are one price and `120.0` and `120` one size. Nothing
/// about how the state arrived takes part: revision, continuity, authority, provenance,
/// source timestamps, and connection metadata are all excluded, while a genuine difference
/// in level set, price, or depth is inequality.
fn economically_equal(left: &PublishedBook, right: &PublishedBook) -> bool {
    if left.market() != right.market() {
        return false;
    }
    let left_levels = left.canonical_levels();
    let right_levels = right.canonical_levels();
    left_levels.len() == right_levels.len()
        && left_levels
            .iter()
            .zip(right_levels.iter())
            .all(|(left, right)| {
                left.side() == right.side()
                    && left.price().value().cmp(right.price().value()).is_eq()
                    && left
                        .quantity()
                        .value()
                        .cmp(right.quantity().value())
                        .is_eq()
            })
}

/// Why a refused promotion cost the book its authority.
///
/// A standby holding a comparable history that disagrees is
/// [`AuthorityReason::ReplicaDivergence`]: the book lost authority because no history could
/// be selected, and that stays distinguishable from every plain connection failure. Any
/// other refusal — a standby with no accepted base, or one whose own stream broke — leaves
/// the reason the publishing connection actually ended for, exactly as a single-source
/// disconnect reports it.
///
/// Shared with [`crate::limitless::shard`], so a refused market on a shard reports the same
/// reason a refused single market does.
pub(crate) fn refusal_reason(
    verdict: &StandbyState,
    ended: ConnectionEndReason,
) -> AuthorityReason {
    match verdict {
        StandbyState::Divergent(DivergenceReason::ContentMismatch) => {
            AuthorityReason::ReplicaDivergence
        }
        _ => end_reason_mapping(ended).1,
    }
}

/// How a closed standby generation is recorded against its role.
///
/// The venue namespace refusing or dropping the subscription is a protocol failure, a
/// notice queue that overflowed past delivery is overload, and everything else is the
/// transport going away.
///
/// Shared with [`crate::limitless::shard`], which records the same three distinctions as
/// counters over a set rather than as one role's state, so redundancy disappearing reads
/// the same on both rails.
pub(crate) fn replica_failure(reason: ConnectionEndReason) -> ReplicaFailureReason {
    match reason {
        ConnectionEndReason::Terminal(TerminalFrameReason::NamespaceDisconnect)
        | ConnectionEndReason::Terminal(TerminalFrameReason::NamespaceConnectError)
        | ConnectionEndReason::HandshakeFailed
        | ConnectionEndReason::SubscribeFailed => ReplicaFailureReason::Protocol,
        ConnectionEndReason::NoticeUndeliverable => ReplicaFailureReason::Overload,
        _ => ReplicaFailureReason::Disconnect,
    }
}

/// How a closed connection generation is reported to the book.
///
/// Every connection end breaks mutation continuity for the same reason —
/// [`ContinuityReason::Reconnect`], the stream is replaced rather than continued — while
/// the authority reason stays specific, so a venue that dropped the namespace stays
/// distinguishable from a socket that died, from a local capture or task failure, and from
/// a queue that overflowed.
///
/// A generation closed because its resubscription produced no base reports
/// [`AuthorityReason::SubscriptionLost`] with the namespace-level losses: what failed is
/// the subscription rail, not the socket, which stayed readable throughout.
pub(crate) fn end_reason_mapping(
    reason: ConnectionEndReason,
) -> (ContinuityReason, AuthorityReason) {
    let authority = match reason {
        ConnectionEndReason::Terminal(TerminalFrameReason::NamespaceDisconnect)
        | ConnectionEndReason::Terminal(TerminalFrameReason::NamespaceConnectError)
        | ConnectionEndReason::ResubscribeTimedOut
        | ConnectionEndReason::SubscriptionReplaced => AuthorityReason::SubscriptionLost,
        ConnectionEndReason::CaptureFailed | ConnectionEndReason::TaskFailed => {
            AuthorityReason::LocalLoss
        }
        ConnectionEndReason::NoticeUndeliverable => AuthorityReason::Overload,
        _ => AuthorityReason::Disconnect,
    };
    (ContinuityReason::Reconnect, authority)
}

/// Whether two source topologies differ in anything a consumer needs a transition notice
/// for.
///
/// A pool's last published key changes with every publication, and it is latest state
/// rather than a transition: reporting each change as one would put a diagnostic notice on
/// the update path for every book update. Everything else about a topology — which
/// connections hold which roles, what each contributes, and whether a pool still publishes
/// — is a transition.
fn topology_changed(previous: &SourceState, next: &SourceState) -> bool {
    if previous.pool_capacity() != next.pool_capacity()
        || previous.pool_degraded() != next.pool_degraded()
    {
        return true;
    }
    if previous.pool_capacity() > 0 {
        return !previous.pool_sockets().eq(next.pool_sockets());
    }
    previous != next
}

/// Maps a rejected book input to a bounded, fixed discriminant name for counting.
pub(crate) fn book_error_key(error: &BookError) -> &'static str {
    match error {
        BookError::MarketMismatch => "book:MarketMismatch",
        BookError::UnsupportedCandidateOperation => "book:UnsupportedCandidateOperation",
        BookError::DuplicateLevelCoordinate => "book:DuplicateLevelCoordinate",
        BookError::EmptyDelta => "book:EmptyDelta",
        BookError::NoEstablishedBase => "book:NoEstablishedBase",
        BookError::ContinuityLost => "book:ContinuityLost",
        BookError::CapacityOutOfRange => "book:CapacityOutOfRange",
        BookError::CounterOverflow => "book:CounterOverflow",
        BookError::Continuity(_) => "book:Continuity",
        BookError::Provenance(_) => "book:Provenance",
        BookError::Mutation(_) => "book:Mutation",
        BookError::NonComplementablePrice(_) => "book:NonComplementablePrice",
        BookError::Complement(_) => "book:Complement",
    }
}

pub(crate) fn elapsed_nanos(start: Instant, at: Instant) -> u64 {
    u64::try_from(at.saturating_duration_since(start).as_nanos()).unwrap_or(u64::MAX)
}

/// Whether a generation that lived `lifetime` counts as a stable connection, which is the
/// only thing that restarts the backoff ladder.
pub(crate) fn resets_backoff(lifetime: Duration, stable_after: Duration) -> bool {
    lifetime >= stable_after
}

/// `initial` doubled once per consecutive failed attempt, capped at `max`.
pub(crate) fn exponential(initial: Duration, attempt: u32, max: Duration) -> Duration {
    let doublings = attempt.saturating_sub(1).min(MAX_BACKOFF_DOUBLINGS);
    initial.saturating_mul(1u32 << doublings).min(max)
}

/// Spreads a backoff over 75%..125% of `base`, so several supervisors that fail together do
/// not reconnect in lockstep.
///
/// `seed` comes from the process's own hasher state rather than a random-number dependency;
/// nothing about the daemon's correctness depends on its distribution.
pub(crate) fn jittered(base: Duration, seed: u64) -> Duration {
    let millis = u64::try_from(base.as_millis()).unwrap_or(u64::MAX);
    let per_mille = JITTER_FLOOR_PER_MILLE + (seed % JITTER_SPAN_PER_MILLE);
    Duration::from_millis(millis.saturating_mul(per_mille) / 1000)
}

pub(crate) async fn sleep_until_opt(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Resolves when a role's connection task finishes, or never when it holds none. A task
/// that ended without returning a reason is reported as [`ConnectionEndReason::TaskFailed`]
/// rather than unwrapped.
async fn join_slot(active: &mut Option<ActiveConnection>) -> ConnectionEndReason {
    match active {
        Some(active) => (&mut active.handle)
            .await
            .unwrap_or(ConnectionEndReason::TaskFailed),
        None => std::future::pending().await,
    }
}

/// Resolves when any standby role's connection task finishes, naming the role, or never
/// when none of them holds one.
///
/// Every role is polled on each wake, and a task that has finished is taken out of its role
/// before the next wait is built, so no handle is ever polled after it completed.
async fn join_any_standby(standbys: &mut [Standby]) -> (usize, ConnectionEndReason) {
    core::future::poll_fn(|context| {
        for (index, standby) in standbys.iter_mut().enumerate() {
            if let Some(active) = standby.slot.active.as_mut()
                && let Poll::Ready(joined) = Pin::new(&mut active.handle).poll(context)
            {
                return Poll::Ready((index, joined.unwrap_or(ConnectionEndReason::TaskFailed)));
            }
        }
        Poll::Pending
    })
    .await
}

/// The deadline half of an earliest-deadline wake.
fn deadline_of(earliest: Option<(Instant, usize)>) -> Option<Instant> {
    earliest.map(|(at, _)| at)
}

/// The standby role an earliest-deadline wake belongs to. The wait arm producing it
/// resolves only when a deadline existed, so the fallback names a role rather than
/// standing for one.
fn standby_slot(earliest: Option<(Instant, usize)>) -> Slot {
    Slot::Standby(earliest.map_or(0, |(_, index)| index))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Side;
    use crate::limitless::decode_event;
    use crate::wire::lexical::LexicalLimits;
    use crate::wire::socketio::{WebSocketOpcode, decode_frame};

    const TEST_SLUG: &str = "btc-up-or-down-5-min-1788172500";

    fn harness(replicas: usize) -> Supervisor {
        Supervisor::new(SupervisorConfig {
            market: TEST_SLUG.to_owned(),
            replicas,
            ..SupervisorConfig::default()
        })
        .expect("the harness configuration is valid")
    }

    /// A pacer key no other test shares.
    ///
    /// [`COMMAND_PACERS`] is process-wide and a reservation advances its slot into the
    /// future, so a test that reused another test's endpoint would stand in that test's
    /// queue. Keying the slot to one test's own label keeps each test's schedule its own;
    /// the labels in this module stay far inside [`MAX_PACED_ENDPOINTS`].
    fn test_endpoint(label: &str) -> String {
        format!("ws://127.0.0.1/{label}")
    }

    /// One venue `orderbookUpdate` for the harness market, decoded exactly as a connection
    /// decodes it, so nothing here is a hand-built candidate the wire could not produce.
    fn update(size: &str, version: u64) -> LimitlessEvent {
        let text = format!(
            "42/markets,[\"orderbookUpdate\",{{\"marketSlug\":\"{TEST_SLUG}\",\"orderbook\":\
             {{\"bids\":[{{\"price\":0.5,\"size\":{size}}}],\"asks\":[{{\"price\":0.6,\"size\":200}}]}},\
             \"timestamp\":\"2026-08-31T00:00:00.000Z\",\"version\":{version}}}]"
        );
        let frame = decode_frame(
            text.as_bytes(),
            WebSocketOpcode::Text,
            LexicalLimits::venue_payload(),
        )
        .expect("the harness frame decodes");
        decode_event(&frame).expect("the harness event decodes")
    }

    fn event_notice(generation: u64, event: LimitlessEvent) -> ConnectionNotice {
        event_notice_under(generation, 1, event)
    }

    fn event_notice_under(
        generation: u64,
        subscription_generation: u64,
        event: LimitlessEvent,
    ) -> ConnectionNotice {
        ConnectionNotice {
            generation,
            note: ConnectionNote::Event {
                event,
                received_at: Instant::now(),
                arrival_time_nanos: harness_arrival_nanos(),
                subscription_generation,
            },
        }
    }

    /// The wall-clock stamp a real connection would have taken at the same socket read this
    /// harness's decoded frame stands in for.
    fn harness_arrival_nanos() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX))
            .unwrap_or(0)
    }

    /// A connection standing in for a live socket: its task never ends, and its control
    /// receiver is handed back so a test can see, and stall, what the supervisor commands.
    ///
    /// `heartbeat_deadline` is zero, so the connection's evidence expires the instant it is
    /// taken: a test puts the deadline in the past by stamping `last_heartbeat` rather than
    /// by waiting for one, and refutes it with a ping stamped later. With no heartbeat
    /// stamped at all there is no evidence and no expiry, which is the state a freshly
    /// spawned connection is in.
    fn live_connection(generation: u64) -> (ActiveConnection, mpsc::Receiver<ConnectionControl>) {
        connection(generation, tokio::spawn(std::future::pending()))
    }

    async fn ended_connection(
        generation: u64,
    ) -> (ActiveConnection, mpsc::Receiver<ConnectionControl>) {
        let (active, commands) = connection(
            generation,
            tokio::spawn(async { ConnectionEndReason::SocketClosed }),
        );
        while !active.handle.is_finished() {
            tokio::task::yield_now().await;
        }
        (active, commands)
    }

    fn connection(
        generation: u64,
        handle: JoinHandle<ConnectionEndReason>,
    ) -> (ActiveConnection, mpsc::Receiver<ConnectionControl>) {
        let (control, commands) = mpsc::channel(1);
        (
            ActiveConnection {
                generation,
                spawned_at: Instant::now(),
                handle,
                control,
                heartbeat_deadline: Some(Duration::ZERO),
                last_heartbeat: None,
                subscription_generation: 1,
                produced_base: false,
                recovery: None,
            },
            commands,
        )
    }

    fn expire_heartbeat(active: &mut ActiveConnection) {
        active.last_heartbeat = Some(Instant::now());
    }

    fn bid_sizes(published: &PublishedBook) -> Vec<String> {
        published
            .canonical_levels()
            .iter()
            .filter(|level| level.side() == Side::Bid)
            .map(|level| level.quantity().value().canonical())
            .collect()
    }

    fn install_standby(supervisor: &mut Supervisor, active: ActiveConnection) {
        install_standby_at(supervisor, 0, active);
    }

    /// Seats a connection in the standby role at `index`, so a ladder deeper than one
    /// standby can be built the same way the two-replica tests build their single one.
    fn install_standby_at(supervisor: &mut Supervisor, index: usize, active: ActiveConnection) {
        let generation = active.generation;
        let standby = supervisor
            .standbys
            .get_mut(index)
            .expect("the harness runs a standby role at this index");
        standby.assigned = ConnectionIdentity::new(CONNECTION_NAME, generation).ok();
        standby.slot.active = Some(active);
    }

    fn pooled_harness(sockets: usize) -> Supervisor {
        Supervisor::new(SupervisorConfig {
            market: TEST_SLUG.to_owned(),
            replicas: sockets,
            pooled: true,
            ..SupervisorConfig::default()
        })
        .expect("the pooled harness configuration is valid")
    }

    /// The last source topology the tap carries, which is what a consumer would be holding.
    fn last_source(tap: &mut mpsc::Receiver<SupervisorNotice>) -> Option<SourceState> {
        let mut last = None;
        while let Ok(notice) = tap.try_recv() {
            if let SupervisorNotice::SourceTransition { source } = notice {
                last = Some(source);
            }
        }
        last
    }

    fn publishing_generations(tap: &mut mpsc::Receiver<SupervisorNotice>) -> Vec<u64> {
        let mut publishing = Vec::new();
        while let Ok(notice) = tap.try_recv() {
            if let SupervisorNotice::SourceTransition { source } = notice
                && let Some(primary) = source.publishing_primary()
            {
                publishing.push(primary.connection().generation());
            }
        }
        publishing
    }

    #[tokio::test]
    async fn a_frame_queued_when_the_heartbeat_deadline_fires_never_reaches_the_book() {
        let mut supervisor = harness(1);
        let (active, _commands) = live_connection(1);
        supervisor.primary.active = Some(active);
        supervisor.on_notice(event_notice(1, update("100", 1)));
        let base = supervisor.writer.published();
        assert_eq!(base.authority(), &AuthorityState::Live);

        expire_heartbeat(
            supervisor
                .primary
                .active
                .as_mut()
                .expect("the harness connection is installed"),
        );
        supervisor
            .notices_tx
            .try_send(event_notice(1, update("999", 2)))
            .expect("the ingest queue has room for the frame read at the deadline");
        supervisor.on_heartbeat_missed(Slot::Primary);

        let after = supervisor.writer.published();
        assert_eq!(
            supervisor.stats.snapshots_applied, 1,
            "the frame queued when the deadline fired was applied to the book"
        );
        assert_eq!(
            supervisor.stats.fenced_events, 1,
            "it must be discarded as the late work of a generation already closed"
        );
        assert_eq!(bid_sizes(&after), vec!["100".to_owned()]);
        assert_eq!(
            after.authority(),
            &AuthorityState::Stale(AuthorityReason::Disconnect),
            "staleness lands exactly as it did before"
        );
        assert!(supervisor.primary.active.is_none());
    }

    #[tokio::test]
    async fn queued_ping_evidence_refutes_the_deadline_and_the_queued_frame_commits() {
        let mut supervisor = harness(1);
        let (active, _commands) = live_connection(1);
        supervisor.primary.active = Some(active);
        supervisor.on_notice(event_notice(1, update("100", 1)));
        expire_heartbeat(
            supervisor
                .primary
                .active
                .as_mut()
                .expect("the harness connection is installed"),
        );

        supervisor
            .notices_tx
            .try_send(ConnectionNotice {
                generation: 1,
                note: ConnectionNote::Heartbeat {
                    observed_at: Instant::now() + Duration::from_secs(60),
                },
            })
            .expect("the ingest queue has room for the queued ping");
        supervisor
            .notices_tx
            .try_send(event_notice(1, update("150", 2)))
            .expect("the ingest queue has room for the queued frame");
        supervisor.on_heartbeat_missed(Slot::Primary);

        assert!(
            supervisor.primary.active.is_some(),
            "queued ping evidence refutes the deadline"
        );
        assert_eq!(supervisor.stats.snapshots_applied, 2);
        assert_eq!(supervisor.stats.fenced_events, 0);
        assert_eq!(bid_sizes(&supervisor.writer.published()), vec!["150"]);
    }

    #[tokio::test]
    async fn a_snapshot_is_stamped_with_the_emit_it_arrived_under() {
        let mut supervisor = harness(1);
        let (active, _commands) = live_connection(1);
        supervisor.primary.active = Some(active);

        supervisor.on_notice(event_notice_under(1, 2, update("100", 1)));

        assert_eq!(
            supervisor
                .primary
                .active
                .as_ref()
                .map(|active| active.subscription_generation),
            Some(1),
            "the notice that announces the re-emit has not been applied yet, which is the \
             window a supervisor-side stamp would get wrong"
        );
        assert_eq!(
            supervisor
                .writer
                .published()
                .provenance()
                .expect("a committed revision carries provenance")
                .subscription_generation(),
            2,
            "a snapshot is stamped with the emit the connection read it under"
        );
    }

    #[tokio::test]
    async fn a_standby_whose_task_has_ended_is_not_promoted() {
        let (tap_tx, mut tap) = mpsc::channel(64);
        let mut supervisor = harness(2).with_diagnostics(tap_tx);
        let (primary, _primary_commands) = live_connection(1);
        supervisor.primary.active = Some(primary);
        let (standby, _standby_commands) = ended_connection(2).await;
        install_standby(&mut supervisor, standby);
        supervisor.on_notice(event_notice(1, update("100", 1)));
        supervisor.on_notice(event_notice(2, update("100", 1)));
        assert_eq!(
            supervisor
                .standbys
                .first()
                .expect("the harness runs a standby role")
                .state,
            StandbyState::Agreeing,
            "the shadow agrees, which is everything but the transport"
        );

        supervisor.primary.active = None;
        supervisor.on_primary_ended(
            ConnectionEndReason::SocketClosed,
            true,
            Duration::from_secs(5),
        );

        assert_eq!(
            supervisor.stats.promotions, 0,
            "an agreeing shadow on a task that has ended is not a promotable source"
        );
        assert_eq!(supervisor.stats.promotions_refused, 1);
        assert_eq!(
            supervisor.writer.published().authority(),
            &AuthorityState::Stale(AuthorityReason::Disconnect),
            "authority is surrendered and a fresh venue base awaited"
        );
        assert!(
            supervisor.primary.reconnect_at.is_some(),
            "recovery falls back to replacing the publishing connection"
        );
        assert!(
            !publishing_generations(&mut tap).contains(&2),
            "no consumer may see a source transition naming the dead standby as publishing"
        );
    }

    #[tokio::test]
    async fn a_standby_whose_heartbeat_expired_is_settled_before_the_switch_is_decided() {
        let (tap_tx, mut tap) = mpsc::channel(64);
        let mut supervisor = harness(2).with_diagnostics(tap_tx);
        let (primary, _primary_commands) = live_connection(1);
        supervisor.primary.active = Some(primary);
        let (mut standby, _standby_commands) = live_connection(2);
        expire_heartbeat(&mut standby);
        install_standby(&mut supervisor, standby);
        supervisor.on_notice(event_notice(1, update("100", 1)));
        supervisor.on_notice(event_notice(2, update("100", 1)));
        assert_eq!(
            supervisor
                .standbys
                .first()
                .expect("the harness runs a standby role")
                .state,
            StandbyState::Agreeing
        );

        supervisor.primary.active = None;
        supervisor.on_primary_ended(
            ConnectionEndReason::SocketClosed,
            true,
            Duration::from_secs(5),
        );

        assert_eq!(supervisor.stats.promotions, 0);
        assert_eq!(
            supervisor.stats.fenced_generations, 1,
            "the standby's own expiry is settled before the loss is decided"
        );
        assert!(
            supervisor
                .standbys
                .first()
                .and_then(|standby| standby.slot.active.as_ref())
                .is_none()
        );
        assert_eq!(
            supervisor.writer.published().authority(),
            &AuthorityState::Stale(AuthorityReason::Disconnect)
        );
        assert!(!publishing_generations(&mut tap).contains(&2));
    }

    #[tokio::test]
    async fn a_standby_that_stops_being_viable_after_the_decision_is_not_a_promotion() {
        let (tap_tx, mut tap) = mpsc::channel(64);
        let mut supervisor = harness(2).with_diagnostics(tap_tx);
        let (primary, _primary_commands) = live_connection(1);
        supervisor.primary.active = Some(primary);
        let (standby, _standby_commands) = live_connection(2);
        install_standby(&mut supervisor, standby);
        supervisor.on_notice(event_notice(1, update("100", 1)));
        supervisor.on_notice(event_notice(2, update("100", 1)));

        let switch = supervisor.decide_switch(ConnectionEndReason::SocketClosed);
        assert!(
            matches!(switch, SourceSwitch::Promote(0)),
            "the standby is promotable at the instant the publishing loss is decided"
        );
        supervisor.primary.active = None;
        expire_heartbeat(
            supervisor
                .standbys
                .first_mut()
                .and_then(|standby| standby.slot.active.as_mut())
                .expect("the harness standby is installed"),
        );
        supervisor.apply_switch(switch, ConnectionEndReason::SocketClosed);

        assert_eq!(
            supervisor.stats.promotions, 0,
            "a takeover that could not be completed is not a promotion"
        );
        assert_eq!(
            supervisor.writer.published().authority(),
            &AuthorityState::Stale(AuthorityReason::Disconnect),
            "the book must not stay live behind a publishing role nothing took over"
        );
        assert!(
            supervisor.primary.active.is_none(),
            "nothing was installed as the publishing connection"
        );
        assert_eq!(
            supervisor.stats.promotions_refused, 1,
            "a standby that was no longer eligible at the takeover is a refusal"
        );
        assert!(
            supervisor.primary.reconnect_at.is_some(),
            "recovery falls back to replacing the publishing connection"
        );
        assert!(
            !publishing_generations(&mut tap).contains(&2),
            "no consumer may see a source transition naming the standby as publishing"
        );
    }

    /// A full control channel is a connection that is alive but has not consumed a command
    /// already queued for it.
    ///
    /// This state machine cannot produce one — it emits from [`RecoveryAttempt::Pending`]
    /// alone, leaves that state on a landed handover, and its only route back takes the
    /// connection and its channel with it — so the queued command here is hand-placed. What
    /// the test pins is the answer given if it ever did arise: a connection that cannot take
    /// work gets the same absence deadline one that never writes gets, and its expiry
    /// escalates to a fresh connection. No retry timer of its own, and nothing on the wire.
    #[tokio::test]
    async fn a_full_control_channel_arms_the_absence_deadline_rather_than_a_retry() {
        let mut supervisor = harness(1);
        let (active, _commands) = live_connection(1);
        supervisor.primary.active = Some(active);
        supervisor.on_notice(event_notice(1, update("100", 1)));
        supervisor.report_loss(ContinuityReason::LocalLoss, AuthorityReason::LocalLoss);
        supervisor
            .primary
            .active
            .as_ref()
            .expect("the harness connection is installed")
            .control
            .try_send(ConnectionControl::Resubscribe {
                granted_at: Instant::now(),
            })
            .expect("the control channel takes one command");

        supervisor.arm_recovery();
        supervisor.emit_resubscribe();
        assert!(
            matches!(
                supervisor
                    .primary
                    .active
                    .as_ref()
                    .and_then(|active| active.recovery),
                Some(RecoveryAttempt::Requested(_))
            ),
            "a connection that cannot take work is given the deadline a connection that \
             cannot write is given"
        );
        assert_eq!(
            supervisor.stats.resubscribes_emitted, 0,
            "a command the channel refused is not one the venue was sent"
        );
    }

    /// A command the connection took waits for the connection to put it on the wire, and the
    /// venue's answering window opens only when the connection says it did.
    #[tokio::test]
    async fn an_accepted_resubscribe_waits_for_the_wire_before_the_venue_owes_an_answer() {
        let mut supervisor = harness(1);
        let (active, _commands) = live_connection(1);
        supervisor.primary.active = Some(active);
        supervisor.on_notice(event_notice(1, update("100", 1)));
        supervisor.report_loss(ContinuityReason::LocalLoss, AuthorityReason::LocalLoss);

        supervisor.arm_recovery();
        supervisor.emit_resubscribe();
        assert!(
            matches!(
                supervisor
                    .primary
                    .active
                    .as_ref()
                    .and_then(|active| active.recovery),
                Some(RecoveryAttempt::Requested(_))
            ),
            "an accepted command waits for the connection to put it on the wire"
        );
        assert_eq!(
            supervisor.stats.resubscribes_emitted, 0,
            "nothing is on the wire until the connection says so"
        );

        supervisor.on_notice(ConnectionNotice {
            generation: 1,
            note: ConnectionNote::Resubscribed {
                subscription_generation: 2,
            },
        });
        assert!(
            matches!(
                supervisor
                    .primary
                    .active
                    .as_ref()
                    .and_then(|active| active.recovery),
                Some(RecoveryAttempt::Awaiting(_))
            ),
            "the venue's answering window opens when the command reaches the wire"
        );
        assert_eq!(supervisor.stats.resubscribes_emitted, 1);
    }

    /// A whole fleet recovering at once is one queue at one pacer, and every deadline in it
    /// has to budget for the commands standing ahead of it.
    ///
    /// [`COMMAND_PACERS`] is keyed by the endpoint every connection of one daemon shares, so
    /// `n` supervisors entering recovery together serialize: the `k`-th command cannot be
    /// written before `k` intervals have passed, however healthy its connection is. A
    /// deadline that allowed one interval regardless of `k` expired while the command was
    /// still queued behind this daemon's own pacing, and the tail of the fleet was fenced and
    /// redialled for a delay the venue had no part in — and the replacements re-entered the
    /// same queue. The deadline is therefore taken from the reservation the command actually
    /// holds.
    ///
    /// Eight peers at a hundred-millisecond floor put the last command seven hundred
    /// milliseconds out, which no window this test could pick would cover on its own. Every
    /// assertion carries the whole window as slack against scheduling jitter, so the test
    /// measures the arithmetic rather than the machine it runs on.
    #[tokio::test]
    async fn a_fleet_recovering_together_budgets_for_the_queue_each_command_stands_in() {
        const PEERS: u32 = 8;
        const INTERVAL: Duration = Duration::from_millis(100);
        const WINDOW: Duration = Duration::from_millis(150);
        let endpoint = test_endpoint("a-fleet-recovering-together");

        let started = Instant::now();
        let mut fleet = Vec::new();
        for peer in 0..PEERS {
            let mut supervisor = Supervisor::new(SupervisorConfig {
                market: TEST_SLUG.to_owned(),
                endpoint: endpoint.clone(),
                resubscribe_window: WINDOW,
                min_command_interval_ms: u64::try_from(INTERVAL.as_millis())
                    .expect("the test floor fits"),
                ..SupervisorConfig::default()
            })
            .expect("the fleet configuration is valid");
            let generation = u64::from(peer) + 1;
            let (active, commands) = live_connection(generation);
            supervisor.primary.active = Some(active);
            supervisor.on_notice(event_notice(generation, update("100", 1)));
            supervisor.report_loss(ContinuityReason::LocalLoss, AuthorityReason::LocalLoss);
            supervisor.arm_recovery();
            fleet.push((supervisor, commands, generation));
        }

        for (supervisor, _, _) in &mut fleet {
            supervisor.emit_resubscribe();
        }

        for (peer, (supervisor, _, _)) in fleet.iter().enumerate() {
            let deadline = match supervisor
                .primary
                .active
                .as_ref()
                .and_then(|active| active.recovery)
            {
                Some(RecoveryAttempt::Requested(at)) => at,
                other => panic!("peer {peer} owes a write, not {other:?}"),
            };
            let queued_ahead = INTERVAL * u32::try_from(peer).expect("a peer index fits");
            assert!(
                deadline >= started + queued_ahead + WINDOW,
                "peer {peer} cannot write before {queued_ahead:?} of pacing has passed, so a \
                 deadline at {:?} after the fleet started blames the venue for this daemon's \
                 own queue",
                deadline.saturating_duration_since(started)
            );
        }

        let mut granted = Vec::new();
        for (peer, (_, commands, _)) in fleet.iter_mut().enumerate() {
            match commands.try_recv() {
                Ok(ConnectionControl::Resubscribe { granted_at }) => granted.push(granted_at),
                other => panic!("peer {peer} handed its connection no command to write: {other:?}"),
            }
        }
        for (peer, writes_at) in granted.iter().enumerate() {
            let queued_ahead = INTERVAL * u32::try_from(peer).expect("a peer index fits");
            assert_eq!(
                *writes_at,
                granted[0] + queued_ahead,
                "peer {peer} holds the {peer}-th place in one queue, spaced by the floor"
            );
        }

        for (peer, (supervisor, _, generation)) in fleet.iter_mut().enumerate() {
            tokio::time::sleep_until(granted[peer]).await;
            supervisor.on_recovery_due();
            assert!(
                matches!(
                    supervisor
                        .primary
                        .active
                        .as_ref()
                        .and_then(|active| active.recovery),
                    Some(RecoveryAttempt::Requested(_))
                ),
                "peer {peer} is still waiting at the pacer and must not be given up on"
            );
            supervisor.on_notice(ConnectionNotice {
                generation: *generation,
                note: ConnectionNote::Resubscribed {
                    subscription_generation: 2,
                },
            });
            assert!(
                matches!(
                    supervisor
                        .primary
                        .active
                        .as_ref()
                        .and_then(|active| active.recovery),
                    Some(RecoveryAttempt::Awaiting(_))
                ),
                "peer {peer}'s command reached the wire, so the venue now owes an answer"
            );
        }

        for (peer, (supervisor, _, _)) in fleet.iter().enumerate() {
            assert_eq!(
                supervisor.stats.resubscribes_escalated, 0,
                "peer {peer} was fenced for this daemon's own pacing"
            );
            assert_eq!(
                supervisor.stats.resubscribes_emitted, 1,
                "peer {peer}'s reissue reached the wire"
            );
        }
    }

    /// A handover to a connection whose task has ended arms no timer at all.
    ///
    /// The channel is closed, which only a connection whose task is ending does, so the
    /// event that recovers this is that connection's end — already a wake the run loop is
    /// waiting on. Scheduling a retry instead would be the supervisor polling for news the
    /// join is about to deliver, and with a configured floor of zero that retry would be due
    /// the instant it was computed.
    #[tokio::test]
    async fn a_resubscribe_the_connection_cannot_take_arms_no_timer() {
        let mut supervisor = Supervisor::new(SupervisorConfig {
            market: TEST_SLUG.to_owned(),
            min_command_interval_ms: 0,
            ..SupervisorConfig::default()
        })
        .expect("a zero command floor is a valid operator choice");
        let (active, commands) = ended_connection(1).await;
        drop(commands);
        supervisor.primary.active = Some(active);

        supervisor.emit_resubscribe();

        let recovery = supervisor
            .primary
            .active
            .as_ref()
            .and_then(|active| active.recovery);
        assert!(
            matches!(recovery, Some(RecoveryAttempt::Unreachable)),
            "a handover to a closed channel leaves no attempt in flight, got {recovery:?}"
        );
        assert_eq!(
            recovery.and_then(RecoveryAttempt::wake_at),
            None,
            "nothing about a gone connection is worth waking for on a clock"
        );
        assert_eq!(
            supervisor.stats.resubscribes_emitted, 0,
            "a command that never left is not one the venue was sent"
        );
    }

    /// An unreachable attempt is left alone rather than re-armed, so the state cannot become
    /// a retry loop by way of the loop's own re-arming pass.
    #[tokio::test]
    async fn an_unreachable_recovery_attempt_is_not_re_armed() {
        let mut supervisor = harness(1);
        let (active, commands) = ended_connection(1).await;
        drop(commands);
        supervisor.primary.active = Some(active);
        supervisor.on_notice(event_notice(1, update("100", 1)));
        supervisor.report_loss(ContinuityReason::LocalLoss, AuthorityReason::LocalLoss);
        supervisor
            .primary
            .active
            .as_mut()
            .expect("the harness connection is installed")
            .recovery = Some(RecoveryAttempt::Unreachable);

        supervisor.arm_recovery();

        assert!(
            matches!(
                supervisor
                    .primary
                    .active
                    .as_ref()
                    .and_then(|active| active.recovery),
                Some(RecoveryAttempt::Unreachable)
            ),
            "an attempt waiting on the connection's end is not given a clock"
        );
    }

    #[tokio::test]
    async fn an_exhausted_recovery_keeps_its_terminal_reason_through_later_failures() {
        let mut supervisor = harness(1);
        let (active, _commands) = live_connection(1);
        supervisor.primary.active = Some(active);
        supervisor.on_notice(event_notice(1, update("100", 1)));
        supervisor.config.max_recovery_attempts = 1;

        supervisor.primary.active = None;
        supervisor.on_primary_ended(ConnectionEndReason::SocketClosed, false, Duration::ZERO);
        assert_eq!(
            supervisor.writer.published().authority(),
            &AuthorityState::Stale(AuthorityReason::RecoveryBaseUnavailable)
        );
        assert_eq!(supervisor.stats.recovery_base_unavailable, 1);

        supervisor.on_primary_ended(ConnectionEndReason::SocketClosed, false, Duration::ZERO);
        assert_eq!(
            supervisor.writer.published().authority(),
            &AuthorityState::Stale(AuthorityReason::RecoveryBaseUnavailable),
            "a later failed retry must not present the standing terminal fact as a disconnect"
        );
        assert_eq!(
            supervisor.stats.recovery_base_unavailable, 1,
            "the notification stays one-time while the condition stands"
        );
    }

    #[tokio::test]
    async fn a_role_movement_over_a_stale_book_never_fabricates_a_recovery_base() {
        let mut supervisor = pooled_harness(2);
        supervisor.config.max_recovery_attempts = 1;
        let (primary, _primary_commands) = live_connection(1);
        supervisor.primary.active = Some(primary);
        let (standby, _standby_commands) = live_connection(2);
        install_standby(&mut supervisor, standby);
        supervisor.on_notice(event_notice(1, update("100", 10)));
        assert_eq!(
            supervisor.writer.published().authority(),
            &AuthorityState::Live
        );

        supervisor.report_loss(ContinuityReason::LocalLoss, AuthorityReason::LocalLoss);
        supervisor.primary.active = None;
        supervisor.on_primary_ended(ConnectionEndReason::SocketClosed, false, Duration::ZERO);

        let moved = supervisor
            .primary
            .active
            .as_ref()
            .expect("a surviving socket fills the publishing slot");
        assert_eq!(moved.generation, 2);
        assert!(
            !moved.produced_base,
            "a socket moved into the publishing slot over a stale book has produced nothing, \
             whatever the book once held"
        );
        assert_eq!(
            supervisor.recovery_attempts, 1,
            "the failed attempt must be counted, not absorbed by the role change"
        );
        assert_eq!(
            supervisor.writer.published().authority(),
            &AuthorityState::Stale(AuthorityReason::RecoveryBaseUnavailable),
            "a truthful terminal reason must not be postponable by moving roles"
        );
        assert_eq!(supervisor.stats.recovery_base_unavailable, 1);
    }

    #[tokio::test]
    async fn a_role_movement_under_a_live_book_carries_the_base_it_is_continuing() {
        let mut supervisor = pooled_harness(2);
        let (primary, _primary_commands) = live_connection(1);
        supervisor.primary.active = Some(primary);
        let (standby, _standby_commands) = live_connection(2);
        install_standby(&mut supervisor, standby);
        supervisor.on_notice(event_notice(1, update("100", 10)));

        supervisor.primary.active = None;
        supervisor.on_primary_ended(ConnectionEndReason::SocketClosed, true, Duration::ZERO);

        let moved = supervisor
            .primary
            .active
            .as_ref()
            .expect("a surviving socket fills the publishing slot");
        assert!(
            moved.produced_base,
            "the book is live, so the socket continuing it arrives holding that base"
        );
        assert_eq!(supervisor.recovery_attempts, 0);
        assert_eq!(supervisor.stats.recovery_base_unavailable, 0);
        assert_eq!(
            supervisor.writer.published().authority(),
            &AuthorityState::Live
        );
    }

    #[tokio::test]
    async fn a_local_loss_on_any_armed_pool_socket_breaks_the_books_continuity() {
        for note in [
            ConnectionNote::Overload { dropped: 3 },
            ConnectionNote::DecodeFailure {
                key: "wire:Frame",
                book_relevant: true,
            },
        ] {
            let mut supervisor = pooled_harness(2);
            let (primary, _primary_commands) = live_connection(1);
            supervisor.primary.active = Some(primary);
            let (standby, _standby_commands) = live_connection(2);
            install_standby(&mut supervisor, standby);
            supervisor.on_notice(event_notice(1, update("100", 10)));
            assert_eq!(
                supervisor.writer.published().authority(),
                &AuthorityState::Live
            );

            supervisor.on_notice(ConnectionNotice {
                generation: 2,
                note: note.clone(),
            });

            assert!(
                matches!(
                    supervisor.writer.published().authority(),
                    AuthorityState::Stale(_)
                ),
                "every socket of an armed pool is an authoritative source, so a frame it \
                 received and lost is a hole in the book's history: {note:?}"
            );
            assert_eq!(supervisor.stats.continuity_losses, 1);
        }
    }

    #[tokio::test]
    async fn the_same_local_loss_on_a_hot_standby_costs_only_its_shadow() {
        let mut supervisor = harness(2);
        let (primary, _primary_commands) = live_connection(1);
        supervisor.primary.active = Some(primary);
        let (standby, _standby_commands) = live_connection(2);
        install_standby(&mut supervisor, standby);
        supervisor.on_notice(event_notice(1, update("100", 10)));

        supervisor.on_notice(ConnectionNotice {
            generation: 2,
            note: ConnectionNote::Overload { dropped: 3 },
        });

        assert_eq!(
            supervisor.writer.published().authority(),
            &AuthorityState::Live,
            "a standby publishes nothing, so what it lost was never the book's history"
        );
        assert_eq!(supervisor.stats.continuity_losses, 0);
    }

    #[tokio::test]
    async fn a_failed_standby_stays_named_in_the_topology_until_its_replacement_spawns() {
        for (ended, expected) in [
            (
                ConnectionEndReason::SocketClosed,
                ReplicaFailureReason::Disconnect,
            ),
            (
                ConnectionEndReason::Terminal(TerminalFrameReason::NamespaceDisconnect),
                ReplicaFailureReason::Protocol,
            ),
            (
                ConnectionEndReason::NoticeUndeliverable,
                ReplicaFailureReason::Overload,
            ),
        ] {
            let (tap_tx, mut tap) = mpsc::channel(64);
            let mut supervisor = harness(2).with_diagnostics(tap_tx);
            let (primary, _primary_commands) = live_connection(1);
            supervisor.primary.active = Some(primary);
            let (standby, _standby_commands) = live_connection(2);
            install_standby(&mut supervisor, standby);
            supervisor.on_notice(event_notice(1, update("100", 10)));

            supervisor.standbys[0].slot.active = None;
            supervisor.on_standby_ended(0, ended, Duration::ZERO);

            let source = last_source(&mut tap).expect("the ending is reported as a transition");
            let named = source
                .standbys()
                .map(|(standby, state)| (standby.connection().generation(), state.clone()))
                .next();
            assert_eq!(
                named,
                Some((2, StandbyState::Failed(expected.clone()))),
                "a coverage shortfall must name the connection and why it failed, not \
                 collapse into an anonymous gap: {ended:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_degrade_whose_preferred_socket_is_gone_leaves_the_publishing_role_where_it_is() {
        let (tap_tx, mut tap) = mpsc::channel(64);
        let mut supervisor = pooled_harness(2).with_diagnostics(tap_tx);
        let (primary, _primary_commands) = live_connection(1);
        supervisor.primary.active = Some(primary);
        let (standby, _standby_commands) = live_connection(2);
        install_standby(&mut supervisor, standby);
        supervisor.on_notice(event_notice(1, update("100", 10)));
        supervisor.on_notice(event_notice(2, update("200", 11)));
        assert_eq!(
            supervisor.pool.as_ref().and_then(PoolGate::last_publisher),
            Some(1),
            "the socket that last published is the one the degrade would prefer"
        );

        supervisor.standbys[0].slot.active = None;
        supervisor.on_notice(event_notice(1, update("300", 9)));

        assert_eq!(
            supervisor.pool.as_ref().and_then(PoolGate::degraded),
            Some(PoolDegradeReason::ConnectionInversion)
        );
        let source = last_source(&mut tap).expect("the degrade reports a topology");
        assert_eq!(
            source
                .publishing_primary()
                .map(|primary| primary.connection().generation()),
            Some(1),
            "the preferred socket cannot take a role it is not there to take, so the \
             publishing role stays with the connection that can still deliver"
        );
        assert_eq!(
            bid_sizes(&supervisor.writer.published()),
            vec!["200"],
            "the arrival that withdrew the licence reached no book"
        );
    }

    #[tokio::test]
    async fn an_armed_pool_session_lost_to_an_overflowed_queue_breaks_the_books_continuity() {
        for slot in [Slot::Primary, Slot::Standby(0)] {
            let mut supervisor = pooled_harness(2);
            let (primary, _primary_commands) = live_connection(1);
            supervisor.primary.active = Some(primary);
            let (standby, _standby_commands) = live_connection(2);
            install_standby(&mut supervisor, standby);
            supervisor.on_notice(event_notice(1, update("100", 10)));
            assert_eq!(
                supervisor.writer.published().authority(),
                &AuthorityState::Live
            );

            if let Some(target) = supervisor.slot_mut(slot) {
                target.active = None;
            }
            supervisor.on_slot_ended(
                slot,
                ConnectionEndReason::NoticeUndeliverable,
                false,
                true,
                Duration::ZERO,
            );

            assert_eq!(
                supervisor.writer.published().authority(),
                &AuthorityState::Stale(AuthorityReason::Overload),
                "the notice this session could not hand over may have been the loss report \
                 itself, so its end is the loss: {slot:?}"
            );
            assert_eq!(supervisor.stats.continuity_losses, 1);
        }
    }

    #[tokio::test]
    async fn a_pool_session_that_never_subscribed_loses_the_book_nothing_when_it_overflows() {
        let mut supervisor = pooled_harness(2);
        let (primary, _primary_commands) = live_connection(1);
        supervisor.primary.active = Some(primary);
        let (standby, _standby_commands) = live_connection(2);
        install_standby(&mut supervisor, standby);
        supervisor.on_notice(event_notice(1, update("100", 10)));

        supervisor.standbys[0].slot.active = None;
        supervisor.on_slot_ended(
            Slot::Standby(0),
            ConnectionEndReason::NoticeUndeliverable,
            false,
            false,
            Duration::ZERO,
        );

        assert_eq!(
            supervisor.writer.published().authority(),
            &AuthorityState::Live,
            "a connection that never reached its subscription had no book state to lose"
        );
        assert_eq!(supervisor.stats.continuity_losses, 0);
    }

    #[tokio::test]
    async fn a_live_pool_spends_no_recovery_attempt_when_a_non_producing_socket_ends() {
        let mut supervisor = pooled_harness(2);
        supervisor.config.max_recovery_attempts = 1;
        let (primary, _primary_commands) = live_connection(1);
        supervisor.primary.active = Some(primary);
        let (standby, _standby_commands) = live_connection(2);
        install_standby(&mut supervisor, standby);
        supervisor.on_notice(event_notice(2, update("100", 10)));
        assert_eq!(
            supervisor.writer.published().authority(),
            &AuthorityState::Live
        );

        supervisor.primary.active = None;
        supervisor.on_primary_ended(ConnectionEndReason::SocketClosed, false, Duration::ZERO);

        assert_eq!(
            supervisor.writer.published().authority(),
            &AuthorityState::Live,
            "the publishing slot is a structural role, not the source of the book's \
             authority: a socket that published nothing while others kept the book live \
             has failed at no recovery"
        );
        assert_eq!(supervisor.recovery_attempts, 0);
        assert_eq!(supervisor.stats.recovery_base_unavailable, 0);
        assert_eq!(supervisor.stats.continuity_losses, 0);
    }

    #[tokio::test]
    async fn a_stale_pool_recovers_from_a_redelivery_of_the_key_it_already_published() {
        let mut supervisor = pooled_harness(2);
        let (primary, _primary_commands) = live_connection(1);
        supervisor.primary.active = Some(primary);
        let (standby, _standby_commands) = live_connection(2);
        install_standby(&mut supervisor, standby);
        supervisor.on_notice(event_notice(1, update("100", 10)));
        let base = supervisor.writer.published();
        assert_eq!(base.authority(), &AuthorityState::Live);

        supervisor.report_loss(ContinuityReason::LocalLoss, AuthorityReason::Overload);
        assert_eq!(
            supervisor.writer.published().authority(),
            &AuthorityState::Stale(AuthorityReason::Overload)
        );

        supervisor.on_notice(event_notice(1, update("100", 10)));

        let recovered = supervisor.writer.published();
        assert_eq!(
            recovered.authority(),
            &AuthorityState::Live,
            "the venue's observed answer to a resubscribe is the same frame again, so a \
             stale pool that could not use it would wait for a higher key a quiet market \
             need never produce"
        );
        assert_eq!(
            recovered.continuity().epoch(),
            base.continuity().epoch() + 1,
            "it is installed under the ordinary recovery-base contract"
        );
        assert_eq!(bid_sizes(&recovered), vec!["100"]);
        let gate = supervisor.pool.as_ref().expect("the harness runs a pool");
        assert!(gate.is_armed());
        assert_eq!(
            gate.last_published().map(DedupKey::to_string),
            Some("10".to_owned()),
            "nothing newer than that key was published, so the floor does not move"
        );
        assert_eq!(gate.published(), 2);
        assert_eq!(
            gate.duplicate_drops(),
            0,
            "the arrival was used, not dropped, so the two totals stay a partition"
        );
    }

    #[tokio::test]
    async fn a_redelivery_of_the_published_key_carrying_other_content_still_withdraws_the_licence()
    {
        let mut supervisor = pooled_harness(2);
        let (primary, _primary_commands) = live_connection(1);
        supervisor.primary.active = Some(primary);
        let (standby, _standby_commands) = live_connection(2);
        install_standby(&mut supervisor, standby);
        supervisor.on_notice(event_notice(1, update("100", 10)));
        supervisor.report_loss(ContinuityReason::LocalLoss, AuthorityReason::Overload);

        supervisor.on_notice(event_notice(2, update("999", 10)));

        assert_eq!(
            supervisor.pool.as_ref().and_then(PoolGate::degraded),
            Some(PoolDegradeReason::EqualKeyContentMismatch),
            "a stale book does not soften what one key naming two states means"
        );
        assert_eq!(
            supervisor.writer.published().authority(),
            &AuthorityState::Stale(AuthorityReason::Overload),
            "the arrival that withdrew the licence is no recovery base"
        );
        assert_eq!(bid_sizes(&supervisor.writer.published()), vec!["100"]);
    }

    #[tokio::test]
    async fn a_pool_socket_between_connections_keeps_its_name_and_its_failure_reason() {
        for (ended, expected) in [
            (
                ConnectionEndReason::SocketClosed,
                ReplicaFailureReason::Disconnect,
            ),
            (
                ConnectionEndReason::Terminal(TerminalFrameReason::NamespaceDisconnect),
                ReplicaFailureReason::Protocol,
            ),
            (
                ConnectionEndReason::NoticeUndeliverable,
                ReplicaFailureReason::Overload,
            ),
        ] {
            let (tap_tx, mut tap) = mpsc::channel(64);
            let mut supervisor = pooled_harness(2).with_diagnostics(tap_tx);
            let (primary, _primary_commands) = live_connection(1);
            supervisor.primary.active = Some(primary);
            let (standby, _standby_commands) = live_connection(2);
            install_standby(&mut supervisor, standby);
            supervisor.on_notice(event_notice(1, update("100", 10)));

            supervisor.standbys[0].slot.active = None;
            supervisor.on_slot_ended(Slot::Standby(0), ended, false, true, Duration::ZERO);

            let source = last_source(&mut tap).expect("the ending is reported as a transition");
            let named: Vec<(u64, PoolSocketState)> = source
                .pool_sockets()
                .map(|(socket, state)| (socket.connection().generation(), state.clone()))
                .collect();
            assert!(
                named.contains(&(2, PoolSocketState::Failed(expected.clone()))),
                "a pool socket between connections must stay a named connection with a \
                 named reason, not an anonymous shortfall: {ended:?} gave {named:?}"
            );
        }
    }

    #[test]
    fn exponential_doubles_then_caps() {
        let initial = Duration::from_millis(100);
        let max = Duration::from_millis(400);
        assert_eq!(exponential(initial, 1, max), Duration::from_millis(100));
        assert_eq!(exponential(initial, 2, max), Duration::from_millis(200));
        assert_eq!(exponential(initial, 3, max), Duration::from_millis(400));
        assert_eq!(exponential(initial, 9, max), max);
    }

    #[test]
    fn only_a_generation_that_reached_stability_resets_the_backoff_ladder() {
        let stable_after = Duration::from_secs(60);
        assert!(!resets_backoff(Duration::ZERO, stable_after));
        assert!(!resets_backoff(Duration::from_millis(59_999), stable_after));
        assert!(resets_backoff(stable_after, stable_after));
        assert!(resets_backoff(Duration::from_secs(3600), stable_after));
    }

    #[tokio::test]
    async fn a_pooled_supervisor_runs_one_connection_role_per_socket() {
        let supervisor = Supervisor::new(SupervisorConfig {
            market: TEST_SLUG.to_owned(),
            replicas: crate::MAX_POOL_SOCKETS,
            pooled: true,
            ..SupervisorConfig::default()
        })
        .expect("a pool at the gate's own socket ceiling is a valid configuration");
        assert_eq!(
            supervisor.standbys.len(),
            crate::MAX_POOL_SOCKETS - 1,
            "the publishing slot plus one standby role per further socket"
        );
        assert!(supervisor.pool_armed());
        assert_eq!(
            supervisor.slots().map(Slot::index).collect::<Vec<_>>(),
            (0..crate::MAX_POOL_SOCKETS).collect::<Vec<_>>(),
            "socket numbering is the role order, publishing slot first"
        );
    }

    /// A non-pooled supervisor builds the full primary-and-standby ladder at
    /// [`MAX_REPLICAS`] and fails over across it — the empirical backing for aligning the
    /// ladder depth with the pool gate's, rather than a claim resting on code reading alone.
    ///
    /// Counting roles would only restate `Supervisor::new`'s arithmetic. What the deeper
    /// ladder has to be shown doing is the thing it exists for: every role shadows the
    /// publishing one, and when the publishing connection is lost a standby takes the book
    /// over and goes on publishing it without the book ever leaving
    /// [`AuthorityState::Live`]. The promotion is the whole ladder's purpose, and nothing in
    /// the promotion path assumes a depth of two.
    #[tokio::test]
    async fn a_supervisor_fails_over_across_the_full_replica_ladder() {
        assert_eq!(
            MAX_REPLICAS, 4,
            "this test pins the ladder depth it exercises: a shallower constant would leave \
             it passing while proving less than it claims"
        );
        let mut supervisor = harness(MAX_REPLICAS);
        assert_eq!(
            supervisor.standbys.len(),
            MAX_REPLICAS - 1,
            "the publishing role plus one hot standby per further connection"
        );
        assert!(!supervisor.pool_armed());
        assert_eq!(
            supervisor.slots().map(Slot::index).collect::<Vec<_>>(),
            (0..MAX_REPLICAS).collect::<Vec<_>>(),
            "role numbering is the primary-first order whatever the ladder depth"
        );

        let (primary, _primary_commands) = live_connection(1);
        supervisor.primary.active = Some(primary);
        let mut standby_commands = Vec::new();
        for index in 0..MAX_REPLICAS - 1 {
            let generation = u64::try_from(index).expect("a role index fits a generation") + 2;
            let (standby, commands) = live_connection(generation);
            standby_commands.push(commands);
            install_standby_at(&mut supervisor, index, standby);
        }
        for generation in 1..=u64::try_from(MAX_REPLICAS).expect("the ladder depth fits") {
            supervisor.on_notice(event_notice(generation, update("100", 1)));
        }
        assert_eq!(
            supervisor.writer.published().authority(),
            &AuthorityState::Live,
            "the publishing role holds the book before anything is lost"
        );
        assert!(
            supervisor
                .standbys
                .iter()
                .all(|standby| standby.state == StandbyState::Agreeing),
            "every standby of the ladder shadows the publishing role"
        );

        supervisor.primary.active = None;
        supervisor.on_primary_ended(
            ConnectionEndReason::SocketClosed,
            true,
            Duration::from_secs(5),
        );

        assert_eq!(
            supervisor.stats.promotions, 1,
            "the first viable standby of the ladder takes the book over"
        );
        assert_eq!(
            supervisor.writer.published().authority(),
            &AuthorityState::Live,
            "a promotion carries the book across without surrendering authority"
        );
        let publishing = supervisor
            .primary
            .active
            .as_ref()
            .map(|active| active.generation);
        assert_eq!(
            publishing,
            Some(2),
            "the promoted standby is the connection now publishing"
        );
        assert_eq!(
            supervisor.standbys.len(),
            MAX_REPLICAS - 1,
            "the ladder keeps its configured depth: a promotion empties a standby role \
             rather than deleting it"
        );
        assert!(
            supervisor
                .standbys
                .first()
                .expect("the emptied standby role is still configured")
                .slot
                .active
                .is_none(),
            "the promoted connection left the role it was taken from"
        );

        supervisor.on_notice(event_notice(2, update("150", 2)));
        assert_eq!(
            bid_sizes(&supervisor.writer.published()),
            vec!["150".to_owned()],
            "the promoted connection goes on publishing into the same book"
        );
    }

    /// A budget an operator sets small is a budget the ledger actually enforces: the second
    /// spawn of a supervisor configured to spend one attempt a day is held back rather than
    /// made.
    ///
    /// The ledger is process-wide and shared with every other test in this binary, so the
    /// assertion is the monotone one — a clamp happened — rather than a count that would
    /// depend on what else had already spent. The endpoint is a loopback address nothing
    /// listens on, so the spawned connection task refuses immediately and no venue is
    /// contacted.
    #[tokio::test]
    async fn a_budget_of_one_clamps_the_next_spawn() {
        let mut supervisor = Supervisor::new(SupervisorConfig {
            market: TEST_SLUG.to_owned(),
            endpoint: "ws://127.0.0.1:1/socket.io/?EIO=4&transport=websocket".to_owned(),
            daily_attempt_budget: 1,
            ..SupervisorConfig::default()
        })
        .expect("a budget of one is within the ledger's ceiling");

        supervisor.spawn_due(Slot::Primary);
        if let Some(active) = supervisor.primary.active.take() {
            active.handle.abort();
        }
        supervisor.primary.reconnect_at = None;
        supervisor.spawn_due(Slot::Primary);

        assert!(
            supervisor.stats.attempts_clamped > 0,
            "a budget of one cannot admit a second attempt inside the rolling day"
        );
        assert!(
            supervisor.primary.active.is_none(),
            "a clamped spawn starts no connection"
        );
        assert!(
            supervisor.primary.reconnect_at.is_some(),
            "a clamped spawn is rescheduled rather than abandoned"
        );
    }

    #[tokio::test]
    async fn a_configuration_without_the_pool_flag_builds_no_gate() {
        let supervisor = harness(2);
        assert!(
            !supervisor.pool_armed(),
            "every connection beyond the first shadows the primary and publishes nothing"
        );
        assert_eq!(supervisor.standbys.len(), 1);
    }

    /// The shortest delay [`jittered`] can return for `base`, which is the most frequent a
    /// stable-at-threshold reset cycle can recur and therefore the worst case this test
    /// drives the ledger with.
    fn jitter_floor(base: Duration) -> Duration {
        jittered(base, 0)
    }

    #[test]
    fn the_attempt_ledger_clamps_a_stable_at_threshold_flap_across_both_roles() {
        let config = SupervisorConfig::default();
        let cycle = config.stable_after + jitter_floor(config.initial_backoff);
        let start = Instant::now();
        let mut ledger = AttemptLedger::new();
        let mut admitted = 0u64;
        let mut refused = 0u64;
        let mut elapsed = Duration::ZERO;
        while elapsed < ONE_DAY {
            for _role in 0..2u32 {
                match ledger.admit(start + elapsed, DAILY_ATTEMPT_BUDGET) {
                    Ok(()) => admitted += 1,
                    Err(free_at) => {
                        refused += 1;
                        assert!(
                            free_at > start + elapsed,
                            "a refusal names a future instant to wait for"
                        );
                    }
                }
            }
            elapsed += cycle;
        }
        assert_eq!(
            admitted, DAILY_ATTEMPT_BUDGET,
            "both roles flapping at the stability threshold spend the budget and no more"
        );
        assert!(refused > 0, "the walk must reach the clamp to prove it");
        assert!(
            ledger.spent.len() <= usize::try_from(DAILY_ATTEMPT_BUDGET).unwrap_or(usize::MAX),
            "the ledger's storage is bounded by the budget it enforces"
        );
        let aged_out = ledger
            .admit(start + ONE_DAY + cycle, DAILY_ATTEMPT_BUDGET)
            .is_ok();
        assert!(
            aged_out,
            "the window rolls: an attempt older than a day frees a slot"
        );
    }

    #[test]
    fn command_grants_are_never_closer_than_the_configured_floor() {
        let now = Instant::now();
        assert_eq!(
            command_grant_at(None, now, MIN_COMMAND_INTERVAL),
            now,
            "the first command waits for nothing"
        );
        let granted = command_grant_at(Some(now), now, MIN_COMMAND_INTERVAL);
        assert_eq!(granted, now + MIN_COMMAND_INTERVAL);
        assert_eq!(
            command_grant_at(
                Some(now),
                now + MIN_COMMAND_INTERVAL * 2,
                MIN_COMMAND_INTERVAL
            ),
            now + MIN_COMMAND_INTERVAL * 2,
            "a permit already free grants at once rather than in the past"
        );
    }

    /// A configured floor of zero is a valid operator choice meaning no pacing at all: the
    /// grant arithmetic (`max(last, now)`) already handles it with no special case, so a
    /// caller placing back-to-back commands is granted immediately rather than refused or
    /// stalled.
    #[test]
    fn a_zero_configured_floor_grants_every_command_at_once() {
        let now = Instant::now();
        assert_eq!(
            command_grant_at(Some(now), now, Duration::ZERO),
            now,
            "no pacing means no wait, not a refusal"
        );
    }

    /// The reservation ledger hands out places in a queue rather than permits to write now.
    ///
    /// Three properties, in the order they matter: consecutive callers are spaced by the
    /// floor whatever instant each of them asked at, a caller arriving after the queue has
    /// drained gets the present moment rather than a stale one, and a floor of zero produces
    /// no queue at all.
    ///
    /// The hole is the fourth reservation here. The ledger has no route by which a command
    /// that was reserved and then never written can give its place back — nothing reports a
    /// write, and nothing releases a slot — so the caller behind it still stands a full floor
    /// behind a slot that was never used. That leaves this process quieter toward the venue
    /// than its configured floor allows, which is the direction a pacer is allowed to err in.
    #[test]
    fn reservations_hand_out_places_in_one_queue_and_never_reclaim_an_unused_place() {
        const INTERVAL: Duration = Duration::from_millis(100);
        let paced = test_endpoint("reservations-hand-out-places");
        let now = Instant::now();

        assert_eq!(
            reserve_command_grant(&paced, now, INTERVAL),
            now,
            "an endpoint with an empty queue grants the present moment"
        );
        assert_eq!(
            reserve_command_grant(&paced, now, INTERVAL),
            now + INTERVAL,
            "the second caller stands behind the place the first holds"
        );
        assert_eq!(
            reserve_command_grant(&paced, now, INTERVAL),
            now + INTERVAL * 2,
            "spacing follows the places already handed out, not the instant a caller asked"
        );
        assert_eq!(
            reserve_command_grant(&paced, now, INTERVAL),
            now + INTERVAL * 3,
            "the place before this one was never written, and is not reclaimed"
        );
        assert_eq!(
            reserve_command_grant(&paced, now + INTERVAL * 20, INTERVAL),
            now + INTERVAL * 20,
            "a queue that has drained grants the present moment rather than a stale place"
        );

        let unpaced = test_endpoint("reservations-with-no-floor");
        assert_eq!(reserve_command_grant(&unpaced, now, Duration::ZERO), now);
        assert_eq!(
            reserve_command_grant(&unpaced, now, Duration::ZERO),
            now,
            "a floor of zero is no queue: every caller is granted the present moment"
        );
    }

    #[test]
    fn jitter_stays_within_a_quarter_of_the_base() {
        let base = Duration::from_millis(1000);
        for seed in 0..1000u64 {
            let delay = jittered(base, seed);
            assert!(delay >= Duration::from_millis(750), "{delay:?}");
            assert!(delay <= Duration::from_millis(1250), "{delay:?}");
        }
    }

    #[test]
    fn namespace_loss_and_socket_loss_stay_distinguishable() {
        assert_eq!(
            end_reason_mapping(ConnectionEndReason::Terminal(
                TerminalFrameReason::NamespaceDisconnect
            )),
            (
                ContinuityReason::Reconnect,
                AuthorityReason::SubscriptionLost
            )
        );
        assert_eq!(
            end_reason_mapping(ConnectionEndReason::SocketClosed),
            (ContinuityReason::Reconnect, AuthorityReason::Disconnect)
        );
        assert_eq!(
            end_reason_mapping(ConnectionEndReason::NoticeUndeliverable),
            (ContinuityReason::Reconnect, AuthorityReason::Overload)
        );
        assert_eq!(
            end_reason_mapping(ConnectionEndReason::TaskFailed),
            (ContinuityReason::Reconnect, AuthorityReason::LocalLoss)
        );
    }

    #[test]
    fn a_divergent_standby_refuses_promotion_with_its_own_reason() {
        assert_eq!(
            refusal_reason(
                &StandbyState::Divergent(DivergenceReason::ContentMismatch),
                ConnectionEndReason::SocketClosed
            ),
            AuthorityReason::ReplicaDivergence
        );
        assert_eq!(
            refusal_reason(
                &StandbyState::Divergent(DivergenceReason::ContinuityMismatch),
                ConnectionEndReason::SocketClosed
            ),
            AuthorityReason::Disconnect
        );
        assert_eq!(
            refusal_reason(
                &StandbyState::Failed(ReplicaFailureReason::Disconnect),
                ConnectionEndReason::Terminal(TerminalFrameReason::NamespaceDisconnect)
            ),
            AuthorityReason::SubscriptionLost
        );
    }

    #[test]
    fn a_standby_records_why_its_own_connection_ended() {
        assert_eq!(
            replica_failure(ConnectionEndReason::SocketClosed),
            ReplicaFailureReason::Disconnect
        );
        assert_eq!(
            replica_failure(ConnectionEndReason::Terminal(
                TerminalFrameReason::NamespaceConnectError
            )),
            ReplicaFailureReason::Protocol
        );
        assert_eq!(
            replica_failure(ConnectionEndReason::NoticeUndeliverable),
            ReplicaFailureReason::Overload
        );
    }

    #[test]
    fn replicas_outside_the_socket_ceiling_are_rejected() {
        let config = |replicas| SupervisorConfig {
            market: "btc-up-or-down-5-min-1788172500".to_owned(),
            replicas,
            ..SupervisorConfig::default()
        };
        assert_eq!(
            Supervisor::new(config(0)).err(),
            Some(SupervisorError::ReplicasOutOfRange)
        );
        assert_eq!(
            Supervisor::new(config(MAX_REPLICAS + 1)).err(),
            Some(SupervisorError::ReplicasOutOfRange)
        );
        assert!(Supervisor::new(config(1)).is_ok());
        assert!(Supervisor::new(config(MAX_REPLICAS)).is_ok());
    }

    /// A refused bound names the key it refused and the bound it enforces.
    ///
    /// These two are operator configuration, so their refusals reach an operator reading a
    /// startup failure. A message that said only "invalid supervisor configuration" would
    /// leave that operator to find which of a dozen keys was wrong by bisection.
    #[test]
    fn an_over_bound_key_is_refused_by_name_and_by_bound() {
        let budget = Supervisor::new(SupervisorConfig {
            market: TEST_SLUG.to_owned(),
            daily_attempt_budget: WORST_CASE_ATTEMPT_CEILING + 1,
            ..SupervisorConfig::default()
        })
        .err()
        .expect("a budget past the ledger's ceiling is refused");
        assert_eq!(budget, SupervisorError::DailyAttemptBudgetTooLarge);
        assert_eq!(
            budget.to_string(),
            "daily_attempt_budget must be at most 1000000"
        );

        let interval = Supervisor::new(SupervisorConfig {
            market: TEST_SLUG.to_owned(),
            min_command_interval_ms: MAX_COMMAND_INTERVAL_MS + 1,
            ..SupervisorConfig::default()
        })
        .err()
        .expect("a floor indistinguishable from an outage is refused");
        assert_eq!(interval, SupervisorError::CommandIntervalTooLarge);
        assert_eq!(
            interval.to_string(),
            "min_command_interval_ms must be at most 60000"
        );

        assert_eq!(
            SupervisorError::IngestCapacityZero.to_string(),
            "invalid supervisor configuration",
            "a variant with no bound of its own keeps the general sentence"
        );
    }
}

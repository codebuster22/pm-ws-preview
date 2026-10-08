//! One Limitless connection carrying a *set* of markets: the multi-market shard.
//!
//! [`crate::limitless::supervisor::Supervisor`] owns one market over its configured
//! replica ladder. A [`Shard`] owns [`ShardConfig::replicas`] connections over many markets.
//! The two are peers, not layers: the desired-set reconciliation, per-market books, and
//! per-market subscription evidence live here. Both draw connection attempts from the same
//! process-wide ledger and reserve subscription-bearing commands from the same configured
//! pacer, because both budgets belong to the source address rather than to one task.
//!
//! Redundancy is the one discipline the two rails share outright, and what a shard's
//! connections beyond the first do follows the venue's own key declaration
//! ([`ShardConfig::dedup_key`]). Where that declaration admits pooled publishing they are a
//! pool: every socket is an authoritative source, every arrival is judged by that market's
//! [`PoolGate`], and the first arrival past a market's last published key publishes whichever
//! socket carried it. Where it does not — and for the whole set for the rest of the process
//! once the gate's live tripwire withdraws the licence — one connection publishes and the
//! others are hot standbys, each subscribing the same replace-set as the publishing
//! connection and feeding a per-market shadow [`OrderBook`] this same task owns, which no
//! consumer ever reads and which is never counted as a market's liquidity. Standbys keep
//! their shadows under either topology, so the one a withdrawal hands back to is warm at the
//! instant it is handed back to.
//!
//! The difference from the supervisor is arity, not policy — one socket carries a whole set,
//! so losing the publishing one asks the promotion question once per market, and the same set
//! can answer it both ways at once. The eligibility predicate is
//! [`crate::limitless::supervisor::agreement`] itself, not a second opinion about it. An
//! armed pool asks that question for no market at all: the gate answered it for every arrival
//! already published, so losing a socket costs coverage and nothing else.
//!
//! The venue replaces a connection's whole subscription set on every
//! `subscribe_market_prices` (`docs/limitless.md`), so the shard holds one aggregate desired
//! set and reissues it whole. It never sends an incremental command and never replays a
//! history of commands: an add or a remove mutates the desired set, and one serialized
//! reissue carries the result.
//!
//! `docs/design.md` requires that a replacement whose old/new boundary carries no causal
//! evidence happen on a fresh connection generation, and `docs/limitless.md` records that
//! this venue publishes no such evidence: its `system` answer cannot be correlated with the
//! command that provoked it, and no echo of the requested set has been proven. So the two
//! kinds of reconciliation are answered differently.
//!
//! A **same-set** reissue — one market's recovery on a set the wire already carries — stays
//! on the connection. Old set and new set are identical, so no frame can be attributed to
//! the wrong one and there is nothing for a boundary to separate; the resubscribe-then-reconnect
//! contract applies unchanged.
//!
//! A **set change** is carried by a fresh connection generation: the running connection is
//! fenced and a new one is dialled with the new set. Frame attribution is then pure
//! generation fencing — a market added or re-added cannot receive anything from the old
//! connection, because that connection's notices are discarded outright — and a market
//! removed from the set is closed when the old connection ends, which is also when the
//! venue stops sending it. Retained markets ride the ordinary per-market reconnect recovery
//! contract, with an honest [`ContinuityReason::Reconnect`] and a fresh base each.
//!
//! The cost is one operator-driven reconnect per set change, bounded by the same attempt
//! ledger and command pacing every dial spends from. `docs/limitless.md` carries the open
//! question that would buy the cheaper path back — "the causal boundary between replacement
//! sets and subsequent book frames" — and a venue that documents its acknowledgment, or a
//! live session proving the answer echoes the requested set, would restore same-connection
//! set changes without changing anything else here.
//!
//! Authority stays evidence-based and per market. No timer here observes market activity: a
//! quiet subscribed market stays [`AuthorityState::Live`] for as long as the connection's
//! heartbeat evidence holds, whatever the rest of the set is doing.

use crate::limitless::connection::{
    ConnectionConfig, ConnectionControl, ConnectionEndReason, ConnectionNote, ConnectionNotice,
    DEFAULT_ENDPOINT, run_connection,
};
use crate::limitless::supervisor::{
    CONNECTION_NAME, DAILY_ATTEMPT_BUDGET, MARKET_RESOLVED_EVENT, MAX_BOOK_LEVELS,
    MAX_COMMAND_INTERVAL_MS, MAX_REPLICAS, MIN_COMMAND_INTERVAL, VENUE, WORST_CASE_ATTEMPT_CEILING,
    admit_attempt, agreement, book_error_key, elapsed_nanos, end_reason_mapping, exponential,
    jittered, refusal_reason, replica_failure, reserve_command_grant, resets_backoff,
    resolution_fits_the_ring, sleep_until_opt,
};
use crate::limitless::{
    LimitlessEvent, MarketResolved, ORDERBOOK_UPDATE_DEDUP_KEY, OrderbookUpdate,
};
use crate::{
    AuthorityReason, AuthorityState, BookCommit, BookError, BookObserver, BookWriter,
    BoundedSourceEvidence, Candidate, CandidateProjection, ConnectionIdentity, ContinuityReason,
    DedupKey, DedupKeyDeclaration, DedupKeySemantics, DeliveryPath, DescriptorError,
    DivergenceReason, IdentityError, LevelCapacity, LocalMonotonicTimestamp, MarketHandle,
    MarketRef, MarketResolution, MutationContinuity, NativeIdentifierKind, NativeLabel,
    NativeMarketKey, NativeOutcome, ObservationError, ObserverCapacity, OrderBook, Origin,
    PoolDegradeReason, PoolError, PoolGate, PoolVerdict, Provenance, ProvenanceInput,
    PublishedBook, ReplicaFailureReason, ReplicaRole, Representation, ResolutionDelivery,
    ResolutionObservation, SegmentWriter, SourceEvidenceCapacity, SourceTimestamp, StandbyState,
    Venue, WriterError, candidate_projection,
};
use core::future::Future;
use core::pin::Pin;
use core::task::Poll;
use core::time::Duration;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::hash::BuildHasher;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

const ORDERBOOK_UPDATE_EVENT: &str = "orderbookUpdate";

/// The most markets one shard will hold in its desired set unless configured lower.
///
/// A hard capacity in the sense `docs/design.md` "Configuration and hard limits" requires:
/// an add that would cross it is rejected as input rather than silently accepted, so the
/// shard's per-market memory and its reissue payload both stay bounded by a declared
/// number. It is a daemon-side bound, not a venue-published one: the venue's per-command
/// market-count limit is an open question in `docs/limitless.md`.
///
/// The ceiling is what a shard's shared-memory segment can address, not what a deployment
/// should ask for: a segment's directory holds at most
/// [`crate::MAX_DIRECTORY_CAPACITY`] markets, and the region that carries their state
/// slots and event rings is itself capped, so the reachable market count falls out of the
/// declared book depth. `daemon::DeliveryProfile` names the tuples that fit, and
/// `daemon`'s resource-envelope test does the arithmetic. Nothing here provisions memory
/// for this number; it bounds what a configuration may declare.
pub const MAX_SHARD_MARKETS: usize = 32_768;

/// The market capacity [`ShardConfig::default`] declares, for a shard configured by hand
/// rather than by a daemon profile.
pub const DEFAULT_SHARD_MARKETS: usize = 1_024;

/// How many microsecond buckets [`QueueAge`] keeps, covering ages up to roughly 2^23 µs
/// (about 8.4 seconds) before the top bucket absorbs the rest.
const QUEUE_AGE_BUCKETS: usize = 24;

/// How many removed markets keep their books while the venue is still subscribed to them.
///
/// A removal is only reconciled when a subscription command excluding it reaches the venue,
/// and until then the venue keeps sending that market's frames, so its book must stay to
/// receive them. This bounds how many such books may wait at once. Overflow is drop-oldest:
/// the market whose removal has waited longest gives up its book immediately, publishing
/// [`AuthorityState::Unsubscribed`] first, and any further frame the venue sends for it is
/// counted as unrouted rather than applied.
const MAX_REMOVING_MARKETS: usize = 64;

/// Everything one shard needs to keep a set of markets alive over one connection.
///
/// The reconnect fields mean exactly what they mean for a single-market supervisor, and
/// spend from the same process-wide daily attempt ledger. `markets` is the operator-pinned
/// starting set; it may be empty, in which case the shard connects and subscribes to
/// nothing until a command adds a market.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardConfig {
    pub endpoint: String,
    /// The operator-pinned desired set this shard starts with. An empty set still
    /// connects and still emits one `subscribe_market_prices` naming no market, which is
    /// what the venue's replace-set semantics say demand for nothing is; the venue's own
    /// answer to an empty set is unverified (`docs/limitless.md` open questions).
    pub markets: Vec<String>,
    pub setup_timeout: Duration,
    /// The bounded ingest queue between the connection task and this shard. Overflow
    /// behavior is the connection's: drop-newest with an ordered
    /// [`ConnectionNote::Overload`] report, never a blocked read loop.
    ///
    /// A connection establishing itself puts two notices in this queue before any book
    /// frame — the venue-negotiated open, and the venue's acknowledgment of the subscription
    /// — so a queue shorter than two drops the first frames of every connection while those
    /// wait for room. That is ordinary bounded-queue behavior rather than a failure, and it
    /// costs a recovery cycle; any realistic capacity is far above it.
    pub ingest_capacity: usize,
    /// The bounded control queue between [`ShardHandle`] callers and this shard. A full
    /// queue answers [`ShardControlError::Busy`] rather than waiting, so a slow venue can
    /// never stall the control plane.
    pub control_capacity: usize,
    /// The mutation-ring capacity given to every market's book. Multiplied by the market
    /// count, so a shard holding hundreds of markets sizes this for the set rather than for
    /// one hot book.
    pub observer_capacity: usize,
    pub level_capacity: usize,
    /// The most markets this shard's desired set may hold. Clamped to
    /// [`MAX_SHARD_MARKETS`] by [`Shard::new`].
    pub max_markets: usize,
    /// How many venue connections this shard runs: 1 — the default, and today's behavior in
    /// full — for a single publishing source, and one redundant connection per replica
    /// beyond that. Anything outside `1..=`[`MAX_REPLICAS`] is rejected by [`Shard::new`].
    ///
    /// Every connection subscribes the same replace-set. What the connections beyond the
    /// first *do* follows [`Self::dedup_key`]. Where the venue's declaration admits pooled
    /// publishing they are a pool: every arrival is judged by one per-market venue-key gate,
    /// the first arrival past a market's last published key publishes whichever connection
    /// carried it, and losing one costs coverage rather than asking a promotion question.
    /// Where it does not — and for the rest of the process after the live tripwire fires —
    /// one connection publishes and the others feed per-market shadow state that no consumer
    /// reads and that is never counted as a market's liquidity. A shard is one physical book
    /// per market under either, so this multiplies sockets and never liquidity.
    ///
    /// Each connection runs its own reconnect ladder out of one shared process-wide attempt
    /// ledger, so the ladders are paced against this count: see `max_backoff`.
    pub replicas: usize,
    /// The venue key declaration this shard's redundant connections are licensed by.
    ///
    /// Defaults to the venue's own recorded declaration
    /// ([`ORDERBOOK_UPDATE_DEDUP_KEY`]), whose `session-monotone` semantics carry the
    /// conformance basis recorded in `docs/limitless.md`. A declaration whose semantics
    /// admit pooled publishing makes `replicas > 1` an active-active pool on that key; one
    /// that does not makes the same connections one publishing primary with hot standbys
    /// from the first dial, reported as such rather than silently.
    ///
    /// Operator configuration in the sense [`Self::daily_attempt_budget`] is: this
    /// project's own statement of what the venue's key has been observed to be, never a
    /// fact the venue enforces on this daemon's behalf. Naming weaker semantics than the
    /// recorded basis withholds a licence the venue would have supported; naming stronger
    /// ones misnames the basis without changing what the gate checks, because every
    /// declaration that admits a pool is gated and tripwired identically.
    pub dedup_key: DedupKeyDeclaration,
    pub initial_backoff: Duration,
    /// The ceiling of one ladder's exponential backoff, stated for a single-connection
    /// shard. Every ladder paces at this value multiplied by `replicas`, so a shard's
    /// worst-case daily attempt total is the same whether it runs one connection or four,
    /// and one number governs the configured daily attempt budget however many roles are
    /// configured. This mirrors `SupervisorConfig::max_backoff` exactly, because both rails
    /// spend from the same ledger.
    pub max_backoff: Duration,
    /// The retry cadence after recovery has been reported exhausted, scaled by `replicas`
    /// exactly as `max_backoff` is.
    pub exhausted_backoff: Duration,
    /// How long a whole-set reissue has to reach the wire, and then to produce the recovery
    /// bases it was sent for, before the connection is replaced.
    pub resubscribe_window: Duration,
    pub max_recovery_attempts: u32,
    pub fenced_linger: Duration,
    pub stable_after: Duration,
    pub daemon_generation: u64,
    /// Diagnostic fault injection: how long this shard withholds one drain cycle after its
    /// first accepted book state, so ingest backlog and queue age become observable
    /// deterministically. `None` — the default — in any configuration serving consumers.
    ///
    /// It arms once per run and fires once. Nothing else changes: the connection keeps
    /// reading and its bounded queue keeps its stated overflow behavior, so a stall longer
    /// than the queue can absorb produces ordinary overload drops rather than a new
    /// failure mode.
    pub ingest_stall: Option<Duration>,
    /// The most connection attempts this shard's daemon will spend in a day, shared with
    /// every other shard and supervisor in the process through the one process-wide ledger.
    ///
    /// Operator configuration, defaulted to
    /// [`crate::limitless::supervisor::DAILY_ATTEMPT_BUDGET`]'s value: no retrieved venue
    /// documentation places a daily attempt ceiling, so this is a conservative default
    /// rather than a fact enforced on this daemon's behalf.
    pub daily_attempt_budget: u64,
    /// The floor, in milliseconds, between two subscription-bearing commands this shard's
    /// connection puts on the wire toward `endpoint`.
    ///
    /// Operator configuration, defaulted to the same value as
    /// [`crate::limitless::supervisor::MIN_COMMAND_INTERVAL`]: no retrieved venue
    /// documentation places a sustained-command ceiling, so this is a conservative default
    /// for a polite wire citizen rather than a fact enforced on this daemon's behalf.
    pub min_command_interval_ms: u64,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            endpoint: DEFAULT_ENDPOINT.to_owned(),
            markets: Vec::new(),
            setup_timeout: Duration::from_secs(15),
            ingest_capacity: 1024,
            control_capacity: 64,
            observer_capacity: 1024,
            level_capacity: MAX_BOOK_LEVELS,
            max_markets: DEFAULT_SHARD_MARKETS,
            replicas: 1,
            dedup_key: ORDERBOOK_UPDATE_DEDUP_KEY,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(600),
            exhausted_backoff: Duration::from_secs(600),
            resubscribe_window: Duration::from_secs(5),
            max_recovery_attempts: 5,
            fenced_linger: Duration::from_secs(1),
            stable_after: Duration::from_secs(60),
            daemon_generation: 1,
            ingest_stall: None,
            daily_attempt_budget: DAILY_ATTEMPT_BUDGET,
            min_command_interval_ms: u64::try_from(MIN_COMMAND_INTERVAL.as_millis())
                .unwrap_or(u64::MAX),
        }
    }
}

/// A reason a shard could not be built.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShardError {
    /// A market in the operator-pinned starting set is not a valid venue-native identifier.
    Market(IdentityError),
    LevelCapacity(ObservationError),
    ObserverCapacity(BookError),
    IngestCapacityZero,
    ControlCapacityZero,
    RecoveryAttemptsZero,
    /// `replicas` is zero — a shard publishing from no connection at all — or above
    /// [`MAX_REPLICAS`].
    ReplicasOutOfRange,
    /// The starting set is larger than the configured market capacity.
    MarketCapacityExceeded,
    /// `daily_attempt_budget` is above
    /// [`WORST_CASE_ATTEMPT_CEILING`](crate::limitless::supervisor::WORST_CASE_ATTEMPT_CEILING),
    /// the ceiling the process-wide attempt ledger's storage is sized against.
    DailyAttemptBudgetTooLarge,
    /// `min_command_interval_ms` is above
    /// [`MAX_COMMAND_INTERVAL_MS`](crate::limitless::supervisor::MAX_COMMAND_INTERVAL_MS),
    /// past which this shard's own pacing would be indistinguishable from an outage.
    CommandIntervalTooLarge,
    /// The delivery segment refused a market this shard holds.
    Segment(WriterError),
}

/// Why one market could not be installed: its identifier, or its place in the delivery
/// segment.
enum InstallRefusal {
    Identity(IdentityError),
    Delivery(WriterError),
    /// A market whose delivery entry is retired could not be resumed through it: its book
    /// would have restarted a stream the retired incarnation already published positions in.
    StreamNotRebased,
    /// The book a resume would have carried forward could not record the break that rebases
    /// it, so nothing was seated.
    ResumeUnavailable,
}

impl InstallRefusal {
    /// The answer an operator gets for this refusal.
    fn rejection(&self) -> MarketRejection {
        match self {
            Self::Identity(_) => MarketRejection::InvalidIdentifier,
            Self::Delivery(_) | Self::StreamNotRebased | Self::ResumeUnavailable => {
                MarketRejection::DeliveryUnavailable
            }
        }
    }
}

impl From<SegmentRefusal> for InstallRefusal {
    fn from(refusal: SegmentRefusal) -> Self {
        match refusal {
            SegmentRefusal::Writer(error) => Self::Delivery(error),
            SegmentRefusal::StreamNotRebased => Self::StreamNotRebased,
        }
    }
}

impl std::fmt::Display for ShardError {
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
            Self::ReplicasOutOfRange => {
                write!(f, "replicas must be between 1 and {MAX_REPLICAS}")
            }
            _ => f.write_str("invalid shard configuration"),
        }
    }
}
impl std::error::Error for ShardError {}

/// Why a market named in a command was not taken into the desired set.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketRejection {
    /// Not a valid venue-native market identifier.
    InvalidIdentifier,
    /// The desired set is already at the shard's configured market capacity.
    CapacityExceeded,
    /// The shard could not give this market a place in its delivery segment, so no
    /// consumer could ever read it.
    ///
    /// A segment binds one directory entry and one state slot to a market for the life of
    /// its generation and never gives either to another market, so this answers two
    /// situations: the directory has no free entry left for a market that has never held
    /// one, and a market whose own entry could not be resumed through — its live
    /// incarnation still holds it, or the book offered could still produce a position the
    /// retired one published. The first needs a segment sized for the deployment, which is
    /// a restart; the second is retryable once the incarnation in the way has ended. Taking
    /// the market into the book and quietly failing to deliver it is the one answer that is
    /// never given: latest state and mutations are two coordinated outputs of one book, and
    /// a book with no delivery is not one of them.
    DeliveryUnavailable,
}

/// How far one market has progressed from desired demand to venue-established
/// subscription.
///
/// The evidence is the venue's own: Limitless acknowledges no individual market
/// subscription, so [`Self::Subscribing`] means the complete-set command carrying this
/// market reached the wire, and [`Self::Established`] means a frame for it then arrived
/// under that emit or a later one. A market that sits in [`Self::Subscribing`] indefinitely
/// is not an error — it is a quiet market, and no timer here turns quiet into failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionState {
    /// In the desired set; no emit carrying it has reached the wire on the current
    /// connection.
    Desired,
    /// An emit carrying it reached the wire; no frame for it has arrived since.
    Subscribing,
    /// A frame for it arrived under the emit that subscribed it, or a later one.
    Established,
    /// No longer desired. The venue still holds it, because the reissue that excludes it
    /// has not reached the wire yet; its book is still authoritative until then.
    Removing,
}

/// What one market named in a command is now, as `docs/design.md` "Subscription control"
/// requires results to distinguish.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketStatus {
    /// Rejected input: the command named something this shard will not take.
    Rejected(MarketRejection),
    /// Accepted desired state, with no venue reconciliation started for it yet.
    Accepted,
    /// Venue reconciliation in progress: it is in the desired set and its subscription is
    /// on the wire or waiting for the next reissue, with no authoritative book yet.
    Reconciling,
    /// Live authoritative state established.
    Live,
    /// The book held authority and lost it, with the reason it was lost for.
    Stale(AuthorityReason),
    /// No longer in the desired set.
    Removed,
}

/// One market's answer to a command.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MarketOutcome {
    pub slug: String,
    pub status: MarketStatus,
}

/// One market's line in [`ShardStatus`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MarketReport {
    pub slug: String,
    pub subscription: SubscriptionState,
    pub status: MarketStatus,
    pub revision: u64,
    pub continuity_epoch: u64,
}

/// What a shard's redundant connections are doing about publishing right now.
///
/// The three states are one-way in this order and never travel back inside a process:
/// a shard whose declaration licenses no pool starts [`Self::Unlicensed`] and stays there,
/// and a pool that was armed and is now [`Self::Degraded`] cannot re-arm, because the
/// evidence licensing it was recorded before the run and a run that has just contradicted
/// it cannot re-record it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PoolState {
    /// Every connection publishes through the per-market venue-key gate.
    Armed,
    /// No pool was ever licensed: the configured declaration's semantics grant no ordering
    /// within a connection-session, so a gate on this venue's key could neither choose the
    /// newer arrival nor recognize a violation. The connections run as one publishing
    /// primary with hot standbys, and this names the semantics that decided it.
    Unlicensed { semantics: String },
    /// The licence was withdrawn live by the arrival this reason names, and the shard runs
    /// one publishing primary with hot standbys for the rest of the process.
    Degraded { reason: PoolDegradeReason },
}

/// A shard's pooled-publishing coverage, as an operator command asks it.
///
/// Present only on a shard configured with more than one connection: a shard at the default
/// `replicas = 1` has no redundancy to describe and reports none, so absence means "no
/// redundancy configured" rather than "a pool covering nothing".
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PoolReport {
    /// How many sockets the pool is configured to hold, which is the shard's `replicas`.
    pub sockets: usize,
    /// How many of them currently hold an established subscription, which is the coverage
    /// an arrival could reach the gate from right now.
    pub covering: usize,
    pub state: PoolState,
}

/// One of a shard's venue connections, and the role it holds.
///
/// A shard running the default single connection reports one row. One running redundant
/// connections reports the publishing connection first and every other after it, in role
/// order, so an operator reads the topology rather than inferring it from a count.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConnectionReport {
    /// Which of the shard's connection slots this connection occupies: the one that owns
    /// set reconciliation, whole-set reissue and — under any topology but an armed pool —
    /// the books, or one of the slots that shadow it. It is the slot the connection holds
    /// now, not the one it was dialled into: a promoted standby reports
    /// [`ReplicaRole::PublishingPrimary`].
    ///
    /// While a pool is armed this is structural rather than exclusive. Every socket of an
    /// armed pool is an authoritative source whose arrivals may reach the books, which is
    /// what [`Self::pool_socket`] names; the slot still decides which connection carries
    /// the shard's subscription bookkeeping and which topology the shard hands back to.
    pub role: ReplicaRole,
    /// The connection generation, which identifies this connection across the whole shard
    /// and is what fences a frame from a generation the shard no longer runs.
    pub generation: u64,
    /// The venue's own Engine.IO session identifier, once the connection has been given
    /// one. Absent for a connection still being dialled.
    pub session: Option<String>,
    /// Whether this connection has its subscription on the wire.
    pub established: bool,
    /// How many of the shard's desired markets this standby's shadow currently agrees with
    /// the published book on, which is exactly how many would carry their authority across
    /// a promotion decided now. `None` for the publishing connection, which maintains no
    /// shadow to agree with.
    ///
    /// A tracked answer rather than a recomputed one. Agreement is a level-by-level
    /// comparison, and a shard answers status and scrape requests on the same task that
    /// feeds every book it holds, so recomputing the whole set against every standby per
    /// request would put observability traffic in front of market data. It is instead
    /// maintained where the compared state moves — one market against one standby per
    /// standby arrival, and against at most `replicas - 1` standbys per published change,
    /// which the replica ceiling bounds at three — and read here as a count. A shard
    /// running no standby maintains nothing and pays nothing.
    pub agreeing_markets: Option<usize>,
    /// Which socket of the shard's pool this connection holds, or `None` on a shard whose
    /// pool is not armed — including one running no pool at all.
    ///
    /// It is the name to resolve a connection by while a pool is armed, because
    /// [`Self::role`] is then structural rather than exclusive: every socket publishes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool_socket: Option<usize>,
}

/// What the shard is doing right now, as an operator command would ask it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ShardStatus {
    /// Every market the shard holds a book for, including one whose removal has not been
    /// reconciled yet, ordered by slug.
    pub markets: Vec<MarketReport>,
    /// How many of those are in the desired set.
    pub desired: usize,
    /// How many venue connection roles this shard is configured to run, publishing role
    /// included. 1 for a shard running no standby.
    ///
    /// Configuration rather than observation: [`Self::connections`] holds only the roles
    /// currently filled, so a shard whose standby is between connections still reports the
    /// topology the operator asked for.
    pub replicas: usize,
    /// Every venue connection this shard currently holds, publishing role first.
    pub connections: Vec<ConnectionReport>,
    /// What this shard's redundant connections are doing about publishing, or `None` for a
    /// shard running the default single connection.
    ///
    /// Absent rather than a nil report at the default, for [`Self::replicas`]'s reason: a
    /// shard nobody configured redundancy for writes the status it wrote before pooling
    /// existed, byte for byte.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<PoolReport>,
    /// How many desired markets the standby best placed to take over currently agrees with
    /// the published book on: the promotion coverage a loss of the publishing connection
    /// would find right now. 0 for a shard running no standby.
    ///
    /// The same figure `ShardMetrics::standby_agreeing_markets` reports, read from the same
    /// tracked state so a scrape and a status page can never name different numbers for it.
    /// It is the coverage of the role that would actually be promoted, not the best any role
    /// could offer, and it counts only markets a takeover would actually carry: a market
    /// whose published book is no longer live is not coverage, however equal its levels
    /// still look. It is coverage against the losses a shard can see coming; a publishing
    /// connection that ends holding a loss it could not report refuses the whole set
    /// whatever this says, because what went missing is unknown to both replicas.
    pub standby_agreeing_markets: usize,
    /// Whether the *publishing* connection is established with its subscription on the
    /// wire. A standby's subscription is never what makes a shard's books reachable, so it
    /// does not answer this.
    pub subscribed: bool,
    /// Whether a whole-set reissue is in flight.
    pub reconciling: bool,
    /// The file name — never the directory — of the shared-memory segment this shard
    /// publishes into, or `None` for a shard publishing into none.
    ///
    /// The name alone, because the directory is configuration the operator already holds
    /// and repeating it in every status line would only lengthen a bounded answer. The
    /// name is the part that is not configuration: it carries this daemon instance's
    /// random identity, which is what makes a stale name from a previous instance fail to
    /// resolve (`docs/notes/shared-memory-model.md` §4.2).
    pub segment: Option<String>,
    /// How many markets hold a live directory entry in that segment.
    pub segment_markets: usize,
    pub queue_age: QueueAgeSummary,
    pub publish_latency: PublishLatencySummary,
}

/// What one shard reports to a metrics scrape: its connection evidence and its run counters.
///
/// The counters are [`ShardStats`], which a run otherwise yields only when it ends, answered
/// mid-run. It carries no per-market rows on purpose: a scrape asks every shard on every
/// collection, and a per-market answer would make that cost grow with the market set while
/// repeating what a status page already says.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardMetrics {
    /// Whether the publishing connection is established with its subscription on the wire.
    pub connected: bool,
    /// Whether a whole-set reissue is in flight.
    pub reconciling: bool,
    /// How many venue connections this shard is configured to run, publishing role
    /// included. 1 for a shard running no standby.
    pub replicas: usize,
    /// How many standby connections currently hold an established subscription. The socket
    /// count a shard contributes right now is this plus one for an established publishing
    /// connection; its *peak*, which the descriptor and connection-cap arithmetic in
    /// `crate::daemon` budgets for, allows one draining connection per role on top.
    pub standbys_established: usize,
    /// How many desired markets the standby best placed to take over currently agrees with
    /// the published book on: the promotion coverage a loss of the publishing connection
    /// would find. 0 for a shard running no standby, which is not the same fact as a
    /// standby agreeing on nothing — [`Self::replicas`] tells those apart.
    pub standby_agreeing_markets: usize,
    /// What this shard's redundant connections are doing about publishing, or `None` for a
    /// shard running the default single connection. The same report
    /// [`ShardStatus::pool`] carries, read from the same state, so a scrape and a status
    /// page can never name different topologies.
    pub pool: Option<PoolReport>,
    /// How many markets are in the desired set.
    pub desired: usize,
    /// How many markets the shard holds a book for, including one whose removal has not been
    /// reconciled yet.
    pub markets: usize,
    /// How many markets hold a live directory entry in this shard's segment.
    pub segment_markets: usize,
    /// Every run counter this shard has accumulated as of this answer.
    pub stats: ShardStats,
}

/// Why a control command could not be delivered to the shard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShardControlError {
    /// The bounded control queue is full. The command was not accepted and nothing changed.
    Busy,
    /// The shard's run has ended.
    Stopped,
}

/// The distribution of ingest queue age this run has sampled, in microseconds.
///
/// Queue age here is *now minus enqueue of the item currently draining*: the interval
/// between the instant the connection task read the frame that produced a book event and
/// the instant this shard dequeued that event to apply it. The read instant precedes the
/// enqueue by one decode step, so every figure is an upper bound on pure queue residency
/// rather than an exact measure of it, and it is the queue-age half of the
/// `docs/design.md` "Latency stage boundaries" ladder between "complete venue message
/// available" and "queued for owning writer".
///
/// Only decoded venue events are sampled; heartbeats and connection lifecycle notices carry
/// no frame instant and are not queue-age evidence. Percentiles are bucket upper bounds, so
/// they are pessimistic within a bucket and never exceed [`Self::max_micros`]. A percentile
/// landing in the topmost bucket — which has no upper bound of its own — reports the observed
/// maximum instead, so an overloaded shard cannot report a thirty-second backlog as the
/// bucket's eight-second bound. A run with no samples reports zeroes with `samples` zero,
/// which is the only way to tell "fast" from "never measured".
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueueAgeSummary {
    pub samples: u64,
    pub last_micros: u64,
    pub max_micros: u64,
    pub p50_micros: u64,
    pub p99_micros: u64,
}

/// The distribution of state-publication latency this run has sampled, in microseconds.
///
/// Publish latency here is *publication complete minus frame arrival*: the interval between
/// the instant the socket read that produced this revision returned and the instant this
/// daemon finished publishing it — slot written and stable, dirty-index entry posted,
/// doorbell rung, publication generation advanced, and the wake posted. It is the daemon's
/// whole share of the `docs/design.md` "Latency stage boundaries" ladder — from "complete
/// venue message available", the same instant [`QueueAgeSummary`] starts from, through
/// "consumer notification published" — observable with no consumer attached, and it stops
/// one rung short of the ladder's last: what a consumer then spends observing the revision
/// is not in it.
///
/// Both readings are [`Instant`]s taken inside this process, one by the connection task at
/// the socket read and one by the shard the moment the writer returned, so the interval is a
/// monotonic one that no wall-clock correction can shorten, lengthen, or invert. That makes
/// it a different measurement from the one the state slot's own `(commit, arrival)` stamps
/// support: those are wall-clock nanoseconds a consumer in another process can read, and
/// their difference ends at the commit stamp the writer takes *before* staging the slot. A
/// consumer-side split derived from the slot stamps is therefore expected to read lower than
/// this, and to move with the system clock where this does not.
///
/// Only a publication a venue frame drove is sampled. A state publication no frame drove — a
/// retirement, a loss report, the install's own first publication, or the republish that
/// carries a resolution's stamps forward — is handed no arrival and is not latency evidence;
/// it is skipped and counted as nothing rather than recorded as zero. Percentiles are bucket
/// upper bounds, pessimistic within a bucket and never past [`Self::max_micros`], exactly as
/// [`QueueAgeSummary`]'s are, and the topmost bucket reports the observed maximum rather than
/// its own unbounded edge. A run with no samples reports zeroes with `samples` zero, which is
/// the only way to tell "fast" from "never measured".
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublishLatencySummary {
    pub samples: u64,
    pub last_micros: u64,
    pub max_micros: u64,
    pub p50_micros: u64,
    pub p99_micros: u64,
    /// The 99.9th percentile, which the tail of a publish path is judged by and a p99 hides.
    pub p999_micros: u64,
}

impl PublishLatencySummary {
    /// Whether nothing has been recorded into this summary.
    ///
    /// What [`crate::ShardReport`] skips the field by: a status answer from a shard that has
    /// published nothing measurable carries no publish-latency object at all, which leaves
    /// that line byte-identical to what a daemon without this metric wrote and makes absence
    /// mean "unmeasured" rather than "zero".
    pub const fn is_unmeasured(&self) -> bool {
        self.samples == 0
    }
}

/// A bounded power-of-two histogram of one microsecond interval.
///
/// Fixed storage: [`QUEUE_AGE_BUCKETS`] counters, a maximum, a last sample, and a count.
/// Nothing accumulates per sample, so recording is O(1) and the structure cannot grow into
/// an unbounded queue of measurements.
///
/// Both distributions this shard reports are recorded into one of these — the ingest queue
/// age and the publish latency — and they differ only in the summary read out of it,
/// [`Self::summary`] or [`Self::publish_latency_summary`].
#[derive(Clone, Copy, Debug)]
struct QueueAge {
    buckets: [u64; QUEUE_AGE_BUCKETS],
    samples: u64,
    last_micros: u64,
    max_micros: u64,
}

impl QueueAge {
    const fn new() -> Self {
        Self {
            buckets: [0; QUEUE_AGE_BUCKETS],
            samples: 0,
            last_micros: 0,
            max_micros: 0,
        }
    }

    fn record(&mut self, micros: u64) {
        let index = usize::try_from(u64::BITS - micros.leading_zeros())
            .unwrap_or(QUEUE_AGE_BUCKETS - 1)
            .min(QUEUE_AGE_BUCKETS - 1);
        self.buckets[index] = self.buckets[index].saturating_add(1);
        self.samples = self.samples.saturating_add(1);
        self.last_micros = micros;
        self.max_micros = self.max_micros.max(micros);
    }

    fn percentile(&self, percent: u64) -> u64 {
        self.permille(percent.saturating_mul(10))
    }

    /// The bucket bound at `per_mille` thousandths of the samples, which is what lets a
    /// summary report a 99.9th percentile without a second histogram.
    ///
    /// The rank is computed in [`u128`] so no sample count can saturate it: a saturating
    /// product would fall *below* the true rank at high counts and select an earlier, faster
    /// bucket than the one the rank names. A rank past [`u64::MAX`] — which needs
    /// `per_mille` above 1000 — no cumulative count can reach, so it falls through to
    /// [`Self::max_micros`], which is the pessimistic answer rather than an early bucket.
    fn permille(&self, per_mille: u64) -> u64 {
        if self.samples == 0 {
            return 0;
        }
        let rank = u128::from(self.samples)
            .saturating_mul(u128::from(per_mille))
            .div_ceil(1_000);
        let target = u64::try_from(rank).unwrap_or(u64::MAX).max(1);
        let mut seen = 0u64;
        for (index, count) in self.buckets.iter().enumerate() {
            seen = seen.saturating_add(*count);
            if seen >= target {
                if index == QUEUE_AGE_BUCKETS - 1 {
                    return self.max_micros;
                }
                let upper = 1u64.checked_shl(u32::try_from(index).unwrap_or(u32::MAX));
                return upper.unwrap_or(u64::MAX).min(self.max_micros);
            }
        }
        self.max_micros
    }

    fn summary(&self) -> QueueAgeSummary {
        QueueAgeSummary {
            samples: self.samples,
            last_micros: self.last_micros,
            max_micros: self.max_micros,
            p50_micros: self.percentile(50),
            p99_micros: self.percentile(99),
        }
    }

    fn publish_latency_summary(&self) -> PublishLatencySummary {
        PublishLatencySummary {
            samples: self.samples,
            last_micros: self.last_micros,
            max_micros: self.max_micros,
            p50_micros: self.percentile(50),
            p99_micros: self.percentile(99),
            p999_micros: self.permille(999),
        }
    }
}

/// Run-level counters. Every field is evidence of something that happened, never a health
/// verdict; each book's own authority is the verdict.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ShardStats {
    pub connection_attempts: u64,
    /// Connection spawns the process-wide daily attempt ledger held back rather than let
    /// the configured daily attempt budget be crossed.
    pub attempts_clamped: u64,
    pub fenced_generations: u64,
    /// Venue events from a generation this shard no longer runs, discarded before routing.
    pub fenced_events: u64,
    pub frames_seen: u64,
    pub events_orderbook: u64,
    pub events_resolved: u64,
    pub events_unknown: u64,
    /// Book updates for a market this shard holds no book for. Counted and dropped: an
    /// unknown market is never a reason to fail, and never a reason to create a book.
    pub frames_unrouted: u64,
    /// Book updates for a market this shard holds a book for, that arrived on a connection
    /// that was not dialled with it. A market added since the dial is carried by the
    /// replacement connection, so nothing this one sends can populate it.
    pub frames_uncarried: u64,
    pub snapshots_applied: u64,
    pub mutations_derived: u64,
    /// Venue snapshots accepted into a standby's shadow state, which no consumer reads and
    /// which never contributes to a market's depth.
    pub shadow_snapshots_applied: u64,
    /// Times the publishing role moved to a standby connection because the connection
    /// holding it was lost. Counted once per switch, whatever the set's markets each did
    /// with it, and never on a shard running no standby.
    pub source_switches: u64,
    /// Markets that carried their authority across such a switch: their shadow agreed with
    /// the published book, so the source changed and the book did not — no continuity loss,
    /// no epoch, no revision spent.
    pub markets_promoted: u64,
    /// Markets refused promotion across such a switch, because their shadow diverged or
    /// held no comparable history. Each lost authority and recovers through the ordinary
    /// rail on the connection that took over.
    pub markets_promotion_refused: u64,
    /// Arrivals a pool's per-market publish gate applied to a published book, whichever
    /// socket carried them. Zero on a shard that never armed a pool.
    pub pool_published: u64,
    /// How many of those each pool socket contributed, indexed by socket, with the
    /// publishing slot first. Empty on a shard that never armed a pool.
    pub pool_published_by_socket: Vec<u64>,
    /// Arrivals dropped because another socket had already published that same frame, which
    /// is what a pool exists to absorb.
    pub pool_duplicate_drops: u64,
    /// Arrivals dropped because the book already held newer state — cross-connection skew,
    /// counted apart from duplicates because it is a different fact about the pool.
    pub pool_stale_drops: u64,
    /// Times a surviving socket was moved into the publishing slot because the connection
    /// holding it went away while the pool was armed. Not a promotion: no market was asked
    /// whether its authority crossed, because the key gate had already answered that for
    /// every arrival the book holds.
    pub pool_handovers: u64,
    /// Why the pool stopped publishing across its sockets, or `None` if it never did —
    /// including on a shard that never armed one.
    pub pool_degraded: Option<PoolDegradeReason>,
    /// Standby generations that ended, keyed by what ended them: `protocol` for a venue
    /// namespace that refused or dropped the subscription, `overload` for a notice queue
    /// that overflowed past delivery, `disconnect` for the transport going away.
    ///
    /// A standby's end costs no book anything, so nothing about it reaches a market's
    /// authority; what it costs is redundancy, and these keep the three reasons for that
    /// loss apart. Empty on a shard running no standby.
    pub standby_ends: BTreeMap<&'static str, u64>,
    /// Venue-reported resolutions forwarded onto this shard's consumer lanes.
    pub resolutions_forwarded: u64,
    /// Resolutions for a market this shard holds no book for. Counted and dropped, for
    /// [`Self::frames_unrouted`]'s reason.
    pub resolutions_unrouted: u64,
    pub continuity_losses: u64,
    pub overload_drops: u64,
    /// Complete-set `subscribe_market_prices` commands this shard put on the wire,
    /// counting a new connection's establishing subscription and every later reissue.
    pub subscriptions_emitted: u64,
    /// Reissues whose window expired without what they were sent for, each of which
    /// replaced the connection through the reconnect ladder.
    pub reissues_escalated: u64,
    /// Connections replaced because the desired market set changed. This venue offers no
    /// boundary a same-connection replacement could be attributed against, so a set change
    /// is carried by a fresh generation.
    pub set_replacements: u64,
    pub recovery_base_unavailable: u64,
    /// Books dropped because a reissue excluding their market reached the wire, or because
    /// the connection carrying them was given up when demand went to zero.
    pub markets_dropped: u64,
    /// Connections given up because the desired set became empty, which is how this shard
    /// expresses a subscription set the venue's replace-set command cannot.
    pub connections_shed: u64,
    /// Removed markets whose books were closed early because more removals were awaiting
    /// reconciliation than [`MAX_REMOVING_MARKETS`] allows.
    pub tombstones_evicted: u64,
    /// The deepest the bounded ingest queue was observed while draining it.
    pub queue_depth_max: usize,
    pub queue_age: QueueAgeSummary,
    /// What this shard's own publications cost, from a frame's arrival to the moment its
    /// publication was complete, on this process's monotonic clock.
    pub publish_latency: PublishLatencySummary,
    pub decode_failures: BTreeMap<&'static str, u64>,
    /// The segment publication that latched, if one did. A run that ends with this set
    /// ended *because* of it: nothing more was published and the shard stopped feeding.
    pub segment_failure: Option<String>,
}

impl ShardStats {
    fn record_failure(&mut self, key: &'static str) {
        *self.decode_failures.entry(key).or_insert(0) += 1;
    }
}

/// Ends a shard run at its next loop iteration, leaving every book at the revision it had
/// reached.
#[derive(Clone, Debug)]
pub struct ShardStopper {
    signal: Arc<Notify>,
}

impl ShardStopper {
    pub fn stop(&self) {
        self.signal.notify_one();
    }
}

enum Command {
    Add {
        slugs: Vec<String>,
        reply: oneshot::Sender<Vec<MarketOutcome>>,
    },
    Remove {
        slugs: Vec<String>,
        reply: oneshot::Sender<Vec<MarketOutcome>>,
    },
    Status {
        reply: oneshot::Sender<ShardStatus>,
    },
    Metrics {
        reply: oneshot::Sender<ShardMetrics>,
    },
    Observe {
        slug: String,
        reply: oneshot::Sender<Option<BookObserver>>,
    },
}

/// The control surface of a running shard: desired-set commands, inspection, and consumer
/// attachment.
///
/// Every method returns as soon as the shard has applied the command to its own desired
/// state, never when the venue answers: reconciliation progress is read back through
/// [`Self::status`] or through each market's own book. The queue behind it is bounded, so a
/// caller learns [`ShardControlError::Busy`] rather than waiting behind a backlog.
#[derive(Clone, Debug)]
pub struct ShardHandle {
    commands: mpsc::Sender<Command>,
}

impl ShardHandle {
    /// Adds markets to the desired set and answers what each one is now.
    ///
    /// Idempotent desired-state, not a sequence of operations: adding a market already in
    /// the set changes nothing and issues no venue command, and the answer for it is its
    /// current state rather than [`MarketStatus::Accepted`]. A change to the set arms one
    /// whole-set reissue. An invalid identifier, or one that would cross the shard's market
    /// capacity, is [`MarketStatus::Rejected`] and leaves the rest of the batch accepted.
    ///
    /// Fails with [`ShardControlError::Busy`] when the bounded control queue is full and
    /// [`ShardControlError::Stopped`] when the run has ended; neither changes the set.
    pub async fn add(&self, slugs: Vec<String>) -> Result<Vec<MarketOutcome>, ShardControlError> {
        let (reply, answer) = oneshot::channel();
        self.commands
            .try_send(Command::Add { slugs, reply })
            .map_err(control_error)?;
        answer.await.map_err(|_| ShardControlError::Stopped)
    }

    /// Removes markets from the desired set and answers what each one is now.
    ///
    /// Removing a market that is not in the set changes nothing and issues no venue
    /// command. A removed market's book stays authoritative and readable until the reissue
    /// that excludes it reaches the wire, which is when it is dropped.
    ///
    /// Fails with [`ShardControlError::Busy`] when the bounded control queue is full and
    /// [`ShardControlError::Stopped`] when the run has ended; neither changes the set.
    pub async fn remove(
        &self,
        slugs: Vec<String>,
    ) -> Result<Vec<MarketOutcome>, ShardControlError> {
        let (reply, answer) = oneshot::channel();
        self.commands
            .try_send(Command::Remove { slugs, reply })
            .map_err(control_error)?;
        answer.await.map_err(|_| ShardControlError::Stopped)
    }

    /// Reports every market the shard holds, its subscription evidence, and the ingest
    /// queue-age distribution sampled so far.
    ///
    /// Fails with [`ShardControlError::Busy`] or [`ShardControlError::Stopped`] exactly as
    /// the desired-set commands do.
    pub async fn status(&self) -> Result<ShardStatus, ShardControlError> {
        let (reply, answer) = oneshot::channel();
        self.commands
            .try_send(Command::Status { reply })
            .map_err(control_error)?;
        answer.await.map_err(|_| ShardControlError::Stopped)
    }

    /// Reports this shard's run counters and its current connection evidence.
    ///
    /// The scrape-side sibling of [`Self::status`]: the same bounded control queue and the
    /// same single reply, carrying [`ShardStats`] — which a run otherwise reports only when
    /// it ends — and none of the per-market rows.
    ///
    /// Fails with [`ShardControlError::Busy`] or [`ShardControlError::Stopped`] exactly as
    /// the desired-set commands do.
    pub async fn metrics(&self) -> Result<ShardMetrics, ShardControlError> {
        let (reply, answer) = oneshot::channel();
        self.commands
            .try_send(Command::Metrics { reply })
            .map_err(control_error)?;
        answer.await.map_err(|_| ShardControlError::Stopped)
    }

    /// Attaches a consumer to one market's book, or answers `None` when the shard holds no
    /// book for that market.
    ///
    /// The observer sees every revision published after the attachment, and the state
    /// current at it. A consumer that must not miss a market's first revision attaches
    /// before that market's first frame; one attaching later reads current state and
    /// continues from there.
    ///
    /// Fails with [`ShardControlError::Busy`] or [`ShardControlError::Stopped`] exactly as
    /// the desired-set commands do.
    pub async fn observe(&self, slug: &str) -> Result<Option<BookObserver>, ShardControlError> {
        let (reply, answer) = oneshot::channel();
        self.commands
            .try_send(Command::Observe {
                slug: slug.to_owned(),
                reply,
            })
            .map_err(control_error)?;
        answer.await.map_err(|_| ShardControlError::Stopped)
    }
}

fn control_error(error: mpsc::error::TrySendError<Command>) -> ShardControlError {
    match error {
        mpsc::error::TrySendError::Full(_) => ShardControlError::Busy,
        mpsc::error::TrySendError::Closed(_) => ShardControlError::Stopped,
    }
}

/// The shared-memory segment one shard publishes every book it holds into: one writer, one
/// directory entry per market, one event ring per market.
///
/// It is the multi-market form of [`crate::limitless::supervisor::BookSegment`] and keeps
/// that type's contract exactly. Publishing is a handful of plain memory stores into a
/// region made resident at creation, so it performs no system call, no allocation and no
/// blocking I/O and can sit on the book's own commit path. A refused publication is
/// latched: nothing more is published into the segment, the run ends, and the daemon that
/// supervises the shard reports it. A consumer must never be left reading a stale segment
/// that looks live because the writer silently stopped, and a refused mutation would leave
/// a hole in a ring every attached consumer waits on forever.
///
/// One writer per segment holds because the shard task is the segment's single owner: the
/// writer is moved into the task with the shard, and a `&mut` is the only way to publish.
///
/// A refused *install* is a different thing from a refused *publication* and is not
/// latched. A market that could not be given a directory entry was never taken into the
/// desired set, nothing about the markets already installed changed, and the run goes on
/// serving them; the operator is told with
/// [`MarketRejection::DeliveryUnavailable`].
pub struct ShardSegment {
    name: String,
    writer: SegmentWriter,
    entries: HashMap<String, SegmentEntry>,
    live: usize,
    failure: Option<WriterError>,
    /// What this segment's publications have cost, recorded into the same bounded
    /// microsecond histogram the ingest queue age uses.
    publish_latency: QueueAge,
}

/// When the frame that drove a publication arrived, on both clocks this daemon reads.
///
/// The two readings are taken adjacently at the same socket read
/// (`ConnectionNote::Event`), so they name one event: `wall_clock_nanos` is what the state
/// slot and every mutation record carry, because a consumer in another process shares no
/// monotonic origin with this one; `monotonic` is what the publish-latency histogram
/// measures from, because `docs/design.md` "Health and observability" measures a local stage
/// on a local monotonic clock — a wall-clock interval reports the clock's own corrections as
/// latency.
///
/// Carrying both in one value is what keeps them the same event: there is no way to hand a
/// publication one without the other, and a publication no venue frame drove is handed
/// neither.
#[derive(Clone, Copy, Debug)]
struct FrameArrival {
    wall_clock_nanos: u64,
    monotonic: Instant,
}

impl FrameArrival {
    /// Pairs the two readings one socket read took.
    const fn new(monotonic: Instant, wall_clock_nanos: u64) -> Self {
        Self {
            wall_clock_nanos,
            monotonic,
        }
    }

    /// The wall-clock stamp a publication carrying `arrival` writes into its slot, which is 0
    /// — "no venue frame drove this" — for a publication carrying none.
    fn slot_stamp(arrival: Option<Self>) -> u64 {
        arrival.map_or(0, |arrival| arrival.wall_clock_nanos)
    }
}

/// One market's place in a segment: the handle its slot is addressed by, whether the book
/// that owns it is still resident, and the mutation-stream epoch its last incarnation
/// published from.
///
/// A retired entry is kept rather than removed, and its slot is never given to another
/// market: that is the ABI's identity rule, and nothing here relaxes it. The *same* market
/// may resume through it, which is not identity reuse — the entry's identity bytes do not
/// change — but it is only safe under a rebased stream. A resumed incarnation restarts its
/// positions at 0, so an incarnation that could still produce a cursor the retired one
/// already published would be spliced onto it inside a ring an attached consumer is reading,
/// with nothing to tell the two apart. [`ShardSegment::install`] refuses that outright:
/// `retired_epoch` is what it checks the offered stream against, and the continuity epoch is
/// the cell the ABI already carries for exactly this.
struct SegmentEntry {
    handle: MarketHandle,
    live: bool,
    /// The continuity epoch this entry's last incarnation was retired at. Meaningless while
    /// `live`, and monotonic across resumes because a book's own epoch only advances.
    retired_epoch: u64,
}

/// Why a segment would not seat a market.
enum SegmentRefusal {
    /// The writer refused: the entry is taken by a live incarnation, the directory has no
    /// entry left, or a publication has latched.
    Writer(WriterError),
    /// A retired entry was offered a stream that could still produce a position its retired
    /// incarnation already published. Refused rather than seated, because an attached
    /// consumer reading that ring would take the old incarnation's event as this one's.
    StreamNotRebased,
}

impl SegmentRefusal {
    /// The writer-level error a startup install reports.
    ///
    /// A freshly formatted segment holds no retired entry, so [`Self::StreamNotRebased`]
    /// cannot arise while [`Shard::publish_into`] is seating the configured set; it is
    /// reported as the entry conflict it is a form of rather than widening an ABI error type
    /// for a state that path cannot reach.
    fn writer_error(self) -> WriterError {
        match self {
            Self::Writer(error) => error,
            Self::StreamNotRebased => WriterError::MarketAlreadyInstalled,
        }
    }
}

/// Whether a book's published stream can only produce positions past `retired_epoch`.
///
/// An intact stream must already stand in a later epoch. A lost one may stand in the same
/// epoch it was retired at: a lost stream has no positions at all, and the only way out of
/// it is a recovery base, which opens `epoch + 1`. Both are the same guarantee stated
/// against the two shapes continuity has.
fn rebased_past(published: &PublishedBook, retired_epoch: u64) -> bool {
    match published.continuity() {
        MutationContinuity::Intact { epoch, .. } => *epoch > retired_epoch,
        MutationContinuity::Lost { epoch, .. } => *epoch >= retired_epoch,
    }
}

impl ShardSegment {
    /// Pairs a formatted segment's writer with the file name it was created under.
    ///
    /// The writer must be freshly formatted and hold no installed market: this type is the
    /// only thing that installs into it, and the handle map it keeps is what routes a book
    /// to its slot.
    pub fn new(name: String, writer: SegmentWriter) -> Self {
        Self {
            name,
            writer,
            entries: HashMap::new(),
            live: 0,
            failure: None,
            publish_latency: QueueAge::new(),
        }
    }

    /// What this segment's publications have cost so far, as a scrape and a status answer
    /// report it.
    pub fn publish_latency(&self) -> PublishLatencySummary {
        self.publish_latency.publish_latency_summary()
    }

    /// The segment's file name, as [`ShardStatus::segment`] reports it.
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// How many markets hold a live directory entry.
    pub fn installed(&self) -> usize {
        self.live
    }

    /// The publication failure this segment latched, if one refused it.
    pub fn failure(&self) -> Option<&WriterError> {
        self.failure.as_ref()
    }

    /// Seats `published`'s market in the directory and publishes its first state into the
    /// slot that entry binds.
    ///
    /// A market with no entry takes a new one. A market whose own entry is retired resumes
    /// through that same entry — same directory index, same state slot, same identity bytes —
    /// which is what lets a market removed by the last release of its demand come back when
    /// demand returns. Resuming is conditional on the offered stream being rebased past the
    /// retired one ([`rebased_past`]); an incarnation that could still produce a cursor the
    /// retired one published is refused with [`SegmentRefusal::StreamNotRebased`] and seated
    /// nowhere.
    ///
    /// The state publication is what makes a late attacher's first read coherent: it starts
    /// from a slot that already names an epoch and a next position rather than from an
    /// unwritten one. On a resume it is also the explicit rebase every attached consumer
    /// reads: a stream standing in another epoch, or lost in this one, is exactly what
    /// [`crate::EventStream::poll`] reports as a continuity loss rather than delivering
    /// across.
    ///
    /// Fails with [`WriterError::MarketAlreadyInstalled`] for a market whose entry is held by
    /// a live incarnation and with [`WriterError::DirectoryFull`] when no entry is left and
    /// none can be resumed. Neither latches. A refused first publication does latch, for
    /// [`Self::publish_state`]'s reason.
    fn install(&mut self, slug: &str, published: &PublishedBook) -> Result<(), SegmentRefusal> {
        self.installable(slug)?;
        let handle = match self.entries.get(slug) {
            Some(entry) => {
                if !rebased_past(published, entry.retired_epoch) {
                    return Err(SegmentRefusal::StreamNotRebased);
                }
                entry.handle
            }
            None => self
                .writer
                .install(published.market())
                .map_err(SegmentRefusal::Writer)?,
        };
        let _replaced = self.entries.insert(
            slug.to_owned(),
            SegmentEntry {
                handle,
                live: true,
                retired_epoch: 0,
            },
        );
        self.live = self.live.saturating_add(1);
        self.publish_state(slug, published, None);
        match self.failure.clone() {
            Some(error) => Err(SegmentRefusal::Writer(error)),
            None => Ok(()),
        }
    }

    /// Whether [`Self::install`] would seat `slug` at all, without touching anything.
    ///
    /// A caller that must destroy something before it can install — a re-add closing the
    /// incarnation a pending removal left behind — asks first, so a refusal leaves what was
    /// there exactly as it was. It answers the structural question only: whether an entry is
    /// available, not whether a particular book may resume through it, which is a property of
    /// the book the caller does not hold yet.
    fn installable(&self, slug: &str) -> Result<(), SegmentRefusal> {
        let resuming = match self.entries.get(slug) {
            Some(entry) if entry.live => {
                return Err(SegmentRefusal::Writer(WriterError::MarketAlreadyInstalled));
            }
            Some(_) => true,
            None => false,
        };
        if self.failure.is_some() {
            return Err(SegmentRefusal::Writer(WriterError::SegmentAlreadyInstalled));
        }
        let capacity = self.writer.geometry().layout().directory_capacity();
        if !resuming && u64::try_from(self.entries.len()).unwrap_or(u64::MAX) >= u64::from(capacity)
        {
            return Err(SegmentRefusal::Writer(WriterError::DirectoryFull));
        }
        Ok(())
    }

    /// Publishes one market's final state and retires its entry, recording the epoch any
    /// resumption of it must start past.
    ///
    /// Every consumer holding the slot reads the book's own last word — no longer
    /// subscribed — before the book goes away, exactly as an in-process reader does, rather
    /// than a state that still says `Live` forever. The entry itself stays: it is this
    /// market's for the life of the segment generation, whether or not the market comes back.
    fn retire(&mut self, slug: &str, published: &PublishedBook) {
        self.publish_state(slug, published, None);
        if let Some(entry) = self.entries.get_mut(slug)
            && entry.live
        {
            entry.live = false;
            entry.retired_epoch = published.continuity().epoch();
            self.live = self.live.saturating_sub(1);
        }
    }

    /// Publishes one revision of latest state for `slug`.
    ///
    /// `arrival` is when the socket read that caused this change returned, on both clocks, or
    /// `None` for a change no venue frame drove — which stamps the slot 0 and records no
    /// latency sample.
    ///
    /// A publication for a market this segment holds no entry for is the fail-closed guard
    /// on a routing mistake: it latches [`WriterError::UnknownHandle`] rather than
    /// discarding a book's state silently. Every resident market is installed before its
    /// first publication, so it is not a reachable state.
    fn publish_state(
        &mut self,
        slug: &str,
        published: &PublishedBook,
        arrival: Option<FrameArrival>,
    ) {
        if self.failure.is_some() {
            return;
        }
        let Some(handle) = self.handle(slug) else {
            self.failure = Some(WriterError::UnknownHandle);
            return;
        };
        if let Err(error) =
            self.writer
                .publish(handle, published, FrameArrival::slot_stamp(arrival))
        {
            self.failure = Some(error);
            return;
        }
        self.record_publish_latency(arrival);
    }

    /// Records what the publication that has just completed cost, from the frame's arrival to
    /// the moment the writer returned.
    ///
    /// Called after [`SegmentWriter::publish`] answered `Ok`, so the interval covers the whole
    /// round a consumer depends on — the levels staged, the slot written and closed stable,
    /// the dirty-index entry posted, the doorbell rung, the publication generation advanced,
    /// and the wake posted — rather than ending at the commit stamp the writer takes before
    /// any of that. It is measured monotonic-to-monotonic: `arrival.monotonic` from the socket
    /// read, and one [`Instant::now`] here. The slot's own wall-clock stamps are never read
    /// back for it, so no system-clock correction between the two can drop the sample or
    /// invent a tail.
    ///
    /// One clock read and one bounded bucket increment on the publish path, and nothing else:
    /// no allocation and no system call, the same cost class as the ingest queue age's
    /// recording. Nothing here can fail, so measuring a publication can never delay or refuse
    /// one. A publication no venue frame drove carries no arrival and is counted as nothing
    /// rather than recorded as zero.
    fn record_publish_latency(&mut self, arrival: Option<FrameArrival>) {
        let Some(arrival) = arrival else {
            return;
        };
        self.publish_latency
            .record(micros_between(arrival.monotonic, Instant::now()));
    }

    /// Publishes one commit's mutations in the writer's own order, then the state that
    /// commit produced.
    ///
    /// Mutations go first so no published state ever advertises a position the ring was not
    /// given: a refused mutation leaves the state slot naming that same position as its next
    /// one, which is the honest report of a stream that stopped rather than a cursor
    /// pointing past a hole.
    fn publish_commit(
        &mut self,
        slug: &str,
        commit: &BookCommit,
        published: &PublishedBook,
        arrival: FrameArrival,
    ) {
        if self.failure.is_some() {
            return;
        }
        let Some(handle) = self.handle(slug) else {
            self.failure = Some(WriterError::UnknownHandle);
            return;
        };
        for record in commit.mutations() {
            if let Err(error) = self.writer.publish_mutation(
                handle,
                commit.revision(),
                record.cursor(),
                record.mutation(),
                arrival.wall_clock_nanos,
            ) {
                self.failure = Some(error);
                return;
            }
        }
        self.publish_state(slug, published, Some(arrival));
    }

    /// Publishes one venue-reported resolution into `slug`'s ring, then republishes the
    /// state that names the position past it.
    ///
    /// The republish carries the stamps the book's own last commit made rather than fresh
    /// ones, because a resolution commits no revision and restamping would make a book that
    /// has not moved look freshly committed. It is not optional tidiness: the state slot's
    /// `(epoch, next_position)` is the boundary every attachment starts from, and a boundary
    /// left behind a live ring lets a resolution-only burst wrap past the slot a new
    /// attachment probes, which fails that attachment forever with no way to report it.
    ///
    /// The republish is therefore handed no arrival and records no publish-latency sample:
    /// it commits no revision, and timing a state slot that only advanced its boundary would
    /// put an interval in the distribution that no revision a consumer reads corresponds to.
    fn publish_resolution(
        &mut self,
        slug: &str,
        delivery: &ResolutionDelivery,
        published: &PublishedBook,
        arrival: FrameArrival,
    ) {
        if self.failure.is_some() {
            return;
        }
        let Some(handle) = self.handle(slug) else {
            self.failure = Some(WriterError::UnknownHandle);
            return;
        };
        if let Err(error) = self.writer.publish_resolution(
            handle,
            delivery.revision(),
            delivery.cursor(),
            delivery.resolution(),
            arrival.wall_clock_nanos,
        ) {
            self.failure = Some(error);
            return;
        }
        if let Err(error) = self.writer.republish_carrying_stamps(handle, published) {
            self.failure = Some(error);
        }
    }

    /// Whether this segment can carry `resolution`'s venue-native texts verbatim.
    fn carries(&self, resolution: &MarketResolution) -> bool {
        resolution_fits_the_ring(resolution)
    }

    fn handle(&self, slug: &str) -> Option<MarketHandle> {
        self.entries.get(slug).map(|entry| entry.handle)
    }
}

/// One market's book and everything the shard knows about its demand and its evidence.
struct MarketEntry {
    market: MarketRef,
    writer: BookWriter,
    desired: bool,
    subscription: SubscriptionState,
    /// The connection generation carrying this market's subscription, or `None` while no
    /// established connection carries it.
    ///
    /// Set when a connection reports its subscription established, for every market that
    /// connection was dialled with, and cleared when that connection ends. It is the whole
    /// of this market's subscription evidence: the venue offers no per-market
    /// acknowledgment, and a set change is carried by a new connection rather than by a
    /// command on this one, so "which generation carries it" is also "which set it belongs
    /// to".
    subscribed_from: Option<u64>,
    /// Whether the connection currently being dialled was given this market.
    dialled: bool,
    /// Whether this book has ever held authority. Sticky: it is what separates a market
    /// that can lose authority from one that has never had any to lose.
    base_accepted: bool,
    /// Whether this market holds a base accepted on the connection currently running.
    ///
    /// Cleared by any loss of this market's authority, not only by the connection ending: a
    /// generation that served a base and then lost it did not carry this market through, and
    /// counting it as a successful cycle would let a market that keeps losing authority
    /// never reach a terminal verdict.
    based_this_generation: bool,
    /// Connection generations that have ended without giving this market a base, counted
    /// per market rather than per shard: one market's healthy stream says nothing about
    /// another's, and a shard-wide counter that any market could reset would let a busy
    /// book mask a silent one's failure to recover for as long as the busy one kept
    /// arriving.
    recovery_attempts: u32,
    /// Whether this market's terminal recovery verdict has been reported.
    unavailable_reported: bool,
    /// The desired-set version this entry was installed at, which is the incarnation of
    /// this market that the books and gates below belong to.
    ///
    /// A market removed and added back is a new entry with a new book, a new epoch and an
    /// empty gate. A connection dialled before that still names the slug in the immutable
    /// set it put on the wire, so carriage judged from that set alone would let the retired
    /// subscription's frames land in the new incarnation. Comparing this against a
    /// connection's own dialled version is what keeps a frame from a retired incarnation out
    /// of a book that is not the one it was subscribed for.
    installed_version: u64,
    /// The most recent resolution the venue reported for this market.
    ///
    /// Retained whatever the book is doing and whatever the lanes could carry: what the
    /// venue reported is true whether or not this daemon's delivery surfaces can reproduce
    /// it. Delivery is what depends on the stream.
    latest_resolution: Option<Arc<MarketResolution>>,
    /// This market's own pooled publish gate, created by the first pooled arrival that
    /// reaches it and dropped when the shard's licence is withdrawn.
    ///
    /// Per market because the venue's counter is venue-global and non-contiguous per market
    /// (`docs/limitless.md`): one shard-wide gate would read a quiet market's key as skew
    /// against a busy one's. Scoping it per market weakens no tripwire condition — a
    /// subsequence of a per-connection monotone stream is itself monotone, so a socket's own
    /// inversion is still visible here, and a replacement's first key is judged against this
    /// market's own published floor.
    ///
    /// Created lazily, for the reason [`Standby::shadows`] are: a shard pays for the markets
    /// the venue actually serves it rather than for the whole declared set. Each gate's
    /// exact-content evidence is bounded twice by [`crate::MAX_EVIDENCE_ENTRIES`] and
    /// [`crate::MAX_EVIDENCE_BYTES`], so a shard's evidence is bounded by
    /// `max_markets * MAX_EVIDENCE_BYTES`; at every book depth this adapter admits the
    /// entry count is what binds, several orders of magnitude below the byte ceiling.
    gate: Option<PoolGate>,
    /// The pool sockets whose current connection-session has been seen at or beyond this
    /// market's published key, one bit per socket index.
    ///
    /// This is what a hand-back reads. The venue's key orders a connection-session's own
    /// stream, so a socket seen at or beyond what the book holds cannot go on to publish
    /// behind it, while one that has only ever been seen below it can — and the topology a
    /// withdrawal hands back to reads no key at all, applying whatever its one publishing
    /// connection sends next as forward history. Which socket published last does not answer
    /// this: the socket that lost the race to publish a frame is standing at that same key,
    /// and the socket that published one the book then passed is not.
    ///
    /// Recorded for every arrival the gate judged, cleared for a socket when that socket's
    /// session ends, and moved with a connection that takes the publishing slot.
    sockets_at_published: u8,
}

impl MarketEntry {
    /// Whether a dial taken at `dialled_set_version` may answer for this market.
    ///
    /// A dial older than the entry was given a slug this shard has since retired and
    /// installed again, so its subscription is to a book that no longer exists. Every place
    /// that reads a connection's dialled set against this market — its arrivals under a pool,
    /// and the subscription evidence a `Ready`, a handover or a promotion writes for the
    /// single-source path — asks this one question, because a dial that cannot publish for a
    /// market must not be recorded as the generation carrying it either.
    fn claimed_by(&self, dialled_set_version: u64) -> bool {
        self.installed_version <= dialled_set_version
    }

    fn live(&self) -> bool {
        matches!(self.writer.book().authority(), AuthorityState::Live)
    }

    /// Whether the venue is subscribed to this market right now, or is about to be because
    /// an emit carrying it is in flight.
    ///
    /// This is what decides whether a removed market must keep its book. A market the venue
    /// never heard of — added and removed between two emits, or added while no connection
    /// was up — can be forgotten the instant it stops being wanted, because no frame for it
    /// will ever arrive.
    fn carried(&self) -> bool {
        self.subscribed_from.is_some() || self.dialled
    }

    /// Whether this market has reported its terminal recovery verdict and still holds it.
    ///
    /// A terminal market's authority reason is sticky: an ordinary rail loss afterwards says
    /// nothing new, and letting it overwrite the verdict would tell an operator the market
    /// is merely disconnected when it is in fact unrecoverable. Only an accepted base clears
    /// it.
    fn terminal(&self) -> bool {
        self.unavailable_reported
    }

    /// Records that this market lost authority, which costs it the base-held evidence for
    /// the connection now running.
    fn lost_base(&mut self) {
        self.based_this_generation = false;
    }

    /// Whether this market is owed a fresh venue base right now: it is wanted, it held
    /// authority, and it does not hold it now.
    ///
    /// Deliberately not "not live". A market that has never established is
    /// [`AuthorityState::Synchronizing`], which is the ordinary state of a quiet market the
    /// venue has not spoken about yet, and treating that as recovery would make one silent
    /// member of a large set demand a reissue — and then a reconnect — forever.
    fn needs_recovery(&self) -> bool {
        self.desired && self.base_accepted && !self.live()
    }

    fn status(&self) -> MarketStatus {
        if !self.desired {
            return MarketStatus::Removed;
        }
        match self.writer.book().authority() {
            AuthorityState::Live => MarketStatus::Live,
            AuthorityState::Stale(reason) => MarketStatus::Stale(reason.clone()),
            AuthorityState::Unsubscribed
            | AuthorityState::Subscribing
            | AuthorityState::Synchronizing
            | AuthorityState::Recovering => MarketStatus::Reconciling,
        }
    }
}

/// Where the one serialized same-set reissue has got to.
///
/// One reissue is in flight at a time. It never changes the desired set — a set change is
/// carried by a fresh connection — so this is only ever the recovery half of
/// resubscribe-then-reconnect: the venue is asked for the set it already has, in the hope of
/// a fresh base for the markets that lost one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Reissue {
    Idle,
    /// Due at this instant.
    Pending(Instant),
    /// Handed to the connection, which owes the write by this instant. The instant is the
    /// pacer slot the command reserved plus the configured window, because the connection
    /// waits for that slot immediately before it writes and the slot is where this shard's
    /// command actually stands in the endpoint's queue. A command that never reaches the
    /// wire is answered with a fresh connection generation.
    ///
    /// It is also the state a handover refused for want of room lands in. The command was
    /// not taken, but the deadline owed is the same one: a connection that has not drained
    /// what is already queued for it, like one that has not written what it took, is a
    /// connection this shard stops waiting on at this instant and replaces.
    Requested(Instant),
    /// On the wire, as the connection reported: the venue has until this instant to serve
    /// the recovery bases it was sent for.
    Awaiting(Instant),
    /// The connection this reissue was for could not be handed the command because its
    /// control channel is closed, which only a connection whose task is ending does.
    ///
    /// It carries no instant, and so arms no timer: recovery from here is the connection's
    /// own end, which the run loop is already waiting on and which runs the reconnect
    /// ladder. Retrying against a channel whose receiver is gone could only ever fail again,
    /// and a timer that scheduled those retries would be this shard polling for news the
    /// join is about to deliver. [`Shard::arm_reissue`] arms only from [`Self::Idle`], so
    /// this state is not re-armed while the connection it belongs to is still being closed,
    /// and the connection's end returns the shard to `Idle`. A shard reports itself
    /// reconciling in this state, which it is: the market's recovery is still owed and the
    /// connection that owes it is being replaced.
    Unreachable,
}

impl Reissue {
    /// When this reissue next needs the run loop's attention, or `None` for a state that
    /// waits on an event rather than a clock.
    fn wake_at(self) -> Option<Instant> {
        match self {
            Self::Idle | Self::Unreachable => None,
            Self::Pending(at) | Self::Requested(at) | Self::Awaiting(at) => Some(at),
        }
    }
}

struct ActiveConnection {
    generation: u64,
    /// The complete market set this connection was dialled with, in slug order. The venue's
    /// subscription is replaced whole and this connection never changes it, so this is what
    /// the wire carries for as long as the connection lives, and what a desired set is
    /// compared against to decide whether a replacement is owed.
    markets: Vec<String>,
    /// The desired-set version this connection's dial carried, which is the incarnation of
    /// every market in [`Self::markets`] that its subscription belongs to.
    ///
    /// Not [`ConnectionSlot::dialled_version`], which is the version of a dial still in
    /// flight and is taken the moment that dial settles. This one belongs to the connection
    /// for as long as it lives, because what it subscribed to does not change.
    ///
    /// The set is text and a market's book is not: a removed market's book is retired and a
    /// market that returns through the same slug returns as a new entry with its own empty
    /// gate. Two dials can therefore name one slug and mean two different books, and this is
    /// what tells them apart.
    dialled_set_version: u64,
    spawned_at: Instant,
    handle: JoinHandle<ConnectionEndReason>,
    control: mpsc::Sender<ConnectionControl>,
    heartbeat_deadline: Option<Duration>,
    last_heartbeat: Option<Instant>,
    established: bool,
    produced_base: bool,
    /// The venue's own Engine.IO session identifier, from the open packet this connection
    /// negotiated. Reported to operators so one socket stays tellable from another on the
    /// venue's terms rather than only on this daemon's.
    session: Option<String>,
}

impl ActiveConnection {
    fn heartbeat_expiry(&self) -> Option<Instant> {
        match (self.last_heartbeat, self.heartbeat_deadline) {
            (Some(at), Some(deadline)) => Some(at + deadline),
            _ => None,
        }
    }

    /// Whether this connection's transport is viable right now: its task is still running
    /// and its heartbeat evidence has not expired. A connection that has not yet announced
    /// a cadence has no expired evidence and is viable on its task alone.
    ///
    /// This is what keeps a correlated loss from being answered with a source transition
    /// onto a connection that is already dead: an agreeing shadow says what a standby knew,
    /// not that it can still produce.
    fn is_viable(&self, now: Instant) -> bool {
        !self.handle.is_finished() && self.heartbeat_expiry().is_none_or(|expiry| expiry > now)
    }
}

struct FencedConnection {
    handle: JoinHandle<ConnectionEndReason>,
    expires_at: Instant,
}

/// Which connection role a notice, timer, or connection end belongs to.
///
/// [`Self::Standby`] carries the index of the role, because a shard may run several: one for
/// every connection beyond the publishing one. The index names the role, not the connection
/// occupying it, so a replaced connection inherits its role's reconnect ladder.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Slot {
    Primary,
    Standby(usize),
}

impl Slot {
    /// The role a connection in this slot holds. It follows the slot rather than the dial:
    /// a promoted standby's connection has moved into [`Self::Primary`] by the time it
    /// publishes anything, so it reports the publishing role from there.
    fn role(self) -> ReplicaRole {
        match self {
            Self::Primary => ReplicaRole::PublishingPrimary,
            Self::Standby(_) => ReplicaRole::HotStandby,
        }
    }

    /// This role's position in the shard's connection order: 0 for the publishing role and
    /// one past its own index for every other.
    fn index(self) -> usize {
        match self {
            Self::Primary => 0,
            Self::Standby(index) => index + 1,
        }
    }
}

/// One role's own transport state: the connection currently feeding it, at most one fenced
/// generation still draining, its own reconnect ladder, and the desired-set versions its
/// dial and its wire carry.
///
/// Every role's set versions are its own because every role dials independently: a set
/// change replaces each role's connection on that role's own schedule, and until it has,
/// that role's wire still carries the old set.
#[derive(Default)]
struct ConnectionSlot {
    active: Option<ActiveConnection>,
    fenced: Option<FencedConnection>,
    reconnect_at: Option<Instant>,
    backoff_attempt: u32,
    /// The desired-set version the connection currently being dialled carries, if one is.
    dialled_version: Option<u64>,
    /// The desired-set version this role's connection established on the wire.
    wire_version: u64,
}

/// One hot standby: its own connection role, and the shadow book of every market its
/// connection has delivered.
///
/// The shadows are second books only in the bookkeeping sense. None is published, none is
/// read by a consumer, and none is added to a market's depth: a market's liquidity stays one
/// physical book with two views however many standbys shadow it.
///
/// A shadow is created by the first arrival that populates it rather than by the dial, so a
/// standby costs memory for the markets the venue actually serves it and not for the whole
/// declared set. A market with no shadow has no comparable history, which is exactly the
/// verdict [`agreement`] gives it.
#[derive(Default)]
struct Standby {
    slot: ConnectionSlot,
    shadows: HashMap<String, OrderBook>,
    /// Every desired market this standby could carry across a promotion decided right now,
    /// maintained as the two sides move rather than recomputed when someone asks.
    ///
    /// The membership rule is [`promotable`], the same predicate the takeover itself
    /// applies, so the coverage an operator reads is the coverage a loss would find and
    /// never an optimistic count of markets a takeover would refuse. Entries are added and
    /// removed at the instant a shadow or a published book changes; a scrape reads
    /// [`HashSet::len`]. Bounded by the shard's market capacity, since a market is only
    /// ever here while this shard holds a book for it.
    agreeing: HashSet<String>,
}

/// What a shard's connections beyond the first do, decided once at construction from the
/// configured venue key declaration and never re-decided upward.
///
/// It is held apart from the per-market gates because the licence is the shard's and the
/// gates are each market's: the venue's counter is venue-global and non-contiguous per
/// market (`docs/limitless.md`), so one market's key stream cannot be ordered against
/// another's, while the observation that withdraws the licence is a fact about the venue
/// and withdraws it for every market at once.
enum Redundancy {
    /// The configured declaration grants no within-session ordering, so no pool was ever
    /// licensed and every connection beyond the publishing one is a hot standby.
    Unlicensed(DedupKeySemantics),
    /// Publishing across sockets on the declared key, until `degraded` says otherwise.
    Pooled {
        declaration: DedupKeyDeclaration,
        sockets: usize,
        degraded: Option<PoolDegradeReason>,
    },
}

impl Redundancy {
    /// Reads the topology the configured declaration licenses for `sockets` connections.
    ///
    /// A declaration [`PoolGate`] refuses licenses no pool, whatever it refused for: a
    /// socket count outside the gate's own structural range is as much a reason to run hot
    /// standbys as a key that orders nothing.
    fn licensed(declaration: DedupKeyDeclaration, sockets: usize) -> Self {
        match PoolGate::new(declaration, sockets) {
            Ok(_) => Self::Pooled {
                declaration,
                sockets,
                degraded: None,
            },
            Err(PoolError::KeyOrdersNothing(semantics)) => Self::Unlicensed(semantics),
            Err(PoolError::SocketCountOutOfRange) => Self::Unlicensed(declaration.semantics()),
        }
    }

    fn armed(&self) -> bool {
        matches!(self, Self::Pooled { degraded: None, .. })
    }

    fn state(&self) -> PoolState {
        match self {
            Self::Unlicensed(semantics) => PoolState::Unlicensed {
                semantics: semantics.as_label().to_owned(),
            },
            Self::Pooled { degraded: None, .. } => PoolState::Armed,
            Self::Pooled {
                degraded: Some(reason),
                ..
            } => PoolState::Degraded { reason: *reason },
        }
    }
}

enum Wake {
    Finished,
    HeartbeatMissed(Slot),
    FencedExpired(Slot),
    Reconnect,
    ReissueDue,
    Ended(Slot, ConnectionEndReason),
    Notice(ConnectionNotice),
    Command(Command),
}

/// One venue connection, one aggregate desired market set, one book per market.
pub struct Shard {
    config: ShardConfig,
    level_capacity: LevelCapacity,
    observer_capacity: ObserverCapacity,
    markets: HashMap<String, MarketEntry>,
    /// The book each market this shard retired from its delivery segment was closed with,
    /// kept so the market can resume through its own entry with a stream that only moves
    /// forward. Empty for a shard publishing into no segment, and bounded by the segment
    /// directory's capacity, since an entry is never given to another market.
    retired: HashMap<String, OrderBook>,
    /// The shared-memory segment every book in this shard publishes into, once one is
    /// installed. Owned by the shard task, which is the segment's single writer.
    segment: Option<ShardSegment>,
    /// The role that owns every book this shard publishes. Exactly one connection at a time
    /// occupies it, which is what makes the shard one writer per book.
    primary: ConnectionSlot,
    /// One role per configured replica beyond the publishing one; empty at the default
    /// `replicas = 1`, where every path below reduces to the single-connection shard.
    standbys: Vec<Standby>,
    /// What those roles do about publishing, or `None` at the default single connection.
    redundancy: Option<Redundancy>,
    /// The pool socket positions where a connection-session has begun and ended, one bit per
    /// socket index.
    ///
    /// A market's gate is created by the first arrival that reaches it, so a session that ran
    /// and ended while that market was quiet would leave no gate to have recorded it, and the
    /// gate built afterwards would read that position's next first key as the pool's first
    /// sight of the socket rather than as a replacement's — the one reading that never checks
    /// the published floor. The fact belongs to the socket and not to whichever market
    /// happened to be busy, so it is held here, where it outlives every gate, and applied to
    /// each gate as that gate is created: a gate built late answers exactly as one that
    /// existed all along.
    pool_sessions_ended: u8,
    next_generation: u64,
    notices_tx: mpsc::Sender<ConnectionNotice>,
    notices_rx: mpsc::Receiver<ConnectionNotice>,
    commands_tx: mpsc::Sender<Command>,
    commands_rx: mpsc::Receiver<Command>,
    frames: Arc<AtomicU64>,
    stop: Arc<Notify>,
    reissue: Reissue,
    /// Bumped by every change to the desired set, so a reissue can name the set it carried.
    desired_version: u64,
    /// How many desired markets are owed a fresh venue base. Maintained incrementally so
    /// nothing on the update path scans the set.
    recovering: usize,
    desired_count: usize,
    /// Removed markets whose books are still waiting for the emit that reconciles their
    /// removal, oldest first. Bounded by [`MAX_REMOVING_MARKETS`].
    removing: VecDeque<String>,
    /// Whether an unscoped rail loss has already been reported against the connection now
    /// running. A second one costs nothing and is not walked over the set again.
    rail_loss_latched: bool,
    position: u64,
    /// The provenance position counter a shadow-only arrival spends, kept apart from
    /// [`Self::position`] so that the published stream's positions stay a function of
    /// arrivals that could have been published.
    ///
    /// Under a hot-standby topology that is every published arrival and no other, so
    /// enabling redundancy changes no number a consumer reads. An armed pool is the
    /// deliberate exception: every socket is an authoritative source, so every pooled
    /// arrival spends [`Self::position`] whichever socket carried it and whatever the gate
    /// then decides — the counter cannot be assigned before the verdict it would depend on.
    /// A pooled run's positions are therefore gappier than a single-source run's, which is
    /// what an evidence counter is allowed to be and what a non-monotonic one would not be.
    shadow_position: u64,
    run_start: Instant,
    queue_age: QueueAge,
    ingest_stall: Option<Duration>,
    jitter: RandomState,
    stats: ShardStats,
}

impl Shard {
    /// Builds a shard for one connection and its starting desired set, with no connection
    /// yet running.
    ///
    /// Fails when a market in the starting set is not a valid venue-native identifier, when
    /// the starting set is larger than the configured market capacity, when a capacity is
    /// out of range, or when `max_recovery_attempts` is zero, which would report recovery
    /// exhaustion before any recovery was attempted. A repeated slug in the starting set is
    /// one market, not two: the desired set is a set.
    ///
    /// The two configured pacing figures are bounded above by structure rather than by
    /// anything a venue places: `daily_attempt_budget` at
    /// [`WORST_CASE_ATTEMPT_CEILING`](crate::limitless::supervisor::WORST_CASE_ATTEMPT_CEILING),
    /// what the attempt ledger's storage is sized against, and `min_command_interval_ms` at
    /// [`MAX_COMMAND_INTERVAL_MS`](crate::limitless::supervisor::MAX_COMMAND_INTERVAL_MS),
    /// past which every reissue deadline built on top of the floor would outlast any answer
    /// a venue could give. A daemon document reaches the same two bounds in
    /// `crate::daemon::DaemonConfig::parse`, which names the field and the bound in its
    /// message.
    pub fn new(config: ShardConfig) -> Result<Self, ShardError> {
        if config.max_recovery_attempts == 0 {
            return Err(ShardError::RecoveryAttemptsZero);
        }
        if config.daily_attempt_budget > WORST_CASE_ATTEMPT_CEILING {
            return Err(ShardError::DailyAttemptBudgetTooLarge);
        }
        if config.min_command_interval_ms > MAX_COMMAND_INTERVAL_MS {
            return Err(ShardError::CommandIntervalTooLarge);
        }
        if config.ingest_capacity == 0 {
            return Err(ShardError::IngestCapacityZero);
        }
        if config.control_capacity == 0 {
            return Err(ShardError::ControlCapacityZero);
        }
        if config.replicas == 0 || config.replicas > MAX_REPLICAS {
            return Err(ShardError::ReplicasOutOfRange);
        }
        let level_capacity =
            LevelCapacity::new(config.level_capacity).map_err(ShardError::LevelCapacity)?;
        let observer_capacity = ObserverCapacity::new(config.observer_capacity)
            .map_err(ShardError::ObserverCapacity)?;
        let capacity = config.max_markets.min(MAX_SHARD_MARKETS);
        let (notices_tx, notices_rx) = mpsc::channel(config.ingest_capacity);
        let (commands_tx, commands_rx) = mpsc::channel(config.control_capacity);
        let mut shard = Self {
            level_capacity,
            observer_capacity,
            markets: HashMap::new(),
            retired: HashMap::new(),
            segment: None,
            primary: ConnectionSlot::default(),
            standbys: (1..config.replicas).map(|_| Standby::default()).collect(),
            redundancy: (config.replicas > 1)
                .then(|| Redundancy::licensed(config.dedup_key, config.replicas)),
            pool_sessions_ended: 0,
            next_generation: 1,
            notices_tx,
            notices_rx,
            commands_tx,
            commands_rx,
            frames: Arc::new(AtomicU64::new(0)),
            stop: Arc::new(Notify::new()),
            reissue: Reissue::Idle,
            desired_version: 0,
            recovering: 0,
            desired_count: 0,
            removing: VecDeque::new(),
            rail_loss_latched: false,
            position: 0,
            shadow_position: 0,
            run_start: Instant::now(),
            queue_age: QueueAge::new(),
            ingest_stall: config.ingest_stall,
            jitter: RandomState::new(),
            stats: ShardStats::default(),
            config,
        };
        shard.config.max_markets = capacity;
        for slug in shard.config.markets.clone() {
            if shard.markets.contains_key(&slug) {
                continue;
            }
            if shard.desired_count >= capacity {
                return Err(ShardError::MarketCapacityExceeded);
            }
            shard.install(slug).map_err(|refusal| match refusal {
                InstallRefusal::Identity(error) => ShardError::Market(error),
                InstallRefusal::Delivery(error) => ShardError::Segment(error),
                InstallRefusal::StreamNotRebased | InstallRefusal::ResumeUnavailable => {
                    ShardError::Segment(WriterError::MarketAlreadyInstalled)
                }
            })?;
        }
        Ok(shard)
    }

    /// Publishes every book this shard holds into `segment` from now on, on each book's own
    /// ordered commit path.
    ///
    /// Every resident market is installed in slug order, so which directory entry a market
    /// takes is a property of the set rather than of the order the operator typed it in, and
    /// each one's current state is published as it is installed. A market added later is
    /// installed when its book is created.
    ///
    /// A shard installs at most one segment for its whole lifetime; a second is refused with
    /// [`WriterError::SegmentAlreadyInstalled`] and never touched, because installing a
    /// second segment over the first would strand every consumer attached to it. Otherwise
    /// fails with whatever refused an install or the initial publication, leaving the shard
    /// publishing into no segment.
    pub fn publish_into(&mut self, mut segment: ShardSegment) -> Result<(), WriterError> {
        if self.segment.is_some() {
            return Err(WriterError::SegmentAlreadyInstalled);
        }
        let mut slugs: Vec<&str> = self.markets.keys().map(String::as_str).collect();
        slugs.sort_unstable();
        for slug in slugs {
            let published = self.markets[slug].writer.published();
            segment
                .install(slug, &published)
                .map_err(SegmentRefusal::writer_error)?;
        }
        self.segment = Some(segment);
        Ok(())
    }

    /// Whether this shard's segment would seat an incarnation of `slug` at all, without
    /// changing anything.
    ///
    /// A shard publishing into no segment always would. One publishing into a segment
    /// answers what its directory says: a market whose entry a live incarnation holds cannot
    /// take a second, and a market with no entry needs a free one. A market whose own entry
    /// is *retired* may resume through it, so this does not refuse it — whether the
    /// particular book offered may resume is [`ShardSegment::install`]'s check, against a
    /// book this caller does not hold yet.
    fn installable(&self, slug: &str) -> Result<(), InstallRefusal> {
        self.segment.as_ref().map_or(Ok(()), |segment| {
            segment.installable(slug).map_err(InstallRefusal::from)
        })
    }

    /// The segment publication that latched, if one refused this shard's writes.
    ///
    /// A run that ends with this set ended because of it: nothing more was published, and a
    /// consumer left reading a segment whose writer had silently stopped is the one outcome
    /// this is here to prevent.
    pub fn segment_failure(&self) -> Option<&WriterError> {
        self.segment.as_ref().and_then(ShardSegment::failure)
    }

    /// The most recent resolution the venue reported for `slug`, or `None` for a market
    /// this shard holds no book for or that the venue has not resolved.
    ///
    /// Independent of book state and of what the delivery lanes could carry.
    pub fn latest_resolution(&self, slug: &str) -> Option<&Arc<MarketResolution>> {
        self.markets
            .get(slug)
            .and_then(|entry| entry.latest_resolution.as_ref())
    }

    /// The control surface for this shard. Cloneable, and usable before the run starts.
    pub fn handle(&self) -> ShardHandle {
        ShardHandle {
            commands: self.commands_tx.clone(),
        }
    }

    /// A handle that ends a run early. Independent of the run deadline.
    pub fn stopper(&self) -> ShardStopper {
        ShardStopper {
            signal: Arc::clone(&self.stop),
        }
    }

    /// Attaches a consumer to one market's book before the run starts, guaranteeing it sees
    /// every revision that market ever publishes. `None` for a market not in the set.
    pub fn observe(&self, slug: &str) -> Option<BookObserver> {
        self.markets.get(slug).map(|entry| entry.writer.attach())
    }

    /// Runs until `deadline`, or until a [`ShardStopper`] fires, keeping every market's book
    /// alive across set changes and connection failures, and returns what the run observed.
    ///
    /// `deadline` is `None` in a daemon: a feed does not expire, and a run that ended on its
    /// own clock would look exactly like one that failed. A bounded deadline is a test and
    /// diagnostic facility.
    ///
    /// The wait is ordered: deadlines and timers are polled before the control queue, the
    /// control queue before the ingest queue, and the ingest queue before the connection's
    /// own end. So market data can never starve liveness detection, a scheduled reconnect,
    /// or an operator command, and everything a generation produced while it was still
    /// current is applied before it is closed.
    pub async fn run_until(&mut self, deadline: Option<Instant>) -> ShardStats {
        let stop = Arc::clone(&self.stop);
        loop {
            if deadline.is_some_and(|at| Instant::now() >= at) {
                break;
            }
            if self.segment_failure().is_some() {
                break;
            }
            if let Some(stall) = self
                .ingest_stall
                .take_if(|_| self.stats.snapshots_applied > 0)
            {
                tokio::time::sleep(stall).await;
            }
            self.shed_idle_connection();
            self.reconcile_set();
            self.spawn_due();
            self.arm_reissue();
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
            let primary_reconnect = self.primary.reconnect_at;
            let standby_reconnect = self.earliest_standby(|slot| slot.reconnect_at);
            let reissue = self.reissue.wake_at();
            let wake = tokio::select! {
                biased;
                () = sleep_until_opt(deadline) => Wake::Finished,
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
                () = sleep_until_opt(reissue) => Wake::ReissueDue,
                command = self.commands_rx.recv() => match command {
                    Some(command) => Wake::Command(command),
                    None => Wake::Finished,
                },
                notice = self.notices_rx.recv() => match notice {
                    Some(notice) => Wake::Notice(notice),
                    None => Wake::Finished,
                },
                reason = join_active(&mut self.primary.active) => Wake::Ended(Slot::Primary, reason),
                ended = join_any_standby(&mut self.standbys) => {
                    Wake::Ended(Slot::Standby(ended.0), ended.1)
                }
            };
            match wake {
                Wake::Finished => break,
                Wake::HeartbeatMissed(slot) => self.on_heartbeat_missed(slot),
                Wake::FencedExpired(slot) => self.release_fenced(slot),
                Wake::Reconnect => {}
                Wake::ReissueDue => self.on_reissue_due(),
                Wake::Command(command) => self.on_command(command),
                Wake::Notice(notice) => self.on_notice(notice),
                Wake::Ended(slot, reason) => {
                    let ended = self.slot_mut(slot).and_then(|slot| slot.active.take());
                    let produced_base = ended.as_ref().is_some_and(|active| active.produced_base);
                    let established = ended.as_ref().is_some_and(|active| active.established);
                    let lifetime = ended.map_or(Duration::ZERO, |active| {
                        Instant::now().saturating_duration_since(active.spawned_at)
                    });
                    self.on_slot_ended(slot, reason, produced_base, established, lifetime);
                }
            }
        }
        self.finish()
    }

    /// The earliest deadline `deadline` reports across the standby roles, and which role it
    /// belongs to.
    ///
    /// Several roles hold the same kinds of timer while one wait arm can carry only one
    /// deadline. Waking on the earliest and re-deciding is exactly what one role per arm
    /// did: every judgement that follows a wake re-reads the role's own state rather than
    /// trusting the wake, so a role whose deadline was not the earliest is woken on a later
    /// iteration.
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

    /// Every connection role this shard runs, publishing role first.
    fn slots(&self) -> impl Iterator<Item = Slot> + use<> {
        let standbys = self.standbys.len();
        core::iter::once(Slot::Primary).chain((0..standbys).map(Slot::Standby))
    }

    /// The role whose running connection carries `generation`, or `None` for a generation
    /// this shard no longer runs — which is exactly what makes a late frame fenced.
    ///
    /// The publishing role is tried first, because on a shard serving consumers almost every
    /// notice is its. Generations come from one counter shared by every role, so a
    /// generation identifies a connection across the whole shard and this is a comparison
    /// rather than a search.
    fn slot_of(&self, generation: u64) -> Option<Slot> {
        if self
            .primary
            .active
            .as_ref()
            .is_some_and(|active| active.generation == generation)
        {
            return Some(Slot::Primary);
        }
        self.standbys
            .iter()
            .position(|standby| {
                standby
                    .slot
                    .active
                    .as_ref()
                    .is_some_and(|active| active.generation == generation)
            })
            .map(Slot::Standby)
    }

    /// The index of the first standby role holding a connection that can still deliver.
    ///
    /// First rather than best: `docs/design.md`'s promotion rule states an eligibility
    /// predicate, not a ranking, so nothing here weighs one eligible standby against
    /// another. A later standby may agree on markets this one does not; those markets are
    /// refused and recover through the ordinary rail.
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

    /// Creates a book for `slug`, gives it a place in the delivery segment, and takes it
    /// into the desired set.
    ///
    /// The segment place comes first, before anything about the shard changes: a market
    /// whose state and mutations no consumer could ever read is refused rather than carried
    /// as a book nobody can see, and a refusal leaves the desired set exactly as it was.
    ///
    /// A market this shard retired while publishing into a segment resumes from the book it
    /// retired ([`Self::resume`]) rather than from an empty one, because the entry it comes
    /// back through is its own and the stream that entry addresses may only ever move
    /// forward. Every other market starts from an empty book at epoch zero.
    fn install(&mut self, slug: String) -> Result<(), InstallRefusal> {
        let market = MarketRef::new(
            Venue::new(VENUE).map_err(InstallRefusal::Identity)?,
            NativeMarketKey::new(NativeIdentifierKind::slug(), slug.as_str())
                .map_err(InstallRefusal::Identity)?,
        );
        let book = match self.retired.remove(slug.as_str()) {
            Some(retired) => Self::resume(retired)?,
            None => OrderBook::new(market.clone()),
        };
        let writer = BookWriter::new(book, self.observer_capacity);
        if let Some(segment) = self.segment.as_mut() {
            segment
                .install(slug.as_str(), &writer.published())
                .map_err(InstallRefusal::from)?;
        }
        self.desired_count = self.desired_count.saturating_add(1);
        self.desired_version = self.desired_version.saturating_add(1);
        self.markets.insert(
            slug,
            MarketEntry {
                market,
                writer,
                desired: true,
                subscription: SubscriptionState::Desired,
                subscribed_from: None,
                dialled: false,
                base_accepted: false,
                based_this_generation: false,
                recovery_attempts: 0,
                unavailable_reported: false,
                installed_version: self.desired_version,
                latest_resolution: None,
                gate: None,
                sockets_at_published: 0,
            },
        );
        Ok(())
    }

    /// Prepares a retired book to come back through its own delivery entry.
    ///
    /// The subscription that fed it lapsed, so what the book is owed is a fresh venue base
    /// and nothing before it: the stream is marked lost, which is the same state a market
    /// holds between a disconnect and its next base. That is what rebases the entry. A lost
    /// stream yields no position at all, and the only way out of it is a recovery base, which
    /// opens the next epoch, restarts positions at 0 and derives no mutation across the gap —
    /// so the resumed incarnation cannot produce a cursor the retired one already published,
    /// and every attached consumer reads an explicit continuity loss rather than a spliced
    /// stream.
    ///
    /// The levels the venue last reported are kept, under [`AuthorityState::Stale`], exactly
    /// as they are between any other loss and the base that ends it: an empty book here would
    /// be an invented one. Nothing the venue sent to the retired incarnation can reach this
    /// one — the writer, and with it every observer channel, is new.
    fn resume(mut retired: OrderBook) -> Result<OrderBook, InstallRefusal> {
        let _changed = retired
            .report_continuity_loss(
                ContinuityReason::Reconnect,
                AuthorityReason::SubscriptionLost,
            )
            .map_err(|_| InstallRefusal::ResumeUnavailable)?;
        Ok(retired)
    }

    fn on_command(&mut self, command: Command) {
        match command {
            Command::Add { slugs, reply } => {
                let outcomes = self.add_markets(slugs);
                let _ = reply.send(outcomes);
            }
            Command::Remove { slugs, reply } => {
                let outcomes = self.remove_markets(slugs);
                let _ = reply.send(outcomes);
            }
            Command::Status { reply } => {
                let status = self.status();
                let _ = reply.send(status);
            }
            Command::Metrics { reply } => {
                let metrics = self.metrics();
                let _ = reply.send(metrics);
            }
            Command::Observe { slug, reply } => {
                let observer = self.observe(slug.as_str());
                let _ = reply.send(observer);
            }
        }
    }

    /// Takes a batch into the desired set, answering each market's resulting state.
    ///
    /// A market already present is left exactly as it is — its book, its evidence, and its
    /// authority all untouched — so a repeated add is a read, not a write, and nothing about
    /// it reaches the venue.
    ///
    /// A market whose removal the venue has not reconciled yet is still resident but no
    /// longer wanted, and adding it back is a fresh incarnation rather than the retained
    /// one: the old writer is closed and a new book is installed, so nothing the venue sent
    /// for the incarnation the removal ended can reach the new one. That is a new member of
    /// the desired set, so it is refused when the set is already at capacity, and a refusal
    /// leaves the removal exactly as it was.
    ///
    /// A market that has come and gone before comes back through its own delivery entry.
    /// The entry is never given to a different market — that is the ABI's identity rule and
    /// it is unchanged — but the same market resuming on it is not identity reuse, and the
    /// ABI already carries the cell that makes it safe: the resumed incarnation carries the
    /// retired book's continuity forward and re-enters with its stream lost, so its first
    /// venue base is a recovery base that opens the next epoch, restarts positions at 0, and
    /// derives nothing across the gap. Every attached consumer reads that as an explicit
    /// rebase. What is refused, with [`MarketRejection::DeliveryUnavailable`], is the case
    /// that cannot be made safe: an entry a live incarnation still holds, a book that could
    /// still produce a position its retired incarnation published, and a directory with no
    /// entry left for a market that has never held one. The refusal is asked for before
    /// anything is closed, so a removal still waiting on the venue is left exactly as it was.
    fn add_markets(&mut self, slugs: Vec<String>) -> Vec<MarketOutcome> {
        let capacity = self.config.max_markets;
        let resident = capacity.saturating_add(MAX_REMOVING_MARKETS);
        let outcomes: Vec<MarketOutcome> = slugs
            .into_iter()
            .map(|slug| {
                let present = self.markets.get(slug.as_str());
                let status = match present {
                    Some(entry) if entry.desired => entry.status(),
                    Some(_) if self.desired_count >= capacity => {
                        MarketStatus::Rejected(MarketRejection::CapacityExceeded)
                    }
                    Some(_) => match self.installable(slug.as_str()) {
                        Err(refusal) => MarketStatus::Rejected(refusal.rejection()),
                        Ok(()) => {
                            self.close(slug.as_str());
                            match self.install(slug.clone()) {
                                Ok(()) => MarketStatus::Accepted,
                                Err(refusal) => MarketStatus::Rejected(refusal.rejection()),
                            }
                        }
                    },
                    None if self.desired_count >= capacity || self.markets.len() >= resident => {
                        MarketStatus::Rejected(MarketRejection::CapacityExceeded)
                    }
                    None => match self.install(slug.clone()) {
                        Ok(()) => MarketStatus::Accepted,
                        Err(refusal) => MarketStatus::Rejected(refusal.rejection()),
                    },
                };
                MarketOutcome { slug, status }
            })
            .collect();
        self.recount_recovering();
        outcomes
    }

    /// Ends one market's book: its final published state says it is no longer subscribed,
    /// and every reader holding it sees that before the writer goes away.
    ///
    /// Returns whether a book was there to close. The market's place in the removal queue
    /// goes with it, so a book is never counted as waiting for a reconciliation it has
    /// already stopped waiting for.
    ///
    /// Every standby's shadow of the market goes with it, and so does the agreement that
    /// shadow was carrying. A shadow outliving its market would be state nothing can update
    /// — no arrival is taken for a market this shard holds no book for — and a market
    /// returning through the same slug would then be compared against the state its previous
    /// incarnation left behind, which is not evidence about the new one.
    ///
    /// A shard publishing into a segment keeps the closed book aside rather than dropping it.
    /// The delivery entry it retires is this market's for the life of the segment generation,
    /// and a market that comes back must come back through that entry with a stream that only
    /// moves forward; the retired book is what carries the epoch that makes that true. The
    /// set is bounded by the directory's own capacity — an entry is never given to another
    /// market — and a book leaves it the moment its market resumes. A shard publishing into
    /// no segment keeps nothing: no consumer addresses a slot there, and a fresh incarnation
    /// is a fresh book.
    fn close(&mut self, slug: &str) -> bool {
        let Some(mut entry) = self.markets.remove(slug) else {
            return false;
        };
        if let Err(error) = entry.writer.unsubscribe() {
            let key = book_error_key(&error);
            self.stats.record_failure(key);
        }
        let retire = self.segment.is_some();
        if let Some(segment) = self.segment.as_mut() {
            segment.retire(slug, &entry.writer.published());
        }
        if retire {
            let _replaced = self
                .retired
                .insert(slug.to_owned(), entry.writer.book().clone());
        }
        if entry.desired {
            self.desired_count = self.desired_count.saturating_sub(1);
        }
        self.removing.retain(|waiting| waiting != slug);
        for standby in &mut self.standbys {
            let _ = standby.shadows.remove(slug);
            let _ = standby.agreeing.remove(slug);
        }
        self.stats.markets_dropped = self.stats.markets_dropped.saturating_add(1);
        true
    }

    /// Records that a market's removal is waiting for the venue, evicting the oldest such
    /// market when more are waiting than [`MAX_REMOVING_MARKETS`] allows.
    fn enqueue_removal(&mut self, slug: &str) {
        self.removing.retain(|waiting| waiting != slug);
        self.removing.push_back(slug.to_owned());
        while self.removing.len() > MAX_REMOVING_MARKETS {
            let Some(evicted) = self.removing.pop_front() else {
                break;
            };
            if self.close(evicted.as_str()) {
                self.stats.tombstones_evicted = self.stats.tombstones_evicted.saturating_add(1);
            }
        }
    }

    /// Takes a batch out of the desired set, answering each market's resulting state.
    ///
    /// Input is validated exactly as [`Self::add_markets`] validates it: a slug that is not
    /// a venue-native market identifier is rejected input, not a market that happens to be
    /// absent, so an operator learns the difference between a typo and a no-op.
    ///
    /// A market that is not in the set is answered [`MarketStatus::Removed`] and nothing
    /// happens: no book is created, no state changes, and no venue command is issued.
    ///
    /// A market the venue is subscribed to stops being desired immediately but keeps its
    /// book and its authority until the reissue that excludes it is acknowledged, because
    /// until then the venue is still sending its frames. It stops being promotion coverage
    /// at once, though: a market nobody wants is one no takeover would carry, whatever any
    /// standby still holds for it. A market the venue never heard of
    /// is forgotten at once — no frame for it can arrive — which is what stops add-and-remove
    /// churn on a shard with no connection from accumulating books nobody wants.
    fn remove_markets(&mut self, slugs: Vec<String>) -> Vec<MarketOutcome> {
        let mut outcomes = Vec::with_capacity(slugs.len());
        let mut version_bumps = 0u64;
        for slug in slugs {
            if !is_market_slug(slug.as_str()) {
                outcomes.push(MarketOutcome {
                    slug,
                    status: MarketStatus::Rejected(MarketRejection::InvalidIdentifier),
                });
                continue;
            }
            let carried = match self.markets.get_mut(slug.as_str()) {
                Some(entry) if entry.desired => {
                    entry.desired = false;
                    entry.subscription = SubscriptionState::Removing;
                    version_bumps = version_bumps.saturating_add(1);
                    Some(entry.carried())
                }
                _ => None,
            };
            if carried.is_some() {
                self.retire_agreement(slug.as_str());
            }
            match carried {
                Some(true) => {
                    self.desired_count = self.desired_count.saturating_sub(1);
                    self.enqueue_removal(slug.as_str());
                }
                Some(false) => {
                    self.desired_count = self.desired_count.saturating_sub(1);
                    let _closed = self.close(slug.as_str());
                }
                None => {}
            }
            outcomes.push(MarketOutcome {
                slug,
                status: MarketStatus::Removed,
            });
        }
        self.desired_version = self.desired_version.saturating_add(version_bumps);
        self.recount_recovering();
        outcomes
    }

    fn status(&self) -> ShardStatus {
        self.check_agreement_tracking();
        let mut markets: Vec<MarketReport> = self
            .markets
            .iter()
            .map(|(slug, entry)| MarketReport {
                slug: slug.clone(),
                subscription: entry.subscription,
                status: entry.status(),
                revision: entry.writer.book().revision(),
                continuity_epoch: entry.writer.book().continuity().epoch(),
            })
            .collect();
        markets.sort_by(|left, right| left.slug.cmp(&right.slug));
        ShardStatus {
            markets,
            desired: self.desired_count,
            replicas: self.config.replicas,
            connections: self.connection_reports(),
            pool: self.pool_report(),
            standby_agreeing_markets: self.promotion_coverage(),
            subscribed: self
                .primary
                .active
                .as_ref()
                .is_some_and(|active| active.established),
            reconciling: !matches!(self.reissue, Reissue::Idle),
            segment: self
                .segment
                .as_ref()
                .map(|segment| segment.name().to_owned()),
            segment_markets: self.segment.as_ref().map_or(0, ShardSegment::installed),
            queue_age: self.queue_age.summary(),
            publish_latency: self.publish_latency(),
        }
    }

    /// Every connection this shard holds, publishing role first, with each standby's
    /// current agreement coverage.
    ///
    /// Agreement is read here, never computed: each standby carries the set of markets it
    /// could take over, maintained as arrivals move the two sides. A scrape and a status
    /// page both reach a shard through its one control queue, on the runtime that also
    /// feeds every book in the set, so a per-request walk comparing every market against
    /// every standby would put observability traffic in front of market data.
    fn connection_reports(&self) -> Vec<ConnectionReport> {
        let armed = self.pool_armed();
        self.slots()
            .filter_map(|slot| {
                let active = self.slot(slot).and_then(|state| state.active.as_ref())?;
                Some(ConnectionReport {
                    role: slot.role(),
                    generation: active.generation,
                    session: active.session.clone(),
                    established: active.established,
                    agreeing_markets: match slot {
                        Slot::Primary => None,
                        Slot::Standby(index) => Some(self.agreeing_markets(index)),
                    },
                    pool_socket: armed.then(|| slot.index()),
                })
            })
            .collect()
    }

    /// How many desired markets standby `index` could carry across a promotion decided now.
    ///
    /// O(1): the answer is the size of the set [`Self::refresh_shadow_agreement`] maintains.
    fn agreeing_markets(&self, index: usize) -> usize {
        self.standbys
            .get(index)
            .map_or(0, |standby| standby.agreeing.len())
    }

    /// The same figure derived from scratch, for the invariant check below.
    #[cfg(debug_assertions)]
    fn agreeing_markets_recomputed(&self, index: usize) -> usize {
        let Some(standby) = self.standbys.get(index) else {
            return 0;
        };
        self.markets
            .iter()
            .filter(|(slug, entry)| {
                promotable(entry, &shadow_verdict(entry, standby.shadows.get(*slug)))
            })
            .count()
    }

    /// Checks the tracked agreement sets against a full recomputation.
    ///
    /// Tracking is only as good as its coverage of the places the two compared sides move,
    /// and the failure of a missed one is silent: a coverage figure an operator trusts that
    /// a takeover would not honor. Every contract that reads a status or a scrape therefore
    /// checks the tracking on the way past, in the builds that carry debug assertions.
    fn check_agreement_tracking(&self) {
        #[cfg(debug_assertions)]
        for index in 0..self.standbys.len() {
            assert_eq!(
                self.agreeing_markets(index),
                self.agreeing_markets_recomputed(index),
                "standby {index}'s tracked agreement disagrees with the set it describes"
            );
        }
    }

    /// Re-reads whether standby `index` could carry `slug`, and records a change of answer.
    ///
    /// One comparison, of the one market whose state moved, against the one standby whose
    /// shadow moved. It is what replaces a per-request walk of the whole set: the work is
    /// done where the evidence changes, in proportion to the change.
    fn refresh_shadow_agreement(&mut self, index: usize, slug: &str) {
        let agreeing = match (self.markets.get(slug), self.standbys.get(index)) {
            (Some(entry), Some(standby)) => {
                promotable(entry, &shadow_verdict(entry, standby.shadows.get(slug)))
            }
            _ => false,
        };
        let Some(standby) = self.standbys.get_mut(index) else {
            return;
        };
        if agreeing {
            if !standby.agreeing.contains(slug) {
                let _new = standby.agreeing.insert(slug.to_owned());
            }
        } else {
            let _removed = standby.agreeing.remove(slug);
        }
    }

    /// The same question asked of every standby, for a market whose published book moved.
    ///
    /// O(standbys), which the configured replica ceiling bounds at three, and nothing at all
    /// on a shard running none.
    fn refresh_agreement(&mut self, slug: &str) {
        for index in 0..self.standbys.len() {
            self.refresh_shadow_agreement(index, slug);
        }
    }

    /// Records that no standby can carry `slug` any more.
    fn retire_agreement(&mut self, slug: &str) {
        for standby in &mut self.standbys {
            let _removed = standby.agreeing.remove(slug);
        }
    }

    /// Records that no standby can carry anything, which is where a whole-rail loss on the
    /// publishing connection leaves every market that held authority on it.
    fn retire_all_agreement(&mut self) {
        for standby in &mut self.standbys {
            standby.agreeing.clear();
        }
    }

    /// The standby best placed to take over, and how many markets it would carry.
    ///
    /// The same role [`Self::first_viable_standby`] would promote, so the figure an operator
    /// watches is the figure a loss right now would produce, not an optimistic maximum over
    /// roles that would not be chosen.
    /// What this shard's redundant connections are doing about publishing, or `None` for a
    /// shard running the default single connection.
    ///
    /// O(replicas), which the ladder ceiling bounds at four, so a status answer and a scrape
    /// both pay a constant for it however many markets the shard holds.
    fn pool_report(&self) -> Option<PoolReport> {
        let redundancy = self.redundancy.as_ref()?;
        Some(PoolReport {
            sockets: self.config.replicas,
            covering: self
                .slots()
                .filter(|slot| {
                    self.slot(*slot)
                        .and_then(|state| state.active.as_ref())
                        .is_some_and(|active| active.established)
                })
                .count(),
            state: redundancy.state(),
        })
    }

    fn promotion_coverage(&self) -> usize {
        self.first_viable_standby()
            .map_or(0, |index| self.agreeing_markets(index))
    }

    fn metrics(&self) -> ShardMetrics {
        self.check_agreement_tracking();
        ShardMetrics {
            connected: self
                .primary
                .active
                .as_ref()
                .is_some_and(|active| active.established),
            reconciling: !matches!(self.reissue, Reissue::Idle),
            replicas: self.config.replicas,
            standbys_established: self
                .standbys
                .iter()
                .filter(|standby| {
                    standby
                        .slot
                        .active
                        .as_ref()
                        .is_some_and(|active| active.established)
                })
                .count(),
            standby_agreeing_markets: self.promotion_coverage(),
            pool: self.pool_report(),
            desired: self.desired_count,
            markets: self.markets.len(),
            segment_markets: self.segment.as_ref().map_or(0, ShardSegment::installed),
            stats: ShardStats {
                frames_seen: self.frames.load(Ordering::Relaxed),
                queue_age: self.queue_age.summary(),
                publish_latency: self.publish_latency(),
                ..self.stats.clone()
            },
        }
    }

    /// What this shard's publications have cost, or an unmeasured summary for a shard
    /// publishing into no segment.
    fn publish_latency(&self) -> PublishLatencySummary {
        self.segment.as_ref().map_or(
            PublishLatencySummary::default(),
            ShardSegment::publish_latency,
        )
    }

    /// The desired set as the venue is asked for it: every wanted market, in slug order so
    /// one emit is byte-comparable with another.
    ///
    /// O(markets) plus a sort, and deliberately off the update path: it is built when the
    /// set changes or a connection is established, never per frame.
    fn desired_slugs(&self) -> Vec<String> {
        let mut slugs: Vec<String> = self
            .markets
            .iter()
            .filter(|(_, entry)| entry.desired)
            .map(|(slug, _)| slug.clone())
            .collect();
        slugs.sort();
        slugs
    }

    /// Starts a connection when the shard wants one, none is running, its backoff has
    /// elapsed, the shared command permit is free, and the configured daily attempt budget
    /// has room.
    ///
    /// The establishing subscription carries the whole desired set, and the emit in flight
    /// is recorded here. The command itself is paced where it is written — the connection
    /// task waits at the configured command floor immediately before the bytes go
    /// out — so nothing here has to reason about what other shards are emitting. A spawn the
    /// daily attempt ledger holds back is rescheduled, never abandoned, and never counted as
    /// an attempt.
    ///
    /// An empty desired set wants no connection at all. A subscription command naming no
    /// market is not a request this venue's behavior is known for — `docs/limitless.md`
    /// records replace-set semantics and says nothing about the empty set — so the shard
    /// holds no socket rather than emitting one, and dials again when a market is added.
    /// Dials whatever connection role is empty and due, publishing role first.
    ///
    /// Generations come from one counter shared by every role, so no two live connections of
    /// one shard ever share a generation and a notice identifies its role by that number
    /// alone. Every role dials the same desired set: a standby is redundancy for this
    /// shard's whole set, never a partition of it.
    ///
    /// Only the publishing role's dial marks markets as dialled and records a dialled set
    /// version against them — [`MarketEntry::dialled`] and [`MarketEntry::subscribed_from`]
    /// gate what may reach a *published* book, and a standby's dial must never widen that
    /// gate.
    fn spawn_due(&mut self) {
        if self.desired_count == 0 {
            return;
        }
        for slot in self.slots().collect::<Vec<_>>() {
            self.spawn_slot(slot);
        }
    }

    fn spawn_slot(&mut self, slot: Slot) {
        let Some(state) = self.slot(slot) else {
            return;
        };
        if state.active.is_some() || state.reconnect_at.is_some_and(|at| at > Instant::now()) {
            return;
        }
        let now = Instant::now();
        if let Err(free_at) = admit_attempt(now, self.config.daily_attempt_budget) {
            self.stats.attempts_clamped = self.stats.attempts_clamped.saturating_add(1);
            if let Some(state) = self.slot_mut(slot) {
                state.reconnect_at = Some(free_at);
            }
            return;
        }
        let generation = self.next_generation;
        self.next_generation = self.next_generation.saturating_add(1);
        self.stats.connection_attempts = self.stats.connection_attempts.saturating_add(1);
        self.stats.subscriptions_emitted = self.stats.subscriptions_emitted.saturating_add(1);
        let markets = self.desired_slugs();
        if matches!(slot, Slot::Primary) {
            for entry in self.markets.values_mut() {
                entry.dialled = entry.desired;
            }
        }
        let config = ConnectionConfig {
            endpoint: self.config.endpoint.clone(),
            markets: markets.clone(),
            setup_timeout: self.config.setup_timeout,
            capture_path: None,
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
        let desired_version = self.desired_version;
        if let Some(state) = self.slot_mut(slot) {
            state.dialled_version = Some(desired_version);
            state.active = Some(ActiveConnection {
                generation,
                markets,
                dialled_set_version: desired_version,
                spawned_at: Instant::now(),
                handle,
                control,
                heartbeat_deadline: None,
                last_heartbeat: None,
                established: false,
                produced_base: false,
                session: None,
            });
            state.reconnect_at = None;
        }
    }

    /// Gives up the connection when nothing is subscribed to it any more.
    ///
    /// The venue's subscription set is replaced whole and never emptied, so a shard whose
    /// demand has gone to zero cannot express that as a command: it closes the socket
    /// instead, which is what tells the venue the set is gone. The task is ended at once
    /// rather than fenced and drained, because there is nothing left for it to drain into:
    /// every book this connection fed belongs to a market nobody wants, and a drain window
    /// would only hold a socket open for a set with no subscriber.
    ///
    /// Every remaining book belongs to a market that is no longer wanted, and closing the
    /// connection is what reconciles its removal, so each publishes
    /// [`AuthorityState::Unsubscribed`] as its last revision and is then dropped. Nothing is
    /// reported as a loss: a market nobody asked for has no authority to lose. A fenced
    /// generation still draining is released too — a shard holding no demand holds no
    /// socket, draining or otherwise — and the reconnect ladder resets, because the next
    /// connection is a fresh start rather than a retry of this one.
    fn shed_idle_connection(&mut self) {
        if self.desired_count > 0 {
            return;
        }
        let mut had_connection = false;
        for slot in self.slots().collect::<Vec<_>>() {
            let shed = self.slot_mut(slot).and_then(|state| state.active.take());
            had_connection |= shed.is_some();
            if let Some(active) = shed {
                active.handle.abort();
            }
            self.release_fenced(slot);
            if let Some(state) = self.slot_mut(slot) {
                state.dialled_version = None;
                state.reconnect_at = None;
                state.backoff_attempt = 0;
            }
        }
        for standby in &mut self.standbys {
            standby.shadows.clear();
            standby.agreeing.clear();
        }
        self.reissue = Reissue::Idle;
        self.rail_loss_latched = false;
        let closing: Vec<String> = self
            .markets
            .iter()
            .filter(|(_, entry)| !entry.desired)
            .map(|(slug, _)| slug.clone())
            .collect();
        for slug in closing {
            let _closed = self.close(slug.as_str());
        }
        self.recount_recovering();
        if had_connection {
            self.stats.connections_shed = self.stats.connections_shed.saturating_add(1);
        }
    }

    /// Arms, holds, or discards the one same-set reissue.
    ///
    /// A reissue is owed when at least one desired market is owed a fresh base — never
    /// because the set changed, which [`Self::reconcile_set`] answers with a fresh
    /// connection instead. Nothing arms while no established connection exists, because a
    /// new connection carries the whole set in its own establishing subscription.
    ///
    /// The configured command floor is enforced where the command is written, inside
    /// the connection task, so a reissue is armed as soon as it is owed and the wire spaces
    /// it against every other command this process sends the venue.
    fn arm_reissue(&mut self) {
        let owed = self.recovering > 0;
        let established = self
            .primary
            .active
            .as_ref()
            .is_some_and(|active| active.established)
            && self.primary.dialled_version.is_none();
        match self.reissue {
            Reissue::Idle if owed && established => {
                self.reissue = Reissue::Pending(Instant::now());
            }
            Reissue::Pending(_) if !owed || !established => self.reissue = Reissue::Idle,
            Reissue::Awaiting(_) if self.recovering == 0 => self.reissue = Reissue::Idle,
            _ => {}
        }
    }

    /// Acts on a due reissue, against everything already queued.
    ///
    /// The reissue timer is polled ahead of the ingest queue, so the bases that would end a
    /// recovery may still be waiting in it. This applies what the queue already holds —
    /// bounded by its capacity, which is all it can hold — and re-decides before acting, so
    /// a venue that has already answered is neither commanded nor replaced.
    fn on_reissue_due(&mut self) {
        self.drain_queued();
        self.arm_reissue();
        let now = Instant::now();
        let due = self.reissue.wake_at().is_some_and(|at| at <= now);
        if !due {
            return;
        }
        match self.reissue {
            Reissue::Pending(_) => self.emit_reissue(),
            Reissue::Requested(_) => self.escalate_reissue(),
            Reissue::Awaiting(_) => {
                if self.recovering > 0 {
                    self.escalate_reissue();
                } else {
                    self.reissue = Reissue::Idle;
                }
            }
            Reissue::Idle | Reissue::Unreachable => {}
        }
    }

    fn drain_queued(&mut self) {
        for _ in 0..self.config.ingest_capacity {
            match self.notices_rx.try_recv() {
                Ok(notice) => self.on_notice(notice),
                Err(_) => break,
            }
        }
    }

    /// Asks the connection to put its own set back on the wire.
    ///
    /// The set is the one the connection was dialled with, unchanged: this is a recovery
    /// command, and the venue is being asked for a fresh base rather than for a different
    /// subscription. The command is offered, never awaited, because the connection owns the
    /// write half and a shard that blocked on it would stall every other market's ingestion.
    /// A handover that does not land is not a command — nothing has been asked of the venue,
    /// so no window starts — and what happens next is decided by *why* it did not land,
    /// because the two answers are not the same failure.
    ///
    /// A closed channel is a connection whose task is ending: its receiver lives for exactly
    /// as long as the task does. Nothing is scheduled — the reissue becomes
    /// [`Reissue::Unreachable`], which carries no instant — because the connection's own end
    /// is already a wake this loop is waiting on, and it runs the reconnect ladder. A timer
    /// here would be this shard polling for news the join is about to deliver.
    ///
    /// A full channel is a connection that is alive but has not consumed a command already
    /// queued for it. It cannot arise from this state machine, which emits from
    /// [`Reissue::Pending`] alone and leaves it on a landed handover, and whose only route
    /// back to `Pending` — [`Self::escalate_reissue`] — takes the connection and its channel
    /// with it; but if it ever did, the honest reading is a connection that cannot take
    /// work. The reissue is given the same absence deadline a landed handover gets, so a
    /// connection that never drains is replaced by the escalation path already built for one
    /// that never writes. No new timer either way.
    ///
    /// The command takes its place in this endpoint's process-wide command queue here, at
    /// the moment it is decided on, and carries the granted instant to the connection. The
    /// window this opens runs from that instant, because it is the earliest the bytes may
    /// leave: a fleet whose shards all reissue at once forms one queue, and a deadline that
    /// allowed one interval regardless of position would expire on every shard standing
    /// further back than second while its command was still waiting for this daemon's own
    /// pacing.
    fn emit_reissue(&mut self) {
        let now = Instant::now();
        let window = self.config.resubscribe_window;
        let interval = Duration::from_millis(self.config.min_command_interval_ms);
        let endpoint = self.config.endpoint.as_str();
        let Some(active) = self.primary.active.as_mut() else {
            self.reissue = Reissue::Idle;
            return;
        };
        let granted_at = reserve_command_grant(endpoint, now, interval);
        match active
            .control
            .try_send(ConnectionControl::Resubscribe { granted_at })
        {
            Ok(()) => {
                self.reissue = Reissue::Requested(granted_at + window);
                self.stats.subscriptions_emitted =
                    self.stats.subscriptions_emitted.saturating_add(1);
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.reissue = Reissue::Requested(granted_at + window);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => self.reissue = Reissue::Unreachable,
        }
    }

    /// Gives up on reconciling through the connection already serving the set and replaces
    /// it, which is the reconnect half of resubscribe-then-reconnect.
    ///
    /// The generation is fenced first, so nothing it produces after this instant can reach
    /// any book, and is then closed through the ordinary end path holding no base.
    fn escalate_reissue(&mut self) {
        let Some(active) = self.primary.active.take() else {
            self.reissue = Reissue::Idle;
            return;
        };
        let lifetime = Instant::now().saturating_duration_since(active.spawned_at);
        let established = active.established;
        self.stats.reissues_escalated = self.stats.reissues_escalated.saturating_add(1);
        self.fence(Slot::Primary, active);
        self.on_slot_ended(
            Slot::Primary,
            ConnectionEndReason::ResubscribeTimedOut,
            false,
            established,
            lifetime,
        );
    }

    /// Accepts a notice only from a generation this shard currently runs, and routes it to
    /// the role running it. Anything else is late work from a closed generation, discarded
    /// before it reaches any book or any shadow.
    ///
    /// This is the whole of the fence: one counter issues every role's generations, so a
    /// generation this shard no longer holds resolves to no role and its work is counted
    /// and dropped, whichever role produced it.
    fn on_notice(&mut self, notice: ConnectionNotice) {
        let Some(slot) = self.slot_of(notice.generation) else {
            if matches!(notice.note, ConnectionNote::Event { .. }) {
                self.stats.fenced_events = self.stats.fenced_events.saturating_add(1);
            }
            return;
        };
        self.stats.queue_depth_max = self.stats.queue_depth_max.max(self.notices_rx.len());
        match notice.note {
            ConnectionNote::Ready {
                open,
                subscription_generation,
                observed_at,
            } => {
                let _ = subscription_generation;
                let window = self.config.resubscribe_window;
                let session = open.sid().to_owned();
                let deadline = open.heartbeat_deadline();
                if let Some(active) = self.active_mut(slot) {
                    active.heartbeat_deadline = Some(deadline);
                    active.last_heartbeat = Some(observed_at);
                    active.established = true;
                    active.session = Some(session);
                }
                self.settle_dial(slot);
                if matches!(slot, Slot::Primary) {
                    self.reissue = if self.recovering > 0 {
                        Reissue::Awaiting(Instant::now() + window)
                    } else {
                        Reissue::Idle
                    };
                }
            }
            ConnectionNote::Heartbeat { observed_at } => {
                if let Some(active) = self.active_mut(slot) {
                    active.last_heartbeat = Some(observed_at);
                }
            }
            ConnectionNote::Resubscribed { .. } => {
                if matches!(slot, Slot::Primary) && matches!(self.reissue, Reissue::Requested(_)) {
                    let window = self.config.resubscribe_window;
                    self.reissue = if self.recovering > 0 {
                        Reissue::Awaiting(Instant::now() + window)
                    } else {
                        Reissue::Idle
                    };
                }
            }
            ConnectionNote::Event {
                event,
                received_at,
                arrival_time_nanos,
                subscription_generation,
            } => self.on_event(
                slot,
                event,
                received_at,
                arrival_time_nanos,
                subscription_generation,
            ),
            ConnectionNote::DecodeFailure { key, book_relevant } => {
                self.stats.record_failure(key);
                if book_relevant {
                    self.report_rail_loss(
                        slot,
                        ContinuityReason::LocalLoss,
                        AuthorityReason::LocalLoss,
                    );
                }
            }
            ConnectionNote::Overload { dropped } => {
                self.stats.overload_drops = self.stats.overload_drops.saturating_add(dropped);
                self.report_rail_loss(slot, ContinuityReason::LocalLoss, AuthorityReason::Overload);
            }
        }
    }

    fn active_mut(&mut self, slot: Slot) -> Option<&mut ActiveConnection> {
        self.slot_mut(slot).and_then(|state| state.active.as_mut())
    }

    /// Reports a whole-rail failure against the role that suffered it.
    ///
    /// The publishing role's rail carries the books, so its loss is the books' loss. A
    /// standby's rail carries only shadows, so its loss costs that standby every shadow it
    /// held and no book anything: the frames that went missing were never going to be
    /// published, and `docs/design.md` is explicit that replica divergence degrades
    /// redundancy without staling a still-authoritative primary.
    ///
    /// While a pool is armed every socket's rail carries the books, so a book-relevant
    /// decode failure or an overflowed notice queue on any of them is an authoritative
    /// continuity loss rather than a shadow one. This venue's counter is non-contiguous per
    /// market, so a frame this daemon received and lost on one socket may carry a state no
    /// other socket's stream contains, and publishing straight past it would present a hole
    /// as an intact history. A standby slot also loses its shadow, because it is equally
    /// true that its own stream broke.
    fn report_rail_loss(
        &mut self,
        slot: Slot,
        continuity: ContinuityReason,
        authority: AuthorityReason,
    ) {
        if self.pool_armed() {
            self.report_shard_loss(continuity, authority);
            if let Some(standby) = slot_index(slot).and_then(|index| self.standbys.get_mut(index)) {
                standby.shadows.clear();
                standby.agreeing.clear();
            }
            return;
        }
        match slot {
            Slot::Primary => self.report_shard_loss(continuity, authority),
            Slot::Standby(index) => {
                if let Some(standby) = self.standbys.get_mut(index) {
                    standby.shadows.clear();
                    standby.agreeing.clear();
                }
            }
        }
    }

    /// Records that the connection now running has its subscription on the wire.
    ///
    /// Every market that connection was dialled with takes its generation as its
    /// subscription evidence, which is the whole of what this venue offers: it acknowledges
    /// no individual market, and it publishes no answer that can be correlated with the
    /// command that provoked it, so the only thing that can attribute a frame to a set is
    /// the connection it was read on. A market added since the dial is deliberately left
    /// without evidence — the connection was never given it, and the replacement carrying it
    /// is a different generation.
    ///
    /// O(markets in the dialled set), once per connection, and only for the publishing
    /// role: a standby's dial changes nothing about which frames may reach a published
    /// book. [`MarketEntry::subscribed_from`] is that gate, so writing it from a standby's
    /// dial would make the standby's frames eligible for the books and end one-writer-per-
    /// book. A standby's own attribution is its connection's dialled set, which its shadows
    /// are keyed against instead.
    fn settle_dial(&mut self, slot: Slot) {
        let Some(version) = self
            .slot_mut(slot)
            .and_then(|state| state.dialled_version.take())
        else {
            return;
        };
        let Some((generation, dialled_set_version, carried)) = self
            .slot(slot)
            .and_then(|state| state.active.as_ref())
            .map(|active| {
                (
                    active.generation,
                    active.dialled_set_version,
                    active.markets.clone(),
                )
            })
        else {
            return;
        };
        if matches!(slot, Slot::Primary) {
            for slug in carried {
                if let Some(entry) = self.markets.get_mut(slug.as_str())
                    && entry.claimed_by(dialled_set_version)
                {
                    entry.dialled = false;
                    entry.subscribed_from = Some(generation);
                    if entry.desired {
                        entry.subscription = SubscriptionState::Subscribing;
                    }
                }
            }
        }
        if let Some(state) = self.slot_mut(slot) {
            state.wire_version = version;
        }
    }

    /// Replaces the connection when the desired set is no longer the set the running
    /// connection carries.
    ///
    /// This venue publishes no boundary a replacement command could be correlated with
    /// (`docs/limitless.md`), so `docs/design.md` "Subscription control" leaves one honest
    /// answer: carry the new set on a fresh connection generation. The running connection is
    /// fenced, which makes every frame it still produces ineligible, and the replacement is
    /// dialled at once with the new set. Nothing is inferred about which set a frame belonged
    /// to, because no frame crosses a generation.
    ///
    /// The cheap check is the version counter; the set comparison runs only when the counter
    /// says something changed, and a change that nets out to the same set costs nothing but
    /// that comparison — unless one of the markets it names is a new incarnation behind an
    /// old slug, which is a set the running connection cannot serve however its text reads.
    ///
    /// The replacement is not a retry: the backoff ladder resets and the dial is due
    /// immediately, because the connection it replaces was not failing. The configured
    /// pacing and budget are still honored — the dial spends an attempt from the
    /// process-wide ledger, and its establishing subscription waits at the wire pacer like
    /// any other.
    /// Every role is reconciled, because every role subscribes the whole desired set: a
    /// standby still carrying the old set is redundancy for a set nobody wants any more.
    /// Each is replaced on its own schedule, and only the publishing role's replacement
    /// reports anything to a book.
    fn reconcile_set(&mut self) {
        for slot in self.slots().collect::<Vec<_>>() {
            self.reconcile_slot(slot);
        }
    }

    fn reconcile_slot(&mut self, slot: Slot) {
        let Some(state) = self.slot(slot) else {
            return;
        };
        if self.desired_version == state.wire_version || state.dialled_version.is_some() {
            return;
        }
        let unchanged = state.active.as_ref().is_some_and(|active| {
            active.established
                && self.desired_slugs() == active.markets
                && self.dial_carries_current_entries(active.dialled_set_version)
        });
        if unchanged {
            let version = self.desired_version;
            if let Some(state) = self.slot_mut(slot) {
                state.wire_version = version;
            }
            return;
        }
        let replaceable = state
            .active
            .as_ref()
            .is_some_and(|active| active.established);
        if !replaceable {
            return;
        }
        let Some(active) = self.slot_mut(slot).and_then(|state| state.active.take()) else {
            return;
        };
        let lifetime = Instant::now().saturating_duration_since(active.spawned_at);
        self.stats.set_replacements = self.stats.set_replacements.saturating_add(1);
        self.fence(slot, active);
        if self.pool_armed() {
            self.on_pool_slot_ended(
                slot,
                ConnectionEndReason::SubscriptionReplaced,
                lifetime,
                true,
                false,
            );
        } else {
            match slot {
                Slot::Primary => self.on_connection_ended(
                    ConnectionEndReason::SubscriptionReplaced,
                    false,
                    lifetime,
                ),
                Slot::Standby(index) => self.on_standby_ended(
                    index,
                    ConnectionEndReason::SubscriptionReplaced,
                    lifetime,
                ),
            }
        }
        if let Some(state) = self.slot_mut(slot) {
            state.backoff_attempt = 0;
            state.reconnect_at = None;
        }
    }

    /// A standby's arrivals are counted exactly as the publishing role's are and then take
    /// the shadow path: a book update keeps that standby's own shadow of the market
    /// current, and a resolution reaches no consumer lane.
    ///
    /// Forwarding a standby's resolution would deliver the venue's one report twice from
    /// one shard, which is the same double-count a second book would be. What the venue
    /// reported is already on the market's lanes from the publishing connection, and a
    /// resolution changes no level, so a shadow has nothing to keep about it.
    fn on_event(
        &mut self,
        slot: Slot,
        event: LimitlessEvent,
        received_at: Instant,
        arrival_time_nanos: u64,
        subscription: u64,
    ) {
        self.queue_age
            .record(micros_between(received_at, Instant::now()));
        let arrival = FrameArrival::new(received_at, arrival_time_nanos);
        match &event {
            LimitlessEvent::OrderbookUpdate(update) => {
                self.stats.events_orderbook = self.stats.events_orderbook.saturating_add(1);
                if self.pool_armed() {
                    self.apply_pooled(slot, update, received_at, arrival, subscription);
                    return;
                }
                match slot {
                    Slot::Primary => self.apply_update(update, received_at, arrival, subscription),
                    Slot::Standby(index) => {
                        self.apply_shadow_update(index, update, received_at, subscription);
                    }
                }
            }
            LimitlessEvent::MarketResolved(resolved) => {
                self.stats.events_resolved = self.stats.events_resolved.saturating_add(1);
                if matches!(slot, Slot::Primary) {
                    self.forward_resolution(resolved, received_at, arrival, subscription);
                }
            }
            LimitlessEvent::Unknown { .. } => {
                self.stats.events_unknown = self.stats.events_unknown.saturating_add(1);
            }
        }
    }

    /// Applies one venue book snapshot to a standby's shadow of the market it names.
    ///
    /// Nothing here can reach a published book, a delivery segment, an observer, or the
    /// shard's own publication position: the shadow is a second copy this task owns for one
    /// purpose, which is to answer whether this standby could carry that market's authority
    /// across a promotion.
    ///
    /// The same attribution rule the published rail uses applies: a frame is taken only for
    /// a market this standby's own connection was dialled with, so a market added since its
    /// dial is shadowed by that standby's replacement rather than by it. A shadow is created
    /// by the first arrival that populates it, so a standby costs memory for the markets the
    /// venue actually serves it.
    ///
    /// A rejected candidate or apply costs that one shadow its comparable history and
    /// nothing else — the standby simply stops being eligible for that market until its
    /// stream is rebased, which is [`crate::DivergenceReason::ContinuityMismatch`] and is
    /// feed-health evidence about the standby, never about the book.
    fn apply_shadow_update(
        &mut self,
        index: usize,
        update: &OrderbookUpdate,
        received_at: Instant,
        subscription: u64,
    ) {
        let slug = update.market_slug();
        let Some(entry) = self.markets.get(slug) else {
            self.stats.frames_unrouted = self.stats.frames_unrouted.saturating_add(1);
            return;
        };
        let market = entry.market.clone();
        let carried = self
            .standbys
            .get(index)
            .and_then(|standby| standby.slot.active.as_ref())
            .is_some_and(|active| active.markets.binary_search(&slug.to_owned()).is_ok());
        if !carried {
            self.stats.frames_uncarried = self.stats.frames_uncarried.saturating_add(1);
            return;
        }
        let Some(position) = self.shadow_position.checked_add(1) else {
            self.stats.record_failure("shadow:PositionCounterOverflow");
            self.drop_shadow(index, slug);
            return;
        };
        self.shadow_position = position;
        let provenance = match self.build_provenance(
            market.clone(),
            update,
            position,
            received_at,
            subscription,
            Slot::Standby(index),
        ) {
            Ok(provenance) => provenance,
            Err(_) => {
                self.stats.record_failure("shadow:InvalidProvenance");
                self.drop_shadow(index, slug);
                return;
            }
        };
        let candidate = match update.snapshot_candidate(provenance, self.level_capacity) {
            Ok(candidate) => candidate,
            Err(_) => {
                self.stats.record_failure("shadow:InvalidCandidate");
                self.drop_shadow(index, slug);
                return;
            }
        };
        self.apply_shadow_candidate(index, slug, market, &candidate);
    }

    /// Applies one already-normalized candidate to standby `index`'s shadow of `slug`.
    ///
    /// The shadow half of both arrival paths: the hot-standby one, whose candidate is built
    /// for the shadow alone, and the pooled one, whose candidate is the very frame the gate
    /// judged, so a socket's shadow holds exactly what that socket delivered whether the
    /// gate published it, deduplicated it, or dropped it as skew.
    fn apply_shadow_candidate(
        &mut self,
        index: usize,
        slug: &str,
        market: MarketRef,
        candidate: &Candidate,
    ) {
        let Some(standby) = self.standbys.get_mut(index) else {
            return;
        };
        let shadow = standby
            .shadows
            .entry(slug.to_owned())
            .or_insert_with(|| OrderBook::new(market));
        match shadow.apply_snapshot(candidate) {
            Ok(_commit) => {
                self.stats.shadow_snapshots_applied =
                    self.stats.shadow_snapshots_applied.saturating_add(1);
            }
            Err(error) => {
                let key = book_error_key(&error);
                self.stats.record_failure(key);
                let _ = standby.shadows.remove(slug);
            }
        }
        self.refresh_shadow_agreement(index, slug);
    }

    /// Gives up one standby's shadow of one market, which is how a shadow says it holds no
    /// comparable history any more.
    fn drop_shadow(&mut self, index: usize, slug: &str) {
        if let Some(standby) = self.standbys.get_mut(index) {
            let _ = standby.shadows.remove(slug);
            let _ = standby.agreeing.remove(slug);
        }
    }

    /// Whether this shard is publishing across its sockets by venue key right now.
    fn pool_armed(&self) -> bool {
        self.redundancy.as_ref().is_some_and(Redundancy::armed)
    }

    /// The declaration and socket count an armed pool's gates are built from.
    fn pool_licence(&self) -> Option<(DedupKeyDeclaration, usize)> {
        match self.redundancy.as_ref() {
            Some(Redundancy::Pooled {
                declaration,
                sockets,
                degraded: None,
            }) => Some((*declaration, *sockets)),
            _ => None,
        }
    }

    /// Whether a dial taken at `dialled_set_version` was given every desired market as the
    /// incarnation that market is now.
    ///
    /// False exactly when a market has been retired and installed again behind the same slug
    /// since that dial: the connection's subscription then names a book this shard no longer
    /// holds, whatever its set text says, and the only honest answer is the fresh connection
    /// a set change always gets. Walked once per reconciliation and never on the update path.
    fn dial_carries_current_entries(&self, dialled_set_version: u64) -> bool {
        self.markets
            .values()
            .all(|entry| !entry.desired || entry.installed_version <= dialled_set_version)
    }

    /// Whether an arrival on `slot` may be published for `slug`.
    ///
    /// Per socket, and deliberately not the market's own [`MarketEntry::subscribed_from`]:
    /// under a pool a market's subscription evidence is held by every socket carrying it,
    /// and there is no one generation that owns it. Each socket answers for its own
    /// arrivals exactly as a standby's shadow always has — the connection was dialled with
    /// this market, and its subscription is on the wire — so a market added since a
    /// socket's dial is published by the socket that was given it and not by that one.
    ///
    /// The dial is read against the market's own incarnation and not only against the slugs
    /// it names. A market removed and added back is a new book behind the same slug, and a
    /// connection dialled before it came back is still subscribed to the one that was
    /// retired: its frames belong to a set this shard has abandoned, and a book that took
    /// one as its base would be rebased onto a frame from a subscription nobody holds.
    fn pool_carries(&self, slot: Slot, slug: &str) -> bool {
        let Some(entry) = self.markets.get(slug) else {
            return false;
        };
        self.slot(slot)
            .and_then(|state| state.active.as_ref())
            .is_some_and(|active| {
                active.established
                    && entry.claimed_by(active.dialled_set_version)
                    && active
                        .markets
                        .binary_search_by(|carried| carried.as_str().cmp(slug))
                        .is_ok()
            })
    }

    /// Every dialled set other than `excluding`'s that a socket can still deliver, which is
    /// what a pooled book's authority survives one socket's loss on.
    ///
    /// Viability is read here and not only that a session was established, because both
    /// pooled tasks can finish before the run loop joins either: a connection that has
    /// already returned answers with a subscription nothing will arrive on, and a book left
    /// live behind it would be claiming authority with no source able to renew it. This is
    /// the same predicate, read at the same instant, that [`Self::take_publishing_slot`] and
    /// [`Self::switch_source`] refuse a dead connection with.
    ///
    /// Cloned once per socket loss and never on the update path, exactly as
    /// [`Self::apply_promotions`]'s carried set is.
    fn pool_survivor_sets(&self, excluding: Slot) -> Vec<Vec<String>> {
        let now = Instant::now();
        self.slots()
            .filter(|slot| *slot != excluding)
            .filter_map(|slot| {
                self.slot(slot)
                    .and_then(|state| state.active.as_ref())
                    .filter(|active| active.established && active.is_viable(now))
                    .map(|active| active.markets.clone())
            })
            .collect()
    }

    /// Judges one venue book snapshot against this market's pooled publish gate, and does
    /// what the verdict says.
    ///
    /// The gate reads the venue's key and the exact content the arrival reported, and every
    /// arrival is stamped with the publishing role before it is judged: in an armed pool
    /// every socket is an authoritative source, and which of them a frame came in on is
    /// provenance rather than permission.
    ///
    /// The arrival that withdraws the licence reaches no book. Two of the ways that happens
    /// are the venue contradicting the basis the pool publishes on, and the frame carrying
    /// that contradiction is the last one to trust; the third is a frame whose key the gate
    /// cannot read, which the topology being handed back to has no established rule for
    /// either. One frame is lost at that instant, and every later arrival is applied under
    /// the topology the withdrawal installed.
    fn apply_pooled(
        &mut self,
        slot: Slot,
        update: &OrderbookUpdate,
        received_at: Instant,
        arrival: FrameArrival,
        subscription: u64,
    ) {
        let Some((declaration, sockets)) = self.pool_licence() else {
            return;
        };
        let slug = update.market_slug();
        let Some(entry) = self.markets.get(slug) else {
            self.stats.frames_unrouted = self.stats.frames_unrouted.saturating_add(1);
            return;
        };
        let market = entry.market.clone();
        if !self.pool_carries(slot, slug) {
            self.stats.frames_uncarried = self.stats.frames_uncarried.saturating_add(1);
            return;
        }
        let Some(position) = self.position.checked_add(1) else {
            self.stats.record_failure("book:PositionCounterOverflow");
            self.report_market_loss(
                slug,
                ContinuityReason::LocalLoss,
                AuthorityReason::LocalLoss,
            );
            return;
        };
        self.position = position;
        let provenance = match self.build_provenance(
            market.clone(),
            update,
            position,
            received_at,
            subscription,
            slot,
        ) {
            Ok(provenance) => provenance,
            Err(_) => {
                self.stats.record_failure("book:InvalidProvenance");
                self.report_market_loss(
                    slug,
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
                self.report_market_loss(
                    slug,
                    ContinuityReason::LocalLoss,
                    AuthorityReason::LocalLoss,
                );
                return;
            }
        };
        let slug = slug.to_owned();
        if self
            .markets
            .get(slug.as_str())
            .is_some_and(|entry| entry.gate.is_none())
        {
            match self.new_pool_gate(declaration, sockets) {
                Some(gate) => {
                    if let Some(entry) = self.markets.get_mut(slug.as_str()) {
                        entry.gate = Some(gate);
                    }
                }
                None => {
                    self.withdraw_licence(PoolDegradeReason::KeyUnavailable);
                    return;
                }
            }
        }
        let key = update.dedup_key().ok().flatten();
        let projection = candidate_projection(&candidate);
        let Some((verdict, redelivered, recovering)) =
            self.judge_pooled(slug.as_str(), slot, key.as_ref(), projection)
        else {
            return;
        };
        match verdict {
            PoolVerdict::Publish => {
                self.publish_pooled(slot, slug.as_str(), market, &candidate, key, arrival);
            }
            PoolVerdict::Duplicate if redelivered && recovering => {
                self.recover_pooled(slot, slug.as_str(), market, &candidate, arrival);
            }
            PoolVerdict::Duplicate => {
                self.stats.pool_duplicate_drops = self.stats.pool_duplicate_drops.saturating_add(1);
                self.note_pool_position(slug.as_str(), slot.index(), true);
                self.shadow_pooled(slot, slug.as_str(), market, &candidate);
            }
            PoolVerdict::Stale => {
                self.stats.pool_stale_drops = self.stats.pool_stale_drops.saturating_add(1);
                self.note_pool_position(slug.as_str(), slot.index(), false);
                self.shadow_pooled(slot, slug.as_str(), market, &candidate);
            }
            PoolVerdict::Degrade(violation) => self.withdraw_licence(violation.reason),
            PoolVerdict::Disarmed => {}
        }
    }

    /// Puts one arrival to this market's gate, and reports the verdict alongside the two
    /// facts the caller had to establish before the gate took the projection: whether the
    /// window already held this exact content for this key, and whether the book is owed a
    /// recovery base.
    fn judge_pooled(
        &mut self,
        slug: &str,
        slot: Slot,
        key: Option<&DedupKey>,
        projection: CandidateProjection,
    ) -> Option<(PoolVerdict, bool, bool)> {
        let entry = self.markets.get_mut(slug)?;
        let recovering = !entry.live();
        let gate = entry.gate.as_mut()?;
        let redelivered = key.is_some_and(|key| gate.evidence_matches(key, &projection));
        let verdict = gate.admit(slot.index(), key, projection);
        Some((verdict, redelivered, recovering))
    }

    /// Commits an arrival this market's gate chose to publish.
    ///
    /// A socket that also holds a standby slot keeps its own shadow current first, so the
    /// role it falls back to on a withdrawal holds the state it delivered rather than a gap
    /// where its own publications were. The gate learns the arrival reached the book only
    /// after it did: an apply that failed leaves that key free to publish when it next
    /// arrives on another socket.
    fn publish_pooled(
        &mut self,
        slot: Slot,
        slug: &str,
        market: MarketRef,
        candidate: &Candidate,
        key: Option<DedupKey>,
        arrival: FrameArrival,
    ) {
        if let Slot::Standby(index) = slot {
            self.apply_shadow_candidate(index, slug, market, candidate);
        }
        let applied =
            self.mutate_market(slug, |entry| match entry.writer.apply_snapshot(candidate) {
                Ok(commit) => {
                    entry.base_accepted = true;
                    entry.based_this_generation = true;
                    entry.recovery_attempts = 0;
                    entry.unavailable_reported = false;
                    if entry.desired {
                        entry.subscription = SubscriptionState::Established;
                    }
                    Ok(commit)
                }
                Err(error) => Err(error),
            });
        match applied {
            Some(Ok(commit)) => {
                let mutations = u64::try_from(commit.mutations().len()).unwrap_or(u64::MAX);
                self.stats.snapshots_applied = self.stats.snapshots_applied.saturating_add(1);
                self.stats.mutations_derived =
                    self.stats.mutations_derived.saturating_add(mutations);
                self.rail_loss_latched = false;
                if let Some(active) = self.slot_mut(slot).and_then(|state| state.active.as_mut()) {
                    active.produced_base = true;
                }
                if let Some(key) = key
                    && let Some(gate) = self
                        .markets
                        .get_mut(slug)
                        .and_then(|entry| entry.gate.as_mut())
                {
                    gate.committed(slot.index(), key);
                }
                if let Some(entry) = self.markets.get_mut(slug) {
                    entry.sockets_at_published = socket_bit(slot.index());
                }
                self.credit_pool_publication(slot.index());
                self.publish_segment_commit(slug, &commit, arrival);
            }
            Some(Err(error)) => {
                let key = book_error_key(&error);
                self.stats.record_failure(key);
                self.report_market_loss(
                    slug,
                    ContinuityReason::LocalLoss,
                    AuthorityReason::LocalLoss,
                );
            }
            None => {}
        }
        self.refresh_agreement(slug);
    }

    /// Installs a redelivery of the published key as a stale book's recovery base.
    ///
    /// A pool that has lost a book's authority recovers the way any source does: from a
    /// complete authoritative snapshot the venue serves. This venue's observed answer to a
    /// resubscribe is the same frame again, carrying the key the book already holds, so
    /// treating that as nothing but a duplicate would leave a stale book waiting for a
    /// higher key that a quiet market need never produce. It is admitted only when the
    /// gate's window holds exact evidence that this key carried exactly this content, so
    /// what is installed is the state the book was already meant to hold; a redelivery whose
    /// content disagrees withdrew the licence before reaching here.
    ///
    /// It does not move the published key: nothing newer than it has been published, and
    /// later arrivals go on being judged against it.
    fn recover_pooled(
        &mut self,
        slot: Slot,
        slug: &str,
        market: MarketRef,
        candidate: &Candidate,
        arrival: FrameArrival,
    ) {
        if let Slot::Standby(index) = slot {
            self.apply_shadow_candidate(index, slug, market, candidate);
        }
        let applied =
            self.mutate_market(slug, |entry| match entry.writer.apply_snapshot(candidate) {
                Ok(commit) => {
                    entry.base_accepted = true;
                    entry.based_this_generation = true;
                    entry.recovery_attempts = 0;
                    entry.unavailable_reported = false;
                    if entry.desired {
                        entry.subscription = SubscriptionState::Established;
                    }
                    Ok(commit)
                }
                Err(error) => Err(error),
            });
        match applied {
            Some(Ok(commit)) => {
                let mutations = u64::try_from(commit.mutations().len()).unwrap_or(u64::MAX);
                self.stats.snapshots_applied = self.stats.snapshots_applied.saturating_add(1);
                self.stats.mutations_derived =
                    self.stats.mutations_derived.saturating_add(mutations);
                self.rail_loss_latched = false;
                if let Some(active) = self.slot_mut(slot).and_then(|state| state.active.as_mut()) {
                    active.produced_base = true;
                }
                if let Some(gate) = self
                    .markets
                    .get_mut(slug)
                    .and_then(|entry| entry.gate.as_mut())
                {
                    gate.recovered(slot.index());
                }
                self.note_pool_position(slug, slot.index(), true);
                self.credit_pool_publication(slot.index());
                self.publish_segment_commit(slug, &commit, arrival);
            }
            Some(Err(error)) => {
                let key = book_error_key(&error);
                self.stats.record_failure(key);
                self.report_market_loss(
                    slug,
                    ContinuityReason::LocalLoss,
                    AuthorityReason::LocalLoss,
                );
            }
            None => {}
        }
        self.refresh_agreement(slug);
    }

    /// Records an arrival the gate did not publish: a duplicate of a frame another socket
    /// already published, or state older than the book already holds.
    ///
    /// It still belongs in that socket's own shadow, which is what keeps a standby slot's
    /// comparison meaningful and what makes it usable the instant the licence is withdrawn.
    /// The publishing slot keeps no shadow, so for it there is nothing to record.
    fn shadow_pooled(&mut self, slot: Slot, slug: &str, market: MarketRef, candidate: &Candidate) {
        if let Slot::Standby(index) = slot {
            self.apply_shadow_candidate(index, slug, market, candidate);
        }
    }

    /// Credits one published arrival to the socket that carried it.
    ///
    /// Every arrival that reaches a book goes through here, a recovery base included, so the
    /// per-socket counts stay a partition of the total rather than two numbers that happen
    /// to be near each other.
    ///
    /// Shard-level rather than summed from the gates on demand: a scrape asks every shard on
    /// every collection, and an answer walking the market set would grow that cost with the
    /// set while the gates already know only their own market's share of it.
    fn credit_pool_publication(&mut self, socket: usize) {
        self.stats.pool_published = self.stats.pool_published.saturating_add(1);
        if self.stats.pool_published_by_socket.len() < self.config.replicas {
            self.stats
                .pool_published_by_socket
                .resize(self.config.replicas, 0);
        }
        if let Some(count) = self.stats.pool_published_by_socket.get_mut(socket) {
            *count = count.saturating_add(1);
        }
    }

    /// Records where `socket`'s session stands against one market's published key.
    ///
    /// `reached` is true for an arrival at or beyond it — the same frame arriving again, or a
    /// redelivery installed as a recovery base — and false for one the book had already
    /// passed. Every arrival the gate judged is recorded, because the hand-back's question is
    /// about where a socket stands and not about which socket published last.
    fn note_pool_position(&mut self, slug: &str, socket: usize, reached: bool) {
        let bit = socket_bit(socket);
        if let Some(entry) = self.markets.get_mut(slug) {
            if reached {
                entry.sockets_at_published |= bit;
            } else {
                entry.sockets_at_published &= !bit;
            }
        }
    }

    /// Records that the connection-session occupying one pool socket has ended, so the next
    /// one there is a replacement whose first key is judged against the published floor, and
    /// so nothing it delivered stands for the session that replaces it.
    ///
    /// The gates that exist are told directly; the shard keeps the same fact for the gates
    /// that do not exist yet.
    fn retire_pool_socket(&mut self, slot: Slot) {
        let socket = slot.index();
        let bit = socket_bit(socket);
        self.pool_sessions_ended |= bit;
        for entry in self.markets.values_mut() {
            entry.sockets_at_published &= !bit;
            if let Some(gate) = entry.gate.as_mut() {
                gate.retire_socket(socket);
            }
        }
    }

    /// Builds one market's gate holding what the shard already knows about its sockets.
    ///
    /// A gate created now has recorded none of the sessions that ended before it existed, so
    /// each of those positions is retired on it as it is built. That is the whole of what a
    /// gate which had lived through those endings would hold about them: a retired position
    /// has no session key of its own, and its next first key is a replacement's.
    fn new_pool_gate(&self, declaration: DedupKeyDeclaration, sockets: usize) -> Option<PoolGate> {
        let mut gate = PoolGate::new(declaration, sockets).ok()?;
        for socket in 0..sockets {
            if self.pool_sessions_ended & socket_bit(socket) != 0 {
                gate.retire_socket(socket);
            }
        }
        Some(gate)
    }

    /// Withdraws this shard's licence to publish across sockets, for the rest of the
    /// process, and hands every book back to the publishing slot.
    ///
    /// One observation withdraws it for the whole set. The conditions are facts about the
    /// venue's key rather than about one market — a socket's own stream inverting, one key
    /// naming two book states, a key the gate cannot order — so a pool that has seen one has
    /// no ground left to keep choosing arrivals by key for any market.
    ///
    /// The gates are dropped with the licence: they can never be consulted again, and their
    /// exact-content evidence is the only unbounded thing a pool holds. The licence never
    /// re-arms inside the process, because the evidence granting it was recorded before the
    /// run and a run that has just contradicted it cannot re-record it.
    fn withdraw_licence(&mut self, reason: PoolDegradeReason) {
        let Some(Redundancy::Pooled { degraded, .. }) = self.redundancy.as_mut() else {
            return;
        };
        if degraded.is_some() {
            return;
        }
        *degraded = Some(reason);
        self.stats.pool_degraded = Some(reason);
        for entry in self.markets.values_mut() {
            entry.gate = None;
        }
        self.hand_back_to_publishing_slot();
    }

    /// Hands every book back to the publishing slot as the licence is withdrawn.
    ///
    /// A book the publishing slot's own session has been seen to reach is untouched: the
    /// hand-back replaces no published state and opens no continuity epoch, because what was
    /// published was never in question — only the licence to keep choosing arrivals by key,
    /// and this venue's key orders that session's own stream, so its next frame cannot stand
    /// behind what the book holds.
    ///
    /// Every other book is told it lost its authority, and the two ways that happens stay
    /// distinct. A book the publishing slot does not carry — because that connection is
    /// between generations, or because the market belongs to an incarnation its dial predates
    /// — has lost the last source that could publish it, which is
    /// [`AuthorityReason::SubscriptionLost`]. A book it does carry but stands below is a
    /// source change with no venue ordering proof that its history is the newer one, which
    /// `docs/design.md` answers the same way it answers a promotion without such proof:
    /// [`AuthorityReason::OrderingUnknown`] until an authoritative recovery base installs
    /// atomically. Applying that connection's next frame instead would present a backward
    /// step as forward history, which no epoch and no revision would make visible.
    fn hand_back_to_publishing_slot(&mut self) {
        let running = self
            .primary
            .active
            .as_ref()
            .filter(|active| active.established)
            .map(|active| active.generation);
        let publishing = socket_bit(Slot::Primary.index());
        let mut losses = 0u64;
        let mut failure = None;
        let mut retired: Vec<String> = Vec::new();
        let Self {
            markets, segment, ..
        } = self;
        for (slug, entry) in markets.iter_mut() {
            let carried = running.is_some() && entry.subscribed_from == running;
            if carried && entry.sockets_at_published & publishing != 0 {
                continue;
            }
            if !entry.base_accepted || entry.terminal() {
                continue;
            }
            entry.lost_base();
            retired.push(slug.clone());
            let authority = if carried {
                AuthorityReason::OrderingUnknown
            } else {
                AuthorityReason::SubscriptionLost
            };
            match entry
                .writer
                .report_continuity_loss(ContinuityReason::Reconnect, authority)
            {
                Ok(true) => {
                    losses = losses.saturating_add(1);
                    if let Some(segment) = segment.as_mut() {
                        segment.publish_state(slug, &entry.writer.published(), None);
                    }
                }
                Ok(false) => {}
                Err(error) => failure = Some(book_error_key(&error)),
            }
        }
        self.stats.continuity_losses = self.stats.continuity_losses.saturating_add(losses);
        if let Some(key) = failure {
            self.stats.record_failure(key);
        }
        for slug in retired {
            self.retire_agreement(slug.as_str());
        }
        self.recount_recovering();
    }

    /// Closes one socket of an armed pool.
    ///
    /// A pool has no publishing primary to lose. Every socket feeds the same books through
    /// the same per-market gate, so one socket going away costs coverage and nothing else:
    /// the promotion question — which of two histories is authoritative — is not asked,
    /// because the key gate has already answered it for every arrival the books hold. A
    /// market another established socket still carries keeps its authority untouched, with
    /// no loss, no epoch and no revision spent; a market that has just lost its last source
    /// is reported exactly as the single-source path reports it, recovery attempt included,
    /// because for that market this *was* the single source.
    ///
    /// `handover` is false for a deliberate replacement. A subscription-set change retires
    /// every socket, so the surviving ones carry the set the change is abandoning and are
    /// themselves being replaced; moving one into the publishing slot would only make it
    /// the next connection to be replaced.
    ///
    /// Only a session that actually began is retired against this socket. A dial that ended
    /// before the venue answered it hosted none, so it delivered nothing, stands nowhere
    /// against any book, and makes none of the claims a replacement's first key is judged
    /// against.
    fn on_pool_slot_ended(
        &mut self,
        slot: Slot,
        reason: ConnectionEndReason,
        lifetime: Duration,
        established: bool,
        handover: bool,
    ) {
        if established {
            self.retire_pool_socket(slot);
        }
        if let Slot::Standby(index) = slot {
            if !matches!(reason, ConnectionEndReason::SubscriptionReplaced) {
                *self
                    .stats
                    .standby_ends
                    .entry(standby_end_key(reason))
                    .or_insert(0) += 1;
            }
            if let Some(standby) = self.standbys.get_mut(index) {
                standby.shadows.clear();
                standby.agreeing.clear();
                standby.slot.dialled_version = None;
            }
        }
        self.report_lost_pool_coverage(slot, reason);
        let stable_after = self.config.stable_after;
        match slot {
            Slot::Primary => {
                self.reissue = Reissue::Idle;
                self.primary.dialled_version = None;
                self.rail_loss_latched = false;
                self.primary.backoff_attempt = if resets_backoff(lifetime, stable_after) {
                    1
                } else {
                    self.primary.backoff_attempt.saturating_add(1)
                };
                if !(handover && self.take_publishing_slot()) {
                    self.schedule_reconnect(Slot::Primary);
                }
            }
            Slot::Standby(index) => {
                if let Some(standby) = self.standbys.get_mut(index) {
                    standby.slot.backoff_attempt = if resets_backoff(lifetime, stable_after) {
                        1
                    } else {
                        standby.slot.backoff_attempt.saturating_add(1)
                    };
                }
                self.schedule_reconnect(Slot::Standby(index));
            }
        }
        self.recount_recovering();
    }

    /// Walks the set once as a pool socket ends, reporting only the markets it was the last
    /// established source for.
    fn report_lost_pool_coverage(&mut self, ended: Slot, reason: ConnectionEndReason) {
        let survivors = self.pool_survivor_sets(ended);
        let (continuity, authority) = end_reason_mapping(reason);
        let attempts = self.config.max_recovery_attempts;
        let publishing = matches!(ended, Slot::Primary);
        if publishing {
            let closing: Vec<String> = self
                .markets
                .iter()
                .filter(|(_, entry)| !entry.desired)
                .map(|(slug, _)| slug.clone())
                .collect();
            for slug in closing {
                let _closed = self.close(slug.as_str());
            }
        }
        let mut losses = 0u64;
        let mut terminal = 0u64;
        let mut failure = None;
        let mut retired: Vec<String> = Vec::new();
        let Self {
            markets, segment, ..
        } = self;
        for (slug, entry) in markets.iter_mut() {
            if publishing {
                entry.subscribed_from = None;
                entry.dialled = false;
                entry.subscription = SubscriptionState::Desired;
            }
            if survivors
                .iter()
                .any(|carried| carried.binary_search(slug).is_ok())
            {
                continue;
            }
            if entry.based_this_generation {
                entry.recovery_attempts = 0;
            } else {
                entry.recovery_attempts = entry.recovery_attempts.saturating_add(1);
            }
            entry.based_this_generation = false;
            let exhausted = entry.recovery_attempts >= attempts;
            if !(entry.base_accepted || exhausted) {
                continue;
            }
            retired.push(slug.clone());
            let reported = if exhausted || entry.terminal() {
                AuthorityReason::RecoveryBaseUnavailable
            } else {
                authority.clone()
            };
            match entry
                .writer
                .report_continuity_loss(continuity.clone(), reported)
            {
                Ok(true) => {
                    losses = losses.saturating_add(1);
                    if let Some(segment) = segment.as_mut() {
                        segment.publish_state(slug, &entry.writer.published(), None);
                    }
                }
                Ok(false) => {}
                Err(error) => failure = Some(book_error_key(&error)),
            }
            if exhausted && !entry.unavailable_reported {
                entry.unavailable_reported = true;
                terminal = terminal.saturating_add(1);
            }
        }
        self.stats.continuity_losses = self.stats.continuity_losses.saturating_add(losses);
        self.stats.recovery_base_unavailable = self
            .stats
            .recovery_base_unavailable
            .saturating_add(terminal);
        if let Some(key) = failure {
            self.stats.record_failure(key);
        }
        for slug in retired {
            self.retire_agreement(slug.as_str());
        }
    }

    /// Moves a surviving socket into the vacated publishing slot, and reports whether one
    /// moved.
    ///
    /// The slot is structural: it is what carries the shard's subscription bookkeeping, its
    /// whole-set reissue, and the topology a withdrawal hands back to, so leaving it empty
    /// while sockets survive would strand all three. Nothing about any book changes with it,
    /// which is what separates this from a promotion — no market is asked whether its
    /// authority crosses, because the gate answered that for every arrival already
    /// published.
    ///
    /// An established survivor is preferred over a merely viable one, so the slot holds a
    /// connection that can actually publish wherever one exists.
    fn take_publishing_slot(&mut self) -> bool {
        let now = Instant::now();
        let viable = |standby: &Standby| {
            standby
                .slot
                .active
                .as_ref()
                .is_some_and(|active| active.is_viable(now))
        };
        let chosen = self
            .standbys
            .iter()
            .position(|standby| {
                viable(standby)
                    && standby
                        .slot
                        .active
                        .as_ref()
                        .is_some_and(|active| active.established)
            })
            .or_else(|| self.standbys.iter().position(viable));
        let Some(index) = chosen else {
            return false;
        };
        let Some(active) = self
            .standbys
            .get_mut(index)
            .and_then(|standby| standby.slot.active.take())
        else {
            return false;
        };
        let generation = active.generation;
        let dialled_set_version = active.dialled_set_version;
        let lifetime = Instant::now().saturating_duration_since(active.spawned_at);
        let carried = active.markets.clone();
        let established = active.established;
        let wire_version = self
            .standbys
            .get(index)
            .map_or(0, |standby| standby.slot.wire_version);
        let backoff_attempt = self
            .standbys
            .get(index)
            .map_or(0, |standby| standby.slot.backoff_attempt);
        self.stats.pool_handovers = self.stats.pool_handovers.saturating_add(1);
        self.reissue = Reissue::Idle;
        self.primary.active = Some(active);
        self.primary.dialled_version = None;
        self.primary.wire_version = wire_version;
        self.primary.reconnect_at = None;
        self.primary.backoff_attempt = backoff_attempt;
        for slug in &carried {
            if let Some(entry) = self.markets.get_mut(slug.as_str())
                && entry.claimed_by(dialled_set_version)
            {
                entry.dialled = false;
                entry.subscribed_from = Some(generation);
                if entry.desired {
                    entry.subscription = if established && entry.live() {
                        SubscriptionState::Established
                    } else {
                        SubscriptionState::Subscribing
                    };
                }
            }
        }
        let from = Slot::Standby(index).index();
        let to = Slot::Primary.index();
        let moved_history = self.pool_sessions_ended & socket_bit(from) != 0;
        self.pool_sessions_ended &= !socket_bit(to);
        if moved_history {
            self.pool_sessions_ended |= socket_bit(to);
        }
        self.pool_sessions_ended |= socket_bit(from);
        for entry in self.markets.values_mut() {
            let stood = entry.sockets_at_published & socket_bit(from) != 0;
            entry.sockets_at_published &= !(socket_bit(from) | socket_bit(to));
            if stood {
                entry.sockets_at_published |= socket_bit(to);
            }
            if let Some(gate) = entry.gate.as_mut() {
                gate.reassign_socket(from, to);
            }
        }
        self.release_fenced(Slot::Standby(index));
        let stable_after = self.config.stable_after;
        if let Some(standby) = self.standbys.get_mut(index) {
            standby.slot.active = None;
            standby.slot.dialled_version = None;
            standby.slot.wire_version = 0;
            standby.slot.backoff_attempt = if resets_backoff(lifetime, stable_after) {
                1
            } else {
                standby.slot.backoff_attempt.saturating_add(1)
            };
            standby.shadows.clear();
            standby.agreeing.clear();
        }
        self.schedule_reconnect(Slot::Standby(index));
        true
    }

    /// Routes one venue book snapshot to the market it names, and applies it there.
    ///
    /// Routing is a single keyed lookup: nothing here scans the set, so the cost of one
    /// frame is the same whether the shard holds three markets or three hundred. A frame
    /// for a market this shard holds no book for is counted and dropped — never fatal, and
    /// never a reason to create a book. So is a frame for a market the running connection
    /// was not dialled with: one added since the dial is carried by the replacement
    /// connection, not this one, and letting this one populate it would attribute a frame to
    /// a set that never contained it.
    ///
    /// A rejected candidate or apply costs that one market its authority and nothing else:
    /// the frame was for it, and this daemon failed to apply it. No other book in the set
    /// is touched.
    ///
    /// `arrival` is when this frame's socket read returned, on both of the clocks the daemon
    /// reads. It is carried into the segment publication an accepted apply produces: the
    /// wall-clock half stamps the slot and every mutation record, so a consumer in another
    /// process measures from the socket read rather than from the instant a slot was written,
    /// and the monotonic half is what this shard's own publish-latency histogram measures
    /// from.
    fn apply_update(
        &mut self,
        update: &OrderbookUpdate,
        received_at: Instant,
        arrival: FrameArrival,
        subscription: u64,
    ) {
        let slug = update.market_slug();
        let running = self.primary.active.as_ref().map(|active| active.generation);
        let Some(entry) = self.markets.get(slug) else {
            self.stats.frames_unrouted = self.stats.frames_unrouted.saturating_add(1);
            return;
        };
        if running.is_none() || entry.subscribed_from != running {
            self.stats.frames_uncarried = self.stats.frames_uncarried.saturating_add(1);
            return;
        }
        let market = entry.market.clone();
        let Some(position) = self.position.checked_add(1) else {
            self.stats.record_failure("book:PositionCounterOverflow");
            self.report_market_loss(
                slug,
                ContinuityReason::LocalLoss,
                AuthorityReason::LocalLoss,
            );
            return;
        };
        self.position = position;
        let provenance = match self.build_provenance(
            market,
            update,
            position,
            received_at,
            subscription,
            Slot::Primary,
        ) {
            Ok(provenance) => provenance,
            Err(_) => {
                self.stats.record_failure("book:InvalidProvenance");
                self.report_market_loss(
                    slug,
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
                self.report_market_loss(
                    slug,
                    ContinuityReason::LocalLoss,
                    AuthorityReason::LocalLoss,
                );
                return;
            }
        };
        let slug = slug.to_owned();
        let applied = self.mutate_market(slug.as_str(), |entry| {
            match entry.writer.apply_snapshot(&candidate) {
                Ok(commit) => {
                    entry.base_accepted = true;
                    entry.based_this_generation = true;
                    entry.recovery_attempts = 0;
                    entry.unavailable_reported = false;
                    if entry.desired {
                        entry.subscription = SubscriptionState::Established;
                    }
                    Ok(commit)
                }
                Err(error) => Err(error),
            }
        });
        match applied {
            Some(Ok(commit)) => {
                let mutations = u64::try_from(commit.mutations().len()).unwrap_or(u64::MAX);
                self.stats.snapshots_applied = self.stats.snapshots_applied.saturating_add(1);
                self.stats.mutations_derived =
                    self.stats.mutations_derived.saturating_add(mutations);
                self.rail_loss_latched = false;
                if let Some(active) = self.primary.active.as_mut() {
                    active.produced_base = true;
                }
                self.publish_segment_commit(slug.as_str(), &commit, arrival);
            }
            Some(Err(error)) => {
                let key = book_error_key(&error);
                self.stats.record_failure(key);
                self.report_market_loss(
                    slug.as_str(),
                    ContinuityReason::LocalLoss,
                    AuthorityReason::LocalLoss,
                );
            }
            None => {}
        }
        self.refresh_agreement(slug.as_str());
    }

    /// Hands one accepted commit to the shared-memory segment, if this shard publishes into
    /// one.
    ///
    /// Called from the single place a book commits, so the segment sees every mutation the
    /// book derived, in the order it derived them, and no in-process consumer stands between
    /// the two.
    fn publish_segment_commit(&mut self, slug: &str, commit: &BookCommit, arrival: FrameArrival) {
        let Some(segment) = self.segment.as_mut() else {
            return;
        };
        let Some(entry) = self.markets.get(slug) else {
            return;
        };
        segment.publish_commit(slug, commit, &entry.writer.published(), arrival);
    }

    /// Hands one market's current published state to the segment, for a change that derived
    /// no mutation: an evidence-based loss, which consumers learn of by reading state rather
    /// than the ring.
    fn publish_segment_state(&mut self, slug: &str) {
        let Some(segment) = self.segment.as_mut() else {
            return;
        };
        let Some(entry) = self.markets.get(slug) else {
            return;
        };
        segment.publish_state(slug, &entry.writer.published(), None);
    }

    /// Forwards one venue-reported resolution for the market it names onto that book's
    /// consumer lanes, without touching the book.
    ///
    /// Routing is by the slug the venue itself put in the payload, so nothing here depends
    /// on subscription evidence the way [`Self::apply_update`] does: a resolution changes no
    /// level, and there is no set a frame could be misattributed to when the frame names its
    /// own market. A resolution for a market this shard holds no book for is counted and
    /// dropped. One from a generation this shard no longer runs never reaches here — those
    /// are discarded whole, before routing.
    ///
    /// Nothing deduplicates by content. `docs/limitless.md` records this venue delivering
    /// one resolution as several byte-identical frames, and reproducing what the venue
    /// reported means forwarding each of them rather than deciding which was the real one.
    /// The supervisor's primary-slot gate has no counterpart here: a shard runs one
    /// connection, so every arrival is the publishing one.
    ///
    /// The resolution is retained on its market whatever the book is doing and whatever the
    /// lanes can carry, so [`Self::latest_resolution`] answers even under an explicit
    /// continuity loss: what the venue reported is true whether or not this daemon's
    /// surfaces can reproduce it. Delivery is what depends on the stream — a lost stream has
    /// no position to allocate, so no lane receives it and consumers already hold a typed
    /// loss — and, while a segment is installed, on the ring being able to carry the venue's
    /// own texts verbatim. A report the ring cannot carry is counted under
    /// `resolution:Unrepresentable` and reaches neither lane, so the two lanes stay in step
    /// and no stream position is consumed. Judging that after allocation would deliver it in
    /// process, leave the ring an unwritten slot every attached consumer polls forever, and
    /// end the run on a value the venue chose.
    ///
    /// Nothing here reaches `apply_update`, authority, recovery, or subscriptions. A book
    /// update arriving after a resolution is applied exactly as one arriving before it.
    fn forward_resolution(
        &mut self,
        resolved: &MarketResolved,
        received_at: Instant,
        arrival: FrameArrival,
        subscription: u64,
    ) {
        let slug = resolved.slug().to_owned();
        if !self.markets.contains_key(slug.as_str()) {
            self.stats.resolutions_unrouted = self.stats.resolutions_unrouted.saturating_add(1);
            return;
        }
        let Some(position) = self.position.checked_add(1) else {
            self.stats
                .record_failure("resolution:PositionCounterOverflow");
            return;
        };
        self.position = position;
        let Some(entry) = self.markets.get(slug.as_str()) else {
            return;
        };
        let Some(resolution) =
            self.build_resolution(entry, resolved, position, received_at, subscription)
        else {
            self.stats.record_failure("resolution:InvalidObservation");
            return;
        };
        let resolution = Arc::new(resolution);
        let carried = self
            .segment
            .as_ref()
            .is_none_or(|segment| segment.carries(&resolution));
        let Some(entry) = self.markets.get_mut(slug.as_str()) else {
            return;
        };
        entry.latest_resolution = Some(Arc::clone(&resolution));
        if !carried {
            self.stats.record_failure("resolution:Unrepresentable");
            return;
        }
        let Ok(delivery) = entry.writer.publish_resolution(resolution) else {
            return;
        };
        let published = entry.writer.published();
        self.stats.resolutions_forwarded = self.stats.resolutions_forwarded.saturating_add(1);
        if let Some(segment) = self.segment.as_mut() {
            segment.publish_resolution(slug.as_str(), &delivery, &published, arrival);
        }
    }

    /// Records one venue resolution as a venue-agnostic observation, or `None` when the
    /// venue's own values do not fit the vocabulary this daemon reproduces them in.
    ///
    /// Provenance mirrors [`Self::build_provenance`]: the same connection identity,
    /// generations, receive position and monotonic times, under this event's own venue
    /// family. `outcome` is deliberately absent — this venue's resolution frame carries no
    /// token, and the winner it does carry is the observation's own field rather than a
    /// second copy in provenance. `local_revision` and `continuity_epoch` are this book's
    /// current values, because that is the point in the stream the resolution is ordered at;
    /// no rebase touches them, since a resolution opens no epoch and commits no revision.
    fn build_resolution(
        &self,
        entry: &MarketEntry,
        resolved: &MarketResolved,
        position: u64,
        received_at: Instant,
        subscription: u64,
    ) -> Option<MarketResolution> {
        let winner = NativeOutcome::venue_defined(resolved.winning_outcome()).ok()?;
        let native_label = NativeLabel::new(resolved.market_type()).ok()?;
        let resolution_date = SourceTimestamp::new(resolved.resolution_date()).ok()?;
        let generation = self
            .primary
            .active
            .as_ref()
            .map_or(0, |active| active.generation);
        let connection = ConnectionIdentity::new(CONNECTION_NAME, generation).ok()?;
        let provenance = Provenance::new(ProvenanceInput {
            market: entry.market.clone(),
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
            subscription_generation: subscription,
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
            replica: ReplicaRole::PublishingPrimary,
            representation: Representation::VenueNative,
            origin: Origin::SourceReported,
            local_revision: entry.writer.book().revision(),
            continuity_epoch: entry.writer.book().continuity().epoch(),
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

    /// Records this frame's provenance truthfully: what the venue reported, and what this
    /// daemon observed locally, exactly as the single-market path records it.
    fn build_provenance(
        &self,
        market: MarketRef,
        update: &OrderbookUpdate,
        position: u64,
        received_at: Instant,
        subscription: u64,
        slot: Slot,
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
        let generation = self
            .slot(slot)
            .and_then(|state| state.active.as_ref())
            .map_or(0, |active| active.generation);
        let connection = ConnectionIdentity::new(CONNECTION_NAME, generation)?;
        Provenance::new(ProvenanceInput {
            market,
            outcome,
            native_family: ORDERBOOK_UPDATE_EVENT.to_owned(),
            source_timestamp: Some(SourceTimestamp::new(update.timestamp())?),
            source_evidence,
            daemon_generation: self.config.daemon_generation,
            connection,
            subscription_generation: subscription,
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
            replica: self.provenance_role(slot),
            representation: Representation::VenueNative,
            origin: Origin::SourceReported,
            local_revision: 0,
            continuity_epoch: 0,
        })
    }

    /// The replica role one accepted arrival's provenance carries.
    ///
    /// In an armed pool every socket is a publishing source — the key gate, not a role
    /// assignment, decides which arrival reaches a book — so a pooled arrival is stamped
    /// [`ReplicaRole::PublishingPrimary`] whichever socket carried it and whatever the gate
    /// then does with it. Without an armed pool the structural role stands, and a promoted
    /// standby stamps the publishing role from its first authoritative snapshot exactly as
    /// it did before.
    fn provenance_role(&self, slot: Slot) -> ReplicaRole {
        if self.pool_armed() {
            ReplicaRole::PublishingPrimary
        } else {
            slot.role()
        }
    }

    /// Changes one market's state, keeping the recovery count exact without scanning the
    /// set. O(1).
    fn mutate_market<R>(
        &mut self,
        slug: &str,
        change: impl FnOnce(&mut MarketEntry) -> R,
    ) -> Option<R> {
        let entry = self.markets.get_mut(slug)?;
        let before = entry.needs_recovery();
        let out = change(entry);
        let after = entry.needs_recovery();
        match (before, after) {
            (false, true) => self.recovering = self.recovering.saturating_add(1),
            (true, false) => self.recovering = self.recovering.saturating_sub(1),
            _ => {}
        }
        Some(out)
    }

    fn recount_recovering(&mut self) {
        self.recovering = self
            .markets
            .values()
            .filter(|entry| entry.needs_recovery())
            .count();
    }

    /// Reports evidence-based loss to one market's book.
    ///
    /// The loss costs this market the base it held on the connection now running, so a
    /// generation that served a base and then lost it is not counted as having carried the
    /// market through. A market already holding its terminal recovery verdict keeps it: an
    /// ordinary loss afterwards is not new evidence, and overwriting the reason would report
    /// an unrecoverable market as merely disconnected.
    fn report_market_loss(
        &mut self,
        slug: &str,
        continuity: ContinuityReason,
        authority: AuthorityReason,
    ) {
        let reported = self.mutate_market(slug, |entry| {
            entry.lost_base();
            if entry.terminal() {
                return Ok(false);
            }
            entry.writer.report_continuity_loss(continuity, authority)
        });
        match reported {
            Some(Ok(true)) => {
                self.stats.continuity_losses = self.stats.continuity_losses.saturating_add(1);
                self.publish_segment_state(slug);
            }
            Some(Err(error)) => {
                let key = book_error_key(&error);
                self.stats.record_failure(key);
            }
            _ => {}
        }
        self.refresh_agreement(slug);
    }

    /// Reports one shard-wide loss to every market that can lose by it.
    ///
    /// A frame this shard could not decode, an ingest queue that overflowed, and a
    /// connection that ended are all facts about the rail rather than about one market: the
    /// bytes that were lost named no market this daemon could read, so every book that
    /// holds authority on that rail loses it. A market that never established has no
    /// authority to lose and is left alone, which is what keeps a quiet member of a large
    /// set out of the recovery path.
    ///
    /// O(markets) once per rail failure, and latched: a storm of undecodable frames on one
    /// generation walks the set once, not once per frame. The latch is released by the next
    /// accepted base — which is the evidence the rail works again — and by the end of the
    /// generation, so a later failure on a recovered rail is reported afresh. A market
    /// already holding its terminal recovery verdict keeps it, for the reason
    /// [`Self::report_market_loss`] gives.
    fn report_shard_loss(&mut self, continuity: ContinuityReason, authority: AuthorityReason) {
        if self.rail_loss_latched {
            return;
        }
        self.rail_loss_latched = true;
        let mut losses = 0u64;
        let mut failure = None;
        let Self {
            markets, segment, ..
        } = self;
        for (slug, entry) in markets.iter_mut() {
            if !entry.base_accepted {
                continue;
            }
            entry.lost_base();
            if entry.terminal() {
                continue;
            }
            match entry
                .writer
                .report_continuity_loss(continuity.clone(), authority.clone())
            {
                Ok(true) => {
                    losses = losses.saturating_add(1);
                    if let Some(segment) = segment.as_mut() {
                        segment.publish_state(slug, &entry.writer.published(), None);
                    }
                }
                Ok(false) => {}
                Err(error) => failure = Some(book_error_key(&error)),
            }
        }
        self.stats.continuity_losses = self.stats.continuity_losses.saturating_add(losses);
        if let Some(key) = failure {
            self.stats.record_failure(key);
        }
        self.retire_all_agreement();
        self.recount_recovering();
    }

    /// Closes the connection, reconciles every removal it was still carrying, reports what
    /// its loss cost each market, and schedules its replacement.
    ///
    /// A removal is reconciled here because this is where the venue stops sending: the
    /// subscription set exists only for as long as the connection carrying it, so a market
    /// nobody wants any more publishes [`AuthorityState::Unsubscribed`] and is forgotten the
    /// moment its connection ends, rather than waiting for an answer this venue never sends.
    ///
    /// Every remaining market that held authority on this rail loses it, with the
    /// connection's own reason; one that never established stays synchronizing, because a
    /// rail that never delivered it anything took nothing from it. Subscription evidence
    /// returns to [`SubscriptionState::Desired`] for all of them: evidence here *is* the
    /// connection generation, so none of it survives the connection.
    ///
    /// Recovery is counted per market, not per shard. A generation that ended without
    /// leaving a market holding a base spends one of that market's recovery attempts — a
    /// base that was served and then lost is a failed cycle, not a successful one — and only
    /// a base still held clears them. Once a market has spent `max_recovery_attempts` it
    /// reports its own truthful [`AuthorityReason::RecoveryBaseUnavailable`] and keeps it
    /// until a base arrives. A shard-wide counter would let one busy market's snapshots
    /// clear the attempts a silent market was accumulating, so a market the venue never
    /// serves again would reconnect forever behind a healthy neighbour and never reach a
    /// terminal verdict. Scheduling stays shard-granular — one connection carries the whole
    /// set — while the verdict is each market's own.
    fn on_connection_ended(
        &mut self,
        reason: ConnectionEndReason,
        produced_base: bool,
        lifetime: Duration,
    ) {
        let _ = produced_base;
        self.reissue = Reissue::Idle;
        self.primary.dialled_version = None;
        self.rail_loss_latched = false;
        let (continuity, authority) = end_reason_mapping(reason);
        let attempts = self.config.max_recovery_attempts;
        let closing: Vec<String> = self
            .markets
            .iter()
            .filter(|(_, entry)| !entry.desired)
            .map(|(slug, _)| slug.clone())
            .collect();
        for slug in closing {
            let _closed = self.close(slug.as_str());
        }
        let mut losses = 0u64;
        let mut terminal = 0u64;
        let mut failure = None;
        let Self {
            markets, segment, ..
        } = self;
        for (slug, entry) in markets.iter_mut() {
            entry.subscribed_from = None;
            entry.dialled = false;
            entry.subscription = SubscriptionState::Desired;
            if entry.based_this_generation {
                entry.recovery_attempts = 0;
            } else {
                entry.recovery_attempts = entry.recovery_attempts.saturating_add(1);
            }
            entry.based_this_generation = false;
            let exhausted = entry.recovery_attempts >= attempts;
            let owed = entry.base_accepted || exhausted;
            if !owed {
                continue;
            }
            let reported = if exhausted || entry.terminal() {
                AuthorityReason::RecoveryBaseUnavailable
            } else {
                authority.clone()
            };
            match entry
                .writer
                .report_continuity_loss(continuity.clone(), reported)
            {
                Ok(true) => {
                    losses = losses.saturating_add(1);
                    if let Some(segment) = segment.as_mut() {
                        segment.publish_state(slug, &entry.writer.published(), None);
                    }
                }
                Ok(false) => {}
                Err(error) => failure = Some(book_error_key(&error)),
            }
            if exhausted && !entry.unavailable_reported {
                entry.unavailable_reported = true;
                terminal = terminal.saturating_add(1);
            }
        }
        self.stats.continuity_losses = self.stats.continuity_losses.saturating_add(losses);
        self.stats.recovery_base_unavailable = self
            .stats
            .recovery_base_unavailable
            .saturating_add(terminal);
        if let Some(key) = failure {
            self.stats.record_failure(key);
        }
        self.retire_all_agreement();
        self.recount_recovering();
        self.primary.backoff_attempt = if resets_backoff(lifetime, self.config.stable_after) {
            1
        } else {
            self.primary.backoff_attempt.saturating_add(1)
        };
        self.schedule_reconnect(Slot::Primary);
    }

    /// Closes whichever role's connection ended, and decides what replaces it.
    ///
    /// While a pool is armed there is no promotion question to ask, and one end reason is
    /// answered before anything else: a generation that ended because its own notice queue
    /// overflowed past delivery hides a loss rather than reporting one, and the notice it
    /// could not hand over may have been a book update. Every socket of an armed pool is an
    /// authoritative source, so that is an authoritative continuity loss on either kind of
    /// slot. Only an established session is treated this way, because one that never reached
    /// its subscription had no book state to lose.
    ///
    /// Without an armed pool the publishing role is where the promotion question lives: if a standby connection is
    /// still viable it takes over the publishing role, and each market in the set then
    /// answers for itself whether its authority crosses with it. Without a viable standby
    /// the role falls through to the single-source path exactly as it always has.
    ///
    /// A *deliberate* replacement does not come through here at all. [`Self::reconcile_slot`]
    /// closes its own connection, because a set change replaces every role's connection:
    /// there is no surviving source to switch to, only sources this shard is itself
    /// retiring, and promoting one would hand the publishing role to a connection carrying
    /// the set the change is abandoning.
    ///
    /// A standby's own end costs the shard nothing but that standby's shadows, which are
    /// what the connection was maintaining. Its role runs its own reconnect ladder and its
    /// replacement rebuilds the shadows from the venue's answer to a fresh subscription.
    fn on_slot_ended(
        &mut self,
        slot: Slot,
        reason: ConnectionEndReason,
        produced_base: bool,
        established: bool,
        lifetime: Duration,
    ) {
        if self.pool_armed() {
            if established && matches!(reason, ConnectionEndReason::NoticeUndeliverable) {
                self.report_shard_loss(ContinuityReason::LocalLoss, AuthorityReason::Overload);
            }
            self.on_pool_slot_ended(slot, reason, lifetime, established, true);
            return;
        }
        match slot {
            Slot::Primary => {
                if !self.promote_a_standby(reason, established) {
                    self.on_connection_ended(reason, produced_base, lifetime);
                }
            }
            Slot::Standby(index) => self.on_standby_ended(index, reason, lifetime),
        }
    }

    /// Hands the publishing role to the first standby that can still take it, and reports
    /// whether one did.
    ///
    /// First rather than best: `docs/design.md`'s promotion rule states an eligibility
    /// predicate, not a ranking, so nothing here weighs one eligible standby against
    /// another. A later standby may agree on markets this one does not; those markets are
    /// refused and recover through the ordinary rail.
    ///
    /// Eligibility is re-read at each attempt because selecting a standby and taking it over
    /// are two instants: [`Self::switch_source`] refuses a connection that stopped being
    /// viable between them, and the next role is asked instead. A shard with nothing left to
    /// promote answers `false`, and the caller runs the single-source path.
    fn promote_a_standby(&mut self, ended: ConnectionEndReason, established: bool) -> bool {
        (0..self.standbys.len()).any(|index| self.switch_source(index, ended, established))
    }

    /// Closes one standby generation. It cannot cost a book anything: the shadows are
    /// discarded, the role records what ended it, and a replacement is scheduled on the
    /// standby's own ladder.
    ///
    /// The reason is kept because a namespace refusal, an overflowed notice queue and a dead
    /// socket are different operator problems: redundancy disappearing is one event to an
    /// operator and three to whoever has to fix it, and collapsing them into an absent
    /// connection row would lose that distinction exactly when it is needed. A deliberate
    /// subscription replacement is not a failure and is already counted by
    /// `set_replacements` at its call site, so it never reaches this counter.
    fn on_standby_ended(&mut self, index: usize, reason: ConnectionEndReason, lifetime: Duration) {
        let stable_after = self.config.stable_after;
        if !matches!(reason, ConnectionEndReason::SubscriptionReplaced) {
            *self
                .stats
                .standby_ends
                .entry(standby_end_key(reason))
                .or_insert(0) += 1;
        }
        if let Some(standby) = self.standbys.get_mut(index) {
            standby.shadows.clear();
            standby.agreeing.clear();
            standby.slot.dialled_version = None;
            standby.slot.backoff_attempt = if resets_backoff(lifetime, stable_after) {
                1
            } else {
                standby.slot.backoff_attempt.saturating_add(1)
            };
        }
        self.schedule_reconnect(Slot::Standby(index));
    }

    /// Hands the publishing role to standby `index`, and asks the promotion question once
    /// per market in the set.
    ///
    /// The connection moves first: it is a viable socket already carrying this shard's whole
    /// subscription, so it is the right thing to be publishing from whatever any individual
    /// market's evidence says. What each market's evidence decides is whether its
    /// *authority* crosses with it.
    ///
    /// A market promotes only when [`promotable`] holds for it at this instant, and only
    /// when the connection that just ended did not end holding a loss it could not report.
    /// [`ConnectionEndReason::NoticeUndeliverable`] on an established publishing connection
    /// is exactly that: the note it could not hand over may have been the overload report,
    /// or a book update that never reached the book. Two replicas agreeing after it says
    /// only that neither holds what went missing, so the loss is reported for every market
    /// that held authority rather than erased by the takeover. A connection that never
    /// established is exempt, having had no book state to lose.
    ///
    /// A market that promotes is not touched at all: no continuity loss, no epoch, no
    /// revision spent, and no publication. The only thing that changes is which generation
    /// its subscription evidence names, because the promoted connection is the one the venue
    /// is now serving it on.
    ///
    /// A market that is refused loses authority with [`refusal_reason`]'s verdict — replica
    /// divergence when two comparable histories disagreed, and the connection's own reason
    /// otherwise — and is left subscribed to the promoted connection so the ordinary
    /// whole-set reissue rail can rebase it. Leaving it with no subscription evidence would
    /// strand it: the venue keeps sending that market on the promoted socket, and every
    /// frame would be discarded as uncarried forever.
    ///
    /// Availability pressure is not part of any of this. A refused market waits for a venue
    /// base, and the shadow it refused is never installed over the book.
    ///
    /// Reports whether the role was handed over. Viability is re-read here rather than
    /// trusted from the selection that led here: the decision and the takeover are two
    /// instants, and a standby whose task ended or whose heartbeat evidence expired between
    /// them would name a dead connection as this shard's source. A refusal leaves that
    /// standby's connection exactly where it was, so its own end is reaped and reported by
    /// the ordinary path.
    ///
    /// The vacated role keeps its transport bookkeeping. Its fenced predecessor, if it holds
    /// one, is aborted rather than dropped — a dropped [`JoinHandle`] detaches its task, and
    /// a detached socket is one nothing can close — and its replacement is scheduled on its
    /// own reconnect ladder rather than dialled the instant [`Self::spawn_due`] next runs,
    /// so a promotion cannot spend attempts the ladder had not released.
    fn switch_source(
        &mut self,
        index: usize,
        ended: ConnectionEndReason,
        established: bool,
    ) -> bool {
        let now = Instant::now();
        let viable = self.standbys.get(index).is_some_and(|standby| {
            standby
                .slot
                .active
                .as_ref()
                .is_some_and(|active| active.is_viable(now))
        });
        if !viable {
            return false;
        }
        let Some(active) = self
            .standbys
            .get_mut(index)
            .and_then(|standby| standby.slot.active.take())
        else {
            return false;
        };
        let generation = active.generation;
        let lifetime = Instant::now().saturating_duration_since(active.spawned_at);
        let shadows = self
            .standbys
            .get_mut(index)
            .map(|standby| core::mem::take(&mut standby.shadows))
            .unwrap_or_default();
        let wire_version = self
            .standbys
            .get(index)
            .map_or(0, |standby| standby.slot.wire_version);
        let backoff_attempt = self
            .standbys
            .get(index)
            .map_or(0, |standby| standby.slot.backoff_attempt);
        let carried = active.markets.clone();
        let dialled_set_version = active.dialled_set_version;
        self.stats.source_switches = self.stats.source_switches.saturating_add(1);
        self.reissue = Reissue::Idle;
        self.rail_loss_latched = false;
        self.primary.active = Some(active);
        self.primary.dialled_version = None;
        self.primary.wire_version = wire_version;
        self.primary.reconnect_at = None;
        self.primary.backoff_attempt = backoff_attempt;
        self.apply_promotions(
            generation,
            dialled_set_version,
            &carried,
            &shadows,
            ended,
            established,
        );
        self.release_fenced(Slot::Standby(index));
        let stable_after = self.config.stable_after;
        if let Some(standby) = self.standbys.get_mut(index) {
            standby.slot.active = None;
            standby.slot.dialled_version = None;
            standby.slot.wire_version = 0;
            standby.slot.backoff_attempt = if resets_backoff(lifetime, stable_after) {
                1
            } else {
                standby.slot.backoff_attempt.saturating_add(1)
            };
            standby.shadows.clear();
            standby.agreeing.clear();
        }
        self.schedule_reconnect(Slot::Standby(index));
        self.recount_recovering();
        true
    }

    /// Walks the set once, deciding each market's authority across a source switch.
    ///
    /// A market the promoted connection's dial predates is carried by nothing it can serve,
    /// whatever slug that dial names, so it is treated exactly as one the dial never named.
    ///
    /// O(markets in the promoted connection's dialled set) once per switch, and off the
    /// update path entirely. The comparison is [`agreement`]'s exact level-by-level
    /// equality rather than any fingerprint of it, because a collision in a fingerprint
    /// would be a promotion this daemon could not justify.
    fn apply_promotions(
        &mut self,
        generation: u64,
        dialled_set_version: u64,
        carried: &[String],
        shadows: &HashMap<String, OrderBook>,
        ended: ConnectionEndReason,
        established: bool,
    ) {
        let lossy = established && matches!(ended, ConnectionEndReason::NoticeUndeliverable);
        let attempts = self.config.max_recovery_attempts;
        let mut promoted = 0u64;
        let mut refused = 0u64;
        let mut losses = 0u64;
        let mut terminal = 0u64;
        let mut failure = None;
        let mut retired: Vec<String> = Vec::new();
        let closing: Vec<String> = self
            .markets
            .iter()
            .filter(|(_, entry)| !entry.desired)
            .map(|(slug, _)| slug.clone())
            .collect();
        for slug in closing {
            let _closed = self.close(slug.as_str());
        }
        let Self {
            markets, segment, ..
        } = self;
        for (slug, entry) in markets.iter_mut() {
            let verdict = shadow_verdict(entry, shadows.get(slug));
            let eligible = !lossy && promotable(entry, &verdict);
            let subscribed =
                carried.binary_search(slug).is_ok() && entry.claimed_by(dialled_set_version);
            entry.dialled = false;
            entry.subscribed_from = subscribed.then_some(generation);
            if eligible {
                promoted = promoted.saturating_add(1);
                entry.based_this_generation = true;
                entry.recovery_attempts = 0;
                entry.unavailable_reported = false;
                entry.subscription = if entry.desired && subscribed {
                    SubscriptionState::Established
                } else {
                    SubscriptionState::Desired
                };
                continue;
            }
            entry.subscription = if entry.desired && subscribed {
                SubscriptionState::Subscribing
            } else {
                SubscriptionState::Desired
            };
            if entry.based_this_generation {
                entry.recovery_attempts = 0;
            } else {
                entry.recovery_attempts = entry.recovery_attempts.saturating_add(1);
            }
            entry.based_this_generation = false;
            retired.push(slug.clone());
            let exhausted = entry.recovery_attempts >= attempts;
            if !(entry.base_accepted || exhausted) {
                continue;
            }
            refused = refused.saturating_add(1);
            entry.lost_base();
            let reported = if exhausted || entry.terminal() {
                AuthorityReason::RecoveryBaseUnavailable
            } else if lossy {
                AuthorityReason::Overload
            } else {
                refusal_reason(&verdict, ended)
            };
            match entry
                .writer
                .report_continuity_loss(ContinuityReason::Reconnect, reported)
            {
                Ok(true) => {
                    losses = losses.saturating_add(1);
                    if let Some(segment) = segment.as_mut() {
                        segment.publish_state(slug, &entry.writer.published(), None);
                    }
                }
                Ok(false) => {}
                Err(error) => failure = Some(book_error_key(&error)),
            }
            if exhausted && !entry.unavailable_reported {
                entry.unavailable_reported = true;
                terminal = terminal.saturating_add(1);
            }
        }
        for slug in retired {
            self.retire_agreement(slug.as_str());
        }
        self.stats.markets_promoted = self.stats.markets_promoted.saturating_add(promoted);
        self.stats.markets_promotion_refused =
            self.stats.markets_promotion_refused.saturating_add(refused);
        self.stats.continuity_losses = self.stats.continuity_losses.saturating_add(losses);
        self.stats.recovery_base_unavailable = self
            .stats
            .recovery_base_unavailable
            .saturating_add(terminal);
        if let Some(key) = failure {
            self.stats.record_failure(key);
        }
    }

    /// Whether every market this shard wants has spent its recovery attempts, which is the
    /// only state in which the slower retry cadence is the right one: a shard still owing a
    /// base to one market that keeps receiving them must keep reconnecting at the ordinary
    /// pace for it.
    fn recovery_exhausted(&self) -> bool {
        let attempts = self.config.max_recovery_attempts;
        self.desired_count > 0
            && self
                .markets
                .values()
                .filter(|entry| entry.desired)
                .all(|entry| entry.recovery_attempts >= attempts)
    }

    /// Confirms a missed heartbeat deadline against everything already queued, and closes
    /// the generation before any of that queue reaches a book.
    ///
    /// The deadline is polled ahead of the ingest queue so market data cannot starve
    /// liveness detection; the cost is that queued work may still be waiting when the
    /// deadline fires. This stages what the queue holds — bounded by its capacity, which is
    /// all it can hold — and dispatches it in two parts.
    ///
    /// Every *other* generation's notices are dispatched first and in full. Nothing about
    /// one role's liveness makes another role's work late, and this is what makes the
    /// takeover decision that may follow a decision about the topology as it actually
    /// stands: a standby's queued snapshot is part of the shadow the promotion question is
    /// asked against, never work that replays over a published book after the question has
    /// been answered.
    ///
    /// Only the expiring generation's own liveness evidence is applied before the verdict,
    /// so a busy connection is never mistaken for a dead one. Its remaining staged notices
    /// are dispatched after it: once the deadline stands the generation is fenced first, and
    /// everything it staged is discarded as the late work it is.
    fn on_heartbeat_missed(&mut self, slot: Slot) {
        let Some(generation) = self
            .slot(slot)
            .and_then(|state| state.active.as_ref())
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
        let expired = self
            .slot(slot)
            .and_then(|state| state.active.as_ref())
            .and_then(ActiveConnection::heartbeat_expiry)
            .is_some_and(|expiry| expiry <= Instant::now());
        if expired && let Some(active) = self.slot_mut(slot).and_then(|state| state.active.take()) {
            let produced_base = active.produced_base;
            let established = active.established;
            let lifetime = Instant::now().saturating_duration_since(active.spawned_at);
            self.fence(slot, active);
            self.on_slot_ended(
                slot,
                ConnectionEndReason::HeartbeatTimeout,
                produced_base,
                established,
                lifetime,
            );
        }
        for notice in expiring {
            self.on_notice(notice);
        }
    }

    /// Makes a generation's work ineligible immediately and lets its task drain for a
    /// bounded window before aborting it. At most one fenced connection is held, so a
    /// flapping venue cannot accumulate sockets.
    fn fence(&mut self, slot: Slot, active: ActiveConnection) {
        let expires_at = Instant::now() + self.config.fenced_linger;
        let Some(state) = self.slot_mut(slot) else {
            active.handle.abort();
            return;
        };
        if let Some(previous) = state.fenced.take() {
            previous.handle.abort();
        }
        state.fenced = Some(FencedConnection {
            handle: active.handle,
            expires_at,
        });
        self.stats.fenced_generations = self.stats.fenced_generations.saturating_add(1);
    }

    fn release_fenced(&mut self, slot: Slot) {
        if let Some(fenced) = self.slot_mut(slot).and_then(|state| state.fenced.take()) {
            fenced.handle.abort();
        }
    }

    /// Schedules one role's next dial on that role's own ladder.
    ///
    /// The configured ceilings are stated for a single-connection shard and are paced here
    /// by the replica count, so a shard's worst-case daily attempt total is the same
    /// whatever its topology. Every ladder draws from one process-wide ledger, and turning
    /// redundancy on must buy a second socket rather than a second day's worth of attempts.
    fn schedule_reconnect(&mut self, slot: Slot) {
        let ladders = u32::try_from(self.config.replicas).unwrap_or(u32::MAX);
        let attempt = self.slot(slot).map_or(0, |state| state.backoff_attempt);
        let base = if self.recovery_exhausted() {
            self.config.exhausted_backoff.saturating_mul(ladders)
        } else {
            exponential(
                self.config.initial_backoff,
                attempt,
                self.config.max_backoff.saturating_mul(ladders),
            )
        };
        let delay = jittered(base, self.jitter.hash_one((slot.index(), attempt)));
        if let Some(state) = self.slot_mut(slot) {
            state.reconnect_at = Some(Instant::now() + delay);
        }
    }

    fn finish(&mut self) -> ShardStats {
        for slot in self.slots().collect::<Vec<_>>() {
            if let Some(active) = self.slot_mut(slot).and_then(|state| state.active.take()) {
                active.handle.abort();
            }
            self.release_fenced(slot);
        }
        self.stats.frames_seen = self.frames.load(Ordering::Relaxed);
        self.stats.queue_age = self.queue_age.summary();
        let publish_latency = self.publish_latency();
        self.stats.publish_latency = publish_latency;
        self.stats.segment_failure = self.segment_failure().map(|error| format!("{error:?}"));
        self.stats.clone()
    }
}

/// Whether `slug` is a venue-native market identifier a shard will accept.
///
/// The one place the answer is decided, so a caller that has to route a market before a
/// shard has seen it — the daemon deciding which shard a command belongs to — asks the same
/// question the shard will answer, rather than keeping a second opinion.
/// What one standby's shadow of a market says about carrying that market's authority.
///
/// A standby holding no shadow of the market holds no comparable history, which is exactly
/// [`DivergenceReason::ContinuityMismatch`]: it is not evidence against the book, only the
/// absence of evidence for a source switch.
fn shadow_verdict(entry: &MarketEntry, shadow: Option<&OrderBook>) -> StandbyState {
    match shadow {
        Some(shadow) => agreement(shadow, &entry.writer.published()),
        None => StandbyState::Divergent(DivergenceReason::ContinuityMismatch),
    }
}

/// Whether a market could carry its authority across a promotion decided right now.
///
/// [`agreement`]'s verdict plus the two facts a shadow cannot speak for: the market is still
/// wanted and its published book holds a base, and that book is live — one that has already
/// lost authority must recover from a fresh venue base, never from a source switch.
///
/// One definition serves the takeover and the telemetry reporting its coverage, so the
/// failover readiness an operator reads counts exactly the markets a takeover would carry
/// and never a market the takeover would refuse.
fn promotable(entry: &MarketEntry, verdict: &StandbyState) -> bool {
    entry.desired && entry.base_accepted && entry.live() && *verdict == StandbyState::Agreeing
}

/// The standby index a slot names, or `None` for the publishing slot.
/// One pool socket's place in a per-market socket set.
///
/// Zero for a socket index no `u8` can address, which is unreachable while
/// [`crate::MAX_POOL_SOCKETS`] bounds a pool: a socket the set cannot hold is one nothing is
/// ever recorded for, which is the same answer as a socket nothing has been recorded for.
fn socket_bit(socket: usize) -> u8 {
    u32::try_from(socket)
        .ok()
        .and_then(|shift| 1u8.checked_shl(shift))
        .unwrap_or(0)
}

fn slot_index(slot: Slot) -> Option<usize> {
    match slot {
        Slot::Primary => None,
        Slot::Standby(index) => Some(index),
    }
}

/// The counter key a closed standby generation is recorded under.
fn standby_end_key(reason: ConnectionEndReason) -> &'static str {
    match replica_failure(reason) {
        ReplicaFailureReason::Protocol => "protocol",
        ReplicaFailureReason::Overload => "overload",
        ReplicaFailureReason::Disconnect => "disconnect",
    }
}

pub fn is_market_slug(slug: &str) -> bool {
    NativeMarketKey::new(NativeIdentifierKind::slug(), slug).is_ok()
}

fn micros_between(start: Instant, end: Instant) -> u64 {
    u64::try_from(end.saturating_duration_since(start).as_micros()).unwrap_or(u64::MAX)
}

/// Resolves when the connection task finishes, or never when none is running. A task that
/// ended without returning a reason is reported as [`ConnectionEndReason::TaskFailed`].
async fn join_active(active: &mut Option<ActiveConnection>) -> ConnectionEndReason {
    match active {
        Some(active) => (&mut active.handle)
            .await
            .unwrap_or(ConnectionEndReason::TaskFailed),
        None => std::future::pending().await,
    }
}

/// Resolves when any standby role's connection task finishes, naming the role, or never
/// when no standby is running one.
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
/// resolves only when a deadline existed, so the fallback names a role rather than standing
/// for one.
fn standby_slot(earliest: Option<(Instant, usize)>) -> Slot {
    Slot::Standby(earliest.map_or(0, |(_, index)| index))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limitless::decode_event;
    use crate::wire::lexical::LexicalLimits;
    use crate::wire::session::EngineIoOpen;
    use crate::wire::socketio::{DecodedFrame, WebSocketOpcode, decode_frame};
    use crate::{SegmentConfig, SegmentLayout, SegmentRegion};
    use std::path::PathBuf;
    use std::sync::Arc;

    fn shard(markets: &[&str]) -> Shard {
        Shard::new(ShardConfig {
            markets: markets.iter().map(|slug| (*slug).to_owned()).collect(),
            ..ShardConfig::default()
        })
        .expect("test shard configuration is valid")
    }

    /// A connection whose task has already returned and whose control receiver has been
    /// dropped: the state a shard is in between a connection dying and the run loop joining
    /// it, in which every handover to that connection fails as `Closed`.
    ///
    /// Its dial is newer than any entry a shard can install, so what it carries is decided by
    /// the set it names and never by an incarnation that outran it.
    async fn ended_connection(generation: u64, markets: Vec<String>) -> ActiveConnection {
        let handle = tokio::spawn(async { ConnectionEndReason::SocketClosed });
        while !handle.is_finished() {
            tokio::task::yield_now().await;
        }
        let (control, commands) = mpsc::channel(1);
        drop(commands);
        ActiveConnection {
            generation,
            markets,
            dialled_set_version: u64::MAX,
            spawned_at: Instant::now(),
            handle,
            control,
            heartbeat_deadline: None,
            last_heartbeat: None,
            established: true,
            produced_base: false,
            session: None,
        }
    }

    /// A standby whose connection stopped being viable between the decision and the
    /// takeover does not become the publishing source.
    ///
    /// Selecting a standby and handing it the publishing role are two instants. On a
    /// multithreaded runtime the connection can finish, or its heartbeat evidence expire,
    /// in between — and the shard would then be publishing from a task that has already
    /// returned, with every book in the set live behind a source that cannot produce.
    /// Refusing leaves the connection where the run loop reaps it and the ordinary
    /// single-source path answers the loss.
    #[tokio::test]
    async fn a_standby_that_stopped_being_viable_is_refused_the_publishing_role() {
        let markets = vec!["btc-up-or-down-5-min-1788172500".to_owned()];
        let mut shard = Shard::new(ShardConfig {
            replicas: 2,
            markets: markets.clone(),
            ..ShardConfig::default()
        })
        .expect("test shard configuration is valid");
        shard.standbys[0].slot.active = Some(ended_connection(7, markets).await);

        let took = shard.switch_source(0, ConnectionEndReason::SocketClosed, true);

        assert!(
            !took,
            "a connection whose task has already returned cannot take the publishing role"
        );
        assert!(
            shard.primary.active.is_none(),
            "the publishing role was left for the single-source path rather than filled              with a dead connection"
        );
        assert!(
            shard.standbys[0].slot.active.is_some(),
            "the refused connection stays where the run loop reaps and reports it"
        );
        assert_eq!(
            shard.stats.source_switches, 0,
            "a takeover that could not happen is not counted as one"
        );

        shard.on_slot_ended(
            Slot::Primary,
            ConnectionEndReason::SocketClosed,
            false,
            true,
            Duration::ZERO,
        );
        assert_eq!(
            shard.stats.source_switches, 0,
            "the publishing role's end fell through to the single-source path"
        );
        assert!(
            shard.primary.reconnect_at.is_some(),
            "the publishing role is scheduled to redial rather than left waiting on a              promotion that was refused"
        );
    }

    /// A pooled book keeps no authority on a socket whose task has already returned.
    ///
    /// Both pooled tasks can finish before the run loop joins either, and the end reaped
    /// first asks which sockets still carry the set. A connection that has already returned
    /// answers with a subscription it can never deliver again, so the book would go on
    /// claiming authority behind two dead sources until the second end was processed — and a
    /// fast replacement establishing in between would hide the boundary entirely. Survivor
    /// evaluation therefore reads the same viability the takeover path reads, at the instant
    /// it decides.
    #[tokio::test]
    async fn a_pooled_book_keeps_no_authority_on_a_socket_whose_task_has_returned() {
        let slug = "btc-up-or-down-5-min-1788172500";
        let markets = vec![slug.to_owned()];
        let mut shard = Shard::new(ShardConfig {
            replicas: 2,
            markets: markets.clone(),
            ..ShardConfig::default()
        })
        .expect("test shard configuration is valid");
        assert!(
            shard.pool_armed(),
            "the venue's own declaration licenses the pool this contract is about"
        );
        shard.standbys[0].slot.active = Some(ended_connection(7, markets).await);
        let entry = shard
            .markets
            .get_mut(slug)
            .expect("the market is in the set");
        entry.base_accepted = true;
        entry.subscribed_from = Some(6);

        shard.on_slot_ended(
            Slot::Primary,
            ConnectionEndReason::SocketClosed,
            false,
            true,
            Duration::ZERO,
        );

        assert!(
            matches!(
                shard.markets[slug].writer.book().authority(),
                AuthorityState::Stale(_)
            ),
            "a book whose every source has stopped producing is not live: {:?}",
            shard.markets[slug].writer.book().authority()
        );
        assert_eq!(
            shard.stats.continuity_losses, 1,
            "the loss is reported when it happened, not when the second end is reaped"
        );
        assert!(
            shard.primary.active.is_none(),
            "the publishing slot was not filled from a connection that has already returned"
        );
        assert_eq!(
            shard.stats.pool_handovers, 0,
            "a dead socket is no survivor to hand the publishing slot to"
        );
    }

    /// A connection whose task is still running and whose session is established: what a
    /// shard's publishing slot holds while a connection feeds it.
    ///
    /// Its dial is newer than any entry a shard can install, so what it carries is decided by
    /// the set it names and never by an incarnation that outran it.
    fn running_connection(generation: u64, markets: Vec<String>) -> ActiveConnection {
        let handle = tokio::spawn(std::future::pending::<ConnectionEndReason>());
        let (control, _commands) = mpsc::channel(1);
        ActiveConnection {
            generation,
            markets,
            dialled_set_version: u64::MAX,
            spawned_at: Instant::now(),
            handle,
            control,
            heartbeat_deadline: None,
            last_heartbeat: None,
            established: true,
            produced_base: false,
            session: None,
        }
    }

    /// A pooled arrival is taken for the incarnation of the market it names, not for the
    /// slug.
    ///
    /// A connection's dialled set is immutable and a market's book is not: a removal retires
    /// the book and a return installs a new one behind the same slug. A dial taken before
    /// that is subscribed to the retired incarnation, so its frames are none of the new
    /// book's business however the set text reads.
    #[tokio::test]
    async fn a_dial_older_than_a_market_s_incarnation_carries_nothing_for_it() {
        let slug = "btc-up-or-down-5-min-1788172500";
        let markets = vec![slug.to_owned()];
        let mut shard = Shard::new(ShardConfig {
            replicas: 2,
            markets: markets.clone(),
            ..ShardConfig::default()
        })
        .expect("test shard configuration is valid");
        let installed = shard.markets[slug].installed_version;
        shard.primary.active = Some(running_connection(9, markets.clone()));

        for dialled in [installed, installed.saturating_add(1)] {
            if let Some(active) = shard.primary.active.as_mut() {
                active.dialled_set_version = dialled;
            }
            assert!(
                shard.pool_carries(Slot::Primary, slug),
                "a dial at or past the entry's own version carries it"
            );
            assert!(
                shard.dial_carries_current_entries(dialled),
                "and asks for no replacement"
            );
        }

        if let Some(active) = shard.primary.active.as_mut() {
            active.dialled_set_version = installed.saturating_sub(1);
        }
        assert!(
            !shard.pool_carries(Slot::Primary, slug),
            "a dial older than the entry is subscribed to an incarnation this shard retired"
        );
        assert!(
            !shard.dial_carries_current_entries(installed.saturating_sub(1)),
            "so that connection is owed the replacement a set change always gets"
        );
    }

    /// A hand-back tells three kinds of book apart by what the publishing socket left behind.
    ///
    /// The licence is what let another socket advance a book, and the topology handed back to
    /// reads no key: whatever the publishing connection sends next is applied as forward
    /// history. So only a book that connection's own session has been seen to stand at can be
    /// left live, and the two ways a book fails that test are different facts about it — no
    /// permitted publisher at all, and a publisher with no proof its history is the newer one.
    #[tokio::test]
    async fn a_hand_back_tells_a_book_it_lost_by_the_evidence_the_publishing_socket_left() {
        let standing = "btc-up-or-down-5-min-1788172500";
        let behind = "eth-up-or-down-5-min-1788172500";
        let uncarried = "sol-up-or-down-5-min-1788172500";
        let markets = vec![standing.to_owned(), behind.to_owned(), uncarried.to_owned()];
        let mut shard = Shard::new(ShardConfig {
            replicas: 2,
            markets: markets.clone(),
            ..ShardConfig::default()
        })
        .expect("test shard configuration is valid");
        let generation = 11;
        shard.primary.active = Some(running_connection(generation, markets));
        for slug in [standing, behind, uncarried] {
            let entry = shard
                .markets
                .get_mut(slug)
                .expect("the market is in the set");
            entry.base_accepted = true;
            entry.subscribed_from = Some(generation);
            entry.sockets_at_published = socket_bit(Slot::Standby(0).index());
        }
        if let Some(entry) = shard.markets.get_mut(standing) {
            entry.sockets_at_published |= socket_bit(Slot::Primary.index());
        }
        if let Some(entry) = shard.markets.get_mut(uncarried) {
            entry.subscribed_from = None;
        }

        shard.withdraw_licence(PoolDegradeReason::KeyUnavailable);

        assert!(
            matches!(
                shard.markets[standing].writer.book().authority(),
                AuthorityState::Synchronizing
            ),
            "a book the publishing socket stands at is not touched by the hand-back: {:?}",
            shard.markets[standing].writer.book().authority()
        );
        assert_eq!(
            shard.markets[behind].writer.book().authority(),
            &AuthorityState::Stale(AuthorityReason::OrderingUnknown),
            "a book its publisher stands below has no proof its next frame is newer"
        );
        assert_eq!(
            shard.markets[uncarried].writer.book().authority(),
            &AuthorityState::Stale(AuthorityReason::SubscriptionLost),
            "a book no permitted publisher carries has lost its last source"
        );
        assert_eq!(
            shard.stats.continuity_losses, 2,
            "one loss each, and none for the book that kept its authority"
        );
    }

    /// One venue text, decoded exactly as a connection decodes what it reads.
    fn venue_frame(text: &str) -> DecodedFrame {
        decode_frame(
            text.as_bytes(),
            WebSocketOpcode::Text,
            LexicalLimits::venue_payload(),
        )
        .expect("the venue text decodes as a frame")
    }

    /// One `orderbookUpdate` as the venue publishes it, carrying `version` only when the
    /// venue would have.
    fn book_event(slug: &str, size: &str, version: Option<u64>) -> LimitlessEvent {
        let keyed = version.map_or_else(String::new, |version| format!(",\"version\":{version}"));
        let text = format!(
            "42/markets,[\"orderbookUpdate\",{{\"marketSlug\":\"{slug}\",\"orderbook\":{{\"bids\":[{{\"price\":0.51,\"size\":{size}}}],\"asks\":[{{\"price\":0.52,\"size\":{size}}}]}},\"timestamp\":\"2026-08-31T00:00:00.000Z\"{keyed}}}]"
        );
        decode_event(&venue_frame(&text)).expect("the venue text decodes as an event")
    }

    fn book_notice(generation: u64, event: LimitlessEvent) -> ConnectionNotice {
        ConnectionNotice {
            generation,
            note: ConnectionNote::Event {
                event,
                received_at: Instant::now(),
                arrival_time_nanos: 1,
                subscription_generation: 1,
            },
        }
    }

    /// The venue's own Engine.IO open packet, which is what a connection announces itself
    /// established with.
    fn ready_notice(generation: u64) -> ConnectionNotice {
        let frame = venue_frame(
            "0{\"sid\":\"session\",\"upgrades\":[],\"pingInterval\":60000,\"pingTimeout\":60000,\"maxPayload\":1000000}",
        );
        let open =
            EngineIoOpen::from_open_payload(frame.payload().expect("the open carries a payload"))
                .expect("the venue open packet is valid");
        ConnectionNotice {
            generation,
            note: ConnectionNote::Ready {
                open,
                subscription_generation: 1,
                observed_at: Instant::now(),
            },
        }
    }

    /// A dial that predates a market's incarnation claims nothing for it, so nothing the
    /// topology switches to afterwards can take its frames for that market either.
    ///
    /// [`Shard::on_heartbeat_missed`] stages what the ingest queue holds and dispatches every
    /// other generation's notices in one pass, with no reconciliation between them. So a
    /// retired dial's `Ready`, a current socket's frame that withdraws the pool licence, and
    /// that same retired dial's book update can all be answered inside one call: the first
    /// two leave the shard publishing from one primary, and the third is then judged by the
    /// subscription evidence the first wrote and by nothing else. The evidence is what has to
    /// be right, because the rule that reads it changes underneath.
    #[tokio::test]
    async fn a_ready_from_a_retired_dial_claims_no_market_a_re_add_replaced() {
        let reinstalled = "btc-up-or-down-5-min-1788172500";
        let untouched = "eth-up-or-down-5-min-1788172500";
        let mut shard = Shard::new(ShardConfig {
            replicas: 3,
            markets: vec![reinstalled.to_owned(), untouched.to_owned()],
            ..ShardConfig::default()
        })
        .expect("test shard configuration is valid");
        let retired = shard.desired_version;
        let _removed = shard.remove_markets(vec![reinstalled.to_owned()]);
        let _added = shard.add_markets(vec![reinstalled.to_owned()]);
        let current = shard.desired_version;
        assert!(
            shard.markets[reinstalled].installed_version > retired,
            "the re-added market belongs to an incarnation the retired dial predates"
        );
        assert!(
            shard.markets[untouched].installed_version <= retired,
            "the market that never left is the one that dial was actually given"
        );
        let carried = vec![reinstalled.to_owned(), untouched.to_owned()];

        let mut retired_dial = running_connection(7, carried.clone());
        retired_dial.established = false;
        retired_dial.dialled_set_version = retired;
        shard.primary.active = Some(retired_dial);
        shard.primary.dialled_version = Some(retired);

        let mut expiring = running_connection(8, carried.clone());
        expiring.dialled_set_version = current;
        expiring.heartbeat_deadline = Some(Duration::from_millis(1));
        expiring.last_heartbeat = Instant::now().checked_sub(Duration::from_secs(1));
        shard.standbys[0].slot.active = Some(expiring);

        let mut serving = running_connection(9, carried);
        serving.dialled_set_version = current;
        shard.standbys[1].slot.active = Some(serving);

        let notices = shard.notices_tx.clone();
        notices
            .send(ready_notice(7))
            .await
            .expect("the shard holds the receiver");
        notices
            .send(book_notice(9, book_event(untouched, "10", None)))
            .await
            .expect("the shard holds the receiver");
        notices
            .send(book_notice(7, book_event(reinstalled, "77", Some(150))))
            .await
            .expect("the shard holds the receiver");

        shard.on_heartbeat_missed(Slot::Standby(0));

        assert!(
            !shard.pool_armed(),
            "the staged keyless frame withdrew the licence, which is what switches the rule"
        );
        assert_eq!(
            shard.markets[reinstalled].subscribed_from, None,
            "a dial that predates the market's incarnation claims no subscription for it"
        );
        assert_eq!(
            shard.markets[untouched].subscribed_from,
            Some(7),
            "and still claims the market it was actually given"
        );
        assert!(
            !shard.markets[reinstalled].base_accepted,
            "so the retired subscription's frame is no base for the new incarnation"
        );
        assert!(
            matches!(
                shard.markets[reinstalled].writer.book().authority(),
                AuthorityState::Synchronizing
            ),
            "the new incarnation is still owed a base: {:?}",
            shard.markets[reinstalled].writer.book().authority()
        );
        assert!(
            shard.stats.frames_uncarried >= 1,
            "the frame was answered as one no subscription this shard holds accounts for"
        );
    }

    /// A reissue whose connection cannot take it arms no timer at all.
    ///
    /// The channel is closed, which only a connection whose task is ending does, so the
    /// event that recovers this is that connection's end — already a wake the run loop is
    /// waiting on. Scheduling a retry instead would be the shard polling for news the join
    /// is about to deliver, and with a configured floor of zero that retry would be due the
    /// instant it was computed.
    #[tokio::test]
    async fn a_reissue_the_connection_cannot_take_arms_no_timer() {
        let mut shard = Shard::new(ShardConfig {
            markets: vec!["a-market".to_owned()],
            min_command_interval_ms: 0,
            ..ShardConfig::default()
        })
        .expect("a zero command floor is a valid operator choice");
        let markets = shard.desired_slugs();
        shard.primary.active = Some(ended_connection(1, markets).await);
        shard.recovering = 1;
        shard.reissue = Reissue::Pending(Instant::now() - Duration::from_millis(50));

        shard.on_reissue_due();

        assert_eq!(
            shard.reissue,
            Reissue::Unreachable,
            "a handover to a closed channel leaves no attempt in flight"
        );
        assert_eq!(
            shard.reissue.wake_at(),
            None,
            "nothing about a gone connection is worth waking for on a clock"
        );
        assert_eq!(
            shard.stats.subscriptions_emitted, 0,
            "a command that never left is not one the venue was sent"
        );
    }

    /// An unreachable reissue is left alone rather than re-armed, so the state cannot become
    /// a retry loop by way of the loop's own re-arming pass.
    #[tokio::test]
    async fn an_unreachable_reissue_is_not_re_armed() {
        let mut shard = Shard::new(ShardConfig {
            markets: vec!["a-market".to_owned()],
            min_command_interval_ms: 0,
            ..ShardConfig::default()
        })
        .expect("a zero command floor is a valid operator choice");
        let markets = shard.desired_slugs();
        shard.primary.active = Some(ended_connection(1, markets).await);
        shard.recovering = 1;
        shard.reissue = Reissue::Unreachable;

        shard.arm_reissue();

        assert_eq!(shard.reissue, Reissue::Unreachable);
        assert_eq!(shard.reissue.wake_at(), None);
    }

    #[test]
    fn a_repeated_slug_in_the_starting_set_is_one_market() {
        let shard = shard(&["a-market", "a-market"]);
        assert_eq!(shard.desired_count, 1);
        assert_eq!(shard.desired_slugs(), vec!["a-market".to_owned()]);
    }

    #[test]
    fn the_desired_set_is_emitted_in_slug_order() {
        let shard = shard(&["c-market", "a-market", "b-market"]);
        assert_eq!(
            shard.desired_slugs(),
            vec![
                "a-market".to_owned(),
                "b-market".to_owned(),
                "c-market".to_owned()
            ]
        );
    }

    #[test]
    fn adding_a_market_already_in_the_set_changes_no_desired_version() {
        let mut shard = shard(&["a-market"]);
        let version = shard.desired_version;
        let outcomes = shard.add_markets(vec!["a-market".to_owned()]);
        assert_eq!(shard.desired_version, version);
        assert_eq!(outcomes[0].status, MarketStatus::Reconciling);
    }

    #[test]
    fn removing_a_market_that_is_absent_changes_no_desired_version() {
        let mut shard = shard(&["a-market"]);
        let version = shard.desired_version;
        let outcomes = shard.remove_markets(vec!["other-market".to_owned()]);
        assert_eq!(shard.desired_version, version);
        assert_eq!(outcomes[0].status, MarketStatus::Removed);
        assert_eq!(shard.desired_count, 1);
    }

    #[test]
    fn a_market_past_the_capacity_is_rejected_and_the_rest_of_the_batch_is_not() {
        let mut shard = Shard::new(ShardConfig {
            markets: vec!["a-market".to_owned()],
            max_markets: 2,
            ..ShardConfig::default()
        })
        .expect("test shard configuration is valid");
        let outcomes = shard.add_markets(vec!["b-market".to_owned(), "c-market".to_owned()]);
        assert_eq!(outcomes[0].status, MarketStatus::Accepted);
        assert_eq!(
            outcomes[1].status,
            MarketStatus::Rejected(MarketRejection::CapacityExceeded)
        );
        assert_eq!(shard.desired_count, 2);
    }

    #[test]
    fn an_invalid_identifier_is_rejected_as_input() {
        let mut shard = shard(&[]);
        let outcomes = shard.add_markets(vec![String::new()]);
        assert_eq!(
            outcomes[0].status,
            MarketStatus::Rejected(MarketRejection::InvalidIdentifier)
        );
        assert_eq!(shard.desired_count, 0);
    }

    #[test]
    fn re_adding_a_market_the_venue_still_carries_cannot_cross_the_capacity() {
        let mut shard = Shard::new(ShardConfig {
            markets: vec!["a-market".to_owned(), "b-market".to_owned()],
            max_markets: 2,
            ..ShardConfig::default()
        })
        .expect("test shard configuration is valid");
        for entry in shard.markets.values_mut() {
            entry.dialled = true;
        }

        let _removed = shard.remove_markets(vec!["b-market".to_owned()]);
        assert_eq!(shard.desired_count, 1);
        assert!(
            shard.markets.contains_key("b-market"),
            "a market the venue still carries keeps its book until its connection ends"
        );
        let accepted = shard.add_markets(vec!["c-market".to_owned()]);
        assert_eq!(accepted[0].status, MarketStatus::Accepted);

        let refused = shard.add_markets(vec!["b-market".to_owned()]);
        assert_eq!(
            refused[0].status,
            MarketStatus::Rejected(MarketRejection::CapacityExceeded),
            "a market coming back is a new member of the set and is counted like one"
        );
        assert_eq!(
            shard.desired_count, 2,
            "the refusal left the desired set at its declared capacity"
        );
        assert!(
            !shard.markets["b-market"].desired,
            "a refused add leaves the removal exactly as it was"
        );
    }

    #[test]
    fn a_carried_removal_batch_past_the_tombstone_bound_evicts_the_oldest() {
        let slugs: Vec<String> = (0..MAX_REMOVING_MARKETS + 1)
            .map(|index| format!("market-{index:04}"))
            .collect();
        let mut shard = Shard::new(ShardConfig {
            markets: slugs.clone(),
            max_markets: MAX_REMOVING_MARKETS + 1,
            ..ShardConfig::default()
        })
        .expect("test shard configuration is valid");
        for entry in shard.markets.values_mut() {
            entry.dialled = true;
        }

        let removed = shard.remove_markets(slugs.clone());
        assert_eq!(removed.len(), MAX_REMOVING_MARKETS + 1);
        assert_eq!(shard.desired_count, 0);
        assert_eq!(
            shard.markets.len(),
            MAX_REMOVING_MARKETS,
            "more removals than the bound allows cost the oldest its book at once"
        );
        assert_eq!(shard.stats.tombstones_evicted, 1);
        assert!(
            !shard.markets.contains_key(slugs[0].as_str()),
            "the removal that has waited longest is the one evicted"
        );

        let back = shard.add_markets(vec![slugs[0].clone()]);
        assert_eq!(
            back[0].status,
            MarketStatus::Accepted,
            "a market whose tombstone was evicted is an ordinary new market when it returns"
        );
        assert_eq!(shard.desired_count, 1);
    }

    #[test]
    fn queue_age_percentiles_stay_within_the_observed_maximum() {
        let mut age = QueueAge::new();
        for micros in [0, 1, 5, 900, 12_000] {
            age.record(micros);
        }
        let summary = age.summary();
        assert_eq!(summary.samples, 5);
        assert_eq!(summary.max_micros, 12_000);
        assert_eq!(summary.last_micros, 12_000);
        assert!(summary.p50_micros <= summary.p99_micros);
        assert!(summary.p99_micros <= summary.max_micros);
        assert!(summary.p50_micros >= 5);
    }

    #[test]
    fn a_percentile_in_the_overflow_bucket_reports_the_observed_maximum() {
        let mut age = QueueAge::new();
        age.record(30_000_000);
        let summary = age.summary();
        assert_eq!(
            summary.p99_micros, 30_000_000,
            "a thirty-second backlog must not be reported as the top bucket's bound"
        );
        assert_eq!(summary.p50_micros, 30_000_000);
        assert_eq!(summary.max_micros, 30_000_000);
    }

    #[test]
    fn an_unmeasured_queue_reports_no_samples_rather_than_zero_latency() {
        let summary = QueueAge::new().summary();
        assert_eq!(summary.samples, 0);
        assert_eq!(summary.max_micros, 0);
        assert_eq!(summary.p99_micros, 0);
    }

    /// The 99.9th percentile separates itself from the 99th when a thousandth of the samples
    /// is slow, which is the whole reason the publish-latency summary carries it.
    ///
    /// Two slow samples in a thousand are past the 99th percentile's rank and inside the
    /// 99.9th's, so a summary reporting the same figure for both would be reporting a tail it
    /// cannot see. Ranks are nearest-rank over the buckets: the p99.9 of a thousand samples
    /// is the 999th of them, which is why one slow sample in a thousand is *not* enough to
    /// move it and two are.
    #[test]
    fn the_publish_latency_summary_reports_a_tail_the_p99_hides() {
        let mut histogram = QueueAge::new();
        for _fast in 0..998 {
            histogram.record(3);
        }
        histogram.record(500_000);
        histogram.record(500_000);

        let summary = histogram.publish_latency_summary();
        assert_eq!(summary.samples, 1_000);
        assert_eq!(summary.last_micros, 500_000);
        assert_eq!(summary.max_micros, 500_000);
        assert!(
            summary.p50_micros <= summary.p99_micros,
            "the percentiles are ordered: {summary:?}"
        );
        assert!(
            summary.p99_micros < summary.p999_micros,
            "one slow sample in a thousand is past the p99 and inside the p99.9: {summary:?}"
        );
        assert_eq!(
            summary.p999_micros, 500_000,
            "the p99.9 of a thousand samples lands in the slow tail: {summary:?}"
        );
        assert_eq!(
            summary.p99_micros, 4,
            "and the p99 lands in the fast bucket, reported as that bucket's upper bound: \
             {summary:?}"
        );
    }

    #[test]
    fn an_unmeasured_publish_latency_reports_no_samples_rather_than_zero_latency() {
        let summary = QueueAge::new().publish_latency_summary();
        assert_eq!(summary.samples, 0);
        assert_eq!(summary.p999_micros, 0);
        assert!(
            summary.is_unmeasured(),
            "a summary nothing was recorded into is the one a status answer omits"
        );
    }

    fn measurement_segment(slug: &str) -> (ShardSegment, PathBuf, PublishedBook) {
        let layout = SegmentLayout::new(2, 2, 8, 8, 16).expect("the test layout is valid");
        let path = PathBuf::from(format!(
            "/tmp/pmws-publish-latency-{}-{slug}.seg",
            std::process::id()
        ));
        let _removed = std::fs::remove_file(path.as_path());
        let region = Arc::new(
            SegmentRegion::create_file(path.as_path(), layout.region_size())
                .expect("the test region is created"),
        );
        let writer = SegmentWriter::create(region, SegmentConfig::new(layout, 1, 1))
            .expect("the test segment is formatted");
        let market = MarketRef::new(
            Venue::new(VENUE).expect("the venue is valid"),
            NativeMarketKey::new(NativeIdentifierKind::slug(), slug).expect("the slug is valid"),
        );
        let published = OrderBook::new(market).publish();
        let mut segment = ShardSegment::new("measured.seg".to_owned(), writer);
        assert!(
            segment.install(slug, &published).is_ok(),
            "the market is seated"
        );
        (segment, path, published)
    }

    fn now_nanos() -> u64 {
        u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("this host's clock is past the epoch")
                .as_nanos(),
        )
        .unwrap_or(u64::MAX)
    }

    /// A publication driven by a venue frame is measured; one that was not, is not.
    ///
    /// The two cases are the whole recording rule. A publication handed an arrival records
    /// exactly one sample, of what that publication actually cost. The install's own first
    /// publication and any later one handed no arrival record nothing, because nothing
    /// arrived to measure from.
    #[test]
    fn only_a_publication_a_frame_drove_is_measured() {
        let slug = "btc-up-or-down-5-min-1788172500";
        let (mut segment, path, published) = measurement_segment(slug);
        assert_eq!(
            segment.publish_latency().samples,
            0,
            "the state published at install is handed no arrival and is not evidence"
        );

        segment.publish_state(
            slug,
            &published,
            Some(FrameArrival::new(Instant::now(), now_nanos())),
        );
        let measured = segment.publish_latency();
        assert_eq!(
            measured.samples, 1,
            "the publication a frame drove is measured"
        );
        assert!(
            measured.max_micros < 1_000_000,
            "the sample is what this publication cost, not an interval since the epoch: \
             {measured:?}"
        );
        assert_eq!(measured.last_micros, measured.max_micros);

        segment.publish_state(slug, &published, None);
        assert_eq!(
            segment.publish_latency().samples,
            1,
            "a publication no venue frame drove is not latency evidence"
        );

        assert!(
            segment.failure().is_none(),
            "nothing here refused a publication"
        );
        let _removed = std::fs::remove_file(path.as_path());
    }

    /// The sample is the monotonic interval this publication took, not the difference of the
    /// wall-clock stamps it wrote into the slot.
    ///
    /// The two are pulled sixty seconds apart on purpose. A recording that read the slot's
    /// `(commit, arrival)` pair back would report roughly sixty million microseconds here;
    /// one measuring `arrival.monotonic` to the instant the writer returned reports what the
    /// publication cost, which on any host is orders of magnitude under the bound asserted.
    /// The slot itself still carries the planted stamp verbatim — it is the cross-process
    /// cell, and nothing about the histogram touches it.
    #[test]
    fn the_sample_is_the_monotonic_interval_not_the_slot_stamps() {
        let slug = "btc-up-or-down-5-min-1788172800";
        let (mut segment, path, published) = measurement_segment(slug);
        let planted = now_nanos().saturating_sub(60_000_000_000);

        segment.publish_state(
            slug,
            &published,
            Some(FrameArrival::new(Instant::now(), planted)),
        );

        let measured = segment.publish_latency();
        assert_eq!(
            measured.samples, 1,
            "the publication a frame drove is measured"
        );
        assert!(
            measured.last_micros < 1_000_000,
            "a sixty-second-old wall-clock arrival stamp is not this publication's cost: \
             {measured:?}"
        );
        let handle = segment.handle(slug).expect("the market is seated");
        assert_eq!(
            segment
                .writer
                .published_stamps(handle)
                .map(|stamps| stamps.1),
            Some(planted),
            "the slot still advertises the wall-clock arrival a consumer reads"
        );
        assert!(
            segment.failure().is_none(),
            "nothing here refused a publication"
        );
        let _removed = std::fs::remove_file(path.as_path());
    }

    /// A wall clock that steps forward between the socket read and the publication costs the
    /// distribution nothing.
    ///
    /// The arrival stamp is planted a minute in the future, which is what a forward step
    /// looks like from the commit stamp's side: the slot's own pair inverts. The monotonic
    /// reading cannot invert, so the sample is still taken — where the read-back rule dropped
    /// it as "commit before arrival" and left the operator a distribution missing exactly the
    /// samples a clock correction touched.
    #[test]
    fn a_wall_clock_step_between_arrival_and_publication_does_not_drop_the_sample() {
        let slug = "btc-up-or-down-5-min-1788173100";
        let (mut segment, path, published) = measurement_segment(slug);

        segment.publish_state(
            slug,
            &published,
            Some(FrameArrival::new(
                Instant::now(),
                now_nanos().saturating_add(60_000_000_000),
            )),
        );

        let measured = segment.publish_latency();
        assert_eq!(
            measured.samples, 1,
            "a slot pair a clock step inverted is still one measured publication: {measured:?}"
        );
        assert!(
            measured.last_micros < 1_000_000,
            "and the sample is the publication's own cost: {measured:?}"
        );
        assert!(
            segment.failure().is_none(),
            "nothing here refused a publication"
        );
        let _removed = std::fs::remove_file(path.as_path());
    }

    /// A rank past what a saturating product can express still selects the slowest bucket.
    ///
    /// The counts are the shape that catches it: nearly every sample sits in the overflow
    /// bucket, so the true p99.9 is the observed maximum, while the earlier bucket holds
    /// slightly more than the rank a saturated `u64` product yields. Computed in `u64` the
    /// answer is that earlier bucket's eight-microsecond bound — a daemon reporting an
    /// eight-microsecond tail while its samples sat in the seconds.
    #[test]
    fn a_rank_no_u64_product_can_express_still_reports_the_slowest_bucket() {
        let mut histogram = QueueAge::new();
        histogram.buckets[3] = 20_000_000_000_000_000;
        histogram.buckets[QUEUE_AGE_BUCKETS - 1] = u64::MAX - 20_000_000_000_000_000;
        histogram.samples = u64::MAX;
        histogram.max_micros = 5_000_000;
        histogram.last_micros = 5_000_000;

        let summary = histogram.publish_latency_summary();
        assert_eq!(
            summary.p999_micros, 5_000_000,
            "the p99.9 of a histogram whose tail is the overflow bucket is its maximum: \
             {summary:?}"
        );
        assert_eq!(
            summary.p99_micros, 5_000_000,
            "and so is the p99, at these counts: {summary:?}"
        );
    }

    /// A refused bound names the key it refused and the bound it enforces.
    ///
    /// These two are operator configuration, so their refusals reach an operator reading a
    /// startup failure. A message that said only "invalid shard configuration" would leave
    /// that operator to find which of a dozen keys was wrong by bisection.
    #[test]
    fn an_over_bound_key_is_refused_by_name_and_by_bound() {
        let budget = Shard::new(ShardConfig {
            markets: vec!["a-market".to_owned()],
            daily_attempt_budget: WORST_CASE_ATTEMPT_CEILING + 1,
            ..ShardConfig::default()
        })
        .err()
        .expect("a budget past the ledger's ceiling is refused");
        assert_eq!(budget, ShardError::DailyAttemptBudgetTooLarge);
        assert_eq!(
            budget.to_string(),
            "daily_attempt_budget must be at most 1000000"
        );

        let interval = Shard::new(ShardConfig {
            markets: vec!["a-market".to_owned()],
            min_command_interval_ms: MAX_COMMAND_INTERVAL_MS + 1,
            ..ShardConfig::default()
        })
        .err()
        .expect("a floor indistinguishable from an outage is refused");
        assert_eq!(interval, ShardError::CommandIntervalTooLarge);
        assert_eq!(
            interval.to_string(),
            "min_command_interval_ms must be at most 60000"
        );

        assert_eq!(
            ShardError::IngestCapacityZero.to_string(),
            "invalid shard configuration",
            "a variant with no bound of its own keeps the general sentence"
        );
    }
}

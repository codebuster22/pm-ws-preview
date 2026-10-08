//! The `pmwsd` daemon's configuration document and the shard plan it produces.
//!
//! One TOML file declares the venue endpoint, the operator-pinned market set, the local
//! control socket, and every hard capacity this deployment runs under, as `docs/design.md`
//! "Configuration and hard limits" requires. Nothing is inferred from the environment and
//! nothing is read from a database: the daemon is told what to subscribe to.
//!
//! Validation is total and typed. Every field is checked before a socket is opened or a
//! connection is dialled, and every rejection names the field and the reason, so a
//! misconfigured deployment fails at startup with a sentence rather than at the venue with a
//! disconnect.
//!
//! There is deliberately no diagnostic logging knob here. The daemon runs its shards and its
//! control listener on one runtime, so a per-command print is a blocking write on the thread
//! that also drives ingestion and liveness — a stalled pipe would stop the feed. `pmwsctl
//! status` is the operator surface.

use crate::limitless::connection::DEFAULT_ENDPOINT;
use crate::limitless::shard::{MAX_SHARD_MARKETS, ShardConfig};
use crate::limitless::supervisor::{
    MAX_COMMAND_INTERVAL_MS, MAX_REPLICAS, WORST_CASE_ATTEMPT_CEILING,
};
use crate::{IdentityError, LayoutError, NativeIdentifierKind, NativeMarketKey, SegmentLayout};
use core::fmt;
use core::time::Duration;
use serde::Deserialize;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

/// The default number of markets one shard carries: the [`DeliveryProfile::Common`] tuple's
/// market capacity.
///
/// A shard now provisions one delivery slot per market at the declared book depth, so this
/// number is a memory commitment rather than only a routing bound. The common profile is
/// declared for the sub-100-market deployment the design document names; a larger set says so
/// with `markets_per_shard`, or takes a profile that declares a shallower book.
pub const DEFAULT_MARKETS_PER_SHARD: usize = DeliveryProfile::Common.markets_per_shard();

/// The fraction of a shard's market capacity its segment carries as spare directory
/// entries, as a divisor.
///
/// A segment binds a market to one directory entry and one state slot for the life of its
/// generation and never reuses either — reusing one would restart a stream's positions
/// inside a ring an attached consumer is still reading — so every removal and re-add spends
/// an entry. This is how many such spends a run absorbs before an add is refused with
/// [`crate::limitless::shard::MarketRejection::DeliveryUnavailable`]. It is deliberately
/// modest: churn headroom is not a substitute for a segment sized for the deployment.
const SEGMENT_CHURN_DIVISOR: usize = 4;

/// The most bytes a control socket path may occupy.
///
/// A Unix domain socket address is a fixed-size `sun_path`: 104 bytes on macOS and 108 on
/// Linux, including the terminator. The stricter platform sets the limit here so a path that
/// validates on one host binds on both, and so an over-long path is a named configuration
/// error rather than an unexplained `bind` failure.
pub const MAX_CONTROL_SOCKET_PATH_BYTES: usize = 100;

/// The longest lease TTL a configuration may declare, in milliseconds: one year.
///
/// A TTL is not a number the daemon stores, it is a deadline it computes from a monotonic
/// clock for every open session and then sleeps on. A value far beyond anything a clock is
/// asked to represent is not a longer deadline; it is arithmetic no platform promises to
/// answer, and a daemon that accepted one would carry that hazard into the sweep path on
/// every request a consumer completes. One year is already past any deployment's meaning of
/// "this consumer is wedged" — the ordinary lifetime of a lease is its connection — so the
/// ceiling refuses nothing a real configuration wanted, and refuses it at startup with the
/// key named rather than at the first session that reaches it.
pub const MAX_LEASE_TTL_MS: u64 = 365 * 24 * 60 * 60 * 1_000;

/// The shortest lease TTL a configuration may declare, in milliseconds, other than `0` for no
/// expiry at all: one second.
///
/// A TTL is only meaningful if a healthy consumer can renew inside it, and a consumer renews
/// inside a *fraction* of it — the daemon measures silence from the request it last answered,
/// so a renewal sent at the deadline has already lost the race to a sweep that is due. The
/// daemon promises this TTL to every attaching consumer
/// ([`crate::Attachment::lease_ttl_ms`]), so this floor is what makes the fraction a consumer
/// derives from it a schedulable interval rather than a busy loop: a third of one second is a
/// renewal every ~333 ms on a local socket, which costs one line and is far above any
/// scheduler's granularity. Below the floor the only honest answers are a consumer that
/// renews continuously or one that loses markets it is still reading, so the value is refused
/// at startup with the key named, exactly as [`MAX_LEASE_TTL_MS`] is.
///
/// `0` stays accepted and means what it always meant: no TTL, no expiry, and no renewal owed.
pub const MIN_LEASE_TTL_MS: u64 = 1_000;

const MIN_CAPACITY: usize = 1;
const MAX_QUEUE_CAPACITY: usize = 1_048_576;

/// What one shard holds open at its peak whatever its topology: three descriptors.
///
/// Two of them are the segment, and both are retained for the daemon's life. The exclusive
/// create returns one, which the region keeps as the object every later identity check is
/// anchored to; the read-only reopen returns the other, which every attach duplicates into
/// its consumer rather than re-opening a mutable name. The third is the sibling doorbell
/// page, a segment of its own with its own retained creation object — counted whether or not
/// this build takes that path, because the platform decides at runtime and a preflight that
/// undercounted on the platforms that do take it would be worse than useless.
///
/// None of the three depends on how many venue connections a shard runs: a shard publishes
/// into one segment however many sockets feed it, which is the same fact as one physical
/// book per market.
const DESCRIPTORS_PER_SHARD: usize = 3;

/// What one of a shard's connection roles holds open at its peak: two venue sockets, the
/// live connection the role runs and one fenced connection still draining while its
/// replacement dials.
///
/// The second is a peak rather than a steady state — a role that never replaces a connection
/// never opens it, and a role never holds two draining at once.
const DESCRIPTORS_PER_CONNECTION_ROLE: usize = 2;

/// What a daemon holds open regardless of how many shards it runs, before any slack.
///
/// The control socket and the lock beside it, plus the three standard streams every process
/// is started with.
const DESCRIPTORS_BASE: usize = 5;

/// Descriptors reserved for the async runtime's own machinery and for anything the process
/// was started holding.
///
/// A stated allowance, not a measurement: the runtime's wakers, an inherited descriptor, a
/// resolver handle. It is here so that a preflight which passes leaves room for the things
/// this arithmetic does not enumerate, rather than clearing the limit by exactly nothing.
const DESCRIPTOR_SLACK: usize = 16;

/// The most open file descriptors a daemon running `shards` shards at `replicas`
/// connection roles each holds at its peak.
///
/// `metrics_listener` counts the bound listener a configured `metrics_listen` holds for the
/// daemon's life; a daemon without one pays nothing for it.
///
/// `replicas` is what makes redundancy visible to the preflight: every role a shard adds is
/// two more sockets at the peak, so a daemon that would exhaust its descriptor limit with
/// standbys enabled is refused before it dials anything rather than after.
///
/// This is arithmetic about how a daemon is built, not a limit anyone declared. It is the
/// resource envelope `docs/design.md` requires a daemon to calculate before accepting
/// production traffic: `pmwsd` checks it against the process descriptor limit at startup,
/// before a segment is created or a venue is dialled, and the fifty-thousand-market envelope
/// test in this module sizes its own declared deployment shape with it, so the two can never
/// drift apart.
///
/// Two costs are deliberately outside it, because both are answers to demand rather than
/// properties of the configuration: an accepted control session holds a descriptor until it
/// closes, and so does a metrics scrape being served. Neither is bounded by the shard count,
/// and budgeting the control session cap into a startup preflight would refuse
/// configurations that will never see that many clients. An operator running many concurrent
/// consumers sizes the limit for them on top of this figure.
pub const fn descriptor_envelope(shards: usize, replicas: usize, metrics_listener: bool) -> usize {
    let listener = if metrics_listener { 1 } else { 0 };
    let sockets = replicas.saturating_mul(DESCRIPTORS_PER_CONNECTION_ROLE);
    shards
        .saturating_mul(DESCRIPTORS_PER_SHARD.saturating_add(sockets))
        .saturating_add(DESCRIPTORS_BASE)
        .saturating_add(DESCRIPTOR_SLACK)
        .saturating_add(listener)
}

/// The daemon's configuration document, exactly as the TOML file spells it.
///
/// Unknown keys are rejected: a typo in a capacity is a misconfiguration to report, never a
/// default to fall back on silently.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DaemonConfig {
    /// The venue's WebSocket endpoint. Overridable so the daemon can be pointed at a
    /// controlled peer; it must still be a `ws://` or `wss://` URL.
    #[serde(default = "default_endpoint")]
    pub endpoint: String,
    /// Where the daemon listens for `pmwsctl`. Absolute, on an existing directory.
    pub control_socket: PathBuf,
    /// The operator-pinned desired market set. May be empty, in which case the daemon starts
    /// with no venue connection and waits for a command.
    #[serde(default)]
    pub markets: Vec<String>,
    /// How many markets one shard carries. The market set is partitioned into shards of at
    /// most this size, and the shard count that partition produces is `ceil(markets /
    /// markets_per_shard)` with no ceiling of its own: a larger market list simply produces
    /// more shards, each dialling its own venue connection. [`Self::max_venue_connections`],
    /// when set, refuses a document whose resulting shard count would need more concurrent
    /// sockets than the operator declared.
    ///
    /// Absent, the [`delivery`](Self::delivery) profile's own market capacity is used, which
    /// for the default [`DeliveryProfile::Common`] is 128 — down from the 512 this key
    /// defaulted to before a shard provisioned a delivery slot per market. A document listing
    /// more markets than that without setting this key gets more shards, not a refusal;
    /// [`MAX_SHARD_MARKETS`] is the only per-shard ceiling this key runs into, and the
    /// segment must have slots for whatever this key names — or the document takes
    /// `delivery.profile = "scale"`, whose shallower declared book depth is what makes a
    /// larger per-shard count fit one segment region.
    #[serde(default)]
    pub markets_per_shard: Option<usize>,
    /// How many venue connections every shard runs: 1 — the default, and the daemon's
    /// behavior in full before this key existed — for one publishing connection per shard,
    /// and one hot standby per replica beyond that.
    ///
    /// Every shard of one daemon runs the same count, because a shard is a partition of one
    /// market set rather than a tier of its own. Every connection subscribes its shard's
    /// whole desired set.
    ///
    /// What the connections beyond the first do follows the venue's own key declaration.
    /// Where that declaration admits pooled publishing — which Limitless's does, on the
    /// conformance basis recorded in `docs/limitless.md` — they are a pool: each shard
    /// publishes the first arrival whose venue key passes a market's last published key,
    /// whichever connection carried it, and losing one costs coverage rather than asking a
    /// promotion question. Where it does not, and for the rest of the process after a
    /// shard's live tripwire fires, one connection publishes while the others feed
    /// per-market shadow state no consumer reads, and losing the publishing one is answered
    /// per market with evidence rather than with a recovery cycle for the whole set. Which
    /// of the two a shard is in is on its status line and its scrape. See
    /// `limitless::shard::ShardConfig::replicas`.
    ///
    /// Raising it multiplies this daemon's venue sockets: a shard peaks at `2 * replicas`
    /// of them, which is what [`Self::max_venue_connections`] and the startup descriptor
    /// preflight both budget against. It does not multiply the connection attempts spent
    /// per day, because each role's reconnect ladder is paced by the count.
    ///
    /// Bounded by `limitless::supervisor::MAX_REPLICAS`, the same ceiling the single-market
    /// rail honors.
    #[serde(default = "default_replicas")]
    pub replicas: usize,
    /// The most concurrent venue sockets this daemon may hold, or `None` for no cap.
    ///
    /// Each of a shard's [`replicas`](Self::replicas) connection roles holds at most one
    /// live connection plus at most one fenced connection still draining while the previous
    /// one closes, so `n` shards peak at `2 * replicas * n` sockets — this is arithmetic
    /// about how a shard drains, not a policy anyone declared. Absent, the default, shard
    /// count follows the market list with no connection-count refusal at all: this daemon
    /// aims to be the go-to connector for a venue's whole open market list, and an
    /// artificial cap here would refuse exactly the deployment that goal describes. Present,
    /// a document whose peak socket count exceeds this value is refused at startup with
    /// [`ConfigError::ShardsExceedConnectionCap`], naming the shard count, the peak sockets
    /// it implies, and the cap. `0` is refused outright: a daemon declared to hold no
    /// connection at all is a misconfiguration, not a valid cap.
    #[serde(default)]
    pub max_venue_connections: Option<usize>,
    /// The mutation-ring capacity given to every market's book, multiplied by the market
    /// count. Absent, the delivery profile's own value is used.
    #[serde(default)]
    pub observer_capacity: Option<usize>,
    /// The deepest book a shard will accept, in levels per snapshot.
    ///
    /// This is what the book itself accepts: a venue snapshot deeper than this is refused as
    /// an invalid candidate and costs that market its authority, so it is market policy and
    /// not a tuning knob. Absent, the delivery profile's own declared depth is used, and
    /// every profile but [`DeliveryProfile::Scale`] declares the full
    /// `limitless::supervisor::MAX_BOOK_LEVELS`. A segment's state slots are sized from this
    /// same number, so no venue book this shard accepts can be refused by its segment.
    #[serde(default)]
    pub level_capacity: Option<usize>,
    /// Each shard's bounded ingest queue.
    #[serde(default = "default_ingest_capacity")]
    pub ingest_capacity: usize,
    /// Each shard's bounded control queue.
    #[serde(default = "default_control_capacity")]
    pub control_capacity: usize,
    /// The generation stamped into every book's provenance for this run.
    #[serde(default = "default_daemon_generation")]
    pub daemon_generation: u64,
    /// How long a control session may hold its market leases without saying anything, in
    /// milliseconds. `0`, the default, is no time limit at all.
    ///
    /// A lease's ordinary lifetime is its connection: the operating system reports a closed
    /// socket whether the consumer exited or crashed, and the daemon releases that session's
    /// leases when it does. This covers the one case that reporting cannot — a peer whose
    /// socket is open and whose process is wedged — and it is off by default because a
    /// deadline is a guess about how long a healthy consumer may be silent, and a wrong guess
    /// unsubscribes a market a live consumer is still reading. Any request on the session
    /// renews it, including [`crate::ControlRequest::Renew`], which exists to be sent by a
    /// consumer with nothing else to say.
    ///
    /// `0`, or between [`MIN_LEASE_TTL_MS`] and [`MAX_LEASE_TTL_MS`]. A document naming more
    /// is refused with [`ConfigError::LeaseTtl`] and one naming a nonzero value below the
    /// floor with [`ConfigError::LeaseTtlTooShort`]: a TTL no consumer can renew inside would
    /// expire the leases of consumers that are doing everything right, and the daemon
    /// promises this value to each of them ([`crate::Attachment::lease_ttl_ms`]).
    #[serde(default)]
    pub lease_ttl_ms: u64,
    /// Where the daemon serves its Prometheus text-format metrics endpoint, as
    /// `host:port` with a numeric host: `127.0.0.1:9090`, or `[::1]:9090`.
    ///
    /// Absent, the default, nothing is bound and the daemon does no metrics work at all. A
    /// host name is not accepted: resolving one is blocking I/O on the thread that also
    /// drives ingestion, so a document naming one is refused with
    /// [`ConfigError::MetricsListen`] rather than resolved at startup. Port `0` binds an
    /// operating-system-chosen port, which the daemon then reports through
    /// [`crate::DaemonStatus::metrics_listen`].
    ///
    /// The endpoint is unauthenticated and exposes counters, not market data, so an address
    /// reachable off the host publishes this daemon's operational state to whoever asks:
    /// bind it to a loopback address unless something else authorizes the reader.
    #[serde(default)]
    pub metrics_listen: Option<String>,
    /// Where each shard's consumer-delivery segment lives and how large it is.
    #[serde(default)]
    pub delivery: DeliveryConfig,
    /// The most connection attempts this daemon will spend in a day, shared across every
    /// shard through the one process-wide attempt ledger.
    ///
    /// No retrieved venue documentation places a daily attempt ceiling: this is a
    /// conservative default an operator can raise or lower, not a fact enforced on this
    /// daemon's behalf. Absent, defaults to
    /// [`crate::limitless::supervisor::DAILY_ATTEMPT_BUDGET`]'s value.
    ///
    /// Bounded above by [`WORST_CASE_ATTEMPT_CEILING`], which is structure rather than
    /// policy: the ledger holds one instant per admitted attempt, so the budget an operator
    /// names is also the storage that ledger is asked to keep.
    #[serde(default = "default_daily_connection_attempt_budget")]
    pub daily_connection_attempt_budget: u64,
    /// The floor, in milliseconds, between two subscription-bearing commands any shard of
    /// this daemon puts on the wire toward `endpoint`.
    ///
    /// No retrieved venue documentation places a sustained-command ceiling: this is a
    /// conservative default for a polite wire citizen, not a fact enforced on this daemon's
    /// behalf. Every shard reads this same value, which is what keeps the process-wide
    /// pacer coherent across a whole daemon's shards. `0` is accepted and means no pacing
    /// at all: the pacer's own arithmetic (grant at `max(last, now)`) handles it without a
    /// special case. Absent, defaults to
    /// [`crate::limitless::supervisor::MIN_COMMAND_INTERVAL`]'s value.
    ///
    /// Bounded above by [`MAX_COMMAND_INTERVAL_MS`], which is structure rather than policy:
    /// every recovery and reissue deadline is the configured window plus this floor, so a
    /// floor past a minute would make this daemon's own pacing indistinguishable from an
    /// unanswering venue.
    #[serde(default = "default_min_command_interval_ms")]
    pub min_command_interval_ms: u64,
    /// How many control connections the daemon accepts at once — operator conversations and
    /// consumer sessions together.
    ///
    /// A session is one open connection held for as long as its consumer wants its leases, so
    /// this is what bounds the daemon's control-side memory and the work one sweep does. A
    /// connection past it is answered with a typed refusal and closed rather than queued: a
    /// queue of connections nothing is serving is a wait with no bound on it. It bounds
    /// accepted sessions only — the descriptor preflight
    /// ([`crate::descriptor_envelope`]) does not reserve descriptors against this key, so an
    /// operator raising it well past the default is also raising the process's own descriptor
    /// pressure.
    ///
    /// Absent, defaults to `256`. Refused outright at `0` and past the same generic capacity
    /// ceiling `observer_capacity`, `ingest_capacity`, and `control_capacity` share, naming
    /// the key and the ceiling: no field of this shape carries a bound of its own here.
    #[serde(default = "default_max_control_sessions")]
    pub max_control_sessions: usize,
}

/// The shared-memory delivery surface: where the segments live, and the capacities they are
/// created with.
///
/// Every capacity here is fixed for the life of a segment — the geometry is in its header
/// and a consumer maps a region sized from it — so all of it is startup configuration and
/// none of it changes at runtime.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DeliveryConfig {
    /// The directory each shard's segment file is created in. Absolute, and already
    /// existing.
    ///
    /// Absent, the control socket's own directory: a deployment that has somewhere to put a
    /// control socket has somewhere to put the segments that socket hands out. The directory
    /// is never created here, for the reason the control socket's is not — a daemon that
    /// creates directories named by a configuration file is a daemon that can be told to
    /// create one anywhere.
    #[serde(default)]
    pub directory: Option<PathBuf>,
    /// Which capacity tuple the unset keys take their values from.
    #[serde(default)]
    pub profile: DeliveryProfile,
    /// How many markets one shard's segment has directory entries and state slots for.
    ///
    /// At least `markets_per_shard`; the surplus is churn headroom, because an entry is
    /// never reused. Absent, the larger of the profile's own figure and `markets_per_shard`
    /// plus a [`SEGMENT_CHURN_DIVISOR`] share of it.
    #[serde(default)]
    pub segment_slots: Option<usize>,
    /// How many deliveries each market's event ring retains. A power of two.
    #[serde(default)]
    pub event_capacity: Option<usize>,
    /// How many entries the segment's one dirty index retains. A power of two.
    #[serde(default)]
    pub dirty_capacity: Option<usize>,
}

/// One named point in the trade between market count, book depth, and retained history.
///
/// A segment's region is capped ([`crate::MAX_REGION_BYTES`]) and every market in it costs a
/// directory entry, a state slot sized by the declared book depth, and an event ring, so the
/// three cannot all be large at once. Each profile names one honest point rather than
/// leaving an operator to discover the ceiling by hitting it; every key it sets can be
/// overridden on its own.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryProfile {
    /// The sub-100-market deployment the design document names as the common case: full book
    /// depth, generous retention, a segment of a few tens of mebibytes.
    #[default]
    Common,
    /// A handful of markets that matter, with the deepest retention: a consumer may fall
    /// thousands of deliveries behind and still be told the truth rather than an overrun.
    Hot,
    /// The 50,000-market envelope. It buys market count with **declared book depth**: a
    /// venue snapshot deeper than [`Self::level_capacity`] is refused as an invalid
    /// candidate and its market goes stale, which is why the depth is part of the profile an
    /// operator names rather than a default anything inherits. Full depth at this market
    /// count does not fit a segment region and never will; `the_fifty_thousand_market_envelope`
    /// does that arithmetic.
    Scale,
}

impl DeliveryProfile {
    /// How many markets one shard carries under this profile.
    pub const fn markets_per_shard(self) -> usize {
        match self {
            Self::Common => 128,
            Self::Hot => 64,
            Self::Scale => 25_000,
        }
    }

    /// The deepest book a shard accepts under this profile, in levels per snapshot.
    pub const fn level_capacity(self) -> usize {
        match self {
            Self::Common | Self::Hot => crate::limitless::supervisor::MAX_BOOK_LEVELS,
            Self::Scale => 256,
        }
    }

    /// The in-process mutation-ring capacity given to every book under this profile.
    pub const fn observer_capacity(self) -> usize {
        match self {
            Self::Common => 1_024,
            Self::Hot => 4_096,
            Self::Scale => 64,
        }
    }

    /// The directory and state-slot capacity of one shard's segment under this profile.
    pub const fn segment_slots(self) -> usize {
        match self {
            Self::Common => 160,
            Self::Hot => 80,
            Self::Scale => 32_768,
        }
    }

    /// How many deliveries each market's event ring retains under this profile.
    pub const fn event_capacity(self) -> usize {
        match self {
            Self::Common => 256,
            Self::Hot => 4_096,
            Self::Scale => 32,
        }
    }

    /// How many entries the segment's one dirty index retains under this profile.
    pub const fn dirty_capacity(self) -> usize {
        match self {
            Self::Common => 4_096,
            Self::Hot => 16_384,
            Self::Scale => 65_536,
        }
    }
}

fn default_endpoint() -> String {
    DEFAULT_ENDPOINT.to_owned()
}
const fn default_ingest_capacity() -> usize {
    1024
}
const fn default_control_capacity() -> usize {
    64
}
const fn default_replicas() -> usize {
    1
}
const fn default_daemon_generation() -> u64 {
    1
}
fn default_daily_connection_attempt_budget() -> u64 {
    crate::limitless::supervisor::DAILY_ATTEMPT_BUDGET
}
fn default_min_command_interval_ms() -> u64 {
    u64::try_from(crate::limitless::supervisor::MIN_COMMAND_INTERVAL.as_millis())
        .unwrap_or(u64::MAX)
}
const fn default_max_control_sessions() -> usize {
    256
}

/// Why a configuration was refused.
///
/// Every variant names the field and what was wrong with it. None of them is recoverable at
/// runtime: the daemon refuses to start rather than running under a configuration it had to
/// guess at.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigError {
    /// The file could not be read.
    Read { path: PathBuf, message: String },
    /// The file is not the TOML document this daemon accepts — malformed, missing a required
    /// key, or carrying a key this daemon does not define.
    Parse(String),
    /// `endpoint` is empty or is not a WebSocket URL.
    Endpoint,
    /// A market slug is not a valid venue-native identifier.
    Market { slug: String, reason: IdentityError },
    /// The same market appears twice. A desired set is a set.
    DuplicateMarket { slug: String },
    /// `markets_per_shard` is zero or above the shard's own market ceiling.
    MarketsPerShard { limit: usize },
    /// The market set needs more shards, and therefore more concurrent venue sockets at
    /// their `2n` peak, than `max_venue_connections` declares.
    ShardsExceedConnectionCap {
        shards: usize,
        peak_sockets: usize,
        cap: usize,
    },
    /// `daily_connection_attempt_budget` is past the ceiling the process-wide attempt
    /// ledger's storage is sized against.
    DailyAttemptBudget { budget: u64, ceiling: u64 },
    /// `min_command_interval_ms` is past the structural maximum a recovery deadline built
    /// from a pacer slot spaced by the floor is computed within.
    CommandInterval { interval_ms: u64, ceiling: u64 },
    /// `lease_ttl_ms` is past the ceiling a lease deadline is computed within.
    LeaseTtl {
        key: &'static str,
        ttl_ms: u64,
        ceiling: u64,
    },
    /// `lease_ttl_ms` is below the floor a consumer can renew inside.
    LeaseTtlTooShort {
        key: &'static str,
        ttl_ms: u64,
        floor: u64,
    },
    /// `metrics_listen` is not an address a listener can bind: a numeric host and a port.
    MetricsListen { value: String },
    /// `control_socket` is empty, relative, or names a directory that does not exist.
    ControlSocket { message: String },
    /// `control_socket` is longer than a Unix domain socket address can carry.
    ControlSocketTooLong { bytes: usize, limit: usize },
    /// A declared capacity is outside the range its surface accepts.
    Capacity { field: &'static str, limit: usize },
    /// `replicas` is zero — a shard publishing from no connection — or above the ladder
    /// depth this tool supports.
    Replicas { limit: usize },
    /// `delivery.segment_slots` is below the market capacity it must carry. A market with no
    /// slot could never be delivered, so it is refused as configuration rather than at the
    /// first add.
    SegmentSlots {
        slots: usize,
        markets_per_shard: usize,
    },
    /// The delivery keys imply a segment geometry no segment can have — most often a region
    /// larger than one may be allocated at, which is the market count, the declared book
    /// depth and the retained history competing for one bounded region.
    SegmentGeometry(LayoutError),
    /// `delivery.directory` is relative, or names something that is not an existing
    /// directory.
    SegmentDirectory { message: String },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read { path, message } => {
                write!(f, "cannot read config {}: {message}", path.display())
            }
            Self::Parse(message) => write!(f, "invalid config: {message}"),
            Self::Endpoint => f.write_str("endpoint must be a ws:// or wss:// URL"),
            Self::Market { slug, reason } => {
                write!(f, "market {slug:?} is not a venue-native slug: {reason:?}")
            }
            Self::DuplicateMarket { slug } => write!(f, "market {slug:?} is listed twice"),
            Self::MarketsPerShard { limit } => {
                write!(f, "markets_per_shard must be between 1 and {limit}")
            }
            Self::ShardsExceedConnectionCap {
                shards,
                peak_sockets,
                cap,
            } => write!(
                f,
                "this market set needs {shards} shards, peaking at {peak_sockets} concurrent \
                 venue sockets, past the configured max_venue_connections of {cap}"
            ),
            Self::DailyAttemptBudget { budget, ceiling } => write!(
                f,
                "daily_connection_attempt_budget is {budget} and the attempt ledger stores \
                 one instant per admitted attempt, so it must be at most {ceiling}"
            ),
            Self::CommandInterval {
                interval_ms,
                ceiling,
            } => write!(
                f,
                "min_command_interval_ms is {interval_ms} and every recovery deadline runs \
                 from a command slot spaced by that floor, so it must be at most {ceiling}"
            ),
            Self::LeaseTtl {
                key,
                ttl_ms,
                ceiling,
            } => write!(
                f,
                "{key} is {ttl_ms} and must be at most {ceiling} milliseconds"
            ),
            Self::LeaseTtlTooShort { key, ttl_ms, floor } => write!(
                f,
                "{key} is {ttl_ms} and must be 0 for no expiry or at least {floor} milliseconds"
            ),
            Self::MetricsListen { value } => write!(
                f,
                "metrics_listen {value:?} is not a numeric host:port address, such as \
                 127.0.0.1:9090"
            ),
            Self::ControlSocket { message } => write!(f, "control_socket {message}"),
            Self::ControlSocketTooLong { bytes, limit } => write!(
                f,
                "control_socket is {bytes} bytes and a unix socket address carries {limit}"
            ),
            Self::Capacity { field, limit } => {
                write!(f, "{field} must be between 1 and {limit}")
            }
            Self::Replicas { limit } => {
                write!(f, "replicas must be between 1 and {limit}")
            }
            Self::SegmentSlots {
                slots,
                markets_per_shard,
            } => write!(
                f,
                "delivery.segment_slots is {slots} and must carry markets_per_shard = {markets_per_shard}"
            ),
            Self::SegmentGeometry(error) => write!(
                f,
                "this delivery geometry is not one a segment can have ({error:?}); lower \
                 markets_per_shard, delivery.segment_slots, level_capacity, or \
                 delivery.event_capacity"
            ),
            Self::SegmentDirectory { message } => write!(f, "delivery.directory {message}"),
        }
    }
}
impl std::error::Error for ConfigError {}

/// A validated configuration: what the daemon runs, with nothing left to check.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DaemonPlan {
    /// Where the control listener binds.
    pub control_socket: PathBuf,
    /// How long a silent control session keeps its leases, or `None` for no limit.
    pub lease_ttl: Option<Duration>,
    /// How many control connections the daemon accepts at once.
    /// See [`DaemonConfig::max_control_sessions`].
    pub max_control_sessions: usize,
    /// Where the metrics endpoint binds, or `None` for a daemon that serves none.
    pub metrics_listen: Option<SocketAddr>,
    /// One entry per shard, in partition order, each carrying its own slice of the set.
    pub shards: Vec<ShardConfig>,
    /// Where each shard's segment is created, and the geometry every one of them is created
    /// with.
    pub delivery: DeliveryPlan,
}

/// The validated delivery surface: one directory, one geometry, one segment per shard.
///
/// Every shard's segment has the same geometry, because a shard's slice of the market set is
/// a partition of one configuration rather than a tier of its own.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryPlan {
    /// The existing, absolute directory the segment files are created in.
    pub directory: PathBuf,
    /// The geometry every shard's segment is created with.
    pub layout: SegmentLayout,
}

impl DaemonPlan {
    /// How many markets one shard may hold, which is also how many a runtime `add` may grow
    /// it to.
    pub fn markets_per_shard(&self) -> usize {
        self.shards
            .first()
            .map_or(DEFAULT_MARKETS_PER_SHARD, |shard| shard.max_markets)
    }

    /// How many venue connection roles every shard of this plan runs, publishing role
    /// included. Uniform across the plan, because every shard is built from one template.
    pub fn replicas(&self) -> usize {
        self.shards.first().map_or(1, |shard| shard.replicas)
    }
}

/// The file name shard `shard` of the daemon instance `daemon_instance_id` publishes into.
///
/// Unpredictable and instance-scoped, as `docs/notes/shared-memory-model.md` §4.2 requires:
/// the name carries this instance's random 128-bit identity, so it is not derivable from the
/// configuration and a name left over from a previous instance never resolves to the current
/// segment. The shard index is the only part an operator can predict, and it identifies
/// nothing on its own.
pub fn segment_file_name(daemon_instance_id: u128, shard: usize) -> String {
    format!("pmws-{daemon_instance_id:032x}-{shard}.seg")
}

/// The most bytes [`segment_file_name`] can produce, at the widest shard index a daemon may
/// run.
pub const MAX_SEGMENT_FILE_NAME_BYTES: usize = 5 + 32 + 1 + 20 + 4;

/// A fresh 128-bit identity for one daemon instance.
///
/// Drawn from the same operating-system entropy `RandomState` seeds its hashers from — two
/// independently seeded hashers make the two halves — so the name a segment takes is not
/// predictable from the daemon's configuration, its pid, or its start time. It is an
/// identity, never a secret: it appears in a file name and in every segment header.
pub fn random_instance_id() -> u128 {
    let high = u128::from(RandomState::new().hash_one(0xA5A5_A5A5_u64));
    let low = u128::from(RandomState::new().hash_one(0x5A5A_5A5A_u64));
    (high << 64) | low
}

impl DaemonConfig {
    /// Reads and validates the configuration at `path`.
    ///
    /// Fails with [`ConfigError::Read`] when the file cannot be read, [`ConfigError::Parse`]
    /// when it is not this daemon's document — including an unknown key — and with a field's
    /// own variant when a value is out of range.
    pub fn load(path: &Path) -> Result<DaemonPlan, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|error| ConfigError::Read {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;
        Self::parse(text.as_str())
    }

    /// Validates a configuration document already in memory.
    pub fn parse(text: &str) -> Result<DaemonPlan, ConfigError> {
        let config: Self =
            toml::from_str(text).map_err(|error| ConfigError::Parse(error.to_string()))?;
        config.validate()
    }

    /// Checks every field and partitions the market set into shards.
    ///
    /// Markets are sorted and partitioned into fixed-size runs, so one document always
    /// produces the same assignment: which shard holds a market is a property of the
    /// configuration, not of the order it happened to be typed in. A configuration with no
    /// market still produces one shard, which holds no connection until a command gives it
    /// something to subscribe to.
    pub fn validate(self) -> Result<DaemonPlan, ConfigError> {
        if self.endpoint.is_empty()
            || !(self.endpoint.starts_with("ws://") || self.endpoint.starts_with("wss://"))
        {
            return Err(ConfigError::Endpoint);
        }
        validate_socket_path(self.control_socket.as_path())?;
        let profile = self.delivery.profile;
        let markets_per_shard = self
            .markets_per_shard
            .unwrap_or_else(|| profile.markets_per_shard());
        let observer_capacity = self
            .observer_capacity
            .unwrap_or_else(|| profile.observer_capacity());
        let level_capacity = self
            .level_capacity
            .unwrap_or_else(|| profile.level_capacity());
        if markets_per_shard == 0 || markets_per_shard > MAX_SHARD_MARKETS {
            return Err(ConfigError::MarketsPerShard {
                limit: MAX_SHARD_MARKETS,
            });
        }
        capacity("observer_capacity", observer_capacity, MAX_QUEUE_CAPACITY)?;
        capacity(
            "level_capacity",
            level_capacity,
            crate::MAX_LEVEL_CAPACITY as usize,
        )?;
        capacity("ingest_capacity", self.ingest_capacity, MAX_QUEUE_CAPACITY)?;
        capacity(
            "control_capacity",
            self.control_capacity,
            MAX_QUEUE_CAPACITY,
        )?;
        capacity(
            "max_control_sessions",
            self.max_control_sessions,
            MAX_QUEUE_CAPACITY,
        )?;
        if self.daily_connection_attempt_budget > WORST_CASE_ATTEMPT_CEILING {
            return Err(ConfigError::DailyAttemptBudget {
                budget: self.daily_connection_attempt_budget,
                ceiling: WORST_CASE_ATTEMPT_CEILING,
            });
        }
        if self.min_command_interval_ms > MAX_COMMAND_INTERVAL_MS {
            return Err(ConfigError::CommandInterval {
                interval_ms: self.min_command_interval_ms,
                ceiling: MAX_COMMAND_INTERVAL_MS,
            });
        }
        if self.lease_ttl_ms > MAX_LEASE_TTL_MS {
            return Err(ConfigError::LeaseTtl {
                key: "lease_ttl_ms",
                ttl_ms: self.lease_ttl_ms,
                ceiling: MAX_LEASE_TTL_MS,
            });
        }
        if self.lease_ttl_ms > 0 && self.lease_ttl_ms < MIN_LEASE_TTL_MS {
            return Err(ConfigError::LeaseTtlTooShort {
                key: "lease_ttl_ms",
                ttl_ms: self.lease_ttl_ms,
                floor: MIN_LEASE_TTL_MS,
            });
        }
        let metrics_listen = match self.metrics_listen.as_deref() {
            Some(value) => {
                Some(
                    value
                        .parse::<SocketAddr>()
                        .map_err(|_| ConfigError::MetricsListen {
                            value: value.to_owned(),
                        })?,
                )
            }
            None => None,
        };
        let delivery = self.delivery_plan(markets_per_shard, level_capacity)?;

        let mut markets = self.markets.clone();
        markets.sort();
        for pair in markets.windows(2) {
            if pair[0] == pair[1] {
                return Err(ConfigError::DuplicateMarket {
                    slug: pair[0].clone(),
                });
            }
        }
        for slug in &markets {
            NativeMarketKey::new(NativeIdentifierKind::slug(), slug.as_str()).map_err(
                |reason| ConfigError::Market {
                    slug: slug.clone(),
                    reason,
                },
            )?;
        }
        let shard_count = markets.len().div_ceil(markets_per_shard).max(1);
        if self.replicas == 0 || self.replicas > MAX_REPLICAS {
            return Err(ConfigError::Replicas {
                limit: MAX_REPLICAS,
            });
        }
        if let Some(cap) = self.max_venue_connections {
            capacity("max_venue_connections", cap, usize::MAX)?;
            let peak_sockets = shard_count.saturating_mul(2).saturating_mul(self.replicas);
            if peak_sockets > cap {
                return Err(ConfigError::ShardsExceedConnectionCap {
                    shards: shard_count,
                    peak_sockets,
                    cap,
                });
            }
        }
        let template = ShardConfig {
            endpoint: self.endpoint.clone(),
            markets: Vec::new(),
            ingest_capacity: self.ingest_capacity,
            control_capacity: self.control_capacity,
            observer_capacity,
            level_capacity,
            max_markets: markets_per_shard,
            replicas: self.replicas,
            daemon_generation: self.daemon_generation,
            daily_attempt_budget: self.daily_connection_attempt_budget,
            min_command_interval_ms: self.min_command_interval_ms,
            ..ShardConfig::default()
        };
        let mut shards: Vec<ShardConfig> = markets
            .chunks(markets_per_shard)
            .map(|chunk| ShardConfig {
                markets: chunk.to_vec(),
                ..template.clone()
            })
            .collect();
        if shards.is_empty() {
            shards.push(template);
        }
        Ok(DaemonPlan {
            control_socket: self.control_socket,
            lease_ttl: (self.lease_ttl_ms > 0).then(|| Duration::from_millis(self.lease_ttl_ms)),
            max_control_sessions: self.max_control_sessions,
            metrics_listen,
            shards,
            delivery,
        })
    }

    /// Resolves the delivery section against its profile and checks the geometry it implies.
    ///
    /// The state slots are sized from the same `level_capacity` the books are, so no venue
    /// book a shard accepts can be refused by the segment it publishes into — a refusal
    /// there is fatal to the run, and no venue value may cause one.
    fn delivery_plan(
        &self,
        markets_per_shard: usize,
        level_capacity: usize,
    ) -> Result<DeliveryPlan, ConfigError> {
        let profile = self.delivery.profile;
        let slots = self.delivery.segment_slots.unwrap_or_else(|| {
            profile.segment_slots().max(
                markets_per_shard
                    .saturating_add(markets_per_shard / SEGMENT_CHURN_DIVISOR)
                    .max(markets_per_shard),
            )
        });
        if slots < markets_per_shard {
            return Err(ConfigError::SegmentSlots {
                slots,
                markets_per_shard,
            });
        }
        let events = self
            .delivery
            .event_capacity
            .unwrap_or_else(|| profile.event_capacity());
        let dirty = self
            .delivery
            .dirty_capacity
            .unwrap_or_else(|| profile.dirty_capacity());
        let layout = segment_layout(slots, level_capacity, events, dirty)?;
        let directory = match self.delivery.directory.clone() {
            Some(directory) => {
                validate_directory(directory.as_path())?;
                directory
            }
            None => self
                .control_socket
                .parent()
                .ok_or_else(|| ConfigError::SegmentDirectory {
                    message: "was not given and the control socket names no directory".to_owned(),
                })?
                .to_path_buf(),
        };
        Ok(DeliveryPlan { directory, layout })
    }
}

/// The geometry one shard's segment is created with, from counts an operator declared.
///
/// Every count is narrowed to the `u32` the layout speaks in before it is offered, so a
/// figure past what a segment can address is the layout's own typed refusal rather than a
/// silent truncation.
fn segment_layout(
    slots: usize,
    level_capacity: usize,
    events: usize,
    dirty: usize,
) -> Result<SegmentLayout, ConfigError> {
    let narrow = |value: usize| u32::try_from(value).unwrap_or(u32::MAX);
    SegmentLayout::new(
        narrow(slots),
        narrow(slots),
        narrow(level_capacity),
        narrow(events),
        narrow(dirty),
    )
    .map_err(ConfigError::SegmentGeometry)
}

/// Checks that a declared directory can hold segment files: absolute, and already there.
fn validate_directory(path: &Path) -> Result<(), ConfigError> {
    if !path.is_absolute() {
        return Err(ConfigError::SegmentDirectory {
            message: "must be an absolute path".to_owned(),
        });
    }
    if !path.is_dir() {
        return Err(ConfigError::SegmentDirectory {
            message: format!("{} is not a directory", path.display()),
        });
    }
    Ok(())
}

fn capacity(field: &'static str, value: usize, limit: usize) -> Result<(), ConfigError> {
    if value < MIN_CAPACITY || value > limit {
        return Err(ConfigError::Capacity { field, limit });
    }
    Ok(())
}

/// Checks that a control socket path can be bound: absolute, short enough for a Unix socket
/// address, and inside a directory that already exists.
///
/// The directory is checked here rather than created, because a daemon that creates
/// directories named by a configuration file is a daemon that can be told to create one
/// anywhere.
fn validate_socket_path(path: &Path) -> Result<(), ConfigError> {
    let bytes = path.as_os_str().as_encoded_bytes().len();
    if bytes == 0 {
        return Err(ConfigError::ControlSocket {
            message: "must not be empty".to_owned(),
        });
    }
    if !path.is_absolute() {
        return Err(ConfigError::ControlSocket {
            message: "must be an absolute path".to_owned(),
        });
    }
    if bytes > MAX_CONTROL_SOCKET_PATH_BYTES {
        return Err(ConfigError::ControlSocketTooLong {
            bytes,
            limit: MAX_CONTROL_SOCKET_PATH_BYTES,
        });
    }
    let parent = path.parent().ok_or_else(|| ConfigError::ControlSocket {
        message: "names no directory".to_owned(),
    })?;
    if !parent.is_dir() {
        return Err(ConfigError::ControlSocket {
            message: format!("is in {}, which is not a directory", parent.display()),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MAX_REGION_BYTES;
    use crate::limitless::shard::MAX_SHARD_MARKETS;

    /// The market count the design document names as designed headroom for the scale profile.
    const ENVELOPE_MARKETS: usize = 50_000;

    /// The shard count this envelope declares for its own deployment shape.
    ///
    /// Not a ceiling anything enforces — shard count is fleet-sized and follows the
    /// configured market list — but a deployment shape someone has to name to do the
    /// envelope's arithmetic at all: this is the scale profile's own two-shard deployment,
    /// the one the design document sizes fifty thousand markets against.
    const ENVELOPE_SHARDS: usize = 2;

    /// What one market costs in a scale segment, as this ABI lays it out: 512 bytes of
    /// directory entry, a 16,640-byte state slot at the profile's 256 declared levels, and an
    /// 8,192-byte event ring at its 32 retained deliveries.
    ///
    /// Pinned rather than merely derived, so a change to any of those widths re-opens the
    /// envelope arithmetic instead of quietly moving it.
    const SCALE_BYTES_PER_MARKET: usize = 25_344;

    /// One scale segment's whole region: 794 MiB of the 1 GiB a region may occupy, and
    /// 1.55 GiB for the pair of shards that carries fifty thousand markets.
    const SCALE_REGION_BYTES: usize = 832_569_728;

    /// The most bytes a whole daemon may map for delivery at the scale profile.
    ///
    /// Two segments, one per shard of this envelope's own declared two-shard deployment
    /// ([`ENVELOPE_SHARDS`]). It is a declared bound on the deployment rather than a
    /// property of the ABI: each segment separately may not exceed [`MAX_REGION_BYTES`], and
    /// this says the pair together stays inside two of them.
    const ENVELOPE_MAPPED_CEILING: usize = 2 * MAX_REGION_BYTES;

    /// The most file descriptors the scale envelope needs, as [`descriptor_envelope`]
    /// computes it for this envelope's own declared shard count — the same function the
    /// daemon's startup preflight checks against the process descriptor limit.
    ///
    /// The declared shape serves metrics, which is the more expensive of the two answers and
    /// therefore the one an envelope states.
    const ENVELOPE_DESCRIPTOR_CEILING: usize = descriptor_envelope(ENVELOPE_SHARDS, 1, true);

    fn document(body: &str) -> String {
        format!("control_socket = \"/tmp/pmwsd-config-test.sock\"\n{body}")
    }

    #[test]
    fn a_minimal_document_produces_one_shard_holding_the_whole_set() {
        let plan = DaemonConfig::parse(&document("markets = [\"b-market\", \"a-market\"]\n"))
            .expect("a minimal document is valid");
        assert_eq!(plan.shards.len(), 1);
        assert_eq!(
            plan.shards[0].markets,
            vec!["a-market".to_owned(), "b-market".to_owned()],
            "the partition is by sorted slug, so one document always assigns the same way"
        );
        assert_eq!(plan.shards[0].endpoint, DEFAULT_ENDPOINT);
    }

    #[test]
    fn a_set_larger_than_one_shard_partitions_deterministically() {
        let plan = DaemonConfig::parse(&document(
            "markets = [\"d\", \"c\", \"b\", \"a\"]\nmarkets_per_shard = 2\n",
        ))
        .expect("two shards is a valid document with no cap declared");
        assert_eq!(plan.shards.len(), 2);
        assert_eq!(plan.shards[0].markets, vec!["a".to_owned(), "b".to_owned()]);
        assert_eq!(plan.shards[1].markets, vec!["c".to_owned(), "d".to_owned()]);
    }

    #[test]
    fn a_document_with_no_market_still_produces_one_shard() {
        let plan = DaemonConfig::parse(&document("")).expect("an empty set is valid");
        assert_eq!(plan.shards.len(), 1);
        assert!(plan.shards[0].markets.is_empty());
    }

    #[test]
    fn the_withdrawn_diagnostic_logging_key_is_now_an_unknown_key() {
        let error = DaemonConfig::parse(&document("log_commands = true\n"))
            .expect_err("per-command logging was withdrawn, not silently ignored");
        assert!(matches!(error, ConfigError::Parse(_)), "{error}");
    }

    #[test]
    fn an_unknown_key_is_rejected_rather_than_ignored() {
        let error = DaemonConfig::parse(&document("market = [\"a-market\"]\n"))
            .expect_err("an unknown key is a misconfiguration");
        assert!(matches!(error, ConfigError::Parse(_)), "{error}");
    }

    #[test]
    fn a_missing_control_socket_is_rejected() {
        let error =
            DaemonConfig::parse("markets = []\n").expect_err("the control socket is required");
        assert!(matches!(error, ConfigError::Parse(_)), "{error}");
    }

    #[test]
    fn a_slug_that_is_not_an_identifier_is_rejected() {
        let error = DaemonConfig::parse(&document("markets = [\"\"]\n"))
            .expect_err("an empty slug is not a market");
        assert!(matches!(error, ConfigError::Market { .. }), "{error}");
    }

    #[test]
    fn a_repeated_market_is_rejected() {
        let error = DaemonConfig::parse(&document("markets = [\"a-market\", \"a-market\"]\n"))
            .expect_err("a desired set is a set");
        assert!(
            matches!(error, ConfigError::DuplicateMarket { .. }),
            "{error}"
        );
    }

    #[test]
    fn a_set_needing_more_shards_than_the_configured_connection_cap_is_rejected() {
        let error = DaemonConfig::parse(&document(
            "markets = [\"a\", \"b\", \"c\"]\nmarkets_per_shard = 1\nmax_venue_connections = 4\n",
        ))
        .expect_err("three shards peak at six sockets, past a declared cap of four");
        match error {
            ConfigError::ShardsExceedConnectionCap {
                shards,
                peak_sockets,
                cap,
            } => {
                assert_eq!(shards, 3);
                assert_eq!(peak_sockets, 6);
                assert_eq!(cap, 4);
            }
            other => panic!("expected a connection-cap refusal, got {other}"),
        }
    }

    /// The one place the replica count crosses from a document into what a shard runs.
    #[test]
    fn a_declared_replica_count_reaches_every_shard() {
        let plan = DaemonConfig::parse(&document(
            "markets = [\"a\", \"b\", \"c\"]\nmarkets_per_shard = 1\nreplicas = 2\n",
        ))
        .expect("a two-replica document is valid");
        assert_eq!(plan.shards.len(), 3);
        assert_eq!(plan.replicas(), 2);
        for (index, shard) in plan.shards.iter().enumerate() {
            assert_eq!(
                shard.replicas, 2,
                "shard {index} runs the declared replica count"
            );
        }
    }

    #[test]
    fn a_document_naming_no_replica_count_runs_one_connection_per_shard() {
        let plan = DaemonConfig::parse(&document("markets = [\"a-market\"]\n"))
            .expect("a minimal document is valid");
        assert_eq!(
            plan.replicas(),
            1,
            "the shipped default is one publishing connection and no standby"
        );
        assert!(plan.shards.iter().all(|shard| shard.replicas == 1));
    }

    #[test]
    fn a_replica_count_past_the_supported_ladder_depth_is_rejected() {
        let over = MAX_REPLICAS + 1;
        let error = DaemonConfig::parse(&document(&format!("replicas = {over}\n")))
            .expect_err("a replica count past the ladder depth is refused");
        match error {
            ConfigError::Replicas { limit } => assert_eq!(limit, MAX_REPLICAS),
            other => panic!("expected a replica refusal, got {other}"),
        }
        let error = DaemonConfig::parse(&document("replicas = 0\n"))
            .expect_err("a shard publishing from no connection is refused");
        assert!(matches!(error, ConfigError::Replicas { .. }), "{error}");
    }

    /// A standby doubles what a shard peaks at, and the connection cap is arithmetic about
    /// that peak rather than about the shard count alone.
    #[test]
    fn a_standby_doubles_the_peak_socket_count_the_connection_cap_judges() {
        let error = DaemonConfig::parse(&document(
            "markets = [\"a\", \"b\"]\nmarkets_per_shard = 1\nreplicas = 2\n\
             max_venue_connections = 6\n",
        ))
        .expect_err("two shards of two roles peak at eight sockets, past a cap of six");
        match error {
            ConfigError::ShardsExceedConnectionCap {
                shards,
                peak_sockets,
                cap,
            } => {
                assert_eq!(shards, 2);
                assert_eq!(peak_sockets, 8, "2 shards * 2 roles * 2 sockets each");
                assert_eq!(cap, 6);
            }
            other => panic!("expected a connection-cap refusal, got {other}"),
        }
        assert!(
            DaemonConfig::parse(&document(
                "markets = [\"a\", \"b\"]\nmarkets_per_shard = 1\nreplicas = 2\n\
                 max_venue_connections = 8\n",
            ))
            .is_ok(),
            "the same shape inside its declared cap is accepted"
        );
    }

    #[test]
    fn the_same_set_without_a_connection_cap_is_fleet_sized_into_three_shards() {
        let plan = DaemonConfig::parse(&document(
            "markets = [\"a\", \"b\", \"c\"]\nmarkets_per_shard = 1\n",
        ))
        .expect("absent a declared cap, a larger market list simply takes more shards");
        assert_eq!(plan.shards.len(), 3);
    }

    #[test]
    fn a_zero_connection_cap_is_rejected() {
        let error = DaemonConfig::parse(&document("max_venue_connections = 0\n"))
            .expect_err("a daemon declared to hold no connection at all is a misconfiguration");
        assert!(matches!(error, ConfigError::Capacity { .. }), "{error}");
    }

    /// Both keys are a property of the daemon, not of one shard, so every shard the market
    /// partition produces has to carry them: the attempt ledger and the wire pacer are
    /// process-wide, and a shard reading a different figure would spend against a budget its
    /// siblings never agreed to. Six markets at two per shard is three shards, which is past
    /// the shard count the deleted concurrency ceiling used to allow at all.
    #[test]
    fn the_configured_daily_budget_and_command_floor_reach_every_shard() {
        let plan = DaemonConfig::parse(&document(
            "markets = [\"a\", \"b\", \"c\", \"d\", \"e\", \"f\"]\nmarkets_per_shard = 2\n\
             daily_connection_attempt_budget = 99\nmin_command_interval_ms = 25\n",
        ))
        .expect("both keys are within range and no connection cap is declared");
        assert_eq!(
            plan.shards.len(),
            3,
            "six markets at two per shard is three"
        );
        for (index, shard) in plan.shards.iter().enumerate() {
            assert_eq!(
                shard.daily_attempt_budget, 99,
                "shard {index} reads the daemon's configured budget"
            );
            assert_eq!(
                shard.min_command_interval_ms, 25,
                "shard {index} reads the daemon's configured command floor"
            );
        }
    }

    /// The one place the descriptor arithmetic is stated, so the daemon's startup preflight
    /// and this module's envelope test cannot disagree about what a shard costs.
    #[test]
    fn the_descriptor_envelope_counts_every_descriptor_a_shard_retains() {
        assert_eq!(
            descriptor_envelope(0, 1, false),
            21,
            "the control socket, its lock, the three standard streams, and the slack"
        );
        assert_eq!(
            descriptor_envelope(0, 1, true),
            22,
            "a configured metrics listener is one more, held for the daemon's life"
        );
        assert_eq!(
            descriptor_envelope(1, 1, false),
            26,
            "one shard retains both segment descriptors, its doorbell page, and two sockets"
        );
        assert_eq!(
            descriptor_envelope(ENVELOPE_SHARDS, 1, true),
            ENVELOPE_DESCRIPTOR_CEILING
        );
        assert_eq!(
            descriptor_envelope(20, 1, false),
            121,
            "the full-fleet deployment shape"
        );
        assert_eq!(
            descriptor_envelope(usize::MAX, 1, true),
            usize::MAX,
            "an absurd shard count saturates rather than wrapping to a figure that fits"
        );
    }

    /// A replica role is two more sockets per shard and nothing else: the segment pair and
    /// the doorbell page are per shard, not per connection.
    #[test]
    fn a_replica_role_costs_two_more_sockets_per_shard_and_no_more() {
        assert_eq!(
            descriptor_envelope(1, 2, false),
            28,
            "one shard's standby adds its live socket and its own draining one"
        );
        assert_eq!(
            descriptor_envelope(20, 2, false),
            161,
            "the full-fleet shape with a standby each: 20 * (3 + 4) + 21"
        );
        assert_eq!(
            descriptor_envelope(20, 1, false) + 20 * 2,
            descriptor_envelope(20, 2, false),
            "every added role is exactly two descriptors per shard"
        );
        assert_eq!(
            descriptor_envelope(usize::MAX, 4, true),
            usize::MAX,
            "an absurd shape saturates rather than wrapping"
        );
    }

    #[test]
    fn a_daily_attempt_budget_past_the_ledger_ceiling_is_rejected() {
        let over = WORST_CASE_ATTEMPT_CEILING + 1;
        let error = DaemonConfig::parse(&document(&format!(
            "daily_connection_attempt_budget = {over}\n"
        )))
        .expect_err("the ledger stores one instant per admitted attempt");
        match error {
            ConfigError::DailyAttemptBudget { budget, ceiling } => {
                assert_eq!(budget, over);
                assert_eq!(ceiling, WORST_CASE_ATTEMPT_CEILING);
            }
            other => panic!("expected an attempt-budget refusal, got {other}"),
        }
    }

    #[test]
    fn a_command_floor_past_the_structural_maximum_is_rejected() {
        let over = MAX_COMMAND_INTERVAL_MS + 1;
        let error = DaemonConfig::parse(&document(&format!("min_command_interval_ms = {over}\n")))
            .expect_err("every recovery deadline runs from a slot spaced by the configured floor");
        match error {
            ConfigError::CommandInterval {
                interval_ms,
                ceiling,
            } => {
                assert_eq!(interval_ms, over);
                assert_eq!(ceiling, MAX_COMMAND_INTERVAL_MS);
            }
            other => panic!("expected a command-floor refusal, got {other}"),
        }
    }

    /// Both bounds are inclusive, and a zero command floor stays the valid operator choice
    /// it was: the two structural maxima refuse what is past them and nothing else.
    #[test]
    fn the_pacing_bounds_accept_their_own_boundary_values() {
        let plan = DaemonConfig::parse(&document(&format!(
            "daily_connection_attempt_budget = {WORST_CASE_ATTEMPT_CEILING}\n\
             min_command_interval_ms = {MAX_COMMAND_INTERVAL_MS}\n"
        )))
        .expect("the boundary values themselves are accepted");
        assert_eq!(
            plan.shards[0].daily_attempt_budget,
            WORST_CASE_ATTEMPT_CEILING
        );
        assert_eq!(
            plan.shards[0].min_command_interval_ms,
            MAX_COMMAND_INTERVAL_MS
        );

        let unpaced = DaemonConfig::parse(&document("min_command_interval_ms = 0\n"))
            .expect("no pacing at all is a valid operator choice");
        assert_eq!(unpaced.shards[0].min_command_interval_ms, 0);
    }

    #[test]
    fn a_relative_socket_path_is_rejected() {
        let error = DaemonConfig::parse("control_socket = \"pmwsd.sock\"\nmarkets = []\n")
            .expect_err("a relative control socket is not bindable from a known place");
        assert!(
            matches!(error, ConfigError::ControlSocket { .. }),
            "{error}"
        );
    }

    #[test]
    fn a_socket_path_longer_than_a_unix_address_is_rejected() {
        let long = "a".repeat(MAX_CONTROL_SOCKET_PATH_BYTES);
        let error = DaemonConfig::parse(&format!(
            "control_socket = \"/tmp/{long}.sock\"\nmarkets = []\n"
        ))
        .expect_err("an over-long path cannot be a unix socket address");
        assert!(
            matches!(error, ConfigError::ControlSocketTooLong { .. }),
            "{error}"
        );
    }

    #[test]
    fn a_socket_path_in_a_missing_directory_is_rejected() {
        let error = DaemonConfig::parse(
            "control_socket = \"/tmp/pm-ws-no-such-directory/pmwsd.sock\"\nmarkets = []\n",
        )
        .expect_err("the daemon binds in an existing directory or not at all");
        assert!(
            matches!(error, ConfigError::ControlSocket { .. }),
            "{error}"
        );
    }

    #[test]
    fn a_capacity_outside_its_range_is_rejected() {
        let error = DaemonConfig::parse(&document("observer_capacity = 0\n"))
            .expect_err("a zero capacity is not a capacity");
        assert!(matches!(error, ConfigError::Capacity { .. }), "{error}");
    }

    #[test]
    fn max_control_sessions_defaults_to_two_hundred_fifty_six_when_absent() {
        let plan = DaemonConfig::parse(&document(""))
            .expect("a document with no max_control_sessions key is valid");
        assert_eq!(plan.max_control_sessions, 256);
    }

    #[test]
    fn a_zero_max_control_sessions_is_rejected() {
        let error = DaemonConfig::parse(&document("max_control_sessions = 0\n"))
            .expect_err("a daemon that accepts no control session at all is a misconfiguration");
        assert!(matches!(error, ConfigError::Capacity { .. }), "{error}");
    }

    #[test]
    fn a_max_control_sessions_past_the_ceiling_is_rejected() {
        let error = DaemonConfig::parse(&document(&format!(
            "max_control_sessions = {}\n",
            MAX_QUEUE_CAPACITY + 1
        )))
        .expect_err(
            "max_control_sessions shares the same generic ceiling every other bounded count \
             in this document is refused by",
        );
        assert!(matches!(error, ConfigError::Capacity { .. }), "{error}");
    }

    #[test]
    fn a_configured_max_control_sessions_is_carried_into_the_plan() {
        let plan = DaemonConfig::parse(&document("max_control_sessions = 3\n"))
            .expect("three sessions is a valid document");
        assert_eq!(plan.max_control_sessions, 3);
    }

    /// A TTL past the ceiling is a configuration error naming the key and the ceiling,
    /// rather than an arithmetic hazard carried into the daemon's sweep path.
    #[test]
    fn a_lease_ttl_past_the_ceiling_is_rejected() {
        let error = DaemonConfig::parse(&document(&format!("lease_ttl_ms = {}\n", u64::MAX)))
            .expect_err("a deadline no clock computes is not a lease TTL");
        let ConfigError::LeaseTtl {
            key,
            ttl_ms,
            ceiling,
        } = error
        else {
            panic!("the refusal names the key and the ceiling: {error}");
        };
        assert_eq!(key, "lease_ttl_ms");
        assert_eq!(ttl_ms, u64::MAX);
        assert_eq!(ceiling, MAX_LEASE_TTL_MS);
        assert!(
            DaemonConfig::parse(&document(
                format!("lease_ttl_ms = {}\n", MAX_LEASE_TTL_MS + 1).as_str()
            ))
            .is_err(),
            "one millisecond past the ceiling is past it"
        );
    }

    /// The ceiling itself is a configuration the daemon runs under, so the refusal above is a
    /// bound on the accepted range and never a bound one millisecond inside it.
    #[test]
    fn the_ceiling_lease_ttl_is_accepted_and_carried_into_the_plan() {
        let plan = DaemonConfig::parse(&document(
            format!("lease_ttl_ms = {MAX_LEASE_TTL_MS}\n").as_str(),
        ))
        .expect("the ceiling is inside the accepted range");
        assert_eq!(
            plan.lease_ttl,
            Some(Duration::from_millis(MAX_LEASE_TTL_MS)),
            "the plan carries the TTL the document declared"
        );
    }

    /// A nonzero TTL below the floor is refused with the key and the floor named, because the
    /// daemon promises the value to every attaching consumer and a consumer cannot renew
    /// inside a deadline shorter than its own scheduling.
    #[test]
    fn a_lease_ttl_below_the_floor_is_rejected() {
        let error = DaemonConfig::parse(&document(
            format!("lease_ttl_ms = {}\n", MIN_LEASE_TTL_MS - 1).as_str(),
        ))
        .expect_err("a TTL no consumer can renew inside is not a lease TTL");
        let ConfigError::LeaseTtlTooShort { key, ttl_ms, floor } = error else {
            panic!("the refusal names the key and the floor: {error}");
        };
        assert_eq!(key, "lease_ttl_ms");
        assert_eq!(ttl_ms, MIN_LEASE_TTL_MS - 1);
        assert_eq!(floor, MIN_LEASE_TTL_MS);
        assert!(
            DaemonConfig::parse(&document("lease_ttl_ms = 1\n")).is_err(),
            "one millisecond is a TTL nothing could renew inside"
        );
    }

    /// The floor itself runs, and zero — no expiry at all — is not below it: the floor bounds
    /// the TTLs a daemon expires sessions under and says nothing about a daemon that expires
    /// none.
    #[test]
    fn the_floor_lease_ttl_is_accepted_and_zero_still_disables_expiry() {
        let plan = DaemonConfig::parse(&document(
            format!("lease_ttl_ms = {MIN_LEASE_TTL_MS}\n").as_str(),
        ))
        .expect("the floor is inside the accepted range");
        assert_eq!(
            plan.lease_ttl,
            Some(Duration::from_millis(MIN_LEASE_TTL_MS)),
            "the plan carries the TTL the document declared"
        );
        let disabled = DaemonConfig::parse(&document("lease_ttl_ms = 0\n"))
            .expect("zero is no TTL rather than a TTL below the floor");
        assert_eq!(
            disabled.lease_ttl, None,
            "no TTL is no deadline, and no renewal owed"
        );
    }

    #[test]
    fn markets_per_shard_above_the_shard_ceiling_is_rejected() {
        let error = DaemonConfig::parse(&document(&format!(
            "markets_per_shard = {}\n",
            MAX_SHARD_MARKETS + 1
        )))
        .expect_err("a shard cannot be asked to hold more than it declares");
        assert!(
            matches!(error, ConfigError::MarketsPerShard { .. }),
            "{error}"
        );
    }

    /// The 50,000-market resource envelope, as arithmetic rather than as a load test.
    ///
    /// The design document calls this designed headroom and says so explicitly: what is checked
    /// is that the scale profile's numbers add up inside bounds this file names, never that a
    /// host was driven at that rate. Everything here comes from
    /// [`SegmentLayout`]'s own accessors, so the arithmetic cannot drift from the geometry a
    /// segment is actually created with.
    ///
    /// The shard count is not a free variable here either: this envelope declares its own
    /// two-shard deployment shape ([`ENVELOPE_SHARDS`]), and the arithmetic below is what
    /// that shape buys, not a ceiling any daemon enforces.
    #[test]
    fn the_fifty_thousand_market_envelope_fits_the_scale_profile() {
        let profile = DeliveryProfile::Scale;
        let per_shard = profile.markets_per_shard();
        assert!(
            per_shard * ENVELOPE_SHARDS >= ENVELOPE_MARKETS,
            "the scale profile carries {} markets across {ENVELOPE_SHARDS} shards, short of {ENVELOPE_MARKETS}",
            per_shard * ENVELOPE_SHARDS
        );
        assert!(
            per_shard <= MAX_SHARD_MARKETS,
            "a shard's declared ceiling {MAX_SHARD_MARKETS} is below the scale profile's {per_shard}"
        );
        let slots = profile.segment_slots();
        assert!(
            slots >= per_shard,
            "the scale segment has {slots} slots for {per_shard} markets"
        );

        let layout = segment_layout(
            slots,
            profile.level_capacity(),
            profile.event_capacity(),
            profile.dirty_capacity(),
        )
        .expect("the scale profile is a geometry a segment can have");

        let directory_bytes = layout.state_slot_offset() - layout.directory_offset();
        let state_bytes = layout.event_offset() - layout.state_slot_offset();
        let ring_bytes = layout.dirty_offset() - layout.event_offset();
        let per_market =
            directory_bytes / slots + layout.state_slot_stride() + layout.event_ring_bytes();
        assert_eq!(
            state_bytes,
            slots * layout.state_slot_stride(),
            "every slot is one stride"
        );
        assert_eq!(ring_bytes, slots * layout.event_ring_bytes());
        assert!(
            per_market * slots < layout.region_size(),
            "the per-market figure must be a share of the region, not the whole of it"
        );

        assert_eq!(
            per_market, SCALE_BYTES_PER_MARKET,
            "one market's share of a scale segment moved; re-do the envelope arithmetic"
        );
        assert_eq!(
            layout.region_size(),
            SCALE_REGION_BYTES,
            "the scale segment's region moved; re-do the envelope arithmetic"
        );
        let region = layout.region_size();
        assert!(
            region <= MAX_REGION_BYTES,
            "one scale segment is {region} bytes, past the {MAX_REGION_BYTES}-byte region ceiling"
        );
        let mapped = region * ENVELOPE_SHARDS;
        assert!(
            mapped <= ENVELOPE_MAPPED_CEILING,
            "the scale envelope maps {mapped} bytes across {ENVELOPE_SHARDS} shards, past the {ENVELOPE_MAPPED_CEILING}-byte ceiling"
        );
        const {
            assert!(
                ENVELOPE_DESCRIPTOR_CEILING <= 64,
                "a daemon's own descriptors stay a named handful even at the default \
                 descriptor limit; consumer attachments are the consumer's own descriptors, \
                 not this daemon's"
            );
        }
        assert!(
            layout.dirty_ring_bytes() < region / 64,
            "the one dirty index is a rounding error beside the per-market cost it saves \
             scanning"
        );
    }

    /// The declared book depth is what the 50,000-market envelope is bought with, and this
    /// pins the price.
    ///
    /// At `MAX_BOOK_LEVELS` a state slot is a quarter of a mebibyte, so one segment region
    /// holds a few thousand markets and no arrangement of two shards reaches fifty thousand.
    /// That is not a number to tune around: it is why [`DeliveryProfile::Scale`] declares a
    /// shallower book, and why no other profile does.
    #[test]
    fn full_book_depth_cannot_reach_the_fifty_thousand_market_envelope() {
        let profile = DeliveryProfile::Scale;
        let refused = segment_layout(
            profile.segment_slots(),
            crate::limitless::supervisor::MAX_BOOK_LEVELS,
            profile.event_capacity(),
            profile.dirty_capacity(),
        );
        assert!(
            matches!(
                refused,
                Err(ConfigError::SegmentGeometry(LayoutError::RegionTooLarge))
            ),
            "the scale market count at full book depth must be refused as geometry, got {refused:?}"
        );

        let full_depth = segment_layout(
            1,
            crate::limitless::supervisor::MAX_BOOK_LEVELS,
            profile.event_capacity(),
            profile.dirty_capacity(),
        )
        .expect("one market at full depth is a valid segment");
        let markets_at_full_depth = MAX_REGION_BYTES / full_depth.state_slot_stride();
        assert!(
            markets_at_full_depth * ENVELOPE_SHARDS < ENVELOPE_MARKETS,
            "if state slots alone at full depth could carry {ENVELOPE_MARKETS} markets, the \
             scale profile would not need to declare a shallower book"
        );
    }

    /// Every profile is a geometry a segment can have, and every one of them carries the
    /// markets it says it does.
    #[test]
    fn every_delivery_profile_is_a_segment_a_daemon_can_create() {
        for profile in [
            DeliveryProfile::Common,
            DeliveryProfile::Hot,
            DeliveryProfile::Scale,
        ] {
            let layout = segment_layout(
                profile.segment_slots(),
                profile.level_capacity(),
                profile.event_capacity(),
                profile.dirty_capacity(),
            )
            .unwrap_or_else(|error| panic!("{profile:?} is not a segment geometry: {error}"));
            assert!(
                profile.segment_slots() >= profile.markets_per_shard(),
                "{profile:?} has fewer slots than markets"
            );
            assert!(
                layout.region_size() <= MAX_REGION_BYTES,
                "{profile:?} implies a {}-byte region",
                layout.region_size()
            );
        }
    }

    /// The common profile is the one an unconfigured deployment gets, so its depth is the
    /// depth this daemon has always accepted: a profile may not narrow what the book takes
    /// from the venue by default.
    #[test]
    fn the_default_profile_declares_the_full_book_depth() {
        let plan = DaemonConfig::parse(&document("markets = [\"a-market\"]\n"))
            .expect("a minimal document is valid");
        assert_eq!(
            plan.shards[0].level_capacity,
            crate::limitless::supervisor::MAX_BOOK_LEVELS
        );
        assert_eq!(DeliveryProfile::default(), DeliveryProfile::Common);
    }

    #[test]
    fn a_profile_supplies_the_defaults_and_an_explicit_key_overrides_it() {
        let plan = DaemonConfig::parse(&document(
            "[delivery]\nprofile = \"scale\"\nevent_capacity = 16\n",
        ))
        .expect("the scale profile with one override is valid");
        assert_eq!(
            plan.markets_per_shard(),
            DeliveryProfile::Scale.markets_per_shard()
        );
        assert_eq!(
            plan.shards[0].level_capacity,
            DeliveryProfile::Scale.level_capacity()
        );
        assert_eq!(plan.delivery.layout.event_capacity(), 16);
        assert_eq!(
            plan.delivery.layout.dirty_capacity(),
            DeliveryProfile::Scale.dirty_capacity() as u32
        );
    }

    /// The profiles are points inside the region ceiling, not permission to leave it: an
    /// override that outgrows the region is refused at startup rather than discovered when a
    /// segment cannot be created.
    #[test]
    fn an_override_that_outgrows_the_region_is_refused() {
        let error = DaemonConfig::parse(&document(
            "[delivery]\nprofile = \"scale\"\nevent_capacity = 64\n",
        ))
        .expect_err("deeper retention at the scale market count does not fit one region");
        assert!(
            matches!(
                error,
                ConfigError::SegmentGeometry(LayoutError::RegionTooLarge)
            ),
            "{error}"
        );
    }

    #[test]
    fn the_segment_directory_defaults_to_the_control_socket_s_own() {
        let plan = DaemonConfig::parse(&document("")).expect("an empty set is valid");
        assert_eq!(plan.delivery.directory, PathBuf::from("/tmp"));
    }

    #[test]
    fn a_segment_directory_that_is_not_a_directory_is_rejected() {
        let error = DaemonConfig::parse(&document(
            "[delivery]\ndirectory = \"/tmp/pm-ws-no-such-directory\"\n",
        ))
        .expect_err("the daemon creates segments in an existing directory or not at all");
        assert!(
            matches!(error, ConfigError::SegmentDirectory { .. }),
            "{error}"
        );
    }

    #[test]
    fn a_segment_with_fewer_slots_than_markets_is_rejected() {
        let error = DaemonConfig::parse(&document(
            "markets_per_shard = 64\n[delivery]\nsegment_slots = 8\n",
        ))
        .expect_err("a market with no slot could never be delivered");
        assert!(matches!(error, ConfigError::SegmentSlots { .. }), "{error}");
    }

    #[test]
    fn a_delivery_geometry_no_segment_can_have_is_rejected_by_name() {
        let error = DaemonConfig::parse(&document("[delivery]\nevent_capacity = 100\n"))
            .expect_err("a ring depth that cannot be masked is not a geometry");
        assert!(matches!(error, ConfigError::SegmentGeometry(_)), "{error}");
    }

    #[test]
    fn the_segments_of_one_instance_are_named_apart_and_the_next_instance_reuses_neither() {
        let instance = random_instance_id();
        let first = segment_file_name(instance, 0);
        let second = segment_file_name(instance, 1);
        assert_ne!(first, second, "one instance's shards take distinct names");
        assert!(first.len() <= MAX_SEGMENT_FILE_NAME_BYTES);
        assert_ne!(
            segment_file_name(random_instance_id(), 0),
            first,
            "a fresh instance never reuses a name a previous one published under"
        );
        assert!(
            first.contains(&format!("{instance:032x}")),
            "the name carries the instance identity a consumer checks the header against"
        );
    }

    #[test]
    fn an_endpoint_that_is_not_a_websocket_url_is_rejected() {
        let error = DaemonConfig::parse(&document("endpoint = \"https://example.invalid\"\n"))
            .expect_err("ingestion is WebSocket-only");
        assert_eq!(error, ConfigError::Endpoint);
    }
}

use crate::{ConnectionIdentity, DedupKey};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AuthorityState {
    Unsubscribed,
    Subscribing,
    Synchronizing,
    Live,
    Recovering,
    Stale(AuthorityReason),
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AuthorityReason {
    Gap,
    Disconnect,
    SubscriptionLost,
    LocalLoss,
    OrderingUnknown,
    Overload,
    ReplicaDivergence,
    #[serde(rename = "recovery_base_unavailable")]
    RecoveryBaseUnavailable,
}
/// Why a book's mutation stream broke.
///
/// [`Self::SyncDivergence`] is the delta-venue case: a snapshot the venue sent as a
/// checkpoint disagreed with the book its own deltas built, which is proof a delta was
/// missed or misapplied and therefore that the mutations between the two snapshots do not
/// describe the transition. It stays distinct from [`Self::Gap`], which is evidence found
/// in the transport, and from [`Self::Overrun`], which is a consumer falling behind.
///
/// Unlike the others it is transition-internal and never reaches published continuity
/// state: the divergence checkpoint is one atomic commit that loses the stream and recovers
/// it before anything is published, so a reader observes only the new epoch. The divergence
/// itself is reported on the commit and cumulatively on the published book, and the signal
/// a mutation consumer sees is the typed continuity loss its own attachment is given when
/// the stream rebases underneath it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ContinuityReason {
    Overrun,
    Gap,
    LocalLoss,
    Reconnect,
    RecoveryBase,
    SyncDivergence,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum MutationContinuity {
    Intact {
        epoch: u64,
        next_position: u64,
    },
    Lost {
        epoch: u64,
        reason: ContinuityReason,
    },
}
impl MutationContinuity {
    pub fn epoch(&self) -> u64 {
        match self {
            Self::Intact { epoch, .. } | Self::Lost { epoch, .. } => *epoch,
        }
    }
    /// Marks the mutation stream lost at the current epoch.
    ///
    /// An already lost stream keeps the reason it first broke for: a later report of the
    /// same break is not new evidence and never restates why.
    pub fn lost(&self, reason: ContinuityReason) -> Self {
        match self {
            Self::Intact { epoch, .. } => Self::Lost {
                epoch: *epoch,
                reason,
            },
            Self::Lost { .. } => self.clone(),
        }
    }
    /// Opens the next epoch from a lost stream, numbering its first mutation
    /// `first_position`.
    ///
    /// Fails with [`ReplicaError::InvalidTransition`] when the stream is intact, and with
    /// [`ReplicaError::EpochOverflow`] when the epoch counter would wrap.
    pub fn recovered(&self, first_position: u64) -> Result<Self, ReplicaError> {
        if !matches!(self, Self::Lost { .. }) {
            return Err(ReplicaError::InvalidTransition);
        }
        Ok(Self::Intact {
            epoch: self
                .epoch()
                .checked_add(1)
                .ok_or(ReplicaError::EpochOverflow)?,
            next_position: first_position,
        })
    }
    /// Advances an intact stream past `emitted` delivered mutations, keeping the epoch.
    ///
    /// Fails with [`ReplicaError::InvalidTransition`] on a lost stream, and with
    /// [`ReplicaError::EpochOverflow`] when the position counter would wrap.
    pub fn advanced(&self, emitted: u64) -> Result<Self, ReplicaError> {
        let Self::Intact {
            epoch,
            next_position,
        } = self
        else {
            return Err(ReplicaError::InvalidTransition);
        };
        Ok(Self::Intact {
            epoch: *epoch,
            next_position: next_position
                .checked_add(emitted)
                .ok_or(ReplicaError::EpochOverflow)?,
        })
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub struct PublishingPrimary(ConnectionIdentity);
impl PublishingPrimary {
    pub fn new(connection: ConnectionIdentity) -> Self {
        Self(connection)
    }
    pub fn connection(&self) -> &ConnectionIdentity {
        &self.0
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub struct HotStandby(ConnectionIdentity);
impl HotStandby {
    pub fn new(connection: ConnectionIdentity) -> Self {
        Self(connection)
    }
    pub fn connection(&self) -> &ConnectionIdentity {
        &self.0
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub struct RecoveryReplica(ConnectionIdentity);
impl RecoveryReplica {
    pub fn new(connection: ConnectionIdentity) -> Self {
        Self(connection)
    }
    pub fn connection(&self) -> &ConnectionIdentity {
        &self.0
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum StandbyState {
    Agreeing,
    Divergent(DivergenceReason),
    Failed(ReplicaFailureReason),
}
/// Why a standby's state is not the authoritative one.
///
/// [`Self::ContentMismatch`] is two comparable histories that disagree: both replicas hold
/// an intact stream and their canonical economic states differ. [`Self::ContinuityMismatch`]
/// is the absence of a comparable history: the standby has accepted no base yet, or its own
/// stream broke and has not been rebased.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum DivergenceReason {
    ContentMismatch,
    ContinuityMismatch,
}
/// Why a standby is not maintaining shadow state at all.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum ReplicaFailureReason {
    Disconnect,
    Protocol,
    Overload,
}
/// One socket of a publishing pool, named by the connection occupying it.
///
/// A pool socket is not a standby: while the pool publishes, every socket's arrivals are
/// eligible for the same publish gate, and none of them is the market's single source.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub struct PoolSocket(ConnectionIdentity);
impl PoolSocket {
    pub fn new(connection: ConnectionIdentity) -> Self {
        Self(connection)
    }
    pub fn connection(&self) -> &ConnectionIdentity {
        &self.0
    }
}

/// What one pool socket currently contributes to a book's coverage.
///
/// [`Self::Covering`] is the only state whose arrivals can reach the publish gate: the
/// connection is subscribed and its transport evidence is unexpired. Everything else names
/// why the socket is not covering, so a consumer reading `sockets` against `capacity` can
/// tell missing coverage from failed coverage.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum PoolSocketState {
    Covering,
    Establishing,
    Failed(ReplicaFailureReason),
}

/// Why a pool withdrew its own licence to publish across sockets.
///
/// The first three are violations of the recorded conformance basis a pool is opted in
/// against, one per clause of it. [`Self::ConnectionInversion`] is one connection's own key
/// stream going backwards, which contradicts the within-session ordering the venue's key
/// was declared to have. [`Self::ReconnectRewind`] is a replacement connection's first key
/// falling below the last published key, which contradicts the recorded observation that
/// the counter continues across a reconnect rather than resetting.
/// [`Self::EqualKeyContentMismatch`] is two frames carrying one key and different content,
/// which contradicts equality meaning "the same frame".
///
/// [`Self::KeyUnavailable`] is not a violation. It is the gate losing its input: the venue
/// no longer supplying a key it can order, or an arrival whose content evidence it could not
/// keep. A condition a pool cannot check is not a condition it may keep publishing under, so
/// it is answered the same way.
///
/// All four are one-way for the process: a degraded pool never re-arms, because the
/// evidence that licensed it was recorded before the run and a run cannot re-establish it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PoolDegradeReason {
    ConnectionInversion,
    ReconnectRewind,
    EqualKeyContentMismatch,
    KeyUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SourceState(SourceStateKind);
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
enum SourceStateKind {
    NoSource,
    Publishing {
        primary: PublishingPrimary,
        standbys: BTreeMap<HotStandby, StandbyState>,
        standby_capacity: usize,
    },
    Pooled {
        sockets: BTreeMap<PoolSocket, PoolSocketState>,
        socket_capacity: usize,
        last_published_key: Option<DedupKey>,
        degraded: Option<PoolDegradeReason>,
    },
    Recovering {
        recovery: RecoveryReplica,
    },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaError {
    StandbyCapacityZero,
    StandbyCapacityExceeded,
    PrimaryUsedAsStandby,
    DuplicateStandby,
    PoolCapacityZero,
    PoolCapacityExceeded,
    DuplicatePoolSocket,
    EpochOverflow,
    InvalidTransition,
}
impl std::fmt::Display for ReplicaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid replica state")
    }
}
impl std::error::Error for ReplicaError {}
impl SourceState {
    pub fn no_source() -> Self {
        Self(SourceStateKind::NoSource)
    }
    pub fn recovering(recovery: RecoveryReplica) -> Self {
        Self(SourceStateKind::Recovering { recovery })
    }
    pub fn primary(primary: PublishingPrimary) -> Self {
        Self(SourceStateKind::Publishing {
            primary,
            standbys: BTreeMap::new(),
            standby_capacity: 0,
        })
    }
    pub fn with_hot_standbys(
        primary: PublishingPrimary,
        standbys: impl IntoIterator<Item = HotStandby>,
        capacity: usize,
    ) -> Result<Self, ReplicaError> {
        if capacity == 0 {
            return Err(ReplicaError::StandbyCapacityZero);
        }
        let mut values = BTreeMap::new();
        for standby in standbys {
            if primary.connection() == standby.connection() {
                return Err(ReplicaError::PrimaryUsedAsStandby);
            }
            if values.contains_key(&standby) {
                return Err(ReplicaError::DuplicateStandby);
            }
            if values.len() == capacity {
                return Err(ReplicaError::StandbyCapacityExceeded);
            }
            let _ = values.insert(standby, StandbyState::Agreeing);
        }
        Ok(Self(SourceStateKind::Publishing {
            primary,
            standbys: values,
            standby_capacity: capacity,
        }))
    }
    /// A book published by a socket pool rather than by a single source.
    ///
    /// `sockets` are the connections currently assigned, which is at most `capacity` and
    /// may be fewer while a dead socket is being replaced; the shortfall is exactly the
    /// coverage a consumer can see it has lost. `last_published_key` is the venue key of
    /// the arrival the gate last published, which is the value every later arrival is
    /// judged against. `degraded` is `None` while the pool publishes; a pool reports it set
    /// exactly once, at the transition that hands the book back to one publishing primary.
    ///
    /// What a run actually delivers of `last_published_key` is its value at each topology
    /// transition, at the degrade, and in the end-of-run summary, plus the per-update
    /// conformance lines a run asks for explicitly. A transition is deliberately not emitted
    /// for a change of published key alone, which would put a notice on the update path for
    /// every book update. A durable per-update consumer surface for pool coverage is
    /// shared-memory work and is not part of this type.
    ///
    /// Fails with [`ReplicaError::PoolCapacityZero`] for a capacity of zero,
    /// [`ReplicaError::PoolCapacityExceeded`] for more sockets than capacity, and
    /// [`ReplicaError::DuplicatePoolSocket`] when one connection is offered twice.
    pub fn pooled(
        sockets: impl IntoIterator<Item = (PoolSocket, PoolSocketState)>,
        capacity: usize,
        last_published_key: Option<DedupKey>,
        degraded: Option<PoolDegradeReason>,
    ) -> Result<Self, ReplicaError> {
        if capacity == 0 {
            return Err(ReplicaError::PoolCapacityZero);
        }
        let mut values = BTreeMap::new();
        for (socket, state) in sockets {
            if values.contains_key(&socket) {
                return Err(ReplicaError::DuplicatePoolSocket);
            }
            if values.len() == capacity {
                return Err(ReplicaError::PoolCapacityExceeded);
            }
            let _ = values.insert(socket, state);
        }
        Ok(Self(SourceStateKind::Pooled {
            sockets: values,
            socket_capacity: capacity,
            last_published_key,
            degraded,
        }))
    }
    /// The pool's sockets and what each currently contributes, ordered by connection
    /// identity. Empty for every non-pooled topology.
    pub fn pool_sockets(&self) -> impl Iterator<Item = (&PoolSocket, &PoolSocketState)> {
        let sockets = match &self.0 {
            SourceStateKind::Pooled { sockets, .. } => Some(sockets),
            _ => None,
        };
        sockets.into_iter().flat_map(BTreeMap::iter)
    }
    /// How many pool sockets are currently able to deliver, which is the coverage a
    /// consumer actually has. Zero for every non-pooled topology.
    pub fn pool_covering(&self) -> usize {
        self.pool_sockets()
            .filter(|(_, state)| matches!(state, PoolSocketState::Covering))
            .count()
    }
    /// How many sockets this pool was configured to hold. Zero for every non-pooled
    /// topology.
    pub fn pool_capacity(&self) -> usize {
        match &self.0 {
            SourceStateKind::Pooled {
                socket_capacity, ..
            } => *socket_capacity,
            _ => 0,
        }
    }
    /// The venue key of the arrival the pool last published, or `None` before its first
    /// publication and for every non-pooled topology.
    pub fn last_published_key(&self) -> Option<&DedupKey> {
        match &self.0 {
            SourceStateKind::Pooled {
                last_published_key, ..
            } => last_published_key.as_ref(),
            _ => None,
        }
    }
    /// Why the pool stopped publishing across its sockets, or `None` while it still does.
    pub fn pool_degraded(&self) -> Option<PoolDegradeReason> {
        match &self.0 {
            SourceStateKind::Pooled { degraded, .. } => *degraded,
            _ => None,
        }
    }
    pub fn report_standby(&mut self, standby: &HotStandby, state: StandbyState) -> bool {
        match &mut self.0 {
            SourceStateKind::Publishing { standbys, .. } => standbys
                .get_mut(standby)
                .map(|stored| {
                    *stored = state;
                })
                .is_some(),
            _ => false,
        }
    }
    pub fn standby_capacity(&self) -> usize {
        match &self.0 {
            SourceStateKind::Publishing {
                standby_capacity, ..
            } => *standby_capacity,
            _ => 0,
        }
    }
    /// The connection currently publishing this market's authoritative book, or `None` when
    /// no source is publishing.
    pub fn publishing_primary(&self) -> Option<&PublishingPrimary> {
        match &self.0 {
            SourceStateKind::Publishing { primary, .. } => Some(primary),
            _ => None,
        }
    }
    /// The assigned hot standbys and what each one's comparison currently says, ordered by
    /// connection identity. Empty for every non-publishing topology.
    pub fn standbys(&self) -> impl Iterator<Item = (&HotStandby, &StandbyState)> {
        let standbys = match &self.0 {
            SourceStateKind::Publishing { standbys, .. } => Some(standbys),
            _ => None,
        };
        standbys.into_iter().flat_map(BTreeMap::iter)
    }
    /// The connection being driven to obtain a fresh authoritative base, or `None` when the
    /// topology is not recovering.
    pub fn recovery(&self) -> Option<&RecoveryReplica> {
        match &self.0 {
            SourceStateKind::Recovering { recovery } => Some(recovery),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct MutationCursor {
    epoch: u64,
    position: u64,
}
impl MutationCursor {
    pub fn new(epoch: u64, position: u64) -> Self {
        Self { epoch, position }
    }
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    pub fn position(&self) -> u64 {
        self.position
    }
}
/// What one consumer's attachment to a book currently is.
///
/// The two stopped states name different facts. [`Self::Overrun`] is a consumer the writer
/// outran: the ring dropped deliveries this attachment needed. [`Self::Rebased`] is a
/// consumer whose stream was replaced under it — a recovery base or a divergence checkpoint
/// opened a new continuity epoch, and the wholesale state replacement that came with it was
/// published as state rather than as mutations. No ring delivery was dropped there; the
/// attachment simply no longer describes a position in the stream. Both are cleared only by
/// reattaching.
///
/// [`Self::Incompatible`] is reserved for attachment-compatibility failure — a reader that
/// cannot validate the surface it attached through, such as a shared-memory header or ABI
/// it does not understand. It is deliberately not used for either stopped state above.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum ConsumerState {
    Detached,
    Attached {
        cursor: MutationCursor,
        continuous: bool,
    },
    Overrun {
        cursor: MutationCursor,
    },
    Rebased {
        cursor: MutationCursor,
    },
    Incompatible,
    Coalesced {
        cursor: MutationCursor,
    },
}

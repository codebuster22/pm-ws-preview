//! The venue-agnostic pooled publish gate.
//!
//! A pool is several connections to one venue, each subscribed to the same market and each
//! delivering the same stream. Every arrival is judged by one rule: the first arrival whose
//! venue key exceeds the last published key becomes the book's next state, whichever socket
//! carried it; an equal key is the same frame arriving again and is dropped as a duplicate,
//! once its content has been checked to be exactly equal; a lower key is ordinary
//! cross-connection skew and is dropped as a stale arrival. Nothing ever rolls the published
//! key back, so nothing can roll the published book back.
//!
//! Publishing across sockets by arrival is stronger than what any venue documents today.
//! It is licensed here only under a venue whose key semantics order frames within a
//! connection-session ([`DedupKeySemantics::admits_pooled_publish`]) and only against
//! conformance evidence an operator recorded in that venue's contract document. Because
//! that licence rests on observation rather than documentation, this gate carries a live
//! tripwire: the first arrival that contradicts the recorded basis — or that leaves it
//! uncheckable — withdraws the licence for the rest of the process, and the caller falls
//! back to one publishing primary with independent hot standbys.
//!
//! The gate publishes nothing itself. It reads keys and the exact content each arrival
//! reported, returns a verdict, and remembers what it has seen in a bounded window of the
//! newest observed keys; applying an arrival to a book, and telling the gate which
//! application succeeded, is the caller's job. Content is compared byte for byte and never
//! through a fingerprint, because a fingerprint collision would mask the one observation the
//! licence depends on never happening.
//!
//! The window is what makes the content check affordable, and it is deliberately what the
//! check is scoped to: an arrival whose key the window no longer holds is judged by key
//! alone. A venue key stream is not required to be contiguous, so a key the window never
//! held and a key it has dropped are indistinguishable here, and treating either as evidence
//! of a violation would degrade healthy pools on ordinary sparse streams. What the window
//! buys is that every mismatch inside it is caught exactly; what it costs is that a mismatch
//! older than it is undetectable by construction.

use crate::{
    CandidateProjection, ContentDigest, DedupKey, DedupKeyDeclaration, DedupKeySemantics,
    KeyRelation, PoolDegradeReason, classify_key,
};
use core::fmt;
use std::collections::VecDeque;

/// The fewest sockets a pool can be built from. One socket is not a pool: it is the
/// single-source topology, and running it through a cross-socket gate would add a way to
/// drop frames without adding any coverage.
pub const MIN_POOL_SOCKETS: usize = 2;

/// The most arrivals one pool keeps exact content evidence for.
///
/// Sized against observed cross-connection skew rather than against memory: the recorded
/// conformance sessions saw two connections deliver identical key streams, so the number of
/// keys in flight between the newest arrival and the oldest an equal key could still arrive
/// for is a handful. Sixty-four is that handful with two orders of magnitude of headroom,
/// which is what makes eviction a fault path rather than a routine one.
pub const MAX_EVIDENCE_ENTRIES: usize = 64;

/// The most bytes one pool's exact content evidence may occupy.
///
/// The second half of the bound, because [`MAX_EVIDENCE_ENTRIES`] alone bounds nothing: one
/// entry is one arrival's projected levels, and a venue that starts sending far deeper books
/// would otherwise grow this store without limit. At the deepest book this adapter accepts
/// it is the binding half; at observed depths the entry count is.
pub const MAX_EVIDENCE_BYTES: usize = 1 << 20;

/// The largest single projection a pool will hold evidence for.
///
/// [`MAX_EVIDENCE_BYTES`] alone does not bound this store intrinsically, because the ledger
/// keeps its newest entry even when that entry alone exceeds the total: evidence for one
/// arrival is worth more than evidence for none. This is what makes that exception bounded.
/// An arrival projecting larger is one whose evidence the gate cannot maintain, so it is
/// answered as a lost input rather than admitted unrecorded — the same reasoning as a key
/// the gate cannot order.
///
/// It is unreachable for any venue this adapter accepts today: the deepest admitted book is
/// `MAX_BOOK_LEVELS` levels, whose canonical projection is an order of magnitude smaller.
/// The store is therefore bounded by [`MAX_EVIDENCE_BYTES`] in every reachable case, and by
/// this value in the one case that would otherwise be unbounded.
pub const MAX_EVIDENCE_PROJECTION_BYTES: usize = 256 * 1024;

/// The most sockets one pool may hold for one market.
///
/// The binding ceiling in practice is the caller's own configuration, not this number: a
/// pool of `n` peaks at `2n` sockets, because each socket may hold one live connection plus
/// one fenced connection still draining. A caller that configures a lower connection budget
/// for its run — an operator-declared cap on the daemon side, `--replicas` or `--pool` on
/// the single-market run tool — is bound by that budget well before reaching this constant;
/// this is only the structural ceiling the gate itself enforces, whatever a caller allows.
pub const MAX_POOL_SOCKETS: usize = 4;

/// One market's bounded window of what each recently observed venue key actually carried.
///
/// It exists so that "equal keys carry equal content" — a licence condition a pool cannot
/// keep publishing without — is checked against the arrivals themselves rather than against
/// a fingerprint of them. Entries are held in observation order and bounded twice, by
/// [`MAX_EVIDENCE_ENTRIES`] and by [`MAX_EVIDENCE_BYTES`]; whichever binds first, the oldest
/// entries are dropped until both hold. The byte bound never drops the newest entry, which
/// is what [`MAX_EVIDENCE_PROJECTION_BYTES`] bounds instead.
///
/// Eviction is silent by design. A venue key stream need not be contiguous, so a key this
/// window never held and a key it has dropped cannot be told apart from the keys themselves;
/// answering either as a violation would degrade a healthy pool on an ordinary sparse
/// stream. The window is sized so that the distinction rarely matters: live cross-socket
/// skew was observed to be adjacent-key, and the window holds the newest
/// [`MAX_EVIDENCE_ENTRIES`] keys, which is orders of magnitude more.
struct EvidenceLedger {
    entries: VecDeque<(DedupKey, CandidateProjection)>,
    bytes: usize,
}

impl EvidenceLedger {
    const fn new() -> Self {
        Self {
            entries: VecDeque::new(),
            bytes: 0,
        }
    }

    fn evidence_for(&self, key: &DedupKey) -> Option<&CandidateProjection> {
        self.entries
            .iter()
            .find(|(recorded, _)| recorded == key)
            .map(|(_, projection)| projection)
    }

    fn record(&mut self, key: DedupKey, projection: CandidateProjection) {
        self.bytes = self.bytes.saturating_add(projection.len());
        self.entries.push_back((key, projection));
        while self.entries.len() > MAX_EVIDENCE_ENTRIES
            || (self.bytes > MAX_EVIDENCE_BYTES && self.entries.len() > 1)
        {
            let Some((_, projection)) = self.entries.pop_front() else {
                break;
            };
            self.bytes = self.bytes.saturating_sub(projection.len());
        }
    }
}

/// A reason a pool could not be built.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PoolError {
    /// The venue's key grants no order within a connection-session, so a pool on it could
    /// neither choose the newer arrival nor recognize a violation.
    KeyOrdersNothing(DedupKeySemantics),
    SocketCountOutOfRange,
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KeyOrdersNothing(_) => {
                f.write_str("the venue's dedup key grants no within-session ordering")
            }
            Self::SocketCountOutOfRange => f.write_str("pool socket count out of range"),
        }
    }
}

impl std::error::Error for PoolError {}

/// What the gate says about one arrival.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PoolVerdict {
    /// This arrival carries state newer than anything published: apply it to the book, and
    /// report the outcome with [`PoolGate::committed`].
    Publish,
    /// The same frame arriving again on another socket, which is exactly what a pool is
    /// for. Nothing to publish.
    Duplicate,
    /// An arrival older than what is already published — ordinary cross-connection skew,
    /// not a violation of anything.
    Stale,
    /// The recorded basis for pooled publishing no longer holds, or the gate lost the key
    /// it runs on. The licence is withdrawn from this instant.
    Degrade(PoolViolation),
    /// The licence was already withdrawn. The gate publishes nothing and counts nothing.
    Disarmed,
}

/// What the gate observed when it withdrew the pool's licence, kept exactly so a record
/// names the frames rather than the conclusion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PoolViolation {
    pub reason: PoolDegradeReason,
    pub socket: usize,
    pub observed_key: Option<DedupKey>,
    /// What the observation was judged against: the socket's own previous key for an
    /// inversion, the same key for a content mismatch, the published floor for a reconnect
    /// rewind, and the highest evicted key for expired evidence.
    pub previous_key: Option<DedupKey>,
    /// The key the pool had last published when the licence was withdrawn, which is the
    /// state a reader of this record can tie the book to.
    pub published_key: Option<DedupKey>,
    /// Fingerprints for reading, never for gating: the tripwire compared projections.
    pub observed_digest: ContentDigest,
    pub previous_digest: Option<ContentDigest>,
}

/// One socket's own history: the last key it delivered on its current connection-session,
/// and how often its arrivals were the ones published.
#[derive(Clone, Debug, Default)]
struct SocketRecord {
    last_key: Option<DedupKey>,
    /// Whether this socket has already hosted a connection-session on this pool, which is
    /// what makes its next first key a *replacement's* first key rather than the pool's
    /// first sight of that socket. Only a replacement's first key is judged against the
    /// published floor: a socket joining a pool that is already publishing may legitimately
    /// be handed a slightly earlier frame by the venue, while a replacement's counter was
    /// recorded continuing from where its predecessor left off.
    replaced: bool,
    published: u64,
}

/// The publish gate for one market's socket pool.
///
/// It holds three separate facts, and conflating any two of them would be a bug:
///
/// - each socket's own last key, which is what makes a per-connection inversion visible
///   and which is cleared when that socket's connection is replaced, because the venue's
///   ordering is declared per connection-session;
/// - an [`EvidenceLedger`] of what the newest observed keys actually carried, which is what
///   makes "equal keys carry equal content" checkable exactly, from any socket, anywhere
///   inside that window;
/// - the last key actually published, which is what the publish decision reads and which is
///   also the floor a replacement connection's first key must not fall below.
///
/// The ledger and the published key differ whenever an arrival was observed but its
/// application to the book failed: the observation stands, the publication did not, and a
/// later delivery of that same key must still be able to publish.
pub struct PoolGate {
    declaration: DedupKeyDeclaration,
    sockets: Vec<SocketRecord>,
    last_published: Option<DedupKey>,
    last_publisher: Option<usize>,
    evidence: EvidenceLedger,
    degraded: Option<PoolDegradeReason>,
    published: u64,
    duplicate_drops: u64,
    stale_drops: u64,
}

impl PoolGate {
    /// Builds a gate for `sockets` connections publishing under `declaration`.
    ///
    /// Fails with [`PoolError::KeyOrdersNothing`] when the venue's declared key semantics
    /// do not order frames within a connection-session — the licence this gate can be
    /// opted into does not exist for such a venue — and with
    /// [`PoolError::SocketCountOutOfRange`] outside
    /// [`MIN_POOL_SOCKETS`]..=[`MAX_POOL_SOCKETS`].
    pub fn new(declaration: DedupKeyDeclaration, sockets: usize) -> Result<Self, PoolError> {
        if !declaration.semantics().admits_pooled_publish() {
            return Err(PoolError::KeyOrdersNothing(declaration.semantics()));
        }
        if !(MIN_POOL_SOCKETS..=MAX_POOL_SOCKETS).contains(&sockets) {
            return Err(PoolError::SocketCountOutOfRange);
        }
        Ok(Self {
            declaration,
            sockets: vec![SocketRecord::default(); sockets],
            last_published: None,
            last_publisher: None,
            evidence: EvidenceLedger::new(),
            degraded: None,
            published: 0,
            duplicate_drops: 0,
            stale_drops: 0,
        })
    }

    /// Judges one arrival delivered on `socket`, carrying the venue's `key` and the exact
    /// `projection` of its own reported content.
    ///
    /// `key` is `None` when the venue supplied no key the caller could carry exactly. A key
    /// of a kind [`classify_key`] cannot order — anything but an integer — and a projection
    /// larger than [`MAX_EVIDENCE_PROJECTION_BYTES`] are refused on the same footing: an
    /// arrival the gate can neither order against the next one nor keep evidence for is one
    /// it cannot go on publishing under.
    ///
    /// The remaining checks run in the order of the evidence they carry, and each of the
    /// first three withdraws the licence rather than publishing:
    ///
    /// 1. one key carrying content the window says that key already carried. This is checked
    ///    first deliberately: it names two projections and two fingerprints, where an
    ///    ordering violation names only two keys, and an arrival that is both a mismatch and
    ///    a backward step is better reported as the mismatch. A backward step whose content
    ///    the window confirms is still reported as the ordering violation it is.
    /// 2. a socket contradicting its own declared within-session ordering;
    /// 3. a replacement connection whose first key falls below the last published key, which
    ///    is the venue's counter having gone backwards across a reconnect — the conformance
    ///    basis records it continuing.
    ///
    /// Only the publish decision that follows can return [`PoolVerdict::Publish`]. A key
    /// below the published key that the window holds no evidence for is ordinary skew and is
    /// dropped with no content requirement at all, whether the window never held it or has
    /// since dropped it: see [`EvidenceLedger`].
    ///
    /// A [`PoolVerdict::Publish`] does not advance the published key. The caller applies the
    /// arrival and calls [`Self::committed`] on success, so an application that failed
    /// leaves the key free to publish when it next arrives.
    ///
    /// An out-of-range `socket` is treated as [`PoolVerdict::Disarmed`]: a caller that
    /// cannot name which socket delivered an arrival cannot be publishing across sockets.
    pub fn admit(
        &mut self,
        socket: usize,
        key: Option<&DedupKey>,
        projection: CandidateProjection,
    ) -> PoolVerdict {
        if self.degraded.is_some() || socket >= self.sockets.len() {
            return PoolVerdict::Disarmed;
        }
        let semantics = self.declaration.semantics();
        let digest = projection.digest();
        let Some(key) = key else {
            return self.withdraw(
                PoolDegradeReason::KeyUnavailable,
                socket,
                None,
                None,
                digest,
                None,
            );
        };
        if key.as_integer().is_none() || projection.len() > MAX_EVIDENCE_PROJECTION_BYTES {
            return self.withdraw(
                PoolDegradeReason::KeyUnavailable,
                socket,
                Some(key.clone()),
                None,
                digest,
                None,
            );
        }
        let recorded = match self.evidence.evidence_for(key) {
            Some(recorded) if recorded != &projection => {
                let previous = recorded.digest();
                return self.withdraw(
                    PoolDegradeReason::EqualKeyContentMismatch,
                    socket,
                    Some(key.clone()),
                    Some(key.clone()),
                    digest,
                    Some(previous),
                );
            }
            Some(_) => true,
            None => false,
        };
        match self.sockets[socket].last_key.clone() {
            Some(last) => {
                if classify_key(semantics, Some(&last), key) == KeyRelation::Inversion {
                    return self.withdraw(
                        PoolDegradeReason::ConnectionInversion,
                        socket,
                        Some(key.clone()),
                        Some(last),
                        digest,
                        None,
                    );
                }
            }
            None if self.sockets[socket].replaced => {
                let floor = self.last_published.clone();
                if floor.as_ref().is_some_and(|floor| {
                    classify_key(semantics, Some(floor), key) == KeyRelation::Inversion
                }) {
                    return self.withdraw(
                        PoolDegradeReason::ReconnectRewind,
                        socket,
                        Some(key.clone()),
                        floor,
                        digest,
                        None,
                    );
                }
            }
            None => {}
        }
        if !recorded {
            self.evidence.record(key.clone(), projection);
        }
        if matches!(
            classify_key(semantics, self.sockets[socket].last_key.as_ref(), key),
            KeyRelation::First | KeyRelation::Advance
        ) {
            self.sockets[socket].last_key = Some(key.clone());
        }
        match classify_key(semantics, self.last_published.as_ref(), key) {
            KeyRelation::First | KeyRelation::Advance => PoolVerdict::Publish,
            KeyRelation::Duplicate => {
                self.duplicate_drops = self.duplicate_drops.saturating_add(1);
                PoolVerdict::Duplicate
            }
            KeyRelation::Inversion => {
                self.stale_drops = self.stale_drops.saturating_add(1);
                PoolVerdict::Stale
            }
            KeyRelation::Unordered => PoolVerdict::Disarmed,
        }
    }

    /// Whether the window already holds evidence that `key` carried exactly `projection`.
    ///
    /// Read before [`Self::admit`] takes the projection, by a caller that needs to know
    /// whether a repeat of the published key is a confirmed redelivery of the state the book
    /// holds — which is what makes it usable as a recovery base — rather than the first
    /// sight of that key.
    pub fn evidence_matches(&self, key: &DedupKey, projection: &CandidateProjection) -> bool {
        self.evidence
            .evidence_for(key)
            .is_some_and(|recorded| recorded == projection)
    }

    /// Records that the arrival `socket` delivered under `key` reached the book.
    ///
    /// This is what advances the published key, so it is the caller's statement that the
    /// book now holds that state — never the gate's assumption that it does.
    pub fn committed(&mut self, socket: usize, key: DedupKey) {
        self.last_published = Some(key);
        self.last_publisher = Some(socket);
        self.published = self.published.saturating_add(1);
        if let Some(record) = self.sockets.get_mut(socket) {
            record.published = record.published.saturating_add(1);
        }
    }

    /// Records that an arrival `socket` delivered at the published key was installed as a
    /// recovery base rather than published as newer state.
    ///
    /// It counts as coverage this socket delivered, because it is exactly that, and it is
    /// taken back out of the duplicate-drop count it was judged into, so the two totals stay
    /// a partition of what arrived rather than double-counting one frame. It does not
    /// move the published key: the book was rebased onto the state that key already named,
    /// so nothing newer has been published and the gate must go on judging later arrivals
    /// against the same floor.
    pub fn recovered(&mut self, socket: usize) {
        self.duplicate_drops = self.duplicate_drops.saturating_sub(1);
        self.published = self.published.saturating_add(1);
        if let Some(record) = self.sockets.get_mut(socket) {
            record.published = record.published.saturating_add(1);
        }
    }

    /// Records that the connection which occupied socket `from` now occupies socket `to`.
    ///
    /// Only the connection-session history moves. A socket's publication count stays with
    /// the socket, because it answers how much coverage that position of the pool has been
    /// worth; the last key belongs to the connection-session, and reading it against a
    /// different connection's stream would manufacture an inversion out of a role change.
    pub fn reassign_socket(&mut self, from: usize, to: usize) {
        if from == to || from >= self.sockets.len() || to >= self.sockets.len() {
            return;
        }
        let moved = self.sockets[from].last_key.take();
        self.sockets[to].last_key = moved;
        self.sockets[to].replaced = self.sockets[from].replaced;
        self.sockets[from].replaced = true;
        if self.last_publisher == Some(from) {
            self.last_publisher = Some(to);
        }
    }

    /// Records that the connections occupying sockets `a` and `b` exchanged positions,
    /// moving each one's session history with it.
    pub fn exchange_sockets(&mut self, a: usize, b: usize) {
        if a == b || a >= self.sockets.len() || b >= self.sockets.len() {
            return;
        }
        self.sockets.swap(a, b);
        let published_a = self.sockets[a].published;
        self.sockets[a].published = self.sockets[b].published;
        self.sockets[b].published = published_a;
        self.last_publisher = match self.last_publisher {
            Some(socket) if socket == a => Some(b),
            Some(socket) if socket == b => Some(a),
            other => other,
        };
    }

    /// The socket that carried the arrival the gate last published, or `None` before its
    /// first publication.
    pub fn last_publisher(&self) -> Option<usize> {
        self.last_publisher
    }

    /// Clears one socket's own key history, because a fresh connection now occupies it.
    ///
    /// The venue's ordering is declared per connection-session, so a fresh connection's
    /// first key is evidence of nothing about whatever preceded it and must not be read as
    /// an inversion. It says nothing about whether this socket ever held a session before —
    /// that is [`Self::retire_socket`]'s job — so a socket's first-ever connection stays a
    /// first-ever connection and its first key is judged only against what is published.
    pub fn reset_socket(&mut self, socket: usize) {
        if let Some(record) = self.sockets.get_mut(socket) {
            record.last_key = None;
        }
    }

    /// Records that the connection-session occupying `socket` has ended, so the next one is
    /// a replacement.
    ///
    /// What the recorded conformance says about a replacement is that the venue's counter
    /// continues rather than resetting, which is a claim only a replacement's first key can
    /// contradict. A socket that has never held a session makes no such claim, and its first
    /// key is judged only against what is published. The published key itself is untouched:
    /// what the book holds does not change because a socket was retired.
    pub fn retire_socket(&mut self, socket: usize) {
        if let Some(record) = self.sockets.get_mut(socket) {
            record.last_key = None;
            record.replaced = true;
        }
    }

    /// Withdraws the licence with an operator-initiated reason rather than an observed one,
    /// and reports whether this call was the one that withdrew it.
    pub fn degrade(&mut self, reason: PoolDegradeReason) -> bool {
        if self.degraded.is_some() {
            return false;
        }
        self.degraded = Some(reason);
        true
    }

    fn withdraw(
        &mut self,
        reason: PoolDegradeReason,
        socket: usize,
        observed_key: Option<DedupKey>,
        previous_key: Option<DedupKey>,
        observed_digest: ContentDigest,
        previous_digest: Option<ContentDigest>,
    ) -> PoolVerdict {
        self.degraded = Some(reason);
        PoolVerdict::Degrade(PoolViolation {
            reason,
            socket,
            observed_key,
            previous_key,
            published_key: self.last_published.clone(),
            observed_digest,
            previous_digest,
        })
    }

    /// Whether the pool may still publish across its sockets.
    pub fn is_armed(&self) -> bool {
        self.degraded.is_none()
    }

    /// Why the licence was withdrawn, or `None` while it stands.
    pub fn degraded(&self) -> Option<PoolDegradeReason> {
        self.degraded
    }

    /// The venue key of the arrival the gate last published.
    pub fn last_published(&self) -> Option<&DedupKey> {
        self.last_published.as_ref()
    }

    /// How many sockets this pool holds.
    pub fn sockets(&self) -> usize {
        self.sockets.len()
    }

    /// How many arrivals this pool has published, in total.
    pub fn published(&self) -> u64 {
        self.published
    }

    /// How many arrivals each socket contributed, indexed by socket.
    pub fn published_by_socket(&self) -> Vec<u64> {
        self.sockets.iter().map(|record| record.published).collect()
    }

    /// Arrivals dropped because another socket had already published that same frame.
    pub fn duplicate_drops(&self) -> u64 {
        self.duplicate_drops
    }

    /// Arrivals dropped because the book already held newer state — cross-connection skew,
    /// counted apart from duplicates because it is a different fact about the pool.
    pub fn stale_drops(&self) -> u64 {
        self.stale_drops
    }

    /// How many arrivals the exact-content evidence ledger currently holds, which is
    /// bounded by [`MAX_EVIDENCE_ENTRIES`] and [`MAX_EVIDENCE_BYTES`].
    pub fn evidence_entries(&self) -> usize {
        self.evidence.entries.len()
    }

    /// How many bytes that evidence occupies.
    pub fn evidence_bytes(&self) -> usize {
        self.evidence.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BoundedLevels, Candidate, ConnectionIdentity, DecimalGrammar, Level, LevelCapacity,
        LocalMonotonicTimestamp, MarketRef, NativeIdentifierKind, NativeMarketKey, Origin, Price,
        Provenance, ProvenanceInput, Quantity, ReplicaRole, Representation, Side, SourceTimestamp,
        Venue, candidate_projection,
    };

    const DECLARATION: DedupKeyDeclaration = DedupKeyDeclaration::new(
        "test-venue",
        "bookUpdate",
        "version",
        DedupKeySemantics::SessionMonotone,
    );

    fn gate(sockets: usize) -> PoolGate {
        PoolGate::new(DECLARATION, sockets).expect("the test declaration admits a pool")
    }

    fn key(value: u64) -> DedupKey {
        DedupKey::integer(value)
    }

    /// One snapshot candidate whose only meaningful variable is `size`, so two arrivals
    /// carrying one key either project identically or differ in exactly the depth a venue
    /// would have had to change.
    fn test_market() -> MarketRef {
        MarketRef::new(
            Venue::new("test-venue").expect("a valid venue"),
            NativeMarketKey::new(NativeIdentifierKind::slug(), "m").expect("a valid slug"),
        )
    }

    fn test_grammar() -> DecimalGrammar {
        DecimalGrammar::new(18, 30, true, false).expect("the test decimal grammar is valid")
    }

    fn test_provenance(market: MarketRef) -> Provenance {
        Provenance::new(ProvenanceInput {
            market,
            outcome: None,
            native_family: "bookUpdate".to_owned(),
            source_timestamp: Some(
                SourceTimestamp::new("2026-09-01T00:00:00.000Z").expect("stamp"),
            ),
            source_evidence: crate::BoundedSourceEvidence::new(
                Vec::new(),
                crate::SourceEvidenceCapacity::new(0).expect("capacity"),
            )
            .expect("evidence"),
            daemon_generation: 1,
            connection: ConnectionIdentity::new("test", 1).expect("connection"),
            subscription_generation: 1,
            receive_position: 1,
            commit_position: 1,
            local_receive_time: LocalMonotonicTimestamp::new(1),
            local_commit_time: LocalMonotonicTimestamp::new(1),
            replica: ReplicaRole::PublishingPrimary,
            representation: Representation::VenueNative,
            origin: Origin::SourceReported,
            local_revision: 0,
            continuity_epoch: 0,
        })
        .expect("test provenance is valid")
    }

    /// One snapshot candidate whose only meaningful variable is `size`, so two arrivals
    /// carrying one key either project identically or differ in exactly the depth a venue
    /// would have had to change.
    fn projection(size: u64) -> CandidateProjection {
        let levels = [Level::new(
            Side::Bid,
            Price::parse("0.5", test_grammar()).expect("price"),
            Quantity::parse(&size.to_string(), test_grammar()).expect("quantity"),
        )];
        let candidate = Candidate::snapshot(
            test_provenance(test_market()),
            BoundedLevels::new(levels, LevelCapacity::new(8).expect("capacity")).expect("levels"),
        )
        .expect("snapshot candidate");
        candidate_projection(&candidate)
    }

    /// A projection past [`MAX_EVIDENCE_PROJECTION_BYTES`], which no venue this adapter
    /// accepts can produce: reaching it takes several times the deepest admitted book.
    fn oversized_projection() -> CandidateProjection {
        let levels: Vec<Level> = (0..20_000u32)
            .map(|index| {
                Level::new(
                    Side::Bid,
                    Price::parse("0.5", test_grammar()).expect("price"),
                    Quantity::parse(
                        &(1_000_000_000_000_000_000u64 + u64::from(index)).to_string(),
                        test_grammar(),
                    )
                    .expect("quantity"),
                )
            })
            .collect();
        let candidate = Candidate::snapshot(
            test_provenance(test_market()),
            BoundedLevels::new(levels, LevelCapacity::new(20_000).expect("capacity"))
                .expect("levels"),
        )
        .expect("snapshot candidate");
        candidate_projection(&candidate)
    }

    fn digest(size: u64) -> ContentDigest {
        projection(size).digest()
    }

    /// Admits one arrival and, when the gate publishes it, tells the gate it reached the
    /// book — the two-step every caller performs.
    fn deliver(gate: &mut PoolGate, socket: usize, value: u64, size: u64) -> PoolVerdict {
        let verdict = gate.admit(socket, Some(&key(value)), projection(size));
        if verdict == PoolVerdict::Publish {
            gate.committed(socket, key(value));
        }
        verdict
    }

    fn degrades_with(verdict: &PoolVerdict, reason: PoolDegradeReason) -> bool {
        matches!(verdict, PoolVerdict::Degrade(violation) if violation.reason == reason)
    }

    #[test]
    fn a_dedup_only_key_refuses_a_pool() {
        let declaration = DedupKeyDeclaration::new(
            "test-venue",
            "bookUpdate",
            "id",
            DedupKeySemantics::DedupOnly,
        );
        assert_eq!(
            PoolGate::new(declaration, 2).err(),
            Some(PoolError::KeyOrdersNothing(DedupKeySemantics::DedupOnly)),
            "a key that orders nothing within a session cannot be pooled on"
        );
    }

    #[test]
    fn a_pool_needs_at_least_two_and_at_most_four_sockets() {
        for sockets in [0, 1, MAX_POOL_SOCKETS + 1] {
            assert_eq!(
                PoolGate::new(DECLARATION, sockets).err(),
                Some(PoolError::SocketCountOutOfRange),
                "{sockets} sockets was accepted"
            );
        }
        for sockets in MIN_POOL_SOCKETS..=MAX_POOL_SOCKETS {
            assert!(PoolGate::new(DECLARATION, sockets).is_ok());
        }
    }

    #[test]
    fn an_ordering_proven_key_also_admits_a_pool() {
        let declaration = DedupKeyDeclaration::new(
            "test-venue",
            "bookUpdate",
            "seq",
            DedupKeySemantics::OrderingProven,
        );
        assert!(PoolGate::new(declaration, 2).is_ok());
    }

    #[test]
    fn the_first_arrival_of_each_key_publishes_and_the_rest_are_dedup_drops() {
        let mut gate = gate(2);
        assert_eq!(deliver(&mut gate, 0, 10, 100), PoolVerdict::Publish);
        assert_eq!(deliver(&mut gate, 1, 10, 100), PoolVerdict::Duplicate);
        assert_eq!(deliver(&mut gate, 1, 11, 101), PoolVerdict::Publish);
        assert_eq!(deliver(&mut gate, 0, 11, 101), PoolVerdict::Duplicate);
        assert_eq!(gate.published(), 2);
        assert_eq!(gate.published_by_socket(), vec![1, 1]);
        assert_eq!(gate.duplicate_drops(), 2);
        assert_eq!(gate.stale_drops(), 0);
        assert_eq!(gate.last_published(), Some(&key(11)));
        assert!(gate.is_armed());
    }

    #[test]
    fn an_arrival_behind_the_published_key_is_a_stale_drop_not_a_violation() {
        let mut gate = gate(2);
        assert_eq!(deliver(&mut gate, 0, 10, 100), PoolVerdict::Publish);
        assert_eq!(deliver(&mut gate, 0, 12, 102), PoolVerdict::Publish);
        assert_eq!(
            deliver(&mut gate, 1, 11, 101),
            PoolVerdict::Stale,
            "a socket running behind is cross-connection skew, which is what a pool is for"
        );
        assert!(gate.is_armed(), "skew must never withdraw the licence");
        assert_eq!(gate.stale_drops(), 1);
        assert_eq!(gate.duplicate_drops(), 0);
        assert_eq!(
            gate.last_published(),
            Some(&key(12)),
            "the published key never rolls back"
        );
    }

    #[test]
    fn a_socket_whose_own_key_stream_goes_backwards_withdraws_the_licence() {
        let mut gate = gate(2);
        assert_eq!(deliver(&mut gate, 0, 10, 100), PoolVerdict::Publish);
        assert_eq!(deliver(&mut gate, 0, 12, 102), PoolVerdict::Publish);
        let verdict = gate.admit(0, Some(&key(11)), projection(101));
        assert_eq!(
            verdict,
            PoolVerdict::Degrade(PoolViolation {
                reason: PoolDegradeReason::ConnectionInversion,
                socket: 0,
                observed_key: Some(key(11)),
                previous_key: Some(key(12)),
                published_key: Some(key(12)),
                observed_digest: digest(101),
                previous_digest: None,
            })
        );
        assert!(!gate.is_armed());
        assert_eq!(
            gate.degraded(),
            Some(PoolDegradeReason::ConnectionInversion)
        );
    }

    #[test]
    fn one_socket_running_behind_another_is_not_that_socket_going_backwards() {
        let mut gate = gate(2);
        assert_eq!(deliver(&mut gate, 0, 10, 100), PoolVerdict::Publish);
        assert_eq!(deliver(&mut gate, 0, 12, 102), PoolVerdict::Publish);
        assert_eq!(deliver(&mut gate, 1, 11, 101), PoolVerdict::Stale);
        assert_eq!(
            deliver(&mut gate, 1, 12, 102),
            PoolVerdict::Duplicate,
            "socket 1's own stream advanced 11 then 12, which is no inversion at all"
        );
        assert!(gate.is_armed());
    }

    #[test]
    fn a_sockets_first_ever_connection_is_not_a_replacement_and_owns_no_floor() {
        let mut gate = gate(2);
        assert_eq!(deliver(&mut gate, 0, 20, 100), PoolVerdict::Publish);
        gate.reset_socket(1);
        assert_eq!(
            gate.admit(1, Some(&key(5)), projection(50)),
            PoolVerdict::Stale,
            "a socket the pool is seeing for the first time makes no claim about a counter \
             continuing, so its first key is judged only against what is published"
        );
        assert!(gate.is_armed());
    }

    #[test]
    fn a_replacement_session_whose_first_key_falls_below_the_published_floor_withdraws_it() {
        let mut gate = gate(2);
        assert_eq!(deliver(&mut gate, 0, 20, 100), PoolVerdict::Publish);
        gate.retire_socket(0);
        let verdict = gate.admit(0, Some(&key(5)), projection(50));
        assert_eq!(
            verdict,
            PoolVerdict::Degrade(PoolViolation {
                reason: PoolDegradeReason::ReconnectRewind,
                socket: 0,
                observed_key: Some(key(5)),
                previous_key: Some(key(20)),
                published_key: Some(key(20)),
                observed_digest: digest(50),
                previous_digest: None,
            }),
            "a replacement session has no history of its own to invert against, so the \
             published key is the floor its first key must not fall below"
        );
    }

    #[test]
    fn a_replacement_session_whose_first_key_is_at_the_floor_is_content_checked_like_any_other() {
        let mut gate = gate(2);
        assert_eq!(deliver(&mut gate, 0, 20, 100), PoolVerdict::Publish);
        gate.retire_socket(0);
        assert_eq!(
            gate.admit(0, Some(&key(20)), projection(100)),
            PoolVerdict::Duplicate,
            "the venue's own adjacent duplicate across a reconnect is what the conformance \
             sessions recorded, and it carries the same content"
        );
        assert!(gate.is_armed());

        let mut other = gate2_at_floor();
        assert!(
            degrades_with(
                &other.admit(0, Some(&key(20)), projection(999)),
                PoolDegradeReason::EqualKeyContentMismatch
            ),
            "the same key across the reconnect carrying different content is the violation"
        );
    }

    fn gate2_at_floor() -> PoolGate {
        let mut gate = gate(2);
        assert_eq!(deliver(&mut gate, 0, 20, 100), PoolVerdict::Publish);
        gate.retire_socket(0);
        gate
    }

    #[test]
    fn a_replacement_session_whose_first_key_is_above_the_floor_publishes_normally() {
        let mut gate = gate(2);
        assert_eq!(deliver(&mut gate, 0, 20, 100), PoolVerdict::Publish);
        gate.retire_socket(0);
        assert_eq!(
            deliver(&mut gate, 0, 21, 101),
            PoolVerdict::Publish,
            "the conformance sessions recorded the counter continuing across a reconnect"
        );
        assert!(gate.is_armed());
    }

    #[test]
    fn two_frames_claiming_one_key_with_different_content_withdraw_the_licence() {
        let mut gate = gate(2);
        assert_eq!(deliver(&mut gate, 0, 10, 100), PoolVerdict::Publish);
        let verdict = gate.admit(1, Some(&key(10)), projection(999));
        assert_eq!(
            verdict,
            PoolVerdict::Degrade(PoolViolation {
                reason: PoolDegradeReason::EqualKeyContentMismatch,
                socket: 1,
                observed_key: Some(key(10)),
                previous_key: Some(key(10)),
                published_key: Some(key(10)),
                observed_digest: digest(999),
                previous_digest: Some(digest(100)),
            }),
            "equality that does not mean the same frame voids the basis the pool runs on"
        );
    }

    #[test]
    fn a_delayed_equal_key_far_behind_the_newest_is_still_content_checked() {
        let mut gate = gate(2);
        assert_eq!(deliver(&mut gate, 0, 10, 100), PoolVerdict::Publish);
        assert_eq!(deliver(&mut gate, 0, 11, 101), PoolVerdict::Publish);
        assert_eq!(deliver(&mut gate, 0, 12, 102), PoolVerdict::Publish);
        assert!(
            degrades_with(
                &gate.admit(1, Some(&key(10)), projection(999)),
                PoolDegradeReason::EqualKeyContentMismatch
            ),
            "an arrival behind the newest key is skew only if it agrees with what that key \
             already carried; disagreeing, it is the violation whatever its distance"
        );
    }

    #[test]
    fn a_content_mismatch_is_reported_ahead_of_the_backward_step_that_carried_it() {
        let mut gate = gate(2);
        assert_eq!(deliver(&mut gate, 0, 10, 100), PoolVerdict::Publish);
        assert_eq!(deliver(&mut gate, 0, 12, 102), PoolVerdict::Publish);
        let verdict = gate.admit(0, Some(&key(10)), projection(999));
        assert_eq!(
            verdict,
            PoolVerdict::Degrade(PoolViolation {
                reason: PoolDegradeReason::EqualKeyContentMismatch,
                socket: 0,
                observed_key: Some(key(10)),
                previous_key: Some(key(10)),
                published_key: Some(key(12)),
                observed_digest: digest(999),
                previous_digest: Some(digest(100)),
            }),
            "the arrival is both a mismatch and a backward step, and the mismatch is the \
             record that names two projections rather than two keys"
        );
    }

    #[test]
    fn a_backward_step_whose_content_the_window_confirms_is_still_an_ordering_violation() {
        let mut gate = gate(2);
        assert_eq!(deliver(&mut gate, 0, 10, 100), PoolVerdict::Publish);
        assert_eq!(deliver(&mut gate, 0, 12, 102), PoolVerdict::Publish);
        assert!(
            degrades_with(
                &gate.admit(0, Some(&key(10)), projection(100)),
                PoolDegradeReason::ConnectionInversion
            ),
            "nothing contradicts what key 10 carried, so what is left is the socket \
             contradicting its own ordering"
        );
    }

    #[test]
    fn a_socket_restating_its_own_older_key_with_different_content_withdraws_the_licence() {
        let mut gate = gate(2);
        assert_eq!(deliver(&mut gate, 0, 10, 100), PoolVerdict::Publish);
        assert_eq!(deliver(&mut gate, 0, 11, 101), PoolVerdict::Publish);
        assert!(
            degrades_with(
                &gate.admit(0, Some(&key(11)), projection(999)),
                PoolDegradeReason::EqualKeyContentMismatch
            ),
            "restating a key is not an inversion, so the content check is what catches it"
        );
    }

    #[test]
    fn an_equal_key_repeated_by_the_same_socket_with_equal_content_is_a_duplicate() {
        let mut gate = gate(2);
        assert_eq!(deliver(&mut gate, 0, 10, 100), PoolVerdict::Publish);
        assert_eq!(
            deliver(&mut gate, 0, 10, 100),
            PoolVerdict::Duplicate,
            "the venue's own adjacent redundant delivery is a duplicate, not a violation"
        );
        assert!(gate.is_armed());
    }

    #[test]
    fn a_never_seen_key_below_the_published_key_is_skew_with_no_content_requirement() {
        let mut gate = gate(2);
        assert_eq!(deliver(&mut gate, 0, 10, 100), PoolVerdict::Publish);
        assert_eq!(deliver(&mut gate, 0, 20, 102), PoolVerdict::Publish);
        assert_eq!(
            gate.admit(1, Some(&key(15)), projection(999)),
            PoolVerdict::Stale,
            "the pool never observed key 15, so it makes no claim about what 15 carried"
        );
        assert!(gate.is_armed());
    }

    #[test]
    fn the_evidence_window_is_bounded_and_a_key_older_than_it_is_ordinary_skew() {
        let mut gate = gate(2);
        let entries = u64::try_from(MAX_EVIDENCE_ENTRIES).expect("the bound fits a u64");
        for step in 0..entries + 8 {
            assert_eq!(
                deliver(&mut gate, 0, (step + 1) * 10, step + 100),
                PoolVerdict::Publish
            );
        }
        assert_eq!(
            gate.evidence_entries(),
            MAX_EVIDENCE_ENTRIES,
            "the window stops growing at its entry bound"
        );
        assert!(gate.evidence_bytes() <= MAX_EVIDENCE_BYTES);
        assert_eq!(
            gate.admit(1, Some(&key(5)), projection(999)),
            PoolVerdict::Stale,
            "a venue key stream is not contiguous, so a key the window never held and one \
             it has dropped cannot be told apart; answering either as a violation would \
             degrade a healthy pool on an ordinary sparse stream"
        );
        assert!(gate.is_armed());
        assert_eq!(
            gate.admit(1, Some(&key(15)), projection(999)),
            PoolVerdict::Stale,
            "a key that fell between two observed keys was never evidence of anything"
        );
        assert!(gate.is_armed());
    }

    #[test]
    fn an_arrival_projecting_more_than_the_gate_can_keep_evidence_for_is_a_lost_input() {
        let mut gate = gate(2);
        assert_eq!(deliver(&mut gate, 0, 10, 100), PoolVerdict::Publish);
        let oversized = oversized_projection();
        assert!(
            oversized.len() > MAX_EVIDENCE_PROJECTION_BYTES,
            "the test arrival must actually exceed the per-projection cap"
        );
        assert!(
            degrades_with(
                &gate.admit(1, Some(&key(11)), oversized),
                PoolDegradeReason::KeyUnavailable
            ),
            "an arrival the gate cannot keep evidence for is one it cannot go on publishing \
             under, exactly like a key it cannot order"
        );
    }

    #[test]
    fn an_arrival_without_a_key_withdraws_the_licence_rather_than_stopping_silently() {
        let mut gate = gate(2);
        assert_eq!(deliver(&mut gate, 0, 10, 100), PoolVerdict::Publish);
        let verdict = gate.admit(1, None, projection(101));
        assert_eq!(
            verdict,
            PoolVerdict::Degrade(PoolViolation {
                reason: PoolDegradeReason::KeyUnavailable,
                socket: 1,
                observed_key: None,
                previous_key: None,
                published_key: Some(key(10)),
                observed_digest: digest(101),
                previous_digest: None,
            }),
            "a gate with no key to run on would otherwise publish nothing for ever"
        );
    }

    #[test]
    fn a_key_kind_the_gate_cannot_order_is_refused_before_it_publishes_anything() {
        let mut gate = gate(2);
        let lexeme = DedupKey::lexeme("abc").expect("a valid lexeme key");
        assert!(
            degrades_with(
                &gate.admit(0, Some(&lexeme), projection(100)),
                PoolDegradeReason::KeyUnavailable
            ),
            "an arrival the gate could publish but never order against the next one is a \
             lost input, refused before it reaches the book"
        );
        assert_eq!(gate.published(), 0);
    }

    #[test]
    fn a_failed_publication_leaves_its_key_free_to_publish_again() {
        let mut gate = gate(2);
        assert_eq!(
            gate.admit(0, Some(&key(10)), projection(100)),
            PoolVerdict::Publish
        );
        assert_eq!(
            gate.admit(1, Some(&key(10)), projection(100)),
            PoolVerdict::Publish,
            "nothing told the gate the first application reached the book"
        );
        gate.committed(1, key(10));
        assert_eq!(gate.last_published(), Some(&key(10)));
        assert_eq!(gate.published_by_socket(), vec![0, 1]);
    }

    #[test]
    fn replacing_a_socket_clears_only_its_own_key_history() {
        let mut gate = gate(2);
        assert_eq!(deliver(&mut gate, 0, 20, 100), PoolVerdict::Publish);
        assert_eq!(deliver(&mut gate, 1, 21, 101), PoolVerdict::Publish);
        gate.retire_socket(0);
        assert_eq!(
            deliver(&mut gate, 0, 22, 102),
            PoolVerdict::Publish,
            "a replacement connection's first key is judged against the published floor, \
             not against its predecessor's history"
        );
        assert_eq!(
            deliver(&mut gate, 1, 21, 101),
            PoolVerdict::Stale,
            "the socket that was not replaced keeps its own session history"
        );
        assert!(gate.is_armed());
    }

    #[test]
    fn a_connection_that_changes_socket_carries_its_session_history_with_it() {
        let mut gate = gate(2);
        assert_eq!(deliver(&mut gate, 1, 20, 100), PoolVerdict::Publish);
        assert_eq!(gate.last_publisher(), Some(1));
        gate.reassign_socket(1, 0);
        assert_eq!(
            gate.last_publisher(),
            Some(0),
            "the connection that last published now occupies socket 0"
        );
        assert!(
            degrades_with(
                &gate.admit(1, Some(&key(19)), projection(99)),
                PoolDegradeReason::ReconnectRewind
            ),
            "the vacated socket carries no session history, so its first key is judged \
             against the published floor"
        );
    }

    #[test]
    fn two_connections_that_exchange_sockets_move_their_history_and_not_their_counts() {
        let mut gate = gate(2);
        assert_eq!(deliver(&mut gate, 0, 10, 100), PoolVerdict::Publish);
        assert_eq!(deliver(&mut gate, 0, 11, 101), PoolVerdict::Publish);
        assert_eq!(deliver(&mut gate, 1, 12, 102), PoolVerdict::Publish);
        assert_eq!(gate.published_by_socket(), vec![2, 1]);
        assert_eq!(gate.last_publisher(), Some(1));

        gate.exchange_sockets(0, 1);

        assert_eq!(
            gate.published_by_socket(),
            vec![2, 1],
            "how much coverage each position of the pool has been worth does not move with \
             the connections occupying it"
        );
        assert_eq!(
            gate.last_publisher(),
            Some(0),
            "the connection that last published now occupies socket 0"
        );
        assert_eq!(
            gate.admit(0, Some(&key(12)), projection(102)),
            PoolVerdict::Duplicate,
            "socket 0 now holds the session whose last key was 12"
        );
        assert!(
            degrades_with(
                &gate.admit(1, Some(&key(10)), projection(100)),
                PoolDegradeReason::ConnectionInversion
            ),
            "socket 1 now holds the session whose last key was 11, and 10 contradicts it"
        );
    }

    #[test]
    fn an_operator_degrade_is_one_way() {
        let mut gate = gate(2);
        assert!(gate.degrade(PoolDegradeReason::KeyUnavailable));
        assert!(!gate.degrade(PoolDegradeReason::ConnectionInversion));
        assert_eq!(gate.degraded(), Some(PoolDegradeReason::KeyUnavailable));
        assert!(!gate.is_armed());
    }

    #[test]
    fn a_disarmed_gate_publishes_and_counts_nothing() {
        let mut gate = gate(2);
        assert_eq!(deliver(&mut gate, 0, 10, 100), PoolVerdict::Publish);
        assert!(gate.degrade(PoolDegradeReason::KeyUnavailable));
        assert_eq!(
            gate.admit(1, Some(&key(11)), projection(101)),
            PoolVerdict::Disarmed
        );
        assert_eq!(gate.published(), 1);
        assert_eq!(gate.duplicate_drops(), 0);
        assert_eq!(gate.stale_drops(), 0);
    }

    #[test]
    fn an_arrival_from_a_socket_the_pool_does_not_hold_publishes_nothing() {
        let mut gate = gate(2);
        assert_eq!(
            gate.admit(7, Some(&key(10)), projection(100)),
            PoolVerdict::Disarmed
        );
        assert!(
            gate.is_armed(),
            "an unknown socket is a caller bug, not a venue violation"
        );
        assert_eq!(gate.published(), 0);
    }
}

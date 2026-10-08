//! The authoritative order book: one writer, one physical level map, source-reported
//! deltas, and derived snapshot differences.
//!
//! State changes come from exactly three inputs — an accepted complete snapshot
//! [`Candidate`], an accepted source-delta [`Candidate`], and an explicit evidence-based
//! loss report. There is no time-based input: a quiet subscribed book stays live, because
//! silence is not evidence.

use crate::{
    AuthorityReason, AuthorityState, BookMutation, BoundedSourceEvidence, Candidate,
    CandidateOperation, ContinuityReason, DecimalError, DecimalGrammar, Derivation,
    DescriptorError, ExactDecimal, Level, LiquidityIdentity, MarketRef, MutationContinuity,
    MutationCursor, ObservationError, Origin, Price, Provenance, ProvenanceInput, Quantity,
    ReplicaError, Representation, Side, SourceEvidenceCapacity,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

const DERIVED_FAMILY: &str = "derived.snapshot-diff";

type PhysicalLevels = BTreeMap<(Side, Price), Quantity>;

/// The representation grammar for locally derived prices.
///
/// It is venue-agnostic on purpose: complementing is arithmetic over already-parsed exact
/// decimals, not a re-parse of venue text. Negatives are forbidden, so a price above the
/// unit surfaces as [`DecimalError::NegativeForbidden`] instead of a nonsense level.
fn normalized_grammar() -> DecimalGrammar {
    DecimalGrammar::new(u16::MAX, 39, false, false).expect("normalized grammar is valid")
}

/// The complementary price `1 - price` under the normalized grammar.
///
/// Fails when the complement is not representable there: a price above the unit yields
/// [`DecimalError::NegativeForbidden`], and a scale finer than the unit can be scaled to
/// yields [`DecimalError::ArithmeticOverflow`]. This is the one place complementing is
/// decided, so what [`OrderBook::apply_snapshot`] admits and what
/// [`PublishedBook::derived_complement_levels`] can produce cannot drift apart.
fn complement_price(price: &Price) -> Result<Price, DecimalError> {
    let grammar = normalized_grammar();
    let unit = ExactDecimal::parse("1", grammar).expect("unit literal parses");
    price
        .value()
        .complement(&unit, grammar)
        .and_then(|value| Price::parse(&value.canonical(), grammar))
}

/// A distinct, programmatically matchable reason a book input was rejected or a derived
/// view could not be produced.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BookError {
    MarketMismatch,
    UnsupportedCandidateOperation,
    DuplicateLevelCoordinate,
    EmptyDelta,
    NoEstablishedBase,
    ContinuityLost,
    CapacityOutOfRange,
    CounterOverflow,
    Continuity(ReplicaError),
    Provenance(DescriptorError),
    Mutation(ObservationError),
    NonComplementablePrice(DecimalError),
    Complement(DecimalError),
}

impl std::fmt::Display for BookError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid book input")
    }
}
impl std::error::Error for BookError {}

/// One level change paired with its position in the book's mutation stream.
///
/// Whether the change was reported by the venue or derived here is read from
/// `mutation().provenance().origin()`; the record itself is the same either way.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationRecord {
    cursor: MutationCursor,
    mutation: Arc<BookMutation>,
}
impl MutationRecord {
    pub fn cursor(&self) -> &MutationCursor {
        &self.cursor
    }
    pub fn mutation(&self) -> &Arc<BookMutation> {
        &self.mutation
    }
}

/// A sync snapshot that disagreed with a delta-built book, and what that cost.
///
/// The disagreement is proof that a source delta was missed or misapplied, so the mutation
/// stream between the two snapshots cannot be trusted. `deltas_invalidated` is how many
/// source deltas the book had accepted since its last snapshot commit — the width of the
/// window the break covers, not a count of levels that differed. Zero is meaningful and not
/// a contradiction: the delta that went missing fell in a window that delivered none, which
/// is exactly the case a per-window counter cannot detect on its own.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SyncDivergence {
    deltas_invalidated: u64,
}
impl SyncDivergence {
    pub fn deltas_invalidated(self) -> u64 {
        self.deltas_invalidated
    }
}

/// What one accepted candidate did to the book.
///
/// `mutations` is empty whenever no level's value changed, and additionally whenever the
/// commit is a base rather than a transition: the first accepted snapshot, a recovery base
/// after a reported loss, and a divergence checkpoint. `recovery_base` marks the last two,
/// and the first base is the commit at revision 1. `divergence` is set only for a sync
/// snapshot that disagreed with a delta-built book.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BookCommit {
    revision: u64,
    epoch: u64,
    recovery_base: bool,
    divergence: Option<SyncDivergence>,
    mutations: Vec<MutationRecord>,
}
impl BookCommit {
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    pub fn recovery_base(&self) -> bool {
        self.recovery_base
    }
    /// The divergence this commit reported, or `None` when the commit agreed with the book
    /// it replaced or had no deltas to check against.
    pub fn divergence(&self) -> Option<SyncDivergence> {
        self.divergence
    }
    pub fn mutations(&self) -> &[MutationRecord] {
        &self.mutations
    }
}

/// The single-writer authoritative book for one market.
///
/// Holds one physical level map in canonical outcome coordinates; the opposite outcome is
/// a derived view of those same levels, never a second stored book.
#[derive(Clone, Debug)]
pub struct OrderBook {
    market: MarketRef,
    levels: PhysicalLevels,
    authority: AuthorityState,
    continuity: MutationContinuity,
    revision: u64,
    provenance: Option<Provenance>,
    delta_rail: bool,
    deltas_since_snapshot: u64,
    sync_divergences: u64,
}

impl OrderBook {
    /// Creates an empty book for `market`, awaiting its first snapshot.
    ///
    /// Authority starts [`AuthorityState::Synchronizing`]: the book holds no venue-reported
    /// state yet. Revision starts at 0 and the mutation stream starts intact at epoch 0,
    /// position 0.
    pub fn new(market: MarketRef) -> Self {
        Self {
            market,
            levels: PhysicalLevels::new(),
            authority: AuthorityState::Synchronizing,
            continuity: MutationContinuity::Intact {
                epoch: 0,
                next_position: 0,
            },
            revision: 0,
            provenance: None,
            delta_rail: false,
            deltas_since_snapshot: 0,
            sync_divergences: 0,
        }
    }

    pub fn market(&self) -> &MarketRef {
        &self.market
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn authority(&self) -> &AuthorityState {
        &self.authority
    }
    pub fn continuity(&self) -> &MutationContinuity {
        &self.continuity
    }
    /// Whether this book has ever accepted a source delta.
    ///
    /// Sticky: it turns on at the first accepted delta and never turns off. A book that has
    /// received one belongs to a delta rail, where every change to the book travels as a
    /// delta — so every later snapshot is a checkpoint, forever, and no snapshot difference
    /// is ever derived again. It is process-local routing state, not published state, and
    /// nothing compares it across replicas.
    pub fn delta_rail(&self) -> bool {
        self.delta_rail
    }
    /// How many source deltas this book has accepted since its last snapshot commit.
    ///
    /// Reset by every accepted snapshot, and pure telemetry: it names the width of the
    /// window a divergence checkpoint invalidated, and never decides whether a snapshot is a
    /// checkpoint. That is [`Self::delta_rail`]'s job, because a delta missed entirely
    /// leaves no trace in this counter.
    pub fn deltas_since_snapshot(&self) -> u64 {
        self.deltas_since_snapshot
    }
    /// How many sync snapshots have disagreed with this book since it was created.
    ///
    /// A per-book cumulative count of divergence checkpoints, never reset. It is local
    /// feed-health evidence about this replica's own delta stream, not shared state: two
    /// replicas of the same market legitimately carry different counts, and nothing compares
    /// them.
    pub fn sync_divergences(&self) -> u64 {
        self.sync_divergences
    }

    /// Commits a complete snapshot atomically and returns what it changed.
    ///
    /// The candidate must be a [`CandidateOperation::Snapshot`] for this book's market;
    /// a repeated level coordinate is rejected rather than silently aggregated. Every
    /// level's price must also be complementable, so that a committed book always has a
    /// readable complement view; a snapshot carrying one that is not is rejected whole,
    /// leaving levels, revision, continuity, authority, and published state untouched. When
    /// continuity is intact and a previous snapshot established the base, level differences
    /// against it are emitted as derived mutations in ascending `(side, price)` order, each
    /// carrying the book's own revision and epoch. A difference describes the transition
    /// between two received books, so the first accepted snapshot derives nothing: the empty
    /// initial book is not a venue-reported state. When continuity was lost, this snapshot is
    /// a recovery base: no mutation is derived across the gap and the epoch advances by one.
    /// In every case the
    /// revision advances, published provenance is refreshed, and authority returns to
    /// [`AuthorityState::Live`], and [`Self::deltas_since_snapshot`] returns to zero.
    ///
    /// Once this book has accepted a single source delta it is on a delta rail
    /// ([`Self::delta_rail`]) and every later snapshot is a checkpoint, permanently — not
    /// only those following a window that happened to carry a delta. **A checkpoint is never
    /// a diff source**: a venue that sends its own deltas did not send the transition this
    /// snapshot would be compared against, so no derived mutation is ever emitted on a delta
    /// rail, whichever way the comparison goes. Deriving one would fabricate a correction
    /// for precisely the missed-delta case the checkpoint exists to expose.
    ///
    /// The comparison is economic — exact-zero quantities are excluded on both sides — so a
    /// coordinate one side rests at zero and the other simply omits is agreement, not a
    /// difference. Agreeing is the checkpoint refresh: zero mutations, refreshed provenance,
    /// revision advanced, epoch unchanged, and the snapshot's own level map adopted as
    /// stored state, zero-rest levels included, because it is the venue's authoritative
    /// statement of the book. Disagreeing is proof that a source delta was missed or
    /// misapplied, so it is a mutation-continuity break rather than a level change: the
    /// snapshot commits as a recovery base with the epoch advanced by one, zero mutations
    /// across the gap, and [`SyncDivergence`] on the commit naming how many deltas the break
    /// invalidated — never a fabricated correction diff. That case is the only one that
    /// advances [`Self::sync_divergences`]. A snapshot-built book — no delta since the last
    /// snapshot — is untouched by all of this and always takes the derived-diff path, where
    /// a zero-quantity level rests and diffs like any other.
    ///
    /// Fails with [`BookError::MarketMismatch`], [`BookError::UnsupportedCandidateOperation`],
    /// [`BookError::DuplicateLevelCoordinate`], [`BookError::NonComplementablePrice`],
    /// [`BookError::CounterOverflow`], or a wrapped provenance, continuity, or mutation
    /// error. A failed apply leaves the book unchanged.
    pub fn apply_snapshot(&mut self, candidate: &Candidate) -> Result<BookCommit, BookError> {
        let source = candidate.provenance();
        if source.market() != &self.market {
            return Err(BookError::MarketMismatch);
        }
        let CandidateOperation::Snapshot(levels) = candidate.operation() else {
            return Err(BookError::UnsupportedCandidateOperation);
        };
        let mut next = PhysicalLevels::new();
        for level in levels.levels() {
            complement_price(level.price()).map_err(BookError::NonComplementablePrice)?;
            if next
                .insert(
                    (level.side(), level.price().clone()),
                    level.quantity().clone(),
                )
                .is_some()
            {
                return Err(BookError::DuplicateLevelCoordinate);
            }
        }
        let revision = self
            .revision
            .checked_add(1)
            .ok_or(BookError::CounterOverflow)?;
        let divergence = (self.delta_rail
            && matches!(self.continuity, MutationContinuity::Intact { .. })
            && !economically_equal(&self.levels, &next))
        .then_some(SyncDivergence {
            deltas_invalidated: self.deltas_since_snapshot,
        });
        let sync_divergences = match divergence {
            Some(_) => self
                .sync_divergences
                .checked_add(1)
                .ok_or(BookError::CounterOverflow)?,
            None => self.sync_divergences,
        };
        let broken = match divergence {
            Some(_) => self.continuity.lost(ContinuityReason::SyncDivergence),
            None => self.continuity.clone(),
        };
        let recovery_base = matches!(broken, MutationContinuity::Lost { .. });
        let continuity = if recovery_base {
            broken.recovered(0).map_err(BookError::Continuity)?
        } else {
            broken
        };
        let MutationContinuity::Intact {
            epoch,
            next_position,
        } = continuity
        else {
            return Err(BookError::Continuity(ReplicaError::InvalidTransition));
        };
        let mutations = if recovery_base || self.provenance.is_none() || self.delta_rail {
            Vec::new()
        } else {
            derive_mutations(&self.levels, &next, source, revision, epoch, next_position)?
        };
        let emitted = u64::try_from(mutations.len()).map_err(|_| BookError::CounterOverflow)?;
        let provenance = rebase_provenance(source, revision, epoch, Rebase::LatestState)
            .map_err(BookError::Provenance)?;
        self.continuity = continuity
            .advanced(emitted)
            .map_err(BookError::Continuity)?;
        self.levels = next;
        self.revision = revision;
        self.authority = AuthorityState::Live;
        self.provenance = Some(provenance);
        self.deltas_since_snapshot = 0;
        self.sync_divergences = sync_divergences;
        Ok(BookCommit {
            revision,
            epoch,
            recovery_base,
            divergence,
            mutations,
        })
    }

    /// Commits a venue-reported delta atomically and returns what it changed.
    ///
    /// The candidate must be a non-empty [`CandidateOperation::SourceDelta`] for this
    /// book's market, applied onto an established base with an intact mutation stream. Each
    /// delta level names a coordinate `(side, price)` and the quantity the venue now reports
    /// resting there: **a zero quantity removes the coordinate and any other quantity sets
    /// or replaces it**. That is the delta grammar, and it deliberately differs from the
    /// snapshot path, where a zero-quantity level is a resting level like any other — a
    /// venue that sends a size of zero in a diff is saying the level is gone, and a venue
    /// that sends one in a full book is saying it is there.
    ///
    /// One source-reported mutation is emitted per delta'd coordinate whose value in the
    /// book actually changes, in the venue's own order — this is the venue's diff, not a
    /// re-sorted one — each carrying the candidate's provenance rebased onto the book's
    /// revision and epoch, so native family, source timestamp, source evidence,
    /// representation, and origin all survive. A level that restates the quantity already
    /// held, or removes a coordinate the book does not hold, emits no mutation: the venue
    /// restated known state. Such a delta still commits — the revision advances, provenance
    /// is refreshed, authority returns to [`AuthorityState::Live`], and the delta counts
    /// toward [`Self::deltas_since_snapshot`] — because it is evidence the feed is alive.
    /// The first accepted delta also puts the book on the delta rail for good.
    ///
    /// Fails with [`BookError::MarketMismatch`],
    /// [`BookError::UnsupportedCandidateOperation`] for a snapshot candidate,
    /// [`BookError::EmptyDelta`] for a delta carrying no level,
    /// [`BookError::NoEstablishedBase`] before any snapshot has been accepted,
    /// [`BookError::ContinuityLost`] while the mutation stream is broken — a delta must
    /// never ride across a gap — [`BookError::DuplicateLevelCoordinate`],
    /// [`BookError::NonComplementablePrice`], [`BookError::CounterOverflow`], or a wrapped
    /// provenance, continuity, or mutation error. A failed apply leaves levels, revision,
    /// continuity, authority, provenance, and both counters untouched.
    ///
    /// Those gates are ordered: whether the candidate addresses this book, and whether it
    /// is structurally a delta at all, is decided before any book state is consulted, so a
    /// malformed candidate is answered the same way whatever the book holds. The level list
    /// is then validated whole before the first coordinate is touched.
    pub fn apply_source_delta(&mut self, candidate: &Candidate) -> Result<BookCommit, BookError> {
        let source = candidate.provenance();
        if source.market() != &self.market {
            return Err(BookError::MarketMismatch);
        }
        let CandidateOperation::SourceDelta(levels) = candidate.operation() else {
            return Err(BookError::UnsupportedCandidateOperation);
        };
        if levels.levels().is_empty() {
            return Err(BookError::EmptyDelta);
        }
        if self.provenance.is_none() {
            return Err(BookError::NoEstablishedBase);
        }
        let MutationContinuity::Intact {
            epoch,
            next_position,
        } = &self.continuity
        else {
            return Err(BookError::ContinuityLost);
        };
        let (epoch, next_position) = (*epoch, *next_position);

        let mut seen = BTreeSet::new();
        let mut staged = Vec::with_capacity(levels.levels().len());
        for level in levels.levels() {
            complement_price(level.price()).map_err(BookError::NonComplementablePrice)?;
            let coordinate = (level.side(), level.price().clone());
            if !seen.insert(coordinate.clone()) {
                return Err(BookError::DuplicateLevelCoordinate);
            }
            let resting = if level.quantity().value().coefficient() == 0 {
                None
            } else {
                Some(level.quantity().clone())
            };
            staged.push((coordinate, resting));
        }

        let revision = self
            .revision
            .checked_add(1)
            .ok_or(BookError::CounterOverflow)?;
        let mut mutations = Vec::new();
        for (coordinate, resting) in &staged {
            let held = self.levels.get(coordinate);
            if held == resting.as_ref() {
                continue;
            }
            let position = next_position
                .checked_add(
                    u64::try_from(mutations.len()).map_err(|_| BookError::CounterOverflow)?,
                )
                .ok_or(BookError::CounterOverflow)?;
            let provenance = rebase_provenance(source, revision, epoch, Rebase::SourceReportedDiff)
                .map_err(BookError::Provenance)?;
            let level = |quantity: &Quantity| {
                Level::new(coordinate.0, coordinate.1.clone(), quantity.clone())
            };
            let mutation = BookMutation::source_reported(
                provenance,
                held.map(level),
                resting.as_ref().map(level),
            )
            .map_err(BookError::Mutation)?;
            mutations.push(MutationRecord {
                cursor: MutationCursor::new(epoch, position),
                mutation: Arc::new(mutation),
            });
        }
        let emitted = u64::try_from(mutations.len()).map_err(|_| BookError::CounterOverflow)?;
        let deltas_since_snapshot = self
            .deltas_since_snapshot
            .checked_add(1)
            .ok_or(BookError::CounterOverflow)?;
        let provenance = rebase_provenance(source, revision, epoch, Rebase::LatestState)
            .map_err(BookError::Provenance)?;
        let continuity = self
            .continuity
            .advanced(emitted)
            .map_err(BookError::Continuity)?;

        for (coordinate, resting) in staged {
            match resting {
                Some(quantity) => {
                    let _ = self.levels.insert(coordinate, quantity);
                }
                None => {
                    let _ = self.levels.remove(&coordinate);
                }
            }
        }
        self.continuity = continuity;
        self.revision = revision;
        self.authority = AuthorityState::Live;
        self.provenance = Some(provenance);
        self.delta_rail = true;
        self.deltas_since_snapshot = deltas_since_snapshot;
        Ok(BookCommit {
            revision,
            epoch,
            recovery_base: false,
            divergence: None,
            mutations,
        })
    }

    /// Ends this book's life as a subscribed market: the last revision it will ever
    /// publish says it is no longer subscribed.
    ///
    /// Reports whether anything changed. Demand for this market has gone — an operator
    /// removed it, or the connection carrying it was given up — so there is no venue left to
    /// recover from and no authority left to hold. `docs/design.md` "Subscription control"
    /// requires that existing shared state be marked unavailable for live use before its
    /// storage is reclaimed, and this is that mark: authority becomes
    /// [`AuthorityState::Unsubscribed`], the revision advances so a reader is woken, and the
    /// levels are left exactly as the venue last reported them rather than replaced by an
    /// invented empty book. A reader that keeps the published state sees a book that says
    /// what it is, instead of retained bytes that still claim to be live.
    ///
    /// Mutation continuity is untouched: an unsubscription is not a gap in a stream that has
    /// ended. Fails only with [`BookError::CounterOverflow`].
    pub fn unsubscribe(&mut self) -> Result<bool, BookError> {
        if self.authority == AuthorityState::Unsubscribed {
            return Ok(false);
        }
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or(BookError::CounterOverflow)?;
        self.authority = AuthorityState::Unsubscribed;
        Ok(true)
    }

    /// Records evidence that the mutation stream broke, on the caller's evidence alone.
    ///
    /// This is the only path to [`AuthorityState::Stale`]; no elapsed time reaches this
    /// type. Continuity keeps the reason it first broke for while authority records the
    /// newest reason. Returns whether published state changed: a repeated identical report
    /// is a no-op and does not advance the revision.
    ///
    /// Fails with [`BookError::CounterOverflow`] when the revision counter would wrap.
    pub fn report_continuity_loss(
        &mut self,
        continuity: ContinuityReason,
        authority: AuthorityReason,
    ) -> Result<bool, BookError> {
        let next_continuity = self.continuity.lost(continuity);
        let next_authority = AuthorityState::Stale(authority);
        if next_continuity == self.continuity && next_authority == self.authority {
            return Ok(false);
        }
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or(BookError::CounterOverflow)?;
        self.continuity = next_continuity;
        self.authority = next_authority;
        Ok(true)
    }

    /// Allocates the next delivery position in this book's mutation stream for an event that
    /// changes no level, and advances the stream past it.
    ///
    /// The position must come from this counter and nowhere else: a position handed out
    /// twice is a delivery silently overwritten by the next commit, which is exactly the
    /// gap the stream model exists to make impossible. Nothing else about the book moves —
    /// revision, levels, authority and provenance are untouched — because occupying a
    /// delivery position is not a book change: it opens no epoch, records no revision, and
    /// establishes no recovery base.
    ///
    /// Fails with [`BookError::ContinuityLost`] on a lost stream, which has no positions to
    /// allocate and whose consumers are already under an explicit continuity loss, and with
    /// [`BookError::Continuity`] when the position counter would wrap. A refused allocation
    /// leaves the stream exactly as it was.
    pub fn note_stream_event(&mut self) -> Result<MutationCursor, BookError> {
        let MutationContinuity::Intact {
            epoch,
            next_position,
        } = &self.continuity
        else {
            return Err(BookError::ContinuityLost);
        };
        let cursor = MutationCursor::new(*epoch, *next_position);
        self.continuity = self.continuity.advanced(1).map_err(BookError::Continuity)?;
        Ok(cursor)
    }

    /// Takes an immutable copy of the current state for publication to readers.
    pub fn publish(&self) -> PublishedBook {
        PublishedBook {
            market: self.market.clone(),
            levels: self
                .levels
                .iter()
                .map(|((side, price), quantity)| Level::new(*side, price.clone(), quantity.clone()))
                .collect(),
            authority: self.authority.clone(),
            continuity: self.continuity.clone(),
            revision: self.revision,
            provenance: self.provenance.clone(),
            sync_divergences: self.sync_divergences,
        }
    }
}

/// An immutable published view of one book revision.
///
/// Readers hold this behind an [`std::sync::Arc`] and never touch the writer's book.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedBook {
    market: MarketRef,
    levels: Vec<Level>,
    authority: AuthorityState,
    continuity: MutationContinuity,
    revision: u64,
    provenance: Option<Provenance>,
    sync_divergences: u64,
}

impl PublishedBook {
    pub fn market(&self) -> &MarketRef {
        &self.market
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn authority(&self) -> &AuthorityState {
        &self.authority
    }
    pub fn continuity(&self) -> &MutationContinuity {
        &self.continuity
    }
    /// The provenance of the candidate behind this revision, rebased onto the book's own
    /// revision and continuity epoch, or `None` before the first accepted snapshot.
    pub fn provenance(&self) -> Option<&Provenance> {
        self.provenance.as_ref()
    }
    /// How many sync snapshots have disagreed with this book since it was created.
    ///
    /// Each one was a mutation-continuity break: a delta-built book that a later snapshot
    /// contradicted. The count only rises, and it is per-book feed-health evidence about
    /// this replica's delta stream rather than shared state — two replicas of one market
    /// legitimately report different counts.
    pub fn sync_divergences(&self) -> u64 {
        self.sync_divergences
    }

    /// The single liquidity both views describe.
    ///
    /// Canonical and complement levels are two presentations of these same resting orders,
    /// so they share one identity and must never be summed together as independent depth.
    pub fn liquidity_identity(&self) -> LiquidityIdentity {
        LiquidityIdentity::Market(self.market.clone())
    }

    /// The venue-reported levels in canonical outcome coordinates, ascending by
    /// `(side, price)` — every bid before every ask, each side ascending by price, rather
    /// than the venue's display order.
    ///
    /// This is also the order derived snapshot diffs are emitted in. It is not the order of
    /// source-reported mutations: those follow the venue's own delta, which names its
    /// coordinates in whatever order it chooses.
    pub fn canonical_levels(&self) -> &[Level] {
        &self.levels
    }

    /// The opposite outcome view, computed from the canonical levels on every read and
    /// never stored.
    ///
    /// Each canonical `(Bid, p, q)` appears as `(Ask, 1-p, q)` and each `(Ask, p, q)` as
    /// `(Bid, 1-p, q)`: the same resting size at the complementary price, sorted ascending
    /// by `(side, price)`. Depth is identical to [`Self::canonical_levels`] by construction
    /// because no quantity is transformed.
    ///
    /// [`BookError::Complement`] is unreachable for a published book: every level of every
    /// committed snapshot was preflighted as complementable by
    /// [`OrderBook::apply_snapshot`], and a [`PublishedBook`] has no other source of levels.
    /// The result stays typed rather than becoming a panic path in a daemon.
    pub fn derived_complement_levels(&self) -> Result<Vec<Level>, BookError> {
        let mut complement = Vec::with_capacity(self.levels.len());
        for level in &self.levels {
            let price = complement_price(level.price()).map_err(BookError::Complement)?;
            let side = match level.side() {
                Side::Bid => Side::Ask,
                Side::Ask => Side::Bid,
            };
            complement.push(Level::new(side, price, level.quantity().clone()));
        }
        complement
            .sort_by(|left, right| (left.side(), left.price()).cmp(&(right.side(), right.price())));
        Ok(complement)
    }
}

/// The coordinates a book rests liquidity at: every entry whose quantity is nonzero.
///
/// A zero-quantity level is venue-reported state the book reproduces faithfully, but it is
/// not depth. Whether such a coordinate appears as an explicit zero or is simply absent is
/// a reporting difference between a full book and a diff that removed it, and must never
/// read as an economic one.
fn resting(levels: &PhysicalLevels) -> impl Iterator<Item = (&(Side, Price), &Quantity)> {
    levels
        .iter()
        .filter(|(_, quantity)| quantity.value().coefficient() != 0)
}

/// Whether two level maps describe the same depth at the same coordinates.
///
/// Exact-zero entries are excluded on both sides, so a coordinate one map removed and the
/// other rests at zero compare equal. Surviving quantities compare by exact decimal value,
/// so `120.0` and `120` are one depth.
fn economically_equal(left: &PhysicalLevels, right: &PhysicalLevels) -> bool {
    resting(left).eq(resting(right))
}

fn derive_mutations(
    previous: &PhysicalLevels,
    next: &PhysicalLevels,
    source: &Provenance,
    revision: u64,
    epoch: u64,
    first_position: u64,
) -> Result<Vec<MutationRecord>, BookError> {
    let mut coordinates: Vec<&(Side, Price)> = previous.keys().chain(next.keys()).collect();
    coordinates.sort_unstable();
    coordinates.dedup();
    let mut mutations = Vec::new();
    for coordinate in coordinates {
        let before = previous.get(coordinate);
        let after = next.get(coordinate);
        if before == after {
            continue;
        }
        let position = first_position
            .checked_add(u64::try_from(mutations.len()).map_err(|_| BookError::CounterOverflow)?)
            .ok_or(BookError::CounterOverflow)?;
        let provenance = rebase_provenance(source, revision, epoch, Rebase::DerivedDiff)
            .map_err(BookError::Provenance)?;
        let level =
            |quantity: &Quantity| Level::new(coordinate.0, coordinate.1.clone(), quantity.clone());
        let mutation = BookMutation::snapshot_diff(provenance, before.map(level), after.map(level))
            .map_err(BookError::Mutation)?;
        mutations.push(MutationRecord {
            cursor: MutationCursor::new(epoch, position),
            mutation: Arc::new(mutation),
        });
    }
    Ok(mutations)
}

enum Rebase {
    LatestState,
    SourceReportedDiff,
    DerivedDiff,
}

/// Rebases venue-native provenance onto the book's own revision and continuity epoch.
///
/// Every venue-reported field — market, outcome, connection identity, generations,
/// receive and commit positions, local monotonic timestamps, replica role, source
/// timestamp, and source evidence — passes through unchanged; the book invents none of
/// them. [`Rebase::LatestState`] labels the book's own published provenance and
/// [`Rebase::SourceReportedDiff`] one level change the venue reported; both keep the
/// candidate's labelling, so a source delta's mutations stay
/// [`Origin::SourceReported`] and venue-native. [`Rebase::DerivedDiff`] instead re-labels
/// the record as this daemon's own snapshot difference: family `derived.snapshot-diff`, no
/// source timestamp, no source evidence, normalized representation, locally derived origin.
fn rebase_provenance(
    source: &Provenance,
    revision: u64,
    epoch: u64,
    kind: Rebase,
) -> Result<Provenance, DescriptorError> {
    let (native_family, source_timestamp, evidence, representation, origin) = match kind {
        Rebase::LatestState | Rebase::SourceReportedDiff => (
            source.native_family().to_owned(),
            source.source_timestamp().cloned(),
            source.source_evidence().to_vec(),
            source.representation().clone(),
            source.origin().clone(),
        ),
        Rebase::DerivedDiff => (
            DERIVED_FAMILY.to_owned(),
            None,
            Vec::new(),
            Representation::Normalized,
            Origin::LocallyDerived(Derivation::SnapshotDiff),
        ),
    };
    let capacity = SourceEvidenceCapacity::new(evidence.len())?;
    Provenance::new(ProvenanceInput {
        market: source.market().clone(),
        outcome: source.outcome().cloned(),
        native_family,
        source_timestamp,
        source_evidence: BoundedSourceEvidence::new(evidence, capacity)?,
        daemon_generation: source.daemon_generation(),
        connection: source.connection().clone(),
        subscription_generation: source.subscription_generation(),
        receive_position: source.receive_position(),
        commit_position: source.commit_position(),
        local_receive_time: source.local_receive_time(),
        local_commit_time: source.local_commit_time(),
        replica: source.replica().clone(),
        representation,
        origin,
        local_revision: revision,
        continuity_epoch: epoch,
    })
}

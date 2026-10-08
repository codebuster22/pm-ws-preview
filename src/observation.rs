use crate::{
    Derivation, NativeOutcome, Origin, Price, Provenance, Quantity, Representation, SourceTimestamp,
};
use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Level {
    side: Side,
    price: Price,
    quantity: Quantity,
}
impl Level {
    pub fn new(side: Side, price: Price, quantity: Quantity) -> Self {
        Self {
            side,
            price,
            quantity,
        }
    }
    pub fn side(&self) -> Side {
        self.side
    }
    pub fn price(&self) -> &Price {
        &self.price
    }
    pub fn quantity(&self) -> &Quantity {
        &self.quantity
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObservationError {
    CapacityZero,
    CapacityExceeded,
    InvalidOrigin,
    EmptyMutation,
    MismatchedMutationCoordinate,
    EmptyLabel,
    LabelTooLong,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct NativeLabel(String);
impl NativeLabel {
    pub fn new(value: impl Into<String>) -> Result<Self, ObservationError> {
        let value = value.into();
        if value.is_empty() {
            Err(ObservationError::EmptyLabel)
        } else if value.len() > 1024 {
            Err(ObservationError::LabelTooLong)
        } else {
            Ok(Self(value))
        }
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct NativeDeliveryPath(String);
impl NativeDeliveryPath {
    pub fn new(value: impl Into<String>) -> Result<Self, ObservationError> {
        let value = value.into();
        if value.is_empty() {
            Err(ObservationError::EmptyLabel)
        } else if value.len() > 256 {
            Err(ObservationError::LabelTooLong)
        } else {
            Ok(Self(value))
        }
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct LevelCapacity(usize);
impl LevelCapacity {
    pub fn new(value: usize) -> Result<Self, ObservationError> {
        if value == 0 {
            Err(ObservationError::CapacityZero)
        } else {
            Ok(Self(value))
        }
    }
    pub fn value(self) -> usize {
        self.0
    }
}
impl std::fmt::Display for ObservationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid observation")
    }
}
impl std::error::Error for ObservationError {}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BoundedLevels {
    levels: Vec<Level>,
    capacity: LevelCapacity,
}
impl BoundedLevels {
    pub fn new(
        levels: impl IntoIterator<Item = Level>,
        capacity: LevelCapacity,
    ) -> Result<Self, ObservationError> {
        let mut collected = Vec::new();
        for level in levels {
            if collected.len() == capacity.value() {
                return Err(ObservationError::CapacityExceeded);
            }
            collected.push(level);
        }
        Ok(Self {
            levels: collected,
            capacity,
        })
    }
    pub fn levels(&self) -> &[Level] {
        &self.levels
    }
    pub fn capacity(&self) -> LevelCapacity {
        self.capacity
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum CandidateOperation {
    Snapshot(BoundedLevels),
    SourceDelta(BoundedLevels),
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Candidate {
    provenance: Provenance,
    operation: CandidateOperation,
}
impl Candidate {
    pub fn snapshot(
        provenance: Provenance,
        levels: BoundedLevels,
    ) -> Result<Self, ObservationError> {
        if matches!(provenance.origin(), Origin::LocallyDerived(_)) {
            return Err(ObservationError::InvalidOrigin);
        }
        Ok(Self {
            provenance,
            operation: CandidateOperation::Snapshot(levels),
        })
    }
    pub fn source_delta(
        provenance: Provenance,
        levels: BoundedLevels,
    ) -> Result<Self, ObservationError> {
        if !matches!(
            (provenance.representation(), provenance.origin()),
            (Representation::VenueNative, Origin::SourceReported)
        ) {
            return Err(ObservationError::InvalidOrigin);
        }
        Ok(Self {
            provenance,
            operation: CandidateOperation::SourceDelta(levels),
        })
    }
    pub fn provenance(&self) -> &Provenance {
        &self.provenance
    }
    pub fn operation(&self) -> &CandidateOperation {
        &self.operation
    }
}

/// One level change in a book's mutation stream, at the coordinate it changed.
///
/// The two constructors are the only two ways a level change can come to exist: one the
/// venue reported itself, and one this daemon computed from two snapshots the venue sent.
/// The shapes are identical on purpose — the record is a coordinate plus a before and an
/// after either way — so consumers tell them apart by `provenance().origin()` rather than
/// by structure, and a general mutation surface carries both without a second type.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BookMutation {
    provenance: Provenance,
    old: Option<Level>,
    new: Option<Level>,
}
impl BookMutation {
    /// A level change this daemon derived by comparing two snapshots the venue sent.
    ///
    /// `provenance` must be labelled [`Origin::LocallyDerived`] with
    /// [`Derivation::SnapshotDiff`] and [`Representation::Normalized`]. `old` is the level
    /// before the change and `new` the level after it; either may be absent, for a
    /// coordinate that appeared or vanished, but not both.
    ///
    /// Fails with [`ObservationError::InvalidOrigin`] under any other labelling,
    /// [`ObservationError::EmptyMutation`] when both sides are absent, and
    /// [`ObservationError::MismatchedMutationCoordinate`] when the two sides do not name
    /// the same `(side, price)`.
    pub fn snapshot_diff(
        provenance: Provenance,
        old: Option<Level>,
        new: Option<Level>,
    ) -> Result<Self, ObservationError> {
        if !matches!(
            provenance.origin(),
            Origin::LocallyDerived(Derivation::SnapshotDiff)
        ) || !matches!(provenance.representation(), Representation::Normalized)
        {
            return Err(ObservationError::InvalidOrigin);
        }
        Self::coordinate_change(provenance, old, new)
    }

    /// A level change the venue reported as its own diff, reproduced rather than derived.
    ///
    /// `provenance` must be labelled [`Origin::SourceReported`], which pairs only with
    /// [`Representation::VenueNative`]. `old` is the book's value at the coordinate before
    /// the delta was applied and `new` the value the venue reported for it; an absent `new`
    /// is the venue removing the coordinate and an absent `old` is it naming one the book
    /// did not hold. No snapshot comparison takes part: the daemon invents neither side.
    ///
    /// Fails with [`ObservationError::InvalidOrigin`] under any other labelling,
    /// [`ObservationError::EmptyMutation`] when both sides are absent, and
    /// [`ObservationError::MismatchedMutationCoordinate`] when the two sides do not name
    /// the same `(side, price)`.
    pub fn source_reported(
        provenance: Provenance,
        old: Option<Level>,
        new: Option<Level>,
    ) -> Result<Self, ObservationError> {
        if !matches!(provenance.origin(), Origin::SourceReported) {
            return Err(ObservationError::InvalidOrigin);
        }
        Self::coordinate_change(provenance, old, new)
    }

    fn coordinate_change(
        provenance: Provenance,
        old: Option<Level>,
        new: Option<Level>,
    ) -> Result<Self, ObservationError> {
        if old.is_none() && new.is_none() {
            return Err(ObservationError::EmptyMutation);
        }
        if let (Some(old), Some(new)) = (&old, &new)
            && (old.side() != new.side() || old.price() != new.price())
        {
            return Err(ObservationError::MismatchedMutationCoordinate);
        }
        Ok(Self {
            provenance,
            old,
            new,
        })
    }

    pub fn provenance(&self) -> &Provenance {
        &self.provenance
    }
    /// The book's value at this coordinate before the change, or `None` when the coordinate
    /// was absent.
    pub fn old(&self) -> Option<&Level> {
        self.old.as_ref()
    }
    /// The book's value at this coordinate after the change, or `None` when the change
    /// removed it.
    pub fn replacement(&self) -> Option<&Level> {
        self.new.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum DeliveryPath {
    MarketFeed,
    LifecycleFeed,
    ResolutionFeed,
    OtherNativePath(NativeDeliveryPath),
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ResolutionObservation {
    provenance: Provenance,
    winner: NativeOutcome,
    native_label: NativeLabel,
    delivery_path: DeliveryPath,
}
impl ResolutionObservation {
    pub fn new(
        provenance: Provenance,
        winner: NativeOutcome,
        native_label: NativeLabel,
        delivery_path: DeliveryPath,
    ) -> Result<Self, ObservationError> {
        if matches!(provenance.origin(), Origin::LocallyDerived(_)) {
            return Err(ObservationError::InvalidOrigin);
        }
        Ok(Self {
            provenance,
            winner,
            native_label,
            delivery_path,
        })
    }
    pub fn provenance(&self) -> &Provenance {
        &self.provenance
    }
    pub fn winner(&self) -> &NativeOutcome {
        &self.winner
    }
    pub fn native_label(&self) -> &NativeLabel {
        &self.native_label
    }
    pub fn delivery_path(&self) -> &DeliveryPath {
        &self.delivery_path
    }
}

/// One venue-reported market resolution, complete enough for every consumer surface to
/// reproduce it without asking the venue again.
///
/// Composes the [`ResolutionObservation`] the daemon records — provenance, the venue's
/// winning outcome as a [`NativeOutcome`], its own [`NativeLabel`] for the market, and the
/// feed the report arrived on — with the two venue-native values that observation has no
/// cell for: the venue's winning index and its resolution timestamp lexeme. The lexeme is
/// reproduced verbatim and never parsed into a number, and no field here is floating point.
///
/// Venue-agnostic: nothing in this type names a venue, and the daemon interprets none of
/// its values. A resolution is independent of book state — it never freezes, clears, or
/// unsubscribes a book — and travels the book's delivery lane only to be ordered against
/// the level changes around it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct MarketResolution {
    observation: ResolutionObservation,
    winning_index: u32,
    resolution_date: SourceTimestamp,
}

impl MarketResolution {
    /// Carries `observation` together with the venue's own `winning_index` and
    /// `resolution_date` lexeme. Every value is the venue's; none is checked against
    /// another, because the daemon reproduces market policy rather than deciding it.
    pub fn new(
        observation: ResolutionObservation,
        winning_index: u32,
        resolution_date: SourceTimestamp,
    ) -> Self {
        Self {
            observation,
            winning_index,
            resolution_date,
        }
    }
    pub fn observation(&self) -> &ResolutionObservation {
        &self.observation
    }
    pub fn provenance(&self) -> &Provenance {
        self.observation.provenance()
    }
    /// The outcome the venue declared the winner, in the venue's own vocabulary.
    pub fn winner(&self) -> &NativeOutcome {
        self.observation.winner()
    }
    /// The venue's own label for the resolved market — its market type where the venue
    /// publishes one. Reproduced, never classified.
    pub fn native_label(&self) -> &NativeLabel {
        self.observation.native_label()
    }
    pub fn delivery_path(&self) -> &DeliveryPath {
        self.observation.delivery_path()
    }
    /// The venue's own index of the winning outcome, as reported.
    pub fn winning_index(&self) -> u32 {
        self.winning_index
    }
    /// The venue's resolution timestamp, kept as the lexeme it published. Never parsed into
    /// an instant here: a consumer that needs one parses it under its own rules.
    pub fn resolution_date(&self) -> &SourceTimestamp {
        &self.resolution_date
    }
}

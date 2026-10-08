//! The segment layout, its fixed header, and the validator a reader must pass before it
//! touches anything else.
//!
//! Every offset and width here is explicit and little-endian. The geometry is fully
//! determined by five capacities, so validation recomputes the layout from the capacities
//! the header declares and compares every remaining word against it: a section cannot be
//! made to overlap another, and a hostile byte between the header and the trailer cannot
//! change what the validator returns because the validator never reads one.

use super::cell::{REGION_ALIGNMENT, Record, RegionCells, SegmentRegion};

/// The segment header's gate word: `PMWSSTA1` little-endian.
pub const MAGIC: u64 = u64::from_le_bytes(*b"PMWSSTA1");
/// The segment trailer's gate word: `PMWSEND1` little-endian.
pub const TRAILER_MAGIC: u64 = u64::from_le_bytes(*b"PMWSEND1");
/// The ABI version this build writes and is the only one it reads.
///
/// A reader built against another version fails validation rather than misreading the
/// layout, which is the whole point of carrying the number: a version-4 reader would park
/// on a cell it believes is reserved space, so it is refused at the header instead.
pub const ABI_VERSION: u32 = 5;

/// The segment declares that every retained-event ring wraps: the writer never blocks and
/// never skips a position, and an overtaken consumer is told so.
///
/// `docs/notes/shared-memory-model.md` §3.3 makes this a *required* feature bit rather
/// than an option, so a segment that does not declare it is refused: a reader must never
/// assume a bounded ring is lossless.
pub const FEATURE_EVENT_RING_WRAPS: u64 = 1 << 0;

/// The segment declares that its doorbell is the header's own [`HDR_DOORBELL`] word: this
/// platform lets a consumer's read-only mapping be waited on directly.
///
/// Decided by the writer at creation and fixed for the segment's life, because the two
/// placements are two different addresses and a waiter parked on the wrong one is never
/// woken.
pub const FEATURE_DOORBELL_IN_HEADER: u64 = 1 << 1;

/// The segment declares that its doorbell lives in a sibling page beside the segment file,
/// which consumers map read-write purely so the platform's wait primitive accepts it.
///
/// The fallback for a platform that refuses to wait on a read-only mapping — Darwin answers
/// `EFAULT` — and the reason a consumer's write access to that page is harmless: nothing is
/// decoded from a doorbell, so the blast radius of corrupting it is a missed or spurious
/// wakeup, never state.
pub const FEATURE_DOORBELL_PAGE: u64 = 1 << 2;

/// Every feature bit this build understands. A segment declaring anything outside this
/// mask is refused with [`SegmentFault::FeatureUnsupported`].
pub(super) const KNOWN_FEATURE_BITS: u64 =
    FEATURE_EVENT_RING_WRAPS | FEATURE_DOORBELL_IN_HEADER | FEATURE_DOORBELL_PAGE;

/// The retained-event capacity a single-market segment is created with, in events.
///
/// 1024 matches the in-process observer ring's default depth, so the two surfaces of one
/// book retain the same amount of history; at 256 bytes per slot that is 256 KiB per
/// market.
pub const DEFAULT_EVENT_CAPACITY: u32 = 1_024;

/// The dirty-index depth a segment is created with, in entries.
///
/// One entry per state publication across the whole segment, 32 bytes each: 128 KiB of ring
/// that a consumer attached to many markets reads instead of rescanning every book. The
/// depth is what a consumer may fall behind by before its overrun becomes the declared
/// full-rescan signal.
pub const DEFAULT_DIRTY_CAPACITY: u32 = 4_096;
/// The reserved `entry_state_slot_index` meaning "no state slot is published for this
/// market". A reader treats it as absence, never as index 4,294,967,295.
pub const NO_STATE_SLOT: u32 = u32::MAX;

pub(super) const HEADER_BYTES: usize = 256;
pub(super) const TRAILER_BYTES: usize = REGION_ALIGNMENT;
pub(super) const DIRECTORY_ENTRY_BYTES: usize = 512;
pub(super) const IDENTITY_CAPACITY: usize = DIRECTORY_ENTRY_BYTES - 16;
pub(super) const SLOT_PREFIX_BYTES: usize = 256;
pub(super) const LEVEL_CELL_BYTES: usize = 64;
pub(super) const DECIMAL_CELL_BYTES: usize = 24;
pub(super) const NATIVE_FAMILY_WORDS: usize = 8;
pub(super) const NATIVE_FAMILY_CAPACITY: usize = NATIVE_FAMILY_WORDS * 8;

pub(super) const EVENT_SLOT_BYTES: usize = 256;
pub(super) const DIRTY_SLOT_BYTES: usize = 32;

pub(super) const MAX_DIRECTORY_CAPACITY: u32 = 65_536;
pub(super) const MAX_STATE_SLOT_CAPACITY: u32 = 65_536;
pub(super) const MAX_EVENT_CAPACITY: u32 = 65_536;
pub(super) const MAX_DIRTY_CAPACITY: u32 = 1_048_576;

/// The deepest book one state slot can carry, in levels.
///
/// Public because every producer that must be able to publish an accepted book has to be
/// checked against it — ideally at build time, so a depth this layout cannot carry is a
/// compile error rather than a startup failure.
pub const MAX_LEVEL_CAPACITY: u32 = 4_096;

pub(super) const HDR_MAGIC: usize = 0;
pub(super) const HDR_ABI_VERSION: usize = 8;
pub(super) const HDR_REGION_ALIGNMENT: usize = 12;
pub(super) const HDR_REGION_SIZE: usize = 16;
pub(super) const HDR_INSTANCE_LOW: usize = 24;
pub(super) const HDR_INSTANCE_HIGH: usize = 32;
pub(super) const HDR_SEGMENT_GENERATION: usize = 40;
pub(super) const HDR_DIRECTORY_OFFSET: usize = 48;
pub(super) const HDR_DIRECTORY_STRIDE: usize = 56;
pub(super) const HDR_DIRECTORY_CAPACITY: usize = 60;
pub(super) const HDR_SLOT_OFFSET: usize = 64;
pub(super) const HDR_SLOT_STRIDE: usize = 72;
pub(super) const HDR_SLOT_CAPACITY: usize = 76;
pub(super) const HDR_LEVEL_CAPACITY: usize = 80;
pub(super) const HDR_LEVEL_STRIDE: usize = 84;
pub(super) const HDR_IDENTITY_CAPACITY: usize = 88;
pub(super) const HDR_FAMILY_CAPACITY: usize = 92;
pub(super) const HDR_TRAILER_OFFSET: usize = 96;
pub(super) const HDR_FEATURE_BITS: usize = 104;
pub(super) const HDR_EVENT_OFFSET: usize = 112;
pub(super) const HDR_EVENT_CAPACITY: usize = 120;
pub(super) const HDR_EVENT_STRIDE: usize = 124;
pub(super) const HDR_PUBLICATION_GENERATION: usize = 128;
pub(super) const HDR_DOORBELL: usize = 136;
pub(super) const HDR_DIRTY_OFFSET: usize = 144;
pub(super) const HDR_DIRTY_CAPACITY: usize = 152;
pub(super) const HDR_DIRTY_STRIDE: usize = 156;

pub(super) const TRL_REGION_SIZE: usize = 0;
pub(super) const TRL_MAGIC: usize = 8;

pub(super) const ENT_REVISION: usize = 0;
pub(super) const ENT_SLOT_INDEX: usize = 8;
pub(super) const ENT_IDENTITY_LEN: usize = 12;
pub(super) const ENT_IDENTITY: usize = 16;

pub(super) const SLOT_REVISION: usize = 0;
pub(super) const SLOT_BOOK_REVISION: usize = 8;
pub(super) const SLOT_CONTINUITY_EPOCH: usize = 16;
pub(super) const SLOT_CONTINUITY_POSITION: usize = 24;
pub(super) const SLOT_SYNC_DIVERGENCES: usize = 32;
pub(super) const SLOT_AUTHORITY_STATE: usize = 40;
pub(super) const SLOT_AUTHORITY_REASON: usize = 44;
pub(super) const SLOT_CONTINUITY_KIND: usize = 48;
pub(super) const SLOT_CONTINUITY_REASON: usize = 52;
pub(super) const SLOT_PROVENANCE_PRESENT: usize = 56;
pub(super) const SLOT_ORIGIN: usize = 60;
pub(super) const SLOT_DERIVATION: usize = 64;
pub(super) const SLOT_REPRESENTATION: usize = 68;
pub(super) const SLOT_FAMILY_LEN: usize = 72;
pub(super) const SLOT_LEVEL_COUNT: usize = 76;
pub(super) const SLOT_DIRECTORY_INDEX: usize = 80;
pub(super) const SLOT_COMMIT_TIME: usize = 88;
pub(super) const SLOT_ARRIVAL_TIME: usize = 96;
pub(super) const SLOT_FAMILY_WORDS: usize = 128;

pub(super) const EVT_SEQUENCE: usize = 0;
pub(super) const EVT_CURSOR_EPOCH: usize = 8;
pub(super) const EVT_CURSOR_POSITION: usize = 16;
pub(super) const EVT_BOOK_REVISION: usize = 24;
pub(super) const EVT_COMMIT_TIME: usize = 32;
pub(super) const EVT_DELIVERY_KIND: usize = 40;
pub(super) const EVT_ORIGIN: usize = 44;
pub(super) const EVT_DERIVATION: usize = 48;
pub(super) const EVT_REPRESENTATION: usize = 52;
pub(super) const EVT_SIDE: usize = 56;
pub(super) const EVT_OLD_PRESENT: usize = 60;
pub(super) const EVT_NEW_PRESENT: usize = 64;
pub(super) const EVT_FAMILY_LEN: usize = 68;
pub(super) const EVT_DIRECTORY_INDEX: usize = 72;
pub(super) const EVT_DAEMON_GENERATION: usize = 80;
pub(super) const EVT_SUBSCRIPTION_GENERATION: usize = 88;
pub(super) const EVT_PRICE: usize = 96;
pub(super) const EVT_OLD_QUANTITY: usize = 120;
pub(super) const EVT_NEW_QUANTITY: usize = 144;
pub(super) const EVT_FAMILY_WORDS: usize = 168;
pub(super) const EVT_ARRIVAL_TIME: usize = 232;

pub(super) const EVT_RES_WINNING_INDEX: usize = 56;
pub(super) const EVT_RES_DELIVERY_PATH: usize = 60;
pub(super) const EVT_RES_OUTCOME_LEN: usize = 64;
pub(super) const EVT_RES_TYPE_LEN: usize = 68;
pub(super) const EVT_RES_DATE_LEN: usize = 96;
pub(super) const EVT_RES_OUTCOME: usize = 104;
pub(super) const EVT_RES_TYPE: usize = 168;
pub(super) const EVT_RES_DATE: usize = 200;

pub(super) const DIRTY_SEQUENCE: usize = 0;
pub(super) const DIRTY_POSITION: usize = 8;
pub(super) const DIRTY_DIRECTORY_INDEX: usize = 16;
pub(super) const DIRTY_BOOK_REVISION: usize = 24;

pub(super) const RES_OUTCOME_WORDS: usize = 8;
pub(super) const RES_TYPE_WORDS: usize = 4;
pub(super) const RES_DATE_WORDS: usize = 4;
/// Bytes the fixed cell for a resolution's venue-native winning outcome holds. A value the
/// vocabulary accepts but this cell cannot carry is never truncated, so a producer must
/// judge a report against these three widths before it occupies a stream position.
pub(crate) const RES_OUTCOME_CAPACITY: usize = RES_OUTCOME_WORDS * 8;
/// Bytes the fixed cell for a resolution's venue-native market type holds.
pub(crate) const RES_TYPE_CAPACITY: usize = RES_TYPE_WORDS * 8;
/// Bytes the fixed cell for a resolution's venue-native resolution-date lexeme holds.
pub(crate) const RES_DATE_CAPACITY: usize = RES_DATE_WORDS * 8;

const _: () = assert!(
    EVT_RES_OUTCOME + RES_OUTCOME_CAPACITY <= EVT_RES_TYPE
        && EVT_RES_TYPE + RES_TYPE_CAPACITY <= EVT_RES_DATE
        && EVT_RES_DATE + RES_DATE_CAPACITY <= EVT_ARRIVAL_TIME,
    "a resolution slot's three venue-native texts must not overlap each other or the words \
     every delivery kind shares"
);

const _: () = assert!(
    EVT_FAMILY_WORDS + NATIVE_FAMILY_CAPACITY <= EVT_ARRIVAL_TIME
        && EVT_ARRIVAL_TIME + 8 <= EVENT_SLOT_BYTES,
    "the arrival stamp is common to both delivery kinds, so no kind's payload may reach it"
);

const _: () = assert!(
    DIRTY_BOOK_REVISION + 8 <= DIRTY_SLOT_BYTES,
    "a dirty-index entry must fit its slot"
);

/// A retained-event slot carrying one level mutation.
///
/// The FFI's synthesized continuity-loss marker takes discriminant 2 and is produced by
/// the binding out of an explicit stream fault; it is never stored in the segment, so a
/// slot carrying it is a malformed record rather than a loss.
pub(super) const DELIVERY_LEVEL_MUTATION: u32 = 1;

/// A retained-event slot carrying one venue-reported market resolution.
///
/// It occupies a stream position exactly as a mutation does — drawn from the book's own
/// position counter, so no commit can overwrite it — and changes no level, no revision and
/// no authority. The kind names the venue-message family, so a resolution slot stores no
/// native-family text: [`EVT_FAMILY_LEN`] and [`EVT_FAMILY_WORDS`] are kind-1 cells, and a
/// resolution overlays that space with its own venue-native texts instead.
pub(super) const DELIVERY_MARKET_RESOLVED: u32 = 3;

pub(super) const LVL_PRICE: usize = 0;
pub(super) const LVL_QUANTITY: usize = 24;
pub(super) const LVL_SIDE: usize = 48;

pub(super) const DEC_COEFFICIENT_LOW: usize = 0;
pub(super) const DEC_COEFFICIENT_HIGH: usize = 8;
pub(super) const DEC_SCALE: usize = 16;

/// Why a segment layout could not be described.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LayoutError {
    CapacityZero,
    CapacityTooLarge,
    StateSlotsBelowDirectory,
    EventCapacityNotPowerOfTwo,
    /// A dirty-index depth that cannot be masked. As with the event rings, a position maps
    /// to a slot by masking rather than by division, so the depth must be a power of two.
    DirtyCapacityNotPowerOfTwo,
    RegionTooLarge,
}
impl core::fmt::Display for LayoutError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("invalid segment layout")
    }
}
impl std::error::Error for LayoutError {}

/// Why a reader refused a segment.
///
/// Every variant is a fail-closed answer: an incompatible or hostile segment is rejected
/// before any directory entry or state slot is read, which is the shared-state half of
/// [`crate::ConsumerState::Incompatible`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SegmentFault {
    RegionTooSmall {
        size: usize,
    },
    RegionMisaligned {
        size: usize,
    },
    HeaderUnpublished,
    MagicMismatch {
        found: u64,
    },
    AbiVersionUnsupported {
        found: u32,
        expected: u32,
    },
    AlignmentMismatch {
        found: u32,
        expected: u32,
    },
    RegionSizeMismatch {
        declared: u64,
        actual: u64,
    },
    CapacityOutOfRange,
    /// The segment declares a feature word this build cannot honour: a bit outside
    /// [`KNOWN_FEATURE_BITS`], a missing [`FEATURE_EVENT_RING_WRAPS`] which every v3 segment
    /// must declare, or a doorbell placement that is not exactly one of
    /// [`FEATURE_DOORBELL_IN_HEADER`] and [`FEATURE_DOORBELL_PAGE`] — neither leaves a
    /// consumer nowhere to park, both leave it parked on an address the writer may not be
    /// waking. `bits` is the word the header declared, so the cases are told apart by
    /// inspecting it.
    FeatureUnsupported {
        bits: u64,
    },
    GeometryMismatch,
    TrailerMismatch,
    GeometryUnreachable,
}
impl core::fmt::Display for SegmentFault {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("incompatible publication segment")
    }
}
impl std::error::Error for SegmentFault {}

/// The five capacities that fix a segment's geometry, and the byte arithmetic they imply.
///
/// Capacities are counts, offsets and strides are bytes. Every stride is a multiple of
/// [`REGION_ALIGNMENT`], so each record starts on its own cache-line boundary on this host.
/// The sections are laid out in one fixed order — header, directory, state slots, event
/// rings, dirty index, trailer — so every offset is a function of the capacities alone.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentLayout {
    directory_capacity: u32,
    state_slot_capacity: u32,
    level_capacity: u32,
    event_capacity: u32,
    dirty_capacity: u32,
}

const fn round_up(value: usize) -> usize {
    value.div_ceil(REGION_ALIGNMENT) * REGION_ALIGNMENT
}

impl SegmentLayout {
    /// Describes a segment holding `directory_capacity` markets, `state_slot_capacity`
    /// state slots, `level_capacity` levels per slot, `event_capacity` retained events per
    /// market, and one `dirty_capacity`-entry dirty index shared by every market.
    ///
    /// A segment binds each directory entry to one state slot for the life of its
    /// generation — slots are never reused, per `docs/notes/shared-memory-model.md` §3.2 —
    /// so `state_slot_capacity` must be at least `directory_capacity`; spare slots above
    /// that are permitted and simply unused. Each directory entry owns exactly one event
    /// ring of `event_capacity` slots, which must be a power of two so a position maps to a
    /// slot by masking rather than by division.
    ///
    /// Fails with [`LayoutError::CapacityZero`] on a zero capacity,
    /// [`LayoutError::CapacityTooLarge`] above the per-capacity ceilings,
    /// [`LayoutError::StateSlotsBelowDirectory`] when the directory could outgrow the
    /// slots, [`LayoutError::EventCapacityNotPowerOfTwo`] and
    /// [`LayoutError::DirtyCapacityNotPowerOfTwo`] for a ring depth that cannot be masked,
    /// and [`LayoutError::RegionTooLarge`] when the implied region exceeds what a region can
    /// be allocated at.
    pub fn new(
        directory_capacity: u32,
        state_slot_capacity: u32,
        level_capacity: u32,
        event_capacity: u32,
        dirty_capacity: u32,
    ) -> Result<Self, LayoutError> {
        if directory_capacity == 0
            || state_slot_capacity == 0
            || level_capacity == 0
            || event_capacity == 0
            || dirty_capacity == 0
        {
            return Err(LayoutError::CapacityZero);
        }
        if directory_capacity > MAX_DIRECTORY_CAPACITY
            || state_slot_capacity > MAX_STATE_SLOT_CAPACITY
            || level_capacity > MAX_LEVEL_CAPACITY
            || event_capacity > MAX_EVENT_CAPACITY
            || dirty_capacity > MAX_DIRTY_CAPACITY
        {
            return Err(LayoutError::CapacityTooLarge);
        }
        if state_slot_capacity < directory_capacity {
            return Err(LayoutError::StateSlotsBelowDirectory);
        }
        if !event_capacity.is_power_of_two() {
            return Err(LayoutError::EventCapacityNotPowerOfTwo);
        }
        if !dirty_capacity.is_power_of_two() {
            return Err(LayoutError::DirtyCapacityNotPowerOfTwo);
        }
        let layout = Self {
            directory_capacity,
            state_slot_capacity,
            level_capacity,
            event_capacity,
            dirty_capacity,
        };
        if layout.region_size() > super::cell::MAX_REGION_BYTES {
            return Err(LayoutError::RegionTooLarge);
        }
        Ok(layout)
    }

    pub fn directory_capacity(self) -> u32 {
        self.directory_capacity
    }
    pub fn state_slot_capacity(self) -> u32 {
        self.state_slot_capacity
    }
    pub fn level_capacity(self) -> u32 {
        self.level_capacity
    }
    /// The depth of every market's retained-event ring, in events. Always a power of two.
    pub fn event_capacity(self) -> u32 {
        self.event_capacity
    }
    /// The mask that turns an absolute stream position into a slot index within one ring.
    pub fn event_position_mask(self) -> u64 {
        u64::from(self.event_capacity) - 1
    }
    /// Bytes one market's whole event ring occupies.
    pub fn event_ring_bytes(self) -> usize {
        self.event_capacity as usize * EVENT_SLOT_BYTES
    }
    /// The depth of the segment's single dirty-index ring, in entries. Always a power of two.
    pub fn dirty_capacity(self) -> u32 {
        self.dirty_capacity
    }
    /// The mask that turns an absolute dirty position into a slot index in that one ring.
    pub fn dirty_position_mask(self) -> u64 {
        u64::from(self.dirty_capacity) - 1
    }
    /// Bytes the whole dirty-index ring occupies.
    pub fn dirty_ring_bytes(self) -> usize {
        self.dirty_capacity as usize * DIRTY_SLOT_BYTES
    }

    /// Bytes from a state slot's base to the next: the prefix plus every level cell,
    /// rounded up to [`REGION_ALIGNMENT`].
    pub fn state_slot_stride(self) -> usize {
        round_up(SLOT_PREFIX_BYTES + self.level_capacity as usize * LEVEL_CELL_BYTES)
    }
    /// Byte offset of the directory: immediately after the fixed header.
    pub fn directory_offset(self) -> usize {
        HEADER_BYTES
    }
    /// Byte offset of the first state slot.
    pub fn state_slot_offset(self) -> usize {
        self.directory_offset() + self.directory_capacity as usize * DIRECTORY_ENTRY_BYTES
    }
    /// Byte offset of the first market's event ring: immediately after the last state slot.
    pub fn event_offset(self) -> usize {
        self.state_slot_offset() + self.state_slot_capacity as usize * self.state_slot_stride()
    }
    /// Byte offset of the segment's dirty-index ring: immediately after the last event ring.
    pub fn dirty_offset(self) -> usize {
        self.event_offset() + self.directory_capacity as usize * self.event_ring_bytes()
    }
    /// Byte offset of the trailer.
    pub fn trailer_offset(self) -> usize {
        self.dirty_offset() + self.dirty_ring_bytes()
    }
    /// Total region size in bytes; always a multiple of [`REGION_ALIGNMENT`].
    pub fn region_size(self) -> usize {
        self.trailer_offset() + TRAILER_BYTES
    }
}

/// A segment's validated header: the geometry a reader may then rely on.
///
/// Two segments compare equal exactly when their headers declare the same instance,
/// generation and geometry, which is what makes "hostile bytes outside the header change
/// nothing" a testable claim.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentGeometry {
    daemon_instance_id: u128,
    segment_generation: u64,
    layout: SegmentLayout,
}

impl SegmentGeometry {
    /// The 128-bit identity of the daemon instance that formatted this segment, carried in
    /// the header as two explicit 64-bit halves.
    pub fn daemon_instance_id(self) -> u128 {
        self.daemon_instance_id
    }
    /// The segment generation. A reader that observes a different one attached to a
    /// different segment and must reattach.
    pub fn segment_generation(self) -> u64 {
        self.segment_generation
    }
    pub fn layout(self) -> SegmentLayout {
        self.layout
    }
}

/// Which attachment a handle belongs to: the segment identity its header declares, plus the
/// process-local identity of the region it was obtained through.
///
/// Carried in every [`MarketHandle`] and checked on every use, so a handle obtained from one
/// segment can never address a slot in another. The header identity alone would not be
/// enough — two segments may declare the same `daemon_instance_id` and `segment_generation`
/// — so the region's own [`SegmentRegion::attachment_id`] is included. A writer and a reader
/// sharing one region therefore agree on handles, while a second mapping of the same file is
/// a different attachment and its handles do not interchange.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentBinding {
    attachment: u64,
    daemon_instance_id: u128,
    segment_generation: u64,
}

impl SegmentBinding {
    pub(super) fn new(region: &SegmentRegion, geometry: SegmentGeometry) -> Self {
        Self {
            attachment: region.attachment_id(),
            daemon_instance_id: geometry.daemon_instance_id,
            segment_generation: geometry.segment_generation,
        }
    }
}

/// A process-local routing aid naming one installed market's directory entry and state
/// slot, bound to the segment it came from.
///
/// It is never an external identifier and never travels outside this process: the public
/// identity of a market stays the venue-native [`crate::MarketRef`] the entry carries. A
/// handle is valid only for the segment generation it was obtained from, and neither index
/// is reused within one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MarketHandle {
    segment: SegmentBinding,
    entry_index: u32,
    state_slot_index: u32,
}

impl MarketHandle {
    pub(super) fn new(segment: SegmentBinding, entry_index: u32, state_slot_index: u32) -> Self {
        Self {
            segment,
            entry_index,
            state_slot_index,
        }
    }
    /// The segment this handle addresses.
    pub fn segment(self) -> SegmentBinding {
        self.segment
    }
    pub fn entry_index(self) -> u32 {
        self.entry_index
    }
    /// The state slot serving this market, or [`NO_STATE_SLOT`] when none is published.
    pub fn state_slot_index(self) -> u32 {
        self.state_slot_index
    }
}

pub(super) fn header_record(cells: RegionCells<'_>) -> Result<Record<'_>, SegmentFault> {
    cells
        .record(0, HEADER_BYTES)
        .ok_or(SegmentFault::GeometryUnreachable)
}

pub(super) fn trailer_record(cells: RegionCells<'_>) -> Result<Record<'_>, SegmentFault> {
    let size = cells.size_bytes();
    cells
        .record(size - TRAILER_BYTES, TRAILER_BYTES)
        .ok_or(SegmentFault::GeometryUnreachable)
}

pub(super) fn directory_record(
    cells: RegionCells<'_>,
    layout: SegmentLayout,
    index: u32,
) -> Result<Record<'_>, SegmentFault> {
    if index >= layout.directory_capacity() {
        return Err(SegmentFault::GeometryUnreachable);
    }
    cells
        .record(
            layout.directory_offset() + index as usize * DIRECTORY_ENTRY_BYTES,
            DIRECTORY_ENTRY_BYTES,
        )
        .ok_or(SegmentFault::GeometryUnreachable)
}

pub(super) fn state_slot_record(
    cells: RegionCells<'_>,
    layout: SegmentLayout,
    index: u32,
) -> Result<Record<'_>, SegmentFault> {
    if index >= layout.state_slot_capacity() {
        return Err(SegmentFault::GeometryUnreachable);
    }
    cells
        .record(
            layout.state_slot_offset() + index as usize * layout.state_slot_stride(),
            layout.state_slot_stride(),
        )
        .ok_or(SegmentFault::GeometryUnreachable)
}

/// The retained-event slot serving absolute stream position `position` of the market at
/// `entry_index`.
///
/// The ring wraps: the slot is `ring_base + (position & (event_capacity - 1)) *
/// EVENT_SLOT_BYTES`, a mask rather than a modulo because the capacity is a power of two.
/// Fails with [`SegmentFault::GeometryUnreachable`] for an entry index outside the
/// directory.
pub(super) fn event_slot_record(
    cells: RegionCells<'_>,
    layout: SegmentLayout,
    entry_index: u32,
    position: u64,
) -> Result<Record<'_>, SegmentFault> {
    if entry_index >= layout.directory_capacity() {
        return Err(SegmentFault::GeometryUnreachable);
    }
    let ring_base = layout.event_offset() + entry_index as usize * layout.event_ring_bytes();
    let slot = (position & layout.event_position_mask()) as usize;
    cells
        .record(ring_base + slot * EVENT_SLOT_BYTES, EVENT_SLOT_BYTES)
        .ok_or(SegmentFault::GeometryUnreachable)
}

/// The dirty-index slot serving absolute dirty position `position`.
///
/// One ring serves the whole segment — a dirty entry names which directory entry changed, so
/// partitioning it per market would defeat its purpose — and it wraps by the same mask
/// arithmetic the event rings use.
pub(super) fn dirty_slot_record(
    cells: RegionCells<'_>,
    layout: SegmentLayout,
    position: u64,
) -> Result<Record<'_>, SegmentFault> {
    let slot = (position & layout.dirty_position_mask()) as usize;
    cells
        .record(
            layout.dirty_offset() + slot * DIRTY_SLOT_BYTES,
            DIRTY_SLOT_BYTES,
        )
        .ok_or(SegmentFault::GeometryUnreachable)
}

/// Validates a region's fixed header and trailer and returns the geometry they declare.
///
/// Reads only the bytes `0..HEADER_BYTES` and the final [`TRAILER_BYTES`], through the
/// typed cells: the header's magic is a write-once gate, so every geometry word is read
/// through the witness an acquire load of that gate produced, and
/// `publication_generation` — the one synchronizing cell in the header — is not read here
/// at all. Filling every byte between header and trailer with any pattern therefore leaves
/// the result identical.
///
/// Fails closed with the [`SegmentFault`] naming what did not match: an unformatted region
/// reads as [`SegmentFault::HeaderUnpublished`] rather than as zeroed geometry.
pub fn validate(region: &SegmentRegion) -> Result<SegmentGeometry, SegmentFault> {
    let cells = region.cells();
    let size = cells.size_bytes();
    if !size.is_multiple_of(REGION_ALIGNMENT) {
        return Err(SegmentFault::RegionMisaligned { size });
    }
    if size < HEADER_BYTES + TRAILER_BYTES {
        return Err(SegmentFault::RegionTooSmall { size });
    }
    let header = header_record(cells)?
        .published(HDR_MAGIC)
        .ok_or(SegmentFault::HeaderUnpublished)?;
    if header.gate() != MAGIC {
        return Err(SegmentFault::MagicMismatch {
            found: header.gate(),
        });
    }
    let abi_version = header.word32(HDR_ABI_VERSION);
    if abi_version != ABI_VERSION {
        return Err(SegmentFault::AbiVersionUnsupported {
            found: abi_version,
            expected: ABI_VERSION,
        });
    }
    let alignment = header.word32(HDR_REGION_ALIGNMENT);
    if alignment as usize != REGION_ALIGNMENT {
        return Err(SegmentFault::AlignmentMismatch {
            found: alignment,
            expected: REGION_ALIGNMENT as u32,
        });
    }
    let declared_size = header.word64(HDR_REGION_SIZE);
    if declared_size != size as u64 {
        return Err(SegmentFault::RegionSizeMismatch {
            declared: declared_size,
            actual: size as u64,
        });
    }
    let features = header.word64(HDR_FEATURE_BITS);
    let doorbell = features & (FEATURE_DOORBELL_IN_HEADER | FEATURE_DOORBELL_PAGE);
    if features & !KNOWN_FEATURE_BITS != 0
        || features & FEATURE_EVENT_RING_WRAPS == 0
        || doorbell.count_ones() != 1
    {
        return Err(SegmentFault::FeatureUnsupported { bits: features });
    }
    let layout = SegmentLayout::new(
        header.word32(HDR_DIRECTORY_CAPACITY),
        header.word32(HDR_SLOT_CAPACITY),
        header.word32(HDR_LEVEL_CAPACITY),
        header.word32(HDR_EVENT_CAPACITY),
        header.word32(HDR_DIRTY_CAPACITY),
    )
    .map_err(|_| SegmentFault::CapacityOutOfRange)?;
    if layout.region_size() != size
        || header.word64(HDR_DIRECTORY_OFFSET) != layout.directory_offset() as u64
        || header.word32(HDR_DIRECTORY_STRIDE) as usize != DIRECTORY_ENTRY_BYTES
        || header.word64(HDR_SLOT_OFFSET) != layout.state_slot_offset() as u64
        || header.word32(HDR_SLOT_STRIDE) as usize != layout.state_slot_stride()
        || header.word32(HDR_LEVEL_STRIDE) as usize != LEVEL_CELL_BYTES
        || header.word32(HDR_IDENTITY_CAPACITY) as usize != IDENTITY_CAPACITY
        || header.word32(HDR_FAMILY_CAPACITY) as usize != NATIVE_FAMILY_CAPACITY
        || header.word64(HDR_EVENT_OFFSET) != layout.event_offset() as u64
        || header.word32(HDR_EVENT_STRIDE) as usize != EVENT_SLOT_BYTES
        || header.word64(HDR_DIRTY_OFFSET) != layout.dirty_offset() as u64
        || header.word32(HDR_DIRTY_STRIDE) as usize != DIRTY_SLOT_BYTES
        || header.word64(HDR_TRAILER_OFFSET) != layout.trailer_offset() as u64
    {
        return Err(SegmentFault::GeometryMismatch);
    }
    let trailer = trailer_record(cells)?
        .published(TRL_MAGIC)
        .ok_or(SegmentFault::TrailerMismatch)?;
    if trailer.gate() != TRAILER_MAGIC || trailer.word64(TRL_REGION_SIZE) != size as u64 {
        return Err(SegmentFault::TrailerMismatch);
    }
    Ok(SegmentGeometry {
        daemon_instance_id: u128::from(header.word64(HDR_INSTANCE_HIGH)) << 64
            | u128::from(header.word64(HDR_INSTANCE_LOW)),
        segment_generation: header.word64(HDR_SEGMENT_GENERATION),
        layout,
    })
}

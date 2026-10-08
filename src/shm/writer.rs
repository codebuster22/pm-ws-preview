//! The single writer that formats a segment and publishes latest state into it.
//!
//! One [`SegmentWriter`] exists per segment: formatting refuses a region that already
//! carries a published header, so a restarting writer never adopts slots whose state it
//! cannot prove. Publication follows `docs/notes/shared-memory-model.md` §3.1 exactly —
//! release an odd `slot_revision`, relaxed-store every data word, release the next even
//! value — and never waits on a reader.
//!
//! A publication round ends by telling consumers there is something to look at: one
//! dirty-index entry naming the market, one relaxed increment of the doorbell before the
//! generation's release store, and one wake syscall after it. The wake is posted only by a
//! state publication, which is the terminal step of every commit round, so the cost is one
//! bounded syscall per round whatever the round contained and whether or not anybody is
//! parked — a read-only consumer owns no cell it could register a waiter count in.

use super::cell::{Record, RegionError, SegmentRegion, WakeAddress, WriteOnceField};
use super::codec;
use super::doorbell;
use super::layout::{
    self, DEC_COEFFICIENT_HIGH, DEC_COEFFICIENT_LOW, DEC_SCALE, DELIVERY_LEVEL_MUTATION,
    DELIVERY_MARKET_RESOLVED, DIRTY_BOOK_REVISION, DIRTY_DIRECTORY_INDEX, DIRTY_POSITION,
    DIRTY_SEQUENCE, DIRTY_SLOT_BYTES, ENT_IDENTITY, ENT_IDENTITY_LEN, ENT_REVISION, ENT_SLOT_INDEX,
    EVENT_SLOT_BYTES, EVT_ARRIVAL_TIME, EVT_BOOK_REVISION, EVT_COMMIT_TIME, EVT_CURSOR_EPOCH,
    EVT_CURSOR_POSITION, EVT_DAEMON_GENERATION, EVT_DELIVERY_KIND, EVT_DERIVATION,
    EVT_DIRECTORY_INDEX, EVT_FAMILY_LEN, EVT_FAMILY_WORDS, EVT_NEW_PRESENT, EVT_NEW_QUANTITY,
    EVT_OLD_PRESENT, EVT_OLD_QUANTITY, EVT_ORIGIN, EVT_PRICE, EVT_REPRESENTATION, EVT_RES_DATE,
    EVT_RES_DATE_LEN, EVT_RES_DELIVERY_PATH, EVT_RES_OUTCOME, EVT_RES_OUTCOME_LEN, EVT_RES_TYPE,
    EVT_RES_TYPE_LEN, EVT_RES_WINNING_INDEX, EVT_SEQUENCE, EVT_SIDE, EVT_SUBSCRIPTION_GENERATION,
    FEATURE_DOORBELL_IN_HEADER, FEATURE_DOORBELL_PAGE, FEATURE_EVENT_RING_WRAPS, HDR_ABI_VERSION,
    HDR_DIRECTORY_CAPACITY, HDR_DIRECTORY_OFFSET, HDR_DIRECTORY_STRIDE, HDR_DIRTY_CAPACITY,
    HDR_DIRTY_OFFSET, HDR_DIRTY_STRIDE, HDR_DOORBELL, HDR_EVENT_CAPACITY, HDR_EVENT_OFFSET,
    HDR_EVENT_STRIDE, HDR_FAMILY_CAPACITY, HDR_FEATURE_BITS, HDR_IDENTITY_CAPACITY,
    HDR_INSTANCE_HIGH, HDR_INSTANCE_LOW, HDR_LEVEL_CAPACITY, HDR_LEVEL_STRIDE, HDR_MAGIC,
    HDR_PUBLICATION_GENERATION, HDR_REGION_ALIGNMENT, HDR_REGION_SIZE, HDR_SEGMENT_GENERATION,
    HDR_SLOT_CAPACITY, HDR_SLOT_OFFSET, HDR_SLOT_STRIDE, HDR_TRAILER_OFFSET, HEADER_BYTES,
    IDENTITY_CAPACITY, LEVEL_CELL_BYTES, LVL_PRICE, LVL_QUANTITY, LVL_SIDE, MarketHandle,
    NATIVE_FAMILY_CAPACITY, NATIVE_FAMILY_WORDS, RES_DATE_WORDS, RES_OUTCOME_WORDS, RES_TYPE_WORDS,
    SLOT_ARRIVAL_TIME, SLOT_AUTHORITY_REASON, SLOT_AUTHORITY_STATE, SLOT_BOOK_REVISION,
    SLOT_COMMIT_TIME, SLOT_CONTINUITY_EPOCH, SLOT_CONTINUITY_KIND, SLOT_CONTINUITY_POSITION,
    SLOT_CONTINUITY_REASON, SLOT_DERIVATION, SLOT_DIRECTORY_INDEX, SLOT_FAMILY_LEN,
    SLOT_FAMILY_WORDS, SLOT_LEVEL_COUNT, SLOT_ORIGIN, SLOT_PREFIX_BYTES, SLOT_PROVENANCE_PRESENT,
    SLOT_REPRESENTATION, SLOT_REVISION, SLOT_SYNC_DIVERGENCES, SegmentBinding, SegmentFault,
    SegmentGeometry, SegmentLayout, TRL_MAGIC, TRL_REGION_SIZE,
};
use crate::{BookMutation, Level, MarketRef, MarketResolution, MutationCursor, PublishedBook};
use std::sync::Arc;

/// Why a write to a segment was refused.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriterError {
    RegionSizeMismatch {
        expected: usize,
        actual: usize,
    },
    SegmentAlreadyFormatted,
    RegionNotWritable,
    Segment(SegmentFault),
    DirectoryFull,
    IdentityTooLong,
    MarketAlreadyInstalled,
    UnknownHandle,
    MarketMismatch,
    LevelCapacityExceeded {
        levels: usize,
        capacity: u32,
    },
    NativeFamilyTooLong,
    /// A resolution's winning-outcome text is wider than the slot's fixed cell. Refused,
    /// never truncated: a truncated outcome names a different winner.
    WinningOutcomeTooLong,
    /// A resolution names its winner by an index alone, which this ABI's text cell cannot
    /// reproduce. Refused rather than stored as an empty string, which would claim the venue
    /// reported no winner.
    WinningOutcomeNotText,
    /// A resolution's venue-native market label is wider than the slot's fixed cell.
    /// Refused, never truncated.
    MarketTypeTooLong,
    /// A resolution's venue timestamp lexeme is wider than the slot's fixed cell. Refused,
    /// never truncated: a truncated lexeme names a different instant.
    ResolutionDateTooLong,
    /// A resolution arrived on a venue's own delivery path, which this ABI has no cell for.
    /// Refused rather than reported as one of the three named feeds.
    UnsupportedDeliveryPath,
    /// A mutation naming neither a level before the change nor one after it. A
    /// [`BookMutation`] refuses that shape at construction, so this is a fail-closed guard
    /// on the coordinate rather than a shape a caller can produce.
    EmptyMutation,
    CounterOverflow,
    /// A second segment offered to a supervisor that already installed one, whether or not
    /// the installed segment latched a publication failure. The offered segment is refused
    /// untouched, before it is written to, and the installed segment — including any
    /// latched failure it carries — is left exactly as it was.
    SegmentAlreadyInstalled,
    /// The sibling doorbell page could not be created beside the segment file. Formatting
    /// fails rather than continuing without a doorbell: a segment whose consumers cannot park
    /// is a silent regression to timer polling, not a degraded mode.
    DoorbellPage(RegionError),
    /// Something already occupies `<segment path>.doorbell`. The page is created exclusively
    /// and this writer never deletes what it did not create: the segment path is an operator
    /// argument, so an unlink here could destroy an unrelated file, FIFO, or socket that
    /// happens to sit at that name. Clearing a page a dead writer left behind is an operator
    /// action, which is why the path is carried rather than only the kind.
    DoorbellPageOccupied(std::path::PathBuf),
    /// A heap-backed region was asked for the sibling doorbell page. A heap region's readers
    /// share the writer's own writable mapping, so it has no file to put a page beside and
    /// needs none; the request is a caller mistake rather than a platform limit.
    DoorbellPageUnavailable,
}
impl core::fmt::Display for WriterError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("segment write refused")
    }
}
impl std::error::Error for WriterError {}

/// Where a segment's doorbell cell is to live.
///
/// The placement is decided once, at creation, and recorded in the header's feature word,
/// because the two placements are two different addresses: a consumer parked on one is never
/// woken by a writer ringing the other.
///
/// The two placements do not reach the same audience. A header doorbell is inside the
/// segment every consumer already maps, so anyone who can read the segment can park on it.
/// The sibling page must be mapped *writable* by a consumer, and is created owner-only for
/// the reason [`DOORBELL_PAGE_MODE`] gives, so on a page-placement platform parked waiting
/// reaches same-user consumers only: a cross-user consumer's page open fails, its first park
/// reports [`super::WaitFault::DoorbellUnavailable`], and spin mode — which never touches the
/// doorbell — is unaffected. Widening that audience needs a shared object whose length cannot
/// be changed after creation, which this ABI does not have.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DoorbellPlacement {
    /// Ask the platform. The writer maps its own file a second time read-only and probes a
    /// wait on that mapping's doorbell word; an immediate answer keeps the doorbell in the
    /// header, and a refusal takes the sibling page. A heap-backed region probes its own
    /// address, since its readers share that very mapping.
    #[default]
    Probe,
    /// Take the sibling page without probing, so the fallback is exercised deterministically
    /// on a host whose probe succeeds. Refused for a heap-backed region, which has no file to
    /// put a page beside.
    ForcePage,
}

/// What one segment is created as.
///
/// `daemon_instance_id` is the 128-bit identity of this daemon instance and
/// `segment_generation` distinguishes segments within it; a reader compares both against
/// what its attachment promised and reattaches on a mismatch. The dirty-index depth is not a
/// field here because it is geometry: it changes the region's size, so it belongs to
/// `layout` alongside the other capacities and is validated with them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentConfig {
    pub layout: SegmentLayout,
    pub daemon_instance_id: u128,
    pub segment_generation: u64,
    pub doorbell: DoorbellPlacement,
}

impl SegmentConfig {
    /// The configuration for `layout` under this daemon identity, with the default doorbell
    /// placement: probe the platform and fall back to the sibling page.
    pub fn new(layout: SegmentLayout, daemon_instance_id: u128, segment_generation: u64) -> Self {
        Self {
            layout,
            daemon_instance_id,
            segment_generation,
            doorbell: DoorbellPlacement::Probe,
        }
    }
}

struct DecimalWords {
    low: u64,
    high: u64,
    scale: u32,
}

struct LevelWords {
    side: u32,
    price: DecimalWords,
    quantity: DecimalWords,
}

struct StagedMutation {
    origin: u32,
    derivation: u32,
    representation: u32,
    side: u32,
    old_present: u32,
    new_present: u32,
    family_length: u32,
    family_words: [u64; NATIVE_FAMILY_WORDS],
    daemon_generation: u64,
    subscription_generation: u64,
    price: DecimalWords,
    old_quantity: DecimalWords,
    new_quantity: DecimalWords,
}

struct StagedResolution {
    origin: u32,
    derivation: u32,
    representation: u32,
    winning_index: u32,
    delivery_path: u32,
    outcome_length: u32,
    type_length: u32,
    date_length: u32,
    outcome_words: [u64; RES_OUTCOME_WORDS],
    type_words: [u64; RES_TYPE_WORDS],
    date_words: [u64; RES_DATE_WORDS],
    daemon_generation: u64,
    subscription_generation: u64,
}

struct StagedState {
    book_revision: u64,
    authority_state: u32,
    authority_reason: u32,
    continuity_kind: u32,
    continuity_reason: u32,
    continuity_epoch: u64,
    continuity_position: u64,
    sync_divergences: u64,
    provenance_present: u32,
    origin: u32,
    derivation: u32,
    representation: u32,
    family_length: u32,
    family_words: [u64; NATIVE_FAMILY_WORDS],
}

/// One market installed in the directory, and the two stamps its state slot last carried.
///
/// The stamps are remembered so a republication that commits no revision — a delivery ordered
/// after the last commit rather than one that changed the book — can carry the recorded values
/// forward instead of inventing fresh ones. Both keep meaning "when this book revision was
/// committed" and "when the frame that produced it was read off the socket", never "when some
/// word of this slot was last written".
struct InstalledMarket {
    market: MarketRef,
    commit_time: u64,
    arrival_time: u64,
}

/// The dirty-index entry one publication round is about to append.
///
/// Staged before the state slot's interval opens, for the same reason every other fallible
/// step is: the counter arithmetic must not be able to fail after the slot has been marked in
/// flight.
struct StagedDirty {
    position: u64,
    in_flight: u64,
    next_stable: u64,
}

/// Where this segment's doorbell actually lives, once the writer has decided.
///
/// The page variant owns the sibling region for the writer's lifetime, which is what keeps
/// the mapping the wake syscall names alive.
enum Doorbell {
    InHeader,
    Page(SegmentRegion),
}

/// The sole writer of one publication segment.
///
/// Not [`Clone`], and the only constructor refuses an already-formatted region, so a
/// segment has one writer for the life of its generation. Readers never take a lock this
/// type holds, and no reader can slow a publication down.
pub struct SegmentWriter {
    region: Arc<SegmentRegion>,
    geometry: SegmentGeometry,
    binding: SegmentBinding,
    installed: Vec<InstalledMarket>,
    staged_levels: Vec<LevelWords>,
    doorbell: Doorbell,
    dirty_position: u64,
    wake_posts: u64,
    wake_faults: u64,
}

impl SegmentWriter {
    /// Formats `region` as an empty segment and returns its writer.
    ///
    /// The region must be exactly [`SegmentLayout::region_size`] bytes and must not already
    /// carry a published header: a region that does fails with
    /// [`WriterError::SegmentAlreadyFormatted`] rather than being adopted, because a new
    /// writer cannot prove what state a previous one left the slots in. On success the
    /// header and trailer are published, `publication_generation` is zero, and no market is
    /// installed.
    ///
    /// The doorbell placement is decided after the trailer's write-once claim and before the
    /// header is published, so the feature word can carry it. After the claim on purpose: the
    /// claim is the one serialization point between two formatters racing on the same region,
    /// and deciding before it would let a loser's page creation collide with the winner's.
    /// The cost is that a page this cannot create — refused as occupied, or refused by the
    /// filesystem — leaves the region with a claimed trailer and no header: unretryable, and
    /// the right answer for a fresh file that is discarded anyway.
    ///
    /// Fails with [`WriterError::RegionNotWritable`] for a read-only mapping,
    /// [`WriterError::RegionSizeMismatch`] on a wrongly sized region,
    /// [`WriterError::DoorbellPage`] when the sibling doorbell page cannot be created,
    /// [`WriterError::DoorbellPageOccupied`] when something already sits at the page's path,
    /// [`WriterError::DoorbellPageUnavailable`] for a heap-backed region asked to use one, and
    /// [`WriterError::Segment`] when the header it just wrote does not validate, which
    /// would be a layout defect rather than a caller error.
    pub fn create(region: Arc<SegmentRegion>, config: SegmentConfig) -> Result<Self, WriterError> {
        let layout = config.layout;
        if !region.is_writable() {
            return Err(WriterError::RegionNotWritable);
        }
        let size = region.size_bytes();
        if size != layout.region_size() {
            return Err(WriterError::RegionSizeMismatch {
                expected: layout.region_size(),
                actual: size,
            });
        }
        let cells = region.cells();
        let trailer = layout::trailer_record(cells).map_err(WriterError::Segment)?;
        let header = layout::header_record(cells).map_err(WriterError::Segment)?;
        if !trailer.publish_write_once(
            TRL_MAGIC,
            layout::TRAILER_MAGIC,
            &[WriteOnceField::Word64 {
                offset: TRL_REGION_SIZE,
                value: size as u64,
            }],
        ) {
            return Err(WriterError::SegmentAlreadyFormatted);
        }
        let doorbell = choose_doorbell(&region, config.doorbell)?;
        let doorbell_bit = match doorbell {
            Doorbell::InHeader => FEATURE_DOORBELL_IN_HEADER,
            Doorbell::Page(_) => FEATURE_DOORBELL_PAGE,
        };
        let words64: [(usize, u64); 10] = [
            (HDR_REGION_SIZE, size as u64),
            (HDR_FEATURE_BITS, FEATURE_EVENT_RING_WRAPS | doorbell_bit),
            (HDR_EVENT_OFFSET, layout.event_offset() as u64),
            (HDR_DIRTY_OFFSET, layout.dirty_offset() as u64),
            (HDR_INSTANCE_LOW, config.daemon_instance_id as u64),
            (HDR_INSTANCE_HIGH, (config.daemon_instance_id >> 64) as u64),
            (HDR_SEGMENT_GENERATION, config.segment_generation),
            (HDR_DIRECTORY_OFFSET, layout.directory_offset() as u64),
            (HDR_SLOT_OFFSET, layout.state_slot_offset() as u64),
            (HDR_TRAILER_OFFSET, layout.trailer_offset() as u64),
        ];
        let words32: [(usize, u32); 13] = [
            (HDR_ABI_VERSION, layout::ABI_VERSION),
            (HDR_EVENT_CAPACITY, layout.event_capacity()),
            (HDR_EVENT_STRIDE, EVENT_SLOT_BYTES as u32),
            (HDR_DIRTY_CAPACITY, layout.dirty_capacity()),
            (HDR_DIRTY_STRIDE, DIRTY_SLOT_BYTES as u32),
            (HDR_REGION_ALIGNMENT, super::cell::REGION_ALIGNMENT as u32),
            (HDR_DIRECTORY_STRIDE, layout::DIRECTORY_ENTRY_BYTES as u32),
            (HDR_DIRECTORY_CAPACITY, layout.directory_capacity()),
            (HDR_SLOT_STRIDE, layout.state_slot_stride() as u32),
            (HDR_SLOT_CAPACITY, layout.state_slot_capacity()),
            (HDR_LEVEL_CAPACITY, layout.level_capacity()),
            (HDR_LEVEL_STRIDE, LEVEL_CELL_BYTES as u32),
            (HDR_IDENTITY_CAPACITY, IDENTITY_CAPACITY as u32),
        ];
        let mut fields: Vec<WriteOnceField<'_>> = words64
            .iter()
            .map(|(offset, value)| WriteOnceField::Word64 {
                offset: *offset,
                value: *value,
            })
            .chain(
                words32
                    .iter()
                    .map(|(offset, value)| WriteOnceField::Word32 {
                        offset: *offset,
                        value: *value,
                    }),
            )
            .collect();
        fields.push(WriteOnceField::Word32 {
            offset: HDR_FAMILY_CAPACITY,
            value: NATIVE_FAMILY_CAPACITY as u32,
        });
        let published = header.publish_write_once(HDR_MAGIC, layout::MAGIC, &fields);
        if !published {
            return Err(WriterError::SegmentAlreadyFormatted);
        }
        let geometry = layout::validate(&region).map_err(WriterError::Segment)?;
        let binding = SegmentBinding::new(&region, geometry);
        let staged_levels = Vec::with_capacity(layout.level_capacity() as usize);
        Ok(Self {
            region,
            geometry,
            binding,
            installed: Vec::new(),
            staged_levels,
            doorbell,
            dirty_position: 0,
            wake_posts: 0,
            wake_faults: 0,
        })
    }

    /// The doorbell feature bit this segment declares: exactly one of
    /// [`FEATURE_DOORBELL_IN_HEADER`] and [`FEATURE_DOORBELL_PAGE`].
    ///
    /// It is the probe's answer, not this build's assumption, so it is worth reporting: the
    /// same source takes the header word on one platform and the sibling page on another.
    pub fn doorbell_feature_bit(&self) -> u64 {
        match self.doorbell {
            Doorbell::InHeader => FEATURE_DOORBELL_IN_HEADER,
            Doorbell::Page(_) => FEATURE_DOORBELL_PAGE,
        }
    }

    /// The device and inode of the sibling doorbell page this writer created, or `None` for a
    /// segment whose doorbell lives in its header.
    ///
    /// It is the page's own `fstat` taken at creation, exposed so that an owner which cleans
    /// the page up at shutdown can make that removal conditional on the object it created
    /// rather than on whatever `<segment>.doorbell` names by then.
    pub fn doorbell_page_identity(&self) -> Option<(u64, u64)> {
        match &self.doorbell {
            Doorbell::InHeader => None,
            Doorbell::Page(page) => page.creation_identity(),
        }
    }

    /// How many wake syscalls this writer has posted: exactly one per publication round that
    /// ended in a state publication, waiters or not.
    ///
    /// Exposed so a proof can assert the cadence — a commit of *n* mutations plus its state
    /// costs one wake, never *n* + 1 — and so a live run can report a measured wake rate
    /// rather than an assumed one.
    pub fn wake_posts(&self) -> u64 {
        self.wake_posts
    }

    /// How many of those posts the platform refused for a reason other than "no waiters".
    ///
    /// A wake failure is counted and never fatal: it costs a consumer one wake-up, which its
    /// own timeout covers, and a writer that latched on it would trade a missed wake for a
    /// stopped feed.
    pub fn wake_faults(&self) -> u64 {
        self.wake_faults
    }

    /// The address consumers of this segment park on.
    pub(super) fn doorbell_address(&self) -> WakeAddress {
        match &self.doorbell {
            Doorbell::InHeader => header_doorbell(&self.region).wake_address(),
            Doorbell::Page(page) => page_doorbell(page).wake_address(),
        }
    }

    /// Advances the doorbell, mirroring it into the sibling page when that is where consumers
    /// park.
    ///
    /// A relaxed store is sufficient and correct: the doorbell carries no data, the
    /// happens-before edge for everything a wake advertises is `publication_generation`'s
    /// release store — which this always precedes — and wake *delivery* ordering comes from
    /// the wait and wake syscalls themselves. A consumer about to park re-reads the doorbell
    /// and re-checks the generation before it blocks, so a bump landing in that window makes
    /// its equality-based wait return at once; the lost-wake window is closed by that recheck,
    /// not by this cell's ordering.
    fn ring_doorbell(&self) {
        let cell = header_doorbell(&self.region);
        let bumped = cell.relaxed_load().wrapping_add(1);
        cell.relaxed_store(bumped);
        if let Doorbell::Page(page) = &self.doorbell {
            page_doorbell(page).relaxed_store(bumped);
        }
    }

    /// Posts one wake for the round that just ended.
    ///
    /// Called only at the end of a state publication — the terminal step of every commit
    /// round on the mutations-before-state path — so a commit of any width costs exactly one
    /// syscall. The residual is named and accepted: a writer that dies between a mutation
    /// publication and its state publication leaves an already-parked consumer unwoken until
    /// its own timeout, which is the same crash window that already leaves a slot's counter
    /// odd.
    fn post_wake(&mut self) {
        let address = self.doorbell_address();
        self.wake_posts = self.wake_posts.wrapping_add(1);
        match doorbell::wake_all(address) {
            Ok(_) => {}
            Err(_) => self.wake_faults = self.wake_faults.wrapping_add(1),
        }
    }

    pub fn geometry(&self) -> SegmentGeometry {
        self.geometry
    }

    /// The binding every handle this writer issues carries.
    pub fn binding(&self) -> SegmentBinding {
        self.binding
    }

    /// The publication generation, acquire-loaded: a coalescible hint that newer data may
    /// exist somewhere in the segment, never the authority for any one market.
    pub fn publication_generation(&self) -> u64 {
        let cells = self.region.cells();
        layout::header_record(cells)
            .map(|header| header.sync(HDR_PUBLICATION_GENERATION).acquire_load())
            .unwrap_or_default()
    }

    /// Installs `market` in the directory, binding it to a state slot for the life of this
    /// segment generation, and returns its process-local handle.
    ///
    /// An entry and its slot share one index: the nth market installed takes directory
    /// entry n and state slot n, and neither is ever reused for another market, which is
    /// what makes a stale handle unable to address a different book.
    /// [`SegmentLayout::new`] already refuses a segment with fewer slots than entries, so
    /// the directory is the only capacity that can run out here.
    ///
    /// The identity bytes and the slot index are written before the entry's revision is
    /// released, so a reader that acquire-loads a non-zero revision may read both.
    ///
    /// Fails with [`WriterError::MarketAlreadyInstalled`], [`WriterError::DirectoryFull`],
    /// or [`WriterError::IdentityTooLong`] when the venue-native identity does not fit the
    /// entry's identity capacity — refused, never truncated.
    pub fn install(&mut self, market: &MarketRef) -> Result<MarketHandle, WriterError> {
        if self
            .installed
            .iter()
            .any(|installed| &installed.market == market)
        {
            return Err(WriterError::MarketAlreadyInstalled);
        }
        let index = u32::try_from(self.installed.len()).map_err(|_| WriterError::DirectoryFull)?;
        let layout = self.geometry.layout();
        if index >= layout.directory_capacity() {
            return Err(WriterError::DirectoryFull);
        }
        let identity = codec::encode_identity(market, IDENTITY_CAPACITY)
            .ok_or(WriterError::IdentityTooLong)?;
        let identity_len =
            u32::try_from(identity.len()).map_err(|_| WriterError::IdentityTooLong)?;
        let cells = self.region.cells();
        let slot = layout::state_slot_record(cells, layout, index).map_err(WriterError::Segment)?;
        slot.word32(SLOT_DIRECTORY_INDEX).relaxed_store(index);
        let entry = layout::directory_record(cells, layout, index).map_err(WriterError::Segment)?;
        let published = entry.publish_write_once(
            ENT_REVISION,
            1,
            &[
                WriteOnceField::Word32 {
                    offset: ENT_SLOT_INDEX,
                    value: index,
                },
                WriteOnceField::Word32 {
                    offset: ENT_IDENTITY_LEN,
                    value: identity_len,
                },
                WriteOnceField::Bytes {
                    offset: ENT_IDENTITY,
                    value: &identity,
                },
            ],
        );
        if !published {
            return Err(WriterError::MarketAlreadyInstalled);
        }
        self.installed.push(InstalledMarket {
            market: market.clone(),
            commit_time: 0,
            arrival_time: 0,
        });
        Ok(MarketHandle::new(self.binding, index, index))
    }

    /// Publishes `book` into the state slot `handle` names.
    ///
    /// Every fallible step — level capacity, native-family capacity, decimal encoding,
    /// counter arithmetic — happens before the slot is marked in flight, so a refused
    /// publication leaves the slot readable at its previous revision. The publication
    /// itself is the even-odd sequence: an odd release store marks the slot in flight,
    /// every data word is relaxed-stored, and the next even release store makes it stable
    /// again, after which `publication_generation` advances once.
    ///
    /// Every store is a plain memory write: publishing performs no system call, no
    /// allocation and no blocking I/O, whichever backing the region has. The level-staging
    /// buffer is allocated once at [`Self::create`] at the segment's level capacity and
    /// reused, so a publication that fits the slot never grows it. A file-backed
    /// region is sized and zero-filled at creation, so no publication extends the file and
    /// none takes a first-touch allocation fault. Residency itself is still the operating
    /// system's to decide — a page reclaimed under memory pressure faults back in on the
    /// next store — which no in-process code controls.
    ///
    /// `SLOT_COMMIT_TIME` is stamped with the current wall clock and `SLOT_ARRIVAL_TIME` with
    /// `arrival_time_nanos`, the wall-clock nanosecond at which the socket read that caused
    /// this commit returned; 0 means this commit was not driven by a venue frame. Both stamps
    /// are remembered for this handle so [`Self::republish_carrying_stamps`] can carry them.
    /// They therefore always mean "when this book revision was stamped" and "when its frame
    /// arrived", never "when some word of this slot was last written". The commit stamp is
    /// taken before the staging and the slot write below, so it marks the start of this
    /// round's writing rather than its completion; see [`Self::published_stamps`].
    ///
    /// The round ends with three further steps a consumer depends on: one dirty-index entry
    /// naming this market, one doorbell increment before the generation's release store, and
    /// one wake syscall after it. The wake is posted here and only here — a state publication
    /// is the terminal step of every commit round on the mutations-before-state path — so a
    /// commit of any number of mutations costs exactly one wake.
    ///
    /// Fails with [`WriterError::UnknownHandle`] for a handle this writer did not issue —
    /// including one bound to a different segment — [`WriterError::MarketMismatch`] when
    /// `book` is not the market installed at that entry, so a book can never be published
    /// under another market's identity, [`WriterError::LevelCapacityExceeded`] for a book
    /// deeper than the slot,
    /// [`WriterError::NativeFamilyTooLong`], and [`WriterError::CounterOverflow`] when the
    /// slot revision or the publication generation would wrap.
    pub fn publish(
        &mut self,
        handle: MarketHandle,
        book: &PublishedBook,
        arrival_time_nanos: u64,
    ) -> Result<(), WriterError> {
        self.publish_stamped(handle, book, commit_time_nanos(), arrival_time_nanos)
    }

    /// Publishes `book` into the state slot `handle` names carrying the stamps that slot
    /// already holds, for a delivery that advanced the stream without committing a revision.
    ///
    /// Every word is written exactly as [`Self::publish`] writes it — the same staging, the
    /// same even-odd interval, the same generation advance, the same dirty entry, doorbell
    /// bump and wake — except that `SLOT_COMMIT_TIME` and `SLOT_ARRIVAL_TIME` take the stamps
    /// this writer last recorded for `handle` rather than the current clock and a fresh
    /// arrival, so a book that has not moved is never advertised as freshly committed or
    /// freshly arrived. A handle for which nothing has been stamped yet carries 0 for both,
    /// which a reader reports as no stamp at all; [`crate::limitless::supervisor::Supervisor::publish_into`] publishes state at
    /// install, so a resolution ahead of any state publication is not a reachable state, and
    /// this is fail-closed rather than a case to rely on.
    ///
    /// This is what keeps the state slot's `(epoch, next_position)` boundary tracking the
    /// ring's tip when the stream advances for a reason other than a commit. That boundary is
    /// where every attachment starts, so a boundary standing still behind a live ring would
    /// let the ring lap the very slot [`super::SegmentReader::attach_stream`] probes, and
    /// every new attachment would fail [`super::ReadFault::Contended`] for good.
    ///
    /// Fails exactly as [`Self::publish`] does.
    pub(crate) fn republish_carrying_stamps(
        &mut self,
        handle: MarketHandle,
        book: &PublishedBook,
    ) -> Result<(), WriterError> {
        let carried = usize::try_from(handle.entry_index())
            .ok()
            .and_then(|index| self.installed.get(index));
        let commit_time = carried.map_or(0, |installed| installed.commit_time);
        let arrival_time = carried.map_or(0, |installed| installed.arrival_time);
        self.publish_stamped(handle, book, commit_time, arrival_time)
    }

    /// The commit and arrival stamps the state slot `handle` names currently advertises, or
    /// `None` for a handle this writer did not issue.
    ///
    /// Both are wall-clock nanoseconds taken in this process — the commit by this writer, the
    /// arrival by whatever read the frame that drove it — so their difference is a same-clock
    /// interval that needs no cross-process calibration. `(0, 0)` for a slot nothing has been
    /// published into, and `arrival` is 0 for a publication no venue frame drove.
    ///
    /// These are the stamps the most recent *accepted* publication wrote, which for a slot
    /// whose last write was [`Self::republish_carrying_stamps`] are an earlier commit's,
    /// carried forward rather than freshly measured.
    ///
    /// Their difference is not this daemon's publish latency and must not be read as it: the
    /// commit stamp is taken before the levels are staged and the slot written, so the
    /// interval stops short of the dirty entry, the generation advance and the wake, and both
    /// stamps move with the system clock. What a publication cost is measured monotonically
    /// where the publication completes, in `crate::limitless::shard::PublishLatencySummary`.
    /// These stamps exist for the consumer that cannot read that: they are the pinned ABI
    /// cells a reader in another process derives its own arrival-to-commit split from.
    pub fn published_stamps(&self, handle: MarketHandle) -> Option<(u64, u64)> {
        if handle.segment() != self.binding {
            return None;
        }
        usize::try_from(handle.entry_index())
            .ok()
            .and_then(|index| self.installed.get(index))
            .map(|installed| (installed.commit_time, installed.arrival_time))
    }

    /// Publishes one state revision stamping `commit_time` and `arrival_time`, and records
    /// both for the handle once the publication is complete, so a refused one records nothing.
    fn publish_stamped(
        &mut self,
        handle: MarketHandle,
        book: &PublishedBook,
        commit_time: u64,
        arrival_time: u64,
    ) -> Result<(), WriterError> {
        let index = handle.entry_index();
        if handle.segment() != self.binding || handle.state_slot_index() != index {
            return Err(WriterError::UnknownHandle);
        }
        match usize::try_from(index)
            .ok()
            .and_then(|index| self.installed.get(index))
            .map(|installed| installed.market == *book.market())
        {
            None => return Err(WriterError::UnknownHandle),
            Some(false) => return Err(WriterError::MarketMismatch),
            Some(true) => {}
        }
        let staged = self.stage(book)?;
        let dirty = self.stage_dirty()?;
        let cells = self.region.cells();
        let layout = self.geometry.layout();
        let header = layout::header_record(cells).map_err(WriterError::Segment)?;
        let slot = layout::state_slot_record(cells, layout, index).map_err(WriterError::Segment)?;
        let generation = header.sync(HDR_PUBLICATION_GENERATION);
        let next_generation = generation
            .acquire_load()
            .checked_add(1)
            .ok_or(WriterError::CounterOverflow)?;

        let revision = slot.seq(SLOT_REVISION);
        let stable = revision.writer_current();
        let in_flight = stable.checked_add(1).ok_or(WriterError::CounterOverflow)?;
        let next_stable = stable.checked_add(2).ok_or(WriterError::CounterOverflow)?;
        let interval = revision.open_write(in_flight);

        for (offset, value) in [
            (SLOT_BOOK_REVISION, staged.book_revision),
            (SLOT_CONTINUITY_EPOCH, staged.continuity_epoch),
            (SLOT_CONTINUITY_POSITION, staged.continuity_position),
            (SLOT_SYNC_DIVERGENCES, staged.sync_divergences),
            (SLOT_COMMIT_TIME, commit_time),
            (SLOT_ARRIVAL_TIME, arrival_time),
        ] {
            slot.word64(offset).relaxed_store(value);
        }
        for (word, value) in staged.family_words.iter().enumerate() {
            slot.word64(SLOT_FAMILY_WORDS + word * 8)
                .relaxed_store(*value);
        }
        for (offset, value) in [
            (SLOT_AUTHORITY_STATE, staged.authority_state),
            (SLOT_AUTHORITY_REASON, staged.authority_reason),
            (SLOT_CONTINUITY_KIND, staged.continuity_kind),
            (SLOT_CONTINUITY_REASON, staged.continuity_reason),
            (SLOT_PROVENANCE_PRESENT, staged.provenance_present),
            (SLOT_ORIGIN, staged.origin),
            (SLOT_DERIVATION, staged.derivation),
            (SLOT_REPRESENTATION, staged.representation),
            (SLOT_FAMILY_LEN, staged.family_length),
        ] {
            slot.word32(offset).relaxed_store(value);
        }
        for (position, level) in self.staged_levels.iter().enumerate() {
            let cell = slot.sub(
                SLOT_PREFIX_BYTES + position * LEVEL_CELL_BYTES,
                LEVEL_CELL_BYTES,
            );
            cell.word32(LVL_SIDE).relaxed_store(level.side);
            store_decimal(
                cell.sub(LVL_PRICE, super::layout::DECIMAL_CELL_BYTES),
                &level.price,
            );
            store_decimal(
                cell.sub(LVL_QUANTITY, super::layout::DECIMAL_CELL_BYTES),
                &level.quantity,
            );
        }
        slot.word32(SLOT_LEVEL_COUNT)
            .relaxed_store(self.staged_levels.len() as u32);

        revision.close_write(interval, next_stable);
        self.commit_dirty(&dirty, index, staged.book_revision);
        self.ring_doorbell();
        generation.release_store(next_generation);
        self.dirty_position = dirty.position.wrapping_add(1);
        self.post_wake();
        if let Some(installed) = usize::try_from(index)
            .ok()
            .and_then(|index| self.installed.get_mut(index))
        {
            installed.commit_time = commit_time;
            installed.arrival_time = arrival_time;
        }
        Ok(())
    }

    /// Reserves this round's dirty-index position and its sequence arithmetic.
    fn stage_dirty(&self) -> Result<StagedDirty, WriterError> {
        let position = self.dirty_position;
        let cells = self.region.cells();
        let layout = self.geometry.layout();
        let slot =
            layout::dirty_slot_record(cells, layout, position).map_err(WriterError::Segment)?;
        let stable = slot.seq(DIRTY_SEQUENCE).writer_current();
        Ok(StagedDirty {
            position,
            in_flight: stable.checked_add(1).ok_or(WriterError::CounterOverflow)?,
            next_stable: stable.checked_add(2).ok_or(WriterError::CounterOverflow)?,
        })
    }

    /// Appends the staged dirty-index entry under its slot's fenced even-odd sequence.
    ///
    /// Posted inside the publication round — after the state slot's own interval closes and
    /// before the generation bump the wake advertises — so a woken consumer always finds the
    /// entry that woke it. The ring wraps by mask and the writer never inspects a reader's
    /// cursor: a consumer the writer outran reads a position ahead of the one it expected,
    /// which is the declared full-rescan signal rather than silence.
    /// The writer's own next position advances at the end of the round rather than here, so
    /// that this borrows the region only shared and the round's closing generation store can
    /// still reach it.
    fn commit_dirty(&self, staged: &StagedDirty, index: u32, book_revision: u64) {
        let cells = self.region.cells();
        let layout = self.geometry.layout();
        let Ok(slot) = layout::dirty_slot_record(cells, layout, staged.position) else {
            return;
        };
        let sequence = slot.seq(DIRTY_SEQUENCE);
        let interval = sequence.open_write(staged.in_flight);
        slot.word64(DIRTY_POSITION).relaxed_store(staged.position);
        slot.word32(DIRTY_DIRECTORY_INDEX).relaxed_store(index);
        slot.word64(DIRTY_BOOK_REVISION)
            .relaxed_store(book_revision);
        sequence.close_write(interval, staged.next_stable);
    }

    /// Publishes one level mutation into the retained-event ring of the market `handle`
    /// names, at the ring slot its `cursor` position maps to.
    ///
    /// **Overflow behaviour.** The ring wraps. The writer never blocks, never skips a
    /// position, and never consults a reader: a ring whose capacity is exhausted overwrites
    /// its oldest slot, and there is no such thing as a failed publication for want of
    /// room. A consumer whose position was overwritten receives
    /// [`crate::ContinuityReason::Overrun`] on its next poll, the same loss on every later
    /// poll until it reattaches, and never partial history.
    ///
    /// Every fallible step — the handle and binding check, the native-family capacity, the
    /// mutation's coordinate, the sequence and generation arithmetic — happens before the
    /// slot is marked in flight, so a refused publication leaves the slot readable at
    /// whatever it last held. The publication itself is the fenced even-odd sequence of
    /// `docs/notes/shared-memory-model.md` §3.3: a relaxed odd store marks the slot in
    /// flight, every data word is relaxed-stored, and the next even release store makes it
    /// stable again, after which the doorbell advances and `publication_generation` advances
    /// once. Unlike [`Self::publish`] it posts no wake and appends no dirty entry: the state
    /// publication that closes this commit round does both, which is what makes a commit of
    /// *n* mutations cost one wake rather than *n* + 1. Its stores are otherwise plain memory
    /// writes — no allocation and no blocking I/O.
    ///
    /// **No ordering coupling with [`Self::publish`] is required.** An attachment reads
    /// state at revision `R` carrying next position `P`; everything below `P` is already
    /// contained in that state, and everything at or above `P` will be written to the ring.
    /// Whether this daemon publishes state before its mutations or after them therefore
    /// affects only how long a consumer idles, never what it sees.
    ///
    /// Fails with [`WriterError::UnknownHandle`] for a handle this writer did not issue —
    /// including one bound to a different segment — [`WriterError::MarketMismatch`] when
    /// the mutation's provenance names a market other than the one installed at that entry,
    /// so a change can never be published into another market's ring,
    /// [`WriterError::NativeFamilyTooLong`], [`WriterError::EmptyMutation`], and
    /// [`WriterError::CounterOverflow`] when the slot sequence or the publication
    /// generation would wrap.
    pub fn publish_mutation(
        &mut self,
        handle: MarketHandle,
        revision: u64,
        cursor: &MutationCursor,
        mutation: &BookMutation,
        arrival_time_nanos: u64,
    ) -> Result<(), WriterError> {
        let index = handle.entry_index();
        if handle.segment() != self.binding || handle.state_slot_index() != index {
            return Err(WriterError::UnknownHandle);
        }
        match usize::try_from(index)
            .ok()
            .and_then(|index| self.installed.get(index))
            .map(|installed| installed.market == *mutation.provenance().market())
        {
            None => return Err(WriterError::UnknownHandle),
            Some(false) => return Err(WriterError::MarketMismatch),
            Some(true) => {}
        }
        let staged = stage_mutation(mutation)?;
        let cells = self.region.cells();
        let layout = self.geometry.layout();
        let header = layout::header_record(cells).map_err(WriterError::Segment)?;
        let slot = layout::event_slot_record(cells, layout, index, cursor.position())
            .map_err(WriterError::Segment)?;
        let generation = header.sync(HDR_PUBLICATION_GENERATION);
        let next_generation = generation
            .acquire_load()
            .checked_add(1)
            .ok_or(WriterError::CounterOverflow)?;

        let sequence = slot.seq(EVT_SEQUENCE);
        let stable = sequence.writer_current();
        let in_flight = stable.checked_add(1).ok_or(WriterError::CounterOverflow)?;
        let next_stable = stable.checked_add(2).ok_or(WriterError::CounterOverflow)?;
        let interval = sequence.open_write(in_flight);

        for (offset, value) in [
            (EVT_CURSOR_EPOCH, cursor.epoch()),
            (EVT_CURSOR_POSITION, cursor.position()),
            (EVT_BOOK_REVISION, revision),
            (EVT_COMMIT_TIME, commit_time_nanos()),
            (EVT_ARRIVAL_TIME, arrival_time_nanos),
            (EVT_DAEMON_GENERATION, staged.daemon_generation),
            (EVT_SUBSCRIPTION_GENERATION, staged.subscription_generation),
        ] {
            slot.word64(offset).relaxed_store(value);
        }
        for (word, value) in staged.family_words.iter().enumerate() {
            slot.word64(EVT_FAMILY_WORDS + word * 8)
                .relaxed_store(*value);
        }
        for (offset, value) in [
            (EVT_DELIVERY_KIND, DELIVERY_LEVEL_MUTATION),
            (EVT_ORIGIN, staged.origin),
            (EVT_DERIVATION, staged.derivation),
            (EVT_REPRESENTATION, staged.representation),
            (EVT_SIDE, staged.side),
            (EVT_OLD_PRESENT, staged.old_present),
            (EVT_NEW_PRESENT, staged.new_present),
            (EVT_FAMILY_LEN, staged.family_length),
            (EVT_DIRECTORY_INDEX, index),
        ] {
            slot.word32(offset).relaxed_store(value);
        }
        for (offset, words) in [
            (EVT_PRICE, &staged.price),
            (EVT_OLD_QUANTITY, &staged.old_quantity),
            (EVT_NEW_QUANTITY, &staged.new_quantity),
        ] {
            store_decimal(slot.sub(offset, super::layout::DECIMAL_CELL_BYTES), words);
        }

        sequence.close_write(interval, next_stable);
        self.ring_doorbell();
        generation.release_store(next_generation);
        Ok(())
    }

    /// Publishes one venue-reported market resolution into the retained-event ring of the
    /// market `handle` names, at the ring slot its `cursor` position maps to.
    ///
    /// The slot is the same 256 bytes a mutation occupies, under delivery kind
    /// [`DELIVERY_MARKET_RESOLVED`], so a resolution is ordered with the book's level
    /// changes rather than carried on a second lane a consumer would have to reconcile.
    /// `revision` is the book revision the resolution is ordered *after*: a resolution
    /// changes no level and advances no revision. Overflow, blocking, the fenced even-odd
    /// sequence, the doorbell bump and the absence of a wake are exactly
    /// [`Self::publish_mutation`]'s.
    ///
    /// The three venue-native texts are stored verbatim and refused when they do not fit —
    /// [`WriterError::WinningOutcomeTooLong`], [`WriterError::MarketTypeTooLong`],
    /// [`WriterError::ResolutionDateTooLong`] — never truncated, because a truncated value
    /// names something the venue did not report. Every fallible step happens before the
    /// slot is marked in flight, so a refused publication leaves the slot readable at
    /// whatever it last held and does not advance `publication_generation`.
    ///
    /// Also fails with [`WriterError::UnknownHandle`] for a handle this writer did not
    /// issue, [`WriterError::MarketMismatch`] when the resolution's provenance names another
    /// market, [`WriterError::UnsupportedDeliveryPath`] for a venue's own delivery path, and
    /// [`WriterError::CounterOverflow`] when the slot sequence or the publication generation
    /// would wrap.
    pub fn publish_resolution(
        &mut self,
        handle: MarketHandle,
        revision: u64,
        cursor: &MutationCursor,
        resolution: &MarketResolution,
        arrival_time_nanos: u64,
    ) -> Result<(), WriterError> {
        let index = handle.entry_index();
        if handle.segment() != self.binding || handle.state_slot_index() != index {
            return Err(WriterError::UnknownHandle);
        }
        match usize::try_from(index)
            .ok()
            .and_then(|index| self.installed.get(index))
            .map(|installed| installed.market == *resolution.provenance().market())
        {
            None => return Err(WriterError::UnknownHandle),
            Some(false) => return Err(WriterError::MarketMismatch),
            Some(true) => {}
        }
        let staged = stage_resolution(resolution)?;
        let cells = self.region.cells();
        let layout = self.geometry.layout();
        let header = layout::header_record(cells).map_err(WriterError::Segment)?;
        let slot = layout::event_slot_record(cells, layout, index, cursor.position())
            .map_err(WriterError::Segment)?;
        let generation = header.sync(HDR_PUBLICATION_GENERATION);
        let next_generation = generation
            .acquire_load()
            .checked_add(1)
            .ok_or(WriterError::CounterOverflow)?;

        let sequence = slot.seq(EVT_SEQUENCE);
        let stable = sequence.writer_current();
        let in_flight = stable.checked_add(1).ok_or(WriterError::CounterOverflow)?;
        let next_stable = stable.checked_add(2).ok_or(WriterError::CounterOverflow)?;
        let interval = sequence.open_write(in_flight);

        for (offset, value) in [
            (EVT_CURSOR_EPOCH, cursor.epoch()),
            (EVT_CURSOR_POSITION, cursor.position()),
            (EVT_BOOK_REVISION, revision),
            (EVT_COMMIT_TIME, commit_time_nanos()),
            (EVT_ARRIVAL_TIME, arrival_time_nanos),
            (EVT_DAEMON_GENERATION, staged.daemon_generation),
            (EVT_SUBSCRIPTION_GENERATION, staged.subscription_generation),
        ] {
            slot.word64(offset).relaxed_store(value);
        }
        for (base, words) in [
            (EVT_RES_OUTCOME, &staged.outcome_words[..]),
            (EVT_RES_TYPE, &staged.type_words[..]),
            (EVT_RES_DATE, &staged.date_words[..]),
        ] {
            for (word, value) in words.iter().enumerate() {
                slot.word64(base + word * 8).relaxed_store(*value);
            }
        }
        for (offset, value) in [
            (EVT_DELIVERY_KIND, DELIVERY_MARKET_RESOLVED),
            (EVT_ORIGIN, staged.origin),
            (EVT_DERIVATION, staged.derivation),
            (EVT_REPRESENTATION, staged.representation),
            (EVT_RES_WINNING_INDEX, staged.winning_index),
            (EVT_RES_DELIVERY_PATH, staged.delivery_path),
            (EVT_RES_OUTCOME_LEN, staged.outcome_length),
            (EVT_RES_TYPE_LEN, staged.type_length),
            (EVT_RES_DATE_LEN, staged.date_length),
            (EVT_DIRECTORY_INDEX, index),
        ] {
            slot.word32(offset).relaxed_store(value);
        }

        sequence.close_write(interval, next_stable);
        self.ring_doorbell();
        generation.release_store(next_generation);
        Ok(())
    }

    fn stage(&mut self, book: &PublishedBook) -> Result<StagedState, WriterError> {
        let layout = self.geometry.layout();
        let levels = book.canonical_levels();
        if levels.len() > layout.level_capacity() as usize {
            return Err(WriterError::LevelCapacityExceeded {
                levels: levels.len(),
                capacity: layout.level_capacity(),
            });
        }
        let (authority_state, authority_reason) = codec::authority_words(book.authority());
        let (continuity_kind, continuity_reason, continuity_epoch, continuity_position) =
            codec::continuity_words(book.continuity());
        let mut family_words = [0_u64; NATIVE_FAMILY_WORDS];
        let (provenance_present, origin, derivation, representation, family_length) =
            match book.provenance() {
                None => (0, 0, 0, 0, 0),
                Some(provenance) => {
                    let family_length = pack_family(provenance.native_family(), &mut family_words)?;
                    let (origin, derivation) = codec::origin_words(provenance.origin());
                    (
                        1,
                        origin,
                        derivation,
                        codec::representation_word(provenance.representation()),
                        family_length,
                    )
                }
            };
        let staged = StagedState {
            book_revision: book.revision(),
            authority_state,
            authority_reason,
            continuity_kind,
            continuity_reason,
            continuity_epoch,
            continuity_position,
            sync_divergences: book.sync_divergences(),
            provenance_present,
            origin,
            derivation,
            representation,
            family_length,
            family_words,
        };
        self.staged_levels.clear();
        self.staged_levels
            .extend(book.canonical_levels().iter().map(|level| LevelWords {
                side: codec::side_word(level.side()),
                price: decimal_words(level.price().value()),
                quantity: decimal_words(level.quantity().value()),
            }));
        Ok(staged)
    }
}

/// Bytes the sibling doorbell page occupies.
///
/// One [`super::cell::REGION_ALIGNMENT`] block rather than the four bytes the cell needs, so
/// the page satisfies the same alignment rule every region of this ABI does and the doorbell
/// word has a whole cache line to itself.
const DOORBELL_PAGE_BYTES: usize = super::cell::REGION_ALIGNMENT;

/// The Unix mode the sibling doorbell page is created with: owner-only, exactly as the
/// segment file itself.
///
/// A consumer must map this page *read-write*, because the platform wait primitive refuses a
/// read-only mapping on the host that needs this fallback — and a writable mapping is a
/// writable mapping: any principal that can open the page can also `ftruncate` it, after
/// which the writer's next mirrored store lands past the end of the object and takes
/// `SIGBUS`. That is ingestion death by a consumer's hand, so the page is not widened for a
/// reader group; the blast radius of the doorbell's *contents* being harmless is not the
/// question, the file's length is.
///
/// The consequence is stated on [`DoorbellPlacement::Probe`] and pinned in
/// `docs/notes/shared-memory-model.md` §6: where a segment lands on the page placement,
/// parked waiting is available to same-user consumers only. A cross-user consumer's page
/// open fails and surfaces as the typed attach fault its first park reports; spin mode, which
/// never touches the doorbell, is unaffected. Lifting the restriction needs a shared object
/// whose length cannot be changed after creation, which this ABI does not have.
const DOORBELL_PAGE_MODE: u32 = 0o600;

/// The header's own doorbell cell.
fn header_doorbell(region: &SegmentRegion) -> super::cell::RelaxedWord32<'_> {
    header_of(region).word32(HDR_DOORBELL)
}

/// The sibling page's doorbell cell, which lives at its first word.
fn page_doorbell(page: &SegmentRegion) -> super::cell::RelaxedWord32<'_> {
    page.cells()
        .record(0, DOORBELL_PAGE_BYTES)
        .expect("a doorbell page is one region-alignment block")
        .word32(0)
}

/// The header record of a region already proven to hold one.
///
/// Every caller reaches it after [`SegmentWriter::create`] validated the geometry, so the
/// record cannot be missing; a layout defect that made it missing would be a programming
/// error rather than a caller error, which is why this asserts rather than returning.
fn header_of(region: &SegmentRegion) -> Record<'_> {
    layout::header_record(region.cells()).expect("a formatted segment has a header record")
}

/// `<segment path>.doorbell`.
fn doorbell_page_path(segment: &std::path::Path) -> std::path::PathBuf {
    let mut name = segment.as_os_str().to_owned();
    name.push(".doorbell");
    std::path::PathBuf::from(name)
}

/// Decides where this segment's doorbell lives, creating the sibling page if it is needed.
///
/// A heap-backed region's readers share the writer's own writable mapping, so its header word
/// is exactly as waitable as any address in this process and it never takes a page — asking
/// for one is [`WriterError::DoorbellPageUnavailable`]. A file-backed region is mapped a
/// second time read-only, exactly as a consumer will map it, and the probe asks that mapping
/// the only question that matters: will this platform wait on it at all. That second mapping
/// is taken from the descriptor the region was created as, never by re-opening its name, so a
/// name swapped in the meantime cannot make the probe answer about some other file.
///
/// A file-backed region this process did not create carries no such descriptor and cannot be
/// probed at all; it takes a page, which is the placement that works everywhere.
///
/// The page is created exclusively and nothing at its path is ever removed. A writer does
/// not choose the path it is handed — the daemon's segment path is an operator argument — so
/// an unlink here is an unlink of whatever an operator's typo names, and "it is only a wake
/// hint" describes the page this writer would have created, never the file it would have
/// destroyed. A path already occupied is therefore [`WriterError::DoorbellPageOccupied`],
/// naming it, and clearing a page a dead writer left behind is an operator action.
fn choose_doorbell(
    region: &SegmentRegion,
    requested: DoorbellPlacement,
) -> Result<Doorbell, WriterError> {
    let Some(path) = region.backing_path() else {
        return match requested {
            DoorbellPlacement::ForcePage => Err(WriterError::DoorbellPageUnavailable),
            DoorbellPlacement::Probe => Ok(Doorbell::InHeader),
        };
    };
    if requested == DoorbellPlacement::Probe && creation_mapping_accepts_a_wait(region) {
        return Ok(Doorbell::InHeader);
    }
    let page_path = doorbell_page_path(path);
    let page =
        SegmentRegion::create_file_with_mode(&page_path, DOORBELL_PAGE_BYTES, DOORBELL_PAGE_MODE)
            .map_err(|error| match error {
            RegionError::Io(std::io::ErrorKind::AlreadyExists) => {
                WriterError::DoorbellPageOccupied(page_path.clone())
            }
            other => WriterError::DoorbellPage(other),
        })?;
    Ok(Doorbell::Page(page))
}

/// Whether a read-only mapping of `region`'s own backing object can be parked on.
///
/// The probe cannot block: it waits for a value the cell is not holding, so a platform that
/// supports the mapping returns at once and one that does not answers with an error. A
/// mapping this cannot even take is treated as unsupported, which costs a page that always
/// works rather than leaving a consumer unable to park.
///
/// The mapping is taken from a duplicate of the region's creation descriptor, which names the
/// object the exclusive create returned however its pathname resolves by now. The descriptor
/// is open read-write and the mapping this takes of it is read-only: what the wait primitive
/// is being asked about is the *mapping's* protection, which is the one a consumer will hold.
fn creation_mapping_accepts_a_wait(region: &SegmentRegion) -> bool {
    let Some(creation) = region.creation_file() else {
        return false;
    };
    let Ok(duplicate) = creation.try_clone() else {
        return false;
    };
    let Ok(read_only) =
        SegmentRegion::open_read_only_from_fd(std::os::fd::OwnedFd::from(duplicate))
    else {
        return false;
    };
    let Ok(header) = read_only
        .cells()
        .record(0, HEADER_BYTES)
        .ok_or(SegmentFault::GeometryUnreachable)
    else {
        return false;
    };
    let cell = header.word32(HDR_DOORBELL);
    doorbell::waiting_is_supported(cell.wake_address(), cell.relaxed_load())
}

/// Packs `text` into `words` as the little-endian concatenation this ABI reads it back as,
/// returning its length in bytes, or `None` when it does not fit.
///
/// Text that does not fit is always refused by the caller rather than truncated: every
/// cell this packs into carries a venue-native value, and a truncated one names something
/// the venue did not report.
fn pack_text<const WORDS: usize>(text: &str, words: &mut [u64; WORDS]) -> Option<u32> {
    let bytes = text.as_bytes();
    if bytes.len() > WORDS * 8 {
        return None;
    }
    for (position, byte) in bytes.iter().enumerate() {
        words[position / 8] |= u64::from(*byte) << ((position % 8) * 8);
    }
    u32::try_from(bytes.len()).ok()
}

/// Packs `family` into `words`, refusing one wider than [`NATIVE_FAMILY_CAPACITY`] with
/// [`WriterError::NativeFamilyTooLong`].
fn pack_family(family: &str, words: &mut [u64; NATIVE_FAMILY_WORDS]) -> Result<u32, WriterError> {
    pack_text(family, words).ok_or(WriterError::NativeFamilyTooLong)
}

/// The words one venue-reported resolution occupies in an event slot.
///
/// The venue's winning outcome must be text this ABI can carry: an outcome the venue names
/// by index alone carries no text to reproduce and is refused as
/// [`WriterError::WinningOutcomeNotText`], distinct from text that simply does not fit. The
/// market label is the venue's own label for the resolved market and the date is its
/// timestamp lexeme, both stored verbatim.
fn stage_resolution(resolution: &MarketResolution) -> Result<StagedResolution, WriterError> {
    let provenance = resolution.provenance();
    let mut outcome_words = [0_u64; RES_OUTCOME_WORDS];
    let mut type_words = [0_u64; RES_TYPE_WORDS];
    let mut date_words = [0_u64; RES_DATE_WORDS];
    let outcome = resolution
        .winner()
        .text_value()
        .ok_or(WriterError::WinningOutcomeNotText)?;
    let outcome_length =
        pack_text(outcome, &mut outcome_words).ok_or(WriterError::WinningOutcomeTooLong)?;
    let type_length = pack_text(resolution.native_label().as_str(), &mut type_words)
        .ok_or(WriterError::MarketTypeTooLong)?;
    let date_length = pack_text(resolution.resolution_date().as_lexeme(), &mut date_words)
        .ok_or(WriterError::ResolutionDateTooLong)?;
    let (origin, derivation) = codec::origin_words(provenance.origin());
    Ok(StagedResolution {
        origin,
        derivation,
        representation: codec::representation_word(provenance.representation()),
        winning_index: resolution.winning_index(),
        delivery_path: codec::delivery_path_word(resolution.delivery_path())
            .ok_or(WriterError::UnsupportedDeliveryPath)?,
        outcome_length,
        type_length,
        date_length,
        outcome_words,
        type_words,
        date_words,
        daemon_generation: provenance.daemon_generation(),
        subscription_generation: provenance.subscription_generation(),
    })
}

/// The words one level mutation occupies in an event slot.
///
/// The coordinate — side and price — is shared by both halves of the change and is taken
/// from whichever half is present; a [`BookMutation`] guarantees they agree. An absent half
/// stores a zero decimal, which its presence word is the authority over.
fn stage_mutation(mutation: &BookMutation) -> Result<StagedMutation, WriterError> {
    let coordinate: &Level = mutation
        .replacement()
        .or_else(|| mutation.old())
        .ok_or(WriterError::EmptyMutation)?;
    let provenance = mutation.provenance();
    let mut family_words = [0_u64; NATIVE_FAMILY_WORDS];
    let family_length = pack_family(provenance.native_family(), &mut family_words)?;
    let (origin, derivation) = codec::origin_words(provenance.origin());
    let quantity = |level: Option<&Level>| {
        level.map_or(
            DecimalWords {
                low: 0,
                high: 0,
                scale: 0,
            },
            |level| decimal_words(level.quantity().value()),
        )
    };
    Ok(StagedMutation {
        origin,
        derivation,
        representation: codec::representation_word(provenance.representation()),
        side: codec::side_word(coordinate.side()),
        old_present: u32::from(mutation.old().is_some()),
        new_present: u32::from(mutation.replacement().is_some()),
        family_length,
        family_words,
        daemon_generation: provenance.daemon_generation(),
        subscription_generation: provenance.subscription_generation(),
        price: decimal_words(coordinate.price().value()),
        old_quantity: quantity(mutation.old()),
        new_quantity: quantity(mutation.replacement()),
    })
}

/// Wall-clock nanoseconds since the Unix epoch, or 0 when the clock is before the epoch.
///
/// Wall clock rather than a monotonic clock because the only consumer is another process:
/// two processes share no monotonic origin, so a wall-clock stamp is the only value that
/// can be differenced across the boundary. It is therefore subject to clock adjustment —
/// a comparison spanning one can be negative or absurd, and a consumer must discard such a
/// sample rather than clamp it.
fn commit_time_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_nanos()).unwrap_or(u64::MAX)
        })
}

fn decimal_words(value: &crate::ExactDecimal) -> DecimalWords {
    let (low, high, scale) = codec::decimal_words(value);
    DecimalWords { low, high, scale }
}

fn store_decimal(cell: super::cell::Record<'_>, words: &DecimalWords) {
    cell.word64(DEC_COEFFICIENT_LOW).relaxed_store(words.low);
    cell.word64(DEC_COEFFICIENT_HIGH).relaxed_store(words.high);
    cell.word32(DEC_SCALE).relaxed_store(words.scale);
}

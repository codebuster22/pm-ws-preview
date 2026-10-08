//! The reader side: validate, resolve a venue-native identity, take a consistent snapshot.
//!
//! A reader owns no cell of the segment, holds no lock, and publishes no progress, so its
//! death is invisible to the writer and its slowness costs the writer nothing. Everything
//! it does is the sequence in `docs/notes/shared-memory-model.md` §3.1: acquire the entry
//! revision, read the write-once identity through the witness that produced, then read the
//! slot under its even-odd counter and accept only an unchanged even value.
//!
//! Retained events are the same construction over a different record (§3.3). A market's
//! ring wraps, so an attachment is a coherent pair — the snapshot and the cursor it stops
//! at, taken from one accepted interval — and every later poll classifies the slot it finds
//! against that cursor lexicographically on `(epoch, position)`, because positions restart
//! at 0 on every new continuity epoch.

use super::cell::{REGION_ALIGNMENT, RegionError, SegmentRegion, SeqOpen};
use super::codec;
use super::doorbell;
use super::layout::{
    self, DEC_COEFFICIENT_HIGH, DEC_COEFFICIENT_LOW, DEC_SCALE, DECIMAL_CELL_BYTES,
    DELIVERY_LEVEL_MUTATION, DELIVERY_MARKET_RESOLVED, DIRTY_BOOK_REVISION, DIRTY_DIRECTORY_INDEX,
    DIRTY_POSITION, DIRTY_SEQUENCE, ENT_IDENTITY, ENT_IDENTITY_LEN, ENT_REVISION, ENT_SLOT_INDEX,
    EVT_ARRIVAL_TIME, EVT_BOOK_REVISION, EVT_COMMIT_TIME, EVT_CURSOR_EPOCH, EVT_CURSOR_POSITION,
    EVT_DAEMON_GENERATION, EVT_DELIVERY_KIND, EVT_DERIVATION, EVT_DIRECTORY_INDEX, EVT_FAMILY_LEN,
    EVT_FAMILY_WORDS, EVT_NEW_PRESENT, EVT_NEW_QUANTITY, EVT_OLD_PRESENT, EVT_OLD_QUANTITY,
    EVT_ORIGIN, EVT_PRICE, EVT_REPRESENTATION, EVT_RES_DATE, EVT_RES_DATE_LEN,
    EVT_RES_DELIVERY_PATH, EVT_RES_OUTCOME, EVT_RES_OUTCOME_LEN, EVT_RES_TYPE, EVT_RES_TYPE_LEN,
    EVT_RES_WINNING_INDEX, EVT_SEQUENCE, EVT_SIDE, EVT_SUBSCRIPTION_GENERATION,
    FEATURE_DOORBELL_IN_HEADER, FEATURE_DOORBELL_PAGE, HDR_DOORBELL, HDR_FEATURE_BITS, HDR_MAGIC,
    HDR_PUBLICATION_GENERATION, IDENTITY_CAPACITY, LEVEL_CELL_BYTES, LVL_PRICE, LVL_QUANTITY,
    LVL_SIDE, MarketHandle, NATIVE_FAMILY_WORDS, NO_STATE_SLOT, RES_DATE_WORDS, RES_OUTCOME_WORDS,
    RES_TYPE_WORDS, SLOT_ARRIVAL_TIME, SLOT_AUTHORITY_REASON, SLOT_AUTHORITY_STATE,
    SLOT_BOOK_REVISION, SLOT_COMMIT_TIME, SLOT_CONTINUITY_EPOCH, SLOT_CONTINUITY_KIND,
    SLOT_CONTINUITY_POSITION, SLOT_CONTINUITY_REASON, SLOT_DERIVATION, SLOT_DIRECTORY_INDEX,
    SLOT_FAMILY_LEN, SLOT_FAMILY_WORDS, SLOT_LEVEL_COUNT, SLOT_ORIGIN, SLOT_PREFIX_BYTES,
    SLOT_PROVENANCE_PRESENT, SLOT_REPRESENTATION, SLOT_REVISION, SLOT_SYNC_DIVERGENCES,
    SegmentBinding, SegmentFault, SegmentGeometry,
};
use crate::{
    AuthorityState, ContinuityReason, DeliveryPath, Level, MarketRef, MutationContinuity,
    MutationCursor, Origin, Price, Quantity, Representation, Side,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How many times a read retries before it reports why it could not settle.
pub const MAX_READ_ATTEMPTS: u32 = 64;

/// Why a snapshot could not be taken.
///
/// [`Self::WriterStalled`] and [`Self::Contended`] are deliberately different answers to
/// the same observation. A slot whose odd counter never moves across every attempt did not
/// complete a publication within this reader's bound — the market reads as unavailable, and
/// never as a half-written book. A counter that keeps moving is a reader losing a race with
/// a live writer, which is retried to this reader's own bound and reported as contention.
///
/// [`Self::ForeignSegment`] is a handle bound to a different segment than this reader
/// attached to; it is refused before any index is used, so a stale handle can never address
/// another segment's slot.
///
/// [`Self::WriterStalled`] is evidence about this reader's bound, not proof the writer
/// died: [`MAX_READ_ATTEMPTS`] spins pass in well under a scheduler quantum, so a writer
/// preempted between its odd and even stores produces the same observation. A consumer
/// that must tell the two apart retries and escalates on repetition; a consumer that only
/// needs current state treats it exactly like [`Self::Contended`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadFault {
    Segment(SegmentFault),
    EntryUnpublished,
    NoPublishedState,
    HandleStale,
    ForeignSegment,
    SlotOwnershipMismatch,
    MalformedIdentity,
    MalformedRecord,
    WriterStalled { slot_revision: u64 },
    Contended { attempts: u32 },
}
impl core::fmt::Display for ReadFault {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("no consistent published state")
    }
}
impl std::error::Error for ReadFault {}

/// How the revision behind a snapshot was labelled: whether the venue reported the change
/// or this daemon derived it, in which representation, and under which venue-native family.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicationOrigin {
    origin: Origin,
    representation: Representation,
    native_family: String,
}
impl PublicationOrigin {
    pub fn origin(&self) -> &Origin {
        &self.origin
    }
    pub fn representation(&self) -> &Representation {
        &self.representation
    }
    pub fn native_family(&self) -> &str {
        &self.native_family
    }
}

/// One consistent published book revision, decoded out of a state slot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BookSnapshot {
    market: MarketRef,
    revision: u64,
    authority: AuthorityState,
    continuity: MutationContinuity,
    sync_divergences: u64,
    commit_time: u64,
    arrival_time: u64,
    publication: Option<PublicationOrigin>,
    levels: Vec<Level>,
}

impl BookSnapshot {
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
    pub fn sync_divergences(&self) -> u64 {
        self.sync_divergences
    }
    /// When the writer stamped this revision, in wall-clock nanoseconds since the Unix
    /// epoch, or `None` when the writer left it unset.
    ///
    /// Wall clock, because a cross-process consumer shares no monotonic origin with the
    /// writer. A difference taken against it therefore includes any clock adjustment
    /// between the two reads, and a consumer must discard a non-positive or implausible
    /// difference rather than clamp it.
    pub fn commit_time_nanos(&self) -> Option<u64> {
        (self.commit_time != 0).then_some(self.commit_time)
    }
    /// When the socket read that drove this revision returned, in wall-clock nanoseconds
    /// since the Unix epoch, or `None` when this commit was not driven by a venue frame.
    ///
    /// The same wall-clock and cross-process caveats as [`Self::commit_time_nanos`] apply:
    /// two processes share no monotonic origin, so a consumer measuring latency against this
    /// stamp must discard a non-positive or implausible difference rather than clamp it.
    pub fn arrival_time_nanos(&self) -> Option<u64> {
        (self.arrival_time != 0).then_some(self.arrival_time)
    }
    /// The provenance labelling of this revision, or `None` before the book's first
    /// accepted snapshot.
    pub fn publication(&self) -> Option<&PublicationOrigin> {
        self.publication.as_ref()
    }
    /// The venue-reported levels in canonical outcome coordinates, ascending by
    /// `(side, price)` — the same order [`crate::PublishedBook::canonical_levels`] uses.
    pub fn levels(&self) -> &[Level] {
        &self.levels
    }
    /// The highest bid resting a non-zero quantity, or `None` when no bid rests depth.
    pub fn best_bid(&self) -> Option<&Level> {
        self.levels
            .iter()
            .rev()
            .find(|level| level.side() == Side::Bid && level.quantity().value().coefficient() != 0)
    }
    /// The lowest ask resting a non-zero quantity, or `None` when no ask rests depth.
    pub fn best_ask(&self) -> Option<&Level> {
        self.levels
            .iter()
            .find(|level| level.side() == Side::Ask && level.quantity().value().coefficient() != 0)
    }
}

struct CursorWords {
    book_revision: u64,
    continuity_epoch: u64,
    continuity_position: u64,
    continuity_kind: u32,
    continuity_reason: u32,
    directory_index: u32,
}

/// The words every retained-event slot carries at the same offsets, whatever its kind.
struct SharedEventWords {
    cursor_epoch: u64,
    cursor_position: u64,
    book_revision: u64,
    commit_time: u64,
    arrival_time: u64,
    daemon_generation: u64,
    subscription_generation: u64,
    origin: u32,
    derivation: u32,
    representation: u32,
    directory_index: u32,
}

struct MutationWords {
    side: u32,
    old_present: u32,
    new_present: u32,
    family_length: u32,
    family_words: [u64; NATIVE_FAMILY_WORDS],
    price: (u64, u64, u32),
    old_quantity: (u64, u64, u32),
    new_quantity: (u64, u64, u32),
}

struct ResolutionWords {
    winning_index: u32,
    delivery_path: u32,
    outcome_length: u32,
    type_length: u32,
    date_length: u32,
    outcome_words: [u64; RES_OUTCOME_WORDS],
    type_words: [u64; RES_TYPE_WORDS],
    date_words: [u64; RES_DATE_WORDS],
}

/// The kind-specific half of a slot, copied under the same accepted seqlock interval as the
/// shared half.
///
/// The kind word decides which half is copied, so a mutation poll never reads a
/// resolution's text cells and a resolution poll never reads a mutation's decimals. A kind
/// this build does not define copies neither and decodes to nothing, which is what makes an
/// unknown kind a malformed record rather than a guess.
enum EventBody {
    Mutation(MutationWords),
    Resolution(ResolutionWords),
    Unknown,
}

struct EventWords {
    shared: SharedEventWords,
    body: EventBody,
}

struct SlotWords {
    book_revision: u64,
    commit_time: u64,
    arrival_time: u64,
    continuity_epoch: u64,
    continuity_position: u64,
    sync_divergences: u64,
    authority_state: u32,
    authority_reason: u32,
    continuity_kind: u32,
    continuity_reason: u32,
    provenance_present: u32,
    origin: u32,
    derivation: u32,
    representation: u32,
    family_length: u32,
    family_words: [u64; NATIVE_FAMILY_WORDS],
    directory_index: u32,
    level_count: u32,
    levels: Vec<(u32, u64, u64, u32, u64, u64, u32)>,
}

/// Where this reader's copy of the segment's doorbell actually lives, resolved once at
/// [`SegmentReader::attach`].
///
/// The header-word case borrows nothing extra: [`Self::Header`] reads straight through the
/// reader's own segment mapping. [`Self::Page`] owns a second, independently opened mapping
/// of the sibling `<segment path>.doorbell` file — read-write, because the platform's wait
/// primitive requires that of the mapping itself, never because this reader stores through
/// it. Opening that page can fail (the file missing, permissions, a heap-backed region
/// wrongly declaring the page bit); the failure is captured here rather than panicking, and
/// is not surfaced until a caller actually asks to park — see
/// [`SegmentReader::wait_for_publication`]'s own doc comment for why attaching still
/// succeeds in that case.
#[derive(Clone)]
enum DoorbellAttachment {
    Header,
    Page(Result<Arc<SegmentRegion>, RegionError>),
}

impl DoorbellAttachment {
    /// Resolves `region`'s declared doorbell placement, opening the sibling page eagerly
    /// when the header declares one so the cost of that `open`/`mmap` is paid once, at
    /// attachment, rather than on a later hot wait call.
    ///
    /// `region`'s header has already passed [`layout::validate`] by the time this runs (it
    /// is only ever called from [`SegmentReader::attach`] after that succeeds), so exactly
    /// one of [`FEATURE_DOORBELL_IN_HEADER`](super::layout::FEATURE_DOORBELL_IN_HEADER) and
    /// [`FEATURE_DOORBELL_PAGE`] is set; "not page" is therefore read directly as "header"
    /// rather than carrying a third, unreachable state.
    fn resolve(region: &SegmentRegion) -> Self {
        let features = header_features(region).unwrap_or(0);
        if features & FEATURE_DOORBELL_PAGE == 0 {
            return Self::Header;
        }
        let opened = region
            .backing_path()
            .ok_or(RegionError::Io(std::io::ErrorKind::NotFound))
            .and_then(|path| SegmentRegion::open_page_read_write(&doorbell_page_path(path)));
        Self::Page(opened.map(Arc::new))
    }

    /// Resolves `region`'s declared doorbell placement against a page descriptor the segment
    /// arrived with, rather than against a path.
    ///
    /// This is the descriptor-transfer form: a consumer that attached by descriptor holds no
    /// path to derive `<segment>.doorbell` from, so a page it is to park on must arrive as its
    /// own descriptor. Whether any sender transfers one is that sender's decision — `pmwsd`
    /// deliberately does not — and this resolves whatever did arrive. A segment declaring its
    /// doorbell in the header needs no page and `page` is closed unused; a segment declaring a
    /// page with no descriptor beside it captures the same missing-page failure
    /// [`Self::resolve`] would, which surfaces on the first park as
    /// [`WaitFault::DoorbellUnavailable`] and leaves spinning consumers unaffected.
    fn from_descriptor(region: &SegmentRegion, page: Option<std::os::fd::OwnedFd>) -> Self {
        let features = header_features(region).unwrap_or(0);
        if features & FEATURE_DOORBELL_PAGE == 0 {
            return Self::Header;
        }
        let opened = page
            .ok_or(RegionError::Io(std::io::ErrorKind::NotFound))
            .and_then(SegmentRegion::open_page_read_write_from_fd);
        Self::Page(opened.map(Arc::new))
    }
}

/// The segment's declared feature word, read the same way [`layout::validate`] does: through
/// the header's own write-once witness, never a plain read of a `write_once_published` byte.
fn header_features(region: &SegmentRegion) -> Option<u64> {
    let cells = region.cells();
    let witness = layout::header_record(cells).ok()?.published(HDR_MAGIC)?;
    Some(witness.word64(HDR_FEATURE_BITS))
}

/// `<segment path>.doorbell`, exactly as [`super::writer`]'s own placement decides it.
fn doorbell_page_path(segment: &std::path::Path) -> std::path::PathBuf {
    let mut name = segment.as_os_str().to_owned();
    name.push(".doorbell");
    std::path::PathBuf::from(name)
}

/// How a wait for [`SegmentReader::publication_generation`] to change ended.
///
/// Both variants carry the generation this reader observed at the moment the wait settled,
/// so a caller never needs a second read to learn what to pass as `last_seen` on its next
/// call — including on [`Self::TimedOut`], where the value may still equal what the caller
/// started with, or may already have moved without this call's own recheck catching a wake
/// for it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WaitOutcome {
    Changed(u64),
    TimedOut(u64),
}

/// Why [`SegmentReader::wait_for_publication`] could not park on the segment's doorbell.
///
/// Every variant here is a defensive answer to a condition the spin phase alone never hits:
/// [`Self::DoorbellUnavailable`] is the sibling page's `open`/`mmap` failure captured at
/// [`SegmentReader::attach`] time and replayed here on first use; [`Self::DoorbellPageUnreachable`]
/// is the page's own record failing to carve, which a correctly sized page never does;
/// [`Self::Segment`] is the header record itself failing to recover, which a reader that
/// already passed [`layout::validate`] does not either; and [`Self::Platform`] is the
/// platform wait syscall itself faulting for a reason other than a timeout, carrying its raw
/// error number. A caller whose `spin` already covers the whole `timeout` may never resolve
/// the doorbell at all, and so never sees any of these.
///
/// [`Self::DoorbellUnavailable`] is also the answer a consumer running as a different user
/// than the writer gets on a page-placement segment: the sibling page is owner-only, for the
/// reason [`super::DoorbellPlacement`] documents, so that consumer cannot park and must spin.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WaitFault {
    Segment(SegmentFault),
    DoorbellUnavailable(RegionError),
    DoorbellPageUnreachable,
    Platform { errno: i32 },
}
impl core::fmt::Display for WaitFault {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("could not wait on the segment's doorbell")
    }
}
impl std::error::Error for WaitFault {}

/// A consumer's position in the segment's single dirty-index ring: which directory entry
/// changed and what state revision it advertised, named by a bounded feed a consumer
/// attached to many markets polls instead of scanning every book in turn.
///
/// Lives in the consumer's own memory exactly as an [`EventStream`]'s cursor does: the
/// writer never inspects it and can never be slowed by one existing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DirtyCursor {
    position: u64,
}
impl DirtyCursor {
    /// The absolute dirty-ring position this cursor will read next.
    pub fn position(&self) -> u64 {
        self.position
    }
}

/// What one poll of the segment's dirty-index ring produced.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DirtyPoll {
    /// One market changed: `directory_index` names which. `book_revision` is a state-read
    /// skip hint only — a resolution republish advertises an unchanged revision, so a
    /// consumer must still poll that market's event ring on every delivered entry, never
    /// only its state.
    Delivered {
        directory_index: u32,
        book_revision: u64,
    },
    /// The writer has not reached this cursor's position yet. Nothing was lost.
    Idle,
    /// The declared full-rescan signal: the writer lapped the ring past this cursor's
    /// expectation. Never sticky — the cursor this call was passed has already rebased past
    /// the entry that overran it — so the caller must re-read every market in its interest
    /// set once, then resume ordinary polling.
    Rescan,
}

/// A reader of one publication segment.
///
/// Cloneable and shareable: any number of readers may attach to the same region, and none
/// of them can block the writer or each other.
#[derive(Clone)]
pub struct SegmentReader {
    region: Arc<SegmentRegion>,
    geometry: SegmentGeometry,
    binding: SegmentBinding,
    doorbell: DoorbellAttachment,
}

impl SegmentReader {
    /// Validates `region`'s header and trailer and returns a reader for it.
    ///
    /// This is the compatibility gate: no directory entry and no state slot is touched
    /// before it succeeds, and it fails closed with the [`SegmentFault`] naming what did
    /// not match.
    ///
    /// The segment's doorbell placement is also resolved here — opening the sibling page
    /// eagerly when the header declares one, so that cost lands at attachment rather than on
    /// a later [`Self::wait_for_publication`] call. Resolution failure does **not** fail
    /// attachment itself: a reader that only ever spins, or never calls
    /// [`Self::wait_for_publication`] at all, has no reason to be refused a segment it can
    /// otherwise read perfectly well. The cost is that such a failure surfaces late — on the
    /// first wait a caller actually makes, which may be deep in that caller's steady-state
    /// loop rather than at startup — as [`WaitFault::DoorbellUnavailable`], never as a panic.
    pub fn attach(region: Arc<SegmentRegion>) -> Result<Self, SegmentFault> {
        let geometry = layout::validate(&region)?;
        let binding = SegmentBinding::new(&region, geometry);
        let doorbell = DoorbellAttachment::resolve(&region);
        Ok(Self {
            region,
            geometry,
            binding,
            doorbell,
        })
    }

    /// [`Self::attach`] for a segment that arrived by descriptor transfer, with the sibling
    /// doorbell page's descriptor beside it.
    ///
    /// Same compatibility gate, same failure modes; only where the doorbell comes from
    /// differs. A region mapped from a transferred descriptor carries no path, so the page
    /// cannot be resolved by name and is handed over instead — `None` for a segment whose
    /// doorbell lives in its header, and `None` also where no page descriptor is available,
    /// which leaves this reader able to read and to spin but not to park, exactly as a
    /// path-resolved page that fails to open does.
    ///
    /// `pmwsd`'s own attachment channel always passes `None`: a page descriptor is a
    /// truncation capability over a file its writer stores through, so that channel transfers
    /// the segment alone (`docs/notes/shared-memory-model.md` §4.2). The parameter stays
    /// because this is the general descriptor-transfer form, and a caller that obtained a page
    /// descriptor some other way is served by it.
    pub fn attach_with_doorbell(
        region: Arc<SegmentRegion>,
        doorbell_page: Option<std::os::fd::OwnedFd>,
    ) -> Result<Self, SegmentFault> {
        let geometry = layout::validate(&region)?;
        let binding = SegmentBinding::new(&region, geometry);
        let doorbell = DoorbellAttachment::from_descriptor(&region, doorbell_page);
        Ok(Self {
            region,
            geometry,
            binding,
            doorbell,
        })
    }

    pub fn geometry(&self) -> SegmentGeometry {
        self.geometry
    }

    /// The doorbell feature bit this segment's validated header declares: exactly one of
    /// [`FEATURE_DOORBELL_IN_HEADER`](super::layout::FEATURE_DOORBELL_IN_HEADER) and
    /// [`FEATURE_DOORBELL_PAGE`].
    ///
    /// The reader's half of [`super::SegmentWriter::doorbell_feature_bit`], and the same
    /// value: it is read from the header, so it says where the doorbell *is*, never whether
    /// this reader managed to reach it. A page this reader could not open still reports
    /// [`FEATURE_DOORBELL_PAGE`] and fails its first park with
    /// [`WaitFault::DoorbellUnavailable`]. Exposed so a caller handed a segment along with a
    /// claim about it can check the claim against the header rather than trust it.
    pub fn doorbell_feature_bit(&self) -> u64 {
        match self.doorbell {
            DoorbellAttachment::Header => FEATURE_DOORBELL_IN_HEADER,
            DoorbellAttachment::Page(_) => FEATURE_DOORBELL_PAGE,
        }
    }

    /// The binding every handle this reader issues and accepts carries.
    pub fn binding(&self) -> SegmentBinding {
        self.binding
    }

    /// The publication generation, acquire-loaded: a hint that newer data may exist
    /// somewhere in the segment. It is never the authority for any one market, and it
    /// coalesces freely.
    pub fn publication_generation(&self) -> u64 {
        let cells = self.region.cells();
        layout::header_record(cells)
            .map(|header| header.sync(HDR_PUBLICATION_GENERATION).acquire_load())
            .unwrap_or_default()
    }

    /// Waits for [`Self::publication_generation`] to change from `last_seen`, or for
    /// `timeout` to pass — `None` parks indefinitely.
    ///
    /// **Phase 1, spin.** For up to `spin`, re-reads the generation with
    /// [`core::hint::spin_loop`] between attempts and no syscall at all: the low-latency
    /// path for a writer publishing every few microseconds, where the cost of even one park
    /// syscall would dwarf the wait itself. `spin` of [`Duration::ZERO`] skips this phase —
    /// pure parked mode.
    ///
    /// **Phase 2, park.** Reads the doorbell's current value, *then* re-checks the
    /// generation, and only then parks on the doorbell expecting the value it just read.
    /// This order is load-bearing, not incidental: it is what closes the lost-wake window. A
    /// bump the writer makes between this call's last generation check and the moment it
    /// registers its wait would otherwise go unnoticed by an equality-based wait that only
    /// compares against a value read *before* the bump — but reading the doorbell first means
    /// any such bump already changed the value this call is about to wait "while unchanged"
    /// against, so the wait returns at once rather than parking through a wake that already
    /// happened. Every spurious wake, timeout-free intermediate return and interrupted wait
    /// the platform allows loops internally, invisible to the caller, until the generation
    /// differs or `timeout` truly elapses.
    ///
    /// Returns the generation observed at the moment this call settled either way — see
    /// [`WaitOutcome`]. Fails with [`WaitFault`] only from the park phase; a caller whose
    /// `spin` alone already produces [`WaitOutcome::Changed`] never resolves the doorbell at
    /// all and so cannot see one.
    pub fn wait_for_publication(
        &self,
        last_seen: u64,
        spin: Duration,
        timeout: Option<Duration>,
    ) -> Result<WaitOutcome, WaitFault> {
        let spin_deadline = Instant::now() + spin;
        loop {
            let current = self.publication_generation();
            if current != last_seen {
                return Ok(WaitOutcome::Changed(current));
            }
            if Instant::now() >= spin_deadline {
                break;
            }
            core::hint::spin_loop();
        }

        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        loop {
            let word = self.doorbell_word()?;
            let observed = word.relaxed_load();
            let current = self.publication_generation();
            if current != last_seen {
                return Ok(WaitOutcome::Changed(current));
            }
            let remaining = match deadline {
                None => None,
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Ok(WaitOutcome::TimedOut(current));
                    }
                    Some(deadline - now)
                }
            };
            match doorbell::wait(word.wake_address(), observed, remaining) {
                Ok(_) => {}
                Err(fault) => {
                    return Err(WaitFault::Platform {
                        errno: fault.errno(),
                    });
                }
            }
        }
    }

    /// The doorbell cell this reader parks on: the header's own word for
    /// [`DoorbellAttachment::Header`], or the sibling page's for
    /// [`DoorbellAttachment::Page`] resolved at [`Self::attach`].
    fn doorbell_word(&self) -> Result<super::cell::RelaxedWord32<'_>, WaitFault> {
        match &self.doorbell {
            DoorbellAttachment::Header => {
                let cells = self.region.cells();
                let header = layout::header_record(cells).map_err(WaitFault::Segment)?;
                Ok(header.word32(HDR_DOORBELL))
            }
            DoorbellAttachment::Page(Ok(page)) => {
                let record = page
                    .cells()
                    .record(0, REGION_ALIGNMENT)
                    .ok_or(WaitFault::DoorbellPageUnreachable)?;
                Ok(record.word32(0))
            }
            DoorbellAttachment::Page(Err(error)) => Err(WaitFault::DoorbellUnavailable(*error)),
        }
    }

    /// Establishes a dirty-index cursor at the ring's current head.
    ///
    /// The dirty ring keeps no separate head counter, mirroring the retained-event rings
    /// (`docs/notes/shared-memory-model.md` §3.3), so the only way to learn where "now" is
    /// is one bounded scan of every slot: the head is one past the highest published
    /// position, or 0 when the ring has never been written. Paid once, here, never on the
    /// steady-state [`Self::next_dirty`] path — a rescan there rebases without a second scan.
    pub fn dirty_cursor(&self) -> DirtyCursor {
        DirtyCursor {
            position: self.dirty_head(),
        }
    }

    /// One best-effort pass over every dirty slot, for [`Self::dirty_cursor`] alone.
    ///
    /// A slot caught mid-publication is simply skipped for this pass rather than retried:
    /// undercounting the head here costs nothing but a handful of already-known entries
    /// redelivered once polling starts, whereas [`Self::next_dirty`]'s own overrun detection
    /// is what a live consumer actually depends on for correctness.
    fn dirty_head(&self) -> u64 {
        let cells = self.region.cells();
        let layout = self.geometry.layout();
        let mut head = 0_u64;
        for slot_index in 0..layout.dirty_capacity() {
            let Ok(slot) = layout::dirty_slot_record(cells, layout, u64::from(slot_index)) else {
                continue;
            };
            let sequence = slot.seq(DIRTY_SEQUENCE);
            if let SeqOpen::Stable(interval) = sequence.open_read() {
                let position = slot.word64(DIRTY_POSITION).relaxed_load();
                if sequence.close_read(interval) {
                    head = head.max(position.wrapping_add(1));
                }
            }
        }
        head
    }

    /// Delivers the dirty-index entry at `cursor`'s position, or says why it cannot, and
    /// advances or rebases `cursor` in place.
    ///
    /// Classification is lexicographic on the slot's stored position against `cursor`'s
    /// expectation, mirroring the event rings: equal delivers and advances the cursor by
    /// one; greater means the writer has lapped the ring past this cursor, which is
    /// [`DirtyPoll::Rescan`] — the cursor rebases to *one past the position this call just
    /// observed*, taken from the very seqlock read that detected the overrun, rather than
    /// paying a second full-ring scan. That rebase target can itself already trail the
    /// writer's true tip if the writer advances again between this read and the caller's own
    /// next poll — but that next poll then finds a slot ahead of the rebased cursor and
    /// reports another [`DirtyPoll::Rescan`] rather than silence, so no window here can ever
    /// present as a gap; a lower stored position, or a slot the writer has never published,
    /// is [`DirtyPoll::Idle`].
    ///
    /// A single seqlock attempt per call: unlike [`Self::read`] this never retries a torn or
    /// in-flight slot, because a dirty entry is a hint a consumer is expected to poll again
    /// shortly on its own, not a value that must settle before this call returns.
    pub fn next_dirty(&self, cursor: &mut DirtyCursor) -> DirtyPoll {
        let cells = self.region.cells();
        let layout = self.geometry.layout();
        let Ok(slot) = layout::dirty_slot_record(cells, layout, cursor.position) else {
            return DirtyPoll::Idle;
        };
        let sequence = slot.seq(DIRTY_SEQUENCE);
        let stable = match sequence.open_read() {
            SeqOpen::Stable(interval) => {
                let words = (
                    slot.word64(DIRTY_POSITION).relaxed_load(),
                    slot.word32(DIRTY_DIRECTORY_INDEX).relaxed_load(),
                    slot.word64(DIRTY_BOOK_REVISION).relaxed_load(),
                );
                sequence.close_read(interval).then_some(words)
            }
            SeqOpen::Unpublished | SeqOpen::InFlight(_) => None,
        };
        let Some((position, directory_index, book_revision)) = stable else {
            return DirtyPoll::Idle;
        };
        match position.cmp(&cursor.position) {
            core::cmp::Ordering::Equal => {
                cursor.position = cursor.position.wrapping_add(1);
                DirtyPoll::Delivered {
                    directory_index,
                    book_revision,
                }
            }
            core::cmp::Ordering::Greater => {
                cursor.position = position.wrapping_add(1);
                DirtyPoll::Rescan
            }
            core::cmp::Ordering::Less => DirtyPoll::Idle,
        }
    }

    /// Resolves a venue-native market identity to its process-local handle, or `None` when
    /// this segment holds no entry for it.
    ///
    /// An installed entry whose identity bytes do not decode is skipped rather than
    /// guessed at: a reader never infers an identity it cannot read.
    pub fn resolve(&self, market: &MarketRef) -> Option<MarketHandle> {
        let cells = self.region.cells();
        let layout = self.geometry.layout();
        (0..layout.directory_capacity()).find_map(|index| {
            let entry = layout::directory_record(cells, layout, index).ok()?;
            let witness = entry.published(ENT_REVISION)?;
            let identity = identity_bytes(witness)?;
            (codec::decode_identity(identity)?.eq(market))
                .then(|| MarketHandle::new(self.binding, index, witness.word32(ENT_SLOT_INDEX)))
        })
    }

    /// Resolves a directory index to its process-local handle, without needing the market's
    /// venue-native identity in advance.
    ///
    /// This is the bridge a dirty-ring consumer needs: [`Self::next_dirty`] names a changed
    /// entry by `directory_index` alone, and [`Self::resolve`] can only look one up by the
    /// identity it already holds. The venue-native identity itself is not decoded or
    /// validated here — this path never needs it, only the entry's own slot-index field —
    /// so a directory entry with unreadable identity bytes still resolves, unlike
    /// [`Self::resolve`]'s identity-matching scan.
    ///
    /// Returns `None` for an index outside the segment's directory capacity or one whose
    /// entry has not been published yet.
    pub fn resolve_directory_index(&self, directory_index: u32) -> Option<MarketHandle> {
        let cells = self.region.cells();
        let layout = self.geometry.layout();
        if directory_index >= layout.directory_capacity() {
            return None;
        }
        let entry = layout::directory_record(cells, layout, directory_index).ok()?;
        let witness = entry.published(ENT_REVISION)?;
        Some(MarketHandle::new(
            self.binding,
            directory_index,
            witness.word32(ENT_SLOT_INDEX),
        ))
    }

    /// Takes a consistent snapshot of the book `handle` names.
    ///
    /// Reads the entry's write-once identity through its witness, then loops the slot's
    /// even-odd counter: reject an odd value, relaxed-load every data word, acquire-load
    /// the counter again, and accept only an unchanged even value. Retries up to
    /// [`MAX_READ_ATTEMPTS`] times and never waits on the writer.
    ///
    /// [`ReadFault::NoPublishedState`] means the writer has published nothing into this
    /// slot. It does not mean the book is empty: a writer that publishes a book which has
    /// accepted no snapshot yields a successful read at revision 0, authority
    /// [`AuthorityState::Synchronizing`] and no [`BookSnapshot::publication`], and that is
    /// the state a consumer must test for to tell "no venue data yet" from "no publication
    /// yet".
    ///
    /// Fails with [`ReadFault::EntryUnpublished`] for an uninstalled entry,
    /// [`ReadFault::NoPublishedState`] for a market with no published revision yet,
    /// [`ReadFault::HandleStale`] when the entry no longer names the slot the handle does,
    /// [`ReadFault::SlotOwnershipMismatch`] when the slot serves another entry,
    /// [`ReadFault::MalformedIdentity`] or [`ReadFault::MalformedRecord`] for words that
    /// decode to nothing this ABI defines, and [`ReadFault::WriterStalled`] or
    /// [`ReadFault::Contended`] when no stable revision was observed.
    pub fn read(&self, handle: MarketHandle) -> Result<BookSnapshot, ReadFault> {
        let (witness, slot) = self.state_slot(handle)?;
        let market =
            codec::decode_identity(identity_bytes(witness).ok_or(ReadFault::MalformedIdentity)?)
                .ok_or(ReadFault::MalformedIdentity)?;
        let capacity = self.geometry.layout().level_capacity();
        let words = under_seqlock(slot.seq(SLOT_REVISION), || read_words(slot, capacity))?
            .ok_or(ReadFault::NoPublishedState)?;
        if words.directory_index != handle.entry_index() {
            return Err(ReadFault::SlotOwnershipMismatch);
        }
        if words.level_count > capacity {
            return Err(ReadFault::MalformedRecord);
        }
        decode(market, words).ok_or(ReadFault::MalformedRecord)
    }

    /// Reads only the state slot's revision and mutation-continuity prefix, under one
    /// accepted seqlock interval.
    ///
    /// This is the cheap read a mutation consumer runs on every poll: it copies five words
    /// and decodes no level, so it costs a fraction of [`Self::read`]. It is what closes
    /// the one hazard a ring alone cannot — a stream rebased onto a new continuity epoch
    /// commits with zero mutations and restarts positions at 0, so nothing appears in the
    /// ring for a parked consumer to notice.
    ///
    /// Fails exactly as [`Self::read`] does, minus the identity decode.
    pub fn read_cursor(&self, handle: MarketHandle) -> Result<StateCursor, ReadFault> {
        let (_, slot) = self.state_slot(handle)?;
        let words = under_seqlock(slot.seq(SLOT_REVISION), || CursorWords {
            book_revision: slot.word64(SLOT_BOOK_REVISION).relaxed_load(),
            continuity_epoch: slot.word64(SLOT_CONTINUITY_EPOCH).relaxed_load(),
            continuity_position: slot.word64(SLOT_CONTINUITY_POSITION).relaxed_load(),
            continuity_kind: slot.word32(SLOT_CONTINUITY_KIND).relaxed_load(),
            continuity_reason: slot.word32(SLOT_CONTINUITY_REASON).relaxed_load(),
            directory_index: slot.word32(SLOT_DIRECTORY_INDEX).relaxed_load(),
        })?
        .ok_or(ReadFault::NoPublishedState)?;
        if words.directory_index != handle.entry_index() {
            return Err(ReadFault::SlotOwnershipMismatch);
        }
        Ok(StateCursor {
            revision: words.book_revision,
            continuity: codec::continuity(
                words.continuity_kind,
                words.continuity_reason,
                words.continuity_epoch,
                words.continuity_position,
            )
            .ok_or(ReadFault::MalformedRecord)?,
        })
    }

    /// Attaches to a book's state and its mutation stream as one coherent step.
    ///
    /// The returned snapshot and the stream's starting cursor come from the *same* accepted
    /// seqlock interval: the cursor is the `(epoch, next_position)` the snapshot's own
    /// [`MutationContinuity`] carries, never a second read. Everything below that position
    /// is already contained in the snapshot; everything at or above it arrives through
    /// [`EventStream::poll`]. A snapshot whose continuity is already
    /// [`MutationContinuity::Lost`] yields a stream that starts in the sticky lost state
    /// carrying that reason, so a consumer is told rather than left polling a stream that
    /// will never resume.
    ///
    /// When the writer has already overwritten the attachment's own position — the ring
    /// lapped between the state read and the probe — the state is re-read and the
    /// attachment retried, up to [`MAX_READ_ATTEMPTS`] times, then reported as
    /// [`ReadFault::Contended`]. An attachment therefore either starts exactly at its
    /// snapshot's next position or fails explicitly; it never starts at a silent gap.
    ///
    /// Fails exactly as [`Self::read`] does, plus [`ReadFault::Contended`] for the
    /// repeatedly outrun attachment above.
    pub fn attach_stream(
        &self,
        handle: MarketHandle,
    ) -> Result<(BookSnapshot, EventStream), ReadFault> {
        let (snapshot, start) = self.attach_at(handle)?;
        let stream = EventStream {
            reader: self.clone(),
            handle,
            market: snapshot.market().clone(),
            cursor: start.cursor(snapshot.continuity()),
            lost: start.lost(),
        };
        Ok((snapshot, stream))
    }

    fn state_slot(
        &self,
        handle: MarketHandle,
    ) -> Result<(super::cell::PublishedWitness<'_>, super::cell::Record<'_>), ReadFault> {
        if handle.segment() != self.binding {
            return Err(ReadFault::ForeignSegment);
        }
        let cells = self.region.cells();
        let layout = self.geometry.layout();
        let entry = layout::directory_record(cells, layout, handle.entry_index())
            .map_err(ReadFault::Segment)?;
        let witness = entry
            .published(ENT_REVISION)
            .ok_or(ReadFault::EntryUnpublished)?;
        let slot_index = witness.word32(ENT_SLOT_INDEX);
        if slot_index == NO_STATE_SLOT {
            return Err(ReadFault::NoPublishedState);
        }
        if slot_index != handle.state_slot_index() {
            return Err(ReadFault::HandleStale);
        }
        if slot_index >= layout.state_slot_capacity() {
            return Err(ReadFault::MalformedRecord);
        }
        let slot =
            layout::state_slot_record(cells, layout, slot_index).map_err(ReadFault::Segment)?;
        Ok((witness, slot))
    }

    fn attach_at(&self, handle: MarketHandle) -> Result<(BookSnapshot, StreamStart), ReadFault> {
        for _ in 0..MAX_READ_ATTEMPTS {
            let snapshot = self.read(handle)?;
            let (epoch, next_position) = match snapshot.continuity() {
                MutationContinuity::Lost { reason, .. } => {
                    let reason = reason.clone();
                    return Ok((snapshot, StreamStart::Lost(reason)));
                }
                MutationContinuity::Intact {
                    epoch,
                    next_position,
                } => (*epoch, *next_position),
            };
            let cursor = MutationCursor::new(epoch, next_position);
            let stored = self.event_cursor(handle, &cursor)?;
            if stored.is_none_or(|stored| !later(&stored, &cursor)) {
                return Ok((snapshot, StreamStart::At(cursor)));
            }
        }
        Err(ReadFault::Contended {
            attempts: MAX_READ_ATTEMPTS,
        })
    }

    fn event_slot(
        &self,
        handle: MarketHandle,
        position: u64,
    ) -> Result<super::cell::Record<'_>, ReadFault> {
        if handle.segment() != self.binding {
            return Err(ReadFault::ForeignSegment);
        }
        layout::event_slot_record(
            self.region.cells(),
            self.geometry.layout(),
            handle.entry_index(),
            position,
        )
        .map_err(ReadFault::Segment)
    }

    fn event_cursor(
        &self,
        handle: MarketHandle,
        cursor: &MutationCursor,
    ) -> Result<Option<MutationCursor>, ReadFault> {
        let slot = self.event_slot(handle, cursor.position())?;
        under_seqlock(slot.seq(EVT_SEQUENCE), || {
            MutationCursor::new(
                slot.word64(EVT_CURSOR_EPOCH).relaxed_load(),
                slot.word64(EVT_CURSOR_POSITION).relaxed_load(),
            )
        })
    }

    fn event_words(
        &self,
        handle: MarketHandle,
        cursor: &MutationCursor,
    ) -> Result<Option<EventWords>, ReadFault> {
        let slot = self.event_slot(handle, cursor.position())?;
        under_seqlock(slot.seq(EVT_SEQUENCE), || read_event_words(slot))
    }
}

/// Where an attachment's mutation stream starts.
enum StreamStart {
    At(MutationCursor),
    Lost(ContinuityReason),
}

impl StreamStart {
    fn cursor(&self, continuity: &MutationContinuity) -> MutationCursor {
        match self {
            Self::At(cursor) => cursor.clone(),
            Self::Lost(_) => MutationCursor::new(continuity.epoch(), 0),
        }
    }
    fn lost(self) -> Option<ContinuityReason> {
        match self {
            Self::At(_) => None,
            Self::Lost(reason) => Some(reason),
        }
    }
}

/// Whether `left` names a strictly later point in a book's mutation stream than `right`.
///
/// Lexicographic on `(epoch, position)` because positions restart at 0 on every new
/// continuity epoch: a bare position comparison would call the first mutation of a rebased
/// stream older than the last mutation of the previous one.
fn later(left: &MutationCursor, right: &MutationCursor) -> bool {
    (left.epoch(), left.position()) > (right.epoch(), right.position())
}

/// A book's published revision and mutation continuity, read as one coherent pair.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateCursor {
    revision: u64,
    continuity: MutationContinuity,
}
impl StateCursor {
    pub fn revision(&self) -> u64 {
        self.revision
    }
    /// The mutation stream this revision publishes: the epoch and next position an intact
    /// stream stands at, or the reason a lost one broke.
    pub fn continuity(&self) -> &MutationContinuity {
        &self.continuity
    }
}

/// What one poll of an [`EventStream`] produced.
#[derive(Clone, Debug, Eq, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "a delivery is returned by value on the poll path; boxing it would put a \
              heap allocation on every event a consumer receives"
)]
pub enum EventPoll {
    Delivered(RetainedEvent),
    /// The writer has not reached this stream's position yet. Nothing was lost.
    Idle,
}

/// One delivery read out of a market's retained-event ring.
///
/// Both kinds occupy positions in the same stream, in the order the writer produced them,
/// so a resolution delivered between two mutations really did arrive between them. A
/// resolution changes no level and advances no revision; it names the revision it was
/// ordered after.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetainedEvent {
    Mutation(MutationEvent),
    Resolution(ResolutionEvent),
}
impl RetainedEvent {
    pub fn market(&self) -> &MarketRef {
        match self {
            Self::Mutation(event) => event.market(),
            Self::Resolution(event) => event.market(),
        }
    }
    /// The book revision a mutation's commit produced, or the one a resolution is ordered
    /// after.
    pub fn revision(&self) -> u64 {
        match self {
            Self::Mutation(event) => event.revision(),
            Self::Resolution(event) => event.revision(),
        }
    }
    pub fn cursor(&self) -> &MutationCursor {
        match self {
            Self::Mutation(event) => event.cursor(),
            Self::Resolution(event) => event.cursor(),
        }
    }
    /// When the writer stamped this delivery, in wall-clock nanoseconds since the Unix
    /// epoch, or `None` when it was left unset.
    pub fn commit_time_nanos(&self) -> Option<u64> {
        match self {
            Self::Mutation(event) => event.commit_time_nanos(),
            Self::Resolution(event) => event.commit_time_nanos(),
        }
    }
    /// When the writer stamped this delivery, in wall-clock nanoseconds since the Unix
    /// epoch, or `None` when it was left unset.
    pub fn arrival_time_nanos(&self) -> Option<u64> {
        match self {
            Self::Mutation(event) => event.arrival_time_nanos(),
            Self::Resolution(event) => event.arrival_time_nanos(),
        }
    }
}

/// How a resolution slot's revision was labelled, minus the native family.
///
/// A resolution slot stores no family text: the delivery kind already names the venue
/// message family, so carrying an empty string in a [`PublicationOrigin`] would claim the
/// writer stored a family it never did.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolutionPublication {
    origin: Origin,
    representation: Representation,
}
impl ResolutionPublication {
    pub fn origin(&self) -> &Origin {
        &self.origin
    }
    pub fn representation(&self) -> &Representation {
        &self.representation
    }
}

/// One venue-reported market resolution read out of a market's retained-event ring.
///
/// A bounded projection of [`crate::MarketResolution`]: the venue's winning outcome, its
/// index, the venue's own label for the market, its resolution timestamp lexeme, and the
/// feed the report arrived on. Every text is reproduced exactly as the venue published it
/// and the timestamp is never parsed into a number. The full provenance envelope —
/// connection identity, source evidence, the venue's own timestamp lexeme inside provenance
/// — deliberately does not cross the segment, exactly as it does not for a mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolutionEvent {
    market: MarketRef,
    revision: u64,
    cursor: MutationCursor,
    commit_time: u64,
    arrival_time: u64,
    publication: ResolutionPublication,
    winning_index: u32,
    delivery_path: DeliveryPath,
    winning_outcome: String,
    market_type: String,
    resolution_date: String,
    daemon_generation: u64,
    subscription_generation: u64,
}

impl ResolutionEvent {
    pub fn market(&self) -> &MarketRef {
        &self.market
    }
    /// The book revision this resolution is ordered after. A resolution advances none.
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn cursor(&self) -> &MutationCursor {
        &self.cursor
    }
    /// When the writer stamped this event, in wall-clock nanoseconds since the Unix epoch,
    /// or `None` when it was left unset. The clock caveat of
    /// [`BookSnapshot::commit_time_nanos`] applies unchanged.
    pub fn commit_time_nanos(&self) -> Option<u64> {
        (self.commit_time != 0).then_some(self.commit_time)
    }
    /// When the writer stamped this event, in wall-clock nanoseconds since the Unix epoch,
    /// or `None` when it was left unset. The clock caveat of
    /// [`BookSnapshot::arrival_time_nanos`] applies unchanged.
    pub fn arrival_time_nanos(&self) -> Option<u64> {
        (self.arrival_time != 0).then_some(self.arrival_time)
    }
    pub fn publication(&self) -> &ResolutionPublication {
        &self.publication
    }
    /// The outcome the venue declared the winner, in the venue's own text.
    pub fn winning_outcome(&self) -> &str {
        &self.winning_outcome
    }
    /// The venue's own index of the winning outcome, as reported.
    pub fn winning_index(&self) -> u32 {
        self.winning_index
    }
    /// The venue's own label for the resolved market — its market type where the venue
    /// publishes one.
    pub fn market_type(&self) -> &str {
        &self.market_type
    }
    /// The venue's resolution timestamp, kept as the lexeme it published and never parsed
    /// into an instant here.
    pub fn resolution_date(&self) -> &str {
        &self.resolution_date
    }
    /// The feed this resolution arrived on.
    pub fn delivery_path(&self) -> &DeliveryPath {
        &self.delivery_path
    }
    pub fn daemon_generation(&self) -> u64 {
        self.daemon_generation
    }
    pub fn subscription_generation(&self) -> u64 {
        self.subscription_generation
    }
}

/// Why a poll produced no delivery.
///
/// [`Self::ContinuityLost`] is terminal for the attachment: it is sticky, repeats unchanged
/// on every later poll, and is cleared only by [`EventStream::reattach`].
///
/// [`Self::Read`] is never sticky — the stream is left exactly where it was — but the two
/// halves of it call for opposite responses, and a caller must tell them apart:
///
/// - [`ReadFault::Contended`] and [`ReadFault::WriterStalled`] are this reader's own bound
///   expiring against a live writer. Nothing about the segment changed; poll again.
/// - [`ReadFault::MalformedRecord`] and [`ReadFault::SlotOwnershipMismatch`] are permanent
///   properties of the bytes at this position. The slot will not become well-formed, so
///   polling again returns the same fault forever: the consumer must reattach or escalate.
///   Spinning on one is the hang the mandatory state-slot step of [`EventStream::poll`]
///   exists to rule out.
///
/// Every other variant is an attachment failure the handle itself carries and is likewise
/// not retryable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StreamFault {
    ContinuityLost { reason: ContinuityReason },
    Read(ReadFault),
}
impl core::fmt::Display for StreamFault {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("no mutation delivery")
    }
}
impl std::error::Error for StreamFault {}

/// One level change read out of a market's retained-event ring.
///
/// A bounded projection of [`crate::BookMutation`]: the coordinate, the value before and
/// after, and the provenance words that tell a venue-reported change from one this daemon
/// derived. The full provenance envelope — connection identity, source evidence, the
/// venue's own timestamp lexeme — deliberately does not cross the segment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationEvent {
    market: MarketRef,
    revision: u64,
    cursor: MutationCursor,
    commit_time: u64,
    arrival_time: u64,
    publication: PublicationOrigin,
    side: Side,
    price: Price,
    old_quantity: Option<Quantity>,
    new_quantity: Option<Quantity>,
    daemon_generation: u64,
    subscription_generation: u64,
}

impl MutationEvent {
    pub fn market(&self) -> &MarketRef {
        &self.market
    }
    /// The book revision whose commit produced this mutation.
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn cursor(&self) -> &MutationCursor {
        &self.cursor
    }
    /// When the writer stamped this event, in wall-clock nanoseconds since the Unix epoch,
    /// or `None` when it was left unset. The clock caveat of
    /// [`BookSnapshot::commit_time_nanos`] applies unchanged.
    pub fn commit_time_nanos(&self) -> Option<u64> {
        (self.commit_time != 0).then_some(self.commit_time)
    }
    /// When the writer stamped this event, in wall-clock nanoseconds since the Unix epoch,
    /// or `None` when it was left unset. The clock caveat of
    /// [`BookSnapshot::arrival_time_nanos`] applies unchanged.
    pub fn arrival_time_nanos(&self) -> Option<u64> {
        (self.arrival_time != 0).then_some(self.arrival_time)
    }
    /// How this change was labelled: source-reported, normalized, or locally derived, in
    /// which representation and under which venue-native family.
    pub fn publication(&self) -> &PublicationOrigin {
        &self.publication
    }
    pub fn side(&self) -> Side {
        self.side
    }
    /// The coordinate's price. Both halves of the change name it, so it is carried once.
    pub fn price(&self) -> &Price {
        &self.price
    }
    /// The quantity resting at the coordinate before the change, or `None` when the
    /// coordinate was absent.
    pub fn old_quantity(&self) -> Option<&Quantity> {
        self.old_quantity.as_ref()
    }
    /// The quantity resting at the coordinate after the change, or `None` when the change
    /// removed it.
    pub fn new_quantity(&self) -> Option<&Quantity> {
        self.new_quantity.as_ref()
    }
    pub fn daemon_generation(&self) -> u64 {
        self.daemon_generation
    }
    pub fn subscription_generation(&self) -> u64 {
        self.subscription_generation
    }
}

/// One consumer's attachment to a book's retained-event ring.
///
/// The cursor lives here, in the consumer's own memory: a reader owns no cell of the
/// segment and publishes no progress, so the writer cannot tell one exists and can never be
/// slowed by one.
///
/// **Overflow behaviour.** The ring wraps. A consumer whose position was overwritten is
/// told so with [`ContinuityReason::Overrun`] on its next poll, receives the same loss on
/// every later poll, and never receives partial history.
pub struct EventStream {
    reader: SegmentReader,
    handle: MarketHandle,
    market: MarketRef,
    cursor: MutationCursor,
    lost: Option<ContinuityReason>,
}

impl EventStream {
    /// The position this stream will deliver next, whether or not the writer has reached it.
    pub fn cursor(&self) -> &MutationCursor {
        &self.cursor
    }

    /// Delivers the mutation at this stream's position, or says why it cannot.
    ///
    /// The order below **is** the contract:
    ///
    /// 1. The state slot's continuity is read first. A published epoch other than this
    ///    stream's means the writer rebased — a recovery base or a divergence checkpoint —
    ///    and reports [`ContinuityReason::RecoveryBase`]; a published stream that is
    ///    [`MutationContinuity::Lost`] at this stream's own epoch reports the reason it
    ///    broke for. This step is mandatory and cannot be optimized away: a recovery base
    ///    commits with **zero** mutations and restarts positions at 0, so a consumer parked
    ///    at a high position would read the previous lap's slot, conclude "not written
    ///    yet", and wait forever. Only the state slot closes that, and an explicit loss is
    ///    what the book's continuity rule requires, never a hang.
    /// 2. The event slot at this stream's position is read under its own seqlock. A stored
    ///    cursor equal to this one is delivered and the stream advances; a strictly later
    ///    one in the same epoch means the writer lapped the ring past this consumer and
    ///    reports [`ContinuityReason::Overrun`]; a later epoch reports
    ///    [`ContinuityReason::RecoveryBase`]; an unwritten slot or an earlier stored cursor
    ///    is [`EventPoll::Idle`].
    /// 3. Any loss is sticky until [`Self::reattach`].
    ///
    /// [`StreamFault::Read`] is transient and never sticky: the stream stays exactly where
    /// it was and the poll may simply be repeated.
    pub fn poll(&mut self) -> Result<EventPoll, StreamFault> {
        if let Some(reason) = &self.lost {
            return Err(StreamFault::ContinuityLost {
                reason: reason.clone(),
            });
        }
        let state = self
            .reader
            .read_cursor(self.handle)
            .map_err(StreamFault::Read)?;
        match state.continuity() {
            MutationContinuity::Intact { epoch, .. } if *epoch != self.cursor.epoch() => {
                return Err(self.lose(ContinuityReason::RecoveryBase));
            }
            MutationContinuity::Lost { epoch, reason } => {
                let reason = if *epoch == self.cursor.epoch() {
                    reason.clone()
                } else {
                    ContinuityReason::RecoveryBase
                };
                return Err(self.lose(reason));
            }
            MutationContinuity::Intact { .. } => {}
        }
        let Some(words) = self
            .reader
            .event_words(self.handle, &self.cursor)
            .map_err(StreamFault::Read)?
        else {
            return Ok(EventPoll::Idle);
        };
        let stored = MutationCursor::new(words.shared.cursor_epoch, words.shared.cursor_position);
        if stored.epoch() > self.cursor.epoch() {
            return Err(self.lose(ContinuityReason::RecoveryBase));
        }
        if later(&stored, &self.cursor) {
            return Err(self.lose(ContinuityReason::Overrun));
        }
        if stored != self.cursor {
            return Ok(EventPoll::Idle);
        }
        if words.shared.directory_index != self.handle.entry_index() {
            return Err(StreamFault::Read(ReadFault::SlotOwnershipMismatch));
        }
        let event = decode_event(self.market.clone(), stored, words)
            .ok_or(StreamFault::Read(ReadFault::MalformedRecord))?;
        let Some(next) = self.cursor.position().checked_add(1) else {
            return Err(self.lose(ContinuityReason::LocalLoss));
        };
        self.cursor = MutationCursor::new(self.cursor.epoch(), next);
        Ok(EventPoll::Delivered(event))
    }

    /// Re-establishes this stream against the book's current state, clearing any loss.
    ///
    /// Returns the snapshot the stream now resumes from, under exactly
    /// [`SegmentReader::attach_stream`]'s coherence rule: the new cursor is the snapshot's
    /// own next position, from the same accepted seqlock interval. A book whose published
    /// continuity is still lost reattaches straight back into the sticky lost state, which
    /// is the honest answer rather than a stream that pretends to resume.
    pub fn reattach(&mut self) -> Result<BookSnapshot, ReadFault> {
        let (snapshot, start) = self.reader.attach_at(self.handle)?;
        self.market = snapshot.market().clone();
        self.cursor = start.cursor(snapshot.continuity());
        self.lost = start.lost();
        Ok(snapshot)
    }

    fn lose(&mut self, reason: ContinuityReason) -> StreamFault {
        self.lost = Some(reason.clone());
        StreamFault::ContinuityLost { reason }
    }
}

/// Copies a seqlock-published record under one accepted interval, per
/// `docs/notes/shared-memory-model.md` §3.1.
///
/// `copy` is run only between an acquire load that observed an even counter and the
/// fenced recheck that accepts it, so what it returns is either a single publication's
/// worth of words or discarded. `Ok(None)` is a record the writer has never published.
/// Retries up to [`MAX_READ_ATTEMPTS`] times and never waits on the writer: an odd counter
/// that never moves is [`ReadFault::WriterStalled`], one that keeps moving is
/// [`ReadFault::Contended`].
fn under_seqlock<T>(
    counter: super::cell::SeqCell<'_>,
    copy: impl Fn() -> T,
) -> Result<Option<T>, ReadFault> {
    let mut stalled_at = None;
    let mut moved = false;
    for _ in 0..MAX_READ_ATTEMPTS {
        let interval = match counter.open_read() {
            SeqOpen::Unpublished => return Ok(None),
            SeqOpen::InFlight(opened) => {
                match stalled_at {
                    None => stalled_at = Some(opened),
                    Some(seen) if seen != opened => moved = true,
                    Some(_) => {}
                }
                core::hint::spin_loop();
                continue;
            }
            SeqOpen::Stable(interval) => interval,
        };
        let copied = copy();
        if !counter.close_read(interval) {
            moved = true;
            core::hint::spin_loop();
            continue;
        }
        return Ok(Some(copied));
    }
    match stalled_at {
        Some(slot_revision) if !moved => Err(ReadFault::WriterStalled { slot_revision }),
        _ => Err(ReadFault::Contended {
            attempts: MAX_READ_ATTEMPTS,
        }),
    }
}

fn identity_bytes(witness: super::cell::PublishedWitness<'_>) -> Option<&[u8]> {
    let length = usize::try_from(witness.word32(ENT_IDENTITY_LEN)).ok()?;
    (length <= IDENTITY_CAPACITY).then(|| witness.bytes(ENT_IDENTITY, length))
}

fn read_decimal(cell: super::cell::Record<'_>) -> (u64, u64, u32) {
    (
        cell.word64(DEC_COEFFICIENT_LOW).relaxed_load(),
        cell.word64(DEC_COEFFICIENT_HIGH).relaxed_load(),
        cell.word32(DEC_SCALE).relaxed_load(),
    )
}

fn read_text_words<const WORDS: usize>(slot: super::cell::Record<'_>, base: usize) -> [u64; WORDS] {
    let mut words = [0_u64; WORDS];
    for (word, value) in words.iter_mut().enumerate() {
        *value = slot.word64(base + word * 8).relaxed_load();
    }
    words
}

fn read_event_words(slot: super::cell::Record<'_>) -> EventWords {
    let shared = SharedEventWords {
        cursor_epoch: slot.word64(EVT_CURSOR_EPOCH).relaxed_load(),
        cursor_position: slot.word64(EVT_CURSOR_POSITION).relaxed_load(),
        book_revision: slot.word64(EVT_BOOK_REVISION).relaxed_load(),
        commit_time: slot.word64(EVT_COMMIT_TIME).relaxed_load(),
        arrival_time: slot.word64(EVT_ARRIVAL_TIME).relaxed_load(),
        daemon_generation: slot.word64(EVT_DAEMON_GENERATION).relaxed_load(),
        subscription_generation: slot.word64(EVT_SUBSCRIPTION_GENERATION).relaxed_load(),
        origin: slot.word32(EVT_ORIGIN).relaxed_load(),
        derivation: slot.word32(EVT_DERIVATION).relaxed_load(),
        representation: slot.word32(EVT_REPRESENTATION).relaxed_load(),
        directory_index: slot.word32(EVT_DIRECTORY_INDEX).relaxed_load(),
    };
    let body = match slot.word32(EVT_DELIVERY_KIND).relaxed_load() {
        DELIVERY_LEVEL_MUTATION => EventBody::Mutation(MutationWords {
            side: slot.word32(EVT_SIDE).relaxed_load(),
            old_present: slot.word32(EVT_OLD_PRESENT).relaxed_load(),
            new_present: slot.word32(EVT_NEW_PRESENT).relaxed_load(),
            family_length: slot.word32(EVT_FAMILY_LEN).relaxed_load(),
            family_words: read_text_words(slot, EVT_FAMILY_WORDS),
            price: read_decimal(slot.sub(EVT_PRICE, DECIMAL_CELL_BYTES)),
            old_quantity: read_decimal(slot.sub(EVT_OLD_QUANTITY, DECIMAL_CELL_BYTES)),
            new_quantity: read_decimal(slot.sub(EVT_NEW_QUANTITY, DECIMAL_CELL_BYTES)),
        }),
        DELIVERY_MARKET_RESOLVED => EventBody::Resolution(ResolutionWords {
            winning_index: slot.word32(EVT_RES_WINNING_INDEX).relaxed_load(),
            delivery_path: slot.word32(EVT_RES_DELIVERY_PATH).relaxed_load(),
            outcome_length: slot.word32(EVT_RES_OUTCOME_LEN).relaxed_load(),
            type_length: slot.word32(EVT_RES_TYPE_LEN).relaxed_load(),
            date_length: slot.word32(EVT_RES_DATE_LEN).relaxed_load(),
            outcome_words: read_text_words(slot, EVT_RES_OUTCOME),
            type_words: read_text_words(slot, EVT_RES_TYPE),
            date_words: read_text_words(slot, EVT_RES_DATE),
        }),
        _ => EventBody::Unknown,
    };
    EventWords { shared, body }
}

/// The text those words carry, or `None` when the declared length exceeds the cell's
/// capacity or the bytes are not UTF-8.
fn stored_text<const WORDS: usize>(length: u32, words: &[u64; WORDS]) -> Option<String> {
    let length = usize::try_from(length).ok()?;
    if length > WORDS * 8 {
        return None;
    }
    String::from_utf8(
        (0..length)
            .map(|position| (words[position / 8] >> ((position % 8) * 8)) as u8)
            .collect(),
    )
    .ok()
}

/// The delivery those words describe, or `None` for anything this ABI does not define.
///
/// Every discriminant is checked rather than guessed at: an unknown delivery kind — the
/// FFI's synthesized continuity marker included, which is never stored — an unknown origin,
/// representation, side or delivery path, a presence word outside `{0, 1}`, a change naming
/// neither half, an unrepresentable decimal, and any text cell whose declared length or
/// bytes do not decode all make the record malformed.
fn decode_event(
    market: MarketRef,
    cursor: MutationCursor,
    words: EventWords,
) -> Option<RetainedEvent> {
    let shared = words.shared;
    match words.body {
        EventBody::Mutation(body) => {
            let present = |word: u32| match word {
                0 => Some(false),
                1 => Some(true),
                _ => None,
            };
            let (old_present, new_present) =
                (present(body.old_present)?, present(body.new_present)?);
            if !old_present && !new_present {
                return None;
            }
            let quantity = |present: bool, (low, high, scale)| -> Option<Option<Quantity>> {
                if !present {
                    return Some(None);
                }
                codec::quantity(low, high, scale).map(Some)
            };
            Some(RetainedEvent::Mutation(MutationEvent {
                market,
                revision: shared.book_revision,
                cursor,
                commit_time: shared.commit_time,
                arrival_time: shared.arrival_time,
                publication: PublicationOrigin {
                    origin: codec::origin(shared.origin, shared.derivation)?,
                    representation: codec::representation(shared.representation)?,
                    native_family: stored_text(body.family_length, &body.family_words)?,
                },
                side: codec::side(body.side)?,
                price: codec::price(body.price.0, body.price.1, body.price.2)?,
                old_quantity: quantity(old_present, body.old_quantity)?,
                new_quantity: quantity(new_present, body.new_quantity)?,
                daemon_generation: shared.daemon_generation,
                subscription_generation: shared.subscription_generation,
            }))
        }
        EventBody::Resolution(body) => Some(RetainedEvent::Resolution(ResolutionEvent {
            market,
            revision: shared.book_revision,
            cursor,
            commit_time: shared.commit_time,
            arrival_time: shared.arrival_time,
            publication: ResolutionPublication {
                origin: codec::origin(shared.origin, shared.derivation)?,
                representation: codec::representation(shared.representation)?,
            },
            winning_index: body.winning_index,
            delivery_path: codec::delivery_path(body.delivery_path)?,
            winning_outcome: stored_text(body.outcome_length, &body.outcome_words)?,
            market_type: stored_text(body.type_length, &body.type_words)?,
            resolution_date: stored_text(body.date_length, &body.date_words)?,
            daemon_generation: shared.daemon_generation,
            subscription_generation: shared.subscription_generation,
        })),
        EventBody::Unknown => None,
    }
}

fn read_words(slot: super::cell::Record<'_>, capacity: u32) -> SlotWords {
    let mut family_words = [0_u64; NATIVE_FAMILY_WORDS];
    for (word, value) in family_words.iter_mut().enumerate() {
        *value = slot.word64(SLOT_FAMILY_WORDS + word * 8).relaxed_load();
    }
    let level_count = slot.word32(SLOT_LEVEL_COUNT).relaxed_load();
    let read = level_count.min(capacity);
    let levels = (0..read as usize)
        .map(|position| {
            let cell = slot.sub(
                SLOT_PREFIX_BYTES + position * LEVEL_CELL_BYTES,
                LEVEL_CELL_BYTES,
            );
            let price = cell.sub(LVL_PRICE, DECIMAL_CELL_BYTES);
            let quantity = cell.sub(LVL_QUANTITY, DECIMAL_CELL_BYTES);
            (
                cell.word32(LVL_SIDE).relaxed_load(),
                price.word64(DEC_COEFFICIENT_LOW).relaxed_load(),
                price.word64(DEC_COEFFICIENT_HIGH).relaxed_load(),
                price.word32(DEC_SCALE).relaxed_load(),
                quantity.word64(DEC_COEFFICIENT_LOW).relaxed_load(),
                quantity.word64(DEC_COEFFICIENT_HIGH).relaxed_load(),
                quantity.word32(DEC_SCALE).relaxed_load(),
            )
        })
        .collect();
    SlotWords {
        book_revision: slot.word64(SLOT_BOOK_REVISION).relaxed_load(),
        commit_time: slot.word64(SLOT_COMMIT_TIME).relaxed_load(),
        arrival_time: slot.word64(SLOT_ARRIVAL_TIME).relaxed_load(),
        continuity_epoch: slot.word64(SLOT_CONTINUITY_EPOCH).relaxed_load(),
        continuity_position: slot.word64(SLOT_CONTINUITY_POSITION).relaxed_load(),
        sync_divergences: slot.word64(SLOT_SYNC_DIVERGENCES).relaxed_load(),
        authority_state: slot.word32(SLOT_AUTHORITY_STATE).relaxed_load(),
        authority_reason: slot.word32(SLOT_AUTHORITY_REASON).relaxed_load(),
        continuity_kind: slot.word32(SLOT_CONTINUITY_KIND).relaxed_load(),
        continuity_reason: slot.word32(SLOT_CONTINUITY_REASON).relaxed_load(),
        provenance_present: slot.word32(SLOT_PROVENANCE_PRESENT).relaxed_load(),
        origin: slot.word32(SLOT_ORIGIN).relaxed_load(),
        derivation: slot.word32(SLOT_DERIVATION).relaxed_load(),
        representation: slot.word32(SLOT_REPRESENTATION).relaxed_load(),
        family_length: slot.word32(SLOT_FAMILY_LEN).relaxed_load(),
        family_words,
        directory_index: slot.word32(SLOT_DIRECTORY_INDEX).relaxed_load(),
        level_count,
        levels,
    }
}

fn decode(market: MarketRef, words: SlotWords) -> Option<BookSnapshot> {
    let publication = match words.provenance_present {
        0 => None,
        1 => Some(PublicationOrigin {
            origin: codec::origin(words.origin, words.derivation)?,
            representation: codec::representation(words.representation)?,
            native_family: stored_text(words.family_length, &words.family_words)?,
        }),
        _ => return None,
    };
    let mut levels = Vec::with_capacity(words.levels.len());
    for (side, price_low, price_high, price_scale, quantity_low, quantity_high, quantity_scale) in
        words.levels
    {
        levels.push(Level::new(
            codec::side(side)?,
            codec::price(price_low, price_high, price_scale)?,
            codec::quantity(quantity_low, quantity_high, quantity_scale)?,
        ));
    }
    Some(BookSnapshot {
        market,
        revision: words.book_revision,
        authority: codec::authority_state(words.authority_state, words.authority_reason)?,
        continuity: codec::continuity(
            words.continuity_kind,
            words.continuity_reason,
            words.continuity_epoch,
            words.continuity_position,
        )?,
        sync_divergences: words.sync_divergences,
        commit_time: words.commit_time,
        arrival_time: words.arrival_time,
        publication,
        levels,
    })
}

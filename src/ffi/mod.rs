//! The C-ABI surface over the shared-memory reader: a `cdylib` entry point for Python, Node,
//! and any other non-Rust consumer of `docs/notes/shared-memory-model.md`.
//!
//! Every function here goes exclusively through [`crate::SegmentRegion`],
//! [`crate::SegmentReader`], [`crate::BookSnapshot`], and [`crate::EventStream`] — all safe
//! Rust. No offset constant and no region byte is ever touched directly; the only unsafe
//! this module performs is boundary crossing: dereferencing a caller-supplied pointer after
//! a null check, and reconstructing a `&[u8]` or `&mut [u8]` from a caller `(ptr, len)` pair.
//!
//! Every entry point runs behind [`guarded`], which converts an unwinding panic into
//! [`PMWS_STATUS_INTERNAL`] rather than letting it cross the ABI boundary as undefined
//! behaviour.
//!
//! The `AUTHORITY_*`, `REASON_*`, `CONTINUITY_*`, `BREAK_*`, `ORIGIN_*`, `DERIVATION_*`,
//! `REPRESENTATION_*`, and `SIDE_*` discriminant words this module writes come from
//! `crate::shm::codec` directly — never renumbered independently of it, since both sides
//! encode the same ABI discriminants and the compiler now enforces that agreement.
#![allow(unsafe_code)]

use crate::limitless::shard::{MarketOutcome, MarketRejection, MarketStatus};
use crate::shm::codec::{
    authority_words, break_word, continuity_words, delivery_path_word, origin_words,
    representation_word, side_word,
};
use crate::{
    Attachment, BookSnapshot, ChannelError, ContinuityReason, ControlRequest, ControlResponse,
    DecimalGrammar, DirtyCursor, DirtyPoll, DoorbellLocation, EventPoll, EventStream, ExactDecimal,
    FEATURE_DOORBELL_IN_HEADER, FEATURE_DOORBELL_PAGE, MAX_CONTROL_LINE_BYTES,
    MAX_TRANSFERRED_DESCRIPTORS, MarketHandle, MarketRef, MutationCursor, MutationEvent,
    NativeIdentifierKind, NativeMarketKey, ReadFault, RegionError, ResolutionEvent, RetainedEvent,
    SegmentFault, SegmentReader, SegmentRegion, StreamFault, Venue, WaitFault, WaitOutcome,
    recv_with_fds,
};
use std::cell::{Cell, RefCell};
use std::ffi::{CStr, c_char};
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The hand-rolled Node-API shim: a JS surface over this module's C ABI, loaded by Node via
/// `process.dlopen` against the built `cdylib` directly, with no `napi.h` build dependency.
mod napi;

pub const PMWS_STATUS_OK: i32 = 0;
pub const PMWS_STATUS_NONE: i32 = 1;
pub const PMWS_STATUS_INVALID_ARGUMENT: i32 = -1;
pub const PMWS_STATUS_IO: i32 = -2;
pub const PMWS_STATUS_SEGMENT_INCOMPATIBLE: i32 = -3;
pub const PMWS_STATUS_MARKET_NOT_FOUND: i32 = -4;
pub const PMWS_STATUS_NO_PUBLISHED_STATE: i32 = -5;
pub const PMWS_STATUS_CONTENDED: i32 = -6;
pub const PMWS_STATUS_WRITER_STALLED: i32 = -7;
pub const PMWS_STATUS_MALFORMED_RECORD: i32 = -8;
pub const PMWS_STATUS_BUFFER_TOO_SMALL: i32 = -9;
pub const PMWS_STATUS_CONTINUITY_LOST: i32 = -10;
pub const PMWS_STATUS_NOT_ATTACHED: i32 = -11;
pub const PMWS_STATUS_INTERNAL: i32 = -12;
pub const PMWS_STATUS_ATTACH_REFUSED: i32 = -13;
pub const PMWS_STATUS_ATTACH_INCOMPLETE: i32 = -14;
/// [`pmws_wait`]'s park phase cannot reach this segment's doorbell: a page-placement
/// segment's sibling page failed to open at [`pmws_open`] time, or never crossed a
/// [`pmws_connect`] attachment at all ([`WaitFault::DoorbellUnavailable`]).
///
/// The segment itself is fine and every other call on it behaves normally; parked waiting
/// specifically is what this session cannot do. A caller seeing this should not retry the
/// park: spin instead, with a `spin_micros` budget up to [`PMWS_MAX_SPIN_MICROS`], or poll
/// [`pmws_publication_generation`] directly.
pub const PMWS_STATUS_DOORBELL_UNAVAILABLE: i32 = -15;
/// [`pmws_lease`] asked for a market this session's segment does not hold: the daemon granted
/// the lease and answered with another shard's segment, which this session has no mapping for.
///
/// Distinct from [`PMWS_STATUS_SEGMENT_INCOMPATIBLE`], which names a segment that failed to
/// validate or is not the one the answer promised. Nothing is wrong here: the segment named is
/// a good one, held by the same daemon, and simply not this session's. The lease is given
/// back, and the daemon's answer confirming that is read, before this is reported, so the
/// refusal leaves no demand at the daemon; a rollback that cannot be confirmed is
/// [`PMWS_STATUS_IO`] instead. A consumer that wants that market opens a second session on it
/// with [`pmws_connect`].
pub const PMWS_STATUS_FOREIGN_SEGMENT: i32 = -16;

const PMWS_FFI_VERSION: i32 = 8;

/// How long [`pmws_connect`] spends on one attachment conversation, from the established
/// connection onward.
///
/// A daemon that accepts a connection and then says nothing — or says one byte at a time —
/// would otherwise hold the calling thread indefinitely, and the caller is a consumer's own
/// thread rather than a runtime that could cancel it. It is one deadline over the exchange
/// rather than a limit per step, because a limit per step is one a peer can reset at will.
/// Generous against a control task that serves one conversation at a time, and far short of a
/// wait that looks like a hang.
///
/// It does not cover the `connect` itself, which the standard library offers no timed form of
/// for a Unix domain socket: a peer that binds the control path and never accepts can hold a
/// caller in that syscall. Every byte after it is inside this bound.
const ATTACH_TIMEOUT: Duration = Duration::from_secs(5);

/// The widest `spin_micros` [`pmws_wait`] accepts: ten seconds of busy spinning.
///
/// A ceiling exists because `spin_micros` is unsigned and a caller's arithmetic mistake is
/// indistinguishable from an intention once it has been narrowed — a language binding that
/// lets `-1` become `u32::MAX` asks this process to burn a core for seventy-one minutes, and
/// the call would honour it. Ten seconds is far past any spin a latency-driven consumer has
/// a use for (the design's spin floor is microseconds) and far short of a wait that looks
/// like a hang, so a value above it is a caller error rather than a slow success:
/// [`pmws_wait`] answers [`PMWS_STATUS_INVALID_ARGUMENT`] and spins for nothing at all.
pub const PMWS_MAX_SPIN_MICROS: u32 = 10_000_000;

/// The widest scale a stored decimal can carry; mirrors `shm::codec::MAX_STORED_SCALE`.
const MAX_STORED_SCALE: u32 = u16::MAX as u32;
/// Bytes a native-family label occupies on the wire; mirrors `shm::layout::NATIVE_FAMILY_CAPACITY`.
const NATIVE_FAMILY_CAPACITY: usize = 64;
/// Bytes each of a resolution's three venue-native texts occupies on the wire; mirrors
/// `shm::layout::RES_OUTCOME_CAPACITY`, `RES_TYPE_CAPACITY` and `RES_DATE_CAPACITY`.
const RESOLUTION_OUTCOME_CAPACITY: usize = 64;
const RESOLUTION_TYPE_CAPACITY: usize = 32;
const RESOLUTION_DATE_CAPACITY: usize = 32;

const DELIVERY_MUTATION: u32 = 1;
/// The FFI's own synthesized continuity-loss marker. Never stored in a segment; see
/// `shm::layout::DELIVERY_LEVEL_MUTATION`'s doc comment.
const DELIVERY_CONTINUITY_LOST: u32 = 2;
/// A delivered market resolution; mirrors `shm::layout::DELIVERY_MARKET_RESOLVED`.
const DELIVERY_RESOLUTION: u32 = 3;

/// One exact decimal, byte-for-byte the ABI decimal cell `shm::codec.rs` reads and writes:
/// a 128-bit two's-complement coefficient as two little-endian-native 64-bit halves, and a
/// decimal scale in digits. Every price and quantity this ABI carries has a non-negative
/// coefficient, so `coefficient_high`'s top bit is always clear in practice, though the
/// field is exactly wide enough to hold a two's-complement sign bit if a future ABI needs
/// one. 24 bytes, pinned at FFI v1: see the `ffi_struct_sizes_are_pinned` test.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PmwsDecimal {
    pub coefficient_low: u64,
    pub coefficient_high: u64,
    pub scale: u32,
    /// Always 0. Reserved so the struct stays 8-byte aligned with no implicit padding.
    pub reserved: u32,
}
impl PmwsDecimal {
    const ZERO: Self = Self {
        coefficient_low: 0,
        coefficient_high: 0,
        scale: 0,
        reserved: 0,
    };
}

/// One resting level: a side, a price, and a quantity. 56 bytes, pinned at FFI v1.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PmwsLevel {
    /// `1` = Bid, `2` = Ask; the same word [`side_word`] and `shm::codec::side_word` use.
    pub side: u32,
    pub reserved: u32,
    pub price: PmwsDecimal,
    pub quantity: PmwsDecimal,
}

/// One book's latest published state, everything [`BookSnapshot`]'s accessors expose except
/// its levels, which land in the caller's separate `PmwsLevel` buffer, and its market
/// identity, which [`pmws_market_identity`] answers separately since it needs no live read.
///
/// `daemon_generation` and `subscription_generation` are deliberately absent: the state slot
/// this struct mirrors carries no such cell (only the retained-event slot behind
/// [`PmwsEvent`] does), so including them here would ship two fields the segment never sets
/// rather than reflect anything [`BookSnapshot`] exposes. 160 bytes, pinned at FFI v3.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PmwsState {
    pub revision: u64,
    /// The mutation stream's epoch: [`MutationContinuity::epoch`] either way.
    pub cursor_epoch: u64,
    /// The stream's next position when intact, or 0 when lost — `shm::codec`'s convention.
    pub cursor_position: u64,
    pub sync_divergences: u64,
    /// Wall-clock nanoseconds since the Unix epoch, or 0 when `commit_time_present == 0`.
    pub commit_time: u64,
    pub authority_state: u32,
    /// 0 unless `authority_state` is Stale (word 6).
    pub authority_reason: u32,
    /// 1 = Intact, 2 = Lost.
    pub continuity_kind: u32,
    /// 0 for an intact stream; a `BREAK_*` word for a lost one.
    pub continuity_reason: u32,
    pub commit_time_present: u32,
    pub publication_present: u32,
    /// Meaningful only when `publication_present == 1`.
    pub origin: u32,
    pub derivation: u32,
    pub representation: u32,
    pub native_family_len: u32,
    /// How many of `levels` this call wrote. 0 when the call returned
    /// [`PMWS_STATUS_BUFFER_TOO_SMALL`].
    pub level_count: u32,
    /// How many levels the caller's buffer needs to hold every level of this revision.
    pub level_capacity_required: u32,
    pub native_family: [u8; NATIVE_FAMILY_CAPACITY],
    /// Wall-clock nanoseconds since the Unix epoch at which the socket read that drove this
    /// revision returned, or 0 when this commit was not driven by a venue frame. Unlike
    /// `commit_time`, this field carries no separate presence flag: 0 always means absent,
    /// matching [`PmwsEvent::commit_time`]'s own convention.
    pub arrival_time: u64,
}

/// One retained delivery: a level mutation (`delivery_kind == 1`), a venue-reported market
/// resolution (`delivery_kind == 3`), or a synthesized continuity-loss marker
/// (`delivery_kind == 2`).
///
/// On a loss, only `delivery_kind`, `continuity_reason`, `missed`, `cursor_epoch`, and
/// `cursor_position` are meaningful; every other field is zeroed. `missed` is always 0 in
/// this ABI generation — no [`ContinuityReason`] this crate reports carries a numeric count
/// — and is carried so a future ABI that adds one needs no new field.
///
/// The mutation fields (`price`, `old_quantity`, `new_quantity`, `side`, `old_present`,
/// `new_present`, `native_family`, `native_family_len`) are zeroed for kind 3, and the
/// resolution fields (`winning_index`, `delivery_path`, the three texts and their lengths)
/// are zeroed for kinds 1 and 2. `book_revision` and the cursor are meaningful for both
/// delivered kinds; for a resolution `book_revision` is the revision it is ordered after,
/// because a resolution advances none. 392 bytes, pinned at FFI v3.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PmwsEvent {
    pub missed: u64,
    pub cursor_epoch: u64,
    pub cursor_position: u64,
    pub book_revision: u64,
    /// Wall-clock nanoseconds since the Unix epoch, or 0 when unset.
    pub commit_time: u64,
    pub daemon_generation: u64,
    pub subscription_generation: u64,
    pub price: PmwsDecimal,
    /// Zeroed when `old_present == 0`.
    pub old_quantity: PmwsDecimal,
    /// Zeroed when `new_present == 0`.
    pub new_quantity: PmwsDecimal,
    /// `1` = a delivered mutation, `2` = a continuity loss, `3` = a delivered resolution.
    pub delivery_kind: u32,
    /// 0 for a delivered kind; a `BREAK_*` word when `delivery_kind == 2`.
    pub continuity_reason: u32,
    pub origin: u32,
    pub derivation: u32,
    pub representation: u32,
    pub native_family_len: u32,
    pub side: u32,
    pub old_present: u32,
    pub new_present: u32,
    pub native_family: [u8; NATIVE_FAMILY_CAPACITY],
    /// The venue's own index of the winning outcome. Meaningful only for kind 3.
    pub winning_index: u32,
    /// `1` = MarketFeed, `2` = LifecycleFeed, `3` = ResolutionFeed. Kind 3 only.
    pub delivery_path: u32,
    pub winning_outcome_len: u32,
    pub market_type_len: u32,
    pub resolution_date_len: u32,
    /// Always 0. Reserved so the struct stays 8-byte aligned with no implicit padding.
    pub reserved2: u32,
    /// The venue's winning-outcome text, UTF-8, `winning_outcome_len` bytes.
    pub winning_outcome: [u8; RESOLUTION_OUTCOME_CAPACITY],
    /// The venue's own label for the resolved market, UTF-8, `market_type_len` bytes.
    pub market_type: [u8; RESOLUTION_TYPE_CAPACITY],
    /// The venue's resolution timestamp lexeme, UTF-8, `resolution_date_len` bytes. Never
    /// parsed into a number by this ABI.
    pub resolution_date: [u8; RESOLUTION_DATE_CAPACITY],
    /// Always 0, reserved so `arrival_time` (below) lands 8-byte aligned with no implicit
    /// padding: `resolution_date` ends this struct at a 4-byte, not 8-byte, offset.
    pub reserved3: u32,
    /// Wall-clock nanoseconds since the Unix epoch at which the writer stamped this
    /// delivery, or 0 when unset. Meaningful for both delivered kinds; 0 for a synthesized
    /// continuity-loss marker (`delivery_kind == 2`), which stamps nothing.
    pub arrival_time: u64,
}

/// A session's segment geometry and current publication generation. 48 bytes, pinned at FFI
/// v1.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PmwsSegmentInfo {
    pub instance_id_low: u64,
    pub instance_id_high: u64,
    pub segment_generation: u64,
    pub publication_generation: u64,
    pub directory_capacity: u32,
    pub state_slot_capacity: u32,
    pub level_capacity: u32,
    pub event_capacity: u32,
}

/// Where each of a market's three identity strings landed inside [`pmws_market_identity`]'s
/// `buf`. 24 bytes, pinned at FFI v1.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PmwsIdentitySpans {
    pub venue_offset: u32,
    pub venue_len: u32,
    pub kind_offset: u32,
    pub kind_len: u32,
    pub key_offset: u32,
    pub key_len: u32,
}

struct Entry {
    market: MarketRef,
    handle: MarketHandle,
    stream: Option<EventStream>,
}

/// An attached reader session over one publication segment: a validated, read-only mapping
/// plus this session's own market-index table and event-stream cursors.
///
/// Opaque to C: reachable only through the pointer [`pmws_open`] returns and the functions
/// below. Deliberately not [`Sync`] — the market table is a [`RefCell`], so two threads
/// calling through the same pointer concurrently is a data race the type system does not
/// stop; one session per thread, or the caller's own external serialization, is required.
/// It is [`Send`]: every field may move to another thread, so a session opened on one thread
/// may be handed to and used from another, just never from two at once.
pub struct PmwsSession {
    reader: SegmentReader,
    /// The control connection this session was attached over, held open for as long as the
    /// session is, or `None` for one opened from a path by [`pmws_open`].
    ///
    /// It is not a channel this session reads books over — every byte of market data comes
    /// through the mapping — and it carries no traffic at all unless [`pmws_renew`] is
    /// called. It is held because the daemon leases the market to *this connection*: closing
    /// it releases the lease, and a consumer that dropped it the moment the descriptor
    /// arrived would be asking the daemon to unsubscribe the market it had just attached to.
    /// [`pmws_close`] closes it, and so does the process exiting, which is what makes a
    /// crashed consumer's demand go away without anything having to notice.
    control: RefCell<Option<UnixStream>>,
    /// The segment file's name as the attachment that granted this session named it, or
    /// `None` for one opened from a path by [`pmws_open`], which named a path instead.
    ///
    /// Diagnostic in the attachment and load-bearing here: it is what [`pmws_lease`] compares
    /// a further attachment's segment against, and the only field that separates two shards of
    /// one daemon, which share an instance identity and, freshly started, a generation too.
    segment: Option<String>,
    table: RefCell<Vec<Entry>>,
    /// This session's cursor into the segment's dirty-index ring, created lazily on the
    /// first [`pmws_next_dirty`] call rather than at [`pmws_open`] — a session that never
    /// calls it pays nothing for the ring's head-discovery scan.
    dirty_cursor: Cell<Option<DirtyCursor>>,
    _not_sync: PhantomData<Cell<()>>,
}

/// Runs `body` and converts an unwinding panic into `default` rather than letting it cross
/// the C ABI boundary, which is undefined behaviour.
///
/// `AssertUnwindSafe` is sound here: every closure this wraps borrows at most a
/// [`PmwsSession`], and the only interior mutability it holds is its market table. A panic
/// mid-mutation of that table leaves it in a valid, if possibly incomplete, `Vec` state,
/// never a torn one — a session never writes the segment itself, so a panic can strand no
/// authoritative state.
fn guarded<T>(default: T, body: impl FnOnce() -> T) -> T {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)).unwrap_or(default)
}

/// Borrows `ptr` as a session, or `None` for a null pointer.
///
/// # Safety
/// `ptr` must be null or a pointer [`pmws_open`] returned that has not since been passed to
/// [`pmws_close`].
unsafe fn session_ref<'a>(ptr: *const PmwsSession) -> Option<&'a PmwsSession> {
    if ptr.is_null() {
        return None;
    }
    // SAFETY: `ptr` is non-null; this function's own contract requires it be a live session
    // handle, which every entry point below documents identically for its `session` parameter.
    Some(unsafe { &*ptr })
}

/// Borrows `len` bytes at `ptr` as a non-empty UTF-8 `&str` — the rule every required input
/// string of this FFI follows (`path`, `venue`, `kind`, `key`): null is valid only paired
/// with a zero length, and a zero length is itself refused because none of these strings may
/// be empty.
///
/// # Safety
/// `ptr` must be null only when `len == 0`, and otherwise point to `len` readable,
/// initialized bytes for the duration of this call.
unsafe fn read_required_str<'a>(ptr: *const u8, len: usize) -> Option<&'a str> {
    if len == 0 || ptr.is_null() {
        return None;
    }
    // SAFETY: `len != 0` and `ptr` is non-null; the caller's (ptr, len) contract for every
    // required input string of this FFI promises `len` readable, initialized bytes at `ptr`.
    let bytes = unsafe { core::slice::from_raw_parts(ptr, len) };
    core::str::from_utf8(bytes).ok()
}

/// Borrows `len` writable slots at `ptr`, or `None` for a null pointer paired with a
/// non-zero length. A zero length is always valid, null or not — the "query the required
/// size" shape every buffer-out parameter of this FFI supports.
///
/// # Safety
/// `ptr` must be null only when `len == 0`, and otherwise point to `len` valid, writable,
/// properly aligned `T` slots, exclusively for the duration of this call.
unsafe fn out_slice<'a, T>(ptr: *mut T, len: usize) -> Option<&'a mut [T]> {
    if len == 0 {
        return Some(&mut []);
    }
    if ptr.is_null() {
        return None;
    }
    // SAFETY: `len != 0` and `ptr` is non-null; every writable (ptr, len) out-buffer of this
    // FFI promises `len` valid, writable, properly aligned `T` slots at `ptr`.
    Some(unsafe { core::slice::from_raw_parts_mut(ptr, len) })
}

/// Writes `value` through `ptr`, or does nothing for a null pointer. Returns whether it wrote.
///
/// # Safety
/// `ptr` must be null or point to a single valid, writable, properly aligned `T`.
unsafe fn write_out<T>(ptr: *mut T, value: T) -> bool {
    if ptr.is_null() {
        return false;
    }
    // SAFETY: `ptr` is non-null; every out-parameter of this FFI is documented as pointing
    // to a single valid, writable, properly aligned `T` when non-null, which is the caller's
    // obligation this function trusts.
    unsafe { ptr.write(value) };
    true
}

fn resolved_handle(session: &PmwsSession, market: u32) -> Option<MarketHandle> {
    session
        .table
        .borrow()
        .get(market as usize)
        .map(|entry| entry.handle)
}

fn stored_grammar() -> DecimalGrammar {
    DecimalGrammar::new(u16::MAX, 39, false, false).expect("stored decimal grammar is valid")
}

fn to_decimal(value: &ExactDecimal) -> PmwsDecimal {
    let bits = value.coefficient() as u128;
    PmwsDecimal {
        coefficient_low: bits as u64,
        coefficient_high: (bits >> 64) as u64,
        scale: u32::from(value.scale()),
        reserved: 0,
    }
}

/// The exact text `decimal` carries, or `None` when its scale exceeds
/// [`MAX_STORED_SCALE`] or its coefficient is negative — a price or quantity never is.
///
/// Rebuilds the crate's own [`ExactDecimal`] from `decimal`'s parts and renders it with
/// [`ExactDecimal::canonical`], so this can never drift from what the crate's own `Display`
/// produces.
fn decimal_text(decimal: &PmwsDecimal) -> Option<String> {
    if decimal.scale > MAX_STORED_SCALE {
        return None;
    }
    let value = ExactDecimal::from_parts(
        decimal.coefficient_low,
        decimal.coefficient_high,
        decimal.scale as u16,
        stored_grammar(),
    )
    .ok()?;
    Some(value.canonical())
}

fn pack_bytes<const CAPACITY: usize>(text: &str) -> ([u8; CAPACITY], u32) {
    let mut buffer = [0_u8; CAPACITY];
    let bytes = text.as_bytes();
    let len = bytes.len().min(CAPACITY);
    buffer[..len].copy_from_slice(&bytes[..len]);
    (buffer, len as u32)
}

fn pack_native_family(text: &str) -> ([u8; NATIVE_FAMILY_CAPACITY], u32) {
    pack_bytes(text)
}

/// Builds `PmwsState`'s scalar fields from `snapshot`, plus the level count required to
/// carry every level. `level_count` is left 0; the caller fills it once it knows the
/// destination buffer fit.
fn encode_state(snapshot: &BookSnapshot) -> (PmwsState, usize) {
    let (authority_state, authority_reason) = authority_words(snapshot.authority());
    let (continuity_kind, continuity_reason, cursor_epoch, cursor_position) =
        continuity_words(snapshot.continuity());
    let (commit_time_present, commit_time) = match snapshot.commit_time_nanos() {
        Some(value) => (1, value),
        None => (0, 0),
    };
    let (publication_present, origin, derivation, representation, native_family, native_family_len) =
        match snapshot.publication() {
            Some(publication) => {
                let (family, len) = pack_native_family(publication.native_family());
                let (origin, derivation) = origin_words(publication.origin());
                (
                    1,
                    origin,
                    derivation,
                    representation_word(publication.representation()),
                    family,
                    len,
                )
            }
            None => (0, 0, 0, 0, [0_u8; NATIVE_FAMILY_CAPACITY], 0),
        };
    let levels = snapshot.levels().len();
    let state = PmwsState {
        revision: snapshot.revision(),
        cursor_epoch,
        cursor_position,
        sync_divergences: snapshot.sync_divergences(),
        commit_time,
        authority_state,
        authority_reason,
        continuity_kind,
        continuity_reason,
        commit_time_present,
        publication_present,
        origin,
        derivation,
        representation,
        native_family_len,
        level_count: 0,
        level_capacity_required: levels as u32,
        native_family,
        arrival_time: snapshot.arrival_time_nanos().unwrap_or(0),
    };
    (state, levels)
}

/// Writes `snapshot` to `*out` and up to `level_capacity` levels to `levels_ptr`, the shared
/// tail of [`pmws_attach`], [`pmws_read_state`], and [`pmws_reattach`].
///
/// `*out` is always written once `out` is non-null, even when the level buffer is too
/// small: only the level array is short in that case, and `out->level_capacity_required`
/// tells the caller how large a retry needs to be.
fn respond_snapshot(
    snapshot: &BookSnapshot,
    out: *mut PmwsState,
    levels_ptr: *mut PmwsLevel,
    level_capacity: u32,
) -> i32 {
    if out.is_null() {
        return PMWS_STATUS_INVALID_ARGUMENT;
    }
    let (mut state, required) = encode_state(snapshot);
    let capacity = level_capacity as usize;
    if required > capacity {
        // SAFETY: `out` was just proven non-null, and every caller of this function
        // documents `out` as a single writable `PmwsState`.
        let _ = unsafe { write_out(out, state) };
        return PMWS_STATUS_BUFFER_TOO_SMALL;
    }
    // SAFETY: as above; every caller documents `levels_ptr` identically for `level_capacity`
    // slots.
    let Some(slice) = (unsafe { out_slice(levels_ptr, capacity) }) else {
        return PMWS_STATUS_INVALID_ARGUMENT;
    };
    for (cell, level) in slice.iter_mut().zip(snapshot.levels()) {
        *cell = PmwsLevel {
            side: side_word(level.side()),
            reserved: 0,
            price: to_decimal(level.price().value()),
            quantity: to_decimal(level.quantity().value()),
        };
    }
    state.level_count = required as u32;
    // SAFETY: as above.
    let _ = unsafe { write_out(out, state) };
    PMWS_STATUS_OK
}

/// A `PmwsEvent` with every field zeroed, so each encoder below sets only the fields its
/// own delivery kind gives meaning to and every other field is provably zero.
const ZEROED_EVENT: PmwsEvent = PmwsEvent {
    missed: 0,
    cursor_epoch: 0,
    cursor_position: 0,
    book_revision: 0,
    commit_time: 0,
    daemon_generation: 0,
    subscription_generation: 0,
    price: PmwsDecimal::ZERO,
    old_quantity: PmwsDecimal::ZERO,
    new_quantity: PmwsDecimal::ZERO,
    delivery_kind: 0,
    continuity_reason: 0,
    origin: 0,
    derivation: 0,
    representation: 0,
    native_family_len: 0,
    side: 0,
    old_present: 0,
    new_present: 0,
    native_family: [0; NATIVE_FAMILY_CAPACITY],
    winning_index: 0,
    delivery_path: 0,
    winning_outcome_len: 0,
    market_type_len: 0,
    resolution_date_len: 0,
    reserved2: 0,
    winning_outcome: [0; RESOLUTION_OUTCOME_CAPACITY],
    market_type: [0; RESOLUTION_TYPE_CAPACITY],
    resolution_date: [0; RESOLUTION_DATE_CAPACITY],
    reserved3: 0,
    arrival_time: 0,
};

fn encode_retained(event: &RetainedEvent) -> PmwsEvent {
    match event {
        RetainedEvent::Mutation(event) => encode_mutation(event),
        RetainedEvent::Resolution(event) => encode_resolution(event),
    }
}

fn encode_mutation(event: &MutationEvent) -> PmwsEvent {
    let publication = event.publication();
    let (origin, derivation) = origin_words(publication.origin());
    let (native_family, native_family_len) = pack_native_family(publication.native_family());
    let cursor = event.cursor();
    PmwsEvent {
        cursor_epoch: cursor.epoch(),
        cursor_position: cursor.position(),
        book_revision: event.revision(),
        commit_time: event.commit_time_nanos().unwrap_or(0),
        arrival_time: event.arrival_time_nanos().unwrap_or(0),
        daemon_generation: event.daemon_generation(),
        subscription_generation: event.subscription_generation(),
        price: to_decimal(event.price().value()),
        old_quantity: event
            .old_quantity()
            .map_or(PmwsDecimal::ZERO, |value| to_decimal(value.value())),
        new_quantity: event
            .new_quantity()
            .map_or(PmwsDecimal::ZERO, |value| to_decimal(value.value())),
        delivery_kind: DELIVERY_MUTATION,
        origin,
        derivation,
        representation: representation_word(publication.representation()),
        native_family_len,
        side: side_word(event.side()),
        old_present: u32::from(event.old_quantity().is_some()),
        new_present: u32::from(event.new_quantity().is_some()),
        native_family,
        ..ZEROED_EVENT
    }
}

/// Encodes one delivered resolution. Every mutation-only field stays zero: a resolution has
/// no coordinate, no quantities and no native family, and reporting one would be an
/// invention rather than a report.
fn encode_resolution(event: &ResolutionEvent) -> PmwsEvent {
    let publication = event.publication();
    let (origin, derivation) = origin_words(publication.origin());
    let (winning_outcome, winning_outcome_len) = pack_bytes(event.winning_outcome());
    let (market_type, market_type_len) = pack_bytes(event.market_type());
    let (resolution_date, resolution_date_len) = pack_bytes(event.resolution_date());
    let cursor = event.cursor();
    PmwsEvent {
        cursor_epoch: cursor.epoch(),
        cursor_position: cursor.position(),
        book_revision: event.revision(),
        commit_time: event.commit_time_nanos().unwrap_or(0),
        arrival_time: event.arrival_time_nanos().unwrap_or(0),
        daemon_generation: event.daemon_generation(),
        subscription_generation: event.subscription_generation(),
        delivery_kind: DELIVERY_RESOLUTION,
        origin,
        derivation,
        representation: representation_word(publication.representation()),
        winning_index: event.winning_index(),
        delivery_path: delivery_path_word(event.delivery_path()).unwrap_or(0),
        winning_outcome_len,
        market_type_len,
        resolution_date_len,
        winning_outcome,
        market_type,
        resolution_date,
        ..ZEROED_EVENT
    }
}

fn loss_event(reason: &ContinuityReason, cursor: &MutationCursor) -> PmwsEvent {
    PmwsEvent {
        cursor_epoch: cursor.epoch(),
        cursor_position: cursor.position(),
        delivery_kind: DELIVERY_CONTINUITY_LOST,
        continuity_reason: break_word(reason),
        ..ZEROED_EVENT
    }
}

/// Maps every [`SegmentFault`] variant explicitly, so a future variant fails to compile here
/// rather than silently returning [`PMWS_STATUS_INTERNAL`].
fn segment_fault_status(fault: SegmentFault) -> i32 {
    match fault {
        SegmentFault::RegionTooSmall { .. }
        | SegmentFault::RegionMisaligned { .. }
        | SegmentFault::HeaderUnpublished
        | SegmentFault::MagicMismatch { .. }
        | SegmentFault::AbiVersionUnsupported { .. }
        | SegmentFault::AlignmentMismatch { .. }
        | SegmentFault::RegionSizeMismatch { .. }
        | SegmentFault::CapacityOutOfRange
        | SegmentFault::FeatureUnsupported { .. }
        | SegmentFault::GeometryMismatch
        | SegmentFault::TrailerMismatch
        | SegmentFault::GeometryUnreachable => PMWS_STATUS_SEGMENT_INCOMPATIBLE,
    }
}

/// Maps every [`RegionError`] variant explicitly, for the same reason as
/// [`segment_fault_status`].
fn region_error_status(error: RegionError) -> i32 {
    match error {
        RegionError::SizeZero
        | RegionError::SizeNotBlockAligned
        | RegionError::SizeTooLarge
        | RegionError::MappingMisaligned
        | RegionError::Io(_) => PMWS_STATUS_IO,
    }
}

/// Maps every [`ReadFault`] variant explicitly, for the same reason as
/// [`segment_fault_status`]. [`ReadFault::HandleStale`], [`ReadFault::ForeignSegment`], and
/// [`ReadFault::MalformedIdentity`] are unreachable through this FFI's public surface — every
/// handle a session presents to a reader came from that same session's own resolve — and are
/// grouped with [`ReadFault::MalformedRecord`] and [`ReadFault::SlotOwnershipMismatch`] under
/// [`PMWS_STATUS_MALFORMED_RECORD`] as the general structural-integrity fallback.
fn read_fault_status(fault: ReadFault) -> i32 {
    match fault {
        ReadFault::Segment(inner) => segment_fault_status(inner),
        ReadFault::EntryUnpublished => PMWS_STATUS_MARKET_NOT_FOUND,
        ReadFault::NoPublishedState => PMWS_STATUS_NO_PUBLISHED_STATE,
        ReadFault::HandleStale
        | ReadFault::ForeignSegment
        | ReadFault::SlotOwnershipMismatch
        | ReadFault::MalformedIdentity
        | ReadFault::MalformedRecord => PMWS_STATUS_MALFORMED_RECORD,
        ReadFault::WriterStalled { .. } => PMWS_STATUS_WRITER_STALLED,
        ReadFault::Contended { .. } => PMWS_STATUS_CONTENDED,
    }
}

/// Maps every [`WaitFault`] variant explicitly, for the same reason as
/// [`segment_fault_status`]. [`WaitFault::DoorbellUnavailable`] is the one variant a caller
/// can act on: it names an absent doorbell, not a broken one, so it gets its own
/// [`PMWS_STATUS_DOORBELL_UNAVAILABLE`] rather than the generic [`PMWS_STATUS_IO`] every
/// other variant here shares as an I/O-shaped failure of the doorbell itself, distinct from
/// anything a caller can retry by adjusting `spin` or `timeout`.
fn wait_fault_status(fault: WaitFault) -> i32 {
    match fault {
        WaitFault::Segment(inner) => segment_fault_status(inner),
        WaitFault::DoorbellUnavailable(_) => PMWS_STATUS_DOORBELL_UNAVAILABLE,
        WaitFault::DoorbellPageUnreachable | WaitFault::Platform { .. } => PMWS_STATUS_IO,
    }
}

fn status_text(code: i32) -> &'static CStr {
    match code {
        PMWS_STATUS_OK => c"ok",
        PMWS_STATUS_NONE => c"none",
        PMWS_STATUS_INVALID_ARGUMENT => c"invalid argument",
        PMWS_STATUS_IO => c"io error",
        PMWS_STATUS_SEGMENT_INCOMPATIBLE => c"segment incompatible",
        PMWS_STATUS_MARKET_NOT_FOUND => c"market not found",
        PMWS_STATUS_NO_PUBLISHED_STATE => c"no published state",
        PMWS_STATUS_CONTENDED => c"contended",
        PMWS_STATUS_WRITER_STALLED => c"writer stalled",
        PMWS_STATUS_MALFORMED_RECORD => c"malformed record",
        PMWS_STATUS_BUFFER_TOO_SMALL => c"buffer too small",
        PMWS_STATUS_CONTINUITY_LOST => c"continuity lost",
        PMWS_STATUS_NOT_ATTACHED => c"not attached",
        PMWS_STATUS_INTERNAL => c"internal error",
        PMWS_STATUS_ATTACH_REFUSED => c"attachment refused by the daemon",
        PMWS_STATUS_ATTACH_INCOMPLETE => c"attachment arrived without its descriptors",
        PMWS_STATUS_DOORBELL_UNAVAILABLE => c"doorbell unavailable: spin or read unparked",
        PMWS_STATUS_FOREIGN_SEGMENT => c"market held by another shard's segment",
        _ => c"unknown status",
    }
}

/// This FFI surface's own version. Bumps only when a `#[repr(C)]` struct's layout or a
/// function's contract changes; [`pmws_abi_version`] names the segment ABI version instead.
#[unsafe(no_mangle)]
pub extern "C" fn pmws_ffi_version() -> i32 {
    guarded(PMWS_STATUS_INTERNAL, || PMWS_FFI_VERSION)
}

/// The publication segment ABI version this build reads: [`crate::ABI_VERSION`], read from
/// the layout constant rather than duplicated as a second literal.
#[unsafe(no_mangle)]
pub extern "C" fn pmws_abi_version() -> u32 {
    guarded(0, || crate::ABI_VERSION)
}

/// A `'static`, NUL-terminated, never-freed name for `code`. An unrecognized code returns
/// "unknown status" rather than a null pointer.
#[unsafe(no_mangle)]
pub extern "C" fn pmws_status_text(code: i32) -> *const c_char {
    guarded(c"internal error".as_ptr(), || status_text(code).as_ptr())
}

/// Opens the segment file at `path` (UTF-8 bytes, no NUL required) read-only, fully
/// validates its header and trailer — [`SegmentReader::attach`] — and writes a new session
/// handle to `*out`.
///
/// Fails with [`PMWS_STATUS_INVALID_ARGUMENT`] for a null or zero-length `path`, non-UTF-8
/// bytes, or a null `out`; [`PMWS_STATUS_IO`] when the file cannot be opened or mapped; and
/// [`PMWS_STATUS_SEGMENT_INCOMPATIBLE`] when the header or trailer does not validate.
/// `*out` is left untouched on every failure.
///
/// # Safety
/// `path` must be null only when `path_len == 0`, and otherwise point to `path_len` readable
/// bytes. `out` must be null or point to a single writable `PmwsSession*`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pmws_open(
    path: *const u8,
    path_len: usize,
    out: *mut *mut PmwsSession,
) -> i32 {
    guarded(PMWS_STATUS_INTERNAL, || {
        if out.is_null() {
            return PMWS_STATUS_INVALID_ARGUMENT;
        }
        // SAFETY: this call's own contract requires `path` be null only when `path_len == 0`.
        let Some(text) = (unsafe { read_required_str(path, path_len) }) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        let region = match SegmentRegion::open_file(Path::new(text)) {
            Ok(region) => Arc::new(region),
            Err(error) => return region_error_status(error),
        };
        let reader = match SegmentReader::attach(region) {
            Ok(reader) => reader,
            Err(fault) => return segment_fault_status(fault),
        };
        let session = Box::new(PmwsSession {
            reader,
            control: RefCell::new(None),
            segment: None,
            table: RefCell::new(Vec::new()),
            dirty_cursor: Cell::new(None),
            _not_sync: PhantomData,
        });
        // SAFETY: `out` was just proven non-null, and this call's own contract requires it
        // point to a single writable `PmwsSession*`.
        let _ = unsafe { write_out(out, Box::into_raw(session)) };
        PMWS_STATUS_OK
    })
}

/// Attaches to the segment carrying `market` by asking the daemon listening on the Unix
/// socket at `control_path` for it, and writes a new session handle to `*out`.
///
/// This is the attachment channel `docs/notes/shared-memory-model.md` §4.2 asks for, done
/// whole: the caller never names a segment path, never opens one, and cannot be pointed at a
/// file it was not given. One request line goes out, and one answer comes back carrying the
/// segment's own descriptor — opened read-only by the daemon when it created the file — on the
/// same message as the line. The daemon refuses a peer running as another user before it
/// discloses anything.
///
/// Exactly one descriptor arrives, whatever doorbell placement the answer names. A segment
/// whose doorbell lives in a sibling page is attached without that page, so this session can
/// read and spin but not park: its first [`pmws_wait`] with a park in it answers
/// [`PMWS_STATUS_DOORBELL_UNAVAILABLE`] — parked waiting is unavailable and the segment
/// itself is fine, exactly as for a [`pmws_open`] session whose page could not be opened —
/// rather than blocking. Parked waiting over a page-placement segment is reached by
/// [`pmws_open`], which opens the page by name as the same user. [`Attachment::descriptors`]
/// records why the page's descriptor does not cross.
///
/// **The attachment leases the market**, and the session holds the control connection open
/// for as long as it lives to keep that lease. A market no other consumer holds and no
/// operator pinned is subscribed at the venue to serve this call; [`pmws_close`], and this
/// process exiting however it exits, releases it. Leases are counted per connection, so a
/// second consumer of the same market costs no venue traffic and keeps the market alive after
/// the first one leaves, and an operator's pin outlives every lease. Against a daemon
/// configured with a lease TTL, [`pmws_renew`] is what a consumer with nothing else to say
/// says.
///
/// The daemon answers as soon as it holds the market, which may be before the venue has said
/// anything about it: what the session reads then is a book reported as synchronizing, exactly
/// as any quiet market's is, until its first venue base lands.
///
/// The session it produces is exactly the one [`pmws_open`] produces: it covers every market
/// in that shard's segment, not only the one asked for, and [`pmws_resolve`],
/// [`pmws_attach`], [`pmws_wait`] and [`pmws_next_dirty`] work on it identically.
///
/// Validation follows §4.4 in order: the mapping is read-only, the header and trailer are
/// validated before any directory entry is touched ([`SegmentReader::attach_with_doorbell`]),
/// and only then is the header's `daemon_instance_id`, `segment_generation` and doorbell
/// placement checked against what the answer promised — a mismatch means the descriptor names
/// a segment other than the one described and is [`PMWS_STATUS_SEGMENT_INCOMPATIBLE`], never
/// a read.
///
/// This call blocks the calling thread for the length of one control conversation, bounded by
/// one five-second deadline over the exchange rather than per step, so a peer that answers
/// slowly cannot extend it. The deadline runs from the established connection; connecting to
/// the socket itself is the one step it does not cover ([`ATTACH_TIMEOUT`]).
///
/// Fails with [`PMWS_STATUS_INVALID_ARGUMENT`] for a null or zero-length input, non-UTF-8
/// bytes, a null `out`, or a market the daemon calls an invalid identifier;
/// [`PMWS_STATUS_IO`] when the socket cannot be reached, the conversation fails, or the answer
/// is not one this build can read; [`PMWS_STATUS_MARKET_NOT_FOUND`] when the daemon answers
/// that it does not hold the market; [`PMWS_STATUS_ATTACH_REFUSED`] when the daemon refused
/// the request — no room to take the market, a peer running as another user, or a shard with
/// no delivery segment;
/// [`PMWS_STATUS_ATTACH_INCOMPLETE`] when the descriptors did not arrive as the answer
/// promised, including a truncated ancillary buffer; and
/// [`PMWS_STATUS_SEGMENT_INCOMPATIBLE`] when the segment does not validate or is not the one
/// promised. `*out` is left untouched on every failure.
///
/// # Safety
/// `control_path` and `market` must each be null only when their paired length is `0`, and
/// otherwise point to that many readable bytes. `out` must be null or point to a single
/// writable `PmwsSession*`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pmws_connect(
    control_path: *const u8,
    path_len: usize,
    market: *const u8,
    market_len: usize,
    out: *mut *mut PmwsSession,
) -> i32 {
    guarded(PMWS_STATUS_INTERNAL, || {
        if out.is_null() {
            return PMWS_STATUS_INVALID_ARGUMENT;
        }
        // SAFETY: this call's own contract requires each pointer be null only when its paired
        // length is zero.
        let (Some(path), Some(market)) = (unsafe {
            (
                read_required_str(control_path, path_len),
                read_required_str(market, market_len),
            )
        }) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        let session = match connect_session(path, market) {
            Ok(session) => Box::new(session),
            Err(status) => return status,
        };
        // SAFETY: `out` was just proven non-null, and this call's own contract requires it
        // point to a single writable `PmwsSession*`.
        let _ = unsafe { write_out(out, Box::into_raw(session)) };
        PMWS_STATUS_OK
    })
}

/// The whole attachment conversation: connect, ask, receive, validate, build the session.
///
/// One deadline covers the conversation rather than each step of it. A per-operation timeout
/// bounds nothing a peer controls the pace of: a daemon — or anything else listening on that
/// path — that sends one byte just inside the limit and repeats resets the clock every time,
/// which turns a five-second bound into hours of a consumer's own thread. The remaining budget
/// is therefore recomputed and installed before every blocking send and receive, and an
/// exhausted budget is the same [`PMWS_STATUS_IO`] an expired one is.
///
/// The `connect` is the exception and is deliberately outside it: the standard library has no
/// timed connect for a Unix domain socket, so bounding it would mean a non-blocking socket and
/// a hand-rolled readiness wait for one syscall against a path the caller named itself. The
/// deadline is therefore taken *after* the connection is established, so that the budget the
/// documentation promises the exchange is the budget the exchange gets, whole, rather than
/// whatever an unbounded step left of it. It is recorded here rather than papered over.
fn connect_session(control_path: &str, market: &str) -> Result<PmwsSession, i32> {
    let request = encode_request(&ControlRequest::Attach {
        market: market.to_owned(),
    })
    .map_err(ConversationError::status)?;
    let mut socket = UnixStream::connect(Path::new(control_path)).map_err(|_| PMWS_STATUS_IO)?;
    let deadline = Instant::now() + ATTACH_TIMEOUT;
    let (attachment, descriptors) = attach_conversation(&mut socket, request.as_str(), deadline)
        .map_err(ConversationError::status)?;
    session_from_attachment(&attachment, descriptors, socket)
}

/// Why a control conversation on an established session did not succeed, and what that leaves
/// the connection in.
///
/// The two are not the same event and must not be answered the same way. A daemon that
/// *answered* — a refusal in the protocol's own per-market vocabulary, or an error for a
/// request it will not serve — has left the connection exactly as it found it: one request,
/// one answer, and the next conversation on it pairs up. A conversation that did not finish
/// has not: an answer may still be owed on that socket, and whatever demand the request took
/// cannot be shown to be gone. The first keeps the connection; the second ends it.
enum ConversationError {
    /// The daemon answered and the answer was a refusal. The connection is unharmed.
    Refused(i32),
    /// The conversation did not complete — a timeout, an I/O failure, an answer this build
    /// cannot read or did not ask for, or a rollback left unconfirmed. The connection cannot
    /// be trusted with another request.
    Broken(i32),
}

impl ConversationError {
    /// The status this failure reports, for the one caller with no connection to keep:
    /// [`connect_session`], whose socket is a local until the session is built around it.
    fn status(self) -> i32 {
        match self {
            Self::Refused(status) | Self::Broken(status) => status,
        }
    }
}

/// Reports one failed conversation, dropping the session's control connection when the
/// failure means this side can no longer trust it.
///
/// Dropping it is the whole of the no-residual-demand contract. A closed control connection
/// releases every lease the session held at the daemon — that is the daemon's own session
/// path, it needs no lease TTL to run, and it works against a daemon that does not understand
/// a release request at all. It is also what keeps the protocol honest: an answer still owed
/// on a kept socket becomes the *next* request's answer and displaces every pairing after it.
///
/// The mapping is untouched, exactly as [`pmws_close`] leaves it, so the segment goes on
/// reading a book the daemon may have stopped maintaining. Every later conversation on the
/// session answers [`PMWS_STATUS_INVALID_ARGUMENT`], as a [`pmws_open`] session's does.
fn report_failure(control: &mut Option<UnixStream>, failure: ConversationError) -> i32 {
    match failure {
        ConversationError::Refused(status) => status,
        ConversationError::Broken(status) => {
            let _dropped = control.take();
            status
        }
    }
}

/// Encodes one control request, refusing a line this protocol could not carry.
///
/// Encoding happens before the socket does anything, so an input this side already knows is
/// unusable is a caller error rather than a failed conversation.
fn encode_request(request: &ControlRequest) -> Result<String, ConversationError> {
    let line = crate::encode_line(request)
        .map_err(|_| ConversationError::Refused(PMWS_STATUS_INTERNAL))?;
    if line.len() > MAX_CONTROL_LINE_BYTES {
        return Err(ConversationError::Refused(PMWS_STATUS_INVALID_ARGUMENT));
    }
    Ok(line)
}

/// The attaching half of a control conversation: `request` out, one answer line and whatever
/// descriptors rode it back, under `deadline`.
///
/// Shared by [`connect_session`], which builds a mapping out of what this returns, and
/// [`lease_market`], which already has the mapping and keeps only the lease. The refusal
/// vocabulary therefore lives in one place: both turn the same daemon answer into the same
/// status.
///
/// A refusal — a per-market outcome, an error, or a busy queue — is an answered conversation
/// and leaves the connection usable; only a failed exchange or an answer to some other
/// request is [`ConversationError::Broken`].
fn attach_conversation(
    socket: &mut UnixStream,
    request: &str,
    deadline: Instant,
) -> Result<(Attachment, Vec<OwnedFd>), ConversationError> {
    write_request(socket, request.as_bytes(), deadline).map_err(ConversationError::Broken)?;
    let (line, descriptors) =
        read_attachment(socket, deadline).map_err(ConversationError::Broken)?;
    match serde_json::from_str::<ControlResponse>(line.trim_end()) {
        Ok(ControlResponse::Attached { attachment }) => Ok((attachment, descriptors)),
        Ok(ControlResponse::Markets { markets }) => Err(ConversationError::Refused(
            refusal_status(markets.as_slice()),
        )),
        Ok(ControlResponse::Error { .. } | ControlResponse::Busy { .. }) => {
            Err(ConversationError::Refused(PMWS_STATUS_ATTACH_REFUSED))
        }
        Ok(
            ControlResponse::Status { .. }
            | ControlResponse::Renewed { .. }
            | ControlResponse::Released { .. },
        )
        | Err(_) => Err(ConversationError::Broken(PMWS_STATUS_IO)),
    }
}

/// What is left of `deadline`, or [`PMWS_STATUS_IO`] once nothing is.
///
/// Never zero: a zero socket timeout means *no* timeout to the platform, so an exhausted
/// budget must never be installed as one. An expired deadline is the same answer an expired
/// read is, because to a caller they are the same event — the conversation did not finish
/// inside its bound.
fn remaining(deadline: Instant) -> Result<Duration, i32> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|left| !left.is_zero())
        .ok_or(PMWS_STATUS_IO)
}

/// Sends the request line under `deadline`, a chunk at a time.
///
/// A send timeout is per syscall, exactly as a receive timeout is, so a partial write followed
/// by a peer that reads slowly would reset the bound if the timeout were installed once around
/// `write_all`. The request is one short line and almost always one syscall; this is here so
/// the bound holds when it is not.
fn write_request(socket: &mut UnixStream, request: &[u8], deadline: Instant) -> Result<(), i32> {
    let mut sent = 0;
    while sent < request.len() {
        socket
            .set_write_timeout(Some(remaining(deadline)?))
            .map_err(|_| PMWS_STATUS_IO)?;
        match socket.write(&request[sent..]) {
            Ok(0) | Err(_) => return Err(PMWS_STATUS_IO),
            Ok(written) => sent += written,
        }
    }
    Ok(())
}

/// Reads the answer line, taking the descriptors that ride the first message of it.
///
/// The daemon sends line and descriptors as one `sendmsg`, so the first receive is the one
/// that carries the transfer; anything left of the line is read ordinarily. Bounded by
/// [`MAX_CONTROL_LINE_BYTES`], which is the same bound the daemon reads a request under, and
/// by `deadline` in time: every receive below installs what remains of it, so a peer that
/// dribbles bytes cannot extend this past the one bound [`pmws_connect`] documents.
fn read_attachment(
    socket: &mut UnixStream,
    deadline: Instant,
) -> Result<(String, Vec<OwnedFd>), i32> {
    let mut buffer = vec![0_u8; MAX_CONTROL_LINE_BYTES];
    socket
        .set_read_timeout(Some(remaining(deadline)?))
        .map_err(|_| PMWS_STATUS_IO)?;
    let (mut filled, descriptors) = recv_with_fds(
        socket.as_fd(),
        buffer.as_mut_slice(),
        MAX_TRANSFERRED_DESCRIPTORS,
    )
    .map_err(|error| match error {
        ChannelError::ControlTruncated => PMWS_STATUS_ATTACH_INCOMPLETE,
        ChannelError::Io(_) => PMWS_STATUS_IO,
    })?;
    while filled < buffer.len() && !buffer[..filled].contains(&b'\n') {
        socket
            .set_read_timeout(Some(remaining(deadline)?))
            .map_err(|_| PMWS_STATUS_IO)?;
        match socket.read(&mut buffer[filled..]) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(_) => return Err(PMWS_STATUS_IO),
        }
    }
    let end = buffer[..filled]
        .iter()
        .position(|byte| *byte == b'\n')
        .ok_or(PMWS_STATUS_IO)?;
    let line = core::str::from_utf8(&buffer[..end])
        .map_err(|_| PMWS_STATUS_IO)?
        .to_owned();
    Ok((line, descriptors))
}

/// Turns one refusal answer into the status that names it.
///
/// The daemon answers an attach it will not serve in the same per-market vocabulary the rest
/// of the protocol uses, so this is where that vocabulary becomes a C status.
fn refusal_status(markets: &[MarketOutcome]) -> i32 {
    match markets.first().map(|outcome| &outcome.status) {
        Some(&MarketStatus::Rejected(MarketRejection::InvalidIdentifier)) => {
            PMWS_STATUS_INVALID_ARGUMENT
        }
        Some(&MarketStatus::Removed) => PMWS_STATUS_MARKET_NOT_FOUND,
        _ => PMWS_STATUS_ATTACH_REFUSED,
    }
}

/// Validates a received transfer against what its answer promised, and maps it.
///
/// The coherence rule is the answer's own and does not vary with the doorbell placement: an
/// accepted attach carries exactly the segment's descriptor, and any other count — including
/// a second descriptor a daemon of some other vintage attached — is a promise the transfer did
/// not keep, refused rather than partly adopted.
///
/// A [`crate::DoorbellLocation::Page`] segment therefore attaches with no page at all. That is the
/// same state a reader whose page failed to open reaches: it reads and spins normally, and its
/// first park answers [`WaitFault::DoorbellUnavailable`] rather than blocking or lying.
///
/// Every other promise in the answer is checked against the *validated header* rather than
/// believed: the daemon instance, the segment generation, and the doorbell placement, which
/// tells a caller whether this attachment can be parked on at all and would otherwise be the
/// one claim in the answer nothing refutes. All three are the same class of failure — the
/// header describes a segment other than the one the answer described — so all three are
/// [`PMWS_STATUS_SEGMENT_INCOMPATIBLE`], not the descriptor-shaped
/// [`PMWS_STATUS_ATTACH_INCOMPLETE`]: the transfer arrived intact and it is its *content* that
/// does not match.
fn session_from_attachment(
    attachment: &Attachment,
    descriptors: Vec<OwnedFd>,
    control: UnixStream,
) -> Result<PmwsSession, i32> {
    if attachment.descriptors != 1 || descriptors.len() != 1 {
        return Err(PMWS_STATUS_ATTACH_INCOMPLETE);
    }
    let promised = u128::from_str_radix(attachment.instance_id.as_str(), 16)
        .map_err(|_| PMWS_STATUS_ATTACH_INCOMPLETE)?;
    let mut descriptors = descriptors.into_iter();
    let segment = descriptors.next().ok_or(PMWS_STATUS_ATTACH_INCOMPLETE)?;
    let region = SegmentRegion::open_read_only_from_fd(segment).map_err(region_error_status)?;
    let reader = SegmentReader::attach_with_doorbell(Arc::new(region), None)
        .map_err(segment_fault_status)?;
    let geometry = reader.geometry();
    let declared = match attachment.doorbell {
        DoorbellLocation::InHeader => FEATURE_DOORBELL_IN_HEADER,
        DoorbellLocation::Page => FEATURE_DOORBELL_PAGE,
    };
    if geometry.daemon_instance_id() != promised
        || geometry.segment_generation() != attachment.segment_generation
        || reader.doorbell_feature_bit() != declared
    {
        return Err(PMWS_STATUS_SEGMENT_INCOMPATIBLE);
    }
    Ok(PmwsSession {
        reader,
        control: RefCell::new(Some(control)),
        segment: Some(attachment.segment.clone()),
        table: RefCell::new(Vec::new()),
        dirty_cursor: Cell::new(None),
        _not_sync: PhantomData,
    })
}

/// Releases a session opened by [`pmws_open`] or [`pmws_connect`]. `session == NULL` is a
/// no-op. This is the only heap allocation this FFI ever hands a caller; every other output
/// crosses through a caller-owned buffer.
///
/// For a session from [`pmws_connect`] this also closes the control connection it attached
/// over, which releases the market lease the daemon granted that connection. A consumer that
/// wants the market to stay subscribed must keep the session, not the mapping: the mapping
/// outlives the lease and goes on reading a book that stops being maintained.
///
/// # Safety
/// `session` must be null or a pointer [`pmws_open`] or [`pmws_connect`] returned that has
/// not already been passed to this function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pmws_close(session: *mut PmwsSession) {
    guarded((), || {
        if session.is_null() {
            return;
        }
        // SAFETY: this call's own contract requires `session` be a live, not-yet-closed
        // `pmws_open` pointer, which is exactly what `Box::from_raw` needs to reclaim it.
        drop(unsafe { Box::from_raw(session) });
    });
}

/// Renews this session's market leases at the daemon that granted them.
///
/// One line out and one line back on the control connection [`pmws_connect`] opened, bounded
/// by the same [`ATTACH_TIMEOUT`] budget the attach conversation is. It carries no market
/// data and moves no cursor; a caller that never sends one is fine unless its daemon is
/// configured with a lease TTL, which is what this exists for. Any request on the connection
/// renews it, so this is the request for a consumer that has nothing else to say.
///
/// The cadence is the caller's. The daemon declares its TTL in the attach answer
/// ([`crate::Attachment::lease_ttl_ms`]) and this ABI does not surface that number, so a
/// caller that does not know its daemon's configuration renews well inside the shortest TTL
/// an operator would set — a fraction of `pm_ws::daemon::MIN_LEASE_TTL_MS` is always safe,
/// and a renewal costs one line on an otherwise silent connection.
///
/// Fails with [`PMWS_STATUS_INVALID_ARGUMENT`] for a null session or one opened from a path
/// by [`pmws_open`] — that session holds no lease, because nothing granted it one — and with
/// [`PMWS_STATUS_IO`] when the conversation does not finish inside its bound or the daemon
/// answers something other than a renewal. An I/O failure here means the control connection
/// is gone, and with it this session's leases: the connection is dropped rather than kept,
/// because an answer still owed on it would be read as the next conversation's, and because a
/// closed connection is what releases the leases at a daemon with no TTL to expire them. The
/// mapping stays readable, and what it will report is a book the daemon has stopped
/// maintaining; every later [`pmws_renew`], [`pmws_lease`] and [`pmws_release`] on this
/// session answers [`PMWS_STATUS_INVALID_ARGUMENT`], as they do for a [`pmws_open`] one.
///
/// # Safety
/// `session` must be null or a live [`pmws_open`] or [`pmws_connect`] pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pmws_renew(session: *const PmwsSession) -> i32 {
    guarded(PMWS_STATUS_INTERNAL, || {
        // SAFETY: this call's own contract requires `session` be null or a live pointer.
        let Some(session) = (unsafe { session_ref(session) }) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        let mut control = session.control.borrow_mut();
        let Some(socket) = control.as_mut() else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        let outcome = renew_leases(socket);
        match outcome {
            Ok(()) => PMWS_STATUS_OK,
            Err(failure) => report_failure(&mut control, failure),
        }
    })
}

/// The renewal conversation: one request line out, one answer line back, under one deadline.
///
/// The answer is read as the protocol's own [`ControlResponse`] rather than matched as text,
/// so a daemon that answered something else — an error, a busy queue, an answer to some other
/// request — is a failed renewal rather than a silent success.
fn renew_leases(socket: &mut UnixStream) -> Result<(), ConversationError> {
    let request = encode_request(&ControlRequest::Renew)?;
    let deadline = Instant::now() + ATTACH_TIMEOUT;
    write_request(socket, request.as_bytes(), deadline).map_err(ConversationError::Broken)?;
    let line = read_answer(socket, deadline).map_err(ConversationError::Broken)?;
    match serde_json::from_str::<ControlResponse>(line.as_str()) {
        Ok(ControlResponse::Renewed { .. }) => Ok(()),
        Ok(_) | Err(_) => Err(ConversationError::Broken(PMWS_STATUS_IO)),
    }
}

/// Gives up this session's lease on `market`, without closing the session and without
/// touching any other lease it holds.
///
/// One line out and one line back on the control connection [`pmws_connect`] opened, bounded
/// by the same [`ATTACH_TIMEOUT`] budget over the whole exchange that the attach conversation
/// is. `market` is UTF-8 bytes, no NUL required, and the daemon validates it exactly as it
/// validates an attach's.
///
/// Idempotent: releasing a market this session never leased, or releasing one a second time,
/// is [`PMWS_STATUS_OK`]. The daemon answers with the number of leases the session holds
/// afterwards, which this ABI does not surface — a caller that needs the count keeps it.
///
/// **The mapping is untouched.** Releasing the very market this session connected with is
/// legal, and is what this call is for: the segment stays mapped and every market in it stays
/// readable, the released one included. That is the doctrine [`pmws_close`] already states
/// from the other side — the mapping outlives the lease and goes on reading a book that stops
/// being maintained once nothing else holds it. Nothing here unmaps, and no descriptor is
/// opened or closed.
///
/// Fails with [`PMWS_STATUS_INVALID_ARGUMENT`] for a null session, a session opened from a
/// path by [`pmws_open`] — which holds no lease, because nothing granted it one — a null,
/// zero-length or non-UTF-8 `market`, and a market the daemon calls an invalid identifier;
/// and with [`PMWS_STATUS_IO`] when the conversation does not finish inside its bound or the
/// daemon answers something other than a release. An I/O failure here means the control
/// connection is gone, and with it every lease this session held: the connection is dropped
/// rather than kept, for the reasons [`pmws_renew`] gives, and every later conversation on
/// this session answers [`PMWS_STATUS_INVALID_ARGUMENT`]. A refusal is not that — an
/// identifier the daemon rejects is a conversation that finished, and the connection carries
/// on. The mapping stays readable either way, and what it will report is a book the daemon may
/// have stopped maintaining.
///
/// # Safety
/// `session` must be null or a live [`pmws_open`] or [`pmws_connect`] pointer. `market` must
/// be null only when `market_len == 0`, and otherwise point to that many readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pmws_release(
    session: *const PmwsSession,
    market: *const u8,
    market_len: usize,
) -> i32 {
    guarded(PMWS_STATUS_INTERNAL, || {
        // SAFETY: this call's own contract requires `session` be null or a live pointer.
        let Some(session) = (unsafe { session_ref(session) }) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        // SAFETY: this call's own contract requires `market` be null only when
        // `market_len == 0`.
        let Some(market) = (unsafe { read_required_str(market, market_len) }) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        let mut control = session.control.borrow_mut();
        let Some(socket) = control.as_mut() else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        let outcome = release_lease(socket, market, Instant::now() + ATTACH_TIMEOUT);
        match outcome {
            Ok(()) => PMWS_STATUS_OK,
            Err(failure) => report_failure(&mut control, failure),
        }
    })
}

/// The release conversation: one request line out, one answer line back, under `deadline`.
///
/// The deadline is the caller's rather than this function's own, because [`lease_market`]'s
/// rollback shares the budget of the attach it undoes: a fresh one there would let a single
/// [`pmws_lease`] block for twice the bound its documentation states.
///
/// The answer is read even by a caller that will discard the outcome — that rollback is one —
/// because an answer left unread stays in the socket and becomes the *next* conversation's
/// answer, which would misread every request after it on this connection.
///
/// A per-market refusal is mapped exactly as an attach's is ([`refusal_status`]): an
/// identifier no shard would accept is a caller error rather than a release that happened,
/// and it is an answered conversation, so the connection survives it.
fn release_lease(
    socket: &mut UnixStream,
    market: &str,
    deadline: Instant,
) -> Result<(), ConversationError> {
    let request = encode_request(&ControlRequest::Release {
        market: market.to_owned(),
    })?;
    write_request(socket, request.as_bytes(), deadline).map_err(ConversationError::Broken)?;
    let line = read_answer(socket, deadline).map_err(ConversationError::Broken)?;
    match serde_json::from_str::<ControlResponse>(line.as_str()) {
        Ok(ControlResponse::Released { .. }) => Ok(()),
        Ok(ControlResponse::Markets { markets }) => Err(ConversationError::Refused(
            refusal_status(markets.as_slice()),
        )),
        Ok(_) | Err(_) => Err(ConversationError::Broken(PMWS_STATUS_IO)),
    }
}

/// Takes a further market lease on the control connection this session already holds.
///
/// One attach line out and one attachment back, bounded by the same [`ATTACH_TIMEOUT`] budget
/// over the whole exchange that [`pmws_connect`]'s conversation is. A session may hold any
/// number of leases this way; each is given back by [`pmws_release`], and all of them by
/// [`pmws_close`] or by this process exiting however it exits. `market` is UTF-8 bytes, no
/// NUL required.
///
/// It maps nothing and returns no handle. The answer carries a descriptor for the segment
/// that holds `market`, which — for a market in this session's own segment — is a duplicate of
/// the one already mapped: it is closed as this call returns, on every path, by
/// [`OwnedFd`]'s own drop, so no descriptor leaks whatever the answer carried. The descriptor
/// *count* is deliberately not enforced the way [`pmws_connect`] enforces it, because nothing
/// is built from it here: refusing an already-granted lease over the shape of a transfer this
/// call discards would orphan that lease at the daemon rather than protect anything.
///
/// **This session's own segment only.** A market another shard holds attaches to another
/// segment, which this session has no mapping for and could not read through the one it has.
/// Such an answer is refused with [`PMWS_STATUS_FOREIGN_SEGMENT`], and the lease the daemon
/// just granted is handed back on the same connection first, so a refused lease leaves no
/// demand behind. The rollback runs under this call's own deadline rather than opening a
/// second one: attach and the release that undoes it share a single [`ATTACH_TIMEOUT`]
/// budget, so the worst case is one bound and not two.
///
/// [`PMWS_STATUS_FOREIGN_SEGMENT`] is reported only when that rollback is *confirmed*. A
/// rollback the daemon refuses, never answers, or that runs out of the shared budget leaves
/// demand this side cannot account for, and is [`PMWS_STATUS_IO`] instead: the control
/// connection is dropped, which is what makes the daemon give the lease back — including a
/// daemon that does not understand a release request at all. A consumer that wants such a
/// market opens a second session on it with [`pmws_connect`].
///
/// Fails with [`PMWS_STATUS_INVALID_ARGUMENT`] for a null session, a session opened from a
/// path by [`pmws_open`] — which has no control connection to take a lease on — a null,
/// zero-length or non-UTF-8 `market`, and a market the daemon calls an invalid identifier;
/// [`PMWS_STATUS_MARKET_NOT_FOUND`] when the daemon answers that it does not hold the market;
/// [`PMWS_STATUS_ATTACH_REFUSED`] when the daemon refused the request — no room to take the
/// market, a peer running as another user, or a shard with no delivery segment;
/// [`PMWS_STATUS_FOREIGN_SEGMENT`] when the market lives in another shard's segment and the
/// lease was confirmed given back; [`PMWS_STATUS_ATTACH_INCOMPLETE`] when the answer's
/// ancillary buffer was truncated; and [`PMWS_STATUS_IO`] when the conversation does not
/// finish inside its bound, the daemon answers something other than an attachment, or a
/// rollback goes unconfirmed.
///
/// The failures that end the conversation end this session's control connection with it:
/// [`PMWS_STATUS_IO`] and the [`PMWS_STATUS_ATTACH_INCOMPLETE`] a truncated transfer
/// produces. The refusals — [`PMWS_STATUS_INVALID_ARGUMENT`],
/// [`PMWS_STATUS_MARKET_NOT_FOUND`], [`PMWS_STATUS_ATTACH_REFUSED`] and
/// [`PMWS_STATUS_FOREIGN_SEGMENT`] — are conversations that finished, and leave it usable.
///
/// # Safety
/// `session` must be null or a live [`pmws_open`] or [`pmws_connect`] pointer. `market` must
/// be null only when `market_len == 0`, and otherwise point to that many readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pmws_lease(
    session: *const PmwsSession,
    market: *const u8,
    market_len: usize,
) -> i32 {
    guarded(PMWS_STATUS_INTERNAL, || {
        // SAFETY: this call's own contract requires `session` be null or a live pointer.
        let Some(session) = (unsafe { session_ref(session) }) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        // SAFETY: this call's own contract requires `market` be null only when
        // `market_len == 0`.
        let Some(market) = (unsafe { read_required_str(market, market_len) }) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        let mut control = session.control.borrow_mut();
        let Some(socket) = control.as_mut() else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        let outcome = lease_market(session, socket, market);
        match outcome {
            Ok(()) => PMWS_STATUS_OK,
            Err(failure) => report_failure(&mut control, failure),
        }
    })
}

/// The further-lease conversation: attach, check the segment the answer names, and either
/// drop the duplicate descriptors or give the lease straight back.
///
/// One deadline covers both halves, so the rollback spends what the attach left rather than
/// starting a second budget. A rollback that does not come back confirmed is
/// [`ConversationError::Broken`] and not the refusal a confirmed one is: the lease this call
/// took is then demand nothing on this side can account for, and ending the connection is the
/// only thing that gives it back.
fn lease_market(
    session: &PmwsSession,
    socket: &mut UnixStream,
    market: &str,
) -> Result<(), ConversationError> {
    let request = encode_request(&ControlRequest::Attach {
        market: market.to_owned(),
    })?;
    let deadline = Instant::now() + ATTACH_TIMEOUT;
    let (attachment, descriptors) = attach_conversation(socket, request.as_str(), deadline)?;
    drop(descriptors);
    if names_this_segment(session, &attachment) {
        return Ok(());
    }
    match release_lease(socket, market, deadline) {
        Ok(()) => Err(ConversationError::Refused(PMWS_STATUS_FOREIGN_SEGMENT)),
        Err(_) => Err(ConversationError::Broken(PMWS_STATUS_IO)),
    }
}

/// Whether `attachment` names the very segment this session validated and mapped.
///
/// The instance and generation are taken from the *validated header* rather than from what
/// the attaching answer claimed, so the comparison is against the segment itself. The name is
/// the field that actually discriminates: two shards of one daemon share an instance
/// identity, and a freshly started pair share a segment generation too, so a check that
/// compared only those two would call every shard's segment this one.
fn names_this_segment(session: &PmwsSession, attachment: &Attachment) -> bool {
    let geometry = session.reader.geometry();
    session.segment.as_deref() == Some(attachment.segment.as_str())
        && u128::from_str_radix(attachment.instance_id.as_str(), 16)
            .is_ok_and(|promised| promised == geometry.daemon_instance_id())
        && attachment.segment_generation == geometry.segment_generation()
}

/// Reads one answer line under `deadline`, for the conversations that carry no descriptors.
///
/// Bounded exactly as [`read_attachment`] is — [`MAX_CONTROL_LINE_BYTES`] of bytes and
/// `deadline` of time, reinstalled before every receive — so a daemon that dribbles bytes
/// cannot extend this past the one bound its caller documents.
fn read_answer(socket: &mut UnixStream, deadline: Instant) -> Result<String, i32> {
    let mut buffer = vec![0_u8; MAX_CONTROL_LINE_BYTES];
    let mut filled = 0;
    while filled < buffer.len() && !buffer[..filled].contains(&b'\n') {
        socket
            .set_read_timeout(Some(remaining(deadline)?))
            .map_err(|_| PMWS_STATUS_IO)?;
        match socket.read(&mut buffer[filled..]) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(_) => return Err(PMWS_STATUS_IO),
        }
    }
    let end = buffer[..filled]
        .iter()
        .position(|byte| *byte == b'\n')
        .ok_or(PMWS_STATUS_IO)?;
    Ok(core::str::from_utf8(&buffer[..end])
        .map_err(|_| PMWS_STATUS_IO)?
        .to_owned())
}

/// Writes this session's segment geometry and current publication generation to `*out`.
///
/// Fails with [`PMWS_STATUS_INVALID_ARGUMENT`] for a null `session` or `out`.
///
/// # Safety
/// `session` must be null or a live [`pmws_open`] pointer. `out` must be null or point to a
/// single writable `PmwsSegmentInfo`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pmws_segment_info(
    session: *const PmwsSession,
    out: *mut PmwsSegmentInfo,
) -> i32 {
    guarded(PMWS_STATUS_INTERNAL, || {
        if out.is_null() {
            return PMWS_STATUS_INVALID_ARGUMENT;
        }
        // SAFETY: this call's own contract requires `session` be null or a live session.
        let Some(session) = (unsafe { session_ref(session) }) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        let geometry = session.reader.geometry();
        let layout = geometry.layout();
        let instance = geometry.daemon_instance_id();
        let info = PmwsSegmentInfo {
            instance_id_low: instance as u64,
            instance_id_high: (instance >> 64) as u64,
            segment_generation: geometry.segment_generation(),
            publication_generation: session.reader.publication_generation(),
            directory_capacity: layout.directory_capacity(),
            state_slot_capacity: layout.state_slot_capacity(),
            level_capacity: layout.level_capacity(),
            event_capacity: layout.event_capacity(),
        };
        // SAFETY: `out` was just proven non-null, and this call's own contract requires it
        // point to a single writable `PmwsSegmentInfo`.
        let _ = unsafe { write_out(out, info) };
        PMWS_STATUS_OK
    })
}

/// Resolves a venue-native identity — `venue`, `kind`, `key`, each UTF-8 bytes with no NUL
/// required — to a dense, session-local index and writes it to `*out_market`.
///
/// The index is a process-local, session-local routing aid: it names a row in this
/// session's own table, never an identifier any other process or session shares, and
/// resolving the same identity twice in one session returns the same index without
/// disturbing any stream already attached at it. Public identity stays the venue-native
/// strings, retrievable again with [`pmws_market_identity`].
///
/// Fails with [`PMWS_STATUS_INVALID_ARGUMENT`] for a null `session` or `out_market`, a null,
/// zero-length, or non-UTF-8 identity string, or one this ABI's identity types refuse (too
/// long, or an invalid identifier-kind length), and [`PMWS_STATUS_MARKET_NOT_FOUND`] when
/// the segment's directory carries no entry for this identity.
///
/// # Safety
/// `session` must be null or a live [`pmws_open`] pointer. `venue`, `kind`, and `key` must
/// each be null only when their paired length is `0`, and otherwise point to that many
/// readable bytes. `out_market` must be null or point to a single writable `uint32_t`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pmws_resolve(
    session: *mut PmwsSession,
    venue: *const u8,
    venue_len: usize,
    kind: *const u8,
    kind_len: usize,
    key: *const u8,
    key_len: usize,
    out_market: *mut u32,
) -> i32 {
    guarded(PMWS_STATUS_INTERNAL, || {
        if out_market.is_null() {
            return PMWS_STATUS_INVALID_ARGUMENT;
        }
        // SAFETY: this call's own contract requires `session` be null or a live session.
        let Some(session) = (unsafe { session_ref(session) }) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        // SAFETY: this call's own contract requires each identity pointer be null only when
        // its paired length is zero, and otherwise point to that many readable bytes.
        let Some(text) = (unsafe { read_required_str(venue, venue_len) }) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        let Ok(venue) = Venue::new(text) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        // SAFETY: as above.
        let Some(text) = (unsafe { read_required_str(kind, kind_len) }) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        let Ok(kind) = NativeIdentifierKind::new(text, 128) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        // SAFETY: as above.
        let Some(text) = (unsafe { read_required_str(key, key_len) }) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        let Ok(key) = NativeMarketKey::new(kind, text) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        let market = MarketRef::new(venue, key);
        let Some(handle) = session.reader.resolve(&market) else {
            return PMWS_STATUS_MARKET_NOT_FOUND;
        };
        let mut table = session.table.borrow_mut();
        let index = match table.iter().position(|entry| entry.handle == handle) {
            Some(index) => index,
            None => {
                table.push(Entry {
                    market,
                    handle,
                    stream: None,
                });
                table.len() - 1
            }
        };
        drop(table);
        // SAFETY: `out_market` was just proven non-null, and this call's own contract
        // requires it point to a single writable `uint32_t`.
        let _ = unsafe { write_out(out_market, index as u32) };
        PMWS_STATUS_OK
    })
}

/// Attaches to book `market`'s latest state and its mutation stream as one coherent step —
/// [`SegmentReader::attach_stream`] — writing state to `*out` and up to `level_capacity`
/// levels to `levels`.
///
/// Replaces any event-stream cursor this session already held for `market` — a fresh
/// attachment, never a resume — but only when this call returns [`PMWS_STATUS_OK`]. `*out` is
/// filled whenever the underlying read succeeded, even when `levels` was too small to carry
/// every level — compare `out->level_count` against `out->level_capacity_required` and retry
/// with a larger buffer — but a failed call, [`PMWS_STATUS_BUFFER_TOO_SMALL`] included, leaves
/// this session's attachment for `market` exactly as it was before the call: the native
/// cursor is replaced if and only if this call returns [`PMWS_STATUS_OK`], so a caller's retry
/// loop can never leave a half-attachment behind.
///
/// Fails with [`PMWS_STATUS_INVALID_ARGUMENT`] for a null `session` or `out`, or a `market`
/// this session has not resolved; [`PMWS_STATUS_BUFFER_TOO_SMALL`] when `level_capacity`
/// falls short of `out->level_capacity_required`; and the read faults
/// [`SegmentReader::attach_stream`] documents.
///
/// # Safety
/// `session` must be null or a live [`pmws_open`] pointer. `out` must be null or point to a
/// single writable `PmwsState`. `levels` must be null only when `level_capacity == 0`, and
/// otherwise point to `level_capacity` writable `PmwsLevel` slots.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pmws_attach(
    session: *mut PmwsSession,
    market: u32,
    out: *mut PmwsState,
    levels: *mut PmwsLevel,
    level_capacity: u32,
) -> i32 {
    guarded(PMWS_STATUS_INTERNAL, || {
        if out.is_null() {
            return PMWS_STATUS_INVALID_ARGUMENT;
        }
        // SAFETY: this call's own contract requires `session` be null or a live session.
        let Some(session) = (unsafe { session_ref(session) }) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        let Some(handle) = resolved_handle(session, market) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        match session.reader.attach_stream(handle) {
            Ok((snapshot, stream)) => {
                let status = respond_snapshot(&snapshot, out, levels, level_capacity);
                if status != PMWS_STATUS_OK {
                    return status;
                }
                if let Some(entry) = session.table.borrow_mut().get_mut(market as usize) {
                    entry.stream = Some(stream);
                }
                status
            }
            Err(fault) => read_fault_status(fault),
        }
    })
}

/// Reads book `market`'s latest published state into `*out` and up to `level_capacity`
/// levels into `levels`, without moving any attached event-stream cursor.
///
/// Fails and is documented exactly as [`pmws_attach`], minus the stream attachment.
///
/// # Safety
/// As [`pmws_attach`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pmws_read_state(
    session: *mut PmwsSession,
    market: u32,
    out: *mut PmwsState,
    levels: *mut PmwsLevel,
    level_capacity: u32,
) -> i32 {
    guarded(PMWS_STATUS_INTERNAL, || {
        if out.is_null() {
            return PMWS_STATUS_INVALID_ARGUMENT;
        }
        // SAFETY: this call's own contract requires `session` be null or a live session.
        let Some(session) = (unsafe { session_ref(session) }) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        let Some(handle) = resolved_handle(session, market) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        match session.reader.read(handle) {
            Ok(snapshot) => respond_snapshot(&snapshot, out, levels, level_capacity),
            Err(fault) => read_fault_status(fault),
        }
    })
}

/// Re-establishes book `market`'s event stream after a continuity loss —
/// [`SegmentReader::attach_stream`], run again against the market's existing handle — writing
/// the snapshot it resumes from to `*out` and its levels to `levels`, exactly as
/// [`pmws_attach`].
///
/// Fails with [`PMWS_STATUS_NOT_ATTACHED`] when `market` has no stream — before the first
/// [`pmws_attach`], or after a fault that never attached one — plus every fault
/// [`pmws_attach`] documents.
///
/// As with [`pmws_attach`], this replaces the stream's cursor and clears any sticky
/// continuity loss if and only if this call returns [`PMWS_STATUS_OK`]: a
/// [`PMWS_STATUS_BUFFER_TOO_SMALL`] retry — or any other failure — leaves the existing stream,
/// its cursor, and its loss state exactly as they were, so a sticky loss stays sticky until a
/// reattach actually succeeds.
///
/// # Safety
/// As [`pmws_attach`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pmws_reattach(
    session: *mut PmwsSession,
    market: u32,
    out: *mut PmwsState,
    levels: *mut PmwsLevel,
    level_capacity: u32,
) -> i32 {
    guarded(PMWS_STATUS_INTERNAL, || {
        if out.is_null() {
            return PMWS_STATUS_INVALID_ARGUMENT;
        }
        // SAFETY: this call's own contract requires `session` be null or a live session.
        let Some(session) = (unsafe { session_ref(session) }) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        let mut table = session.table.borrow_mut();
        let Some(entry) = table.get_mut(market as usize) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        if entry.stream.is_none() {
            return PMWS_STATUS_NOT_ATTACHED;
        }
        match session.reader.attach_stream(entry.handle) {
            Ok((snapshot, stream)) => {
                let status = respond_snapshot(&snapshot, out, levels, level_capacity);
                if status == PMWS_STATUS_OK {
                    entry.stream = Some(stream);
                }
                status
            }
            Err(fault) => read_fault_status(fault),
        }
    })
}

/// Delivers book `market`'s next retained mutation, or says why it cannot, into `*out`.
///
/// Three outcomes: [`PMWS_STATUS_OK`] with `out->delivery_kind` naming the delivery — 1 for
/// a level mutation, 3 for a venue-reported market resolution — and that kind's fields
/// filled, every other field zeroed; [`PMWS_STATUS_NONE`] when the writer has not reached this stream's
/// position yet (`*out` is left untouched); or [`PMWS_STATUS_CONTINUITY_LOST`] with
/// `out->delivery_kind == 2` and `out->continuity_reason` set to the codec's `BREAK_*` word
/// — sticky, repeated on every later poll until [`pmws_reattach`], mirroring
/// [`EventStream::poll`].
///
/// Fails with [`PMWS_STATUS_INVALID_ARGUMENT`] for a null `session` or `out`, or an
/// unresolved `market`; [`PMWS_STATUS_NOT_ATTACHED`] before [`pmws_attach`]; and the
/// transient ([`PMWS_STATUS_CONTENDED`], [`PMWS_STATUS_WRITER_STALLED`], poll again) or
/// terminal (everything else, reattach or escalate) read faults `poll` documents.
///
/// # Safety
/// `session` must be null or a live [`pmws_open`] pointer. `out` must be null or point to a
/// single writable `PmwsEvent`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pmws_next_event(
    session: *mut PmwsSession,
    market: u32,
    out: *mut PmwsEvent,
) -> i32 {
    guarded(PMWS_STATUS_INTERNAL, || {
        if out.is_null() {
            return PMWS_STATUS_INVALID_ARGUMENT;
        }
        // SAFETY: this call's own contract requires `session` be null or a live session.
        let Some(session) = (unsafe { session_ref(session) }) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        let mut table = session.table.borrow_mut();
        let Some(entry) = table.get_mut(market as usize) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        let Some(stream) = entry.stream.as_mut() else {
            return PMWS_STATUS_NOT_ATTACHED;
        };
        match stream.poll() {
            Ok(EventPoll::Idle) => PMWS_STATUS_NONE,
            Ok(EventPoll::Delivered(event)) => {
                let event = encode_retained(&event);
                // SAFETY: `out` was just proven non-null, and this call's own contract
                // requires it point to a single writable `PmwsEvent`.
                let _ = unsafe { write_out(out, event) };
                PMWS_STATUS_OK
            }
            Err(StreamFault::ContinuityLost { reason }) => {
                let event = loss_event(&reason, stream.cursor());
                // SAFETY: as above.
                let _ = unsafe { write_out(out, event) };
                PMWS_STATUS_CONTINUITY_LOST
            }
            Err(StreamFault::Read(fault)) => read_fault_status(fault),
        }
    })
}

/// Waits for [`pmws_publication_generation`] to change from `last_seen_generation`, writing
/// the generation observed when this call settles to `*out_generation` either way —
/// [`SegmentReader::wait_for_publication`].
///
/// Spins for up to `spin_micros` microseconds with no syscall, then parks on the segment's
/// doorbell for up to `timeout_millis` milliseconds; a negative `timeout_millis` parks
/// indefinitely once the spin phase ends. Returns [`PMWS_STATUS_OK`] when the generation
/// changed and [`PMWS_STATUS_NONE`] when `timeout_millis` elapsed with no change — `*out_generation`
/// is filled on both.
///
/// Fails with [`PMWS_STATUS_INVALID_ARGUMENT`] for a null `session` or `out_generation`, or a
/// `spin_micros` above [`PMWS_MAX_SPIN_MICROS`] — refused before any spinning happens, so a
/// value a caller's own narrowing produced costs nothing rather than a core; once the spin
/// phase ends with no change, [`PMWS_STATUS_DOORBELL_UNAVAILABLE`] when the sibling page
/// failed to open at [`pmws_open`] time or never crossed a [`pmws_connect`] attachment — the
/// segment itself is fine; spin with a wider `spin_micros` or poll
/// [`pmws_publication_generation`] instead — or [`PMWS_STATUS_IO`] when the platform's own
/// wait primitive faults for some other reason, which [`WaitFault`] documents.
///
/// # Safety
/// `session` must be null or a live [`pmws_open`] pointer. `out_generation` must be null or
/// point to a single writable `uint64_t`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pmws_wait(
    session: *const PmwsSession,
    last_seen_generation: u64,
    spin_micros: u32,
    timeout_millis: i32,
    out_generation: *mut u64,
) -> i32 {
    guarded(PMWS_STATUS_INTERNAL, || {
        if out_generation.is_null() || spin_micros > PMWS_MAX_SPIN_MICROS {
            return PMWS_STATUS_INVALID_ARGUMENT;
        }
        // SAFETY: this call's own contract requires `session` be null or a live session.
        let Some(session) = (unsafe { session_ref(session) }) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        let spin = Duration::from_micros(u64::from(spin_micros));
        let timeout = (timeout_millis >= 0).then(|| Duration::from_millis(timeout_millis as u64));
        match session
            .reader
            .wait_for_publication(last_seen_generation, spin, timeout)
        {
            Ok(WaitOutcome::Changed(generation)) => {
                // SAFETY: `out_generation` was just proven non-null, and this call's own
                // contract requires it point to a single writable `uint64_t`.
                let _ = unsafe { write_out(out_generation, generation) };
                PMWS_STATUS_OK
            }
            Ok(WaitOutcome::TimedOut(generation)) => {
                // SAFETY: as above.
                let _ = unsafe { write_out(out_generation, generation) };
                PMWS_STATUS_NONE
            }
            Err(fault) => wait_fault_status(fault),
        }
    })
}

/// Delivers the next entry of the segment's dirty-index ring — which directory entry
/// changed and the state revision it advertised — into `*out_directory_index` and
/// `*out_book_revision`, or says why it cannot.
///
/// This session's cursor into the ring is created, lazily, at the ring's current head on the
/// first call. [`PMWS_STATUS_OK`] means one market changed: `out_directory_index` is the
/// segment's own directory index for it (not a [`pmws_resolve`]d session-local `market`
/// index — a session may not have resolved every market the segment carries), and
/// `out_book_revision` is a state-read skip hint only, never a reason to skip that market's
/// event ring: a resolution republish advertises an unchanged revision.
/// [`PMWS_STATUS_NONE`] means the writer has not reached this cursor's position yet.
/// [`PMWS_STATUS_CONTINUITY_LOST`] is the declared full-rescan signal — never sticky, this
/// session's cursor has already rebased past the entry that overran it — and means the
/// caller should re-read every market in its interest set once before resuming ordinary
/// polling.
///
/// The intended pattern for turning a delivered `out_directory_index` back into a market this
/// session knows: call [`pmws_market_directory_index`] once for every market this session has
/// [`pmws_resolve`]d and keep the inverse map. The two indices are different namespaces — one
/// is this session's own dense table, the other is the segment's directory — and nothing in a
/// delivered entry carries the first, so a consumer that does not build that map cannot say
/// which of its markets a dirty entry named.
///
/// Fails with [`PMWS_STATUS_INVALID_ARGUMENT`] for a null `session`.
///
/// # Safety
/// `session` must be null or a live [`pmws_open`] pointer. `out_directory_index` must be
/// null or point to a single writable `uint32_t`; `out_book_revision` must be null or point
/// to a single writable `uint64_t`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pmws_next_dirty(
    session: *mut PmwsSession,
    out_directory_index: *mut u32,
    out_book_revision: *mut u64,
) -> i32 {
    guarded(PMWS_STATUS_INTERNAL, || {
        // SAFETY: this call's own contract requires `session` be null or a live session.
        let Some(session) = (unsafe { session_ref(session) }) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        let mut cursor = session
            .dirty_cursor
            .get()
            .unwrap_or_else(|| session.reader.dirty_cursor());
        let outcome = session.reader.next_dirty(&mut cursor);
        session.dirty_cursor.set(Some(cursor));
        match outcome {
            DirtyPoll::Delivered {
                directory_index,
                book_revision,
            } => {
                // SAFETY: this call's own contract requires `out_directory_index` be null or
                // point to a single writable `uint32_t`.
                let _ = unsafe { write_out(out_directory_index, directory_index) };
                // SAFETY: as above, for `out_book_revision` and `uint64_t`.
                let _ = unsafe { write_out(out_book_revision, book_revision) };
                PMWS_STATUS_OK
            }
            DirtyPoll::Idle => PMWS_STATUS_NONE,
            DirtyPoll::Rescan => PMWS_STATUS_CONTINUITY_LOST,
        }
    })
}

/// Writes the segment directory index of the resolved `market` to `*out_directory_index`.
///
/// This is the bridge between the two index namespaces this ABI uses. A `market` is a dense,
/// session-local row number [`pmws_resolve`] hands out in resolution order; a directory index
/// is the segment's own slot for that book, the same number [`pmws_next_dirty`] delivers. They
/// coincide only by accident — a session that resolves the segment's third market first holds
/// `market` 0 for directory index 2 — so a consumer that wants to know which of its markets a
/// dirty entry named calls this once per resolved market and keeps the inverse map.
///
/// The answer is fixed for the life of the session: a directory entry never moves, and
/// resolving the same identity twice returns the same `market`.
///
/// Fails with [`PMWS_STATUS_INVALID_ARGUMENT`] for a null `session` or
/// `out_directory_index`, or a `market` this session has not resolved.
///
/// # Safety
/// `session` must be null or a live [`pmws_open`] pointer. `out_directory_index` must be null
/// or point to a single writable `uint32_t`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pmws_market_directory_index(
    session: *const PmwsSession,
    market: u32,
    out_directory_index: *mut u32,
) -> i32 {
    guarded(PMWS_STATUS_INTERNAL, || {
        if out_directory_index.is_null() {
            return PMWS_STATUS_INVALID_ARGUMENT;
        }
        // SAFETY: this call's own contract requires `session` be null or a live session.
        let Some(session) = (unsafe { session_ref(session) }) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        let Some(handle) = resolved_handle(session, market) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        // SAFETY: `out_directory_index` was just proven non-null, and this call's own
        // contract requires it point to a single writable `uint32_t`.
        let _ = unsafe { write_out(out_directory_index, handle.entry_index()) };
        PMWS_STATUS_OK
    })
}

/// The publication generation: a coalescible hint that newer data exists somewhere in the
/// segment, never the authority for any one market's state. A null `session` reads as `0`.
///
/// # Safety
/// `session` must be null or a live [`pmws_open`] pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pmws_publication_generation(session: *const PmwsSession) -> u64 {
    guarded(0, || {
        // SAFETY: this call's own contract requires `session` be null or a live session.
        match unsafe { session_ref(session) } {
            Some(session) => session.reader.publication_generation(),
            None => 0,
        }
    })
}

/// Writes `market`'s venue-native identity — venue, kind, key, each UTF-8 — concatenated
/// into `buf`, and their `(offset, len)` spans into `*out`.
///
/// The identity echoed is exactly the [`MarketRef`] this session resolved `market` from:
/// [`SegmentReader::resolve`] returns a handle only when the segment's own decoded identity
/// equals the caller's, so echoing the caller's value back echoes the segment's without
/// re-decoding it, and needs no live read of published state — this answers even before
/// anything has been published.
///
/// Fails with [`PMWS_STATUS_INVALID_ARGUMENT`] for a null `session` or `out`, or an
/// unresolved `market`, and [`PMWS_STATUS_BUFFER_TOO_SMALL`] when `cap` is short of the
/// three strings' combined length — `*out`'s span lengths are filled either way, so a caller
/// can size a retry.
///
/// # Safety
/// `session` must be null or a live [`pmws_open`] pointer. `buf` must be null only when
/// `cap == 0`, and otherwise point to `cap` writable bytes. `out` must be null or point to a
/// single writable `PmwsIdentitySpans`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pmws_market_identity(
    session: *const PmwsSession,
    market: u32,
    buf: *mut u8,
    cap: usize,
    out: *mut PmwsIdentitySpans,
) -> i32 {
    guarded(PMWS_STATUS_INTERNAL, || {
        if out.is_null() {
            return PMWS_STATUS_INVALID_ARGUMENT;
        }
        // SAFETY: this call's own contract requires `session` be null or a live session.
        let Some(session) = (unsafe { session_ref(session) }) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        let table = session.table.borrow();
        let Some(entry) = table.get(market as usize) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        let venue = entry.market.venue().as_str().as_bytes();
        let kind = entry.market.key().kind().as_str().as_bytes();
        let key = entry.market.key().value().as_bytes();
        let total = venue.len() + kind.len() + key.len();
        let spans = PmwsIdentitySpans {
            venue_offset: 0,
            venue_len: venue.len() as u32,
            kind_offset: venue.len() as u32,
            kind_len: kind.len() as u32,
            key_offset: (venue.len() + kind.len()) as u32,
            key_len: key.len() as u32,
        };
        // SAFETY: `out` was just proven non-null, and this call's own contract requires it
        // point to a single writable `PmwsIdentitySpans`.
        let _ = unsafe { write_out(out, spans) };
        if total > cap {
            return PMWS_STATUS_BUFFER_TOO_SMALL;
        }
        // SAFETY: this call's own contract requires `buf` be null only when `cap == 0`, and
        // otherwise point to `cap` writable bytes.
        let Some(slice) = (unsafe { out_slice(buf, cap) }) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        slice[..venue.len()].copy_from_slice(venue);
        slice[venue.len()..venue.len() + kind.len()].copy_from_slice(kind);
        slice[venue.len() + kind.len()..total].copy_from_slice(key);
        PMWS_STATUS_OK
    })
}

/// Renders `decimal`'s exact value into `buf` as digit-placement text — no float appears on
/// this path — and writes the rendered byte length to `*out_len` whether or not it fit.
///
/// Reuses [`ExactDecimal`]'s own [`ExactDecimal::canonical`] rendering by converting
/// `decimal` back into that type first: see [`decimal_text`].
///
/// Fails with [`PMWS_STATUS_INVALID_ARGUMENT`] for a null `decimal` or `out_len`, or a
/// `decimal` this ABI's stored-decimal grammar refuses (an out-of-range scale, or a negative
/// coefficient — a price or quantity is never negative), and
/// [`PMWS_STATUS_BUFFER_TOO_SMALL`] when `cap` is short of `*out_len`.
///
/// # Safety
/// `decimal` must be null or point to a single readable, initialized `PmwsDecimal`. `buf`
/// must be null only when `cap == 0`, and otherwise point to `cap` writable bytes. `out_len`
/// must be null or point to a single writable `uintptr_t`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pmws_decimal_text(
    decimal: *const PmwsDecimal,
    buf: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> i32 {
    guarded(PMWS_STATUS_INTERNAL, || {
        if decimal.is_null() || out_len.is_null() {
            return PMWS_STATUS_INVALID_ARGUMENT;
        }
        // SAFETY: `decimal` was just proven non-null, and this call's own contract requires
        // it point to a single readable, initialized `PmwsDecimal`.
        let decimal = unsafe { &*decimal };
        let Some(text) = decimal_text(decimal) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        let bytes = text.as_bytes();
        // SAFETY: `out_len` was just proven non-null, and this call's own contract requires
        // it point to a single writable `uintptr_t`.
        let _ = unsafe { write_out(out_len, bytes.len()) };
        if bytes.len() > cap {
            return PMWS_STATUS_BUFFER_TOO_SMALL;
        }
        // SAFETY: this call's own contract requires `buf` be null only when `cap == 0`, and
        // otherwise point to `cap` writable bytes.
        let Some(slice) = (unsafe { out_slice(buf, cap) }) else {
            return PMWS_STATUS_INVALID_ARGUMENT;
        };
        slice[..bytes.len()].copy_from_slice(bytes);
        PMWS_STATUS_OK
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::{align_of, size_of};

    #[test]
    fn ffi_guarded_converts_a_panic_into_the_default() {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = guarded(42, || -> i32 { panic!("deliberate") });
        std::panic::set_hook(previous);
        assert_eq!(result, 42);
    }

    /// A change to any of these numbers requires bumping
    /// [`PMWS_FFI_VERSION`].
    #[test]
    fn ffi_struct_layout_is_pinned() {
        assert_eq!(
            (size_of::<PmwsDecimal>(), align_of::<PmwsDecimal>()),
            (24, 8)
        );
        assert_eq!((size_of::<PmwsLevel>(), align_of::<PmwsLevel>()), (56, 8));
        assert_eq!((size_of::<PmwsState>(), align_of::<PmwsState>()), (160, 8));
        assert_eq!((size_of::<PmwsEvent>(), align_of::<PmwsEvent>()), (392, 8));
        assert_eq!(
            (size_of::<PmwsSegmentInfo>(), align_of::<PmwsSegmentInfo>()),
            (48, 8)
        );
        assert_eq!(
            (
                size_of::<PmwsIdentitySpans>(),
                align_of::<PmwsIdentitySpans>()
            ),
            (24, 4)
        );
    }

    #[test]
    fn ffi_decimal_text_matches_the_shared_accept_and_reject_vectors() {
        let widest = (1_u128 << 127) - 1;
        let (low, high) = (widest as u64, (widest >> 64) as u64);
        let accepted = [
            ((0, 0, 0), "0"),
            ((0, 0, 5), "0"),
            ((1200, 0, 2), "12"),
            ((low, high, 0), "170141183460469231731687303715884105727"),
            ((low, high, 38), "1.70141183460469231731687303715884105727"),
        ];
        for ((coefficient_low, coefficient_high, scale), expected) in accepted {
            let decimal = PmwsDecimal {
                coefficient_low,
                coefficient_high,
                scale,
                reserved: 0,
            };
            assert_eq!(decimal_text(&decimal).as_deref(), Some(expected));
        }
        let single_digit_widest_scale = PmwsDecimal {
            coefficient_low: 1,
            coefficient_high: 0,
            scale: u32::from(u16::MAX),
            reserved: 0,
        };
        let text = decimal_text(&single_digit_widest_scale).expect("widest scale renders");
        assert_eq!(text.len(), usize::from(u16::MAX) + 2);
        assert!(text.starts_with("0."));
        assert!(text.ends_with('1'));

        let rejected = [
            (0, 1_u64 << 63, 0),
            (low, high | (1_u64 << 63), 0),
            (1, 0, u32::from(u16::MAX) + 1),
            (1, 0, u32::MAX),
        ];
        for (coefficient_low, coefficient_high, scale) in rejected {
            let decimal = PmwsDecimal {
                coefficient_low,
                coefficient_high,
                scale,
                reserved: 0,
            };
            assert_eq!(decimal_text(&decimal), None);
        }
    }
}

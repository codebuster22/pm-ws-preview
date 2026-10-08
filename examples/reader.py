#!/usr/bin/env python3
"""An independent Python reader of the pm-ws shared-memory publication ABI.

Standard library only: mmap, struct, memoryview. It shares no code with the Rust
implementation and calls no Rust helper -- that is the point. It exists to prove that the
ABI in ``docs/notes/shared-memory-model.md`` is specified well enough for a second,
independent implementation to read a live segment correctly. It is a proof artifact, not
the production path: foreign runtimes reach books through the Rust helper that owns the
atomics, because Python cannot express the acquire/release loads this ABI is defined in
terms of.

What that means for correctness here: CPython has no atomic loads, so every read below is a
plain aligned machine-word read. Soundness rests on two things -- the platform not tearing
an aligned 32- or 64-bit access, and the seqlock discipline, which discards any copy the
writer touched. A word this reader observes half-written is caught by the revalidation, not
by the read itself.

Usage:
    reader.py --segment <path> --market <native-key> [--venue limitless] [--kind slug]
              [--seconds 10] [--events]

``--events`` follows the market's retained-event ring instead of its latest state, over the
same coherent attachment the Rust reader takes: the cursor comes from the state slot's own
seqlock interval, and every slot found afterwards is classified against it lexicographically
on ``(epoch, position)``.
"""

import argparse
import mmap
import struct
import sys
import time

REGION_ALIGNMENT = 128
HEADER_BYTES = 256
TRAILER_BYTES = 128
DIRECTORY_ENTRY_BYTES = 512
IDENTITY_CAPACITY = DIRECTORY_ENTRY_BYTES - 16
SLOT_PREFIX_BYTES = 256
LEVEL_CELL_BYTES = 64
DECIMAL_CELL_BYTES = 24
NATIVE_FAMILY_WORDS = 8
EVENT_SLOT_BYTES = 256
MAGIC = b"PMWSSTA1"
TRAILER_MAGIC = b"PMWSEND1"
ABI_VERSION = 5
NO_STATE_SLOT = 0xFFFFFFFF
MAX_READ_ATTEMPTS = 64
MAX_DIRECTORY_CAPACITY = 65536
MAX_STATE_SLOT_CAPACITY = 65536
MAX_LEVEL_CAPACITY = 4096
MAX_EVENT_CAPACITY = 65536
MAX_DIRTY_CAPACITY = 1_048_576
MAX_STORED_SCALE = 0xFFFF
# Every retained-event ring wraps: the writer never blocks and never skips a position, and
# an overtaken consumer is told so. The bit is required, not optional -- a segment that does
# not declare it is refused, because a reader must never assume a bounded ring is lossless.
FEATURE_EVENT_RING_WRAPS = 1 << 0
# The segment declares its doorbell placement with exactly one of these two bits: the
# header's own word, or a sibling page mapped writable by readers. A segment declaring
# neither or both is refused, mirroring the Rust reader.
FEATURE_DOORBELL_IN_HEADER = 1 << 1
FEATURE_DOORBELL_PAGE = 1 << 2
KNOWN_FEATURE_BITS = FEATURE_EVENT_RING_WRAPS | FEATURE_DOORBELL_IN_HEADER | FEATURE_DOORBELL_PAGE
# The two delivery kinds ever stored in an event slot: a level mutation, and one
# venue-reported market resolution. Discriminant 2 is the FFI's synthesized continuity-loss
# marker, which the binding produces from an explicit stream fault and never writes into the
# segment.
DELIVERY_LEVEL_MUTATION = 1
DELIVERY_MARKET_RESOLVED = 3
# The feed an observation arrived on, as the resolution slot stores it.
DELIVERY_PATH = {1: "MarketFeed", 2: "LifecycleFeed", 3: "ResolutionFeed"}
RES_OUTCOME_CAPACITY, RES_TYPE_CAPACITY, RES_DATE_CAPACITY = 64, 32, 32
# The value a write-once gate holds between being claimed by a formatter and being
# published. It is not a published value: a gate reading this is unpublished, exactly as it
# is to the Rust reader, so a claimed or abandoned record is never followed.
GATE_CLAIMED = 0xFFFFFFFFFFFFFFFF

HDR_MAGIC = 0
HDR_ABI_VERSION, HDR_REGION_ALIGNMENT, HDR_REGION_SIZE = 8, 12, 16
HDR_INSTANCE_LOW, HDR_INSTANCE_HIGH, HDR_SEGMENT_GENERATION = 24, 32, 40
HDR_DIRECTORY_OFFSET, HDR_DIRECTORY_STRIDE, HDR_DIRECTORY_CAPACITY = 48, 56, 60
HDR_SLOT_OFFSET, HDR_SLOT_STRIDE, HDR_SLOT_CAPACITY = 64, 72, 76
HDR_LEVEL_CAPACITY, HDR_LEVEL_STRIDE, HDR_IDENTITY_CAPACITY = 80, 84, 88
HDR_FAMILY_CAPACITY, HDR_TRAILER_OFFSET = 92, 96
HDR_FEATURE_BITS, HDR_EVENT_OFFSET, HDR_EVENT_CAPACITY, HDR_EVENT_STRIDE = 104, 112, 120, 124
HDR_PUBLICATION_GENERATION = 128
HDR_DOORBELL = 136
HDR_DIRTY_OFFSET, HDR_DIRTY_CAPACITY, HDR_DIRTY_STRIDE = 144, 152, 156

TRL_REGION_SIZE, TRL_MAGIC = 0, 8

ENT_REVISION, ENT_SLOT_INDEX, ENT_IDENTITY_LEN, ENT_IDENTITY = 0, 8, 12, 16

SLOT_REVISION, SLOT_BOOK_REVISION = 0, 8
SLOT_CONTINUITY_EPOCH, SLOT_CONTINUITY_POSITION, SLOT_SYNC_DIVERGENCES = 16, 24, 32
SLOT_AUTHORITY_STATE, SLOT_AUTHORITY_REASON = 40, 44
SLOT_CONTINUITY_KIND, SLOT_CONTINUITY_REASON = 48, 52
SLOT_PROVENANCE_PRESENT, SLOT_ORIGIN, SLOT_DERIVATION, SLOT_REPRESENTATION = 56, 60, 64, 68
SLOT_FAMILY_LEN, SLOT_LEVEL_COUNT, SLOT_DIRECTORY_INDEX = 72, 76, 80
SLOT_COMMIT_TIME, SLOT_ARRIVAL_TIME, SLOT_FAMILY_WORDS = 88, 96, 128
LVL_PRICE, LVL_QUANTITY, LVL_SIDE = 0, 24, 48
DEC_LOW, DEC_HIGH, DEC_SCALE = 0, 8, 16

EVT_SEQUENCE, EVT_CURSOR_EPOCH, EVT_CURSOR_POSITION = 0, 8, 16
EVT_BOOK_REVISION, EVT_COMMIT_TIME = 24, 32
EVT_DELIVERY_KIND, EVT_ORIGIN, EVT_DERIVATION, EVT_REPRESENTATION = 40, 44, 48, 52
EVT_SIDE, EVT_OLD_PRESENT, EVT_NEW_PRESENT, EVT_FAMILY_LEN = 56, 60, 64, 68
EVT_DIRECTORY_INDEX = 72
EVT_DAEMON_GENERATION, EVT_SUBSCRIPTION_GENERATION = 80, 88
EVT_PRICE, EVT_OLD_QUANTITY, EVT_NEW_QUANTITY, EVT_FAMILY_WORDS = 96, 120, 144, 168
EVT_ARRIVAL_TIME = 232

# The segment's single dirty-index ring: one entry per state publish or republish, after
# the event rings and before the trailer. `DIRTY_POSITION` is absolute and segment-lifetime
# monotone, exactly like an event slot's cursor position.
DIRTY_SEQUENCE, DIRTY_POSITION = 0, 8
DIRTY_DIRECTORY_INDEX, DIRTY_BOOK_REVISION = 16, 24
DIRTY_SLOT_BYTES = 32

# The kind-3 half of an event slot. It overlays the mutation half: a resolution's winning
# index occupies the four bytes a mutation's side word does, and the delivery kind is what
# says which reading is the right one.
EVT_RES_WINNING_INDEX, EVT_RES_DELIVERY_PATH = 56, 60
EVT_RES_OUTCOME_LEN, EVT_RES_TYPE_LEN, EVT_RES_DATE_LEN = 64, 68, 96
EVT_RES_OUTCOME, EVT_RES_TYPE, EVT_RES_DATE = 104, 168, 200

# Every mutable field of the v4 header, state slot and event slot, as (name, offset, width
# in bytes). The event slot appears twice, once per delivery kind, because the two kinds
# overlay one coordinate space. Printed by `--abi-table` so a Rust test can compare it
# against the layout module's own constants and fail loudly on drift rather than silently
# decoding the wrong bytes.
ABI_TABLE = [
    ("header_magic", HDR_MAGIC, 8),
    ("header_publication_generation", HDR_PUBLICATION_GENERATION, 8),
    ("header_feature_bits", HDR_FEATURE_BITS, 8),
    ("header_event_offset", HDR_EVENT_OFFSET, 8),
    ("header_event_capacity", HDR_EVENT_CAPACITY, 4),
    ("header_event_stride", HDR_EVENT_STRIDE, 4),
    ("header_doorbell", HDR_DOORBELL, 4),
    ("header_dirty_offset", HDR_DIRTY_OFFSET, 8),
    ("header_dirty_capacity", HDR_DIRTY_CAPACITY, 4),
    ("header_dirty_stride", HDR_DIRTY_STRIDE, 4),
    ("header_bytes", HEADER_BYTES, 0),
    ("trailer_region_size", TRL_REGION_SIZE, 8),
    ("trailer_magic", TRL_MAGIC, 8),
    ("trailer_bytes", TRAILER_BYTES, 0),
    ("entry_revision", ENT_REVISION, 8),
    ("entry_state_slot_index", ENT_SLOT_INDEX, 4),
    ("entry_identity_length", ENT_IDENTITY_LEN, 4),
    ("entry_identity", ENT_IDENTITY, IDENTITY_CAPACITY),
    ("directory_entry_bytes", DIRECTORY_ENTRY_BYTES, 0),
    ("slot_revision", SLOT_REVISION, 8),
    ("book_revision", SLOT_BOOK_REVISION, 8),
    ("continuity_epoch", SLOT_CONTINUITY_EPOCH, 8),
    ("continuity_next_position", SLOT_CONTINUITY_POSITION, 8),
    ("sync_divergences", SLOT_SYNC_DIVERGENCES, 8),
    ("authority_state", SLOT_AUTHORITY_STATE, 4),
    ("authority_reason", SLOT_AUTHORITY_REASON, 4),
    ("continuity_kind", SLOT_CONTINUITY_KIND, 4),
    ("continuity_reason", SLOT_CONTINUITY_REASON, 4),
    ("provenance_present", SLOT_PROVENANCE_PRESENT, 4),
    ("provenance_origin", SLOT_ORIGIN, 4),
    ("provenance_derivation", SLOT_DERIVATION, 4),
    ("provenance_representation", SLOT_REPRESENTATION, 4),
    ("native_family_length", SLOT_FAMILY_LEN, 4),
    ("level_count", SLOT_LEVEL_COUNT, 4),
    ("directory_index", SLOT_DIRECTORY_INDEX, 4),
    ("commit_time", SLOT_COMMIT_TIME, 8),
    ("arrival_time", SLOT_ARRIVAL_TIME, 8),
    ("native_family_words", SLOT_FAMILY_WORDS, 64),
    ("slot_prefix_bytes", SLOT_PREFIX_BYTES, 0),
    ("level_price", LVL_PRICE, 24),
    ("level_quantity", LVL_QUANTITY, 24),
    ("level_side", LVL_SIDE, 4),
    ("level_cell_bytes", LEVEL_CELL_BYTES, 0),
    ("decimal_coefficient_low", DEC_LOW, 8),
    ("decimal_coefficient_high", DEC_HIGH, 8),
    ("decimal_scale", DEC_SCALE, 4),
    ("decimal_cell_bytes", DECIMAL_CELL_BYTES, 0),
    ("event_sequence", EVT_SEQUENCE, 8),
    ("event_cursor_epoch", EVT_CURSOR_EPOCH, 8),
    ("event_cursor_position", EVT_CURSOR_POSITION, 8),
    ("event_book_revision", EVT_BOOK_REVISION, 8),
    ("event_commit_time", EVT_COMMIT_TIME, 8),
    ("event_delivery_kind", EVT_DELIVERY_KIND, 4),
    ("event_origin", EVT_ORIGIN, 4),
    ("event_derivation", EVT_DERIVATION, 4),
    ("event_representation", EVT_REPRESENTATION, 4),
    ("event_side", EVT_SIDE, 4),
    ("event_old_present", EVT_OLD_PRESENT, 4),
    ("event_new_present", EVT_NEW_PRESENT, 4),
    ("event_native_family_length", EVT_FAMILY_LEN, 4),
    ("event_directory_index", EVT_DIRECTORY_INDEX, 4),
    ("event_daemon_generation", EVT_DAEMON_GENERATION, 8),
    ("event_subscription_generation", EVT_SUBSCRIPTION_GENERATION, 8),
    ("event_price", EVT_PRICE, 24),
    ("event_old_quantity", EVT_OLD_QUANTITY, 24),
    ("event_new_quantity", EVT_NEW_QUANTITY, 24),
    ("event_native_family_words", EVT_FAMILY_WORDS, 64),
    ("event_arrival_time", EVT_ARRIVAL_TIME, 8),
    ("event_slot_bytes", EVENT_SLOT_BYTES, 0),
    ("resolution_sequence", EVT_SEQUENCE, 8),
    ("resolution_cursor_epoch", EVT_CURSOR_EPOCH, 8),
    ("resolution_cursor_position", EVT_CURSOR_POSITION, 8),
    ("resolution_book_revision", EVT_BOOK_REVISION, 8),
    ("resolution_commit_time", EVT_COMMIT_TIME, 8),
    ("resolution_delivery_kind", EVT_DELIVERY_KIND, 4),
    ("resolution_origin", EVT_ORIGIN, 4),
    ("resolution_derivation", EVT_DERIVATION, 4),
    ("resolution_representation", EVT_REPRESENTATION, 4),
    ("resolution_winning_index", EVT_RES_WINNING_INDEX, 4),
    ("resolution_delivery_path", EVT_RES_DELIVERY_PATH, 4),
    ("resolution_outcome_length", EVT_RES_OUTCOME_LEN, 4),
    ("resolution_type_length", EVT_RES_TYPE_LEN, 4),
    ("resolution_directory_index", EVT_DIRECTORY_INDEX, 4),
    ("resolution_daemon_generation", EVT_DAEMON_GENERATION, 8),
    ("resolution_subscription_generation", EVT_SUBSCRIPTION_GENERATION, 8),
    ("resolution_date_length", EVT_RES_DATE_LEN, 4),
    ("resolution_winning_outcome", EVT_RES_OUTCOME, RES_OUTCOME_CAPACITY),
    ("resolution_market_type", EVT_RES_TYPE, RES_TYPE_CAPACITY),
    ("resolution_date", EVT_RES_DATE, RES_DATE_CAPACITY),
    ("resolution_arrival_time", EVT_ARRIVAL_TIME, 8),
    ("resolution_slot_bytes", EVENT_SLOT_BYTES, 0),
    ("dirty_sequence", DIRTY_SEQUENCE, 8),
    ("dirty_position", DIRTY_POSITION, 8),
    ("dirty_directory_index", DIRTY_DIRECTORY_INDEX, 4),
    ("dirty_book_revision", DIRTY_BOOK_REVISION, 8),
    ("dirty_slot_bytes", DIRTY_SLOT_BYTES, 0),
]

CONTINUITY_INTACT, CONTINUITY_LOST = 1, 2
BREAK = {1: "Overrun", 2: "Gap", 3: "LocalLoss", 4: "Reconnect", 5: "RecoveryBase",
         6: "SyncDivergence"}
ORIGIN = {1: "SourceReported", 2: "NormalizedFromSource", 3: "LocallyDerived"}
DERIVATION = {0: None, 1: "SnapshotDiff"}
AUTHORITY = {1: "Unsubscribed", 2: "Subscribing", 3: "Synchronizing", 4: "Live",
             5: "Recovering", 6: "Stale"}
REASON = {1: "Gap", 2: "Disconnect", 3: "SubscriptionLost", 4: "LocalLoss",
          5: "OrderingUnknown", 6: "Overload", 7: "ReplicaDivergence",
          8: "RecoveryBaseUnavailable"}
SIDE = {1: "Bid", 2: "Ask"}


class AbiError(Exception):
    """The segment is not one this reader understands. Always fail closed."""


def raw(view, offset, length):
    """`length` bytes at `offset`, copied out of the mapping."""
    return bytes(view[offset:offset + length])


def u32(view, offset):
    return struct.unpack_from("<I", view, offset)[0]


def u64(view, offset):
    return struct.unpack_from("<Q", view, offset)[0]


def round_up(value):
    return -(-value // REGION_ALIGNMENT) * REGION_ALIGNMENT


def gate(view, offset):
    """The published value of the write-once gate at `offset`, or None.

    Zero is untouched and GATE_CLAIMED is a formatter that has not finished — possibly one
    that never will. Both mean unpublished, and the bytes the gate protects must not be
    read: they may be under a formatter's non-atomic copy right now.
    """
    value = u64(view, offset)
    return None if value in (0, GATE_CLAIMED) else value


def validate(view):
    """Validate the fixed header and the trailer, and return the recomputed geometry.

    Nothing between them is read. The three capacities are the only geometry words trusted;
    every stride and offset is recomputed from them and compared, so a header that declares
    a geometry inconsistent with its own capacities is refused rather than believed.
    """
    size = len(view)
    if size < HEADER_BYTES + TRAILER_BYTES or size % REGION_ALIGNMENT:
        raise AbiError(f"region size {size} is not a usable segment")
    if gate(view, HDR_MAGIC) != struct.unpack_from("<Q", MAGIC, 0)[0]:
        raise AbiError("header magic mismatch, unpublished, or claimed")
    abi = u32(view, HDR_ABI_VERSION)
    if abi != ABI_VERSION:
        raise AbiError(f"abi version {abi}, expected {ABI_VERSION}")
    if u32(view, HDR_REGION_ALIGNMENT) != REGION_ALIGNMENT:
        raise AbiError("region alignment mismatch")
    if u64(view, HDR_REGION_SIZE) != size:
        raise AbiError("declared region size does not match the mapping")

    features = u64(view, HDR_FEATURE_BITS)
    doorbell_bits = features & (FEATURE_DOORBELL_IN_HEADER | FEATURE_DOORBELL_PAGE)
    if (
        features & ~KNOWN_FEATURE_BITS
        or not features & FEATURE_EVENT_RING_WRAPS
        or doorbell_bits not in (FEATURE_DOORBELL_IN_HEADER, FEATURE_DOORBELL_PAGE)
    ):
        raise AbiError(f"unsupported feature word 0x{features:x}")

    directory_capacity = u32(view, HDR_DIRECTORY_CAPACITY)
    slot_capacity = u32(view, HDR_SLOT_CAPACITY)
    level_capacity = u32(view, HDR_LEVEL_CAPACITY)
    event_capacity = u32(view, HDR_EVENT_CAPACITY)
    dirty_capacity = u32(view, HDR_DIRTY_CAPACITY)
    if not 0 < directory_capacity <= slot_capacity:
        raise AbiError("capacities out of range")
    if directory_capacity > MAX_DIRECTORY_CAPACITY or slot_capacity > MAX_STATE_SLOT_CAPACITY:
        raise AbiError("capacities out of range")
    if not 0 < level_capacity <= MAX_LEVEL_CAPACITY:
        raise AbiError("capacities out of range")
    if not 0 < event_capacity <= MAX_EVENT_CAPACITY:
        raise AbiError("capacities out of range")
    if event_capacity & (event_capacity - 1):
        raise AbiError("event capacity is not a power of two")
    if not 0 < dirty_capacity <= MAX_DIRTY_CAPACITY:
        raise AbiError("capacities out of range")
    if dirty_capacity & (dirty_capacity - 1):
        raise AbiError("dirty capacity is not a power of two")
    slot_stride = round_up(SLOT_PREFIX_BYTES + level_capacity * LEVEL_CELL_BYTES)
    directory_offset = HEADER_BYTES
    slot_offset = directory_offset + directory_capacity * DIRECTORY_ENTRY_BYTES
    event_offset = slot_offset + slot_capacity * slot_stride
    ring_bytes = event_capacity * EVENT_SLOT_BYTES
    dirty_offset = event_offset + directory_capacity * ring_bytes
    dirty_ring_bytes = dirty_capacity * DIRTY_SLOT_BYTES
    trailer_offset = dirty_offset + dirty_ring_bytes
    expected = [
        (u64(view, HDR_DIRECTORY_OFFSET), directory_offset),
        (u32(view, HDR_DIRECTORY_STRIDE), DIRECTORY_ENTRY_BYTES),
        (u64(view, HDR_SLOT_OFFSET), slot_offset),
        (u32(view, HDR_SLOT_STRIDE), slot_stride),
        (u32(view, HDR_LEVEL_STRIDE), LEVEL_CELL_BYTES),
        (u32(view, HDR_IDENTITY_CAPACITY), IDENTITY_CAPACITY),
        (u32(view, HDR_FAMILY_CAPACITY), NATIVE_FAMILY_WORDS * 8),
        (u64(view, HDR_EVENT_OFFSET), event_offset),
        (u32(view, HDR_EVENT_STRIDE), EVENT_SLOT_BYTES),
        (u64(view, HDR_DIRTY_OFFSET), dirty_offset),
        (u32(view, HDR_DIRTY_STRIDE), DIRTY_SLOT_BYTES),
        (u64(view, HDR_TRAILER_OFFSET), trailer_offset),
        (trailer_offset + TRAILER_BYTES, size),
    ]
    for declared, recomputed in expected:
        if declared != recomputed:
            raise AbiError(f"geometry mismatch: {declared} != {recomputed}")
    if gate(view, trailer_offset + TRL_MAGIC) != struct.unpack_from("<Q", TRAILER_MAGIC, 0)[0]:
        raise AbiError("trailer magic mismatch, unpublished, or claimed")
    if u64(view, trailer_offset + TRL_REGION_SIZE) != size:
        raise AbiError("trailer size echo mismatch")
    return {
        "instance": (u64(view, HDR_INSTANCE_HIGH) << 64) | u64(view, HDR_INSTANCE_LOW),
        "generation": u64(view, HDR_SEGMENT_GENERATION),
        "directory_offset": directory_offset,
        "directory_capacity": directory_capacity,
        "slot_offset": slot_offset,
        "slot_stride": slot_stride,
        "slot_capacity": slot_capacity,
        "level_capacity": level_capacity,
        "event_offset": event_offset,
        "event_capacity": event_capacity,
        "ring_bytes": ring_bytes,
        "dirty_offset": dirty_offset,
        "dirty_capacity": dirty_capacity,
    }


def decode_identity(raw):
    if len(raw) < 8:
        return None
    venue_len, kind_len = struct.unpack_from("<HH", raw, 0)
    value_len = struct.unpack_from("<I", raw, 4)[0]
    body = raw[8:]
    if len(body) < venue_len + kind_len + value_len:
        return None
    try:
        venue = bytes(body[:venue_len]).decode("utf-8")
        kind = bytes(body[venue_len:venue_len + kind_len]).decode("utf-8")
        value = bytes(body[venue_len + kind_len:venue_len + kind_len + value_len]).decode("utf-8")
    except UnicodeDecodeError:
        return None
    return (venue, kind, value)


def resolve(view, geometry, identity):
    """The (entry_index, slot_index) of a market, or None. Identity is read only after the
    entry revision is observed non-zero, which is the write-once publication rule."""
    for index in range(geometry["directory_capacity"]):
        base = geometry["directory_offset"] + index * DIRECTORY_ENTRY_BYTES
        if gate(view, base + ENT_REVISION) is None:
            continue
        length = u32(view, base + ENT_IDENTITY_LEN)
        if length > IDENTITY_CAPACITY:
            continue
        found = decode_identity(view[base + ENT_IDENTITY:base + ENT_IDENTITY + length])
        if found == identity:
            return (index, u32(view, base + ENT_SLOT_INDEX))
    return None


def decimal_text(low, high, scale):
    """The exact decimal those three words carry, as a string, using Python ints only.

    No float and no Decimal appears on this path: the coefficient is a 128-bit two's
    complement integer and the scale is a digit count, so the value is rendered by string
    placement of the decimal point rather than by any division.
    """
    coefficient = check_decimal(low, high, scale)
    # Normalize exactly as the Rust decoder does: strip trailing zeros so one value has one
    # rendering, which also collapses every non-canonical zero to plain "0".
    while scale > 0 and coefficient % 10 == 0:
        coefficient //= 10
        scale -= 1
    digits = str(coefficient)
    if scale == 0:
        return digits
    if len(digits) <= scale:
        return "0." + "0" * (scale - len(digits)) + digits
    split = len(digits) - scale
    return digits[:split] + "." + digits[split:]


def validate_levels(levels):
    """Check every copied level, exactly as the Rust decoder does before it accepts a read.

    Runs after the seqlock closes, so it judges a copy the writer did not touch. A level
    naming a side this ABI does not define, or carrying a decimal `check_decimal` refuses,
    makes the whole record malformed rather than one level unprintable.
    """
    for side_word, price, quantity in levels:
        if side_word not in SIDE:
            raise AbiError(f"unknown side discriminant {side_word}")
        check_decimal(*price)
        check_decimal(*quantity)


def check_decimal(low, high, scale):
    """The non-negative 128-bit coefficient those words carry, or a typed failure.

    The same two gates the Rust decoder applies before it builds anything: the stored scale
    must fit the 16-bit scale the representation actually has, and the coefficient's sign
    bit must be clear, because a price or a quantity is never negative. Returns the
    coefficient so a caller that only validates allocates nothing.
    """
    if scale > MAX_STORED_SCALE:
        raise AbiError(f"stored scale {scale} exceeds the representable range")
    coefficient = (high << 64) | low
    if coefficient >= 1 << 127:
        raise AbiError("a price or quantity coefficient may not be negative")
    return coefficient


def decimal(view, base):
    return (u64(view, base + DEC_LOW), u64(view, base + DEC_HIGH), u32(view, base + DEC_SCALE))


def read_slot(view, geometry, entry_index, slot_index):
    """One consistent snapshot, or a reason string.

    The seqlock discipline exactly: read `slot_revision`, reject an odd value, copy the
    slot, read the counter again, and accept only an unchanged even value. Bounded retries;
    an odd counter that never moves is a writer that stopped mid-publication.
    """
    if slot_index == NO_STATE_SLOT:
        return "no-state"
    if slot_index >= geometry["slot_capacity"]:
        raise AbiError(f"slot index {slot_index} is outside the segment's capacity")
    base = geometry["slot_offset"] + slot_index * geometry["slot_stride"]
    stalled_at, moved = None, False
    for _ in range(MAX_READ_ATTEMPTS):
        opened = u64(view, base + SLOT_REVISION)
        if opened == 0:
            return "no-state"
        if opened & 1:
            if stalled_at is None:
                stalled_at = opened
            elif stalled_at != opened:
                moved = True
            continue
        revision = u64(view, base + SLOT_BOOK_REVISION)
        authority = u32(view, base + SLOT_AUTHORITY_STATE)
        reason = u32(view, base + SLOT_AUTHORITY_REASON)
        continuity = (u32(view, base + SLOT_CONTINUITY_KIND),
                      u32(view, base + SLOT_CONTINUITY_REASON),
                      u64(view, base + SLOT_CONTINUITY_EPOCH),
                      u64(view, base + SLOT_CONTINUITY_POSITION))
        owner = u32(view, base + SLOT_DIRECTORY_INDEX)
        count = u32(view, base + SLOT_LEVEL_COUNT)
        arrival_time = u64(view, base + SLOT_ARRIVAL_TIME)
        levels = []
        for position in range(min(count, geometry["level_capacity"])):
            cell = base + SLOT_PREFIX_BYTES + position * LEVEL_CELL_BYTES
            levels.append((
                u32(view, cell + LVL_SIDE),
                decimal(view, cell + LVL_PRICE),
                decimal(view, cell + LVL_QUANTITY),
            ))
        if u64(view, base + SLOT_REVISION) != opened:
            moved = True
            continue
        if owner != entry_index:
            return "slot-ownership-mismatch"
        if count > geometry["level_capacity"]:
            return "malformed-record"
        validate_levels(levels)
        return {"revision": revision, "authority": authority, "reason": reason,
                "levels": levels, "continuity_kind": continuity[0],
                "continuity_reason": continuity[1], "continuity_epoch": continuity[2],
                "continuity_position": continuity[3],
                "arrival_time": arrival_time or None}
    return "writer-stalled" if stalled_at is not None and not moved else "contended"


def origin_text(origin, derivation):
    """The provenance origin those two words name, or a typed failure.

    Source-reported and locally derived changes travel the same slot shape, so this pairing
    is the only thing that distinguishes them and an unknown pairing is refused rather than
    rendered as one of them.
    """
    name = ORIGIN.get(origin)
    if name is None:
        raise AbiError(f"unknown origin discriminant {origin}")
    if derivation not in DERIVATION:
        raise AbiError(f"unknown derivation discriminant {derivation}")
    derived = DERIVATION[derivation]
    if name == "LocallyDerived":
        if derived is None:
            raise AbiError("a locally derived origin carries no derivation")
        return f"{name}({derived})"
    if derived is not None:
        raise AbiError(f"origin {name} carries derivation {derivation}")
    return name


def read_event(view, geometry, entry_index, position):
    """One retained event at `position`, None when its slot was never written, or a reason.

    The ring wraps, so the slot is `(position & (event_capacity - 1))` of the market's ring
    -- a mask, never a modulo. The seqlock discipline is exactly `read_slot`'s: read the
    sequence, reject an odd value, copy the slot, read the sequence again, and keep the copy
    only when it is unchanged.

    The delivery kind decides which half of the slot is copied, because the two kinds
    overlay one coordinate space. Text is copied as raw bytes and decoded only after the
    copy is accepted, so a half-written slot can never raise a decode error instead of being
    discarded.
    """
    ring = geometry["event_offset"] + entry_index * geometry["ring_bytes"]
    base = ring + (position & (geometry["event_capacity"] - 1)) * EVENT_SLOT_BYTES
    for _ in range(MAX_READ_ATTEMPTS):
        opened = u64(view, base + EVT_SEQUENCE)
        if opened == 0:
            return None
        if opened & 1:
            continue
        kind = u32(view, base + EVT_DELIVERY_KIND)
        event = {
            "epoch": u64(view, base + EVT_CURSOR_EPOCH),
            "position": u64(view, base + EVT_CURSOR_POSITION),
            "revision": u64(view, base + EVT_BOOK_REVISION),
            "kind": kind,
            "origin": u32(view, base + EVT_ORIGIN),
            "derivation": u32(view, base + EVT_DERIVATION),
            "owner": u32(view, base + EVT_DIRECTORY_INDEX),
            "arrival_time": u64(view, base + EVT_ARRIVAL_TIME) or None,
        }
        if kind == DELIVERY_MARKET_RESOLVED:
            event.update({
                "winning_index": u32(view, base + EVT_RES_WINNING_INDEX),
                "delivery_path": u32(view, base + EVT_RES_DELIVERY_PATH),
                "outcome": (u32(view, base + EVT_RES_OUTCOME_LEN),
                            raw(view, base + EVT_RES_OUTCOME, RES_OUTCOME_CAPACITY)),
                "market_type": (u32(view, base + EVT_RES_TYPE_LEN),
                                raw(view, base + EVT_RES_TYPE, RES_TYPE_CAPACITY)),
                "resolution_date": (u32(view, base + EVT_RES_DATE_LEN),
                                    raw(view, base + EVT_RES_DATE, RES_DATE_CAPACITY)),
            })
        else:
            event.update({
                "side": u32(view, base + EVT_SIDE),
                "old_present": u32(view, base + EVT_OLD_PRESENT),
                "new_present": u32(view, base + EVT_NEW_PRESENT),
                "price": decimal(view, base + EVT_PRICE),
                "old_quantity": decimal(view, base + EVT_OLD_QUANTITY),
                "new_quantity": decimal(view, base + EVT_NEW_QUANTITY),
            })
        if u64(view, base + EVT_SEQUENCE) != opened:
            continue
        return event
    return "contended"


def stored_text(field, cell):
    """One venue-native text cell as `(declared length, raw bytes)`, or a typed failure.

    A declared length past the cell, or bytes that are not UTF-8, make the record malformed:
    this reader never truncates a venue value and never guesses at one it cannot decode.
    """
    length, raw_bytes = cell
    if length > len(raw_bytes):
        raise AbiError(f"{field} length {length} exceeds its cell")
    try:
        return raw_bytes[:length].decode("utf-8")
    except UnicodeDecodeError as error:
        raise AbiError(f"{field} is not UTF-8") from error


def validate_event(event, entry_index):
    """Check a copied event exactly as the Rust decoder does before it accepts one.

    Delivery kind 2 is the FFI's synthesized continuity-loss marker and is never stored, so
    a slot carrying it is malformed rather than a loss, as is any kind this ABI does not
    define.
    """
    if event["owner"] != entry_index:
        raise AbiError("event slot serves another entry")
    if event["kind"] == DELIVERY_MARKET_RESOLVED:
        validate_resolution(event)
        return
    if event["kind"] != DELIVERY_LEVEL_MUTATION:
        raise AbiError(f"unknown delivery kind {event['kind']}")
    if event["side"] not in SIDE:
        raise AbiError(f"unknown side discriminant {event['side']}")
    if event["old_present"] not in (0, 1) or event["new_present"] not in (0, 1):
        raise AbiError("a presence word outside {0, 1}")
    if not event["old_present"] and not event["new_present"]:
        raise AbiError("a mutation naming neither half")
    origin_text(event["origin"], event["derivation"])
    check_decimal(*event["price"])
    if event["old_present"]:
        check_decimal(*event["old_quantity"])
    if event["new_present"]:
        check_decimal(*event["new_quantity"])


def validate_resolution(event):
    """Check a copied resolution slot before it is printed.

    A resolution stores no side, no presence words and no decimals; what it must survive is
    its provenance words, its delivery path, and three venue-native texts reproduced
    verbatim.
    """
    origin_text(event["origin"], event["derivation"])
    if event["delivery_path"] not in DELIVERY_PATH:
        raise AbiError(f"unknown delivery path {event['delivery_path']}")
    stored_text("winning outcome", event["outcome"])
    stored_text("market type", event["market_type"])
    stored_text("resolution date", event["resolution_date"])


def resolution_text(event):
    return (f"resolution epoch={event['epoch']} position={event['position']} "
            f"revision={event['revision']} "
            f"origin={origin_text(event['origin'], event['derivation'])} "
            f"path={DELIVERY_PATH[event['delivery_path']]} "
            f"winner={stored_text('winning outcome', event['outcome'])} "
            f"index={event['winning_index']} "
            f"type={stored_text('market type', event['market_type'])} "
            f"date={stored_text('resolution date', event['resolution_date'])}")


def event_text(event):
    if event["kind"] == DELIVERY_MARKET_RESOLVED:
        return resolution_text(event)

    def half(present, words):
        return decimal_text(*words) if present else "-"

    return (f"event epoch={event['epoch']} position={event['position']} "
            f"revision={event['revision']} "
            f"origin={origin_text(event['origin'], event['derivation'])} "
            f"side={SIDE[event['side']]} price={decimal_text(*event['price'])} "
            f"old={half(event['old_present'], event['old_quantity'])} "
            f"new={half(event['new_present'], event['new_quantity'])}")


def settled_state(view, geometry, entry_index, slot_index):
    """One consistent state read, or None while the reader keeps losing the race.

    `no-state`, `contended` and `writer-stalled` are all this reader's own bound expiring
    rather than segment facts, so they are retried; anything else is a typed failure.
    """
    state = read_slot(view, geometry, entry_index, slot_index)
    if not isinstance(state, str):
        return state
    if state not in ("no-state", "contended", "writer-stalled"):
        raise AbiError(state)
    return None


def follow_events(view, geometry, entry_index, slot_index, deadline):
    """Consume one market's retained events from a coherent attachment.

    The attachment cursor is the `(epoch, next_position)` the state slot publishes inside
    the same seqlock interval as its revision -- never a second read -- so everything below
    it is already contained in that state and everything at or above it comes from the ring.

    Every slot found is then classified against that cursor lexicographically, because
    positions restart at 0 on a new continuity epoch: a bare position comparison would call
    a rebased stream's first event older than the previous stream's last. The state slot's
    continuity is re-read first on every pass, and it is what makes a rebase observable at
    all -- a recovery base commits zero mutations, so a parked consumer would otherwise wait
    on a ring that will never reach it. An overtaken cursor is reported, never resynchronized.
    """
    state = None
    while state is None and time.monotonic() < deadline:
        state = settled_state(view, geometry, entry_index, slot_index)
    if state is None:
        return
    if state["continuity_kind"] != CONTINUITY_INTACT:
        reason = BREAK.get(state["continuity_reason"])
        if reason is None:
            raise AbiError(f"unknown continuity reason {state['continuity_reason']}")
        print(f"event continuity_lost reason={reason}", flush=True)
        return
    epoch, position = state["continuity_epoch"], state["continuity_position"]
    print(f"attached_stream epoch={epoch} position={position}", flush=True)
    while time.monotonic() < deadline:
        state = settled_state(view, geometry, entry_index, slot_index)
        if state is None:
            continue
        if state["continuity_epoch"] != epoch:
            print("event continuity_lost reason=RecoveryBase", flush=True)
            return
        if state["continuity_kind"] != CONTINUITY_INTACT:
            reason = BREAK.get(state["continuity_reason"])
            if reason is None:
                raise AbiError(f"unknown continuity reason {state['continuity_reason']}")
            print(f"event continuity_lost reason={reason}", flush=True)
            return
        event = read_event(view, geometry, entry_index, position)
        if event is None or isinstance(event, str):
            time.sleep(0.0002)
            continue
        stored = (event["epoch"], event["position"])
        if stored > (epoch, position):
            overtaken = "Overrun" if stored[0] == epoch else "RecoveryBase"
            print(f"event continuity_lost reason={overtaken}", flush=True)
            return
        if stored != (epoch, position):
            time.sleep(0.0002)
            continue
        validate_event(event, entry_index)
        print(event_text(event), flush=True)
        position += 1


def authority_text(state, reason):
    """The authority state those two words name, or a typed failure.

    An unknown discriminant is refused rather than printed: a reader that renders a word it
    does not understand is reporting a state it cannot vouch for.
    """
    name = AUTHORITY.get(state)
    if name is None:
        raise AbiError(f"unknown authority discriminant {state}")
    if name != "Stale":
        if reason != 0:
            raise AbiError(f"authority {name} carries reason {reason}")
        return name
    if reason not in REASON:
        raise AbiError(f"unknown authority reason {reason}")
    return f"Stale({REASON[reason]})"


def level_text(level):
    if level is None:
        return "-"
    _, price, quantity = level
    return f"{decimal_text(*price)}@{decimal_text(*quantity)}"


def best(levels, side):
    """Best bid or ask, relying on the ABI's canonical ordering: levels ascend by
    (side, price), so the best bid is the last bid and the best ask is the first ask.
    A level resting an exact zero quantity is reported state, not depth, and is skipped.
    Every level was already validated when the seqlock closed, so the side discriminant is
    known here."""
    matching = [lv for lv in levels if SIDE[lv[0]] == side and (lv[2][0] or lv[2][1])]
    if not matching:
        return None
    return matching[-1] if side == "Bid" else matching[0]


def bounded_seconds(text):
    """A positive, finite run length in seconds, at most one day.

    `float(text)` alone accepts `inf` and `nan`, either of which turns the run loop into a
    non-terminating one or an immediately exiting one.
    """
    value = float(text)
    if value != value or value in (float("inf"), float("-inf")):
        raise argparse.ArgumentTypeError("--seconds must be a finite number")
    if not 0 < value <= 86_400:
        raise argparse.ArgumentTypeError("--seconds must be positive and at most 86400")
    return value


def self_test():
    """The edges this ABI must survive, using exact integers only.

    The Rust decoder is checked against the same cases; if the two ever disagree the ABI is
    underspecified, which is what an independent implementation is for. The accept and
    reject lists below are the shared set.
    """
    widest = (1 << 127) - 1
    low, high = widest & 0xFFFFFFFFFFFFFFFF, widest >> 64
    accepted = [
        ((0, 0, 0), "0"),
        ((0, 0, 5), "0"),
        ((1200, 0, 2), "12"),
        ((1, 0, MAX_STORED_SCALE), "0." + "0" * (MAX_STORED_SCALE - 1) + "1"),
        ((low, high, 0), str(widest)),
        ((low, high, 38), str(widest)[:1] + "." + str(widest)[1:]),
    ]
    for parts, expected in accepted:
        produced = decimal_text(*parts)
        assert produced == expected, f"decimal {parts}: {produced!r} != {expected!r}"
        assert isinstance(produced, str)
    rejected = [
        (0, 1 << 63, 0),
        (low, high | (1 << 63), 0),
        (1, 0, MAX_STORED_SCALE + 1),
        (1, 0, 0xFFFFFFFF),
    ]
    for parts in rejected:
        try:
            decimal_text(*parts)
        except AbiError:
            continue
        raise AbiError(f"decimal {parts} was accepted")

    for state, reason in ((0, 0), (7, 0), (4, 1), (6, 99)):
        try:
            authority_text(state, reason)
        except AbiError:
            continue
        raise AbiError(f"authority ({state}, {reason}) was accepted")

    assert origin_text(1, 0) == "SourceReported"
    assert origin_text(2, 0) == "NormalizedFromSource"
    assert origin_text(3, 1) == "LocallyDerived(SnapshotDiff)"
    for origin, derivation in ((0, 0), (4, 0), (1, 1), (3, 0), (2, 1), (1, 9)):
        try:
            origin_text(origin, derivation)
        except AbiError:
            continue
        raise AbiError(f"origin ({origin}, {derivation}) was accepted")

    sound = {"owner": 0, "kind": DELIVERY_LEVEL_MUTATION, "side": 1, "origin": 1,
             "derivation": 0, "old_present": 1, "new_present": 1, "price": (5, 0, 1),
             "old_quantity": (1, 0, 0), "new_quantity": (2, 0, 0), "epoch": 0,
             "position": 7, "revision": 3}
    validate_event(sound, 0)
    assert event_text(sound) == ("event epoch=0 position=7 revision=3 "
                                 "origin=SourceReported side=Bid price=0.5 old=1 new=2")
    for field, value in (("owner", 1), ("kind", 2), ("kind", 0), ("side", 3),
                         ("origin", 9), ("old_present", 2), ("new_present", 2)):
        broken = dict(sound, **{field: value})
        try:
            validate_event(broken, 0)
        except AbiError:
            continue
        raise AbiError(f"event with {field}={value} was accepted")
    absent = dict(sound, old_present=0, new_present=0)
    try:
        validate_event(absent, 0)
        raise AbiError("an event naming neither half was accepted")
    except AbiError as error:
        if "neither half" not in str(error):
            raise
    assert event_text(dict(sound, old_present=0)) .endswith("old=- new=2")

    resolved = {"owner": 0, "kind": DELIVERY_MARKET_RESOLVED, "origin": 1, "derivation": 0,
                "epoch": 0, "position": 8, "revision": 3, "winning_index": 1,
                "delivery_path": 1,
                "outcome": (3, b"Yes" + bytes(RES_OUTCOME_CAPACITY - 3)),
                "market_type": (4, b"clob" + bytes(RES_TYPE_CAPACITY - 4)),
                "resolution_date": (20, b"2026-09-01T12:00:00Z" + bytes(RES_DATE_CAPACITY - 20))}
    validate_event(resolved, 0)
    assert event_text(resolved) == ("resolution epoch=0 position=8 revision=3 "
                                    "origin=SourceReported path=MarketFeed winner=Yes "
                                    "index=1 type=clob date=2026-09-01T12:00:00Z")
    for field, value in (("owner", 1), ("kind", 4), ("delivery_path", 0),
                         ("delivery_path", 4), ("origin", 9)):
        broken = dict(resolved, **{field: value})
        try:
            validate_event(broken, 0)
        except AbiError:
            continue
        raise AbiError(f"resolution with {field}={value} was accepted")
    for field in ("outcome", "market_type", "resolution_date"):
        _, raw_bytes = resolved[field]
        for cell in ((len(raw_bytes) + 1, raw_bytes), (2, b"\xff\xfe" + raw_bytes[2:])):
            broken = dict(resolved, **{field: cell})
            try:
                validate_event(broken, 0)
            except AbiError:
                continue
            raise AbiError(f"resolution with a broken {field} was accepted")

    # Lexicographic ordering on (epoch, position): a rebased stream's first event is later
    # than the previous stream's last, which a bare position comparison would get backwards.
    assert (1, 0) > (0, 9_000)
    assert not (0, 5) > (0, 5)
    for bad in ([(3, (1, 0, 0), (1, 0, 0))], [(1, (0, 1 << 63, 0), (1, 0, 0))]):
        try:
            validate_levels(bad)
        except AbiError:
            continue
        raise AbiError(f"level {bad} was accepted")
    validate_levels([(1, (1, 0, 3), (5, 0, 0)), (2, (0, 0, 9), (0, 0, 0))])

    for value in (1 << 32, (1 << 32) + 7, 1 << 63, 0xFFFFFFFFFFFFFFFF):
        buffer = memoryview(bytearray(struct.pack("<Q", value)))
        assert u64(buffer, 0) == value, f"high word lost for {value}"

    identity = struct.pack("<HHI", 9, 4, 3) + b"limitlessslugabc"
    directory = bytearray(2 * DIRECTORY_ENTRY_BYTES)
    for index, revision in ((0, GATE_CLAIMED), (1, 1)):
        base = index * DIRECTORY_ENTRY_BYTES
        struct.pack_into("<Q", directory, base + ENT_REVISION, revision)
        struct.pack_into("<I", directory, base + ENT_SLOT_INDEX, index)
        struct.pack_into("<I", directory, base + ENT_IDENTITY_LEN, len(identity))
        directory[base + ENT_IDENTITY:base + ENT_IDENTITY + len(identity)] = identity
    view = memoryview(directory)
    geometry = {"directory_offset": 0, "directory_capacity": 2}
    located = resolve(view, geometry, ("limitless", "slug", "abc"))
    assert located == (1, 1), f"a claimed entry was followed: {located}"
    assert gate(view, ENT_REVISION) is None, "a claimed gate read as published"
    print("self-test ok")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--abi-table", action="store_true",
                        help="print this reader's v4 slot layout and exit")
    parser.add_argument("--self-test", action="store_true",
                        help="check the decimal and discriminant edges and exit")
    parser.add_argument("--events", action="store_true",
                        help="follow the market's retained-event ring instead of its state")
    parser.add_argument("--segment")
    parser.add_argument("--market")
    parser.add_argument("--venue", default="limitless")
    parser.add_argument("--kind", default="slug")
    parser.add_argument("--seconds", type=bounded_seconds, default=10.0)
    args = parser.parse_args()
    if args.abi_table:
        for name, offset, width in ABI_TABLE:
            print(f"{name}={offset}:{width}")
        return
    if args.self_test:
        self_test()
        return
    if not args.segment or not args.market:
        parser.error("--segment and --market are required")

    with open(args.segment, "rb") as handle:
        mapped = mmap.mmap(handle.fileno(), 0, access=mmap.ACCESS_READ)
        view = memoryview(mapped)
        try:
            geometry = validate(view)
            print(f"attached instance=0x{geometry['instance']:032x} "
                  f"generation={geometry['generation']} "
                  f"markets={geometry['directory_capacity']}", flush=True)
            identity = (args.venue, args.kind, args.market)
            deadline = time.monotonic() + args.seconds
            located = None
            while located is None and time.monotonic() < deadline:
                located = resolve(view, geometry, identity)
                if located is None:
                    time.sleep(0.0002)
            if located is None:
                raise AbiError(f"market {identity} is not installed in the segment")
            entry_index, slot_index = located
            if args.events:
                follow_events(view, geometry, entry_index, slot_index, deadline)
                return
            last_generation, last_revision = None, None
            while time.monotonic() < deadline:
                generation = u64(view, HDR_PUBLICATION_GENERATION)
                if generation == last_generation:
                    time.sleep(0.0002)
                    continue
                last_generation = generation
                state = read_slot(view, geometry, entry_index, slot_index)
                if isinstance(state, str):
                    # `writer-stalled` is this reader's bound expiring, not proof the
                    # writer died: a writer preempted between its odd and even stores looks
                    # identical at this timescale, so a latest-state consumer retries.
                    if state in ("no-state", "contended", "writer-stalled"):
                        last_generation = None
                        continue
                    raise AbiError(state)
                if state["revision"] == last_revision:
                    continue
                last_revision = state["revision"]
                print(f"bbo revision={state['revision']} "
                      f"authority={authority_text(state['authority'], state['reason'])} "
                      f"best_bid={level_text(best(state['levels'], 'Bid'))} "
                      f"best_ask={level_text(best(state['levels'], 'Ask'))}", flush=True)
        finally:
            view.release()
            mapped.close()


if __name__ == "__main__":
    try:
        main()
    except AbiError as error:
        print(f"error: {error}", file=sys.stderr)
        sys.exit(1)

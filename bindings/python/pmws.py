"""The Python binding over pm-ws's C-ABI cdylib (`src/ffi/mod.rs`).

Standard library only: ctypes. Unlike ``examples/reader.py`` -- an independent proof that the
segment ABI is specified well enough for a second implementation -- this module is the
production path for a Python consumer: every read crosses through the Rust helper, which
performs the acquire/release loads CPython cannot express itself.

No float appears on this path. A price or a quantity is an :class:`ExactDecimal`: a signed
128-bit coefficient, a scale, and the exact text `pmws_decimal_text` renders for it.

Usage::

    import pmws
    with pmws.Segment("/path/to/segment") as segment:
        market = segment.resolve("limitless", "slug", "some-market-slug")
        state, stream = market.attach()
        print(state.best("Bid"), state.best("Ask"))
        mutation = stream.next_event()
"""

import ctypes
import os
import platform
import threading
from pathlib import Path

PMWS_STATUS_OK = 0
PMWS_STATUS_NONE = 1
PMWS_STATUS_INVALID_ARGUMENT = -1
PMWS_STATUS_IO = -2
PMWS_STATUS_SEGMENT_INCOMPATIBLE = -3
PMWS_STATUS_MARKET_NOT_FOUND = -4
PMWS_STATUS_NO_PUBLISHED_STATE = -5
PMWS_STATUS_CONTENDED = -6
PMWS_STATUS_WRITER_STALLED = -7
PMWS_STATUS_MALFORMED_RECORD = -8
PMWS_STATUS_BUFFER_TOO_SMALL = -9
PMWS_STATUS_CONTINUITY_LOST = -10
PMWS_STATUS_NOT_ATTACHED = -11
PMWS_STATUS_INTERNAL = -12
PMWS_STATUS_ATTACH_REFUSED = -13
PMWS_STATUS_ATTACH_INCOMPLETE = -14
# `wait`'s park phase cannot reach this segment's doorbell -- a page-placement segment's
# sibling page failed to open, or never crossed a `connect()` attachment at all. The segment
# itself is fine; parked waiting specifically is unavailable. Spin with a `spin_micros`
# budget instead, or poll `publication_generation()` directly.
PMWS_STATUS_DOORBELL_UNAVAILABLE = -15
# `lease()` asked for a market this segment does not hold: it lives in another shard, whose
# segment this session has no mapping for. The lease is handed back before this is raised, so
# the refusal leaves no demand at the daemon. Connect a second `Segment` for that market.
PMWS_STATUS_FOREIGN_SEGMENT = -16

CODES = {
    PMWS_STATUS_OK: "OK",
    PMWS_STATUS_NONE: "NONE",
    PMWS_STATUS_INVALID_ARGUMENT: "INVALID_ARGUMENT",
    PMWS_STATUS_IO: "IO",
    PMWS_STATUS_SEGMENT_INCOMPATIBLE: "SEGMENT_INCOMPATIBLE",
    PMWS_STATUS_MARKET_NOT_FOUND: "MARKET_NOT_FOUND",
    PMWS_STATUS_NO_PUBLISHED_STATE: "NO_PUBLISHED_STATE",
    PMWS_STATUS_CONTENDED: "CONTENDED",
    PMWS_STATUS_WRITER_STALLED: "WRITER_STALLED",
    PMWS_STATUS_MALFORMED_RECORD: "MALFORMED_RECORD",
    PMWS_STATUS_BUFFER_TOO_SMALL: "BUFFER_TOO_SMALL",
    PMWS_STATUS_CONTINUITY_LOST: "CONTINUITY_LOST",
    PMWS_STATUS_NOT_ATTACHED: "NOT_ATTACHED",
    PMWS_STATUS_INTERNAL: "INTERNAL",
    PMWS_STATUS_ATTACH_REFUSED: "ATTACH_REFUSED",
    PMWS_STATUS_ATTACH_INCOMPLETE: "ATTACH_INCOMPLETE",
    PMWS_STATUS_DOORBELL_UNAVAILABLE: "DOORBELL_UNAVAILABLE",
    PMWS_STATUS_FOREIGN_SEGMENT: "FOREIGN_SEGMENT",
}

NATIVE_FAMILY_CAPACITY = 64
RESOLUTION_OUTCOME_CAPACITY = 64
RESOLUTION_TYPE_CAPACITY = 32
RESOLUTION_DATE_CAPACITY = 32
EXPECTED_FFI_VERSION = 8
EXPECTED_ABI_VERSION = 5

# `PMWS_MAX_SPIN_MICROS` in `src/ffi/mod.rs`: the widest `spin_micros` `pmws_wait` accepts.
# Mirrored rather than read back because it is a compile-time constant of the C ABI, not an
# exported function; the FFI version gate above is what keeps the two from drifting apart.
PMWS_MAX_SPIN_MICROS = 10_000_000

# The widest `timeout_ms` the C ABI's `int32_t` parameter carries. A larger value is refused
# rather than handed to `ctypes`, whose narrowing of an oversized Python int is silent.
PMWS_MAX_TIMEOUT_MS = 2 ** 31 - 1

# The generation counter is a `uint64_t` on the wire.
_MAX_GENERATION = 2 ** 64 - 1

# The delivery kinds this binding knows how to present: a level mutation (class `Mutation`)
# and a venue-reported market resolution (class `Resolution`). Any other kind is reported as
# a malformed record rather than presented as a level change or a resolution that never
# happened.
DELIVERY_MUTATION = 1
DELIVERY_RESOLUTION = 3

DELIVERY_PATHS = {1: "marketFeed", 2: "lifecycleFeed", 3: "resolutionFeed"}

AUTHORITY_STATES = {
    1: "Unsubscribed", 2: "Subscribing", 3: "Synchronizing", 4: "Live", 5: "Recovering",
    6: "Stale",
}
AUTHORITY_REASONS = {
    1: "Gap", 2: "Disconnect", 3: "SubscriptionLost", 4: "LocalLoss", 5: "OrderingUnknown",
    6: "Overload", 7: "ReplicaDivergence", 8: "RecoveryBaseUnavailable",
}
CONTINUITY_REASONS = {
    1: "Overrun", 2: "Gap", 3: "LocalLoss", 4: "Reconnect", 5: "RecoveryBase",
    6: "SyncDivergence",
}
ORIGINS = {1: "sourceReported", 2: "normalizedFromSource", 3: "locallyDerived"}
DERIVATIONS = {0: None, 1: "snapshotDiff"}
REPRESENTATIONS = {1: "venueNative", 2: "normalized"}
SIDES = {1: "Bid", 2: "Ask"}


class PmwsSession(ctypes.Structure):
    """Opaque: reachable only through the pointer `pmws_open` returns."""


class PmwsDecimal(ctypes.Structure):
    _fields_ = [
        ("coefficient_low", ctypes.c_uint64),
        ("coefficient_high", ctypes.c_uint64),
        ("scale", ctypes.c_uint32),
        ("reserved", ctypes.c_uint32),
    ]


class PmwsLevel(ctypes.Structure):
    _fields_ = [
        ("side", ctypes.c_uint32),
        ("reserved", ctypes.c_uint32),
        ("price", PmwsDecimal),
        ("quantity", PmwsDecimal),
    ]


class PmwsState(ctypes.Structure):
    _fields_ = [
        ("revision", ctypes.c_uint64),
        ("cursor_epoch", ctypes.c_uint64),
        ("cursor_position", ctypes.c_uint64),
        ("sync_divergences", ctypes.c_uint64),
        ("commit_time", ctypes.c_uint64),
        ("authority_state", ctypes.c_uint32),
        ("authority_reason", ctypes.c_uint32),
        ("continuity_kind", ctypes.c_uint32),
        ("continuity_reason", ctypes.c_uint32),
        ("commit_time_present", ctypes.c_uint32),
        ("publication_present", ctypes.c_uint32),
        ("origin", ctypes.c_uint32),
        ("derivation", ctypes.c_uint32),
        ("representation", ctypes.c_uint32),
        ("native_family_len", ctypes.c_uint32),
        ("level_count", ctypes.c_uint32),
        ("level_capacity_required", ctypes.c_uint32),
        ("native_family", ctypes.c_uint8 * NATIVE_FAMILY_CAPACITY),
        ("arrival_time", ctypes.c_uint64),
    ]


class PmwsEvent(ctypes.Structure):
    _fields_ = [
        ("missed", ctypes.c_uint64),
        ("cursor_epoch", ctypes.c_uint64),
        ("cursor_position", ctypes.c_uint64),
        ("book_revision", ctypes.c_uint64),
        ("commit_time", ctypes.c_uint64),
        ("daemon_generation", ctypes.c_uint64),
        ("subscription_generation", ctypes.c_uint64),
        ("price", PmwsDecimal),
        ("old_quantity", PmwsDecimal),
        ("new_quantity", PmwsDecimal),
        ("delivery_kind", ctypes.c_uint32),
        ("continuity_reason", ctypes.c_uint32),
        ("origin", ctypes.c_uint32),
        ("derivation", ctypes.c_uint32),
        ("representation", ctypes.c_uint32),
        ("native_family_len", ctypes.c_uint32),
        ("side", ctypes.c_uint32),
        ("old_present", ctypes.c_uint32),
        ("new_present", ctypes.c_uint32),
        ("native_family", ctypes.c_uint8 * NATIVE_FAMILY_CAPACITY),
        ("winning_index", ctypes.c_uint32),
        ("delivery_path", ctypes.c_uint32),
        ("winning_outcome_len", ctypes.c_uint32),
        ("market_type_len", ctypes.c_uint32),
        ("resolution_date_len", ctypes.c_uint32),
        ("reserved2", ctypes.c_uint32),
        ("winning_outcome", ctypes.c_uint8 * RESOLUTION_OUTCOME_CAPACITY),
        ("market_type", ctypes.c_uint8 * RESOLUTION_TYPE_CAPACITY),
        ("resolution_date", ctypes.c_uint8 * RESOLUTION_DATE_CAPACITY),
        ("reserved3", ctypes.c_uint32),
        ("arrival_time", ctypes.c_uint64),
    ]


class PmwsSegmentInfo(ctypes.Structure):
    _fields_ = [
        ("instance_id_low", ctypes.c_uint64),
        ("instance_id_high", ctypes.c_uint64),
        ("segment_generation", ctypes.c_uint64),
        ("publication_generation", ctypes.c_uint64),
        ("directory_capacity", ctypes.c_uint32),
        ("state_slot_capacity", ctypes.c_uint32),
        ("level_capacity", ctypes.c_uint32),
        ("event_capacity", ctypes.c_uint32),
    ]


class PmwsIdentitySpans(ctypes.Structure):
    _fields_ = [
        ("venue_offset", ctypes.c_uint32),
        ("venue_len", ctypes.c_uint32),
        ("kind_offset", ctypes.c_uint32),
        ("kind_len", ctypes.c_uint32),
        ("key_offset", ctypes.c_uint32),
        ("key_len", ctypes.c_uint32),
    ]


_PINNED_SIZES = {
    PmwsDecimal: 24,
    PmwsLevel: 56,
    PmwsState: 160,
    PmwsEvent: 392,
    PmwsSegmentInfo: 48,
    PmwsIdentitySpans: 24,
}
for _struct_type, _expected_size in _PINNED_SIZES.items():
    _actual_size = ctypes.sizeof(_struct_type)
    if _actual_size != _expected_size:
        raise RuntimeError(
            f"{_struct_type.__name__} is {_actual_size} bytes on this platform, expected "
            f"{_expected_size}; pmws.py is out of sync with src/ffi/mod.rs"
        )


def _library_name():
    system = platform.system()
    if system == "Darwin":
        return "libpm_ws.dylib"
    if system == "Windows":
        return "pm_ws.dll"
    return "libpm_ws.so"


def _library_path():
    override = os.environ.get("PMWS_LIB")
    if override:
        return Path(override)
    name = _library_name()
    repo_root = Path(__file__).resolve().parents[2]
    for profile in ("release", "debug"):
        candidate = repo_root / "target" / profile / name
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(
        f"pm-ws cdylib not found; build it with `cargo build --release` (or --debug) or "
        f"set PMWS_LIB to its path (looked for {name} under {repo_root / 'target'}/"
        "{release,debug})"
    )


_lib = ctypes.CDLL(str(_library_path()))

_lib.pmws_ffi_version.argtypes = []
_lib.pmws_ffi_version.restype = ctypes.c_int32
_lib.pmws_abi_version.argtypes = []
_lib.pmws_abi_version.restype = ctypes.c_uint32
_lib.pmws_status_text.argtypes = [ctypes.c_int32]
_lib.pmws_status_text.restype = ctypes.c_char_p
_lib.pmws_open.argtypes = [
    ctypes.c_char_p, ctypes.c_size_t, ctypes.POINTER(ctypes.POINTER(PmwsSession)),
]
_lib.pmws_open.restype = ctypes.c_int32
_lib.pmws_connect.argtypes = [
    ctypes.c_char_p, ctypes.c_size_t,
    ctypes.c_char_p, ctypes.c_size_t,
    ctypes.POINTER(ctypes.POINTER(PmwsSession)),
]
_lib.pmws_connect.restype = ctypes.c_int32
_lib.pmws_close.argtypes = [ctypes.POINTER(PmwsSession)]
_lib.pmws_close.restype = None
_lib.pmws_renew.argtypes = [ctypes.POINTER(PmwsSession)]
_lib.pmws_renew.restype = ctypes.c_int32
_lib.pmws_lease.argtypes = [
    ctypes.POINTER(PmwsSession), ctypes.c_char_p, ctypes.c_size_t,
]
_lib.pmws_lease.restype = ctypes.c_int32
_lib.pmws_release.argtypes = [
    ctypes.POINTER(PmwsSession), ctypes.c_char_p, ctypes.c_size_t,
]
_lib.pmws_release.restype = ctypes.c_int32
_lib.pmws_segment_info.argtypes = [
    ctypes.POINTER(PmwsSession), ctypes.POINTER(PmwsSegmentInfo),
]
_lib.pmws_segment_info.restype = ctypes.c_int32
_lib.pmws_resolve.argtypes = [
    ctypes.POINTER(PmwsSession),
    ctypes.c_char_p, ctypes.c_size_t,
    ctypes.c_char_p, ctypes.c_size_t,
    ctypes.c_char_p, ctypes.c_size_t,
    ctypes.POINTER(ctypes.c_uint32),
]
_lib.pmws_resolve.restype = ctypes.c_int32
_read_argtypes = [
    ctypes.POINTER(PmwsSession), ctypes.c_uint32, ctypes.POINTER(PmwsState),
    ctypes.POINTER(PmwsLevel), ctypes.c_uint32,
]
_lib.pmws_attach.argtypes = _read_argtypes
_lib.pmws_attach.restype = ctypes.c_int32
_lib.pmws_read_state.argtypes = _read_argtypes
_lib.pmws_read_state.restype = ctypes.c_int32
_lib.pmws_reattach.argtypes = _read_argtypes
_lib.pmws_reattach.restype = ctypes.c_int32
_lib.pmws_next_event.argtypes = [
    ctypes.POINTER(PmwsSession), ctypes.c_uint32, ctypes.POINTER(PmwsEvent),
]
_lib.pmws_next_event.restype = ctypes.c_int32
_lib.pmws_publication_generation.argtypes = [ctypes.POINTER(PmwsSession)]
_lib.pmws_publication_generation.restype = ctypes.c_uint64
_lib.pmws_wait.argtypes = [
    ctypes.POINTER(PmwsSession), ctypes.c_uint64, ctypes.c_uint32, ctypes.c_int32,
    ctypes.POINTER(ctypes.c_uint64),
]
_lib.pmws_wait.restype = ctypes.c_int32
_lib.pmws_next_dirty.argtypes = [
    ctypes.POINTER(PmwsSession), ctypes.POINTER(ctypes.c_uint32), ctypes.POINTER(ctypes.c_uint64),
]
_lib.pmws_next_dirty.restype = ctypes.c_int32
_lib.pmws_market_directory_index.argtypes = [
    ctypes.POINTER(PmwsSession), ctypes.c_uint32, ctypes.POINTER(ctypes.c_uint32),
]
_lib.pmws_market_directory_index.restype = ctypes.c_int32
_lib.pmws_market_identity.argtypes = [
    ctypes.POINTER(PmwsSession), ctypes.c_uint32, ctypes.c_char_p, ctypes.c_size_t,
    ctypes.POINTER(PmwsIdentitySpans),
]
_lib.pmws_market_identity.restype = ctypes.c_int32
_lib.pmws_decimal_text.argtypes = [
    ctypes.POINTER(PmwsDecimal), ctypes.c_char_p, ctypes.c_size_t,
    ctypes.POINTER(ctypes.c_size_t),
]
_lib.pmws_decimal_text.restype = ctypes.c_int32

_ffi_version = _lib.pmws_ffi_version()
_abi_version = _lib.pmws_abi_version()
if _ffi_version != EXPECTED_FFI_VERSION:
    raise RuntimeError(
        f"pmws.py speaks FFI v{EXPECTED_FFI_VERSION}; the loaded library reports "
        f"v{_ffi_version}"
    )
if _abi_version != EXPECTED_ABI_VERSION:
    raise RuntimeError(
        f"pmws.py speaks segment ABI v{EXPECTED_ABI_VERSION}; the loaded library reports "
        f"v{_abi_version}"
    )


def _status_text(code):
    raw = _lib.pmws_status_text(ctypes.c_int32(code))
    return raw.decode("utf-8") if raw is not None else "unknown status"


class PmwsError(Exception):
    """One FFI status, other than OK -- code, name, and `pmws_status_text`'s own text."""

    def __init__(self, code, detail=None):
        self.code = code
        self.name = CODES.get(code, "UNKNOWN")
        self.text = _status_text(code)
        message = f"{self.name} ({code}): {self.text}"
        if detail:
            message = f"{message} -- {detail}"
        super().__init__(message)


class PmwsContinuityLost(PmwsError):
    """`PMWS_STATUS_CONTINUITY_LOST`. Sticky on the stream until `reattach()`."""

    def __init__(self, reason):
        super().__init__(PMWS_STATUS_CONTINUITY_LOST, detail=f"reason={reason}")
        self.reason = reason


class PmwsDirtyRescan(PmwsError):
    """`PMWS_STATUS_CONTINUITY_LOST` from `Segment.next_dirty` -- the declared full-rescan
    signal, distinct from :class:`PmwsContinuityLost`.

    Never sticky: the native session's dirty cursor has already rebased to the ring's
    current head by the time this raises, so the very next `next_dirty()` call resumes
    ordinary polling. On catching this, re-read every market in the caller's interest set
    once before resuming.
    """

    def __init__(self):
        super().__init__(
            PMWS_STATUS_CONTINUITY_LOST,
            detail="dirty-index ring lapped this cursor; re-read every attached market's "
            "state and events once, then resume next_dirty()",
        )


class PmwsSessionClosed(PmwsError):
    """Raised by any operation on a :class:`Segment` -- or a `Market`/`EventStream` resolved
    from one -- attempted after that segment's `close()` has already run.

    Detected entirely in this binding, before any call reaches the freed native session, so
    it carries no `PMWS_STATUS_*` code from the C ABI; it mirrors the Node binding's own
    synthetic ``PMWS_SESSION_CLOSED`` error the same way.
    """

    def __init__(self):
        self.code = None
        self.name = "PMWS_SESSION_CLOSED"
        self.text = "session is closed"
        Exception.__init__(self, f"{self.name}: {self.text}")


def _check_range(name, value, low, high):
    """Refuses a `value` that is not an `int` in `[low, high]`, before `ctypes` can narrow it.

    `bool` is rejected along with every other non-`int`: `True` is a Python `int`, but a
    caller passing one to a microsecond budget meant something else.
    """
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{name} must be an int, not {type(value).__name__}")
    if not low <= value <= high:
        raise ValueError(f"{name} must be in [{low}, {high}], got {value}")


def _malformed(detail):
    raise PmwsError(PMWS_STATUS_MALFORMED_RECORD, detail)


def _authority_text(state_word, reason_word):
    name = AUTHORITY_STATES.get(state_word)
    if name is None:
        _malformed(f"unknown authority discriminant {state_word}")
    if name != "Stale":
        if reason_word != 0:
            _malformed(f"authority {name} carries reason {reason_word}")
        return name
    reason = AUTHORITY_REASONS.get(reason_word)
    if reason is None:
        _malformed(f"unknown authority reason {reason_word}")
    return f"Stale({reason})"


def _origin_text(origin_word, derivation_word):
    name = ORIGINS.get(origin_word)
    if name is None:
        _malformed(f"unknown origin discriminant {origin_word}")
    if derivation_word not in DERIVATIONS:
        _malformed(f"unknown derivation discriminant {derivation_word}")
    derivation = DERIVATIONS[derivation_word]
    if name == "locallyDerived":
        if derivation is None:
            _malformed("a locally derived origin carries no derivation")
        return f"{name}({derivation})"
    if derivation is not None:
        _malformed(f"origin {name} carries derivation {derivation_word}")
    return name


def _side_text(side_word):
    side = SIDES.get(side_word)
    if side is None:
        _malformed(f"unknown side discriminant {side_word}")
    return side


def _signed_coefficient(low, high):
    value = (high << 64) | low
    if value >= 1 << 127:
        value -= 1 << 128
    return value


def _decimal_text(raw):
    cap = 40
    while True:
        buf = ctypes.create_string_buffer(cap)
        out_len = ctypes.c_size_t()
        status = _lib.pmws_decimal_text(ctypes.byref(raw), buf, cap, ctypes.byref(out_len))
        if status == PMWS_STATUS_OK:
            return buf.raw[: out_len.value].decode("utf-8")
        if status == PMWS_STATUS_BUFFER_TOO_SMALL:
            cap = out_len.value
            continue
        raise PmwsError(status)


class ExactDecimal:
    """A price or a quantity: a signed exact coefficient, a scale, and its rendered text.

    `text` is never produced locally -- it comes from `pmws_decimal_text`, the same decoder
    every other consumer of this ABI reads through. No float appears anywhere on this type.
    """

    __slots__ = ("coefficient", "scale", "text")

    def __init__(self, raw):
        self.coefficient = _signed_coefficient(raw.coefficient_low, raw.coefficient_high)
        self.scale = raw.scale
        self.text = _decimal_text(raw)

    def __str__(self):
        return self.text

    def __repr__(self):
        return f"ExactDecimal({self.text!r})"

    def __eq__(self, other):
        return (
            isinstance(other, ExactDecimal)
            and self.coefficient == other.coefficient
            and self.scale == other.scale
        )


class Level:
    """One resting level: a side, a price, and a quantity."""

    __slots__ = ("side", "price", "quantity")

    def __init__(self, raw):
        self.side = _side_text(raw.side)
        self.price = ExactDecimal(raw.price)
        self.quantity = ExactDecimal(raw.quantity)


class BookState:
    """One book's latest published state, as `pmws_attach`/`pmws_read_state` fill it."""

    def __init__(self, raw, levels):
        self.revision = raw.revision
        self.cursor_epoch = raw.cursor_epoch
        self.cursor_position = raw.cursor_position
        self.sync_divergences = raw.sync_divergences
        self.commit_time = raw.commit_time if raw.commit_time_present else None
        self.arrival_time = raw.arrival_time or None
        self.authority = _authority_text(raw.authority_state, raw.authority_reason)
        self.continuity_intact = raw.continuity_kind == 1
        self.continuity_reason = (
            None if self.continuity_intact else CONTINUITY_REASONS.get(raw.continuity_reason)
        )
        if not self.continuity_intact and self.continuity_reason is None:
            _malformed(f"unknown continuity reason {raw.continuity_reason}")
        if raw.publication_present:
            self.origin = _origin_text(raw.origin, raw.derivation)
            self.representation = REPRESENTATIONS.get(raw.representation)
            if self.representation is None:
                _malformed(f"unknown representation discriminant {raw.representation}")
            self.native_family = bytes(raw.native_family[: raw.native_family_len]).decode(
                "utf-8"
            )
        else:
            self.origin = None
            self.representation = None
            self.native_family = None
        self.levels = [Level(level) for level in levels]

    def best(self, side):
        """The best level on `side` ("Bid" or "Ask"), or `None`.

        Levels ascend by `(side, price)`, so the best bid is the last matching level and the
        best ask is the first. A level resting an exact zero quantity is reported state, not
        depth, and is skipped.
        """
        matching = [
            level for level in self.levels if level.side == side and level.quantity.coefficient
        ]
        if not matching:
            return None
        return matching[-1] if side == "Bid" else matching[0]


class Mutation:
    """One delivered retained mutation, as `pmws_next_event` fills it."""

    def __init__(self, raw):
        self.cursor_epoch = raw.cursor_epoch
        self.cursor_position = raw.cursor_position
        self.revision = raw.book_revision
        self.commit_time = raw.commit_time or None
        self.arrival_time = raw.arrival_time or None
        self.daemon_generation = raw.daemon_generation
        self.subscription_generation = raw.subscription_generation
        self.side = _side_text(raw.side)
        self.origin = _origin_text(raw.origin, raw.derivation)
        self.representation = REPRESENTATIONS.get(raw.representation)
        if self.representation is None:
            _malformed(f"unknown representation discriminant {raw.representation}")
        self.native_family = bytes(raw.native_family[: raw.native_family_len]).decode("utf-8")
        self.price = ExactDecimal(raw.price)
        self.old_quantity = ExactDecimal(raw.old_quantity) if raw.old_present else None
        self.new_quantity = ExactDecimal(raw.new_quantity) if raw.new_present else None


class Resolution:
    """One delivered venue-reported market resolution, as `pmws_next_event` fills it.

    `revision` is the book revision this resolution is ordered after -- a resolution commits
    none of its own. `winning_outcome`, `market_type`, and `resolution_date` are the venue's
    own texts, verbatim; `resolution_date` is never parsed into a number.
    """

    def __init__(self, raw):
        self.cursor_epoch = raw.cursor_epoch
        self.cursor_position = raw.cursor_position
        self.revision = raw.book_revision
        self.commit_time = raw.commit_time or None
        self.arrival_time = raw.arrival_time or None
        self.daemon_generation = raw.daemon_generation
        self.subscription_generation = raw.subscription_generation
        self.origin = _origin_text(raw.origin, raw.derivation)
        self.representation = REPRESENTATIONS.get(raw.representation)
        if self.representation is None:
            _malformed(f"unknown representation discriminant {raw.representation}")
        self.winning_index = raw.winning_index
        self.winning_outcome = bytes(
            raw.winning_outcome[: raw.winning_outcome_len]
        ).decode("utf-8")
        self.market_type = bytes(raw.market_type[: raw.market_type_len]).decode("utf-8")
        self.resolution_date = bytes(
            raw.resolution_date[: raw.resolution_date_len]
        ).decode("utf-8")
        self.delivery_path = DELIVERY_PATHS.get(raw.delivery_path)
        if self.delivery_path is None:
            _malformed(f"unknown delivery path discriminant {raw.delivery_path}")


class SegmentInfo:
    """A session's segment geometry and the publication generation observed at the read."""

    def __init__(self, raw):
        self.instance_id = (raw.instance_id_high << 64) | raw.instance_id_low
        self.segment_generation = raw.segment_generation
        self.publication_generation = raw.publication_generation
        self.directory_capacity = raw.directory_capacity
        self.state_slot_capacity = raw.state_slot_capacity
        self.level_capacity = raw.level_capacity
        self.event_capacity = raw.event_capacity


class EventStream:
    """A market's retained-mutation cursor, attached by `Market.attach`."""

    def __init__(self, market):
        self._market = market

    def next_event(self):
        """The next retained mutation or resolution, or `None` when the writer has not
        reached it yet.

        Returns a :class:`Mutation` for a delivered level mutation and a :class:`Resolution`
        for a delivered venue-reported market resolution. Raises :class:`PmwsContinuityLost`
        on a loss -- sticky, repeated on every later call until :meth:`reattach`. Raises
        :class:`PmwsError` carrying ``PMWS_STATUS_MALFORMED_RECORD`` for any other delivery
        kind. Raises :class:`PmwsSessionClosed` if the owning `Segment` has been closed.
        """
        segment = self._market._segment
        event = PmwsEvent()
        status = segment._call(
            _lib.pmws_next_event, self._market._index, ctypes.byref(event)
        )
        if status == PMWS_STATUS_NONE:
            return None
        if status == PMWS_STATUS_OK:
            if event.delivery_kind == DELIVERY_MUTATION:
                return Mutation(event)
            if event.delivery_kind == DELIVERY_RESOLUTION:
                return Resolution(event)
            _malformed(f"unsupported delivery kind {event.delivery_kind}")
        if status == PMWS_STATUS_CONTINUITY_LOST:
            reason = CONTINUITY_REASONS.get(event.continuity_reason)
            if reason is None:
                _malformed(f"unknown continuity reason {event.continuity_reason}")
            raise PmwsContinuityLost(reason)
        raise PmwsError(status)

    def reattach(self):
        """Re-establishes this stream past a continuity loss; returns the resumed state."""
        return self._market._read(_lib.pmws_reattach)


class Market:
    """One book resolved in a :class:`Segment`, addressed by a session-local index.

    `directory_index` is the segment's own directory index for this book -- the number
    :meth:`Segment.next_dirty` delivers -- and is what maps a dirty entry back to a market.
    It is not `index`: `index` is a dense row number the session hands out in resolution
    order, so a session that resolves the segment's third market first holds `index` 0 for
    `directory_index` 2. Both are fixed for the life of the session.
    """

    def __init__(self, segment, index, directory_index, venue, kind, key):
        self._segment = segment
        self._index = index
        self._level_capacity = segment.info.level_capacity
        self.directory_index = directory_index
        self.venue = venue
        self.kind = kind
        self.key = key

    def identity(self):
        """This market's venue-native `(venue, kind, key)`, echoed from the segment."""
        cap = 256
        while True:
            buf = ctypes.create_string_buffer(cap)
            spans = PmwsIdentitySpans()
            status = self._segment._call(
                _lib.pmws_market_identity, self._index, buf, cap, ctypes.byref(spans)
            )
            if status == PMWS_STATUS_OK:
                raw = buf.raw
                return (
                    raw[spans.venue_offset: spans.venue_offset + spans.venue_len].decode(
                        "utf-8"
                    ),
                    raw[spans.kind_offset: spans.kind_offset + spans.kind_len].decode("utf-8"),
                    raw[spans.key_offset: spans.key_offset + spans.key_len].decode("utf-8"),
                )
            if status == PMWS_STATUS_BUFFER_TOO_SMALL:
                cap = spans.venue_len + spans.kind_len + spans.key_len
                continue
            raise PmwsError(status)

    def attach(self):
        """Attaches this market's latest state and its mutation stream as one step."""
        state = self._read(_lib.pmws_attach)
        return state, EventStream(self)

    def read_state(self):
        """This market's latest published state, without moving any attached stream."""
        return self._read(_lib.pmws_read_state)

    def _read(self, entry_point):
        capacity = self._level_capacity
        while True:
            out = PmwsState()
            levels = (PmwsLevel * capacity)()
            status = self._segment._call(
                entry_point, self._index, ctypes.byref(out), levels, capacity
            )
            if status == PMWS_STATUS_OK:
                return BookState(out, levels[: out.level_count])
            if status == PMWS_STATUS_BUFFER_TOO_SMALL:
                capacity = out.level_capacity_required
                continue
            raise PmwsError(status)


class Segment:
    """An attached, read-only publication segment: open/close, and the markets in it.

    Thread-safe by serialization, not by native concurrency: the Rust `PmwsSession` behind
    this segment is deliberately ``!Sync`` (its market table is a `RefCell`), and ctypes
    releases the GIL around every foreign call, so two Python threads sharing one `Segment`
    without help could race that `RefCell` -- undefined behavior. This class carries one
    `threading.Lock`, held only for the duration of a single native call, and every
    operation that passes the session pointer into the library -- `segment_info`, `resolve`,
    `publication_generation`, `wait`, `attach`, `read_state`, `reattach`, `next_event`,
    `next_dirty`, `directory_index`, `identity`, `close` -- goes through :meth:`_call` (or
    `close`'s own copy of the same pattern) to acquire it. There is one native session per `Segment`; the lock does not
    protect any compound operation spanning more than one call.

    :meth:`wait` holds it for its whole native call like every other operation, which is
    what keeps a concurrent `close()` from freeing the session out from under a parked
    wait -- see both docstrings for what that costs.

    A `Market` or `EventStream` resolved from this segment reaches the native handle only
    through this same `Segment` object -- never a copied pointer -- so `close()` on any
    thread is immediately visible everywhere: a later operation through any of them raises
    :class:`PmwsSessionClosed` instead of touching freed native memory.
    """

    def __init__(self, path):
        self._lock = threading.Lock()
        path_bytes = os.fspath(path).encode("utf-8")
        handle = ctypes.POINTER(PmwsSession)()
        status = _lib.pmws_open(path_bytes, len(path_bytes), ctypes.byref(handle))
        if status != PMWS_STATUS_OK:
            raise PmwsError(status)
        self._handle = handle
        self.info = self.segment_info()

    @classmethod
    def connect(cls, control_socket, market):
        """Attaches to the segment carrying `market` by asking the `pmwsd` listening on the
        Unix socket at `control_socket` for it, over descriptor transfer.

        No path to a segment is named, opened, or needed: the daemon sends the segment's own
        read-only descriptor -- the one it opened when it created the file -- on the same
        message as its answer, and possession of that descriptor is the authorization.

        **The caller must run as the same operating-system user as the daemon.** A peer
        running as another user is refused before the daemon discloses anything, and raises
        :class:`PmwsError` carrying ``PMWS_STATUS_ATTACH_REFUSED``; peer identity is a check
        on top of the segment's own file permissions, never a replacement for them.

        **On a page-placement segment, a segment reached this way spins or polls; it does not
        park.** The doorbell page's descriptor is never transferred -- parking needs a writable
        mapping, and a writable mapping carries a writable length, so that descriptor would let
        any holder truncate a file the daemon stores through. :meth:`wait` on such a segment
        raises :class:`PmwsError` carrying ``PMWS_STATUS_DOORBELL_UNAVAILABLE`` on its first
        park rather than blocking; poll :meth:`publication_generation`, or spin with a
        ``spin_micros`` budget, instead, or use :class:`Segment` on a path (same user as the
        daemon) when parked waiting is wanted. ``segment.info`` does not distinguish the two
        placements; a wait that raises does.

        **The attachment leases the market.** Connecting takes a lease on `market` for as
        long as this segment is open: a market no other consumer holds and no operator pinned
        is subscribed at the venue to serve this call, and released when the last lease on it
        goes. :meth:`close` -- and this process exiting, however it exits -- releases it.
        Leases are counted, so a second consumer of the same market costs no venue traffic and
        keeps the market alive after the first one leaves. An operator's pin outlives every
        lease.

        A daemon configured with a lease TTL also expects to hear from this connection within
        it; :meth:`renew` is how a consumer with nothing else to say says something. A daemon
        configured without one -- the default -- needs no renewals at all.

        The result is an ordinary :class:`Segment` covering every market in that shard's
        segment, not only the one named: resolve any of them on it.

        The daemon answers as soon as it holds the market, which may be before the venue has
        said anything about it: the book reads as synchronizing until its first venue base
        lands, exactly as any quiet market's does.

        Blocks for the length of one control conversation, bounded by one five-second deadline
        over the exchange rather than per step, so a daemon answering slowly cannot extend it.
        The deadline runs from the established connection; connecting to the socket is the one
        step outside it, because no timed connect exists for a Unix domain socket. Raises
        :class:`PmwsError` carrying
        ``PMWS_STATUS_IO`` when the socket cannot be reached, ``PMWS_STATUS_MARKET_NOT_FOUND``
        when the daemon holds no such market, ``PMWS_STATUS_ATTACH_INCOMPLETE`` when the
        descriptors did not arrive as the answer promised, and
        ``PMWS_STATUS_SEGMENT_INCOMPATIBLE`` when the segment is not the one promised.
        """
        segment = cls.__new__(cls)
        segment._lock = threading.Lock()
        socket_bytes = os.fspath(control_socket).encode("utf-8")
        market_bytes = market.encode("utf-8")
        handle = ctypes.POINTER(PmwsSession)()
        status = _lib.pmws_connect(
            socket_bytes, len(socket_bytes), market_bytes, len(market_bytes),
            ctypes.byref(handle),
        )
        if status != PMWS_STATUS_OK:
            raise PmwsError(status)
        segment._handle = handle
        segment.info = segment.segment_info()
        return segment

    def _call(self, fn, *args):
        """Invokes `fn(handle, *args)` with `handle` this segment's native session pointer,
        serialized by `self._lock` and refused with :class:`PmwsSessionClosed` once
        `close()` has nulled the handle.

        Every FFI entry point that takes a session pointer as its first argument -- called
        directly here, or via `Market`/`EventStream` through `self._segment`/`self._market`
        -- goes through this one choke point, so no call site can forget the lock or the
        closed check.
        """
        with self._lock:
            if self._handle is None:
                raise PmwsSessionClosed()
            return fn(self._handle, *args)

    def renew(self):
        """Renews this segment's market leases at the daemon that granted them.

        One line out and one line back on the control connection :meth:`connect` opened,
        bounded by the same five-second deadline the attach conversation is. It carries no
        market data and moves no cursor.

        Only for a segment from :meth:`connect`: a segment opened from a path holds no lease,
        because nothing granted it one, and renewing it raises :class:`PmwsError` carrying
        ``PMWS_STATUS_INVALID_ARGUMENT``. ``PMWS_STATUS_IO`` means the control connection is
        gone, and with it this segment's leases -- the mapping stays readable, and what it
        reports from then on is a book the daemon may have stopped maintaining.

        Needed only against a daemon configured with a lease TTL. Sending one anyway is
        harmless: any request on the connection renews it, and a daemon with no TTL answers
        this one like any other.
        """
        status = self._call(_lib.pmws_renew)
        if status != PMWS_STATUS_OK:
            raise PmwsError(status)

    def lease(self, market):
        """Takes a further market lease on the control connection :meth:`connect` opened, for
        a market in this segment's own shard.

        One line out and one line back, bounded by the same five-second deadline the attach
        conversation is -- one budget for the whole call, the rollback below included. This
        segment already maps every market in its shard, so nothing new
        is mapped and nothing is returned: what the call buys is the *demand*, so the daemon
        keeps `market` subscribed for this session. Resolve it afterwards as any other market
        in the segment. A session may hold any number of leases this way; :meth:`release`
        gives one back and :meth:`close` gives back all of them.

        Raises :class:`PmwsError` carrying ``PMWS_STATUS_FOREIGN_SEGMENT`` when `market` lives
        in another shard: its book is in a segment this session does not map, so the lease is
        handed back -- and confirmed back -- and a consumer that wants that market opens a
        second :class:`Segment` on it with :meth:`connect`. ``PMWS_STATUS_INVALID_ARGUMENT``
        for a segment opened from a path -- which has no control connection to lease on -- or
        an identifier the daemon rejects, ``PMWS_STATUS_ATTACH_REFUSED`` when the daemon
        refuses the request, including for want of room to take the market,
        ``PMWS_STATUS_MARKET_NOT_FOUND`` when the daemon answers that it does not hold it,
        ``PMWS_STATUS_ATTACH_INCOMPLETE`` when the answer's descriptors did not arrive as
        promised, and ``PMWS_STATUS_IO`` when the conversation fails or a rollback goes
        unconfirmed.

        The two that end the conversation -- ``PMWS_STATUS_IO`` and
        ``PMWS_STATUS_ATTACH_INCOMPLETE`` -- end the control connection with it, and with it
        every lease this segment held: later :meth:`lease`, :meth:`release` and :meth:`renew`
        calls raise ``PMWS_STATUS_INVALID_ARGUMENT``, as they do for a segment opened from a
        path. The refusals leave the connection usable. The mapping stays readable either
        way.
        """
        market_bytes = market.encode("utf-8")
        status = self._call(_lib.pmws_lease, market_bytes, len(market_bytes))
        if status != PMWS_STATUS_OK:
            raise PmwsError(status)

    def release(self, market):
        """Gives up this segment's lease on `market`, keeping the session, its other leases,
        and its mapping.

        One line out and one line back, bounded by the same five-second deadline. Idempotent:
        releasing a market this session never leased, or releasing one twice, is a success.

        The mapping is untouched -- releasing even the market :meth:`connect` was called with
        is legal, and the segment stays readable afterwards. What goes away is this session's
        demand: once nothing else holds `market` and no operator pinned it, the daemon
        unsubscribes it at the venue and this segment keeps reading a book that has stopped
        being maintained.

        Raises :class:`PmwsError` carrying ``PMWS_STATUS_INVALID_ARGUMENT`` for a segment
        opened from a path, which holds no lease to give back, or an identifier the daemon
        rejects, and ``PMWS_STATUS_IO`` when the conversation fails, which means the control
        connection is gone and with it every lease this segment held: later :meth:`lease`,
        :meth:`release` and :meth:`renew` calls raise ``PMWS_STATUS_INVALID_ARGUMENT``, and the
        mapping stays readable. A rejected identifier is not that -- the conversation finished,
        and the connection carries on.
        """
        market_bytes = market.encode("utf-8")
        status = self._call(_lib.pmws_release, market_bytes, len(market_bytes))
        if status != PMWS_STATUS_OK:
            raise PmwsError(status)

    def close(self):
        """Releases this segment's market leases and frees the native session.

        Idempotent: frees the native session at most once and nulls the shared handle so
        every `Market`/`EventStream` resolved from this segment sees the close -- a later
        operation through any of them raises :class:`PmwsSessionClosed` rather than reaching
        freed memory. A second `close()` is a silent no-op, matching a Python file object.

        For a segment from :meth:`connect` this closes the control connection too, which is
        what releases its leases: a market nothing else holds is unsubscribed at the venue
        shortly after. Keeping the mapping alive without the session does not keep the
        subscription alive -- it keeps a reader on a book that stops being maintained.

        Blocks while another thread is inside :meth:`wait`, for as long as that wait has
        left to run. It has to: freeing the session unmaps the very segment the parked
        wait's kernel-side address lives in, and the C ABI has no way to interrupt a wait
        in flight. A closer that must not block behind a waiter is a closer whose waiters
        must pass a finite `timeout_ms` -- an indefinite wait makes `close()`, and the
        `__del__` that calls it, wait forever.
        """
        with self._lock:
            if self._handle is None:
                return
            handle = self._handle
            self._handle = None
            _lib.pmws_close(handle)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def segment_info(self):
        """This session's segment geometry and the publication generation at this read."""
        raw = PmwsSegmentInfo()
        status = self._call(_lib.pmws_segment_info, ctypes.byref(raw))
        if status != PMWS_STATUS_OK:
            raise PmwsError(status)
        return SegmentInfo(raw)

    def publication_generation(self):
        """A coalescible hint that newer data exists somewhere in the segment."""
        return self._call(_lib.pmws_publication_generation)

    def wait(self, last_generation, spin_micros=0, timeout_ms=None):
        """Blocks until :meth:`publication_generation` changes from `last_generation`,
        returning the new generation, or blocks for `timeout_ms` milliseconds and returns
        `None` if it never does. `timeout_ms=None` parks indefinitely.

        Spins for up to `spin_micros` microseconds with no syscall before parking on the
        segment's doorbell -- `spin_micros=0` is pure parked mode.

        Parking needs a doorbell this session can reach. A segment opened by path always has
        one; a segment obtained from :meth:`connect` has one only where the platform puts the
        doorbell in the segment header, because the sibling-page placement is never transferred
        over the control channel. Where it cannot park, the park itself raises
        :class:`PmwsError` -- it never blocks and never reports a change that did not happen --
        so a consumer that wants to work under either placement polls
        :meth:`publication_generation` or passes a `spin_micros` budget and treats the raise as
        "this attachment spins".

        Holds `self._lock` for the whole native call, exactly as every other operation
        does. That is the point rather than an oversight: releasing it would let a
        concurrent `close()` free the session and unmap the segment while this thread is
        parked on an address inside it, which is a use-after-free the native side cannot
        defend against. The cost is that `close()` -- and `__del__` -- block until this
        wait's own timeout expires, so a segment that is closed from another thread should
        be waited on with a finite `timeout_ms` and a re-check loop, never `None`.

        Every argument is range-checked here rather than handed to `ctypes`, whose
        narrowing is silent: `spin_micros=-1` would otherwise reach the C ABI as
        `4294967295` and ask this process to spin for seventy-one minutes.

        Raises `TypeError` for a non-integer argument and `ValueError` for one outside
        `0 <= last_generation < 2**64`, `0 <= spin_micros <= PMWS_MAX_SPIN_MICROS`, or
        `0 <= timeout_ms <= PMWS_MAX_TIMEOUT_MS`.
        """
        _check_range("last_generation", last_generation, 0, _MAX_GENERATION)
        _check_range("spin_micros", spin_micros, 0, PMWS_MAX_SPIN_MICROS)
        if timeout_ms is None:
            timeout_millis = -1
        else:
            _check_range("timeout_ms", timeout_ms, 0, PMWS_MAX_TIMEOUT_MS)
            timeout_millis = timeout_ms
        generation = ctypes.c_uint64()
        status = self._call(
            _lib.pmws_wait,
            ctypes.c_uint64(last_generation),
            ctypes.c_uint32(spin_micros),
            ctypes.c_int32(timeout_millis),
            ctypes.byref(generation),
        )
        if status == PMWS_STATUS_OK:
            return generation.value
        if status == PMWS_STATUS_NONE:
            return None
        raise PmwsError(status)

    def next_dirty(self):
        """The next dirty-index ring entry as `(directory_index, book_revision)`, or `None`
        when the writer has not reached this session's cursor position yet.

        `directory_index` is the segment's own directory index for the changed market, not
        a session-local `Market` index. To learn which of its markets an entry named, a
        consumer builds the inverse map once from `Market.directory_index` over the markets
        it resolved; nothing in a delivered entry carries the session-local index.
        `book_revision` is a state-read skip hint only -- a resolution republish advertises
        an unchanged revision, so the caller must still poll that market's event stream on
        every delivered entry.

        Raises :class:`PmwsDirtyRescan` -- never sticky -- when the ring lapped this
        session's cursor; the cursor has already rebased to the ring's current head, so the
        very next call resumes ordinary polling.
        """
        directory_index = ctypes.c_uint32()
        book_revision = ctypes.c_uint64()
        status = self._call(
            _lib.pmws_next_dirty, ctypes.byref(directory_index), ctypes.byref(book_revision)
        )
        if status == PMWS_STATUS_OK:
            return (directory_index.value, book_revision.value)
        if status == PMWS_STATUS_NONE:
            return None
        if status == PMWS_STATUS_CONTINUITY_LOST:
            raise PmwsDirtyRescan()
        raise PmwsError(status)

    def resolve(self, venue, kind, key):
        """Resolves a venue-native identity to a :class:`Market` in this session."""
        venue_bytes = venue.encode("utf-8")
        kind_bytes = kind.encode("utf-8")
        key_bytes = key.encode("utf-8")
        index = ctypes.c_uint32()
        status = self._call(
            _lib.pmws_resolve,
            venue_bytes, len(venue_bytes),
            kind_bytes, len(kind_bytes),
            key_bytes, len(key_bytes),
            ctypes.byref(index),
        )
        if status != PMWS_STATUS_OK:
            raise PmwsError(status)
        directory_index = ctypes.c_uint32()
        status = self._call(
            _lib.pmws_market_directory_index, index.value, ctypes.byref(directory_index)
        )
        if status != PMWS_STATUS_OK:
            raise PmwsError(status)
        return Market(self, index.value, directory_index.value, venue, kind, key)

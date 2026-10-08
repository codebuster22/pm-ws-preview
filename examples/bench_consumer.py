#!/usr/bin/env python3
"""The S8 full-fleet benchmark consumer, over the pm-ws Python binding
(`bindings/python/pmws.py`).

Leases half of a live `pmwsd`'s open market set through the control channel, parks on the
segment's doorbell, and reports the socket-arrival -> consumer-observable latency
distribution for the markets it holds, while churning with the venue: a market whose
resolution this process observes has its lease released immediately, and a market the runner
appends to the slug file mid-run is leased mid-run. Nothing here publishes, and nothing here
talks to a venue -- the daemon owns every venue connection.

Measurement semantics are `examples/latency_probe.rs`'s, deliberately: the same
discard-not-clamp rule on a delta, the same nearest-rank quantiles over nanoseconds, the same
1000-sample floor below which percentiles are withheld rather than printed, and the same
`wakes`/`timeouts`/`rescans`/`dirty_delivered`/`samples_*` accounting. Percentiles are printed
as whole microseconds (truncated), where the probe prints three decimals; the underlying
computation is identical, so a number here and a number there are the same statistic
formatted differently.

Report compatibility: this report is `latency_probe`'s shape, not its schema. Percentiles are
whole microseconds where the probe prints three decimals, and these keys have no probe
equivalent at all: `consumer`, `runtime`, `clock`, `lease_ttl_ms`, `renew_interval_ms`,
`anchor_market`, `markets_leased_peak`, `markets_held`, `dirty_delivered_ours`,
`continuity_losses`, `anchor_renewals`, `anchor_renew_failures`, `lease_attach_failures`,
`resolutions_observed`, `leases_released_on_resolution`, `anchor_resolutions_unreleased`,
`slugs_added_midrun`, `control_stall_count`, `control_stall_total_ms`, `control_stall_max_ms`,
`samples_after_control`, `transient_event_faults`, and the whole
`daemon_`/`consumer_`/`end_to_end` split described below.
Read the two reports side by side, never diffed key for key.

Three distributions are reported. `daemon_*` is `commit_time - arrival_time`, `consumer_*` is
`observation - commit_time`, and the unprefixed keys (`samples_kept`, `p50_us` through
`max_us`) remain the end-to-end `observation - arrival_time` they have always been. Each is
ordered independently, so their percentiles do not add, even though per observation
`daemon + consumer == end_to_end` exactly.

**Clock domain.** The daemon stamps `arrival_time` with
`SystemTime::now().duration_since(UNIX_EPOCH)` (`wall_clock_arrival_nanos` in
`src/limitless/connection.rs`) -- CLOCK_REALTIME nanoseconds since the Unix epoch. This
process observes with `time.time_ns()`, which is that same clock on both benchmark hosts
(CPython documents it as the realtime clock, and it reads identically to
`time.clock_gettime_ns(time.CLOCK_REALTIME)` on macOS and Linux). No conversion, no
calibration, and no monotonic clock appears anywhere on the measured path; `time.monotonic()`
is used only for this process's own deadlines, never for a sample. `examples/bench_consumer.ts`
has to derive the same domain because Node exposes no epoch-nanosecond clock -- see its own
clock note for the correction it applies and what that costs.

One control session for the whole run: `connect()` takes the anchor market's lease and opens
the connection, and every further market is leased on that same connection with `lease()` and
given back with `release()` -- one more message on a socket this process already holds open,
not a fresh connection and handshake. That is cheap enough to run inline in the loop that
parks on the segment's doorbell, exactly as the anchor's own renewal always has; there is no
longer a separate lease-keeper thread, because the traffic `latency_probe`'s keeper thread
existed to keep off the measurement thread no longer exists as a distinct cost.
`examples/bench_consumer.ts` cannot fold its own keeper thread away the same way -- its clock
calibration is genuinely independent of any session -- so it keeps a thread for that alone;
see its own note.

Lease renewal follows the probe's rule: renew at `--lease-ttl-ms` / 3, clamped to
[100 ms, 5 s]. The C ABI does not surface the TTL the daemon declared in its attach answer
(`pmws_renew` says so, and says what a consumer that cannot see it must do instead), so the
runner passes the TTL it configured. Without the flag this falls back to the documented blind
rule -- a fraction of `MIN_LEASE_TTL_MS` -- which is safe against any TTL an operator may set
and costs more renewal traffic than a consumer that knows the number.

Usage:
    bench_consumer.py --control <socket-path> --slugs <file> --seconds <n> --label <text>
                      [--lease-ttl-ms <n>] [--spin-micros 0] [--rescan-ms 2000]
                      [--venue limitless] [--kind slug] [--spin-only]
                      [--until-resolutions <n>] [--hold-until <path>]

`--until-resolutions` ends the measurement loop as soon as that many resolutions have been
observed, instead of at `--seconds`, and `--hold-until` keeps the process alive -- still
holding every lease it has not released -- until the named path appears. Neither is used by a
benchmark run; together they are what lets a deterministic test drive this consumer by events
rather than by the clock, and inspect the released leases against the held ones while the
process is still there to hold them.

`--slugs` names an append-only file, one venue-native market key per line; blank lines and
lines beginning with `#` are ignored. The runner grows that file to lease newly listed
markets mid-run: this process re-reads it every `--rescan-ms` and leases whatever is new.
The file must only ever be appended to -- a rewritten file renumbers the markets already
leased, and this process would then lease a market twice and never release the other.
"""

import argparse
import os
import platform
import signal
import subprocess
import sys
import tempfile
import time
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "bindings" / "python"))
import pmws

IMPLAUSIBLE_NANOS = 1_000_000_000
MIN_REPORTABLE_SAMPLES = 1_000
PARK_POLL_TIMEOUT_MS = 100
PENDING_POLL_TIMEOUT_MS = 5
PENDING_EAGER_SECONDS = 2.0
MAX_SECONDS = 86_400
MAX_OBS_ROWS = 4_000_000

MIN_LEASE_TTL_MS = 1_000
RENEW_TTL_DIVISOR = 3
RENEW_FLOOR_MS = 100
RENEW_CEILING_MS = 5_000

ATTACH_SETTLE_SECONDS = 15.0
HOLD_POLL_SECONDS = 0.02
HOLD_CAP_SECONDS = 120.0

TRANSIENT = (
    pmws.PMWS_STATUS_CONTENDED,
    pmws.PMWS_STATUS_WRITER_STALLED,
    pmws.PMWS_STATUS_NO_PUBLISHED_STATE,
)

POISON = (
    pmws.PMWS_STATUS_IO,
    pmws.PMWS_STATUS_ATTACH_INCOMPLETE,
)


class BenchError(Exception):
    """A condition this consumer refuses to measure through, named for the runner."""


def renew_interval_seconds(lease_ttl_ms):
    """The renewal period for a daemon whose lease TTL is `lease_ttl_ms`, in seconds, or
    `None` when no renewal is needed at all.

    `None` means the caller did not say, and the interval is then the blind rule `pmws_renew`
    documents for a consumer that cannot see its daemon's configuration: a fraction of
    `MIN_LEASE_TTL_MS`, safe against the shortest TTL an operator may set. `0` means the
    daemon is configured to expire nothing, so a lease lives exactly as long as its
    connection and no renewal is needed. Any other TTL renews at a third of it, clamped to
    [100 ms, 5 s] -- the rule `examples/latency_probe.rs` applies to the TTL its own attach
    answer declared.
    """
    if lease_ttl_ms == 0:
        return None
    declared = MIN_LEASE_TTL_MS if lease_ttl_ms is None else lease_ttl_ms
    interval_ms = max(RENEW_FLOOR_MS, min(RENEW_CEILING_MS, declared // RENEW_TTL_DIVISOR))
    return interval_ms / 1000.0


MEASURED = "measured"
IMPLAUSIBLE = "implausible"
ABSENT = "absent"


def difference(start, end):
    """`end - start` as `(MEASURED, delta)`, or `(IMPLAUSIBLE, 0)` when both stamps are there
    and their difference is one no delivery path can have taken, or `(ABSENT, 0)` when a stamp
    it needs is missing.

    The implausible case is what a clock adjustment between the two stamps produces; it is
    counted and discarded, never clamped, exactly as `examples/latency_probe.rs` treats one.
    """
    if start is None or end is None:
        return (ABSENT, 0)
    delta = end - start
    if 0 <= delta < IMPLAUSIBLE_NANOS:
        return (MEASURED, delta)
    return (IMPLAUSIBLE, 0)


def split_latencies(arrival, commit, observed):
    """One observation's three latencies, keyed `daemon`, `consumer` and `end_to_end`.

    `daemon` is `commit - arrival`: how long the daemon took to make a venue frame's state
    consumer-readable. Both of its stamps are written by the daemon, on one clock, in one
    process, so no consumer clock appears in it at all. `consumer` is `observed - commit` --
    how long this process took to make use of a state already published -- and `end_to_end` is
    `observed - arrival`, the number this consumer has always reported.

    Per observation `daemon + consumer == end_to_end` exactly whenever all three are measured,
    being three differences of the same three instants. Their percentiles do not add: each
    distribution is ordered on its own.
    """
    return {
        "daemon": difference(arrival, commit),
        "consumer": difference(commit, observed),
        "end_to_end": difference(arrival, observed),
    }


def quantile(sorted_samples, permille):
    """The `permille`-th value of a sorted nanosecond sample set, by nearest rank; 0 when
    empty. Identical to `examples/latency_probe.rs`'s own `quantile`.
    """
    if not sorted_samples:
        return 0
    count = len(sorted_samples)
    rank = max(-(-count * permille // 1000), 1) - 1
    return sorted_samples[min(rank, count - 1)]


def read_slug_file(path):
    """Every venue-native key the slug file names, in file order, without blanks or comments.

    A key's position in this list is its stable index for the run, which is what makes the
    file append-only rather than merely growing: a rescan tells an appended market from an
    already-tracked one by comparing lengths alone, never by diffing contents.
    """
    slugs = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if text and not text.startswith("#"):
                slugs.append(text)
    return slugs


def canonical_decimal(text):
    """The canonical form of a decimal lexeme, per `bench/sdk-harness/README.md`'s Content
    digest section: e-notation expanded to plain decimal first, a leading `+` stripped,
    redundant leading zeros stripped, and -- when a `.` is present -- trailing zeros then a
    trailing `.` stripped. Identical to the copy in `bench/sdk-harness/python/sdk_leg.py`.
    """
    value = text
    if "e" in value or "E" in value:
        value = format(Decimal(value), "f")
    sign = ""
    if value.startswith("+"):
        value = value[1:]
    elif value.startswith("-"):
        sign = "-"
        value = value[1:]
    if "." in value:
        int_part, frac_part = value.split(".", 1)
    else:
        int_part, frac_part = value, None
    int_part = int_part.lstrip("0") or "0"
    if frac_part is not None:
        frac_part = frac_part.rstrip("0")
        value = int_part if frac_part == "" else f"{int_part}.{frac_part}"
    else:
        value = int_part
    return sign + value


def fnv1a_64_hex(data):
    """FNV-1a 64-bit digest of `data` (UTF-8 bytes) as 16 lowercase hex digits. Identical to
    the copy in `bench/sdk-harness/python/sdk_leg.py`.
    """
    digest = 0xCBF29CE484222325
    for byte in data:
        digest ^= byte
        digest = (digest * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return format(digest, "016x")


def digest_book(bids, asks):
    """FNV-1a 64 digest of one book's content. `bids`/`asks` are each an iterable of
    `(price_numeric, price_text, qty_numeric, qty_text)`; zero-quantity levels are excluded,
    bids are ordered by price descending and asks ascending, and the digest input is `B` +
    `price:qty;` per bid + `|A` + `price:qty;` per ask, using canonical decimals. Identical
    to the copy in `bench/sdk-harness/python/sdk_leg.py`.
    """
    kept_bids = sorted(
        (level for level in bids if level[2]), key=lambda level: level[0], reverse=True
    )
    kept_asks = sorted((level for level in asks if level[2]), key=lambda level: level[0])
    parts = ["B"]
    for _, price_text, _, qty_text in kept_bids:
        parts.append(f"{canonical_decimal(price_text)}:{canonical_decimal(qty_text)};")
    parts.append("|A")
    for _, price_text, _, qty_text in kept_asks:
        parts.append(f"{canonical_decimal(price_text)}:{canonical_decimal(qty_text)};")
    return fnv1a_64_hex("".join(parts).encode("utf-8"))


CANONICAL_VECTORS = (
    ("0.530", "0.53"),
    ("1000000.0", "1000000"),
    ("100", "100"),
    ("0.5", "0.5"),
    ("1e-7", "0.0000001"),
    ("0.0", "0"),
)
GOLDEN_BIDS = ((Decimal("0.61"), "0.61", 12, "12"),)
GOLDEN_ASKS = ((Decimal("0.62"), "0.62", 13, "13"),)
GOLDEN_DIGEST = "a9c3f0bc66964c0a"


def _levels_by_side(levels):
    """Splits a `BookState`'s levels into `digest_book`'s `(price_numeric, price_text,
    qty_numeric, qty_text)` shape by side, keyed on the level's exact rendered `text` and its
    quantity's coefficient (zero coefficient means zero quantity regardless of scale).
    """
    bids = []
    asks = []
    for level in levels:
        entry = (
            Decimal(level.price.text),
            level.price.text,
            level.quantity.coefficient,
            level.quantity.text,
        )
        (bids if level.side == "Bid" else asks).append(entry)
    return bids, asks


def _pmws_git_rev():
    """The pm-ws git revision this process runs, or `"unknown"` when it cannot be read (no
    `git` binary, or this file running outside a checkout).
    """
    root = Path(__file__).resolve().parent.parent
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except OSError:
        return "unknown"
    if result.returncode != 0:
        return "unknown"
    return result.stdout.strip()


def write_obs_file(path, label, size, report):
    """Writes `report.obs_rows` as one `bench/sdk-harness/README.md` `.obs` file: the
    `py-binding` leg's observation log, pinned to this checkout's git revision rather than a
    package version, since this leg consumes the daemon and binding, not an SDK release.
    """
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("# pmws-obs v1\n")
        handle.write("# leg: py-binding\n")
        handle.write(f"# host: {label}\n")
        handle.write("# clock: epoch_ns\n")
        handle.write(f"# size: {size}\n")
        handle.write(f"# pin: {_pmws_git_rev()}\n")
        for slug, t_obs, digest, seq, revision in report.obs_rows:
            handle.write(f"obs {slug} {t_obs} {digest} {seq} rev={revision}\n")
        handle.write(f"# events_total: {len(report.obs_rows) + report.obs_dropped}\n")
        handle.write(f"# dropped: {report.obs_dropped}\n")


def _check_obs_file(path, expected_leg, expected_rows, expected_dropped):
    """Asserts one written `.obs` file matches `bench/sdk-harness/README.md`'s shape: the six
    header lines in order, one `obs` row per kept observation, and the two trailers last.
    """
    with open(path, "r", encoding="utf-8") as handle:
        lines = handle.read().splitlines()
    assert lines[0] == "# pmws-obs v1", lines[0]
    assert lines[1] == f"# leg: {expected_leg}", lines[1]
    assert lines[2].startswith("# host: "), lines[2]
    assert lines[3] == "# clock: epoch_ns", lines[3]
    assert lines[4].startswith("# size: "), lines[4]
    assert lines[5].startswith("# pin: "), lines[5]
    obs_lines = [line for line in lines if line.startswith("obs ")]
    assert len(obs_lines) == expected_rows, obs_lines
    for line in obs_lines:
        fields = line.split(" ")
        assert len(fields) >= 5, line
        assert len(fields[3]) == 16, line
    assert lines[-2].startswith("# events_total: "), lines[-2]
    assert lines[-1] == f"# dropped: {expected_dropped}", lines[-1]


def _run_digest_self_test():
    """Runs the canonical-decimal vectors and the shared digest golden value from
    `bench/sdk-harness/README.md`, proving this file's digest code agrees with
    `bench/sdk-harness/python/sdk_leg.py`'s copy, then exercises `write_obs_file` against a
    synthetic report so the one artifact the S8c matcher actually reads has run at least
    once. Touches no segment, no control socket, no network.
    """
    for text, expected in CANONICAL_VECTORS:
        got = canonical_decimal(text)
        if got != expected:
            print(
                f"FAIL canonical_decimal({text!r}) == {got!r}, expected {expected!r}",
                file=sys.stderr,
            )
            return 1
    digest = digest_book(GOLDEN_BIDS, GOLDEN_ASKS)
    if digest != GOLDEN_DIGEST:
        print(
            f"FAIL digest_book(...) == {digest!r}, expected {GOLDEN_DIGEST!r}",
            file=sys.stderr,
        )
        return 1
    report = Report()
    report.obs_rows = [
        ("golden-market", 1_700_000_000_000_000_000, digest, 0, 7),
        ("golden-market", 1_700_000_000_100_000_000, digest, 1, 8),
    ]
    report.obs_dropped = 3
    handle, path = tempfile.mkstemp(prefix="bench_consumer_self_test_", suffix=".obs")
    os.close(handle)
    try:
        write_obs_file(path, "self-test", 1, report)
        _check_obs_file(path, "py-binding", len(report.obs_rows), report.obs_dropped)
    finally:
        os.remove(path)
    print("digest self-test passed")
    print(f"golden digest: {digest}")
    return 0


class Report:
    """Everything one run measured, in the order :meth:`print_report` emits it."""

    def __init__(self):
        self.kept = []
        self.skipped = 0
        self.discarded = 0
        self.daemon_kept = []
        self.daemon_discarded = 0
        self.consumer_kept = []
        self.consumer_discarded = 0
        self.wakes = 0
        self.timeouts = 0
        self.delivered = 0
        self.delivered_ours = 0
        self.rescans = 0
        self.continuity_losses = 0
        self.resolutions = 0
        self.released_on_resolution = 0
        self.anchor_resolutions_unreleased = 0
        self.slugs_added_midrun = 0
        self.markets_seen = set()
        self.elapsed = 0.0
        self.samples_after_control = 0
        self.transient_event_faults = 0
        self.obs_rows = []
        self.obs_dropped = 0


class BenchConsumer:
    """The measurement thread: one session over the whole shard segment, every market of this
    consumer's half resolved on it, and one arrival-to-observation sample per state read.

    The session is a `connect()` on the *anchor* market -- the first key in the slug file --
    and it is held for the whole run, because closing it would unmap the segment this loop is
    parked on. That lease is therefore never released, not even when the anchor market
    resolves; the report says so on its own line rather than leaving the runner to notice a
    market whose lease count never reached zero. Every other market is leased on that same
    session with `lease()`, in this loop, the first time it is seen pending, and given back
    with `release()` the moment this loop observes that market's resolution -- there is no
    keeper thread and no second session for it.

    A refusal on that session -- the daemon has no room, a market lives on another shard -- is
    counted and the run continues. `PMWS_STATUS_IO` and `PMWS_STATUS_ATTACH_INCOMPLETE` are not
    a refusal: they are the control connection ending, taking every lease this session held
    with it, so `_lease`, `_poll_events`'s release, and `_renew_anchor` raise `BenchError` for
    those two instead of counting one more failure and continuing a run whose demand no longer
    exists.
    """

    def __init__(self, args):
        self.args = args
        self.report = Report()
        self.anchor_slug = None
        self.segment = None
        self.markets = {}
        self.streams = {}
        self.by_directory_index = {}
        self.pending_resolve = {}
        self.leased = set()
        self.released = set()
        self.last_sampled_revision = {}
        self.per_market_seq = {}
        self.pinned_size = 0
        self.mode_effective = "spin" if args.spin_only else "parked"
        self.note = None
        self.anchor_renewals = 0
        self.anchor_renew_failures = 0
        self.lease_attach_failures = 0
        self.leases_peak = 0
        self.control_stall_count = 0
        self.control_stall_total_ms = 0.0
        self.control_stall_max_ms = 0.0
        self._contamination_budget = 0

    def run(self, after_measurement):
        """Runs the whole benchmark, calling `after_measurement` with this run's report once
        the measurement loop has ended and while every lease this consumer took is still
        held.

        The report is emitted before the leases are dropped so an observer -- a test, or an
        operator watching `pmwsctl status` -- can see the released markets against the ones
        still held, which after this process exits it no longer can: exiting releases
        everything. `--hold-until` then keeps this process alive, still holding, until the
        named path appears.

        `self.pinned_size` is set to the slug file's line count read here, before this loop
        or a mid-run rescan can grow `self.markets` -- the rung's own size, for `--obs-out`'s
        `# size:` line, not however many markets this consumer went on to hold.
        """
        slugs = read_slug_file(self.args.slugs)
        if not slugs:
            raise BenchError(f"{self.args.slugs} names no market")
        self.pinned_size = len(slugs)
        self.anchor_slug = slugs[0]
        self.segment = pmws.Segment.connect(self.args.control, self.anchor_slug)
        try:
            self._resolve(self.anchor_slug)
            self._drain_loop(slugs)
            after_measurement(self.report)
            self._hold()
        finally:
            self.segment.close()
        return self.report

    def _hold(self):
        """Blocks until `--hold-until` exists, or returns at once when it was not given.

        Bounded by `HOLD_CAP_SECONDS` so a runner that never creates the path costs a run,
        not a wedged process.
        """
        if self.args.hold_until is None:
            return
        deadline = time.monotonic() + HOLD_CAP_SECONDS
        while not os.path.exists(self.args.hold_until):
            if time.monotonic() >= deadline:
                return
            time.sleep(HOLD_POLL_SECONDS)

    def _resolve(self, slug):
        """Resolves `slug` on the reader's session and attaches its stream, or returns False
        while the segment cannot serve it yet.

        A transient status is a "not yet", not a failure: the daemon installs a market's
        directory entry before it publishes that market's first book, so a session resolving
        one in the window between the two meets a writer mid-publish. The caller retries on
        its next pass, exactly as it retries a market the directory does not carry at all.
        """
        try:
            market = self.segment.resolve(self.args.venue, self.args.kind, slug)
            _state, stream = market.attach()
        except pmws.PmwsError as error:
            if error.code == pmws.PMWS_STATUS_MARKET_NOT_FOUND or error.code in TRANSIENT:
                return False
            raise
        self.markets[slug] = market
        self.streams[slug] = stream
        self.by_directory_index[market.directory_index] = slug
        return True

    def _drain_loop(self, slugs):
        start = time.monotonic()
        deadline = start + self.args.seconds
        for slug in slugs[1:]:
            self.pending_resolve[slug] = start
        last_generation = self.segment.publication_generation()
        next_rescan = start
        renew_interval = renew_interval_seconds(self.args.lease_ttl_ms)
        next_anchor_renew = start + renew_interval if renew_interval is not None else None
        while True:
            now = time.monotonic()
            if now >= deadline:
                break
            if self.args.until_resolutions and self.report.resolutions >= self.args.until_resolutions:
                break
            if now >= next_rescan:
                self._rescan(slugs, now)
                next_rescan = now + self.args.rescan_ms / 1000.0
            elif self.pending_resolve:
                self._resolve_pending(now)
            if next_anchor_renew is not None and now >= next_anchor_renew:
                self._renew_anchor()
                next_anchor_renew = time.monotonic() + renew_interval
            last_generation = self._one_round(
                last_generation, min(deadline, next_rescan), self._park_timeout_ms(now)
            )
        self.report.elapsed = time.monotonic() - start

    def _park_timeout_ms(self, now):
        """How long one park may block before this loop looks at its own housekeeping again.

        Short for the first `PENDING_EAGER_SECONDS` after a market is leased, so a market this
        session has just leased is resolved and its stream attached within milliseconds rather
        than at the next `--rescan-ms` tick. A market's first venue frames can arrive inside
        that window, and a stream attached after them starts past them: a resolution delivered
        in that gap would sit unread for the rest of the run and its lease would never be
        released.

        Bounded by that window rather than by "anything is pending" because a market that
        never appears -- one whose lease the daemon refused, or one this session is about to
        give up on -- would otherwise hold the whole run at a five-millisecond park cadence,
        and the wake and timeout counts the report prints would then describe a poll this
        benchmark never meant to measure.
        """
        eager = any(
            now - first_seen <= PENDING_EAGER_SECONDS
            for first_seen in self.pending_resolve.values()
        )
        return PENDING_POLL_TIMEOUT_MS if eager else PARK_POLL_TIMEOUT_MS

    def _renew_anchor(self):
        """Renews the one control session this consumer holds, on the reader's own thread.

        The one control syscall this consumer charges to the measurement thread -- and now
        the only one there is, since every market's lease lives on this same session rather
        than a keeper thread's own. `pmws.Segment.renew` renews every lease the session
        holds in one line out and one line back, so the anchor's attachment and every market
        leased since are all renewed by this one call. One renewal per
        `renew_interval_seconds` (at most one per 5 s against a daemon that declares a TTL,
        and none at all against one that declares none) is what that costs.

        `PMWS_STATUS_IO` and `PMWS_STATUS_ATTACH_INCOMPLETE` end the control connection and
        release every lease it held, so a renewal that raises either is fatal, not counted:
        this raises `BenchError` and ends the run rather than incrementing
        `anchor_renew_failures` for a session that no longer holds anything. Every other
        refusal is counted exactly as before, and so is a raw `OSError`, which carries no
        status to classify.
        """
        try:
            self.segment.renew()
            self.anchor_renewals += 1
        except pmws.PmwsError as error:
            if error.code in POISON:
                raise self._poisoned_error(error) from error
            self.anchor_renew_failures += 1
        except OSError:
            self.anchor_renew_failures += 1

    def _timed_control(self, action):
        """Runs one `lease()`/`release()` conversation on this session, timing its wall
        duration into `control_stall_count`/`control_stall_total_ms`/`control_stall_max_ms`
        and arming the drain-scoped exclusion `_observe` spends into `samples_after_control`.

        Both conversations run on this consumer's one session, inline on the measurement
        thread this loop parks on -- the one-session design this file commits to, and
        `renew()` already charges the same thread the same way -- so each one blocks the loop
        for however long the round trip takes. That delay is the benchmark's own control
        plane, not the daemon's delivery path. A single stall can delay observations from more
        than one market at once -- one blocked round trip, several dirty entries queued behind
        it -- so this arms `_contamination_budget` to 2 rather than counting one token per
        conversation: `_drain_dirty` spends one unit of that budget at the end of every drain
        pass it runs, which excludes the remainder of whatever pass is under way (if any)
        through the end of the next one, and a further conversation before that budget is
        spent simply re-arms it to 2 rather than stacking more passes on top. The timing and
        the re-armed budget are recorded whether or not `action` raises, because the loop was
        blocked either way; the caller's own exception handling is untouched.
        """
        start = time.monotonic()
        try:
            return action()
        finally:
            elapsed_ms = (time.monotonic() - start) * 1000.0
            self.control_stall_count += 1
            self.control_stall_total_ms += elapsed_ms
            self.control_stall_max_ms = max(self.control_stall_max_ms, elapsed_ms)
            self._contamination_budget = 2

    def _one_round(self, last_generation, round_deadline, park_timeout_ms):
        """One wait-then-drain pass, returning the generation the next pass waits against."""
        if self.mode_effective == "parked":
            try:
                generation = self.segment.wait(
                    last_generation, self.args.spin_micros, park_timeout_ms
                )
            except pmws.PmwsError as error:
                if error.code != pmws.PMWS_STATUS_DOORBELL_UNAVAILABLE:
                    raise
                self.mode_effective = "spin"
                self.note = (
                    "this attachment cannot park -- the segment's doorbell is on a sibling "
                    "page whose descriptor the control channel never transfers -- so the run "
                    "continued as a spin poll"
                )
                return last_generation
        else:
            generation = self._spin_poll(last_generation, round_deadline)
        if generation is None:
            self.report.timeouts += 1
            return last_generation
        self.report.wakes += 1
        self._drain_dirty()
        return generation

    def _spin_poll(self, last_generation, round_deadline):
        """A tight generation poll with no syscall, bounded by this round's own deadline so
        the caller still reaches its rescan and its run deadline.
        """
        while True:
            current = self.segment.publication_generation()
            if current != last_generation:
                return current
            if time.monotonic() >= round_deadline:
                return None

    def _drain_dirty(self):
        """Drains the dirty ring, observing each delivered entry that names one of this
        consumer's markets and ignoring the other consumer's half.

        A rescan re-reads this consumer's own interest set rather than the segment's whole
        directory, which is `latency_probe`'s recovery restricted to the markets this process
        actually holds: the other half's markets belong to the other consumer's report and
        sampling them here would count one delivery twice across the two reports.

        One call is one drain pass for `_contamination_budget`'s purposes: whatever budget a
        conversation armed is still in effect for every observation this pass makes, and the
        `finally` below spends exactly one unit of it on the way out, however the pass ends --
        ring exhausted, a rescan swept every market, or an exception. Spending one unit per
        pass, rather than per observation, is what turns a conversation's armed budget of 2
        into "the remainder of this pass, plus the whole of the next one."
        """
        try:
            while True:
                try:
                    entry = self.segment.next_dirty()
                except pmws.PmwsDirtyRescan:
                    self.report.rescans += 1
                    for slug in list(self.markets):
                        self._observe(slug)
                    return
                if entry is None:
                    return
                directory_index, _revision = entry
                self.report.delivered += 1
                slug = self.by_directory_index.get(directory_index)
                if slug is None:
                    continue
                self.report.delivered_ours += 1
                self.report.markets_seen.add(directory_index)
                self._observe(slug)
        finally:
            if self._contamination_budget > 0:
                self._contamination_budget -= 1

    def _observe(self, slug):
        """Reads one market's state, samples its arrival-to-observation delta unless this
        revision was already sampled, and then drains its event stream.

        A transient read failure is silently skipped exactly as `latency_probe`'s own
        `observe` skips one: the next delivered entry or rescan for the same market tries
        again, and neither `samples_skipped` nor `samples_discarded` -- both of which name an
        observation that was taken and rejected -- moves.

        Every observation made while `_contamination_budget` is armed is diverted to
        `samples_after_control` instead of `end_to_end`/`consumer`, whether or not the
        conversation that armed it actually overlapped the publication of the state being read
        here -- a conservative exclusion, not a proven one. A single stall can delay
        observations from more than one market at once, so this is not "the first observation
        after a conversation": it is every observation yielded from the moment a conversation
        completes through the end of the next drain pass (`_drain_dirty` owns spending the
        budget down), because that conversation blocked this loop on the measurement thread
        and any of those observations' `consumer`/`end_to_end` delta could measure this
        benchmark's own control-plane stall as much as it measures anything the daemon did.
        `daemon` -- `commit_time - arrival_time`, two daemon stamps this process never sat
        between -- is untouched, because no stall on this thread can have contributed to it.
        """
        market = self.markets[slug]
        try:
            state = market.read_state()
        except pmws.PmwsError as error:
            if error.code in TRANSIENT:
                return
            raise
        index = market.directory_index
        if self.last_sampled_revision.get(index) != state.revision:
            self.last_sampled_revision[index] = state.revision
            after_control = self._contamination_budget > 0
            t_obs = time.time_ns()
            split = split_latencies(state.arrival_time, state.commit_time, t_obs)
            if self.args.obs_out:
                self._record_obs(slug, state, t_obs)
            if after_control:
                self.report.samples_after_control += 1
            else:
                kind, value = split["end_to_end"]
                if kind == MEASURED:
                    self.report.kept.append(value)
                elif kind == IMPLAUSIBLE:
                    self.report.discarded += 1
                else:
                    self.report.skipped += 1
                kind, value = split["consumer"]
                if kind == MEASURED:
                    self.report.consumer_kept.append(value)
                elif kind == IMPLAUSIBLE:
                    self.report.consumer_discarded += 1
            kind, value = split["daemon"]
            if kind == MEASURED:
                self.report.daemon_kept.append(value)
            elif kind == IMPLAUSIBLE:
                self.report.daemon_discarded += 1
        self._poll_events(slug)

    def _record_obs(self, slug, state, t_obs):
        """Appends one `.obs` row for this sampled revision: `slug`, the same `t_obs` this
        observation's latency split was measured against, this book's content digest, this
        market's own 0-based sequence number, and `state.revision`.

        Recorded whether or not `_contamination_budget` diverted this same observation's
        latency into `samples_after_control` -- content matching and latency measurement are
        separate concerns, and a row this leg observed is a row the S8c matcher should be
        able to pair, contaminated latency or not. `match_events.py` sees no marker for it;
        a delta drawn from such a pair can carry this benchmark's own control-plane stall.

        Capped at `MAX_OBS_ROWS` in memory, exactly as `bench/sdk-harness/README.md`
        specifies; past the cap a row is counted in `obs_dropped` instead of kept, and the
        run continues -- this is bookkeeping alongside the measurement, never a reason to
        skip `_poll_events` or end the run.
        """
        seq = self.per_market_seq.get(slug, 0)
        self.per_market_seq[slug] = seq + 1
        bids, asks = _levels_by_side(state.levels)
        digest = digest_book(bids, asks)
        if len(self.report.obs_rows) < MAX_OBS_ROWS:
            self.report.obs_rows.append((slug, t_obs, digest, seq, state.revision))
        else:
            self.report.obs_dropped += 1

    def _poll_events(self, slug):
        """Drains this market's retained deliveries, releasing its lease on a resolution.

        Polled on every delivered dirty entry rather than only on a revision change, because
        a resolution republishes the state at an unchanged revision on purpose -- the dirty
        entry's `book_revision` is a state-read skip hint and never a reason to skip the
        stream.

        A market's lease is released once, on the first resolution delivered for it. Limitless
        was observed sending one market's `marketResolved` three times byte-identically inside
        200 ms, and no delivery identity exists to deduplicate them by, so every copy is
        counted in `resolutions_observed` and only the first releases anything.

        `PMWS_STATUS_IO` and `PMWS_STATUS_ATTACH_INCOMPLETE` from that release are fatal for
        the same reason they are in `_lease` and `_renew_anchor`: the control connection is
        gone, and with it every other lease this session held, so this raises `BenchError`
        rather than letting the raw `PmwsError` propagate as an unexplained crash. Every other
        `PmwsError` from the release keeps propagating exactly as before.

        `stream.next_event()` itself can fail transiently: `pmws_next_event`'s own contract
        (`src/ffi/mod.rs`) names `PMWS_STATUS_CONTENDED` and `PMWS_STATUS_WRITER_STALLED` as
        transient -- "poll again" -- against everything else, which is terminal -- "reattach
        or escalate". The check below is against this consumer's own `TRANSIENT`, which is
        those two plus `PMWS_STATUS_NO_PUBLISHED_STATE`. A transient failure here is counted
        in `transient_event_faults` and treated exactly like `event is None`: no event
        available right now, and a later dirty signal will revisit this market. Anything else
        keeps propagating.
        """
        stream = self.streams[slug]
        while True:
            try:
                event = stream.next_event()
            except pmws.PmwsContinuityLost:
                self.report.continuity_losses += 1
                try:
                    stream.reattach()
                except pmws.PmwsError as error:
                    if error.code not in TRANSIENT:
                        raise
                return
            except pmws.PmwsError as error:
                if error.code in TRANSIENT:
                    self.report.transient_event_faults += 1
                    return
                raise
            if event is None:
                return
            if isinstance(event, pmws.Resolution):
                self.report.resolutions += 1
                if slug == self.anchor_slug:
                    self.report.anchor_resolutions_unreleased += 1
                elif slug not in self.released:
                    self.released.add(slug)
                    try:
                        self._timed_control(lambda: self.segment.release(slug))
                    except pmws.PmwsError as error:
                        if error.code in POISON:
                            raise self._poisoned_error(error) from error
                        raise
                    self.leased.discard(slug)
                    self.report.released_on_resolution += 1

    def _rescan(self, slugs, now):
        """Picks up whatever the runner appended to the slug file and leases and resolves the
        markets that are new since the last rescan.

        Every held market's event stream is drained here as well as on its dirty entries.
        A resolution is the last thing a venue says about a market -- observed on Limitless,
        the market's whole flow stops at it -- so a market whose resolution shares a wake with
        its own last update would otherwise have that one delivery sitting unread in its ring
        for the rest of the run and its lease never released. This drain carries no state read
        and takes no sample, so it costs the distribution nothing.
        """
        current = read_slug_file(self.args.slugs)
        if len(current) > len(slugs):
            fresh = current[len(slugs):]
            self.report.slugs_added_midrun += len(fresh)
            for slug in fresh:
                self.pending_resolve[slug] = now
            slugs.extend(fresh)
        for slug in list(self.markets):
            self._poll_events(slug)
        self._resolve_pending(now)

    def _cross_shard_error(self, slug):
        """The one refusal family a market this session cannot ever serve raises, whether the
        daemon says so immediately (`_lease`, `PMWS_STATUS_FOREIGN_SEGMENT`) or only after this
        session gave up waiting for the market to resolve (`_resolve_pending`'s own timeout).
        """
        return BenchError(
            f"{slug} is leased but absent from this consumer's segment, so this half spans "
            "more than one shard; raise the daemon's markets_per_shard so the whole fleet "
            "lands in one segment, or split the halves by shard"
        )

    def _poisoned_error(self, error):
        """The fatal condition a poisoned control connection raises: `PMWS_STATUS_IO` or
        `PMWS_STATUS_ATTACH_INCOMPLETE` from any lease/release/renew conversation ends the
        control connection and releases every lease this session held with it, so this run's
        demand no longer exists and there is nothing left to report normally.
        """
        return BenchError(
            f"the control connection was lost ({error.name}): every lease this session held "
            "was released with it, so this run's demand no longer exists"
        )

    def _lease(self, slug):
        """Takes this session's lease on `slug`, the first time `slug` is seen pending.

        Returns whether the lease was taken. A market this shard's segment does not hold
        raises `PMWS_STATUS_FOREIGN_SEGMENT`, which the daemon has already handed the lease
        back for by the time this raises; that is the same "spans more than one shard"
        condition `_resolve_pending`'s own settle-timeout refuses further down, surfacing
        immediately here instead of only after a wasted wait. `PMWS_STATUS_IO` and
        `PMWS_STATUS_ATTACH_INCOMPLETE` are fatal for the same reason they are in
        `_renew_anchor`: both end the control connection and release every lease this session
        held, so this raises `BenchError` rather than counting one more attach failure against
        a session that no longer holds anything. Any other refusal -- the daemon has no room
        for the market, the connection is gone -- counts as an ordinary attach failure and
        gives up on `slug` for the rest of the run, exactly as a failed `connect()` used to.
        """
        try:
            self._timed_control(lambda: self.segment.lease(slug))
        except pmws.PmwsError as error:
            if error.code == pmws.PMWS_STATUS_FOREIGN_SEGMENT:
                raise self._cross_shard_error(slug) from error
            if error.code in POISON:
                raise self._poisoned_error(error) from error
            self.lease_attach_failures += 1
            del self.pending_resolve[slug]
            return False
        except OSError:
            self.lease_attach_failures += 1
            del self.pending_resolve[slug]
            return False
        self.leased.add(slug)
        self.leases_peak = max(self.leases_peak, len(self.leased))
        return True

    def _resolve_pending(self, now):
        """Leases and resolves every market this session cannot yet address.

        A market stays unresolvable until the daemon has installed it in the segment
        directory, which is why an unresolved key is retried rather than refused -- the same
        settle window a freshly leased market gets whether or not the daemon's `lease()`
        answer turns out to be immediately resolvable. A key still unresolved after
        `ATTACH_SETTLE_SECONDS` from the moment its lease was taken is a market this session
        was wrong to think shared its shard: one session covers one segment, and a second
        segment would need a second parked thread whose wakes are not this one's. A market
        refused as foreign at lease time never reaches this timeout at all; see `_lease`.
        """
        for slug, leased_at in list(self.pending_resolve.items()):
            if slug not in self.leased:
                if not self._lease(slug):
                    continue
                leased_at = now
                self.pending_resolve[slug] = now
            if self._resolve(slug):
                del self.pending_resolve[slug]
                self._observe(slug)
                continue
            if now - leased_at > ATTACH_SETTLE_SECONDS:
                raise self._cross_shard_error(slug)


def print_report(args, consumer, report):
    """Prints the run's report as `key: value` lines on stdout, `latency_probe`'s own shape.

    Percentiles are whole microseconds, truncated from the nanosecond samples the probe
    prints to three decimals; below `MIN_REPORTABLE_SAMPLES` kept samples none is printed at
    all, because a p99.9 resting on a handful of samples is two outliers wearing a
    distribution's name.

    `lease_renewals` and `lease_renew_failures` print a permanent zero: with one session,
    `_renew_anchor`'s single `renew()` call renews every lease the session holds, so there is
    no longer a separate per-market renewal cadence for these two keys to count, and they stay
    in the report -- under `anchor_renewals`/`anchor_renew_failures` instead -- so a reader
    diffing this report against an older one is not met with a missing key.

    `control_stall_count`/`control_stall_total_ms`/`control_stall_max_ms` are `_timed_control`'s
    own count and wall duration of every `lease()`/`release()` conversation this run made, and
    `samples_after_control` is how many observations `_observe` diverted away from
    `end_to_end`/`consumer` because a conversation's drain-scoped exclusion was still armed --
    every observation from the remainder of the drain pass a conversation completed in through
    the end of the next one, not just the first. Both exist so a control-plane stall this
    benchmark caused is visible as itself, never folded into a latency number that reads as the
    daemon's. A `PMWS_STATUS_IO`/`PMWS_STATUS_ATTACH_INCOMPLETE` conversation never reaches
    this report at all: the control connection, and every lease on it, is already gone, so
    `_lease`, `_poll_events`'s release, and `_renew_anchor` end the run through `BenchError`
    instead.
    """
    print(f"label: {args.label}")
    print("consumer: bench_consumer.py")
    print(f"runtime: python {platform.python_version()}")
    print(f"mode: {'spin' if args.spin_only else 'parked'}")
    if consumer.mode_effective != ("spin" if args.spin_only else "parked"):
        print(f"mode_effective: {consumer.mode_effective}")
    if consumer.note is not None:
        print(f"note: {consumer.note}")
    print(f"spin_budget_us: {args.spin_micros}")
    print("clock: CLOCK_REALTIME epoch nanoseconds, read directly (time.time_ns)")
    print(f"lease_ttl_ms: {'unspecified' if args.lease_ttl_ms is None else args.lease_ttl_ms}")
    interval = renew_interval_seconds(args.lease_ttl_ms)
    print(f"renew_interval_ms: {'none' if interval is None else int(interval * 1000)}")
    print(f"duration_seconds: {report.elapsed:.3f}")
    print(f"anchor_market: {consumer.anchor_slug}")
    print(f"markets_leased_peak: {consumer.leases_peak + 1}")
    print(f"markets_held: {len(consumer.markets)}")
    print(f"markets_seen: {len(report.markets_seen)}")
    print(f"samples_kept: {len(report.kept)}")
    if len(report.kept) < MIN_REPORTABLE_SAMPLES:
        print(
            f"percentiles: withheld, only {len(report.kept)} kept samples "
            f"(floor is {MIN_REPORTABLE_SAMPLES})"
        )
    else:
        ordered = sorted(report.kept)
        print(f"p50_us: {quantile(ordered, 500) // 1000}")
        print(f"p95_us: {quantile(ordered, 950) // 1000}")
        print(f"p99_us: {quantile(ordered, 990) // 1000}")
        print(f"p99.9_us: {quantile(ordered, 999) // 1000}")
        print(f"max_us: {ordered[-1] // 1000}")
    print(f"wakes: {report.wakes}")
    rate = report.wakes / report.elapsed if report.elapsed > 0 else 0.0
    print(f"wakes_per_second: {rate:.3f}")
    print(f"timeouts: {report.timeouts}")
    print(f"dirty_delivered: {report.delivered}")
    print(f"dirty_delivered_ours: {report.delivered_ours}")
    print(f"rescans: {report.rescans}")
    print(f"samples_skipped: {report.skipped}")
    print(f"samples_discarded: {report.discarded}")
    print(f"samples_after_control: {report.samples_after_control}")
    print(f"transient_event_faults: {report.transient_event_faults}")
    print(f"continuity_losses: {report.continuity_losses}")
    print("lease_renewals: 0")
    print("lease_renew_failures: 0")
    print(f"anchor_renewals: {consumer.anchor_renewals}")
    print(f"anchor_renew_failures: {consumer.anchor_renew_failures}")
    print(f"lease_attach_failures: {consumer.lease_attach_failures}")
    print(f"control_stall_count: {consumer.control_stall_count}")
    print(f"control_stall_total_ms: {consumer.control_stall_total_ms:.3f}")
    print(f"control_stall_max_ms: {consumer.control_stall_max_ms:.3f}")
    print(f"resolutions_observed: {report.resolutions}")
    print(f"leases_released_on_resolution: {report.released_on_resolution}")
    print(f"anchor_resolutions_unreleased: {report.anchor_resolutions_unreleased}")
    print(f"slugs_added_midrun: {report.slugs_added_midrun}")
    print(
        "daemon_latency: commit_time - arrival_time, both stamped by the daemon on one clock "
        "in one process (no consumer clock is involved)"
    )
    print_distribution("daemon", report.daemon_kept, report.daemon_discarded)
    print("consumer_latency: observation - commit_time")
    print_distribution("consumer", report.consumer_kept, report.consumer_discarded)
    print(
        "end_to_end: observation - arrival_time, reported above as samples_kept and p50_us "
        "through max_us"
    )


def print_distribution(prefix, kept, discarded):
    """Prints one split distribution under `prefix`, in the shape the end-to-end block prints
    its own: the same nearest-rank quantiles, the same `MIN_REPORTABLE_SAMPLES` floor, and
    percentiles withheld rather than printed below it.

    The end-to-end block is written out longhand rather than routed through this so that every
    key this consumer has printed since it shipped keeps its exact text.
    """
    print(f"{prefix}_samples_kept: {len(kept)}")
    if len(kept) < MIN_REPORTABLE_SAMPLES:
        print(
            f"{prefix}_percentiles: withheld, only {len(kept)} kept samples "
            f"(floor is {MIN_REPORTABLE_SAMPLES})"
        )
    else:
        ordered = sorted(kept)
        print(f"{prefix}_p50_us: {quantile(ordered, 500) // 1000}")
        print(f"{prefix}_p95_us: {quantile(ordered, 950) // 1000}")
        print(f"{prefix}_p99_us: {quantile(ordered, 990) // 1000}")
        print(f"{prefix}_p99.9_us: {quantile(ordered, 999) // 1000}")
        print(f"{prefix}_max_us: {ordered[-1] // 1000}")
    print(f"{prefix}_samples_discarded: {discarded}")


def bounded_seconds(text):
    value = float(text)
    if not 0 < value <= MAX_SECONDS:
        raise argparse.ArgumentTypeError(f"--seconds must be in (0, {MAX_SECONDS}]")
    return value


def bounded_ttl(text):
    value = int(text)
    if value != 0 and value < MIN_LEASE_TTL_MS:
        raise argparse.ArgumentTypeError(
            f"--lease-ttl-ms must be 0 or at least {MIN_LEASE_TTL_MS}, matching the daemon"
        )
    return value


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="pm-ws S8 full-fleet benchmark consumer")
    parser.add_argument("--control", required=True)
    parser.add_argument("--slugs", required=True)
    parser.add_argument("--seconds", type=bounded_seconds, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--lease-ttl-ms", type=bounded_ttl, default=None)
    parser.add_argument("--spin-micros", type=int, default=0)
    parser.add_argument("--rescan-ms", type=int, default=2_000)
    parser.add_argument("--venue", default="limitless")
    parser.add_argument("--kind", default="slug")
    parser.add_argument("--spin-only", action="store_true")
    parser.add_argument("--until-resolutions", type=int, default=0)
    parser.add_argument("--hold-until", default=None)
    parser.add_argument("--obs-out", default=None)
    args = parser.parse_args(argv)
    if not 0 <= args.spin_micros <= pmws.PMWS_MAX_SPIN_MICROS:
        parser.error(f"--spin-micros must be in [0, {pmws.PMWS_MAX_SPIN_MICROS}]")
    if not 50 <= args.rescan_ms <= 60_000:
        parser.error("--rescan-ms must be in [50, 60000]")
    if args.until_resolutions < 0:
        parser.error("--until-resolutions must not be negative")
    return args


def main():
    if "--digest-self-test" in sys.argv[1:]:
        return _run_digest_self_test()
    args = parse_args()
    consumer = BenchConsumer(args)

    def emit(report):
        print_report(args, consumer, report)
        if args.obs_out:
            write_obs_file(args.obs_out, args.label, consumer.pinned_size, report)
        sys.stdout.flush()

    if args.obs_out:
        def _flush_obs_on_term(_signum, _frame):
            """SIGTERM's whole-process default would otherwise take every `.obs` row this
            run collected with it; this writes what is held so far -- `bench/sdk-harness/
            README.md`'s "graceful flush on SIGTERM" -- before ending the process the same
            way SIGTERM always has (exit code 143), and is only ever installed when
            `--obs-out` was given.
            """
            write_obs_file(args.obs_out, args.label, consumer.pinned_size, consumer.report)
            sys.exit(143)

        signal.signal(signal.SIGTERM, _flush_obs_on_term)

    try:
        consumer.run(emit)
    except BenchError as error:
        print(f"bench_consumer.py: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

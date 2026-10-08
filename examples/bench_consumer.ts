#!/usr/bin/env node
/**
 * The S8 full-fleet benchmark consumer, over the pm-ws Node binding
 * (`bindings/node/pmws.ts`) — the TypeScript half of the pair whose Python half is
 * `examples/bench_consumer.py`.
 *
 * Leases half of a live `pmwsd`'s open market set through the control channel, parks on the
 * segment's doorbell, and reports the socket-arrival → consumer-observable latency
 * distribution for the markets it holds, while churning with the venue: a market whose
 * resolution this process observes has its lease released immediately, and a market the
 * runner appends to the slug file mid-run is leased mid-run. Nothing here publishes, and
 * nothing here talks to a venue — the daemon owns every venue connection.
 *
 * Measurement semantics are `examples/latency_probe.rs`'s, deliberately: the same
 * discard-not-clamp rule on a delta, the same nearest-rank quantiles over nanoseconds, the
 * same 1000-sample floor below which percentiles are withheld, and the same
 * `wakes`/`timeouts`/`rescans`/`dirty_delivered`/`samples_*` accounting. Percentiles print as
 * whole microseconds, truncated, exactly as `bench_consumer.py` prints them, so the two
 * consumers' reports are read side by side.
 *
 * Report compatibility: this report is `latency_probe`'s shape, not its schema. Percentiles are
 * whole microseconds where the probe prints three decimals, and these keys have no probe
 * equivalent at all: `consumer`, `runtime`, `clock`, `clock_derivations`,
 * `clock_divergence_ns_per_s`, `lease_ttl_ms`, `renew_interval_ms`, `anchor_market`,
 * `markets_leased_peak`, `markets_held`, `dirty_delivered_ours`, `continuity_losses`,
 * `anchor_renewals`, `anchor_renew_failures`, `lease_attach_failures`, `resolutions_observed`,
 * `leases_released_on_resolution`, `anchor_resolutions_unreleased`, `slugs_added_midrun`,
 * `control_stall_count`, `control_stall_total_ms`, `control_stall_max_ms`,
 * `samples_after_control`, `transient_event_faults`. The first two of those are this
 * consumer's alone, because only it derives its clock. Read the reports side by side, never
 * diffed key for key.
 *
 * Three distributions are reported. `daemon_*` is `commit_time - arrival_time`, `consumer_*`
 * is `observation - commit_time`, and the unprefixed keys (`samples_kept`, `p50_us` through
 * `max_us`) remain the end-to-end `observation - arrival_time` they have always been. Each is
 * ordered independently, so their percentiles do not add, even though per observation
 * `daemon + consumer == end_to_end` exactly. Only the latter two are measured with this
 * consumer's derived clock; the daemon half is two daemon stamps and is never touched by the
 * calibration, step or poison accounting above.
 *
 * **Clock domain — the one place this consumer cannot simply mirror the Python one.** The
 * daemon stamps `arrival_time` with `SystemTime::now().duration_since(UNIX_EPOCH)`
 * (`wall_clock_arrival_nanos` in `src/limitless/connection.rs`): CLOCK_REALTIME nanoseconds
 * since the Unix epoch. Python reads that clock directly with `time.time_ns()`. Node exposes
 * no epoch-nanosecond clock at all: `Date.now()` is milliseconds, and
 * `process.hrtime.bigint()` is monotonic, a different clock. So this consumer derives the
 * realtime domain from the monotonic one, and the derivation has to keep being redone,
 * because the two clocks measurably diverge:
 *
 * - macOS M1, measured 2026-09-02 over 30 × 1 s derivations: CLOCK_REALTIME ran
 *   +30_030 ns/s faster than the monotonic clock (range 29_738..30_229). That is the host,
 *   not the runtime — CPython's own `time.monotonic_ns()` against `time.time_ns()` diverged
 *   at +30_245 ns/s in the same window. A once-derived offset would therefore be ~9 ms wrong
 *   by the end of a 300 s run.
 * - Linux i9-14900K (WSL2), same measurement: −1.0 ns/s median, i.e. no measurable *drift* —
 *   but the offset does not merely drift there. The host steps the guest's CLOCK_REALTIME
 *   backwards against CLOCK_MONOTONIC, by 111–171 ms roughly every thirty seconds, measured
 *   2026-09-02 across three separate runs. An excursion first read as one descheduled
 *   calibration was one of these steps; it is a movement of the clock, not of the estimator.
 *
 * The correction, run entirely on the keeper thread so no calibration cost is ever charged to
 * a measured observation: derive the offset by millisecond-edge detection (spin until
 * `Date.now()` ticks over, then read `process.hrtime.bigint()` — the estimate
 * `ms * 1e6 − hrtime` is a *lower* bound on the true offset, tight to the loop's own
 * iteration), take the best of four edges, and repeat once a second. The published offset is
 * that one derivation, projected forward at the divergence rate the last sixteen derivations
 * imply (the median of their consecutive slopes, clamped to
 * `MAX_DIVERGENCE_MILLI_NS_PER_S`). Taking the best of several edges is what rejects a
 * derivation the scheduler interrupted; it is done *inside* one derivation, across the few
 * milliseconds its edges span, where the true offset cannot have moved.
 *
 * It is deliberately not a maximum over the window's estimates. Each estimate bounds the
 * offset at the moment it was taken, so a maximum is sound only while the true offset never
 * falls — and on WSL2 it does: the host steps the guest's CLOCK_REALTIME backwards against
 * CLOCK_MONOTONIC by 111–171 ms roughly every thirty seconds. Measured on the Linux proof
 * host with both estimators side by side, the maximum-over-window offset stood 111–171 ms too
 * high for fifteen seconds at a time — a sawtooth that put a consumer's p50 at 60 ms and
 * compressed its p99 and p99.9 together at the latch height — while each derivation's own
 * estimate stayed inside 0.2 ms of the truth throughout.
 *
 * Two guards sit on top. A derivation disagreeing with the previous calibration's projection
 * by more than `CLOCK_STEP_THRESHOLD_NS` means the host stepped its clock somewhere inside
 * the calibration interval that just ended: that whole generation is announced as poisoned,
 * and every sample measured under it is discarded at report time and counted in
 * `samples_discarded_clock_step` — discarded, never clamped, the same rule this consumer
 * already applies to a delta that runs backwards or absurdly long. And a candidate offset
 * that does not put the derived epoch within a millisecond of `Date.now()` is not published
 * at all. That last check is the invariant whose absence let a latched offset ship once
 * already; `clock_calibrations_rejected` reports it.
 *
 * What this leaves: the residual between recalibrations is the run-to-run spread of the rate
 * — sub-microsecond on both hosts — and every estimate is a lower bound, so an error can only
 * make a reported latency slightly smaller than the truth, never larger. Verified on the M1:
 * the consumer reported a divergence of 12_961 ns/s in the same minute a direct
 * `time.monotonic_ns()` against `time.time_ns()` measurement gave 13_243 ns/s. A step in the
 * final calibration interval of a run has no later derivation to reveal it, so the last
 * second of samples is the one window this cannot clean.
 *
 * `performance.timeOrigin + performance.now()` is not used: measured on the macOS host it
 * disagreed with the epoch by ~1.45e15 ns.
 *
 * Two threads, but not for the reason `latency_probe`'s keeper thread exists any more: a
 * native session cannot cross a worker boundary, so the one control session this consumer
 * holds — opened by `connect()`ing the anchor market, and every further market's `lease()`,
 * `release()`, and the whole session's `renew()` besides — lives entirely on the main thread,
 * inline in the loop that parks on the segment's doorbell, the same way the anchor's own
 * renewal always ran. The worker keeps only the clock calibration, which is genuinely
 * independent of any session and cannot be folded onto the main thread the way the control
 * calls were: it must keep running every second regardless of how long the main thread spends
 * parked. The two communicate through one `SharedArrayBuffer`, never `postMessage`, because
 * the main thread never returns to the event loop while it is parked on a doorbell.
 *
 * Lease renewal follows the probe's rule: renew at `--lease-ttl-ms` / 3, clamped to
 * [100 ms, 5 s]. The C ABI does not surface the TTL the daemon declared in its attach answer,
 * so the runner passes the TTL it configured; without the flag this falls back to the blind
 * rule `pmws_renew` documents — a fraction of `MIN_LEASE_TTL_MS`.
 *
 * Usage:
 *     node bench_consumer.ts --control <socket-path> --slugs <file> --seconds <n>
 *                            --label <text> [--lease-ttl-ms <n>] [--spin-micros 0]
 *                            [--rescan-ms 2000] [--venue limitless] [--kind slug]
 *                            [--spin-only] [--until-resolutions <n>] [--hold-until <path>]
 *                            [--obs-out <path>]
 *     node bench_consumer.ts --self-test
 *
 * `--until-resolutions` ends the measurement loop as soon as that many resolutions have been
 * observed, instead of at `--seconds`, and `--hold-until` keeps the process alive — still
 * holding every lease it has not released — until the named path appears. Neither is used by
 * a benchmark run; together they are what lets a deterministic test drive this consumer by
 * events rather than by the clock.
 *
 * `--slugs` names an append-only file, one venue-native market key per line; blank lines and
 * lines beginning with `#` are ignored. The runner grows that file to lease newly listed
 * markets mid-run: the main thread re-reads it every `--rescan-ms` and a key's line number is
 * its index for the run, which is why the file must only ever be appended to.
 *
 * `--obs-out` is the S8c comparative-benchmark hook (`bench/sdk-harness/README.md`): additive
 * only, and inert unless given. When set, every state observation this consumer already
 * performs also appends an in-memory row -- slug, the same `t_obs` already stamped, a content
 * digest of the state's levels, a per-market sequence number, and the book revision as
 * `rev=<n>` -- flushed to that path at run end or `SIGTERM`. Without it, this file's runtime
 * path is exactly what it always was: no digest computed, nothing recorded, every other flag,
 * report line, and key unchanged.
 *
 * `--self-test` checks the canonical-decimal vectors and the FNV-1a 64 digest this consumer
 * shares with `bench/sdk-harness/ts/sdk_leg.ts` against a golden value, and exits before
 * touching `--control`, `--slugs`, or any other flag.
 */

import { execFileSync } from "node:child_process";
import { existsSync, readFileSync, writeFileSync, writeSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { isMainThread, parentPort, workerData, Worker } from "node:worker_threads";
import {
  isPmwsError,
  isTransient,
  MAX_SPIN_MICROS,
  PmwsContinuityLost,
  PmwsDirtyRescan,
  Segment,
  type Event,
  type EventStream,
  type Market,
} from "../bindings/node/pmws.ts";

const IMPLAUSIBLE_NANOS = 1_000_000_000n;
const MIN_REPORTABLE_SAMPLES = 1_000;
const PARK_POLL_TIMEOUT_MS = 100;
const PENDING_POLL_TIMEOUT_MS = 5;
const PENDING_EAGER_MS = 2_000;
const MAX_SECONDS = 86_400;

const MIN_LEASE_TTL_MS = 1_000;
const RENEW_TTL_DIVISOR = 3;
const RENEW_FLOOR_MS = 100;
const RENEW_CEILING_MS = 5_000;

const KEEPER_POLL_MS = 20;
const CALIBRATION_INTERVAL_MS = 1_000;
/** How often the keeper derives while its window is still filling.
 *
 * The published offset is only as good as the divergence rate it is projected at, and the
 * rate is a median of slopes between derivations: with two or three points that median is
 * noise, and on a host whose clocks diverge at 30 µs/s a rate that is wrong by half is an
 * offset that is wrong by tens of microseconds a second later. Filling the window at this
 * cadence converges the rate in a few seconds, and the measurement loop does not start until
 * it is full. */
const CALIBRATION_WARMUP_MS = 200;
const CALIBRATION_EDGES = 4;
const CALIBRATION_WINDOW = 16;
const CALIBRATION_READY_TIMEOUT_MS = 15_000;
const ATTACH_SETTLE_MS = 15_000;
const HOLD_POLL_MS = 20;
const HOLD_CAP_MS = 120_000;

const MAX_MARKETS = 4_096;
const MAX_OBS_ROWS = 4_000_000;

const CLOCK_ORIGIN = 0;
const CLOCK_OFFSET = 1;
const CLOCK_RATE = 2;
const CLOCK_DERIVATIONS = 3;
const CLOCK_WORDS = 4;

/** The widest realtime-against-monotonic divergence this consumer will project at, in
 * thousandths of a nanosecond per second — 200 µs/s, about 200 ppm.
 *
 * Six times the fastest divergence either benchmark host has shown (the M1's ~30 µs/s), and
 * far short of anything a slope this estimator could compute from a stepped clock. A rate
 * outside it is a bad estimate rather than a fast clock, and projecting at it would move the
 * offset further than any drift ever does. */
const MAX_DIVERGENCE_MILLI_NS_PER_S = 200_000_000n;

/** How far one derivation may disagree with the previous calibration's projection before it
 * is read as the host stepping its clock rather than as drift.
 *
 * Well above what an honest derivation varies by — the millisecond-edge estimate measured
 * inside 0.2 ms of the truth on both hosts, and one calibration interval of drift at the
 * clamp above is 200 µs — and far below the 111–171 ms steps WSL2 was measured taking. */
const CLOCK_STEP_THRESHOLD_NS = 2_000_000n;

/** How far the published calibration may disagree with `Date.now()` before it is refused.
 *
 * `Date.now()` is CLOCK_REALTIME truncated to a millisecond, so an honest derived epoch sits
 * within one millisecond above it, and the slack below covers the truncation. A calibration
 * outside this band is not published at all: it is the invariant whose absence let a latched
 * offset ship once already. */
const CLOCK_SANITY_LOW_NS = -2_000_000n;
const CLOCK_SANITY_HIGH_NS = 3_000_000n;

const CONTROL_VERSION = 0;
const CONTROL_STOP = 1;
const CONTROL_SLEEPER = 2;
const CONTROL_CLOCK_GENERATION = 3;
const CONTROL_CLOCK_POISONED = 4;
const CONTROL_CLOCK_STEPS = 5;
const CONTROL_CLOCK_REJECTED = 6;
const CONTROL_WORDS = 7;

/** The whole cross-thread surface: the derived clock plus the calibration worker's own
 * counters. A native session cannot cross a worker boundary, so the one control session this
 * consumer holds -- and every lease, release, and renewal on it -- lives entirely on the main
 * thread; nothing about a market's lease crosses this buffer any more. `MAX_MARKETS` is no
 * longer a `SharedArrayBuffer` sizing consequence -- this buffer is fixed-size regardless of
 * how many markets a run carries -- but stays as the practical ceiling `rescan`'s own loops
 * assume. */
interface Shared {
  clock: BigInt64Array;
  control: Int32Array;
}

function sharedViews(buffer: SharedArrayBuffer): Shared {
  return {
    clock: new BigInt64Array(buffer, 0, CLOCK_WORDS),
    control: new Int32Array(buffer, CLOCK_WORDS * 8, CONTROL_WORDS),
  };
}

function sharedBuffer(): SharedArrayBuffer {
  return new SharedArrayBuffer(CLOCK_WORDS * 8 + CONTROL_WORDS * 4);
}

interface Args {
  control: string;
  slugs: string;
  seconds: number;
  label: string;
  leaseTtlMs: number | null;
  spinMicros: number;
  rescanMs: number;
  venue: string;
  kind: string;
  spinOnly: boolean;
  untilResolutions: number;
  holdUntil: string | null;
  /** S8c comparative-benchmark hook, additive only: when set, every state observation this
   * consumer already performs also appends an in-memory row -- slug, the same `t_obs` already
   * stamped, a content digest of the state's levels, a per-market sequence number, and the
   * book revision -- to an `.obs` file flushed at run end or `SIGTERM`. `null` (the default,
   * when `--obs-out` is not given) takes none of this: no digest is computed, nothing is
   * recorded, and every other line this consumer prints is unchanged. See
   * `bench/sdk-harness/README.md` for the file format and the digest this leg shares,
   * duplicated rather than imported, with `bench/sdk-harness/ts/sdk_leg.ts`. */
  obsOut: string | null;
}

class UsageError extends Error {}

class BenchError extends Error {}

/** Statuses that end the control connection and release every lease the session held with it
 * — `bindings/node/pmws.ts`'s own `lease`/`release`/`renew` documentation names these two as
 * the pair that ends the conversation, as opposed to every other status, which is an ordinary
 * refusal that leaves the connection usable. */
const CONNECTION_LOST_CODES: ReadonlySet<string> = new Set(["PMWS_IO", "PMWS_ATTACH_INCOMPLETE"]);

function usage(): string {
  return (
    "usage: bench_consumer.ts --control <socket-path> --slugs <file> --seconds <n> " +
    "--label <text> [--lease-ttl-ms <n>] [--spin-micros 0] [--rescan-ms 2000] " +
    "[--venue limitless] [--kind slug] [--spin-only] [--until-resolutions <n>] " +
    "[--hold-until <path>] [--obs-out <path>]"
  );
}

function parseArgs(argv: string[]): Args {
  let control: string | undefined;
  let slugs: string | undefined;
  let seconds: number | undefined;
  let label: string | undefined;
  let leaseTtlMs: number | null = null;
  let spinMicros = 0;
  let rescanMs = 2_000;
  let venue = "limitless";
  let kind = "slug";
  let spinOnly = false;
  let untilResolutions = 0;
  let holdUntil: string | null = null;
  let obsOut: string | null = null;

  for (let i = 0; i < argv.length; i += 1) {
    const flag = argv[i];
    const next = (): string => {
      i += 1;
      const value = argv[i];
      if (value === undefined) {
        throw new UsageError(`${flag} needs a value\n${usage()}`);
      }
      return value;
    };
    const wholeNumber = (name: string, low: number, high: number): number => {
      const value = Number(next());
      if (!Number.isInteger(value) || value < low || value > high) {
        throw new UsageError(`${name} must be an integer in [${low}, ${high}]\n${usage()}`);
      }
      return value;
    };
    switch (flag) {
      case "--control":
        control = next();
        break;
      case "--slugs":
        slugs = next();
        break;
      case "--label":
        label = next();
        break;
      case "--seconds": {
        const value = Number(next());
        if (!Number.isFinite(value) || value <= 0 || value > MAX_SECONDS) {
          throw new UsageError(`--seconds must be in (0, ${MAX_SECONDS}]\n${usage()}`);
        }
        seconds = value;
        break;
      }
      case "--lease-ttl-ms":
        leaseTtlMs = wholeNumber("--lease-ttl-ms", 0, Number.MAX_SAFE_INTEGER);
        if (leaseTtlMs !== 0 && leaseTtlMs < MIN_LEASE_TTL_MS) {
          throw new UsageError(
            `--lease-ttl-ms must be 0 or at least ${MIN_LEASE_TTL_MS}, matching the daemon\n${usage()}`,
          );
        }
        break;
      case "--spin-micros":
        spinMicros = wholeNumber("--spin-micros", 0, MAX_SPIN_MICROS);
        break;
      case "--rescan-ms":
        rescanMs = wholeNumber("--rescan-ms", 50, 60_000);
        break;
      case "--venue":
        venue = next();
        break;
      case "--kind":
        kind = next();
        break;
      case "--spin-only":
        spinOnly = true;
        break;
      case "--until-resolutions":
        untilResolutions = wholeNumber("--until-resolutions", 0, Number.MAX_SAFE_INTEGER);
        break;
      case "--hold-until":
        holdUntil = next();
        break;
      case "--obs-out":
        obsOut = next();
        break;
      default:
        throw new UsageError(`unrecognized argument: ${flag}\n${usage()}`);
    }
  }
  if (control === undefined) throw new UsageError(`--control is required\n${usage()}`);
  if (slugs === undefined) throw new UsageError(`--slugs is required\n${usage()}`);
  if (seconds === undefined) throw new UsageError(`--seconds is required\n${usage()}`);
  if (label === undefined) throw new UsageError(`--label is required\n${usage()}`);
  return {
    control,
    slugs,
    seconds,
    label,
    leaseTtlMs,
    spinMicros,
    rescanMs,
    venue,
    kind,
    spinOnly,
    untilResolutions,
    holdUntil,
    obsOut,
  };
}

/** The renewal period for a daemon whose lease TTL is `leaseTtlMs`, in milliseconds, or
 * `null` when no renewal is needed at all.
 *
 * `null` in means the caller did not say, and the interval is then the blind rule `pmws_renew`
 * documents for a consumer that cannot see its daemon's configuration: a fraction of
 * `MIN_LEASE_TTL_MS`. `0` means the daemon expires nothing, so a lease lives exactly as long
 * as its connection and no renewal is needed. Any other TTL renews at a third of it, clamped
 * to [100 ms, 5 s] — `examples/latency_probe.rs`'s own rule for the TTL its attach answer
 * declared. */
function renewIntervalMs(leaseTtlMs: number | null): number | null {
  if (leaseTtlMs === 0) {
    return null;
  }
  const declared = leaseTtlMs === null ? MIN_LEASE_TTL_MS : leaseTtlMs;
  return Math.min(RENEW_CEILING_MS, Math.max(RENEW_FLOOR_MS, Math.floor(declared / RENEW_TTL_DIVISOR)));
}

/** Every venue-native key the slug file names, in file order, without blanks or comments.
 *
 * A key's position in this list is its stable index for the run: both threads address a
 * market by it, so the file has to be appended to and never rewritten. */
function readSlugFile(path: string): string[] {
  return readFileSync(path, "utf8")
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0 && !line.startsWith("#"));
}

/** One level's price and quantity as decimal text, the shared digest's only input shape.
 *
 * Identical in `bench/sdk-harness/ts/sdk_leg.ts`: there `text` is `String(n)` on the SDK's
 * float, here it is the ABI's own exact decimal lexeme (`Level.price.text` /
 * `Level.quantity.text`, `bindings/node/pmws.ts`) -- per `bench/sdk-harness/README.md`'s
 * canonicalization rule for each source. */
interface DigestLevel {
  price: string;
  qty: string;
}

const EXPONENTIAL_RE = /^([+-]?)(\d+)(?:\.(\d+))?[eE]([+-]?\d+)$/;

/** Expands `e`/`E` scientific notation to plain decimal digits, exactly, with no floating
 * arithmetic. Identical in `bench/sdk-harness/ts/sdk_leg.ts`. */
function expandExponential(raw: string): string {
  const match = EXPONENTIAL_RE.exec(raw);
  if (match === null) {
    return raw;
  }
  const sign = match[1];
  const intPart = match[2];
  const fracPart = match[3] ?? "";
  const exponent = Number.parseInt(match[4], 10);
  const digits = intPart + fracPart;
  const pointPosition = intPart.length + exponent;
  let magnitude: string;
  if (pointPosition <= 0) {
    magnitude = `0.${"0".repeat(-pointPosition)}${digits}`;
  } else if (pointPosition >= digits.length) {
    magnitude = `${digits}${"0".repeat(pointPosition - digits.length)}`;
  } else {
    magnitude = `${digits.slice(0, pointPosition)}.${digits.slice(pointPosition)}`;
  }
  return sign + magnitude;
}

/** Strips redundant leading zeros from the integer part (`"007"` -> `"7"`), leaving a lone
 * `"0"` alone. Identical in `bench/sdk-harness/ts/sdk_leg.ts`. */
function stripLeadingZeros(raw: string): string {
  const negative = raw.startsWith("-");
  const unsigned = negative ? raw.slice(1) : raw;
  const dot = unsigned.indexOf(".");
  const intPart = dot === -1 ? unsigned : unsigned.slice(0, dot);
  const fracPart = dot === -1 ? null : unsigned.slice(dot + 1);
  const strippedInt = intPart.replace(/^0+(?=\d)/, "");
  return (negative ? "-" : "") + strippedInt + (fracPart === null ? "" : `.${fracPart}`);
}

/** Strips trailing fractional zeros, then a bare trailing `.`. Identical in
 * `bench/sdk-harness/ts/sdk_leg.ts`. */
function stripTrailingZerosAndDot(raw: string): string {
  if (!raw.includes(".")) {
    return raw;
  }
  return raw.replace(/0+$/, "").replace(/\.$/, "");
}

/** The canonical decimal form `bench/sdk-harness/README.md` specifies: expand scientific
 * notation, strip a leading `+`, strip redundant leading zeros, then (if a `.` remains)
 * strip trailing zeros and a bare trailing `.`. Identical in
 * `bench/sdk-harness/ts/sdk_leg.ts`. */
function canonicalDecimal(raw: string): string {
  let value = expandExponential(raw);
  if (value.startsWith("+")) {
    value = value.slice(1);
  }
  value = stripLeadingZeros(value);
  value = stripTrailingZerosAndDot(value);
  return value;
}

const FNV_OFFSET_BASIS = 0xcbf29ce484222325n;
const FNV_PRIME = 0x100000001b3n;
const FNV_MASK_64 = (1n << 64n) - 1n;

/** FNV-1a 64-bit over `input`'s UTF-8 bytes, printed as 16 lowercase hex digits. Identical in
 * `bench/sdk-harness/ts/sdk_leg.ts`. */
function fnv1a64Hex(input: string): string {
  let hash = FNV_OFFSET_BASIS;
  const bytes = new TextEncoder().encode(input);
  for (let i = 0; i < bytes.length; i += 1) {
    hash ^= BigInt(bytes[i]);
    hash = (hash * FNV_PRIME) & FNV_MASK_64;
  }
  return hash.toString(16).padStart(16, "0");
}

/** The content digest `bench/sdk-harness/README.md` specifies: exclude zero-quantity levels,
 * sort bids price-descending and asks price-ascending, serialize as
 * `B` + `price:qty;`* + `|A` + `price:qty;`* using canonical decimals, then FNV-1a 64 the
 * UTF-8 bytes. Identical in `bench/sdk-harness/ts/sdk_leg.ts`. */
function canonicalDigest(bids: DigestLevel[], asks: DigestLevel[]): string {
  const nonZero = (level: DigestLevel): boolean => canonicalDecimal(level.qty) !== "0";
  const byPriceAscending = (a: DigestLevel, b: DigestLevel): number =>
    Number(canonicalDecimal(a.price)) - Number(canonicalDecimal(b.price));
  const sortedBids = bids.filter(nonZero).sort((a, b) => byPriceAscending(b, a));
  const sortedAsks = asks.filter(nonZero).sort(byPriceAscending);
  let serialized = "B";
  for (const level of sortedBids) {
    serialized += `${canonicalDecimal(level.price)}:${canonicalDecimal(level.qty)};`;
  }
  serialized += "|A";
  for (const level of sortedAsks) {
    serialized += `${canonicalDecimal(level.price)}:${canonicalDecimal(level.qty)};`;
  }
  return fnv1a64Hex(serialized);
}

/** The canonicalization vectors and one digest golden value `bench/sdk-harness/README.md`
 * pins, asserted identically by `bench/sdk-harness/ts/sdk_leg.ts --self-test`. Runs no
 * control-channel or segment code and touches no other flag. */
function selfTest(): number {
  let failures = 0;
  const vectors: Array<[string, string]> = [
    ["0.530", "0.53"],
    ["1000000.0", "1000000"],
    ["100", "100"],
    ["0.5", "0.5"],
    ["1e-7", "0.0000001"],
    ["0.0", "0"],
  ];
  for (const [input, expected] of vectors) {
    const actual = canonicalDecimal(input);
    if (actual !== expected) {
      failures += 1;
      console.error(`canonicalDecimal(${JSON.stringify(input)}) = ${actual}, expected ${expected}`);
    }
  }
  const sdkFloatCase = canonicalDecimal(String(1e-7));
  if (sdkFloatCase !== "0.0000001") {
    failures += 1;
    console.error(`canonicalDecimal(String(1e-7)) = ${sdkFloatCase}, expected 0.0000001`);
  }
  const goldenInput = "B0.6:10;0.5:20;|A0.7:5;0.8:15;";
  const goldenDigest = "8e46092ae63fb7c8";
  const sorted = canonicalDigest(
    [
      { price: "0.6", qty: "10" },
      { price: "0.5", qty: "20" },
    ],
    [
      { price: "0.7", qty: "5" },
      { price: "0.8", qty: "15" },
    ],
  );
  if (sorted !== goldenDigest) {
    failures += 1;
    console.error(`canonicalDigest(presorted) = ${sorted}, expected ${goldenDigest} for "${goldenInput}"`);
  }
  const unsorted = canonicalDigest(
    [
      { price: "0.5", qty: "20" },
      { price: "0.9", qty: "0" },
      { price: "0.6", qty: "10" },
    ],
    [
      { price: "0.8", qty: "15" },
      { price: "0.7", qty: "5" },
    ],
  );
  if (unsorted !== goldenDigest) {
    failures += 1;
    console.error(`canonicalDigest(unsorted+zero-qty) = ${unsorted}, expected ${goldenDigest}`);
  }
  if (failures === 0) {
    console.log(
      `self-test: PASS -- ${vectors.length + 1} canonicalization checks, digest golden ` +
        `${goldenDigest} for "${goldenInput}" (presorted and unsorted+zero-filtered inputs)`,
    );
    return 0;
  }
  console.error(`self-test: FAIL (${failures} failing checks)`);
  return 1;
}

/** This build's pm-ws git revision, for the `.obs` file's `# pin:` line -- best-effort: a
 * checkout without `git` on `PATH`, or not a git working tree at all, prints `unknown` rather
 * than failing a run over metadata. */
function pmwsGitRevision(): string {
  try {
    const repoRoot = fileURLToPath(new URL("..", import.meta.url));
    return execFileSync("git", ["rev-parse", "HEAD"], { cwd: repoRoot, encoding: "utf8" }).trim();
  } catch {
    return "unknown";
  }
}

/** The observation log the S8c comparative benchmark reads, flushed once at measurement-loop
 * end or on `SIGTERM`; `null` when `--obs-out` was not given, in which case nothing below
 * this point in the file ever runs. Rows are pre-allocated up to `MAX_OBS_ROWS`; anything
 * past that is counted in `dropped` instead of stored, per `bench/sdk-harness/README.md`'s
 * cap. */
interface ObsRecorder {
  rows: (string | null)[];
  count: number;
  dropped: number;
  eventsTotal: number;
  seqByMarket: Map<string, number>;
  path: string;
}

function createObsRecorder(path: string): ObsRecorder {
  return {
    rows: new Array(MAX_OBS_ROWS).fill(null),
    count: 0,
    dropped: 0,
    eventsTotal: 0,
    seqByMarket: new Map<string, number>(),
    path,
  };
}

/** Records one observation, or counts it dropped past `MAX_OBS_ROWS`. `eventsTotal` counts
 * every row attempted -- matched only by `count + dropped`, the invariant the `.obs` file's
 * own `dropped` trailer is meaningful against -- and is therefore the count of
 * non-contaminated state observations this run recorded, not of every state observation
 * `observe` performed: one measured while `contaminationBudget` was armed never reaches this
 * function at all, exactly as it never reaches `end_to_end`/`consumer`. */
function recordObs(recorder: ObsRecorder, slug: string, tObsNs: bigint, digest: string, revision: bigint): void {
  recorder.eventsTotal += 1;
  const seq = recorder.seqByMarket.get(slug) ?? 0;
  recorder.seqByMarket.set(slug, seq + 1);
  if (recorder.count >= MAX_OBS_ROWS) {
    recorder.dropped += 1;
    return;
  }
  recorder.rows[recorder.count] = `obs ${slug} ${tObsNs} ${digest} ${seq} rev=${revision}`;
  recorder.count += 1;
}

function flushObsRecorder(recorder: ObsRecorder, label: string, marketCount: number): void {
  const lines: string[] = [
    "# pmws-obs v1",
    "# leg: ts-binding",
    `# host: ${label}`,
    "# clock: epoch_ns",
    `# size: ${marketCount}`,
    `# pin: ${pmwsGitRevision()}`,
  ];
  for (let i = 0; i < recorder.count; i += 1) {
    lines.push(recorder.rows[i] as string);
  }
  lines.push(`# events_total: ${recorder.eventsTotal}`);
  lines.push(`# dropped: ${recorder.dropped}`);
  writeFileSync(recorder.path, `${lines.join("\n")}\n`);
}

/** One half of a split observation: measured, absent for want of a stamp, or rejected as
 * implausible — what a clock adjustment between two stamps produces. */
type Sample =
  | { kind: "measured"; value: bigint }
  | { kind: "implausible" }
  | { kind: "absent" };

/** `end - start` when both stamps are present and their difference is plausible. */
function difference(start: bigint | null, end: bigint | null): Sample {
  if (start === null || end === null) {
    return { kind: "absent" };
  }
  const delta = end - start;
  if (delta >= 0n && delta < IMPLAUSIBLE_NANOS) {
    return { kind: "measured", value: delta };
  }
  return { kind: "implausible" };
}

/** The `permille`-th value of a sorted nanosecond sample set, by nearest rank; 0 when empty.
 * Identical to `examples/latency_probe.rs`'s own `quantile`. */
function quantile(sorted: bigint[], permille: number): bigint {
  if (sorted.length === 0) {
    return 0n;
  }
  const rank = Math.max(Math.ceil((sorted.length * permille) / 1000), 1) - 1;
  return sorted[Math.min(rank, sorted.length - 1)];
}

interface Derivation {
  hr: bigint;
  estimate: bigint;
}

/** One millisecond-edge derivation of the realtime-minus-monotonic offset.
 *
 * Spins until `Date.now()` reports a new millisecond and reads `process.hrtime.bigint()`
 * immediately after, so `ms * 1e6 − hrtime` is a lower bound on the true offset, short by
 * however long the loop took to notice the edge. The best of `CALIBRATION_EDGES` edges is
 * returned; a spin that is descheduled produces a far-too-low estimate, which the caller's
 * maximum-over-the-window then discards. */
function deriveOffset(): Derivation {
  let best: bigint | null = null;
  let bestHr = 0n;
  for (let edge = 0; edge < CALIBRATION_EDGES; edge += 1) {
    const start = Date.now();
    let ms = start;
    let hr = 0n;
    do {
      ms = Date.now();
      hr = process.hrtime.bigint();
    } while (ms === start);
    const estimate = BigInt(ms) * 1_000_000n - hr;
    if (best === null || estimate > best) {
      best = estimate;
      bestHr = hr;
    }
  }
  return { hr: bestHr, estimate: best as bigint };
}

/** The divergence rate the window implies, in thousandths of a nanosecond per second.
 *
 * The median of consecutive slopes rather than a fit, because one descheduled derivation
 * makes exactly two slopes absurd and a median of sixteen is unmoved by them. */
function divergenceRate(window: Derivation[]): bigint {
  if (window.length < 2) {
    return 0n;
  }
  const slopes: bigint[] = [];
  for (let i = 1; i < window.length; i += 1) {
    const span = window[i].hr - window[i - 1].hr;
    if (span > 0n) {
      slopes.push(((window[i].estimate - window[i - 1].estimate) * 1_000_000_000_000n) / span);
    }
  }
  if (slopes.length === 0) {
    return 0n;
  }
  slopes.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  const median = slopes[Math.floor(slopes.length / 2)];
  if (median > MAX_DIVERGENCE_MILLI_NS_PER_S) {
    return MAX_DIVERGENCE_MILLI_NS_PER_S;
  }
  if (median < -MAX_DIVERGENCE_MILLI_NS_PER_S) {
    return -MAX_DIVERGENCE_MILLI_NS_PER_S;
  }
  return median;
}

interface Calibration {
  offset: bigint;
  rate: bigint;
  /** Which published calibration this is. Every sample records the one it was measured
   * under, so a calibration the keeper later announces as spanning a clock step takes its
   * own samples out of the distribution and nothing else's. */
  generation: number;
}

/** Reads the keeper's published calibration without tearing, through the sequence counter the
 * keeper brackets every publication with. */
function readCalibration(shared: Shared): Calibration {
  for (;;) {
    const first = Atomics.load(shared.control, CONTROL_VERSION);
    if ((first & 1) === 1) {
      continue;
    }
    const offset = Atomics.load(shared.clock, CLOCK_OFFSET);
    const rate = Atomics.load(shared.clock, CLOCK_RATE);
    const generation = Atomics.load(shared.control, CONTROL_CLOCK_GENERATION);
    if (Atomics.load(shared.control, CONTROL_VERSION) === first) {
      return { offset, rate, generation };
    }
  }
}

/** CLOCK_REALTIME epoch nanoseconds for a monotonic reading, under `calibration`. */
function epochNanos(hr: bigint, origin: bigint, calibration: Calibration): bigint {
  return hr + calibration.offset + (calibration.rate * (hr - origin)) / 1_000_000_000_000n;
}

interface KeeperOptions {
  buffer: SharedArrayBuffer;
}

/** The keeper thread: the clock calibration alone.
 *
 * It no longer holds any market's lease. A native session cannot cross a worker boundary, so
 * once every market shared the one session the anchor's `connect()` opens, there was nothing
 * left here that was not a segment operation belonging on the main thread instead -- lease,
 * release, and renew all moved there, inline in the loop that parks on the doorbell, the way
 * the anchor's own renewal always ran. What stays is what never touched a session at all. */
function runKeeper(options: KeeperOptions): void {
  const shared = sharedViews(options.buffer);
  let nextCalibration = 0;
  let derivations = 0;
  let published: Calibration | null = null;
  const window: Derivation[] = [];

  /** Derives the realtime-minus-monotonic offset once and publishes it, unless it does not
   * survive its own checks.
   *
   * The published offset is anchored on **this** derivation alone, projected forward at the
   * window's divergence rate. It is deliberately not a maximum over the window's estimates:
   * every estimate is a lower bound on the offset *at the moment it was taken*, so a maximum
   * is only sound while the true offset never falls, and on WSL2 it does — the host steps the
   * guest's CLOCK_REALTIME backwards against CLOCK_MONOTONIC by 111–171 ms roughly every 30
   * seconds. A maximum then latches the last pre-step estimate for the whole window and every
   * sample measured under it reads that much too slow. Measured on the Linux proof host: the
   * maximum-over-window offset stood 111–171 ms wrong for 15 s at a time, while this
   * derivation's own estimate stayed inside 0.2 ms throughout.
   *
   * Outlier rejection therefore lives *inside* one derivation, across the few milliseconds
   * `deriveOffset` spends on its edges, where the true offset cannot have moved — never
   * across seconds, where it can.
   *
   * Two guards remain. A derivation that disagrees with the previous calibration's projection
   * by more than `CLOCK_STEP_THRESHOLD_NS` means the clock stepped somewhere inside the
   * interval that just ended, so that whole calibration generation is announced as poisoned
   * and the reader drops the samples it measured under it. And a candidate that does not
   * agree with `Date.now()` to within a millisecond is not published at all; the previous
   * calibration stands, and its generation is poisoned too, because it is now older than it
   * was meant to get. */
  const calibrate = (): void => {
    derivations += 1;
    const derived = deriveOffset();
    const origin = Atomics.load(shared.clock, CLOCK_ORIGIN);

    if (published !== null) {
      const projected =
        published.offset + (published.rate * (derived.hr - origin)) / 1_000_000_000_000n;
      const disagreement = derived.estimate - projected;
      if (disagreement > CLOCK_STEP_THRESHOLD_NS || disagreement < -CLOCK_STEP_THRESHOLD_NS) {
        Atomics.add(shared.control, CONTROL_CLOCK_STEPS, 1);
        Atomics.store(shared.control, CONTROL_CLOCK_POISONED, published.generation);
      }
    }

    window.push(derived);
    if (window.length > CALIBRATION_WINDOW) {
      window.shift();
    }
    const rate = divergenceRate(window);
    const offset = derived.estimate - (rate * (derived.hr - origin)) / 1_000_000_000_000n;

    const hr = process.hrtime.bigint();
    const disagreement =
      hr + offset + (rate * (hr - origin)) / 1_000_000_000_000n - BigInt(Date.now()) * 1_000_000n;
    if (disagreement < CLOCK_SANITY_LOW_NS || disagreement > CLOCK_SANITY_HIGH_NS) {
      Atomics.add(shared.control, CONTROL_CLOCK_REJECTED, 1);
      if (published !== null) {
        Atomics.store(shared.control, CONTROL_CLOCK_POISONED, published.generation);
      }
      return;
    }

    const generation = (published?.generation ?? 0) + 1;
    Atomics.add(shared.control, CONTROL_VERSION, 1);
    Atomics.store(shared.clock, CLOCK_OFFSET, offset);
    Atomics.store(shared.clock, CLOCK_RATE, rate);
    Atomics.store(shared.clock, CLOCK_DERIVATIONS, BigInt(derivations));
    Atomics.store(shared.control, CONTROL_CLOCK_GENERATION, generation);
    Atomics.add(shared.control, CONTROL_VERSION, 1);
    published = { offset, rate, generation };
  };

  while (Atomics.load(shared.control, CONTROL_STOP) === 0) {
    const now = Date.now();
    if (now >= nextCalibration) {
      calibrate();
      nextCalibration =
        Date.now() +
        (window.length < CALIBRATION_WINDOW ? CALIBRATION_WARMUP_MS : CALIBRATION_INTERVAL_MS);
    }
    Atomics.wait(shared.control, CONTROL_SLEEPER, 0, KEEPER_POLL_MS);
  }
}

/** Everything one run measured, in the order `printReport` emits it. */
interface Report {
  kept: bigint[];
  /** The calibration generation each kept sample was measured under, in the same order.
   * A generation the keeper announces as spanning a clock step takes its own samples out of
   * the distribution at report time and leaves every other sample untouched. */
  keptGeneration: number[];
  poisoned: Set<number>;
  /** `commit_time - arrival_time`, the daemon's own half.
   *
   * Untagged and never filtered, unlike the two halves below it: both of its stamps are
   * written by the daemon, on one clock, in one process, so this consumer's derived clock —
   * and with it every calibration, step and poison this file carries — has nothing to do with
   * it. A host stepping its clock cannot move this number. */
  daemonKept: bigint[];
  daemonDiscarded: number;
  /** `observation - commit_time`, this process's half. Tagged like the end-to-end samples,
   * because it differences this consumer's clock against a daemon stamp. */
  consumerKept: bigint[];
  consumerKeptGeneration: number[];
  consumerDiscarded: number;
  skipped: number;
  discarded: number;
  wakes: number;
  timeouts: number;
  delivered: number;
  deliveredOurs: number;
  rescans: number;
  continuityLosses: number;
  resolutions: number;
  releasedOnResolution: number;
  anchorResolutionsUnreleased: number;
  anchorRenewals: number;
  anchorRenewFailures: number;
  leaseAttachFailures: number;
  leasesPeak: number;
  /** Count, total wall duration and longest wall duration of every `lease()`/`release()`
   * conversation this run made on the one session. Both run inline on the measurement thread
   * -- the one-session design this file commits to -- so each blocks the loop for however
   * long its round trip takes; this is that cost made visible instead of silently folded into
   * a sample's latency. */
  controlStallCount: number;
  controlStallTotalMs: number;
  controlStallMaxMs: number;
  /** How many observations `observe` diverted away from `end_to_end`/`consumer` because a
   * `lease()`/`release()` conversation's drain-scoped exclusion was still armed — every
   * observation from the remainder of the drain pass a conversation completed in through the
   * end of the next one, not just the first, because a single stall can delay observations
   * from more than one market at once. `daemon` is never diverted: it differences two daemon
   * stamps this process never sat between, so no stall on this thread can have contributed to
   * it. A `PMWS_IO`/`PMWS_ATTACH_INCOMPLETE` conversation never reaches this counter at all —
   * see `runReader`'s own note on the fatal-vs-counted split. */
  samplesAfterControl: number;
  /** How many times `pollEvents` saw `nextEvent()` fail with a transient status —
   * `PMWS_CONTENDED` or `PMWS_WRITER_STALLED` per `pmws_next_event`'s own contract
   * (`src/ffi/mod.rs`), or this consumer's own `PMWS_NO_PUBLISHED_STATE` — and treated it
   * like no event being available right now. Permanent zero in a healthy run. */
  transientEventFaults: number;
  slugsAddedMidrun: number;
  marketsSeen: Set<number>;
  marketsHeld: number;
  elapsedSeconds: number;
}

function emptyReport(): Report {
  return {
    kept: [],
    keptGeneration: [],
    poisoned: new Set<number>(),
    daemonKept: [],
    daemonDiscarded: 0,
    consumerKept: [],
    consumerKeptGeneration: [],
    consumerDiscarded: 0,
    skipped: 0,
    discarded: 0,
    wakes: 0,
    timeouts: 0,
    delivered: 0,
    deliveredOurs: 0,
    rescans: 0,
    continuityLosses: 0,
    resolutions: 0,
    releasedOnResolution: 0,
    anchorResolutionsUnreleased: 0,
    anchorRenewals: 0,
    anchorRenewFailures: 0,
    leaseAttachFailures: 0,
    leasesPeak: 0,
    controlStallCount: 0,
    controlStallTotalMs: 0,
    controlStallMaxMs: 0,
    samplesAfterControl: 0,
    transientEventFaults: 0,
    slugsAddedMidrun: 0,
    marketsSeen: new Set<number>(),
    marketsHeld: 0,
    elapsedSeconds: 0,
  };
}

/** What one measurement loop produced, handed to the caller before any lease is released. */
interface Outcome {
  report: Report;
  modeEffective: string;
  note: string | null;
  anchor: string;
}

interface Held {
  index: number;
  slug: string;
  market: Market;
  stream: EventStream;
}

/** The measurement thread: one session over the whole shard segment, every market of this
 * consumer's half resolved on it, and one arrival-to-observation sample per state read.
 *
 * The session is a `connect()` on the anchor market — the first key in the slug file — held
 * for the whole run, because closing it would unmap the segment this loop is parked on. That
 * one lease is therefore never released, not even when the anchor market resolves; the report
 * says so on its own line rather than leaving the runner to wonder why one market's lease
 * count never reached zero.
 *
 * A refusal on that session — the daemon has no room, a market lives on another shard — is
 * counted and the run continues. `PMWS_IO` and `PMWS_ATTACH_INCOMPLETE` are not a refusal:
 * they are the control connection ending, taking every lease this session held with it, so
 * `leaseMarket`, `pollEvents`'s release, and `renewAnchor` throw `BenchError` for those two
 * instead of counting one more failure and continuing a run whose demand no longer exists. */
function runReader(
  args: Args,
  shared: Shared,
  afterMeasurement: (outcome: Outcome) => void,
): void {
  const slugs = readSlugFile(args.slugs);
  if (slugs.length === 0) {
    throw new BenchError(`${args.slugs} names no market`);
  }
  if (slugs.length > MAX_MARKETS) {
    throw new BenchError(`${args.slugs} names more than ${MAX_MARKETS} markets`);
  }
  const report = emptyReport();
  const requested = args.spinOnly ? "spin" : "parked";
  let modeEffective = requested;
  let note: string | null = null;
  const origin = Atomics.load(shared.clock, CLOCK_ORIGIN);
  const obsRecorder: ObsRecorder | null = args.obsOut !== null ? createObsRecorder(args.obsOut) : null;

  const segment = Segment.connect(args.control, slugs[0]);
  try {
    const held = new Map<number, Held>();
    const byDirectoryIndex = new Map<number, Held>();
    const pending = new Map<number, number>();
    const released = new Set<number>();
    const lastSampledRevision = new Map<number, bigint>();

    /** Resolves `slug` on the reader's session and attaches its stream, answering `false`
     * while the segment cannot serve it yet.
     *
     * A transient status is a "not yet", not a failure: the daemon installs a market's
     * directory entry before it publishes that market's first book, so a session that resolves
     * one in the window between the two meets a writer mid-publish. The caller retries on its
     * next pass, exactly as it retries a market the directory does not carry at all. */
    const holdMarket = (index: number, slug: string): boolean => {
      let market: Market;
      let stream: EventStream;
      try {
        market = segment.resolve(args.venue, args.kind, slug);
        stream = market.attach().stream;
      } catch (error) {
        if (isPmwsError(error) && error.code === "PMWS_MARKET_NOT_FOUND") {
          return false;
        }
        if (isTransient(error)) {
          return false;
        }
        throw error;
      }
      const entry = { index, slug, market, stream };
      held.set(index, entry);
      byDirectoryIndex.set(market.directoryIndex, entry);
      report.marketsHeld = held.size;
      return true;
    };

    /** Records any calibration generation the keeper has announced as spanning a clock step.
     *
     * Read once per loop pass — at least every park timeout, and the keeper announces at most
     * one per calibration interval, so no announcement is missed between two passes. */
    const notePoisonedCalibration = (): void => {
      const poisoned = Atomics.load(shared.control, CONTROL_CLOCK_POISONED);
      if (poisoned !== 0) {
        report.poisoned.add(poisoned);
      }
    };

    let contaminationBudget = 0;

    /** Runs one `lease()`/`release()` conversation on this session, timing its wall duration
     * into `controlStallCount`/`controlStallTotalMs`/`controlStallMaxMs` and arming the
     * drain-scoped exclusion `observe` spends into `samplesAfterControl`.
     *
     * Both conversations run on this consumer's one session, inline on the measurement thread
     * this loop parks on — the one-session design this file commits to, and `renewAnchor`
     * already charges the same thread the same way — so each one blocks the loop for however
     * long the round trip takes. That delay is this benchmark's own control plane, not the
     * daemon's delivery path. A single stall can delay observations from more than one market
     * at once — one blocked round trip, several dirty entries queued behind it — so this arms
     * `contaminationBudget` to 2 rather than counting one token per conversation: `drainDirty`
     * spends one unit of that budget at the end of every drain pass it runs, which excludes
     * the remainder of whatever pass is under way (if any) through the end of the next one,
     * and a further conversation before that budget is spent simply re-arms it to 2 rather
     * than stacking more passes on top. The timing and the re-armed budget are recorded
     * whether or not `action` throws, because the loop was blocked either way; the caller's
     * own error handling is untouched. */
    const timedControl = <T,>(action: () => T): T => {
      const start = process.hrtime.bigint();
      try {
        return action();
      } finally {
        const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
        report.controlStallCount += 1;
        report.controlStallTotalMs += elapsedMs;
        report.controlStallMaxMs = Math.max(report.controlStallMaxMs, elapsedMs);
        contaminationBudget = 2;
      }
    };

    /** Renews the one control session this consumer holds, on the reader's own thread.
     *
     * The one control syscall this consumer charges to the measurement thread — and now the
     * only one there is, since every market's lease lives on this same session rather than a
     * worker's own: a native session cannot cross a worker boundary, so lease, release, and
     * renew all run here. `segment.renew()` renews every lease the session holds in one line
     * out and one line back, so the anchor's attachment and every market leased since are all
     * renewed by this one call. One renewal per `renewIntervalMs` — at most one per 5 s
     * against a daemon that declares a TTL, and none against one that declares none — is what
     * that costs.
     *
     * `PMWS_IO` and `PMWS_ATTACH_INCOMPLETE` end the control connection and release every
     * lease it held, so a renewal that throws either is fatal, not counted: this throws
     * `BenchError` and ends the run rather than incrementing `anchorRenewFailures` for a
     * session that no longer holds anything. Every other failure is counted exactly as
     * before, including one that carries no status to classify at all. */
    const renewAnchor = (): void => {
      try {
        segment.renew();
        report.anchorRenewals += 1;
      } catch (error) {
        if (isPmwsError(error) && CONNECTION_LOST_CODES.has(error.code)) {
          throw connectionLostError(error);
        }
        report.anchorRenewFailures += 1;
      }
    };

    /** Drains one market's retained deliveries, releasing its lease on its first resolution.
     *
     * A market's lease is released once. Limitless was observed sending one market's
     * `marketResolved` three times byte-identically inside 200 ms, and no delivery identity
     * exists to deduplicate them by, so every copy is counted in `resolutions_observed` and
     * only the first releases anything.
     *
     * `PMWS_IO` and `PMWS_ATTACH_INCOMPLETE` from that release are fatal for the same reason
     * they are in `leaseMarket` and `renewAnchor`: the control connection is gone, and with it
     * every other lease this session held, so this throws `BenchError` rather than letting the
     * raw error propagate as an unexplained crash. Every other error from the release keeps
     * propagating exactly as before.
     *
     * `entry.stream.nextEvent()` itself can fail transiently: `pmws_next_event`'s own contract
     * (`src/ffi/mod.rs`) names `PMWS_CONTENDED` and `PMWS_WRITER_STALLED` as transient — poll
     * again — against everything else, which is terminal — reattach or escalate. The
     * `isTransient` check below is against this consumer's own transient set, which is those
     * two plus `PMWS_NO_PUBLISHED_STATE`. A transient failure here is counted in
     * `transientEventFaults` and treated exactly like a `null` event: no event available right
     * now, and a later dirty signal will revisit this market. Anything else keeps
     * propagating. */
    const pollEvents = (entry: Held): void => {
      for (;;) {
        let event: Event | null;
        try {
          event = entry.stream.nextEvent();
        } catch (error) {
          if (error instanceof PmwsContinuityLost) {
            report.continuityLosses += 1;
            try {
              entry.stream.reattach();
            } catch (reattachError) {
              if (!isTransient(reattachError)) {
                throw reattachError;
              }
            }
            return;
          }
          if (isTransient(error)) {
            report.transientEventFaults += 1;
            return;
          }
          throw error;
        }
        if (event === null) {
          return;
        }
        if (event.kind === "resolution") {
          report.resolutions += 1;
          if (entry.index === 0) {
            report.anchorResolutionsUnreleased += 1;
          } else if (!released.has(entry.index)) {
            released.add(entry.index);
            try {
              timedControl(() => segment.release(entry.slug));
            } catch (error) {
              if (isPmwsError(error) && CONNECTION_LOST_CODES.has(error.code)) {
                throw connectionLostError(error);
              }
              throw error;
            }
            leased.delete(entry.index);
            report.releasedOnResolution += 1;
          }
        }
      }
    };

    /** Reads one market's state, samples its arrival-to-observation delta unless this revision
     * was already sampled, and then drains its event stream.
     *
     * Every observation made while `contaminationBudget` is armed is diverted to
     * `samplesAfterControl` instead of `endToEnd`/`consumer`, whether or not the conversation
     * that armed it actually overlapped the publication of the state being read here — a
     * conservative exclusion, not a proven one. A single stall can delay observations from
     * more than one market at once, so this is not "the first observation after a
     * conversation": it is every observation yielded from the moment a conversation completes
     * through the end of the next drain pass (`drainDirty` owns spending the budget down),
     * because that conversation blocked this loop on the measurement thread and any of those
     * observations' `consumer`/`endToEnd` delta could measure this benchmark's own
     * control-plane stall as much as it measures anything the daemon did. `daemon` —
     * `commitTime - arrivalTime`, two daemon stamps this process never sat between — is
     * untouched, because no stall on this thread can have contributed to it. */
    const observe = (entry: Held, calibration: Calibration): void => {
      let state;
      try {
        state = entry.market.readState();
      } catch (error) {
        if (isTransient(error)) {
          return;
        }
        throw error;
      }
      const directoryIndex = entry.market.directoryIndex;
      if (lastSampledRevision.get(directoryIndex) !== state.revision) {
        lastSampledRevision.set(directoryIndex, state.revision);
        const afterControl = contaminationBudget > 0;
        if (afterControl) {
          report.samplesAfterControl += 1;
        } else {
          const observed = epochNanos(process.hrtime.bigint(), origin, calibration);
          const endToEnd = difference(state.arrivalTime, observed);
          if (endToEnd.kind === "measured") {
            report.kept.push(endToEnd.value);
            report.keptGeneration.push(calibration.generation);
          } else if (endToEnd.kind === "implausible") {
            report.discarded += 1;
          } else {
            report.skipped += 1;
          }
          const consumer = difference(state.commitTime, observed);
          if (consumer.kind === "measured") {
            report.consumerKept.push(consumer.value);
            report.consumerKeptGeneration.push(calibration.generation);
          } else if (consumer.kind === "implausible") {
            report.consumerDiscarded += 1;
          }
          if (obsRecorder !== null) {
            const bids: DigestLevel[] = [];
            const asks: DigestLevel[] = [];
            for (const level of state.levels) {
              const target = level.side === "bid" ? bids : asks;
              target.push({ price: level.price.text, qty: level.quantity.text });
            }
            const digest = canonicalDigest(bids, asks);
            recordObs(obsRecorder, entry.slug, observed, digest, state.revision);
          }
        }
        const daemon = difference(state.arrivalTime, state.commitTime);
        if (daemon.kind === "measured") {
          report.daemonKept.push(daemon.value);
        } else if (daemon.kind === "implausible") {
          report.daemonDiscarded += 1;
        }
      }
      pollEvents(entry);
    };

    /** Drains the dirty ring, observing each delivered entry that names one of this consumer's
     * markets and ignoring the other consumer's half.
     *
     * One call is one drain pass for `contaminationBudget`'s purposes: whatever budget a
     * conversation armed is still in effect for every observation this pass makes, and the
     * `finally` below spends exactly one unit of it on the way out, however the pass ends —
     * ring exhausted, a rescan swept every market, or a throw. Spending one unit per pass,
     * rather than per observation, is what turns a conversation's armed budget of 2 into "the
     * remainder of this pass, plus the whole of the next one." */
    const drainDirty = (): void => {
      const calibration = readCalibration(shared);
      try {
        for (;;) {
          let entry;
          try {
            entry = segment.nextDirty();
          } catch (error) {
            if (error instanceof PmwsDirtyRescan) {
              report.rescans += 1;
              for (const candidate of held.values()) {
                observe(candidate, calibration);
              }
              return;
            }
            throw error;
          }
          if (entry === null) {
            return;
          }
          report.delivered += 1;
          const ours = byDirectoryIndex.get(entry.directoryIndex);
          if (ours === undefined) {
            continue;
          }
          report.deliveredOurs += 1;
          report.marketsSeen.add(entry.directoryIndex);
          observe(ours, calibration);
        }
      } finally {
        if (contaminationBudget > 0) {
          contaminationBudget -= 1;
        }
      }
    };

    const leased = new Set<number>();

    /** The one refusal family a market this session cannot ever serve throws, whether the
     * daemon says so immediately (`leaseMarket`, `PMWS_FOREIGN_SEGMENT`) or only after this
     * session gave up waiting for the market to resolve (`resolvePending`'s own timeout). */
    const crossShardError = (slug: string): BenchError =>
      new BenchError(
        `${slug} is leased but absent from this consumer's segment, so this half spans more ` +
          "than one shard; raise the daemon's markets_per_shard so the whole fleet lands in " +
          "one segment, or split the halves by shard",
      );

    /** The fatal condition a poisoned control connection throws: `PMWS_IO` or
     * `PMWS_ATTACH_INCOMPLETE` from any lease/release/renew conversation ends the control
     * connection and releases every lease this session held with it, so this run's demand no
     * longer exists and there is nothing left to report normally. */
    const connectionLostError = (error: unknown): BenchError =>
      new BenchError(
        `the control connection was lost (${isPmwsError(error) ? error.code : String(error)}): ` +
          "every lease this session held was released with it, so this run's demand no " +
          "longer exists",
      );

    /** Takes this session's lease on `known[index]`, the first time `index` is seen pending.
     *
     * Returns whether the lease was taken. A market this shard's segment does not hold throws
     * `PMWS_FOREIGN_SEGMENT`, which the daemon has already handed the lease back for by the
     * time this throws; that is the same "spans more than one shard" condition
     * `resolvePending`'s own settle-timeout raises further down, surfacing immediately here
     * instead of only after a wasted wait. `PMWS_IO` and `PMWS_ATTACH_INCOMPLETE` are fatal
     * for the same reason they are in `renewAnchor`: both end the control connection and
     * release every lease this session held, so this throws `BenchError` rather than counting
     * one more attach failure against a session that no longer holds anything. Any other
     * refusal — the daemon has no room for the market, the connection is gone — counts as an
     * ordinary attach failure and gives up on this market for the rest of the run, exactly as
     * a failed `connect()` used to. */
    const leaseMarket = (index: number): boolean => {
      try {
        timedControl(() => segment.lease(known[index]));
      } catch (error) {
        if (isPmwsError(error) && error.code === "PMWS_FOREIGN_SEGMENT") {
          throw crossShardError(known[index]);
        }
        if (isPmwsError(error) && CONNECTION_LOST_CODES.has(error.code)) {
          throw connectionLostError(error);
        }
        report.leaseAttachFailures += 1;
        pending.delete(index);
        return false;
      }
      leased.add(index);
      report.leasesPeak = Math.max(report.leasesPeak, leased.size);
      return true;
    };

    /** Leases and resolves every market this session cannot yet address.
     *
     * A market stays unresolvable until the daemon has installed it in the segment directory,
     * which is why an unresolved key is retried rather than refused — the same settle window
     * a freshly leased market gets whether or not the daemon's `lease()` answer turns out to
     * be immediately resolvable. A key still unresolved after `ATTACH_SETTLE_MS` from the
     * moment its lease was taken is a market this session was wrong to think shared its
     * shard: one session covers one segment, and a second segment would need a second parked
     * thread whose wakes are not this one's. A market refused as foreign at lease time never
     * reaches this timeout at all; see `leaseMarket`. */
    const resolvePending = (now: number): void => {
      for (const [index, firstSeen] of [...pending]) {
        let leasedAt = firstSeen;
        if (!leased.has(index)) {
          if (!leaseMarket(index)) {
            continue;
          }
          leasedAt = now;
          pending.set(index, now);
        }
        if (holdMarket(index, known[index])) {
          pending.delete(index);
          const entry = held.get(index);
          if (entry !== undefined) {
            observe(entry, readCalibration(shared));
          }
          continue;
        }
        if (now - leasedAt > ATTACH_SETTLE_MS) {
          throw crossShardError(known[index]);
        }
      }
    };

    /** Picks up whatever the runner appended to the slug file, drains every held market's
     * event stream, and leases and resolves whatever is new since the last rescan.
     *
     * The drain is here as well as on each dirty entry because a resolution is the last thing
     * a venue says about a market — observed on Limitless, the market's whole flow stops at it
     * — so a market whose resolution shares a wake with its own last update would otherwise
     * have that one delivery sitting unread for the rest of the run and its lease never
     * released. It carries no state read and takes no sample, so it costs the distribution
     * nothing. */
    const rescan = (known: string[], now: number): void => {
      for (const entry of held.values()) {
        pollEvents(entry);
      }
      const current = readSlugFile(args.slugs);
      for (let index = known.length; index < current.length && index < MAX_MARKETS; index += 1) {
        report.slugsAddedMidrun += 1;
        pending.set(index, now);
        known.push(current[index]);
      }
      resolvePending(now);
    };

    /** How long one park may block before this loop looks at its own housekeeping again.
     *
     * Short for the first `PENDING_EAGER_MS` after a market is leased, so a market this
     * session has just leased is resolved and its stream attached within milliseconds rather
     * than at the next `--rescan-ms` tick. A market's first venue frames can arrive inside that
     * window, and a stream attached after them starts past them: a resolution delivered in
     * that gap would sit unread for the rest of the run and its lease would never be released.
     *
     * Bounded by that window rather than by "anything is pending" because a market that never
     * appears would otherwise hold the whole run at a five-millisecond park cadence, and the
     * wake and timeout counts the report prints would then describe a poll this benchmark
     * never meant to measure. */
    const parkTimeoutMs = (now: number): number => {
      for (const firstSeen of pending.values()) {
        if (now - firstSeen <= PENDING_EAGER_MS) {
          return PENDING_POLL_TIMEOUT_MS;
        }
      }
      return PARK_POLL_TIMEOUT_MS;
    };

    holdMarket(0, slugs[0]);
    const known = [...slugs];
    if (obsRecorder !== null) {
      process.once("SIGTERM", () => {
        flushObsRecorder(obsRecorder, args.label, known.length);
        process.exit(0);
      });
    }
    const startedAt = process.hrtime.bigint();
    const deadline = startedAt + BigInt(Math.round(args.seconds * 1e9));
    for (let index = 1; index < slugs.length; index += 1) {
      pending.set(index, 0);
    }
    let lastGeneration = segment.publicationGeneration();
    let nextRescan = 0;
    const renewEvery = renewIntervalMs(args.leaseTtlMs);
    let nextAnchorRenew = renewEvery === null ? Infinity : renewEvery;
    for (;;) {
      const elapsedMs = Number(process.hrtime.bigint() - startedAt) / 1e6;
      if (process.hrtime.bigint() >= deadline) {
        break;
      }
      notePoisonedCalibration();
      if (args.untilResolutions > 0 && report.resolutions >= args.untilResolutions) {
        break;
      }
      if (elapsedMs >= nextRescan) {
        rescan(known, elapsedMs);
        nextRescan = elapsedMs + args.rescanMs;
      } else if (pending.size > 0) {
        resolvePending(elapsedMs);
      }
      if (elapsedMs >= nextAnchorRenew) {
        renewAnchor();
        nextAnchorRenew = elapsedMs + (renewEvery as number);
      }
      if (modeEffective === "parked") {
        let generation: bigint | null;
        try {
          generation = segment.wait(lastGeneration, {
            spinMicros: args.spinMicros,
            timeoutMs: parkTimeoutMs(elapsedMs),
          });
        } catch (error) {
          if (!isPmwsError(error) || error.code !== "PMWS_DOORBELL_UNAVAILABLE") {
            throw error;
          }
          modeEffective = "spin";
          note =
            "this attachment cannot park -- the segment's doorbell is on a sibling page whose " +
            "descriptor the control channel never transfers -- so the run continued as a spin poll";
          continue;
        }
        if (generation === null) {
          report.timeouts += 1;
        } else {
          report.wakes += 1;
          lastGeneration = generation;
          drainDirty();
        }
      } else {
        const roundDeadline = startedAt + BigInt(Math.round((nextRescan + args.rescanMs) * 1e6));
        let current = lastGeneration;
        for (;;) {
          current = segment.publicationGeneration();
          if (current !== lastGeneration) {
            break;
          }
          const hr = process.hrtime.bigint();
          if (hr >= roundDeadline || hr >= deadline) {
            break;
          }
        }
        if (current === lastGeneration) {
          report.timeouts += 1;
        } else {
          report.wakes += 1;
          lastGeneration = current;
          drainDirty();
        }
      }
    }
    notePoisonedCalibration();
    report.elapsedSeconds = Number(process.hrtime.bigint() - startedAt) / 1e9;
    if (obsRecorder !== null) {
      flushObsRecorder(obsRecorder, args.label, known.length);
    }
    afterMeasurement({ report, modeEffective, note, anchor: slugs[0] });
    holdUntilSentinel(args);
  } finally {
    segment.close();
  }
}

/** Blocks until `--hold-until` exists, or returns at once when it was not given.
 *
 * The report is emitted before this and every lease is still held while it blocks, so an
 * observer can see the markets this run released against the ones it still holds -- which
 * after this process exits it no longer can, because exiting releases everything. Bounded by
 * `HOLD_CAP_MS`, so a runner that never creates the path costs a run and not a wedged
 * process. */
function holdUntilSentinel(args: Args): void {
  if (args.holdUntil === null) {
    return;
  }
  const deadline = Date.now() + HOLD_CAP_MS;
  const sleeper = new Int32Array(new SharedArrayBuffer(4));
  while (!existsSync(args.holdUntil)) {
    if (Date.now() >= deadline) {
      return;
    }
    Atomics.wait(sleeper, 0, 0, HOLD_POLL_MS);
  }
}

/** Prints the run's report as `key: value` lines on stdout, `latency_probe`'s own shape and
 * `bench_consumer.py`'s exact key set.
 *
 * Percentiles are whole microseconds, truncated from the nanosecond samples; below
 * `MIN_REPORTABLE_SAMPLES` kept samples none is printed at all. */
/** Drops the samples measured under a calibration the keeper announced as spanning a host
 * clock step, and says how many went.
 *
 * Discarded, never clamped -- the same rule this consumer applies to a delta that runs
 * backwards or absurdly long. Only the two halves that difference this process's clock against
 * a daemon stamp go through here; the daemon's own half never does. */
function withoutPoisoned(
  samples: bigint[],
  generations: number[],
  poisoned: Set<number>,
): { kept: bigint[]; dropped: number } {
  const kept: bigint[] = [];
  let dropped = 0;
  for (let i = 0; i < samples.length; i += 1) {
    if (poisoned.has(generations[i])) {
      dropped += 1;
    } else {
      kept.push(samples[i]);
    }
  }
  return { kept, dropped };
}

/** Appends one split distribution under `prefix`, in the shape the end-to-end block prints its
 * own: the same nearest-rank quantiles, the same `MIN_REPORTABLE_SAMPLES` floor, percentiles
 * withheld rather than printed below it.
 *
 * The end-to-end block is written out longhand rather than routed through this so that every
 * key this consumer has printed since it shipped keeps its exact text. */
function pushDistribution(
  out: string[],
  prefix: string,
  kept: bigint[],
  discarded: number,
  steppedOut: number | null,
): void {
  out.push(`${prefix}_samples_kept: ${kept.length}`);
  if (kept.length < MIN_REPORTABLE_SAMPLES) {
    out.push(
      `${prefix}_percentiles: withheld, only ${kept.length} kept samples ` +
        `(floor is ${MIN_REPORTABLE_SAMPLES})`,
    );
  } else {
    const ordered = [...kept].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    out.push(`${prefix}_p50_us: ${quantile(ordered, 500) / 1000n}`);
    out.push(`${prefix}_p95_us: ${quantile(ordered, 950) / 1000n}`);
    out.push(`${prefix}_p99_us: ${quantile(ordered, 990) / 1000n}`);
    out.push(`${prefix}_p99.9_us: ${quantile(ordered, 999) / 1000n}`);
    out.push(`${prefix}_max_us: ${ordered[ordered.length - 1] / 1000n}`);
  }
  out.push(`${prefix}_samples_discarded: ${discarded}`);
  if (steppedOut !== null) {
    out.push(`${prefix}_samples_discarded_clock_step: ${steppedOut}`);
  }
}

/** Prints the run's report as `key: value` lines on stdout.
 *
 * `lease_renewals` and `lease_renew_failures` print a permanent zero: with one session,
 * `renewAnchor`'s single `renew()` call renews every lease the session holds, so there is no
 * longer a separate per-market renewal cadence for these two keys to count, and they stay in
 * the report — under `anchor_renewals`/`anchor_renew_failures` instead — so a reader diffing
 * this report against an older one is not met with a missing key.
 *
 * `control_stall_count`/`control_stall_total_ms`/`control_stall_max_ms` are `timedControl`'s
 * own count and wall duration of every `lease()`/`release()` conversation this run made, and
 * `samples_after_control` is how many observations `observe` diverted away from
 * `end_to_end`/`consumer` because a conversation's drain-scoped exclusion was still armed —
 * every observation from the remainder of the drain pass a conversation completed in through
 * the end of the next one, not just the first. Both exist so a control-plane stall this
 * benchmark caused is visible as itself, never folded into a latency number that reads as the
 * daemon's. A `PMWS_IO`/`PMWS_ATTACH_INCOMPLETE` conversation never reaches this report at
 * all: the control connection, and every lease on it, is already gone, so
 * `leaseMarket`, `pollEvents`'s release, and `renewAnchor` end the run through `BenchError`
 * instead. */
function printReport(args: Args, shared: Shared, outcome: Outcome): void {
  const out: string[] = [];
  const report = outcome.report;
  const requested = args.spinOnly ? "spin" : "parked";
  const calibration = readCalibration(shared);
  // A sample measured under a calibration that turned out to span a host clock step is
  // discarded, never clamped -- the same rule this consumer already applies to a delta that
  // runs backwards or absurdly long. The step is the host's, not the delivery path's, and a
  // distribution that kept those samples would report the host's clock as latency.
  const endToEnd = withoutPoisoned(report.kept, report.keptGeneration, report.poisoned);
  const consumer = withoutPoisoned(
    report.consumerKept,
    report.consumerKeptGeneration,
    report.poisoned,
  );
  const kept = endToEnd.kept;
  const steppedOut = endToEnd.dropped;
  out.push(`label: ${args.label}`);
  out.push("consumer: bench_consumer.ts");
  out.push(`runtime: node ${process.version}`);
  out.push(`mode: ${requested}`);
  if (outcome.modeEffective !== requested) {
    out.push(`mode_effective: ${outcome.modeEffective}`);
  }
  if (outcome.note !== null) {
    out.push(`note: ${outcome.note}`);
  }
  out.push(`spin_budget_us: ${args.spinMicros}`);
  out.push(
    "clock: monotonic process.hrtime.bigint plus a millisecond-edge calibrated offset, " +
      "recalibrated once a second off the measured thread",
  );
  out.push(`clock_derivations: ${Atomics.load(shared.clock, CLOCK_DERIVATIONS)}`);
  out.push(`clock_divergence_ns_per_s: ${Number(calibration.rate) / 1000}`);
  out.push(`clock_steps_detected: ${Atomics.load(shared.control, CONTROL_CLOCK_STEPS)}`);
  out.push(`clock_calibrations_rejected: ${Atomics.load(shared.control, CONTROL_CLOCK_REJECTED)}`);
  out.push(`lease_ttl_ms: ${args.leaseTtlMs === null ? "unspecified" : args.leaseTtlMs}`);
  const interval = renewIntervalMs(args.leaseTtlMs);
  out.push(`renew_interval_ms: ${interval === null ? "none" : interval}`);
  out.push(`duration_seconds: ${report.elapsedSeconds.toFixed(3)}`);
  out.push(`anchor_market: ${outcome.anchor}`);
  out.push(`markets_leased_peak: ${report.leasesPeak + 1}`);
  out.push(`markets_held: ${report.marketsHeld}`);
  out.push(`markets_seen: ${report.marketsSeen.size}`);
  out.push(`samples_kept: ${kept.length}`);
  if (kept.length < MIN_REPORTABLE_SAMPLES) {
    out.push(
      `percentiles: withheld, only ${kept.length} kept samples ` +
        `(floor is ${MIN_REPORTABLE_SAMPLES})`,
    );
  } else {
    const ordered = [...kept].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    out.push(`p50_us: ${quantile(ordered, 500) / 1000n}`);
    out.push(`p95_us: ${quantile(ordered, 950) / 1000n}`);
    out.push(`p99_us: ${quantile(ordered, 990) / 1000n}`);
    out.push(`p99.9_us: ${quantile(ordered, 999) / 1000n}`);
    out.push(`max_us: ${ordered[ordered.length - 1] / 1000n}`);
  }
  out.push(`wakes: ${report.wakes}`);
  const rate = report.elapsedSeconds > 0 ? report.wakes / report.elapsedSeconds : 0;
  out.push(`wakes_per_second: ${rate.toFixed(3)}`);
  out.push(`timeouts: ${report.timeouts}`);
  out.push(`dirty_delivered: ${report.delivered}`);
  out.push(`dirty_delivered_ours: ${report.deliveredOurs}`);
  out.push(`rescans: ${report.rescans}`);
  out.push(`samples_skipped: ${report.skipped}`);
  out.push(`samples_discarded: ${report.discarded}`);
  out.push(`samples_discarded_clock_step: ${steppedOut}`);
  out.push(`samples_after_control: ${report.samplesAfterControl}`);
  out.push(`transient_event_faults: ${report.transientEventFaults}`);
  out.push(`continuity_losses: ${report.continuityLosses}`);
  out.push("lease_renewals: 0");
  out.push("lease_renew_failures: 0");
  out.push(`lease_attach_failures: ${report.leaseAttachFailures}`);
  out.push(`anchor_renewals: ${report.anchorRenewals}`);
  out.push(`anchor_renew_failures: ${report.anchorRenewFailures}`);
  out.push(`control_stall_count: ${report.controlStallCount}`);
  out.push(`control_stall_total_ms: ${report.controlStallTotalMs.toFixed(3)}`);
  out.push(`control_stall_max_ms: ${report.controlStallMaxMs.toFixed(3)}`);
  out.push(`resolutions_observed: ${report.resolutions}`);
  out.push(`leases_released_on_resolution: ${report.releasedOnResolution}`);
  out.push(`anchor_resolutions_unreleased: ${report.anchorResolutionsUnreleased}`);
  out.push(`slugs_added_midrun: ${report.slugsAddedMidrun}`);
  out.push(
    "daemon_latency: commit_time - arrival_time, both stamped by the daemon on one clock in " +
      "one process -- this consumer's derived clock, and every calibration and step it " +
      "carries, has nothing to do with it",
  );
  pushDistribution(out, "daemon", report.daemonKept, report.daemonDiscarded, null);
  out.push("consumer_latency: observation - commit_time");
  pushDistribution(out, "consumer", consumer.kept, report.consumerDiscarded, consumer.dropped);
  out.push(
    "end_to_end: observation - arrival_time, reported above as samples_kept and p50_us " +
      "through max_us",
  );
  writeSync(1, `${out.join("\n")}\n`);
}

function main(): number {
  if (process.argv.slice(2).includes("--self-test")) {
    return selfTest();
  }
  let args: Args;
  try {
    args = parseArgs(process.argv.slice(2));
  } catch (error) {
    console.error(error instanceof UsageError ? error.message : String(error));
    return 2;
  }
  const buffer = sharedBuffer();
  const shared = sharedViews(buffer);
  Atomics.store(shared.clock, CLOCK_ORIGIN, process.hrtime.bigint());
  const keeper = new Worker(new URL(import.meta.url), {
    workerData: { buffer },
  });
  keeper.unref();
  const readyBy = Date.now() + CALIBRATION_READY_TIMEOUT_MS;
  const sleeper = new Int32Array(new SharedArrayBuffer(4));
  while (Atomics.load(shared.clock, CLOCK_DERIVATIONS) < BigInt(CALIBRATION_WINDOW)) {
    if (Date.now() > readyBy) {
      break;
    }
    Atomics.wait(sleeper, 0, 0, CALIBRATION_WARMUP_MS);
  }
  if (Atomics.load(shared.clock, CLOCK_DERIVATIONS) === 0n) {
    Atomics.store(shared.control, CONTROL_STOP, 1);
    console.error("bench_consumer.ts: the keeper thread never published a clock calibration");
    return 1;
  }
  try {
    runReader(args, shared, (outcome) => printReport(args, shared, outcome));
    return 0;
  } catch (error) {
    console.error(`bench_consumer.ts: ${error instanceof Error ? error.message : String(error)}`);
    return 1;
  } finally {
    Atomics.store(shared.control, CONTROL_STOP, 1);
    Atomics.notify(shared.control, CONTROL_SLEEPER);
  }
}

if (isMainThread) {
  process.exitCode = main();
} else {
  const options = workerData as KeeperOptions;
  try {
    runKeeper(options);
  } catch (error) {
    parentPort?.postMessage(String(error));
  }
}

/**
 * The Node.js binding over pm-ws's hand-rolled Node-API shim (`src/ffi/napi.rs`).
 *
 * Loaded with `process.dlopen`, not `require("pm-ws")`: there is no npm package, no
 * `node-gyp`, no `.node` build step. The shim resolves every `napi_*` symbol itself and
 * exports exactly twelve functions on `module.exports`; this file wraps those calls in
 * typed classes that mirror `bindings/python/pmws.py`'s `Segment`/`Market`/`EventStream`
 * shape. Every price and quantity stays a `{coefficient, scale, text}` triple — `text` comes
 * from the shim's own decimal renderer, never a locally formatted float.
 *
 * The loaded artifact is version-gated at import, as `pmws.py` gates its own: the shim's
 * `versions()` must name exactly the pair below. Nothing else in this module runs until it
 * does, because the search order below prefers a `target/release` build over
 * `target/debug` and a stale artifact left in either would otherwise serve wrong behaviour
 * silently.
 *
 * Usage:
 *
 *     import { Segment } from "../bindings/node/pmws.ts";
 *     const segment = new Segment("/path/to/segment");
 *     const market = segment.resolve("limitless", "slug", "some-market-slug");
 *     const { state, stream } = market.attach();
 *     console.log(state.best("bid"), state.best("ask"));
 *     const mutation = stream.nextEvent();
 */

import { existsSync } from "node:fs";
import { join } from "node:path";

export interface Decimal {
  coefficient: bigint;
  scale: number;
  text: string;
}

export interface Level {
  side: "bid" | "ask";
  price: Decimal;
  quantity: Decimal;
}

export interface Cursor {
  epoch: bigint;
  position: bigint;
}

/** A delivered retained mutation, as `pmws_next_event` fills it — already friendly-shaped by the shim. */
export interface MutationEvent {
  kind: "mutation";
  revision: bigint;
  cursor: Cursor;
  commitTime: bigint | undefined;
  /** Wall-clock nanoseconds since the Unix epoch at which the writer stamped this delivery,
   * or `undefined` when unset — mirrors `commitTime`'s own absence convention. */
  arrivalTime: bigint | undefined;
  origin: "sourceReported" | "normalizedFromSource" | "snapshotDiff" | "unknown";
  representation: number;
  nativeFamily: string;
  side: "bid" | "ask" | "unknown";
  price: Decimal;
  oldQuantity: Decimal | undefined;
  newQuantity: Decimal | undefined;
  daemonGeneration: bigint;
  subscriptionGeneration: bigint;
}

/** A delivered venue-reported market resolution, as `pmws_next_event` fills it. `revision` is
 * the book revision the resolution is ordered after — a resolution commits none of its own. */
export interface ResolutionEvent {
  kind: "resolution";
  revision: bigint;
  cursor: Cursor;
  commitTime: bigint | undefined;
  /** Wall-clock nanoseconds since the Unix epoch at which the writer stamped this delivery,
   * or `undefined` when unset — mirrors `commitTime`'s own absence convention. */
  arrivalTime: bigint | undefined;
  origin: "sourceReported" | "normalizedFromSource" | "snapshotDiff" | "unknown";
  representation: number;
  winningIndex: number;
  winningOutcome: string;
  marketType: string;
  resolutionDate: string;
  deliveryPath: "marketFeed" | "lifecycleFeed" | "resolutionFeed" | "unknown";
  daemonGeneration: bigint;
  subscriptionGeneration: bigint;
}

/** One delivered retained event: a level mutation or a venue-reported resolution, discriminated by `kind`. */
export type Event = MutationEvent | ResolutionEvent;

export interface SegmentInfo {
  instanceId: bigint;
  segmentGeneration: bigint;
  publicationGeneration: bigint;
  directoryCapacity: number;
  stateSlotCapacity: number;
  levelCapacity: number;
  eventCapacity: number;
}

interface RawState {
  revision: bigint;
  cursorEpoch: bigint;
  cursorPosition: bigint;
  syncDivergences: bigint;
  commitTime: bigint | undefined;
  arrivalTime: bigint | undefined;
  authorityState: number;
  authorityReason: number;
  continuityKind: number;
  continuityReason: number;
  origin: string | undefined;
  representation: number | undefined;
  nativeFamily: string | undefined;
  levels: Level[];
}

/** One entry of the segment's dirty-index ring: which directory entry changed and the state
 * revision it advertised. `bookRevision` is a state-read skip hint only, never a reason to
 * skip that market's event stream — a resolution republish advertises an unchanged revision.
 *
 * `directoryIndex` is the segment's own index, never the session-local index
 * {@link Segment.resolve} returns: build the inverse map once from {@link Market.directoryIndex}
 * over the markets this session resolved. */
export interface DirtyEntry {
  directoryIndex: number;
  bookRevision: bigint;
}

const AUTHORITY_STATES: Record<number, string> = {
  1: "Unsubscribed", 2: "Subscribing", 3: "Synchronizing", 4: "Live", 5: "Recovering", 6: "Stale",
};

const AUTHORITY_REASONS: Record<number, string> = {
  1: "Gap", 2: "Disconnect", 3: "SubscriptionLost", 4: "LocalLoss", 5: "OrderingUnknown",
  6: "Overload", 7: "ReplicaDivergence", 8: "RecoveryBaseUnavailable",
};

/** Matches `src/ffi/mod.rs`'s `break_word` — also duplicated in `bindings/python/pmws.py`. */
const CONTINUITY_REASONS: Record<number, string> = {
  1: "Overrun", 2: "Gap", 3: "LocalLoss", 4: "Reconnect", 5: "RecoveryBase", 6: "SyncDivergence",
};

/** `PMWS_STATUS_MALFORMED_RECORD` — an unrecognized discriminant. The import-time version gate
 * rules out a dylib built against another generation of this ABI, so what reaches here is a
 * record this build's own decoder could not read. */
export class PmwsMalformedRecord extends Error {
  readonly code = "PMWS_MALFORMED_RECORD";

  constructor(detail: string) {
    super(`malformed record: ${detail}`);
    this.name = "PmwsMalformedRecord";
  }
}

function malformed(detail: string): never {
  throw new PmwsMalformedRecord(detail);
}

function authorityText(stateWord: number, reasonWord: number): string {
  const name = AUTHORITY_STATES[stateWord];
  if (name === undefined) {
    malformed(`unknown authority discriminant ${stateWord}`);
  }
  if (name !== "Stale") {
    if (reasonWord !== 0) {
      malformed(`authority ${name} carries reason ${reasonWord}`);
    }
    return name;
  }
  const reason = AUTHORITY_REASONS[reasonWord];
  if (reason === undefined) {
    malformed(`unknown authority reason ${reasonWord}`);
  }
  return `Stale(${reason})`;
}

/**
 * The napi boundary only ever throws `PMWS_CONTINUITY_LOST` with the reason word folded into
 * the message text (`throw_continuity_lost` in `src/ffi/napi.rs`) — no structured field
 * carries it across the JS boundary, so this is the only place it can be recovered.
 */
function parseContinuityReason(message: string): string {
  const match = /reason word (\d+)/.exec(message);
  if (match === null) {
    malformed(`unparsable continuity-lost message: ${message}`);
  }
  const word = Number(match[1]);
  const reason = CONTINUITY_REASONS[word];
  if (reason === undefined) {
    malformed(`unknown continuity reason ${word}`);
  }
  return reason;
}

/** One published book state, wrapped from the shim's raw `attach`/`readState`/`reattach` object. */
export class State {
  readonly revision: bigint;
  readonly cursorEpoch: bigint;
  readonly cursorPosition: bigint;
  readonly syncDivergences: bigint;
  readonly commitTime: bigint | null;
  readonly arrivalTime: bigint | null;
  readonly authority: string;
  readonly continuityIntact: boolean;
  readonly continuityReason: string | null;
  readonly origin: string | null;
  readonly representation: number | null;
  readonly nativeFamily: string | null;
  readonly levels: Level[];

  constructor(raw: RawState) {
    this.revision = raw.revision;
    this.cursorEpoch = raw.cursorEpoch;
    this.cursorPosition = raw.cursorPosition;
    this.syncDivergences = raw.syncDivergences;
    this.commitTime = raw.commitTime ?? null;
    this.arrivalTime = raw.arrivalTime ?? null;
    this.authority = authorityText(raw.authorityState, raw.authorityReason);
    this.continuityIntact = raw.continuityKind === 1;
    this.continuityReason = this.continuityIntact ? null : (CONTINUITY_REASONS[raw.continuityReason] ?? null);
    if (!this.continuityIntact && this.continuityReason === null) {
      malformed(`unknown continuity reason ${raw.continuityReason}`);
    }
    this.origin = raw.origin ?? null;
    this.representation = raw.representation ?? null;
    this.nativeFamily = raw.nativeFamily ?? null;
    this.levels = raw.levels;
  }

  /**
   * The best level on `side`, or `null`. Levels ascend by `(side, price)`, so the best bid is
   * the last matching level and the best ask is the first. A level resting an exact zero
   * quantity is reported state, not depth, and is skipped.
   */
  best(side: "bid" | "ask"): Level | null {
    const matching = this.levels.filter((level) => level.side === side && level.quantity.coefficient !== 0n);
    if (matching.length === 0) {
      return null;
    }
    return side === "bid" ? matching[matching.length - 1] : matching[0];
  }
}

/** `PMWS_STATUS_CONTINUITY_LOST`, thrown by `EventStream.nextEvent`. Sticky until `reattach()`. */
export class PmwsContinuityLost extends Error {
  readonly code = "PMWS_CONTINUITY_LOST";
  readonly reason: string;

  constructor(reason: string) {
    super(`continuity lost: ${reason}`);
    this.name = "PmwsContinuityLost";
    this.reason = reason;
  }
}

/** `PMWS_DIRTY_RESCAN`, thrown by `Segment.nextDirty`: the declared full-rescan signal.
 *
 * Distinct from {@link PmwsContinuityLost} and never sticky — this segment's dirty cursor
 * has already rebased to the ring's current head by the time this throws, so the very next
 * `nextDirty()` call resumes ordinary polling. On catching this, re-read every market in the
 * caller's interest set once before resuming.
 */
export class PmwsDirtyRescan extends Error {
  readonly code = "PMWS_DIRTY_RESCAN";

  constructor() {
    super(
      "dirty-index ring lapped this cursor; re-read every attached market's state and " +
        "events once, then resume nextDirty()",
    );
    this.name = "PmwsDirtyRescan";
  }
}

/** Status codes retryable at the call site rather than fatal — mirrors `bbo.py`'s `TRANSIENT`. */
export const TRANSIENT_CODES: ReadonlySet<string> = new Set([
  "PMWS_CONTENDED",
  "PMWS_WRITER_STALLED",
  "PMWS_NO_PUBLISHED_STATE",
]);

export function isPmwsError(error: unknown): error is Error & { code: string } {
  return error instanceof Error && typeof (error as { code?: unknown }).code === "string";
}

export function isTransient(error: unknown): boolean {
  return isPmwsError(error) && TRANSIENT_CODES.has(error.code);
}

interface NativeExports {
  versions(): { ffi: number; abi: number };
  open(path: string): unknown;
  connect(controlSocket: string, market: string): unknown;
  close(session: unknown): void;
  renew(session: unknown): void;
  lease(session: unknown, market: string): void;
  release(session: unknown, market: string): void;
  segmentInfo(session: unknown): SegmentInfo;
  resolve(session: unknown, venue: string, kind: string, key: string): number;
  attach(session: unknown, market: number): RawState;
  readState(session: unknown, market: number): RawState;
  reattach(session: unknown, market: number): RawState;
  nextEvent(session: unknown, market: number): Event | undefined;
  publicationGeneration(session: unknown): bigint;
  wait(session: unknown, lastGeneration: bigint, spinMicros: number, timeoutMillis: number): bigint | null;
  nextDirty(session: unknown): DirtyEntry | null;
  marketDirectoryIndex(session: unknown, market: number): number;
}

const REQUIRED_FUNCTIONS = [
  "versions", "open", "connect", "close", "renew", "lease", "release", "segmentInfo",
  "resolve", "attach", "readState", "reattach", "nextEvent", "publicationGeneration", "wait",
  "nextDirty", "marketDirectoryIndex",
] as const;

/** `PMWS_FFI_VERSION` in `src/ffi/mod.rs`. Bumps with that constant, never on its own. */
const EXPECTED_FFI_VERSION = 8;
/** `ABI_VERSION` in `src/shm/layout.rs`. Bumps with that constant, never on its own. */
const EXPECTED_ABI_VERSION = 5;

/** `PMWS_MAX_SPIN_MICROS` in `src/ffi/mod.rs`: the widest `spinMicros` {@link Segment.wait}
 * accepts. Mirrored rather than read back — it is a compile-time constant of the C ABI, not an
 * exported function — and the import-time version gate is what keeps the two from drifting. */
export const MAX_SPIN_MICROS = 10_000_000;

/** The widest `timeoutMs` the C ABI's `int32_t` parameter carries. */
export const MAX_TIMEOUT_MS = 2_147_483_647;

/** The generation counter is a `uint64_t` on the wire. */
const MAX_GENERATION = (1n << 64n) - 1n;

/** Refuses a value that is not a safe integer in `[low, high]` before it reaches the native
 * narrowing, which applies ECMAScript's own wrapping `ToInt32`/`ToUint32` rather than failing:
 * `-1` would otherwise arrive as `4294967295`. */
function checkedInteger(name: string, value: number, low: number, high: number): number {
  if (!Number.isInteger(value)) {
    throw new TypeError(`${name} must be an integer, got ${value}`);
  }
  if (value < low || value > high) {
    throw new RangeError(`${name} must be in [${low}, ${high}], got ${value}`);
  }
  return value;
}

function libraryName(): string {
  if (process.platform === "darwin") {
    return "libpm_ws.dylib";
  }
  if (process.platform === "win32") {
    return "pm_ws.dll";
  }
  return "libpm_ws.so";
}

function libraryPath(): string {
  const override = process.env.PMWS_LIB;
  if (override !== undefined && override.length > 0) {
    return override;
  }
  const name = libraryName();
  const repoRoot = join(import.meta.dirname, "..", "..");
  for (const profile of ["release", "debug"]) {
    const candidate = join(repoRoot, "target", profile, name);
    if (existsSync(candidate)) {
      return candidate;
    }
  }
  throw new Error(
    `pm-ws cdylib not found; build it with \`cargo build --release\` (or --debug) or set ` +
      `PMWS_LIB to its path (looked for ${name} under ${join(repoRoot, "target")}/{release,debug})`,
  );
}

interface DlopenModule {
  exports: Record<string, unknown>;
}

function loadNative(): NativeExports {
  const path = libraryPath();
  const mod: DlopenModule = { exports: {} };
  process.dlopen(mod, path);
  for (const name of REQUIRED_FUNCTIONS) {
    if (typeof mod.exports[name] !== "function") {
      throw new Error(
        `pm-ws node binding: missing native export '${name}' in ${path}; the dylib and this ` +
          "binding are out of sync",
      );
    }
  }
  const exports = mod.exports as unknown as NativeExports;
  const found = exports.versions();
  if (found.ffi !== EXPECTED_FFI_VERSION || found.abi !== EXPECTED_ABI_VERSION) {
    throw new Error(
      `pm-ws node binding: version mismatch in ${path}; this binding expects ` +
        `ffi=${EXPECTED_FFI_VERSION} abi=${EXPECTED_ABI_VERSION}, the loaded library reports ` +
        `ffi=${found.ffi} abi=${found.abi}. Rebuild the cdylib from this source tree, or ` +
        "point PMWS_LIB at the matching artifact.",
    );
  }
  return exports;
}

const native = loadNative();

/** A market's retained-mutation cursor, attached by `Market.attach`. */
export class EventStream {
  private readonly market: Market;

  constructor(market: Market) {
    this.market = market;
  }

  /**
   * The next retained mutation, or `null` when the writer has not reached it yet.
   *
   * Throws {@link PmwsContinuityLost} on a loss — sticky, repeated on every later call until
   * {@link reattach}; the native session tracks the stickiness, not this wrapper.
   */
  nextEvent(): Event | null {
    try {
      const raw = native.nextEvent(this.market.handle, this.market.index);
      return raw === undefined ? null : raw;
    } catch (error) {
      if (isPmwsError(error) && error.code === "PMWS_CONTINUITY_LOST") {
        throw new PmwsContinuityLost(parseContinuityReason(error.message));
      }
      throw error;
    }
  }

  /** Re-establishes this stream past a continuity loss; returns the resumed state. */
  reattach(): State {
    return new State(native.reattach(this.market.handle, this.market.index));
  }
}

/** One book resolved in a {@link Segment}, addressed by a session-local index.
 *
 * `directoryIndex` is the segment's own directory index for this book — the number
 * {@link Segment.nextDirty} delivers — and is what maps a dirty entry back to a market. It is
 * not `index`: `index` is a dense row number the session hands out in resolution order, so a
 * session that resolves the segment's third market first holds `index` 0 for `directoryIndex`
 * 2. Both are fixed for the life of the session. */
export class Market {
  readonly handle: unknown;
  readonly index: number;
  readonly directoryIndex: number;
  readonly venue: string;
  readonly kind: string;
  readonly key: string;

  constructor(
    handle: unknown,
    index: number,
    directoryIndex: number,
    venue: string,
    kind: string,
    key: string,
  ) {
    this.handle = handle;
    this.index = index;
    this.directoryIndex = directoryIndex;
    this.venue = venue;
    this.kind = kind;
    this.key = key;
  }

  /** Attaches this market's latest state and its mutation stream as one step. */
  attach(): { state: State; stream: EventStream } {
    const state = new State(native.attach(this.handle, this.index));
    return { state, stream: new EventStream(this) };
  }

  /** This market's latest published state, without moving any attached stream. */
  readState(): State {
    return new State(native.readState(this.handle, this.index));
  }
}

/** Options for {@link Segment.wait}. `spinMicros` defaults to `0` (pure parked mode);
 * `timeoutMs` defaults to `undefined`, which parks indefinitely. */
export interface WaitOptions {
  spinMicros?: number;
  timeoutMs?: number;
}

/** A native session this module already obtained, handed to {@link Segment}'s constructor in
 * place of a path. Not exported: the only way to make one is {@link Segment.connect}. */
const ADOPT: unique symbol = Symbol("pmws.adopt");
interface Adopted {
  readonly [ADOPT]: unknown;
}

/** An attached, read-only publication segment: open/close, and the markets in it. */
export class Segment {
  private handle: unknown;
  private closed: boolean;
  readonly info: SegmentInfo;

  constructor(source: string | Adopted) {
    this.handle = typeof source === "string" ? native.open(source) : source[ADOPT];
    this.closed = false;
    this.info = native.segmentInfo(this.handle);
  }

  /**
   * Attaches to the segment carrying `market` by asking the `pmwsd` listening on the Unix
   * socket at `controlSocket` for it, over descriptor transfer.
   *
   * No path to a segment is named, opened, or needed: the daemon sends the segment's own
   * read-only descriptor — the one it opened when it created the file — on the same message as
   * its answer, and possession of that descriptor is the authorization.
   *
   * **The caller must run as the same operating-system user as the daemon.** A peer running as
   * another user is refused before the daemon discloses anything, and throws an `Error` whose
   * `code` is `PMWS_ATTACH_REFUSED`; peer identity is a check on top of the segment's own file
   * permissions, never a replacement for them.
   *
   * **On a page-placement segment, a segment reached this way spins or polls; it does not
   * park.** The doorbell page's descriptor is never transferred — parking needs a writable
   * mapping, and a writable mapping carries a writable length, so that descriptor would let any
   * holder truncate a file the daemon stores through. {@link Segment.wait} on such a segment
   * throws an `Error` whose `code` is `PMWS_DOORBELL_UNAVAILABLE` on its first park rather than
   * blocking — the segment itself is fine, only parked waiting is not; poll
   * {@link Segment.publicationGeneration}, or spin with a `spinMicros` budget, instead.
   * Construct a `Segment` on a path (same user as the daemon) when parked waiting is wanted.
   *
   * **The attachment leases the market.** Connecting takes a lease on `market` for as long as
   * this segment is open: a market no other consumer holds and no operator pinned is subscribed
   * at the venue to serve this call, and released when the last lease on it goes.
   * {@link Segment.close} — and this process exiting, however it exits — releases it. Leases are
   * counted, so a second consumer of the same market costs no venue traffic and keeps the market
   * alive after the first one leaves. An operator's pin outlives every lease.
   *
   * A daemon configured with a lease TTL also expects to hear from this connection within it;
   * {@link Segment.renew} is how a consumer with nothing else to say says something. A daemon
   * configured without one — the default — needs no renewals at all.
   *
   * The result is an ordinary {@link Segment} covering every market in that shard's segment,
   * not only the one named: resolve any of them on it. The daemon answers as soon as it holds
   * the market, which may be before the venue has said anything about it: the book reads as
   * synchronizing until its first venue base lands, exactly as any quiet market's does.
   *
   * This call is synchronous and blocks the JS event loop for the length of one control
   * conversation, bounded by one five-second deadline over the exchange rather than per step,
   * so a daemon answering slowly cannot extend it. The deadline runs from the established
   * connection; connecting to the socket is the one step outside it, because no timed connect
   * exists for a Unix domain socket.
   */
  static connect(controlSocket: string, market: string): Segment {
    return new Segment({ [ADOPT]: native.connect(controlSocket, market) });
  }

  /**
   * Renews this segment's market leases at the daemon that granted them.
   *
   * One line out and one line back on the control connection {@link Segment.connect} opened,
   * synchronous and bounded by the same five-second deadline the attach conversation is. It
   * carries no market data and moves no cursor.
   *
   * Only for a segment from {@link Segment.connect}: a segment constructed from a path holds no
   * lease, because nothing granted it one, and renewing it throws an `Error` whose `code` is
   * `PMWS_INVALID_ARGUMENT`. A `PMWS_IO` throw means the control connection is gone, and with it
   * this segment's leases — the mapping stays readable, and what it reports from then on is a
   * book the daemon may have stopped maintaining.
   *
   * Needed only against a daemon configured with a lease TTL. Sending one anyway is harmless:
   * any request on the connection renews it.
   */
  renew(): void {
    native.renew(this.handle);
  }

  /**
   * Takes a further market lease on the control connection {@link Segment.connect} opened, for
   * a market in this segment's own shard.
   *
   * One line out and one line back, synchronous and bounded by the same five-second deadline
   * the attach conversation is — one budget for the whole call, the rollback below included.
   * This segment already maps every market in its shard, so nothing
   * new is mapped and nothing is returned: what the call buys is the *demand*, so the daemon
   * keeps `market` subscribed for this session. Resolve it afterwards like any other market in
   * the segment. A session may hold any number of leases this way; {@link Segment.release}
   * gives one back and {@link Segment.close} gives back all of them.
   *
   * Throws an `Error` whose `code` is `PMWS_FOREIGN_SEGMENT` when `market` lives in another
   * shard: its book is in a segment this session does not map, so the lease is handed back —
   * and confirmed back — connect a second `Segment` for that market. `PMWS_INVALID_ARGUMENT`
   * for a segment constructed from a path, which has no control connection to lease on, or an
   * identifier the daemon rejects; `PMWS_ATTACH_REFUSED` when the daemon refuses the request,
   * including for want of room to take the market; `PMWS_MARKET_NOT_FOUND` when the daemon
   * answers that it does not hold it; `PMWS_ATTACH_INCOMPLETE` when the answer's descriptors
   * did not arrive as promised; `PMWS_IO` when the conversation fails or a rollback goes
   * unconfirmed.
   *
   * The two that end the conversation — `PMWS_IO` and `PMWS_ATTACH_INCOMPLETE` — end the
   * control connection with it, and with it every lease this segment held: later `lease`,
   * `release` and `renew` calls throw `PMWS_INVALID_ARGUMENT`, as they do for a segment
   * constructed from a path. The refusals leave the connection usable, and the mapping stays
   * readable either way.
   */
  lease(market: string): void {
    native.lease(this.handle, market);
  }

  /**
   * Gives up this segment's lease on `market`, keeping the session, its other leases, and its
   * mapping. Idempotent: releasing a market this session never leased, or releasing one twice,
   * succeeds.
   *
   * The mapping is untouched — releasing even the market {@link Segment.connect} was called
   * with is legal, and the segment stays readable afterwards. What goes away is this session's
   * demand: once nothing else holds `market` and no operator pinned it, the daemon unsubscribes
   * it at the venue and this segment goes on reading a book that has stopped being maintained.
   *
   * Throws an `Error` whose `code` is `PMWS_INVALID_ARGUMENT` for a segment constructed from a
   * path, which holds no lease to give back, or an identifier the daemon rejects, and `PMWS_IO`
   * when the conversation fails — which means the control connection is gone, and with it every
   * lease this segment held: later `lease`, `release` and `renew` calls throw
   * `PMWS_INVALID_ARGUMENT`, and the mapping stays readable. A rejected identifier is not that
   * — the conversation finished, and the connection carries on.
   */
  release(market: string): void {
    native.release(this.handle, market);
  }

  /**
   * Releases this segment's market leases and frees the native session.
   *
   * For a segment from {@link Segment.connect} this closes the control connection too, which is
   * what releases its leases: a market nothing else holds is unsubscribed at the venue shortly
   * after. Keeping the mapping without the session does not keep the subscription alive — it
   * keeps a reader on a book that stops being maintained. Idempotent.
   */
  close(): void {
    if (!this.closed) {
      native.close(this.handle);
      this.closed = true;
    }
  }

  /** A coalescible hint that newer data exists somewhere in the segment. */
  publicationGeneration(): bigint {
    return native.publicationGeneration(this.handle);
  }

  /**
   * Blocks the calling thread until {@link publicationGeneration} changes from
   * `lastGeneration`, returning the new generation, or until `options.timeoutMs`
   * milliseconds pass with no change, returning `null`. `timeoutMs` left `undefined` parks
   * indefinitely.
   *
   * Spins for up to `options.spinMicros` microseconds with no syscall before parking on the
   * segment's doorbell — `spinMicros: 0` (the default) is pure parked mode, and
   * {@link MAX_SPIN_MICROS} is the widest budget the C ABI accepts.
   *
   * Parking needs a doorbell this session can reach. A segment constructed from a path always
   * has one; a segment from {@link Segment.connect} has one only where the platform puts the
   * doorbell in the segment header, because the sibling-page placement is never transferred
   * over the control channel. Where it cannot park, the park itself throws — it never blocks
   * and never reports a change that did not happen — so a consumer that wants to work under
   * either placement polls {@link Segment.publicationGeneration} or passes a `spinMicros`
   * budget and treats the throw as "this attachment spins".
   *
   * Every argument is checked here rather than left to the native narrowing, which applies
   * ECMAScript's wrapping conversions instead of failing: `spinMicros: -1` would otherwise
   * reach the C ABI as `4294967295` and ask this thread to spin for seventy-one minutes.
   * Throws `TypeError` for a wrong type or a non-integer and `RangeError` for an out-of-range
   * value.
   *
   * This call is synchronous and blocks the JS event loop for its full duration: the
   * dedicated, non-JS-event-loop consumer pattern this binding assumes for a wake-driven
   * reader, not something to call from a server handling concurrent requests on the same
   * thread.
   */
  wait(lastGeneration: bigint, options: WaitOptions = {}): bigint | null {
    if (typeof lastGeneration !== "bigint") {
      throw new TypeError(`lastGeneration must be a bigint, got ${typeof lastGeneration}`);
    }
    if (lastGeneration < 0n || lastGeneration > MAX_GENERATION) {
      throw new RangeError(`lastGeneration must be in [0, 2**64), got ${lastGeneration}`);
    }
    const spinMicros = checkedInteger("spinMicros", options.spinMicros ?? 0, 0, MAX_SPIN_MICROS);
    const timeoutMillis =
      options.timeoutMs === undefined
        ? -1
        : checkedInteger("timeoutMs", options.timeoutMs, 0, MAX_TIMEOUT_MS);
    return native.wait(this.handle, lastGeneration, spinMicros, timeoutMillis);
  }

  /**
   * The next entry of the segment's dirty-index ring — which directory entry changed and
   * the state revision it advertised — or `null` when the writer has not reached this
   * session's cursor position yet.
   *
   * This session's cursor into the ring is created, lazily, at the ring's current head on
   * the first call. Throws {@link PmwsDirtyRescan} — never sticky — for the declared
   * full-rescan signal.
   *
   * `directoryIndex` names a segment directory entry, not a session-local market: a consumer
   * that wants to know which of its markets changed keeps the inverse map built once from
   * {@link Market.directoryIndex}.
   */
  nextDirty(): DirtyEntry | null {
    try {
      return native.nextDirty(this.handle);
    } catch (error) {
      if (isPmwsError(error) && error.code === "PMWS_DIRTY_RESCAN") {
        throw new PmwsDirtyRescan();
      }
      throw error;
    }
  }

  /** Resolves a venue-native identity to a {@link Market} in this session. */
  resolve(venue: string, kind: string, key: string): Market {
    const index = native.resolve(this.handle, venue, kind, key);
    const directoryIndex = native.marketDirectoryIndex(this.handle, index);
    return new Market(this.handle, index, directoryIndex, venue, kind, key);
  }
}

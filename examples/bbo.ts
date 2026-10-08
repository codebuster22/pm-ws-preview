#!/usr/bin/env node
/**
 * A thin strategy-shaped consumer over the pm-ws Node binding (`bindings/node/pmws.ts`).
 *
 * Attaches to one market's shared-memory book and follows it: one `bbo` line per revision
 * change, and with `--events` one `mutation` line per delivered mutation and one `resolution`
 * line per delivered venue-reported market resolution. Revision changes are observed through
 * `Segment.wait` — a spin-then-park block on the segment's doorbell, not a fixed-interval
 * poll — so a woken pass always finds fresh state waiting; the wait itself is bounded so the
 * run still exits at its own deadline with nothing new to report. A continuity loss on the
 * mutation stream prints `continuity_lost reason=...`, reattaches, and prints
 * `reattach revision=R` — the same recovery `examples/bbo.py` performs.
 *
 * Usage:
 *     node bbo.ts --segment <path> --market <native-key> [--venue limitless] [--kind slug]
 *                 [--seconds 10] [--events]
 */

import {
  Segment,
  Market,
  State,
  isPmwsError,
  isTransient,
  PmwsContinuityLost,
  type Event,
  type ResolutionEvent,
} from "../bindings/node/pmws.ts";

const POLL_INTERVAL_MS = 1;
const SPIN_MICROS = 200;
const WAIT_TIMEOUT_MS = 1000;

interface Args {
  segment: string;
  market: string;
  venue: string;
  kind: string;
  seconds: number;
  events: boolean;
}

class UsageError extends Error {}

function usage(): string {
  return (
    "usage: bbo.ts --segment <path> --market <native-key> [--venue limitless] " +
    "[--kind slug] [--seconds 10] [--events]"
  );
}

function parseArgs(argv: string[]): Args {
  let segment: string | undefined;
  let market: string | undefined;
  let venue = "limitless";
  let kind = "slug";
  let seconds = 10;
  let events = false;

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
    switch (flag) {
      case "--segment":
        segment = next();
        break;
      case "--market":
        market = next();
        break;
      case "--venue":
        venue = next();
        break;
      case "--kind":
        kind = next();
        break;
      case "--seconds": {
        const value = Number(next());
        if (!Number.isFinite(value) || value <= 0 || value > 86_400) {
          throw new UsageError(`--seconds must be a finite number, positive and at most 86400\n${usage()}`);
        }
        seconds = value;
        break;
      }
      case "--events":
        events = true;
        break;
      default:
        throw new UsageError(`unrecognized argument: ${flag}\n${usage()}`);
    }
  }
  if (segment === undefined) {
    throw new UsageError(`--segment is required\n${usage()}`);
  }
  if (market === undefined) {
    throw new UsageError(`--market is required\n${usage()}`);
  }
  return { segment, market, venue, kind, seconds, events };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function levelText(level: { price: { text: string }; quantity: { text: string } } | null): string {
  return level === null ? "-" : `${level.price.text}@${level.quantity.text}`;
}

function bboLine(state: State): string {
  return (
    `bbo revision=${state.revision} authority=${state.authority} ` +
    `best_bid=${levelText(state.best("bid"))} best_ask=${levelText(state.best("ask"))}`
  );
}

function mutationLine(mutation: Extract<Event, { kind: "mutation" }>): string {
  const old = mutation.oldQuantity === undefined ? "none" : mutation.oldQuantity.text;
  const next = mutation.newQuantity === undefined ? "none" : mutation.newQuantity.text;
  return (
    `mutation revision=${mutation.revision} cursor=${mutation.cursor.epoch}:${mutation.cursor.position} ` +
    `origin=${mutation.origin} side=${mutation.side} price=${mutation.price.text} qty=${old}->${next}`
  );
}

function resolutionLine(resolution: ResolutionEvent): string {
  return (
    `resolution revision=${resolution.revision} cursor=${resolution.cursor.epoch}:${resolution.cursor.position} ` +
    `origin=${resolution.origin} outcome=${resolution.winningOutcome} index=${resolution.winningIndex} ` +
    `type=${resolution.marketType} date=${resolution.resolutionDate} path=${resolution.deliveryPath}`
  );
}

/**
 * Retries `read()` while it throws a transient {@link isPmwsError}, until it succeeds or
 * `deadline` (a `performance.now()` timestamp) passes. Returns `null`, rather than throwing,
 * if `deadline` passes first — every caller treats that as "give up this pass", never as a
 * decode failure.
 */
async function retryTransient<T>(read: () => T, deadline: number): Promise<T | null> {
  for (;;) {
    try {
      return read();
    } catch (error) {
      if (!isTransient(error)) {
        throw error;
      }
    }
    if (performance.now() >= deadline) {
      return null;
    }
    await sleep(POLL_INTERVAL_MS);
  }
}

/**
 * Resolves `args.market` in `segment`, retrying `PMWS_MARKET_NOT_FOUND` and any transient
 * status until `deadline` passes. Returns `null` on timeout rather than throwing, so the
 * caller can print a clean message instead of a stack trace.
 */
async function waitForMarket(segment: Segment, args: Args, deadline: number): Promise<Market | null> {
  for (;;) {
    try {
      return segment.resolve(args.venue, args.kind, args.market);
    } catch (error) {
      if (!isTransient(error) && !(isPmwsError(error) && error.code === "PMWS_MARKET_NOT_FOUND")) {
        throw error;
      }
    }
    if (performance.now() >= deadline) {
      return null;
    }
    await sleep(POLL_INTERVAL_MS);
  }
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const segment = new Segment(args.segment);
  try {
    const deadline = performance.now() + args.seconds * 1000;
    const market = await waitForMarket(segment, args, deadline);
    if (market === null) {
      console.error("error: market not installed in the segment");
      process.exitCode = 1;
      return;
    }
    const attached = await retryTransient(() => market.attach(), deadline);
    if (attached === null) {
      return;
    }
    const { state, stream } = attached;
    console.log(bboLine(state));
    let lastGeneration = segment.publicationGeneration();
    let lastRevision = state.revision;

    while (performance.now() < deadline) {
      if (args.events) {
        while (performance.now() < deadline) {
          let mutation: Event | null;
          try {
            mutation = stream.nextEvent();
          } catch (error) {
            if (error instanceof PmwsContinuityLost) {
              console.log(`continuity_lost reason=${error.reason}`);
              const resumed = await retryTransient(() => stream.reattach(), deadline);
              if (resumed === null) {
                break;
              }
              console.log(`reattach revision=${resumed.revision}`);
              lastRevision = resumed.revision;
              continue;
            }
            if (isTransient(error)) {
              break;
            }
            throw error;
          }
          if (mutation === null) {
            break;
          }
          if (mutation.kind === "resolution") {
            console.log(resolutionLine(mutation));
          } else {
            console.log(mutationLine(mutation));
          }
        }
      }

      const remainingMs = deadline - performance.now();
      if (remainingMs <= 0) {
        break;
      }
      const timeoutMs = Math.min(WAIT_TIMEOUT_MS, Math.trunc(remainingMs) + 1);
      const generation = segment.wait(lastGeneration, { spinMicros: SPIN_MICROS, timeoutMs });
      if (generation === null) {
        continue;
      }
      lastGeneration = generation;
      const refreshed = await retryTransient(() => market.readState(), deadline);
      if (refreshed !== null && refreshed.revision !== lastRevision) {
        lastRevision = refreshed.revision;
        console.log(bboLine(refreshed));
      }
    }
  } finally {
    segment.close();
  }
}

main()
  .then(() => {
    process.exitCode ??= 0;
  })
  .catch((error: unknown) => {
    if (error instanceof UsageError) {
      console.error(`error: ${error.message}`);
    } else {
      console.error(error);
    }
    process.exitCode = 1;
  });

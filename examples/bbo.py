#!/usr/bin/env python3
"""A thin strategy-shaped consumer over the pm-ws Python binding (`bindings/python/pmws.py`).

Attaches to one market's shared-memory book and follows it: one `bbo` line per revision
change, and with ``--events`` one `mutation` line per delivered mutation and one `resolution`
line per delivered venue-reported market resolution. Revision changes are observed through
`Segment.wait` -- a spin-then-park block on the segment's doorbell, not a fixed-interval
poll -- so a woken pass always finds fresh state waiting; the wait itself is bounded so the
run still exits at its own deadline with nothing new to report. A continuity loss on the
mutation stream prints `continuity_lost reason=...`, reattaches, and prints
`reattach revision=R` -- the same recovery every in-process consumer of this crate performs.

Usage:
    bbo.py --segment <path> --market <native-key> [--venue limitless] [--kind slug]
           [--seconds 10] [--events]
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "bindings" / "python"))
import pmws

POLL_INTERVAL = 0.0005
SPIN_MICROS = 200
WAIT_TIMEOUT_MS = 1000
TRANSIENT = (
    pmws.PMWS_STATUS_CONTENDED,
    pmws.PMWS_STATUS_WRITER_STALLED,
    pmws.PMWS_STATUS_NO_PUBLISHED_STATE,
)


def bounded_seconds(text):
    value = float(text)
    if value != value or value in (float("inf"), float("-inf")):
        raise argparse.ArgumentTypeError("--seconds must be a finite number")
    if not 0 < value <= 86_400:
        raise argparse.ArgumentTypeError("--seconds must be positive and at most 86400")
    return value


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--segment", required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--venue", default="limitless")
    parser.add_argument("--kind", default="slug")
    parser.add_argument("--seconds", type=bounded_seconds, default=10.0)
    parser.add_argument("--events", action="store_true")
    return parser.parse_args()


def level_text(level):
    return "-" if level is None else f"{level.price}@{level.quantity}"


def bbo_line(state):
    return (
        f"bbo revision={state.revision} authority={state.authority} "
        f"best_bid={level_text(state.best('Bid'))} best_ask={level_text(state.best('Ask'))}"
    )


def mutation_line(mutation):
    old = "none" if mutation.old_quantity is None else str(mutation.old_quantity)
    new = "none" if mutation.new_quantity is None else str(mutation.new_quantity)
    return (
        f"mutation revision={mutation.revision} "
        f"cursor={mutation.cursor_epoch}:{mutation.cursor_position} "
        f"origin={mutation.origin} side={mutation.side} price={mutation.price} "
        f"qty={old}->{new}"
    )


def resolution_line(resolution):
    return (
        f"resolution revision={resolution.revision} "
        f"cursor={resolution.cursor_epoch}:{resolution.cursor_position} "
        f"origin={resolution.origin} outcome={resolution.winning_outcome} "
        f"index={resolution.winning_index} type={resolution.market_type} "
        f"date={resolution.resolution_date} path={resolution.delivery_path}"
    )


def wait_for_market(segment, args, deadline):
    while True:
        try:
            return segment.resolve(args.venue, args.kind, args.market)
        except pmws.PmwsError as error:
            if error.code != pmws.PMWS_STATUS_MARKET_NOT_FOUND:
                raise
        if time.monotonic() >= deadline:
            print("error: market not installed in the segment", file=sys.stderr)
            sys.exit(1)
        time.sleep(POLL_INTERVAL)


def retry_transient(read, deadline):
    """Calls `read()`, retrying while it raises a transient `PmwsError`, until it succeeds or
    `deadline` passes. Returns `None`, rather than raising, if `deadline` passes first --
    every caller treats that as "give up this pass", never as a decode failure.
    """
    while True:
        try:
            return read()
        except pmws.PmwsError as error:
            if error.code not in TRANSIENT:
                raise
        if time.monotonic() >= deadline:
            return None
        time.sleep(POLL_INTERVAL)


def main():
    args = parse_args()
    with pmws.Segment(args.segment) as segment:
        deadline = time.monotonic() + args.seconds
        market = wait_for_market(segment, args, deadline)
        attached = retry_transient(market.attach, deadline)
        if attached is None:
            sys.exit(0)
        state, stream = attached
        print(bbo_line(state), flush=True)
        last_generation = segment.publication_generation()
        last_revision = state.revision

        while time.monotonic() < deadline:
            if args.events:
                while time.monotonic() < deadline:
                    try:
                        mutation = stream.next_event()
                    except pmws.PmwsContinuityLost as loss:
                        print(f"continuity_lost reason={loss.reason}", flush=True)
                        resumed = retry_transient(stream.reattach, deadline)
                        if resumed is None:
                            break
                        print(f"reattach revision={resumed.revision}", flush=True)
                        last_revision = resumed.revision
                        continue
                    except pmws.PmwsError as error:
                        if error.code not in TRANSIENT:
                            raise
                        break
                    if mutation is None:
                        break
                    if isinstance(mutation, pmws.Resolution):
                        print(resolution_line(mutation), flush=True)
                    else:
                        print(mutation_line(mutation), flush=True)

            remaining_ms = (deadline - time.monotonic()) * 1000
            if remaining_ms <= 0:
                break
            timeout_ms = min(WAIT_TIMEOUT_MS, int(remaining_ms) + 1)
            generation = segment.wait(
                last_generation, spin_micros=SPIN_MICROS, timeout_ms=timeout_ms
            )
            if generation is None:
                continue
            last_generation = generation
            state = retry_transient(market.read_state, deadline)
            if state is not None and state.revision != last_revision:
                last_revision = state.revision
                print(bbo_line(state), flush=True)
    sys.exit(0)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Summarize `pmwsd --tape` lines from stdin; never writes a file.

  ./target/release/pmwsd upstream ... --tape | python3 bench/tape_summary.py
  ... --tape | python3 bench/tape_summary.py --follow <market-id> --peek 3

Each tape line is one admitted batch: t_ns, venue, stream, family, market, events, bytes,
handoff_ns, generation, text (the source text, cut at 600 bytes with an ellipsis). Every
--interval seconds, when the next line arrives, a table of lines, events and bytes per venue and
family is printed for the interval and since the start. --follow echoes one market's lines with
their text; --peek N echoes the first N lines verbatim. Lines that are not JSON pass through.
"""
import argparse
import json
import sys
import time

FIELDS = ("t_ns", "venue", "stream", "family", "market", "events", "bytes", "handoff_ns", "generation", "text")


class Tally:
    def __init__(self):
        self.rows = {}

    def add(self, line):
        row = self.rows.setdefault((line["venue"], line["family"]), [0, 0, 0])
        row[0] += 1
        row[1] += line["events"]
        row[2] += line["bytes"]

    def total(self):
        return [sum(row[i] for row in self.rows.values()) for i in range(3)]

    def render(self, title):
        lines, events, size = self.total()
        out = [f"{title}: lines={lines} events={events} bytes={size}"]
        for (venue, family), (count, ev, by) in sorted(self.rows.items()):
            out.append(f"  {venue:<10} {family:<20} lines={count:<7} events={ev:<8} bytes={by}")
        return "\n".join(out)


def run(stream, out, interval, follow, peek, clock=time.monotonic):
    since_start, window, seen, last = Tally(), Tally(), 0, clock()
    for raw in stream:
        raw = raw.rstrip("\n")
        try:
            line = json.loads(raw)
            assert isinstance(line, dict) and all(key in line for key in FIELDS)
        except (ValueError, AssertionError):
            print(raw, file=out, flush=True)
            continue
        seen += 1
        since_start.add(line)
        window.add(line)
        if seen <= peek:
            print(raw, file=out, flush=True)
        if line["market"] in follow:
            print(f"{line['venue']} {line['family']} {line['market']} events={line['events']} bytes={line['bytes']} "
                  f"handoff_us={line['handoff_ns'] / 1000:.1f} gen={line['generation']} text={line['text']}",
                  file=out, flush=True)
        now = clock()
        if now - last >= interval:
            print(window.render(f"tape last {now - last:.0f}s"), file=out)
            print(since_start.render("tape since start"), file=out, flush=True)
            window, last = Tally(), now
    print(since_start.render("tape at end of input"), file=out, flush=True)


def self_test():
    import io
    tape = [{"t_ns": i, "venue": "limitless" if i % 2 else "polymarket", "stream": "s-0",
             "family": "orderbookUpdate" if i % 2 else "price_change", "market": f"m{i % 3}",
             "events": i + 1, "bytes": 100 * (i + 1), "handoff_ns": 5000, "generation": 1, "text": "…"} for i in range(6)]
    raw = [json.dumps(line) for line in tape] + ["not json"]
    ticks = iter([0, 0, 0, 0, 11, 11, 11, 11, 11])
    out = io.StringIO()
    run(iter(raw), out, 10, {"m1"}, 2, clock=lambda: next(ticks))
    text = out.getvalue()
    assert text.count("tape last") == 1 and "tape since start" in text and "tape at end of input" in text, text
    assert text.startswith(raw[0] + "\n" + raw[1] + "\n"), text
    assert "not json" in text and text.count("text=…") == 2, text
    assert "lines=6 events=21 bytes=2100" in text and "limitless  orderbookUpdate      lines=3" in text, text
    print("tape_summary self-test: ok")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--interval", type=float, default=10.0, help="seconds between tables")
    parser.add_argument("--follow", action="append", default=[], help="market id whose lines are echoed with text")
    parser.add_argument("--peek", type=int, default=0, help="echo the first N lines verbatim")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    try:
        run(sys.stdin, sys.stdout, args.interval, set(args.follow), args.peek)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

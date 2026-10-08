#!/usr/bin/env python3
"""Summarize one pmwsd snapshot or final report (schema pm-ws-native-upstream-v2).

  python3 bench/snapshot_summary.py runs/<ts>/snapshot.json
  python3 bench/snapshot_summary.py runs/<ts>/report.json

Prints the run's reason and qualification, how the measured window opened and how long the run
has been going, then per shard its venue, stream, connection generation, coverage, received
messages, faults and control counts, and per family the event count with the p50, p99 and
maximum of the receive-to-typed-handoff stage. Quantiles are histogram upper bounds in
microseconds (1 µs buckets below 1 ms, doubling above); maxima are exact.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from report_upstream import percentile  # noqa: E402

STAGE = "receive_to_typed_handoff"


def summarize(report, name):
    out = [f"{name}: schema={report.get('schema')} reason={report.get('reason')} qualified={str(report.get('qualified')).lower()}"]
    snapshot = report.get("snapshot")
    if snapshot:
        out.append(f"  snapshot after {snapshot['elapsed_ns'] / 1e9:.0f}s serve={str(snapshot['serve']).lower()} "
                   f"window_opened_by={snapshot.get('window_opened_by')} tape_dropped={snapshot['tape_dropped']}")
    start, end = report.get("measured_start_ns", 0), report.get("measured_end_ns", 0)
    if start and end >= start:
        out.append(f"  measured window: {(end - start) / 1e9:.0f}s")
    elif not start:
        out.append("  measured window: not opened")
    stage = report["stage_names"].index(STAGE)
    for index, shard in enumerate(report.get("shards", [])):
        health = (shard.get("health") or [{}])[-1]
        out.append(f"shard {index} {shard['venue']} {shard['stream']} generation={health.get('generation')} "
                   f"connected={str(health.get('connected')).lower()} covered={shard['ready_targets']}/{shard['required_targets']} "
                   f"received={shard['received_messages']} decoded={shard['decoded_events']} "
                   f"faults={json.dumps(shard.get('faults', {}))} controls={json.dumps(shard.get('controls', {}))}")
        out.append(f"  {'family':<22}{'events':>9}{'p50 µs':>10}{'p99 µs':>10}{'max µs':>12}")
        for family in shard.get("receiver", {}).get("families", []):
            dist = family["stages"][stage]
            if not dist["count"]:
                continue
            out.append(f"  {family['family']:<22}{dist['count']:>9}{percentile(dist, 5000):>10}"
                       f"{percentile(dist, 9900):>10}{dist['max_ns'] / 1000:>12.3f}")
    return "\n".join(out)


def synthetic_report():
    def dist(count, bucket, max_ns):
        buckets = [0] * 1024
        buckets[bucket] = count
        return {"count": count, "buckets": buckets, "max_ns": max_ns, "above_100us": 0, "above_250us": 0, "above_1ms": 0}
    names = ["json_decode", "typed_validation", "admission_gate", "observer_audit", STAGE, "receive_to_audited_observation"]
    return {"schema": "pm-ws-native-upstream-v2", "phase": "A", "qualified": False, "reason": "snapshot",
            "measured_start_ns": 1_000_000_000, "measured_end_ns": 61_000_000_000, "stage_names": names,
            "snapshot": {"elapsed_ns": 65_000_000_000, "serve": True, "interval_seconds": 5, "tape_dropped": 0,
                         "window_opened_by": "readiness"},
            "shards": [{"venue": "limitless", "stream": "limitless-0", "ready_targets": 4, "required_targets": 4,
                        "received_messages": 12, "decoded_events": 12, "faults": {}, "controls": {"engineio_ping": 2},
                        "health": [{"generation": 1, "connected": True}],
                        "receiver": {"families": [{"family": "orderbookUpdate", "stages": [dist(12, 7, 7_900) if i == 4 else dist(0, 0, 0) for i in range(6)]},
                                                  {"family": "system", "stages": [dist(0, 0, 0)] * 6}]}}]}


def self_test():
    text = summarize(synthetic_report(), "synthetic")
    assert "reason=snapshot qualified=false" in text and "snapshot after 65s serve=true window_opened_by=readiness" in text, text
    assert "measured window: 60s" in text, text
    assert "shard 0 limitless limitless-0 generation=1 connected=true covered=4/4 received=12 decoded=12" in text, text
    assert "orderbookUpdate" in text and "≤8" in text and "7.900" in text and "system" not in text, text
    print("snapshot_summary self-test: ok")


def main():
    if len(sys.argv) == 2 and sys.argv[1] == "--self-test":
        return self_test()
    if len(sys.argv) != 2:
        raise SystemExit("usage: snapshot_summary.py <snapshot.json | report.json> | --self-test")
    with open(sys.argv[1]) as handle:
        report = json.load(handle)
    if report.get("schema") != "pm-ws-native-upstream-v2":
        raise SystemExit(f"{sys.argv[1]}: not a pm-ws-native-upstream-v2 report")
    print(summarize(report, sys.argv[1]))


if __name__ == "__main__":
    main()

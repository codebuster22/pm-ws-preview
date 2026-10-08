#!/usr/bin/env python3
"""Validate and render metrics-only, single-source upstream qualification."""
import argparse
import copy
import hashlib
import json
import re
import sys
from pathlib import Path

SCHEMA = "pm-ws-native-upstream-v2"
STAGES = ["json_decode", "typed_validation", "admission_gate", "observer_audit",
          "receive_to_typed_handoff", "receive_to_audited_observation"]
GATES = ["received", "admitted", "stale", "overload"]
FAMILIES = ["orderbookUpdate", "newPriceData", "marketCreated", "marketResolved",
            "system", "exception", "book", "price_change", "last_trade_price",
            "tick_size_change", "best_bid_ask", "new_market", "market_resolved",
            "pong_control", "unknown"]
INDEXES = {"limitless": list(range(6)) + [14], "polymarket": list(range(6, 15))}
MAX_BYTES = 64 * 1024 * 1024
CPU_STAGES = ["decode", "validate", "gate", "receiver", "total", "audited_total"]
CPU_FIELDS = {f"cpu_{name}_ns" for name in CPU_STAGES}
HEARTBEAT_STAMPS = ["ping_enqueued_ns", "writer_started_ns", "write_completed_ns",
                    "receipt_observed_ns", "deadline_ns", "pong_processed_ns",
                    "last_ws_message_received_ns"]


class ReportError(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise ReportError(message)


def integer(value):
    require(type(value) is int and 0 <= value <= 2**64 - 1, "invalid unsigned integer")
    return value


def shape(value, keys, optional=frozenset()):
    require(isinstance(value, dict) and set(keys) <= set(value) <= set(keys) | set(optional),
            "unexpected object fields")
    return value


def counts(values, length):
    require(isinstance(values, list) and len(values) == length, "invalid counter vector")
    for value in values:
        integer(value)
    return values


def bucket(ns):
    return max(0, ns - 1) // 1000 if ns <= 1_000_000 else min(1023, 999 + ((ns - 1) // 1_000_000).bit_length())


def distribution(value):
    shape(value, {"count", "buckets", "max_ns", "above_100us", "above_250us", "above_1ms"})
    counts(value["buckets"], 1024)
    for key in set(value) - {"buckets"}:
        integer(value[key])
    require(sum(value["buckets"]) == value["count"], "histogram count mismatch")
    for offset, name in [(100, "above_100us"), (250, "above_250us"), (1000, "above_1ms")]:
        require(sum(value["buckets"][offset:]) == value[name], "threshold count mismatch")
    occupied = [i for i, n in enumerate(value["buckets"]) if n]
    require((not occupied and value["max_ns"] == 0) or
            (occupied and occupied[-1] == bucket(value["max_ns"])), "histogram maximum mismatch")
    return value


def heartbeat_witness(value, shard, health):
    shape(value, {"generation", "observed_ns", "cause", *HEARTBEAT_STAMPS})
    require(shard["venue"] == "polymarket" and shard["faults"].get("heartbeat_timeout", 0) > 0,
            "heartbeat witness without corresponding Polymarket fault")
    require(0 < integer(value["generation"]) <= health["generation"], "invalid heartbeat generation")
    observed = integer(value["observed_ns"])
    for key in HEARTBEAT_STAMPS:
        if value[key] is not None:
            require(integer(value[key]) <= observed, "heartbeat timestamp after fault observation")
    ordered = [value[key] for key in HEARTBEAT_STAMPS[:5] if value[key] is not None]
    require(ordered == sorted(ordered) and value["deadline_ns"] is not None,
            "invalid heartbeat timeline")
    require(value["cause"] in ("deadline_elapsed", "late_pong_processed"), "invalid heartbeat cause")
    if value["cause"] == "late_pong_processed":
        require(value["pong_processed_ns"] is not None and
                value["pong_processed_ns"] >= value["deadline_ns"], "invalid late-PONG timestamp")


def cpu_evidence(value):
    shape(value, {"clock", "calibration", "read_failures", "nonmonotonic_spans"})
    require(value["clock"] == "CLOCK_THREAD_CPUTIME_ID", "unsupported CPU clock")
    for key in ("read_failures", "nonmonotonic_spans"):
        integer(value[key])
    calibration = shape(value["calibration"], {"paired_reads", "minimum_nonzero_ns", "p95_ns", "p99_ns", "max_ns"})
    for number in calibration.values():
        integer(number)
    require(0 < calibration["paired_reads"] <= 100_000 and
            calibration["p95_ns"] <= calibration["p99_ns"] <= calibration["max_ns"], "invalid CPU calibration")


def parse(report):
    keys = {"schema", "phase", "qualified", "reason", "invocation", "receive_clock",
                   "measured_start_ns", "measured_end_ns", "stage_names", "gate_count_names",
                   "consumed_selection", "shards", "capacities"}
    require(isinstance(report, dict) and keys <= set(report) <= keys | {"cpu_timing", "snapshot"},
            "unexpected report fields")
    require(report["schema"] == SCHEMA and report["phase"] == "A", "unsupported report schema")
    require(report["stage_names"] == STAGES and report["gate_count_names"] == GATES, "stage/counter schema mismatch")
    require(type(report["qualified"]) is bool and isinstance(report["reason"], str), "invalid result")
    start, end = integer(report["measured_start_ns"]), integer(report["measured_end_ns"])
    require((start == end == 0) or (0 < start <= end), "invalid measurement window")
    inv = report["invocation"]
    keys = {"selection", "output", "min_seconds", "max_seconds", "min_events", "workers", "diagnostic"}
    require(isinstance(inv, dict) and keys <= set(inv) <= keys | {"control_socket", "cpu_timing",
            "snapshot", "snapshot_seconds", "serve", "tape"}, "invalid invocation")
    for key in ("min_seconds", "max_seconds", "min_events", "workers"):
        integer(inv[key])
    require(type(inv["diagnostic"]) is bool, "invalid diagnostic flag")
    cpu_enabled = inv.get("cpu_timing", False)
    require(type(cpu_enabled) is bool and cpu_enabled == ("cpu_timing" in report), "CPU timing mode mismatch")
    if cpu_enabled:
        cpu_evidence(report["cpu_timing"])
    require(1 <= inv["workers"] <= 8 and 0 < inv["min_seconds"] <= inv["max_seconds"] <= 14400 and inv["min_events"] > 0, "invalid invocation limits")
    shape(report["consumed_selection"], {"limitless", "polymarket"})
    require(all(isinstance(v, list) for v in report["consumed_selection"].values()), "invalid selection")
    require(isinstance(report["shards"], list) and len(report["shards"]) == 2, "requires two venue shards")
    seen = set()
    for shard in report["shards"]:
        shape(shard, {"venue", "stream", "ready_targets", "required_targets", "received_messages",
                      "decoded_events", "malformed_messages", "routing_rejections", "gate_all",
                      "gate_measured", "controls", "faults", "receiver", "connections", "health"},
              optional={"recent", "targets"})
        venue = shard["venue"]
        require(venue in INDEXES and venue not in seen, "duplicate/unsupported venue")
        seen.add(venue)
        require(isinstance(shard["stream"], str), "invalid stream")
        for key in ("ready_targets", "required_targets", "received_messages", "decoded_events",
                    "malformed_messages", "routing_rejections"):
            integer(shard[key])
        require(shard["ready_targets"] <= shard["required_targets"], "invalid subscription evidence")
        for key in ("controls", "faults"):
            require(isinstance(shard[key], dict) and len(shard[key]) <= 32, "unbounded controls/faults")
            for value in shard[key].values():
                integer(value)
        for key in ("gate_all", "gate_measured"):
            counts(shard[key], 4)
        require(len(shard["connections"]) == len(shard["health"]) == 1, "one connection per shard required")
        connection = shard["connections"][0]
        keys = {"received_messages", "malformed_messages", "arrivals_all", "arrivals_measured",
                "published_all", "published_measured", "gate_all", "gate_measured"}
        require(isinstance(connection, dict) and keys <= set(connection) <= keys | {"first_selected_resolution", "heartbeat_timeout"},
                "unexpected connection fields")
        resolution = connection.get("first_selected_resolution")
        if resolution is not None:
            require(integer(resolution) < shard["required_targets"] and
                    shard["controls"].get("selected_target_resolved", 0) > 0,
                    "invalid selected-resolution coordinate")
        for key in ("received_messages", "malformed_messages"):
            require(integer(connection[key]) == shard[key], "connection accounting mismatch")
        for suffix in ("all", "measured"):
            arrivals = counts(connection["arrivals_" + suffix], 15)
            published = counts(connection["published_" + suffix], 15)
            gate = counts(connection["gate_" + suffix], 4)
            require(gate == shard["gate_" + suffix], "gate accounting mismatch")
            require(sum(arrivals) == gate[0] and sum(published) == gate[1], "family/gate totals mismatch")
            require(all(p <= a for p, a in zip(published, arrivals)), "publication exceeds arrivals")
            require(not any(arrivals[i] or published[i] for i in range(15) if i not in INDEXES[venue]), "wrong venue family")
        require(all(m <= a for m, a in zip(connection["gate_measured"], connection["gate_all"])), "measured exceeds total")
        health = shape(shard["health"][0], {"generation", "connected", "subscription_sent",
            "subscription_evidence", "evidenced_targets", "requested_targets", "last_heartbeat_ns"})
        for key in ("generation", "evidenced_targets", "requested_targets"):
            integer(health[key])
        require(type(health["connected"]) is bool and type(health["subscription_sent"]) is bool, "invalid health flags")
        require(health["subscription_evidence"] == ("acknowledged" if venue == "limitless" else "observed_data"), "invalid evidence type")
        require(health["requested_targets"] == shard["required_targets"] and health["evidenced_targets"] == shard["ready_targets"], "health count mismatch")
        if health["last_heartbeat_ns"] is not None:
            integer(health["last_heartbeat_ns"])
        if connection.get("heartbeat_timeout") is not None:
            heartbeat_witness(connection["heartbeat_timeout"], shard, health)
        receiver = shape(shard["receiver"], {"families", "events", "activity_events", "batches",
            "last_sequence", "tails", "tails_omitted", "sequence_errors", "generation_errors", "member_order_errors"})
        for key in set(receiver) - {"families", "tails"}:
            integer(receiver[key])
        require(receiver["events"] == connection["gate_measured"][1], "receiver publication mismatch")
        require(receiver["batches"] <= receiver["events"] and receiver["batches"] <= receiver["last_sequence"], "invalid delivery counts")
        require([f["family"] for f in receiver["families"]] == [FAMILIES[i] for i in INDEXES[venue]], "family inventory mismatch")
        activity = 0
        for index, family in zip(INDEXES[venue], receiver["families"]):
            shape(family, {"family", "stages"})
            require(len(family["stages"]) == 6, "missing timing stages")
            for value in family["stages"]:
                require(distribution(value)["count"] == connection["published_measured"][index], "unpaired timing samples")
            if index in (0, 6, 7):
                activity += family["stages"][0]["count"]
        require(activity == receiver["activity_events"], "activity includes non-book events")
        require(isinstance(receiver["tails"], list) and len(receiver["tails"]) <= 256, "unbounded tails")
        last_tail = 0
        for tail in receiver["tails"]:
            keys = {"sequence", "source_slot", "source_generation", "stream_generation",
                "receive_ns", "decode_ns", "validate_ns", "gate_ns", "receiver_ns",
                "total_ns", "audited_total_ns", "event_count", "input_bytes"}
            allowed = keys | CPU_FIELDS if cpu_enabled else keys
            require(isinstance(tail, dict) and keys <= set(tail) <= allowed, "unexpected tail fields")
            for value in tail.values():
                integer(value)
            if cpu_enabled:
                present = CPU_FIELDS & set(tail)
                require(present == CPU_FIELDS or report["cpu_timing"]["read_failures"] > 0 or
                        report["cpu_timing"]["nonmonotonic_spans"] > 0, "missing CPU evidence without a recorded failure")
                for parts, total in [(CPU_STAGES[:3], "total"), (["total", "receiver"], "audited_total")]:
                    fields = [f"cpu_{part}_ns" for part in parts]
                    total_field = f"cpu_{total}_ns"
                    if all(field in tail for field in fields + [total_field]):
                        require(sum(tail[field] for field in fields) == tail[total_field], "unpaired CPU intervals")
            require(last_tail < tail["sequence"] <= receiver["last_sequence"], "invalid tail order")
            last_tail = tail["sequence"]
            require(tail["source_generation"] == tail["stream_generation"] and tail["source_slot"] == 0, "invalid tail source")
            require(start <= tail["receive_ns"] < end and tail["audited_total_ns"] > 1_000_000, "invalid tail window")
            require(tail["decode_ns"] + tail["validate_ns"] + tail["gate_ns"] == tail["total_ns"] and
                    tail["total_ns"] + tail["receiver_ns"] == tail["audited_total_ns"], "unpaired tail intervals")
            require(0 < tail["event_count"] <= 4096 and tail["input_bytes"] <= 1_048_576, "invalid tail bounds")
    caps = shape(report["capacities"], {"shards", "connections_per_shard", "max_input_bytes",
        "admission_bytes_per_shard", "atomic_events", "typed_nodes", "typed_depth", "allocator", "declared_data_bytes"})
    for key in set(caps) - {"allocator"}:
        integer(caps[key])
    require(caps["shards"] == 2 and caps["connections_per_shard"] == 1 and
            caps["declared_data_bytes"] == 2 * caps["admission_bytes_per_shard"], "capacity topology mismatch")
    return report


def provenance(manifest, report, raw):
    if not isinstance(manifest, dict):
        return False
    try:
        measurement, receipt = manifest["measurement"], manifest["build_receipt"]
        valid_hash = lambda value: isinstance(value, str) and re.fullmatch("[0-9a-f]{64}", value) is not None
        return (all(valid_hash(manifest[key]) for key in ("binary_sha256", "source_sha256", "selection_sha256"))
            and manifest["report_sha256"] == hashlib.sha256(raw).hexdigest()
            and manifest["selection_snapshot"] == report["consumed_selection"]
            and manifest["resource_limits"] == report["capacities"]
            and measurement["live_started"] is True and measurement["outcome"] == "ok"
            and measurement["exit_code"] == 0
            and all(measurement[key] is True for key in ("source_binary_hash_match", "selection_hash_match", "build_matches", "leaf_selection_verified"))
            and all(manifest["config"][key] == report["invocation"][key] == measurement[key]
                    for key in ("workers", "min_seconds", "max_seconds", "min_events"))
            and manifest["config"].get("cpu_timing", False) == report["invocation"].get("cpu_timing", False)
                == measurement.get("cpu_timing", False)
            and manifest["config"]["selection"] == report["invocation"]["selection"]
            and receipt["source_sha256"] == receipt["actual_source_sha256"] == manifest["source_sha256"]
            and receipt["binary_sha256"] == receipt["actual_binary_sha256"] == manifest["binary_sha256"]
            and receipt["compiler"] == manifest["rustc"]
            and receipt["flags"] == ["--release", "--locked", "--bin", "pmwsd"])
    except (KeyError, TypeError):
        return False


def qualifies(report, manifest=None, raw=b""):
    inv, selection = report["invocation"], report["consumed_selection"]
    duration = report["measured_end_ns"] - report["measured_start_ns"]
    if (not report["qualified"] or inv["diagnostic"] or report["reason"] != "healthy_floors_reached"
            or duration < max(900, inv["min_seconds"]) * 1_000_000_000
            or not provenance(manifest, report, raw)):
        return False
    if len(selection["limitless"]) != 100 or len(selection["polymarket"]) != 100:
        return False
    try:
        slugs = {r["slug"] for r in selection["limitless"]}
        markets = {r["condition_id"] for r in selection["polymarket"]}
        assets = [a for r in selection["polymarket"] for a in r["clob_token_ids"]]
        if len(slugs) != 100 or len(markets) != 100 or len(assets) != len(set(assets)):
            return False
        if len(assets) != 200 or any(len(r["clob_token_ids"]) != 2 for r in selection["polymarket"]):
            return False
        deadline = manifest["created_unix_ns"] // 1_000_000_000 + inv["max_seconds"] + 210
        if any(r["end_epoch"] is None or r["end_epoch"] < deadline for rows in selection.values() for r in rows):
            return False
    except (KeyError, TypeError):
        return False
    for shard in report["shards"]:
        receiver, health = shard["receiver"], shard["health"][0]
        if (shard["required_targets"] != (100 if shard["venue"] == "limitless" else 200)
                or any(shard["faults"].values()) or shard["malformed_messages"] or shard["routing_rejections"]
                or shard["controls"].get("selected_target_resolved", 0)
                or any(shard["gate_all"][2:]) or shard["gate_all"][0] != shard["gate_all"][1]
                or not health["subscription_sent"] or not health["last_heartbeat_ns"]
                or (shard["venue"] == "limitless" and shard["ready_targets"] != 100)
                or any(receiver[key] for key in ("sequence_errors", "generation_errors", "member_order_errors"))
                or receiver["activity_events"] < max(10_000, inv["min_events"])):
            return False
    return True


def percentile(value, numerator):
    if not value["count"]:
        return "—"
    rank, seen = max(1, (value["count"] * numerator + 9999) // 10000), 0
    for index, count in enumerate(value["buckets"]):
        seen += count
        if seen >= rank:
            return f"≤{index + 1}" if index < 1000 else ("overflow" if index == 1023 else f"≤{1000 * 2**(index - 999)}")
    raise ReportError("invalid percentile")


def aggregate(values):
    result = copy.deepcopy(values[0])
    for value in values[1:]:
        result["buckets"] = [a + b for a, b in zip(result["buckets"], value["buckets"])]
        for key in ("count", "above_100us", "above_250us", "above_1ms"):
            result[key] += value[key]
        result["max_ns"] = max(result["max_ns"], value["max_ns"])
    return result


def render(report, manifest=None, raw=b""):
    q = qualifies(report, manifest, raw)
    seconds = (report["measured_end_ns"] - report["measured_start_ns"]) / 1e9
    lines = ["# Single-source upstream measurement", "", f"Qualification: {'qualified' if q else 'not qualified'}",
        f"Reason: {report['reason']}; measured duration: {seconds:.3f} s.",
        "Configured workload: two connections; 100 Limitless identifiers + 100 Polymarket conditions / 200 tokens. No event deduplication.",
        "Upstream-only: no production IPC or foreign-runtime latency claim.", ""]
    if manifest:
        lines += [f"Machine: {manifest.get('cpu_model')}, {manifest.get('machine')}, {manifest.get('os')}; "
                  f"logical CPUs={manifest.get('cpu_logical')}, RAM bytes={manifest.get('ram_bytes')}.",
                  f"Binary SHA-256: {manifest.get('binary_sha256')}; source SHA-256: {manifest.get('source_sha256')}.", ""]
        power = manifest.get("power_state")
        if isinstance(power, str):
            lines += ["Power at launch: " + "; ".join(power.splitlines()), ""]
    lines += ["Timing quantiles are histogram upper bounds in microseconds; maxima are exact measured microseconds.",
              "No-sample families show —. Batch stages are event-weighted; do not add stage percentiles.", ""]
    cpu = report.get("cpu_timing")
    if cpu is not None:
        calibration = cpu["calibration"]
        lines += [f"Thread CPU timing enabled: {cpu['clock']}; failed sampled reads={cpu['read_failures']}; nonmonotonic spans={cpu['nonmonotonic_spans']}.",
                  f"Paired-read wall overhead ({calibration['paired_reads']} samples), ns: p95={calibration['p95_ns']}, p99={calibration['p99_ns']}, max={calibration['max_ns']}; minimum observed nonzero CPU increment={calibration['minimum_nonzero_ns']} ns.",
                  "Overhead is not subtracted. Elapsed minus thread CPU includes non-CPU delays and clock-sampling skew; it does not distinguish blocking from preemption. Missing CPU values are unknown, not zero.", ""]
        tails = [tail for shard in report["shards"] for tail in shard["receiver"]["tails"]]
        handoff = [tail for tail in tails if tail["total_ns"] > 1_000_000]
        known = [tail for tail in handoff if "cpu_total_ns" in tail]
        omitted = sum(shard["receiver"]["tails_omitted"] for shard in report["shards"])
        lines += [f"Retained >1 ms handoff evidence: {len(handoff)} batches / {sum(t['event_count'] for t in handoff)} events; thread CPU known for {len(known)} batches.",
                  f"Among CPU-known retained handoff batches: {sum(t['cpu_total_ns'] < 250_000 for t in known)} below 250 µs CPU; {sum(t['cpu_total_ns'] < 1_000_000 for t in known)} below 1 ms CPU.",
                  f"Other retained audited tails: {len(tails) - len(handoff)} batches; omitted tail witnesses: {omitted}.",
                  "These are bounded tail samples, not a CPU-time distribution over all events.", ""]
    for shard in report["shards"]:
        receiver, health = shard["receiver"], shard["health"][0]
        lines += [f"## {shard['venue']}", "",
                  f"Book-data arrivals: {receiver['activity_events']}; total events: {receiver['events']}; batches: {receiver['batches']}.",
                  f"Subscription evidence: {shard['ready_targets']}/{shard['required_targets']} "
                  f"({health['subscription_evidence']}); sent={health['subscription_sent']}.",
                  f"Faults: {json.dumps(shard['faults'], sort_keys=True)}. Tail witnesses: {len(receiver['tails'])}; omitted={receiver['tails_omitted']}.", ""]
        resolution = shard["connections"][0].get("first_selected_resolution")
        if resolution is not None:
            coordinate = "Limitless slug" if shard["venue"] == "limitless" else "Polymarket asset-token"
            lines += [f"First selected resolution: connection 0, zero-based sorted {coordinate} coordinate {resolution}.", ""]
        witness = shard["connections"][0].get("heartbeat_timeout")
        if witness is not None:
            lines += [f"First heartbeat timeout: connection 0, generation {witness['generation']}, cause={witness['cause']}.",
                      "Common process-origin nanoseconds; PONG timestamps record application processing, not kernel arrival.", "",
                      "| Timing | ns |", "|---|---:|"]
            lines += [f"| {key} | {witness[key] if witness[key] is not None else '—'} |"
                      for key in [*HEARTBEAT_STAMPS, "observed_ns"]]
            lines += [""]
        lines += ["| Family | Stage | Samples | p50 µs | p95 µs | p99 µs | p99.9 µs | max µs | >1ms |",
                  "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
        totals = {"family": "ALL", "stages": [aggregate([f["stages"][i] for f in receiver["families"]]) for i in range(6)]}
        for family in [totals] + receiver["families"]:
            for name, value in zip(STAGES, family["stages"]):
                lines.append(f"| {family['family']} | {name} | {value['count']} | "
                    + " | ".join(percentile(value, p) for p in (5000, 9500, 9900, 9990))
                    + f" | {value['max_ns']/1000:.3f} | {value['above_1ms']} |")
        lines.append("")
        if cpu is not None and receiver["tails"]:
            lines += ["### Paired tail evidence", "", "| Sequence | Stage | Elapsed µs | Thread CPU µs | Elapsed − CPU µs |",
                      "|---|---|---:|---:|---:|"]
            for tail in receiver["tails"]:
                for name, stage in zip(CPU_STAGES, STAGES):
                    wall, used = tail[f"{name}_ns"], tail.get(f"cpu_{name}_ns")
                    cpu_text = "—" if used is None else f"{used/1000:.3f}"
                    difference = "—" if used is None else f"{(wall-used)/1000:.3f}"
                    lines.append(f"| {tail['sequence']} | {stage} | {wall/1000:.3f} | {cpu_text} | {difference} |")
            lines.append("")
    return "\n".join(lines)


def self_test():
    require(percentile(distribution({"count": 1, "buckets": [1] + [0]*1023, "max_ns": 250,
            "above_100us": 0, "above_250us": 0, "above_1ms": 0}), 9900) == "≤1", "percentile")
    for invalid in ({"schema": "pm-ws-native-upstream-v1"}, {}, {"qualified": True}):
        try:
            parse(invalid)
        except ReportError:
            continue
        raise AssertionError("invalid report accepted")
    require(not provenance({}, {}, b"") and not provenance(None, {}, b""), "missing provenance")


def read_json(path):
    with Path(path).open("rb") as source:
        raw = source.read(MAX_BYTES + 1)
    require(len(raw) <= MAX_BYTES, "metrics exceed bounded input")
    return json.loads(raw), raw


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input")
    parser.add_argument("--output")
    parser.add_argument("--manifest")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        print("report_upstream self-test: ok")
        return
    require(bool(args.input and args.output), "--input and --output required")
    report, raw = read_json(args.input)
    manifest = read_json(args.manifest)[0] if args.manifest else None
    Path(args.output).write_text(render(parse(report), manifest, raw))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"report_upstream: {error}", file=sys.stderr)
        raise SystemExit(2)

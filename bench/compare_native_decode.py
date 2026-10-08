#!/usr/bin/env python3
"""Compare two prebuilt native decoders on generated inputs; retain metrics only."""

import argparse
import hashlib
import json
import os
import platform
from pathlib import Path
import subprocess
import time


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def command_text(command):
    result = subprocess.run(command, text=True, capture_output=True, check=True)
    return result.stdout.strip()


def render_report(source, output):
    with source.open(encoding="utf-8") as stream:
        report = json.load(stream)
    if report["schema"] != "pm-ws-native-decode-comparison-v1":
        raise ValueError("unsupported comparison schema")
    lines = [
        "# Generated-input native decoder comparison", "",
        f"Machine: {report['cpu']}, {report['logical_cpus']} logical CPUs; {report['machine']}.",
        f"Boundary: {report['boundary']}.",
        f"Samples per case per run: {report['iterations_per_case_per_run']}; "
        f"order: {', '.join(report['order'])}.", "",
        "Each row is one complete run. Timings are microseconds; no live or consumer claim.", "",
        "| Run | Variant | Generated case | Bytes | Samples | p95 | p99 | Max |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for index, run in enumerate(report["runs"], 1):
        for case in run["result"]["cases"]:
            lines.append(
                f"| {index} | {run['variant']} | {case['case']} | {case['input_bytes']} | "
                f"{case['count']} | {case['p95_ns'] / 1000:.3f} | "
                f"{case['p99_ns'] / 1000:.3f} | {case['max_ns'] / 1000:.3f} |"
            )
    if all("stages" in case for run in report["runs"] for case in run["result"]["cases"]):
        lines.extend(["", "## Same-call stages", "",
                      "Stages are recorded within each complete call. Do not add independent percentiles.",
                      "Parse includes complete document construction; validation includes exact economic",
                      "string conversion and venue metadata. Destruction is timed separately. These are",
                      "not the live runner's broader routing/admission stage boundaries.", "",
                      "| Run | Variant | Case | Stage | p95 µs | p99 µs | Max µs |",
                      "| --- | --- | --- | --- | ---: | ---: | ---: |"])
        for index, run in enumerate(report["runs"], 1):
            for case in run["result"]["cases"]:
                for name, stage in case["stages"].items():
                    lines.append(
                        f"| {index} | {run['variant']} | {case['case']} | {name} | "
                        f"{stage['p95_ns'] / 1000:.3f} | {stage['p99_ns'] / 1000:.3f} | "
                        f"{stage['max_ns'] / 1000:.3f} |"
                    )
    lines.extend(["", "## Provenance", ""])
    for variant, binary_hash in report["binary_sha256"].items():
        lines.append(f"- {variant} binary SHA-256: `{binary_hash}`.")
    lines.extend(["", "## Power state", ""])
    for key in ("power_before", "power_after", "power_settings_before", "power_settings_after"):
        if key in report:
            lines.extend([key.replace("_", " ") + ":", "", "```text", report[key], "```", ""])
    with output.open("x", encoding="utf-8") as stream:
        stream.write("\n".join(lines).rstrip() + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--candidate", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--render", type=Path, help="render an existing metrics JSON report without timing")
    parser.add_argument("--iterations", type=int, default=10000)
    args = parser.parse_args()
    if args.output.exists() or not 1 <= args.iterations <= 100000:
        parser.error("output must be new and iterations must be within 1..100000")
    if args.render:
        if args.baseline or args.candidate:
            parser.error("--render cannot be combined with decoder binaries")
        render_report(args.render, args.output)
        return
    if not args.baseline or not args.candidate:
        parser.error("timing requires --baseline and --candidate")
    binaries = {name: getattr(args, name).resolve() for name in ("baseline", "candidate")}
    hashes = {name: digest(path) for name, path in binaries.items()}
    order = ["baseline", "candidate", "candidate", "baseline"]
    report = {
        "schema": "pm-ws-native-decode-comparison-v1",
        "boundary": "generated-input decode, validation and result destruction; not live handoff or IPC",
        "iterations_per_case_per_run": args.iterations,
        "order": order,
        "machine": platform.platform(),
        "logical_cpus": os.cpu_count(),
        "cpu": command_text(["sysctl", "-n", "machdep.cpu.brand_string"])
            if platform.system() == "Darwin" else platform.processor(),
        "binary_sha256": hashes,
        "binaries": {name: str(path) for name, path in binaries.items()},
        "runs": [],
    }
    if platform.system() == "Darwin":
        report["power_before"] = command_text(["pmset", "-g", "batt"])
        report["power_settings_before"] = command_text(["pmset", "-g", "custom"])
    shape = None
    for variant in order:
        run = {"variant": variant, "start_unix_ns": time.time_ns(),
               "start_load": os.getloadavg()}
        result = subprocess.run([str(binaries[variant]), "--iterations", str(args.iterations)],
                                text=True, capture_output=True, check=True, timeout=600)
        run.update(end_unix_ns=time.time_ns(), end_load=os.getloadavg(),
                   result=json.loads(result.stdout))
        cases = run["result"]["cases"]
        current_shape = [(case["case"], case["input_bytes"], case["count"]) for case in cases]
        if len(cases) != 3 or any(case["count"] != args.iterations for case in cases):
            raise RuntimeError("benchmark case or sample count mismatch")
        if shape is not None and current_shape != shape:
            raise RuntimeError("benchmark inputs differ between runs")
        shape = current_shape
        for case in cases:
            if "stages" in case and (
                set(case["stages"]) != {"parse_document", "venue_validation", "destruction"}
                or any(stage["count"] != args.iterations for stage in case["stages"].values())
            ):
                raise RuntimeError("benchmark stage shape or sample count mismatch")
        report["runs"].append(run)
        print(json.dumps({"variant": variant, "cases": cases}), flush=True)
    if hashes != {name: digest(path) for name, path in binaries.items()}:
        raise RuntimeError("benchmark binary changed during measurement")
    if platform.system() == "Darwin":
        report["power_after"] = command_text(["pmset", "-g", "batt"])
        report["power_settings_after"] = command_text(["pmset", "-g", "custom"])
    with args.output.open("x", encoding="utf-8") as output:
        json.dump(report, output, indent=2)
        output.write("\n")


if __name__ == "__main__":
    main()

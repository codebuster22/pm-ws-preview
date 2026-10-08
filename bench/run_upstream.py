#!/usr/bin/env python3
"""Safe, metrics-only launcher for the book-free upstream qualification rail."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BINARY = ROOT / "target/release/pmwsd"
SOURCE_NAMES = ("Cargo.toml", "Cargo.lock", "rust-toolchain", "rust-toolchain.toml")
MAX_SELECTION_BYTES = 1_048_576
RESOURCE_LIMITS = {
    "shards": 2, "connections_per_shard": 1, "max_input_bytes": 1_048_576,
    "admission_bytes_per_shard": 64 * 1_048_576, "atomic_events": 4096,
    "typed_nodes": 262144, "typed_depth": 20,
    "allocator": "system; bounded transient batches; no identity or payload history",
    "declared_data_bytes": 2 * 64 * 1_048_576,
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def selection_snapshot(path: Path) -> tuple[str, dict[str, Any]]:
    with path.open("rb") as source:
        raw = source.read(MAX_SELECTION_BYTES + 1)
    if len(raw) > MAX_SELECTION_BYTES:
        raise ValueError("selection exceeds bounded input")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("selection must be an object")
    limitless = value.get("limitless", [])
    polymarket = value.get("polymarket", [])
    if not isinstance(limitless, list) or not isinstance(polymarket, list):
        raise ValueError("selection venue lists invalid")
    def ll(row: Any) -> dict[str, Any]:
        if not isinstance(row, dict) or not isinstance(row.get("slug"), str): raise ValueError("limitless selection invalid")
        if row.get("market_kind") != "leaf" or not isinstance(row.get("condition_id"), str) or not re.fullmatch(r"0x[0-9a-fA-F]{64}", row["condition_id"]):
            raise ValueError("Limitless selection requires leaf-market condition evidence, not group containers")
        return {"slug": row["slug"], "end_epoch": row.get("end_epoch")}
    def pm(row: Any) -> dict[str, Any]:
        if not isinstance(row, dict) or not isinstance(row.get("condition_id"), str) or not isinstance(row.get("clob_token_ids"), list): raise ValueError("polymarket selection invalid")
        return {"condition_id": row["condition_id"], "clob_token_ids": row["clob_token_ids"], "end_epoch": row.get("end_epoch")}
    snapshot = {"limitless": [ll(row) for row in limitless], "polymarket": [pm(row) for row in polymarket]}
    if len({row["condition_id"] for row in limitless}) != len(limitless):
        raise ValueError("duplicate Limitless physical condition in selection")
    return hashlib.sha256(raw).hexdigest(), snapshot


def source_fingerprint() -> str:
    digest = hashlib.sha256()
    paths = [path for path in (ROOT / "src").rglob("*.rs")]
    paths += [ROOT / name for name in SOURCE_NAMES if (ROOT / name).is_file()]
    for path in sorted(paths):
        digest.update(str(path.relative_to(ROOT)).encode())
        digest.update(b"\0")
        with path.open("rb") as source:
            for block in iter(lambda: source.read(1 << 20), b""):
                digest.update(block)
    return digest.hexdigest()


def receipt_path(binary: Path) -> Path:
    return binary.with_suffix(".build.json")


def build_receipt(binary: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(receipt_path(binary).read_text())
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def receipt_matches(binary: Path, source: str, compiler: str | None) -> bool:
    receipt = build_receipt(binary)
    return bool(receipt and receipt.get("source_sha256") == source and receipt.get("binary_sha256") == sha256(binary) and receipt.get("compiler") == compiler and receipt.get("flags") == ["--release", "--locked", "--bin", "pmwsd"])


def build_only(binary: Path) -> int:
    if binary.resolve() != DEFAULT_BINARY.resolve():
        raise ValueError("--build-only requires the default target/release/pmwsd binary")
    before, compiler = source_fingerprint(), command_version(["rustc", "--version"])
    result = subprocess.run(["cargo", "build", "--release", "--locked", "--bin", "pmwsd"], cwd=ROOT, check=False)
    after = source_fingerprint()
    if result.returncode or before != after or not binary.is_file():
        return 2
    receipt = {"schema": 1, "source_sha256": before, "binary_sha256": sha256(binary), "actual_source_sha256": before, "actual_binary_sha256": sha256(binary), "compiler": compiler, "flags": ["--release", "--locked", "--bin", "pmwsd"]}
    temporary = receipt_path(binary).with_suffix(".build.json.tmp")
    temporary.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, receipt_path(binary))
    return 0


def git_status() -> str | None:
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain=v1"], cwd=ROOT, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, check=False,
        )
    except OSError:
        return None
    if result.returncode:
        return None
    return "clean" if not result.stdout else "dirty"


def command_version(command: list[str]) -> str | None:
    try:
        result = subprocess.run(command, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL, check=False)
    except OSError:
        return None
    if result.returncode:
        return None
    return result.stdout.strip().splitlines()[0] if result.stdout.strip() else None


def command_text(command: list[str]) -> str | None:
    try:
        result = subprocess.run(command, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL, check=False)
    except OSError:
        return None
    return result.stdout.strip() if result.returncode == 0 else None


def mac_sysctl(name: str) -> str | None:
    return command_text(["sysctl", "-n", name]) if sys.platform == "darwin" else None


def clock_calibration() -> dict[str, int | str]:
    last = time.monotonic_ns()
    minimum = None
    for _ in range(10_000):
        now = time.monotonic_ns()
        if now > last:
            minimum = now - last if minimum is None else min(minimum, now - last)
        last = now
    info = time.get_clock_info("monotonic")
    return {"implementation": info.implementation, "resolution_ns": int(info.resolution * 1e9), "minimum_nonzero_ns": minimum or 0}


def linux_memory() -> dict[str, int]:
    result: dict[str, int] = {}
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            name, value = line.split(":", 1)
            fields = value.split()
            if fields and fields[0].isdigit():
                result[name] = int(fields[0]) * 1024
    except OSError:
        pass
    return result


def memory_bytes() -> int | None:
    memory = linux_memory().get("MemTotal")
    if memory is not None:
        return memory
    value = mac_sysctl("hw.memsize")
    return int(value) if value and value.isdigit() else None


def metadata(binary: Path, args: argparse.Namespace) -> dict[str, Any]:
    memory = linux_memory()
    mac_ram = mac_sysctl("hw.memsize")
    selection_hash, selection = selection_snapshot(args.selection)
    return {
        "schema": 1,
        "created_unix_ns": time.time_ns(),
        "binary": str(binary),
        "binary_sha256": sha256(binary) if binary.is_file() else None,
        "source_sha256": source_fingerprint(),
        "selection_sha256": selection_hash,
        "selection_snapshot": selection,
        "config": {"selection": str(args.selection), "workers": args.workers, "min_seconds": args.min_seconds, "max_seconds": args.max_seconds, "min_events": args.min_events, "cpu_timing": getattr(args, "cpu_timing", False)},
        "resource_limits": RESOURCE_LIMITS,
        "git_status": git_status(),
        "python": platform.python_version(),
        "rustc": command_version(["rustc", "--version"]),
        "os": platform.platform(),
        "machine": platform.machine(), "cpu_model": mac_sysctl("machdep.cpu.brand_string"),
        "cpu_logical": os.cpu_count(), "cpu_physical": mac_sysctl("hw.physicalcpu"),
        "ram_bytes": memory.get("MemTotal") or (int(mac_ram) if mac_ram and mac_ram.isdigit() else None),
        "power_state": command_text(["pmset", "-g", "batt"]) if sys.platform == "darwin" else None,
        "affinity": None,
        "loadavg": list(os.getloadavg()) if hasattr(os, "getloadavg") else None,
        "clock": {"monotonic_ns": time.monotonic_ns(), "wall_unix_ns": time.time_ns(), "calibration": clock_calibration()},
        "measurement": {
            "stage": "A_upstream_only",
            "workers": args.workers,
            "min_seconds": args.min_seconds,
            "max_seconds": args.max_seconds,
            "min_events": args.min_events,
            "cpu_timing": getattr(args, "cpu_timing", False),
            "bindings_claimed": False,
            "leaf_selection_verified": True,
            "live_started": False, "outcome": "preflight", "build_matches": False,
        },
    }


def require_valid(args: argparse.Namespace) -> None:
    if args.workers < 1 or args.workers > 8 or args.min_seconds < 1 or args.max_seconds < args.min_seconds or args.max_seconds > 14400:
        raise ValueError("workers and durations must be positive; max-seconds must cover min-seconds")
    if args.min_events < 1:
        raise ValueError("min-events must be positive")
    if args.sample_seconds < 1 or args.sample_seconds > 5:
        raise ValueError("sample-seconds must be between 1 and 5")
    if not args.selection.is_file() or args.selection.stat().st_size > MAX_SELECTION_BYTES:
        raise ValueError("selection must name an existing descriptor JSON file")
    selection_snapshot(args.selection)
    if not args.diagnostic_nonqualify and (args.min_seconds < 900 or args.min_events < 10_000):
        raise ValueError("qualification runs require min-seconds >=900 and min-events >=10000")
    if args.run and not args.diagnostic_nonqualify:
        if not isinstance(os.cpu_count(), int) or os.cpu_count() <= args.workers: raise ValueError("qualification requires one CPU for control headroom")
        ram = memory_bytes()
        if ram is None or ram < 2 * RESOURCE_LIMITS["declared_data_bytes"]: raise ValueError("qualification RAM is below twice declared data bytes")


def parse_proc_stat(text: str) -> tuple[int, int] | None:
    """`(utime_ticks, stime_ticks)` from the content of `/proc/<pid>/stat`, or `None`.

    The `comm` field is parenthesised and may itself contain spaces and parentheses, so
    this splits on the line's last `)` rather than on whitespace; `utime` and `stime` are
    fields 14 and 15 of the stat line, landing at indices 11 and 12 of the fields after
    that `)`.
    """
    close = text.rfind(")")
    if close == -1:
        return None
    fields = text[close + 1:].split()
    if len(fields) < 13:
        return None
    try:
        return int(fields[11]), int(fields[12])
    except ValueError:
        return None


def read_process_proc(pid: int) -> tuple[int, float] | None:
    """`(rss_bytes, cpu_seconds)` read from `/proc/<pid>/{stat,statm}`, or `None`.

    CPU time is read at the kernel's clock-tick resolution (typically 10 ms), rather than
    the whole-second resolution of `ps` TIME on Linux. `None` covers a platform without
    /proc, a `pid` that has exited, and a stat line this parser cannot account for;
    callers fall back to `ps` sampling in every case.
    """
    try:
        stat_text = Path(f"/proc/{pid}/stat").read_text()
        statm_text = Path(f"/proc/{pid}/statm").read_text()
    except (OSError, ValueError):
        return None
    ticks = parse_proc_stat(stat_text)
    if ticks is None:
        return None
    statm_fields = statm_text.split()
    if len(statm_fields) < 2:
        return None
    try:
        resident_pages = int(statm_fields[1])
        clock_ticks_per_second = os.sysconf("SC_CLK_TCK")
        page_size_bytes = os.sysconf("SC_PAGE_SIZE")
    except (ValueError, OSError):
        return None
    if clock_ticks_per_second <= 0 or page_size_bytes <= 0:
        return None
    cpu_seconds = (ticks[0] + ticks[1]) / clock_ticks_per_second
    return resident_pages * page_size_bytes, cpu_seconds


def process_sample(pid: int, previous: tuple[float, int] | None) -> tuple[dict[str, float | int], tuple[float, int] | None]:
    now = time.monotonic()
    proc_reading = read_process_proc(pid)
    if proc_reading is not None:
        rss, seconds = proc_reading
        cpu = 0.0 if previous is None else max(0.0, (seconds - previous[0]) / max(now - previous[1], 1e-9))
        return {"monotonic_ns": time.monotonic_ns(), "pid": pid, "alive": 1, "rss_bytes": rss, "process_cpu_fraction": cpu}, (seconds, now)
    try:
        result = subprocess.run(
            ["ps", "-o", "rss=,time=", "-p", str(pid)], text=True,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, check=False,
        )
        fields = result.stdout.split()
        if result.returncode or len(fields) != 2:
            return {"monotonic_ns": time.monotonic_ns(), "pid": pid, "alive": 0}, previous
        rss = int(fields[0]) * 1024
        parts = fields[1].split(":")
        seconds = sum(float(piece) * 60 ** (len(parts) - index - 1) for index, piece in enumerate(parts))
        cpu = 0.0 if previous is None else max(0.0, (seconds - previous[0]) / max(now - previous[1], 1e-9))
        return {"monotonic_ns": time.monotonic_ns(), "pid": pid, "alive": 1, "rss_bytes": rss, "process_cpu_fraction": cpu}, (seconds, now)
    except (OSError, ValueError):
        return {"monotonic_ns": time.monotonic_ns(), "pid": pid, "alive": 0}, previous


def host_ticks() -> tuple[int, int] | None:
    try:
        fields = Path("/proc/stat").read_text().splitlines()[0].split()[1:]
        ticks = [int(value) for value in fields]
        return sum(ticks), ticks[3] + (ticks[4] if len(ticks) > 4 else 0)
    except (OSError, ValueError, IndexError):
        return None


def host_sample(previous: tuple[int, int] | None = None) -> tuple[dict[str, float | int], tuple[int, int] | None]:
    memory = linux_memory()
    result: dict[str, float | int] = {"host_monotonic_ns": time.monotonic_ns()}
    if hasattr(os, "getloadavg"):
        one, five, fifteen = os.getloadavg()
        result.update(host_load_1=one, host_load_5=five, host_load_15=fifteen)
    if "MemTotal" in memory:
        result["host_ram_total_bytes"] = memory["MemTotal"]
    if "MemAvailable" in memory:
        result["host_ram_available_bytes"] = memory["MemAvailable"]
    ticks = host_ticks()
    if ticks is not None and previous is not None and ticks[0] > previous[0]:
        result["host_cpu_fraction"] = 1.0 - (ticks[1] - previous[1]) / (ticks[0] - previous[0])
    if sys.platform == "darwin":
        top = command_text(["top", "-l", "1", "-n", "0"])
        match = re.search(r"CPU usage:\s*([0-9.]+)% user,\s*([0-9.]+)% sys", top or "")
        if match:
            result["host_cpu_fraction"] = (float(match.group(1)) + float(match.group(2))) / 100.0
    return result, ticks


def daemon_command(binary: Path, args: argparse.Namespace) -> list[str]:
    command = [str(binary), "upstream", "--selection", str(args.selection), "--output", str(args.output_dir / "report.json"), "--workers", str(args.workers), "--min-seconds", str(args.min_seconds), "--max-seconds", str(args.max_seconds), "--min-events", str(args.min_events)]
    if getattr(args, "cpu_timing", False):
        command.append("--cpu-timing")
    if args.diagnostic_nonqualify:
        command.append("--diagnostic")
    return command


def collect(binary: Path, args: argparse.Namespace, manifest: dict[str, Any]) -> int:
    args.output_dir.mkdir(parents=True)
    (args.output_dir / "metadata.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    command = daemon_command(binary, args)
    try:
        process = subprocess.Popen(command, cwd=ROOT)
    except OSError as error:
        manifest["measurement"].update(outcome="launch_failed", exit_code=None, source_binary_hash_match=False, finished_unix_ns=time.time_ns(), error=str(error))
        (args.output_dir / "metadata.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        return 3
    manifest["measurement"].update(live_started=True, child_pid=process.pid, outcome="running", build_matches=True)
    (args.output_dir / "metadata.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    prior: tuple[float, int] | None = None
    host_prior: tuple[int, int] | None = None
    started = time.monotonic()
    interrupted = False
    try:
        with (args.output_dir / "resources.jsonl").open("x") as samples:
            while process.poll() is None:
                sample, prior = process_sample(process.pid, prior)
                host, host_prior = host_sample(host_prior)
                sample.update(host)
                samples.write(json.dumps(sample, separators=(",", ":")) + "\n")
                samples.flush()
                time.sleep(args.sample_seconds)
                if time.monotonic() - started > args.max_seconds + 240 + 30:
                    interrupted = True
                    break
    except (KeyboardInterrupt, OSError):
        interrupted = True
    if interrupted and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
    code = process.wait()
    matched = selection_match = build_matches = False
    try:
        matched = manifest["binary_sha256"] == sha256(binary) and manifest["source_sha256"] == source_fingerprint()
        final_selection_hash, final_selection = selection_snapshot(args.selection)
        selection_match = manifest["selection_sha256"] == final_selection_hash and manifest["selection_snapshot"] == final_selection
        build_matches = receipt_matches(binary, manifest["source_sha256"], manifest["rustc"])
    except (OSError, ValueError):
        manifest["measurement"]["verification_error"] = "post-run provenance unavailable"
    report = args.output_dir / "report.json"
    manifest["report_sha256"] = sha256(report) if report.is_file() else None
    manifest["measurement"].update(outcome="interrupted" if interrupted else ("ok" if code == 0 else "daemon_failed"), exit_code=code, source_binary_hash_match=matched, selection_hash_match=selection_match, build_matches=build_matches, timeout_reason="wrapper_guard" if interrupted else None, finished_unix_ns=time.time_ns())
    manifest["build_receipt"] = build_receipt(binary)
    (args.output_dir / "metadata.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return 3 if not matched else code


def self_test() -> int:
    assert source_fingerprint() == source_fingerprint()
    assert host_sample()[0]["host_monotonic_ns"] > 0
    args = argparse.Namespace(selection=Path("x.json"), output_dir=Path("out"), workers=4, min_seconds=1, max_seconds=2, min_events=1, diagnostic_nonqualify=True)
    command = daemon_command(Path("pmwsd"), args)
    assert command[1] == "upstream" and "--diagnostic" in command and "--diagnostic-nonqualify" not in command
    assert "--cpu-timing" not in command
    args.cpu_timing = True
    args.diagnostic_nonqualify = False
    command = daemon_command(Path("pmwsd"), args)
    assert "--cpu-timing" in command and "--diagnostic" not in command
    assert 2 + 240 + 30 > 2
    assert clock_calibration()["minimum_nonzero_ns"] > 0
    assert not receipt_matches(Path("missing-pmwsd"), "source", "compiler")
    return 0


def parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selection", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--binary", type=Path, default=DEFAULT_BINARY)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--min-seconds", type=int, default=900)
    parser.add_argument("--max-seconds", type=int, default=7200)
    parser.add_argument("--min-events", type=int, default=10000)
    parser.add_argument("--sample-seconds", type=int, default=2)
    parser.add_argument("--diagnostic-nonqualify", action="store_true")
    parser.add_argument("--cpu-timing", action="store_true", help="pair native stage wall time with thread CPU time; does not waive qualification floors")
    parser.add_argument("--check-only", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--build-only", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse()
    if args.self_test:
        return self_test()
    if args.build_only:
        try: return build_only(args.binary)
        except (OSError, ValueError) as error: raise SystemExit(f"build failed: {error}")
    if args.selection is None or args.output_dir is None:
        raise SystemExit("--selection and --output-dir are required")
    if args.run and args.check_only:
        raise SystemExit("choose --run or --check-only")
    try:
        require_valid(args)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        raise SystemExit(f"preflight failed: {error}")
    manifest = metadata(args.binary, args)
    if not args.run:
        print(json.dumps(manifest, indent=2, sort_keys=True))
        return 0
    if args.output_dir.exists():
        raise SystemExit("output-dir must not already exist")
    if not args.binary.is_file() or not os.access(args.binary, os.X_OK):
        raise SystemExit("binary must exist and be executable; this wrapper never builds it")
    if not receipt_matches(args.binary, manifest["source_sha256"], manifest["rustc"]):
        raise SystemExit("--run requires a matching --build-only receipt")
    return collect(args.binary, args, manifest)


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Scaled market discovery: N Limitless leaf markets and M Polymarket conditions.

Reuses bench/discover_evidence_markets.py helpers; Gamma pages are fetched through curl because
some networks reset Python's TLS handshake to gamma-api. Writes the same selection shape the
daemon reads. Each venue phase can be run alone (--skip-limitless / --skip-polymarket) so the two
REST budgets are spent at different times; --merge combines two partial files.
"""

import argparse
import json
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import discover_markets as limitless  # noqa: E402
import discover_polymarket as pm  # noqa: E402
import discover_evidence_markets as ev  # noqa: E402


def fetch_page_curl(cursor, limit, timeout, include_tags=False):
    query_args = {"closed": "false", "limit": limit}
    if cursor is not None:
        query_args["after_cursor"] = cursor
    url = f"{pm.API_URL}?{urllib.parse.urlencode(query_args)}"
    last = None
    for attempt in range(5):
        result = subprocess.run(["curl", "-s", "--max-time", str(int(timeout)), "--retry", "0", "-H",
                                 "User-Agent: pm-ws-bench-discover/1.0", "-w", "\n%{http_code}", url],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
        body, _, code = result.stdout.rpartition("\n")
        if result.returncode == 0 and code == "200":
            decoded = json.loads(body)
            rows = decoded.get("markets")
            if not isinstance(rows, list):
                raise pm.DiscoveryError("Gamma response has no markets array")
            return rows, decoded.get("next_cursor")
        if code == "429":
            raise pm.DiscoveryError("venue pushback: HTTP 429 from Gamma")
        last = f"curl exit {result.returncode} http {code!r}"
        print(f"gamma page attempt {attempt + 1} failed: {last}; backing off", file=sys.stderr, flush=True)
        time.sleep(2 * (2 ** attempt))
    raise pm.DiscoveryError(f"network error from Gamma via curl: {last}")


def discover_limitless(pages, min_remaining, exclude_matches, budget):
    now = time.time()
    state = limitless.FetchState(max_calls=budget)
    left, excluded = {}, set()
    for page in range(1, pages + 1):
        rows = limitless.fetch_page(page, state, sort_by="high_value")
        print(f"limitless page {page}: {len(rows)} rows, eligible so far {len(left)}", file=sys.stderr, flush=True)
        if not rows:
            break
        candidates, dropped = ev.limitless_candidates(rows, now, min_remaining, exclude_matches)
        left.update(candidates)
        excluded.update(dropped)
        time.sleep(limitless.PAGE_PACING_SECONDS)
    ranked = sorted(left.values(), key=lambda r: ("up-or-down-daily" in r["slug"], r["volume"]), reverse=True)
    return ranked, state.call_count, len(excluded)


def discover_polymarket(pages, min_remaining):
    now = time.time()
    right, cursor, calls = {}, None, 0
    for page in range(pages):
        rows, cursor = fetch_page_curl(cursor, 100, 30)
        calls += 1
        print(f"gamma page {page + 1}: {len(rows)} rows, eligible so far {len(right)}", file=sys.stderr, flush=True)
        for row in rows:
            selected = pm.selected_descriptor(row, 0)
            if selected is None:
                continue
            end = ev.epoch(row.get("endDate"))
            if end is None or end < now + min_remaining:
                continue
            selected.update(slug=row.get("slug"), title=row.get("question"), end_epoch=int(end),
                            volume_24hr=row.get("volume24hr", 0))
            right[selected["condition_id"]] = selected
        if cursor is None:
            break
        time.sleep(0.5)
    ranked = sorted(right.values(), key=lambda r: float(r.get("volume_24hr") or 0), reverse=True)
    return ranked, calls


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", required=True)
    parser.add_argument("--limitless-count", type=int, default=500)
    parser.add_argument("--polymarket-count", type=int, default=1000)
    parser.add_argument("--limitless-pages", type=int, default=26)
    parser.add_argument("--limitless-budget", type=int, default=30)
    parser.add_argument("--polymarket-pages", type=int, default=30)
    parser.add_argument("--min-remaining-seconds", type=int, default=10800)
    parser.add_argument("--exclude-match-markets", action="store_true")
    parser.add_argument("--skip-limitless", action="store_true")
    parser.add_argument("--skip-polymarket", action="store_true")
    parser.add_argument("--merge", nargs="*", default=[], help="prior partial selection files to take venue lists from")
    args = parser.parse_args()
    result = {"discovered_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "min_remaining_seconds": args.min_remaining_seconds,
              "limitless": [], "polymarket": [], "limitless_rest_calls": 0, "polymarket_rest_calls": 0}
    for path in args.merge:
        prior = json.load(open(path))
        for venue in ("limitless", "polymarket"):
            if prior.get(venue) and not result[venue]:
                result[venue] = prior[venue]
    if not args.skip_limitless:
        ranked, calls, excluded = discover_limitless(args.limitless_pages, args.min_remaining_seconds,
                                                     args.exclude_match_markets, args.limitless_budget)
        result["limitless"] = ranked[: args.limitless_count]
        result["limitless_rest_calls"] = calls
        result["limitless_eligible"] = len(ranked)
        result["limitless_excluded_matches"] = excluded
    if not args.skip_polymarket:
        ranked, calls = discover_polymarket(args.polymarket_pages, args.min_remaining_seconds)
        result["polymarket"] = ranked[: args.polymarket_count]
        result["polymarket_rest_calls"] = calls
        result["polymarket_eligible"] = len(ranked)
    with open(args.output, "w") as handle:
        json.dump(result, handle, indent=1)
    print(json.dumps({k: v for k, v in result.items() if k not in ("limitless", "polymarket")}
                     | {"limitless": len(result["limitless"]), "polymarket": len(result["polymarket"])}))


if __name__ == "__main__":
    main()

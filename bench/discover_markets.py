#!/usr/bin/env python3
"""Walks limitless.exchange's open-market listing and prints CLOB slugs.

Default mode pages `GET /markets/active?page=N` from page 1 until the first empty page or
`--max-pages`, printing every `tradeType == "clob"` slug it sees, one per line, in the order
pages returned them. `--page1-only` instead fetches page 1 alone and, given one or more
`--known` files, prints only the clob slugs absent from all of them -- the mid-run poll for
newly listed markets.

This script holds no venue credentials and calls only the public endpoint above. Pacing sleeps
500ms between page calls; an HTTP 429 backs off (Retry-After when the venue sends one, else
1s/2s/4s.../doubling) and a third 429 in one invocation aborts rather than continuing to press
a venue that has already said no three times (project abort threshold: >3 rejections/10min).
Any other HTTP or network error gets one retry after 1s, then fails loudly. The total REST
call count for the invocation is printed to stderr on exit, always, as `rest_calls: <n>`, so a
caller can accumulate it against the operator's REST budget without parsing prose.
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.request

API_URL = "https://api.limitless.exchange/markets/active"
MAX_TOTAL_429S = 3
PAGE_PACING_SECONDS = 0.5
OTHER_ERROR_RETRY_SECONDS = 1.0


class AbortError(Exception):
    pass


class FetchState:
    def __init__(self, max_calls=60):
        if not 1 <= max_calls <= 60:
            raise ValueError("REST request budget must be between 1 and 60")
        self.max_calls = max_calls
        self.call_count = 0
        self.rejection_count = 0


def _extract_rows(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "markets", "rows", "items", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
        list_values = [value for value in payload.values() if isinstance(value, list)]
        if len(list_values) == 1:
            return list_values[0]
    raise AbortError(
        f"discover_markets.py: unrecognized markets/active response shape: {type(payload).__name__}"
    )


def fetch_page(page, state, *, sort_by=None):
    url = f"{API_URL}?page={page}"
    if sort_by is not None:
        if sort_by not in ("lp_rewards", "ending_soon", "newest", "high_value", "liquidity"):
            raise ValueError("unsupported active-market sort")
        url += f"&limit=25&sortBy={sort_by}"
    backoff = 1.0
    retried_other = False
    while True:
        if state.call_count >= state.max_calls:
            raise AbortError("public market selection request budget exhausted")
        state.call_count += 1
        request = urllib.request.Request(
            url,
            method="GET",
            headers={"User-Agent": "pm-ws-bench-discover/1.0 (operator market selection)"},
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = response.read(8 * 1024 * 1024 + 1)
                if len(body) > 8 * 1024 * 1024:
                    raise AbortError("public market listing exceeds 8 MiB")
            return _extract_rows(json.loads(body.decode("utf-8")))
        except urllib.error.HTTPError as error:
            if error.code in (400, 401, 403):
                raise AbortError(f"terminal discovery HTTP {error.code}") from error
            if error.code == 429:
                state.rejection_count += 1
                if state.rejection_count >= MAX_TOTAL_429S:
                    print(
                        f"discover_markets.py: ABORT after {state.rejection_count} HTTP 429 "
                        "rejections in this invocation; not retrying further "
                        "(project abort threshold: >3 rejections/10min)",
                        file=sys.stderr,
                    )
                    raise AbortError("too many HTTP 429 rejections") from error
                retry_after = error.headers.get("Retry-After") if error.headers else None
                delay = backoff
                if retry_after is not None:
                    try:
                        delay = float(retry_after)
                    except ValueError:
                        pass
                    else:
                        backoff *= 2
                else:
                    backoff *= 2
                time.sleep(delay)
                continue
            if not retried_other:
                retried_other = True
                time.sleep(OTHER_ERROR_RETRY_SECONDS)
                continue
            raise AbortError(
                f"discover_markets.py: HTTP {error.code} from {url} after one retry: {error}"
            ) from error
        except urllib.error.URLError as error:
            if not retried_other:
                retried_other = True
                time.sleep(OTHER_ERROR_RETRY_SECONDS)
                continue
            raise AbortError(
                f"discover_markets.py: network error from {url} after one retry: {error}"
            ) from error


def clob_slugs(rows):
    return [row["slug"] for row in rows if row.get("tradeType") == "clob" and "slug" in row]


def read_known(paths):
    known = set()
    for path in paths:
        try:
            handle = open(path, "r", encoding="utf-8")
        except OSError:
            continue
        with handle:
            for line in handle:
                stripped = line.strip()
                if stripped and not stripped.startswith("#"):
                    known.add(stripped)
    return known


def walk_pages(max_pages, state):
    for page in range(1, max_pages + 1):
        rows = fetch_page(page, state)
        if not rows:
            break
        for slug in clob_slugs(rows):
            print(slug)
            sys.stdout.flush()
        if page < max_pages:
            time.sleep(PAGE_PACING_SECONDS)


def page1_only(known_paths, state):
    known = read_known(known_paths)
    rows = fetch_page(1, state)
    for slug in clob_slugs(rows):
        if slug not in known:
            print(slug)
            sys.stdout.flush()


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--max-pages", type=int, default=40)
    parser.add_argument("--page1-only", action="store_true")
    parser.add_argument("--known", action="append", default=[])
    args = parser.parse_args()

    state = FetchState()
    try:
        if args.page1_only:
            page1_only(args.known, state)
        else:
            walk_pages(args.max_pages, state)
    except AbortError as error:
        print(f"discover_markets.py: {error}", file=sys.stderr)
        return 1
    finally:
        print(f"rest_calls: {state.call_count}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

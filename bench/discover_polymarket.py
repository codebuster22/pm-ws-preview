#!/usr/bin/env python3
"""Select active binary Polymarket CLOB markets without retaining API responses.

Walks Gamma's public ``GET /markets/keyset`` listing with its documented ``limit``,
``after_cursor`` and ``closed=false`` query parameters.  A selected row must explicitly be active, not
closed, order-book enabled, accepting orders, and have exactly two distinct CLOB token IDs
and exactly two outcomes.  It writes one compact JSON descriptor per physical market:
``{"condition_id": "…", "clob_token_ids": ["yes", "no"]}``.  This is market selection
metadata, not a WebSocket capture; HTTP bodies and any WebSocket frames are never written.

The command exits non-zero if fewer than ``--min-markets`` qualifying binary markets were
found, so a benchmark cannot silently degrade below its requested Polymarket coverage.
``--self-test`` runs wholly offline.
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


API_URL = "https://gamma-api.polymarket.com/markets/keyset"
DEFAULT_LIMIT = 100
DEFAULT_PACING_SECONDS = 0.25
DEFAULT_TIMEOUT_SECONDS = 30


class DiscoveryError(Exception):
    """The venue listing was unavailable, pushed back, or did not match its contract."""


def parse_json_array(value, field, row_number):
    """Return ``value`` as a JSON array or raise a schema error naming the affected row."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as error:
            raise DiscoveryError(
                f"row {row_number}: {field} is not a JSON array string"
            ) from error
    if not isinstance(value, list):
        raise DiscoveryError(f"row {row_number}: {field} is not an array")
    return value


def tag_slugs(row, row_number):
    """Return unique native tag slugs or reject missing/malformed requested metadata."""
    tags = row.get("tags")
    if not isinstance(tags, list) or any(not isinstance(tag, dict) for tag in tags):
        raise DiscoveryError(f"row {row_number}: tags are not an object array")
    slugs = [tag.get("slug") for tag in tags]
    if any(not isinstance(slug, str) or not slug for slug in slugs):
        raise DiscoveryError(f"row {row_number}: tag has no slug")
    return sorted(set(slugs))


def selected_descriptor(row, row_number):
    """Return a binary CLOB descriptor, ``None`` for a non-qualifying row, or fail closed.

    The Gamma response documents ``clobTokenIds`` and ``outcomes`` as strings, but accepts an
    already-decoded array too so a documented representation change is not silently treated
    as an empty market universe.  A row that claims to be an active order-book market but has
    malformed binary identity fails the whole selection rather than being skipped.
    """
    if row.get("active") is not True or row.get("closed") is True:
        return None
    if row.get("enableOrderBook") is not True or row.get("acceptingOrders") is not True:
        return None

    condition_id = row.get("conditionId")
    if not isinstance(condition_id, str) or not condition_id:
        raise DiscoveryError(f"row {row_number}: active CLOB market has no conditionId")
    tokens = parse_json_array(row.get("clobTokenIds"), "clobTokenIds", row_number)
    outcomes = parse_json_array(row.get("outcomes"), "outcomes", row_number)
    if len(tokens) != 2 or len(outcomes) != 2:
        return None
    if any(not isinstance(token, str) or not token for token in tokens):
        raise DiscoveryError(f"row {row_number}: CLOB token ID is not a non-empty string")
    if len(set(tokens)) != 2:
        raise DiscoveryError(f"row {row_number}: binary market repeats a CLOB token ID")
    if any(not isinstance(outcome, str) or not outcome for outcome in outcomes):
        raise DiscoveryError(f"row {row_number}: outcome is not a non-empty string")
    return {"condition_id": condition_id, "clob_token_ids": tokens}


def fetch_page(cursor, limit, timeout, include_tags=False):
    """Fetch one Gamma page, optionally with tags; HTTP pushback is a terminal failure."""
    query_args = {"closed": "false", "limit": limit}
    if cursor is not None:
        query_args["after_cursor"] = cursor
    if include_tags:
        query_args["include_tag"] = "true"
    query = urllib.parse.urlencode(query_args)
    request = urllib.request.Request(
        f"{API_URL}?{query}",
        method="GET",
        headers={"User-Agent": "pm-ws-bench-discover/1.0"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read()
    except urllib.error.HTTPError as error:
        if error.code == 429:
            retry_after = error.headers.get("Retry-After") if error.headers else None
            suffix = f" (Retry-After: {retry_after})" if retry_after else ""
            raise DiscoveryError(f"venue pushback: HTTP 429 from Gamma{suffix}") from error
        raise DiscoveryError(f"HTTP {error.code} from Gamma") from error
    except urllib.error.URLError as error:
        raise DiscoveryError(f"network error from Gamma: {error.reason}") from error
    try:
        decoded = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DiscoveryError("Gamma returned invalid JSON") from error
    if not isinstance(decoded, dict):
        raise DiscoveryError("Gamma /markets/keyset response is not an object")
    rows = decoded.get("markets")
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise DiscoveryError("Gamma /markets response contains a non-object row")
    next_cursor = decoded.get("next_cursor")
    if next_cursor is not None and (not isinstance(next_cursor, str) or not next_cursor):
        raise DiscoveryError("Gamma /markets/keyset returned an invalid next_cursor")
    return rows, next_cursor


def discover(limit, max_pages, pacing_seconds, timeout, target_markets):
    """Return at least the requested descriptors without walking unrelated history."""
    descriptors = {}
    calls = 0
    cursor = None
    for page in range(max_pages):
        rows, next_cursor = fetch_page(cursor, limit, timeout)
        calls += 1
        if not rows:
            break
        for index, row in enumerate(rows):
            descriptor = selected_descriptor(row, page * limit + index)
            if descriptor is not None:
                key = descriptor["condition_id"]
                prior = descriptors.get(key)
                if prior is not None and prior != descriptor:
                    raise DiscoveryError(
                        f"conditionId {key!r} appeared with conflicting CLOB token IDs"
                    )
                descriptors[key] = descriptor
        if len(descriptors) >= target_markets:
            break
        if next_cursor is None:
            break
        if next_cursor == cursor:
            raise DiscoveryError("Gamma /markets/keyset repeated its next_cursor")
        cursor = next_cursor
        time.sleep(pacing_seconds)
    else:
        raise DiscoveryError(f"reached --max-pages {max_pages} before Gamma listing ended")
    return [descriptors[key] for key in sorted(descriptors)], calls


def self_test():
    """Exercise filtering, JSON-string parsing, and malformed active-market refusal offline."""
    binary = {
        "active": True,
        "closed": False,
        "enableOrderBook": True,
        "acceptingOrders": True,
        "conditionId": "0xcondition",
        "clobTokenIds": '["yes-token", "no-token"]',
        "outcomes": '["Yes", "No"]',
    }
    assert selected_descriptor(binary, 0) == {
        "condition_id": "0xcondition",
        "clob_token_ids": ["yes-token", "no-token"],
    }
    assert selected_descriptor({**binary, "closed": True}, 1) is None
    assert selected_descriptor({**binary, "outcomes": '["A", "B", "C"]'}, 2) is None
    try:
        selected_descriptor({**binary, "clobTokenIds": "not-json"}, 3)
    except DiscoveryError:
        pass
    else:
        raise AssertionError("malformed active CLOB identity must fail closed")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", help="JSONL destination for selected market descriptors")
    parser.add_argument("--min-markets", type=int, default=1000)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--max-pages", type=int, default=1000)
    parser.add_argument("--pacing-seconds", type=float, default=DEFAULT_PACING_SECONDS)
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        self_test()
        print("discover_polymarket.py: self-test passed")
        return 0
    if not args.output:
        parser.error("--output is required unless --self-test is used")
    if args.min_markets < 1 or args.limit < 1 or args.max_pages < 1:
        parser.error("--min-markets, --limit, and --max-pages must be positive")
    if args.pacing_seconds < 0 or args.timeout_seconds <= 0:
        parser.error("--pacing-seconds must be non-negative and --timeout-seconds positive")

    calls = 0
    try:
        descriptors, calls = discover(
            args.limit,
            args.max_pages,
            args.pacing_seconds,
            args.timeout_seconds,
            args.min_markets,
        )
        if len(descriptors) < args.min_markets:
            raise DiscoveryError(
                f"selected {len(descriptors)} active binary CLOB markets; "
                f"need at least {args.min_markets}"
            )
        with open(args.output, "w", encoding="utf-8") as output:
            for descriptor in descriptors:
                output.write(json.dumps(descriptor, separators=(",", ":")) + "\n")
    except (DiscoveryError, OSError) as error:
        print(f"discover_polymarket.py: {error}", file=sys.stderr)
        return 1
    finally:
        print(f"rest_calls: {calls}", file=sys.stderr)
    print(f"selected_markets: {len(descriptors)}", file=sys.stderr)
    print(f"selected_tokens: {2 * len(descriptors)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

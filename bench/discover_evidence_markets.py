#!/usr/bin/env python3
"""Select 100 currently open markets per venue; retain descriptors, never feed data."""

import argparse
import datetime
import hashlib
import json
import pathlib
import re
import time

import discover_markets as limitless
import discover_polymarket as polymarket


def match_market(title):
    return bool(re.search(r"\bvs\.?\s|\bmap\s+\d+\s+winner\b|\btotal (?:goals|corners|cards)\b|\bboth(?:\s+teams)?\s+to\s+score\b",
                          str(title), re.IGNORECASE))


def epoch(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return value / 1000 if value > 100_000_000_000 else value
    try:
        return datetime.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except (ValueError, AttributeError):
        return None


def limitless_candidates(rows, now, minimum_remaining, exclude_matches):
    selected, excluded = {}, set()
    for parent in rows:
        children = parent.get("markets") if isinstance(parent, dict) else None
        leaves = ((child, parent.get("title")) for child in children) if isinstance(children, list) else [(parent, None)]
        for row, parent_title in leaves:
            if not isinstance(row, dict) or row.get("markets") is not None:
                continue
            slug, condition = row.get("slug"), row.get("conditionId")
            end = epoch(row.get("expirationTimestamp")) or epoch(row.get("expirationDate"))
            start = epoch(row.get("startAt"))
            if (row.get("tradeType") != "clob" or not isinstance(slug, str) or not slug or not isinstance(condition, str)
                    or not condition or row.get("expired") is not False or end is None
                    or end < now + minimum_remaining or (start is not None and start > now)):
                continue
            if exclude_matches and (match_market(row.get("title")) or match_market(parent_title)):
                excluded.add(slug)
                continue
            selected[slug] = {"slug": slug, "title": row.get("title") or parent_title,
                "condition_id": condition, "market_kind": "leaf", "market_type": row.get("marketType"), "end_epoch": int(end),
                "start_epoch": start, "volume": int(row.get("volume") or 0)}
    return selected, excluded


def cached_rows(path):
    root = json.loads(path.read_text(encoding="utf-8"))
    rows = root.get("rows") if isinstance(root, dict) else None
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise RuntimeError("Limitless descriptor cache must be an object with a rows object array")
    digest = hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return rows, {"path": str(path), "sha256": digest, "rows": len(rows)}


def reused_limitless(path, now, minimum_remaining, exclude_matches):
    prior = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
    rows = prior.get("limitless")
    if not isinstance(rows, list):
        raise RuntimeError("reused selection has no Limitless descriptor array")
    selected, excluded, slugs, conditions = {}, set(), set(), set()
    for row in rows:
        if not isinstance(row, dict):
            raise RuntimeError("reused Limitless descriptor is not an object")
        slug, condition = row.get("slug"), row.get("condition_id")
        end, start = epoch(row.get("end_epoch")), epoch(row.get("start_epoch"))
        if (row.get("market_kind") != "leaf" or not isinstance(slug, str) or not slug
                or not isinstance(condition, str) or not condition or end is None
                or end < now + minimum_remaining or (start is not None and start > now)):
            raise RuntimeError("reused Limitless descriptor is not an active eligible leaf")
        if slug in slugs or condition in conditions:
            raise RuntimeError("reused Limitless selection has duplicate slug or condition ID")
        if exclude_matches and match_market(row.get("title")):
            excluded.add(slug)
            continue
        selected[slug] = {**row, "end_epoch": int(end), "start_epoch": start,
                          "volume": int(row.get("volume") or 0)}
        slugs.add(slug)
        conditions.add(condition)
    digest = hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return selected, excluded, {"path": str(path), "sha256": digest, "rows": len(rows),
                                "old_discovered_at": prior.get("discovered_at"),
                                "venue_status": "not_refreshed"}


def reused_polymarket(path, now, minimum_remaining, excluded_tags):
    prior = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
    rows = prior.get("polymarket")
    if not isinstance(rows, list):
        raise RuntimeError("reused selection has no Polymarket descriptor array")
    selected, conditions, tokens, excluded = [], set(), set(), {tag: 0 for tag in excluded_tags}
    for row in rows[:100]:
        if not isinstance(row, dict):
            raise RuntimeError("reused Polymarket descriptor is not an object")
        condition = row.get("condition_id")
        asset_ids = row.get("clob_token_ids")
        if (not isinstance(condition, str) or not condition or not isinstance(asset_ids, list)
                or len(asset_ids) != 2 or not all(isinstance(token, str) and token for token in asset_ids)
                or asset_ids[0] == asset_ids[1]):
            raise RuntimeError("reused Polymarket descriptor is not a binary condition")
        if condition in conditions or any(token in tokens for token in asset_ids):
            raise RuntimeError("reused Polymarket selection has duplicate condition or token IDs")
        tag_slugs = row.get("tag_slugs")
        if excluded_tags and (not isinstance(tag_slugs, list)
                              or any(not isinstance(tag, str) or not tag for tag in tag_slugs)):
            raise RuntimeError("reused Polymarket selection lacks tag evidence; fresh discovery needed")
        matched = excluded_tags.intersection(tag_slugs or ())
        if matched:
            for tag in matched:
                excluded[tag] += 1
            continue
        end = epoch(row.get("end_epoch"))
        if end is None or end < now + minimum_remaining:
            raise RuntimeError("reused Polymarket descriptor expires before the requested window")
        selected.append({**row, "end_epoch": int(end)})
        conditions.add(condition)
        tokens.update(asset_ids)
    if len(selected) != 100:
        raise RuntimeError("reused selection has fewer than 100 Polymarket conditions")
    return selected, prior.get("discovered_at"), excluded


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--limitless-pages", type=int, default=26)
    parser.add_argument("--limitless-start-page", type=int, default=1)
    parser.add_argument("--limitless-sort-by", default="high_value",
                        choices=("lp_rewards", "ending_soon", "newest", "high_value", "liquidity"))
    parser.add_argument("--limitless-descriptor-cache", type=pathlib.Path)
    parser.add_argument("--reuse-limitless-selection", action="append", type=pathlib.Path, default=[])
    parser.add_argument("--limitless-request-budget", type=int, default=60)
    parser.add_argument("--polymarket-pages", type=int, default=12)
    parser.add_argument("--min-remaining-seconds", type=int, default=7200)
    parser.add_argument("--reuse-polymarket-selection", type=pathlib.Path)
    parser.add_argument("--polymarket-exclude-tag", action="append", default=[])
    parser.add_argument("--exclude-match-markets", action="store_true",
                        help="Exclude recognizable individual-match titles from Limitless selection")
    args = parser.parse_args()
    if pathlib.Path(args.output).exists():
        raise RuntimeError("selection output already exists; choose a new path")
    if (not 0 <= args.limitless_pages <= 26 or not 1 <= args.limitless_start_page
            or not 1 <= args.limitless_request_budget <= 60 or not 1 <= args.polymarket_pages <= 30):
        raise RuntimeError("discovery page counts exceed bounded selection budget")
    if args.limitless_pages == 0 and not (args.limitless_descriptor_cache or args.reuse_limitless_selection):
        raise RuntimeError("zero Limitless pages requires a descriptor cache or reused selection")
    now = time.time()
    excluded_tags = set(args.polymarket_exclude_tag)
    if any(not tag or tag != tag.lower() or tag != tag.strip() for tag in excluded_tags):
        raise RuntimeError("Polymarket exclusion tags must be non-empty lowercase slugs")
    state = limitless.FetchState(max_calls=args.limitless_request_budget)
    cached, cache = (cached_rows(args.limitless_descriptor_cache)
                     if args.limitless_descriptor_cache else ([], None))
    left, excluded_matches = limitless_candidates(
        cached, now, args.min_remaining_seconds, args.exclude_match_markets)
    limitless_reuse = []
    for path in args.reuse_limitless_selection:
        selected, excluded, provenance = reused_limitless(
            path, now, args.min_remaining_seconds, args.exclude_match_markets)
        left.update(selected)
        excluded_matches.update(excluded)
        limitless_reuse.append(provenance)
    for page in range(args.limitless_start_page, args.limitless_start_page + args.limitless_pages):
        rows = limitless.fetch_page(page, state, sort_by=args.limitless_sort_by)
        if not rows:
            break
        candidates, excluded = limitless_candidates(
            rows, now, args.min_remaining_seconds, args.exclude_match_markets)
        left.update(candidates)
        excluded_matches.update(excluded)
        time.sleep(limitless.PAGE_PACING_SECONDS)
    left = sorted(left.values(), key=lambda r: ("up-or-down-daily" in r["slug"], r["volume"]), reverse=True)
    if len(left) < 100:
        raise RuntimeError(f"Only {len(left)} eligible Limitless markets; no smaller workload accepted")
    reuse = None
    excluded_tag_counts = {tag: 0 for tag in excluded_tags}
    if args.reuse_polymarket_selection:
        right, old_discovered_at, excluded_tag_counts = reused_polymarket(
            args.reuse_polymarket_selection, now, args.min_remaining_seconds, excluded_tags)
        calls = 0
        reuse = {"path": str(args.reuse_polymarket_selection), "old_discovered_at": old_discovered_at}
    else:
        right, cursor, calls = {}, None, 0
        for _ in range(args.polymarket_pages):
            rows, cursor = polymarket.fetch_page(cursor, 100, 30, bool(excluded_tags))
            calls += 1
            for row in rows:
                selected = polymarket.selected_descriptor(row, 0)
                if selected is None:
                    continue
                tag_slugs = polymarket.tag_slugs(row, 0) if excluded_tags else None
                matched = excluded_tags.intersection(tag_slugs or ())
                if matched:
                    for tag in matched:
                        excluded_tag_counts[tag] += 1
                    continue
                end = epoch(row.get("endDate"))
                if end is None or end < now + args.min_remaining_seconds:
                    continue
                selected.update(slug=row.get("slug"), title=row.get("question"), end_epoch=int(end),
                    volume_24hr=row.get("volume24hr", 0))
                if tag_slugs is not None:
                    selected["tag_slugs"] = tag_slugs
                right[selected["condition_id"]] = selected
            if cursor is None:
                break
            time.sleep(0.5)
        right = sorted(right.values(), key=lambda r: float(r.get("volume_24hr") or 0), reverse=True)
        if len(right) < 100:
            raise RuntimeError("Fewer than 100 eligible Polymarket conditions")
    result = {"discovered_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "limitless_rest_calls": state.call_count, "polymarket_rest_calls": calls,
        "selection_rule": "active started nonexpired CLOB leaf markets with conditionId and own expiry, >=minimum remaining lifetime; daily Limitless then cumulative volume; Polymarket24h volume within bounded keyset discovery; expiry conservatively floored to whole seconds",
        "min_remaining_seconds": args.min_remaining_seconds,
        "limitless": left[:100], "polymarket": right[:100]}
    if args.exclude_match_markets:
        result["selection_rule"] += "; recognizable individual-match titles excluded (selection heuristic, not a venue guarantee)"
        result["excluded_match_markets"] = len(excluded_matches)
    if reuse:
        result["polymarket_reuse"] = reuse
    if excluded_tags:
        result["selection_rule"] += "; operator Polymarket tag exclusions applied"
        result["polymarket_excluded_tags"] = sorted(excluded_tags)
        result["polymarket_excluded_tag_counts"] = excluded_tag_counts
    if cache:
        result["limitless_descriptor_cache"] = cache
    if limitless_reuse:
        result["limitless_reuse"] = limitless_reuse
    pathlib.Path(args.output).write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"limitless": len(result["limitless"]), "polymarket":len(result["polymarket"]),
        "limitless_calls":state.call_count,"polymarket_calls":calls}), flush=True)


if __name__ == "__main__":
    main()

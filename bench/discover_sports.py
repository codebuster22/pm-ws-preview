#!/usr/bin/env python3
"""Select Polymarket sports conditions by Gamma tag into a pm-ws selection file.

Fetches `GET https://gamma-api.polymarket.com/events` through curl, one tag at a time, keeping
binary order-book markets whose end date is at least --min-days away, at most --per-event markets
per event so one game's props do not fill the file, upcoming games first by start time, then
season-long markets. Writes the daemon's selection shape (`limitless` and `polymarket` rows) plus a
title, game_start and tag per row and a `valid_until` header. Descriptors only; no feed data.

  python3 bench/discover_sports.py --output selections/nfl-ncaa.json
  python3 bench/discover_sports.py --output selections/mixed.json --merge-limitless selections/limitless.json
  python3 bench/discover_sports.py --prune selections/nfl-ncaa.json --output selections/nfl-ncaa.json

One expired row blocks a --serve run, so prune or regenerate before each run. End dates are the
venue's market end, not kickoff. Gamma allows about 60 requests an hour; this tool makes at most
--pages requests per tag, paced half a second apart.
"""
import argparse
import datetime
import json
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import discover_polymarket as pm  # noqa: E402

EVENTS_URL = "https://gamma-api.polymarket.com/events"
USER_AGENT = "pm-ws-discover-sports/1.0"
NOTE = ("one expired row blocks a --serve run: prune or regenerate; "
        "end dates are the venue's market end, not kickoff")


def split_response(stdout):
    """Body, HTTP status and Retry-After from curl output ending in a `\\n<status> <retry-after>` trailer."""
    body, _, trailer = stdout.rpartition("\n")
    status, _, retry_after = trailer.partition(" ")
    return body, status, retry_after.strip()


def fetch(url, pushback):
    """One Gamma GET via curl: 200 returns the parsed body; 400/401 stop; 429 and 5xx count as venue
    pushback and the third one stops the run; a transport failure retries up to ten times with backoff."""
    delay = 1.0
    transport_failures = 0
    while True:
        result = subprocess.run(["curl", "-sS", "--max-time", "30", "-w", "\n%{http_code} %header{retry-after}",
                                 "-H", f"User-Agent: {USER_AGENT}", url],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
        body, status, retry_after = split_response(result.stdout)
        if result.returncode != 0:
            transport_failures += 1
            if transport_failures >= 10:
                raise SystemExit(f"Gamma unreachable after ten attempts (last: {result.stderr.strip()})")
            print(f"gamma: {result.stderr.strip() or 'transport error'}; retrying in {delay:.0f}s",
                  file=sys.stderr, flush=True)
            time.sleep(delay)
            delay = min(delay * 2, 8.0)
            continue
        if status == "200":
            return json.loads(body)
        if status in ("400", "401"):
            raise SystemExit(f"Gamma rejected the request (HTTP {status}), not retried: {url}")
        pushback[0] += 1
        if pushback[0] >= 3:
            raise SystemExit(f"Gamma pushed back three times, stopping (last: HTTP {status})")
        wait = float(retry_after) if status == "429" and retry_after.isdigit() else delay
        print(f"gamma: HTTP {status}; retrying in {wait:.0f}s", file=sys.stderr, flush=True)
        time.sleep(wait)
        delay = min(delay * 2, 8.0)


def whole_second_epoch(value):
    """ISO-8601 end date as an integer epoch, or None when absent or not on a whole second."""
    if not isinstance(value, str):
        return None
    try:
        stamp = datetime.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None
    return int(stamp) if stamp == int(stamp) else None


def event_rows(event, tag, min_end, per_event):
    rows = []
    for number, market in enumerate(event.get("markets") or []):
        descriptor = pm.selected_descriptor(market, number)
        end = whole_second_epoch(market.get("endDate"))
        if descriptor is None or end is None or end < min_end:
            continue
        descriptor.update(end_epoch=end, title=market.get("question") or event.get("title"),
                          game_start=market.get("gameStartTime") or market.get("startDate") or event.get("startDate"),
                          tag=tag)
        rows.append(descriptor)
    return rows[:per_event]


def discover(tags, min_days, pages, max_conditions, per_event):
    now = int(time.time())
    min_end = now + min_days * 86400
    end_date_min = datetime.datetime.fromtimestamp(min_end, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    found, pushback = {}, [0]
    for tag in tags:
        for page in range(pages):
            query = urllib.parse.urlencode({"tag_slug": tag, "closed": "false", "active": "true",
                                            "end_date_min": end_date_min, "order": "startDate", "ascending": "false",
                                            "limit": 100, "offset": page * 100})
            events = fetch(f"{EVENTS_URL}?{query}", pushback)
            if not isinstance(events, list):
                raise SystemExit("Gamma /events did not answer with an array")
            if page == 0 and not events:
                print(f"gamma: tag {tag!r} returned no events; check the tag slug", file=sys.stderr, flush=True)
            for event in events:
                for row in event_rows(event, tag, min_end, per_event):
                    found.setdefault(row["condition_id"], row)
            print(f"gamma {tag} page {page + 1}: {len(events)} events, {len(found)} conditions so far",
                  file=sys.stderr, flush=True)
            if len(events) < 100:
                break
            time.sleep(0.5)
    return rank(found.values(), end_date_min)[:max_conditions]


def rank(rows, now_iso):
    """Upcoming games first, soonest start first; then rows that already started or have no start, oldest first."""
    return sorted(rows, key=lambda row: ((row["game_start"] or "") < now_iso, row["game_start"] or "", row["condition_id"]))


def selection(limitless, polymarket):
    ends = [row["end_epoch"] for row in limitless + polymarket]
    valid_until = datetime.datetime.fromtimestamp(min(ends), datetime.timezone.utc).strftime("%Y-%m-%d") if ends else None
    return {"generated_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "valid_until": valid_until, "note": NOTE, "limitless": limitless, "polymarket": polymarket}


def prune(path, now):
    prior = json.load(open(path))
    keep = lambda rows: [row for row in rows if int(row["end_epoch"]) >= now + 3600]
    return selection(keep(prior.get("limitless", [])), keep(prior.get("polymarket", [])))


def self_test():
    event = {"title": "Team A vs. Team B", "startDate": "2027-01-10T18:00:00Z", "markets": [
        {"active": True, "closed": False, "enableOrderBook": True, "acceptingOrders": True, "conditionId": "0xc1",
         "clobTokenIds": '["1", "2"]', "outcomes": '["Team A", "Team B"]', "question": "A or B?",
         "endDate": "2027-01-11T00:00:00Z", "gameStartTime": "2027-01-10T18:00:00+00:00"},
        {"active": True, "closed": False, "enableOrderBook": True, "acceptingOrders": True, "conditionId": "0xc2",
         "clobTokenIds": '["3", "4"]', "outcomes": '["Yes", "No"]', "question": "Fractional end",
         "endDate": "2027-01-11T00:00:00.500Z"},
        {"active": False, "closed": True, "conditionId": "0xc3"}]}
    rows = event_rows(event, "nfl", 0, 4)
    assert [row["condition_id"] for row in rows] == ["0xc1"], rows
    assert event_rows(event, "nfl", 0, 0) == []
    assert rows[0]["clob_token_ids"] == ["1", "2"] and rows[0]["end_epoch"] == 1799625600 and rows[0]["tag"] == "nfl"
    assert event_rows(event, "nfl", 1799625601, 4) == []
    assert whole_second_epoch("2027-01-11T00:00:00.500Z") is None and whole_second_epoch(None) is None
    built = selection([{"slug": "s", "end_epoch": 1799798400, "title": "t"}], rows)
    assert built["valid_until"] == "2027-01-11" and built["note"] == NOTE and selection([], [])["valid_until"] is None
    assert split_response('{"a": 1}\n429 7') == ('{"a": 1}', "429", "7")
    assert split_response("[]\n200 ") == ("[]", "200", "")
    starts = [{"game_start": s, "condition_id": c} for s, c in
              [("2026-01-01T00:00:00Z", "a"), ("2026-03-02T00:00:00Z", "b"), (None, "c"), ("2026-03-01T00:00:00Z", "d")]]
    assert [row["condition_id"] for row in rank(starts, "2026-02-01T00:00:00Z")] == ["d", "b", "c", "a"]
    print("discover_sports self-test: ok")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output")
    parser.add_argument("--tag", action="append", help="Gamma tag slug; default nfl and cfb")
    parser.add_argument("--min-days", type=int, default=5)
    parser.add_argument("--max-conditions", type=int, default=200, help="each condition is two targets")
    parser.add_argument("--per-event", type=int, default=4, help="markets kept per event, in the venue's order")
    parser.add_argument("--pages", type=int, default=3, help="Gamma pages of 100 events per tag")
    parser.add_argument("--merge-limitless", help="selection file whose limitless rows are copied in")
    parser.add_argument("--prune", help="selection file to copy without rows ending within an hour; no network")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    if not args.output:
        parser.error("--output is required")
    if args.prune:
        result = prune(args.prune, int(time.time()))
    else:
        limitless = []
        if args.merge_limitless:
            limitless = [{"slug": row["slug"], "end_epoch": row["end_epoch"], "title": row.get("title")}
                         for row in json.load(open(args.merge_limitless)).get("limitless", [])]
        result = selection(limitless, discover(args.tag or ["nfl", "cfb"], args.min_days,
                                               args.pages, args.max_conditions, args.per_event))
    with open(args.output, "w") as handle:
        json.dump(result, handle, indent=1)
        handle.write("\n")
    print(f"{args.output}: {len(result['limitless'])} limitless, {len(result['polymarket'])} polymarket, "
          f"valid until {result['valid_until']}")


if __name__ == "__main__":
    main()

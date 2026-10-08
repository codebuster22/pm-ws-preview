#!/usr/bin/env python3
"""Offline verifier for bounded Polymarket price_change conflict samples."""
import argparse, hashlib, json, sys
from decimal import Decimal, InvalidOperation
from pathlib import Path

MAX_RAW = 1 << 20
ECONOMIC = {"price", "size", "best_bid", "best_ask"}

def reject_duplicates(pairs):
    result = {}
    for key, value in pairs:
        if key in result: raise ValueError("duplicate_json_key:" + key)
        result[key] = value
    return result

def parse(data):
    return json.loads(data, parse_int=Decimal, parse_float=Decimal,
                      object_pairs_hook=reject_duplicates)

def decimal(value):
    try:
        value = Decimal(value)
    except (InvalidOperation, ValueError): raise ValueError("invalid_decimal")
    if not value.is_finite(): raise ValueError("nonfinite_decimal")
    return value

def canon(value, path=()):
    if isinstance(value, Decimal): return ("number", decimal(value))
    if isinstance(value, str):
        economic = len(path) == 3 and path[0] == "price_changes" and path[2] in ECONOMIC
        return ("decimal_string", decimal(value)) if economic else ("string", value)
    if isinstance(value, list): return ("array", tuple(canon(x, path + (i,)) for i, x in enumerate(value)))
    if isinstance(value, dict): return ("object", tuple((k, canon(v, path + (k,))) for k, v in sorted(value.items())))
    if value is None or isinstance(value, bool): return (type(value).__name__, value)
    raise ValueError("unsupported_json_value")

def text(event, name):
    value = event.get(name)
    if not isinstance(value, str) or not value: raise ValueError("invalid_" + name)
    return value

def key(event):
    changes = event.get("price_changes")
    if event.get("event_type") != "price_change" or not isinstance(changes, list): raise ValueError("not_price_change")
    return ["polymarket", "price_change", text(event, "market"), text(event, "timestamp"),
            [[text(x, "asset_id"), text(x, "hash")] for x in changes if isinstance(x, dict)] if all(isinstance(x, dict) for x in changes) else (_ for _ in ()).throw(ValueError("invalid_change"))]

def load(report_path, side):
    name = side["filename"]
    if not isinstance(name, str) or Path(name).name != name: raise ValueError("unsafe_filename")
    raw_path = report_path.parent / name
    with raw_path.open("rb") as source: data = source.read(MAX_RAW + 1)
    if len(data) > MAX_RAW: raise ValueError("raw_too_large:" + name)
    if hashlib.sha256(data).hexdigest() != side["sha256"]: raise ValueError("sha256:" + name)
    frame = parse(data.decode("utf-8"))
    events = frame if isinstance(frame, list) else [frame]
    index = side["event_index"]
    if isinstance(index, Decimal) and index == index.to_integral_value(): index = int(index)
    if not isinstance(index, int) or isinstance(index, bool) or not 0 <= index < len(events): raise ValueError("event_index:" + name)
    return events[index], data

def diff(left, right, path=(), found=None):
    found = [] if found is None else found
    if canon(left, path) == canon(right, path) or len(found) == 16: return found
    if isinstance(left, list) and isinstance(right, list):
        for i in range(max(len(left), len(right))):
            diff(left[i] if i < len(left) else "<absent>", right[i] if i < len(right) else "<absent>", path + (i,), found)
    elif isinstance(left, dict) and isinstance(right, dict):
        for name in sorted(set(left) | set(right)): diff(left.get(name, "<absent>"), right.get(name, "<absent>"), path + (name,), found)
    else: found.append((".".join(map(str, path)), repr(canon(left, path)), repr(canon(right, path))))
    return found

def rows(event):
    return "; ".join("%s price=%r size=%r side=%r hash=%r T=%s" % (x.get("asset_id"), x.get("price"), x.get("size"), x.get("side"), x.get("hash"), event.get("timestamp")) for x in event["price_changes"])

def verify(report_path):
    report = parse(report_path.read_text())
    samples = report.get("samples")
    if not isinstance(samples, list) or len(samples) > 3: raise ValueError("sample_bound")
    subscription = report.get("subscription", {}).get("assets_ids", [])
    targets = report.get("targets", [])
    for index, sample in enumerate(samples, 1):
        first, _ = load(report_path, sample["first"]); second, _ = load(report_path, sample["second"])
        actual = key(first)
        if actual != sample.get("key") or key(second) != actual: raise ValueError("key_mismatch:%d" % index)
        if canon(first) == canon(second): raise ValueError("semantic_equal:%d" % index)
        assets = [x[0] for x in actual[4]]
        proof = [(x.get("condition_id"), x.get("clob_token_ids"),
                  set(x.get("clob_token_ids", [])) <= set(subscription),
                  set(x.get("clob_token_ids", [])) <= set(assets)) for x in targets if isinstance(x, dict) and x.get("condition_id") == actual[2]]
        requested = [asset for asset in assets if asset in subscription]
        print("sample %d key=%r requested_in_event=%r event_assets=%r pairs(condition,tokens,subscribed_both,event_both)=%r" % (index, actual, requested, assets, proof))
        print(" first  " + rows(first)); print(" second " + rows(second))
        for item in diff(first, second): print(" diff %s: %s != %s" % item)
    print("verified %d conflicting sample pair(s)" % len(samples))

def self_test():
    a = parse('{"event_type":"price_change","market":"m","timestamp":"1","price_changes":[{"asset_id":"a","hash":"h","price":"0.50","size":"1","side":"BUY","best_bid":"0.5","best_ask":"0.6"}],"z":9007199254740992}')
    b = parse('{"z":9007199254740992,"price_changes":[{"best_ask":"0.60","side":"BUY","size":"1.0","hash":"h","asset_id":"a","best_bid":".5","price":".5"}],"timestamp":"1","market":"m","event_type":"price_change"}')
    c = parse('{"event_type":"price_change","market":"m","timestamp":"1","price_changes":[{"asset_id":"a","hash":"h","price":"0.51","size":"1","side":"BUY","best_bid":"0.5","best_ask":"0.6"}],"z":9007199254740992}')
    d = parse('{"event_type":"price_change","market":"m","timestamp":"1","price_changes":[{"asset_id":"a","hash":"h","price":"0.50","size":"1","side":"BUY","best_bid":"0.5","best_ask":"0.6"}],"z":9007199254740993}')
    assert key(a) == key(b) == key(c) and canon(a) == canon(b) and not diff(a, b)
    assert canon(a) != canon(c) and diff(a, c)[0][0] == "price_changes.0.price"
    assert canon(a) != canon(d)
    assert decimal("1234567890123456789012345678901") != decimal("1234567890123456789012345678902")
    try: parse('{"x":1,"x":2}')
    except ValueError: pass
    else: raise AssertionError("duplicate keys accepted")
    print("verify polymarket samples self-test: ok")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(); parser.add_argument("--report", type=Path); parser.add_argument("--self-test", action="store_true"); args = parser.parse_args()
    try:
        if args.self_test: self_test()
        elif args.report: verify(args.report)
        else: parser.error("--report is required unless --self-test")
    except (OSError, UnicodeError, ValueError, KeyError, json.JSONDecodeError) as error: print("verification failed: " + str(error), file=sys.stderr); sys.exit(2)

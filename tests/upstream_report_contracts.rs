use std::process::Command;

#[test]
fn upstream_selection_requires_leaf_conditions_and_tests_group_discovery() {
    let status = Command::new("python3")
        .args(["-m", "unittest", "bench/test_discover_evidence_markets.py"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()
        .expect("python3 starts");
    assert!(status.success());
    let program = r#"
import json, pathlib, sys, tempfile
sys.path.insert(0, 'bench')
import run_upstream
with tempfile.TemporaryDirectory() as temporary:
    path = pathlib.Path(temporary) / 'selection.json'
    row = {'slug': 'child', 'end_epoch': 2000000000, 'market_kind': 'leaf',
           'market_type': 'group', 'condition_id': '0x' + 'a' * 64}
    value = {'limitless': [row], 'polymarket': []}
    path.write_text(json.dumps(value))
    assert run_upstream.selection_snapshot(path)[1]['limitless'][0]['slug'] == 'child'
    for replacement in ({**row, 'market_kind': 'container'}, {**row, 'condition_id': ''}):
        path.write_text(json.dumps({'limitless': [replacement], 'polymarket': []}))
        try: run_upstream.selection_snapshot(path)
        except ValueError: pass
        else: raise AssertionError('invalid leaf evidence accepted')
    value['limitless'].append({**row, 'slug': 'alias'})
    path.write_text(json.dumps(value))
    try: run_upstream.selection_snapshot(path)
    except ValueError: pass
    else: raise AssertionError('two slugs for one physical condition accepted')
"#;
    let status = Command::new("python3")
        .args(["-c", program])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()
        .expect("python3 starts");
    assert!(status.success());
}

#[test]
fn upstream_metrics_report_self_test_is_deterministic() {
    let status = Command::new("python3")
        .args(["bench/report_upstream.py", "--self-test"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()
        .expect("python3 starts");
    assert!(status.success(), "upstream report self-test failed");
}

#[test]
fn upstream_qualification_rejects_authored_contract_regressions() {
    let program = r#"
import argparse
import copy
import hashlib
import json
import pathlib
import sys
import tempfile

sys.path.insert(0, 'bench')
import report_upstream as report
import run_upstream

CAPACITIES = {
    'shards': 2, 'connections_per_shard': 1, 'max_input_bytes': 1048576,
    'admission_bytes_per_shard': 67108864, 'atomic_events': 4096,
    'typed_nodes': 262144, 'typed_depth': 20,
    'allocator': 'system; bounded transient batches; no identity or payload history',
    'declared_data_bytes': 134217728,
}
assert CAPACITIES == run_upstream.RESOURCE_LIMITS

with tempfile.TemporaryDirectory() as temporary:
    selection_path = pathlib.Path(temporary) / 'selection.json'
    selection_path.write_text(json.dumps({'limitless': [{'slug': 'fixture',
        'market_kind': 'leaf', 'condition_id': '0x' + '0' * 64}], 'polymarket': []}))
    valid_run = argparse.Namespace(selection=selection_path, workers=2, min_seconds=900,
        max_seconds=14400, min_events=10000, sample_seconds=2,
        diagnostic_nonqualify=False, run=False)
    run_upstream.require_valid(valid_run)
    invalid_run = copy.copy(valid_run)
    invalid_run.max_seconds = 14401
    try: run_upstream.require_valid(invalid_run)
    except ValueError: pass
    else: raise AssertionError('run wrapper accepted max-seconds above four-hour cap')

def histogram(count):
    return {'count': count, 'buckets': [count] + [0] * 1023, 'max_ns': 1,
            'above_100us': 0, 'above_250us': 0, 'above_1ms': 0}

def family(index, count):
    return {'family': report.FAMILIES[index], 'stages': [histogram(count) for _ in report.STAGES]}

def shard(venue, activity):
    indexes = report.INDEXES[venue]
    published = [0] * 15
    if venue == 'limitless':
        published[0] = activity
    else:
        published[6], published[7] = activity // 2, activity - activity // 2
    for index in indexes:
        if not published[index]: published[index] = 1
    total = sum(published)
    return {
        'venue': venue, 'stream': venue + '-fixture',
        'ready_targets': 100 if venue == 'limitless' else 0,
        'required_targets': 100 if venue == 'limitless' else 200,
        'received_messages': total, 'decoded_events': total, 'malformed_messages': 0,
        'routing_rejections': 0, 'gate_all': [total, total, 0, 0],
        'gate_measured': [total, total, 0, 0], 'controls': {}, 'faults': {},
        'receiver': {'families': [family(i, published[i]) for i in indexes], 'events': total,
            'activity_events': activity, 'batches': 1, 'last_sequence': total, 'tails': [],
            'tails_omitted': 0, 'sequence_errors': 0, 'generation_errors': 0,
            'member_order_errors': 0},
        'connections': [{'received_messages': total, 'malformed_messages': 0,
            'first_selected_resolution': None,
            'arrivals_all': published, 'arrivals_measured': published,
            'published_all': published, 'published_measured': published,
            'gate_all': [total, total, 0, 0], 'gate_measured': [total, total, 0, 0]}],
        'health': [{'generation': 1, 'connected': True, 'subscription_sent': True,
            'subscription_evidence': 'acknowledged' if venue == 'limitless' else 'observed_data',
            'evidenced_targets': 100 if venue == 'limitless' else 0,
            'requested_targets': 100 if venue == 'limitless' else 200, 'last_heartbeat_ns': 1}],
    }

selection = {
    'limitless': [{'slug': 'll-' + str(i), 'end_epoch': 2000000000} for i in range(100)],
    'polymarket': [{'condition_id': 'pm-' + str(i),
        'clob_token_ids': ['token-' + str(2 * i), 'token-' + str(2 * i + 1)],
        'end_epoch': 2000000000} for i in range(100)],
}
base = {
    'schema': report.SCHEMA, 'phase': 'A', 'qualified': True, 'reason': 'healthy_floors_reached',
    'receive_clock': 'monotonic', 'measured_start_ns': 1, 'measured_end_ns': 900000000001,
    'stage_names': report.STAGES, 'gate_count_names': report.GATES,
    'invocation': {'selection': 'fixture-selection.json', 'output': 'fixture-report.json',
        'min_seconds': 900, 'max_seconds': 900, 'min_events': 10000, 'workers': 2, 'diagnostic': False},
    'consumed_selection': selection, 'shards': [shard('limitless', 10000), shard('polymarket', 10000)],
    'capacities': CAPACITIES,
}

def signed(value):
    raw = json.dumps(value, sort_keys=True, separators=(',', ':')).encode()
    digest = 'a' * 64
    manifest = {
        'binary_sha256': digest, 'source_sha256': digest, 'selection_sha256': digest,
        'report_sha256': hashlib.sha256(raw).hexdigest(), 'selection_snapshot': selection,
        'resource_limits': CAPACITIES, 'created_unix_ns': 1000000000000000000, 'rustc': 'rustc fixture',
        'config': {'selection': 'fixture-selection.json', 'workers': 2, 'min_seconds': 900,
                   'max_seconds': 900, 'min_events': 10000},
        'measurement': {'live_started': True, 'outcome': 'ok', 'exit_code': 0,
            'leaf_selection_verified': True,
            'source_binary_hash_match': True, 'selection_hash_match': True, 'build_matches': True,
            'workers': 2, 'min_seconds': 900, 'max_seconds': 900, 'min_events': 10000},
        'build_receipt': {'source_sha256': digest, 'actual_source_sha256': digest,
            'binary_sha256': digest, 'actual_binary_sha256': digest, 'compiler': 'rustc fixture',
            'flags': ['--release', '--locked', '--bin', 'pmwsd']},
    }
    if 'cpu_timing' in value['invocation']:
        for section in ('config', 'measurement'):
            manifest[section]['cpu_timing'] = value['invocation']['cpu_timing']
    return raw, manifest

def qualifies(value):
    raw, manifest = signed(value)
    report.parse(value)
    return report.qualifies(value, manifest, raw)

assert qualifies(base), 'complete two-shard fixture must qualify'
assert qualifies(base), 'qualification must be deterministic'

cpu = copy.deepcopy(base)
cpu['invocation']['cpu_timing'] = True
cpu['cpu_timing'] = {'clock': 'CLOCK_THREAD_CPUTIME_ID', 'read_failures': 0,
    'nonmonotonic_spans': 0, 'calibration': {'paired_reads': 10000,
    'minimum_nonzero_ns': 1, 'p95_ns': 100, 'p99_ns': 200, 'max_ns': 4000}}
cpu['shards'][0]['receiver']['tails'] = [{
    'sequence': 1, 'source_slot': 0, 'source_generation': 1, 'stream_generation': 1,
    'receive_ns': 2, 'decode_ns': 600000, 'validate_ns': 450000, 'gate_ns': 1000,
    'receiver_ns': 1000, 'total_ns': 1051000, 'audited_total_ns': 1052000,
    'event_count': 1, 'input_bytes': 600,
    'cpu_decode_ns': 10000, 'cpu_validate_ns': 20000, 'cpu_gate_ns': 1000,
    'cpu_receiver_ns': 1000, 'cpu_total_ns': 31000, 'cpu_audited_total_ns': 32000}]
assert qualifies(cpu), 'CPU timing must not waive or invalidate healthy qualification'
assert 'Paired tail evidence' in report.render(cpu)
assert '| json_decode | 600.000 | 10.000 | 590.000 |' in report.render(cpu)
assert 'p95=100, p99=200, max=4000' in report.render(cpu)
assert '1 batches / 1 events; thread CPU known for 1 batches' in report.render(cpu)
assert '1 below 250 µs CPU; 1 below 1 ms CPU' in report.render(cpu)
raw, manifest = signed(cpu)
manifest['power_state'] = 'Battery Power\n80%; discharging'
assert 'Power at launch: Battery Power; 80%; discharging' in report.render(cpu, manifest, raw)
manifest['measurement']['cpu_timing'] = False
assert not report.qualifies(cpu, manifest, raw), 'CPU mode provenance mismatch'

skew = copy.deepcopy(cpu)
skew['shards'][0]['receiver']['tails'][0].update(cpu_decode_ns=1600000,
    cpu_total_ns=1621000, cpu_audited_total_ns=1622000)
report.parse(skew)
assert '-1000.000' in report.render(skew), 'raw CPU sampling skew must not be clamped'

missing = copy.deepcopy(cpu)
missing['cpu_timing']['read_failures'] = 1
for field in ('cpu_decode_ns', 'cpu_total_ns', 'cpu_audited_total_ns'):
    missing['shards'][0]['receiver']['tails'][0].pop(field)
report.parse(missing)
assert '| json_decode | 600.000 | — | — |' in report.render(missing)
assert 'failed sampled reads=1' in report.render(missing)
assert 'thread CPU known for 0 batches' in report.render(missing)
assert '0 below 250 µs CPU; 0 below 1 ms CPU' in report.render(missing)

for name, change in [
    ('undeclared CPU mode', lambda v: v['invocation'].pop('cpu_timing')),
    ('missing CPU report', lambda v: v.pop('cpu_timing')),
    ('missing unexplained CPU span', lambda v: v['shards'][0]['receiver']['tails'][0].pop('cpu_gate_ns')),
    ('CPU sum mismatch', lambda v: v['shards'][0]['receiver']['tails'][0].__setitem__('cpu_total_ns', 1)),
    ('CPU invalid value', lambda v: v['shards'][0]['receiver']['tails'][0].__setitem__('cpu_decode_ns', True)),
    ('CPU calibration order', lambda v: v['cpu_timing']['calibration'].__setitem__('p95_ns', 201)),
    ('CPU calibration bound', lambda v: v['cpu_timing']['calibration'].__setitem__('paired_reads', 100001)),
    ('CPU unknown clock', lambda v: v['cpu_timing'].__setitem__('clock', 'payload-sentinel')),
]:
    invalid = copy.deepcopy(cpu)
    change(invalid)
    try: report.parse(invalid)
    except report.ReportError: pass
    else: raise AssertionError(name)

four_hour = copy.deepcopy(base)
four_hour['invocation']['max_seconds'] = 14400
report.parse(four_hour)
above_four_hours = copy.deepcopy(four_hour)
above_four_hours['invocation']['max_seconds'] = 14401
try: report.parse(above_four_hours)
except report.ReportError: pass
else: raise AssertionError('report validator accepted max-seconds above four-hour cap')

def rejected(name, change, parse_error=False):
    value = copy.deepcopy(base)
    change(value)
    try: accepted = qualifies(value)
    except report.ReportError:
        assert parse_error, name
        return
    assert not parse_error and not accepted, name

for field in ('source_binary_hash_match', 'selection_hash_match', 'build_matches', 'leaf_selection_verified'):
    value = copy.deepcopy(base); raw, manifest = signed(value); manifest['measurement'][field] = False
    assert not report.qualifies(value, manifest, raw), field
for field in ('binary_sha256', 'source_sha256', 'selection_sha256'):
    value = copy.deepcopy(base); raw, manifest = signed(value); manifest.pop(field)
    assert not report.qualifies(value, manifest, raw), field
for field in ('selection_snapshot', 'build_receipt', 'measurement'):
    value = copy.deepcopy(base); raw, manifest = signed(value); manifest.pop(field)
    assert not report.qualifies(value, manifest, raw), field

rejected('receiver count', lambda v: v['shards'][0]['receiver'].__setitem__('events', 1), True)
rejected('family sums', lambda v: v['shards'][0]['connections'][0]['published_measured'].__setitem__(0, 9999), True)
rejected('missing stage', lambda v: v['shards'][0]['receiver']['families'][0].__setitem__('stages', []), True)
rejected('short duration', lambda v: v.__setitem__('measured_end_ns', 700000000001))
rejected('activity floor', lambda v: v['shards'][0]['receiver'].__setitem__('activity_events', 9999), True)
rejected('duplicate venue', lambda v: v['shards'][1].__setitem__('venue', 'limitless'), True)
rejected('unconfirmed limitless', lambda v: (v['shards'][0].__setitem__('ready_targets', 99), v['shards'][0]['health'][0].__setitem__('evidenced_targets', 99)))
rejected('malformed', lambda v: v['shards'][0].__setitem__('malformed_messages', 1), True)
rejected('routing', lambda v: v['shards'][0].__setitem__('routing_rejections', 1))
rejected('stale', lambda v: v['shards'][0]['gate_all'].__setitem__(2, 1), True)
rejected('overload', lambda v: v['shards'][0]['gate_all'].__setitem__(3, 1), True)
rejected('generation', lambda v: v['shards'][0]['receiver'].__setitem__('generation_errors', 1))
rejected('diagnostic', lambda v: v['invocation'].__setitem__('diagnostic', True))
rejected('duplicate polymarket token', lambda v: v['consumed_selection']['polymarket'][1].__setitem__('clob_token_ids', ['token-0', 'token-3']))
rejected('expired selection', lambda v: v['consumed_selection']['limitless'][0].__setitem__('end_epoch', 1))
rejected('early resolution', lambda v: v['shards'][0]['controls'].__setitem__('selected_target_resolved', 1))
rejected('resolution witness without control', lambda v: v['shards'][0]['connections'][0].__setitem__('first_selected_resolution', 0), True)
rejected('invalid resolution coordinate', lambda v: (v['shards'][0]['controls'].__setitem__('selected_target_resolved', 1),
    v['shards'][0]['connections'][0].__setitem__('first_selected_resolution', 100)), True)
rejected('boolean resolution coordinate', lambda v: (v['shards'][0]['controls'].__setitem__('selected_target_resolved', 1),
    v['shards'][0]['connections'][0].__setitem__('first_selected_resolution', True)), True)

resolved = copy.deepcopy(base)
resolved['qualified'], resolved['reason'] = False, 'selected_target_resolved'
resolved['shards'][0]['controls']['selected_target_resolved'] = 2
resolved['shards'][0]['connections'][0]['first_selected_resolution'] = 1
report.parse(resolved)
assert 'sorted Limitless slug coordinate 1' in report.render(resolved)
assert not qualifies(resolved)

heartbeat = copy.deepcopy(base)
heartbeat['qualified'], heartbeat['reason'] = False, 'observed_fault'
heartbeat['shards'][1]['faults']['heartbeat_timeout'] = 1
heartbeat['shards'][1]['connections'][0]['heartbeat_timeout'] = {
    'generation': 1, 'observed_ns': 100, 'cause': 'deadline_elapsed',
    'ping_enqueued_ns': 10, 'writer_started_ns': 20, 'write_completed_ns': 30,
    'receipt_observed_ns': 40, 'deadline_ns': 90, 'pong_processed_ns': None,
    'last_ws_message_received_ns': 50,
}
report.parse(heartbeat)
assert 'cause=deadline_elapsed' in report.render(heartbeat)
assert '| write_completed_ns | 30 |' in report.render(heartbeat)
assert not qualifies(heartbeat)
late = copy.deepcopy(heartbeat)
late['shards'][1]['connections'][0]['heartbeat_timeout'].update(
    cause='late_pong_processed', pong_processed_ns=95)
report.parse(late)
assert 'cause=late_pong_processed' in report.render(late)
for key, value in [('generation', True), ('generation', 2), ('observed_ns', 80),
                   ('writer_started_ns', 31), ('receipt_observed_ns', 29),
                   ('deadline_ns', None), ('last_ws_message_received_ns', -1),
                   ('cause', 'payload-sentinel'), ('pong_processed_ns', True)]:
    invalid = copy.deepcopy(heartbeat)
    invalid['shards'][1]['connections'][0]['heartbeat_timeout'][key] = value
    try: report.parse(invalid)
    except report.ReportError: pass
    else: raise AssertionError('invalid heartbeat witness accepted: ' + key)
for faultless in (True, False):
    invalid = copy.deepcopy(late)
    if faultless: invalid['shards'][1]['faults'].clear()
    else: invalid['shards'][1]['connections'][0]['heartbeat_timeout']['pong_processed_ns'] = 89
    try: report.parse(invalid)
    except report.ReportError: pass
    else: raise AssertionError('unsupported heartbeat evidence accepted')

rejected('control padding', lambda v: (v['shards'][0].__setitem__('controls', {'pong': 10000}),
    v['shards'][0]['receiver'].__setitem__('activity_events', 0)), True)

raw, manifest = signed(base); manifest['report_sha256'] = 'b' * 64
assert not report.qualifies(base, manifest, raw), 'report hash mismatch'
"#;
    let status = Command::new("python3")
        .args(["-c", program])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()
        .expect("python3 starts");
    assert!(
        status.success(),
        "upstream qualification contract regression"
    );
}

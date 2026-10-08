use super::*;
use std::{
    io::Write,
    process::{Command, Stdio},
};

fn plan(venue: NativeVenue, index: usize) -> StreamPlan {
    StreamPlan {
        venue,
        index,
        targets: vec![NativeTarget {
            market: Arc::from(format!("m{index}")),
            asset: (venue == NativeVenue::Polymarket).then(|| Arc::from(format!("a{index}"))),
            amm: false,
        }],
        endpoint: "test".into(),
    }
}

#[test]
fn authored_terminal_report_renders_unqualified_in_python() {
    let plans = [
        plan(NativeVenue::Limitless, 0),
        plan(NativeVenue::Polymarket, 0),
    ];
    let shards: Vec<_> = plans
        .iter()
        .map(|plan| shard_state(plan, false))
        .map(|state| shard_report(&state.borrow()))
        .collect();
    let invocation = Invocation {
        selection: "selection.json".into(),
        output: "report.json".into(),
        min_seconds: 900,
        max_seconds: 900,
        min_events: 10_000,
        workers: 2,
        diagnostic: false,
        cpu_timing: true,
        control_socket: Some("control.sock".into()),
        snapshot: None,
        snapshot_seconds: None,
        serve: false,
        tape: false,
    };
    let cpu_timing = calibrate_cpu_clock().unwrap();
    let selection = Selection {
        limitless: vec![],
        polymarket: vec![],
    };
    let report = RunReport {
        schema: "pm-ws-native-upstream-v2",
        phase: "A",
        qualified: false,
        reason: "not_started",
        invocation: &invocation,
        receive_clock: "authored",
        cpu_timing: Some(&cpu_timing),
        measured_start_ns: 0,
        measured_end_ns: 0,
        stage_names: [
            "json_decode",
            "typed_validation",
            "admission_gate",
            "observer_audit",
            "receive_to_typed_handoff",
            "receive_to_audited_observation",
        ],
        gate_count_names: ["received", "admitted", "stale", "overload"],
        shards: shards.iter().collect(),
        capacities: capacities(2),
        consumed_selection: &selection,
        snapshot: None,
    };
    let encoded = serde_json::to_vec(&report).unwrap();
    let text = std::str::from_utf8(&encoded).unwrap();
    assert!(!text.contains("\"snapshot\""));
    assert!(!text.contains("\"snapshot_seconds\""));
    assert!(!text.contains("\"serve\""));
    assert!(!text.contains("\"recent\""));
    let root = env!("CARGO_MANIFEST_DIR");
    let script = format!(
        "import json,sys;sys.path.insert(0,{root:?}+'/bench');import report_upstream as r;x=r.parse(json.load(sys.stdin));o=r.render(x);assert 'not qualified' in o"
    );
    let mut child = Command::new("python3")
        .args(["-c", &script])
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(&encoded).unwrap();
    assert!(child.wait().unwrap().success());
}

#[test]
fn cpu_clock_calibration_reports_paired_read_wall_overhead() {
    let calibration = calibrate_cpu_clock().unwrap();
    println!("{}", serde_json::to_string(&calibration).unwrap());
    assert_eq!(calibration.calibration.paired_reads, 10_000);
    assert!(calibration.calibration.p95_ns <= calibration.calibration.p99_ns);
    assert!(calibration.calibration.p99_ns <= calibration.calibration.max_ns);
}

#[test]
fn invocation_accepts_four_hour_cap_and_rejects_one_second_over() {
    let args = |max_seconds: &str| {
        [
            "--selection".to_owned(),
            "selection.json".to_owned(),
            "--output".to_owned(),
            "report.json".to_owned(),
            "--max-seconds".to_owned(),
            max_seconds.to_owned(),
        ]
        .into_iter()
    };
    assert_eq!(
        Invocation::parse(args("14400")).unwrap().max_seconds,
        14_400
    );
    assert!(Invocation::parse(args("14401")).is_err());
    let cpu = Invocation::parse(
        [
            "--selection".to_owned(),
            "selection.json".to_owned(),
            "--output".to_owned(),
            "report.json".to_owned(),
            "--cpu-timing".to_owned(),
        ]
        .into_iter(),
    )
    .unwrap();
    assert!(cpu.cpu_timing);
}

#[test]
fn repeated_and_same_key_different_content_arrivals_all_publish_without_payload_metrics() {
    let context = test_context(vec![NativeTarget {
        market: Arc::from("m"),
        asset: Some(Arc::from("a")),
        amm: false,
    }]);
    for size in ["123456789", "987654321", "123456789"] {
        let input = format!(
            r#"{{"event_type":"book","market":"m","asset_id":"a","bids":[{{"price":"0.5","size":"{size}"}}],"asks":[],"timestamp":"1","hash":"h"}}"#
        );
        let received = context.stamp();
        let batch =
            crate::polymarket::native::decode_message(input.as_bytes(), context.source(received))
                .unwrap();
        assert_eq!(
            context.admit(
                batch,
                received,
                context.stamp(),
                StageStamp {
                    wall_ns: context.now_ns(),
                    cpu_ns: None
                },
            ),
            Ok(())
        );
    }
    let state = context.state.borrow();
    assert_eq!(state.receiver.events, 3);
    assert_eq!(state.receiver.sequence_errors, 0);
    assert_eq!(state.receiver.generation_errors, 0);
    let report = shard_report(&state);
    assert!(report.faults.is_empty());
    assert_eq!(report.gate_all, [3, 3, 0, 0]);
    let encoded = serde_json::to_string(&report).unwrap();
    assert!(!encoded.contains("123456789"));
    assert!(!encoded.contains("987654321"));
}

#[test]
fn context_rejects_old_generation_before_admission() {
    let context = test_context(vec![]);
    let received = StageStamp {
        wall_ns: 1,
        cpu_ns: None,
    };
    let mut source = context.source(received);
    source.generation = 0;
    assert_eq!(
        context.admit(
            NativeBatch {
                source,
                input_bytes: 1,
                events: vec![]
            },
            received,
            context.stamp(),
            StageStamp {
                wall_ns: context.now_ns(),
                cpu_ns: None
            },
        ),
        Err(PeerEnd::AdmissionFailed)
    );
}

#[test]
fn selected_resolution_is_delivered_and_invalidates_sampling_without_faking_feed_failure() {
    let context = test_context(vec![NativeTarget {
        market: Arc::from("selected"),
        asset: None,
        amm: false,
    }]);
    context.connected();
    context.subscription_sent();
    context.heartbeat_received();
    context.coverage("selected");
    for (market, expected) in [("unrelated", 0), ("selected", 1)] {
        let input = format!(
            r#"42/markets,["marketResolved",{{"slug":"{market}","type":"CLOB","winningOutcome":"YES","winningIndex":0,"resolutionDate":"2026-09-12"}}]"#
        );
        let frame = crate::wire::socketio::decode_limitless_frame(
            input.as_bytes(),
            crate::wire::socketio::WebSocketOpcode::Text,
            crate::wire::lexical::LexicalLimits::venue_payload(),
            crate::limitless::native::decimal_grammar(),
        )
        .unwrap();
        let received = context.stamp();
        let batch = crate::limitless::native::decode_native_frame(
            frame,
            context.source(received),
            input.len(),
        )
        .unwrap()
        .unwrap();
        context
            .admit(
                batch,
                received,
                context.stamp(),
                StageStamp {
                    wall_ns: context.now_ns(),
                    cpu_ns: None,
                },
            )
            .unwrap();
        let status = progress(std::slice::from_ref(&context.state));
        assert_eq!(status.resolved_targets, expected);
        assert_eq!(status.healthy_connections, 1);
        assert_eq!(status.faults, 0);
    }
    assert_eq!(context.state.borrow().receiver.events, 2);
}

#[test]
fn a_subscribed_outcome_keeps_the_complete_paired_price_change() {
    let context = test_context(vec![NativeTarget {
        market: Arc::from("m"),
        asset: Some(Arc::from("yes")),
        amm: false,
    }]);
    let input = br#"{"event_type":"price_change","market":"m","timestamp":"1","price_changes":[{"asset_id":"yes","price":"0.5","size":"2","side":"BUY","hash":"h1","best_bid":"0.5","best_ask":"0.6"},{"asset_id":"no","price":"0.5","size":"2","side":"SELL","hash":"h2","best_bid":"0.4","best_ask":"0.5"}]}"#;
    let received = context.stamp();
    let batch = crate::polymarket::native::decode_message(input, context.source(received)).unwrap();
    assert_eq!(batch.events[0].assets.len(), 2);
    assert_eq!(
        context.admit(
            batch,
            received,
            context.stamp(),
            StageStamp {
                wall_ns: context.now_ns(),
                cpu_ns: None
            },
        ),
        Ok(())
    );
    assert_eq!(context.state.borrow().receiver.events, 1);
}

#[test]
fn one_connection_can_own_many_markets_without_replicas() {
    let mut p = plan(NativeVenue::Limitless, 0);
    p.targets.push(NativeTarget {
        market: Arc::from("second"),
        asset: None,
        amm: false,
    });
    let state = shard_state(&p, false);
    assert_eq!(state.borrow().peers.len(), 1);
    assert_eq!(state.borrow().peers[0].coverage.len(), 2);
    assert_eq!(state.borrow().connections.len(), 1);
}

#[test]
fn unknown_market_has_one_existing_owner_and_paired_assets_stay_together() {
    for count in [1, 4] {
        let plans: Vec<_> = (0..count)
            .map(|i| plan(NativeVenue::Polymarket, i))
            .collect();
        let router = routing::AssignmentRouter::new(&plans);
        let desired: Vec<_> = ["yes", "no"]
            .into_iter()
            .map(|asset| demand::TargetRef {
                venue: "polymarket".into(),
                market: "new-market".into(),
                asset: Some(asset.into()),
                amm: false,
            })
            .collect();
        let assigned: Vec<_> = (0..count)
            .map(|i| router.desired(NativeVenue::Polymarket, i, &desired))
            .collect();
        assert_eq!(
            assigned
                .iter()
                .filter(|targets| !targets.is_empty())
                .count(),
            1
        );
        assert_eq!(assigned.iter().map(Vec::len).sum::<usize>(), 2);
    }
}

#[test]
fn quiet_polymarket_health_needs_heartbeat_and_sent_subscription_not_market_updates() {
    let context = test_context(vec![NativeTarget {
        market: Arc::from("m"),
        asset: Some(Arc::from("a")),
        amm: false,
    }]);
    context.connected();
    context.subscription_sent();
    assert_eq!(
        progress(std::slice::from_ref(&context.state)).healthy_connections,
        0
    );
    context.heartbeat_received();
    let health = progress(std::slice::from_ref(&context.state));
    assert_eq!(health.healthy_connections, 1);
    assert_eq!(health.ready_targets, 0);
    assert_eq!(context.accounting(), (0, 0, 0, 0));
}

#[test]
fn quiet_limitless_health_needs_acknowledged_targets_and_heartbeat() {
    let context = test_context(vec![NativeTarget {
        market: Arc::from("m"),
        asset: None,
        amm: false,
    }]);
    context.connected();
    context.subscription_sent();
    context.heartbeat_received();
    assert_eq!(
        progress(std::slice::from_ref(&context.state)).healthy_connections,
        0
    );
    context.coverage("m");
    assert_eq!(
        progress(std::slice::from_ref(&context.state)).healthy_connections,
        1
    );
    assert_eq!(context.accounting(), (0, 0, 0, 0));
}

#[test]
fn snapshot_and_serve_flags_default_off_and_bound_the_interval() {
    let args = |extra: &[&str]| {
        let mut values = vec![
            "--selection".to_owned(),
            "selection.json".to_owned(),
            "--output".to_owned(),
            "report.json".to_owned(),
        ];
        values.extend(extra.iter().map(|value| (*value).to_owned()));
        values.into_iter()
    };
    let plain = Invocation::parse(args(&[])).unwrap();
    assert!(plain.snapshot.is_none());
    assert!(plain.snapshot_seconds.is_none());
    assert!(!plain.serve);
    assert_eq!(plain.snapshot_interval(), 2);
    let enabled = Invocation::parse(args(&["--snapshot", "live.json"])).unwrap();
    assert_eq!(
        enabled.snapshot.as_deref(),
        Some(std::path::Path::new("live.json"))
    );
    assert_eq!(enabled.snapshot_interval(), 2);
    for seconds in ["1", "60"] {
        let bounded = Invocation::parse(args(&[
            "--snapshot",
            "live.json",
            "--snapshot-seconds",
            seconds,
        ]))
        .unwrap();
        assert_eq!(bounded.snapshot_interval(), seconds.parse::<u64>().unwrap());
    }
    for seconds in ["0", "61"] {
        assert!(Invocation::parse(args(&["--snapshot-seconds", seconds])).is_err());
    }
    assert!(Invocation::parse(args(&["--min-seconds", "5"])).is_err());
    let serve = Invocation::parse(args(&[
        "--serve",
        "--min-seconds",
        "5",
        "--min-events",
        "1",
    ]))
    .unwrap();
    assert!(serve.serve);
    assert!(serve.diagnostic);
    assert!(!serve.tape);
    let taped = Invocation::parse(args(&["--tape"])).unwrap();
    assert!(taped.tape);
    assert!(!taped.serve);
    assert!(!taped.diagnostic);
}

#[test]
fn recent_tape_keeps_the_last_admissions_and_drops_the_oldest_without_payload() {
    let context = test_context(vec![NativeTarget {
        market: Arc::from("m"),
        asset: Some(Arc::from("a")),
        amm: false,
    }]);
    context.state.borrow_mut().recent = Some(VecDeque::with_capacity(RECENT_CAPACITY));
    let input = br#"{"event_type":"book","market":"m","asset_id":"a","bids":[{"price":"0.5","size":"1"}],"asks":[],"timestamp":"1","hash":"h"}"#;
    let mut stamps = Vec::new();
    for _ in 0..RECENT_CAPACITY + 1 {
        let received = context.stamp();
        stamps.push(received.wall_ns);
        let batch =
            crate::polymarket::native::decode_message(input, context.source(received)).unwrap();
        context
            .admit(batch, received, context.stamp(), context.stamp())
            .unwrap();
    }
    let state = context.state.borrow();
    let tape = state.recent.as_ref().unwrap();
    assert_eq!(tape.len(), RECENT_CAPACITY);
    assert_eq!(tape.front().unwrap().received_ns, stamps[1]);
    let newest = tape.back().unwrap();
    assert_eq!(newest.received_ns, *stamps.last().unwrap());
    assert_eq!(newest.family, "book");
    assert_eq!(newest.market.as_deref(), Some("m"));
    assert_eq!(newest.events, 1);
    assert_eq!(newest.generation, 1);
    let report = shard_report(&state);
    assert_eq!(report.recent.len(), RECENT_CAPACITY);
    let encoded = serde_json::to_string(&report).unwrap();
    assert!(encoded.contains("\"recent\""));
    assert!(!encoded.contains("0.5"));
}

#[test]
fn interim_snapshot_renames_into_place_and_carries_its_liveness_envelope() {
    let plans = [
        plan(NativeVenue::Limitless, 0),
        plan(NativeVenue::Polymarket, 0),
    ];
    let shards: Vec<_> = plans
        .iter()
        .map(|plan| shard_state(plan, true))
        .map(|state| shard_report(&state.borrow()))
        .collect();
    let invocation = Invocation::parse(
        [
            "--selection",
            "selection.json",
            "--output",
            "report.json",
            "--serve",
            "--snapshot",
            "live.json",
            "--snapshot-seconds",
            "3",
        ]
        .into_iter()
        .map(str::to_owned),
    )
    .unwrap();
    let selection = Selection {
        limitless: vec![],
        polymarket: vec![],
    };
    let report = RunReport {
        schema: "pm-ws-native-upstream-v2",
        phase: "A",
        qualified: false,
        reason: "snapshot",
        invocation: &invocation,
        receive_clock: "authored",
        cpu_timing: None,
        measured_start_ns: 7,
        measured_end_ns: 0,
        stage_names: STAGE_NAMES,
        gate_count_names: GATE_COUNT_NAMES,
        shards: shards.iter().collect(),
        capacities: capacities(2),
        consumed_selection: &selection,
        snapshot: Some(SnapshotInfo {
            elapsed_ns: 11,
            serve: invocation.serve,
            interval_seconds: invocation.snapshot_interval(),
            tape_dropped: 3,
            window_opened_by: Some("timeout"),
        }),
    };
    let path = std::env::temp_dir().join(format!("pm-ws-snapshot-{}.json", std::process::id()));
    let _ = std::fs::remove_file(&path);
    write_snapshot(&path, &report).unwrap();
    let decoded: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let _ = std::fs::remove_file(&path);
    assert!(!path.with_extension("json.tmp").exists());
    assert_eq!(decoded["reason"], "snapshot");
    assert_eq!(decoded["qualified"], false);
    assert_eq!(decoded["measured_end_ns"], 0);
    assert_eq!(decoded["snapshot"]["elapsed_ns"], 11);
    assert_eq!(decoded["snapshot"]["serve"], true);
    assert_eq!(decoded["snapshot"]["interval_seconds"], 3);
    assert_eq!(decoded["snapshot"]["tape_dropped"], 3);
    assert_eq!(decoded["snapshot"]["window_opened_by"], "timeout");
    assert_eq!(decoded["invocation"]["serve"], true);
    assert_eq!(decoded["invocation"]["snapshot_seconds"], 3);
    assert_eq!(decoded["shards"].as_array().unwrap().len(), 2);
    assert!(decoded["shards"][0].get("recent").is_none());
}

#[test]
fn tape_text_truncation_cuts_on_a_character_boundary() {
    let short = r#"{"event_type":"book"}"#;
    assert_eq!(truncate_tape_frame(short.as_bytes()), short);
    let exact = "x".repeat(TAPE_TEXT_BYTES);
    assert_eq!(truncate_tape_frame(exact.as_bytes()), exact);
    let wide = format!("a{}", "\u{20ac}".repeat(300));
    assert_eq!(wide.len(), 901);
    let cut = truncate_tape_frame(wide.as_bytes());
    assert!(cut.ends_with('\u{2026}'));
    let kept = cut.len() - '\u{2026}'.len_utf8();
    assert_eq!(kept, 598);
    assert!(kept <= TAPE_TEXT_BYTES);
    assert_eq!(&cut[..kept], &wide[..kept]);
}

fn polymarket_tape_context(tape: TapeSender) -> ConnectionContext {
    let mut context = test_context(vec![NativeTarget {
        market: Arc::from("m"),
        asset: Some(Arc::from("a")),
        amm: false,
    }]);
    context.tape = Some(tape);
    context.state.borrow_mut().recent = Some(VecDeque::with_capacity(RECENT_CAPACITY));
    context
}

fn admit_tape_frame(context: &ConnectionContext, input: &str) {
    context.tape_frame(&tokio_tungstenite::tungstenite::Bytes::copy_from_slice(
        input.as_bytes(),
    ));
    let received = context.stamp();
    let batch =
        crate::polymarket::native::decode_message(input.as_bytes(), context.source(received))
            .unwrap();
    context
        .admit(batch, received, context.stamp(), context.stamp())
        .unwrap();
}

#[test]
fn tape_line_carries_the_schema_keys_and_the_truncated_frame_text() {
    let (lines, received) = std::sync::mpsc::sync_channel(4);
    let dropped = Arc::new(AtomicU64::new(0));
    let context = polymarket_tape_context(TapeSender {
        lines,
        dropped: dropped.clone(),
    });
    let input = format!(
        r#"{{"event_type":"book","market":"m","asset_id":"a","bids":[],"asks":[],"timestamp":"1","hash":"{}"}}"#,
        "\u{20ac}".repeat(300)
    );
    admit_tape_frame(&context, &input);
    let line = received.try_recv().unwrap();
    assert_eq!(dropped.load(Ordering::Relaxed), 0);
    let encoded = serde_json::to_value(&line).unwrap();
    let keys: std::collections::BTreeSet<&str> = encoded
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        [
            "t_ns",
            "venue",
            "stream",
            "family",
            "market",
            "events",
            "bytes",
            "handoff_ns",
            "generation",
            "text",
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<&str>>()
    );
    assert_eq!(encoded["venue"], "polymarket");
    assert_eq!(encoded["stream"], "test");
    assert_eq!(encoded["family"], "book");
    assert_eq!(encoded["market"], "m");
    assert_eq!(encoded["events"], 1);
    assert_eq!(encoded["bytes"], input.len());
    assert_eq!(encoded["generation"], 1);
    assert!(encoded["t_ns"].is_u64() && encoded["handoff_ns"].is_u64());
    let text = encoded["text"].as_str().unwrap();
    assert!(text.ends_with('\u{2026}'));
    assert!(text.starts_with(r#"{"event_type":"book","market":"m""#));
    assert_eq!(text.len(), TAPE_TEXT_BYTES + '\u{2026}'.len_utf8());
    let report = serde_json::to_string(&shard_report(&context.state.borrow())).unwrap();
    assert_eq!(
        context.state.borrow().recent.as_ref().unwrap().len(),
        1,
        "the relocated tape still witnesses the admission"
    );
    assert!(!report.contains('\u{20ac}'));
    assert!(!report.contains("\"text\""));
}

#[test]
fn a_full_tape_queue_drops_lines_and_never_blocks_admission() {
    let (lines, received) = std::sync::mpsc::sync_channel(1);
    let dropped = Arc::new(AtomicU64::new(0));
    let context = polymarket_tape_context(TapeSender {
        lines,
        dropped: dropped.clone(),
    });
    let input = r#"{"event_type":"book","market":"m","asset_id":"a","bids":[],"asks":[],"timestamp":"1","hash":"h"}"#;
    for _ in 0..3 {
        admit_tape_frame(&context, input);
    }
    assert_eq!(dropped.load(Ordering::Relaxed), 2);
    assert!(received.try_recv().is_ok());
    assert!(received.try_recv().is_err());
    assert_eq!(context.state.borrow().receiver.events, 3);
    assert_eq!(context.state.borrow().recent.as_ref().unwrap().len(), 3);
}

#[test]
fn target_health_lists_every_coverage_key_with_its_market_only_with_snapshots() {
    let limitless = StreamPlan {
        venue: NativeVenue::Limitless,
        index: 0,
        targets: (0..100)
            .map(|row| NativeTarget {
                market: Arc::from(format!("slug-{row}")),
                asset: None,
                amm: false,
            })
            .collect(),
        endpoint: "test".into(),
    };
    let polymarket = StreamPlan {
        venue: NativeVenue::Polymarket,
        index: 0,
        targets: (0..100)
            .flat_map(|row| {
                ["yes", "no"].map(|side| NativeTarget {
                    market: Arc::from(format!("condition-{row}")),
                    asset: Some(Arc::from(format!("token-{row}-{side}"))),
                    amm: false,
                })
            })
            .collect(),
        endpoint: "test".into(),
    };
    let states = [
        shard_state(&limitless, true),
        shard_state(&polymarket, true),
    ];
    states[0].borrow_mut().peers[0]
        .coverage
        .insert(Arc::from("slug-7"), true);
    let reports: Vec<_> = states
        .iter()
        .map(|state| shard_report(&state.borrow()))
        .collect();
    assert_eq!(reports[0].targets.len(), 100);
    assert_eq!(reports[1].targets.len(), 200);
    assert_eq!(
        reports
            .iter()
            .map(|report| report.targets.len())
            .sum::<usize>(),
        300
    );
    let evidenced = reports[0]
        .targets
        .iter()
        .find(|row| row.id == "slug-7")
        .unwrap();
    assert_eq!(evidenced.market, "slug-7");
    assert!(evidenced.covered);
    assert_eq!(
        reports[0].targets.iter().filter(|row| row.covered).count(),
        1
    );
    let keyed = reports[1]
        .targets
        .iter()
        .find(|row| row.id == "token-3-no")
        .unwrap();
    assert_eq!(keyed.market, "condition-3");
    assert!(!keyed.covered);
    assert_eq!(
        serde_json::to_value(&reports[1]).unwrap()["targets"]
            .as_array()
            .unwrap()
            .len(),
        200
    );
    let without = shard_report(&shard_state(&polymarket, false).borrow());
    assert!(without.targets.is_empty());
    assert!(
        serde_json::to_value(&without)
            .unwrap()
            .get("targets")
            .is_none()
    );
}

#[test]
fn the_measured_window_opens_on_readiness_and_only_in_serve_mode_on_timeout() {
    assert_eq!(window_open_cause(false, false, None, 10_000_000_000), None);
    assert_eq!(
        window_open_cause(false, true, Some(0), WINDOW_READINESS_HOLD_NS - 1),
        None
    );
    assert_eq!(
        window_open_cause(false, true, Some(0), WINDOW_READINESS_HOLD_NS),
        Some("readiness")
    );
    assert_eq!(
        window_open_cause(
            false,
            true,
            Some(now_ns_after_readiness()),
            SERVE_WINDOW_TIMEOUT_NS
        ),
        Some("readiness")
    );
    assert_eq!(
        window_open_cause(false, false, None, SERVE_WINDOW_TIMEOUT_NS * 10),
        None
    );
    assert_eq!(
        window_open_cause(true, false, None, SERVE_WINDOW_TIMEOUT_NS - 1),
        None
    );
    assert_eq!(
        window_open_cause(true, false, None, SERVE_WINDOW_TIMEOUT_NS),
        Some("timeout")
    );
    assert_eq!(
        window_open_cause(true, true, Some(0), WINDOW_READINESS_HOLD_NS),
        Some("readiness")
    );
    assert_eq!(
        window_open_cause(
            true,
            true,
            Some(SERVE_WINDOW_TIMEOUT_NS),
            SERVE_WINDOW_TIMEOUT_NS
        ),
        Some("timeout")
    );
}

fn now_ns_after_readiness() -> u64 {
    SERVE_WINDOW_TIMEOUT_NS - WINDOW_READINESS_HOLD_NS
}

#[test]
fn flags_off_terminal_report_carries_no_snapshot_recent_or_target_keys() {
    let plans = [
        plan(NativeVenue::Limitless, 0),
        plan(NativeVenue::Polymarket, 0),
    ];
    let shards: Vec<_> = plans
        .iter()
        .map(|plan| shard_state(plan, false))
        .map(|state| shard_report(&state.borrow()))
        .collect();
    let invocation = Invocation::parse(
        ["--selection", "selection.json", "--output", "report.json"]
            .into_iter()
            .map(str::to_owned),
    )
    .unwrap();
    let selection = Selection {
        limitless: vec![],
        polymarket: vec![],
    };
    let report = RunReport {
        schema: "pm-ws-native-upstream-v2",
        phase: "A",
        qualified: true,
        reason: "healthy_floors_reached",
        invocation: &invocation,
        receive_clock: "authored",
        cpu_timing: None,
        measured_start_ns: 1,
        measured_end_ns: 2,
        stage_names: STAGE_NAMES,
        gate_count_names: GATE_COUNT_NAMES,
        shards: shards.iter().collect(),
        capacities: capacities(2),
        consumed_selection: &selection,
        snapshot: None,
    };
    let encoded = serde_json::to_value(&report).unwrap();
    assert!(encoded.get("snapshot").is_none());
    for key in ["tape", "serve", "snapshot", "snapshot_seconds"] {
        assert!(encoded["invocation"].get(key).is_none());
    }
    for shard in encoded["shards"].as_array().unwrap() {
        assert!(shard.get("recent").is_none());
        assert!(shard.get("targets").is_none());
    }
}

fn selection_of(limitless: usize, polymarket: usize, end_epoch: u64) -> Selection {
    Selection {
        limitless: (0..limitless)
            .map(|index| LimitlessSelection {
                slug: format!("slug-{index}"),
                end_epoch: Some(serde_json::Number::from(end_epoch)),
            })
            .collect(),
        polymarket: (0..polymarket)
            .map(|index| PolymarketSelection {
                condition_id: format!("condition-{index}"),
                clob_token_ids: vec![format!("{index}-yes"), format!("{index}-no")],
                end_epoch: Some(serde_json::Number::from(end_epoch)),
            })
            .collect(),
    }
}

fn selection_invocation(serve: bool) -> Invocation {
    Invocation {
        selection: "selection.json".into(),
        output: "report.json".into(),
        min_seconds: 900,
        max_seconds: 7_200,
        min_events: 10_000,
        workers: 2,
        diagnostic: serve,
        cpu_timing: false,
        control_socket: None,
        snapshot: None,
        snapshot_seconds: None,
        serve,
        tape: false,
    }
}

fn seconds_from_now(offset: u64) -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + offset
}

fn refusal(selection: Selection, invocation: &Invocation) -> String {
    match plans(selection, invocation) {
        Ok(accepted) => panic!("expected a refusal, planned {} streams", accepted.len()),
        Err(message) => message,
    }
}

fn shape(plans: &[StreamPlan]) -> Vec<(&'static str, usize, usize)> {
    plans
        .iter()
        .map(|plan| (venue_name(plan.venue), plan.index, plan.targets.len()))
        .collect()
}

#[test]
fn a_small_selection_plans_one_connection_per_venue() {
    let plans = plans(
        selection_of(3, 5, seconds_from_now(30_000)),
        &selection_invocation(false),
    )
    .expect("a small selection is accepted");
    assert_eq!(
        shape(&plans),
        vec![("limitless", 0, 3), ("polymarket", 0, 10)]
    );
}

#[test]
fn rows_beyond_one_connection_chunk_in_file_order() {
    let plans = plans(
        selection_of(250, 0, seconds_from_now(30_000)),
        &selection_invocation(false),
    )
    .expect("250 Limitless rows are accepted");
    assert_eq!(
        shape(&plans),
        vec![
            ("limitless", 0, 100),
            ("limitless", 1, 100),
            ("limitless", 2, 50)
        ]
    );
    assert_eq!(plans[0].targets[0].market.as_ref(), "slug-0");
    assert_eq!(plans[2].targets[49].market.as_ref(), "slug-249");
}

#[test]
fn one_venue_alone_is_a_complete_selection() {
    let plans = plans(
        selection_of(0, 100, seconds_from_now(30_000)),
        &selection_invocation(false),
    )
    .expect("a Polymarket-only selection is accepted");
    assert_eq!(shape(&plans), vec![("polymarket", 0, 200)]);
    assert_eq!(
        refusal(
            selection_of(0, 0, seconds_from_now(30_000)),
            &selection_invocation(false)
        ),
        "the selection names no market"
    );
}

#[test]
fn the_selection_target_bound_is_refused_before_any_connection() {
    let far = seconds_from_now(30_000);
    assert_eq!(
        plans(selection_of(4_096, 0, far), &selection_invocation(false))
            .expect("4096 targets are accepted")
            .len(),
        41
    );
    let refused = refusal(selection_of(4_097, 0, far), &selection_invocation(false));
    assert!(
        refused.contains("4097 targets") && refused.contains("4096"),
        "{refused}"
    );
    let conditions = refusal(selection_of(0, 2_049, far), &selection_invocation(false));
    assert!(conditions.contains("4098 targets"), "{conditions}");
}

#[test]
fn readiness_totals_come_from_the_plans() {
    for (limitless, polymarket, targets, connections) in
        [(100, 100, 300, 2), (500, 1_000, 2_500, 15), (3, 0, 3, 1)]
    {
        let plans = plans(
            selection_of(limitless, polymarket, seconds_from_now(30_000)),
            &selection_invocation(false),
        )
        .expect("the selection is accepted");
        let planned_targets: usize = plans.iter().map(|plan| plan.targets.len()).sum();
        assert_eq!((planned_targets, plans.len()), (targets, connections));
        let states: Vec<_> = plans.iter().map(|plan| shard_state(plan, false)).collect();
        let observed = progress(&states);
        assert_eq!(observed.required_targets, planned_targets);
        assert_eq!(observed.required_connections, plans.len());
        assert_eq!(observed.healthy_connections, 0);
    }
}

#[test]
fn a_short_lived_market_is_selectable_only_for_a_served_run() {
    let soon = selection_of(1, 1, seconds_from_now(300));
    let measured = refusal(soon.clone(), &selection_invocation(false));
    assert!(
        measured.contains("insufficient remaining lifetime"),
        "{measured}"
    );
    assert_eq!(
        shape(&plans(soon, &selection_invocation(true)).expect("a served run accepts it")),
        vec![("limitless", 0, 1), ("polymarket", 0, 2)]
    );
    let expired = selection_of(1, 0, seconds_from_now(0) - 1);
    assert!(
        refusal(expired, &selection_invocation(true)).contains("insufficient remaining lifetime")
    );
}

#[test]
fn a_limitless_peer_with_a_partial_acknowledgement_counts_healthy_only_on_evidence() {
    let plan = StreamPlan {
        venue: NativeVenue::Limitless,
        index: 0,
        targets: ["live", "resolved"]
            .map(|market| NativeTarget {
                market: Arc::from(market),
                asset: None,
                amm: false,
            })
            .to_vec(),
        endpoint: "test".into(),
    };
    let state = shard_state(&plan, false);
    {
        let mut borrowed = state.borrow_mut();
        let peer = &mut borrowed.peers[0];
        peer.connected = true;
        peer.subscription_sent = true;
        peer.last_heartbeat_ns = Some(1);
    }
    let states = vec![state.clone()];
    assert_eq!(progress(&states).healthy_connections, 0);
    state.borrow_mut().peers[0].partial_ack = true;
    assert_eq!(
        progress(&states).healthy_connections,
        0,
        "a partial acknowledgement without one covered market is no evidence"
    );
    state.borrow_mut().peers[0]
        .coverage
        .insert(Arc::from("live"), true);
    let observed = progress(&states);
    assert_eq!(
        (
            observed.healthy_connections,
            observed.ready_targets,
            observed.required_targets
        ),
        (1, 1, 2)
    );
    state.borrow_mut().peers[0].partial_ack = false;
    assert_eq!(progress(&states).healthy_connections, 0);
}

#[test]
fn every_planned_target_keeps_its_planned_shard_through_the_router() {
    for (limitless, polymarket, total) in
        [(500, 1_000, 2_500), (0, 2_048, 4_096), (4_096, 0, 4_096)]
    {
        let plans = plans(
            selection_of(limitless, polymarket, seconds_from_now(30_000)),
            &selection_invocation(true),
        )
        .expect("the selection is accepted");
        let router = routing::AssignmentRouter::new(&plans);
        let initial = router.initial();
        let mut routed = Vec::new();
        for plan in &plans {
            let mut planned = plan.targets.clone();
            planned.sort();
            assert_eq!(
                router.desired(plan.venue, plan.index, &initial),
                planned,
                "shard {} of {} keeps exactly its planned targets",
                plan.index,
                venue_name(plan.venue)
            );
            routed.extend(planned);
        }
        let mut selected: Vec<_> = plans
            .iter()
            .flat_map(|plan| plan.targets.iter().cloned())
            .collect();
        selected.sort();
        routed.sort();
        assert_eq!(routed, selected);
        assert_eq!(routed.len(), total);
        routed.dedup();
        assert_eq!(routed.len(), total, "no target is routed to two shards");
    }
}

#[test]
fn tape_frame_truncation_validates_only_the_bounded_prefix() {
    let mut binary = vec![b'a'; TAPE_TEXT_BYTES];
    binary.extend_from_slice(&[0xff, 0xfe]);
    let cut = truncate_tape_frame(&binary);
    assert_eq!(cut.len(), TAPE_TEXT_BYTES + '\u{2026}'.len_utf8());
    assert!(cut.ends_with('\u{2026}'));
    let mut mid = b"ab".to_vec();
    mid.push(0xff);
    mid.extend_from_slice(b"cd");
    assert_eq!(truncate_tape_frame(&mid), "ab\u{2026}");
    assert_eq!(truncate_tape_frame(b"plain"), "plain");
    assert_eq!(truncate_tape_frame(b""), "");
}

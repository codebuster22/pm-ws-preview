use pm_ws::{
    DecimalGrammar,
    native::{
        NativeBatch, NativeEvent, NativeFamily, NativeIdentity, NativePayload, NativeSource,
        NativeVenue,
        document::NativeDocument,
        gate::{GateDecision, GateFault, GateLimits, SingleSourceGate},
    },
    wire::lexical::LexicalLimits,
};
use std::sync::Arc;

fn source(id: &str, generation: u64) -> NativeSource {
    NativeSource {
        stream: Arc::from("stream"),
        source: Arc::from(id),
        slot: 0,
        generation,
        stream_generation: generation,
        sequence: 1,
        received_ns: 1,
        validated_ns: 2,
    }
}
fn event(id: &str, value: &str, index: usize) -> NativeEvent {
    let payload = NativePayload::from_document(
        NativeDocument::parse(
            format!(r#"{{"price":{value}}}"#).as_bytes(),
            LexicalLimits::venue_payload(),
            DecimalGrammar::new(8, 20, false, false).unwrap(),
        )
        .unwrap(),
    );
    NativeEvent {
        venue: NativeVenue::Polymarket,
        family: NativeFamily::PolymarketBook,
        family_name: None,
        market: Some(Arc::from("m")),
        assets: vec![Arc::from("a")],
        identity: Some(NativeIdentity::Components(vec![
            Arc::from(id),
            Arc::from("stamp"),
        ])),
        payload,
        member_index: index,
    }
}
fn batch(source_id: &str, generation: u64, events: Vec<NativeEvent>) -> NativeBatch {
    NativeBatch {
        source: source(source_id, generation),
        input_bytes: 32,
        events,
    }
}

#[test]
fn identical_arrivals_are_each_admitted() {
    let mut gate = SingleSourceGate::new(GateLimits::default(), 1);
    let first = batch("one", 1, vec![event("same", "1", 0)]);
    let repeated = batch("one", 1, vec![event("same", "1", 0)]);
    assert!(matches!(gate.admit(first), GateDecision::Admit(_)));
    assert!(matches!(gate.admit(repeated), GateDecision::Admit(_)));
    assert_eq!(gate.counters().admitted, 2);
}

#[test]
fn same_identity_with_different_source_value_is_admitted() {
    let mut gate = SingleSourceGate::new(GateLimits::default(), 1);
    assert!(matches!(
        gate.admit(batch("one", 1, vec![event("same", "1", 0)])),
        GateDecision::Admit(_)
    ));
    assert!(matches!(
        gate.admit(batch("one", 1, vec![event("same", "2", 0)])),
        GateDecision::Admit(_)
    ));
    assert_eq!(gate.counters().admitted, 2);
    assert_eq!(gate.counters().received, 2);
}

#[test]
fn stale_generation_is_rejected_without_publication() {
    let mut gate = SingleSourceGate::new(GateLimits::default(), 2);
    assert!(matches!(
        gate.admit(batch("one", 1, vec![event("a", "1", 0)])),
        GateDecision::Fault(GateFault::StaleSource)
    ));
    assert_eq!(gate.counters().stale, 1);
    assert_eq!(gate.counters().admitted, 0);
}

#[test]
fn invalid_or_oversized_member_rejects_the_entire_batch() {
    let mut gate = SingleSourceGate::new(GateLimits::default(), 1);
    let mut invalid = event("b", "2", 1);
    invalid.identity = None;
    assert!(matches!(
        gate.admit(batch("one", 1, vec![event("a", "1", 0), invalid])),
        GateDecision::Fault(GateFault::InvalidEvent)
    ));
    assert_eq!(gate.counters().admitted, 0);
    let mut gate = SingleSourceGate::new(
        GateLimits {
            bytes: 256,
            ..GateLimits::default()
        },
        1,
    );
    let mut oversized = event("big", "1", 0);
    oversized.identity = Some(NativeIdentity::Components(vec![
        Arc::from("x".repeat(1024)),
        Arc::from("stamp"),
    ]));
    assert!(matches!(
        gate.admit(batch("one", 1, vec![oversized])),
        GateDecision::Fault(GateFault::Capacity)
    ));
    assert_eq!(gate.counters().admitted, 0);
}

#[test]
fn retained_document_storage_rejects_the_entire_batch() {
    let document = NativeDocument::parse(
        format!(r#"{{"text":"{}"}}"#, "x".repeat(8192)).as_bytes(),
        LexicalLimits {
            max_string_bytes: 8192,
            ..LexicalLimits::venue_payload()
        },
        DecimalGrammar::new(18, 30, false, false).unwrap(),
    )
    .unwrap();
    assert!(document.memory_bytes() >= 8192);
    let mut oversized = event("big", "1", 1);
    oversized.payload = NativePayload::from_document(document);
    let mut gate = SingleSourceGate::new(
        GateLimits {
            bytes: 4096,
            ..GateLimits::default()
        },
        1,
    );
    assert!(matches!(
        gate.admit(batch("one", 1, vec![event("small", "1", 0), oversized])),
        GateDecision::Fault(GateFault::Capacity)
    ));
    assert_eq!(gate.counters().admitted, 0);
}

#[test]
fn shared_document_charged_once_but_distinct_documents_each_count() {
    let make_payload = || {
        NativePayload::from_document(
            NativeDocument::parse(
                format!(r#"{{"text":"{}"}}"#, "x".repeat(3000)).as_bytes(),
                LexicalLimits::venue_payload(),
                DecimalGrammar::new(18, 30, false, false).unwrap(),
            )
            .unwrap(),
        )
    };
    let mut first = event("one", "1", 0);
    first.payload = make_payload();
    let mut second = event("two", "1", 1);
    second.payload = first.payload.clone();
    let shared_batch = batch("one", 1, vec![first.clone(), second.clone()]);
    let minimum = (1..100_000)
        .find(|bytes| {
            matches!(
                SingleSourceGate::new(
                    GateLimits {
                        bytes: *bytes,
                        ..GateLimits::default()
                    },
                    1
                )
                .admit(shared_batch.clone()),
                GateDecision::Admit(_)
            )
        })
        .unwrap();
    second.payload = make_payload();
    let mut gate = SingleSourceGate::new(
        GateLimits {
            bytes: minimum,
            ..GateLimits::default()
        },
        1,
    );
    assert!(matches!(
        gate.admit(batch("one", 1, vec![first, second])),
        GateDecision::Fault(GateFault::Capacity)
    ));
}

#[test]
fn subtree_limits_apply_to_every_member_of_a_shared_document() {
    let batch = pm_ws::polymarket::native::decode_message(
        br#"[{"event_type":"future"},{"event_type":"future","nested":[[1]]}]"#,
        source("one", 1),
    )
    .unwrap();
    let mut gate = SingleSourceGate::new(
        GateLimits {
            max_value_depth: 2,
            ..GateLimits::default()
        },
        1,
    );
    assert!(matches!(
        gate.admit(batch),
        GateDecision::Fault(GateFault::Capacity)
    ));
    assert_eq!(gate.counters().admitted, 0);
}

#[test]
fn unused_event_vector_capacity_is_retained_memory() {
    let mut events = Vec::with_capacity(128);
    events.push(event("one", "1", 0));
    let mut gate = SingleSourceGate::new(
        GateLimits {
            bytes: 4096,
            ..GateLimits::default()
        },
        1,
    );
    assert!(matches!(
        gate.admit(batch("one", 1, events)),
        GateDecision::Fault(GateFault::Capacity)
    ));
    assert_eq!(gate.counters().admitted, 0);
}

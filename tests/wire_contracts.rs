use pm_ws::limitless::{LimitlessEvent, decode_event};
use pm_ws::wire::lexical::{LexicalError, LexicalKind, LexicalLimits, LexicalValue, parse_lexical};
use pm_ws::wire::socketio::{DecodedFrame, FrameError, WebSocketOpcode, decode_frame};
use pm_ws::*;

const BINARY_ATTACHMENT_EVENT: &str =
    r#"451-/markets,["orderbookUpdate",{"_placeholder":true,"num":0}]"#;
const ENGINEIO_CLIENT_PONG: &str = r#"3"#;
const ENGINEIO_CLOSE: &str = r#"1"#;
const ENGINEIO_OPEN: &str = r#"0{"sid":"REPLAY-SESSION-0001","upgrades":[],"pingInterval":25000,"pingTimeout":20000,"maxPayload":1000000}"#;
const ENGINEIO_SERVER_PING: &str = r#"2"#;
const MARKET_CREATED_LIFECYCLE_ROOM: &str = r#"42/markets,["marketCreated",{"slug":"example-market-slug-two","title":"Example replay market","type":"CLOB","groupSlug":"example-market-group","categoryIds":[1,5],"createdAt":"2026-08-26T11:50:00.000Z"}]"#;
const MARKET_RESOLVED_LIFECYCLE_ROOM: &str = r#"42/markets,["marketResolved",{"slug":"example-market-slug","type":"CLOB","winningOutcome":"YES","winningIndex":0,"resolutionDate":"2026-08-26T12:10:00.000Z"}]"#;
const MARKET_RESOLVED_MARKET_ROOM: &str = r#"42/markets,["marketResolved",{"slug":"example-market-slug","type":"CLOB","winningOutcome":"YES","winningIndex":0,"resolutionDate":"2026-08-26T12:10:00.000Z"}]"#;
const ORDERBOOK_UPDATE_EMPTY_BOOK: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"example-market-slug","orderbook":{"bids":[],"asks":[]},"timestamp":"2026-08-26T12:00:15.000Z"}]"#;
const ORDERBOOK_UPDATE_EXCESS_PRECISION: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"example-market-slug","orderbook":{"bids":[{"price":0.5123456789012,"size":120}],"asks":[{"price":0.52,"size":75}]},"timestamp":"2026-08-26T12:00:30.000Z"}]"#;
const ORDERBOOK_UPDATE_EXPONENT_AND_TRAILING_ZERO_LEXEMES: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"example-market-slug","orderbook":{"bids":[{"price":5.1E-1,"size":1.20E2}],"asks":[{"price":0.520000,"size":75.000}]},"timestamp":"2026-08-26T12:00:25.000Z"}]"#;
const ORDERBOOK_UPDATE_FULL_BOOK: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"example-market-slug","orderbook":{"bids":[{"price":0.51,"size":120},{"price":0.50,"size":340.5}],"asks":[{"price":0.52,"size":75},{"price":0.53,"size":10}]},"timestamp":"2026-08-26T12:00:00.000Z"}]"#;
const ORDERBOOK_UPDATE_MALFORMED_JSON: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"example-market-slug","orderbook":{"bids":[{"price":0.51,"size":120}"#;
const ORDERBOOK_UPDATE_MIRRORED_COMPLEMENT: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"example-market-slug","orderbook":{"bids":[{"price":0.48,"size":75},{"price":0.47,"size":10}],"asks":[{"price":0.49,"size":120},{"price":0.50,"size":340.5}]},"timestamp":"2026-08-26T12:00:00.000Z"}]"#;
const ORDERBOOK_UPDATE_OVERSIZED_LEVELS: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"example-market-slug","orderbook":{"bids":[{"price":0.51,"size":1},{"price":0.50,"size":2},{"price":0.49,"size":3},{"price":0.48,"size":4}],"asks":[{"price":0.52,"size":5}]},"timestamp":"2026-08-26T12:00:40.000Z"}]"#;
const ORDERBOOK_UPDATE_SECOND_REVISION: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"example-market-slug","orderbook":{"bids":[{"price":0.51,"size":95},{"price":0.49,"size":200}],"asks":[{"price":0.52,"size":75},{"price":0.54,"size":40}]},"timestamp":"2026-08-26T12:00:05.000Z"}]"#;
const ORDERBOOK_UPDATE_STANDBY_SKEW: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"example-market-slug","orderbook":{"bids":[{"price":0.51,"size":120},{"price":0.50,"size":340.5}],"asks":[{"price":0.52,"size":75},{"price":0.53,"size":10}]},"timestamp":"2026-08-26T11:59:59.000Z"}]"#;
const ORDERBOOK_UPDATE_UNORDERED_BIDS: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"example-market-slug","orderbook":{"bids":[{"price":0.50,"size":340.5},{"price":0.51,"size":120}],"asks":[{"price":0.52,"size":75}]},"timestamp":"2026-08-26T12:00:35.000Z"}]"#;
const ORDERBOOK_UPDATE_WITH_TOKEN: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"example-market-slug-two","orderbook":{"tokenId":"7451820993000000000000000000000000000001","bids":[{"price":0.40,"size":10}],"asks":[{"price":0.41,"size":12}]},"timestamp":"2026-08-26T12:00:10.000Z"}]"#;
const ORDERBOOK_UPDATE_ZERO_SIZE_LEVEL: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"example-market-slug","orderbook":{"bids":[{"price":0.51,"size":0}],"asks":[{"price":0.52,"size":75}]},"timestamp":"2026-08-26T12:00:20.000Z"}]"#;
const SOCKETIO_NAMESPACE_CONNECT_ACK: &str = r#"40/markets,{"sid":"REPLAY-NAMESPACE-0001"}"#;
const SOCKETIO_NAMESPACE_CONNECT_ERROR: &str = r#"44/markets,{"message":"namespace unavailable"}"#;
const SOCKETIO_NAMESPACE_CONNECT_REQUEST: &str = r#"40/markets,"#;
const SUBSCRIBE_MARKET_PRICES_COMMAND: &str = r#"42/markets,["subscribe_market_prices",{"marketSlugs":["example-market-slug","example-market-slug-two"]}]"#;
const UNSUPPORTED_FUTURE_EVENT: &str =
    r#"42/markets,["orderbookDepthDelta",{"marketSlug":"example-market-slug","changes":[]}]"#;
const WEBSOCKET_PING_CONTROL_FRAME: &str = r#"replay-probe"#;

type FrameCase = (&'static str, &'static str, WebSocketOpcode, &'static str);

const FRAME_CASES: &[FrameCase] = &[
    (
        "binary-attachment-event",
        BINARY_ATTACHMENT_EVENT,
        WebSocketOpcode::Text,
        "Ok engine_io=Some(Message) socket_io=Some(BinaryEvent) ns=Some(\"/markets\") ack=None attachments=1 event=Some(\"orderbookUpdate\") payload_kind=Some(Object) extra=0",
    ),
    (
        "engineio-client-pong",
        ENGINEIO_CLIENT_PONG,
        WebSocketOpcode::Text,
        "Ok engine_io=Some(Pong) socket_io=None ns=None ack=None attachments=0 event=None payload_kind=None extra=0",
    ),
    (
        "engineio-close",
        ENGINEIO_CLOSE,
        WebSocketOpcode::Text,
        "Ok engine_io=Some(Close) socket_io=None ns=None ack=None attachments=0 event=None payload_kind=None extra=0",
    ),
    (
        "engineio-open",
        ENGINEIO_OPEN,
        WebSocketOpcode::Text,
        "Ok engine_io=Some(Open) socket_io=None ns=None ack=None attachments=0 event=None payload_kind=Some(Object) extra=0",
    ),
    (
        "engineio-server-ping",
        ENGINEIO_SERVER_PING,
        WebSocketOpcode::Text,
        "Ok engine_io=Some(Ping) socket_io=None ns=None ack=None attachments=0 event=None payload_kind=None extra=0",
    ),
    (
        "market-created-lifecycle-room",
        MARKET_CREATED_LIFECYCLE_ROOM,
        WebSocketOpcode::Text,
        "Ok engine_io=Some(Message) socket_io=Some(Event) ns=Some(\"/markets\") ack=None attachments=0 event=Some(\"marketCreated\") payload_kind=Some(Object) extra=0",
    ),
    (
        "market-resolved-lifecycle-room",
        MARKET_RESOLVED_LIFECYCLE_ROOM,
        WebSocketOpcode::Text,
        "Ok engine_io=Some(Message) socket_io=Some(Event) ns=Some(\"/markets\") ack=None attachments=0 event=Some(\"marketResolved\") payload_kind=Some(Object) extra=0",
    ),
    (
        "market-resolved-market-room",
        MARKET_RESOLVED_MARKET_ROOM,
        WebSocketOpcode::Text,
        "Ok engine_io=Some(Message) socket_io=Some(Event) ns=Some(\"/markets\") ack=None attachments=0 event=Some(\"marketResolved\") payload_kind=Some(Object) extra=0",
    ),
    (
        "orderbook-update-empty-book",
        ORDERBOOK_UPDATE_EMPTY_BOOK,
        WebSocketOpcode::Text,
        "Ok engine_io=Some(Message) socket_io=Some(Event) ns=Some(\"/markets\") ack=None attachments=0 event=Some(\"orderbookUpdate\") payload_kind=Some(Object) extra=0",
    ),
    (
        "orderbook-update-excess-precision",
        ORDERBOOK_UPDATE_EXCESS_PRECISION,
        WebSocketOpcode::Text,
        "Ok engine_io=Some(Message) socket_io=Some(Event) ns=Some(\"/markets\") ack=None attachments=0 event=Some(\"orderbookUpdate\") payload_kind=Some(Object) extra=0",
    ),
    (
        "orderbook-update-exponent-and-trailing-zero-lexemes",
        ORDERBOOK_UPDATE_EXPONENT_AND_TRAILING_ZERO_LEXEMES,
        WebSocketOpcode::Text,
        "Ok engine_io=Some(Message) socket_io=Some(Event) ns=Some(\"/markets\") ack=None attachments=0 event=Some(\"orderbookUpdate\") payload_kind=Some(Object) extra=0",
    ),
    (
        "orderbook-update-full-book",
        ORDERBOOK_UPDATE_FULL_BOOK,
        WebSocketOpcode::Text,
        "Ok engine_io=Some(Message) socket_io=Some(Event) ns=Some(\"/markets\") ack=None attachments=0 event=Some(\"orderbookUpdate\") payload_kind=Some(Object) extra=0",
    ),
    (
        "orderbook-update-malformed-json",
        ORDERBOOK_UPDATE_MALFORMED_JSON,
        WebSocketOpcode::Text,
        "Err Lexical(UnexpectedEnd { offset: 101 })",
    ),
    (
        "orderbook-update-mirrored-complement",
        ORDERBOOK_UPDATE_MIRRORED_COMPLEMENT,
        WebSocketOpcode::Text,
        "Ok engine_io=Some(Message) socket_io=Some(Event) ns=Some(\"/markets\") ack=None attachments=0 event=Some(\"orderbookUpdate\") payload_kind=Some(Object) extra=0",
    ),
    (
        "orderbook-update-oversized-levels",
        ORDERBOOK_UPDATE_OVERSIZED_LEVELS,
        WebSocketOpcode::Text,
        "Ok engine_io=Some(Message) socket_io=Some(Event) ns=Some(\"/markets\") ack=None attachments=0 event=Some(\"orderbookUpdate\") payload_kind=Some(Object) extra=0",
    ),
    (
        "orderbook-update-second-revision",
        ORDERBOOK_UPDATE_SECOND_REVISION,
        WebSocketOpcode::Text,
        "Ok engine_io=Some(Message) socket_io=Some(Event) ns=Some(\"/markets\") ack=None attachments=0 event=Some(\"orderbookUpdate\") payload_kind=Some(Object) extra=0",
    ),
    (
        "orderbook-update-standby-skew",
        ORDERBOOK_UPDATE_STANDBY_SKEW,
        WebSocketOpcode::Text,
        "Ok engine_io=Some(Message) socket_io=Some(Event) ns=Some(\"/markets\") ack=None attachments=0 event=Some(\"orderbookUpdate\") payload_kind=Some(Object) extra=0",
    ),
    (
        "orderbook-update-unordered-bids",
        ORDERBOOK_UPDATE_UNORDERED_BIDS,
        WebSocketOpcode::Text,
        "Ok engine_io=Some(Message) socket_io=Some(Event) ns=Some(\"/markets\") ack=None attachments=0 event=Some(\"orderbookUpdate\") payload_kind=Some(Object) extra=0",
    ),
    (
        "orderbook-update-with-token",
        ORDERBOOK_UPDATE_WITH_TOKEN,
        WebSocketOpcode::Text,
        "Ok engine_io=Some(Message) socket_io=Some(Event) ns=Some(\"/markets\") ack=None attachments=0 event=Some(\"orderbookUpdate\") payload_kind=Some(Object) extra=0",
    ),
    (
        "orderbook-update-zero-size-level",
        ORDERBOOK_UPDATE_ZERO_SIZE_LEVEL,
        WebSocketOpcode::Text,
        "Ok engine_io=Some(Message) socket_io=Some(Event) ns=Some(\"/markets\") ack=None attachments=0 event=Some(\"orderbookUpdate\") payload_kind=Some(Object) extra=0",
    ),
    (
        "socketio-namespace-connect-ack",
        SOCKETIO_NAMESPACE_CONNECT_ACK,
        WebSocketOpcode::Text,
        "Ok engine_io=Some(Message) socket_io=Some(Connect) ns=Some(\"/markets\") ack=None attachments=0 event=None payload_kind=Some(Object) extra=0",
    ),
    (
        "socketio-namespace-connect-error",
        SOCKETIO_NAMESPACE_CONNECT_ERROR,
        WebSocketOpcode::Text,
        "Ok engine_io=Some(Message) socket_io=Some(ConnectError) ns=Some(\"/markets\") ack=None attachments=0 event=None payload_kind=Some(Object) extra=0",
    ),
    (
        "socketio-namespace-connect-request",
        SOCKETIO_NAMESPACE_CONNECT_REQUEST,
        WebSocketOpcode::Text,
        "Ok engine_io=Some(Message) socket_io=Some(Connect) ns=Some(\"/markets\") ack=None attachments=0 event=None payload_kind=None extra=0",
    ),
    (
        "subscribe-market-prices-command",
        SUBSCRIBE_MARKET_PRICES_COMMAND,
        WebSocketOpcode::Text,
        "Ok engine_io=Some(Message) socket_io=Some(Event) ns=Some(\"/markets\") ack=None attachments=0 event=Some(\"subscribe_market_prices\") payload_kind=Some(Object) extra=0",
    ),
    (
        "unsupported-future-event",
        UNSUPPORTED_FUTURE_EVENT,
        WebSocketOpcode::Text,
        "Ok engine_io=Some(Message) socket_io=Some(Event) ns=Some(\"/markets\") ack=None attachments=0 event=Some(\"orderbookDepthDelta\") payload_kind=Some(Object) extra=0",
    ),
    (
        "websocket-ping-control-frame",
        WEBSOCKET_PING_CONTROL_FRAME,
        WebSocketOpcode::Ping,
        "Ok engine_io=None socket_io=None ns=None ack=None attachments=0 event=None payload_kind=None extra=0",
    ),
];

fn summarize(outcome: &Result<DecodedFrame, FrameError>) -> String {
    match outcome {
        Ok(frame) => format!(
            "Ok engine_io={:?} socket_io={:?} ns={:?} ack={:?} attachments={} event={:?} payload_kind={:?} extra={}",
            frame.engine_io(),
            frame.socket_io(),
            frame.namespace(),
            frame.acknowledgment_id(),
            frame.binary_attachments(),
            frame.event_name(),
            frame.payload().map(LexicalValue::kind),
            frame.extra_arguments().len(),
        ),
        Err(error) => format!("Err {error:?}"),
    }
}

#[test]
fn every_recorded_frame_decodes_to_its_pinned_classification() {
    for (name, frame, opcode, expected) in FRAME_CASES {
        let outcome = decode_frame(frame.as_bytes(), *opcode, LexicalLimits::venue_payload());
        assert_eq!(&summarize(&outcome), expected, "frame {name} reclassified");
    }
}

#[test]
fn a_truncated_event_payload_is_reported_as_a_lexical_failure() {
    let outcome = decode_frame(
        ORDERBOOK_UPDATE_MALFORMED_JSON.as_bytes(),
        WebSocketOpcode::Text,
        LexicalLimits::venue_payload(),
    );
    assert!(matches!(
        outcome,
        Err(FrameError::Lexical(LexicalError::UnexpectedEnd { .. }))
    ));
}

#[test]
fn a_websocket_control_frame_carries_no_application_payload() {
    let frame = decode_frame(
        WEBSOCKET_PING_CONTROL_FRAME.as_bytes(),
        WebSocketOpcode::Ping,
        LexicalLimits::venue_payload(),
    )
    .expect("control frames decode to an empty envelope");
    assert_eq!(frame.engine_io(), None);
    assert_eq!(frame.socket_io(), None);
    assert_eq!(frame.payload(), None);
}

#[test]
fn lexical_parser_preserves_exact_number_lexemes() {
    let value = parse_lexical(
        br#"{"price":"unused","levels":[0.0100,1E-2,12345678901234567890,-0.5,0]}"#,
        LexicalLimits::venue_payload(),
    )
    .expect("valid payload");
    let levels = value
        .path("levels")
        .and_then(LexicalValue::as_array)
        .unwrap();
    let lexemes: Vec<&str> = levels
        .iter()
        .map(|entry| entry.as_number().expect("number").as_str())
        .collect();
    assert_eq!(
        lexemes,
        vec!["0.0100", "1E-2", "12345678901234567890", "-0.5", "0"]
    );
}

#[test]
fn lexical_parser_rejects_structural_violations() {
    let limits = LexicalLimits::venue_payload();
    assert!(matches!(
        parse_lexical(b"", limits),
        Err(LexicalError::EmptyInput)
    ));
    assert!(matches!(
        parse_lexical(br#"{"a":1}{"a":2}"#, limits),
        Err(LexicalError::TrailingBytes { .. })
    ));
    assert!(matches!(
        parse_lexical(br#"{"a":1,"a":2}"#, limits),
        Err(LexicalError::DuplicateField { .. })
    ));
    assert!(matches!(
        parse_lexical(br#"{"a":01}"#, limits),
        Err(LexicalError::UnexpectedByte { .. })
    ));
    assert!(matches!(
        parse_lexical(br#"{"a":1."#, limits),
        Err(LexicalError::InvalidNumber { .. })
    ));
    assert!(matches!(
        parse_lexical(b"{\"a\":NaN}", limits),
        Err(LexicalError::UnexpectedByte { .. })
    ));
    assert!(matches!(
        parse_lexical(br#"{"a":1"#, limits),
        Err(LexicalError::UnexpectedEnd { .. })
    ));
    assert!(matches!(
        parse_lexical(b"{\"a\":\"\x01\"}", limits),
        Err(LexicalError::InvalidString { .. })
    ));
    assert!(matches!(
        parse_lexical(br#"{"a":"\q"}"#, limits),
        Err(LexicalError::InvalidEscape { .. })
    ));
    assert!(matches!(
        parse_lexical(br#"{"a":"\ud800"}"#, limits),
        Err(LexicalError::InvalidEscape { .. })
    ));
}

#[test]
fn lexical_parser_enforces_declared_bounds() {
    let limits = LexicalLimits {
        max_bytes: 64,
        max_depth: 2,
        max_array_elements: 2,
        max_object_entries: 2,
        max_string_bytes: 4,
        max_number_bytes: 3,
    };
    assert!(matches!(
        parse_lexical(&[b'0'; 65], limits),
        Err(LexicalError::InputTooLarge { .. })
    ));
    assert!(matches!(
        parse_lexical(br#"{"a":{"b":{"c":1}}}"#, limits),
        Err(LexicalError::DepthExceeded { .. })
    ));
    assert!(matches!(
        parse_lexical(br#"[1,2,3]"#, limits),
        Err(LexicalError::ArrayCapacityExceeded { .. })
    ));
    assert!(matches!(
        parse_lexical(br#"{"a":1,"b":2,"c":3}"#, limits),
        Err(LexicalError::ObjectCapacityExceeded { .. })
    ));
    assert!(matches!(
        parse_lexical(br#""abcdefgh""#, limits),
        Err(LexicalError::StringTooLong { .. })
    ));
    assert!(matches!(
        parse_lexical(br#"12345"#, limits),
        Err(LexicalError::NumberTooLong { .. })
    ));
}

#[test]
fn lexical_parser_reads_escaped_and_multibyte_text() {
    let value = parse_lexical(
        "{\"label\":\"a\\u0062\\ud83d\\ude00\\n\u{00e9}\"}".as_bytes(),
        LexicalLimits::venue_payload(),
    )
    .expect("valid payload");
    assert_eq!(
        value.path("label").and_then(LexicalValue::as_text),
        Some("ab\u{1f600}\n\u{00e9}")
    );
    assert_eq!(value.kind(), LexicalKind::Object);
}

fn full_book_update() -> pm_ws::limitless::OrderbookUpdate {
    let frame = decode_frame(
        ORDERBOOK_UPDATE_FULL_BOOK.as_bytes(),
        WebSocketOpcode::Text,
        LexicalLimits::venue_payload(),
    )
    .unwrap();
    let LimitlessEvent::OrderbookUpdate(update) = decode_event(&frame).unwrap() else {
        panic!("orderbook-update-full-book decodes to an orderbookUpdate event");
    };
    update
}

fn snapshot_provenance(representation: Representation, origin: Origin) -> Provenance {
    Provenance::new(ProvenanceInput {
        market: MarketRef::new(
            Venue::new("limitless").unwrap(),
            NativeMarketKey::new(NativeIdentifierKind::slug(), "example-market-slug").unwrap(),
        ),
        outcome: Some(NativeOutcome::side("yes").unwrap()),
        native_family: "orderbookUpdate".into(),
        source_timestamp: Some(SourceTimestamp::new("2026-08-26T12:00:00.000Z").unwrap()),
        source_evidence: BoundedSourceEvidence::new([], SourceEvidenceCapacity::new(0).unwrap())
            .unwrap(),
        daemon_generation: 1,
        connection: ConnectionIdentity::new("ws.limitless.exchange", 1).unwrap(),
        subscription_generation: 1,
        receive_position: 1,
        commit_position: 1,
        local_receive_time: LocalMonotonicTimestamp::new(1),
        local_commit_time: LocalMonotonicTimestamp::new(1),
        replica: ReplicaRole::PublishingPrimary,
        representation,
        origin,
        local_revision: 0,
        continuity_epoch: 0,
    })
    .unwrap()
}

#[test]
fn wire_limitless_snapshot_candidate_requires_venue_native_source_reported_provenance() {
    let update = full_book_update();
    let capacity = LevelCapacity::new(64).unwrap();

    let accepted = update
        .snapshot_candidate(
            snapshot_provenance(Representation::VenueNative, Origin::SourceReported),
            capacity,
        )
        .expect("venue-native, source-reported provenance is a valid snapshot candidate");
    assert_eq!(
        accepted.provenance().representation(),
        &Representation::VenueNative
    );
    assert_eq!(accepted.provenance().origin(), &Origin::SourceReported);

    assert_eq!(
        update.snapshot_candidate(
            snapshot_provenance(Representation::Normalized, Origin::NormalizedFromSource),
            capacity,
        ),
        Err(ObservationError::InvalidOrigin)
    );
    assert_eq!(
        update.snapshot_candidate(
            snapshot_provenance(
                Representation::Normalized,
                Origin::LocallyDerived(Derivation::SnapshotDiff)
            ),
            capacity,
        ),
        Err(ObservationError::InvalidOrigin)
    );
}

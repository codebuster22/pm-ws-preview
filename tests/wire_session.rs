use pm_ws::DecimalError;
use pm_ws::limitless::{LimitlessDecodeError, LimitlessEvent, decode_event};
use pm_ws::observation::Side;
use pm_ws::wire::lexical::{LexicalLimits, LexicalValue};
use pm_ws::wire::session::{
    EngineIoOpen, SessionError, TerminalFrameReason, encode_engine_pong, encode_namespace_connect,
    encode_namespace_disconnect, encode_subscribe_market_prices, terminal_frame_reason,
};
use pm_ws::wire::socketio::{WebSocketOpcode, decode_frame};

const MARKET_RESOLVED_MARKET_ROOM: &str = r#"42/markets,["marketResolved",{"slug":"example-market-slug","type":"CLOB","winningOutcome":"YES","winningIndex":0,"resolutionDate":"2026-08-26T12:10:00.000Z"}]"#;
const ORDERBOOK_UPDATE_DUPLICATE_BID_PRICE: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"example-market-slug","orderbook":{"bids":[{"price":0.50,"size":10},{"price":0.5,"size":20}],"asks":[{"price":0.52,"size":75}]},"timestamp":"2026-08-26T12:00:45.000Z"}]"#;
const ORDERBOOK_UPDATE_EXACT_DECIMAL_SENTINEL: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"example-market-slug","orderbook":{"bids":[{"price":0.123456789012345678,"size":9007199254740993}],"asks":[{"price":0.52,"size":75}]},"timestamp":"2026-08-26T12:00:05.000Z"}]"#;
const ORDERBOOK_UPDATE_TOKEN_ID_NULL: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"example-market-slug","orderbook":{"tokenId":null,"bids":[{"price":0.51,"size":120}],"asks":[{"price":0.52,"size":75}]},"timestamp":"2026-08-26T12:00:40.000Z"}]"#;
const ORDERBOOK_UPDATE_TOKEN_ID_NUMBER: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"example-market-slug","orderbook":{"tokenId":123,"bids":[{"price":0.51,"size":120}],"asks":[{"price":0.52,"size":75}]},"timestamp":"2026-08-26T12:00:30.000Z"}]"#;
const ORDERBOOK_UPDATE_EMPTY_BOOK: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"example-market-slug","orderbook":{"bids":[],"asks":[]},"timestamp":"2026-08-26T12:00:15.000Z"}]"#;
const ORDERBOOK_UPDATE_EXPONENT_AND_TRAILING_ZERO_LEXEMES: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"example-market-slug","orderbook":{"bids":[{"price":5.1E-1,"size":1.20E2}],"asks":[{"price":0.520000,"size":75.000}]},"timestamp":"2026-08-26T12:00:25.000Z"}]"#;
const ORDERBOOK_UPDATE_FULL_BOOK: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"example-market-slug","orderbook":{"bids":[{"price":0.51,"size":120},{"price":0.50,"size":340.5}],"asks":[{"price":0.52,"size":75},{"price":0.53,"size":10}]},"timestamp":"2026-08-26T12:00:00.000Z"}]"#;
const ORDERBOOK_UPDATE_MISSING_MARKET_SLUG: &str = r#"42/markets,["orderbookUpdate",{"orderbook":{"bids":[],"asks":[]},"timestamp":"2026-08-26T12:01:00.000Z"}]"#;
const ORDERBOOK_UPDATE_OVERSIZED_SIZE_PRECISION: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"example-market-slug","orderbook":{"bids":[{"price":0.51,"size":9999999999999999999999999999999}],"asks":[{"price":0.52,"size":75}]},"timestamp":"2026-08-26T12:00:55.000Z"}]"#;
const ORDERBOOK_UPDATE_PRICE_OUT_OF_DOMAIN: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"example-market-slug","orderbook":{"bids":[{"price":1.01,"size":10}],"asks":[{"price":0.52,"size":75}]},"timestamp":"2026-08-26T12:00:50.000Z"}]"#;
const ORDERBOOK_UPDATE_UNORDERED_BIDS: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"example-market-slug","orderbook":{"bids":[{"price":0.50,"size":340.5},{"price":0.51,"size":120}],"asks":[{"price":0.52,"size":75}]},"timestamp":"2026-08-26T12:00:35.000Z"}]"#;
const ORDERBOOK_UPDATE_WITH_TOKEN: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"example-market-slug-two","orderbook":{"tokenId":"7451820993000000000000000000000000000001","bids":[{"price":0.40,"size":10}],"asks":[{"price":0.41,"size":12}]},"timestamp":"2026-08-26T12:00:10.000Z"}]"#;
const ORDERBOOK_UPDATE_ZERO_SIZE_LEVEL: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"example-market-slug","orderbook":{"bids":[{"price":0.51,"size":0}],"asks":[{"price":0.52,"size":75}]},"timestamp":"2026-08-26T12:00:20.000Z"}]"#;
const SOCKETIO_NAMESPACE_CONNECT_ACK: &str = r#"40/markets,{"sid":"REPLAY-NAMESPACE-0001"}"#;
const SUBSCRIBE_MARKET_PRICES_COMMAND: &str = r#"42/markets,["subscribe_market_prices",{"marketSlugs":["example-market-slug","example-market-slug-two"]}]"#;
const UNSUPPORTED_FUTURE_EVENT: &str =
    r#"42/markets,["orderbookDepthDelta",{"marketSlug":"example-market-slug","changes":[]}]"#;

fn open_payload(json_object: &str) -> LexicalValue {
    let packet = format!("0{json_object}");
    let frame = decode_frame(
        packet.as_bytes(),
        WebSocketOpcode::Text,
        LexicalLimits::venue_payload(),
    )
    .expect("engine.io open packet decodes at the wire layer");
    frame
        .payload()
        .cloned()
        .expect("open packet carries a payload")
}

fn decode(packet: &str) -> Result<LimitlessEvent, LimitlessDecodeError> {
    let frame = decode_frame(
        packet.as_bytes(),
        WebSocketOpcode::Text,
        LexicalLimits::venue_payload(),
    )
    .expect("packet decodes at the wire layer");
    decode_event(&frame)
}

#[test]
fn wire_engineio_open_parses_documented_payload() {
    let payload = open_payload(
        r#"{"sid":"REPLAY-SESSION-0001","upgrades":[],"pingInterval":25000,"pingTimeout":20000,"maxPayload":1000000}"#,
    );
    let open = EngineIoOpen::from_open_payload(&payload).expect("documented payload is valid");
    assert_eq!(open.sid(), "REPLAY-SESSION-0001");
    assert_eq!(open.ping_interval_ms(), 25_000);
    assert_eq!(open.ping_timeout_ms(), 20_000);
    assert_eq!(open.max_payload_bytes(), 1_000_000);
    assert_eq!(
        open.heartbeat_deadline(),
        core::time::Duration::from_millis(45_000)
    );
}

#[test]
fn wire_engineio_open_rejects_missing_field() {
    let payload =
        open_payload(r#"{"pingInterval":25000,"pingTimeout":20000,"maxPayload":1000000}"#);
    assert_eq!(
        EngineIoOpen::from_open_payload(&payload),
        Err(SessionError::MissingField("sid"))
    );
}

#[test]
fn wire_engineio_open_rejects_wrong_type() {
    let payload = open_payload(
        r#"{"sid":"x","pingInterval":"25000","pingTimeout":20000,"maxPayload":1000000}"#,
    );
    assert!(matches!(
        EngineIoOpen::from_open_payload(&payload),
        Err(SessionError::UnexpectedType {
            field: "pingInterval",
            ..
        })
    ));
}

#[test]
fn wire_engineio_open_rejects_non_integer_lexeme() {
    let payload = open_payload(
        r#"{"sid":"x","pingInterval":25000.5,"pingTimeout":20000,"maxPayload":1000000}"#,
    );
    assert_eq!(
        EngineIoOpen::from_open_payload(&payload),
        Err(SessionError::NonIntegerLexeme {
            field: "pingInterval"
        })
    );
}

#[test]
fn wire_engineio_open_rejects_ping_interval_below_policy_floor() {
    let payload =
        open_payload(r#"{"sid":"x","pingInterval":0,"pingTimeout":20000,"maxPayload":1000000}"#);
    assert_eq!(
        EngineIoOpen::from_open_payload(&payload),
        Err(SessionError::OutOfPolicy {
            field: "pingInterval",
            value: 0
        })
    );
}

#[test]
fn wire_engineio_open_rejects_ping_timeout_above_policy_ceiling() {
    let payload = open_payload(
        r#"{"sid":"x","pingInterval":25000,"pingTimeout":400000,"maxPayload":1000000}"#,
    );
    assert_eq!(
        EngineIoOpen::from_open_payload(&payload),
        Err(SessionError::OutOfPolicy {
            field: "pingTimeout",
            value: 400_000
        })
    );
}

#[test]
fn wire_engineio_open_rejects_max_payload_below_policy_floor() {
    let payload =
        open_payload(r#"{"sid":"x","pingInterval":25000,"pingTimeout":20000,"maxPayload":100}"#);
    assert_eq!(
        EngineIoOpen::from_open_payload(&payload),
        Err(SessionError::OutOfPolicy {
            field: "maxPayload",
            value: 100
        })
    );
}

#[test]
fn wire_encoders_produce_exact_wire_bytes() {
    assert_eq!(encode_namespace_connect("/markets"), "40/markets,");
    assert_eq!(encode_namespace_disconnect("/markets"), "41/markets,");
    assert_eq!(encode_engine_pong(), "3");
    let slugs = vec![
        "example-market-slug".to_owned(),
        "example-market-slug-two".to_owned(),
    ];
    assert_eq!(
        encode_subscribe_market_prices("/markets", &slugs),
        SUBSCRIBE_MARKET_PRICES_COMMAND
    );
}

#[test]
fn wire_limitless_full_book_decodes_exact_canonical_values() {
    let event = decode(ORDERBOOK_UPDATE_FULL_BOOK).expect("full book decodes");
    let LimitlessEvent::OrderbookUpdate(update) = event else {
        panic!("expected orderbookUpdate");
    };
    assert_eq!(update.market_slug(), "example-market-slug");
    assert_eq!(update.token_id(), None);
    assert_eq!(update.timestamp(), "2026-08-26T12:00:00.000Z");
    let bids: Vec<(String, String)> = update
        .bids()
        .iter()
        .map(|(price, quantity)| (price.value().canonical(), quantity.value().canonical()))
        .collect();
    assert_eq!(
        bids,
        vec![
            ("0.51".to_owned(), "120".to_owned()),
            ("0.5".to_owned(), "340.5".to_owned()),
        ]
    );
    let asks: Vec<(String, String)> = update
        .asks()
        .iter()
        .map(|(price, quantity)| (price.value().canonical(), quantity.value().canonical()))
        .collect();
    assert_eq!(
        asks,
        vec![
            ("0.52".to_owned(), "75".to_owned()),
            ("0.53".to_owned(), "10".to_owned()),
        ]
    );
}

#[test]
fn wire_limitless_exponent_and_trailing_zero_lexemes_normalize() {
    let event =
        decode(ORDERBOOK_UPDATE_EXPONENT_AND_TRAILING_ZERO_LEXEMES).expect("fixture decodes");
    let LimitlessEvent::OrderbookUpdate(update) = event else {
        panic!("expected orderbookUpdate");
    };
    assert_eq!(update.bids()[0].0.value().canonical(), "0.51");
    assert_eq!(update.bids()[0].1.value().canonical(), "120");
    assert_eq!(update.asks()[0].0.value().canonical(), "0.52");
    assert_eq!(update.asks()[0].1.value().canonical(), "75");
}

#[test]
fn wire_limitless_token_id_present_when_reported() {
    let event = decode(ORDERBOOK_UPDATE_WITH_TOKEN).expect("fixture decodes");
    let LimitlessEvent::OrderbookUpdate(update) = event else {
        panic!("expected orderbookUpdate");
    };
    assert_eq!(
        update.token_id(),
        Some("7451820993000000000000000000000000000001")
    );
}

#[test]
fn wire_limitless_empty_book_decodes() {
    let event = decode(ORDERBOOK_UPDATE_EMPTY_BOOK).expect("empty book decodes");
    let LimitlessEvent::OrderbookUpdate(update) = event else {
        panic!("expected orderbookUpdate");
    };
    assert!(update.bids().is_empty());
    assert!(update.asks().is_empty());
}

#[test]
fn wire_limitless_zero_size_level_reproduced() {
    let event = decode(ORDERBOOK_UPDATE_ZERO_SIZE_LEVEL).expect("fixture decodes");
    let LimitlessEvent::OrderbookUpdate(update) = event else {
        panic!("expected orderbookUpdate");
    };
    assert_eq!(update.bids()[0].1.value().canonical(), "0");
}

#[test]
fn wire_limitless_unordered_bids_rejected() {
    let error = decode(ORDERBOOK_UPDATE_UNORDERED_BIDS).unwrap_err();
    assert_eq!(
        error,
        LimitlessDecodeError::LevelOrdering { side: Side::Bid }
    );
}

#[test]
fn wire_limitless_duplicate_price_rejected() {
    let error = decode(ORDERBOOK_UPDATE_DUPLICATE_BID_PRICE).unwrap_err();
    assert_eq!(
        error,
        LimitlessDecodeError::LevelOrdering { side: Side::Bid }
    );
}

#[test]
fn wire_limitless_price_out_of_domain_rejected() {
    let error = decode(ORDERBOOK_UPDATE_PRICE_OUT_OF_DOMAIN).unwrap_err();
    assert_eq!(
        error,
        LimitlessDecodeError::PriceOutOfDomain { field: "price" }
    );
}

#[test]
fn wire_limitless_oversized_size_precision_rejected() {
    let error = decode(ORDERBOOK_UPDATE_OVERSIZED_SIZE_PRECISION).unwrap_err();
    assert!(matches!(
        error,
        LimitlessDecodeError::Decimal {
            field: "size",
            source: DecimalError::PrecisionExceeded,
        }
    ));
}

#[test]
fn wire_limitless_missing_market_slug_rejected() {
    let error = decode(ORDERBOOK_UPDATE_MISSING_MARKET_SLUG).unwrap_err();
    assert_eq!(
        error,
        LimitlessDecodeError::InvalidField {
            field: "marketSlug"
        }
    );
}

#[test]
fn wire_limitless_market_resolved_decodes_all_fields() {
    let event = decode(MARKET_RESOLVED_MARKET_ROOM).expect("fixture decodes");
    let LimitlessEvent::MarketResolved(resolved) = event else {
        panic!("expected marketResolved");
    };
    assert_eq!(resolved.slug(), "example-market-slug");
    assert_eq!(resolved.market_type(), "CLOB");
    assert_eq!(resolved.winning_outcome(), "YES");
    assert_eq!(resolved.winning_index(), 0);
    assert_eq!(resolved.resolution_date(), "2026-08-26T12:10:00.000Z");
}

#[test]
fn wire_limitless_unknown_event_name_preserved() {
    let event = decode(UNSUPPORTED_FUTURE_EVENT).expect("fixture decodes");
    assert_eq!(
        event,
        LimitlessEvent::Unknown {
            name: "orderbookDepthDelta".to_owned()
        }
    );
}

#[test]
fn wire_limitless_decode_event_rejects_a_non_event_frame() {
    let frame = decode_frame(
        SOCKETIO_NAMESPACE_CONNECT_ACK.as_bytes(),
        WebSocketOpcode::Text,
        LexicalLimits::venue_payload(),
    )
    .expect("connect ack decodes at the wire layer");
    assert_eq!(decode_event(&frame), Err(LimitlessDecodeError::NotAnEvent));
}

#[test]
fn wire_limitless_token_id_null_rejected() {
    let error = decode(ORDERBOOK_UPDATE_TOKEN_ID_NULL).unwrap_err();
    assert_eq!(
        error,
        LimitlessDecodeError::InvalidField { field: "tokenId" }
    );
}

#[test]
fn wire_limitless_token_id_number_rejected() {
    let error = decode(ORDERBOOK_UPDATE_TOKEN_ID_NUMBER).unwrap_err();
    assert_eq!(
        error,
        LimitlessDecodeError::InvalidField { field: "tokenId" }
    );
}

#[test]
fn wire_limitless_exact_decimal_sentinel_survives_without_an_f64_detour() {
    let event = decode(ORDERBOOK_UPDATE_EXACT_DECIMAL_SENTINEL).expect("fixture decodes");
    let LimitlessEvent::OrderbookUpdate(update) = event else {
        panic!("expected orderbookUpdate");
    };
    let (price, quantity) = &update.bids()[0];
    assert_eq!(price.value().coefficient(), 123_456_789_012_345_678);
    assert_eq!(price.value().scale(), 18);
    assert_eq!(quantity.value().coefficient(), 9_007_199_254_740_993);
    assert_eq!(quantity.value().scale(), 0);
}

#[test]
fn wire_terminal_frame_reason_classifies_session_enders() {
    let engine_io_close = decode_frame(b"1", WebSocketOpcode::Text, LexicalLimits::venue_payload())
        .expect("engine.io close decodes at the wire layer");
    assert_eq!(
        terminal_frame_reason(&engine_io_close, "/markets"),
        Some(TerminalFrameReason::EngineIoClose)
    );

    let namespace_disconnect = decode_frame(
        b"41/markets,",
        WebSocketOpcode::Text,
        LexicalLimits::venue_payload(),
    )
    .expect("namespace disconnect decodes at the wire layer");
    assert_eq!(
        terminal_frame_reason(&namespace_disconnect, "/markets"),
        Some(TerminalFrameReason::NamespaceDisconnect)
    );

    let namespace_connect_error = decode_frame(
        br#"44/markets,{"message":"x"}"#,
        WebSocketOpcode::Text,
        LexicalLimits::venue_payload(),
    )
    .expect("namespace connect error decodes at the wire layer");
    assert_eq!(
        terminal_frame_reason(&namespace_connect_error, "/markets"),
        Some(TerminalFrameReason::NamespaceConnectError)
    );

    let other_namespace_disconnect = decode_frame(
        b"41/other,",
        WebSocketOpcode::Text,
        LexicalLimits::venue_payload(),
    )
    .expect("other-namespace disconnect decodes at the wire layer");
    assert_eq!(
        terminal_frame_reason(&other_namespace_disconnect, "/markets"),
        None
    );

    let ordinary_frame = decode_frame(
        SOCKETIO_NAMESPACE_CONNECT_ACK.as_bytes(),
        WebSocketOpcode::Text,
        LexicalLimits::venue_payload(),
    )
    .expect("connect ack decodes at the wire layer");
    assert_eq!(terminal_frame_reason(&ordinary_frame, "/markets"), None);
}

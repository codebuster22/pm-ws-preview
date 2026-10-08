use pm_ws::wire::lexical::{LexicalLimits, LexicalValue};
use pm_ws::wire::session::{
    EngineIoOpen, SessionError, TerminalFrameReason, encode_engine_pong, encode_namespace_connect,
    encode_namespace_disconnect, encode_subscribe_market_prices, terminal_frame_reason,
};
use pm_ws::wire::socketio::{WebSocketOpcode, decode_frame};

const SOCKETIO_NAMESPACE_CONNECT_ACK: &str = r#"40/markets,{"sid":"REPLAY-NAMESPACE-0001"}"#;
const SUBSCRIBE_MARKET_PRICES_COMMAND: &str = r#"42/markets,["subscribe_market_prices",{"marketSlugs":["example-market-slug","example-market-slug-two"]}]"#;

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

use std::sync::Arc;

use pm_ws::{
    limitless::native::{LimitlessNativeError, decode_native_frame},
    native::{NativeFamily, NativeIdentity, NativeSource},
    numeric::DecimalGrammar,
    wire::{
        lexical::LexicalLimits,
        session::{TerminalFrameReason, terminal_frame_reason},
        socketio::{
            FrameError, SocketIoPacket, WebSocketOpcode, decode_frame, decode_limitless_frame,
        },
    },
};

fn source() -> NativeSource {
    NativeSource {
        stream: Arc::from("limitless"),
        source: Arc::from("one"),
        slot: 0,
        generation: 1,
        stream_generation: 1,
        sequence: 7,
        received_ns: 11,
        validated_ns: 12,
    }
}

fn decode(text: &str) -> Result<Option<pm_ws::native::NativeBatch>, LimitlessNativeError> {
    let frame = decode_limitless_frame(
        text.as_bytes(),
        WebSocketOpcode::Text,
        LexicalLimits::venue_payload(),
        grammar(),
    )
    .unwrap();
    decode_native_frame(frame, source(), text.len())
}

fn event(text: &str) -> pm_ws::native::NativeEvent {
    decode(text).unwrap().unwrap().events.pop().unwrap()
}

fn grammar() -> DecimalGrammar {
    DecimalGrammar::new(30, 39, true, true).unwrap()
}

fn payload(event: &pm_ws::native::NativeEvent) -> pm_ws::native::document::ValueRef<'_> {
    event.payload.view()
}

#[test]
fn all_selected_families_keep_full_payload_and_route_natively() {
    let cases = [
        (
            r#"42/markets,["orderbookUpdate",{"marketSlug":"clob","orderbook":{"tokenId":"asset","bids":[{"price":0.5100,"size":0}],"asks":[]},"version":0,"timestamp":"t","extension":{"n":1e2}}]"#,
            NativeFamily::LimitlessOrderbookUpdate,
            Some("clob"),
            Some("asset"),
        ),
        (
            r#"42/markets,["newPriceData",{"marketAddress":"0xabc","updatedPrices":{"yes":"0.6500","no":"0.3500"},"blockNumber":0,"timestamp":"t","extension":true}]"#,
            NativeFamily::LimitlessNewPriceData,
            Some("0xabc"),
            None,
        ),
        (
            r#"42/markets,["marketCreated",{"slug":"new","title":"T","type":"CLOB","groupSlug":null,"categoryIds":[1,2],"createdAt":"t","extension":[]}]"#,
            NativeFamily::LimitlessMarketCreated,
            Some("new"),
            None,
        ),
        (
            r#"42/markets,["marketResolved",{"slug":"done","type":"AMM","winningOutcome":"YES","winningIndex":0,"resolutionDate":"t","extension":null}]"#,
            NativeFamily::LimitlessMarketResolved,
            Some("done"),
            None,
        ),
        (
            r#"42/markets,["system",{"message":"ok","markets":["clob"],"extension":1}]"#,
            NativeFamily::LimitlessSystem,
            None,
            None,
        ),
        (
            r#"42/markets,["exception",{"message":"bad","code":0,"extension":{"why":"x"}}]"#,
            NativeFamily::LimitlessException,
            None,
            None,
        ),
    ];
    for (wire, family, market, asset) in cases {
        let decoded = event(wire);
        assert_eq!(decoded.family, family);
        assert_eq!(decoded.market.as_deref(), market);
        assert_eq!(decoded.assets.first().map(AsRef::as_ref), asset);
        assert_eq!(payload(&decoded).children().count(), 2);
    }
}

#[test]
fn orderbook_identity_uses_timestamp_and_canonical_version_without_ordering() {
    let decoded = event(
        r#"42/markets,["orderbookUpdate",{"marketSlug":"m","orderbook":{"bids":[],"asks":[]},"version":0.0,"timestamp":"same"}]"#,
    );
    assert_eq!(
        decoded.identity,
        Some(NativeIdentity::Components(vec![
            Arc::from("same"),
            Arc::from("0")
        ]))
    );
    assert_eq!(
        payload(&decoded)
            .children()
            .nth(1)
            .unwrap()
            .field("version")
            .unwrap()
            .exact_decimal()
            .unwrap()
            .to_string(),
        "0"
    );
}

#[test]
fn escaped_required_keys_are_viewed_without_reconstructing_the_payload() {
    let decoded = event(
        r#"42/markets,["orderbookUpdate",{"market\u0053lug":"m","orderbook":{"bids":[],"asks":[]},"version":0,"timestamp":"t"}]"#,
    );
    assert_eq!(decoded.market.as_deref(), Some("m"));
    assert_eq!(payload(&decoded).children().count(), 2);
}

#[test]
fn amm_prices_are_exact_decimal_strings_and_extra_arguments_are_not_lost() {
    let decoded = event(
        r#"42/markets,["newPriceData",{"marketAddress":"a","updatedPrices":{"yes":"0.6500","no":"0.3500"},"blockNumber":9007199254740993,"timestamp":"t"},{"trace":0.000} ]"#,
    );
    let mut values = payload(&decoded).children();
    let _ = values.next();
    let source_payload = values.next().unwrap();
    let extra = values.next().unwrap();
    assert!(values.next().is_none());
    assert!(
        source_payload
            .field("updatedPrices")
            .unwrap()
            .field("yes")
            .unwrap()
            .kind()
            == pm_ws::native::document::NativeKind::DecimalString
    );
    assert!(extra.field("trace").unwrap().number_lexeme() == Some("0.000"));
}

#[test]
fn missing_required_shape_and_unrepresentable_decimal_reject_whole_event() {
    let missing = decode(
        r#"42/markets,["orderbookUpdate",{"marketSlug":"m","orderbook":{"bids":[],"asks":[]},"timestamp":"t"}]"#,
    );
    assert!(matches!(
        missing,
        Err(LimitlessNativeError::InvalidField("version"))
    ));
    assert!(matches!(
        decode(
            r#"42/markets,["newPriceData",{"marketAddress":"a","updatedPrices":{"yes":"0.1234567890123456789012345678901234567890","no":"0"},"blockNumber":0,"timestamp":"t"}]"#
        ),
        Err(LimitlessNativeError::Decimal(_))
    ));
}

#[test]
fn unknown_is_bounded_envelope_and_controls_are_not_market_events() {
    let unknown = event(r#"42/markets,["futureFamily",{"exact":0.000,"extra":[true]}]"#);
    assert_eq!(unknown.family, NativeFamily::Unknown);
    assert_eq!(unknown.family_name.as_deref(), Some("futureFamily"));
    assert_eq!(payload(&unknown).children().count(), 2);
    let control = decode("2").unwrap();
    assert!(control.is_none());
}

#[test]
fn system_welcome_is_preserved_without_markets_but_malformed_ack_rejects() {
    let welcome =
        event(r#"42/markets,["system",{"message":"Successfully registered connection"}]"#);
    assert_eq!(welcome.family, NativeFamily::LimitlessSystem);
    assert!(
        payload(&welcome)
            .children()
            .nth(1)
            .unwrap()
            .field("markets")
            .is_none()
    );
    assert!(matches!(
        decode(r#"42/markets,["system",{"message":"ok","markets":"clob"}]"#),
        Err(LimitlessNativeError::InvalidField("markets"))
    ));
}

#[test]
fn lexical_frame_rejects_malformed_and_native_delivery_rejects_unrepresentable_arguments() {
    assert!(matches!(
        decode_frame(
            br#"42/markets,["system",{"message":"ok"}"#,
            WebSocketOpcode::Text,
            LexicalLimits::venue_payload(),
        ),
        Err(FrameError::Lexical(_))
    ));
    assert!(matches!(
        decode_limitless_frame(
            br#"42/markets,["future",1234567890123456789012345678901234567890]"#,
            WebSocketOpcode::Text,
            LexicalLimits::venue_payload(),
            grammar(),
        ),
        Err(FrameError::Document(_))
    ));
    assert!(matches!(
        decode_limitless_frame(
            br#"42/markets,{"event":"future"}"#,
            WebSocketOpcode::Text,
            LexicalLimits::venue_payload(),
            grammar(),
        ),
        Err(FrameError::MalformedEventEnvelope)
    ));
}

#[test]
fn controls_and_other_namespaces_do_not_apply_limitless_decimal_policy() {
    let wide = "1234567890123456789012345678901234567890";
    let connect_error = decode_limitless_frame(
        format!(r#"44/markets,{{"code":{wide}}}"#).as_bytes(),
        WebSocketOpcode::Text,
        LexicalLimits::venue_payload(),
        grammar(),
    )
    .unwrap();
    assert_eq!(
        terminal_frame_reason(&connect_error, "/markets"),
        Some(TerminalFrameReason::NamespaceConnectError)
    );
    assert!(connect_error.payload().is_some());
    let acknowledgment = decode_limitless_frame(
        format!(r#"43/markets,{{"n":{wide}}}"#).as_bytes(),
        WebSocketOpcode::Text,
        LexicalLimits::venue_payload(),
        grammar(),
    )
    .unwrap();
    assert_eq!(acknowledgment.socket_io(), Some(SocketIoPacket::Ack));
    assert!(
        decode_native_frame(acknowledgment, source(), 0)
            .unwrap()
            .is_none()
    );
    let other = decode_limitless_frame(
        format!(r#"42/other,["future",{wide}]"#).as_bytes(),
        WebSocketOpcode::Text,
        LexicalLimits::venue_payload(),
        grammar(),
    )
    .unwrap();
    assert_eq!(other.namespace(), Some("/other"));
    assert_eq!(other.event_name(), Some("future"));
    assert!(decode_native_frame(other, source(), 0).unwrap().is_none());
    assert!(matches!(
        decode_limitless_frame(
            br#"42/other,["future",}"#,
            WebSocketOpcode::Text,
            LexicalLimits::venue_payload(),
            grammar(),
        ),
        Err(FrameError::Lexical(_))
    ));
    assert!(matches!(
        decode_limitless_frame(
            br#"42/other,{"event":"future"}"#,
            WebSocketOpcode::Text,
            LexicalLimits::venue_payload(),
            grammar(),
        ),
        Err(FrameError::MalformedEventEnvelope)
    ));
}

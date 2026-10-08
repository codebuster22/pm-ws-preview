//! Drives every frame in the observed live-venue corpus through the native rail's decode
//! path (`decode_limitless_frame` + `decode_native_frame`) and pins each frame's typed
//! outcome: family, native market identifier, identity components, and an exact decimal
//! value read directly from the frame text.

mod support;

use std::sync::Arc;

use pm_ws::{
    limitless::native::decode_native_frame,
    native::{NativeBatch, NativeEvent, NativeFamily, NativeIdentity},
    numeric::DecimalGrammar,
    wire::{
        lexical::LexicalLimits,
        socketio::{WebSocketOpcode, decode_limitless_frame},
    },
};
use support::observed_frames::{
    OBSERVED_ENGINEIO_OPEN, OBSERVED_MARKET_RESOLVED_OWN_ROOM, OBSERVED_NAMESPACE_CONNECT_ACK,
    OBSERVED_ORACLE_PRICE_DATA, OBSERVED_ORDERBOOK_UPDATE_FIRST_FULL_BOOK,
    OBSERVED_ORDERBOOK_UPDATE_LAST_BEFORE_RESOLUTION, OBSERVED_RESOLUTION_MARKET_SLUG,
    OBSERVED_SYSTEM_REGISTERED, OBSERVED_SYSTEM_SUBSCRIBED,
};

fn grammar() -> DecimalGrammar {
    DecimalGrammar::new(30, 39, true, true).unwrap()
}

fn source() -> pm_ws::native::NativeSource {
    pm_ws::native::NativeSource {
        stream: Arc::from("limitless"),
        source: Arc::from("observed"),
        slot: 0,
        generation: 1,
        stream_generation: 1,
        sequence: 1,
        received_ns: 0,
        validated_ns: 0,
    }
}

fn decode(text: &str) -> Option<NativeBatch> {
    let frame = decode_limitless_frame(
        text.as_bytes(),
        WebSocketOpcode::Text,
        LexicalLimits::venue_payload(),
        grammar(),
    )
    .expect("observed corpus frame decodes at the wire layer");
    decode_native_frame(frame, source(), text.len())
        .expect("observed corpus frame's native event is well-formed")
}

fn event(text: &str) -> NativeEvent {
    decode(text).unwrap().events.into_iter().next().unwrap()
}

fn best_bid_price(event: &NativeEvent) -> String {
    event
        .payload
        .view()
        .children()
        .nth(1)
        .unwrap()
        .field("orderbook")
        .unwrap()
        .field("bids")
        .unwrap()
        .children()
        .next()
        .unwrap()
        .field("price")
        .unwrap()
        .exact_decimal()
        .unwrap()
        .to_string()
}

#[test]
fn observed_engineio_open_is_not_a_native_application_event() {
    assert!(decode(OBSERVED_ENGINEIO_OPEN).is_none());
}

#[test]
fn observed_namespace_connect_ack_is_not_a_native_application_event() {
    assert!(decode(OBSERVED_NAMESPACE_CONNECT_ACK).is_none());
}

#[test]
fn observed_system_registered_pins_family_with_no_market_identity() {
    let event = event(OBSERVED_SYSTEM_REGISTERED);
    assert_eq!(event.family, NativeFamily::LimitlessSystem);
    assert_eq!(event.market, None);
}

#[test]
fn observed_system_subscribed_pins_family_with_no_market_identity() {
    let event = event(OBSERVED_SYSTEM_SUBSCRIBED);
    assert_eq!(event.family, NativeFamily::LimitlessSystem);
    assert_eq!(event.market, None);
}

#[test]
fn observed_oracle_price_data_is_unknown_family_with_its_venue_name_preserved() {
    let event = event(OBSERVED_ORACLE_PRICE_DATA);
    assert_eq!(event.family, NativeFamily::Unknown);
    assert_eq!(event.family_name.as_deref(), Some("oraclePriceData"));
    assert_eq!(event.market, None);
}

#[test]
fn observed_orderbook_update_first_full_book_pins_market_asset_and_best_bid_price() {
    let event = event(OBSERVED_ORDERBOOK_UPDATE_FIRST_FULL_BOOK);
    assert_eq!(event.family, NativeFamily::LimitlessOrderbookUpdate);
    assert_eq!(
        event.market.as_deref(),
        Some("eth-up-or-down-daily-1788105600")
    );
    assert_eq!(
        event.assets.first().map(AsRef::as_ref),
        Some("25018063611559838047404811982184442876005199660833597814711111046007291893507")
    );
    assert_eq!(
        event.identity,
        Some(NativeIdentity::Components(vec![
            Arc::from("2026-08-31T07:12:32.741Z"),
            Arc::from("7861372"),
        ]))
    );
    assert_eq!(best_bid_price(&event), "0.012");
}

#[test]
fn observed_orderbook_update_last_before_resolution_pins_market_asset_and_bid_price() {
    let event = event(OBSERVED_ORDERBOOK_UPDATE_LAST_BEFORE_RESOLUTION);
    assert_eq!(event.family, NativeFamily::LimitlessOrderbookUpdate);
    assert_eq!(
        event.market.as_deref(),
        Some(OBSERVED_RESOLUTION_MARKET_SLUG)
    );
    assert_eq!(
        event.assets.first().map(AsRef::as_ref),
        Some("83416341894274737086755695271958877285974747994652828356442717729857948830534")
    );
    assert_eq!(
        event.identity,
        Some(NativeIdentity::Components(vec![
            Arc::from("2026-09-01T13:09:45.277Z"),
            Arc::from("504852"),
        ]))
    );
    assert_eq!(best_bid_price(&event), "0.002");
}

#[test]
fn observed_market_resolved_own_room_pins_family_market_slug_and_resolution_fields() {
    let event = event(OBSERVED_MARKET_RESOLVED_OWN_ROOM);
    assert_eq!(event.family, NativeFamily::LimitlessMarketResolved);
    assert_eq!(
        event.market.as_deref(),
        Some(OBSERVED_RESOLUTION_MARKET_SLUG)
    );
    let body = event.payload.view().children().nth(1).unwrap();
    assert_eq!(body.field("type").unwrap().as_text(), Some("CLOB"));
    assert_eq!(body.field("winningOutcome").unwrap().as_text(), Some("NO"));
    assert_eq!(
        body.field("winningIndex")
            .unwrap()
            .exact_decimal()
            .unwrap()
            .to_string(),
        "1"
    );
    assert_eq!(
        body.field("resolutionDate").unwrap().as_text(),
        Some("2026-09-01T13:11:02.813Z")
    );
}

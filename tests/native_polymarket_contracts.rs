use std::sync::Arc;

use pm_ws::{
    native::{NativeFamily, NativeIdentity, NativeSource, document::NativeKind},
    polymarket::native::{NativeDecodeError, decode_message},
};

fn source() -> NativeSource {
    NativeSource {
        stream: Arc::from("market"),
        source: Arc::from("socket-a"),
        slot: 0,
        generation: 1,
        stream_generation: 1,
        sequence: 1,
        received_ns: 1,
        validated_ns: 2,
    }
}

fn decode(text: &str) -> pm_ws::native::NativeBatch {
    decode_message(text.as_bytes(), source()).unwrap()
}

#[test]
fn all_public_families_and_pong_are_complete_native_events() {
    let cases = [
        (
            r#"{"event_type":"book","market":"m","asset_id":"a","bids":[{"price":"0","size":"0"}],"asks":[],"timestamp":"1","hash":"h","extension":{"kept":true}}"#,
            NativeFamily::PolymarketBook,
        ),
        (
            r#"{"event_type":"price_change","market":"m","timestamp":"1","price_changes":[{"asset_id":"a","price":"0.5","size":"0","side":"BUY","hash":"h","best_bid":"0.5","best_ask":"1"}]}"#,
            NativeFamily::PolymarketPriceChange,
        ),
        (
            r#"{"event_type":"last_trade_price","market":"m","asset_id":"a","price":"0.5","size":"1","fee_rate_bps":"0","side":"BUY","timestamp":"1","transaction_hash":"t"}"#,
            NativeFamily::PolymarketLastTradePrice,
        ),
        (
            r#"{"event_type":"tick_size_change","market":"m","asset_id":"a","old_tick_size":"0.01","new_tick_size":"0.001","timestamp":"1"}"#,
            NativeFamily::PolymarketTickSizeChange,
        ),
        (
            r#"{"event_type":"best_bid_ask","market":"m","asset_id":"a","best_bid":"0.5","best_ask":"0.6","spread":"0.1","timestamp":"1"}"#,
            NativeFamily::PolymarketBestBidAsk,
        ),
        (
            r#"{"event_type":"new_market","id":"i","question":"q","market":"m","slug":"s","description":"d","assets_ids":["a"],"outcomes":["Yes"],"event_message":{},"timestamp":"1","unexpected":{"nested":[0]}}"#,
            NativeFamily::PolymarketNewMarket,
        ),
        (
            r#"{"event_type":"market_resolved","id":"i","market":"m","assets_ids":["a"],"winning_asset_id":"a","winning_outcome":"Yes","timestamp":"1"}"#,
            NativeFamily::PolymarketMarketResolved,
        ),
    ];
    for (text, family) in cases {
        let event = &decode(text).events[0];
        assert_eq!(event.family, family);
        assert_eq!(
            event.payload.field("event_type").unwrap().kind(),
            NativeKind::String
        );
    }
    assert_eq!(
        decode("PONG").events[0].family,
        NativeFamily::PolymarketPongControl
    );
}

#[test]
fn unknown_fields_and_unknown_family_preserve_the_complete_envelope_without_routing() {
    let event = &decode(
        r#"{"event_type":"future","market":"not-a-demand","extension":{"levels":[{"x":"0.1"}]}}"#,
    )
    .events[0];
    assert_eq!(event.family, NativeFamily::Unknown);
    assert_eq!(event.family_name.as_deref(), Some("future"));
    assert!(event.market.is_none());
    assert!(event.assets.is_empty());
    assert_eq!(
        event.payload.field("extension").unwrap().kind(),
        NativeKind::Object
    );
}

#[test]
fn malformed_final_member_rejects_the_entire_source_array() {
    let result = decode_message(br#"[{"event_type":"book","market":"m","asset_id":"a","bids":[],"asks":[],"timestamp":"1","hash":"h"},{"event_type":"best_bid_ask","market":"m","asset_id":"a","best_bid":"0.5","best_ask":"0.6","spread":false,"timestamp":"2"}]"#, source());
    assert!(matches!(result, Err(NativeDecodeError::Field("spread"))));
}

#[test]
fn empty_top_level_array_is_rejected() {
    assert!(matches!(
        decode_message(b"[]", source()),
        Err(NativeDecodeError::EmptyArray)
    ));
}

#[test]
fn price_change_identity_keeps_entry_order_length_and_repetitions() {
    let event = &decode(r#"{"event_type":"price_change","market":"m","timestamp":"1","price_changes":[{"asset_id":"a","price":"0.5","size":"1","side":"BUY","hash":"h","best_bid":"0.5","best_ask":"1"},{"asset_id":"a","price":"0.5","size":"1","side":"BUY","hash":"h","best_bid":"0.5","best_ask":"1"}]}"#).events[0];
    let Some(NativeIdentity::PolymarketDelta { timestamp, entries }) = &event.identity else {
        panic!("delta identity");
    };
    assert_eq!(timestamp.as_ref(), "1");
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0], entries[1]);
}

#[test]
fn unrepresentable_decimal_is_rejected_without_rounding() {
    let result = decode_message(br#"{"event_type":"last_trade_price","market":"m","asset_id":"a","price":"0.1234567890123456789","size":"1","fee_rate_bps":"0","side":"BUY","timestamp":"1","transaction_hash":"t"}"#, source());
    assert!(matches!(
        result,
        Err(NativeDecodeError::Decimal { field: "price", .. })
    ));
}

#[test]
fn economic_strings_are_promoted_exactly_and_reject_wrong_types_or_precision() {
    let cases = [
        (
            r#"{"event_type":"book","market":"m","asset_id":"a","bids":[{"price":"0.5000","size":"1"}],"asks":[],"timestamp":"1","hash":"h"}"#,
            "price",
        ),
        (
            r#"{"event_type":"price_change","market":"m","timestamp":"1","price_changes":[{"asset_id":"a","price":"0.5000","size":"1","side":"BUY","hash":"h","best_bid":"0.4","best_ask":"0.6"}]}"#,
            "price",
        ),
        (
            r#"{"event_type":"last_trade_price","market":"m","asset_id":"a","price":"0.5000","size":"1","fee_rate_bps":"0","side":"BUY","timestamp":"1","transaction_hash":"t"}"#,
            "price",
        ),
        (
            r#"{"event_type":"tick_size_change","market":"m","asset_id":"a","old_tick_size":"0.01","new_tick_size":"0.5000","timestamp":"1"}"#,
            "new_tick_size",
        ),
        (
            r#"{"event_type":"best_bid_ask","market":"m","asset_id":"a","best_bid":"0.4","best_ask":"0.9","spread":"0.5000","timestamp":"1"}"#,
            "spread",
        ),
        (
            r#"{"event_type":"new_market","id":"i","question":"q","market":"m","slug":"s","description":"d","assets_ids":["a"],"outcomes":["Yes"],"event_message":{},"timestamp":"1","order_price_min_tick_size":"0.5000"}"#,
            "order_price_min_tick_size",
        ),
    ];
    for (wire, field) in cases {
        let batch = decode(wire);
        let payload = batch.events[0].payload.view();
        let value = if field == "price" && payload.field("price").is_none() {
            let array = payload
                .field("bids")
                .or_else(|| payload.field("price_changes"));
            array
                .unwrap()
                .children()
                .next()
                .unwrap()
                .field(field)
                .unwrap()
        } else {
            payload.field(field).unwrap()
        };
        assert_eq!(value.kind(), NativeKind::DecimalString);
        assert_eq!(value.as_text(), Some("0.5000"));
        assert_eq!(value.exact_decimal().unwrap().to_string(), "0.5");
        let numeric = wire.replace(r#""0.5000""#, "0.5000");
        assert!(matches!(decode_message(numeric.as_bytes(), source()),
            Err(NativeDecodeError::Field(rejected)) if rejected == field));
        let excessive = wire.replace("0.5000", "0.1234567890123456789");
        assert!(matches!(decode_message(excessive.as_bytes(), source()),
            Err(NativeDecodeError::Decimal { field: rejected, .. }) if rejected == field));
    }
}

#[test]
fn members_share_one_immutable_document_without_losing_their_roots() {
    let batch = decode(r#"[{"event_type":"future","n":0.5000},{"event_type":"future","n":1.00}]"#);
    assert!(
        batch.events[0]
            .payload
            .shares_document(&batch.events[1].payload)
    );
    let retained = batch.events[1].payload.clone();
    drop(batch);
    assert_eq!(retained.field("n").unwrap().number_lexeme(), Some("1.00"));
    assert_eq!(
        retained
            .field("n")
            .unwrap()
            .exact_decimal()
            .unwrap()
            .to_string(),
        "1"
    );
}

#[test]
fn escaped_keys_and_economic_strings_preserve_semantics_without_duplicate_fields() {
    let batch = decode(
        r#"{"event_type":"last_trade_price","market":"m","asset_id":"a","pr\u0069ce":"0.5\u0030","size":"1","fee_rate_bps":"0","side":"BUY","timestamp":"1","transaction_hash":"t"}"#,
    );
    let price = batch.events[0].payload.field("price").unwrap();
    assert_eq!(price.as_text(), Some("0.50"));
    assert_eq!(price.exact_decimal().unwrap().to_string(), "0.5");
    assert!(matches!(
        decode_message(br#"{"event_type":"future","a":1,"\u0061":2}"#, source()),
        Err(NativeDecodeError::Lexical)
    ));
}

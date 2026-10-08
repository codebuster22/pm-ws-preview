mod support;

use pm_ws::limitless::{LimitlessEvent, ORDERBOOK_UPDATE_DEDUP_KEY, decode_event};
use pm_ws::wire::lexical::LexicalLimits;
use pm_ws::wire::session::EngineIoOpen;
use pm_ws::wire::socketio::{DecodedFrame, FrameError, WebSocketOpcode, decode_frame};
use pm_ws::{DedupKey, DedupKeySemantics, SourceEvidence};
use support::observed_frames::{
    OBSERVED_MARKET_RESOLVED_OWN_ROOM, OBSERVED_ORDERBOOK_UPDATE_LAST_BEFORE_RESOLUTION,
};

const OBSERVED_ENGINEIO_OPEN: &str = r#"0{"sid":"REPLAY-SESSION-0002","upgrades":[],"pingInterval":25000,"pingTimeout":60000,"maxPayload":1000000}"#;
const OBSERVED_NAMESPACE_CONNECT_ACK: &str = r#"40/markets,{"sid":"REPLAY-NAMESPACE-0002"}"#;
const OBSERVED_SYSTEM_REGISTERED: &str =
    r#"42/markets,["system",{"message":"Successfully registered connection"}]"#;
const OBSERVED_SYSTEM_SUBSCRIBED: &str = r#"42/markets,["system",{"message":"Successfully subscribed to market price updates","markets":["eth-up-or-down-daily-1788105600"]}]"#;
const OBSERVED_ORDERBOOK_UPDATE_FIRST_FULL_BOOK: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"eth-up-or-down-daily-1788105600","orderbook":{"bids":[{"price":0.012,"size":83334000,"side":"BUY"},{"price":0.011,"size":100000000,"side":"BUY"},{"price":0.01,"size":21000000,"side":"BUY"},{"price":0.009,"size":1000000,"side":"BUY"},{"price":0.008,"size":1000000,"side":"BUY"},{"price":0.007,"size":1000000,"side":"BUY"},{"price":0.006,"size":167000000,"side":"BUY"},{"price":0.005,"size":1201000000,"side":"BUY"},{"price":0.002,"size":50000000,"side":"BUY"},{"price":0.001,"size":2000000000,"side":"BUY"}],"asks":[{"price":0.219,"size":100000000,"side":"SELL"},{"price":0.22,"size":12000000,"side":"SELL"},{"price":0.239,"size":100000000,"side":"SELL"},{"price":0.249,"size":100000000,"side":"SELL"},{"price":0.259,"size":100000000,"side":"SELL"},{"price":0.27,"size":5000000,"side":"SELL"},{"price":0.279,"size":100000000,"side":"SELL"},{"price":0.293,"size":100000000,"side":"SELL"},{"price":0.306,"size":1441000,"side":"SELL"},{"price":0.65,"size":185714000,"side":"SELL"},{"price":0.969,"size":50000000,"side":"SELL"},{"price":0.989,"size":100000000,"side":"SELL"},{"price":0.99,"size":21000000,"side":"SELL"},{"price":0.991,"size":1000000,"side":"SELL"},{"price":0.992,"size":1000000,"side":"SELL"},{"price":0.993,"size":1000000,"side":"SELL"},{"price":0.994,"size":1000000,"side":"SELL"},{"price":0.995,"size":1000000,"side":"SELL"},{"price":0.998,"size":550000000,"side":"SELL"},{"price":0.999,"size":2000000000,"side":"SELL"}],"tokenId":"25018063611559838047404811982184442876005199660833597814711111046007291893507","adjustedMidpoint":0.115,"midpoint":0.1155,"maxSpread":0.035,"minSize":100000000},"version":7861372,"timestamp":"2026-08-31T07:12:32.741Z"}]"#;
const OBSERVED_ORACLE_PRICE_DATA: &str = r#"42/markets,["oraclePriceData",{"marketAddress":null,"marketSlug":"eth-up-or-down-daily-1788105600","source":"pyth-pro","timestamp":1788160353000,"value":2440.91810993}]"#;

type FrameCase = (&'static str, &'static str, &'static str);

const FRAME_CASES: &[FrameCase] = &[
    (
        "observed-engineio-open",
        OBSERVED_ENGINEIO_OPEN,
        "Ok engine_io=Some(Open) socket_io=None ns=None ack=None attachments=0 event=None payload_kind=Some(Object) extra=0",
    ),
    (
        "observed-namespace-connect-ack",
        OBSERVED_NAMESPACE_CONNECT_ACK,
        "Ok engine_io=Some(Message) socket_io=Some(Connect) ns=Some(\"/markets\") ack=None attachments=0 event=None payload_kind=Some(Object) extra=0",
    ),
    (
        "observed-system-registered",
        OBSERVED_SYSTEM_REGISTERED,
        "Ok engine_io=Some(Message) socket_io=Some(Event) ns=Some(\"/markets\") ack=None attachments=0 event=Some(\"system\") payload_kind=Some(Object) extra=0",
    ),
    (
        "observed-system-subscribed",
        OBSERVED_SYSTEM_SUBSCRIBED,
        "Ok engine_io=Some(Message) socket_io=Some(Event) ns=Some(\"/markets\") ack=None attachments=0 event=Some(\"system\") payload_kind=Some(Object) extra=0",
    ),
    (
        "observed-orderbook-update-first-full-book",
        OBSERVED_ORDERBOOK_UPDATE_FIRST_FULL_BOOK,
        "Ok engine_io=Some(Message) socket_io=Some(Event) ns=Some(\"/markets\") ack=None attachments=0 event=Some(\"orderbookUpdate\") payload_kind=Some(Object) extra=0",
    ),
    (
        "observed-oracle-price-data",
        OBSERVED_ORACLE_PRICE_DATA,
        "Ok engine_io=Some(Message) socket_io=Some(Event) ns=Some(\"/markets\") ack=None attachments=0 event=Some(\"oraclePriceData\") payload_kind=Some(Object) extra=0",
    ),
    (
        "observed-market-resolved-own-room",
        OBSERVED_MARKET_RESOLVED_OWN_ROOM,
        "Ok engine_io=Some(Message) socket_io=Some(Event) ns=Some(\"/markets\") ack=None attachments=0 event=Some(\"marketResolved\") payload_kind=Some(Object) extra=0",
    ),
    (
        "observed-orderbook-update-last-before-resolution",
        OBSERVED_ORDERBOOK_UPDATE_LAST_BEFORE_RESOLUTION,
        "Ok engine_io=Some(Message) socket_io=Some(Event) ns=Some(\"/markets\") ack=None attachments=0 event=Some(\"orderbookUpdate\") payload_kind=Some(Object) extra=0",
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
            frame
                .payload()
                .map(pm_ws::wire::lexical::LexicalValue::kind),
            frame.extra_arguments().len(),
        ),
        Err(error) => format!("Err {error:?}"),
    }
}

fn decode(packet: &str) -> DecodedFrame {
    decode_frame(
        packet.as_bytes(),
        WebSocketOpcode::Text,
        LexicalLimits::venue_payload(),
    )
    .expect("observed capture frame decodes at the wire layer")
}

/// Frames retained from two live connections to `wss://ws.limitless.exchange`: a 60 s
/// session on 2026-08-31 subscribed to the CLOB market `eth-up-or-down-daily-1788105600`,
/// and a 420 s session on 2026-09-01 subscribed to `btc-up-or-down-5-min-1788267900` that
/// spanned the market's resolution. Session identifiers are redacted; every other byte,
/// including venue-native `tokenId`, is reproduced unchanged. See `docs/limitless.md` for
/// the observations these frames back.
#[test]
fn wire_observed_frames_decode_to_pinned_classification() {
    for (name, frame, expected) in FRAME_CASES {
        let outcome = decode_frame(
            frame.as_bytes(),
            WebSocketOpcode::Text,
            LexicalLimits::venue_payload(),
        );
        assert_eq!(
            &summarize(&outcome),
            expected,
            "observed frame {name} reclassified"
        );
    }
}

#[test]
fn wire_observed_engineio_open_parses_to_pinned_session_values() {
    let frame = decode(OBSERVED_ENGINEIO_OPEN);
    let payload = frame.payload().expect("open packet carries a payload");
    let open = EngineIoOpen::from_open_payload(payload).expect("observed open payload is valid");
    assert_eq!(open.sid(), "REPLAY-SESSION-0002");
    assert_eq!(open.ping_interval_ms(), 25_000);
    assert_eq!(open.ping_timeout_ms(), 60_000);
    assert_eq!(open.max_payload_bytes(), 1_000_000);
}

#[test]
fn wire_observed_orderbook_update_decodes_to_pinned_book() {
    let frame = decode(OBSERVED_ORDERBOOK_UPDATE_FIRST_FULL_BOOK);
    let event = decode_event(&frame).expect("observed orderbookUpdate decodes");
    let LimitlessEvent::OrderbookUpdate(update) = event else {
        panic!("expected orderbookUpdate");
    };
    assert_eq!(update.market_slug(), "eth-up-or-down-daily-1788105600");
    assert_eq!(
        update.token_id(),
        Some("25018063611559838047404811982184442876005199660833597814711111046007291893507")
    );
    assert_eq!(update.timestamp(), "2026-08-31T07:12:32.741Z");
    assert_eq!(update.bids().len(), 10);
    assert_eq!(update.asks().len(), 20);
    let (best_bid_price, best_bid_size) = &update.bids()[0];
    assert_eq!(best_bid_price.value().canonical(), "0.012");
    assert_eq!(best_bid_size.value().canonical(), "83334000");
    let (best_ask_price, best_ask_size) = &update.asks()[0];
    assert_eq!(best_ask_price.value().canonical(), "0.219");
    assert_eq!(best_ask_size.value().canonical(), "100000000");
    let (last_bid_price, last_bid_size) = update.bids().last().expect("bids present");
    assert_eq!(last_bid_price.value().canonical(), "0.001");
    assert_eq!(last_bid_size.value().canonical(), "2000000000");
}

/// The observed frame's top-level `version` reaches both surfaces that carry it, exactly as
/// the venue wrote it: the provenance evidence keeps the lexeme, and the dedup key keeps
/// the integer. Neither is derived from the other's rounding.
#[test]
fn wire_observed_orderbook_update_carries_the_exact_version() {
    let frame = decode(OBSERVED_ORDERBOOK_UPDATE_FIRST_FULL_BOOK);
    let LimitlessEvent::OrderbookUpdate(update) =
        decode_event(&frame).expect("observed orderbookUpdate decodes")
    else {
        panic!("expected orderbookUpdate");
    };
    assert_eq!(update.version(), Some("7861372"));
    assert_eq!(update.dedup_key(), Ok(Some(DedupKey::integer(7_861_372))));
    let evidence = update
        .version_evidence()
        .expect("the observed version is within the evidence bound")
        .expect("the observed frame carries a version");
    let SourceEvidence::Version(value) = &evidence else {
        panic!("the version reaches provenance as version evidence, not as another kind");
    };
    assert_eq!(value.as_str(), "7861372");
}

/// What the venue's `version` is declared to mean, pinned against `docs/limitless.md`:
/// monotone within one connection-session and nothing more. A change here is a claim about
/// the venue and needs conformance evidence, not a passing run.
#[test]
fn wire_observed_version_is_declared_session_monotone_only() {
    assert_eq!(ORDERBOOK_UPDATE_DEDUP_KEY.venue(), "limitless");
    assert_eq!(ORDERBOOK_UPDATE_DEDUP_KEY.family(), "orderbookUpdate");
    assert_eq!(ORDERBOOK_UPDATE_DEDUP_KEY.field(), "version");
    assert_eq!(
        ORDERBOOK_UPDATE_DEDUP_KEY.semantics(),
        DedupKeySemantics::SessionMonotone
    );
    assert!(
        !ORDERBOOK_UPDATE_DEDUP_KEY
            .semantics()
            .orders_across_connections(),
        "no venue may claim cross-connection ordering without documentation or conformance proof"
    );
}

#[test]
fn wire_observed_oracle_price_data_decodes_to_unknown_event() {
    let frame = decode(OBSERVED_ORACLE_PRICE_DATA);
    let event = decode_event(&frame).expect("observed oraclePriceData decodes");
    assert_eq!(
        event,
        LimitlessEvent::Unknown {
            name: "oraclePriceData".to_owned()
        }
    );
}

#[test]
fn wire_observed_system_frames_decode_to_unknown_event() {
    for packet in [OBSERVED_SYSTEM_REGISTERED, OBSERVED_SYSTEM_SUBSCRIBED] {
        let frame = decode(packet);
        let event = decode_event(&frame).expect("observed system frame decodes");
        assert_eq!(
            event,
            LimitlessEvent::Unknown {
                name: "system".to_owned()
            }
        );
    }
}

/// The observed resolution, delivered to the market's own room, reproduced field for
/// field. This frame arrived three times byte-identically within 200 ms; the fixture keeps
/// one copy and `docs/limitless.md` records the redundancy.
#[test]
fn wire_observed_market_resolved_decodes_to_pinned_fields() {
    let frame = decode(OBSERVED_MARKET_RESOLVED_OWN_ROOM);
    let event = decode_event(&frame).expect("observed marketResolved decodes");
    let LimitlessEvent::MarketResolved(resolved) = event else {
        panic!("expected marketResolved");
    };
    assert_eq!(resolved.slug(), "btc-up-or-down-5-min-1788267900");
    assert_eq!(resolved.market_type(), "CLOB");
    assert_eq!(resolved.winning_outcome(), "NO");
    assert_eq!(resolved.winning_index(), 1);
    assert_eq!(resolved.resolution_date(), "2026-09-01T13:11:02.813Z");
}

/// The last book update the venue sent before the observed resolution, from the same
/// session and market, kept as the resolution replay's companion frame.
#[test]
fn wire_observed_last_orderbook_update_before_resolution_decodes() {
    let frame = decode(OBSERVED_ORDERBOOK_UPDATE_LAST_BEFORE_RESOLUTION);
    let LimitlessEvent::OrderbookUpdate(update) =
        decode_event(&frame).expect("observed orderbookUpdate decodes")
    else {
        panic!("expected orderbookUpdate");
    };
    assert_eq!(update.market_slug(), "btc-up-or-down-5-min-1788267900");
    assert_eq!(update.version(), Some("504852"));
    assert_eq!(update.timestamp(), "2026-09-01T13:09:45.277Z");
    assert_eq!(update.bids().len(), 1);
    assert_eq!(update.asks().len(), 1);
    let (bid_price, bid_size) = &update.bids()[0];
    assert_eq!(bid_price.value().canonical(), "0.002");
    assert_eq!(bid_size.value().canonical(), "50000000");
    let (ask_price, ask_size) = &update.asks()[0];
    assert_eq!(ask_price.value().canonical(), "0.998");
    assert_eq!(ask_size.value().canonical(), "50000000");
}

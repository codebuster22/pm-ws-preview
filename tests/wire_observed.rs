mod support;

use pm_ws::wire::lexical::LexicalLimits;
use pm_ws::wire::session::EngineIoOpen;
use pm_ws::wire::socketio::{DecodedFrame, FrameError, WebSocketOpcode, decode_frame};
use support::observed_frames::{
    OBSERVED_ENGINEIO_OPEN, OBSERVED_MARKET_RESOLVED_OWN_ROOM, OBSERVED_NAMESPACE_CONNECT_ACK,
    OBSERVED_ORACLE_PRICE_DATA, OBSERVED_ORDERBOOK_UPDATE_FIRST_FULL_BOOK,
    OBSERVED_ORDERBOOK_UPDATE_LAST_BEFORE_RESOLUTION, OBSERVED_SYSTEM_REGISTERED,
    OBSERVED_SYSTEM_SUBSCRIBED,
};

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

/// Every retained frame keeps the Socket.IO and Engine.IO classification it had when captured.
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

#![forbid(unsafe_code)]

//! Smoke test for `support::controlled_peer`: proves a plain tokio-tungstenite client
//! observes exactly what the daemon observes when talking to the real venue — the same
//! Engine.IO open, namespace ack, and `orderbookUpdate` frames, decoded by the crate's
//! own decoders.
mod support;

use futures_util::{SinkExt, StreamExt};
use pm_ws::limitless::{LimitlessEvent, decode_event};
use pm_ws::wire::lexical::LexicalLimits;
use pm_ws::wire::session::{
    EngineIoOpen, encode_engine_pong, encode_namespace_connect, encode_subscribe_market_prices,
};
use pm_ws::wire::socketio::{DecodedFrame, SocketIoPacket, WebSocketOpcode, decode_frame};
use pm_ws::{Price, Quantity};
use std::time::Duration;
use support::controlled_peer::{ControlledPeer, PeerConfig};
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::protocol::Message;

const NAMESPACE: &str = "/markets";
const STEP_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn controlled_peer_speaks_the_daemon_dialect() {
    let mut peer = ControlledPeer::start(PeerConfig::default()).await;
    let endpoint = peer.endpoint();
    let slug = "btc-up-or-down-5-min-test".to_owned();
    let server_slug = slug.clone();

    let server = tokio::spawn(async move {
        let mut connection = peer.next_connection().await;
        let subscription = connection.complete_handshake().await;
        assert_eq!(subscription.slugs, vec![server_slug.clone()]);
        connection
            .send_orderbook(
                &server_slug,
                &[("0.51", "120"), ("0.5", "340.5")],
                &[("0.52", "75"), ("0.53", "10")],
                Some(7),
            )
            .await;
        connection.send_ping().await;
        connection.expect_pong(STEP_TIMEOUT).await;
        connection.close().await;
    });

    let (mut client, _response) = tokio_tungstenite::connect_async(endpoint.as_str())
        .await
        .expect("client connects to the controlled peer");
    let open_frame = decode(&expect_client_text(&mut client, "engine.io open").await);
    let open_payload = open_frame.payload().expect("open packet carries a payload");
    let open = EngineIoOpen::from_open_payload(open_payload)
        .expect("controlled peer open payload is policy-valid");
    let default_config = PeerConfig::default();
    assert_eq!(open.ping_interval_ms(), default_config.ping_interval_ms);
    assert_eq!(open.ping_timeout_ms(), default_config.ping_timeout_ms);
    assert_eq!(open.max_payload_bytes(), default_config.max_payload_bytes);
    client
        .send(Message::text(encode_namespace_connect(NAMESPACE)))
        .await
        .expect("send namespace connect");
    let ack_frame = decode(&expect_client_text(&mut client, "namespace connect ack").await);
    assert_eq!(ack_frame.socket_io(), Some(SocketIoPacket::Connect));
    assert_eq!(ack_frame.namespace(), Some(NAMESPACE));
    let subscribe = encode_subscribe_market_prices(NAMESPACE, std::slice::from_ref(&slug));
    client
        .send(Message::text(subscribe))
        .await
        .expect("send subscribe_market_prices");
    let ack = decode(&expect_client_text(&mut client, "subscription acknowledgment").await);
    assert_eq!(
        ack.event_name(),
        Some("system"),
        "the peer answers a subscription the way the venue does"
    );
    let update_frame = decode(&expect_client_text(&mut client, "orderbookUpdate").await);
    let event = decode_event(&update_frame).expect("orderbookUpdate event decodes");
    let LimitlessEvent::OrderbookUpdate(update) = event else {
        panic!("expected orderbookUpdate");
    };
    assert_eq!(update.market_slug(), slug);
    assert_eq!(update.version(), Some("7"));
    assert_levels(update.bids(), &[("0.51", "120"), ("0.5", "340.5")]);
    assert_levels(update.asks(), &[("0.52", "75"), ("0.53", "10")]);

    let ping_text = expect_client_text(&mut client, "engine.io ping").await;
    assert_eq!(ping_text, "2");
    client
        .send(Message::text(encode_engine_pong()))
        .await
        .expect("send engine.io pong");

    server.await.expect("controlled peer task completes");
}

#[tokio::test]
async fn drop_abruptly_ends_the_client_read_stream() {
    let mut peer = ControlledPeer::start(PeerConfig::default()).await;
    let endpoint = peer.endpoint();

    let server = tokio::spawn(async move {
        let connection = peer.next_connection().await;
        connection.drop_abruptly().await;
    });

    let (mut client, _response) = tokio_tungstenite::connect_async(endpoint.as_str())
        .await
        .expect("client connects to the controlled peer");
    expect_client_text(&mut client, "engine.io open").await;

    server.await.expect("controlled peer task completes");

    let result = tokio::time::timeout(STEP_TIMEOUT, client.next())
        .await
        .expect("client read stream ends promptly after drop_abruptly");
    assert!(
        matches!(result, None | Some(Err(_))),
        "expected the client read stream to end after drop_abruptly, got {result:?}"
    );
}

fn decode(text: &str) -> DecodedFrame {
    decode_frame(
        text.as_bytes(),
        WebSocketOpcode::Text,
        LexicalLimits::venue_payload(),
    )
    .unwrap_or_else(|error| panic!("client: frame does not decode: {error:?}"))
}

fn assert_levels(levels: &[(Price, Quantity)], expected: &[(&str, &str)]) {
    let actual: Vec<(String, String)> = levels
        .iter()
        .map(|(price, quantity)| (price.value().canonical(), quantity.value().canonical()))
        .collect();
    let expected: Vec<(String, String)> = expected
        .iter()
        .map(|(price, size)| ((*price).to_owned(), (*size).to_owned()))
        .collect();
    assert_eq!(actual, expected);
}

async fn expect_client_text<S>(stream: &mut S, step: &str) -> String
where
    S: futures_util::Stream<Item = Result<Message, WsError>> + Unpin,
{
    match tokio::time::timeout(STEP_TIMEOUT, stream.next()).await {
        Err(_) => panic!("client: timed out waiting for {step}"),
        Ok(None) => panic!("client: stream ended waiting for {step}"),
        Ok(Some(Err(error))) => panic!("client: read error waiting for {step}: {error}"),
        Ok(Some(Ok(Message::Text(text)))) => text.as_str().to_owned(),
        Ok(Some(Ok(_))) => panic!("client: expected a text frame for {step}"),
    }
}

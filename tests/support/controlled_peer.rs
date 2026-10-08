#![forbid(unsafe_code)]

//! A scripted local WebSocket server that impersonates the Limitless Socket.IO `/markets`
//! feed, for controlled-peer fault-injection tests. Speaks the same Engine.IO/Socket.IO
//! dialect the daemon's client decodes: an `open` packet on accept, a namespace connect
//! ack, a parsed `subscribe_market_prices` request, and `orderbookUpdate` events built
//! from caller-supplied decimal-string levels. Every blocking step is bounded by a
//! timeout that panics naming the step on expiry, so a wedged test fails loudly instead
//! of hanging.

use futures_util::{SinkExt, StreamExt};
use pm_ws::wire::lexical::{LexicalLimits, LexicalValue};
use pm_ws::wire::session::encode_engine_pong;
use pm_ws::wire::socketio::{SocketIoPacket, WebSocketOpcode, decode_frame};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::protocol::Message;

const NAMESPACE: &str = "/markets";
const ACCEPT_TIMEOUT: Duration = Duration::from_secs(5);
const HANDSHAKE_STEP_TIMEOUT: Duration = Duration::from_secs(5);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
const ORDERBOOK_TIMESTAMP: &str = "2026-08-31T00:00:00.000Z";

/// Engine.IO session parameters a [`ControlledPeer`] announces in its `open` packet.
///
/// `ping_interval_ms` and `ping_timeout_ms` default to the 1000 ms policy floor
/// `pm_ws::wire::session::EngineIoOpen` enforces on both fields — the smallest values a
/// decoded open packet can carry without being rejected as out of policy.
/// `max_payload_bytes` defaults to the value observed from the live venue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PeerConfig {
    pub ping_interval_ms: u64,
    pub ping_timeout_ms: u64,
    pub max_payload_bytes: u64,
}

impl Default for PeerConfig {
    fn default() -> Self {
        Self {
            ping_interval_ms: 1_000,
            ping_timeout_ms: 1_000,
            max_payload_bytes: 1_000_000,
        }
    }
}

/// A scripted local WebSocket server bound to an ephemeral `127.0.0.1` port.
///
/// Each accepted connection is handed to the caller as a [`PeerConnection`] with the
/// Engine.IO `open` packet already sent; the caller drives every later protocol step
/// explicitly, so a test controls exactly what the venue "says" and when.
pub struct ControlledPeer {
    listener: TcpListener,
    port: u16,
    config: PeerConfig,
    next_session_id: u64,
}

impl ControlledPeer {
    /// Binds `127.0.0.1:0` and returns once the ephemeral port is known.
    pub async fn start(config: PeerConfig) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("controlled peer: bind 127.0.0.1:0");
        let port = listener
            .local_addr()
            .expect("controlled peer: read bound local address")
            .port();
        Self {
            listener,
            port,
            config,
            next_session_id: 0,
        }
    }

    /// The `ws://` URL a client dials, matching the daemon's path and query shape
    /// (`/socket.io/?EIO=4&transport=websocket`) against this server's bound port.
    pub fn endpoint(&self) -> String {
        let port = self.port;
        format!("ws://127.0.0.1:{port}/socket.io/?EIO=4&transport=websocket")
    }

    /// Accepts the next client connection, completes the WebSocket handshake, and sends
    /// the Engine.IO `open` packet built from this peer's [`PeerConfig`].
    ///
    /// Panics naming the step if no client connects, or the WebSocket handshake does not
    /// finish, within `ACCEPT_TIMEOUT`.
    pub async fn next_connection(&mut self) -> PeerConnection {
        let (tcp, _peer_addr) = timeout(ACCEPT_TIMEOUT, self.listener.accept())
            .await
            .unwrap_or_else(|_| {
                panic!("controlled peer: no client connected within {ACCEPT_TIMEOUT:?}")
            })
            .expect("controlled peer: tcp accept failed");
        let ws = timeout(ACCEPT_TIMEOUT, accept_async(tcp))
            .await
            .unwrap_or_else(|_| {
                panic!("controlled peer: websocket handshake timed out after {ACCEPT_TIMEOUT:?}")
            })
            .expect("controlled peer: websocket handshake failed");
        self.next_session_id += 1;
        let mut connection = PeerConnection {
            ws,
            session_id: self.next_session_id,
        };
        connection.send_open(self.config).await;
        connection
    }

    /// Whether a client is already waiting to be accepted, without waiting for one.
    ///
    /// Polls the listener exactly once. This proves an ordering — that the daemon had not
    /// yet dialled another connection at the moment it is asked — rather than measuring any
    /// interval. A connection it finds is consumed and dropped, because the only caller is
    /// an assertion that there is none.
    pub async fn has_pending_connection(&mut self) -> bool {
        timeout(Duration::ZERO, self.listener.accept())
            .await
            .is_ok()
    }

    /// Waits `within` for a connection that must not arrive, and panics if one does.
    ///
    /// The negative half of [`Self::next_connection`]: it proves an idempotent command
    /// dialled nothing, by the absence of a socket rather than by a daemon-side counter. The
    /// window must be longer than the caller's configured command floor, or a dial that was
    /// going to happen may simply not have happened yet.
    pub async fn expect_no_connection(&mut self, within: Duration) {
        if timeout(within, self.listener.accept()).await.is_ok() {
            panic!("controlled peer: a connection was dialled inside {within:?}");
        }
    }
}

/// The `marketSlugs` a client requested via `subscribe_market_prices`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscriptionRequest {
    pub slugs: Vec<String>,
}

/// One accepted WebSocket connection, scripted from the server side of the Limitless
/// `/markets` dialect.
pub struct PeerConnection {
    ws: WebSocketStream<TcpStream>,
    session_id: u64,
}

impl PeerConnection {
    async fn send_open(&mut self, config: PeerConfig) {
        let sid = self.engine_sid();
        let ping_interval_ms = config.ping_interval_ms;
        let ping_timeout_ms = config.ping_timeout_ms;
        let max_payload_bytes = config.max_payload_bytes;
        let text = format!(
            "0{{\"sid\":\"{sid}\",\"upgrades\":[],\"pingInterval\":{ping_interval_ms},\"pingTimeout\":{ping_timeout_ms},\"maxPayload\":{max_payload_bytes}}}"
        );
        self.send_raw(&text).await;
    }

    /// The Engine.IO session id this connection announced in its `open` packet.
    ///
    /// It is unique per accepted connection, and the daemon reports it back on its
    /// diagnostics tap, so a test can tell which of several concurrent connections the
    /// daemon assigned to which replica role without depending on accept ordering.
    pub fn engine_sid(&self) -> String {
        let session_id = self.session_id;
        format!("controlled-peer-engine-session-{session_id}")
    }

    fn namespace_sid(&self) -> String {
        let session_id = self.session_id;
        format!("controlled-peer-namespace-session-{session_id}")
    }

    /// Awaits the client's `40/markets,` namespace connect and acks it with
    /// `40/markets,{"sid":"..."}`, leaving the subscription still to come.
    ///
    /// Splitting this out of [`Self::complete_handshake`] lets a test hold several clients
    /// at the same point of their handshakes and then release them together, which is how
    /// converging handshakes are scripted.
    ///
    /// Panics naming the step if the connect does not arrive, or does not decode, within
    /// `HANDSHAKE_STEP_TIMEOUT`.
    pub async fn complete_namespace(&mut self) {
        self.await_namespace_connect().await;
        let sid = self.namespace_sid();
        let ack = format!("40{NAMESPACE},{{\"sid\":\"{sid}\"}}");
        self.send_raw(&ack).await;
    }

    /// Completes the namespace handshake, awaits the client's `subscribe_market_prices`
    /// emit, and acknowledges it as the venue does.
    ///
    /// Panics naming the step if either frame does not arrive, or does not decode, within
    /// `HANDSHAKE_STEP_TIMEOUT`.
    pub async fn complete_handshake(&mut self) -> SubscriptionRequest {
        self.complete_namespace().await;
        let request = self.await_subscription(HANDSHAKE_STEP_TIMEOUT).await;
        self.acknowledge_subscription(&request.slugs).await;
        request
    }

    /// Awaits one further `subscribe_market_prices` emit on this already-established
    /// connection, acknowledges it, and returns the set it carried.
    ///
    /// This is the same parse [`Self::complete_handshake`] performs on the first emit, so a
    /// re-emit is proven by the bytes on the wire rather than by a daemon-side counter.
    /// Panics naming the step if nothing matching arrives within `within`.
    pub async fn expect_resubscription(&mut self, within: Duration) -> SubscriptionRequest {
        let request = self.await_subscription(within).await;
        self.acknowledge_subscription(&request.slugs).await;
        request
    }

    /// Awaits one further `subscribe_market_prices` emit and returns it **without**
    /// acknowledging it, leaving the client's set boundary unestablished.
    ///
    /// This is the window a venue that has read a replacement but not yet answered it leaves
    /// open, and the only way to script what a client does with a frame that arrives inside
    /// it. Panics naming the step if nothing matching arrives within `within`.
    pub async fn expect_unacknowledged_resubscription(
        &mut self,
        within: Duration,
    ) -> SubscriptionRequest {
        self.await_subscription(within).await
    }

    /// Sends the venue's own subscription acknowledgment for `slugs`, matching the frame
    /// observed on 2026-08-31 and recorded in `docs/limitless.md`.
    pub async fn acknowledge_subscription(&mut self, slugs: &[String]) {
        let listed = slugs
            .iter()
            .map(|slug| format!("\"{slug}\""))
            .collect::<Vec<_>>()
            .join(",");
        let text = format!(
            "42{NAMESPACE},[\"system\",{{\"message\":\"Successfully subscribed to market price updates\",\"markets\":[{listed}]}}]"
        );
        self.send_raw(&text).await;
    }

    /// Waits `within` for a `subscribe_market_prices` emit that must not arrive, and
    /// panics naming the set if one does.
    ///
    /// This is the negative half of [`Self::expect_resubscription`]: it proves an
    /// idempotent desired-state command put nothing on the wire, by the bytes rather than
    /// by a daemon-side counter. It consumes every other frame the client sends meanwhile,
    /// so a caller that also needs to observe a pong reads it before calling this.
    pub async fn expect_no_subscription(&mut self, within: Duration) {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return;
            }
            let read = timeout(remaining, self.ws.next()).await;
            let text = match read {
                Err(_) => return,
                Ok(None | Some(Err(_)) | Some(Ok(Message::Close(_)))) => return,
                Ok(Some(Ok(Message::Text(text)))) => text.as_str().to_owned(),
                Ok(Some(Ok(_))) => continue,
            };
            let frame = decode_frame(
                text.as_bytes(),
                WebSocketOpcode::Text,
                LexicalLimits::venue_payload(),
            );
            let Ok(frame) = frame else { continue };
            assert!(
                frame.event_name() != Some("subscribe_market_prices"),
                "controlled peer: expected no subscribe_market_prices within {within:?}, got {text}"
            );
        }
    }

    async fn await_namespace_connect(&mut self) {
        loop {
            let text = self
                .read_text_frame(HANDSHAKE_STEP_TIMEOUT)
                .await
                .unwrap_or_else(|| {
                    panic!("controlled peer: connection closed awaiting namespace connect")
                });
            let frame = decode_frame(
                text.as_bytes(),
                WebSocketOpcode::Text,
                LexicalLimits::venue_payload(),
            )
            .unwrap_or_else(|error| {
                panic!("controlled peer: undecodable frame awaiting namespace connect: {error:?}")
            });
            if frame.socket_io() == Some(SocketIoPacket::Connect)
                && frame.namespace() == Some(NAMESPACE)
            {
                return;
            }
        }
    }

    async fn await_subscription(&mut self, within: Duration) -> SubscriptionRequest {
        loop {
            let text = self.read_text_frame(within).await.unwrap_or_else(|| {
                panic!("controlled peer: connection closed awaiting subscribe_market_prices")
            });
            let frame = decode_frame(
                text.as_bytes(),
                WebSocketOpcode::Text,
                LexicalLimits::venue_payload(),
            )
            .unwrap_or_else(|error| {
                panic!(
                    "controlled peer: undecodable frame awaiting subscribe_market_prices: {error:?}"
                )
            });
            if frame.event_name() != Some("subscribe_market_prices") {
                continue;
            }
            let payload = frame.payload().unwrap_or_else(|| {
                panic!("controlled peer: subscribe_market_prices carries no payload")
            });
            let slugs = payload
                .field("marketSlugs")
                .and_then(LexicalValue::as_array)
                .unwrap_or_else(|| {
                    panic!("controlled peer: subscribe_market_prices missing marketSlugs array")
                })
                .iter()
                .map(|value| {
                    value
                        .as_text()
                        .unwrap_or_else(|| {
                            panic!("controlled peer: marketSlugs entry is not a string")
                        })
                        .to_owned()
                })
                .collect();
            return SubscriptionRequest { slugs };
        }
    }

    /// Sends one `orderbookUpdate` event for `slug`.
    ///
    /// `bids` and `asks` are `(price, size)` decimal-string pairs, embedded as bare JSON
    /// number lexemes verbatim — never parsed through a float. Bids must already be
    /// strictly descending and asks strictly ascending, matching the venue's own
    /// contract; this builder does not reorder or validate them. `version` becomes the
    /// event's top-level `version` field when present, and is omitted entirely when
    /// `None`.
    pub async fn send_orderbook(
        &mut self,
        slug: &str,
        bids: &[(&str, &str)],
        asks: &[(&str, &str)],
        version: Option<u64>,
    ) {
        let bids_json = levels_json(bids);
        let asks_json = levels_json(asks);
        let mut text = format!(
            "42{NAMESPACE},[\"orderbookUpdate\",{{\"marketSlug\":\"{slug}\",\"orderbook\":{{\"bids\":{bids_json},\"asks\":{asks_json}}},\"timestamp\":\"{ORDERBOOK_TIMESTAMP}\""
        );
        if let Some(version) = version {
            text.push_str(&format!(",\"version\":{version}"));
        }
        text.push_str("}]");
        self.send_raw(&text).await;
    }

    /// Sends one `marketResolved` event on `/markets`, reproducing the venue's own field
    /// names and shape.
    ///
    /// `winning_outcome`, `market_type` and `resolution_date` are placed verbatim, and
    /// `winning_index` is written as a bare integer, exactly as the venue publishes them.
    pub async fn send_market_resolved(
        &mut self,
        slug: &str,
        market_type: &str,
        winning_outcome: &str,
        winning_index: u32,
        resolution_date: &str,
    ) {
        self.send_raw(&format!(
            "42{NAMESPACE},[\"marketResolved\",{{\"slug\":\"{slug}\",\"type\":\"{market_type}\",\"winningOutcome\":\"{winning_outcome}\",\"winningIndex\":{winning_index},\"resolutionDate\":\"{resolution_date}\"}}]"
        ))
        .await;
    }

    /// Sends `text` as one WebSocket text frame, verbatim.
    ///
    /// Panics naming the step if the write does not complete within `WRITE_TIMEOUT`.
    pub async fn send_raw(&mut self, text: &str) {
        timeout(WRITE_TIMEOUT, self.ws.send(Message::text(text.to_owned())))
            .await
            .unwrap_or_else(|_| {
                panic!("controlled peer: send timed out after {WRITE_TIMEOUT:?}: {text}")
            })
            .unwrap_or_else(|error| panic!("controlled peer: send failed: {error}"));
    }

    /// Sends an Engine.IO ping (`2`). The harness never sends this on its own timer; a
    /// test calls it exactly when it wants the client to see a ping.
    pub async fn send_ping(&mut self) {
        self.send_raw("2").await;
    }

    /// Awaits the client's Engine.IO pong (`3`) within `within`.
    ///
    /// Panics naming what arrived instead, on a wrong frame, a closed connection, or a
    /// timeout.
    pub async fn expect_pong(&mut self, within: Duration) {
        match self.read_text_frame(within).await {
            Some(text) if text == encode_engine_pong() => {}
            Some(other) => panic!("controlled peer: expected engine.io pong, got {other:?}"),
            None => panic!("controlled peer: expected engine.io pong, connection closed"),
        }
    }

    /// Reads the next WebSocket text frame within `within`, skipping any non-text frame.
    /// Returns `None` on a close frame, a read error, or stream end; panics naming the
    /// timeout if nothing arrives within `within`.
    pub async fn read_text_frame(&mut self, within: Duration) -> Option<String> {
        timeout(within, async {
            loop {
                match self.ws.next().await {
                    None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return None,
                    Some(Ok(Message::Text(text))) => return Some(text.as_str().to_owned()),
                    Some(Ok(_)) => continue,
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("controlled peer: read_text_frame timed out after {within:?}"))
    }

    /// Sends a WebSocket close frame and waits for the flush, then drops the connection.
    ///
    /// Panics naming the step on a send failure or timeout.
    pub async fn close(mut self) {
        timeout(CLOSE_TIMEOUT, self.ws.close(None))
            .await
            .unwrap_or_else(|_| panic!("controlled peer: close timed out after {CLOSE_TIMEOUT:?}"))
            .expect("controlled peer: close frame send failed");
    }

    /// Drops the underlying TCP connection immediately, without sending a WebSocket close
    /// frame — simulating a crashed or network-severed peer.
    pub async fn drop_abruptly(self) {
        drop(self.ws.into_inner());
    }
}

fn levels_json(levels: &[(&str, &str)]) -> String {
    let mut json = String::from("[");
    for (index, (price, size)) in levels.iter().enumerate() {
        if index > 0 {
            json.push(',');
        }
        json.push_str(&format!("{{\"price\":{price},\"size\":{size}}}"));
    }
    json.push(']');
    json
}

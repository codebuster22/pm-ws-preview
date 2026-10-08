//! One real Limitless `/markets` Socket.IO peer without book construction.

use core::time::Duration;
use futures_util::{SinkExt, StreamExt};
use std::{collections::BTreeSet, sync::Arc};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot, watch},
    time::{Instant, sleep_until, timeout},
};
use tokio_tungstenite::tungstenite::Bytes;
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};

use crate::{
    etiquette::reserve_command_grant,
    limitless::native::{decimal_grammar, decode_native_frame},
    upstream::{ConnectionContext, MAX_INPUT_BYTES, NativeTarget, PeerConfig, PeerEnd},
    wire::{
        lexical::LexicalLimits,
        session::{
            EngineIoOpen, encode_engine_pong, encode_namespace_connect, terminal_frame_reason,
        },
        socketio::{
            DecodedFrame, EngineIoPacket, SocketIoPacket, WebSocketOpcode, decode_frame,
            decode_limitless_frame,
        },
    },
};

const NAMESPACE: &str = "/markets";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const COMMAND_CAPACITY: usize = 8;
const SUBSCRIPTION_ACK_TIMEOUT: Duration = Duration::from_secs(15);

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Runs one static desired-set Limitless source generation until it stops or loses evidence.
pub(crate) async fn run(
    config: PeerConfig,
    context: ConnectionContext,
    stop: watch::Receiver<bool>,
) -> Result<(), PeerEnd> {
    run_with_ack_timeout(config, context, stop, SUBSCRIPTION_ACK_TIMEOUT).await
}

async fn run_with_ack_timeout(
    config: PeerConfig,
    context: ConnectionContext,
    mut stop: watch::Receiver<bool>,
    subscription_ack_timeout: Duration,
) -> Result<(), PeerEnd> {
    let socket = tokio::select! {
        _ = stopped(&mut stop) => return Ok(()),
        socket = timeout(
        CONNECT_TIMEOUT,
        connect_async_with_config(
            config.endpoint.as_str(),
            Some(
                WebSocketConfig::default()
                    .max_message_size(Some(MAX_INPUT_BYTES))
                    .max_frame_size(Some(MAX_INPUT_BYTES)),
            ),
            true,
        ),
    ) => socket
    .map_err(|_| PeerEnd::ConnectTimedOut)?
    .map_err(|_| PeerEnd::ConnectFailed)?
    .0,
    };
    context.connected();
    let (write, mut read) = socket.split();
    let (commands, command_rx) = mpsc::channel(COMMAND_CAPACITY);
    let writer_stop = stop.clone();
    let observe_stop = stop.clone();
    let mut writer = tokio::spawn(writer(
        write,
        command_rx,
        writer_stop,
        config.endpoint.clone(),
        config.min_command_interval,
    ));
    let limits = LexicalLimits::venue_payload().with_max_bytes(MAX_INPUT_BYTES);
    let inbound = async {
        let open = wait_open(&mut read, &limits, &mut stop, &context, &commands).await?;
        context.control("engineio_open");
        queue(
            &commands,
            Command::text(encode_namespace_connect(NAMESPACE), false),
        )?;
        wait_namespace(&mut read, &limits, &mut stop, &context, &commands).await?;
        context.control("namespace_connect");
        let (subscription, receipt) = Command::confirmed(subscription(&config.targets), true);
        queue(&commands, subscription)?;
        match receipt.await {
            Ok(Ok(())) => context.subscription_sent(),
            Ok(Err(end)) => return Err(end),
            Err(_) => return Err(PeerEnd::WriteFailed),
        }
        if config.lifecycle {
            queue(&commands, Command::text(lifecycle_subscription(), true))?;
        }
        let expected = expected_targets(&config.targets);
        read_events(
            &mut read,
            &mut stop,
            &context,
            &open,
            expected,
            &commands,
            subscription_ack_timeout,
            config.tolerate_partial_ack,
        )
        .await
    };
    tokio::pin!(inbound);
    let result = tokio::select! {
        result = &mut inbound => result,
        result = &mut writer => match result {
            Ok(Ok(())) if *observe_stop.borrow() => Ok(()),
            Ok(Err(error)) => Err(error),
            _ => Err(PeerEnd::WriteFailed),
        },
    };
    writer.abort();
    if !writer.is_finished() {
        let _ = writer.await;
    }
    result
}

async fn stopped(stop: &mut watch::Receiver<bool>) {
    while !*stop.borrow() {
        if stop.changed().await.is_err() {
            return;
        }
    }
}

struct Command {
    message: Option<Message>,
    paced: bool,
    done: Option<oneshot::Sender<Result<(), PeerEnd>>>,
}
impl Command {
    fn text(text: String, paced: bool) -> Self {
        Self {
            message: Some(Message::text(text)),
            paced,
            done: None,
        }
    }
    fn confirmed(text: String, paced: bool) -> (Self, oneshot::Receiver<Result<(), PeerEnd>>) {
        let (done, receipt) = oneshot::channel();
        (
            Self {
                message: Some(Message::text(text)),
                paced,
                done: Some(done),
            },
            receipt,
        )
    }
    fn flush() -> Self {
        Self {
            message: None,
            paced: false,
            done: None,
        }
    }
}

async fn writer(
    mut write: futures_util::stream::SplitSink<Socket, Message>,
    mut commands: mpsc::Receiver<Command>,
    mut stop: watch::Receiver<bool>,
    endpoint: String,
    interval: Duration,
) -> Result<(), PeerEnd> {
    loop {
        let command = tokio::select! {
            _ = stopped(&mut stop) => return Ok(()),
            command = commands.recv() => match command { Some(command) => command, None => return Ok(()) },
        };
        let Command {
            message,
            paced,
            done,
        } = command;
        if paced {
            let granted = reserve_command_grant(&endpoint, Instant::now(), interval);
            tokio::select! {
                _ = stopped(&mut stop) => return Ok(()),
                _ = sleep_until(granted) => {}
            }
        }
        let outcome = timeout(WRITE_TIMEOUT, async {
            match message {
                Some(message) => write.send(message).await,
                None => write.flush().await,
            }
        })
        .await
        .map_err(|_| PeerEnd::WriteFailed)
        .and_then(|result| result.map_err(|_| PeerEnd::WriteFailed));
        if let Some(done) = done {
            let _ = done.send(outcome);
        }
        outcome?;
    }
}

fn queue(commands: &mpsc::Sender<Command>, command: Command) -> Result<(), PeerEnd> {
    commands
        .try_send(command)
        .map_err(|_| PeerEnd::ControlOverload)
}

async fn wait_open(
    read: &mut futures_util::stream::SplitStream<Socket>,
    limits: &LexicalLimits,
    stop: &mut watch::Receiver<bool>,
    context: &ConnectionContext,
    commands: &mpsc::Sender<Command>,
) -> Result<EngineIoOpen, PeerEnd> {
    loop {
        let (frame, _, _, _) = timeout(
            CONNECT_TIMEOUT,
            next_frame(read, limits, stop, context, commands),
        )
        .await
        .map_err(|_| PeerEnd::ConnectTimedOut)??;
        if frame.engine_io() == Some(EngineIoPacket::Ping) {
            queue(commands, Command::text(encode_engine_pong(), false))?;
            context.control("engineio_pong_queued");
            continue;
        }
        if frame.engine_io() != Some(EngineIoPacket::Open) {
            context.fault("expected_engineio_open");
            return Err(PeerEnd::DecodeFailed);
        }
        return EngineIoOpen::from_open_payload(frame.payload().ok_or(PeerEnd::DecodeFailed)?)
            .map_err(|_| PeerEnd::DecodeFailed);
    }
}

async fn wait_namespace(
    read: &mut futures_util::stream::SplitStream<Socket>,
    limits: &LexicalLimits,
    stop: &mut watch::Receiver<bool>,
    context: &ConnectionContext,
    commands: &mpsc::Sender<Command>,
) -> Result<(), PeerEnd> {
    loop {
        let (frame, _, _, _) = timeout(
            CONNECT_TIMEOUT,
            next_frame(read, limits, stop, context, commands),
        )
        .await
        .map_err(|_| PeerEnd::VenueRejected)??;
        if terminal_frame_reason(&frame, NAMESPACE).is_some() {
            context.fault("namespace_rejected");
            return Err(PeerEnd::VenueRejected);
        }
        if frame.engine_io() == Some(EngineIoPacket::Ping) {
            queue(commands, Command::text(encode_engine_pong(), false))?;
            context.control("engineio_pong_queued");
            continue;
        }
        if frame.socket_io() == Some(SocketIoPacket::Connect)
            && frame.namespace() == Some(NAMESPACE)
        {
            return Ok(());
        }
        context.control("handshake_control");
    }
}

/// Reads one acknowledged generation until it stops or loses evidence.
///
/// The venue's `system` acknowledgement names the markets it registered. An acknowledgement
/// equal to `expected` covers every target. An acknowledgement naming a market that was never
/// requested is always a fault and rejects the venue. An acknowledgement missing requested
/// markets rejects the venue too, unless `tolerate_partial_ack` is set: then the acknowledged
/// markets are covered, the missing ones stay uncovered, the per-market control counts are kept
/// and one `subscription_ack_partial` control is recorded instead of a fault. Only the first
/// acknowledgement of a generation is read either way.
#[allow(clippy::too_many_arguments)]
async fn read_events(
    read: &mut futures_util::stream::SplitStream<Socket>,
    stop: &mut watch::Receiver<bool>,
    context: &ConnectionContext,
    open: &EngineIoOpen,
    expected: BTreeSet<Arc<str>>,
    commands: &mpsc::Sender<Command>,
    subscription_ack_timeout: Duration,
    tolerate_partial_ack: bool,
) -> Result<(), PeerEnd> {
    let limits = LexicalLimits::venue_payload().with_max_bytes(MAX_INPUT_BYTES);
    let mut last_ping = Instant::now();
    let mut covered = false;
    let subscription_deadline = Instant::now() + subscription_ack_timeout;
    loop {
        let deadline = last_ping + open.heartbeat_deadline();
        let (frame, input_bytes, received, decoded) = tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() { return Ok(()); }
                continue;
            }
            _ = sleep_until(subscription_deadline), if !covered => {
                context.fault("subscription_ack_timeout");
                return Err(PeerEnd::VenueRejected);
            }
            _ = sleep_until(deadline) => { context.fault("heartbeat_timeout"); return Err(PeerEnd::HeartbeatTimeout); }
            frame = next_frame_no_stop(read, &limits, context, commands, |bytes, opcode, limits| decode_limitless_frame(bytes, opcode, limits, decimal_grammar())) => frame?,
        };
        if terminal_frame_reason(&frame, NAMESPACE).is_some() {
            context.fault("terminal_frame");
            return Err(PeerEnd::SocketClosed);
        }
        if frame.engine_io() == Some(EngineIoPacket::Ping) {
            last_ping = Instant::now();
            context.heartbeat_received();
            context.control("engineio_ping");
            queue(commands, Command::text(encode_engine_pong(), false))?;
            context.control("engineio_pong_queued");
            continue;
        }
        let Some(batch) = decode_native_frame(frame, context.source(received), input_bytes)
            .map_err(|_| {
                context.malformed("native_decode");
                PeerEnd::DecodeFailed
            })?
        else {
            context.control("socketio_control");
            continue;
        };
        let valid = crate::upstream::StageStamp {
            wall_ns: context.now_ns(),
            cpu_ns: None,
        };
        let is_system = batch
            .events
            .iter()
            .any(|event| matches!(event.family, crate::native::NativeFamily::LimitlessSystem));
        let is_exception = batch.events.iter().any(|event| {
            matches!(
                event.family,
                crate::native::NativeFamily::LimitlessException
            )
        });
        let acknowledgement = if is_system && !covered {
            system_markets(&batch.events[0].payload)
        } else {
            None
        };
        context
            .admit(batch, received, decoded, valid)
            .map_err(|_| PeerEnd::AdmissionFailed)?;
        if let Some(markets) = acknowledgement {
            if markets != expected {
                for _ in expected.difference(&markets) {
                    context.control("subscription_ack_missing_target");
                }
                for _ in markets.difference(&expected) {
                    context.control("subscription_ack_unexpected_target");
                }
                if !(tolerate_partial_ack && markets.is_subset(&expected)) {
                    context.fault("subscription_ack_mismatch");
                    return Err(PeerEnd::VenueRejected);
                }
                context.control("subscription_ack_partial");
                context.partial_acknowledgement();
            }
            for market in &markets {
                context.coverage(market);
            }
            covered = true;
        }
        if is_exception {
            context.fault("venue_exception");
            return Err(PeerEnd::VenueRejected);
        }
    }
}

async fn next_frame(
    read: &mut futures_util::stream::SplitStream<Socket>,
    limits: &LexicalLimits,
    stop: &mut watch::Receiver<bool>,
    context: &ConnectionContext,
    commands: &mpsc::Sender<Command>,
) -> Result<
    (
        DecodedFrame,
        usize,
        crate::upstream::StageStamp,
        crate::upstream::StageStamp,
    ),
    PeerEnd,
> {
    tokio::select! {
        changed = stop.changed() => { if changed.is_err() || *stop.borrow() { Err(PeerEnd::Stopped) } else { Err(PeerEnd::ReadFailed) } }
        frame = next_frame_no_stop(read, limits, context, commands, decode_frame) => frame,
    }
}

async fn next_frame_no_stop(
    read: &mut futures_util::stream::SplitStream<Socket>,
    limits: &LexicalLimits,
    context: &ConnectionContext,
    commands: &mpsc::Sender<Command>,
    decode: impl Fn(
        &[u8],
        WebSocketOpcode,
        LexicalLimits,
    ) -> Result<DecodedFrame, crate::wire::socketio::FrameError>,
) -> Result<
    (
        DecodedFrame,
        usize,
        crate::upstream::StageStamp,
        crate::upstream::StageStamp,
    ),
    PeerEnd,
> {
    loop {
        let message = read
            .next()
            .await
            .ok_or(PeerEnd::SocketClosed)?
            .map_err(|_| PeerEnd::ReadFailed)?;
        let received = context.stamp();
        match message {
            Message::Text(text) => {
                let text = Bytes::from(text);
                let bytes = text.len();
                context.message_received();
                context.tape_frame(&text);
                return decode(&text, WebSocketOpcode::Text, *limits)
                    .map(|frame| (frame, bytes, received, context.stamp()))
                    .map_err(|_| {
                        context.malformed("frame_decode");
                        PeerEnd::DecodeFailed
                    });
            }
            Message::Binary(bytes) => {
                let length = bytes.len();
                context.message_received();
                context.tape_frame(&bytes);
                return decode(&bytes, WebSocketOpcode::Binary, *limits)
                    .map(|frame| (frame, length, received, context.stamp()))
                    .map_err(|_| {
                        context.malformed("frame_decode");
                        PeerEnd::DecodeFailed
                    });
            }
            Message::Close(_) => return Err(PeerEnd::SocketClosed),
            Message::Ping(_) => {
                queue(commands, Command::flush())?;
                context.control("websocket_ping");
                continue;
            }
            Message::Pong(_) => {
                context.control("websocket_pong");
                continue;
            }
            Message::Frame(_) => return Err(PeerEnd::DecodeFailed),
        }
    }
}

fn subscription(targets: &[NativeTarget]) -> String {
    let slugs: Vec<_> = targets
        .iter()
        .filter(|target| !target.amm)
        .map(|target| target.market.as_ref())
        .collect();
    let addresses: Vec<_> = targets
        .iter()
        .filter(|target| target.amm)
        .map(|target| target.market.as_ref())
        .collect();
    format!(
        "42{NAMESPACE},{}",
        serde_json::json!(["subscribe_market_prices", { "marketSlugs": slugs, "marketAddresses": addresses }])
    )
}

fn lifecycle_subscription() -> String {
    format!("42{NAMESPACE},[\"subscribe_market_lifecycle\"]")
}

fn expected_targets(targets: &[NativeTarget]) -> BTreeSet<Arc<str>> {
    targets.iter().map(|target| target.market.clone()).collect()
}

fn system_markets(payload: &crate::native::NativePayload) -> Option<BTreeSet<Arc<str>>> {
    payload
        .view()
        .children()
        .nth(1)
        .and_then(|value| value.field("markets"))
        .filter(|value| value.kind() == crate::native::document::NativeKind::Array)
        .map(|values| {
            values
                .children()
                .filter_map(|value| value.as_text().map(Arc::from))
                .collect()
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    fn config(endpoint: String) -> PeerConfig {
        PeerConfig {
            venue: crate::native::NativeVenue::Limitless,
            endpoint,
            slot: 0,
            generation: 1,
            targets: vec![NativeTarget {
                market: Arc::from("clob"),
                asset: None,
                amm: false,
            }],
            lifecycle: false,
            min_command_interval: Duration::ZERO,
            tolerate_partial_ack: false,
        }
    }

    fn two_market_config(endpoint: String, tolerate_partial_ack: bool) -> PeerConfig {
        PeerConfig {
            targets: ["live", "resolved"]
                .map(|market| NativeTarget {
                    market: Arc::from(market),
                    asset: None,
                    amm: false,
                })
                .to_vec(),
            tolerate_partial_ack,
            ..config(endpoint)
        }
    }

    const OPEN: &str =
        r#"0{"sid":"s","upgrades":[],"pingInterval":1000,"pingTimeout":1000,"maxPayload":1048576}"#;

    async fn acknowledge_subset(
        socket: &mut WebSocketStream<tokio::net::TcpStream>,
    ) -> Result<(), tokio_tungstenite::tungstenite::Error> {
        socket.send(Message::text(OPEN)).await?;
        let _ = text(socket).await;
        socket.send(Message::text("40/markets,{}")).await?;
        let subscribe = text(socket).await;
        assert!(subscribe.contains("live") && subscribe.contains("resolved"));
        socket
            .send(Message::text(
                r#"42/markets,["system",{"message":"ok","markets":["live"]}]"#,
            ))
            .await
    }

    async fn text(socket: &mut WebSocketStream<tokio::net::TcpStream>) -> String {
        match timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        {
            Message::Text(text) => text.to_string(),
            other => panic!("expected text, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn real_peer_handshake_preserves_event_answers_ping_and_stops() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "ws://{}/socket.io/?EIO=4&transport=websocket",
            listener.local_addr().unwrap()
        );
        let (stop_tx, stop_rx) = watch::channel(false);
        let context = crate::upstream::test_context(config(endpoint.clone()).targets.clone());
        let client = run(config(endpoint), context.clone(), stop_rx);
        let peer = async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(tcp).await.unwrap();
            socket.send(Message::text(r#"0{"sid":"s","upgrades":[],"pingInterval":1000,"pingTimeout":1000,"maxPayload":1048576}"#)).await.unwrap();
            assert_eq!(text(&mut socket).await, "40/markets,");
            socket.send(Message::text("40/markets,{}")).await.unwrap();
            let subscribe = text(&mut socket).await;
            assert!(subscribe.contains("subscribe_market_prices") && subscribe.contains("clob"));
            socket
                .send(Message::text(
                    r#"42/markets,["system",{"message":"Successfully registered connection"}]"#,
                ))
                .await
                .unwrap();
            socket
                .send(Message::text(
                    r#"42/markets,["system",{"message":"ok","markets":["clob"]}]"#,
                ))
                .await
                .unwrap();
            socket
                .send(Message::Ping(vec![1, 2, 3].into()))
                .await
                .unwrap();
            let response = timeout(Duration::from_secs(1), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(matches!(response, Message::Pong(ref payload) if payload.as_ref() == [1,2,3]));
            socket.send(Message::Pong(vec![4].into())).await.unwrap();
            socket.send(Message::text("2")).await.unwrap();
            assert_eq!(text(&mut socket).await, "3");
            stop_tx.send(true).unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let (ended, _) = tokio::join!(client, peer);
        assert_eq!(ended, Ok(()));
        assert_eq!(context.accounting(), (5, 0, 2, 2));
    }

    #[tokio::test]
    async fn failed_system_ack_never_establishes_coverage() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "ws://{}/socket.io/?EIO=4&transport=websocket",
            listener.local_addr().unwrap()
        );
        let config = config(endpoint);
        let context = crate::upstream::test_context(config.targets.clone());
        let (_stop_tx, stop_rx) = watch::channel(false);
        let client = run(config, context.clone(), stop_rx);
        let peer = async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(tcp).await.unwrap();
            socket.send(Message::text(r#"0{"sid":"s","upgrades":[],"pingInterval":1000,"pingTimeout":1000,"maxPayload":1048576}"#)).await.unwrap();
            let _ = text(&mut socket).await;
            socket.send(Message::text("40/markets,{}")).await.unwrap();
            let _ = text(&mut socket).await;
            socket
                .send(Message::text(
                    r#"42/markets,["system",{"message":"ok","markets":[]}]"#,
                ))
                .await
                .unwrap();
        };
        let (ended, _) = tokio::join!(client, peer);
        assert_eq!(ended, Err(PeerEnd::VenueRejected));
        assert_eq!(context.accounting(), (3, 0, 1, 1));
        assert_eq!(context.health().3, 0);
    }

    #[tokio::test]
    async fn heartbeat_without_a_market_ack_fails_subscription_without_market_inactivity() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "ws://{}/socket.io/?EIO=4&transport=websocket",
            listener.local_addr().unwrap()
        );
        let context = crate::upstream::test_context(config(endpoint.clone()).targets.clone());
        let peer = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(tcp).await.unwrap();
            socket.send(Message::text(r#"0{"sid":"s","upgrades":[],"pingInterval":1000,"pingTimeout":1000,"maxPayload":1048576}"#)).await.unwrap();
            let _ = text(&mut socket).await;
            socket.send(Message::text("40/markets,{}")).await.unwrap();
            let _ = text(&mut socket).await;
            for _ in 0..40 {
                if socket.send(Message::text("2")).await.is_err() {
                    break;
                }
                let _ = timeout(Duration::from_millis(80), socket.next()).await;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        let (_stop_tx, stop_rx) = watch::channel(false);
        assert_eq!(
            timeout(
                Duration::from_secs(1),
                run_with_ack_timeout(
                    config(endpoint),
                    context.clone(),
                    stop_rx,
                    Duration::from_millis(200),
                )
            )
            .await
            .unwrap(),
            Err(PeerEnd::VenueRejected)
        );
        let (connected, sent, heartbeat, covered) = context.health();
        assert!(connected && sent && heartbeat.is_some());
        assert_eq!(covered, 0);
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn acknowledged_quiet_subscription_fails_only_when_engineio_heartbeat_expires() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "ws://{}/socket.io/?EIO=4&transport=websocket",
            listener.local_addr().unwrap()
        );
        let config = config(endpoint);
        let context = crate::upstream::test_context(config.targets.clone());
        let (_stop_tx, stop_rx) = watch::channel(false);
        let (release, hold_open) = oneshot::channel();
        let client = async {
            let ended = timeout(
                Duration::from_secs(4),
                run(config, context.clone(), stop_rx),
            )
            .await;
            let _ = release.send(());
            ended
        };
        let peer_context = context.clone();
        let peer = async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(tcp).await.unwrap();
            socket.send(Message::text(r#"0{"sid":"s","upgrades":[],"pingInterval":1000,"pingTimeout":1000,"maxPayload":1048576}"#)).await.unwrap();
            let _ = text(&mut socket).await;
            socket.send(Message::text("40/markets,{}")).await.unwrap();
            let _ = text(&mut socket).await;
            socket
                .send(Message::text(
                    r#"42/markets,["system",{"message":"ok","markets":["clob"]}]"#,
                ))
                .await
                .unwrap();
            timeout(Duration::from_secs(1), async {
                while peer_context.health().3 != 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            hold_open.await.unwrap();
        };
        let (ended, ()) = tokio::join!(client, peer);
        assert_eq!(ended.unwrap(), Err(PeerEnd::HeartbeatTimeout));
        assert_eq!(context.health().3, 1);
    }

    #[tokio::test]
    async fn malformed_complete_frame_is_counted_before_any_admission() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "ws://{}/socket.io/?EIO=4&transport=websocket",
            listener.local_addr().unwrap()
        );
        let config = config(endpoint);
        let context = crate::upstream::test_context(config.targets.clone());
        let (_stop_tx, stop_rx) = watch::channel(false);
        let client = run(config, context.clone(), stop_rx);
        let peer = async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(tcp).await.unwrap();
            socket.send(Message::text("not-a-frame")).await.unwrap();
        };
        let (ended, _) = tokio::join!(client, peer);
        assert_eq!(ended, Err(PeerEnd::DecodeFailed));
        assert_eq!(context.accounting(), (1, 1, 0, 0));
    }

    #[tokio::test]
    async fn venue_exception_is_admitted_then_rejects_the_peer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "ws://{}/socket.io/?EIO=4&transport=websocket",
            listener.local_addr().unwrap()
        );
        let config = config(endpoint);
        let context = crate::upstream::test_context(config.targets.clone());
        let (_stop_tx, stop_rx) = watch::channel(false);
        let client = run(config, context.clone(), stop_rx);
        let peer = async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(tcp).await.unwrap();
            socket.send(Message::text(r#"0{"sid":"s","upgrades":[],"pingInterval":1000,"pingTimeout":1000,"maxPayload":1048576}"#)).await.unwrap();
            let _ = text(&mut socket).await;
            socket.send(Message::text("40/markets,{}")).await.unwrap();
            let _ = text(&mut socket).await;
            socket
                .send(Message::text(
                    r#"42/markets,["system",{"message":"ok","markets":["clob"]}]"#,
                ))
                .await
                .unwrap();
            socket
                .send(Message::text(
                    r#"42/markets,["exception",{"message":"bad","code":0}]"#,
                ))
                .await
                .unwrap();
        };
        let (ended, _) = tokio::join!(client, peer);
        assert_eq!(ended, Err(PeerEnd::VenueRejected));
        assert_eq!(context.accounting(), (4, 0, 2, 2));
    }

    #[tokio::test]
    async fn a_tolerated_partial_acknowledgement_covers_only_the_acknowledged_markets() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "ws://{}/socket.io/?EIO=4&transport=websocket",
            listener.local_addr().unwrap()
        );
        let config = two_market_config(endpoint, true);
        let context = crate::upstream::test_context(config.targets.clone());
        let (stop_tx, stop_rx) = watch::channel(false);
        let client = run(config, context.clone(), stop_rx);
        let peer_context = context.clone();
        let peer = async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(tcp).await.unwrap();
            acknowledge_subset(&mut socket).await.unwrap();
            timeout(Duration::from_secs(2), async {
                while peer_context.health().3 == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the acknowledged market is covered");
            stop_tx.send(true).unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let (ended, _) = tokio::join!(client, peer);
        assert_eq!(ended, Ok(()));
        assert_eq!(context.health().3, 1);
        assert!(context.partial_acknowledged());
        assert_eq!(context.controls("subscription_ack_partial"), 1);
        assert_eq!(context.controls("subscription_ack_missing_target"), 1);
        assert_eq!(context.controls("subscription_ack_unexpected_target"), 0);
    }

    #[tokio::test]
    async fn an_untolerated_partial_acknowledgement_still_rejects_the_venue() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "ws://{}/socket.io/?EIO=4&transport=websocket",
            listener.local_addr().unwrap()
        );
        let config = two_market_config(endpoint, false);
        let context = crate::upstream::test_context(config.targets.clone());
        let (_stop_tx, stop_rx) = watch::channel(false);
        let client = run(config, context.clone(), stop_rx);
        let peer = async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(tcp).await.unwrap();
            let _ = acknowledge_subset(&mut socket).await;
        };
        let (ended, _) = tokio::join!(client, peer);
        assert_eq!(ended, Err(PeerEnd::VenueRejected));
        assert_eq!(context.health().3, 0);
        assert!(!context.partial_acknowledged());
        assert_eq!(context.controls("subscription_ack_partial"), 0);
        assert_eq!(context.controls("subscription_ack_missing_target"), 1);
    }

    #[tokio::test]
    async fn an_unexpected_market_rejects_the_venue_even_when_partial_acks_are_tolerated() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "ws://{}/socket.io/?EIO=4&transport=websocket",
            listener.local_addr().unwrap()
        );
        let config = two_market_config(endpoint, true);
        let context = crate::upstream::test_context(config.targets.clone());
        let (_stop_tx, stop_rx) = watch::channel(false);
        let client = run(config, context.clone(), stop_rx);
        let peer = async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(tcp).await.unwrap();
            socket.send(Message::text(OPEN)).await.unwrap();
            let _ = text(&mut socket).await;
            socket.send(Message::text("40/markets,{}")).await.unwrap();
            let _ = text(&mut socket).await;
            let _ = socket
                .send(Message::text(
                    r#"42/markets,["system",{"message":"ok","markets":["live","resolved","other"]}]"#,
                ))
                .await;
        };
        let (ended, _) = tokio::join!(client, peer);
        assert_eq!(ended, Err(PeerEnd::VenueRejected));
        assert_eq!(context.health().3, 0);
        assert!(!context.partial_acknowledged());
        assert_eq!(context.controls("subscription_ack_unexpected_target"), 1);
        assert_eq!(context.controls("subscription_ack_partial"), 0);
    }
}

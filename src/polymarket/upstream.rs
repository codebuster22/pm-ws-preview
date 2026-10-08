//! Book-free Polymarket public-market upstream connection.

use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::{Instant, sleep_until, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::Bytes,
    tungstenite::protocol::{Message, WebSocketConfig},
};

use crate::{
    etiquette::reserve_command_grant,
    upstream::{
        ConnectionContext, HeartbeatTimeoutCause, HeartbeatTimeoutWitness, MAX_INPUT_BYTES,
        NativeTarget, PeerConfig, PeerEnd, StageStamp,
    },
    wire::lexical::LexicalLimits,
};

use super::native::{NativeDecodeError, decode_document, parse_document};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const PONG_TIMEOUT: Duration = Duration::from_secs(15);
const WRITER_CAPACITY: usize = 16;

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
type Writer = SplitSink<Socket, Message>;

enum WriteCommand {
    Text {
        text: String,
        paced: bool,
        done: Option<oneshot::Sender<Result<WriteTimes, PeerEnd>>>,
    },
    Ping {
        started: Arc<AtomicBool>,
        done: oneshot::Sender<Result<WriteTimes, PeerEnd>>,
    },
    Flush,
}

struct PendingPing {
    receipt: oneshot::Receiver<Result<WriteTimes, PeerEnd>>,
    started: Arc<AtomicBool>,
    pong_received: bool,
}

#[derive(Debug)]
struct WriteTimes {
    writer_started_ns: Option<u64>,
    write_completed_ns: Option<u64>,
}

#[derive(Default)]
struct HeartbeatTiming {
    ping_enqueued_ns: Option<u64>,
    writer_started_ns: Option<u64>,
    write_completed_ns: Option<u64>,
    receipt_observed_ns: Option<u64>,
    deadline_ns: Option<u64>,
    last_ws_message_received_ns: Option<u64>,
}

impl HeartbeatTiming {
    fn record_write(
        &mut self,
        context: &ConnectionContext,
        times: WriteTimes,
        deadline: Option<Instant>,
    ) {
        self.writer_started_ns = times.writer_started_ns;
        self.write_completed_ns = times.write_completed_ns;
        self.deadline_ns = deadline.map(|at| {
            duration_ns(
                at.into_std()
                    .saturating_duration_since(context.heartbeat_origin()),
            )
        });
    }

    fn witness(
        &self,
        context: &ConnectionContext,
        generation: u64,
        cause: HeartbeatTimeoutCause,
        pong_processed_ns: Option<u64>,
    ) -> HeartbeatTimeoutWitness {
        HeartbeatTimeoutWitness {
            generation,
            observed_ns: context.now_ns(),
            cause,
            ping_enqueued_ns: self.ping_enqueued_ns,
            writer_started_ns: self.writer_started_ns,
            write_completed_ns: self.write_completed_ns,
            receipt_observed_ns: self.receipt_observed_ns,
            deadline_ns: self.deadline_ns,
            pong_processed_ns,
            last_ws_message_received_ns: self.last_ws_message_received_ns,
        }
    }
}

/// Runs one static Polymarket public-market connection generation.
pub(crate) async fn run(
    config: PeerConfig,
    context: ConnectionContext,
    stop: watch::Receiver<bool>,
) -> Result<(), PeerEnd> {
    run_with_timing(config, context, stop, HEARTBEAT_INTERVAL, PONG_TIMEOUT).await
}

async fn run_with_timing(
    config: PeerConfig,
    context: ConnectionContext,
    mut stop: watch::Receiver<bool>,
    heartbeat_interval: Duration,
    pong_timeout: Duration,
) -> Result<(), PeerEnd> {
    if *stop.borrow() {
        return Err(PeerEnd::Stopped);
    }
    let socket = connect(&config, &mut stop).await?;
    context.connected();
    let (write, mut read) = socket.split();
    let (commands, mut writer) = start_writer(
        write,
        config.endpoint.clone(),
        config.min_command_interval,
        stop.clone(),
        context.heartbeat_origin(),
    );
    let inbound = async {
        let (done, receipt) = oneshot::channel();
        commands
            .try_send(WriteCommand::Text {
                text: initial_command(&config.targets, config.lifecycle),
                paced: true,
                done: Some(done),
            })
            .map_err(|_| PeerEnd::ControlOverload)?;
        match receipt.await {
            Ok(Ok(_)) => context.subscription_sent(),
            Ok(Err(end)) => return Err(end),
            Err(_) => return Err(PeerEnd::WriteFailed),
        }
        read_loop(
            &context,
            &mut read,
            &commands,
            &config,
            &mut stop,
            heartbeat_interval,
            pong_timeout,
        )
        .await
    };
    tokio::pin!(inbound);
    let result = tokio::select! {
        result = &mut inbound => result,
        result = &mut writer => result.map_err(|_| PeerEnd::WriteFailed)?.and(Err(PeerEnd::WriteFailed)),
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

async fn connect(config: &PeerConfig, stop: &mut watch::Receiver<bool>) -> Result<Socket, PeerEnd> {
    let settings = WebSocketConfig::default()
        .max_message_size(Some(MAX_INPUT_BYTES))
        .max_frame_size(Some(MAX_INPUT_BYTES));
    tokio::select! {
        _ = stopped(stop) => Err(PeerEnd::Stopped),
        result = timeout(CONNECT_TIMEOUT, connect_async_with_config(&config.endpoint, Some(settings), true)) => match result {
            Err(_) => Err(PeerEnd::ConnectTimedOut),
            Ok(Err(_)) => Err(PeerEnd::ConnectFailed),
            Ok(Ok((socket, _))) => Ok(socket),
        },
    }
}

fn start_writer(
    write: Writer,
    endpoint: String,
    interval: Duration,
    stop: watch::Receiver<bool>,
    origin: std::time::Instant,
) -> (
    mpsc::Sender<WriteCommand>,
    tokio::task::JoinHandle<Result<(), PeerEnd>>,
) {
    let (sender, receiver) = mpsc::channel(WRITER_CAPACITY);
    let task = tokio::spawn(writer_loop(
        write, receiver, endpoint, interval, stop, origin,
    ));
    (sender, task)
}

async fn writer_loop(
    mut write: Writer,
    mut commands: mpsc::Receiver<WriteCommand>,
    endpoint: String,
    interval: Duration,
    mut stop: watch::Receiver<bool>,
    origin: std::time::Instant,
) -> Result<(), PeerEnd> {
    loop {
        let command = tokio::select! {
            _ = stopped(&mut stop) => return Err(PeerEnd::Stopped),
            command = commands.recv() => match command { Some(command) => command, None => return Ok(()) },
        };
        let (message, paced, receipt, ping_started) = match command {
            WriteCommand::Text { text, paced, done } => {
                (Some(Message::Text(text.into())), paced, done, None)
            }
            WriteCommand::Ping { started, done } => (
                Some(Message::Text("PING".into())),
                false,
                Some(done),
                Some(started),
            ),
            WriteCommand::Flush => (None, false, None, None),
        };
        if paced {
            let grant = reserve_command_grant(&endpoint, Instant::now(), interval);
            tokio::select! { _ = stopped(&mut stop) => { if let Some(done) = receipt { let _ = done.send(Err(PeerEnd::Stopped)); } return Err(PeerEnd::Stopped); }, _ = sleep_until(grant) => {} }
        }
        let writer_started_ns = ping_started.as_ref().map(|started| {
            started.store(true, Ordering::Release);
            elapsed_ns(origin)
        });
        let outcome = timeout(WRITE_TIMEOUT, async {
            match message {
                Some(message) => write.send(message).await,
                None => write.flush().await,
            }
        })
        .await
        .map_err(|_| PeerEnd::WriteFailed)
        .and_then(|result| result.map_err(|_| PeerEnd::WriteFailed));
        let write_completed_ns = outcome.is_ok().then(|| elapsed_ns(origin));
        if let Some(done) = receipt {
            let _ = done.send(outcome.map(|()| WriteTimes {
                writer_started_ns,
                write_completed_ns,
            }));
        }
        outcome?;
    }
}

async fn read_loop(
    context: &ConnectionContext,
    read: &mut futures_util::stream::SplitStream<Socket>,
    commands: &mpsc::Sender<WriteCommand>,
    config: &PeerConfig,
    stop: &mut watch::Receiver<bool>,
    heartbeat_interval: Duration,
    pong_timeout: Duration,
) -> Result<(), PeerEnd> {
    let configured_assets: BTreeSet<_> = config
        .targets
        .iter()
        .filter_map(|target| target.asset.clone())
        .collect();
    let mut heartbeat =
        tokio::time::interval_at(Instant::now() + heartbeat_interval, heartbeat_interval);
    let mut pong_deadline = None;
    let mut pending_ping: Option<PendingPing> = None;
    let mut timing = HeartbeatTiming::default();
    loop {
        let deadline = async {
            match pong_deadline {
                Some(deadline) => sleep_until(deadline).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            _ = stopped(stop) => return Err(PeerEnd::Stopped),
            _ = deadline => {
                context.heartbeat_timeout(timing.witness(context, config.generation, HeartbeatTimeoutCause::DeadlineElapsed, None));
                return Err(PeerEnd::HeartbeatTimeout);
            },
            sent = async {
                match &mut pending_ping {
                    Some(ping) => Some((&mut ping.receipt).await),
                    None => std::future::pending().await,
                }
            } => {
                timing.receipt_observed_ns = Some(context.now_ns());
                let ping = pending_ping.take().expect("pending ping");
                let times = confirm_ping(context, ping.pong_received, sent.expect("pending ping receipt"), &mut pong_deadline, pong_timeout)?;
                timing.record_write(context, times, pong_deadline);
            }
            _ = heartbeat.tick(), if pong_deadline.is_none() && pending_ping.is_none() => {
                let (done, receipt) = oneshot::channel();
                let started = Arc::new(AtomicBool::new(false));
                timing = HeartbeatTiming {
                    ping_enqueued_ns: Some(context.now_ns()),
                    last_ws_message_received_ns: timing.last_ws_message_received_ns,
                    ..HeartbeatTiming::default()
                };
                commands.try_send(WriteCommand::Ping { started: started.clone(), done }).map_err(|_| PeerEnd::ControlOverload)?;
                pending_ping = Some(PendingPing { receipt, started, pong_received: false });
            }
            message = read.next() => {
                let message = match message { None => return Err(PeerEnd::SocketClosed), Some(Err(_)) => return Err(PeerEnd::ReadFailed), Some(Ok(message)) => message };
                let received = context.stamp();
                timing.last_ws_message_received_ns = Some(received.wall_ns);
                match message {
                    Message::Text(text) if text == "PONG" => record_pong(context, config.generation, &mut pending_ping, &mut pong_deadline, &timing)?,
                    Message::Text(text) => {
                        let text = Bytes::from(text);
                        context.tape_frame(&text);
                        admit_text(&text, received, context, &configured_assets)?
                    }
                    Message::Ping(_) => {
                        commands.try_send(WriteCommand::Flush).map_err(|_| PeerEnd::ControlOverload)?;
                        context.control("websocket_ping");
                    }
                    Message::Close(_) => return Err(PeerEnd::SocketClosed),
                    Message::Pong(_) => context.control("websocket_pong"),
                    Message::Binary(_) => {
                        context.message_received();
                        context.malformed("frame");
                        return Err(PeerEnd::DecodeFailed);
                    }
                    Message::Frame(_) => { context.fault("frame"); return Err(PeerEnd::DecodeFailed); }
                }
            }
        }
    }
}

fn record_pong(
    context: &ConnectionContext,
    generation: u64,
    pending: &mut Option<PendingPing>,
    deadline: &mut Option<Instant>,
    timing: &HeartbeatTiming,
) -> Result<(), PeerEnd> {
    if let Some(expires) = deadline.take() {
        if Instant::now() >= expires {
            context.heartbeat_timeout(timing.witness(
                context,
                generation,
                HeartbeatTimeoutCause::LatePongProcessed,
                Some(context.now_ns()),
            ));
            return Err(PeerEnd::HeartbeatTimeout);
        }
    } else if let Some(ping) = pending.as_mut() {
        if ping.started.load(Ordering::Acquire) {
            ping.pong_received = true;
        } else {
            context.control("unsolicited_pong");
        }
        return Ok(());
    } else {
        context.control("unsolicited_pong");
        return Ok(());
    }
    context.heartbeat_received();
    context.control("pong");
    Ok(())
}

fn confirm_ping(
    context: &ConnectionContext,
    pong_received: bool,
    receipt: Result<Result<WriteTimes, PeerEnd>, oneshot::error::RecvError>,
    deadline: &mut Option<Instant>,
    timeout: Duration,
) -> Result<WriteTimes, PeerEnd> {
    let times = receipt.map_err(|_| PeerEnd::WriteFailed)??;
    context.control("ping_sent");
    if pong_received {
        context.heartbeat_received();
        context.control("pong");
    } else {
        let expires = Instant::now() + timeout;
        *deadline = Some(expires);
    }
    Ok(times)
}

fn elapsed_ns(origin: std::time::Instant) -> u64 {
    u64::try_from(origin.elapsed().as_nanos()).unwrap_or(u64::MAX)
}
fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn admit_text(
    bytes: &[u8],
    received: StageStamp,
    context: &ConnectionContext,
    configured_assets: &BTreeSet<std::sync::Arc<str>>,
) -> Result<(), PeerEnd> {
    context.message_received();
    let document = parse_document(
        bytes,
        LexicalLimits::venue_payload().with_max_bytes(MAX_INPUT_BYTES),
    )
    .map_err(|error| {
        context.malformed(if matches!(error, NativeDecodeError::Lexical) {
            "lexical"
        } else {
            "native"
        });
        PeerEnd::DecodeFailed
    })?;
    let decoded = context.stamp();
    let batch = decode_document(document, context.source(received), bytes.len()).map_err(|_| {
        context.malformed("native");
        PeerEnd::DecodeFailed
    })?;
    let observed: Vec<_> = batch
        .events
        .iter()
        .flat_map(|event| event.assets.iter())
        .filter(|asset| configured_assets.contains(*asset))
        .cloned()
        .collect();
    let validated = StageStamp {
        wall_ns: context.now_ns(),
        cpu_ns: None,
    };
    context.admit(batch, received, decoded, validated)?;
    for asset in observed {
        context.coverage(&asset);
    }
    Ok(())
}

fn initial_command(targets: &[NativeTarget], lifecycle: bool) -> String {
    let assets: Vec<_> = targets
        .iter()
        .filter_map(|target| target.asset.as_deref())
        .collect();
    let _ = lifecycle;
    serde_json::json!({ "assets_ids": assets, "type": "market", "custom_feature_enabled": true })
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upstream::test_context;
    use futures_util::{SinkExt, StreamExt};
    use std::sync::Arc;
    use tokio::{net::TcpListener, sync::oneshot};
    use tokio_tungstenite::{accept_async, tungstenite::protocol::Message};

    fn targets() -> Vec<NativeTarget> {
        vec![NativeTarget {
            market: Arc::from("m"),
            asset: Some(Arc::from("a")),
            amm: false,
        }]
    }

    fn all_families() -> &'static str {
        r#"[{"event_type":"book","market":"m","asset_id":"a","bids":[],"asks":[],"timestamp":"1","hash":"h"},{"event_type":"price_change","market":"m","timestamp":"2","price_changes":[{"asset_id":"a","price":"0.5","size":"0","side":"BUY","hash":"h","best_bid":"0.5","best_ask":"1"}]},{"event_type":"last_trade_price","market":"m","asset_id":"a","price":"0.5","size":"1","fee_rate_bps":"0","side":"BUY","timestamp":"3","transaction_hash":"t"},{"event_type":"tick_size_change","market":"m","asset_id":"a","old_tick_size":"0.01","new_tick_size":"0.001","timestamp":"4"},{"event_type":"best_bid_ask","market":"m","asset_id":"a","best_bid":"0.5","best_ask":"0.6","spread":"0.1","timestamp":"5"},{"event_type":"new_market","id":"i","question":"q","market":"m","slug":"s","description":"d","assets_ids":["a"],"outcomes":["Yes"],"event_message":{},"timestamp":"6"},{"event_type":"market_resolved","id":"i","market":"m","assets_ids":["a"],"winning_asset_id":"a","winning_outcome":"Yes","timestamp":"7"}]"#
    }

    #[test]
    fn native_path_admits_all_families_and_rejects_an_entire_bad_array() {
        let context = test_context(targets());
        let assets = [Arc::from("a")].into_iter().collect();
        admit_text(
            all_families().as_bytes(),
            context.stamp(),
            &context,
            &assets,
        )
        .expect("all documented families");
        assert_eq!(context.accounting(), (1, 0, 7, 7));
        let malformed = br#"[{"event_type":"book","market":"m","asset_id":"a","bids":[],"asks":[],"timestamp":"8","hash":"h"},{"event_type":"best_bid_ask","market":"m","asset_id":"a","best_bid":"0.5","best_ask":false,"spread":"0.1","timestamp":"9"}]"#;
        assert_eq!(
            admit_text(malformed, context.stamp(), &context, &assets),
            Err(PeerEnd::DecodeFailed)
        );
        assert_eq!(context.accounting(), (2, 1, 7, 7));
        admit_text(br#"{"event_type":"book","market":"m","asset_id":"a","bids":[],"asks":[],"timestamp":"8","hash":"h"}"#, context.stamp(), &context, &assets).expect("bad array did not partially enter the gate");
        assert_eq!(context.accounting(), (3, 1, 8, 8));
    }

    #[test]
    fn empty_top_level_array_is_a_malformed_message_without_admission() {
        let context = test_context(targets());
        let assets = [Arc::from("a")].into_iter().collect();
        assert_eq!(
            admit_text(b"[]", context.stamp(), &context, &assets),
            Err(PeerEnd::DecodeFailed)
        );
        assert_eq!(context.accounting(), (1, 1, 0, 0));
    }

    #[tokio::test]
    async fn connection_sends_custom_feature_subscription_and_stops_cleanly() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let endpoint = format!("ws://{}", listener.local_addr().expect("address"));
        let (stopped, stop) = watch::channel(false);
        let (initial, initial_rx) = oneshot::channel();
        let (release, released) = oneshot::channel();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.expect("accept");
            let mut socket = accept_async(tcp).await.expect("handshake");
            let Some(Ok(Message::Text(text))) = socket.next().await else {
                panic!("initial subscription");
            };
            assert!(text.contains("\"custom_feature_enabled\":true"));
            assert!(text.contains("\"assets_ids\":[\"a\"]"));
            socket
                .send(Message::Text(all_families().into()))
                .await
                .expect("events");
            let ping = tokio::time::timeout(Duration::from_secs(12), socket.next())
                .await
                .expect("application ping")
                .expect("open socket")
                .expect("ping frame");
            assert!(matches!(ping, Message::Text(ref text) if text.as_str() == "PING"));
            socket
                .send(Message::Text("PONG".into()))
                .await
                .expect("pong");
            let _ = initial.send(());
            let _ = released.await;
        });
        let config = PeerConfig {
            venue: crate::native::NativeVenue::Polymarket,
            endpoint,
            slot: 0,
            generation: 1,
            targets: targets(),
            lifecycle: true,
            min_command_interval: Duration::ZERO,
            tolerate_partial_ack: false,
        };
        let context = test_context(targets());
        let connection = run(config, context.clone(), stop);
        tokio::pin!(connection);
        tokio::select! {
            result = &mut connection => panic!("connection ended early: {result:?}"),
            result = initial_rx => result.expect("subscription observed"),
        }
        stopped.send(true).expect("stop");
        assert_eq!(connection.await, Err(PeerEnd::Stopped));
        assert_eq!(context.accounting(), (1, 0, 7, 7));
        let _ = release.send(());
    }

    #[tokio::test]
    async fn quiet_subscription_is_healthy_after_a_written_ping_and_pong() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let endpoint = format!("ws://{}", listener.local_addr().expect("address"));
        let (stopped, stop) = watch::channel(false);
        let (evidenced, evidence) = oneshot::channel();
        let (release, released) = oneshot::channel();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.expect("accept");
            let mut socket = accept_async(tcp).await.expect("handshake");
            let _ = socket
                .next()
                .await
                .expect("subscription")
                .expect("subscription frame");
            let ping = timeout(Duration::from_secs(1), socket.next())
                .await
                .expect("ping deadline")
                .expect("open socket")
                .expect("ping frame");
            assert!(matches!(ping, Message::Text(ref value) if value.as_str() == "PING"));
            socket
                .send(Message::Text("PONG".into()))
                .await
                .expect("pong");
            let _ = evidenced.send(());
            let _ = released.await;
        });
        let config = PeerConfig {
            venue: crate::native::NativeVenue::Polymarket,
            endpoint,
            slot: 0,
            generation: 1,
            targets: targets(),
            lifecycle: false,
            min_command_interval: Duration::ZERO,
            tolerate_partial_ack: false,
        };
        let context = test_context(targets());
        let connection = run_with_timing(
            config,
            context.clone(),
            stop,
            Duration::from_millis(30),
            Duration::from_millis(90),
        );
        tokio::pin!(connection);
        tokio::select! {
            result = &mut connection => panic!("connection ended before pong: {result:?}"),
            result = timeout(Duration::from_secs(1), evidence) => result.expect("pong deadline").expect("pong observed"),
        }
        tokio::select! {
            result = &mut connection => panic!("connection ended before heartbeat: {result:?}"),
            result = timeout(Duration::from_secs(1), async {
                while context.health().2.is_none() {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }) => result.expect("heartbeat recorded"),
        }
        let (connected, sent, heartbeat, covered) = context.health();
        assert!(connected && sent && heartbeat.is_some());
        assert_eq!(covered, 0);
        assert_eq!(context.heartbeat_timeout_cause(), None);
        stopped.send(true).expect("stop");
        let _ = release.send(());
        assert_eq!(
            timeout(Duration::from_secs(1), &mut connection)
                .await
                .expect("stop deadline"),
            Err(PeerEnd::Stopped)
        );
    }

    #[test]
    fn unsolicited_or_late_pong_does_not_establish_health() {
        let context = test_context(targets());
        let timing = HeartbeatTiming::default();
        let (sent, receipt) = oneshot::channel();
        let started = Arc::new(AtomicBool::new(false));
        let mut pending = Some(PendingPing {
            receipt,
            started: started.clone(),
            pong_received: false,
        });
        let mut deadline = None;
        assert_eq!(
            record_pong(&context, 1, &mut pending, &mut deadline, &timing,),
            Ok(())
        );
        assert!(context.health().2.is_none());
        assert!(pending.is_some());
        started.store(true, Ordering::Release);
        assert_eq!(
            record_pong(&context, 1, &mut pending, &mut deadline, &timing,),
            Ok(())
        );
        assert!(context.health().2.is_none());
        sent.send(Ok(WriteTimes {
            writer_started_ns: None,
            write_completed_ns: None,
        }))
        .unwrap();
        let mut ping = pending.take().unwrap();
        let receipt = ping.receipt.try_recv().unwrap();
        assert!(
            confirm_ping(
                &context,
                ping.pong_received,
                Ok(receipt),
                &mut deadline,
                Duration::from_secs(1)
            )
            .is_ok()
        );
        assert!(context.health().2.is_some());
        assert!(deadline.is_none());

        let quiet = test_context(targets());
        let mut expired = Some(Instant::now());
        assert_eq!(
            record_pong(
                &quiet,
                1,
                &mut None,
                &mut expired,
                &HeartbeatTiming {
                    deadline_ns: Some(1),
                    ..HeartbeatTiming::default()
                },
            ),
            Err(PeerEnd::HeartbeatTimeout)
        );
        assert!(quiet.health().2.is_none());
        assert_eq!(
            quiet.heartbeat_timeout_cause(),
            Some(HeartbeatTimeoutCause::LatePongProcessed)
        );
    }

    #[test]
    fn heartbeat_witness_records_the_actual_armed_deadline() {
        let context = test_context(targets());
        let armed = Instant::from_std(context.heartbeat_origin() + Duration::from_secs(5));
        let mut timing = HeartbeatTiming {
            receipt_observed_ns: Some(30),
            ..HeartbeatTiming::default()
        };
        timing.record_write(
            &context,
            WriteTimes {
                writer_started_ns: Some(10),
                write_completed_ns: Some(20),
            },
            Some(armed),
        );
        assert_eq!(timing.deadline_ns, Some(5_000_000_000));
        assert_eq!(timing.receipt_observed_ns, Some(30));
        assert_eq!(timing.writer_started_ns, Some(10));
        assert_eq!(timing.write_completed_ns, Some(20));
    }

    #[tokio::test]
    async fn busy_market_data_does_not_mask_a_missing_pong() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let endpoint = format!("ws://{}", listener.local_addr().expect("address"));
        let peer = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.expect("accept");
            let mut socket = accept_async(tcp).await.expect("handshake");
            let _ = socket
                .next()
                .await
                .expect("subscription")
                .expect("subscription frame");
            for timestamp in 0..40 {
                let frame = format!(
                    r#"{{"event_type":"book","market":"m","asset_id":"a","bids":[],"asks":[],"timestamp":"{timestamp}","hash":"h{timestamp}"}}"#
                );
                if socket.send(Message::Text(frame.into())).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
        let config = PeerConfig {
            venue: crate::native::NativeVenue::Polymarket,
            endpoint,
            slot: 0,
            generation: 1,
            targets: targets(),
            lifecycle: false,
            min_command_interval: Duration::ZERO,
            tolerate_partial_ack: false,
        };
        let context = test_context(targets());
        let (_stopped, stop) = watch::channel(false);
        assert_eq!(
            timeout(
                Duration::from_secs(1),
                run_with_timing(
                    config,
                    context.clone(),
                    stop,
                    Duration::from_millis(30),
                    Duration::from_millis(90),
                ),
            )
            .await
            .expect("heartbeat deadline"),
            Err(PeerEnd::HeartbeatTimeout)
        );
        assert!(context.accounting().0 > 2);
        assert_eq!(
            context.heartbeat_timeout_cause(),
            Some(HeartbeatTimeoutCause::DeadlineElapsed)
        );
        peer.await.expect("peer");
    }

    #[tokio::test]
    async fn malformed_complete_message_is_counted_without_admission() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let endpoint = format!("ws://{}", listener.local_addr().expect("address"));
        let config = PeerConfig {
            venue: crate::native::NativeVenue::Polymarket,
            endpoint,
            slot: 0,
            generation: 1,
            targets: targets(),
            lifecycle: false,
            min_command_interval: Duration::ZERO,
            tolerate_partial_ack: false,
        };
        let context = test_context(targets());
        let (_stopped, stop) = watch::channel(false);
        let connection = run(config, context.clone(), stop);
        let peer = async move {
            let (tcp, _) = listener.accept().await.expect("accept");
            let mut socket = accept_async(tcp).await.expect("handshake");
            let _ = socket.next().await;
            socket
                .send(Message::Text("not-json".into()))
                .await
                .expect("malformed");
        };
        let (ended, _) = tokio::join!(connection, peer);
        assert_eq!(ended, Err(PeerEnd::DecodeFailed));
        assert_eq!(context.accounting(), (1, 1, 0, 0));
    }
}

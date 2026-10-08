//! A loopback-only scripted venue peer that sustains a stated update rate, so the S8
//! integrated table can be produced from deterministic local runs.
//!
//! This process never contacts a venue. It binds a `127.0.0.1` port, speaks the same
//! Engine.IO/Socket.IO `/markets` dialect `tests/support/controlled_peer.rs` speaks to the
//! daemon's decoder — an `open` packet on accept, a namespace connect ack, a parsed
//! `subscribe_market_prices`, its `system` acknowledgment, server Engine.IO pings — and then
//! emits `orderbookUpdate` events for the set the client subscribed to, in the exact frame
//! shape `tests/support/observed_frames.rs` recorded from the live venue.
//!
//! The peer never invents the market set: it serves whatever the client subscribes to, which
//! is what makes one binary drive a 1-market and a 10 000-market row without a market list of
//! its own. Every connection is served independently, so a daemon running one shard per
//! connection is served by one peer.
//!
//! **The update rate is synthetic and every table row that uses this peer must say so.** The
//! socket-arrival stamp the daemon takes is real — these are real WebSocket frames over a real
//! loopback socket — but the cadence is this process's schedule, not a venue's. `--rate` is the
//! per-market rate; the aggregate this peer offers one connection is `--rate` times the number
//! of markets that connection subscribed to. What the daemon actually ingested is its own
//! `pmws_shard_frames_seen`, and the two are reported side by side rather than the offered rate
//! being reported as if it were achieved.
//!
//! Each emitted book carries `--depth` levels per side at fixed prices, with the top bid's size
//! stepped on every update so no update is a no-op an equal-content check could drop, and a
//! per-market `version` counter that increases by one per update so continuity evidence is
//! unbroken. `--depth` must not exceed the daemon's configured `level_capacity`, or every
//! snapshot is refused as too deep and the run measures nothing.
//!
//! Usage:
//!
//! ```text
//! cargo run --release --example table_peer -- --rate 200 --depth 5
//! ```
//!
//! The bound endpoint is printed to stdout as one `endpoint: <url>` line and flushed before
//! the first accept, so a script can read it and write it into a `pmwsd` configuration.
//! Everything else this process says goes to stderr.

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use pm_ws::wire::lexical::{LexicalLimits, LexicalValue};
use pm_ws::wire::socketio::{SocketIoPacket, WebSocketOpcode, decode_frame};
use std::io::Write;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::protocol::Message;

const NAMESPACE: &str = "/markets";

/// The timestamp every emitted `orderbookUpdate` carries, held constant exactly as
/// `tests/support/controlled_peer.rs` holds it: the daemon keeps the venue's timestamp as
/// uninterpreted provenance and orders books by their `version` evidence, so a fixed value
/// removes one varying field from the generator without changing what is measured.
const ORDERBOOK_TIMESTAMP: &str = "2026-08-31T00:00:00.000Z";

/// The `tokenId` every emitted book carries. The daemon does not key on it; it is present
/// because the observed venue frame is present, and a decoder regression is worth catching
/// here rather than only in the fixtures.
const TOKEN_ID: &str =
    "83416341894274737086755695271958877285974747994652828356442717729857948830534";

/// How often the emission schedule is re-evaluated.
///
/// The schedule is computed from elapsed time rather than from a tick count, so a late tick
/// does not lose updates; this only bounds how finely the offered rate is spread.
const TICK: Duration = Duration::from_millis(1);

/// The longest stretch of offered updates one tick may emit while catching up, expressed as a
/// multiple of [`TICK`].
///
/// A generator that fell behind and then emitted its whole arrears in one tick would report a
/// queue-age tail that its own burst produced. Catch-up is bounded here so arrears show up as
/// an achieved rate below the offered rate — which the table states — rather than as a spike
/// attributed to the daemon.
const MAX_CATCH_UP_TICKS: u64 = 20;

/// The most markets a `system` acknowledgment echoes back.
///
/// The venue's acknowledgment repeats the subscribed set, and the daemon decodes it under
/// `LexicalLimits::venue_payload`, whose `max_array_elements` is this number. Echoing a larger
/// set would be an acknowledgment the client must refuse, counted against
/// `pmws_shard_decode_failures` and misread in a table as a daemon fault. The daemon never
/// waits for this acknowledgment — `establish` returns as soon as the command is written — so
/// a larger set is acknowledged with the same `system` message carrying no echo at all.
const MAX_ACK_ECHO_MARKETS: usize = 4_096;

/// The deepest book this generator will build, in levels per side.
///
/// Prices are laid out one milli-unit apart either side of the midpoint, so the depth that
/// keeps every level inside `(0, 1)` is the bound, not a memory limit.
const MAX_DEPTH: usize = 400;

/// The limits this peer decodes client frames under.
///
/// Wider than `LexicalLimits::venue_payload` in exactly two dimensions, because a
/// `subscribe_market_prices` naming ten thousand markets is a quarter-megabyte array of ten
/// thousand elements and the venue's own server plainly accepts one. Nothing about the frames
/// this peer *emits* is widened.
fn client_limits() -> LexicalLimits {
    LexicalLimits {
        max_bytes: 4 * 1024 * 1024,
        max_array_elements: 65_536,
        ..LexicalLimits::venue_payload()
    }
}

#[derive(Debug)]
struct Args {
    listen: String,
    rate: u64,
    depth: usize,
    ping_interval_ms: u64,
    ping_timeout_ms: u64,
    max_payload_bytes: u64,
    fault: Option<FaultSpec>,
}

const USAGE: &str = "usage: table_peer [--listen <addr>] --rate <per-market-hz> \
                     [--depth <levels>] [--ping-interval-ms <n>] [--ping-timeout-ms <n>] \
                     [--max-payload-bytes <n>] \
                     [--fault-session <n> --fault-after-ms <n> --fault-kind drop|stall]";

/// Which of two shapes an injected connection fault takes.
///
/// Both are additive diagnostic fault injection for a reconnect benchmark, never anything a
/// live-traffic run would set. `Drop` simulates a crashed or network-severed peer: the
/// accepted TCP stream is dropped mid-flow with no WebSocket close frame, exactly as
/// `tests/support/controlled_peer.rs`'s own `drop_abruptly` does, so the client's read path
/// sees an end of stream rather than a graceful close. `Stall` leaves the TCP connection open
/// but stops every send on it — book updates and Engine.IO pings alike — so detection is
/// whatever heartbeat deadline the client negotiated at handshake, not a severed socket.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FaultKind {
    Drop,
    Stall,
}

/// One armed fault: which accepted connection it fires on, how long after that connection's
/// accept it fires, and what it does.
///
/// `session` matches [`Settings::session_id`], the peer's own 1-based count of accepted
/// connections — the only handle this process has on "the primary" or "the replacement",
/// since it never sees the client's internal role assignment. A caller that wants to fault a
/// specific role (a hot-standby run's primary, say) picks the session by the order it expects
/// that role to connect in, and confirms which session actually held the role from the
/// client's own connection log.
#[derive(Clone, Copy, Debug)]
struct FaultSpec {
    session: u64,
    after: Duration,
    kind: FaultKind,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = match parse_args(std::env::args()) {
        Ok(args) => args,
        Err(message) => {
            eprintln!("table_peer: {message}");
            eprintln!("{USAGE}");
            std::process::exit(2);
        }
    };
    let listener = match TcpListener::bind(&args.listen).await {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("table_peer: cannot bind {}: {error}", args.listen);
            std::process::exit(1);
        }
    };
    let bound = match listener.local_addr() {
        Ok(address) => address,
        Err(error) => {
            eprintln!("table_peer: bound, and not readable back: {error}");
            std::process::exit(1);
        }
    };
    println!("endpoint: ws://{bound}/socket.io/?EIO=4&transport=websocket");
    if std::io::stdout().flush().is_err() {
        eprintln!("table_peer: cannot announce the bound endpoint");
        std::process::exit(1);
    }
    eprintln!(
        "table_peer: listening on {bound}, offering {} update(s) per market per second at \
         depth {}",
        args.rate, args.depth
    );

    let book = BookShape::new(args.depth);
    let mut session_id: u64 = 0;
    loop {
        let accepted = listener.accept().await;
        let Ok((tcp, _peer)) = accepted else {
            eprintln!("table_peer: accept failed; the listener stays open");
            continue;
        };
        if let Err(error) = tcp.set_nodelay(true) {
            eprintln!("table_peer: cannot disable Nagle on an accepted connection: {error}");
        }
        session_id += 1;
        let settings = Settings {
            session_id,
            rate: args.rate,
            ping_interval_ms: args.ping_interval_ms,
            ping_timeout_ms: args.ping_timeout_ms,
            max_payload_bytes: args.max_payload_bytes,
            fault: args.fault,
        };
        let book = book.clone();
        tokio::spawn(async move {
            serve(tcp, settings, book).await;
        });
    }
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let _binary = args.next();
    let mut listen = "127.0.0.1:0".to_owned();
    let mut rate = None;
    let mut depth = 5usize;
    let mut ping_interval_ms = 1_000u64;
    let mut ping_timeout_ms = 1_000u64;
    let mut max_payload_bytes = 1_000_000u64;
    let mut fault_session = None;
    let mut fault_after_ms = None;
    let mut fault_kind = None;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--listen" => listen = args.next().ok_or("--listen requires a value")?,
            "--rate" => {
                let value = args.next().ok_or("--rate requires a value")?;
                rate = Some(
                    value
                        .parse::<u64>()
                        .map_err(|_| "--rate takes a positive integer".to_owned())?,
                );
            }
            "--depth" => {
                let value = args.next().ok_or("--depth requires a value")?;
                depth = value
                    .parse::<usize>()
                    .map_err(|_| "--depth takes a positive integer".to_owned())?;
            }
            "--ping-interval-ms" => {
                let value = args.next().ok_or("--ping-interval-ms requires a value")?;
                ping_interval_ms = value
                    .parse::<u64>()
                    .map_err(|_| "--ping-interval-ms takes an integer".to_owned())?;
            }
            "--ping-timeout-ms" => {
                let value = args.next().ok_or("--ping-timeout-ms requires a value")?;
                ping_timeout_ms = value
                    .parse::<u64>()
                    .map_err(|_| "--ping-timeout-ms takes an integer".to_owned())?;
            }
            "--max-payload-bytes" => {
                let value = args.next().ok_or("--max-payload-bytes requires a value")?;
                max_payload_bytes = value
                    .parse::<u64>()
                    .map_err(|_| "--max-payload-bytes takes an integer".to_owned())?;
            }
            "--fault-session" => {
                let value = args.next().ok_or("--fault-session requires a value")?;
                fault_session = Some(
                    value
                        .parse::<u64>()
                        .map_err(|_| "--fault-session takes a positive integer".to_owned())?,
                );
            }
            "--fault-after-ms" => {
                let value = args.next().ok_or("--fault-after-ms requires a value")?;
                fault_after_ms = Some(
                    value
                        .parse::<u64>()
                        .map_err(|_| "--fault-after-ms takes an integer".to_owned())?,
                );
            }
            "--fault-kind" => {
                let value = args.next().ok_or("--fault-kind requires a value")?;
                fault_kind = Some(match value.as_str() {
                    "drop" => FaultKind::Drop,
                    "stall" => FaultKind::Stall,
                    other => {
                        return Err(format!("--fault-kind must be drop or stall, got {other}"));
                    }
                });
            }
            other => return Err(format!("unknown flag {other}")),
        }
    }
    let rate = rate.ok_or("--rate is required (updates per market per second)")?;
    if rate == 0 {
        return Err("--rate must be at least 1".to_owned());
    }
    if depth == 0 || depth > MAX_DEPTH {
        return Err(format!("--depth must be between 1 and {MAX_DEPTH}"));
    }
    let fault = match (fault_session, fault_after_ms, fault_kind) {
        (None, None, None) => None,
        (Some(session), Some(after_ms), Some(kind)) => Some(FaultSpec {
            session,
            after: Duration::from_millis(after_ms),
            kind,
        }),
        _ => {
            return Err(
                "--fault-session, --fault-after-ms and --fault-kind must be given together"
                    .to_owned(),
            );
        }
    };
    Ok(Args {
        listen,
        rate,
        depth,
        ping_interval_ms,
        ping_timeout_ms,
        max_payload_bytes,
        fault,
    })
}

#[derive(Clone, Copy, Debug)]
struct Settings {
    session_id: u64,
    rate: u64,
    ping_interval_ms: u64,
    ping_timeout_ms: u64,
    max_payload_bytes: u64,
    fault: Option<FaultSpec>,
}

/// The fixed part of every emitted book: the bid levels below the top one, and every ask.
///
/// Held per depth rather than rebuilt per update, because only the top bid's size varies and
/// the rest of the JSON is identical for the life of the process.
#[derive(Clone, Debug)]
struct BookShape {
    top_bid_price: String,
    bids_tail: String,
    asks: String,
}

impl BookShape {
    /// Lays out `depth` levels per side one milli-unit apart, bids strictly descending from
    /// `0.500` and asks strictly ascending from `0.501`, matching the venue's own contract
    /// that the daemon reproduces rather than re-sorts.
    fn new(depth: usize) -> Self {
        let mut bids_tail = String::new();
        for index in 1..depth {
            let price = 500 - index;
            let size = 50_000_000u64 + index as u64;
            bids_tail.push_str(&format!(
                ",{{\"price\":0.{price:03},\"size\":{size},\"side\":\"BUY\"}}"
            ));
        }
        let mut asks = String::from("[");
        for index in 0..depth {
            if index > 0 {
                asks.push(',');
            }
            let price = 501 + index;
            let size = 50_000_000u64 + index as u64;
            asks.push_str(&format!(
                "{{\"price\":0.{price:03},\"size\":{size},\"side\":\"SELL\"}}"
            ));
        }
        asks.push(']');
        Self {
            top_bid_price: "0.500".to_owned(),
            bids_tail,
            asks,
        }
    }

    /// One `orderbookUpdate` event for `slug`, carrying `version` and a top-of-book size
    /// stepped by `step` so no two consecutive updates for a market are equal.
    fn frame(&self, slug: &str, version: u64, step: u64) -> String {
        let top_size = 50_000_000u64 + (step % 1_000_000);
        let price = &self.top_bid_price;
        let tail = &self.bids_tail;
        let asks = &self.asks;
        format!(
            "42{NAMESPACE},[\"orderbookUpdate\",{{\"marketSlug\":\"{slug}\",\"orderbook\":\
             {{\"bids\":[{{\"price\":{price},\"size\":{top_size},\"side\":\"BUY\"}}{tail}],\
             \"asks\":{asks},\"tokenId\":\"{TOKEN_ID}\",\"adjustedMidpoint\":0.5,\
             \"midpoint\":0.5,\"maxSpread\":0.065,\"minSize\":50000000}},\
             \"version\":{version},\"timestamp\":\"{ORDERBOOK_TIMESTAMP}\"}}]"
        )
    }
}

type Writer = SplitSink<WebSocketStream<TcpStream>, Message>;
type Reader = SplitStream<WebSocketStream<TcpStream>>;

/// Runs one client connection from the WebSocket handshake to its close.
///
/// Every step reports and returns rather than panicking: one wedged or malformed client must
/// not stop the peer serving the other connections a multi-shard daemon opens.
async fn serve(tcp: TcpStream, settings: Settings, book: BookShape) {
    let accepted_at = Instant::now();
    let Ok(ws) = accept_async(tcp).await else {
        eprintln!("table_peer: websocket handshake failed");
        return;
    };
    let (mut write, mut read) = ws.split();
    let session_id = settings.session_id;
    let open = format!(
        "0{{\"sid\":\"table-peer-engine-session-{session_id}\",\"upgrades\":[],\
         \"pingInterval\":{},\"pingTimeout\":{},\"maxPayload\":{}}}",
        settings.ping_interval_ms, settings.ping_timeout_ms, settings.max_payload_bytes
    );
    if send(&mut write, open).await.is_err() {
        return;
    }
    if await_namespace_connect(&mut read).await.is_none() {
        return;
    }
    let ack = format!("40{NAMESPACE},{{\"sid\":\"table-peer-namespace-session-{session_id}\"}}");
    if send(&mut write, ack).await.is_err() {
        return;
    }
    let Some(slugs) = await_subscription(&mut read).await else {
        return;
    };
    eprintln!(
        "table_peer: session {session_id} subscribed to {} market(s)",
        slugs.len()
    );
    if acknowledge(&mut write, &slugs).await.is_err() {
        return;
    }
    feed(write, read, settings, book, slugs, accepted_at).await;
}

async fn send(write: &mut Writer, text: String) -> Result<(), ()> {
    write.send(Message::text(text)).await.map_err(|error| {
        eprintln!("table_peer: send failed: {error}");
    })
}

async fn await_namespace_connect(read: &mut Reader) -> Option<()> {
    loop {
        let text = read_text(read).await?;
        let Ok(frame) = decode_frame(text.as_bytes(), WebSocketOpcode::Text, client_limits())
        else {
            continue;
        };
        if frame.socket_io() == Some(SocketIoPacket::Connect)
            && frame.namespace() == Some(NAMESPACE)
        {
            return Some(());
        }
    }
}

async fn await_subscription(read: &mut Reader) -> Option<Vec<String>> {
    loop {
        let text = read_text(read).await?;
        if let Some(slugs) = subscription_slugs(&text) {
            return Some(slugs);
        }
    }
}

/// The `marketSlugs` a `subscribe_market_prices` emit carries, or `None` for any other frame.
fn subscription_slugs(text: &str) -> Option<Vec<String>> {
    let frame = decode_frame(text.as_bytes(), WebSocketOpcode::Text, client_limits()).ok()?;
    if frame.event_name() != Some("subscribe_market_prices") {
        return None;
    }
    let slugs = frame
        .payload()?
        .field("marketSlugs")
        .and_then(LexicalValue::as_array)?
        .iter()
        .filter_map(|value| value.as_text().map(str::to_owned))
        .collect();
    Some(slugs)
}

async fn acknowledge(write: &mut Writer, slugs: &[String]) -> Result<(), ()> {
    let listed = if slugs.len() > MAX_ACK_ECHO_MARKETS {
        String::new()
    } else {
        slugs
            .iter()
            .map(|slug| format!("\"{slug}\""))
            .collect::<Vec<_>>()
            .join(",")
    };
    send(
        write,
        format!(
            "42{NAMESPACE},[\"system\",{{\"message\":\"Successfully subscribed to market price \
             updates\",\"markets\":[{listed}]}}]"
        ),
    )
    .await
}

async fn read_text(read: &mut Reader) -> Option<String> {
    loop {
        match read.next().await {
            None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return None,
            Some(Ok(Message::Text(text))) => return Some(text.as_str().to_owned()),
            Some(Ok(_)) => continue,
        }
    }
}

/// Emits the offered rate until the client goes away.
///
/// Updates are spread round-robin over the subscribed set: every market is emitted once before
/// any market is emitted twice, so a per-market rate is what the set actually receives rather
/// than an average over a set some of whose members are starved. A `subscribe_market_prices`
/// arriving mid-run replaces the set, as the venue treats it, and restarts the rotation with
/// version counters carried over for the markets that stayed.
///
/// When `settings.fault` names this connection's own `session_id`, one more timer runs
/// alongside the emission schedule, armed for `accepted_at + fault.after`. A [`FaultKind::Drop`]
/// ends this function the instant it fires, dropping `write` and `read` with no WebSocket close
/// frame — the client's read path sees an end of stream. A [`FaultKind::Stall`] instead flips a
/// flag this loop already checks before every send: book updates and Engine.IO pings both stop,
/// the read half stays live so a close the client itself sends is still noticed, and nothing
/// about the schedule's own bookkeeping changes, so a fault that never fires (every other
/// session, and an unfaulted run) costs this loop one more `if`.
async fn feed(
    mut write: Writer,
    mut read: Reader,
    settings: Settings,
    book: BookShape,
    mut slugs: Vec<String>,
    accepted_at: Instant,
) {
    let mut versions: Vec<u64> = vec![0; slugs.len()];
    let mut cursor = 0usize;
    let mut emitted: u128 = 0;
    let started = Instant::now();
    let mut tick = tokio::time::interval(TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut ping = tokio::time::interval(Duration::from_millis(settings.ping_interval_ms.max(1)));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let max_burst = burst_ceiling(settings.rate, slugs.len());

    let armed = settings
        .fault
        .filter(|fault| fault.session == settings.session_id);
    let mut fault_pending = armed.is_some();
    let mut stalled = false;
    let fault_sleep = tokio::time::sleep(armed.map_or(Duration::ZERO, |fault| {
        fault.after.saturating_sub(accepted_at.elapsed())
    }));
    tokio::pin!(fault_sleep);

    loop {
        tokio::select! {
            biased;
            () = &mut fault_sleep, if fault_pending => {
                fault_pending = false;
                let fault = armed.expect("fault_pending is only set while armed holds a fault");
                match fault.kind {
                    FaultKind::Drop => {
                        eprintln!(
                            "table_peer: session {} fault fired: drop",
                            settings.session_id
                        );
                        return;
                    }
                    FaultKind::Stall => {
                        eprintln!(
                            "table_peer: session {} fault fired: stall",
                            settings.session_id
                        );
                        stalled = true;
                    }
                }
            }
            _ = ping.tick() => {
                if !stalled && send(&mut write, "2".to_owned()).await.is_err() {
                    return;
                }
            }
            _ = tick.tick() => {
                if stalled || slugs.is_empty() {
                    continue;
                }
                let target = offered_by(started.elapsed(), settings.rate, slugs.len());
                let due = usize::try_from(target.saturating_sub(emitted))
                    .unwrap_or(usize::MAX)
                    .min(max_burst);
                for _ in 0..due {
                    let index = cursor % slugs.len();
                    cursor = index + 1;
                    versions[index] += 1;
                    let frame = book.frame(&slugs[index], versions[index], versions[index]);
                    if write.feed(Message::text(frame)).await.is_err() {
                        return;
                    }
                }
                emitted += due as u128;
                if due > 0 && write.flush().await.is_err() {
                    return;
                }
            }
            message = read.next() => {
                let text = match message {
                    Some(Ok(Message::Text(text))) => text.as_str().to_owned(),
                    Some(Ok(_)) => continue,
                    None | Some(Err(_)) => return,
                };
                let Some(next) = subscription_slugs(&text) else {
                    continue;
                };
                versions = carry_versions(&slugs, &versions, &next);
                slugs = next;
                cursor = 0;
                if acknowledge(&mut write, &slugs).await.is_err() {
                    return;
                }
            }
        }
    }
}

/// How many updates the offered rate has owed since the connection's set was established.
fn offered_by(elapsed: Duration, rate: u64, markets: usize) -> u128 {
    elapsed.as_micros() * u128::from(rate) * markets as u128 / 1_000_000
}

/// The most updates one tick may emit, so arrears surface as an achieved rate below the
/// offered one rather than as a burst charged to the daemon's queue age.
fn burst_ceiling(rate: u64, markets: usize) -> usize {
    let per_second = u128::from(rate) * markets as u128;
    let window = per_second * u128::from(MAX_CATCH_UP_TICKS) * TICK.as_millis() / 1_000;
    usize::try_from(window.max(1)).unwrap_or(usize::MAX)
}

/// Carries each surviving market's `version` counter across a set replacement.
///
/// A market that stays subscribed must not have its versions restart: a restart is a
/// continuity break the daemon would report, and it would be this generator's artifact rather
/// than anything the run was measuring.
fn carry_versions(previous: &[String], versions: &[u64], next: &[String]) -> Vec<u64> {
    next.iter()
        .map(|slug| {
            previous
                .iter()
                .position(|held| held == slug)
                .and_then(|index| versions.get(index).copied())
                .unwrap_or(0)
        })
        .collect()
}

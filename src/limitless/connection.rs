//! One connection generation to the Limitless `/markets` Socket.IO feed.
//!
//! A connection is a task. It dials an endpoint, completes the Engine.IO and namespace
//! handshake, emits the subscription for its assigned markets, then forwards every decoded
//! venue event, every heartbeat, and every local decode failure to its supervisor as a
//! generation-tagged [`ConnectionNotice`].
//!
//! It owns no book and no policy: it never concludes from silence that a connection is
//! dead, never retries, never backs off, and never touches book authority. Liveness
//! judgement and reconnect policy belong to the supervisor, which is the only place that
//! sees more than one generation.
//!
//! The write half never leaves this task, so a supervisor that wants a command on the wire
//! asks for it through a bounded [`ConnectionControl`] channel. The only such command is
//! [`ConnectionControl::Resubscribe`], which re-emits the complete desired set; deciding
//! when a resubscription is worth attempting, and reserving the pacer slot its bytes will
//! be written in, stays with the supervisor.

use crate::limitless::supervisor::reserve_command_grant;
use crate::limitless::{LimitlessDecodeError, LimitlessEvent, decode_event};
use crate::wire::lexical::LexicalLimits;
use crate::wire::session::{
    EngineIoOpen, MAX_PAYLOAD_BYTES_MAX, TerminalFrameReason, encode_engine_pong,
    encode_namespace_connect, encode_subscribe_market_prices, terminal_frame_reason,
};
use crate::wire::socketio::{
    DecodedFrame, EngineIoPacket, FrameError, SocketIoPacket, WebSocketOpcode, decode_frame,
};
use core::time::Duration;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};

/// The Limitless public market-data WebSocket endpoint.
pub const DEFAULT_ENDPOINT: &str =
    "wss://ws.limitless.exchange/socket.io/?EIO=4&transport=websocket";

/// The Socket.IO namespace carrying public order-book events.
pub const NAMESPACE: &str = "/markets";

const ORDERBOOK_UPDATE_EVENT: &str = "orderbookUpdate";
/// The venue's own out-of-band channel on `/markets`, which is where a subscription
/// acknowledgment arrives (`docs/limitless.md`).
/// The field that separates a `system` frame acknowledging a subscription from one merely
/// announcing the connection: only the subscription acknowledgment names the set.
const POLICY_MAX_PAYLOAD_BYTES: usize = MAX_PAYLOAD_BYTES_MAX as usize;

type WsStream = tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsWrite = SplitSink<WsStream, Message>;
type WsRead = SplitStream<WsStream>;

/// Everything one connection attempt needs, and nothing about the book it feeds.
///
/// `setup_timeout` bounds the whole pre-subscription phase — dial, Engine.IO open,
/// namespace acknowledgment, subscription emit — after which the supervisor's heartbeat
/// evidence takes over and no further timeout exists on this side.
///
/// `capture_path`, when set, appends every inbound text frame to that file verbatim. It is
/// a diagnostic capture facility only: the append is blocking file I/O performed on this
/// connection's own task, so it must stay `None` in any configuration that carries live
/// book traffic for consumers.
///
/// `min_command_interval` is the caller's configured floor between two subscription-bearing
/// commands this connection puts on the wire toward `endpoint`, spent immediately before
/// the write. It is the caller's value, not a fact this task knows anything about: a
/// supervisor or a shard reads it from its own configuration and every connection it spawns
/// carries the same one, which is what keeps the process-wide, per-endpoint pacer coherent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectionConfig {
    pub endpoint: String,
    pub markets: Vec<String>,
    pub setup_timeout: Duration,
    pub capture_path: Option<PathBuf>,
    pub min_command_interval: Duration,
}

/// Why one connection generation stopped producing.
///
/// Every variant is terminal for that generation: the task returns it and never retries.
/// The supervisor maps it to book continuity and authority reasons.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionEndReason {
    ConnectFailed,
    ConnectTimedOut,
    HandshakeFailed,
    HandshakeTimedOut,
    SubscribeFailed,
    SocketClosed,
    ReadFailed,
    WriteFailed,
    Terminal(TerminalFrameReason),
    CaptureFailed,
    NoticeUndeliverable,
    /// A resubscription this connection emitted did not produce an accepted recovery base
    /// within the supervisor's bounded window. Raised by the supervisor, never by this task.
    ResubscribeTimedOut,
    /// The desired market set changed, and this venue offers no correlatable boundary
    /// between the old set and its replacement, so the replacement is carried by a fresh
    /// connection generation instead. Raised by the shard, never by this task.
    SubscriptionReplaced,
    /// The supervisor saw no Engine.IO ping within the venue-negotiated deadline. Raised
    /// by the supervisor, never by this task.
    HeartbeatTimeout,
    /// The connection task did not return a reason of its own — it panicked or was
    /// cancelled.
    TaskFailed,
}

/// What a supervisor asks of one running connection.
///
/// Control is one-way and advisory: this task performs the command or ends with a reason,
/// and never argues about whether it was worth sending.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectionControl {
    /// Re-emit the complete desired market set as one `subscribe_market_prices` command,
    /// which the venue treats as a replacement of this connection's current set. Reported
    /// back as [`ConnectionNote::Resubscribed`] once it is on the wire.
    ///
    /// `granted_at` is the instant the endpoint's process-wide command pacer reserved for
    /// these bytes, taken by the caller at the moment it decided to send them. The
    /// connection waits until then and writes; it does not reserve a second place in the
    /// queue, because the caller has already budgeted its deadline against this one.
    Resubscribe { granted_at: Instant },
    /// Adopt this complete market set as the connection's desired set and emit it as one
    /// `subscribe_market_prices` command, which the venue treats as a replacement of the
    /// connection's current set.
    ///
    /// The set is replaced whole and never merged with what the connection held: the caller
    /// owns the desired state and this task reproduces it. A later
    /// [`Self::Resubscribe`] re-emits whatever set arrived last. Reported back as
    /// [`ConnectionNote::Resubscribed`] once it is on the wire.
    ///
    /// It carries no reservation: the connection takes its own place in the endpoint's
    /// queue immediately before writing, as the establishing subscription does.
    Replace { markets: Vec<String> },
}

/// What one connection has to say to its supervisor.
///
/// [`Self::Heartbeat`] is the only liveness evidence this feed produces: it is emitted for
/// a server Engine.IO ping and for nothing else. Market data never counts as heartbeat
/// evidence, so a quiet subscribed market cannot keep a dead socket alive and cannot be
/// mistaken for one.
#[derive(Clone, Debug)]
pub enum ConnectionNote {
    /// The venue accepted the namespace and the subscription was emitted. `observed_at` is
    /// when the Engine.IO open packet was read, which is the first instant this connection
    /// has liveness evidence for.
    Ready {
        open: EngineIoOpen,
        subscription_generation: u64,
        observed_at: Instant,
    },
    /// A server Engine.IO ping arrived at `observed_at` and was answered.
    Heartbeat { observed_at: Instant },
    /// A subscription command put the complete desired set back on the wire, advancing this
    /// connection's *emitted* subscription generation to `subscription_generation`.
    ///
    /// This is a fact about the local write, not about the venue: it says the bytes left
    /// this process, and nothing about which frames belong to the new set. Ordered ahead of
    /// every event that followed the emit and never dropped by queue pressure.
    Resubscribed { subscription_generation: u64 },
    /// A decoded `/markets` event, with the instant its frame was read and the subscription
    /// generation this connection had emitted when it was read.
    ///
    /// The stamp is provenance, not set attribution: this venue publishes no correlatable
    /// acknowledgment, so which *set* a frame belongs to is decided by the connection
    /// generation the notice carries rather than by anything inside it. It is assigned here,
    /// where the frame is read, so it names the emit that was outstanding at that instant
    /// whatever else has happened since.
    Event {
        event: LimitlessEvent,
        /// The monotonic instant this frame's socket read returned.
        ///
        /// Every interval this daemon measures within its own process is measured from
        /// here — the ingest queue age, and the publish latency of whatever revision this
        /// frame produces — because `docs/design.md` "Health and observability" measures a
        /// local stage on a local monotonic clock, and a wall-clock interval would report
        /// the clock's own corrections as latency.
        received_at: Instant,
        /// Wall-clock nanoseconds since the Unix epoch, sampled at the same socket read as
        /// `received_at` and adjacent to it, so the two name one event on two clocks.
        ///
        /// A downstream shared-memory consumer runs in another process and shares no
        /// monotonic origin with this one, so provenance that must cross that boundary is
        /// stamped from a clock every process reads the same way, never from `Instant`. 0
        /// never occurs on this path — it would need a system clock reporting a time before
        /// the Unix epoch — but every downstream consumer treats 0 as "no venue frame drove
        /// this" regardless of cause.
        arrival_time_nanos: u64,
        subscription_generation: u64,
    },
    /// A frame or event that would not decode. `key` is a bounded, fixed discriminant
    /// name. `book_relevant` is true when the undecodable bytes could have carried a book
    /// update — every frame-level failure, and an event-level failure on an
    /// `orderbookUpdate` — and false for an event this decoder models but rejects for
    /// another reason, which hides no book state.
    DecodeFailure {
        key: &'static str,
        book_relevant: bool,
    },
    /// `dropped` notices could not be handed to the supervisor because its bounded queue
    /// was full. Reported as soon as the queue has room, ahead of any later note, so the
    /// supervisor learns of the loss before it accepts anything that followed it.
    Overload { dropped: u64 },
}

/// One note stamped with the connection generation that produced it.
///
/// The generation is the only thing that makes a late note distinguishable from a current
/// one; the supervisor discards every notice whose generation is not the one it currently
/// publishes from.
#[derive(Clone, Debug)]
pub struct ConnectionNotice {
    pub generation: u64,
    pub note: ConnectionNote,
}

/// Runs one connection generation to completion and returns why it ended.
///
/// Forwards notices through `notices`, a bounded queue whose overflow behavior is
/// drop-newest with explicit ordered reporting: a notice that does not fit is dropped
/// rather than awaited, so a busy supervisor can never stall this read loop, and the drop
/// is reported as [`ConnectionNote::Overload`] before any notice that followed it — see
/// [`NoticeSink`]. Room for that report is waited on alongside the socket, so a loss
/// reaches the supervisor whether or not the venue speaks again. `frames` counts every
/// inbound WebSocket message this generation read after its subscription was emitted,
/// shared across generations for run-level diagnostics only; nothing reads it for a
/// decision.
///
/// `control` carries supervisor commands for this connection's write half. It is polled
/// ahead of inbound traffic, so a flood of market data cannot starve a resubscription, and
/// a closed control channel simply means no further command will ever arrive — it never
/// ends the connection.
pub async fn run_connection(
    generation: u64,
    config: ConnectionConfig,
    notices: mpsc::Sender<ConnectionNotice>,
    frames: Arc<AtomicU64>,
    control: mpsc::Receiver<ConnectionControl>,
) -> ConnectionEndReason {
    let capture = match open_capture(config.capture_path.as_ref()) {
        Ok(capture) => capture,
        Err(reason) => return reason,
    };
    let deadline = Instant::now() + config.setup_timeout;
    let ws_config = WebSocketConfig::default()
        .max_message_size(Some(POLICY_MAX_PAYLOAD_BYTES))
        .max_frame_size(Some(POLICY_MAX_PAYLOAD_BYTES));
    let connected = tokio::time::timeout_at(
        deadline,
        connect_async_with_config(config.endpoint.as_str(), Some(ws_config), true),
    )
    .await;
    let stream = match connected {
        Err(_) => return ConnectionEndReason::ConnectTimedOut,
        Ok(Err(_)) => return ConnectionEndReason::ConnectFailed,
        Ok(Ok((stream, _response))) => stream,
    };
    let (write, mut read) = stream.split();
    let mut session = Session {
        endpoint: config.endpoint.clone(),
        write,
        capture,
        limits: LexicalLimits::venue_payload(),
        write_timeout: config.setup_timeout,
        max_command_bytes: POLICY_MAX_PAYLOAD_BYTES,
        min_command_interval: config.min_command_interval,
    };

    let opened = match tokio::time::timeout_at(deadline, await_open(&mut read, &mut session)).await
    {
        Err(_) => return ConnectionEndReason::HandshakeTimedOut,
        Ok(Err(reason)) => return reason,
        Ok(Ok(opened)) => opened,
    };
    session.max_command_bytes =
        (opened.open.max_payload_bytes() as usize).min(POLICY_MAX_PAYLOAD_BYTES);
    session.limits = LexicalLimits::venue_payload().with_max_bytes(session.max_command_bytes);
    session.write_timeout = Duration::from_millis(opened.open.ping_timeout_ms());

    let ready = tokio::time::timeout_at(
        deadline,
        establish(&mut read, &mut session, &config.markets),
    )
    .await;
    let subscription_generation = match ready {
        Err(_) => return ConnectionEndReason::HandshakeTimedOut,
        Ok(Err(reason)) => return reason,
        Ok(Ok(subscription_generation)) => subscription_generation,
    };

    let announced = tokio::time::timeout_at(
        deadline,
        notices.send(ConnectionNotice {
            generation,
            note: ConnectionNote::Ready {
                open: opened.open,
                subscription_generation,
                observed_at: opened.observed_at,
            },
        }),
    )
    .await;
    if !matches!(announced, Ok(Ok(()))) {
        return ConnectionEndReason::NoticeUndeliverable;
    }

    read_loop(ReadLoop {
        read: &mut read,
        session: &mut session,
        notices: &notices,
        control,
        generation,
        frames: &frames,
        markets: config.markets,
        subscription_generation,
    })
    .await
}

/// The negotiated, post-open half of a connection: the write half, the diagnostic capture
/// file, the derived lexical decode limits, the bounded-write timeout, and the negotiated
/// outbound command size cap.
struct Session {
    endpoint: String,
    write: WsWrite,
    capture: Option<std::fs::File>,
    limits: LexicalLimits,
    write_timeout: Duration,
    max_command_bytes: usize,
    min_command_interval: Duration,
}

struct Opened {
    open: EngineIoOpen,
    observed_at: Instant,
}

fn open_capture(path: Option<&PathBuf>) -> Result<Option<std::fs::File>, ConnectionEndReason> {
    match path {
        None => Ok(None),
        Some(path) => std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map(Some)
            .map_err(|_| ConnectionEndReason::CaptureFailed),
    }
}

async fn await_open(
    read: &mut WsRead,
    session: &mut Session,
) -> Result<Opened, ConnectionEndReason> {
    let frame = next_frame(read, session).await?;
    if frame.engine_io() != Some(EngineIoPacket::Open) {
        return Err(ConnectionEndReason::HandshakeFailed);
    }
    let payload = frame
        .payload()
        .ok_or(ConnectionEndReason::HandshakeFailed)?;
    let open = EngineIoOpen::from_open_payload(payload)
        .map_err(|_| ConnectionEndReason::HandshakeFailed)?;
    Ok(Opened {
        open,
        observed_at: Instant::now(),
    })
}

/// Connects the `/markets` namespace, waits for its acknowledgment, then emits the
/// complete desired subscription set as one command, and returns the connection's
/// subscription generation.
///
/// The subscription generation counts emits on this connection: it starts at zero and
/// advances with every complete-set emit, so provenance can name which emit an event
/// arrived under. A connection that never subscribes never reaches this value.
async fn establish(
    read: &mut WsRead,
    session: &mut Session,
    markets: &[String],
) -> Result<u64, ConnectionEndReason> {
    send_bounded(session, Message::text(encode_namespace_connect(NAMESPACE))).await?;
    loop {
        let frame = next_frame(read, session).await?;
        match frame.socket_io() {
            Some(SocketIoPacket::Connect) if frame.namespace() == Some(NAMESPACE) => break,
            Some(SocketIoPacket::ConnectError) if frame.namespace() == Some(NAMESPACE) => {
                return Err(ConnectionEndReason::HandshakeFailed);
            }
            _ => continue,
        }
    }
    let mut subscription_generation: u64 = 0;
    resubscribe(session, markets, None).await?;
    subscription_generation = subscription_generation.saturating_add(1);
    Ok(subscription_generation)
}

/// Emits the complete desired market set as one `subscribe_market_prices` command, which is
/// what both the initial subscription and every later re-emit put on the wire.
///
/// `granted_at` is the place in the endpoint's command queue these bytes already hold, taken
/// by whoever decided to send them; `None` means this call takes its own. Either way the
/// wait happens here, immediately around the write, because the floor is a property of the
/// bytes on the wire rather than of the decision to send them: a daemon that metered its own
/// decision points would still cluster commands inside the floor whenever two connections'
/// dials and handshakes converged, since neither the dial nor the handshake is timed by the
/// code that authorized the command.
///
/// A caller that reserved for itself — a supervisor or shard emitting a recovery re-emit —
/// budgets its own deadline against the instant it was granted, so reserving again here
/// would put the command behind a second place in the queue that no deadline allowed for.
///
/// Fails with [`ConnectionEndReason::SubscribeFailed`] when the encoded command exceeds the
/// session's negotiated payload cap, and with [`ConnectionEndReason::WriteFailed`] when the
/// bounded write does not complete. Never partially emits a set: the venue sees one
/// replacement command or nothing. The size check precedes an unreserved call's own
/// reservation, so nothing is taken for a command that cannot be sent; a reservation made
/// upstream for a command that then fails this check is one of the schedule holes
/// [`reserve_command_grant`] documents as accepted.
async fn resubscribe(
    session: &mut Session,
    markets: &[String],
    granted_at: Option<Instant>,
) -> Result<(), ConnectionEndReason> {
    let subscribe = encode_subscribe_market_prices(NAMESPACE, markets);
    check_command_size(subscribe.len(), session.max_command_bytes)
        .map_err(|_| ConnectionEndReason::SubscribeFailed)?;
    let granted = granted_at.unwrap_or_else(|| {
        reserve_command_grant(
            session.endpoint.as_str(),
            Instant::now(),
            session.min_command_interval,
        )
    });
    tokio::time::sleep_until(granted).await;
    send_bounded(session, Message::text(subscribe)).await
}

/// Everything the steady-state loop borrows, gathered so the loop keeps one parameter per
/// concern rather than a long positional list.
struct ReadLoop<'a> {
    read: &'a mut WsRead,
    session: &'a mut Session,
    notices: &'a mpsc::Sender<ConnectionNotice>,
    control: mpsc::Receiver<ConnectionControl>,
    generation: u64,
    frames: &'a AtomicU64,
    markets: Vec<String>,
    subscription_generation: u64,
}

/// Forwards decoded traffic until the socket ends, the venue sends a terminal frame, or a
/// write fails. Never returns because the market went quiet: silence is not a reason here.
async fn read_loop(loop_state: ReadLoop<'_>) -> ConnectionEndReason {
    let ReadLoop {
        read,
        session,
        notices,
        mut control,
        generation,
        frames,
        mut markets,
        mut subscription_generation,
    } = loop_state;
    let mut sink = NoticeSink::new(notices, generation);
    let mut commandable = true;
    loop {
        sink.flush_pending();
        let message = tokio::select! {
            biased;
            reserved = notices.reserve(), if sink.has_pending() => {
                match reserved {
                    Ok(room) => sink.deliver(room),
                    Err(_) => sink.abandon(),
                }
                continue;
            }
            command = control.recv(), if commandable => {
                match command {
                    Some(command) => {
                        let granted_at = match command {
                            ConnectionControl::Resubscribe { granted_at } => Some(granted_at),
                            ConnectionControl::Replace { markets: next } => {
                                markets = next;
                                None
                            }
                        };
                        if let Err(reason) = resubscribe(session, &markets, granted_at).await {
                            return sink.finish(reason);
                        }
                        subscription_generation = subscription_generation.saturating_add(1);
                        sink.offer_resubscribed(subscription_generation);
                    }
                    None => commandable = false,
                }
                continue;
            }
            message = read.next() => message,
        };
        let message = match message {
            None => return sink.finish(ConnectionEndReason::SocketClosed),
            Some(Err(_)) => return sink.finish(ConnectionEndReason::ReadFailed),
            Some(Ok(message)) => message,
        };
        frames.fetch_add(1, Ordering::Relaxed);
        let received_at = Instant::now();
        let arrival_time_nanos = wall_clock_arrival_nanos();
        if capture_message(session, &message).is_err() {
            return sink.finish(ConnectionEndReason::CaptureFailed);
        }
        let (opcode, bytes) = wire_view(&message);
        let frame = match decode_frame(bytes, opcode, session.limits) {
            Ok(frame) => frame,
            Err(error) => {
                sink.offer(ConnectionNote::DecodeFailure {
                    key: frame_error_key(&error),
                    book_relevant: true,
                });
                continue;
            }
        };
        if let Some(reason) = terminal_frame_reason(&frame, NAMESPACE) {
            return sink.finish(ConnectionEndReason::Terminal(reason));
        }
        if frame.engine_io() == Some(EngineIoPacket::Ping) {
            if send_bounded(session, Message::text(encode_engine_pong()))
                .await
                .is_err()
            {
                return sink.finish(ConnectionEndReason::WriteFailed);
            }
            sink.offer(ConnectionNote::Heartbeat {
                observed_at: received_at,
            });
        }
        if frame.event_name().is_some() {
            let note = match decode_event(&frame) {
                Ok(event) => ConnectionNote::Event {
                    event,
                    received_at,
                    arrival_time_nanos,
                    subscription_generation,
                },
                Err(error) => ConnectionNote::DecodeFailure {
                    key: event_error_key(&error),
                    book_relevant: frame.event_name() == Some(ORDERBOOK_UPDATE_EVENT),
                },
            };
            sink.offer(note);
        }
    }
}

/// Wall-clock nanoseconds since the Unix epoch, sampled at socket-frame arrival.
///
/// Saturates to 0 when the system clock reports a time before the epoch, which is the same
/// value downstream already reads as "not driven by a venue frame" — a clock that far wrong
/// is a machine-configuration fault this daemon has no way to distinguish from that case.
fn wall_clock_arrival_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn wire_view(message: &Message) -> (WebSocketOpcode, &[u8]) {
    match message {
        Message::Text(text) => (WebSocketOpcode::Text, text.as_bytes()),
        Message::Binary(data) => (WebSocketOpcode::Binary, data.as_ref()),
        Message::Ping(_) => (WebSocketOpcode::Ping, &[]),
        Message::Pong(_) => (WebSocketOpcode::Pong, &[]),
        Message::Close(_) => (WebSocketOpcode::Close, &[]),
        Message::Frame(_) => (WebSocketOpcode::Binary, &[]),
    }
}

fn capture_message(session: &mut Session, message: &Message) -> Result<(), ()> {
    if let Message::Text(text) = message
        && let Some(file) = &mut session.capture
    {
        writeln!(file, "{text}").map_err(|_| ())?;
    }
    Ok(())
}

/// The bounded, drop-newest path from one connection to its supervisor, and the ordering
/// rule that keeps a loss report ahead of the data that followed it.
///
/// Two notes are must-deliver: [`ConnectionNote::Overload`], which says continuity broke,
/// and [`ConnectionNote::Resubscribed`], which says a re-emit reached the wire. Both are
/// re-offered ahead of every ordinary notice and in the order they occurred; an ordinary
/// notice that would reach the supervisor first is itself counted and dropped, so the
/// supervisor can never apply what followed a loss before it learns of the loss, and never
/// start a response window for a command that is not on the wire.
///
/// The pending set is bounded at one of each by construction: repeated drops accumulate
/// into one count, and a second re-emit supersedes the first, whose subscription generation
/// no event can still be arriving under.
struct NoticeSink<'a> {
    notices: &'a mpsc::Sender<ConnectionNotice>,
    generation: u64,
    dropped: u64,
    dropped_at: Option<u64>,
    resubscribed: Option<u64>,
    resubscribed_at: Option<u64>,
    sequence: u64,
}

/// One must-deliver note awaiting room in the supervisor's queue.
///
/// Each kind occupies one slot and coalesces: a second emit before the first has been
/// delivered supersedes it, because the generations are monotone and the newer one carries
/// everything the older said. Storage is therefore two slots whatever the venue does, and
/// their delivery order is the order they occurred.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Pending {
    Overload,
    Resubscribed,
}

impl<'a> NoticeSink<'a> {
    fn new(notices: &'a mpsc::Sender<ConnectionNotice>, generation: u64) -> Self {
        Self {
            notices,
            generation,
            dropped: 0,
            dropped_at: None,
            resubscribed: None,
            resubscribed_at: None,
            sequence: 0,
        }
    }

    /// The next occurrence stamp, which is what orders the must-deliver slots against one
    /// another.
    fn stamp(&mut self) -> u64 {
        self.sequence = self.sequence.saturating_add(1);
        self.sequence
    }

    /// Hands an ordinary notice to the supervisor without ever waiting for it.
    ///
    /// A full queue drops this notice and counts it; so does a pending must-deliver note
    /// that still does not fit, because an ordinary notice may never overtake one.
    fn offer(&mut self, note: ConnectionNote) {
        if !self.flush_pending() || !self.send(note) {
            if self.dropped == 0 {
                self.dropped_at = Some(self.stamp());
            }
            self.dropped = self.dropped.saturating_add(1);
        }
    }

    /// Records that a subscription command reached the wire, and offers it ahead of ordinary
    /// traffic.
    ///
    /// The note is retained until the queue takes it, so queue pressure can delay the
    /// supervisor's knowledge of the emit but never erase it.
    fn offer_resubscribed(&mut self, subscription_generation: u64) {
        if self.resubscribed.is_none() {
            self.resubscribed_at = Some(self.stamp());
        }
        self.resubscribed = Some(subscription_generation);
        self.flush_pending();
    }

    /// Delivers every pending must-deliver note, oldest first, and reports whether none
    /// remains. Stops at the first note that does not fit, so their order is preserved.
    fn flush_pending(&mut self) -> bool {
        self.pending_order()
            .into_iter()
            .all(|pending| self.flush_one(pending))
    }

    /// Whether a must-deliver note is still waiting for room.
    fn has_pending(&self) -> bool {
        self.dropped > 0 || self.resubscribed.is_some()
    }

    /// Sends the oldest pending must-deliver note into room reserved for it.
    ///
    /// Reserving room is how a loss reaches the supervisor without another inbound frame to
    /// carry it: a connection that has dropped something waits for the queue to drain in
    /// parallel with reading, rather than holding the report until the venue happens to
    /// speak again.
    fn deliver(&mut self, room: mpsc::Permit<'_, ConnectionNotice>) {
        for pending in self.pending_order() {
            let Some(note) = self.take(pending) else {
                continue;
            };
            room.send(ConnectionNotice {
                generation: self.generation,
                note,
            });
            return;
        }
    }

    /// Gives up on notes nobody is left to read: a closed queue means the supervisor is
    /// gone, and this generation's own end reason is all it can still report.
    fn abandon(&mut self) {
        self.dropped = 0;
        self.dropped_at = None;
        self.resubscribed = None;
        self.resubscribed_at = None;
    }

    /// The order pending notes must reach the supervisor in, which is the order they
    /// occurred. A slot holding nothing sorts last and is skipped.
    fn pending_order(&self) -> [Pending; 2] {
        let mut slots = [
            (self.dropped_at.unwrap_or(u64::MAX), Pending::Overload),
            (
                self.resubscribed_at.unwrap_or(u64::MAX),
                Pending::Resubscribed,
            ),
        ];
        slots.sort_by_key(|(at, _)| *at);
        [slots[0].1, slots[1].1]
    }

    /// Takes one pending note out of its slot, or `None` when that slot holds nothing.
    fn take(&mut self, pending: Pending) -> Option<ConnectionNote> {
        match pending {
            Pending::Overload => {
                if self.dropped == 0 {
                    return None;
                }
                let note = ConnectionNote::Overload {
                    dropped: self.dropped,
                };
                self.dropped = 0;
                self.dropped_at = None;
                Some(note)
            }
            Pending::Resubscribed => {
                let subscription_generation = self.resubscribed.take()?;
                self.resubscribed_at = None;
                Some(ConnectionNote::Resubscribed {
                    subscription_generation,
                })
            }
        }
    }

    /// Sends one pending note if its slot holds one, restoring the slot when the queue is
    /// full so nothing is lost by trying.
    fn flush_one(&mut self, pending: Pending) -> bool {
        let restore = match pending {
            Pending::Overload => (self.dropped, self.dropped_at),
            Pending::Resubscribed => (0, self.resubscribed_at),
        };
        let value = match pending {
            Pending::Overload => None,
            Pending::Resubscribed => self.resubscribed,
        };
        let Some(note) = self.take(pending) else {
            return true;
        };
        if self.send(note) {
            return true;
        }
        match pending {
            Pending::Overload => {
                self.dropped = restore.0;
                self.dropped_at = restore.1;
            }
            Pending::Resubscribed => {
                self.resubscribed = value;
                self.resubscribed_at = restore.1;
            }
        }
        false
    }

    /// Whether `note` is no longer this sink's concern. A closed queue means the supervisor
    /// is gone and nothing is owed to it; only a full queue is a drop.
    fn send(&self, note: ConnectionNote) -> bool {
        !matches!(
            self.notices.try_send(ConnectionNotice {
                generation: self.generation,
                note,
            }),
            Err(mpsc::error::TrySendError::Full(_))
        )
    }

    /// Ends this generation, giving a pending loss report one last chance to be delivered.
    ///
    /// An overload the supervisor never learned of outlives the connection that dropped it,
    /// so a generation still holding one ends as [`ConnectionEndReason::NoticeUndeliverable`]
    /// rather than under a transport reason that would present the loss as an ordinary
    /// disconnect.
    fn finish(mut self, reason: ConnectionEndReason) -> ConnectionEndReason {
        self.flush_pending();
        if self.dropped > 0 {
            return ConnectionEndReason::NoticeUndeliverable;
        }
        reason
    }
}

/// Reads and decodes the next text frame, answering an Engine.IO ping with a bounded write.
async fn next_frame(
    read: &mut WsRead,
    session: &mut Session,
) -> Result<DecodedFrame, ConnectionEndReason> {
    loop {
        let message = match read.next().await {
            None => return Err(ConnectionEndReason::SocketClosed),
            Some(Err(_)) => return Err(ConnectionEndReason::ReadFailed),
            Some(Ok(message)) => message,
        };
        if capture_message(session, &message).is_err() {
            return Err(ConnectionEndReason::CaptureFailed);
        }
        let Message::Text(text) = &message else {
            continue;
        };
        let frame = decode_frame(text.as_bytes(), WebSocketOpcode::Text, session.limits)
            .map_err(|_| ConnectionEndReason::HandshakeFailed)?;
        if frame.engine_io() == Some(EngineIoPacket::Ping) {
            send_bounded(session, Message::text(encode_engine_pong())).await?;
        }
        return Ok(frame);
    }
}

async fn send_bounded(session: &mut Session, message: Message) -> Result<(), ConnectionEndReason> {
    tokio::time::timeout(session.write_timeout, session.write.send(message))
        .await
        .map_err(|_| ConnectionEndReason::WriteFailed)?
        .map_err(|_| ConnectionEndReason::WriteFailed)
}

/// An encoded outbound command exceeded the negotiated `maxPayload` cap.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandTooLarge {
    pub bytes: usize,
    pub limit: usize,
}

/// Checks an encoded outbound command's byte length against the session's negotiated
/// `maxPayload` cap, clamped to the transport policy ceiling. The caller must check before
/// the frame is queued for send, never after.
pub fn check_command_size(bytes: usize, limit: usize) -> Result<(), CommandTooLarge> {
    if bytes > limit {
        return Err(CommandTooLarge { bytes, limit });
    }
    Ok(())
}

/// Maps a frame decode failure to a bounded, fixed discriminant name for counting. Never
/// embeds the error's offset, length, or value fields, so a hostile stream of distinct
/// malformed inputs cannot grow a failure-count map beyond one entry per variant.
pub fn frame_error_key(error: &FrameError) -> &'static str {
    match error {
        FrameError::EmptyFrame => "frame:EmptyFrame",
        FrameError::UnknownEngineIoPacket => "frame:UnknownEngineIoPacket",
        FrameError::UnknownSocketIoPacket => "frame:UnknownSocketIoPacket",
        FrameError::MalformedFraming => "frame:MalformedFraming",
        FrameError::MalformedEventEnvelope => "frame:MalformedEventEnvelope",
        FrameError::MissingAttachmentCount => "frame:MissingAttachmentCount",
        FrameError::UnexpectedControlPayload => "frame:UnexpectedControlPayload",
        FrameError::Lexical(_) => "frame:Lexical",
    }
}

/// Maps a `/markets` event decode failure to a bounded, fixed discriminant name for
/// counting, for the same reason as [`frame_error_key`].
pub fn event_error_key(error: &LimitlessDecodeError) -> &'static str {
    match error {
        LimitlessDecodeError::NotAnEvent => "event:NotAnEvent",
        LimitlessDecodeError::InvalidField { .. } => "event:InvalidField",
        LimitlessDecodeError::Decimal { .. } => "event:Decimal",
        LimitlessDecodeError::PriceOutOfDomain { .. } => "event:PriceOutOfDomain",
        LimitlessDecodeError::LevelOrdering { .. } => "event:LevelOrdering",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_command_size_allows_frame_at_cap() {
        assert!(check_command_size(100, 100).is_ok());
    }

    #[test]
    fn check_command_size_allows_frame_under_cap() {
        assert!(check_command_size(99, 100).is_ok());
    }

    fn decode_failure(key: &'static str) -> ConnectionNote {
        ConnectionNote::DecodeFailure {
            key,
            book_relevant: false,
        }
    }

    fn note_of(notice: Option<ConnectionNotice>) -> ConnectionNote {
        notice.expect("the queue holds a notice").note
    }

    #[test]
    fn an_ordinary_notice_never_overtakes_the_overload_marker() {
        let (tx, mut rx) = mpsc::channel(1);
        let mut sink = NoticeSink::new(&tx, 7);
        sink.offer(decode_failure("first"));
        sink.offer(decode_failure("dropped"));
        assert_eq!(sink.dropped, 1, "the full queue dropped the second notice");

        let _first = rx.try_recv();
        sink.offer(decode_failure("after the loss"));
        assert!(
            matches!(
                note_of(rx.try_recv().ok()),
                ConnectionNote::Overload { dropped: 1 }
            ),
            "the freed slot must carry the loss report, not the notice that followed it"
        );
        assert_eq!(
            sink.dropped, 1,
            "the notice that would have overtaken the marker is itself counted"
        );
    }

    #[test]
    fn a_resubscription_is_never_dropped_by_queue_pressure() {
        let (tx, mut rx) = mpsc::channel(1);
        let mut sink = NoticeSink::new(&tx, 7);
        sink.offer(decode_failure("occupies the queue"));
        sink.offer_resubscribed(2);
        assert_eq!(
            sink.resubscribed,
            Some(2),
            "the re-emit is retained, not lost"
        );

        sink.offer(decode_failure("after the re-emit"));
        assert_eq!(sink.dropped, 1);
        let _occupant = rx.try_recv();
        sink.flush_pending();
        assert!(
            matches!(
                note_of(rx.try_recv().ok()),
                ConnectionNote::Resubscribed {
                    subscription_generation: 2
                }
            ),
            "the re-emit is delivered before the loss that followed it"
        );
        sink.flush_pending();
        assert!(matches!(
            note_of(rx.try_recv().ok()),
            ConnectionNote::Overload { dropped: 1 }
        ));
    }

    #[test]
    fn a_generation_ending_with_an_undelivered_overload_reports_it() {
        let (tx, rx) = mpsc::channel(1);
        let mut sink = NoticeSink::new(&tx, 7);
        sink.offer(decode_failure("occupies the queue"));
        sink.offer(decode_failure("dropped"));
        assert_eq!(
            sink.finish(ConnectionEndReason::SocketClosed),
            ConnectionEndReason::NoticeUndeliverable,
            "a transport reason must not mask a loss the supervisor never learned of"
        );
        drop(rx);
    }

    #[test]
    fn a_generation_ending_with_a_delivered_overload_keeps_its_transport_reason() {
        let (tx, mut rx) = mpsc::channel(2);
        let mut sink = NoticeSink::new(&tx, 7);
        sink.offer(decode_failure("occupies the queue"));
        sink.offer(decode_failure("occupies the queue"));
        sink.offer(decode_failure("dropped"));
        let _first = rx.try_recv();
        assert_eq!(
            sink.finish(ConnectionEndReason::SocketClosed),
            ConnectionEndReason::SocketClosed
        );
    }

    #[test]
    fn check_command_size_rejects_frame_over_cap() {
        let error = check_command_size(101, 100).unwrap_err();
        assert_eq!(
            error,
            CommandTooLarge {
                bytes: 101,
                limit: 100
            }
        );
    }
}

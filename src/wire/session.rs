use crate::wire::lexical::{LexicalKind, LexicalValue};
use crate::wire::socketio::{DecodedFrame, EngineIoPacket, SocketIoPacket};
use core::fmt;
use core::time::Duration;

const PING_INTERVAL_MS_MIN: u64 = 1_000;
const PING_INTERVAL_MS_MAX: u64 = 300_000;
const PING_TIMEOUT_MS_MIN: u64 = 1_000;
const PING_TIMEOUT_MS_MAX: u64 = 300_000;
const MAX_PAYLOAD_BYTES_MIN: u64 = 1_024;

/// The policy ceiling for `maxPayload`: the ceiling `EngineIoOpen` enforces on the
/// venue's negotiated value, the WebSocket transport's configured maximum message
/// and frame size, and the upper bound clamping post-negotiation lexical decode
/// limits.
pub const MAX_PAYLOAD_BYTES_MAX: u64 = 16 * 1024 * 1024;

/// The negotiated Engine.IO session announced by the server's `open` packet.
///
/// `sid` is the opaque session identifier. `ping_interval_ms` and `ping_timeout_ms`
/// are the server-advertised heartbeat cadence and grace period; a connection has
/// lost liveness evidence if no server ping arrives within their sum.
/// `max_payload_bytes` is the largest message the server accepts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineIoOpen {
    sid: String,
    ping_interval_ms: u64,
    ping_timeout_ms: u64,
    max_payload_bytes: u64,
}

impl EngineIoOpen {
    /// Parses and validates an Engine.IO `open` packet payload.
    ///
    /// `payload` is the decoded JSON object carried by the packet. Returns a
    /// distinct [`SessionError`] for a missing field, a field of the wrong
    /// JSON kind, a numeric field whose lexeme is not a plain non-negative
    /// integer, or a numeric field outside its pinned policy bounds
    /// (`ping_interval_ms` and `ping_timeout_ms`: 1s..=5min; `max_payload_bytes`:
    /// 1KiB..=16MiB).
    pub fn from_open_payload(payload: &LexicalValue) -> Result<Self, SessionError> {
        let sid = read_text(payload, "sid")?.to_owned();
        let ping_interval_ms = read_bounded_integer(
            payload,
            "pingInterval",
            PING_INTERVAL_MS_MIN,
            PING_INTERVAL_MS_MAX,
        )?;
        let ping_timeout_ms = read_bounded_integer(
            payload,
            "pingTimeout",
            PING_TIMEOUT_MS_MIN,
            PING_TIMEOUT_MS_MAX,
        )?;
        let max_payload_bytes = read_bounded_integer(
            payload,
            "maxPayload",
            MAX_PAYLOAD_BYTES_MIN,
            MAX_PAYLOAD_BYTES_MAX,
        )?;
        Ok(Self {
            sid,
            ping_interval_ms,
            ping_timeout_ms,
            max_payload_bytes,
        })
    }

    pub fn sid(&self) -> &str {
        &self.sid
    }

    pub fn ping_interval_ms(&self) -> u64 {
        self.ping_interval_ms
    }

    pub fn ping_timeout_ms(&self) -> u64 {
        self.ping_timeout_ms
    }

    pub fn max_payload_bytes(&self) -> u64 {
        self.max_payload_bytes
    }

    /// The liveness deadline: the daemon must see a server ping within this
    /// duration of the previous one, or connection health evidence is lost.
    pub fn heartbeat_deadline(&self) -> Duration {
        Duration::from_millis(self.ping_interval_ms + self.ping_timeout_ms)
    }
}

fn read_text<'a>(payload: &'a LexicalValue, field: &'static str) -> Result<&'a str, SessionError> {
    match payload.field(field) {
        None => Err(SessionError::MissingField(field)),
        Some(LexicalValue::Text(value)) => Ok(value),
        Some(other) => Err(SessionError::UnexpectedType {
            field,
            kind: other.kind(),
        }),
    }
}

fn read_bounded_integer(
    payload: &LexicalValue,
    field: &'static str,
    min: u64,
    max: u64,
) -> Result<u64, SessionError> {
    let number = match payload.field(field) {
        None => return Err(SessionError::MissingField(field)),
        Some(LexicalValue::Number(number)) => number,
        Some(other) => {
            return Err(SessionError::UnexpectedType {
                field,
                kind: other.kind(),
            });
        }
    };
    let value = number
        .as_str()
        .parse::<u64>()
        .map_err(|_| SessionError::NonIntegerLexeme { field })?;
    if value < min || value > max {
        return Err(SessionError::OutOfPolicy { field, value });
    }
    Ok(value)
}

/// A distinct, programmatically matchable reason an Engine.IO `open` payload
/// was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionError {
    MissingField(&'static str),
    UnexpectedType {
        field: &'static str,
        kind: LexicalKind,
    },
    NonIntegerLexeme {
        field: &'static str,
    },
    OutOfPolicy {
        field: &'static str,
        value: u64,
    },
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid Engine.IO open session")
    }
}

impl std::error::Error for SessionError {}

/// Encodes a Socket.IO namespace connect packet: `40<namespace>,`.
pub fn encode_namespace_connect(namespace: &str) -> String {
    format!("40{namespace},")
}

/// Encodes a Socket.IO namespace disconnect packet: `41<namespace>,`.
pub fn encode_namespace_disconnect(namespace: &str) -> String {
    format!("41{namespace},")
}

/// Encodes an Engine.IO pong packet: `3`.
pub fn encode_engine_pong() -> String {
    "3".to_owned()
}

/// Encodes a `subscribe_market_prices` Socket.IO event for `namespace`:
/// `42<namespace>,["subscribe_market_prices",{"marketSlugs":[<slugs>]}]`.
pub fn encode_subscribe_market_prices(namespace: &str, slugs: &[String]) -> String {
    let payload = serde_json::json!(["subscribe_market_prices", { "marketSlugs": slugs }]);
    format!("42{namespace},{payload}")
}

/// A distinct reason a decoded frame ends the session rather than being one
/// more frame in an ongoing stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminalFrameReason {
    /// The Engine.IO transport itself closed (wire packet `1`).
    EngineIoClose,
    /// The venue disconnected the subscribed namespace (`41<namespace>,`).
    NamespaceDisconnect,
    /// The venue rejected the subscribed namespace's connect
    /// (`44<namespace>,...`).
    NamespaceConnectError,
}

/// Classifies a decoded frame as ending the session, or `None` if the stream
/// continues.
///
/// An Engine.IO close packet always ends the session. A Socket.IO disconnect
/// or connect-error packet ends the session only when it targets `namespace`;
/// the same packets addressed to a different namespace leave this session's
/// stream unaffected and map to `None`.
pub fn terminal_frame_reason(frame: &DecodedFrame, namespace: &str) -> Option<TerminalFrameReason> {
    match frame.engine_io() {
        Some(EngineIoPacket::Close) => Some(TerminalFrameReason::EngineIoClose),
        Some(EngineIoPacket::Message) => match frame.socket_io() {
            Some(SocketIoPacket::Disconnect) if frame.namespace() == Some(namespace) => {
                Some(TerminalFrameReason::NamespaceDisconnect)
            }
            Some(SocketIoPacket::ConnectError) if frame.namespace() == Some(namespace) => {
                Some(TerminalFrameReason::NamespaceConnectError)
            }
            _ => None,
        },
        _ => None,
    }
}

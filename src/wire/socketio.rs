use crate::wire::lexical::{LexicalError, LexicalLimits, LexicalValue, parse_lexical};
use core::fmt;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum WebSocketOpcode {
    Continuation,
    Text,
    Binary,
    Close,
    Ping,
    Pong,
}

impl WebSocketOpcode {
    pub fn carries_application_payload(self) -> bool {
        matches!(self, Self::Text | Self::Binary)
    }

    pub fn permits_fragmentation(self) -> bool {
        self.carries_application_payload()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum EngineIoPacket {
    Open,
    Close,
    Ping,
    Pong,
    Message,
    Upgrade,
    Noop,
}

impl EngineIoPacket {
    pub fn wire_digit(self) -> u8 {
        match self {
            Self::Open => b'0',
            Self::Close => b'1',
            Self::Ping => b'2',
            Self::Pong => b'3',
            Self::Message => b'4',
            Self::Upgrade => b'5',
            Self::Noop => b'6',
        }
    }

    pub fn from_wire_digit(digit: u8) -> Option<Self> {
        match digit {
            b'0' => Some(Self::Open),
            b'1' => Some(Self::Close),
            b'2' => Some(Self::Ping),
            b'3' => Some(Self::Pong),
            b'4' => Some(Self::Message),
            b'5' => Some(Self::Upgrade),
            b'6' => Some(Self::Noop),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum SocketIoPacket {
    Connect,
    Disconnect,
    Event,
    Ack,
    ConnectError,
    BinaryEvent,
    BinaryAck,
}

impl SocketIoPacket {
    pub fn wire_digit(self) -> u8 {
        match self {
            Self::Connect => b'0',
            Self::Disconnect => b'1',
            Self::Event => b'2',
            Self::Ack => b'3',
            Self::ConnectError => b'4',
            Self::BinaryEvent => b'5',
            Self::BinaryAck => b'6',
        }
    }

    pub fn from_wire_digit(digit: u8) -> Option<Self> {
        match digit {
            b'0' => Some(Self::Connect),
            b'1' => Some(Self::Disconnect),
            b'2' => Some(Self::Event),
            b'3' => Some(Self::Ack),
            b'4' => Some(Self::ConnectError),
            b'5' => Some(Self::BinaryEvent),
            b'6' => Some(Self::BinaryAck),
            _ => None,
        }
    }

    pub fn carries_binary_attachments(self) -> bool {
        matches!(self, Self::BinaryEvent | Self::BinaryAck)
    }

    pub fn carries_event_name(self) -> bool {
        matches!(self, Self::Event | Self::BinaryEvent)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedFrame {
    engine_io: Option<EngineIoPacket>,
    socket_io: Option<SocketIoPacket>,
    namespace: Option<String>,
    acknowledgment_id: Option<u64>,
    binary_attachments: u16,
    event_name: Option<String>,
    payload: Option<LexicalValue>,
    extra_arguments: Vec<LexicalValue>,
}

impl DecodedFrame {
    pub fn engine_io(&self) -> Option<EngineIoPacket> {
        self.engine_io
    }

    pub fn socket_io(&self) -> Option<SocketIoPacket> {
        self.socket_io
    }

    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    pub fn acknowledgment_id(&self) -> Option<u64> {
        self.acknowledgment_id
    }

    pub fn binary_attachments(&self) -> u16 {
        self.binary_attachments
    }

    pub fn event_name(&self) -> Option<&str> {
        self.event_name.as_deref()
    }

    pub fn payload(&self) -> Option<&LexicalValue> {
        self.payload.as_ref()
    }

    pub fn extra_arguments(&self) -> &[LexicalValue] {
        &self.extra_arguments
    }
}

pub fn decode_frame(
    bytes: &[u8],
    opcode: WebSocketOpcode,
    limits: LexicalLimits,
) -> Result<DecodedFrame, FrameError> {
    if !opcode.carries_application_payload() {
        return Ok(DecodedFrame {
            engine_io: None,
            socket_io: None,
            namespace: None,
            acknowledgment_id: None,
            binary_attachments: 0,
            event_name: None,
            payload: None,
            extra_arguments: Vec::new(),
        });
    }
    let (first, rest) = bytes.split_first().ok_or(FrameError::EmptyFrame)?;
    let engine_io =
        EngineIoPacket::from_wire_digit(*first).ok_or(FrameError::UnknownEngineIoPacket)?;
    match engine_io {
        EngineIoPacket::Open => {
            let payload = parse_lexical(rest, limits).map_err(FrameError::Lexical)?;
            Ok(DecodedFrame {
                engine_io: Some(engine_io),
                socket_io: None,
                namespace: None,
                acknowledgment_id: None,
                binary_attachments: 0,
                event_name: None,
                payload: Some(payload),
                extra_arguments: Vec::new(),
            })
        }
        EngineIoPacket::Message => decode_socket_io(engine_io, rest, limits),
        _ => {
            if rest.is_empty() {
                Ok(DecodedFrame {
                    engine_io: Some(engine_io),
                    socket_io: None,
                    namespace: None,
                    acknowledgment_id: None,
                    binary_attachments: 0,
                    event_name: None,
                    payload: None,
                    extra_arguments: Vec::new(),
                })
            } else {
                Err(FrameError::UnexpectedControlPayload)
            }
        }
    }
}

fn decode_socket_io(
    engine_io: EngineIoPacket,
    bytes: &[u8],
    limits: LexicalLimits,
) -> Result<DecodedFrame, FrameError> {
    let (first, mut rest) = bytes.split_first().ok_or(FrameError::EmptyFrame)?;
    let socket_io =
        SocketIoPacket::from_wire_digit(*first).ok_or(FrameError::UnknownSocketIoPacket)?;
    let mut binary_attachments = 0u16;
    if socket_io.carries_binary_attachments() {
        let separator = rest
            .iter()
            .position(|byte| *byte == b'-')
            .ok_or(FrameError::MissingAttachmentCount)?;
        let digits =
            core::str::from_utf8(&rest[..separator]).map_err(|_| FrameError::MalformedFraming)?;
        binary_attachments = digits
            .parse::<u16>()
            .map_err(|_| FrameError::MalformedFraming)?;
        rest = &rest[separator + 1..];
    }
    let mut namespace = None;
    if rest.first() == Some(&b'/') {
        let separator = rest
            .iter()
            .position(|byte| *byte == b',')
            .ok_or(FrameError::MalformedFraming)?;
        namespace = Some(
            core::str::from_utf8(&rest[..separator])
                .map_err(|_| FrameError::MalformedFraming)?
                .to_owned(),
        );
        rest = &rest[separator + 1..];
    }
    let acknowledgment_digits = rest
        .iter()
        .position(|byte| !byte.is_ascii_digit())
        .unwrap_or(rest.len());
    let mut acknowledgment_id = None;
    if acknowledgment_digits > 0 {
        let digits = core::str::from_utf8(&rest[..acknowledgment_digits])
            .map_err(|_| FrameError::MalformedFraming)?;
        acknowledgment_id = Some(
            digits
                .parse::<u64>()
                .map_err(|_| FrameError::MalformedFraming)?,
        );
        rest = &rest[acknowledgment_digits..];
    }
    let payload = if rest.is_empty() {
        None
    } else {
        Some(parse_lexical(rest, limits).map_err(FrameError::Lexical)?)
    };
    let mut event_name = None;
    let mut argument = payload.clone();
    let mut extra_arguments = Vec::new();
    if socket_io.carries_event_name() {
        let values = payload
            .as_ref()
            .and_then(LexicalValue::as_array)
            .ok_or(FrameError::MalformedEventEnvelope)?;
        let name = values
            .first()
            .and_then(LexicalValue::as_text)
            .ok_or(FrameError::MalformedEventEnvelope)?;
        event_name = Some(name.to_owned());
        argument = values.get(1).cloned();
        extra_arguments = values.iter().skip(2).cloned().collect();
    }
    Ok(DecodedFrame {
        engine_io: Some(engine_io),
        socket_io: Some(socket_io),
        namespace,
        acknowledgment_id,
        binary_attachments,
        event_name,
        payload: argument,
        extra_arguments,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FrameError {
    EmptyFrame,
    UnknownEngineIoPacket,
    UnknownSocketIoPacket,
    MalformedFraming,
    MalformedEventEnvelope,
    MissingAttachmentCount,
    UnexpectedControlPayload,
    Lexical(LexicalError),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("frame decoding failure")
    }
}

impl std::error::Error for FrameError {}

use core::fmt;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LexicalLimits {
    pub max_bytes: usize,
    pub max_depth: u16,
    pub max_array_elements: usize,
    pub max_object_entries: usize,
    pub max_string_bytes: usize,
    pub max_number_bytes: usize,
}

impl LexicalLimits {
    pub const fn venue_payload() -> Self {
        Self {
            max_bytes: 262_144,
            max_depth: 16,
            max_array_elements: 4_096,
            max_object_entries: 256,
            max_string_bytes: 4_096,
            max_number_bytes: 128,
        }
    }

    pub fn with_max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = max_bytes;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct NumberLexeme(String);

impl NumberLexeme {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the lexeme and returns its original text buffer without copying it.
    pub fn into_string(self) -> String {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum LexicalValue {
    Null,
    Bool(bool),
    Number(NumberLexeme),
    Text(String),
    Array(Vec<LexicalValue>),
    Object(Vec<(String, LexicalValue)>),
}

impl LexicalValue {
    pub fn field(&self, name: &str) -> Option<&LexicalValue> {
        match self {
            Self::Object(entries) => entries
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value),
            _ => None,
        }
    }

    pub fn path(&self, dotted: &str) -> Option<&LexicalValue> {
        let mut current = self;
        for segment in dotted.split('.') {
            if segment.is_empty() {
                return None;
            }
            current = current.field(segment)?;
        }
        Some(current)
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_number(&self) -> Option<&NumberLexeme> {
        match self {
            Self::Number(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[LexicalValue]> {
        match self {
            Self::Array(values) => Some(values),
            _ => None,
        }
    }

    pub fn kind(&self) -> LexicalKind {
        match self {
            Self::Null => LexicalKind::Null,
            Self::Bool(_) => LexicalKind::Bool,
            Self::Number(_) => LexicalKind::Number,
            Self::Text(_) => LexicalKind::Text,
            Self::Array(_) => LexicalKind::Array,
            Self::Object(_) => LexicalKind::Object,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LexicalKind {
    Null,
    Bool,
    Number,
    Text,
    Array,
    Object,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LexicalError {
    InputTooLarge { bytes: usize, limit: usize },
    EmptyInput,
    UnexpectedEnd { offset: usize },
    UnexpectedByte { offset: usize },
    TrailingBytes { offset: usize },
    DepthExceeded { offset: usize, limit: u16 },
    ArrayCapacityExceeded { offset: usize, limit: usize },
    ObjectCapacityExceeded { offset: usize, limit: usize },
    DuplicateField { offset: usize },
    StringTooLong { offset: usize, limit: usize },
    NumberTooLong { offset: usize, limit: usize },
    InvalidNumber { offset: usize },
    InvalidString { offset: usize },
    InvalidEscape { offset: usize },
    NotUtf8 { offset: usize },
}

impl fmt::Display for LexicalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid lexical JSON")
    }
}

impl std::error::Error for LexicalError {}

pub fn parse_lexical(bytes: &[u8], limits: LexicalLimits) -> Result<LexicalValue, LexicalError> {
    parse_json(bytes, limits, LexicalSink::default())
}

/// Crate-private event receiver used by the single strict JSON scanner.
pub(crate) trait JsonSink {
    type Output;

    fn null(&mut self);
    fn boolean(&mut self, value: bool);
    fn number(&mut self, lexeme: &str, start: usize, end: usize);
    fn text(&mut self, value: JsonText<'_>);
    fn begin_array(&mut self);
    fn end_array(&mut self);
    fn begin_object(&mut self);
    fn key(&mut self, value: JsonText<'_>, offset: usize) -> Result<(), LexicalError>;
    fn end_object(&mut self);
    fn finish(self) -> Self::Output;
}

pub(crate) enum JsonText<'a> {
    Source {
        value: &'a str,
        start: usize,
        end: usize,
    },
    Escaped {
        value: String,
        start: usize,
        end: usize,
    },
}

impl JsonText<'_> {
    pub(crate) fn value(&self) -> &str {
        match self {
            Self::Source { value, .. } => value,
            Self::Escaped { value, .. } => value,
        }
    }
    pub(crate) fn span(&self) -> (usize, usize) {
        match self {
            Self::Source { start, end, .. } | Self::Escaped { start, end, .. } => (*start, *end),
        }
    }
}

pub(crate) fn parse_json<S: JsonSink>(
    bytes: &[u8],
    limits: LexicalLimits,
    mut sink: S,
) -> Result<S::Output, LexicalError> {
    if bytes.is_empty() {
        return Err(LexicalError::EmptyInput);
    }
    if bytes.len() > limits.max_bytes {
        return Err(LexicalError::InputTooLarge {
            bytes: bytes.len(),
            limit: limits.max_bytes,
        });
    }
    let mut scanner = Scanner {
        bytes,
        offset: 0,
        limits,
    };
    scanner.skip_whitespace();
    scanner.value(0, &mut sink)?;
    scanner.skip_whitespace();
    if scanner.offset != bytes.len() {
        return Err(LexicalError::TrailingBytes {
            offset: scanner.offset,
        });
    }
    Ok(sink.finish())
}

struct Scanner<'a> {
    bytes: &'a [u8],
    offset: usize,
    limits: LexicalLimits,
}

impl<'a> Scanner<'a> {
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.offset).copied()
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.offset += 1;
        }
    }

    fn expect(&mut self, byte: u8) -> Result<(), LexicalError> {
        match self.peek() {
            Some(found) if found == byte => {
                self.offset += 1;
                Ok(())
            }
            Some(_) => Err(LexicalError::UnexpectedByte {
                offset: self.offset,
            }),
            None => Err(LexicalError::UnexpectedEnd {
                offset: self.offset,
            }),
        }
    }

    fn literal(&mut self, word: &[u8]) -> Result<(), LexicalError> {
        if self.bytes[self.offset..].starts_with(word) {
            self.offset += word.len();
            Ok(())
        } else {
            Err(LexicalError::UnexpectedByte {
                offset: self.offset,
            })
        }
    }

    fn value<S: JsonSink>(&mut self, depth: u16, sink: &mut S) -> Result<(), LexicalError> {
        if depth > self.limits.max_depth {
            return Err(LexicalError::DepthExceeded {
                offset: self.offset,
                limit: self.limits.max_depth,
            });
        }
        match self.peek() {
            None => Err(LexicalError::UnexpectedEnd {
                offset: self.offset,
            }),
            Some(b'n') => {
                self.literal(b"null")?;
                sink.null();
                Ok(())
            }
            Some(b't') => {
                self.literal(b"true")?;
                sink.boolean(true);
                Ok(())
            }
            Some(b'f') => {
                self.literal(b"false")?;
                sink.boolean(false);
                Ok(())
            }
            Some(b'"') => {
                sink.text(self.text()?);
                Ok(())
            }
            Some(b'[') => self.array(depth, sink),
            Some(b'{') => self.object(depth, sink),
            Some(byte) if byte == b'-' || byte.is_ascii_digit() => {
                let start = self.offset;
                let number = self.number()?;
                sink.number(number, start, self.offset);
                Ok(())
            }
            Some(_) => Err(LexicalError::UnexpectedByte {
                offset: self.offset,
            }),
        }
    }

    fn array<S: JsonSink>(&mut self, depth: u16, sink: &mut S) -> Result<(), LexicalError> {
        let start = self.offset;
        self.expect(b'[')?;
        let mut values = 0usize;
        sink.begin_array();
        self.skip_whitespace();
        if self.peek() == Some(b']') {
            self.offset += 1;
            sink.end_array();
            return Ok(());
        }
        loop {
            self.skip_whitespace();
            if values == self.limits.max_array_elements {
                return Err(LexicalError::ArrayCapacityExceeded {
                    offset: start,
                    limit: self.limits.max_array_elements,
                });
            }
            self.value(depth + 1, sink)?;
            values += 1;
            self.skip_whitespace();
            match self.peek() {
                Some(b',') => self.offset += 1,
                Some(b']') => {
                    self.offset += 1;
                    sink.end_array();
                    return Ok(());
                }
                Some(_) => {
                    return Err(LexicalError::UnexpectedByte {
                        offset: self.offset,
                    });
                }
                None => {
                    return Err(LexicalError::UnexpectedEnd {
                        offset: self.offset,
                    });
                }
            }
        }
    }

    fn object<S: JsonSink>(&mut self, depth: u16, sink: &mut S) -> Result<(), LexicalError> {
        let start = self.offset;
        self.expect(b'{')?;
        let mut entries = 0usize;
        sink.begin_object();
        self.skip_whitespace();
        if self.peek() == Some(b'}') {
            self.offset += 1;
            sink.end_object();
            return Ok(());
        }
        loop {
            self.skip_whitespace();
            if entries == self.limits.max_object_entries {
                return Err(LexicalError::ObjectCapacityExceeded {
                    offset: start,
                    limit: self.limits.max_object_entries,
                });
            }
            let key_offset = self.offset;
            let key = self.text()?;
            sink.key(key, key_offset)?;
            self.skip_whitespace();
            self.expect(b':')?;
            self.skip_whitespace();
            self.value(depth + 1, sink)?;
            entries += 1;
            self.skip_whitespace();
            match self.peek() {
                Some(b',') => self.offset += 1,
                Some(b'}') => {
                    self.offset += 1;
                    sink.end_object();
                    return Ok(());
                }
                Some(_) => {
                    return Err(LexicalError::UnexpectedByte {
                        offset: self.offset,
                    });
                }
                None => {
                    return Err(LexicalError::UnexpectedEnd {
                        offset: self.offset,
                    });
                }
            }
        }
    }

    fn text(&mut self) -> Result<JsonText<'a>, LexicalError> {
        let start = self.offset;
        self.expect(b'"')?;
        let content_start = self.offset;
        let raw = &self.bytes[self.offset..];
        let plain = raw
            .iter()
            .position(|byte| matches!(byte, b'"' | b'\\' | 0x00..=0x1f));
        if let Some(length) = plain
            && raw[length] == b'"'
        {
            if length > self.limits.max_string_bytes {
                return Err(LexicalError::StringTooLong {
                    offset: start,
                    limit: self.limits.max_string_bytes,
                });
            }
            let value =
                core::str::from_utf8(&raw[..length]).map_err(|error| LexicalError::NotUtf8 {
                    offset: content_start + error.valid_up_to(),
                })?;
            self.offset += length + 1;
            return Ok(JsonText::Source {
                value,
                start: content_start,
                end: content_start + length,
            });
        }
        let mut output = String::new();
        loop {
            if output.len() > self.limits.max_string_bytes {
                return Err(LexicalError::StringTooLong {
                    offset: start,
                    limit: self.limits.max_string_bytes,
                });
            }
            let rest = &self.bytes[self.offset..];
            let length = rest
                .iter()
                .position(|byte| matches!(byte, b'"' | b'\\' | 0x00..=0x1f))
                .unwrap_or(rest.len());
            let chunk =
                core::str::from_utf8(&rest[..length]).map_err(|error| LexicalError::NotUtf8 {
                    offset: self.offset + error.valid_up_to(),
                })?;
            if chunk.len() > self.limits.max_string_bytes - output.len() {
                return Err(LexicalError::StringTooLong {
                    offset: start,
                    limit: self.limits.max_string_bytes,
                });
            }
            output.push_str(chunk);
            self.offset += length;
            match self.peek() {
                Some(b'"') => {
                    self.offset += 1;
                    return Ok(JsonText::Escaped {
                        value: output,
                        start: content_start,
                        end: self.offset - 1,
                    });
                }
                Some(b'\\') => {
                    self.offset += 1;
                    output.push(self.escape()?);
                }
                Some(_) => {
                    return Err(LexicalError::InvalidString {
                        offset: self.offset,
                    });
                }
                None => {
                    return Err(LexicalError::UnexpectedEnd {
                        offset: self.offset,
                    });
                }
            }
        }
    }

    fn escape(&mut self) -> Result<char, LexicalError> {
        let offset = self.offset;
        let byte = self.peek().ok_or(LexicalError::UnexpectedEnd { offset })?;
        self.offset += 1;
        let value = match byte {
            b'"' => '"',
            b'\\' => '\\',
            b'/' => '/',
            b'b' => '\u{0008}',
            b'f' => '\u{000c}',
            b'n' => '\n',
            b'r' => '\r',
            b't' => '\t',
            b'u' => {
                let first = self.hex4()?;
                if (0xdc00..=0xdfff).contains(&first) {
                    return Err(LexicalError::InvalidEscape { offset });
                }
                if (0xd800..=0xdbff).contains(&first) {
                    if self.peek() != Some(b'\\') {
                        return Err(LexicalError::InvalidEscape { offset });
                    }
                    self.offset += 1;
                    if self.peek() != Some(b'u') {
                        return Err(LexicalError::InvalidEscape { offset });
                    }
                    self.offset += 1;
                    let second = self.hex4()?;
                    if !(0xdc00..=0xdfff).contains(&second) {
                        return Err(LexicalError::InvalidEscape { offset });
                    }
                    let combined = 0x1_0000
                        + ((u32::from(first) - 0xd800) << 10)
                        + (u32::from(second) - 0xdc00);
                    return char::from_u32(combined).ok_or(LexicalError::InvalidEscape { offset });
                }
                return char::from_u32(u32::from(first))
                    .ok_or(LexicalError::InvalidEscape { offset });
            }
            _ => return Err(LexicalError::InvalidEscape { offset }),
        };
        Ok(value)
    }

    fn hex4(&mut self) -> Result<u16, LexicalError> {
        let offset = self.offset;
        if self.bytes.len() < self.offset + 4 {
            return Err(LexicalError::UnexpectedEnd { offset });
        }
        let mut value: u16 = 0;
        for index in 0..4 {
            let byte = self.bytes[self.offset + index];
            let digit = match byte {
                b'0'..=b'9' => byte - b'0',
                b'a'..=b'f' => byte - b'a' + 10,
                b'A'..=b'F' => byte - b'A' + 10,
                _ => return Err(LexicalError::InvalidEscape { offset }),
            };
            value = value * 16 + u16::from(digit);
        }
        self.offset += 4;
        Ok(value)
    }

    fn number(&mut self) -> Result<&'a str, LexicalError> {
        let start = self.offset;
        if self.peek() == Some(b'-') {
            self.offset += 1;
        }
        match self.peek() {
            Some(b'0') => self.offset += 1,
            Some(byte) if byte.is_ascii_digit() => {
                while matches!(self.peek(), Some(byte) if byte.is_ascii_digit()) {
                    self.offset += 1;
                }
            }
            _ => return Err(LexicalError::InvalidNumber { offset: start }),
        }
        if self.peek() == Some(b'.') {
            self.offset += 1;
            if !matches!(self.peek(), Some(byte) if byte.is_ascii_digit()) {
                return Err(LexicalError::InvalidNumber { offset: start });
            }
            while matches!(self.peek(), Some(byte) if byte.is_ascii_digit()) {
                self.offset += 1;
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.offset += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.offset += 1;
            }
            if !matches!(self.peek(), Some(byte) if byte.is_ascii_digit()) {
                return Err(LexicalError::InvalidNumber { offset: start });
            }
            while matches!(self.peek(), Some(byte) if byte.is_ascii_digit()) {
                self.offset += 1;
            }
        }
        let lexeme = &self.bytes[start..self.offset];
        if lexeme.len() > self.limits.max_number_bytes {
            return Err(LexicalError::NumberTooLong {
                offset: start,
                limit: self.limits.max_number_bytes,
            });
        }
        core::str::from_utf8(lexeme).map_err(|_| LexicalError::InvalidNumber { offset: start })
    }
}

#[derive(Default)]
struct LexicalSink {
    root: Option<LexicalValue>,
    stack: Vec<LexicalFrame>,
}

enum LexicalFrame {
    Array(Vec<LexicalValue>),
    Object {
        entries: Vec<(String, LexicalValue)>,
        key: Option<String>,
    },
}

impl LexicalSink {
    fn push(&mut self, value: LexicalValue) {
        match self.stack.last_mut() {
            Some(LexicalFrame::Array(values)) => values.push(value),
            Some(LexicalFrame::Object { entries, key }) => {
                entries.push((key.take().expect("JSON object key precedes value"), value));
            }
            None => self.root = Some(value),
        }
    }
}

impl JsonSink for LexicalSink {
    type Output = LexicalValue;

    fn null(&mut self) {
        self.push(LexicalValue::Null);
    }
    fn boolean(&mut self, value: bool) {
        self.push(LexicalValue::Bool(value));
    }
    fn number(&mut self, lexeme: &str, _start: usize, _end: usize) {
        self.push(LexicalValue::Number(NumberLexeme(lexeme.to_owned())));
    }
    fn text(&mut self, value: JsonText<'_>) {
        self.push(LexicalValue::Text(value.value().to_owned()));
    }
    fn begin_array(&mut self) {
        self.stack.push(LexicalFrame::Array(Vec::new()));
    }
    fn end_array(&mut self) {
        let Some(LexicalFrame::Array(values)) = self.stack.pop() else {
            unreachable!("scanner balances arrays")
        };
        self.push(LexicalValue::Array(values));
    }
    fn begin_object(&mut self) {
        self.stack.push(LexicalFrame::Object {
            entries: Vec::new(),
            key: None,
        });
    }
    fn key(&mut self, value: JsonText<'_>, offset: usize) -> Result<(), LexicalError> {
        let value = value.value().to_owned();
        let Some(LexicalFrame::Object { entries, key }) = self.stack.last_mut() else {
            unreachable!("key belongs to object")
        };
        if entries.iter().any(|(existing, _)| existing == &value) {
            return Err(LexicalError::DuplicateField { offset });
        }
        *key = Some(value);
        Ok(())
    }
    fn end_object(&mut self) {
        let Some(LexicalFrame::Object { entries, .. }) = self.stack.pop() else {
            unreachable!("scanner balances objects")
        };
        self.push(LexicalValue::Object(entries));
    }
    fn finish(self) -> Self::Output {
        self.root.expect("scanner supplies root")
    }
}

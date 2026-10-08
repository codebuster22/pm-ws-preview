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
    let value = scanner.value(0)?;
    scanner.skip_whitespace();
    if scanner.offset != bytes.len() {
        return Err(LexicalError::TrailingBytes {
            offset: scanner.offset,
        });
    }
    Ok(value)
}

struct Scanner<'a> {
    bytes: &'a [u8],
    offset: usize,
    limits: LexicalLimits,
}

impl Scanner<'_> {
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

    fn value(&mut self, depth: u16) -> Result<LexicalValue, LexicalError> {
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
                Ok(LexicalValue::Null)
            }
            Some(b't') => {
                self.literal(b"true")?;
                Ok(LexicalValue::Bool(true))
            }
            Some(b'f') => {
                self.literal(b"false")?;
                Ok(LexicalValue::Bool(false))
            }
            Some(b'"') => self.text().map(LexicalValue::Text),
            Some(b'[') => self.array(depth),
            Some(b'{') => self.object(depth),
            Some(byte) if byte == b'-' || byte.is_ascii_digit() => {
                self.number().map(LexicalValue::Number)
            }
            Some(_) => Err(LexicalError::UnexpectedByte {
                offset: self.offset,
            }),
        }
    }

    fn array(&mut self, depth: u16) -> Result<LexicalValue, LexicalError> {
        let start = self.offset;
        self.expect(b'[')?;
        let mut values = Vec::new();
        self.skip_whitespace();
        if self.peek() == Some(b']') {
            self.offset += 1;
            return Ok(LexicalValue::Array(values));
        }
        loop {
            self.skip_whitespace();
            if values.len() == self.limits.max_array_elements {
                return Err(LexicalError::ArrayCapacityExceeded {
                    offset: start,
                    limit: self.limits.max_array_elements,
                });
            }
            values.push(self.value(depth + 1)?);
            self.skip_whitespace();
            match self.peek() {
                Some(b',') => self.offset += 1,
                Some(b']') => {
                    self.offset += 1;
                    return Ok(LexicalValue::Array(values));
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

    fn object(&mut self, depth: u16) -> Result<LexicalValue, LexicalError> {
        let start = self.offset;
        self.expect(b'{')?;
        let mut entries: Vec<(String, LexicalValue)> = Vec::new();
        self.skip_whitespace();
        if self.peek() == Some(b'}') {
            self.offset += 1;
            return Ok(LexicalValue::Object(entries));
        }
        loop {
            self.skip_whitespace();
            if entries.len() == self.limits.max_object_entries {
                return Err(LexicalError::ObjectCapacityExceeded {
                    offset: start,
                    limit: self.limits.max_object_entries,
                });
            }
            let key_offset = self.offset;
            let key = self.text()?;
            if entries.iter().any(|(existing, _)| existing == &key) {
                return Err(LexicalError::DuplicateField { offset: key_offset });
            }
            self.skip_whitespace();
            self.expect(b':')?;
            self.skip_whitespace();
            let value = self.value(depth + 1)?;
            entries.push((key, value));
            self.skip_whitespace();
            match self.peek() {
                Some(b',') => self.offset += 1,
                Some(b'}') => {
                    self.offset += 1;
                    return Ok(LexicalValue::Object(entries));
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

    fn text(&mut self) -> Result<String, LexicalError> {
        let start = self.offset;
        self.expect(b'"')?;
        let mut output = String::new();
        loop {
            if output.len() > self.limits.max_string_bytes {
                return Err(LexicalError::StringTooLong {
                    offset: start,
                    limit: self.limits.max_string_bytes,
                });
            }
            let byte = self.peek().ok_or(LexicalError::UnexpectedEnd {
                offset: self.offset,
            })?;
            match byte {
                b'"' => {
                    self.offset += 1;
                    return Ok(output);
                }
                b'\\' => {
                    self.offset += 1;
                    output.push(self.escape()?);
                }
                0x00..=0x1f => {
                    return Err(LexicalError::InvalidString {
                        offset: self.offset,
                    });
                }
                _ => {
                    let rest = &self.bytes[self.offset..];
                    let width = utf8_width(byte);
                    if width == 0 || rest.len() < width {
                        return Err(LexicalError::NotUtf8 {
                            offset: self.offset,
                        });
                    }
                    let chunk = core::str::from_utf8(&rest[..width]).map_err(|_| {
                        LexicalError::NotUtf8 {
                            offset: self.offset,
                        }
                    })?;
                    output.push_str(chunk);
                    self.offset += width;
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

    fn number(&mut self) -> Result<NumberLexeme, LexicalError> {
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
        core::str::from_utf8(lexeme)
            .map(|value| NumberLexeme(value.to_owned()))
            .map_err(|_| LexicalError::InvalidNumber { offset: start })
    }
}

fn utf8_width(byte: u8) -> usize {
    match byte {
        0x00..=0x7f => 1,
        0xc2..=0xdf => 2,
        0xe0..=0xef => 3,
        0xf0..=0xf4 => 4,
        _ => 0,
    }
}

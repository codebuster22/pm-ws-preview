//! Transport-independent, complete venue-native application events.

use std::sync::Arc;

#[cfg(test)]
use crate::{
    numeric::{DecimalError, DecimalGrammar, ExactDecimal},
    wire::lexical::LexicalValue,
};

pub mod document;
pub mod gate;

use document::{NativeDocument, NodeId, ValueRef};

/// An immutable event view retaining its complete decoded source document.
#[derive(Clone, Debug)]
pub struct NativePayload {
    document: Arc<NativeDocument>,
    root: NodeId,
}

impl NativePayload {
    /// Retains an entire decoded document as one payload without reconstructing its values.
    pub fn from_document(document: NativeDocument) -> Self {
        let root = document.root();
        Self::new(Arc::new(document), root)
    }

    pub(crate) fn new(document: Arc<NativeDocument>, root: NodeId) -> Self {
        Self { document, root }
    }

    /// Returns the typed event root; its data stays valid while this payload is retained.
    pub fn view(&self) -> ValueRef<'_> {
        self.document.view(self.root)
    }

    /// Looks up a source object field without reparsing or allocating.
    pub fn field(&self, name: &str) -> Option<ValueRef<'_>> {
        self.view().field(name)
    }

    /// Reports whether two payloads retain the same decoded message allocation.
    pub fn shares_document(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.document, &other.document)
    }

    pub(crate) fn bounded_measure(
        &self,
        max_nodes: usize,
        max_depth: u16,
    ) -> Option<(usize, usize)> {
        let (nodes, bytes) = self
            .document
            .bounded_measure(self.root, max_nodes, max_depth)?;
        Some((nodes, bytes.checked_add(2 * std::mem::size_of::<usize>())?))
    }
}

/// Venue which supplied an event.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum NativeVenue {
    Limitless,
    Polymarket,
}

/// Bounded family vocabulary. `Unknown` retains a validated envelope without creating labels.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum NativeFamily {
    LimitlessOrderbookUpdate,
    LimitlessNewPriceData,
    LimitlessMarketCreated,
    LimitlessMarketResolved,
    LimitlessSystem,
    LimitlessException,
    PolymarketBook,
    PolymarketPriceChange,
    PolymarketLastTradePrice,
    PolymarketTickSizeChange,
    PolymarketBestBidAsk,
    PolymarketNewMarket,
    PolymarketMarketResolved,
    PolymarketPongControl,
    Unknown,
}

/// Test oracle preserving source-order lexical tree conversion.
#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum NativeValue {
    Null,
    Bool(bool),
    String(String),
    Number { lexeme: String, value: ExactDecimal },
    Array(Vec<NativeValue>),
    Object(Vec<(String, NativeValue)>),
}

#[cfg(test)]
impl NativeValue {
    /// Consumes a bounded lexical JSON tree, moving text buffers and parsing every number exactly.
    /// Returns an error instead of a partial value if any number is unrepresentable.
    pub fn from_lexical(
        value: LexicalValue,
        grammar: DecimalGrammar,
    ) -> Result<Self, DecimalError> {
        Ok(match value {
            LexicalValue::Null => Self::Null,
            LexicalValue::Bool(value) => Self::Bool(value),
            LexicalValue::Text(value) => Self::String(value),
            LexicalValue::Number(number) => {
                let value = ExactDecimal::parse(number.as_str(), grammar)?;
                Self::Number {
                    lexeme: number.into_string(),
                    value,
                }
            }
            LexicalValue::Array(values) => {
                let mut native = Vec::with_capacity(values.len());
                for value in values {
                    native.push(Self::from_lexical(value, grammar)?);
                }
                Self::Array(native)
            }
            LexicalValue::Object(values) => {
                let mut native = Vec::with_capacity(values.len());
                for (key, value) in values {
                    native.push((key, Self::from_lexical(value, grammar)?));
                }
                Self::Object(native)
            }
        })
    }
}

/// Equality material supplied by a venue adapter after full validation.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum NativeIdentity {
    Components(Vec<Arc<str>>),
    PolymarketDelta {
        timestamp: Arc<str>,
        entries: Vec<(Arc<str>, Arc<str>)>,
    },
}

/// Routing and local provenance for an application batch.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct NativeSource {
    pub stream: Arc<str>,
    pub source: Arc<str>,
    pub slot: u16,
    pub generation: u64,
    pub stream_generation: u64,
    pub sequence: u64,
    pub received_ns: u64,
    pub validated_ns: u64,
}

/// A validated complete native event. `member_index` is its original index in an application array.
#[derive(Clone, Debug)]
pub struct NativeEvent {
    pub venue: NativeVenue,
    pub family: NativeFamily,
    pub family_name: Option<Arc<str>>,
    pub market: Option<Arc<str>>,
    pub assets: Vec<Arc<str>>,
    pub identity: Option<NativeIdentity>,
    pub payload: NativePayload,
    pub member_index: usize,
}

/// One decoded application message; adapters construct it only after validating every member.
#[derive(Clone, Debug)]
pub struct NativeBatch {
    pub source: NativeSource,
    pub input_bytes: usize,
    pub events: Vec<NativeEvent>,
}

//! Flat, bounded ownership of one strict JSON source document.

use core::{mem::size_of, num::NonZeroU32};

use crate::{
    numeric::{DecimalError, DecimalGrammar, ExactDecimal},
    wire::lexical::{JsonSink, LexicalError, LexicalLimits, parse_json},
};

/// Failure while constructing a native document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DocumentError {
    /// The source was not within the strict bounded JSON grammar.
    Lexical(LexicalError),
    /// A JSON number could not be represented under the supplied decimal grammar.
    Decimal(DecimalError),
}

impl core::fmt::Display for DocumentError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Lexical(error) => error.fmt(f),
            Self::Decimal(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for DocumentError {}

/// Source JSON type retained by a [`NativeDocument`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeKind {
    Null,
    Bool,
    Number,
    String,
    DecimalString,
    Array,
    Object,
}

/// Process-local location of a value in one native document.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct NodeId(NonZeroU32);

impl NodeId {
    fn new(index: usize) -> Self {
        let value = u32::try_from(index)
            .expect("input bounds node count")
            .checked_add(1)
            .expect("input bounds node count");
        Self(NonZeroU32::new(value).expect("nonzero node id"))
    }

    fn index(self) -> usize {
        (self.0.get() - 1) as usize
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Span {
    start: usize,
    end: usize,
}

impl Span {
    const EMPTY: Self = Self { start: 0, end: 0 };
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Node {
    kind: NativeKind,
    boolean: bool,
    text: Span,
    key_text: Span,
    decimal: Option<ExactDecimal>,
    first_child: Option<NonZeroU32>,
    next_sibling: Option<NonZeroU32>,
    child_count: u32,
    subtree_end: u32,
    depth: u16,
}

impl Node {
    fn scalar(kind: NativeKind) -> Self {
        Self {
            kind,
            boolean: false,
            text: Span::EMPTY,
            key_text: Span::EMPTY,
            decimal: None,
            first_child: None,
            next_sibling: None,
            child_count: 0,
            subtree_end: 0,
            depth: 0,
        }
    }
}

/// Source-preserving JSON document with preorder nodes and shared text storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeDocument {
    bytes: Vec<u8>,
    nodes: Vec<Node>,
    root: NodeId,
}

impl NativeDocument {
    /// Parses one bounded strict JSON document and exactly parses every JSON number.
    ///
    /// Input is capped at the smaller of `max_bytes` and `u32::MAX`, which keeps every
    /// private node index and count representable while exceeding venue payload limits.
    pub fn parse(
        bytes: &[u8],
        limits: LexicalLimits,
        grammar: DecimalGrammar,
    ) -> Result<Self, DocumentError> {
        if bytes.is_empty() {
            return Err(DocumentError::Lexical(LexicalError::EmptyInput));
        }
        let input_limit = limits.max_bytes.min(u32::MAX as usize);
        if bytes.len() > input_limit {
            return Err(DocumentError::Lexical(LexicalError::InputTooLarge {
                bytes: bytes.len(),
                limit: input_limit,
            }));
        }
        parse_json(bytes, limits, DocumentSink::new(bytes, grammar))
            .map_err(DocumentError::Lexical)?
    }

    /// Returns a borrowed view of the root value.
    pub fn root_view(&self) -> ValueRef<'_> {
        self.view(self.root)
    }

    /// Returns bytes charged by the actual source and node-vector allocations.
    pub fn memory_bytes(&self) -> usize {
        size_of::<Self>()
            + self.bytes.capacity()
            + self.nodes.capacity().saturating_mul(size_of::<Node>())
    }

    pub(crate) fn root(&self) -> NodeId {
        self.root
    }
    pub(crate) fn view(&self, id: NodeId) -> ValueRef<'_> {
        ValueRef { document: self, id }
    }

    pub(crate) fn promote_decimal_string(
        &mut self,
        id: NodeId,
        grammar: DecimalGrammar,
    ) -> Result<(), DecimalError> {
        let node = &mut self.nodes[id.index()];
        if node.kind == NativeKind::String {
            let text = node.text;
            let lexeme = core::str::from_utf8(&self.bytes[text.start..text.end])
                .expect("scanner produced UTF-8");
            node.decimal = Some(ExactDecimal::parse(lexeme, grammar)?);
            node.kind = NativeKind::DecimalString;
        }
        Ok(())
    }

    pub(crate) fn bounded_measure(
        &self,
        root: NodeId,
        max_nodes: usize,
        max_depth: u16,
    ) -> Option<(usize, usize)> {
        let root_index = root.index();
        let node = &self.nodes[root_index];
        let subtree_end = node.subtree_end as usize;
        let count = subtree_end.checked_sub(root_index)?;
        if count > max_nodes {
            return None;
        }
        for descendant in &self.nodes[root_index..subtree_end] {
            if descendant.depth.checked_sub(node.depth)? > max_depth {
                return None;
            }
        }
        Some((count, self.memory_bytes()))
    }

    fn text(&self, span: Span) -> &str {
        core::str::from_utf8(&self.bytes[span.start..span.end]).expect("scanner produced UTF-8")
    }
}

/// Borrowed zero-allocation view of a native JSON value.
#[derive(Clone, Copy)]
pub struct ValueRef<'a> {
    document: &'a NativeDocument,
    id: NodeId,
}

impl<'a> ValueRef<'a> {
    /// Returns this value's process-local node identifier.
    pub fn id(self) -> NodeId {
        self.id
    }
    /// Returns the original JSON type, including promoted decimal strings.
    pub fn kind(self) -> NativeKind {
        self.document.nodes[self.id.index()].kind
    }
    /// Returns a JSON boolean value.
    pub fn as_bool(self) -> Option<bool> {
        (self.kind() == NativeKind::Bool).then_some(self.document.nodes[self.id.index()].boolean)
    }
    /// Returns decoded source text for a JSON string or promoted decimal string.
    pub fn as_text(self) -> Option<&'a str> {
        if matches!(self.kind(), NativeKind::String | NativeKind::DecimalString) {
            Some(
                self.document
                    .text(self.document.nodes[self.id.index()].text),
            )
        } else {
            None
        }
    }
    /// Returns the exact decimal for JSON numbers and promoted decimal strings.
    pub fn exact_decimal(self) -> Option<&'a ExactDecimal> {
        self.document.nodes[self.id.index()].decimal.as_ref()
    }
    /// Returns the unmodified JSON number lexeme.
    pub fn number_lexeme(self) -> Option<&'a str> {
        if self.kind() == NativeKind::Number {
            Some(
                self.document
                    .text(self.document.nodes[self.id.index()].text),
            )
        } else {
            None
        }
    }
    /// Finds an object field by its decoded JSON key.
    pub fn field(self, name: &str) -> Option<Self> {
        self.entries()
            .find_map(|(key, value)| (key == name).then_some(value))
    }
    /// Iterates children in original array/object source order.
    pub fn children(self) -> Children<'a> {
        Children::new(
            self.document,
            self.document.nodes[self.id.index()].first_child.map(NodeId),
            self.document.nodes[self.id.index()].child_count as usize,
        )
    }
    /// Iterates object key/value pairs in original source order.
    pub fn entries(self) -> Entries<'a> {
        let children = if self.kind() == NativeKind::Object {
            self.children()
        } else {
            Children::new(self.document, None, 0)
        };
        Entries { children }
    }
    /// Returns the next value in its parent's source-order child chain.
    pub fn next_sibling(self) -> Option<Self> {
        self.document.nodes[self.id.index()]
            .next_sibling
            .map(NodeId)
            .map(|id| self.document.view(id))
    }
}

/// Exact-size iterator over direct child values.
pub struct Children<'a> {
    document: &'a NativeDocument,
    next: Option<NodeId>,
    remaining: usize,
}
impl<'a> Children<'a> {
    fn new(document: &'a NativeDocument, next: Option<NodeId>, remaining: usize) -> Self {
        Self {
            document,
            next,
            remaining,
        }
    }
}
impl<'a> Iterator for Children<'a> {
    type Item = ValueRef<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        let id = self.next?;
        self.next = self.document.nodes[id.index()].next_sibling.map(NodeId);
        self.remaining -= 1;
        Some(self.document.view(id))
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}
impl ExactSizeIterator for Children<'_> {}

/// Iterator over decoded object keys and their values.
pub struct Entries<'a> {
    children: Children<'a>,
}
impl<'a> Iterator for Entries<'a> {
    type Item = (&'a str, ValueRef<'a>);
    fn next(&mut self) -> Option<Self::Item> {
        let value = self.children.next()?;
        let key = value.document.nodes[value.id.index()].key_text;
        Some((value.document.text(key), value))
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.children.size_hint()
    }
}
impl ExactSizeIterator for Entries<'_> {}

struct DocumentFrame {
    id: NodeId,
    last_child: Option<NodeId>,
    pending_key: Option<Span>,
}
struct DocumentSink {
    bytes: Vec<u8>,
    nodes: Vec<Node>,
    stack: Vec<DocumentFrame>,
    root: Option<NodeId>,
    grammar: DecimalGrammar,
    decimal_error: Option<DecimalError>,
}

impl DocumentSink {
    fn new(source: &[u8], grammar: DecimalGrammar) -> Self {
        Self {
            bytes: source.to_vec(),
            nodes: Vec::new(),
            stack: Vec::new(),
            root: None,
            grammar,
            decimal_error: None,
        }
    }
    fn tail(&mut self, text: &str) -> Span {
        let start = self.bytes.len();
        self.bytes.extend_from_slice(text.as_bytes());
        Span {
            start,
            end: self.bytes.len(),
        }
    }
    fn push(&mut self, mut node: Node) {
        let id = NodeId::new(self.nodes.len());
        if let Some(frame) = self.stack.last_mut() {
            node.depth = self.nodes[frame.id.index()]
                .depth
                .checked_add(1)
                .expect("lexical depth is bounded");
            if let Some(key_text) = frame.pending_key.take() {
                node.key_text = key_text;
            }
            if let Some(last) = frame.last_child {
                self.nodes[last.index()].next_sibling = Some(id.0);
            } else {
                self.nodes[frame.id.index()].first_child = Some(id.0);
            }
            frame.last_child = Some(id);
            self.nodes[frame.id.index()].child_count += 1;
        } else {
            self.root = Some(id);
        }
        node.subtree_end = u32::try_from(id.index() + 1).expect("input bounds node count");
        self.nodes.push(node);
    }
    fn begin(&mut self, kind: NativeKind) {
        let id = NodeId::new(self.nodes.len());
        self.push(Node::scalar(kind));
        self.stack.push(DocumentFrame {
            id,
            last_child: None,
            pending_key: None,
        });
    }
}

impl JsonSink for DocumentSink {
    type Output = Result<NativeDocument, DocumentError>;
    fn null(&mut self) {
        self.push(Node::scalar(NativeKind::Null));
    }
    fn boolean(&mut self, value: bool) {
        let mut node = Node::scalar(NativeKind::Bool);
        node.boolean = value;
        self.push(node);
    }
    fn number(&mut self, lexeme: &str, start: usize, end: usize) {
        let mut node = Node::scalar(NativeKind::Number);
        node.text = Span { start, end };
        match ExactDecimal::parse(lexeme, self.grammar) {
            Ok(value) => node.decimal = Some(value),
            Err(error) if self.decimal_error.is_none() => self.decimal_error = Some(error),
            Err(_) => {}
        }
        self.push(node);
    }
    fn text(&mut self, value: crate::wire::lexical::JsonText<'_>) {
        let mut node = Node::scalar(NativeKind::String);
        node.text = match value {
            crate::wire::lexical::JsonText::Source { start, end, .. } => Span { start, end },
            crate::wire::lexical::JsonText::Escaped { value, .. } => self.tail(&value),
        };
        self.push(node);
    }
    fn begin_array(&mut self) {
        self.begin(NativeKind::Array);
    }
    fn end_array(&mut self) {
        let frame = self.stack.pop().expect("scanner balances arrays");
        self.nodes[frame.id.index()].subtree_end =
            u32::try_from(self.nodes.len()).expect("input bounds node count");
    }
    fn begin_object(&mut self) {
        self.begin(NativeKind::Object);
    }
    fn key(
        &mut self,
        value: crate::wire::lexical::JsonText<'_>,
        offset: usize,
    ) -> Result<(), LexicalError> {
        let (start, end) = value.span();
        let source = Span { start, end };
        let key_text = match &value {
            crate::wire::lexical::JsonText::Source { .. } => source,
            crate::wire::lexical::JsonText::Escaped { value, .. } => self.tail(value),
        };
        let key = value.value();
        let parent = self.stack.last().expect("key belongs to object").id;
        let mut child = self.nodes[parent.index()].first_child.map(NodeId);
        let mut duplicate = false;
        while let Some(id) = child {
            let node = &self.nodes[id.index()];
            if core::str::from_utf8(&self.bytes[node.key_text.start..node.key_text.end])
                .expect("scanner produced UTF-8")
                == key
            {
                duplicate = true;
                break;
            }
            child = node.next_sibling.map(NodeId);
        }
        if duplicate {
            return Err(LexicalError::DuplicateField { offset });
        }
        self.stack
            .last_mut()
            .expect("key belongs to object")
            .pending_key = Some(key_text);
        Ok(())
    }
    fn end_object(&mut self) {
        let frame = self.stack.pop().expect("scanner balances objects");
        self.nodes[frame.id.index()].subtree_end =
            u32::try_from(self.nodes.len()).expect("input bounds node count");
    }
    fn finish(mut self) -> Self::Output {
        if let Some(error) = self.decimal_error {
            return Err(DocumentError::Decimal(error));
        }
        let root = self.root.expect("scanner supplies root");
        if self.nodes[root.index()].subtree_end == 0 {
            self.nodes[root.index()].subtree_end =
                u32::try_from(self.nodes.len()).expect("input bounds node count");
        }
        Ok(NativeDocument {
            bytes: self.bytes,
            nodes: self.nodes,
            root,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{native::NativeValue, wire::lexical::parse_lexical};

    fn grammar() -> DecimalGrammar {
        DecimalGrammar::new(18, 30, true, true).expect("valid grammar")
    }

    #[test]
    fn node_stays_compact() {
        assert!(
            size_of::<Node>() <= 112,
            "node is {} bytes",
            size_of::<Node>()
        );
    }

    fn same(flat: ValueRef<'_>, tree: &NativeValue) {
        match tree {
            NativeValue::Null => assert_eq!(flat.kind(), NativeKind::Null),
            NativeValue::Bool(value) => assert_eq!(flat.as_bool(), Some(*value)),
            NativeValue::String(value) => assert_eq!(flat.as_text(), Some(value.as_str())),
            NativeValue::Number { lexeme, value } => {
                assert_eq!(flat.number_lexeme(), Some(lexeme.as_str()));
                assert_eq!(flat.exact_decimal(), Some(value));
            }
            NativeValue::Array(values) => {
                assert_eq!(flat.children().len(), values.len());
                for (flat, tree) in flat.children().zip(values) {
                    same(flat, tree);
                }
            }
            NativeValue::Object(entries) => {
                assert_eq!(flat.entries().len(), entries.len());
                for ((key, flat), (expected, tree)) in flat.entries().zip(entries) {
                    assert_eq!(key, expected);
                    same(flat, tree);
                }
            }
        }
    }

    #[test]
    fn flat_document_matches_legacy_native_oracle() {
        let source = br#"{"plain":"text","escaped":"a\nb","n":1.20e2,"items":[null,false,{"x":"\uD83D\uDE00"}]}"#;
        let limits = LexicalLimits::venue_payload();
        let flat = NativeDocument::parse(source, limits, grammar()).expect("flat parse");
        let tree = NativeValue::from_lexical(
            parse_lexical(source, limits).expect("lexical parse"),
            grammar(),
        )
        .expect("native parse");
        same(flat.root_view(), &tree);
        assert!(flat.memory_bytes() >= source.len());
    }

    #[test]
    fn escaped_duplicate_keys_are_rejected_semantically() {
        let error = NativeDocument::parse(
            br#"{"a":1,"\u0061":2}"#,
            LexicalLimits::venue_payload(),
            grammar(),
        )
        .expect_err("duplicate key");
        assert!(matches!(
            error,
            DocumentError::Lexical(LexicalError::DuplicateField { .. })
        ));
    }

    #[test]
    fn bounds_and_accounting_preserve_source_views() {
        let document = NativeDocument::parse(
            br#"{"a":["plain",2]}"#,
            LexicalLimits::venue_payload(),
            grammar(),
        )
        .expect("parse");
        let array = document.root_view().field("a").expect("array");
        assert_eq!(
            array.children().next().and_then(ValueRef::as_text),
            Some("plain")
        );
        assert_eq!(
            document.bounded_measure(document.root(), 4, 2),
            Some((4, document.memory_bytes()))
        );
        assert_eq!(document.bounded_measure(document.root(), 3, 2), None);
        assert_eq!(document.bounded_measure(document.root(), 4, 1), None);
        assert_eq!(
            document.memory_bytes(),
            size_of::<NativeDocument>()
                + document.bytes.capacity()
                + document.nodes.capacity() * size_of::<Node>()
        );
    }

    #[test]
    fn oversized_input_is_rejected_before_document_copy() {
        let limits = LexicalLimits::venue_payload().with_max_bytes(2);
        assert!(matches!(
            NativeDocument::parse(b"123", limits, grammar()),
            Err(DocumentError::Lexical(LexicalError::InputTooLarge { .. }))
        ));
    }
}

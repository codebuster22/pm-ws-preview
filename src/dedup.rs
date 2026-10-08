//! The venue-agnostic dedup and ordering-evidence seam.
//!
//! Every venue names one frame-scoped value that identifies a frame — a sequence number
//! where the venue publishes one, some other per-frame token otherwise — and declares, once
//! per venue and event family, what that value is evidence of. The core carries the value
//! ([`DedupKey`]) and the declaration ([`DedupKeyDeclaration`]) and reads nothing into
//! either: ordering exists here only where a venue has declared it, and no venue is
//! special-cased.
//!
//! Nothing in this module changes publishing. It exists so that the socket pool has a
//! typed thing to gate on: first-wins dedup across sockets needs a key whose equality means
//! "the same frame", and publishing by arrival across sockets needs
//! [`DedupKeySemantics::OrderingProven`], which no venue declares today. Until a venue earns
//! that declaration, [`classify_key`] is conformance analysis and diagnostics only.

use crate::{Candidate, CandidateOperation, PublishedBook, Side};
use core::fmt;
use serde::Serialize;

/// The longest venue key lexeme this seam carries, in bytes.
pub const MAX_DEDUP_LEXEME_BYTES: usize = 128;

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
const FIELD_SEPARATOR: u8 = 0x1f;
const RECORD_SEPARATOR: u8 = 0x1e;

/// A venue-reported value that identifies one frame within whatever scope its venue
/// declares.
///
/// The value is the venue's own, kept exactly: an integer key is the venue's integer, and
/// any other key is the venue's lexeme byte for byte. Two keys are equal only when they are
/// the same kind carrying the same value, so an integer key and the lexeme spelling of the
/// same digits are never conflated.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum DedupKey {
    Integer(u64),
    Lexeme(DedupLexeme),
}

/// A non-integer venue key lexeme.
///
/// Restricted to non-empty printable ASCII without spaces and at most
/// [`MAX_DEDUP_LEXEME_BYTES`] bytes, because a key is rendered into single-line
/// machine-parsable conformance records; a venue whose key needs other bytes must be
/// admitted deliberately rather than by breaking that rendering.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DedupLexeme(String);

impl DedupLexeme {
    /// Fails with [`DedupKeyError::Empty`] on an empty lexeme, [`DedupKeyError::TooLong`]
    /// beyond [`MAX_DEDUP_LEXEME_BYTES`], and [`DedupKeyError::UnsupportedCharacter`] for
    /// any byte outside printable ASCII excluding space.
    pub fn new(value: impl Into<String>) -> Result<Self, DedupKeyError> {
        let value = value.into();
        if value.is_empty() {
            return Err(DedupKeyError::Empty);
        }
        if value.len() > MAX_DEDUP_LEXEME_BYTES {
            return Err(DedupKeyError::TooLong);
        }
        if value.bytes().any(|byte| !(b'!'..=b'~').contains(&byte)) {
            return Err(DedupKeyError::UnsupportedCharacter);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl DedupKey {
    /// The key a venue reports as an integer it has already parsed.
    pub fn integer(value: u64) -> Self {
        Self::Integer(value)
    }

    /// Reads a venue's integer key from its reported lexeme, exactly.
    ///
    /// Accepts a bare non-negative decimal integer and nothing else. Fails with
    /// [`DedupKeyError::Empty`] on an empty lexeme, [`DedupKeyError::NotAnExactInteger`] for
    /// anything carrying a sign, a decimal point, an exponent, whitespace, or any other
    /// non-digit byte, and [`DedupKeyError::OutOfRange`] for a value beyond `u64`. It never
    /// rounds, truncates, or reinterprets: a venue value this cannot represent exactly is
    /// refused rather than approximated.
    ///
    /// A lexeme with leading zeros is admitted and compares by its numeric value, so `007`
    /// and `7` are one key.
    pub fn parse_exact_integer(lexeme: &str) -> Result<Self, DedupKeyError> {
        if lexeme.is_empty() {
            return Err(DedupKeyError::Empty);
        }
        if !lexeme.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(DedupKeyError::NotAnExactInteger);
        }
        lexeme
            .parse::<u64>()
            .map(Self::Integer)
            .map_err(|_| DedupKeyError::OutOfRange)
    }

    /// Reads a venue's non-integer key from its reported lexeme. Fails exactly as
    /// [`DedupLexeme::new`] does.
    pub fn lexeme(value: impl Into<String>) -> Result<Self, DedupKeyError> {
        DedupLexeme::new(value).map(Self::Lexeme)
    }

    /// The integer value of an integer key, or `None` for a lexeme key.
    pub fn as_integer(&self) -> Option<u64> {
        match self {
            Self::Integer(value) => Some(*value),
            Self::Lexeme(_) => None,
        }
    }

    /// The lexeme of a lexeme key, or `None` for an integer key.
    pub fn as_lexeme(&self) -> Option<&str> {
        match self {
            Self::Integer(_) => None,
            Self::Lexeme(value) => Some(value.as_str()),
        }
    }
}

impl fmt::Display for DedupKey {
    /// Renders the key as its venue value: the decimal digits of an integer key, the
    /// lexeme of a lexeme key. Both are single tokens free of whitespace, so a key is safe
    /// to write into a machine-parsable record.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Integer(value) => write!(f, "{value}"),
            Self::Lexeme(value) => f.write_str(value.as_str()),
        }
    }
}

/// A distinct, programmatically matchable reason a venue value could not be carried as a
/// dedup key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DedupKeyError {
    Empty,
    TooLong,
    NotAnExactInteger,
    OutOfRange,
    UnsupportedCharacter,
}

impl fmt::Display for DedupKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid venue dedup key")
    }
}

impl std::error::Error for DedupKeyError {}

/// What a venue declares its dedup key is evidence of.
///
/// The declaration belongs to the venue and its event family, never to a frame: a venue
/// cannot claim more ordering for one frame than it publishes for the family. It is a claim
/// about the venue, so it is raised only by venue documentation or by recorded conformance
/// evidence — never by a run that happened not to observe a counterexample.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DedupKeySemantics {
    /// Equality is evidence that two frames are the same frame. Nothing else: two unequal
    /// keys say nothing about which frame came first.
    DedupOnly,
    /// Observed non-decreasing along one connection-session, and dedup-only beyond it by
    /// documentation. Two keys from two connections are not a documented ordering fact
    /// under this declaration, so a pool publishes across sockets on it only where an
    /// operator has opted in against recorded cross-connection conformance evidence, and
    /// only behind a live tripwire that withdraws the licence on the first violation. See
    /// [`crate::PoolGate`].
    SessionMonotone,
    /// Venue-documented or conformance-proven to order frames across connections. No venue
    /// declares this today; it exists so the pool's gate has a name.
    OrderingProven,
}

impl DedupKeySemantics {
    /// Whether keys observed on one connection-session may be compared for order.
    pub fn orders_within_session(self) -> bool {
        matches!(self, Self::SessionMonotone | Self::OrderingProven)
    }

    /// Whether keys observed on different connections may be compared for order on the
    /// venue's own documentation. False for everything but [`Self::OrderingProven`], which
    /// no venue holds today.
    ///
    /// This is documentation, not permission: a pool may still be opted into publishing
    /// across sockets under [`Self::SessionMonotone`] on recorded conformance evidence, and
    /// [`crate::PoolGate::new`] is where that distinction is enforced.
    pub fn orders_across_connections(self) -> bool {
        matches!(self, Self::OrderingProven)
    }

    /// Whether a socket pool may be opted into publishing by arrival across its sockets.
    ///
    /// True from [`Self::SessionMonotone`] up: the key orders frames within a
    /// connection-session, which is what makes a per-connection inversion detectable and
    /// therefore what makes the tripwire possible. [`Self::DedupOnly`] grants no order at
    /// all — a pool on it could neither choose the newer arrival nor recognize a violation
    /// — so it is refused.
    pub fn admits_pooled_publish(self) -> bool {
        self.orders_within_session()
    }

    /// The stable label this declaration is written as in machine-parsable records.
    pub fn as_label(self) -> &'static str {
        match self {
            Self::DedupOnly => "dedup-only",
            Self::SessionMonotone => "session-monotone",
            Self::OrderingProven => "ordering-proven",
        }
    }
}

/// One venue's dedup-key declaration for one event family: which field carries the key and
/// what the venue's own evidence says it means.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DedupKeyDeclaration {
    venue: &'static str,
    family: &'static str,
    field: &'static str,
    semantics: DedupKeySemantics,
}

impl DedupKeyDeclaration {
    /// `venue` is the venue half of published identity, `family` the venue's own event
    /// name, and `field` the venue's own field path carrying the key, so a declaration
    /// names the venue's vocabulary rather than a local alias.
    pub const fn new(
        venue: &'static str,
        family: &'static str,
        field: &'static str,
        semantics: DedupKeySemantics,
    ) -> Self {
        Self {
            venue,
            family,
            field,
            semantics,
        }
    }

    pub const fn venue(&self) -> &'static str {
        self.venue
    }

    pub const fn family(&self) -> &'static str {
        self.family
    }

    pub const fn field(&self) -> &'static str {
        self.field
    }

    pub const fn semantics(&self) -> DedupKeySemantics {
        self.semantics
    }
}

/// What one key says about the key that preceded it on the same stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyRelation {
    /// No prior key on this stream. Evidence of nothing.
    First,
    /// The same key again: under every declaration, evidence that this is a frame already
    /// seen.
    Duplicate,
    /// A later key under a declaration that orders it.
    Advance,
    /// An earlier key under a declaration that orders it — the pool's tripwire, and a
    /// counterexample to the declaration when it appears within one connection-session.
    Inversion,
    /// Two different keys the declaration gives no way to order.
    Unordered,
}

/// Classifies `next` against `last` under a venue's declared `semantics`.
///
/// Pure: it reads only the two keys and the declaration, and publishes nothing. `last` is
/// `None` for the first key on a stream, which is [`KeyRelation::First`].
///
/// Equal keys are [`KeyRelation::Duplicate`] under every declaration, because equality is
/// the one meaning every venue key carries. Order is reported only where the declaration
/// grants it and the keys are comparable: two integer keys under a declaration that
/// [`DedupKeySemantics::orders_within_session`] are [`KeyRelation::Advance`] or
/// [`KeyRelation::Inversion`]; everything else — a dedup-only declaration, two lexeme keys,
/// or a kind mismatch — is [`KeyRelation::Unordered`].
///
/// Under [`DedupKeySemantics::SessionMonotone`] an ordering answer is meaningful only for
/// keys observed on one connection-session; feeding it keys from two connections asks a
/// question that declaration does not answer, and the caller, not this function, owns that
/// distinction.
pub fn classify_key(
    semantics: DedupKeySemantics,
    last: Option<&DedupKey>,
    next: &DedupKey,
) -> KeyRelation {
    let Some(last) = last else {
        return KeyRelation::First;
    };
    if last == next {
        return KeyRelation::Duplicate;
    }
    match (last.as_integer(), next.as_integer()) {
        (Some(last), Some(next)) if semantics.orders_within_session() => {
            if next > last {
                KeyRelation::Advance
            } else {
                KeyRelation::Inversion
            }
        }
        _ => KeyRelation::Unordered,
    }
}

/// A 64-bit fingerprint of one book's economic content.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
pub struct ContentDigest(u64);

impl ContentDigest {
    pub fn value(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ContentDigest {
    /// Sixteen lowercase hex digits, zero-padded and unprefixed, so digests from different
    /// sockets compare as plain strings in a conformance record.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// Fingerprints exactly what makes two books economically the same: the market, and every
/// canonical level's side, price, and quantity in canonical decimal form, so `0.50` and
/// `0.5` fingerprint alike. Revision, authority, continuity, provenance, and connection
/// metadata take no part.
///
/// Diagnostic evidence only, and deliberately not the standby agreement authority: replica
/// comparison stays an exact level-by-level equality, which no 64-bit fingerprint can
/// stand in for without letting a collision authorize a promotion. What this is for is
/// comparing what two sockets delivered, offline, from records neither socket could hold in
/// memory.
///
/// FNV-1a over a separator-delimited encoding of that projection: not cryptographic, and
/// deterministic across processes, builds, and platforms. It allocates one canonical
/// lexeme per level, so it belongs on diagnostic paths rather than on the update path.
pub fn content_digest(book: &PublishedBook) -> ContentDigest {
    let market = book.market();
    let mut state = absorb(FNV_OFFSET_BASIS, market.venue().as_str().as_bytes());
    state = absorb(state, &[FIELD_SEPARATOR]);
    state = absorb(state, market.key().kind().as_str().as_bytes());
    state = absorb(state, &[FIELD_SEPARATOR]);
    state = absorb(state, market.key().value().as_bytes());
    state = absorb(state, &[RECORD_SEPARATOR]);
    for level in book.canonical_levels() {
        let side = match level.side() {
            Side::Bid => b'B',
            Side::Ask => b'A',
        };
        state = absorb(state, &[side, FIELD_SEPARATOR]);
        state = absorb(state, level.price().value().canonical().as_bytes());
        state = absorb(state, &[FIELD_SEPARATOR]);
        state = absorb(state, level.quantity().value().canonical().as_bytes());
        state = absorb(state, &[RECORD_SEPARATOR]);
    }
    ContentDigest(state)
}

fn absorb(state: u64, bytes: &[u8]) -> u64 {
    bytes.iter().fold(state, |state, byte| {
        (state ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}

/// The exact bytes one candidate's own reported content projects to.
///
/// Two projections are equal exactly when two arrivals reported the same content, with no
/// fingerprint standing between the comparison and the answer. That is why a pool's
/// equal-key check compares these and not [`ContentDigest`]s: a 64-bit collision there
/// would mask one venue key naming two different book states, which is precisely the
/// observation a pool's licence depends on never happening.
///
/// The projection is the candidate's market, which operation it carries, and every level's
/// side, price, and quantity in canonical decimal form, so `0.50` and `0.5` project alike.
/// Provenance takes no part: two sockets reporting the same venue frame project alike
/// however differently they were stamped.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidateProjection(Vec<u8>);

impl CandidateProjection {
    /// The projected bytes. Meaningful only against another projection: they are an
    /// encoding chosen for exact comparison, not a wire or storage format.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// How many bytes this projection occupies, which is what a bounded store of them
    /// counts against its byte ceiling.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// A 64-bit fingerprint of this projection, for telemetry and single-line records.
    ///
    /// Never a substitute for comparing the projections themselves: everything
    /// [`content_digest`] says about collisions applies here.
    pub fn digest(&self) -> ContentDigest {
        ContentDigest(absorb(FNV_OFFSET_BASIS, &self.0))
    }
}

/// Projects exactly what one candidate reported, for exact comparison with another
/// candidate's projection. See [`CandidateProjection`] for what is and is not included.
///
/// Its encoding is deliberately distinguishable from [`content_digest`]'s — the operation
/// tag is absorbed where a book digest absorbs its levels — because the two answer
/// different questions: this one asks what one arrival said, that one asks what a book now
/// holds. The two are never compared with each other.
///
/// Allocates the projection and one canonical lexeme per level, so a caller that runs it on
/// an update path is paying for the exactness deliberately.
pub fn candidate_projection(candidate: &Candidate) -> CandidateProjection {
    let market = candidate.provenance().market();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(market.venue().as_str().as_bytes());
    bytes.push(FIELD_SEPARATOR);
    bytes.extend_from_slice(market.key().kind().as_str().as_bytes());
    bytes.push(FIELD_SEPARATOR);
    bytes.extend_from_slice(market.key().value().as_bytes());
    bytes.push(RECORD_SEPARATOR);
    let (tag, levels) = match candidate.operation() {
        CandidateOperation::Snapshot(levels) => (b'S', levels),
        CandidateOperation::SourceDelta(levels) => (b'D', levels),
    };
    bytes.push(tag);
    bytes.push(RECORD_SEPARATOR);
    for level in levels.levels() {
        bytes.push(match level.side() {
            Side::Bid => b'B',
            Side::Ask => b'A',
        });
        bytes.push(FIELD_SEPARATOR);
        bytes.extend_from_slice(level.price().value().canonical().as_bytes());
        bytes.push(FIELD_SEPARATOR);
        bytes.extend_from_slice(level.quantity().value().canonical().as_bytes());
        bytes.push(RECORD_SEPARATOR);
    }
    CandidateProjection(bytes)
}

/// The fingerprint of [`candidate_projection`], for telemetry and single-line records.
///
/// Everything [`content_digest`] says about a 64-bit fingerprint applies: it is evidence to
/// read, never evidence to gate on. A pool's equal-key check compares projections.
pub fn candidate_digest(candidate: &Candidate) -> ContentDigest {
    candidate_projection(candidate).digest()
}

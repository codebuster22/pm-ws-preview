pub mod connection;
pub mod shard;
pub mod supervisor;

use crate::observation::Side;
use crate::wire::lexical::LexicalValue;
use crate::wire::socketio::DecodedFrame;
use crate::{
    BoundedLevels, Candidate, DecimalError, DecimalGrammar, DedupKey, DedupKeyDeclaration,
    DedupKeyError, DedupKeySemantics, DescriptorError, Level, LevelCapacity, ObservationError,
    Origin, Price, Provenance, Quantity, Representation, SourceEvidence, SourceEvidenceValue,
};
use core::fmt;

const MARKETS_NAMESPACE: &str = "/markets";

/// The dedup and ordering-evidence key this venue declares for `orderbookUpdate`: the
/// event's top-level `version`, carried as an exact integer.
///
/// [`DedupKeySemantics::SessionMonotone`] is exactly what the recorded observations in
/// `docs/limitless.md` support: `version` was observed strictly increasing in arrival order
/// along one connection-session, non-contiguous per market, with its scope unestablished
/// and no cross-connection ordering meaning. It is deliberately not
/// [`DedupKeySemantics::OrderingProven`] — the venue documents no ordering guarantee for
/// this field, so nothing may publish by arrival across connections on it until conformance
/// evidence or venue documentation raises the declaration.
pub const ORDERBOOK_UPDATE_DEDUP_KEY: DedupKeyDeclaration = DedupKeyDeclaration::new(
    supervisor::VENUE,
    "orderbookUpdate",
    "version",
    DedupKeySemantics::SessionMonotone,
);

fn venue_decimal_grammar() -> DecimalGrammar {
    DecimalGrammar::new(18, 30, true, false).expect("pinned Limitless decimal grammar is valid")
}

/// A decoded Limitless `/markets` Socket.IO event.
///
/// An event name outside the daemon's required scope is preserved as
/// [`LimitlessEvent::Unknown`] rather than treated as book data or a decode
/// failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LimitlessEvent {
    OrderbookUpdate(OrderbookUpdate),
    MarketResolved(MarketResolved),
    Unknown { name: String },
}

/// A venue-reported order-book snapshot for one market.
///
/// `bids` are strictly descending by price and `asks` strictly ascending, as
/// the venue documents. A zero-size level is reproduced as reported. `token_id`
/// is `Some` only when the venue payload actually carries `orderbook.tokenId`.
/// `timestamp` and `version` are kept as the reported lexemes, uninterpreted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderbookUpdate {
    market_slug: String,
    token_id: Option<String>,
    bids: Vec<(Price, Quantity)>,
    asks: Vec<(Price, Quantity)>,
    timestamp: String,
    version: Option<String>,
}

impl OrderbookUpdate {
    pub fn market_slug(&self) -> &str {
        &self.market_slug
    }

    pub fn token_id(&self) -> Option<&str> {
        self.token_id.as_deref()
    }

    pub fn bids(&self) -> &[(Price, Quantity)] {
        &self.bids
    }

    pub fn asks(&self) -> &[(Price, Quantity)] {
        &self.asks
    }

    pub fn timestamp(&self) -> &str {
        &self.timestamp
    }

    /// The venue's top-level `version` lexeme, when the payload carries one.
    ///
    /// Kept exactly as reported and never interpreted here. The venue documents no meaning
    /// for it, so no code path may detect a gap by it — it is non-contiguous per market —
    /// and nothing orders or deduplicates by it by default.
    ///
    /// The one exception is an operator opting a run into a publishing pool, which orders
    /// and deduplicates by it against the conformance sessions recorded under "Ordering and
    /// redundant connections" in `docs/limitless.md`, and only behind the live tripwire that
    /// withdraws the licence on the first observation contradicting them. Even there the
    /// value stays the venue's own: see [`Self::dedup_key`].
    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    /// This event's dedup key under [`ORDERBOOK_UPDATE_DEDUP_KEY`]: the top-level `version`
    /// as an exact integer, or `None` when the payload carries no `version`.
    ///
    /// Fails with [`DedupKeyError::NotAnExactInteger`] for a `version` that is not a bare
    /// non-negative integer and [`DedupKeyError::OutOfRange`] for one beyond `u64` — the
    /// venue's value is refused rather than rounded. A failure costs only the key: the
    /// event's book content is unaffected, and the lexeme still reaches provenance through
    /// [`Self::version_evidence`], which keeps it exactly as reported.
    pub fn dedup_key(&self) -> Result<Option<DedupKey>, DedupKeyError> {
        self.version
            .as_deref()
            .map(DedupKey::parse_exact_integer)
            .transpose()
    }

    /// The `version` lexeme as provenance source evidence, or `None` when absent.
    ///
    /// Fails with [`DescriptorError::InvalidSourceEvidence`] for a lexeme longer than the
    /// evidence bound.
    pub fn version_evidence(&self) -> Result<Option<SourceEvidence>, DescriptorError> {
        self.version
            .as_ref()
            .map(|value| Ok(SourceEvidence::Version(SourceEvidenceValue::new(value)?)))
            .transpose()
    }

    /// Normalizes this complete venue book into a snapshot [`Candidate`] under
    /// caller-supplied provenance.
    ///
    /// Every reported bid and ask becomes one [`Level`] tagged with its side, preserving the
    /// venue's exact prices and sizes and its reported order. The daemon invents no
    /// provenance: the caller supplies the connection, generations, positions, timestamps,
    /// and replica role that the event actually arrived under.
    ///
    /// Fails with [`ObservationError::CapacityExceeded`] when the book holds more levels
    /// than `capacity`, and with [`ObservationError::InvalidOrigin`] when the provenance is
    /// anything but [`Representation::VenueNative`] plus [`Origin::SourceReported`] — a
    /// Limitless book snapshot is never normalized or locally derived.
    pub fn snapshot_candidate(
        &self,
        provenance: Provenance,
        capacity: LevelCapacity,
    ) -> Result<Candidate, ObservationError> {
        if !matches!(
            (provenance.representation(), provenance.origin()),
            (Representation::VenueNative, Origin::SourceReported)
        ) {
            return Err(ObservationError::InvalidOrigin);
        }
        let levels = self
            .bids
            .iter()
            .map(|(price, size)| (Side::Bid, price, size))
            .chain(
                self.asks
                    .iter()
                    .map(|(price, size)| (Side::Ask, price, size)),
            )
            .map(|(side, price, size)| Level::new(side, price.clone(), size.clone()));
        Candidate::snapshot(provenance, BoundedLevels::new(levels, capacity)?)
    }
}

/// A venue-reported market resolution, reproduced without interpretation.
///
/// Resolution does not close, freeze, or clear the local order book; this
/// type carries only the venue's own fields.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketResolved {
    slug: String,
    market_type: String,
    winning_outcome: String,
    winning_index: u32,
    resolution_date: String,
}

impl MarketResolved {
    pub fn slug(&self) -> &str {
        &self.slug
    }

    pub fn market_type(&self) -> &str {
        &self.market_type
    }

    pub fn winning_outcome(&self) -> &str {
        &self.winning_outcome
    }

    pub fn winning_index(&self) -> u32 {
        self.winning_index
    }

    pub fn resolution_date(&self) -> &str {
        &self.resolution_date
    }
}

/// A distinct, programmatically matchable reason a `/markets` event payload
/// was rejected.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LimitlessDecodeError {
    NotAnEvent,
    InvalidField {
        field: &'static str,
    },
    Decimal {
        field: &'static str,
        source: DecimalError,
    },
    PriceOutOfDomain {
        field: &'static str,
    },
    LevelOrdering {
        side: Side,
    },
}

impl fmt::Display for LimitlessDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid Limitless /markets event")
    }
}

impl std::error::Error for LimitlessDecodeError {}

/// Decodes a Socket.IO event frame from the `/markets` namespace into a
/// [`LimitlessEvent`].
///
/// Returns [`LimitlessDecodeError::NotAnEvent`] for a frame that carries no
/// `/markets` event (wrong namespace, or no event name).
pub fn decode_event(frame: &DecodedFrame) -> Result<LimitlessEvent, LimitlessDecodeError> {
    if frame.namespace() != Some(MARKETS_NAMESPACE) {
        return Err(LimitlessDecodeError::NotAnEvent);
    }
    let name = frame.event_name().ok_or(LimitlessDecodeError::NotAnEvent)?;
    match name {
        "orderbookUpdate" => decode_orderbook_update(frame).map(LimitlessEvent::OrderbookUpdate),
        "marketResolved" => decode_market_resolved(frame).map(LimitlessEvent::MarketResolved),
        other => Ok(LimitlessEvent::Unknown {
            name: other.to_owned(),
        }),
    }
}

fn decode_orderbook_update(frame: &DecodedFrame) -> Result<OrderbookUpdate, LimitlessDecodeError> {
    let payload = frame
        .payload()
        .ok_or(LimitlessDecodeError::InvalidField { field: "payload" })?;
    let market_slug = text_field(payload, "marketSlug")?.to_owned();
    let orderbook = payload
        .field("orderbook")
        .ok_or(LimitlessDecodeError::InvalidField { field: "orderbook" })?;
    let token_id = optional_text_field(orderbook, "tokenId")?;
    let bids = decode_levels(orderbook, "bids", Side::Bid)?;
    let asks = decode_levels(orderbook, "asks", Side::Ask)?;
    let timestamp = text_field(payload, "timestamp")?.to_owned();
    let version = optional_number_field(payload, "version")?;
    Ok(OrderbookUpdate {
        market_slug,
        token_id,
        bids,
        asks,
        timestamp,
        version,
    })
}

fn decode_market_resolved(frame: &DecodedFrame) -> Result<MarketResolved, LimitlessDecodeError> {
    let payload = frame
        .payload()
        .ok_or(LimitlessDecodeError::InvalidField { field: "payload" })?;
    let slug = text_field(payload, "slug")?.to_owned();
    let market_type = text_field(payload, "type")?.to_owned();
    let winning_outcome = text_field(payload, "winningOutcome")?.to_owned();
    let winning_index = number_field(payload, "winningIndex")?
        .parse::<u32>()
        .map_err(|_| LimitlessDecodeError::InvalidField {
            field: "winningIndex",
        })?;
    let resolution_date = text_field(payload, "resolutionDate")?.to_owned();
    Ok(MarketResolved {
        slug,
        market_type,
        winning_outcome,
        winning_index,
        resolution_date,
    })
}

fn decode_levels(
    orderbook: &LexicalValue,
    field: &'static str,
    side: Side,
) -> Result<Vec<(Price, Quantity)>, LimitlessDecodeError> {
    let array = orderbook
        .field(field)
        .and_then(LexicalValue::as_array)
        .ok_or(LimitlessDecodeError::InvalidField { field })?;
    let grammar = venue_decimal_grammar();
    let domain_upper_bound = Price::parse("1", grammar).expect("literal price domain bound parses");
    let mut levels: Vec<(Price, Quantity)> = Vec::with_capacity(array.len());
    for entry in array {
        let price_lexeme = number_field(entry, "price")?;
        let price = Price::parse(price_lexeme, grammar).map_err(|source| {
            LimitlessDecodeError::Decimal {
                field: "price",
                source,
            }
        })?;
        if price.value() > domain_upper_bound.value() {
            return Err(LimitlessDecodeError::PriceOutOfDomain { field: "price" });
        }
        let size_lexeme = number_field(entry, "size")?;
        let quantity = Quantity::parse(size_lexeme, grammar).map_err(|source| {
            LimitlessDecodeError::Decimal {
                field: "size",
                source,
            }
        })?;
        if let Some((last_price, _)) = levels.last() {
            let ordered = match side {
                Side::Bid => price.value() < last_price.value(),
                Side::Ask => price.value() > last_price.value(),
            };
            if !ordered {
                return Err(LimitlessDecodeError::LevelOrdering { side });
            }
        }
        levels.push((price, quantity));
    }
    Ok(levels)
}

fn text_field<'a>(
    value: &'a LexicalValue,
    field: &'static str,
) -> Result<&'a str, LimitlessDecodeError> {
    value
        .field(field)
        .and_then(LexicalValue::as_text)
        .ok_or(LimitlessDecodeError::InvalidField { field })
}

fn number_field<'a>(
    value: &'a LexicalValue,
    field: &'static str,
) -> Result<&'a str, LimitlessDecodeError> {
    value
        .field(field)
        .and_then(LexicalValue::as_number)
        .map(crate::wire::lexical::NumberLexeme::as_str)
        .ok_or(LimitlessDecodeError::InvalidField { field })
}

/// Reads an optional JSON number field as its exact lexeme: absent decodes to `None`, a
/// JSON number decodes to `Some` carrying its reported digits unchanged, and any other
/// present JSON kind (string, null, bool, array, object) is a decode error rather than
/// being conflated with absence.
fn optional_number_field(
    value: &LexicalValue,
    field: &'static str,
) -> Result<Option<String>, LimitlessDecodeError> {
    match value.field(field) {
        None => Ok(None),
        Some(LexicalValue::Number(number)) => Ok(Some(number.as_str().to_owned())),
        Some(_) => Err(LimitlessDecodeError::InvalidField { field }),
    }
}

/// Reads an optional text field: absent decodes to `None`, a JSON string decodes to
/// `Some`, and any other present JSON kind (number, null, bool, array, object) is a decode
/// error rather than being conflated with absence.
fn optional_text_field(
    value: &LexicalValue,
    field: &'static str,
) -> Result<Option<String>, LimitlessDecodeError> {
    match value.field(field) {
        None => Ok(None),
        Some(LexicalValue::Text(text)) => Ok(Some(text.clone())),
        Some(_) => Err(LimitlessDecodeError::InvalidField { field }),
    }
}

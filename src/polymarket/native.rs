//! Complete Polymarket market-channel event decoding.

use std::sync::Arc;

use crate::{
    native::document::{DocumentError, NativeDocument, NativeKind, NodeId, ValueRef},
    native::{
        NativeBatch, NativeEvent, NativeFamily, NativeIdentity, NativePayload, NativeSource,
        NativeVenue,
    },
    numeric::{DecimalError, DecimalGrammar},
    polymarket::MAX_PAYLOAD_BYTES,
    wire::lexical::LexicalLimits,
};

fn grammar() -> DecimalGrammar {
    DecimalGrammar::new(18, 30, false, false).expect("fixed Polymarket decimal grammar")
}

/// A complete application message could not be retained as a native event batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NativeDecodeError {
    Lexical,
    EmptyArray,
    Unsupported,
    Field(&'static str),
    Decimal {
        field: &'static str,
        source: DecimalError,
    },
}

impl core::fmt::Display for NativeDecodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("invalid Polymarket native event")
    }
}

impl std::error::Error for NativeDecodeError {}

/// Decodes one complete Polymarket application message after validating every array member.
///
/// The returned batch retains source field and array order. `PONG` is a distinct control
/// family. Unknown discriminated object families are retained as bounded complete envelopes.
pub fn decode_message(
    bytes: &[u8],
    source: NativeSource,
) -> Result<NativeBatch, NativeDecodeError> {
    if bytes == b"PONG" {
        return Ok(NativeBatch {
            source,
            input_bytes: bytes.len(),
            events: vec![NativeEvent {
                venue: NativeVenue::Polymarket,
                family: NativeFamily::PolymarketPongControl,
                family_name: None,
                market: None,
                assets: Vec::new(),
                identity: None,
                payload: NativePayload::from_document(parse_document(
                    br#""PONG""#,
                    LexicalLimits::venue_payload(),
                )?),
                member_index: 0,
            }],
        });
    }
    let document = parse_document(
        bytes,
        LexicalLimits::venue_payload().with_max_bytes(MAX_PAYLOAD_BYTES),
    )?;
    decode_document(document, source, bytes.len())
}

/// Parses a bounded complete message directly into exact typed values.
pub fn parse_document(
    bytes: &[u8],
    limits: LexicalLimits,
) -> Result<NativeDocument, NativeDecodeError> {
    NativeDocument::parse(bytes, limits, grammar()).map_err(|error| match error {
        DocumentError::Lexical(_) => NativeDecodeError::Lexical,
        DocumentError::Decimal(source) => NativeDecodeError::Decimal {
            field: "number",
            source,
        },
    })
}

/// Validates every event and promotes economic strings before freezing the shared document.
/// A malformed final member rejects the entire message, with no partial batch returned.
pub fn decode_document(
    mut document: NativeDocument,
    source: NativeSource,
    input_bytes: usize,
) -> Result<NativeBatch, NativeDecodeError> {
    let root = document.root_view();
    let array = root.kind() == NativeKind::Array;
    let capacity = if array { root.children().len() } else { 1 };
    if capacity == 0 {
        return Err(NativeDecodeError::EmptyArray);
    }
    let mut next = if array {
        root.children().next().map(|value| value.id())
    } else {
        Some(root.id())
    };
    let mut prepared = Vec::with_capacity(capacity);
    while let Some(id) = next {
        next = if array {
            document.view(id).next_sibling().map(|value| value.id())
        } else {
            None
        };
        let (shape, family_name) = decode_one(&mut document, id)?;
        prepared.push((id, shape, family_name));
    }
    let document = Arc::new(document);
    let events = prepared
        .into_iter()
        .enumerate()
        .map(
            |(member_index, (id, (family, market, assets, identity), family_name))| NativeEvent {
                venue: NativeVenue::Polymarket,
                family,
                family_name,
                market,
                assets,
                identity,
                payload: NativePayload::new(document.clone(), id),
                member_index,
            },
        )
        .collect();
    Ok(NativeBatch {
        source,
        input_bytes,
        events,
    })
}

fn decode_one(
    document: &mut NativeDocument,
    root: NodeId,
) -> Result<(Shape, Option<Arc<str>>), NativeDecodeError> {
    let value = document.view(root);
    let event_type = required_text(value, "event_type")?;
    let (family, market, assets, identity) = match event_type {
        "book" => book(value)?,
        "price_change" => price_change(value)?,
        "last_trade_price" => asset_event(
            value,
            NativeFamily::PolymarketLastTradePrice,
            &[
                "price",
                "size",
                "fee_rate_bps",
                "side",
                "timestamp",
                "transaction_hash",
            ],
        )?,
        "tick_size_change" => asset_event(
            value,
            NativeFamily::PolymarketTickSizeChange,
            &["old_tick_size", "new_tick_size", "timestamp"],
        )?,
        "best_bid_ask" => asset_event(
            value,
            NativeFamily::PolymarketBestBidAsk,
            &["best_bid", "best_ask", "spread", "timestamp"],
        )?,
        "new_market" => new_market(value)?,
        "market_resolved" => market_resolved(value)?,
        _ => {
            require_object(value)?;
            (NativeFamily::Unknown, None, Vec::new(), None)
        }
    };
    let family_name = (family == NativeFamily::Unknown).then(|| Arc::from(event_type));
    promote_economics(document, root, family)?;
    Ok(((family, market, assets, identity), family_name))
}

type Shape = (
    NativeFamily,
    Option<Arc<str>>,
    Vec<Arc<str>>,
    Option<NativeIdentity>,
);

fn book(value: ValueRef<'_>) -> Result<Shape, NativeDecodeError> {
    let market = required_text(value, "market")?;
    let asset = required_text(value, "asset_id")?;
    let timestamp = required_text(value, "timestamp")?;
    let hash = required_text(value, "hash")?;
    levels(value, "bids")?;
    levels(value, "asks")?;
    Ok((
        NativeFamily::PolymarketBook,
        Some(Arc::from(market)),
        vec![Arc::from(asset)],
        Some(NativeIdentity::Components(vec![
            Arc::from(timestamp),
            Arc::from(hash),
        ])),
    ))
}

fn price_change(value: ValueRef<'_>) -> Result<Shape, NativeDecodeError> {
    let market = required_text(value, "market")?;
    let timestamp = required_text(value, "timestamp")?;
    let entries = required_array(value, "price_changes")?;
    let mut assets = Vec::with_capacity(entries.len());
    let mut identity = Vec::with_capacity(entries.len());
    for entry in entries {
        require_object(entry)?;
        let asset = required_text(entry, "asset_id")?;
        required_text(entry, "price")?;
        required_text(entry, "size")?;
        let side = required_text(entry, "side")?;
        if side != "BUY" && side != "SELL" {
            return Err(NativeDecodeError::Field("side"));
        }
        let hash = required_text(entry, "hash")?;
        required_text(entry, "best_bid")?;
        required_text(entry, "best_ask")?;
        assets.push(Arc::from(asset));
        identity.push((Arc::from(asset), Arc::from(hash)));
    }
    Ok((
        NativeFamily::PolymarketPriceChange,
        Some(Arc::from(market)),
        assets,
        Some(NativeIdentity::PolymarketDelta {
            timestamp: Arc::from(timestamp),
            entries: identity,
        }),
    ))
}

fn asset_event(
    value: ValueRef<'_>,
    family: NativeFamily,
    fields: &[&'static str],
) -> Result<Shape, NativeDecodeError> {
    let market = required_text(value, "market")?;
    let asset = required_text(value, "asset_id")?;
    for field in fields {
        required_text(value, field)?;
    }
    if family == NativeFamily::PolymarketLastTradePrice {
        let side = required_text(value, "side")?;
        if side != "BUY" && side != "SELL" {
            return Err(NativeDecodeError::Field("side"));
        }
    }
    Ok((
        family,
        Some(Arc::from(market)),
        vec![Arc::from(asset)],
        None,
    ))
}

fn new_market(value: ValueRef<'_>) -> Result<Shape, NativeDecodeError> {
    let market = required_text(value, "market")?;
    for field in ["id", "question", "slug", "description", "timestamp"] {
        required_text(value, field)?;
    }
    let assets = text_array(value, "assets_ids")?;
    text_array(value, "outcomes")?;
    require_object(required(value, "event_message")?)?;
    optional_text_array(value, "tags")?;
    optional_text(value, "condition_id")?;
    optional_bool(value, "active")?;
    optional_text_array(value, "clob_token_ids")?;
    for field in [
        "sports_market_type",
        "line",
        "game_start_time",
        "group_item_title",
    ] {
        optional_text(value, field)?;
    }
    optional_text(value, "order_price_min_tick_size")?;
    Ok((
        NativeFamily::PolymarketNewMarket,
        Some(Arc::from(market)),
        assets.into_iter().map(Arc::from).collect(),
        None,
    ))
}

fn market_resolved(value: ValueRef<'_>) -> Result<Shape, NativeDecodeError> {
    let market = required_text(value, "market")?;
    for field in ["id", "winning_asset_id", "winning_outcome", "timestamp"] {
        required_text(value, field)?;
    }
    let assets = text_array(value, "assets_ids")?;
    optional_text_array(value, "tags")?;
    Ok((
        NativeFamily::PolymarketMarketResolved,
        Some(Arc::from(market)),
        assets.into_iter().map(Arc::from).collect(),
        None,
    ))
}

fn levels(value: ValueRef<'_>, field: &'static str) -> Result<(), NativeDecodeError> {
    for level in required_array(value, field)? {
        require_object(level)?;
        required_text(level, "price")?;
        required_text(level, "size")?;
    }
    Ok(())
}

fn promote_economics(
    document: &mut NativeDocument,
    root: NodeId,
    family: NativeFamily,
) -> Result<(), NativeDecodeError> {
    let fields: &[&str] = match family {
        NativeFamily::PolymarketBook | NativeFamily::PolymarketPriceChange => &["timestamp"],
        NativeFamily::PolymarketLastTradePrice => &["price", "size", "fee_rate_bps", "timestamp"],
        NativeFamily::PolymarketTickSizeChange => &["old_tick_size", "new_tick_size", "timestamp"],
        NativeFamily::PolymarketBestBidAsk => &["best_bid", "best_ask", "spread", "timestamp"],
        NativeFamily::PolymarketNewMarket | NativeFamily::PolymarketMarketResolved => {
            &["timestamp"]
        }
        _ => &[],
    };
    for field in fields {
        promote(document, root, field)?;
    }
    if family == NativeFamily::PolymarketNewMarket
        && document
            .view(root)
            .field("order_price_min_tick_size")
            .is_some()
    {
        promote(document, root, "order_price_min_tick_size")?;
    }
    let arrays: &[(&str, &[&str])] = match family {
        NativeFamily::PolymarketBook => {
            &[("bids", &["price", "size"]), ("asks", &["price", "size"])]
        }
        NativeFamily::PolymarketPriceChange => {
            &[("price_changes", &["price", "size", "best_bid", "best_ask"])]
        }
        _ => &[],
    };
    for (array, fields) in arrays {
        let mut next = required_array(document.view(root), array)?
            .next()
            .map(|value| value.id());
        while let Some(id) = next {
            next = document.view(id).next_sibling().map(|value| value.id());
            for field in *fields {
                promote(document, id, field)?;
            }
        }
    }
    Ok(())
}

fn promote(
    document: &mut NativeDocument,
    root: NodeId,
    field: &'static str,
) -> Result<(), NativeDecodeError> {
    let id = required(document.view(root), field)?.id();
    document
        .promote_decimal_string(id, grammar())
        .map_err(|source| NativeDecodeError::Decimal { field, source })
}

fn require_object(value: ValueRef<'_>) -> Result<(), NativeDecodeError> {
    (value.kind() == NativeKind::Object)
        .then_some(())
        .ok_or(NativeDecodeError::Unsupported)
}
fn required<'a>(
    value: ValueRef<'a>,
    field: &'static str,
) -> Result<ValueRef<'a>, NativeDecodeError> {
    value.field(field).ok_or(NativeDecodeError::Field(field))
}
fn required_text<'a>(
    value: ValueRef<'a>,
    field: &'static str,
) -> Result<&'a str, NativeDecodeError> {
    required(value, field)?
        .as_text()
        .ok_or(NativeDecodeError::Field(field))
}
fn required_array<'a>(
    value: ValueRef<'a>,
    field: &'static str,
) -> Result<impl ExactSizeIterator<Item = ValueRef<'a>>, NativeDecodeError> {
    let array = required(value, field)?;
    (array.kind() == NativeKind::Array)
        .then(|| array.children())
        .ok_or(NativeDecodeError::Field(field))
}
fn text_array<'a>(
    value: ValueRef<'a>,
    field: &'static str,
) -> Result<Vec<&'a str>, NativeDecodeError> {
    let values = required_array(value, field)?;
    values
        .map(|value| value.as_text().ok_or(NativeDecodeError::Field(field)))
        .collect()
}
fn optional_text(value: ValueRef<'_>, field: &'static str) -> Result<(), NativeDecodeError> {
    value.field(field).map_or(Ok(()), |value| {
        if value.kind() == NativeKind::Null {
            return Ok(());
        }
        value
            .as_text()
            .map(|_| ())
            .ok_or(NativeDecodeError::Field(field))
    })
}
fn optional_bool(value: ValueRef<'_>, field: &'static str) -> Result<(), NativeDecodeError> {
    value.field(field).map_or(Ok(()), |value| {
        matches!(value.kind(), NativeKind::Null | NativeKind::Bool)
            .then_some(())
            .ok_or(NativeDecodeError::Field(field))
    })
}
fn optional_text_array(value: ValueRef<'_>, field: &'static str) -> Result<(), NativeDecodeError> {
    let Some(value) = value.field(field) else {
        return Ok(());
    };
    if value.kind() == NativeKind::Null {
        return Ok(());
    }
    if value.kind() != NativeKind::Array {
        return Err(NativeDecodeError::Field(field));
    }
    let values = value.children();
    for value in values {
        value.as_text().ok_or(NativeDecodeError::Field(field))?;
    }
    Ok(())
}

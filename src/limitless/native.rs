//! Complete native-event decoding for the Limitless `/markets` namespace.

use std::sync::Arc;

use crate::{
    native::document::{NativeDocument, NodeId, ValueRef},
    native::{
        NativeBatch, NativeEvent, NativeFamily, NativeIdentity, NativePayload, NativeSource,
        NativeVenue,
    },
    numeric::{DecimalError, DecimalGrammar},
    wire::socketio::{DecodedFrame, SocketIoPacket},
};

const MARKETS_NAMESPACE: &str = "/markets";

/// Converts one already-decoded Socket.IO frame into a complete Limitless application batch.
///
/// Non-application transport frames, other namespaces, and Socket.IO controls return `None`.
/// A selected application event is accepted only after its complete documented shape and every
/// argument have been retained. All JSON number lexemes become exact values; AMM price strings
/// are additionally represented as exact decimal strings.
pub fn decode_native_frame(
    frame: DecodedFrame,
    source: NativeSource,
    input_bytes: usize,
) -> Result<Option<NativeBatch>, LimitlessNativeError> {
    if frame.namespace() != Some(MARKETS_NAMESPACE)
        || frame.socket_io() != Some(SocketIoPacket::Event)
    {
        return Ok(None);
    }
    let (mut document, root, name) = frame
        .into_native_document()
        .ok_or(LimitlessNativeError::MissingPayload)?;
    let name = name.ok_or(LimitlessNativeError::MissingEventName)?;
    let known = known_family(&name);
    let family_name = (!known).then(|| Arc::from(name.as_str()));
    let mut arguments = document.view(root).children();
    let _ = arguments
        .next()
        .ok_or(LimitlessNativeError::MissingEventName)?;
    let payload = arguments.next().map(ValueRef::id);
    let (family, market, assets, identity) = if let Some(payload) = payload {
        let (family, market, assets, identity) = validate(&name, document.view(payload))?;
        (
            family,
            market.map(Arc::from),
            assets.into_iter().map(Arc::from).collect(),
            identity,
        )
    } else if known {
        return Err(LimitlessNativeError::MissingPayload);
    } else {
        (NativeFamily::Unknown, None, Vec::new(), None)
    };
    if family == NativeFamily::LimitlessNewPriceData {
        promote(
            &mut document,
            payload.expect("known price payload"),
            &["updatedPrices", "yes"],
        )?;
        promote(
            &mut document,
            payload.expect("known price payload"),
            &["updatedPrices", "no"],
        )?;
    }
    let event = NativeEvent {
        venue: NativeVenue::Limitless,
        family,
        family_name,
        market,
        assets,
        identity,
        payload: NativePayload::new(Arc::new(document), root),
        member_index: 0,
    };
    Ok(Some(NativeBatch {
        source,
        input_bytes,
        events: vec![event],
    }))
}

pub(crate) fn decimal_grammar() -> DecimalGrammar {
    DecimalGrammar::new(30, 39, true, true).expect("native decimal grammar is representable")
}

fn known_family(name: &str) -> bool {
    matches!(
        name,
        "orderbookUpdate"
            | "newPriceData"
            | "marketCreated"
            | "marketResolved"
            | "system"
            | "exception"
    )
}

type ValidatedFields<'a> = (
    NativeFamily,
    Option<&'a str>,
    Vec<&'a str>,
    Option<NativeIdentity>,
);

fn validate<'a>(
    name: &str,
    source: ValueRef<'a>,
) -> Result<ValidatedFields<'a>, LimitlessNativeError> {
    match name {
        "orderbookUpdate" => orderbook(source),
        "newPriceData" => prices(source),
        "marketCreated" => lifecycle_created(source),
        "marketResolved" => lifecycle_resolved(source),
        "system" => system(source),
        "exception" => exception(source),
        _ => Ok((NativeFamily::Unknown, None, Vec::new(), None)),
    }
}

fn orderbook<'a>(source: ValueRef<'a>) -> Result<ValidatedFields<'a>, LimitlessNativeError> {
    let market = text(source, "marketSlug")?;
    let book = object(source, "orderbook")?;
    levels(book, "bids")?;
    levels(book, "asks")?;
    let timestamp = text(source, "timestamp")?;
    let version = number(source, "version")?;
    let asset = optional_text(book, "tokenId")?;
    let canonical_version = version
        .exact_decimal()
        .ok_or(LimitlessNativeError::InvalidField("version"))?
        .to_string();
    let identity = key(&[timestamp, &canonical_version]);
    Ok((
        NativeFamily::LimitlessOrderbookUpdate,
        Some(market),
        asset.into_iter().collect(),
        Some(identity),
    ))
}

fn levels(book: ValueRef<'_>, field: &'static str) -> Result<(), LimitlessNativeError> {
    let entries = array(book, field)?;
    for entry in entries.children() {
        number(entry, "price")?;
        number(entry, "size")?;
    }
    Ok(())
}

fn prices<'a>(source: ValueRef<'a>) -> Result<ValidatedFields<'a>, LimitlessNativeError> {
    let market = text(source, "marketAddress")?;
    let prices = object(source, "updatedPrices")?;
    text(prices, "yes")?;
    text(prices, "no")?;
    number(source, "blockNumber")?;
    text(source, "timestamp")?;
    Ok((
        NativeFamily::LimitlessNewPriceData,
        Some(market),
        Vec::new(),
        None,
    ))
}

fn lifecycle_created(source: ValueRef<'_>) -> Result<ValidatedFields<'_>, LimitlessNativeError> {
    let market = text(source, "slug")?;
    text(source, "title")?;
    market_type(source)?;
    optional_text(source, "groupSlug")?;
    if source.field("categoryIds").is_some() {
        for category in array(source, "categoryIds")?.children() {
            number_value(category, "categoryIds[]")?;
        }
    }
    text(source, "createdAt")?;
    Ok((
        NativeFamily::LimitlessMarketCreated,
        Some(market),
        Vec::new(),
        None,
    ))
}

fn lifecycle_resolved(source: ValueRef<'_>) -> Result<ValidatedFields<'_>, LimitlessNativeError> {
    let market = text(source, "slug")?;
    market_type(source)?;
    one_of(
        text(source, "winningOutcome")?,
        "winningOutcome",
        &["YES", "NO"],
    )?;
    let winning_index = number(source, "winningIndex")?
        .exact_decimal()
        .ok_or(LimitlessNativeError::InvalidField("winningIndex"))?
        .to_string();
    one_of(&winning_index, "winningIndex", &["0", "1"])?;
    text(source, "resolutionDate")?;
    Ok((
        NativeFamily::LimitlessMarketResolved,
        Some(market),
        Vec::new(),
        None,
    ))
}

fn system(source: ValueRef<'_>) -> Result<ValidatedFields<'_>, LimitlessNativeError> {
    text(source, "message")?;
    if source.field("markets").is_some() {
        for market in array(source, "markets")?.children() {
            text_value(market, "markets[]")?;
        }
    }
    Ok((NativeFamily::LimitlessSystem, None, Vec::new(), None))
}

fn exception(source: ValueRef<'_>) -> Result<ValidatedFields<'_>, LimitlessNativeError> {
    if source.kind() != crate::native::document::NativeKind::Object {
        return Err(LimitlessNativeError::InvalidField("payload"));
    }
    Ok((NativeFamily::LimitlessException, None, Vec::new(), None))
}

fn key(parts: &[&str]) -> NativeIdentity {
    NativeIdentity::Components(parts.iter().map(|part| Arc::from(*part)).collect())
}

fn text<'a>(value: ValueRef<'a>, field: &'static str) -> Result<&'a str, LimitlessNativeError> {
    text_value(
        value
            .field(field)
            .ok_or(LimitlessNativeError::InvalidField(field))?,
        field,
    )
}
fn optional_text<'a>(
    value: ValueRef<'a>,
    field: &'static str,
) -> Result<Option<&'a str>, LimitlessNativeError> {
    match value.field(field) {
        None => Ok(None),
        Some(value) if value.kind() == crate::native::document::NativeKind::Null => Ok(None),
        Some(value) => text_value(value, field).map(Some),
    }
}

fn market_type(value: ValueRef<'_>) -> Result<(), LimitlessNativeError> {
    one_of(text(value, "type")?, "type", &["AMM", "CLOB"])
}

fn one_of(value: &str, field: &'static str, accepted: &[&str]) -> Result<(), LimitlessNativeError> {
    accepted
        .contains(&value)
        .then_some(())
        .ok_or(LimitlessNativeError::InvalidField(field))
}
fn text_value<'a>(
    value: ValueRef<'a>,
    field: &'static str,
) -> Result<&'a str, LimitlessNativeError> {
    value
        .as_text()
        .ok_or(LimitlessNativeError::InvalidField(field))
}
fn number<'a>(
    value: ValueRef<'a>,
    field: &'static str,
) -> Result<ValueRef<'a>, LimitlessNativeError> {
    number_value(
        value
            .field(field)
            .ok_or(LimitlessNativeError::InvalidField(field))?,
        field,
    )
}
fn number_value<'a>(
    value: ValueRef<'a>,
    field: &'static str,
) -> Result<ValueRef<'a>, LimitlessNativeError> {
    (value.kind() == crate::native::document::NativeKind::Number)
        .then_some(value)
        .ok_or(LimitlessNativeError::InvalidField(field))
}
fn object<'a>(
    value: ValueRef<'a>,
    field: &'static str,
) -> Result<ValueRef<'a>, LimitlessNativeError> {
    match value.field(field) {
        Some(value) if value.kind() == crate::native::document::NativeKind::Object => Ok(value),
        _ => Err(LimitlessNativeError::InvalidField(field)),
    }
}
fn array<'a>(
    value: ValueRef<'a>,
    field: &'static str,
) -> Result<ValueRef<'a>, LimitlessNativeError> {
    match value.field(field) {
        Some(value) if value.kind() == crate::native::document::NativeKind::Array => Ok(value),
        _ => Err(LimitlessNativeError::InvalidField(field)),
    }
}
fn promote(
    document: &mut NativeDocument,
    value: NodeId,
    path: &[&str],
) -> Result<(), LimitlessNativeError> {
    let mut current = document.view(value);
    for name in &path[..path.len() - 1] {
        current = current
            .field(name)
            .ok_or(LimitlessNativeError::InvalidField("payload"))?;
    }
    let field = current
        .field(path[path.len() - 1])
        .ok_or(LimitlessNativeError::InvalidField("payload"))?
        .id();
    document
        .promote_decimal_string(field, decimal_grammar())
        .map_err(LimitlessNativeError::Decimal)
}

/// A named validation failure for a Limitless native application event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LimitlessNativeError {
    MissingEventName,
    MissingPayload,
    InvalidField(&'static str),
    Decimal(DecimalError),
}

impl core::fmt::Display for LimitlessNativeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("invalid Limitless native event")
    }
}
impl std::error::Error for LimitlessNativeError {}

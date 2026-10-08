use core::fmt;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Serialize};

const MAX_VENUE_BYTES: usize = 128;
const MAX_KIND_BYTES: usize = 128;
const MAX_NATIVE_BYTES: usize = 1024;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
#[serde(transparent)]
pub struct Venue(String);
impl Venue {
    pub fn new(value: impl Into<String>) -> Result<Self, IdentityError> {
        let value = checked(value.into(), MAX_VENUE_BYTES, IdentityError::InvalidVenue)?;
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl<'de> Deserialize<'de> for Venue {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Self::new(String::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
#[serde(transparent)]
pub struct NativeIdentifierKind(String);
impl NativeIdentifierKind {
    pub fn new(value: impl Into<String>, max_bytes: usize) -> Result<Self, IdentityError> {
        if max_bytes == 0 || max_bytes > MAX_KIND_BYTES {
            return Err(IdentityError::InvalidIdentifierKind);
        }
        Ok(Self(checked(
            value.into(),
            max_bytes,
            IdentityError::InvalidIdentifierKind,
        )?))
    }
    pub fn slug() -> Self {
        Self("slug".into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl<'de> Deserialize<'de> for NativeIdentifierKind {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Self::new(String::deserialize(d)?, MAX_KIND_BYTES).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
pub struct NativeMarketKey {
    kind: NativeIdentifierKind,
    value: String,
}
impl NativeMarketKey {
    pub fn new(
        kind: NativeIdentifierKind,
        value: impl Into<String>,
    ) -> Result<Self, IdentityError> {
        Ok(Self {
            kind,
            value: checked(
                value.into(),
                MAX_NATIVE_BYTES,
                IdentityError::InvalidNativeKey,
            )?,
        })
    }
    pub fn kind(&self) -> &NativeIdentifierKind {
        &self.kind
    }
    pub fn value(&self) -> &str {
        &self.value
    }
}
impl<'de> Deserialize<'de> for NativeMarketKey {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Wire {
            kind: NativeIdentifierKind,
            value: String,
        }
        let wire = Wire::deserialize(d)?;
        Self::new(wire.kind, wire.value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
pub struct MarketRef {
    venue: Venue,
    key: NativeMarketKey,
}
impl MarketRef {
    pub fn new(venue: Venue, key: NativeMarketKey) -> Self {
        Self { venue, key }
    }
    pub fn venue(&self) -> &Venue {
        &self.venue
    }
    pub fn key(&self) -> &NativeMarketKey {
        &self.key
    }
}
impl<'de> Deserialize<'de> for MarketRef {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Wire {
            venue: Venue,
            key: NativeMarketKey,
        }
        let wire = Wire::deserialize(d)?;
        Ok(Self::new(wire.venue, wire.key))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum NativeOutcomeKind {
    Token,
    Side,
    Index,
    VenueDefined,
}
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct NativeOutcome {
    kind: NativeOutcomeKind,
    value: NativeOutcomeValue,
}
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
enum NativeOutcomeValue {
    Text(String),
    Index(u16),
}
impl NativeOutcome {
    pub fn token(value: impl Into<String>) -> Result<Self, IdentityError> {
        Self::text(NativeOutcomeKind::Token, value)
    }
    pub fn side(value: impl Into<String>) -> Result<Self, IdentityError> {
        Self::text(NativeOutcomeKind::Side, value)
    }
    pub fn venue_defined(value: impl Into<String>) -> Result<Self, IdentityError> {
        Self::text(NativeOutcomeKind::VenueDefined, value)
    }
    pub fn index(value: u16) -> Self {
        Self {
            kind: NativeOutcomeKind::Index,
            value: NativeOutcomeValue::Index(value),
        }
    }
    fn text(kind: NativeOutcomeKind, value: impl Into<String>) -> Result<Self, IdentityError> {
        Ok(Self {
            kind,
            value: NativeOutcomeValue::Text(checked(
                value.into(),
                MAX_NATIVE_BYTES,
                IdentityError::InvalidOutcome,
            )?),
        })
    }
    pub fn kind(&self) -> NativeOutcomeKind {
        self.kind
    }
    pub fn text_value(&self) -> Option<&str> {
        match &self.value {
            NativeOutcomeValue::Text(value) => Some(value),
            NativeOutcomeValue::Index(_) => None,
        }
    }
    pub fn index_value(&self) -> Option<u16> {
        match self.value {
            NativeOutcomeValue::Index(value) => Some(value),
            NativeOutcomeValue::Text(_) => None,
        }
    }
}
impl Serialize for NativeOutcome {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("NativeOutcome", 2)?;
        state.serialize_field("kind", &self.kind)?;
        match &self.value {
            NativeOutcomeValue::Text(value) => state.serialize_field("value", value)?,
            NativeOutcomeValue::Index(value) => state.serialize_field("value", value)?,
        };
        state.end()
    }
}
impl<'de> Deserialize<'de> for NativeOutcome {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        enum WireValue {
            Text(String),
            Index(u16),
        }
        impl<'de> Deserialize<'de> for WireValue {
            fn deserialize<E: serde::Deserializer<'de>>(d: E) -> Result<Self, E::Error> {
                struct WireValueVisitor;
                impl<'de> Visitor<'de> for WireValueVisitor {
                    type Value = WireValue;

                    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        f.write_str("a native outcome string or unsigned 16-bit index")
                    }

                    fn visit_borrowed_str<E: serde::de::Error>(
                        self,
                        value: &'de str,
                    ) -> Result<Self::Value, E> {
                        Ok(WireValue::Text(value.into()))
                    }

                    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                        Ok(WireValue::Text(value.into()))
                    }

                    fn visit_string<E: serde::de::Error>(
                        self,
                        value: String,
                    ) -> Result<Self::Value, E> {
                        Ok(WireValue::Text(value))
                    }

                    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Self::Value, E> {
                        u16::try_from(value)
                            .map(WireValue::Index)
                            .map_err(|_| E::custom("native outcome index exceeds u16"))
                    }

                    fn visit_u128<E: serde::de::Error>(
                        self,
                        value: u128,
                    ) -> Result<Self::Value, E> {
                        u16::try_from(value)
                            .map(WireValue::Index)
                            .map_err(|_| E::custom("native outcome index exceeds u16"))
                    }
                }
                d.deserialize_any(WireValueVisitor)
            }
        }

        struct NativeOutcomeVisitor;
        impl<'de> Visitor<'de> for NativeOutcomeVisitor {
            type Value = NativeOutcome;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a native outcome object with kind and value")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
                let mut kind = None;
                let mut value = None;
                while let Some(field) = access.next_key::<String>()? {
                    match field.as_str() {
                        "kind" => {
                            if kind.is_some() {
                                return Err(serde::de::Error::duplicate_field("kind"));
                            }
                            kind = Some(access.next_value()?);
                        }
                        "value" => {
                            if value.is_some() {
                                return Err(serde::de::Error::duplicate_field("value"));
                            }
                            value = Some(access.next_value::<WireValue>()?);
                        }
                        _ => {
                            return Err(serde::de::Error::unknown_field(
                                &field,
                                &["kind", "value"],
                            ));
                        }
                    }
                }
                let kind = kind.ok_or_else(|| serde::de::Error::missing_field("kind"))?;
                let value = value.ok_or_else(|| serde::de::Error::missing_field("value"))?;
                match (kind, value) {
                    (NativeOutcomeKind::Index, WireValue::Index(value)) => {
                        Ok(NativeOutcome::index(value))
                    }
                    (NativeOutcomeKind::Index, WireValue::Text(_)) => Err(
                        serde::de::Error::custom("index outcome requires an unsigned index"),
                    ),
                    (kind, WireValue::Text(value)) => {
                        NativeOutcome::text(kind, value).map_err(serde::de::Error::custom)
                    }
                    (_, WireValue::Index(_)) => Err(serde::de::Error::custom(
                        "text outcome requires a string value",
                    )),
                }
            }
        }
        d.deserialize_map(NativeOutcomeVisitor)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
pub struct OutcomeRef {
    market: MarketRef,
    native: NativeOutcome,
}
impl OutcomeRef {
    pub fn new(market: MarketRef, native: NativeOutcome) -> Self {
        Self { market, native }
    }
    pub fn market(&self) -> &MarketRef {
        &self.market
    }
    pub fn native(&self) -> &NativeOutcome {
        &self.native
    }
}
impl<'de> Deserialize<'de> for OutcomeRef {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Wire {
            market: MarketRef,
            native: NativeOutcome,
        }
        let wire = Wire::deserialize(d)?;
        Ok(Self::new(wire.market, wire.native))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum LiquidityIdentity {
    Market(MarketRef),
    Outcome(OutcomeRef),
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum IdentityError {
    InvalidVenue,
    InvalidNativeKey,
    InvalidOutcome,
    InvalidIdentifierKind,
}
impl fmt::Display for IdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid identity")
    }
}
impl std::error::Error for IdentityError {}
fn checked(value: String, limit: usize, error: IdentityError) -> Result<String, IdentityError> {
    if value.is_empty() || value.len() > limit {
        Err(error)
    } else {
        Ok(value)
    }
}

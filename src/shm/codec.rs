//! Explicit-width encodings for everything the state slot carries.
//!
//! Discriminants are 32-bit words with values fixed by this ABI, never by Rust's own enum
//! layout, so adding a variant to a core enum is a compile error here rather than a silent
//! renumbering. Every decode is total: an unknown word yields `None` and the reader reports
//! a malformed record instead of guessing.

use crate::{
    AuthorityReason, AuthorityState, ContinuityReason, DecimalGrammar, DeliveryPath, ExactDecimal,
    IdentityError, MarketRef, MutationContinuity, NativeIdentifierKind, NativeMarketKey, Origin,
    Price, Quantity, Representation, Side, Venue,
};

/// The widest scale this ABI can carry.
///
/// The representation's scale cell is 32 bits wide, but the value it encodes is a 16-bit
/// decimal scale, so anything above this is not a scale at all and is rejected before
/// anything is built from it. `examples/reader.py` mirrors this bound.
pub(super) const MAX_STORED_SCALE: u32 = u16::MAX as u32;

const AUTHORITY_UNSUBSCRIBED: u32 = 1;
const AUTHORITY_SUBSCRIBING: u32 = 2;
const AUTHORITY_SYNCHRONIZING: u32 = 3;
const AUTHORITY_LIVE: u32 = 4;
const AUTHORITY_RECOVERING: u32 = 5;
const AUTHORITY_STALE: u32 = 6;

const REASON_GAP: u32 = 1;
const REASON_DISCONNECT: u32 = 2;
const REASON_SUBSCRIPTION_LOST: u32 = 3;
const REASON_LOCAL_LOSS: u32 = 4;
const REASON_ORDERING_UNKNOWN: u32 = 5;
const REASON_OVERLOAD: u32 = 6;
const REASON_REPLICA_DIVERGENCE: u32 = 7;
const REASON_RECOVERY_BASE_UNAVAILABLE: u32 = 8;

const CONTINUITY_INTACT: u32 = 1;
const CONTINUITY_LOST: u32 = 2;

const BREAK_OVERRUN: u32 = 1;
const BREAK_GAP: u32 = 2;
const BREAK_LOCAL_LOSS: u32 = 3;
const BREAK_RECONNECT: u32 = 4;
const BREAK_RECOVERY_BASE: u32 = 5;
const BREAK_SYNC_DIVERGENCE: u32 = 6;

const ORIGIN_SOURCE_REPORTED: u32 = 1;
const ORIGIN_NORMALIZED_FROM_SOURCE: u32 = 2;
const ORIGIN_LOCALLY_DERIVED: u32 = 3;
const DERIVATION_NONE: u32 = 0;
const DERIVATION_SNAPSHOT_DIFF: u32 = 1;

const REPRESENTATION_VENUE_NATIVE: u32 = 1;
const REPRESENTATION_NORMALIZED: u32 = 2;

const SIDE_BID: u32 = 1;
const SIDE_ASK: u32 = 2;

const DELIVERY_PATH_MARKET_FEED: u32 = 1;
const DELIVERY_PATH_LIFECYCLE_FEED: u32 = 2;
const DELIVERY_PATH_RESOLUTION_FEED: u32 = 3;

/// The grammar every price and quantity read back out of a slot is parsed under.
///
/// Negatives are forbidden and the digit ceiling is the widest an `i128` coefficient can
/// be, so a hostile coefficient decodes to `None` rather than to a nonsense level.
fn stored_grammar() -> DecimalGrammar {
    DecimalGrammar::new(u16::MAX, 39, false, false).expect("stored decimal grammar is valid")
}

/// The authority state as `(state, reason)` words. `reason` is zero unless the state is
/// [`AuthorityState::Stale`].
pub(crate) fn authority_words(state: &AuthorityState) -> (u32, u32) {
    match state {
        AuthorityState::Unsubscribed => (AUTHORITY_UNSUBSCRIBED, 0),
        AuthorityState::Subscribing => (AUTHORITY_SUBSCRIBING, 0),
        AuthorityState::Synchronizing => (AUTHORITY_SYNCHRONIZING, 0),
        AuthorityState::Live => (AUTHORITY_LIVE, 0),
        AuthorityState::Recovering => (AUTHORITY_RECOVERING, 0),
        AuthorityState::Stale(reason) => (
            AUTHORITY_STALE,
            match reason {
                AuthorityReason::Gap => REASON_GAP,
                AuthorityReason::Disconnect => REASON_DISCONNECT,
                AuthorityReason::SubscriptionLost => REASON_SUBSCRIPTION_LOST,
                AuthorityReason::LocalLoss => REASON_LOCAL_LOSS,
                AuthorityReason::OrderingUnknown => REASON_ORDERING_UNKNOWN,
                AuthorityReason::Overload => REASON_OVERLOAD,
                AuthorityReason::ReplicaDivergence => REASON_REPLICA_DIVERGENCE,
                AuthorityReason::RecoveryBaseUnavailable => REASON_RECOVERY_BASE_UNAVAILABLE,
            },
        ),
    }
}

/// The authority state named by `(state, reason)`, or `None` for an unknown pairing.
pub(super) fn authority_state(state: u32, reason: u32) -> Option<AuthorityState> {
    let plain = |value| (reason == 0).then_some(value);
    match state {
        AUTHORITY_UNSUBSCRIBED => plain(AuthorityState::Unsubscribed),
        AUTHORITY_SUBSCRIBING => plain(AuthorityState::Subscribing),
        AUTHORITY_SYNCHRONIZING => plain(AuthorityState::Synchronizing),
        AUTHORITY_LIVE => plain(AuthorityState::Live),
        AUTHORITY_RECOVERING => plain(AuthorityState::Recovering),
        AUTHORITY_STALE => Some(AuthorityState::Stale(match reason {
            REASON_GAP => AuthorityReason::Gap,
            REASON_DISCONNECT => AuthorityReason::Disconnect,
            REASON_SUBSCRIPTION_LOST => AuthorityReason::SubscriptionLost,
            REASON_LOCAL_LOSS => AuthorityReason::LocalLoss,
            REASON_ORDERING_UNKNOWN => AuthorityReason::OrderingUnknown,
            REASON_OVERLOAD => AuthorityReason::Overload,
            REASON_REPLICA_DIVERGENCE => AuthorityReason::ReplicaDivergence,
            REASON_RECOVERY_BASE_UNAVAILABLE => AuthorityReason::RecoveryBaseUnavailable,
            _ => return None,
        })),
        _ => None,
    }
}

/// The `BREAK_*` word naming a lost stream's [`ContinuityReason`].
pub(crate) fn break_word(reason: &ContinuityReason) -> u32 {
    match reason {
        ContinuityReason::Overrun => BREAK_OVERRUN,
        ContinuityReason::Gap => BREAK_GAP,
        ContinuityReason::LocalLoss => BREAK_LOCAL_LOSS,
        ContinuityReason::Reconnect => BREAK_RECONNECT,
        ContinuityReason::RecoveryBase => BREAK_RECOVERY_BASE,
        ContinuityReason::SyncDivergence => BREAK_SYNC_DIVERGENCE,
    }
}

/// Mutation continuity as `(kind, reason, epoch, next_position)`.
///
/// `reason` is zero for an intact stream and `next_position` is zero for a lost one: a lost
/// stream has no position, and publishing the last one would invite a reader to treat it as
/// live.
pub(crate) fn continuity_words(continuity: &MutationContinuity) -> (u32, u32, u64, u64) {
    match continuity {
        MutationContinuity::Intact {
            epoch,
            next_position,
        } => (CONTINUITY_INTACT, 0, *epoch, *next_position),
        MutationContinuity::Lost { epoch, reason } => {
            (CONTINUITY_LOST, break_word(reason), *epoch, 0)
        }
    }
}

/// The mutation continuity named by those four words, or `None` for an unknown pairing.
pub(super) fn continuity(
    kind: u32,
    reason: u32,
    epoch: u64,
    next_position: u64,
) -> Option<MutationContinuity> {
    match kind {
        CONTINUITY_INTACT if reason == 0 => Some(MutationContinuity::Intact {
            epoch,
            next_position,
        }),
        CONTINUITY_LOST if next_position == 0 => Some(MutationContinuity::Lost {
            epoch,
            reason: match reason {
                BREAK_OVERRUN => ContinuityReason::Overrun,
                BREAK_GAP => ContinuityReason::Gap,
                BREAK_LOCAL_LOSS => ContinuityReason::LocalLoss,
                BREAK_RECONNECT => ContinuityReason::Reconnect,
                BREAK_RECOVERY_BASE => ContinuityReason::RecoveryBase,
                BREAK_SYNC_DIVERGENCE => ContinuityReason::SyncDivergence,
                _ => return None,
            },
        }),
        _ => None,
    }
}

/// The provenance origin as `(origin, derivation)`. `derivation` is zero unless the origin
/// is [`Origin::LocallyDerived`], which is how a reader tells a venue-reported change from
/// one this daemon derived.
pub(crate) fn origin_words(origin: &Origin) -> (u32, u32) {
    match origin {
        Origin::SourceReported => (ORIGIN_SOURCE_REPORTED, DERIVATION_NONE),
        Origin::NormalizedFromSource => (ORIGIN_NORMALIZED_FROM_SOURCE, DERIVATION_NONE),
        Origin::LocallyDerived(crate::Derivation::SnapshotDiff) => {
            (ORIGIN_LOCALLY_DERIVED, DERIVATION_SNAPSHOT_DIFF)
        }
    }
}

/// The provenance origin named by `(origin, derivation)`, or `None` for an unknown pairing.
pub(super) fn origin(origin: u32, derivation: u32) -> Option<Origin> {
    match (origin, derivation) {
        (ORIGIN_SOURCE_REPORTED, DERIVATION_NONE) => Some(Origin::SourceReported),
        (ORIGIN_NORMALIZED_FROM_SOURCE, DERIVATION_NONE) => Some(Origin::NormalizedFromSource),
        (ORIGIN_LOCALLY_DERIVED, DERIVATION_SNAPSHOT_DIFF) => {
            Some(Origin::LocallyDerived(crate::Derivation::SnapshotDiff))
        }
        _ => None,
    }
}

pub(crate) fn representation_word(representation: &Representation) -> u32 {
    match representation {
        Representation::VenueNative => REPRESENTATION_VENUE_NATIVE,
        Representation::Normalized => REPRESENTATION_NORMALIZED,
    }
}

pub(super) fn representation(word: u32) -> Option<Representation> {
    match word {
        REPRESENTATION_VENUE_NATIVE => Some(Representation::VenueNative),
        REPRESENTATION_NORMALIZED => Some(Representation::Normalized),
        _ => None,
    }
}

pub(crate) fn side_word(side: Side) -> u32 {
    match side {
        Side::Bid => SIDE_BID,
        Side::Ask => SIDE_ASK,
    }
}

pub(super) fn side(word: u32) -> Option<Side> {
    match word {
        SIDE_BID => Some(Side::Bid),
        SIDE_ASK => Some(Side::Ask),
        _ => None,
    }
}

/// The word naming the feed an observation arrived on, or `None` for
/// [`DeliveryPath::OtherNativePath`].
///
/// This ABI has no cell for a venue's own path string, so such a path is refused rather
/// than flattened onto one of the three named feeds: a consumer must never be told an
/// observation arrived somewhere it did not.
pub(crate) fn delivery_path_word(path: &DeliveryPath) -> Option<u32> {
    match path {
        DeliveryPath::MarketFeed => Some(DELIVERY_PATH_MARKET_FEED),
        DeliveryPath::LifecycleFeed => Some(DELIVERY_PATH_LIFECYCLE_FEED),
        DeliveryPath::ResolutionFeed => Some(DELIVERY_PATH_RESOLUTION_FEED),
        DeliveryPath::OtherNativePath(_) => None,
    }
}

/// The feed that word names, or `None` for anything this ABI does not define.
pub(super) fn delivery_path(word: u32) -> Option<DeliveryPath> {
    match word {
        DELIVERY_PATH_MARKET_FEED => Some(DeliveryPath::MarketFeed),
        DELIVERY_PATH_LIFECYCLE_FEED => Some(DeliveryPath::LifecycleFeed),
        DELIVERY_PATH_RESOLUTION_FEED => Some(DeliveryPath::ResolutionFeed),
        _ => None,
    }
}

/// One exact decimal as `(coefficient_low, coefficient_high, scale)`.
///
/// The 128-bit two's-complement coefficient travels as two explicit 64-bit halves, so no
/// cell of this ABI is ever a 128-bit atomic; the scale is a decimal exponent in digits,
/// widened from `u16` to fill its 32-bit cell.
pub(super) fn decimal_words(value: &ExactDecimal) -> (u64, u64, u32) {
    let bits = value.coefficient() as u128;
    (bits as u64, (bits >> 64) as u64, u32::from(value.scale()))
}

/// The price those three words carry, or `None` when they name no representable
/// non-negative decimal.
///
/// The scale is range-checked against the 16-bit scale the representation actually has
/// **before** anything is built, so a hostile 32-bit scale is rejected as a malformed
/// record rather than sizing an allocation, and the value is then rebuilt from its parts
/// rather than round-tripped through a lexeme — a legitimately encoded large scale would
/// otherwise exceed the grammar's lexeme-length limit and be misreported as malformed.
pub(super) fn price(low: u64, high: u64, scale: u32) -> Option<Price> {
    if scale > MAX_STORED_SCALE {
        return None;
    }
    Price::from_parts(low, high, scale as u16, stored_grammar()).ok()
}

/// The quantity those three words carry, under the same rules as [`price`].
pub(super) fn quantity(low: u64, high: u64, scale: u32) -> Option<Quantity> {
    if scale > MAX_STORED_SCALE {
        return None;
    }
    Quantity::from_parts(low, high, scale as u16, stored_grammar()).ok()
}

/// The venue-native identity of `market` as write-once bytes.
///
/// The encoding is a fixed prefix — venue length `u16`, identifier-kind length `u16`, native
/// value length `u32`, all little-endian — followed by those three UTF-8 byte runs in that
/// order. Returns `None` when the encoding would exceed `capacity`: an identity that does
/// not fit is refused, never truncated, because a truncated venue identifier would name a
/// different market.
pub(super) fn encode_identity(market: &MarketRef, capacity: usize) -> Option<Vec<u8>> {
    let venue = market.venue().as_str().as_bytes();
    let kind = market.key().kind().as_str().as_bytes();
    let value = market.key().value().as_bytes();
    let venue_len = u16::try_from(venue.len()).ok()?;
    let kind_len = u16::try_from(kind.len()).ok()?;
    let value_len = u32::try_from(value.len()).ok()?;
    let total = 8usize
        .checked_add(venue.len())?
        .checked_add(kind.len())?
        .checked_add(value.len())?;
    if total > capacity {
        return None;
    }
    let mut bytes = Vec::with_capacity(total);
    bytes.extend_from_slice(&venue_len.to_le_bytes());
    bytes.extend_from_slice(&kind_len.to_le_bytes());
    bytes.extend_from_slice(&value_len.to_le_bytes());
    bytes.extend_from_slice(venue);
    bytes.extend_from_slice(kind);
    bytes.extend_from_slice(value);
    Some(bytes)
}

/// The market those identity bytes name, or `None` when they are not a well-formed
/// encoding of one.
pub(super) fn decode_identity(bytes: &[u8]) -> Option<MarketRef> {
    if bytes.len() < 8 {
        return None;
    }
    let venue_len = usize::from(u16::from_le_bytes([bytes[0], bytes[1]]));
    let kind_len = usize::from(u16::from_le_bytes([bytes[2], bytes[3]]));
    let value_len =
        usize::try_from(u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]])).ok()?;
    let body = bytes.get(8..)?;
    let venue = body.get(..venue_len)?;
    let kind = body.get(venue_len..venue_len.checked_add(kind_len)?)?;
    let value_start = venue_len.checked_add(kind_len)?;
    let value = body.get(value_start..value_start.checked_add(value_len)?)?;
    let text = |raw: &[u8]| core::str::from_utf8(raw).ok().map(str::to_owned);
    let market = |venue: String, kind: String, value: String| -> Result<MarketRef, IdentityError> {
        Ok(MarketRef::new(
            Venue::new(venue)?,
            NativeMarketKey::new(NativeIdentifierKind::new(kind, 128)?, value)?,
        ))
    };
    market(text(venue)?, text(kind)?, text(value)?).ok()
}

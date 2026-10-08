//! Single-writer bounded admission for one selected venue source.
use super::{NativeBatch, NativeEvent, NativeFamily, NativeIdentity, NativeVenue};
use std::collections::{HashSet, VecDeque};
use std::mem::size_of;
use std::sync::Arc;

/// Hard admission limits, including bounds for public batch construction.
#[derive(Clone, Debug)]
pub struct GateLimits {
    pub bytes: usize,
    pub sticky_faults: usize,
    pub max_input_bytes: usize,
    pub max_events: usize,
    pub max_value_nodes: usize,
    pub max_value_depth: u16,
}

impl Default for GateLimits {
    fn default() -> Self {
        Self {
            bytes: 1_048_576,
            sticky_faults: 32,
            max_input_bytes: 262_144,
            max_events: 4_096,
            max_value_nodes: 65_536,
            max_value_depth: 16,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GateCounters {
    pub received: u64,
    pub admitted: u64,
    pub stale: u64,
    pub overload: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GateFault {
    StaleSource,
    Capacity,
    InvalidEvent,
}

#[derive(Clone, Debug)]
pub enum GateDecision {
    Admit(Arc<NativeBatch>),
    Fault(GateFault),
}

/// Owns one stream's publication decision. Calls are serialized by its owner.
pub struct SingleSourceGate {
    limits: GateLimits,
    generation: u64,
    faults: VecDeque<GateFault>,
    counters: GateCounters,
}

impl SingleSourceGate {
    /// Creates a bounded gate accepting the specified local stream generation.
    pub fn new(limits: GateLimits, generation: u64) -> Self {
        Self {
            limits,
            generation,
            faults: VecDeque::new(),
            counters: GateCounters::default(),
        }
    }
    /// Returns saturating counts for all batches seen by this gate, including rejected batches.
    pub fn counters(&self) -> &GateCounters {
        &self.counters
    }

    /// Returns the only stream generation this gate may publish.
    pub fn generation(&self) -> u64 {
        self.generation
    }
    /// Advances the accepted source generation; an older batch cannot publish.
    pub fn set_generation(&mut self, generation: u64) {
        self.generation = generation;
    }
    /// Iterates retained admission faults from oldest to newest; retention is bounded by `sticky_faults`.
    pub fn faults(&self) -> impl ExactSizeIterator<Item = &GateFault> {
        self.faults.iter()
    }
    fn fault(&mut self, fault: GateFault) -> GateDecision {
        if self.limits.sticky_faults != 0 {
            if self.faults.len() == self.limits.sticky_faults {
                self.faults.pop_front();
            }
            self.faults.push_back(fault.clone());
        }
        GateDecision::Fault(fault)
    }
    fn text_bytes(value: &Arc<str>) -> Option<usize> {
        size_of::<Arc<str>>().checked_add(value.len())
    }
    fn batch_bytes(batch: &NativeBatch) -> Option<usize> {
        size_of::<NativeBatch>()
            .checked_add(2 * size_of::<usize>())?
            .checked_add(Self::text_bytes(&batch.source.stream)?)?
            .checked_add(Self::text_bytes(&batch.source.source)?)?
            .checked_add(
                (batch.events.capacity() - batch.events.len())
                    .checked_mul(size_of::<NativeEvent>())?,
            )
    }
    fn event_bytes(event: &NativeEvent, payload: usize) -> Option<usize> {
        let mut bytes = size_of::<NativeEvent>().checked_add(payload)?;
        for value in event
            .family_name
            .iter()
            .chain(event.market.iter())
            .chain(event.assets.iter())
        {
            bytes = bytes.checked_add(Self::text_bytes(value)?)?;
        }
        bytes = bytes.checked_add(event.assets.capacity().checked_mul(size_of::<Arc<str>>())?)?;
        match &event.identity {
            None => {}
            Some(NativeIdentity::Components(parts)) => {
                bytes = bytes.checked_add(parts.capacity().checked_mul(size_of::<Arc<str>>())?)?;
                for part in parts {
                    bytes = bytes.checked_add(Self::text_bytes(part)?)?;
                }
            }
            Some(NativeIdentity::PolymarketDelta { timestamp, entries }) => {
                bytes = bytes
                    .checked_add(Self::text_bytes(timestamp)?)?
                    .checked_add(
                        entries
                            .capacity()
                            .checked_mul(size_of::<(Arc<str>, Arc<str>)>())?,
                    )?;
                for (asset, hash) in entries {
                    bytes = bytes
                        .checked_add(Self::text_bytes(asset)?)?
                        .checked_add(Self::text_bytes(hash)?)?;
                }
            }
        }
        Some(bytes)
    }
    fn valid(event: &NativeEvent) -> bool {
        let venue = matches!(
            (event.venue, event.family),
            (
                NativeVenue::Limitless,
                NativeFamily::LimitlessOrderbookUpdate
                    | NativeFamily::LimitlessNewPriceData
                    | NativeFamily::LimitlessMarketCreated
                    | NativeFamily::LimitlessMarketResolved
                    | NativeFamily::LimitlessSystem
                    | NativeFamily::LimitlessException
                    | NativeFamily::Unknown
            ) | (
                NativeVenue::Polymarket,
                NativeFamily::PolymarketBook
                    | NativeFamily::PolymarketPriceChange
                    | NativeFamily::PolymarketLastTradePrice
                    | NativeFamily::PolymarketTickSizeChange
                    | NativeFamily::PolymarketBestBidAsk
                    | NativeFamily::PolymarketNewMarket
                    | NativeFamily::PolymarketMarketResolved
                    | NativeFamily::PolymarketPongControl
                    | NativeFamily::Unknown
            )
        );
        if !venue || (event.family == NativeFamily::Unknown) != event.family_name.is_some() {
            return false;
        }
        match event.family {
            NativeFamily::LimitlessOrderbookUpdate => {
                event.market.is_some()
                    && event.assets.len() <= 1
                    && matches!(&event.identity, Some(NativeIdentity::Components(parts)) if parts.len() == 2)
            }
            NativeFamily::PolymarketBook => {
                event.market.is_some()
                    && event.assets.len() == 1
                    && matches!(&event.identity, Some(NativeIdentity::Components(parts)) if parts.len() == 2)
            }
            NativeFamily::PolymarketPriceChange => {
                event.market.is_some()
                    && matches!(&event.identity, Some(NativeIdentity::PolymarketDelta { timestamp, .. }) if !timestamp.is_empty())
            }
            _ => event.identity.is_none(),
        }
    }
    /// Validates a full batch before publication. Equal or repeated native identities are independent arrivals.
    pub fn admit(&mut self, batch: NativeBatch) -> GateDecision {
        let count = batch.events.len() as u64;
        self.counters.received = self.counters.received.saturating_add(count);
        if batch.input_bytes > self.limits.max_input_bytes
            || batch.events.len() > self.limits.max_events
        {
            self.counters.overload = self.counters.overload.saturating_add(count);
            return self.fault(GateFault::Capacity);
        }
        if batch.source.stream_generation != self.generation {
            self.counters.stale = self.counters.stale.saturating_add(count);
            return self.fault(GateFault::StaleSource);
        }
        let Some(mut bytes) = Self::batch_bytes(&batch) else {
            return self.fault(GateFault::Capacity);
        };
        if bytes > self.limits.bytes {
            self.counters.overload = self.counters.overload.saturating_add(count);
            return self.fault(GateFault::Capacity);
        }
        let mut documents = HashSet::new();
        for event in &batch.events {
            if !Self::valid(event) {
                return self.fault(GateFault::InvalidEvent);
            }
            let Some((_, payload_bytes)) = event
                .payload
                .bounded_measure(self.limits.max_value_nodes, self.limits.max_value_depth)
            else {
                self.counters.overload = self.counters.overload.saturating_add(count);
                return self.fault(GateFault::Capacity);
            };
            let payload_bytes = if batch.events.len() == 1
                || documents.insert(Arc::as_ptr(&event.payload.document))
            {
                payload_bytes
            } else {
                0
            };
            let Some(event_bytes) = Self::event_bytes(event, payload_bytes) else {
                self.counters.overload = self.counters.overload.saturating_add(count);
                return self.fault(GateFault::Capacity);
            };
            let Some(next) = bytes.checked_add(event_bytes) else {
                self.counters.overload = self.counters.overload.saturating_add(count);
                return self.fault(GateFault::Capacity);
            };
            bytes = next;
            if bytes > self.limits.bytes {
                self.counters.overload = self.counters.overload.saturating_add(count);
                return self.fault(GateFault::Capacity);
            }
        }
        self.counters.admitted = self.counters.admitted.saturating_add(count);
        GateDecision::Admit(Arc::new(batch))
    }
}

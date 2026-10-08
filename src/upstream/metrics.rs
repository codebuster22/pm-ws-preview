//! Metrics-only observation of complete admitted events; no source payload is retained or hashed.

use crate::native::{NativeBatch, NativeFamily, NativeVenue};
use serde::Serialize;

const BUCKETS: usize = 1_024;
const STAGES: usize = 6;
const FAMILIES: usize = 15;

/// Same-event upstream intervals, in nanoseconds.
#[derive(Clone, Copy)]
pub struct Timings {
    pub decode: u64,
    pub validate: u64,
    pub gate: u64,
    pub receiver: u64,
    pub total: u64,
    pub audited_total: u64,
    pub cpu: Option<CpuTimings>,
}

#[derive(Clone, Copy)]
enum CpuSpan {
    Measured(u64),
    Missing,
    Nonmonotonic,
}

#[derive(Clone, Copy)]
/// Same-thread CPU intervals in nanoseconds; missing or regressing clocks remain unknown.
pub struct CpuTimings {
    spans: [CpuSpan; STAGES],
    missing_reads: u64,
    nonmonotonic_spans: u64,
}

impl CpuTimings {
    /// Partitions five ordered stage boundaries without clamping CPU duration to wall time.
    /// Counts missing reads once per boundary and backward differences once per affected span.
    pub fn from_stamps(
        received: Option<u64>,
        decoded: Option<u64>,
        gate_start: Option<u64>,
        observed: Option<u64>,
        audited: Option<u64>,
    ) -> Self {
        let span = |start: Option<u64>, end: Option<u64>| match (start, end) {
            (Some(start), Some(end)) => end
                .checked_sub(start)
                .map_or(CpuSpan::Nonmonotonic, CpuSpan::Measured),
            _ => CpuSpan::Missing,
        };
        let spans = [
            span(received, decoded),
            span(decoded, gate_start),
            span(gate_start, observed),
            span(observed, audited),
            span(received, observed),
            span(received, audited),
        ];
        Self {
            missing_reads: [received, decoded, gate_start, observed, audited]
                .into_iter()
                .filter(|value| value.is_none())
                .count() as u64,
            nonmonotonic_spans: spans
                .iter()
                .filter(|value| matches!(value, CpuSpan::Nonmonotonic))
                .count() as u64,
            spans,
        }
    }
}

/// Disjoint 1 us buckets through 1 ms, then doubling buckets and an overflow bucket.
#[derive(Clone, Debug, Serialize)]
pub struct Distribution {
    pub count: u64,
    pub buckets: Vec<u64>,
    pub max_ns: u64,
    pub above_100us: u64,
    pub above_250us: u64,
    pub above_1ms: u64,
}
impl Default for Distribution {
    fn default() -> Self {
        Self {
            count: 0,
            buckets: vec![0; BUCKETS],
            max_ns: 0,
            above_100us: 0,
            above_250us: 0,
            above_1ms: 0,
        }
    }
}
impl Distribution {
    fn record(&mut self, ns: u64) {
        let index = if ns <= 1_000_000 {
            ns.saturating_sub(1) as usize / 1_000
        } else {
            (999 + (u64::BITS - ((ns - 1) / 1_000_000).leading_zeros()) as usize).min(BUCKETS - 1)
        };
        self.count = self.count.saturating_add(1);
        self.buckets[index] = self.buckets[index].saturating_add(1);
        self.max_ns = self.max_ns.max(ns);
        self.above_100us += u64::from(ns > 100_000);
        self.above_250us += u64::from(ns > 250_000);
        self.above_1ms += u64::from(ns > 1_000_000);
    }
}

/// Full distributions for one bounded venue family.
#[derive(Clone, Serialize)]
pub struct FamilyMetrics {
    pub family: String,
    pub stages: [Distribution; STAGES],
}

/// Numeric correlation witness for one admitted application batch; contains no source fields.
#[derive(Clone, Serialize)]
pub struct TailWitness {
    pub sequence: u64,
    pub source_slot: u16,
    pub source_generation: u64,
    pub stream_generation: u64,
    pub receive_ns: u64,
    pub decode_ns: u64,
    pub validate_ns: u64,
    pub gate_ns: u64,
    pub receiver_ns: u64,
    pub total_ns: u64,
    pub audited_total_ns: u64,
    pub event_count: usize,
    pub input_bytes: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_decode_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_validate_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_gate_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_receiver_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_total_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_audited_total_ns: Option<u64>,
}

/// Bounded in-memory observation of delivery coordinates, independent of event identities.
pub struct ReceiverMetrics {
    families: [FamilyMetrics; FAMILIES],
    pub events: u64,
    pub activity_events: u64,
    pub batches: u64,
    pub last_sequence: u64,
    pub sequence_errors: u64,
    pub generation_errors: u64,
    pub member_order_errors: u64,
    tails: Vec<TailWitness>,
    pub tails_omitted: u64,
    cpu_missing_reads: u64,
    cpu_nonmonotonic_spans: u64,
}

/// Stable labels for the bounded native vocabulary.
pub fn family_name(family: NativeFamily) -> &'static str {
    match family {
        NativeFamily::LimitlessOrderbookUpdate => "orderbookUpdate",
        NativeFamily::LimitlessNewPriceData => "newPriceData",
        NativeFamily::LimitlessMarketCreated => "marketCreated",
        NativeFamily::LimitlessMarketResolved => "marketResolved",
        NativeFamily::LimitlessSystem => "system",
        NativeFamily::LimitlessException => "exception",
        NativeFamily::PolymarketBook => "book",
        NativeFamily::PolymarketPriceChange => "price_change",
        NativeFamily::PolymarketLastTradePrice => "last_trade_price",
        NativeFamily::PolymarketTickSizeChange => "tick_size_change",
        NativeFamily::PolymarketBestBidAsk => "best_bid_ask",
        NativeFamily::PolymarketNewMarket => "new_market",
        NativeFamily::PolymarketMarketResolved => "market_resolved",
        NativeFamily::PolymarketPongControl => "pong_control",
        NativeFamily::Unknown => "unknown",
    }
}
const ALL_FAMILIES: [NativeFamily; FAMILIES] = [
    NativeFamily::LimitlessOrderbookUpdate,
    NativeFamily::LimitlessNewPriceData,
    NativeFamily::LimitlessMarketCreated,
    NativeFamily::LimitlessMarketResolved,
    NativeFamily::LimitlessSystem,
    NativeFamily::LimitlessException,
    NativeFamily::PolymarketBook,
    NativeFamily::PolymarketPriceChange,
    NativeFamily::PolymarketLastTradePrice,
    NativeFamily::PolymarketTickSizeChange,
    NativeFamily::PolymarketBestBidAsk,
    NativeFamily::PolymarketNewMarket,
    NativeFamily::PolymarketMarketResolved,
    NativeFamily::PolymarketPongControl,
    NativeFamily::Unknown,
];
pub(super) fn family_index(family: NativeFamily) -> usize {
    ALL_FAMILIES
        .iter()
        .position(|value| *value == family)
        .expect("bounded family")
}

impl ReceiverMetrics {
    /// Allocates fixed histograms and a fixed-capacity tail witness buffer.
    pub fn new() -> Self {
        Self {
            families: std::array::from_fn(|i| FamilyMetrics {
                family: family_name(ALL_FAMILIES[i]).into(),
                stages: std::array::from_fn(|_| Distribution::default()),
            }),
            events: 0,
            activity_events: 0,
            batches: 0,
            last_sequence: 0,
            sequence_errors: 0,
            generation_errors: 0,
            member_order_errors: 0,
            tails: Vec::with_capacity(256),
            tails_omitted: 0,
            cpu_missing_reads: 0,
            cpu_nonmonotonic_spans: 0,
        }
    }
    /// Audits delivery coordinates only. Repeated native identities and equal payloads are valid arrivals.
    pub fn observe(&mut self, batch: &NativeBatch) {
        if self.batches != 0 && batch.source.sequence != self.last_sequence.saturating_add(1) {
            self.sequence_errors = self.sequence_errors.saturating_add(1);
        }
        if batch.source.generation != batch.source.stream_generation {
            self.generation_errors = self.generation_errors.saturating_add(1);
        }
        self.last_sequence = batch.source.sequence;
        self.batches = self.batches.saturating_add(1);
        for (expected_member_index, event) in batch.events.iter().enumerate() {
            self.events = self.events.saturating_add(1);
            self.activity_events += u64::from(matches!(
                event.family,
                NativeFamily::LimitlessOrderbookUpdate
                    | NativeFamily::PolymarketBook
                    | NativeFamily::PolymarketPriceChange
            ));
            if event.member_index != expected_member_index {
                self.member_order_errors = self.member_order_errors.saturating_add(1);
            }
        }
    }
    /// Records paired stages only after complete observation, separating observer-audit cost from handoff.
    pub fn record(&mut self, batch: &NativeBatch, timings: Timings) {
        let intervals = [
            timings.decode,
            timings.validate,
            timings.gate,
            timings.receiver,
            timings.total,
            timings.audited_total,
        ];
        for event in &batch.events {
            for (stage, ns) in self.families[family_index(event.family)]
                .stages
                .iter_mut()
                .zip(intervals)
            {
                stage.record(ns);
            }
        }
        let cpu = timings.cpu.map(|timings| {
            let mut values = [None; STAGES];
            self.cpu_missing_reads = self.cpu_missing_reads.saturating_add(timings.missing_reads);
            self.cpu_nonmonotonic_spans = self
                .cpu_nonmonotonic_spans
                .saturating_add(timings.nonmonotonic_spans);
            for (index, span) in timings.spans.into_iter().enumerate() {
                match span {
                    CpuSpan::Measured(value) => values[index] = Some(value),
                    CpuSpan::Missing | CpuSpan::Nonmonotonic => {}
                }
            }
            values
        });
        if timings.audited_total > 1_000_000 {
            if self.tails.len() == 256 {
                self.tails_omitted = self.tails_omitted.saturating_add(1);
            } else {
                self.tails.push(TailWitness {
                    sequence: batch.source.sequence,
                    source_slot: batch.source.slot,
                    source_generation: batch.source.generation,
                    stream_generation: batch.source.stream_generation,
                    receive_ns: batch.source.received_ns,
                    decode_ns: timings.decode,
                    validate_ns: timings.validate,
                    gate_ns: timings.gate,
                    receiver_ns: timings.receiver,
                    total_ns: timings.total,
                    audited_total_ns: timings.audited_total,
                    event_count: batch.events.len(),
                    input_bytes: batch.input_bytes,
                    cpu_decode_ns: cpu.and_then(|values| values[0]),
                    cpu_validate_ns: cpu.and_then(|values| values[1]),
                    cpu_gate_ns: cpu.and_then(|values| values[2]),
                    cpu_receiver_ns: cpu.and_then(|values| values[3]),
                    cpu_total_ns: cpu.and_then(|values| values[4]),
                    cpu_audited_total_ns: cpu.and_then(|values| values[5]),
                });
            }
        }
    }
    /// Copies metrics for terminal, off-path serialization; no payload can enter this type.
    pub fn snapshot(&self, venue: NativeVenue) -> ReceiverReport {
        ReceiverReport {
            families: self
                .families
                .iter()
                .enumerate()
                .filter(|(index, _)| match venue {
                    NativeVenue::Limitless => *index < 6 || *index == 14,
                    NativeVenue::Polymarket => *index >= 6,
                })
                .map(|(_, family)| family.clone())
                .collect(),
            events: self.events,
            activity_events: self.activity_events,
            batches: self.batches,
            last_sequence: self.last_sequence,
            sequence_errors: self.sequence_errors,
            generation_errors: self.generation_errors,
            member_order_errors: self.member_order_errors,
            tails: self.tails.clone(),
            tails_omitted: self.tails_omitted,
            cpu_missing_reads: self.cpu_missing_reads,
            cpu_nonmonotonic_spans: self.cpu_nonmonotonic_spans,
        }
    }
}

/// Serializable metrics-only receiver result.
#[derive(Clone, Serialize)]
pub struct ReceiverReport {
    pub families: Vec<FamilyMetrics>,
    pub events: u64,
    pub activity_events: u64,
    pub batches: u64,
    pub last_sequence: u64,
    pub sequence_errors: u64,
    pub generation_errors: u64,
    pub member_order_errors: u64,
    pub tails: Vec<TailWitness>,
    pub tails_omitted: u64,
    #[serde(skip)]
    pub cpu_missing_reads: u64,
    #[serde(skip)]
    pub cpu_nonmonotonic_spans: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::{NativeEvent, NativePayload, NativeSource};
    use std::sync::Arc;
    #[test]
    fn histogram_boundaries_and_thresholds() {
        let mut h = Distribution::default();
        for ns in [
            0,
            100_000,
            100_001,
            250_000,
            250_001,
            1_000_000,
            1_000_001,
            u64::MAX,
        ] {
            h.record(ns);
        }
        assert_eq!(h.buckets.iter().sum::<u64>(), 8);
        assert_eq!((h.above_100us, h.above_250us, h.above_1ms), (6, 4, 2));
        assert_eq!(h.buckets[1000], 1);
    }
    fn batch(sequence: u64, generation: u64, member_index: usize) -> NativeBatch {
        NativeBatch {
            source: NativeSource {
                stream: Arc::from("stream"),
                source: Arc::from("source"),
                slot: 1,
                generation,
                stream_generation: generation,
                sequence,
                received_ns: 1,
                validated_ns: 2,
            },
            input_bytes: 1,
            events: vec![NativeEvent {
                venue: NativeVenue::Limitless,
                family: NativeFamily::LimitlessSystem,
                family_name: None,
                market: None,
                assets: vec![],
                identity: None,
                payload: NativePayload::from_document(
                    crate::native::document::NativeDocument::parse(
                        b"null",
                        crate::wire::lexical::LexicalLimits::venue_payload(),
                        crate::DecimalGrammar::new(18, 30, false, false).unwrap(),
                    )
                    .unwrap(),
                ),
                member_index,
            }],
        }
    }
    #[test]
    fn repeat_arrivals_are_observed_without_identity_faults() {
        let mut receiver = ReceiverMetrics::new();
        let first = batch(1, 1, 0);
        let repeated = batch(2, 1, 0);
        receiver.observe(&first);
        receiver.observe(&repeated);
        let report = receiver.snapshot(NativeVenue::Limitless);
        assert_eq!(
            (
                report.events,
                report.sequence_errors,
                report.generation_errors,
                report.member_order_errors
            ),
            (2, 0, 0, 0)
        );
    }
    #[test]
    fn audited_tail_is_visible_when_typed_handoff_is_fast() {
        let batch = batch(1, 1, 0);
        let mut receiver = ReceiverMetrics::new();
        receiver.observe(&batch);
        receiver.record(
            &batch,
            Timings {
                decode: 20_000,
                validate: 20_000,
                gate: 10_000,
                receiver: 2_000_000,
                total: 50_000,
                audited_total: 2_050_000,
                cpu: None,
            },
        );
        let report = receiver.snapshot(NativeVenue::Limitless);
        assert_eq!(report.families[4].stages[4].above_1ms, 0);
        assert_eq!(report.families[4].stages[5].above_1ms, 1);
        assert_eq!(report.tails.len(), 1);
        assert_eq!(report.tails[0].audited_total_ns, 2_050_000);
        assert!(report.tails[0].cpu_decode_ns.is_none());
    }

    #[test]
    fn cpu_spans_partition_without_clamping_or_synthetic_zeroes() {
        let timings = CpuTimings::from_stamps(Some(10), Some(23), Some(42), Some(71), Some(101));
        assert!(matches!(
            timings.spans,
            [
                CpuSpan::Measured(13),
                CpuSpan::Measured(19),
                CpuSpan::Measured(29),
                CpuSpan::Measured(30),
                CpuSpan::Measured(61),
                CpuSpan::Measured(91),
            ]
        ));
        let missing = CpuTimings::from_stamps(Some(10), None, Some(42), Some(71), Some(101));
        assert_eq!(missing.missing_reads, 1);
        assert!(matches!(missing.spans[0], CpuSpan::Missing));
        assert!(matches!(missing.spans[1], CpuSpan::Missing));
        let nonmonotonic =
            CpuTimings::from_stamps(Some(10), Some(9), Some(42), Some(71), Some(101));
        assert_eq!(nonmonotonic.missing_reads, 0);
        assert_eq!(nonmonotonic.nonmonotonic_spans, 1);
        assert!(matches!(nonmonotonic.spans[0], CpuSpan::Nonmonotonic));
    }

    #[test]
    fn cpu_tail_marks_missing_and_nonmonotonic_evidence_unknown() {
        let batch = batch(1, 1, 0);
        let mut receiver = ReceiverMetrics::new();
        receiver.record(
            &batch,
            Timings {
                decode: 1,
                validate: 1,
                gate: 1,
                receiver: 1,
                total: 3,
                audited_total: 1_000_001,
                cpu: Some(CpuTimings::from_stamps(
                    Some(10),
                    None,
                    Some(20),
                    Some(12),
                    Some(15),
                )),
            },
        );
        let report = receiver.snapshot(NativeVenue::Limitless);
        assert_eq!(report.cpu_missing_reads, 1);
        assert_eq!(report.cpu_nonmonotonic_spans, 1);
        assert!(report.tails[0].cpu_decode_ns.is_none());
        assert!(report.tails[0].cpu_validate_ns.is_none());
        assert!(report.tails[0].cpu_gate_ns.is_none());
        assert_eq!(report.tails[0].cpu_receiver_ns, Some(3));
    }
}

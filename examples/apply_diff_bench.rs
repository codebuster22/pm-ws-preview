//! Measures `OrderBook::apply_snapshot` — commit plus snapshot diff — by alternating the
//! observed 30-level Limitless book with a one-level variant of it.
//!
//! Reports the per-apply distribution in nanoseconds. Publication and observer delivery are
//! excluded; this is the writer's critical section only.

use pm_ws::limitless::{LimitlessEvent, decode_event};
use pm_ws::wire::lexical::LexicalLimits;
use pm_ws::wire::socketio::{WebSocketOpcode, decode_frame};
use pm_ws::*;
use std::time::Instant;

const OBSERVED_ORDERBOOK_UPDATE: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"eth-up-or-down-daily-1788105600","orderbook":{"bids":[{"price":0.012,"size":83334000,"side":"BUY"},{"price":0.011,"size":100000000,"side":"BUY"},{"price":0.01,"size":21000000,"side":"BUY"},{"price":0.009,"size":1000000,"side":"BUY"},{"price":0.008,"size":1000000,"side":"BUY"},{"price":0.007,"size":1000000,"side":"BUY"},{"price":0.006,"size":167000000,"side":"BUY"},{"price":0.005,"size":1201000000,"side":"BUY"},{"price":0.002,"size":50000000,"side":"BUY"},{"price":0.001,"size":2000000000,"side":"BUY"}],"asks":[{"price":0.219,"size":100000000,"side":"SELL"},{"price":0.22,"size":12000000,"side":"SELL"},{"price":0.239,"size":100000000,"side":"SELL"},{"price":0.249,"size":100000000,"side":"SELL"},{"price":0.259,"size":100000000,"side":"SELL"},{"price":0.27,"size":5000000,"side":"SELL"},{"price":0.279,"size":100000000,"side":"SELL"},{"price":0.293,"size":100000000,"side":"SELL"},{"price":0.306,"size":1441000,"side":"SELL"},{"price":0.65,"size":185714000,"side":"SELL"},{"price":0.969,"size":50000000,"side":"SELL"},{"price":0.989,"size":100000000,"side":"SELL"},{"price":0.99,"size":21000000,"side":"SELL"},{"price":0.991,"size":1000000,"side":"SELL"},{"price":0.992,"size":1000000,"side":"SELL"},{"price":0.993,"size":1000000,"side":"SELL"},{"price":0.994,"size":1000000,"side":"SELL"},{"price":0.995,"size":1000000,"side":"SELL"},{"price":0.998,"size":550000000,"side":"SELL"},{"price":0.999,"size":2000000000,"side":"SELL"}],"tokenId":"25018063611559838047404811982184442876005199660833597814711111046007291893507","adjustedMidpoint":0.115,"midpoint":0.1155,"maxSpread":0.035,"minSize":100000000},"version":7861372,"timestamp":"2026-08-31T07:12:32.741Z"}]"#;
const ITERATIONS: usize = 10_000;

fn provenance(market: MarketRef, position: u64) -> Provenance {
    Provenance::new(ProvenanceInput {
        market,
        outcome: Some(NativeOutcome::side("yes").expect("outcome")),
        native_family: "orderbookUpdate".into(),
        source_timestamp: Some(SourceTimestamp::new("2026-08-31T07:12:32.741Z").expect("stamp")),
        source_evidence: BoundedSourceEvidence::new(
            [],
            SourceEvidenceCapacity::new(0).expect("capacity"),
        )
        .expect("evidence"),
        daemon_generation: 1,
        connection: ConnectionIdentity::new("ws.limitless.exchange", 1).expect("connection"),
        subscription_generation: 1,
        receive_position: position,
        commit_position: position,
        local_receive_time: LocalMonotonicTimestamp::new(position),
        local_commit_time: LocalMonotonicTimestamp::new(position),
        replica: ReplicaRole::PublishingPrimary,
        representation: Representation::VenueNative,
        origin: Origin::SourceReported,
        local_revision: 0,
        continuity_epoch: 0,
    })
    .expect("provenance")
}

fn percentile(sorted: &[u64], fraction: f64) -> u64 {
    let index = ((sorted.len() as f64 - 1.0) * fraction).round() as usize;
    sorted[index]
}

fn main() {
    let frame = decode_frame(
        OBSERVED_ORDERBOOK_UPDATE.as_bytes(),
        WebSocketOpcode::Text,
        LexicalLimits::venue_payload(),
    )
    .expect("observed frame decodes");
    let LimitlessEvent::OrderbookUpdate(update) = decode_event(&frame).expect("event decodes")
    else {
        panic!("the observed frame carries an orderbookUpdate");
    };
    let market = MarketRef::new(
        Venue::new("limitless").expect("venue"),
        NativeMarketKey::new(NativeIdentifierKind::slug(), update.market_slug()).expect("key"),
    );
    let capacity = LevelCapacity::new(64).expect("level capacity");
    let base = update
        .snapshot_candidate(provenance(market.clone(), 1), capacity)
        .expect("snapshot candidate");
    let mut levels = update
        .bids()
        .iter()
        .map(|(price, size)| Level::new(Side::Bid, price.clone(), size.clone()))
        .chain(
            update
                .asks()
                .iter()
                .map(|(price, size)| Level::new(Side::Ask, price.clone(), size.clone())),
        )
        .collect::<Vec<Level>>();
    let grammar = DecimalGrammar::new(18, 30, true, false).expect("grammar");
    levels[0] = Level::new(
        Side::Bid,
        levels[0].price().clone(),
        Quantity::parse("90000000", grammar).expect("variant size"),
    );
    let variant = Candidate::snapshot(
        provenance(market.clone(), 2),
        BoundedLevels::new(levels, capacity).expect("bounded levels"),
    )
    .expect("variant candidate");

    let mut book = OrderBook::new(market);
    let mut samples = Vec::with_capacity(ITERATIONS);
    let mut mutations = 0usize;
    for iteration in 0..ITERATIONS {
        let candidate = if iteration % 2 == 0 { &base } else { &variant };
        let started = Instant::now();
        let commit = book.apply_snapshot(candidate).expect("apply");
        samples.push(started.elapsed().as_nanos() as u64);
        mutations += commit.mutations().len();
    }
    samples.sort_unstable();
    println!(
        "apply+diff count={} mutations={} p50={}ns p99={}ns p99.9={}ns max={}ns",
        samples.len(),
        mutations,
        percentile(&samples, 0.50),
        percentile(&samples, 0.99),
        percentile(&samples, 0.999),
        samples.last().copied().unwrap_or_default(),
    );
}

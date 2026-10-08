//! Embedded Rust consumer mode: `BookWriter`/`BookObserver` driven directly in-process — no
//! venue traffic, no shared memory, an observer packaged as a standalone surface.
//!
//! Deterministic replay of the snapshot/delta/sync rhythm, with one extra snapshot
//! before the first delta so both mutation origins — venue-reported and locally derived —
//! are exercised and printed distinguishably. Every step's expectation is asserted; a
//! violated one panics with the failing assertion rather than printing a wrong answer. This
//! is a proof command, not a demo.
//!
//! Run: `cargo run --example embedded`

use pm_ws::*;

const RING_CAPACITY: usize = 4;
const BURST_DELTAS: u64 = 12;

fn market() -> MarketRef {
    MarketRef::new(
        Venue::new("limitless").unwrap(),
        NativeMarketKey::new(
            NativeIdentifierKind::slug(),
            "btc-up-or-down-daily-1788105600",
        )
        .unwrap(),
    )
}

fn grammar() -> DecimalGrammar {
    DecimalGrammar::new(18, 30, true, false).unwrap()
}

fn price(lexeme: &str) -> Price {
    Price::parse(lexeme, grammar()).unwrap()
}

fn quantity(lexeme: &str) -> Quantity {
    Quantity::parse(lexeme, grammar()).unwrap()
}

fn level(side: Side, price_lexeme: &str, quantity_lexeme: &str) -> Level {
    Level::new(side, price(price_lexeme), quantity(quantity_lexeme))
}

fn provenance(family: &str, position: u64) -> Provenance {
    Provenance::new(ProvenanceInput {
        market: market(),
        outcome: Some(NativeOutcome::side("yes").unwrap()),
        native_family: family.into(),
        source_timestamp: Some(SourceTimestamp::new("2026-09-01T00:00:00.000Z").unwrap()),
        source_evidence: BoundedSourceEvidence::new(
            [SourceEvidence::sequence(position.to_string()).unwrap()],
            SourceEvidenceCapacity::new(1).unwrap(),
        )
        .unwrap(),
        daemon_generation: 1,
        connection: ConnectionIdentity::new("embedded.example", 1).unwrap(),
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
    .unwrap()
}

fn bounded(levels: Vec<Level>) -> BoundedLevels {
    let capacity = LevelCapacity::new(levels.len().max(1)).unwrap();
    BoundedLevels::new(levels, capacity).unwrap()
}

fn snapshot(levels: Vec<Level>, position: u64) -> Candidate {
    Candidate::snapshot(provenance("book", position), bounded(levels)).unwrap()
}

fn delta(levels: Vec<Level>, position: u64) -> Candidate {
    Candidate::source_delta(provenance("price_change", position), bounded(levels)).unwrap()
}

/// What this attachment actually saw. `source_reported` and `derived` count mutations the
/// observer delivered, never ones a commit produced: a mutation fenced by a rebase or
/// dropped by an overrun is real on the writer's side but never reaches this consumer, so it
/// is not counted here either.
#[derive(Default)]
struct Tally {
    source_reported: u64,
    derived: u64,
    continuity_losses: u64,
}

fn print_attach(observer: &BookObserver) {
    let ConsumerState::Attached { cursor, .. } = observer.state() else {
        panic!("a fresh attachment is always Attached");
    };
    println!(
        "attach revision={} epoch={} position={}",
        observer.latest().revision(),
        cursor.epoch(),
        cursor.position()
    );
}

fn render_qty(level: Option<&Level>) -> String {
    level.map_or_else(
        || "none".to_owned(),
        |level| level.quantity().value().canonical(),
    )
}

fn print_resolution(delivery: &ResolutionDelivery) {
    let resolution = delivery.resolution();
    println!(
        "resolution revision={} cursor={}:{} winner={} index={} type={} date={}",
        delivery.revision(),
        delivery.cursor().epoch(),
        delivery.cursor().position(),
        resolution.winner().text_value().unwrap_or(""),
        resolution.winning_index(),
        resolution.native_label().as_str(),
        resolution.resolution_date().as_lexeme(),
    );
}

fn print_mutation(delivery: &MutationDelivery, tally: &mut Tally) {
    let mutation = delivery.mutation();
    let derived = matches!(mutation.provenance().origin(), Origin::LocallyDerived(_));
    let origin = if derived {
        "LocallyDerived(SnapshotDiff)"
    } else {
        "SourceReported"
    };
    if derived {
        tally.derived += 1;
    } else {
        tally.source_reported += 1;
    }
    let anchor = mutation
        .replacement()
        .or(mutation.old())
        .expect("at least one side");
    println!(
        "mutation revision={} cursor={}:{} origin={origin} side={:?} price={} qty={}->{}",
        delivery.revision(),
        delivery.cursor().epoch(),
        delivery.cursor().position(),
        anchor.side(),
        anchor.price().value().canonical(),
        render_qty(mutation.old()),
        render_qty(mutation.replacement()),
    );
}

/// Drains every mutation currently in the ring, printing each, until the ring is empty or a
/// continuity loss is found.
fn drain(observer: &mut BookObserver, tally: &mut Tally) -> Result<(), ObserverRecvError> {
    loop {
        match observer.try_recv()? {
            Some(StreamDelivery::Mutation(delivery)) => print_mutation(&delivery, tally),
            Some(StreamDelivery::Resolution(delivery)) => print_resolution(&delivery),
            None => return Ok(()),
        }
    }
}

fn best(levels: &[Level], side: Side) -> Option<&Level> {
    let mut matching = levels.iter().filter(|level| level.side() == side);
    match side {
        Side::Bid => matching.next_back(),
        Side::Ask => matching.next(),
    }
}

fn print_bbo(published: &PublishedBook) {
    let render = |level: Option<&Level>| {
        level.map_or_else(
            || "-".to_owned(),
            |level| {
                format!(
                    "{}@{}",
                    level.price().value().canonical(),
                    level.quantity().value().canonical()
                )
            },
        )
    };
    println!(
        "bbo revision={} epoch={} authority={:?} best_bid={} best_ask={}",
        published.revision(),
        published.continuity().epoch(),
        published.authority(),
        render(best(published.canonical_levels(), Side::Bid)),
        render(best(published.canonical_levels(), Side::Ask)),
    );
}

/// Prints the loss, reattaches, and prints the state the attachment resumed from — the
/// documented answer to every `ObserverRecvError::ContinuityLost`.
fn handle_loss(
    observer: &mut BookObserver,
    error: ObserverRecvError,
    expected: ContinuityReason,
    tally: &mut Tally,
) {
    let ObserverRecvError::ContinuityLost { reason, missed } = error else {
        panic!("embedded mode never closes its own writer mid-script");
    };
    assert_eq!(reason, expected, "wrong continuity-loss reason");
    match expected {
        ContinuityReason::RecoveryBase => assert_eq!(missed, 0, "no overrun was pending"),
        ContinuityReason::Overrun => assert!(missed > 0, "an overrun states what it dropped"),
        _ => unreachable!("this script raises only RecoveryBase and Overrun"),
    }
    tally.continuity_losses += 1;
    println!("book continuity_lost reason={reason:?} missed={missed}");
    let resumed = observer.reattach();
    println!("reattach revision={}", resumed.revision());
    print_bbo(&resumed);
}

fn main() {
    let mut tally = Tally::default();
    let mut seq = 0u64;
    let mut next = || {
        seq += 1;
        seq
    };

    let mut writer = BookWriter::new(
        OrderBook::new(market()),
        ObserverCapacity::new(RING_CAPACITY).unwrap(),
    );
    let mut observer = writer.attach();
    print_attach(&observer);

    let base = vec![
        level(Side::Bid, "0.39", "50"),
        level(Side::Bid, "0.40", "100"),
        level(Side::Ask, "0.60", "200"),
        level(Side::Ask, "0.61", "75"),
    ];
    let established = writer.apply_snapshot(&snapshot(base, next())).unwrap();
    assert!(
        established.mutations().is_empty(),
        "establishes, not a transition"
    );
    drain(&mut observer, &mut tally).unwrap();
    print_bbo(&observer.latest());

    let widened = vec![
        level(Side::Bid, "0.39", "50"),
        level(Side::Bid, "0.40", "120"),
        level(Side::Ask, "0.60", "200"),
        level(Side::Ask, "0.61", "90"),
    ];
    let widened_commit = writer.apply_snapshot(&snapshot(widened, next())).unwrap();
    assert_eq!(
        widened_commit.mutations().len(),
        2,
        "two coordinates changed"
    );
    drain(&mut observer, &mut tally).unwrap();
    print_bbo(&observer.latest());
    assert_eq!(tally.derived, 2, "no delta yet: both diffs are derived");
    assert!(!writer.book().delta_rail());

    for levels in [
        vec![level(Side::Bid, "0.40", "130")],
        vec![level(Side::Ask, "0.61", "0")],
        vec![level(Side::Bid, "0.38", "30")],
    ] {
        let commit = writer.apply_source_delta(&delta(levels, next())).unwrap();
        assert_eq!(commit.mutations().len(), 1, "one coordinate per delta");
        drain(&mut observer, &mut tally).unwrap();
        print_bbo(&observer.latest());
    }
    assert_eq!(
        tally.source_reported, 3,
        "the tally counts observer-delivered mutations only"
    );
    assert!(writer.book().delta_rail());
    assert_eq!(writer.book().deltas_since_snapshot(), 3);

    let agreeing = vec![
        level(Side::Bid, "0.380", "30.0"),
        level(Side::Bid, "0.39", "50"),
        level(Side::Bid, "0.400", "130"),
        level(Side::Ask, "0.6", "200.00"),
    ];
    let sync = writer.apply_snapshot(&snapshot(agreeing, next())).unwrap();
    assert!(sync.mutations().is_empty());
    assert!(!sync.recovery_base());
    assert!(sync.divergence().is_none());
    assert_eq!(sync.epoch(), 0);
    assert_eq!(
        observer.try_recv(),
        Ok(None),
        "agreeing checkpoint: zero mutations"
    );
    println!(
        "sync revision={} zero mutations, provenance refreshed",
        sync.revision()
    );
    print_bbo(&observer.latest());
    assert_eq!(writer.book().deltas_since_snapshot(), 0);

    let differs = vec![
        level(Side::Bid, "0.38", "30"),
        level(Side::Bid, "0.39", "50"),
        level(Side::Bid, "0.40", "130"),
        level(Side::Ask, "0.60", "250"),
    ];
    let commit = writer.apply_snapshot(&snapshot(differs, next())).unwrap();
    let divergence = commit
        .divergence()
        .expect("a real depth difference diverges");
    assert!(commit.recovery_base());
    assert!(
        commit.mutations().is_empty(),
        "no fabricated diff crosses the gap"
    );
    assert_eq!(commit.epoch(), 1);
    println!(
        "sync divergence deltas_invalidated={} revision={} epoch={}",
        divergence.deltas_invalidated(),
        commit.revision(),
        commit.epoch(),
    );
    assert_eq!(
        observer.try_recv(),
        Ok(None),
        "a recovery base emits no mutation"
    );
    print_bbo(&observer.latest());

    // A recovery base's zero mutations leave the observer nothing to fence on directly; the
    // rebase is only detectable on the next delivery it receives, whose epoch no longer
    // matches this attachment's boundary. One probe delta supplies that delivery.
    writer
        .apply_source_delta(&delta(vec![level(Side::Ask, "0.60", "260")], next()))
        .unwrap();
    let error = observer
        .try_recv()
        .expect_err("a new-epoch delivery fences the attachment");
    handle_loss(
        &mut observer,
        error,
        ContinuityReason::RecoveryBase,
        &mut tally,
    );
    assert!(matches!(
        observer.state(),
        ConsumerState::Attached {
            continuous: true,
            ..
        }
    ));

    println!("burst: {BURST_DELTAS} deltas fired without draining, deliberately coalesced");
    for step in 0..BURST_DELTAS {
        let levels = vec![level(Side::Ask, "0.60", &(261 + step).to_string())];
        writer.apply_source_delta(&delta(levels, next())).unwrap();
    }
    let error = observer
        .try_recv()
        .expect_err("a burst past a 4-slot ring must overrun");
    handle_loss(&mut observer, error, ContinuityReason::Overrun, &mut tally);
    assert_eq!(
        observer.try_recv(),
        Ok(None),
        "reattach cleared the overrun cleanly"
    );

    assert_eq!(tally.continuity_losses, 2);
    println!(
        "summary revisions={} source_reported={} derived={} continuity_losses={}",
        writer.book().revision(),
        tally.source_reported,
        tally.derived,
        tally.continuity_losses,
    );
}

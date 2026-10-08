use pm_ws::limitless::{LimitlessEvent, OrderbookUpdate, decode_event};
use pm_ws::wire::lexical::LexicalLimits;
use pm_ws::wire::socketio::{WebSocketOpcode, decode_frame};
use pm_ws::*;

/// The `orderbookUpdate` frame retained from the 2026-08-31 live connection to
/// `wss://ws.limitless.exchange`, reproduced byte for byte from `tests/wire_observed.rs`.
const OBSERVED_ORDERBOOK_UPDATE: &str = r#"42/markets,["orderbookUpdate",{"marketSlug":"eth-up-or-down-daily-1788105600","orderbook":{"bids":[{"price":0.012,"size":83334000,"side":"BUY"},{"price":0.011,"size":100000000,"side":"BUY"},{"price":0.01,"size":21000000,"side":"BUY"},{"price":0.009,"size":1000000,"side":"BUY"},{"price":0.008,"size":1000000,"side":"BUY"},{"price":0.007,"size":1000000,"side":"BUY"},{"price":0.006,"size":167000000,"side":"BUY"},{"price":0.005,"size":1201000000,"side":"BUY"},{"price":0.002,"size":50000000,"side":"BUY"},{"price":0.001,"size":2000000000,"side":"BUY"}],"asks":[{"price":0.219,"size":100000000,"side":"SELL"},{"price":0.22,"size":12000000,"side":"SELL"},{"price":0.239,"size":100000000,"side":"SELL"},{"price":0.249,"size":100000000,"side":"SELL"},{"price":0.259,"size":100000000,"side":"SELL"},{"price":0.27,"size":5000000,"side":"SELL"},{"price":0.279,"size":100000000,"side":"SELL"},{"price":0.293,"size":100000000,"side":"SELL"},{"price":0.306,"size":1441000,"side":"SELL"},{"price":0.65,"size":185714000,"side":"SELL"},{"price":0.969,"size":50000000,"side":"SELL"},{"price":0.989,"size":100000000,"side":"SELL"},{"price":0.99,"size":21000000,"side":"SELL"},{"price":0.991,"size":1000000,"side":"SELL"},{"price":0.992,"size":1000000,"side":"SELL"},{"price":0.993,"size":1000000,"side":"SELL"},{"price":0.994,"size":1000000,"side":"SELL"},{"price":0.995,"size":1000000,"side":"SELL"},{"price":0.998,"size":550000000,"side":"SELL"},{"price":0.999,"size":2000000000,"side":"SELL"}],"tokenId":"25018063611559838047404811982184442876005199660833597814711111046007291893507","adjustedMidpoint":0.115,"midpoint":0.1155,"maxSpread":0.035,"minSize":100000000},"version":7861372,"timestamp":"2026-08-31T07:12:32.741Z"}]"#;

const OBSERVED_SLUG: &str = "eth-up-or-down-daily-1788105600";
const OBSERVED_TIMESTAMP: &str = "2026-08-31T07:12:32.741Z";

fn market() -> MarketRef {
    MarketRef::new(
        Venue::new("limitless").unwrap(),
        NativeMarketKey::new(NativeIdentifierKind::slug(), OBSERVED_SLUG).unwrap(),
    )
}

fn grammar() -> DecimalGrammar {
    DecimalGrammar::new(18, 30, true, false).unwrap()
}

fn price(value: &str) -> Price {
    Price::parse(value, grammar()).unwrap()
}

fn quantity(value: &str) -> Quantity {
    Quantity::parse(value, grammar()).unwrap()
}

fn level(side: Side, price_lexeme: &str, quantity_lexeme: &str) -> Level {
    Level::new(side, price(price_lexeme), quantity(quantity_lexeme))
}

fn capacity() -> LevelCapacity {
    LevelCapacity::new(64).unwrap()
}

/// Venue-native provenance as the reader would supply it, deliberately carrying
/// `local_revision` and `continuity_epoch` of 0 so that any published value above 0 can
/// only have come from the book rebasing it.
fn provenance(timestamp: &str, position: u64) -> Provenance {
    Provenance::new(ProvenanceInput {
        market: market(),
        outcome: Some(NativeOutcome::side("yes").unwrap()),
        native_family: "orderbookUpdate".into(),
        source_timestamp: Some(SourceTimestamp::new(timestamp).unwrap()),
        source_evidence: BoundedSourceEvidence::new(
            [SourceEvidence::Version(
                SourceEvidenceValue::new("7861372").unwrap(),
            )],
            SourceEvidenceCapacity::new(1).unwrap(),
        )
        .unwrap(),
        daemon_generation: 1,
        connection: ConnectionIdentity::new("ws.limitless.exchange", 1).unwrap(),
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

fn snapshot(levels: Vec<Level>, timestamp: &str, position: u64) -> Candidate {
    Candidate::snapshot(
        provenance(timestamp, position),
        BoundedLevels::new(levels, capacity()).unwrap(),
    )
    .unwrap()
}

fn observed_update() -> OrderbookUpdate {
    let frame = decode_frame(
        OBSERVED_ORDERBOOK_UPDATE.as_bytes(),
        WebSocketOpcode::Text,
        LexicalLimits::venue_payload(),
    )
    .unwrap();
    let LimitlessEvent::OrderbookUpdate(update) = decode_event(&frame).unwrap() else {
        panic!("the observed frame carries an orderbookUpdate");
    };
    update
}

fn observed_book() -> (OrderBook, OrderbookUpdate) {
    let update = observed_update();
    let candidate = update
        .snapshot_candidate(provenance(update.timestamp(), 1), capacity())
        .unwrap();
    let mut book = OrderBook::new(market());
    book.apply_snapshot(&candidate).unwrap();
    (book, update)
}

fn total_depth(levels: &[Level]) -> Quantity {
    levels.iter().fold(quantity("0"), |total, level| {
        total.checked_add(level.quantity(), grammar()).unwrap()
    })
}

fn two_sided(bid: &str, ask: &str) -> Vec<Level> {
    vec![level(Side::Bid, "0.4", bid), level(Side::Ask, "0.6", ask)]
}

/// A quiet book stays authoritative. Elapsed time is not evidence: no input carrying only
/// a later local timestamp changes authority, and the only route to
/// [`AuthorityState::Stale`] is an explicit evidence-based report.
#[test]
fn book_quiet_interval_leaves_authority_live() {
    let mut book = OrderBook::new(market());
    book.apply_snapshot(&snapshot(two_sided("100", "200"), OBSERVED_TIMESTAMP, 1))
        .unwrap();
    assert_eq!(book.authority(), &AuthorityState::Live);

    let much_later = snapshot(
        two_sided("100", "200"),
        "2027-08-31T07:12:32.741Z",
        86_400_000_000_000,
    );
    let commit = book.apply_snapshot(&much_later).unwrap();
    assert!(commit.mutations().is_empty());
    assert_eq!(book.authority(), &AuthorityState::Live);
    assert_eq!(
        book.continuity(),
        &MutationContinuity::Intact {
            epoch: 0,
            next_position: 0,
        }
    );

    assert!(
        book.report_continuity_loss(ContinuityReason::LocalLoss, AuthorityReason::LocalLoss)
            .unwrap()
    );
    assert_eq!(
        book.authority(),
        &AuthorityState::Stale(AuthorityReason::LocalLoss)
    );
}

#[test]
fn book_known_gap_suppresses_derived_mutations_and_advances_the_epoch() {
    let mut book = OrderBook::new(market());
    book.apply_snapshot(&snapshot(two_sided("100", "200"), OBSERVED_TIMESTAMP, 1))
        .unwrap();
    let commit = book
        .apply_snapshot(&snapshot(two_sided("150", "200"), OBSERVED_TIMESTAMP, 2))
        .unwrap();
    assert_eq!(commit.mutations().len(), 1);
    let changed = commit.mutations()[0].mutation();
    assert_eq!(changed.old(), Some(&level(Side::Bid, "0.4", "100")));
    assert_eq!(changed.replacement(), Some(&level(Side::Bid, "0.4", "150")));

    for cycle in 1..=2u64 {
        assert!(
            book.report_continuity_loss(ContinuityReason::Gap, AuthorityReason::Gap)
                .unwrap()
        );
        assert_eq!(
            book.authority(),
            &AuthorityState::Stale(AuthorityReason::Gap)
        );
        assert_eq!(
            book.continuity(),
            &MutationContinuity::Lost {
                epoch: cycle - 1,
                reason: ContinuityReason::Gap,
            }
        );

        let recovery = book
            .apply_snapshot(&snapshot(
                two_sided(&format!("{}00", 2 + cycle), "200"),
                OBSERVED_TIMESTAMP,
                2 + cycle,
            ))
            .unwrap();
        assert!(recovery.mutations().is_empty());
        assert!(recovery.recovery_base());
        assert_eq!(recovery.epoch(), cycle);
        assert_eq!(book.continuity().epoch(), cycle);
        assert_eq!(book.authority(), &AuthorityState::Live);
        let published = book.publish();
        assert_eq!(published.provenance().unwrap().continuity_epoch(), cycle);
        assert_eq!(
            published.provenance().unwrap().local_revision(),
            published.revision()
        );
    }
}

#[test]
fn book_unchanged_snapshot_refreshes_provenance_without_false_depth_changes() {
    let mut book = OrderBook::new(market());
    book.apply_snapshot(&snapshot(
        vec![level(Side::Bid, "0.5", "100")],
        OBSERVED_TIMESTAMP,
        1,
    ))
    .unwrap();
    let before = book.publish();

    let commit = book
        .apply_snapshot(&snapshot(
            vec![level(Side::Bid, "0.50", "100.0")],
            "2026-08-31T07:12:33.842Z",
            2,
        ))
        .unwrap();
    let after = book.publish();

    assert!(commit.mutations().is_empty());
    assert!(!commit.recovery_base());
    assert_eq!(after.canonical_levels(), before.canonical_levels());
    assert_eq!(after.revision(), before.revision() + 1);
    assert_eq!(
        after
            .provenance()
            .unwrap()
            .source_timestamp()
            .unwrap()
            .as_lexeme(),
        "2026-08-31T07:12:33.842Z"
    );
    assert_eq!(
        after.provenance().unwrap().local_revision(),
        after.revision()
    );
    assert_eq!(after.authority(), &AuthorityState::Live);
}

#[test]
fn book_reproduces_every_observed_level_exactly() {
    let (book, update) = observed_book();
    let published = book.publish();
    let levels = published.canonical_levels();
    assert_eq!(levels.len(), 30);

    let depth_at = |side: Side, lexeme: &str| {
        levels
            .iter()
            .find(|level| level.side() == side && level.price() == &price(lexeme))
            .unwrap()
            .quantity()
            .value()
            .canonical()
    };
    assert_eq!(depth_at(Side::Bid, "0.012"), "83334000");
    assert_eq!(depth_at(Side::Bid, "0.001"), "2000000000");
    assert_eq!(depth_at(Side::Ask, "0.306"), "1441000");
    assert_eq!(depth_at(Side::Ask, "0.999"), "2000000000");
    assert_eq!(update.version(), Some("7861372"));
    assert_eq!(
        update.version_evidence().unwrap(),
        Some(SourceEvidence::Version(
            SourceEvidenceValue::new("7861372").unwrap()
        ))
    );
}

#[test]
fn book_derived_complement_view_is_the_same_liquidity_side_swapped() {
    let (book, _) = observed_book();
    let published = book.publish();
    let canonical = published.canonical_levels();
    let complement = published.derived_complement_levels().unwrap();

    assert_eq!(complement.len(), canonical.len());
    assert!(complement.contains(&level(Side::Ask, "0.988", "83334000")));
    assert!(complement.contains(&level(Side::Ask, "0.999", "2000000000")));
    assert!(complement.contains(&level(Side::Bid, "0.781", "100000000")));
    assert!(complement.contains(&level(Side::Bid, "0.001", "2000000000")));
    assert_eq!(total_depth(&complement), total_depth(canonical));
    assert!(
        complement
            .windows(2)
            .all(|pair| (pair[0].side(), pair[0].price()) < (pair[1].side(), pair[1].price()))
    );
    assert_eq!(
        published.liquidity_identity(),
        LiquidityIdentity::Market(market())
    );
}

#[test]
fn book_mutated_snapshot_yields_the_expected_derived_mutations() {
    let (mut book, _) = observed_book();
    let mut levels = book.publish().canonical_levels().to_vec();
    levels.retain(|level| !(level.side() == Side::Bid && level.price() == &price("0.001")));
    for entry in &mut levels {
        if entry.side() == Side::Bid && entry.price() == &price("0.012") {
            *entry = level(Side::Bid, "0.012", "90000000");
        }
    }
    levels.push(level(Side::Ask, "0.5", "1000000"));

    let commit = book
        .apply_snapshot(&snapshot(levels, OBSERVED_TIMESTAMP, 2))
        .unwrap();
    let observed: Vec<(Option<Level>, Option<Level>, u64)> = commit
        .mutations()
        .iter()
        .map(|record| {
            (
                record.mutation().old().cloned(),
                record.mutation().replacement().cloned(),
                record.cursor().position(),
            )
        })
        .collect();
    assert_eq!(
        observed,
        vec![
            (Some(level(Side::Bid, "0.001", "2000000000")), None, 0),
            (
                Some(level(Side::Bid, "0.012", "83334000")),
                Some(level(Side::Bid, "0.012", "90000000")),
                1,
            ),
            (None, Some(level(Side::Ask, "0.5", "1000000")), 2),
        ]
    );

    let derived = commit.mutations()[0].mutation().provenance();
    assert_eq!(derived.native_family(), "derived.snapshot-diff");
    assert_eq!(
        derived.origin(),
        &Origin::LocallyDerived(Derivation::SnapshotDiff)
    );
    assert_eq!(derived.representation(), &Representation::Normalized);
    assert_eq!(derived.source_timestamp(), None);
    assert!(derived.source_evidence().is_empty());
    assert_eq!(derived.local_revision(), 2);
    assert_eq!(derived.continuity_epoch(), 0);
    assert_eq!(derived.connection().value(), "ws.limitless.exchange");
    assert!(
        commit
            .mutations()
            .iter()
            .all(|record| record.cursor().epoch() == 0)
    );
}

#[test]
fn book_rejects_inputs_it_cannot_authoritatively_apply() {
    let mut book = OrderBook::new(market());
    let foreign = MarketRef::new(
        Venue::new("limitless").unwrap(),
        NativeMarketKey::new(NativeIdentifierKind::slug(), "another-market").unwrap(),
    );
    let mut foreign_book = OrderBook::new(foreign);
    assert_eq!(
        foreign_book.apply_snapshot(&snapshot(two_sided("100", "200"), OBSERVED_TIMESTAMP, 1)),
        Err(BookError::MarketMismatch)
    );

    let delta = Candidate::source_delta(
        provenance(OBSERVED_TIMESTAMP, 1),
        BoundedLevels::new(two_sided("100", "200"), capacity()).unwrap(),
    )
    .unwrap();
    assert_eq!(
        book.apply_snapshot(&delta),
        Err(BookError::UnsupportedCandidateOperation)
    );

    let duplicated = vec![
        level(Side::Bid, "0.4", "100"),
        level(Side::Bid, "0.40", "300"),
    ];
    assert_eq!(
        book.apply_snapshot(&snapshot(duplicated, OBSERVED_TIMESTAMP, 1)),
        Err(BookError::DuplicateLevelCoordinate)
    );
    assert_eq!(book.revision(), 0);
    assert_eq!(book.authority(), &AuthorityState::Synchronizing);
}

/// A price grammar finer than the venue's, so a scale the complement arithmetic cannot
/// represent can be constructed at all: scaling the unit to 39 places overflows the
/// coefficient, while 38 places still fits.
fn wide_grammar() -> DecimalGrammar {
    DecimalGrammar::new(39, 39, true, false).unwrap()
}

/// Whether a wait on the state surface is still pending right now.
///
/// Deterministic and timer-free: the biased select polls the wait first, so the ready
/// branch only wins when the wait is genuinely not ready.
async fn state_wait_is_pending(observer: &mut BookObserver) -> bool {
    tokio::select! {
        biased;
        _ = observer.state_changed() => false,
        () = std::future::ready(()) => true,
    }
}

async fn next_event_is_pending(observer: &mut BookObserver) -> bool {
    tokio::select! {
        biased;
        _ = observer.next_event() => false,
        () = std::future::ready(()) => true,
    }
}

#[tokio::test]
async fn book_observer_receives_mutations_and_a_coalesced_state_notification() {
    let mut writer = BookWriter::new(OrderBook::new(market()), ObserverCapacity::new(64).unwrap());
    let mut observer = writer.attach();
    assert_eq!(
        observer.state(),
        ConsumerState::Attached {
            cursor: MutationCursor::new(0, 0),
            continuous: true,
        }
    );

    writer
        .apply_snapshot(&snapshot(two_sided("100", "200"), OBSERVED_TIMESTAMP, 1))
        .unwrap();
    writer
        .apply_snapshot(&snapshot(two_sided("150", "200"), OBSERVED_TIMESTAMP, 2))
        .unwrap();

    let mut deliveries = Vec::new();
    while let Some(delivery) = observer.try_recv().unwrap() {
        deliveries.push((
            delivery.revision(),
            delivery.cursor().epoch(),
            delivery.cursor().position(),
        ));
    }
    assert_eq!(
        deliveries,
        vec![(2, 0, 0)],
        "the first snapshot is a base, and only mutations reach the ring"
    );

    let published = observer.state_changed().await.unwrap();
    assert_eq!(published.revision(), 2);
    assert_eq!(observer.latest().revision(), writer.book().revision());
    assert_eq!(
        observer.state(),
        ConsumerState::Attached {
            cursor: MutationCursor::new(0, 0),
            continuous: true,
        }
    );
}

/// A ring with room for one delivery, driven by revisions that derive no mutation, never
/// reports a continuity loss: the state surface coalesces them and the ring stays untouched.
#[tokio::test]
async fn book_observer_capacity_one_ring_survives_mutation_free_revisions() {
    let mut writer = BookWriter::new(OrderBook::new(market()), ObserverCapacity::new(1).unwrap());
    let mut observer = writer.attach();
    for position in 1..=64u64 {
        writer
            .apply_snapshot(&snapshot(
                two_sided("100", "200"),
                OBSERVED_TIMESTAMP,
                position,
            ))
            .unwrap();
    }
    assert_eq!(writer.book().revision(), 64);

    assert_eq!(
        observer.try_recv(),
        Ok(None),
        "a revision that derives no mutation never consumes ring capacity"
    );
    assert_eq!(
        observer.state(),
        ConsumerState::Attached {
            cursor: MutationCursor::new(0, 0),
            continuous: true,
        }
    );

    let published = observer.state_changed().await.unwrap();
    assert_eq!(
        published.revision(),
        64,
        "the state surface coalesces every skipped revision onto the newest"
    );
    assert!(
        state_wait_is_pending(&mut observer).await,
        "a coalesced notification leaves nothing further to report"
    );
}

#[test]
fn book_observer_overrun_reports_continuity_loss_and_reattaches() {
    let mut writer = BookWriter::new(OrderBook::new(market()), ObserverCapacity::new(4).unwrap());
    let mut observer = writer.attach();
    for position in 1..=12u64 {
        writer
            .apply_snapshot(&snapshot(
                two_sided(&format!("{}00", position), "200"),
                OBSERVED_TIMESTAMP,
                position,
            ))
            .unwrap();
    }

    let error = observer.try_recv().unwrap_err();
    let ObserverRecvError::ContinuityLost { reason, missed } = error else {
        panic!("an overtaken consumer is told continuity was lost");
    };
    assert_eq!(reason, ContinuityReason::Overrun);
    assert!(
        missed > 0,
        "the loss states how many mutation deliveries were dropped"
    );
    assert_eq!(
        observer.state(),
        ConsumerState::Overrun {
            cursor: MutationCursor::new(0, 0),
        }
    );

    let published = observer.reattach();
    assert_eq!(published.revision(), writer.book().revision());
    assert_eq!(published.authority(), &AuthorityState::Live);
    assert!(matches!(
        observer.state(),
        ConsumerState::Attached {
            continuous: true,
            ..
        }
    ));
    assert_eq!(
        observer.try_recv().unwrap(),
        None,
        "state already contains every mutation at or below the re-read revision"
    );

    writer
        .apply_snapshot(&snapshot(two_sided("999", "200"), OBSERVED_TIMESTAMP, 13))
        .unwrap();
    let delivery = observer.try_recv().unwrap().unwrap();
    assert!(delivery.revision() > published.revision());
    assert_eq!(observer.latest().revision(), writer.book().revision());
}

/// An overtaken consumer is never handed partial history. Every receive path keeps
/// refusing until the attachment is restarted, and the restart neither replays nor skips
/// the mutations of the commits that straddle it.
#[tokio::test]
async fn book_observer_refuses_every_receive_until_reattach_after_overrun() {
    let mut writer = BookWriter::new(OrderBook::new(market()), ObserverCapacity::new(2).unwrap());
    let mut observer = writer.attach();
    for position in 1..=12u64 {
        writer
            .apply_snapshot(&snapshot(
                two_sided(&format!("{}00", position), "200"),
                OBSERVED_TIMESTAMP,
                position,
            ))
            .unwrap();
    }

    let first = observer.try_recv().unwrap_err();
    let ObserverRecvError::ContinuityLost { missed, .. } = first else {
        panic!("an overtaken consumer is told continuity was lost");
    };
    let lost = ObserverRecvError::ContinuityLost {
        reason: ContinuityReason::Overrun,
        missed,
    };
    assert_eq!(observer.try_recv().unwrap_err(), lost);
    assert_eq!(observer.recv().await.unwrap_err(), lost);
    assert_eq!(observer.next_event().await.unwrap_err(), lost);

    let straddling = vec![
        level(Side::Bid, "0.4", "1200"),
        level(Side::Ask, "0.6", "999"),
        level(Side::Ask, "0.7", "5"),
    ];
    let straddled = writer
        .apply_snapshot(&snapshot(straddling, OBSERVED_TIMESTAMP, 13))
        .unwrap();
    assert_eq!(straddled.mutations().len(), 2);
    assert_eq!(
        observer.try_recv().unwrap_err(),
        lost,
        "deliveries stay refused while the consumer has not restarted"
    );

    let published = observer.reattach();
    assert_eq!(published.revision(), writer.book().revision());
    assert_eq!(
        observer.try_recv().unwrap(),
        None,
        "the commits the loss spanned are already in the state read back"
    );

    let resumed = vec![
        level(Side::Bid, "0.4", "1300"),
        level(Side::Ask, "0.6", "1000"),
        level(Side::Ask, "0.7", "5"),
    ];
    let commit = writer
        .apply_snapshot(&snapshot(resumed, OBSERVED_TIMESTAMP, 14))
        .unwrap();
    assert_eq!(commit.mutations().len(), 2);

    let mut delivered = Vec::new();
    while let Some(delivery) = observer.try_recv().unwrap() {
        delivered.push((delivery.revision(), delivery.cursor().clone()));
    }
    let expected: Vec<(u64, MutationCursor)> = commit
        .mutations()
        .iter()
        .map(|record| (commit.revision(), record.cursor().clone()))
        .collect();
    assert_eq!(
        delivered, expected,
        "every mutation of the post-reattach commit arrives exactly once"
    );
    assert_eq!(
        observer.state(),
        ConsumerState::Attached {
            cursor: commit.mutations().last().unwrap().cursor().clone(),
            continuous: true,
        }
    );
}

/// Dropping the writer closes both surfaces. Buffered mutations still drain, and only then
/// does the typed closure appear, so a consumer never waits forever on a gone writer.
#[tokio::test]
async fn book_observer_drains_buffered_mutations_then_reports_closed() {
    let mut writer = BookWriter::new(OrderBook::new(market()), ObserverCapacity::new(16).unwrap());
    let mut observer = writer.attach();
    writer
        .apply_snapshot(&snapshot(two_sided("100", "200"), OBSERVED_TIMESTAMP, 1))
        .unwrap();
    let commit = writer
        .apply_snapshot(&snapshot(two_sided("150", "250"), OBSERVED_TIMESTAMP, 2))
        .unwrap();
    assert_eq!(commit.mutations().len(), 2);
    let final_revision = writer.book().revision();
    drop(writer);

    let mut drained = 0u64;
    let closure = loop {
        match observer.try_recv() {
            Ok(Some(_)) => drained += 1,
            Ok(None) => panic!("a gone writer leaves the ring closed, not merely empty"),
            Err(error) => break error,
        }
    };
    assert_eq!(drained, 2, "buffered mutations drain before the closure");
    assert_eq!(closure, ObserverRecvError::Closed);
    assert_eq!(observer.state(), ConsumerState::Detached);
    assert_eq!(
        observer.next_event().await.unwrap_err(),
        ObserverRecvError::Closed
    );
    assert_eq!(
        observer.state_changed().await.unwrap().revision(),
        final_revision,
        "the last revision published before the writer went away is still reported"
    );
    assert_eq!(observer.state_changed().await, Err(WriterGone));
    assert_eq!(
        observer.latest().revision(),
        final_revision,
        "the final published revision stays readable after the writer is gone"
    );
}

#[test]
fn book_publishing_never_blocks_on_an_unpolled_observer() {
    let mut writer = BookWriter::new(OrderBook::new(market()), ObserverCapacity::new(2).unwrap());
    let idle = writer.attach();
    for position in 1..=500u64 {
        writer
            .apply_snapshot(&snapshot(
                two_sided(&format!("{position}00"), "200"),
                OBSERVED_TIMESTAMP,
                position,
            ))
            .unwrap();
    }
    assert_eq!(writer.book().revision(), 500);
    assert_eq!(writer.published().revision(), 500);

    drop(idle);
    writer
        .apply_snapshot(&snapshot(two_sided("1", "200"), OBSERVED_TIMESTAMP, 501))
        .unwrap();
    assert_eq!(writer.published().revision(), 501);
}

#[tokio::test]
async fn book_observer_awaits_the_next_event_on_either_surface() {
    let mut writer = BookWriter::new(OrderBook::new(market()), ObserverCapacity::new(8).unwrap());
    let mut observer = writer.attach();
    writer
        .apply_snapshot(&snapshot(two_sided("100", "200"), OBSERVED_TIMESTAMP, 1))
        .unwrap();
    writer
        .apply_snapshot(&snapshot(two_sided("150", "200"), OBSERVED_TIMESTAMP, 2))
        .unwrap();

    let ObserverEvent::Mutation(delivery) = observer.next_event().await.unwrap() else {
        panic!("pending history drains before the coalescing state notification");
    };
    assert_eq!(delivery.revision(), 2);
    assert_eq!(delivery.cursor(), &MutationCursor::new(0, 0));

    let ObserverEvent::Published(published) = observer.next_event().await.unwrap() else {
        panic!("the state surface then reports the newest revision");
    };
    assert_eq!(published.revision(), 2);
    assert!(
        next_event_is_pending(&mut observer).await,
        "both surfaces are caught up"
    );
}

#[tokio::test]
async fn book_reported_loss_is_published_once_and_repeats_are_no_ops() {
    let mut writer = BookWriter::new(OrderBook::new(market()), ObserverCapacity::new(8).unwrap());
    writer
        .apply_snapshot(&snapshot(two_sided("100", "200"), OBSERVED_TIMESTAMP, 1))
        .unwrap();
    let mut observer = writer.attach();

    assert!(
        writer
            .report_continuity_loss(ContinuityReason::Reconnect, AuthorityReason::Disconnect)
            .unwrap()
    );
    let published = observer.state_changed().await.unwrap();
    assert_eq!(published.revision(), 2);
    assert_eq!(
        published.authority(),
        &AuthorityState::Stale(AuthorityReason::Disconnect)
    );
    assert_eq!(
        published.continuity(),
        &MutationContinuity::Lost {
            epoch: 0,
            reason: ContinuityReason::Reconnect,
        }
    );
    assert_eq!(
        observer.try_recv().unwrap(),
        None,
        "a reported loss derives no mutation and never reaches the ring"
    );

    assert!(
        !writer
            .report_continuity_loss(ContinuityReason::Reconnect, AuthorityReason::Disconnect)
            .unwrap()
    );
    assert_eq!(writer.published().revision(), published.revision());
    assert!(state_wait_is_pending(&mut observer).await);
}

/// A snapshot is admitted only if every level's price is complementable, so the complement
/// view of a committed book is always readable. A rejection changes nothing at all.
#[test]
fn book_rejects_a_non_complementable_price_atomically() {
    let mut book = OrderBook::new(market());
    book.apply_snapshot(&snapshot(two_sided("100", "200"), OBSERVED_TIMESTAMP, 1))
        .unwrap();
    let before = book.publish();

    let above_unit = Level::new(
        Side::Bid,
        Price::parse("2", grammar()).unwrap(),
        quantity("100"),
    );
    let finer_than_the_complement = Level::new(
        Side::Ask,
        Price::parse("1e-39", wide_grammar()).unwrap(),
        quantity("100"),
    );

    for rejected in [above_unit, finer_than_the_complement] {
        let outcome = book.apply_snapshot(&snapshot(vec![rejected], OBSERVED_TIMESTAMP, 2));
        assert!(
            matches!(outcome, Err(BookError::NonComplementablePrice(_))),
            "a price whose complement is unrepresentable is its own rejection reason, got {outcome:?}"
        );
        let after = book.publish();
        assert_eq!(after.revision(), before.revision());
        assert_eq!(after.canonical_levels(), before.canonical_levels());
        assert_eq!(after.continuity(), before.continuity());
        assert_eq!(after.authority(), before.authority());
    }

    assert!(
        book.publish().derived_complement_levels().is_ok(),
        "a committed book always has a readable complement view"
    );
}

/// The pure overrun rule, driven directly.
///
/// A ring reports only how many deliveries it dropped, never which. What it does reveal is
/// where it resumed, and the dropped deliveries are exactly the positions immediately
/// preceding that point — so the resuming cursor decides the verdict.
#[test]
fn book_overrun_verdict_turns_on_the_attachment_boundary() {
    let boundary = MutationCursor::new(3, 10);

    assert_eq!(
        classify_overrun(&boundary, Some(&MutationCursor::new(3, 4))),
        OverrunVerdict::Harmless,
        "every dropped position precedes the boundary, so the attached state holds them all"
    );
    assert_eq!(
        classify_overrun(&boundary, Some(&MutationCursor::new(3, 10))),
        OverrunVerdict::Harmless,
        "resuming exactly at the boundary means the drops stopped one position short of it, \
         and the boundary delivery itself is new and still passes to be applied"
    );
    assert_eq!(
        classify_overrun(&boundary, Some(&MutationCursor::new(3, 11))),
        OverrunVerdict::Lost,
        "resuming one past the boundary leaves the boundary position itself dropped"
    );
    assert_eq!(
        classify_overrun(&boundary, Some(&MutationCursor::new(4, 0))),
        OverrunVerdict::Lost,
        "a gap spanning an epoch change is not provably harmless whatever its position"
    );
    assert_eq!(
        classify_overrun(&boundary, Some(&MutationCursor::new(2, 4))),
        OverrunVerdict::Lost,
        "a resuming epoch below the boundary's is equally unprovable"
    );
    assert_eq!(
        classify_overrun(&boundary, None),
        OverrunVerdict::Lost,
        "a ring that closes without resuming leaves the gap forever unexaminable"
    );
    assert_eq!(
        classify_overrun(&MutationCursor::new(0, 0), Some(&MutationCursor::new(0, 1))),
        OverrunVerdict::Lost,
        "a fresh attachment contains nothing, so its first dropped position is a real loss"
    );
}

/// Consecutive overruns before any delivery are one unresolved span: the ring cannot be
/// asked again what it already dropped, so the counts add rather than replace.
#[test]
fn book_pending_overruns_accumulate_until_a_delivery_resolves_them() {
    assert_eq!(PendingOverrun::new(3).missed(), 3);
    assert_eq!(
        PendingOverrun::new(3).accumulated(4).missed(),
        7,
        "a second overrun before any delivery extends the same span"
    );
    assert_eq!(
        PendingOverrun::new(3)
            .accumulated(4)
            .accumulated(1)
            .missed(),
        8
    );
    assert_eq!(
        PendingOverrun::default().accumulated(5).missed(),
        5,
        "the first overrun of a span starts the total"
    );
    assert_eq!(
        PendingOverrun::new(u64::MAX).accumulated(9).missed(),
        u64::MAX,
        "an unbounded run of overruns saturates rather than wrapping"
    );
}

/// The first stream position a published state does not yet contain — the boundary an
/// attachment taken against that state records for itself.
fn boundary_position(published: &PublishedBook) -> u64 {
    match published.continuity() {
        MutationContinuity::Intact { next_position, .. } => *next_position,
        MutationContinuity::Lost { .. } => 0,
    }
}

/// One non-blocking drain step on whichever receive path this round uses.
///
/// All three share one classifier, so the property asserted below holds whichever drives a
/// round. Polling `recv` and `next_event` once and dropping them also exercises the
/// cancellation an unresolved overrun has to survive.
fn drain_step(
    runtime: &tokio::runtime::Runtime,
    observer: &mut BookObserver,
    round: u64,
) -> Result<bool, ObserverRecvError> {
    match round % 3 {
        0 => observer.try_recv().map(|delivery| delivery.is_some()),
        1 => runtime.block_on(async {
            tokio::select! {
                biased;
                delivery = observer.recv() => delivery.map(|_| true),
                () = std::future::ready(()) => Ok(false),
            }
        }),
        _ => runtime.block_on(async {
            tokio::select! {
                biased;
                event = observer.next_event() => event.map(|_| true),
                () = std::future::ready(()) => Ok(false),
            }
        }),
    }
}

/// A reattachment that lands between a revision's publication and that revision's mutation
/// sends must not be charged for the deliveries it already holds as state.
///
/// Only a second thread can reach that window: `attach` borrows the writer immutably and
/// `apply_snapshot` mutably, so the two cannot overlap, and inside one thread a fresh
/// subscription always starts past everything already sent. So the race is driven rather
/// than staged, and what is asserted is the property no schedule may violate — a reported
/// loss implies the writer had really emitted a position at or beyond the consumer's
/// boundary.
///
/// The assertion is sound whatever the schedule, because stream positions never decrease:
/// a published `next_position` read back after a loss is an upper bound on what had been
/// emitted when that loss was reported, so finding it still equal to the boundary proves
/// nothing at or beyond the boundary had ever been emitted, and therefore that nothing
/// there could have been dropped.
#[test]
fn book_observer_reattachment_races_report_no_false_continuity_loss() {
    const COMMITS: u64 = 3000;
    const LEVELS: u64 = 24;
    const STEPS: u32 = 64;

    let mut writer = BookWriter::new(OrderBook::new(market()), ObserverCapacity::new(1).unwrap());
    let mut observer = writer.attach();
    let writing = std::thread::spawn(move || {
        for commit in 1..=COMMITS {
            let levels = (0..LEVELS)
                .map(|index| {
                    level(
                        Side::Bid,
                        &format!("0.{:03}", index + 1),
                        &(commit + index).to_string(),
                    )
                })
                .collect();
            writer
                .apply_snapshot(&snapshot(levels, OBSERVED_TIMESTAMP, commit))
                .unwrap();
        }
        writer.book().revision()
    });

    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let mut rounds = 0u64;
    'consuming: loop {
        let attached = observer.reattach();
        let boundary = boundary_position(&attached);
        rounds += 1;
        for _ in 0..STEPS {
            match drain_step(&runtime, &mut observer, rounds) {
                Ok(true) => continue,
                Ok(false) => break,
                Err(ObserverRecvError::ContinuityLost { missed, .. }) => {
                    let reached = boundary_position(&observer.latest());
                    assert!(
                        reached > boundary,
                        "round {rounds} reported {missed} missed against boundary {boundary}, \
                         but the writer had emitted nothing at or beyond it, so no delivery \
                         this attachment needed can have been dropped"
                    );
                    break;
                }
                Err(ObserverRecvError::Closed) => break 'consuming,
            }
        }
    }

    assert_eq!(writing.join().unwrap(), COMMITS);
    assert!(
        rounds > 1,
        "the consumer restarted its attachment repeatedly against the running writer"
    );
}

const RESOLUTION_DATE: &str = "2026-09-01T12:05:00Z";

/// One venue-reported resolution for this market, under the same venue-native provenance a
/// book update carries.
fn resolution(winner: &str, index: u32, native_label: &str) -> std::sync::Arc<MarketResolution> {
    let observation = ResolutionObservation::new(
        provenance(OBSERVED_TIMESTAMP, 9),
        NativeOutcome::venue_defined(winner).unwrap(),
        NativeLabel::new(native_label).unwrap(),
        DeliveryPath::MarketFeed,
    )
    .unwrap();
    std::sync::Arc::new(MarketResolution::new(
        observation,
        index,
        SourceTimestamp::new(RESOLUTION_DATE).unwrap(),
    ))
}

/// A stream event's position is drawn from the book's own counter, so the next commit
/// starts one past it.
///
/// This is the whole reason resolutions go through [`OrderBook::note_stream_event`] rather
/// than reusing the current position: a position handed out twice is a delivery the next
/// commit silently overwrites, and no consumer could ever detect it.
#[test]
fn book_stream_event_position_is_never_reused_by_the_next_commit() {
    let mut book = OrderBook::new(market());
    book.apply_snapshot(&snapshot(two_sided("100", "200"), OBSERVED_TIMESTAMP, 1))
        .unwrap();
    let first = book
        .apply_snapshot(&snapshot(two_sided("150", "200"), OBSERVED_TIMESTAMP, 2))
        .unwrap();
    assert_eq!(first.mutations().len(), 1);
    assert_eq!(first.mutations()[0].cursor(), &MutationCursor::new(0, 0));

    let allocated = book.note_stream_event().unwrap();
    assert_eq!(allocated, MutationCursor::new(0, 1));

    let next = book
        .apply_snapshot(&snapshot(two_sided("175", "200"), OBSERVED_TIMESTAMP, 3))
        .unwrap();
    assert_eq!(next.mutations().len(), 1);
    assert_eq!(
        next.mutations()[0].cursor(),
        &MutationCursor::new(0, 2),
        "the commit after a stream event starts one position past it"
    );
}

/// Allocating a stream position moves the stream and nothing else, and a lost stream has no
/// position to give.
#[test]
fn book_stream_event_moves_only_the_stream_and_is_refused_when_it_is_lost() {
    let mut book = OrderBook::new(market());
    book.apply_snapshot(&snapshot(two_sided("100", "200"), OBSERVED_TIMESTAMP, 1))
        .unwrap();
    let revision = book.revision();
    let levels = book.publish().canonical_levels().to_vec();

    assert_eq!(book.note_stream_event().unwrap(), MutationCursor::new(0, 0));
    assert_eq!(book.revision(), revision, "no revision is committed");
    assert_eq!(book.authority(), &AuthorityState::Live);
    assert_eq!(book.publish().canonical_levels(), levels.as_slice());
    assert_eq!(
        book.continuity(),
        &MutationContinuity::Intact {
            epoch: 0,
            next_position: 1
        }
    );

    assert!(
        book.report_continuity_loss(ContinuityReason::Gap, AuthorityReason::Gap)
            .unwrap()
    );
    assert_eq!(book.note_stream_event(), Err(BookError::ContinuityLost));
    assert_eq!(
        book.continuity(),
        &MutationContinuity::Lost {
            epoch: 0,
            reason: ContinuityReason::Gap
        },
        "a refused allocation leaves the stream exactly as it was"
    );
}

/// A resolution is ordered against the level changes around it, on the one lane.
#[tokio::test]
async fn book_observer_orders_a_resolution_between_the_mutations_around_it() {
    let mut writer = BookWriter::new(OrderBook::new(market()), ObserverCapacity::new(64).unwrap());
    writer
        .apply_snapshot(&snapshot(two_sided("100", "200"), OBSERVED_TIMESTAMP, 1))
        .unwrap();
    let mut observer = writer.attach();
    writer
        .apply_snapshot(&snapshot(two_sided("150", "200"), OBSERVED_TIMESTAMP, 2))
        .unwrap();
    let resolved = resolution("Yes", 1, "clob");
    let delivery = writer
        .publish_resolution(std::sync::Arc::clone(&resolved))
        .unwrap();
    assert_eq!(delivery.cursor(), &MutationCursor::new(0, 1));
    assert_eq!(
        delivery.revision(),
        2,
        "a resolution is ordered after the revision current when it arrived"
    );
    writer
        .apply_snapshot(&snapshot(two_sided("175", "200"), OBSERVED_TIMESTAMP, 3))
        .unwrap();

    let first = observer.try_recv().unwrap().unwrap();
    assert!(matches!(first, StreamDelivery::Mutation(_)));
    assert_eq!(first.cursor(), &MutationCursor::new(0, 0));

    let StreamDelivery::Resolution(second) = observer.try_recv().unwrap().unwrap() else {
        panic!("the resolution is delivered between the two mutations");
    };
    assert_eq!(second.cursor(), &MutationCursor::new(0, 1));
    assert_eq!(second.revision(), 2);
    assert_eq!(second.resolution(), &resolved);

    let third = observer.try_recv().unwrap().unwrap();
    assert!(matches!(third, StreamDelivery::Mutation(_)));
    assert_eq!(third.cursor(), &MutationCursor::new(0, 2));

    assert_eq!(observer.try_recv().unwrap(), None);
    assert_eq!(
        observer.state(),
        ConsumerState::Attached {
            cursor: MutationCursor::new(0, 2),
            continuous: true
        }
    );
    assert_eq!(
        writer.published().revision(),
        3,
        "the resolution committed no revision of its own"
    );
}

/// A resolution arriving at the attachment's own revision is delivered, not suppressed.
///
/// A resolution advances no revision, so it carries the revision the attachment already
/// read. Judging it by revision the way a mutation is judged would call every live
/// resolution redundant and drop it silently — the one failure a consumer could never
/// detect, because nothing is reported and the stream stays continuous.
#[tokio::test]
async fn book_observer_delivers_a_resolution_at_the_attachments_own_revision() {
    let mut writer = BookWriter::new(OrderBook::new(market()), ObserverCapacity::new(64).unwrap());
    writer
        .apply_snapshot(&snapshot(two_sided("100", "200"), OBSERVED_TIMESTAMP, 1))
        .unwrap();
    let mut observer = writer.attach();
    let attached = observer.latest().revision();

    let resolved = resolution("No", 0, "clob");
    let delivery = writer
        .publish_resolution(std::sync::Arc::clone(&resolved))
        .unwrap();
    assert_eq!(
        delivery.revision(),
        attached,
        "the resolution really does carry the revision the attachment read"
    );

    let StreamDelivery::Resolution(received) = observer
        .try_recv()
        .unwrap()
        .expect("a resolution at the attachment's revision reaches the consumer")
    else {
        panic!("the delivery is the resolution");
    };
    assert_eq!(received.resolution(), &resolved);
    assert_eq!(received.cursor(), &MutationCursor::new(0, 0));
    assert_eq!(
        observer.state(),
        ConsumerState::Attached {
            cursor: MutationCursor::new(0, 0),
            continuous: true
        }
    );
}

/// An attachment taken after a resolution starts past it, and loses nothing by it.
///
/// The published stream advances past a resolution, so a later attachment's boundary is
/// already beyond it: the resolution costs the new consumer no wait, no gap, and no loss,
/// and its next delivery is the following mutation at a contiguous position.
#[tokio::test]
async fn book_attaching_after_a_resolution_starts_past_it_without_a_loss() {
    let mut writer = BookWriter::new(OrderBook::new(market()), ObserverCapacity::new(64).unwrap());
    writer
        .apply_snapshot(&snapshot(two_sided("100", "200"), OBSERVED_TIMESTAMP, 1))
        .unwrap();
    let _ = writer
        .publish_resolution(resolution("Yes", 1, "clob"))
        .unwrap();

    let mut observer = writer.attach();
    assert_eq!(
        observer.state(),
        ConsumerState::Attached {
            cursor: MutationCursor::new(0, 1),
            continuous: true
        },
        "the published stream names the position past the resolution"
    );
    assert_eq!(observer.try_recv().unwrap(), None);

    writer
        .apply_snapshot(&snapshot(two_sided("150", "200"), OBSERVED_TIMESTAMP, 2))
        .unwrap();
    let delivery = observer.try_recv().unwrap().unwrap();
    assert!(matches!(delivery, StreamDelivery::Mutation(_)));
    assert_eq!(delivery.cursor(), &MutationCursor::new(0, 1));
    assert_eq!(observer.try_recv().unwrap(), None);
}

/// A rebase found on a resolution is the same loss it is on a mutation, and reattaching
/// clears it.
#[tokio::test]
async fn book_observer_reports_a_rebase_carried_by_a_resolution() {
    let mut writer = BookWriter::new(OrderBook::new(market()), ObserverCapacity::new(64).unwrap());
    writer
        .apply_snapshot(&snapshot(two_sided("100", "200"), OBSERVED_TIMESTAMP, 1))
        .unwrap();
    let mut observer = writer.attach();

    assert!(
        writer
            .report_continuity_loss(ContinuityReason::Reconnect, AuthorityReason::Disconnect)
            .unwrap()
    );
    let base = writer
        .apply_snapshot(&snapshot(two_sided("150", "200"), OBSERVED_TIMESTAMP, 2))
        .unwrap();
    assert!(base.recovery_base());
    assert!(base.mutations().is_empty());

    let rebased = writer
        .publish_resolution(resolution("Yes", 1, "clob"))
        .unwrap();
    assert_eq!(rebased.cursor(), &MutationCursor::new(1, 0));

    let lost = ObserverRecvError::ContinuityLost {
        reason: ContinuityReason::RecoveryBase,
        missed: 0,
    };
    assert_eq!(observer.try_recv(), Err(lost.clone()));
    assert_eq!(observer.try_recv(), Err(lost), "the loss is sticky");
    assert!(matches!(observer.state(), ConsumerState::Rebased { .. }));

    let resumed = observer.reattach();
    assert_eq!(resumed.continuity().epoch(), 1);
    let next = writer
        .publish_resolution(resolution("No", 0, "clob"))
        .unwrap();
    let delivered = observer.try_recv().unwrap().unwrap();
    assert_eq!(delivered.cursor(), next.cursor());
}

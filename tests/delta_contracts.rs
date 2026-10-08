//! Source-delta application and sync-snapshot checkpoint semantics, driven by synthetic
//! candidates in the Polymarket rhythm: a snapshot base, a run of `price_change`-shaped
//! deltas, then a sync snapshot that either agrees or does not.

use pm_ws::*;

const OBSERVED_TIMESTAMP: &str = "2026-08-31T07:12:32.741Z";

fn market() -> MarketRef {
    MarketRef::new(
        Venue::new("limitless").unwrap(),
        NativeMarketKey::new(
            NativeIdentifierKind::slug(),
            "eth-up-or-down-daily-1788105600",
        )
        .unwrap(),
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

/// Venue-native provenance as an adapter would supply it, carrying `local_revision` and
/// `continuity_epoch` of 0 so any published value above 0 can only be the book's rebase.
fn provenance(family: &str, timestamp: &str, position: u64) -> Provenance {
    Provenance::new(ProvenanceInput {
        market: market(),
        outcome: Some(NativeOutcome::side("yes").unwrap()),
        native_family: family.into(),
        source_timestamp: Some(SourceTimestamp::new(timestamp).unwrap()),
        source_evidence: BoundedSourceEvidence::new(
            [SourceEvidence::sequence(position.to_string()).unwrap()],
            SourceEvidenceCapacity::new(1).unwrap(),
        )
        .unwrap(),
        daemon_generation: 1,
        connection: ConnectionIdentity::new("ws.venue.example", 1).unwrap(),
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

fn snapshot(levels: Vec<Level>, position: u64) -> Candidate {
    Candidate::snapshot(
        provenance("book", OBSERVED_TIMESTAMP, position),
        BoundedLevels::new(levels, capacity()).unwrap(),
    )
    .unwrap()
}

/// A delta in the venue's own order: the levels are handed over exactly as listed.
fn delta(levels: Vec<Level>, position: u64) -> Candidate {
    Candidate::source_delta(
        provenance("price_change", OBSERVED_TIMESTAMP, position),
        BoundedLevels::new(levels, capacity()).unwrap(),
    )
    .unwrap()
}

fn base() -> Vec<Level> {
    vec![
        level(Side::Bid, "0.39", "50"),
        level(Side::Bid, "0.4", "100"),
        level(Side::Ask, "0.6", "200"),
        level(Side::Ask, "0.61", "75"),
    ]
}

/// One accepted-base book: the first snapshot, which establishes state rather than
/// describing a transition.
fn based_book() -> OrderBook {
    let mut book = OrderBook::new(market());
    let commit = book.apply_snapshot(&snapshot(base(), 1)).unwrap();
    assert!(commit.mutations().is_empty());
    assert_eq!(book.deltas_since_snapshot(), 0);
    book
}

fn changes(commit: &BookCommit) -> Vec<(Option<Level>, Option<Level>, u64)> {
    commit
        .mutations()
        .iter()
        .map(|record| {
            (
                record.mutation().old().cloned(),
                record.mutation().replacement().cloned(),
                record.cursor().position(),
            )
        })
        .collect()
}

/// The scripted Polymarket rhythm end to end: a snapshot base, three deltas, then a sync
/// snapshot that agrees. Exact book state is asserted after every step.
#[test]
fn delta_scripted_snapshot_then_deltas_then_agreeing_sync_snapshot() {
    let mut book = based_book();
    assert_eq!(book.publish().canonical_levels(), base());

    let first = book
        .apply_source_delta(&delta(vec![level(Side::Bid, "0.4", "150")], 2))
        .unwrap();
    assert_eq!(
        changes(&first),
        vec![(
            Some(level(Side::Bid, "0.4", "100")),
            Some(level(Side::Bid, "0.4", "150")),
            0,
        )]
    );
    assert!(!first.recovery_base());
    assert_eq!(first.divergence(), None);
    assert_eq!(
        book.publish().canonical_levels(),
        vec![
            level(Side::Bid, "0.39", "50"),
            level(Side::Bid, "0.4", "150"),
            level(Side::Ask, "0.6", "200"),
            level(Side::Ask, "0.61", "75"),
        ]
    );

    let second = book
        .apply_source_delta(&delta(
            vec![
                level(Side::Ask, "0.61", "0"),
                level(Side::Bid, "0.38", "25"),
                level(Side::Bid, "0.39", "50.00"),
            ],
            3,
        ))
        .unwrap();
    assert_eq!(
        changes(&second),
        vec![
            (Some(level(Side::Ask, "0.61", "75")), None, 1),
            (None, Some(level(Side::Bid, "0.38", "25")), 2),
        ],
        "mutations follow the venue's own order, not ascending coordinates, and the level \
         restating what the book already held emits nothing"
    );
    assert_eq!(
        book.publish().canonical_levels(),
        vec![
            level(Side::Bid, "0.38", "25"),
            level(Side::Bid, "0.39", "50"),
            level(Side::Bid, "0.4", "150"),
            level(Side::Ask, "0.6", "200"),
        ]
    );

    let third = book
        .apply_source_delta(&delta(vec![level(Side::Ask, "0.62", "0")], 4))
        .unwrap();
    assert!(
        third.mutations().is_empty(),
        "removing a coordinate the book does not hold changes nothing"
    );
    assert_eq!(book.revision(), 4);
    assert_eq!(book.deltas_since_snapshot(), 3);
    assert_eq!(
        book.continuity(),
        &MutationContinuity::Intact {
            epoch: 0,
            next_position: 3,
        }
    );

    let settled = book.publish();
    let sync = book
        .apply_snapshot(
            &Candidate::snapshot(
                provenance("book", "2026-08-31T07:13:00.000Z", 5),
                BoundedLevels::new(
                    vec![
                        level(Side::Bid, "0.380", "25.0"),
                        level(Side::Bid, "0.39", "50"),
                        level(Side::Bid, "0.400", "150"),
                        level(Side::Ask, "0.6", "200.00"),
                    ],
                    capacity(),
                )
                .unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
    let checkpointed = book.publish();

    assert!(sync.mutations().is_empty());
    assert!(!sync.recovery_base());
    assert_eq!(sync.divergence(), None);
    assert_eq!(sync.epoch(), 0);
    assert_eq!(
        checkpointed.canonical_levels(),
        settled.canonical_levels(),
        "an agreeing sync snapshot is the same economic book whatever its lexemes"
    );
    assert_eq!(checkpointed.revision(), settled.revision() + 1);
    assert_eq!(
        checkpointed.provenance().unwrap().local_revision(),
        checkpointed.revision()
    );
    assert_eq!(
        checkpointed
            .provenance()
            .unwrap()
            .source_timestamp()
            .unwrap()
            .as_lexeme(),
        "2026-08-31T07:13:00.000Z",
        "provenance is refreshed even though no level changed"
    );
    assert_eq!(checkpointed.authority(), &AuthorityState::Live);
    assert_eq!(checkpointed.sync_divergences(), 0);
    assert_eq!(book.deltas_since_snapshot(), 0);
}

/// Every field a source-reported mutation carries is the candidate's own, rebased onto the
/// book's revision and epoch. Nothing is relabelled as locally derived.
#[test]
fn delta_mutations_carry_rebased_source_reported_provenance() {
    let mut book = based_book();
    let commit = book
        .apply_source_delta(&delta(vec![level(Side::Bid, "0.4", "150")], 2))
        .unwrap();

    let reported = commit.mutations()[0].mutation().provenance();
    assert_eq!(reported.origin(), &Origin::SourceReported);
    assert_eq!(reported.representation(), &Representation::VenueNative);
    assert_eq!(reported.native_family(), "price_change");
    assert_eq!(
        reported.source_timestamp().unwrap().as_lexeme(),
        OBSERVED_TIMESTAMP
    );
    assert_eq!(
        reported.source_evidence(),
        [SourceEvidence::sequence("2").unwrap()]
    );
    assert_eq!(reported.local_revision(), 2);
    assert_eq!(reported.continuity_epoch(), 0);
    assert_eq!(reported.connection().value(), "ws.venue.example");
    assert_eq!(commit.mutations()[0].cursor(), &MutationCursor::new(0, 0));
}

/// A sync snapshot that disagrees with a delta-built book is evidence a delta was missed or
/// misapplied. It commits as a recovery base with no diff across the gap, and says so.
#[tokio::test]
async fn delta_disagreeing_sync_snapshot_is_a_divergence_checkpoint() {
    let mut writer = BookWriter::new(OrderBook::new(market()), ObserverCapacity::new(16).unwrap());
    let mut observer = writer.attach();
    writer.apply_snapshot(&snapshot(base(), 1)).unwrap();
    writer
        .apply_source_delta(&delta(vec![level(Side::Bid, "0.4", "150")], 2))
        .unwrap();
    writer
        .apply_source_delta(&delta(vec![level(Side::Ask, "0.6", "210")], 3))
        .unwrap();
    while observer.try_recv().unwrap().is_some() {}

    let contradicting = vec![
        level(Side::Bid, "0.39", "50"),
        level(Side::Bid, "0.4", "900"),
        level(Side::Ask, "0.6", "210"),
        level(Side::Ask, "0.61", "75"),
    ];
    let checkpoint = writer
        .apply_snapshot(&snapshot(contradicting.clone(), 4))
        .unwrap();

    assert!(checkpoint.mutations().is_empty(), "no diff across the gap");
    assert!(checkpoint.recovery_base());
    assert_eq!(checkpoint.epoch(), 1);
    assert_eq!(
        checkpoint
            .divergence()
            .map(SyncDivergence::deltas_invalidated),
        Some(2),
        "the break covers every delta since the last snapshot"
    );

    let published = writer.published();
    assert_eq!(published.canonical_levels(), contradicting);
    assert_eq!(published.authority(), &AuthorityState::Live);
    assert_eq!(
        published.continuity(),
        &MutationContinuity::Intact {
            epoch: 1,
            next_position: 0,
        }
    );
    assert_eq!(published.sync_divergences(), 1);
    assert_eq!(published.provenance().unwrap().continuity_epoch(), 1);
    assert_eq!(
        observer.try_recv().unwrap(),
        None,
        "a divergence checkpoint never fabricates a correction diff onto the ring"
    );

    let resumed = writer
        .apply_source_delta(&delta(vec![level(Side::Bid, "0.4", "901")], 5))
        .unwrap();
    assert_eq!(resumed.epoch(), 1);
    assert_eq!(
        resumed.mutations()[0].cursor(),
        &MutationCursor::new(1, 0),
        "the delta stream resumes at the start of the new epoch"
    );
    assert_eq!(writer.published().sync_divergences(), 1);
}

/// A break the transport already reported is not rediagnosed as a divergence.
///
/// A snapshot arriving on a lost stream is a recovery base whatever it says, because the
/// reason the epoch ended is already known and better evidence than a disagreement. So the
/// divergence counter stays at zero: it counts only breaks that a sync snapshot detected,
/// never every break a delta-built book suffered.
#[test]
fn delta_recovery_after_a_reported_loss_is_not_a_sync_divergence() {
    let mut book = based_book();
    book.apply_source_delta(&delta(vec![level(Side::Bid, "0.4", "150")], 2))
        .unwrap();
    book.report_continuity_loss(ContinuityReason::Gap, AuthorityReason::Gap)
        .unwrap();

    let mut recovered = base();
    recovered[1] = level(Side::Bid, "0.4", "900");
    let commit = book
        .apply_snapshot(&snapshot(recovered.clone(), 3))
        .unwrap();

    assert_eq!(commit.divergence(), None);
    assert!(commit.recovery_base());
    assert!(commit.mutations().is_empty());
    assert_eq!(commit.epoch(), 1);

    let published = book.publish();
    assert_eq!(published.sync_divergences(), 0);
    assert_eq!(published.canonical_levels(), recovered);
    assert_eq!(published.authority(), &AuthorityState::Live);
    assert_eq!(book.deltas_since_snapshot(), 0);
}

/// The divergence checkpoint is reserved for delta-built books. A snapshot-built one takes
/// the derived-diff path whether the next snapshot agrees or not.
#[test]
fn delta_free_book_keeps_deriving_diffs_from_disagreeing_snapshots() {
    let mut book = based_book();
    let mut changed = base();
    changed[1] = level(Side::Bid, "0.4", "900");
    let commit = book.apply_snapshot(&snapshot(changed, 2)).unwrap();

    assert_eq!(commit.mutations().len(), 1);
    assert_eq!(
        commit.mutations()[0].mutation().provenance().origin(),
        &Origin::LocallyDerived(Derivation::SnapshotDiff)
    );
    assert!(!commit.recovery_base());
    assert_eq!(commit.divergence(), None);
    assert_eq!(commit.epoch(), 0);
    assert_eq!(book.publish().sync_divergences(), 0);
}

/// One ring, two kinds of delivery, told apart by provenance rather than by shape.
#[tokio::test]
async fn delta_observer_sees_source_reported_and_derived_deliveries_distinguishably() {
    let mut writer = BookWriter::new(OrderBook::new(market()), ObserverCapacity::new(16).unwrap());
    let mut observer = writer.attach();

    writer.apply_snapshot(&snapshot(base(), 1)).unwrap();
    let mut changed = base();
    changed[0] = level(Side::Bid, "0.39", "60");
    writer.apply_snapshot(&snapshot(changed, 2)).unwrap();
    writer
        .apply_source_delta(&delta(vec![level(Side::Ask, "0.6", "0")], 3))
        .unwrap();

    let mut delivered = Vec::new();
    while let Some(delivery) = observer.try_recv().unwrap() {
        let StreamDelivery::Mutation(delivery) = delivery else {
            panic!("this book publishes no resolution");
        };
        delivered.push((
            delivery.revision(),
            delivery.mutation().provenance().origin().clone(),
            delivery.mutation().provenance().native_family().to_owned(),
        ));
    }
    assert_eq!(
        delivered,
        vec![
            (
                2,
                Origin::LocallyDerived(Derivation::SnapshotDiff),
                "derived.snapshot-diff".to_owned(),
            ),
            (3, Origin::SourceReported, "price_change".to_owned()),
        ]
    );
    assert_eq!(observer.latest().revision(), 3);
}

/// A delta is applied only onto a base this book actually holds, with an intact stream.
#[test]
fn delta_without_a_base_or_with_a_broken_stream_is_refused() {
    let mut empty = OrderBook::new(market());
    assert_eq!(
        empty.apply_source_delta(&delta(vec![level(Side::Bid, "0.4", "100")], 1)),
        Err(BookError::NoEstablishedBase)
    );
    assert_eq!(
        empty.apply_source_delta(&delta(Vec::new(), 1)),
        Err(BookError::EmptyDelta),
        "a structurally malformed candidate is answered before book state is consulted"
    );
    assert_eq!(empty.revision(), 0);
    assert_eq!(empty.authority(), &AuthorityState::Synchronizing);

    let mut book = based_book();
    book.report_continuity_loss(ContinuityReason::Gap, AuthorityReason::Gap)
        .unwrap();
    let before = book.publish();
    assert_eq!(
        book.apply_source_delta(&delta(vec![level(Side::Bid, "0.4", "150")], 2)),
        Err(BookError::ContinuityLost),
        "a delta must never ride across a gap"
    );
    assert_eq!(book.publish(), before);
}

/// Every malformed delta is its own refusal, and every refusal leaves the book untouched.
#[test]
fn delta_malformed_input_is_refused_atomically() {
    let mut book = based_book();
    let before = book.publish();

    assert_eq!(
        book.apply_source_delta(&delta(Vec::new(), 2)),
        Err(BookError::EmptyDelta)
    );
    assert_eq!(
        book.apply_source_delta(&delta(
            vec![
                level(Side::Bid, "0.4", "150"),
                level(Side::Bid, "0.400", "160"),
            ],
            2,
        )),
        Err(BookError::DuplicateLevelCoordinate)
    );
    assert!(matches!(
        book.apply_source_delta(&delta(
            vec![Level::new(
                Side::Bid,
                Price::parse("2", grammar()).unwrap(),
                quantity("150"),
            )],
            2,
        )),
        Err(BookError::NonComplementablePrice(_))
    ));
    assert_eq!(
        book.apply_source_delta(&snapshot(base(), 2)),
        Err(BookError::UnsupportedCandidateOperation)
    );

    let foreign = MarketRef::new(
        Venue::new("limitless").unwrap(),
        NativeMarketKey::new(NativeIdentifierKind::slug(), "another-market").unwrap(),
    );
    let mut foreign_book = OrderBook::new(foreign);
    assert_eq!(
        foreign_book.apply_source_delta(&delta(vec![level(Side::Bid, "0.4", "150")], 2)),
        Err(BookError::MarketMismatch),
        "the market gate is decided before the base gate"
    );

    assert_eq!(book.publish(), before);
    assert_eq!(book.deltas_since_snapshot(), 0);
}

/// The zero-quantity contract, and the one case where the two rules meet.
///
/// A snapshot's zero is a resting level; a delta's zero is a removal. So a delta restating
/// `0` at a coordinate a snapshot rested at zero still mutates: the coordinate leaves the
/// book, which is a change to its presence even though the number is the same.
#[test]
fn delta_zero_removes_where_a_snapshot_zero_rests() {
    let resting_zero = vec![level(Side::Bid, "0.4", "100"), level(Side::Ask, "0.6", "0")];
    let mut book = OrderBook::new(market());
    book.apply_snapshot(&snapshot(resting_zero.clone(), 1))
        .unwrap();
    assert_eq!(
        book.publish().canonical_levels(),
        resting_zero,
        "a snapshot reproduces a zero-quantity level as the venue reported it"
    );

    let commit = book
        .apply_source_delta(&delta(vec![level(Side::Ask, "0.6", "0")], 2))
        .unwrap();
    assert_eq!(
        changes(&commit),
        vec![(Some(level(Side::Ask, "0.6", "0")), None, 0)],
        "the coordinate leaves the book, so its presence changed even at the same number"
    );
    assert_eq!(
        book.publish().canonical_levels(),
        vec![level(Side::Bid, "0.4", "100")]
    );

    let repeated = book
        .apply_source_delta(&delta(vec![level(Side::Ask, "0.6", "0.00")], 3))
        .unwrap();
    assert!(
        repeated.mutations().is_empty(),
        "removing the now-absent coordinate changes nothing"
    );
    assert_eq!(book.revision(), 3);
    assert_eq!(book.authority(), &AuthorityState::Live);
    assert_eq!(book.deltas_since_snapshot(), 2);
}

/// A delta that only restates known state still commits: the venue said the feed is alive.
#[test]
fn delta_restating_known_state_commits_without_mutating() {
    let mut book = based_book();
    let before = book.publish();
    let commit = book
        .apply_source_delta(&delta(
            vec![
                level(Side::Bid, "0.4", "100.000"),
                level(Side::Ask, "0.61", "75"),
            ],
            2,
        ))
        .unwrap();

    assert!(commit.mutations().is_empty());
    let after = book.publish();
    assert_eq!(after.canonical_levels(), before.canonical_levels());
    assert_eq!(after.revision(), before.revision() + 1);
    assert_eq!(after.provenance().unwrap().native_family(), "price_change");
    assert_eq!(
        after.provenance().unwrap().local_revision(),
        after.revision()
    );
    assert_eq!(after.authority(), &AuthorityState::Live);
    assert_eq!(
        after.continuity(),
        &MutationContinuity::Intact {
            epoch: 0,
            next_position: 0,
        },
        "an emitted-nothing delta advances the stream past nothing"
    );
    assert_eq!(book.deltas_since_snapshot(), 1);
}

/// The general mutation record admits exactly two labellings, one per constructor.
#[test]
fn delta_mutation_constructors_refuse_the_wrong_labelling() {
    let source = provenance("price_change", OBSERVED_TIMESTAMP, 1);
    let replacement = level(Side::Bid, "0.4", "150");

    assert_eq!(
        BookMutation::snapshot_diff(source.clone(), None, Some(replacement.clone())),
        Err(ObservationError::InvalidOrigin)
    );
    assert!(BookMutation::source_reported(source.clone(), None, Some(replacement.clone())).is_ok());
    assert_eq!(
        BookMutation::source_reported(source.clone(), None, None),
        Err(ObservationError::EmptyMutation)
    );
    assert_eq!(
        BookMutation::source_reported(
            source,
            Some(level(Side::Bid, "0.39", "50")),
            Some(replacement)
        ),
        Err(ObservationError::MismatchedMutationCoordinate)
    );
}

/// Whether a wait on a receive path is still pending right now: deterministic and
/// timer-free, since the biased select polls the wait first.
async fn recv_is_pending(observer: &mut BookObserver) -> bool {
    tokio::select! {
        biased;
        _ = observer.recv() => false,
        () = std::future::ready(()) => true,
    }
}

fn rebased() -> ObserverRecvError {
    ObserverRecvError::ContinuityLost {
        reason: ContinuityReason::RecoveryBase,
        missed: 0,
    }
}

/// An attachment is never walked across an epoch boundary in silence.
///
/// The divergence checkpoint replaced the book wholesale and published that as state, not as
/// mutations, so an attachment applying mutations incrementally would miss the correction.
/// Every receive path refuses instead, and keeps refusing until the attachment restarts.
#[tokio::test]
async fn delta_observer_is_fenced_at_a_divergence_checkpoint_epoch() {
    let mut writer = BookWriter::new(OrderBook::new(market()), ObserverCapacity::new(64).unwrap());
    let mut observer = writer.attach();
    writer.apply_snapshot(&snapshot(base(), 1)).unwrap();
    writer
        .apply_source_delta(&delta(vec![level(Side::Bid, "0.4", "150")], 2))
        .unwrap();
    while observer.try_recv().unwrap().is_some() {}

    let mut contradicting = base();
    contradicting[1] = level(Side::Bid, "0.4", "900");
    writer
        .apply_snapshot(&snapshot(contradicting.clone(), 3))
        .unwrap();
    assert_eq!(
        observer.try_recv(),
        Ok(None),
        "the checkpoint itself puts nothing on the ring"
    );

    writer
        .apply_source_delta(&delta(vec![level(Side::Bid, "0.4", "901")], 4))
        .unwrap();
    assert_eq!(
        observer.try_recv().unwrap_err(),
        rebased(),
        "the first mutation of the new epoch is refused, not applied"
    );
    assert_eq!(
        observer.state(),
        ConsumerState::Rebased {
            cursor: MutationCursor::new(0, 0),
        },
        "the attachment stopped at the last position it applied, in the epoch it was on"
    );
    assert_eq!(observer.try_recv().unwrap_err(), rebased());
    assert_eq!(observer.recv().await.unwrap_err(), rebased());
    assert_eq!(observer.next_event().await.unwrap_err(), rebased());

    let restarted = observer.reattach();
    assert_eq!(restarted.revision(), 4);
    assert_eq!(restarted.continuity().epoch(), 1);
    assert_eq!(
        restarted.canonical_levels()[1],
        level(Side::Bid, "0.4", "901"),
        "the attachment restarts from state that already contains the correction"
    );
    assert!(matches!(
        observer.state(),
        ConsumerState::Attached {
            continuous: true,
            ..
        }
    ));

    let resumed = writer
        .apply_source_delta(&delta(vec![level(Side::Bid, "0.4", "902")], 5))
        .unwrap();
    let delivery = observer.try_recv().unwrap().unwrap();
    assert_eq!(delivery.cursor(), &MutationCursor::new(1, 1));
    assert_eq!(delivery.revision(), resumed.revision());
    assert!(recv_is_pending(&mut observer).await);
}

/// The same fence on the delta rail: a reported loss, a recovery-base snapshot, then derived
/// diffs in the new epoch.
#[tokio::test]
async fn delta_observer_is_fenced_across_a_reported_loss_recovery_base() {
    let mut writer = BookWriter::new(OrderBook::new(market()), ObserverCapacity::new(64).unwrap());
    let mut observer = writer.attach();
    writer.apply_snapshot(&snapshot(base(), 1)).unwrap();
    writer
        .report_continuity_loss(ContinuityReason::Reconnect, AuthorityReason::Disconnect)
        .unwrap();
    writer.apply_snapshot(&snapshot(base(), 2)).unwrap();
    assert_eq!(
        observer.try_recv(),
        Ok(None),
        "a recovery base derives nothing across the gap"
    );

    let mut moved = base();
    moved[0] = level(Side::Bid, "0.39", "60");
    let commit = writer.apply_snapshot(&snapshot(moved, 3)).unwrap();
    assert_eq!(
        commit.mutations()[0].mutation().provenance().origin(),
        &Origin::LocallyDerived(Derivation::SnapshotDiff)
    );
    assert_eq!(observer.recv().await.unwrap_err(), rebased());
    assert_eq!(
        observer.state(),
        ConsumerState::Rebased {
            cursor: MutationCursor::new(0, 0),
        },
        "the attachment stopped at the last position it applied, in the epoch it was on"
    );

    observer.reattach();
    assert_eq!(observer.try_recv(), Ok(None));
    writer
        .apply_snapshot(&snapshot(base(), 4))
        .unwrap()
        .mutations()
        .first()
        .expect("the book moves back, which is a derived diff in the new epoch");
    assert_eq!(observer.try_recv().unwrap().unwrap().cursor().epoch(), 1);
}

/// Absent and zero are the same depth, so a sync snapshot resting a zero the deltas removed
/// agrees. Anything else would churn the epoch on every checkpoint of a zero-resting venue.
#[test]
fn delta_sync_snapshot_treats_absent_and_zero_rest_as_agreement() {
    let zero_rest = vec![level(Side::Bid, "0.4", "100"), level(Side::Ask, "0.6", "0")];
    let mut book = OrderBook::new(market());
    book.apply_snapshot(&snapshot(zero_rest.clone(), 1))
        .unwrap();
    book.apply_source_delta(&delta(vec![level(Side::Ask, "0.6", "0")], 2))
        .unwrap();
    assert_eq!(
        book.publish().canonical_levels(),
        vec![level(Side::Bid, "0.4", "100")],
        "the delta removed the coordinate the snapshot rested at zero"
    );

    let commit = book
        .apply_snapshot(&snapshot(zero_rest.clone(), 3))
        .unwrap();
    assert_eq!(commit.divergence(), None);
    assert!(commit.mutations().is_empty());
    assert!(!commit.recovery_base());
    assert_eq!(commit.epoch(), 0);
    assert_eq!(
        book.publish().canonical_levels(),
        zero_rest,
        "an agreeing checkpoint adopts the venue's own map, zero-rest levels included"
    );
    assert_eq!(book.publish().sync_divergences(), 0);
    assert_eq!(book.deltas_since_snapshot(), 0);
}

/// The mirror case: a coordinate the book never held, restated as zero by a delta, then
/// rested at zero by the sync snapshot.
#[test]
fn delta_sync_snapshot_agrees_when_a_zero_rest_appears_where_nothing_was_held() {
    let mut book = based_book();
    book.apply_source_delta(&delta(vec![level(Side::Ask, "0.7", "0")], 2))
        .unwrap();

    let mut zero_rest = base();
    zero_rest.push(level(Side::Ask, "0.7", "0"));
    let commit = book
        .apply_snapshot(&snapshot(zero_rest.clone(), 3))
        .unwrap();

    assert_eq!(commit.divergence(), None);
    assert!(commit.mutations().is_empty());
    assert_eq!(commit.epoch(), 0);
    assert_eq!(book.publish().canonical_levels(), zero_rest);
    assert_eq!(book.publish().sync_divergences(), 0);
}

/// A real depth difference still diverges: the economic filter drops zeros, not disagreement.
#[test]
fn delta_sync_snapshot_still_diverges_on_a_nonzero_difference() {
    let mut book = based_book();
    book.apply_source_delta(&delta(vec![level(Side::Ask, "0.61", "0")], 2))
        .unwrap();

    let mut restored = base();
    restored[3] = level(Side::Ask, "0.61", "1");
    let commit = book.apply_snapshot(&snapshot(restored, 3)).unwrap();

    assert_eq!(
        commit.divergence().map(SyncDivergence::deltas_invalidated),
        Some(1)
    );
    assert!(commit.recovery_base());
    assert!(commit.mutations().is_empty());
    assert_eq!(commit.epoch(), 1);
    assert_eq!(book.publish().sync_divergences(), 1);
}

/// A book that has ever taken a source delta stays on the delta rail: every later snapshot
/// is a checkpoint, including one whose window carried no delta at all.
///
/// This is the missed-delta case a per-window counter cannot see. The venue sent a delta the
/// daemon never received, so the window looks snapshot-built while the book is anything but,
/// and deriving a diff here would fabricate exactly the correction the checkpoint exists to
/// expose.
#[test]
fn delta_rail_keeps_checkpointing_after_a_window_with_no_deltas() {
    let mut book = OrderBook::new(market());
    book.apply_snapshot(&snapshot(vec![level(Side::Bid, "0.4", "100")], 1))
        .unwrap();
    assert!(!book.delta_rail());
    book.apply_source_delta(&delta(vec![level(Side::Bid, "0.4", "150")], 2))
        .unwrap();
    assert!(book.delta_rail());

    let agreeing = book
        .apply_snapshot(&snapshot(vec![level(Side::Bid, "0.4", "150")], 3))
        .unwrap();
    assert_eq!(agreeing.divergence(), None);
    assert!(agreeing.mutations().is_empty());
    assert_eq!(
        book.deltas_since_snapshot(),
        0,
        "the agreeing checkpoint closed the window"
    );

    let checkpoint = book
        .apply_snapshot(&snapshot(vec![level(Side::Bid, "0.4", "200")], 4))
        .unwrap();
    assert!(
        checkpoint.mutations().is_empty(),
        "a delta rail never derives a correction diff"
    );
    assert!(checkpoint.recovery_base());
    assert_eq!(checkpoint.epoch(), 1);
    assert_eq!(
        checkpoint
            .divergence()
            .map(SyncDivergence::deltas_invalidated),
        Some(0),
        "the delta that went missing fell in a window that delivered none"
    );
    assert_eq!(book.publish().sync_divergences(), 1);
    assert!(book.delta_rail(), "the rail is never left");
}

/// A zero-rest coordinate appearing and vanishing across checkpoints is agreement, and on a
/// delta rail it can never reopen the derived-diff path even in a window with no delta.
#[test]
fn delta_rail_zero_rest_flip_flop_never_falls_back_to_derived_diffs() {
    let zero_rest = vec![level(Side::Bid, "0.4", "100"), level(Side::Ask, "0.6", "0")];
    let mut book = OrderBook::new(market());
    book.apply_snapshot(&snapshot(zero_rest.clone(), 1))
        .unwrap();
    book.apply_source_delta(&delta(vec![level(Side::Ask, "0.6", "0")], 2))
        .unwrap();
    let agreeing = book.apply_snapshot(&snapshot(zero_rest, 3)).unwrap();
    assert_eq!(agreeing.divergence(), None);
    assert_eq!(book.deltas_since_snapshot(), 0);

    let flipped = vec![level(Side::Bid, "0.4", "100")];
    let commit = book.apply_snapshot(&snapshot(flipped.clone(), 4)).unwrap();

    assert!(
        commit.mutations().is_empty(),
        "a delta rail derives no diff, whatever the window carried"
    );
    assert_eq!(
        commit.divergence(),
        None,
        "absent and zero-rest are the same depth"
    );
    assert!(!commit.recovery_base());
    assert_eq!(commit.epoch(), 0);
    assert_eq!(book.publish().canonical_levels(), flipped);
    assert_eq!(book.publish().sync_divergences(), 0);
}

/// A rebase found with an overrun still unresolved reports the rebase, not the overrun.
///
/// Both facts are real, but only one is actionable: restarting the attachment recovers from
/// the dropped deliveries and the epoch change together, so the reason names the rebase and
/// `missed` carries what the span had dropped.
#[tokio::test]
async fn delta_observer_rebase_dominates_an_unresolved_overrun() {
    let mut writer = BookWriter::new(OrderBook::new(market()), ObserverCapacity::new(2).unwrap());
    let mut observer = writer.attach();
    writer
        .apply_snapshot(&snapshot(vec![level(Side::Bid, "0.4", "100")], 1))
        .unwrap();
    for step in 0..10u64 {
        writer
            .apply_source_delta(&delta(
                vec![level(Side::Bid, "0.4", &(101 + step).to_string())],
                2 + step,
            ))
            .unwrap();
    }

    writer
        .apply_snapshot(&snapshot(vec![level(Side::Bid, "0.4", "500")], 12))
        .unwrap();
    let resumed = writer
        .apply_source_delta(&delta(
            vec![level(Side::Bid, "0.4", "501"), level(Side::Ask, "0.6", "9")],
            13,
        ))
        .unwrap();
    assert_eq!(resumed.epoch(), 1);
    assert_eq!(resumed.mutations().len(), 2);

    let error = observer.try_recv().unwrap_err();
    let ObserverRecvError::ContinuityLost { reason, missed } = error.clone() else {
        panic!("the rebase is reported as a continuity loss");
    };
    assert_eq!(
        reason,
        ContinuityReason::RecoveryBase,
        "the rebase is the actionable fact, not the overrun it dominated"
    );
    assert!(
        missed > 0,
        "the span the ring had dropped is still reported, as {missed}"
    );
    assert_eq!(
        observer.state(),
        ConsumerState::Rebased {
            cursor: MutationCursor::new(0, 0),
        }
    );
    assert_eq!(observer.try_recv().unwrap_err(), error);
    assert_eq!(observer.recv().await.unwrap_err(), error);
    assert_eq!(observer.next_event().await.unwrap_err(), error);

    let restarted = observer.reattach();
    assert_eq!(restarted.revision(), 13);
    assert_eq!(restarted.continuity().epoch(), 1);
    assert_eq!(
        observer.try_recv(),
        Ok(None),
        "reattaching recovers from the drops and the rebase at once"
    );
}

/// The same fence reached through `next_event` itself, on an attachment that has used no
/// other receive path. Only the checkpoint's own state notifications may surface first.
#[tokio::test]
async fn delta_observer_next_event_refuses_the_first_post_rebase_delivery() {
    let mut writer = BookWriter::new(OrderBook::new(market()), ObserverCapacity::new(64).unwrap());
    writer.apply_snapshot(&snapshot(base(), 1)).unwrap();
    writer
        .apply_source_delta(&delta(vec![level(Side::Bid, "0.4", "150")], 2))
        .unwrap();
    let mut observer = writer.attach();

    let mut contradicting = base();
    contradicting[1] = level(Side::Bid, "0.4", "900");
    writer.apply_snapshot(&snapshot(contradicting, 3)).unwrap();
    writer
        .apply_source_delta(&delta(vec![level(Side::Bid, "0.4", "901")], 4))
        .unwrap();

    let mut published = 0u32;
    let error = loop {
        match observer.next_event().await {
            Ok(ObserverEvent::Published(book)) => {
                published += 1;
                assert!(
                    book.revision() >= 3,
                    "only states at or past the checkpoint may surface"
                );
                assert!(published <= 4, "next_event never reached the fence");
            }
            Ok(ObserverEvent::Mutation(delivery)) => panic!(
                "a mutation of the rebased stream was surfaced at {:?}",
                delivery.cursor()
            ),
            Ok(ObserverEvent::Resolution(delivery)) => panic!(
                "this book publishes no resolution, but one was surfaced at {:?}",
                delivery.cursor()
            ),
            Err(error) => break error,
        }
    };
    assert_eq!(
        error,
        ObserverRecvError::ContinuityLost {
            reason: ContinuityReason::RecoveryBase,
            missed: 0,
        }
    );
    assert_eq!(
        observer.state(),
        ConsumerState::Rebased {
            cursor: MutationCursor::new(0, 1),
        }
    );
}

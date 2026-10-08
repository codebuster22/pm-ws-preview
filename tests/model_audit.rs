use pm_ws::*;

fn market(name: &str) -> MarketRef {
    MarketRef::new(
        Venue::new("venue").unwrap(),
        NativeMarketKey::new(NativeIdentifierKind::slug(), name).unwrap(),
    )
}

fn provenance(market: MarketRef, origin: Origin, representation: Representation) -> Provenance {
    Provenance::new(provenance_input(market, origin, representation)).unwrap()
}

fn provenance_input(
    market: MarketRef,
    origin: Origin,
    representation: Representation,
) -> ProvenanceInput {
    ProvenanceInput {
        market,
        outcome: Some(NativeOutcome::side("yes").unwrap()),
        native_family: "book".into(),
        source_timestamp: None,
        source_evidence: BoundedSourceEvidence::new([], SourceEvidenceCapacity::new(0).unwrap())
            .unwrap(),
        daemon_generation: 1,
        connection: ConnectionIdentity::new("connection", 1).unwrap(),
        subscription_generation: 1,
        receive_position: 1,
        commit_position: 1,
        local_receive_time: LocalMonotonicTimestamp::new(1),
        local_commit_time: LocalMonotonicTimestamp::new(1),
        replica: ReplicaRole::PublishingPrimary,
        representation,
        origin,
        local_revision: 1,
        continuity_epoch: 1,
    }
}

#[test]
fn provenance_requires_a_truthful_representation_and_origin_pair() {
    for (representation, origin, valid) in [
        (Representation::VenueNative, Origin::SourceReported, true),
        (
            Representation::VenueNative,
            Origin::NormalizedFromSource,
            false,
        ),
        (
            Representation::VenueNative,
            Origin::LocallyDerived(Derivation::SnapshotDiff),
            false,
        ),
        (Representation::Normalized, Origin::SourceReported, false),
        (
            Representation::Normalized,
            Origin::NormalizedFromSource,
            true,
        ),
        (
            Representation::Normalized,
            Origin::LocallyDerived(Derivation::SnapshotDiff),
            true,
        ),
    ] {
        assert_eq!(
            Provenance::new(provenance_input(market("m"), origin, representation)).is_ok(),
            valid
        );
    }
}

#[test]
fn bounded_levels_and_evidence_reject_overflow() {
    let grammar = DecimalGrammar::new(4, 10, true, false).unwrap();
    let level = Level::new(
        Side::Bid,
        Price::parse("1", grammar).unwrap(),
        Quantity::parse("1", grammar).unwrap(),
    );
    assert!(BoundedLevels::new([level.clone(), level], LevelCapacity::new(1).unwrap()).is_err());
    assert!(
        BoundedSourceEvidence::new(
            [
                SourceEvidence::sequence("a").unwrap(),
                SourceEvidence::sequence("b").unwrap()
            ],
            SourceEvidenceCapacity::new(1).unwrap()
        )
        .is_err()
    );
}

#[test]
fn recovery_base_unavailable_is_available_to_authority() {
    let reason = AuthorityReason::RecoveryBaseUnavailable;
    assert_eq!(
        serde_json::to_value(AuthorityReason::Gap).unwrap(),
        serde_json::json!("Gap")
    );
    assert_eq!(
        serde_json::to_value(&reason).unwrap(),
        serde_json::json!("recovery_base_unavailable")
    );
    assert_eq!(
        serde_json::from_value::<AuthorityReason>(serde_json::json!("recovery_base_unavailable"))
            .unwrap(),
        reason
    );
    assert_eq!(
        serde_json::to_value(AuthorityState::Stale(reason)).unwrap(),
        serde_json::json!({"Stale":"recovery_base_unavailable"})
    );
}

#[test]
fn liquidity_views_are_explicit() {
    let first = market("first");
    let second = market("second");
    assert_eq!(
        LiquidityIdentity::Market(first.clone()),
        LiquidityIdentity::Market(first)
    );
    assert_eq!(
        LiquidityIdentity::Outcome(OutcomeRef::new(
            second.clone(),
            NativeOutcome::side("yes").unwrap(),
        )),
        LiquidityIdentity::Outcome(OutcomeRef::new(second, NativeOutcome::side("yes").unwrap()))
    );
}

#[test]
fn lifecycle_and_mutations_reject_derived_or_invalid_coordinates() {
    let derived = provenance(
        market("m"),
        Origin::LocallyDerived(Derivation::SnapshotDiff),
        Representation::Normalized,
    );
    assert!(
        ResolutionObservation::new(
            derived.clone(),
            NativeOutcome::side("yes").unwrap(),
            NativeLabel::new("resolved").unwrap(),
            DeliveryPath::ResolutionFeed
        )
        .is_err()
    );
    assert!(BookMutation::snapshot_diff(derived, None, None).is_err());
}

#[test]
fn candidates_keep_shared_source_provenance_and_distinct_operations() {
    let source = provenance(
        market("m"),
        Origin::SourceReported,
        Representation::VenueNative,
    );
    let grammar = DecimalGrammar::new(4, 10, true, false).unwrap();
    let level = Level::new(
        Side::Bid,
        Price::parse("1", grammar).unwrap(),
        Quantity::parse("2", grammar).unwrap(),
    );
    let snapshot = Candidate::snapshot(
        source.clone(),
        BoundedLevels::new([level.clone()], LevelCapacity::new(1).unwrap()).unwrap(),
    )
    .unwrap();
    let delta = Candidate::source_delta(
        source.clone(),
        BoundedLevels::new([level], LevelCapacity::new(1).unwrap()).unwrap(),
    )
    .unwrap();
    assert_eq!(snapshot.provenance(), &source);
    assert_eq!(delta.provenance(), &source);
    assert!(matches!(
        snapshot.operation(),
        CandidateOperation::Snapshot(_)
    ));
    assert!(matches!(
        delta.operation(),
        CandidateOperation::SourceDelta(_)
    ));
}

#[test]
fn source_delta_requires_venue_native_source_provenance() {
    let normalized = provenance(
        market("m"),
        Origin::NormalizedFromSource,
        Representation::Normalized,
    );
    let levels = BoundedLevels::new([], LevelCapacity::new(1).unwrap()).unwrap();
    assert_eq!(
        Candidate::source_delta(normalized, levels),
        Err(ObservationError::InvalidOrigin)
    );
}

#[test]
fn provenance_retains_original_numeric_lexeme_and_source_timestamp() {
    let market = market("m");
    let original = OriginalDecimalLexeme::new("1.200").unwrap();
    let evidence = SourceEvidence::numeric_lexeme(
        SourceFieldPath::new("levels[0].price").unwrap(),
        original.clone(),
    );
    let provenance = Provenance::new(ProvenanceInput {
        market,
        outcome: Some(NativeOutcome::side("yes").unwrap()),
        native_family: "book".into(),
        source_timestamp: Some(SourceTimestamp::new("2026-08-26T00:00:00.000Z").unwrap()),
        source_evidence: BoundedSourceEvidence::new(
            [evidence],
            SourceEvidenceCapacity::new(1).unwrap(),
        )
        .unwrap(),
        daemon_generation: 1,
        connection: ConnectionIdentity::new("connection", 1).unwrap(),
        subscription_generation: 1,
        receive_position: 1,
        commit_position: 2,
        local_receive_time: LocalMonotonicTimestamp::new(1),
        local_commit_time: LocalMonotonicTimestamp::new(2),
        replica: ReplicaRole::PublishingPrimary,
        representation: Representation::VenueNative,
        origin: Origin::SourceReported,
        local_revision: 1,
        continuity_epoch: 1,
    })
    .unwrap();
    assert_eq!(
        provenance.source_timestamp().unwrap().as_lexeme(),
        "2026-08-26T00:00:00.000Z"
    );
    let SourceEvidence::NumericLexeme { field, lexeme } = &provenance.source_evidence()[0] else {
        panic!("expected numeric lexeme evidence")
    };
    assert_eq!(field.as_str(), "levels[0].price");
    assert_eq!(lexeme, &original);
    let grammar = DecimalGrammar::new(4, 10, true, false).unwrap();
    assert_eq!(
        ExactDecimal::parse(lexeme.as_str(), grammar).unwrap(),
        ExactDecimal::parse("1.2", grammar).unwrap()
    );
}

#[test]
fn source_state_rejects_primary_and_duplicate_standbys_before_capacity() {
    let primary = PublishingPrimary::new(ConnectionIdentity::new("primary", 1).unwrap());
    assert_eq!(
        SourceState::with_hot_standbys(
            primary.clone(),
            [HotStandby::new(
                ConnectionIdentity::new("primary", 1).unwrap()
            )],
            1,
        ),
        Err(ReplicaError::PrimaryUsedAsStandby)
    );
    let standby = HotStandby::new(ConnectionIdentity::new("standby", 1).unwrap());
    assert_eq!(
        SourceState::with_hot_standbys(primary.clone(), [standby.clone(), standby], 1),
        Err(ReplicaError::DuplicateStandby)
    );
    assert_eq!(
        SourceState::with_hot_standbys(
            primary.clone(),
            [
                HotStandby::new(ConnectionIdentity::new("one", 1).unwrap()),
                HotStandby::new(ConnectionIdentity::new("two", 1).unwrap()),
            ],
            1,
        ),
        Err(ReplicaError::StandbyCapacityExceeded)
    );
    assert_eq!(
        SourceState::with_hot_standbys(primary, [], 0),
        Err(ReplicaError::StandbyCapacityZero)
    );
}

#[test]
fn normalized_observations_and_derived_mutations_preserve_traceability() {
    let grammar = DecimalGrammar::new(4, 10, true, false).unwrap();
    let old = Level::new(
        Side::Bid,
        Price::parse("1", grammar).unwrap(),
        Quantity::parse("2", grammar).unwrap(),
    );
    let replacement = Level::new(
        Side::Bid,
        Price::parse("1", grammar).unwrap(),
        Quantity::parse("3", grammar).unwrap(),
    );
    let derived_provenance = provenance(
        market("m"),
        Origin::LocallyDerived(Derivation::SnapshotDiff),
        Representation::Normalized,
    );
    let mutation = BookMutation::snapshot_diff(
        derived_provenance.clone(),
        Some(old.clone()),
        Some(replacement.clone()),
    )
    .unwrap();
    assert_eq!(mutation.provenance(), &derived_provenance);
    assert_eq!(mutation.old(), Some(&old));
    assert_eq!(mutation.replacement(), Some(&replacement));

    let event_provenance = provenance(
        market("m"),
        Origin::NormalizedFromSource,
        Representation::Normalized,
    );
    let path = DeliveryPath::OtherNativePath(NativeDeliveryPath::new("status-topic").unwrap());

    let winner = NativeOutcome::side("yes").unwrap();
    let resolution_label = NativeLabel::new("native-resolved").unwrap();
    let resolution = ResolutionObservation::new(
        event_provenance.clone(),
        winner.clone(),
        resolution_label.clone(),
        path.clone(),
    )
    .unwrap();
    assert_eq!(resolution.provenance(), &event_provenance);
    assert_eq!(resolution.winner(), &winner);
    assert_eq!(resolution.native_label(), &resolution_label);
    assert_eq!(resolution.delivery_path(), &path);
}

#[test]
fn recovering_a_mutation_stream_advances_the_epoch_and_refuses_to_wrap() {
    let lost = MutationContinuity::Intact {
        epoch: 3,
        next_position: 8,
    }
    .lost(ContinuityReason::Gap);
    assert_eq!(
        lost.recovered(99).unwrap(),
        MutationContinuity::Intact {
            epoch: 4,
            next_position: 99,
        }
    );
    assert_eq!(
        MutationContinuity::Lost {
            epoch: u64::MAX,
            reason: ContinuityReason::Gap,
        }
        .recovered(0),
        Err(ReplicaError::EpochOverflow)
    );
}

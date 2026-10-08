use pm_ws::*;
use proptest::prelude::*;

proptest! {
    #[test]
    fn canonical_decimal_round_trips(value in 0u32..1_000_000) {
        let grammar = DecimalGrammar::new(12, 20, true, false).unwrap();
        let original = ExactDecimal::parse(&format!("{value}.00"), grammar).unwrap();
        prop_assert_eq!(ExactDecimal::parse(&original.canonical(), grammar).unwrap(), original);
    }
}

#[test]
fn hostile_identity_json_cannot_bypass_checked_constructors() {
    assert!(serde_json::from_str::<Venue>("\"\"").is_err());
    assert!(serde_json::from_str::<Venue>(&format!("\"{}\"", "x".repeat(129))).is_err());
    assert!(serde_json::from_str::<NativeOutcome>(r#"{"kind":"Side","value":""}"#).is_err());
    assert!(
        serde_json::from_str::<NativeOutcome>(&format!(
            r#"{{"kind":"Token","value":"{}"}}"#,
            "x".repeat(1025)
        ))
        .is_err()
    );
    assert!(serde_json::from_str::<NativeMarketKey>(r#"{"kind":"","value":"m"}"#).is_err());
}

#[test]
fn hostile_numeric_json_cannot_create_negative_authoritative_values() {
    let nonnegative = DecimalGrammar::new(12, 20, true, false).unwrap();
    assert!(Price::parse("-1", nonnegative).is_err());
    assert!(Quantity::parse("-1", nonnegative).is_err());
    assert!(Depth::parse("-1", nonnegative).is_err());
    let grammar = DecimalGrammar::new(4, 10, true, true)
        .unwrap()
        .with_limits(8, 3, 10)
        .unwrap();
    assert!(matches!(
        ExactDecimal::parse("123456789", grammar),
        Err(DecimalError::LexemeTooLong)
    ));
    assert!(matches!(
        ExactDecimal::parse("1e100", grammar),
        Err(DecimalError::ExponentTooLarge)
    ));
}

#[test]
fn descriptor_and_provenance_decode_reapply_bounds() {
    assert!(serde_json::from_str::<SourceDigest>("\"\"").is_err());
    assert!(serde_json::from_str::<ConnectionIdentity>(r#"{"value":"","generation":1}"#).is_err());
}

#[test]
fn checked_observation_and_state_transitions_reject_invalid_pairs() {
    let market = MarketRef::new(
        Venue::new("venue").unwrap(),
        NativeMarketKey::new(NativeIdentifierKind::slug(), "market").unwrap(),
    );
    let provenance = Provenance::new(ProvenanceInput {
        market,
        outcome: Some(NativeOutcome::side("yes").unwrap()),
        native_family: "book".into(),
        source_timestamp: Some(SourceTimestamp::new("2026-08-26T00:00:00Z").unwrap()),
        source_evidence: BoundedSourceEvidence::new(
            [SourceEvidence::sequence("1").unwrap()],
            SourceEvidenceCapacity::new(1).unwrap(),
        )
        .unwrap(),
        daemon_generation: 1,
        connection: ConnectionIdentity::new("connection", 1).unwrap(),
        subscription_generation: 1,
        receive_position: 1,
        commit_position: 1,
        local_receive_time: LocalMonotonicTimestamp::new(1),
        local_commit_time: LocalMonotonicTimestamp::new(1),
        replica: ReplicaRole::PublishingPrimary,
        representation: Representation::Normalized,
        origin: Origin::LocallyDerived(Derivation::SnapshotDiff),
        local_revision: 1,
        continuity_epoch: 1,
    })
    .unwrap();
    assert!(
        Candidate::source_delta(
            provenance,
            BoundedLevels::new([], LevelCapacity::new(1).unwrap()).unwrap()
        )
        .is_err()
    );
    assert!(LevelCapacity::new(0).is_err());
}

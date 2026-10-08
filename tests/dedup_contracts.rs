#![forbid(unsafe_code)]

//! Contracts for the venue-agnostic dedup and ordering-evidence seam: what a venue key is
//! allowed to mean, what a classification says about two keys, what a content digest
//! fingerprints, and — against the scripted controlled peer — that carrying a key changes
//! nothing about publishing.
//!
//! Suppression is pool behaviour, and a pool is opt-in per run against recorded conformance
//! evidence, so the claim proven here is the one that holds without it: in a run with no
//! pool a repeated key is recorded and published exactly as any other frame. What a pool
//! does with one is pinned in `pool_contracts.rs`.

mod support;

use pm_ws::limitless::supervisor::{
    Stopper, Supervisor, SupervisorConfig, SupervisorNotice, SupervisorStats,
};
use pm_ws::limitless::{LimitlessEvent, ORDERBOOK_UPDATE_DEDUP_KEY, decode_event};
use pm_ws::wire::lexical::LexicalLimits;
use pm_ws::wire::socketio::{WebSocketOpcode, decode_frame};
use pm_ws::{
    AuthorityState, BookObserver, BoundedLevels, BoundedSourceEvidence, Candidate,
    ConnectionIdentity, ContentDigest, DecimalGrammar, DedupKey, DedupKeyError, DedupKeySemantics,
    KeyRelation, Level, LevelCapacity, LocalMonotonicTimestamp, MarketRef, NativeIdentifierKind,
    NativeMarketKey, OrderBook, Origin, Price, Provenance, ProvenanceInput, PublishedBook,
    Quantity, ReplicaRole, Representation, Side, SourceEvidence, SourceEvidenceCapacity,
    SourceEvidenceValue, SourceTimestamp, Venue, candidate_digest, classify_key, content_digest,
};
use std::time::Duration;
use support::controlled_peer::{ControlledPeer, PeerConfig};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;

const SLUG: &str = "btc-up-or-down-5-min-1788172500";
const REPEATED_VERSION: u64 = 77;
const STEP_TIMEOUT: Duration = Duration::from_secs(10);
const RUN_CAP: Duration = Duration::from_secs(120);
const TAP_CAPACITY: usize = 1024;
const PATIENT_HEARTBEAT_MS: u64 = 60_000;

fn key(value: u64) -> DedupKey {
    DedupKey::integer(value)
}

fn text(value: &str) -> DedupKey {
    DedupKey::lexeme(value).expect("test lexeme keys are printable and bounded")
}

/// The classification a venue's declaration licenses, case by case: order is reported only
/// where a declaration grants it and the keys are comparable, equality is a duplicate
/// everywhere because equality is the one meaning every venue key carries, and a first key
/// is evidence of nothing.
#[test]
fn dedup_key_classification_matrix_is_pinned() {
    use DedupKeySemantics::{DedupOnly, OrderingProven, SessionMonotone};
    use KeyRelation::{Advance, Duplicate, First, Inversion, Unordered};
    let seen = |semantics, last: Option<DedupKey>, next: DedupKey| {
        classify_key(semantics, last.as_ref(), &next)
    };
    assert_eq!(seen(DedupOnly, None, key(1)), First, "dedup-only first");
    assert_eq!(
        seen(SessionMonotone, None, key(500)),
        First,
        "monotone first"
    );
    assert_eq!(seen(OrderingProven, None, key(500)), First, "proven first");
    assert_eq!(seen(DedupOnly, None, text("a1")), First, "first lexeme");
    let repeat = Some(key(7));
    assert_eq!(
        seen(DedupOnly, repeat.clone(), key(7)),
        Duplicate,
        "dedup-only repeat"
    );
    assert_eq!(
        seen(SessionMonotone, repeat.clone(), key(7)),
        Duplicate,
        "monotone repeat"
    );
    assert_eq!(
        seen(OrderingProven, repeat, key(7)),
        Duplicate,
        "proven repeat"
    );
    let repeated_text = Some(text("a1"));
    assert_eq!(
        seen(SessionMonotone, repeated_text, text("a1")),
        Duplicate,
        "lexeme repeat"
    );
    assert_eq!(
        seen(DedupOnly, Some(key(1)), key(2)),
        Unordered,
        "dedup-only later"
    );
    assert_eq!(
        seen(DedupOnly, Some(key(2)), key(1)),
        Unordered,
        "dedup-only earlier"
    );
    assert_eq!(
        seen(SessionMonotone, Some(key(1)), key(2)),
        Advance,
        "monotone advance"
    );
    assert_eq!(
        seen(SessionMonotone, Some(key(526_135)), key(564_536)),
        Advance,
        "monotone jump"
    );
    assert_eq!(
        seen(SessionMonotone, Some(key(2)), key(1)),
        Inversion,
        "monotone inversion"
    );
    assert_eq!(
        seen(OrderingProven, Some(key(1)), key(2)),
        Advance,
        "proven advance"
    );
    assert_eq!(
        seen(OrderingProven, Some(key(9)), key(4)),
        Inversion,
        "proven inversion"
    );
    assert_eq!(
        seen(SessionMonotone, Some(key(0)), key(u64::MAX)),
        Advance,
        "range advance"
    );
    assert_eq!(
        seen(SessionMonotone, Some(key(u64::MAX)), key(0)),
        Inversion,
        "wrap to zero"
    );
    assert_eq!(
        seen(SessionMonotone, Some(text("a")), text("b")),
        Unordered,
        "lexemes unordered"
    );
    assert_eq!(
        seen(OrderingProven, Some(text("b")), text("a")),
        Unordered,
        "lexemes, proven"
    );
    assert_eq!(
        seen(SessionMonotone, Some(key(7)), text("7")),
        Unordered,
        "integer vs spelling"
    );
    assert_eq!(
        seen(SessionMonotone, Some(text("7")), key(7)),
        Unordered,
        "spelling vs integer"
    );
}

/// A run of keys classifies stepwise: the first is evidence of nothing, a rising run
/// advances, a repeat is a duplicate, and a step backwards is the inversion a pool would
/// trip on.
#[test]
fn dedup_key_classification_walks_a_session_run() {
    let mut last: Option<DedupKey> = None;
    let mut seen = Vec::new();
    for step in [10, 11, 40, 40, 39, 41] {
        let next = key(step);
        seen.push(classify_key(
            DedupKeySemantics::SessionMonotone,
            last.as_ref(),
            &next,
        ));
        last = Some(next);
    }
    use KeyRelation::{Advance, Duplicate, First, Inversion};
    assert_eq!(
        seen,
        vec![First, Advance, Advance, Duplicate, Inversion, Advance]
    );
}

/// An integer key is the venue's integer or nothing: every lexeme that is not a bare
/// non-negative decimal integer is refused rather than coerced, and a value beyond the
/// representable range is refused rather than truncated.
#[test]
fn integer_dedup_keys_admit_only_exact_integer_lexemes() {
    use DedupKeyError::{Empty, NotAnExactInteger, OutOfRange};
    let cases: &[(&str, Result<DedupKey, DedupKeyError>)] = &[
        ("7861372", Ok(key(7_861_372))),
        ("0", Ok(key(0))),
        ("007", Ok(key(7))),
        ("18446744073709551615", Ok(key(u64::MAX))),
        ("18446744073709551616", Err(OutOfRange)),
        ("", Err(Empty)),
        ("+7", Err(NotAnExactInteger)),
        ("-7", Err(NotAnExactInteger)),
        ("7.0", Err(NotAnExactInteger)),
        ("7e3", Err(NotAnExactInteger)),
        ("0x7", Err(NotAnExactInteger)),
        ("7_000", Err(NotAnExactInteger)),
        (" 7", Err(NotAnExactInteger)),
        ("7 ", Err(NotAnExactInteger)),
    ];
    for (lexeme, expected) in cases {
        assert_eq!(
            &DedupKey::parse_exact_integer(lexeme),
            expected,
            "the exact-integer key parse of {lexeme:?} changed"
        );
    }
}

/// A lexeme key stays a single token a conformance record can carry, and renders as the
/// venue's own value.
#[test]
fn lexeme_dedup_keys_stay_single_printable_tokens() {
    assert_eq!(DedupKey::lexeme(""), Err(DedupKeyError::Empty));
    assert_eq!(
        DedupKey::lexeme("a b"),
        Err(DedupKeyError::UnsupportedCharacter)
    );
    assert_eq!(
        DedupKey::lexeme("a\nb"),
        Err(DedupKeyError::UnsupportedCharacter)
    );
    assert_eq!(
        DedupKey::lexeme("x".repeat(pm_ws::MAX_DEDUP_LEXEME_BYTES + 1)),
        Err(DedupKeyError::TooLong)
    );
    assert_eq!(text("01J8Z-9f").to_string(), "01J8Z-9f");
    assert_eq!(key(7_861_372).to_string(), "7861372");
    assert_eq!(key(7).as_integer(), Some(7));
    assert_eq!(key(7).as_lexeme(), None);
    assert_eq!(text("7").as_integer(), None);
    assert_eq!(text("7").as_lexeme(), Some("7"));
}

fn orderbook_update(version_field: &str) -> pm_ws::limitless::OrderbookUpdate {
    let frame = format!(
        "42/markets,[\"orderbookUpdate\",{{\"marketSlug\":\"{SLUG}\",\"orderbook\":{{\"bids\":[{{\"price\":0.5,\"size\":100}}],\"asks\":[]}},\"timestamp\":\"2026-08-31T00:00:00.000Z\"{version_field}}}]"
    );
    let decoded = decode_frame(
        frame.as_bytes(),
        WebSocketOpcode::Text,
        LexicalLimits::venue_payload(),
    )
    .expect("the frame decodes at the wire layer");
    let LimitlessEvent::OrderbookUpdate(update) =
        decode_event(&decoded).expect("the payload decodes as a book update")
    else {
        panic!("expected orderbookUpdate");
    };
    update
}

/// A venue value the key seam refuses costs only the key: the frame still decodes, its book
/// content is untouched, and the venue's own lexeme still reaches provenance evidence
/// exactly as reported. A frame carrying no key at all stays a different fact from one
/// whose key could not be represented.
#[test]
fn a_version_the_key_seam_refuses_still_decodes_and_still_reaches_provenance() {
    let fractional = orderbook_update(",\"version\":1.5");
    assert_eq!(fractional.version(), Some("1.5"));
    assert_eq!(fractional.bids().len(), 1);
    assert_eq!(
        fractional.dedup_key(),
        Err(DedupKeyError::NotAnExactInteger)
    );
    assert_eq!(
        fractional
            .version_evidence()
            .expect("the lexeme is within the evidence bound"),
        Some(SourceEvidence::Version(value("1.5")))
    );

    let versionless = orderbook_update("");
    assert_eq!(versionless.version(), None);
    assert_eq!(versionless.dedup_key(), Ok(None));
    assert_eq!(versionless.bids().len(), 1);
}

fn value(lexeme: &str) -> SourceEvidenceValue {
    SourceEvidenceValue::new(lexeme).expect("test evidence lexemes are bounded")
}

fn grammar() -> DecimalGrammar {
    DecimalGrammar::new(18, 30, true, false).expect("the venue decimal grammar is valid")
}

fn market(slug: &str) -> MarketRef {
    MarketRef::new(
        Venue::new("limitless").expect("venue"),
        NativeMarketKey::new(NativeIdentifierKind::slug(), slug).expect("slug"),
    )
}

fn book_provenance(slug: &str) -> Provenance {
    Provenance::new(ProvenanceInput {
        market: market(slug),
        outcome: None,
        native_family: "orderbookUpdate".into(),
        source_timestamp: Some(SourceTimestamp::new("2026-08-31T00:00:00.000Z").expect("stamp")),
        source_evidence: BoundedSourceEvidence::new(
            [SourceEvidence::Version(value("77"))],
            SourceEvidenceCapacity::new(1).expect("capacity"),
        )
        .expect("evidence"),
        daemon_generation: 1,
        connection: ConnectionIdentity::new("controlled-peer", 1).expect("connection"),
        subscription_generation: 1,
        receive_position: 1,
        commit_position: 1,
        local_receive_time: LocalMonotonicTimestamp::new(1),
        local_commit_time: LocalMonotonicTimestamp::new(1),
        replica: ReplicaRole::PublishingPrimary,
        representation: Representation::VenueNative,
        origin: Origin::SourceReported,
        local_revision: 0,
        continuity_epoch: 0,
    })
    .expect("test provenance is valid")
}

fn published(slug: &str, levels: &[(Side, &str, &str)]) -> PublishedBook {
    let levels = levels.iter().map(|(side, price, quantity)| {
        Level::new(
            *side,
            Price::parse(price, grammar()).expect("price"),
            Quantity::parse(quantity, grammar()).expect("quantity"),
        )
    });
    let candidate = Candidate::snapshot(
        book_provenance(slug),
        BoundedLevels::new(levels, LevelCapacity::new(64).expect("capacity")).expect("levels"),
    )
    .expect("snapshot candidate");
    let mut book = OrderBook::new(market(slug));
    book.apply_snapshot(&candidate).expect("snapshot applies");
    book.publish()
}

/// One snapshot candidate for `slug`, stamped with `connection` so two sockets reporting
/// one venue frame differ in everything but what they reported.
fn snapshot_candidate(slug: &str, connection: u64, levels: &[(Side, &str, &str)]) -> Candidate {
    let mut provenance = ProvenanceInput {
        market: market(slug),
        outcome: None,
        native_family: "orderbookUpdate".into(),
        source_timestamp: Some(SourceTimestamp::new("2026-08-31T00:00:00.000Z").expect("stamp")),
        source_evidence: BoundedSourceEvidence::new(
            [SourceEvidence::Version(value("77"))],
            SourceEvidenceCapacity::new(1).expect("capacity"),
        )
        .expect("evidence"),
        daemon_generation: 1,
        connection: ConnectionIdentity::new("controlled-peer", connection).expect("connection"),
        subscription_generation: connection,
        receive_position: connection,
        commit_position: connection,
        local_receive_time: LocalMonotonicTimestamp::new(connection),
        local_commit_time: LocalMonotonicTimestamp::new(connection),
        replica: ReplicaRole::PublishingPrimary,
        representation: Representation::VenueNative,
        origin: Origin::SourceReported,
        local_revision: 0,
        continuity_epoch: 0,
    };
    if connection > 1 {
        provenance.replica = ReplicaRole::HotStandby;
    }
    let levels = levels.iter().map(|(side, price, quantity)| {
        Level::new(
            *side,
            Price::parse(price, grammar()).expect("price"),
            Quantity::parse(quantity, grammar()).expect("quantity"),
        )
    });
    Candidate::snapshot(
        Provenance::new(provenance).expect("test provenance is valid"),
        BoundedLevels::new(levels, LevelCapacity::new(64).expect("capacity")).expect("levels"),
    )
    .expect("snapshot candidate")
}

/// What the pool's tripwire compares: two sockets reporting one venue frame must fingerprint
/// alike, or "equal keys carry equal content" could not be checked at all. Nothing about how
/// an arrival was stamped may enter the fingerprint.
#[test]
fn a_candidate_digest_fingerprints_what_was_reported_and_not_who_reported_it() {
    let levels = [(Side::Bid, "0.5", "100"), (Side::Ask, "0.6", "200.0")];
    let first = snapshot_candidate(SLUG, 1, &levels);
    let second = snapshot_candidate(SLUG, 2, &levels);
    assert_eq!(
        candidate_digest(&first),
        candidate_digest(&second),
        "two sockets delivering one frame must fingerprint alike whatever else differs"
    );
    let restated = snapshot_candidate(
        SLUG,
        1,
        &[(Side::Bid, "0.50", "100.000"), (Side::Ask, "0.600", "200")],
    );
    assert_eq!(
        candidate_digest(&first),
        candidate_digest(&restated),
        "the same economic content spelled differently is the same content"
    );
    let deeper = snapshot_candidate(
        SLUG,
        1,
        &[(Side::Bid, "0.5", "101"), (Side::Ask, "0.6", "200")],
    );
    assert_ne!(
        candidate_digest(&first),
        candidate_digest(&deeper),
        "a genuine depth difference under one key is what the tripwire exists to catch"
    );
    let elsewhere = snapshot_candidate("eth-up-or-down-hourly-1788213600", 1, &levels);
    assert_ne!(
        candidate_digest(&first),
        candidate_digest(&elsewhere),
        "one key on two markets is not one frame"
    );
}

/// The two digest spaces are disjoint by construction, so a candidate fingerprint can never
/// be mistaken for a book fingerprint even where both describe the same levels. Each answers
/// a different question — what one arrival said, and what a book now holds — and neither is
/// ever compared with the other.
#[test]
fn a_candidate_digest_and_a_book_digest_never_answer_for_each_other() {
    let levels = [(Side::Bid, "0.5", "100"), (Side::Ask, "0.6", "200")];
    let candidate = snapshot_candidate(SLUG, 1, &levels);
    let book = published(SLUG, &levels);
    assert_ne!(
        candidate_digest(&candidate).value(),
        content_digest(&book).value(),
        "the candidate fingerprint absorbs which operation it carries where a book \
         fingerprint absorbs its levels, so the two spaces cannot collide into each other"
    );
}

/// The comparison the standby agreement performs, restated from its documented basis so the
/// digest can be held to it: the market, and every canonical level's side, price, and
/// quantity by exact decimal value. The agreement itself stays that exact comparison — the
/// digest is diagnostic evidence and never stands in for it — so this reference is what
/// ties the two together.
fn economically_equal_reference(left: &PublishedBook, right: &PublishedBook) -> bool {
    left.market() == right.market()
        && left.canonical_levels().len() == right.canonical_levels().len()
        && left
            .canonical_levels()
            .iter()
            .zip(right.canonical_levels().iter())
            .all(|(left, right)| {
                left.side() == right.side()
                    && left.price().value().cmp(right.price().value()).is_eq()
                    && left
                        .quantity()
                        .value()
                        .cmp(right.quantity().value())
                        .is_eq()
            })
}

/// A content digest agrees with the economic comparison on every pair: two books that are
/// the same economic state fingerprint alike whatever their decimal spelling, and any
/// genuine difference in market, level set, price, side, or depth fingerprints differently.
#[test]
fn content_digest_tracks_the_economic_comparison_basis() {
    let books = [
        (
            "baseline",
            published(
                SLUG,
                &[(Side::Bid, "0.5", "100"), (Side::Ask, "0.6", "200")],
            ),
        ),
        (
            "same state, other decimal spelling",
            published(
                SLUG,
                &[(Side::Bid, "0.50", "100.0"), (Side::Ask, "0.600", "200.00")],
            ),
        ),
        (
            "deeper bid",
            published(
                SLUG,
                &[(Side::Bid, "0.5", "150"), (Side::Ask, "0.6", "200")],
            ),
        ),
        (
            "other bid price",
            published(
                SLUG,
                &[(Side::Bid, "0.4", "100"), (Side::Ask, "0.6", "200")],
            ),
        ),
        (
            "one side only",
            published(SLUG, &[(Side::Bid, "0.5", "100")]),
        ),
        (
            "same levels on the other side",
            published(
                SLUG,
                &[(Side::Ask, "0.5", "100"), (Side::Ask, "0.6", "200")],
            ),
        ),
        (
            "other market",
            published(
                "btc-up-or-down-5-min-1788172800",
                &[(Side::Bid, "0.5", "100"), (Side::Ask, "0.6", "200")],
            ),
        ),
    ];
    for (left_name, left) in &books {
        for (right_name, right) in &books {
            assert_eq!(
                content_digest(left) == content_digest(right),
                economically_equal_reference(left, right),
                "the digest of {left_name} against {right_name} disagrees with the economic \
                 comparison"
            );
        }
    }
    let rendered = content_digest(&books[0].1).to_string();
    assert_eq!(
        rendered.len(),
        16,
        "a record's digest is a fixed-width token"
    );
    assert!(rendered.bytes().all(|byte| byte.is_ascii_hexdigit()));
}

/// One accepted-update conformance record, as an offline analysis would read it.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Record {
    position: u64,
    generation: u64,
    replica: ReplicaRole,
    key: Result<Option<DedupKey>, DedupKeyError>,
    digest: ContentDigest,
}

/// What one scripted run leaves behind: what consumers saw, and what the conformance tap
/// recorded.
struct Outcome {
    levels: Vec<(Side, String, String)>,
    revision: u64,
    evidence: Vec<SourceEvidence>,
    records: Vec<Record>,
    stats: SupervisorStats,
}

struct Running {
    observer: BookObserver,
    tap: mpsc::Receiver<SupervisorNotice>,
    stop: Stopper,
    handle: JoinHandle<SupervisorStats>,
}

fn start(endpoint: String, log_dedup_keys: bool) -> Running {
    let (tap_tx, tap) = mpsc::channel(TAP_CAPACITY);
    let config = SupervisorConfig {
        endpoint,
        market: SLUG.to_owned(),
        setup_timeout: Duration::from_secs(10),
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(40),
        exhausted_backoff: Duration::from_millis(40),
        log_dedup_keys,
        ..SupervisorConfig::default()
    };
    let mut supervisor = Supervisor::new(config)
        .expect("test supervisor configuration is valid")
        .with_diagnostics(tap_tx);
    let observer = supervisor.attach();
    let stop = supervisor.stopper();
    let handle = tokio::spawn(async move { supervisor.run_until(Instant::now() + RUN_CAP).await });
    Running {
        observer,
        tap,
        stop,
        handle,
    }
}

async fn await_bid(observer: &mut BookObserver, quantity: &str) {
    let deadline = Instant::now() + STEP_TIMEOUT;
    loop {
        let latest = observer.latest();
        let matched = latest.authority() == &AuthorityState::Live
            && latest.canonical_levels().iter().any(|level| {
                level.side() == Side::Bid && level.quantity().value().canonical() == quantity
            });
        if matched {
            return;
        }
        match tokio::time::timeout_at(deadline, observer.state_changed()).await {
            Err(_) => panic!(
                "timed out waiting for a live book with bid quantity {quantity}; last seen \
                 revision={} authority={:?}",
                latest.revision(),
                latest.authority()
            ),
            Ok(Err(_)) => panic!("the book writer went away"),
            Ok(Ok(_)) => {}
        }
    }
}

fn drain_records(tap: &mut mpsc::Receiver<SupervisorNotice>, records: &mut Vec<Record>) {
    while let Ok(notice) = tap.try_recv() {
        if let SupervisorNotice::AcceptedUpdate {
            market,
            position,
            generation,
            replica,
            key,
            digest,
        } = notice
        {
            assert_eq!(market, SLUG, "a record names the market it belongs to");
            records.push(Record {
                position,
                generation,
                replica,
                key,
                digest,
            });
        }
    }
}

/// Serves two `orderbookUpdate` frames carrying one and the same `version` and different
/// depth, and reports what the run left behind.
async fn serve_repeated_version(log_dedup_keys: bool) -> Outcome {
    let mut peer = ControlledPeer::start(PeerConfig {
        ping_interval_ms: PATIENT_HEARTBEAT_MS,
        ping_timeout_ms: PATIENT_HEARTBEAT_MS,
        ..PeerConfig::default()
    })
    .await;
    let mut running = start(peer.endpoint(), log_dedup_keys);
    let mut connection = peer.next_connection().await;
    let request = connection.complete_handshake().await;
    assert_eq!(request.slugs, vec![SLUG.to_owned()]);

    let mut records = Vec::new();
    connection
        .send_orderbook(
            SLUG,
            &[("0.5", "100")],
            &[("0.6", "200")],
            Some(REPEATED_VERSION),
        )
        .await;
    await_bid(&mut running.observer, "100").await;
    connection
        .send_orderbook(
            SLUG,
            &[("0.5", "150")],
            &[("0.6", "200")],
            Some(REPEATED_VERSION),
        )
        .await;
    await_bid(&mut running.observer, "150").await;

    let published = running.observer.latest();
    let levels = published
        .canonical_levels()
        .iter()
        .map(|level| {
            (
                level.side(),
                level.price().value().canonical(),
                level.quantity().value().canonical(),
            )
        })
        .collect();
    let evidence = published
        .provenance()
        .expect("a published book that accepted a snapshot carries provenance")
        .source_evidence()
        .to_vec();
    let revision = published.revision();
    running.stop.stop();
    let stats = tokio::time::timeout(STEP_TIMEOUT, running.handle)
        .await
        .expect("supervisor run ends after stop")
        .expect("supervisor task completes");
    drain_records(&mut running.tap, &mut records);
    Outcome {
        levels,
        revision,
        evidence,
        records,
        stats,
    }
}

/// A repeated venue key is evidence and nothing else today. Two frames carrying one and the
/// same `version` both publish: the second is committed on top of the first rather than
/// suppressed as a duplicate, the venue's key reaches provenance on the committed revision,
/// and the recorded run publishes exactly what the unrecorded one does — the conformance
/// flag is diagnostic, not a behaviour switch.
///
/// Suppression is pool behaviour, and the pool is gated on cross-connection ordering no
/// venue has proven. When that gate opens this test is what has to change first.
#[tokio::test]
async fn duplicate_dedup_keys_are_recorded_but_never_suppressed() {
    let recorded = serve_repeated_version(true).await;
    let silent = serve_repeated_version(false).await;

    assert_eq!(
        recorded.levels,
        vec![
            (Side::Bid, "0.5".to_owned(), "150".to_owned()),
            (Side::Ask, "0.6".to_owned(), "200".to_owned()),
        ],
        "the second frame's depth is what consumers see"
    );
    assert_eq!(
        recorded.evidence,
        vec![SourceEvidence::Version(value("77"))],
        "the venue's own key reaches the committed revision's provenance"
    );
    assert_eq!(recorded.stats.snapshots_applied, 2);
    assert_eq!(recorded.stats.continuity_losses, 0);
    assert_eq!(
        recorded.stats.diagnostics_dropped, 0,
        "a conformance record is only complete when nothing was dropped"
    );
    assert!(recorded.stats.decode_failures.is_empty());

    assert_eq!(silent.levels, recorded.levels);
    assert_eq!(silent.evidence, recorded.evidence);
    assert_eq!(silent.revision, recorded.revision);
    assert_eq!(
        silent.stats.snapshots_applied,
        recorded.stats.snapshots_applied
    );
    assert!(
        silent.records.is_empty(),
        "without the flag the supervisor records nothing"
    );

    assert_eq!(recorded.records.len(), 2, "one record per accepted update");
    let first = &recorded.records[0];
    let second = &recorded.records[1];
    assert_eq!(first.key, Ok(Some(key(REPEATED_VERSION))));
    assert_eq!(second.key, first.key);
    assert_eq!(first.replica, ReplicaRole::PublishingPrimary);
    assert_eq!(second.replica, ReplicaRole::PublishingPrimary);
    assert_eq!(second.generation, first.generation);
    assert_eq!(
        second.position,
        first.position + 1,
        "positions are contiguous, so an analysis can tell a complete record from a holed one"
    );
    assert_ne!(
        second.digest, first.digest,
        "one key over two different book states is exactly what a conformance analysis has to \
         be able to see"
    );
    let (Ok(Some(last)), Ok(Some(next))) = (&first.key, &second.key) else {
        panic!("both records carry the venue's key");
    };
    assert_eq!(
        classify_key(ORDERBOOK_UPDATE_DEDUP_KEY.semantics(), Some(last), next),
        KeyRelation::Duplicate,
        "the seam names the repeat a duplicate while publishing stays untouched"
    );
}

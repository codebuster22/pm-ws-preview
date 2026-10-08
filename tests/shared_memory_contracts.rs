//! Latest-state shared publication: a real book published into a region and read back
//! exactly, and a torn read proven impossible under contention.

use pm_ws::limitless::supervisor::MAX_BOOK_LEVELS;
use pm_ws::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

const SLUG: &str = "btc-up-or-down-5-min-1788188100";
const STATES: usize = 32;

fn market() -> MarketRef {
    MarketRef::new(
        Venue::new("limitless").unwrap(),
        NativeMarketKey::new(NativeIdentifierKind::slug(), SLUG).unwrap(),
    )
}

fn grammar() -> DecimalGrammar {
    DecimalGrammar::new(18, 30, true, false).unwrap()
}

fn level(side: Side, price: &str, quantity: &str) -> Level {
    Level::new(
        side,
        Price::parse(price, grammar()).unwrap(),
        Quantity::parse(quantity, grammar()).unwrap(),
    )
}

fn provenance(step: usize) -> Provenance {
    provenance_for(market(), step)
}

fn provenance_for(market: MarketRef, step: usize) -> Provenance {
    let venue_native = step.is_multiple_of(2);
    Provenance::new(ProvenanceInput {
        market,
        outcome: Some(NativeOutcome::side("yes").unwrap()),
        native_family: if venue_native {
            "orderbookUpdate".into()
        } else {
            "normalized.orderbook".into()
        },
        source_timestamp: Some(SourceTimestamp::new("2026-08-31T07:12:32.741Z").unwrap()),
        source_evidence: BoundedSourceEvidence::new([], SourceEvidenceCapacity::new(0).unwrap())
            .unwrap(),
        daemon_generation: 1,
        connection: ConnectionIdentity::new("ws.limitless.exchange", 1).unwrap(),
        subscription_generation: 1,
        receive_position: step as u64,
        commit_position: step as u64,
        local_receive_time: LocalMonotonicTimestamp::new(step as u64),
        local_commit_time: LocalMonotonicTimestamp::new(step as u64),
        replica: ReplicaRole::PublishingPrimary,
        representation: if venue_native {
            Representation::VenueNative
        } else {
            Representation::Normalized
        },
        origin: if venue_native {
            Origin::SourceReported
        } else {
            Origin::NormalizedFromSource
        },
        local_revision: 0,
        continuity_epoch: 0,
    })
    .unwrap()
}

/// One self-consistent book state: every level, every count, and the provenance labelling
/// are functions of `step` alone, so any combination of words from two different states is
/// detectable by comparing a decoded snapshot against the state its revision names.
fn levels(step: usize) -> Vec<Level> {
    let bids = 1 + step % 5;
    let asks = 1 + (step + 2) % 4;
    let mut levels = Vec::new();
    for offset in 0..bids {
        levels.push(level(
            Side::Bid,
            &format!("0.{:03}", 400 - step * 3 - offset),
            &format!("{}", 1_000_000 + step * 7 + offset),
        ));
    }
    for offset in 0..asks {
        levels.push(level(
            Side::Ask,
            &format!("0.{:03}", 600 + step * 3 + offset),
            &format!("{}", 2_000_000 + step * 11 + offset),
        ));
    }
    levels
}

fn snapshot(step: usize) -> Candidate {
    snapshot_for(market(), step)
}

fn snapshot_for(market: MarketRef, step: usize) -> Candidate {
    Candidate::snapshot(
        provenance_for(market, step),
        BoundedLevels::new(levels(step), LevelCapacity::new(64).unwrap()).unwrap(),
    )
    .unwrap()
}

/// `STATES` successive published revisions of one book, indexed by `revision - 1`.
fn published_states() -> Vec<PublishedBook> {
    let mut book = OrderBook::new(market());
    (0..STATES)
        .map(|step| {
            book.apply_snapshot(&snapshot(step)).unwrap();
            book.publish()
        })
        .collect()
}

fn segment(level_capacity: u32, event_capacity: u32) -> (Arc<SegmentRegion>, SegmentWriter) {
    let layout = SegmentLayout::new(2, 2, level_capacity, event_capacity, 16).unwrap();
    let region = Arc::new(SegmentRegion::zeroed(layout.region_size()).unwrap());
    let writer = SegmentWriter::create(
        Arc::clone(&region),
        SegmentConfig::new(layout, 0x2026_0901_0000_0001_dead_beef_cafe_f00d, 3),
    )
    .unwrap();
    (region, writer)
}

fn assert_faithful(snapshot: &BookSnapshot, book: &PublishedBook) {
    assert_eq!(snapshot.market(), book.market());
    assert_eq!(snapshot.revision(), book.revision());
    assert_eq!(snapshot.authority(), book.authority());
    assert_eq!(snapshot.continuity(), book.continuity());
    assert_eq!(snapshot.sync_divergences(), book.sync_divergences());
    assert_eq!(snapshot.levels(), book.canonical_levels());
    match (snapshot.publication(), book.provenance()) {
        (Some(published), Some(source)) => {
            assert_eq!(published.origin(), source.origin());
            assert_eq!(published.representation(), source.representation());
            assert_eq!(published.native_family(), source.native_family());
        }
        (None, None) => {}
        (published, source) => panic!("provenance presence differs: {published:?} vs {source:?}"),
    }
}

#[test]
fn shm_publishes_and_reads_back_one_book_exactly() {
    let (region, mut writer) = segment(16, 16);
    let handle = writer.install(&market()).unwrap();
    let reader = SegmentReader::attach(Arc::clone(&region)).unwrap();
    assert_eq!(
        reader.geometry().daemon_instance_id(),
        0x2026_0901_0000_0001_dead_beef_cafe_f00d
    );
    assert_eq!(reader.geometry().segment_generation(), 3);
    assert_eq!(reader.resolve(&market()), Some(handle));
    assert_eq!(reader.read(handle), Err(ReadFault::NoPublishedState));

    let states = published_states();
    for (step, book) in states.iter().enumerate() {
        writer.publish(handle, book, 0).unwrap();
        assert_eq!(writer.publication_generation(), step as u64 + 1);
        let read = reader.read(handle).unwrap();
        assert_faithful(&read, book);
        let canonical = book.canonical_levels();
        assert_eq!(
            read.best_bid(),
            canonical.iter().rfind(|level| level.side() == Side::Bid)
        );
        assert_eq!(
            read.best_ask(),
            canonical.iter().find(|level| level.side() == Side::Ask)
        );
    }
}

#[test]
fn shm_publishes_a_stale_book_with_its_broken_continuity() {
    let (region, mut writer) = segment(16, 16);
    let handle = writer.install(&market()).unwrap();
    let reader = SegmentReader::attach(Arc::clone(&region)).unwrap();
    let mut book = OrderBook::new(market());
    book.apply_snapshot(&snapshot(0)).unwrap();
    assert!(
        book.report_continuity_loss(ContinuityReason::Gap, AuthorityReason::Disconnect)
            .unwrap()
    );
    let published = book.publish();
    writer.publish(handle, &published, 0).unwrap();
    let read = reader.read(handle).unwrap();
    assert_faithful(&read, &published);
    assert_eq!(
        read.authority(),
        &AuthorityState::Stale(AuthorityReason::Disconnect)
    );
    assert!(matches!(
        read.continuity(),
        MutationContinuity::Lost {
            epoch: 0,
            reason: ContinuityReason::Gap
        }
    ));
}

#[test]
fn shm_refuses_a_book_deeper_than_its_slot_without_disturbing_published_state() {
    let (region, mut writer) = segment(4, 16);
    let handle = writer.install(&market()).unwrap();
    let reader = SegmentReader::attach(Arc::clone(&region)).unwrap();
    let states = published_states();
    writer.publish(handle, &states[0], 0).unwrap();
    let deep = states
        .iter()
        .find(|book| book.canonical_levels().len() > 4)
        .unwrap();
    assert_eq!(
        writer.publish(handle, deep, 0),
        Err(WriterError::LevelCapacityExceeded {
            levels: deep.canonical_levels().len(),
            capacity: 4
        })
    );
    assert_faithful(&reader.read(handle).unwrap(), &states[0]);
}

/// The segment must be able to carry any book the supervisor accepts, so this builds a
/// snapshot at exactly [`MAX_BOOK_LEVELS`] and publishes it through a segment sized from
/// that same constant. If the two ever drift apart, the daemon accepts books its own
/// consumer surface cannot publish.
#[test]
fn shm_carries_a_book_at_the_supervisors_maximum_accepted_depth() {
    let levels: Vec<Level> = (0..MAX_BOOK_LEVELS)
        .map(|step| {
            let side = if step < MAX_BOOK_LEVELS / 2 {
                Side::Bid
            } else {
                Side::Ask
            };
            Level::new(
                side,
                Price::parse(&format!("0.{:04}", step + 1), grammar()).unwrap(),
                Quantity::parse(&format!("{}", 1_000 + step), grammar()).unwrap(),
            )
        })
        .collect();
    let capacity = u32::try_from(MAX_BOOK_LEVELS).unwrap();
    let mut book = OrderBook::new(market());
    book.apply_snapshot(
        &Candidate::snapshot(
            provenance(0),
            BoundedLevels::new(levels, LevelCapacity::new(MAX_BOOK_LEVELS).unwrap()).unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    let published = book.publish();
    assert_eq!(published.canonical_levels().len(), MAX_BOOK_LEVELS);

    let layout = SegmentLayout::new(1, 1, capacity, 8, 16).unwrap();
    let region = Arc::new(SegmentRegion::zeroed(layout.region_size()).unwrap());
    let mut writer =
        SegmentWriter::create(Arc::clone(&region), SegmentConfig::new(layout, 9, 1)).unwrap();
    let handle = writer.install(&market()).unwrap();
    writer.publish(handle, &published, 0).unwrap();
    let reader = SegmentReader::attach(region).unwrap();
    assert_faithful(&reader.read(handle).unwrap(), &published);
}

#[test]
fn shm_contended_readers_never_accept_a_torn_state() {
    let (region, mut writer) = segment(16, 16);
    let handle = writer.install(&market()).unwrap();
    let states = published_states();
    let accepted = AtomicU64::new(0);
    let deadline = Instant::now() + Duration::from_millis(1500);

    std::thread::scope(|scope| {
        for _ in 0..4 {
            let region = Arc::clone(&region);
            let states = &states;
            let accepted = &accepted;
            let _ = scope.spawn(move || {
                let reader = SegmentReader::attach(region).unwrap();
                while Instant::now() < deadline {
                    match reader.read(handle) {
                        Ok(read) => {
                            let book = &states[read.revision() as usize - 1];
                            assert_faithful(&read, book);
                            accepted.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(ReadFault::NoPublishedState | ReadFault::Contended { .. }) => {}
                        Err(ReadFault::WriterStalled { .. }) => {}
                        Err(other) => panic!("unexpected read fault: {other:?}"),
                    }
                }
            });
        }
        let mut step = 0_usize;
        while Instant::now() < deadline {
            writer.publish(handle, &states[step % STATES], 0).unwrap();
            step += 1;
        }
        assert!(step > 1_000, "writer published only {step} times");
    });

    assert!(
        accepted.load(Ordering::Relaxed) > 1_000,
        "readers accepted only {} states",
        accepted.load(Ordering::Relaxed)
    );
}

/// One source-reported delta setting the canonical bid at 0.500 to `1_000_000 + step`.
///
/// Exactly one coordinate changes value per step, so each accepted delta emits exactly one
/// mutation and a delivered event's quantity identifies the position it was written at.
fn delta(step: usize) -> Candidate {
    Candidate::source_delta(
        provenance(0),
        BoundedLevels::new(
            vec![level(
                Side::Bid,
                "0.500",
                &format!("{}", 1_000_000 + step as u64),
            )],
            LevelCapacity::new(8).unwrap(),
        )
        .unwrap(),
    )
    .unwrap()
}

/// Publishes a commit's state and then every mutation it produced, in commit order.
fn publish_commit(
    writer: &mut SegmentWriter,
    handle: MarketHandle,
    book: &OrderBook,
    commit: &BookCommit,
) {
    writer.publish(handle, &book.publish(), 0).unwrap();
    for record in commit.mutations() {
        writer
            .publish_mutation(
                handle,
                commit.revision(),
                record.cursor(),
                record.mutation(),
                0,
            )
            .unwrap();
    }
}

fn assert_event_matches(event: &MutationEvent, revision: u64, record: &MutationRecord) {
    let mutation = record.mutation();
    let coordinate = mutation.replacement().or_else(|| mutation.old()).unwrap();
    assert_eq!(event.market(), &market());
    assert_eq!(event.revision(), revision);
    assert_eq!(event.cursor(), record.cursor());
    assert_eq!(event.side(), coordinate.side());
    assert_eq!(event.price(), coordinate.price());
    assert_eq!(event.old_quantity(), mutation.old().map(Level::quantity));
    assert_eq!(
        event.new_quantity(),
        mutation.replacement().map(Level::quantity)
    );
    let source = mutation.provenance();
    assert_eq!(event.publication().origin(), source.origin());
    assert_eq!(
        event.publication().representation(),
        source.representation()
    );
    assert_eq!(event.publication().native_family(), source.native_family());
    assert_eq!(event.daemon_generation(), source.daemon_generation());
    assert_eq!(
        event.subscription_generation(),
        source.subscription_generation()
    );
    assert!(event.commit_time_nanos().is_some_and(|stamp| stamp > 0));
}

fn delivered(stream: &mut EventStream) -> RetainedEvent {
    match stream.poll() {
        Ok(EventPoll::Delivered(event)) => event,
        other => panic!("expected a delivered event, got {other:?}"),
    }
}

fn delivered_mutation(stream: &mut EventStream) -> MutationEvent {
    match delivered(stream) {
        RetainedEvent::Mutation(event) => event,
        other => panic!("expected a delivered mutation, got {other:?}"),
    }
}

fn delivered_resolution(stream: &mut EventStream) -> ResolutionEvent {
    match delivered(stream) {
        RetainedEvent::Resolution(event) => event,
        other => panic!("expected a delivered resolution, got {other:?}"),
    }
}

/// Both mutation origins survive the ring byte for byte.
///
/// A snapshot-diff mutation this daemon derived and a delta the venue reported itself take
/// the same slot shape, so the only thing that tells them apart is the provenance the
/// writer stored — which is exactly what a consumer must not have to guess at.
#[test]
fn shm_ring_round_trips_derived_and_source_reported_mutations() {
    for source_reported in [false, true] {
        let (region, mut writer) = segment(16, 64);
        let handle = writer.install(&market()).unwrap();
        let reader = SegmentReader::attach(Arc::clone(&region)).unwrap();
        let mut book = OrderBook::new(market());
        let base = book.apply_snapshot(&snapshot(0)).unwrap();
        assert!(base.mutations().is_empty());
        writer.publish(handle, &book.publish(), 0).unwrap();

        let (_, mut stream) = reader.attach_stream(handle).unwrap();
        let commit = if source_reported {
            book.apply_source_delta(&delta(3)).unwrap()
        } else {
            book.apply_snapshot(&snapshot(1)).unwrap()
        };
        assert!(!commit.mutations().is_empty());
        publish_commit(&mut writer, handle, &book, &commit);

        for record in commit.mutations() {
            let event = delivered_mutation(&mut stream);
            assert_event_matches(&event, commit.revision(), record);
            let expected = if source_reported {
                Origin::SourceReported
            } else {
                Origin::LocallyDerived(Derivation::SnapshotDiff)
            };
            assert_eq!(event.publication().origin(), &expected);
        }
        assert_eq!(stream.poll(), Ok(EventPoll::Idle));
    }
}

/// A consumer the writer lapped is told so, once, and never handed partial history.
///
/// The ring wraps: publishing ten events into four slots overwrites the position this
/// attachment was parked at, and the only correct answer is an explicit overrun.
#[test]
fn shm_a_lapped_ring_reports_an_overrun_and_recovers_by_reattaching() {
    let (region, mut writer) = segment(16, 4);
    let handle = writer.install(&market()).unwrap();
    let reader = SegmentReader::attach(Arc::clone(&region)).unwrap();
    let mut book = OrderBook::new(market());
    book.apply_snapshot(&snapshot(0)).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();

    let (_, mut stream) = reader.attach_stream(handle).unwrap();
    assert_eq!(stream.cursor().position(), 0);
    assert_eq!(stream.poll(), Ok(EventPoll::Idle));

    let mut last = None;
    for step in 0..10 {
        let commit = book.apply_source_delta(&delta(step)).unwrap();
        assert_eq!(commit.mutations().len(), 1);
        publish_commit(&mut writer, handle, &book, &commit);
        last = Some(commit);
    }
    assert_eq!(
        stream.poll(),
        Err(StreamFault::ContinuityLost {
            reason: ContinuityReason::Overrun
        })
    );

    let resumed = stream.reattach().unwrap();
    let last = last.unwrap();
    assert_eq!(resumed.revision(), last.revision());
    assert_eq!(stream.cursor().position(), 10);
    assert_eq!(stream.poll(), Ok(EventPoll::Idle));
}

/// An attachment starts exactly where its own snapshot stops.
#[test]
fn shm_an_attachment_starts_at_its_snapshots_next_position() {
    let (region, mut writer) = segment(16, 64);
    let handle = writer.install(&market()).unwrap();
    let reader = SegmentReader::attach(Arc::clone(&region)).unwrap();
    let mut book = OrderBook::new(market());
    book.apply_snapshot(&snapshot(0)).unwrap();
    let first = book.apply_source_delta(&delta(1)).unwrap();
    publish_commit(&mut writer, handle, &book, &first);

    let (attached, mut stream) = reader.attach_stream(handle).unwrap();
    let MutationContinuity::Intact { next_position, .. } = attached.continuity() else {
        panic!("an established book publishes an intact stream");
    };
    assert_eq!(stream.cursor().position(), *next_position);
    let cursor = reader.read_cursor(handle).unwrap();
    assert_eq!(cursor.revision(), attached.revision());
    assert_eq!(cursor.continuity(), attached.continuity());
    assert_eq!(stream.poll(), Ok(EventPoll::Idle));

    let commit = book.apply_source_delta(&delta(2)).unwrap();
    publish_commit(&mut writer, handle, &book, &commit);
    let event = delivered_mutation(&mut stream);
    assert_eq!(event.cursor().position(), *next_position);
    assert_event_matches(&event, commit.revision(), &commit.mutations()[0]);
}

/// A rebase is found through the state slot, with nothing at all in the ring to find it by.
///
/// A recovery base commits zero mutations and restarts positions at 0, so a parked
/// attachment reads its own lap's untouched slot and would wait forever on the ring alone.
/// The state slot's continuity epoch is what makes that an explicit loss instead of a hang.
#[test]
fn shm_a_recovery_base_is_reported_with_an_empty_ring() {
    let (region, mut writer) = segment(16, 64);
    let handle = writer.install(&market()).unwrap();
    let reader = SegmentReader::attach(Arc::clone(&region)).unwrap();
    let mut book = OrderBook::new(market());
    book.apply_snapshot(&snapshot(0)).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();
    let (_, mut stream) = reader.attach_stream(handle).unwrap();

    let commit = book.apply_source_delta(&delta(1)).unwrap();
    publish_commit(&mut writer, handle, &book, &commit);
    let _ = delivered_mutation(&mut stream);
    assert_eq!(stream.cursor().position(), 1);
    assert_eq!(stream.poll(), Ok(EventPoll::Idle));

    assert!(
        book.report_continuity_loss(ContinuityReason::Reconnect, AuthorityReason::Disconnect)
            .unwrap()
    );
    let rebase = book.apply_snapshot(&snapshot(2)).unwrap();
    assert!(rebase.recovery_base());
    assert!(rebase.mutations().is_empty());
    writer.publish(handle, &book.publish(), 0).unwrap();

    assert_eq!(
        stream.poll(),
        Err(StreamFault::ContinuityLost {
            reason: ContinuityReason::RecoveryBase
        })
    );
}

/// A continuity loss is sticky: it repeats unchanged until the consumer reattaches.
#[test]
fn shm_a_continuity_loss_repeats_until_the_stream_is_reattached() {
    let (region, mut writer) = segment(16, 64);
    let handle = writer.install(&market()).unwrap();
    let reader = SegmentReader::attach(Arc::clone(&region)).unwrap();
    let mut book = OrderBook::new(market());
    book.apply_snapshot(&snapshot(0)).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();
    let (_, mut stream) = reader.attach_stream(handle).unwrap();

    assert!(
        book.report_continuity_loss(ContinuityReason::Gap, AuthorityReason::Disconnect)
            .unwrap()
    );
    writer.publish(handle, &book.publish(), 0).unwrap();
    let lost = Err(StreamFault::ContinuityLost {
        reason: ContinuityReason::Gap,
    });
    for _ in 0..8 {
        assert_eq!(stream.poll(), lost);
    }

    let rebase = book.apply_snapshot(&snapshot(1)).unwrap();
    assert!(rebase.recovery_base());
    writer.publish(handle, &book.publish(), 0).unwrap();
    for _ in 0..4 {
        assert_eq!(
            stream.poll(),
            lost,
            "a loss must not be relabelled in place"
        );
    }

    let resumed = stream.reattach().unwrap();
    assert_eq!(resumed.revision(), rebase.revision());
    assert_eq!(stream.cursor().epoch(), rebase.epoch());
    assert_eq!(stream.cursor().position(), 0);
    let commit = book.apply_source_delta(&delta(9)).unwrap();
    publish_commit(&mut writer, handle, &book, &commit);
    let event = delivered_mutation(&mut stream);
    assert_event_matches(&event, commit.revision(), &commit.mutations()[0]);
}

/// A stream reattached onto a still-lost book resumes straight back into the loss.
#[test]
fn shm_reattaching_a_lost_book_reports_the_loss_again() {
    let (region, mut writer) = segment(16, 64);
    let handle = writer.install(&market()).unwrap();
    let reader = SegmentReader::attach(Arc::clone(&region)).unwrap();
    let mut book = OrderBook::new(market());
    book.apply_snapshot(&snapshot(0)).unwrap();
    assert!(
        book.report_continuity_loss(ContinuityReason::LocalLoss, AuthorityReason::Overload)
            .unwrap()
    );
    writer.publish(handle, &book.publish(), 0).unwrap();

    let (attached, mut stream) = reader.attach_stream(handle).unwrap();
    assert!(matches!(
        attached.continuity(),
        MutationContinuity::Lost {
            reason: ContinuityReason::LocalLoss,
            ..
        }
    ));
    let lost = Err(StreamFault::ContinuityLost {
        reason: ContinuityReason::LocalLoss,
    });
    assert_eq!(stream.poll(), lost);
    let _ = stream.reattach().unwrap();
    assert_eq!(stream.poll(), lost);
}

/// Readers polling a ring the writer keeps lapping never accept an inconsistent event.
///
/// Every delivered event's quantity is a function of its stream position alone, so any
/// combination of words from two publications is detectable. Each reader's positions are
/// contiguous within a run, and a run ends only at an explicit overrun.
///
/// What this can and cannot show, per `docs/notes/shared-memory-model.md` §3.3: deleting
/// the writer's release fence, and separately the reader's acquire fence, both leave this
/// class of test passing on the `aarch64-apple-darwin` profile. The fences are justified by
/// the memory model, not by this test; the test is a regression net for the closing recheck.
#[test]
fn shm_contended_readers_never_accept_a_torn_event() {
    let (region, mut writer) = segment(16, 64);
    let handle = writer.install(&market()).unwrap();
    let mut book = OrderBook::new(market());
    book.apply_snapshot(&snapshot(0)).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();
    let accepted = AtomicU64::new(0);
    let overruns = AtomicU64::new(0);
    let deadline = Instant::now() + Duration::from_millis(1500);

    std::thread::scope(|scope| {
        for _ in 0..4 {
            let region = Arc::clone(&region);
            let accepted = &accepted;
            let overruns = &overruns;
            let _ = scope.spawn(move || {
                let reader = SegmentReader::attach(region).unwrap();
                let mut stream = loop {
                    match reader.attach_stream(handle) {
                        Ok((_, stream)) => break stream,
                        Err(ReadFault::Contended { .. } | ReadFault::WriterStalled { .. }) => {}
                        Err(other) => panic!("attach: {other:?}"),
                    }
                    assert!(Instant::now() < deadline, "never attached");
                };
                let mut expected = stream.cursor().position();
                while Instant::now() < deadline {
                    match stream.poll() {
                        Ok(EventPoll::Delivered(RetainedEvent::Mutation(event))) => {
                            assert_eq!(event.cursor().epoch(), 0);
                            assert_eq!(event.cursor().position(), expected);
                            assert_eq!(
                                event.new_quantity().map(|value| value.value().canonical()),
                                Some(format!("{}", 1_000_000 + expected))
                            );
                            assert_eq!(event.publication().origin(), &Origin::SourceReported);
                            expected += 1;
                            accepted.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(EventPoll::Delivered(RetainedEvent::Resolution(event))) => {
                            panic!("this writer publishes no resolution: {event:?}")
                        }
                        Ok(EventPoll::Idle) => {}
                        Err(StreamFault::ContinuityLost {
                            reason: ContinuityReason::Overrun,
                        }) => {
                            overruns.fetch_add(1, Ordering::Relaxed);
                            loop {
                                match stream.reattach() {
                                    Ok(_) => break,
                                    Err(
                                        ReadFault::Contended { .. }
                                        | ReadFault::WriterStalled { .. },
                                    ) => {}
                                    Err(other) => panic!("reattach: {other:?}"),
                                }
                                assert!(Instant::now() < deadline, "never reattached");
                            }
                            expected = stream.cursor().position();
                        }
                        Err(StreamFault::Read(
                            ReadFault::Contended { .. } | ReadFault::WriterStalled { .. },
                        )) => {}
                        Err(other) => panic!("poll: {other:?}"),
                    }
                }
            });
        }
        let mut step = 0_usize;
        while Instant::now() < deadline {
            let commit = book.apply_source_delta(&delta(step)).unwrap();
            assert_eq!(commit.mutations().len(), 1);
            publish_commit(&mut writer, handle, &book, &commit);
            step += 1;
        }
        assert!(step > 1_000, "writer published only {step} commits");
    });

    assert!(
        accepted.load(Ordering::Relaxed) > 1_000,
        "readers accepted only {} events ({} overruns)",
        accepted.load(Ordering::Relaxed),
        overruns.load(Ordering::Relaxed),
    );
}

/// An attachment taken against a live writer never starts at a silent gap.
///
/// Every cycle either starts exactly at the next position its own snapshot published — the
/// coherence the whole attachment contract rests on — or fails explicitly because the
/// writer outran it. There is no third outcome.
#[test]
fn shm_attachments_against_a_live_writer_are_coherent_or_explicit() {
    let (region, mut writer) = segment(16, 8);
    let handle = writer.install(&market()).unwrap();
    let mut book = OrderBook::new(market());
    book.apply_snapshot(&snapshot(0)).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();
    let cycles = AtomicU64::new(0);
    let coherent = AtomicU64::new(0);
    let deadline = Instant::now() + Duration::from_millis(2_000);

    std::thread::scope(|scope| {
        let region = Arc::clone(&region);
        let cycles = &cycles;
        let coherent = &coherent;
        let _ = scope.spawn(move || {
            let reader = SegmentReader::attach(region).unwrap();
            while cycles.load(Ordering::Relaxed) < 1_000 && Instant::now() < deadline {
                match reader.attach_stream(handle) {
                    Ok((attached, stream)) => {
                        let MutationContinuity::Intact { next_position, .. } =
                            attached.continuity()
                        else {
                            panic!("an established book publishes an intact stream");
                        };
                        assert_eq!(stream.cursor().position(), *next_position);
                        let _ = coherent.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(ReadFault::Contended { .. } | ReadFault::WriterStalled { .. }) => {}
                    Err(other) => panic!("attach: {other:?}"),
                }
                let _ = cycles.fetch_add(1, Ordering::Relaxed);
            }
        });
        let mut step = 0_usize;
        while cycles.load(Ordering::Relaxed) < 1_000 && Instant::now() < deadline {
            let commit = book.apply_source_delta(&delta(step)).unwrap();
            publish_commit(&mut writer, handle, &book, &commit);
            step += 1;
        }
    });

    assert_eq!(cycles.load(Ordering::Relaxed), 1_000);
    let coherent_cycles = coherent.load(Ordering::Relaxed);
    assert!(
        coherent_cycles >= 100,
        "expected at least 100 of 1000 attachment cycles to observe a coherent seqlock read \
         against a writer that paces its commits behind this loop -- an implementation that \
         always reports Contended/WriterStalled would pass the raw 1000-cycle count above with \
         zero of them coherent, which this floor is here to catch; observed {coherent_cycles}"
    );
}

const RESOLUTION_DATE: &str = "2026-09-01T12:05:00Z";

/// One venue-reported resolution under the same venue-native provenance step 0 publishes.
fn resolution(winner: &str, index: u32, native_label: &str, date: &str) -> MarketResolution {
    MarketResolution::new(
        ResolutionObservation::new(
            provenance(0),
            NativeOutcome::venue_defined(winner).unwrap(),
            NativeLabel::new(native_label).unwrap(),
            DeliveryPath::MarketFeed,
        )
        .unwrap(),
        index,
        SourceTimestamp::new(date).unwrap(),
    )
}

/// Publishes one resolution at the position the book allocates for it, then the state that
/// names the position past it — the writer's own ring-before-state order.
fn publish_resolution(
    writer: &mut SegmentWriter,
    handle: MarketHandle,
    book: &mut OrderBook,
    payload: &MarketResolution,
) -> MutationCursor {
    let cursor = book.note_stream_event().unwrap();
    writer
        .publish_resolution(handle, book.revision(), &cursor, payload, 0)
        .unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();
    cursor
}

/// A resolution crosses the segment byte for byte, in its place in the book's own stream.
///
/// The consumer attached before it receives mutation, resolution, mutation at contiguous
/// positions; the one attached after it starts past it, because published state advanced
/// over the position the resolution occupies.
#[test]
fn shm_ring_round_trips_a_resolution_ordered_with_the_book() {
    let (region, mut writer) = segment(16, 64);
    let handle = writer.install(&market()).unwrap();
    let reader = SegmentReader::attach(Arc::clone(&region)).unwrap();
    let mut book = OrderBook::new(market());
    book.apply_snapshot(&snapshot(0)).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();
    let (_, mut early) = reader.attach_stream(handle).unwrap();
    assert_eq!(early.cursor(), &MutationCursor::new(0, 0));

    let first = book.apply_source_delta(&delta(1)).unwrap();
    publish_commit(&mut writer, handle, &book, &first);
    let ordered_after = book.revision();

    let payload = resolution("Yes", 1, "clob", RESOLUTION_DATE);
    let cursor = publish_resolution(&mut writer, handle, &mut book, &payload);
    assert_eq!(cursor, MutationCursor::new(0, 1));

    let second = book.apply_source_delta(&delta(2)).unwrap();
    publish_commit(&mut writer, handle, &book, &second);

    assert_eq!(
        delivered_mutation(&mut early).cursor(),
        &MutationCursor::new(0, 0)
    );
    let event = delivered_resolution(&mut early);
    assert_eq!(event.market(), &market());
    assert_eq!(event.cursor(), &MutationCursor::new(0, 1));
    assert_eq!(event.revision(), ordered_after);
    assert_eq!(event.winning_outcome(), "Yes");
    assert_eq!(event.winning_index(), 1);
    assert_eq!(event.market_type(), "clob");
    assert_eq!(event.resolution_date(), RESOLUTION_DATE);
    assert_eq!(event.delivery_path(), &DeliveryPath::MarketFeed);
    assert_eq!(event.publication().origin(), &Origin::SourceReported);
    assert_eq!(
        event.publication().representation(),
        &Representation::VenueNative
    );
    assert_eq!(
        event.daemon_generation(),
        provenance(0).daemon_generation(),
        "the resolution's provenance generations cross the segment"
    );
    assert_eq!(
        event.subscription_generation(),
        provenance(0).subscription_generation()
    );
    assert!(event.commit_time_nanos().is_some_and(|stamp| stamp > 0));
    assert_eq!(
        delivered_mutation(&mut early).cursor(),
        &MutationCursor::new(0, 2)
    );
    assert_eq!(early.poll(), Ok(EventPoll::Idle));

    let (state, mut late) = reader.attach_stream(handle).unwrap();
    assert_eq!(
        state.continuity(),
        &MutationContinuity::Intact {
            epoch: 0,
            next_position: 3
        },
        "published state names the position past every delivery, the resolution included"
    );
    assert_eq!(late.cursor(), &MutationCursor::new(0, 3));
    assert_eq!(late.poll(), Ok(EventPoll::Idle));
}

/// A venue text wider than its cell is refused, never truncated, and publishes nothing.
///
/// Truncating any of the three would name something the venue did not report: a different
/// winner, a different market, a different instant. The refusal happens before the slot is
/// marked in flight, so the ring is untouched and the publication generation does not move.
#[test]
fn shm_resolution_texts_wider_than_their_cells_are_refused_and_publish_nothing() {
    let (region, mut writer) = segment(16, 64);
    let handle = writer.install(&market()).unwrap();
    let reader = SegmentReader::attach(Arc::clone(&region)).unwrap();
    let mut book = OrderBook::new(market());
    book.apply_snapshot(&snapshot(0)).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();
    let (_, mut stream) = reader.attach_stream(handle).unwrap();
    let generation = reader.publication_generation();
    let cursor = book.note_stream_event().unwrap();
    let revision = book.revision();

    let refusals = [
        (
            resolution(&"y".repeat(65), 1, "clob", RESOLUTION_DATE),
            WriterError::WinningOutcomeTooLong,
        ),
        (
            resolution("Yes", 1, &"c".repeat(33), RESOLUTION_DATE),
            WriterError::MarketTypeTooLong,
        ),
        (
            resolution("Yes", 1, "clob", &"9".repeat(33)),
            WriterError::ResolutionDateTooLong,
        ),
    ];
    for (payload, expected) in refusals {
        assert_eq!(
            writer
                .publish_resolution(handle, revision, &cursor, &payload, 0)
                .err(),
            Some(expected)
        );
        assert_eq!(
            reader.publication_generation(),
            generation,
            "a refused publication advances no generation"
        );
        assert_eq!(
            stream.poll(),
            Ok(EventPoll::Idle),
            "a refused publication writes no slot"
        );
    }

    let widest = resolution(&"y".repeat(64), 7, &"c".repeat(32), &"9".repeat(32));
    writer
        .publish_resolution(handle, revision, &cursor, &widest, 0)
        .unwrap();
    let event = delivered_resolution(&mut stream);
    assert_eq!(event.winning_outcome(), "y".repeat(64));
    assert_eq!(event.market_type(), "c".repeat(32));
    assert_eq!(event.resolution_date(), "9".repeat(32));
    assert_eq!(event.winning_index(), 7);
}

/// A ring lapped across a resolution is still an ordinary overrun.
///
/// The resolution occupies a position like any other delivery, so losing it is the same
/// explicit loss losing a mutation is — never a silent gap, and never partial history.
#[test]
fn shm_a_lapped_ring_that_swallowed_a_resolution_reports_an_overrun() {
    let (region, mut writer) = segment(16, 4);
    let handle = writer.install(&market()).unwrap();
    let reader = SegmentReader::attach(Arc::clone(&region)).unwrap();
    let mut book = OrderBook::new(market());
    book.apply_snapshot(&snapshot(0)).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();
    let (_, mut stream) = reader.attach_stream(handle).unwrap();
    assert_eq!(stream.cursor(), &MutationCursor::new(0, 0));

    let first = book.apply_source_delta(&delta(0)).unwrap();
    publish_commit(&mut writer, handle, &book, &first);
    let payload = resolution("Yes", 1, "clob", RESOLUTION_DATE);
    assert_eq!(
        publish_resolution(&mut writer, handle, &mut book, &payload),
        MutationCursor::new(0, 1)
    );
    for step in 1..10 {
        let commit = book.apply_source_delta(&delta(step)).unwrap();
        assert_eq!(commit.mutations().len(), 1);
        publish_commit(&mut writer, handle, &book, &commit);
    }

    assert_eq!(
        stream.poll(),
        Err(StreamFault::ContinuityLost {
            reason: ContinuityReason::Overrun
        })
    );
    let resumed = stream.reattach().unwrap();
    assert_eq!(
        resumed.continuity(),
        &MutationContinuity::Intact {
            epoch: 0,
            next_position: 11
        }
    );
    assert_eq!(stream.cursor(), &MutationCursor::new(0, 11));
    assert_eq!(stream.poll(), Ok(EventPoll::Idle));
}

fn other_market() -> MarketRef {
    MarketRef::new(
        Venue::new("limitless").unwrap(),
        NativeMarketKey::new(
            NativeIdentifierKind::slug(),
            "btc-up-or-down-5-min-1788188199",
        )
        .unwrap(),
    )
}

/// One of an arbitrarily large family of distinct markets, indexed for bulk directory
/// filling rather than named individually the way [`market`] and [`other_market`] are.
fn market_n(index: u32) -> MarketRef {
    MarketRef::new(
        Venue::new("limitless").unwrap(),
        NativeMarketKey::new(
            NativeIdentifierKind::slug(),
            format!(
                "btc-up-or-down-5-min-{}",
                1_800_000_000_u64 + u64::from(index)
            ),
        )
        .unwrap(),
    )
}

fn temp_segment_path(tag: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "pm-ws-shm-contract-{tag}-{}-{}.seg",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    path
}

/// A real file-backed segment, exactly as a cross-process consumer attaches to one: the
/// writer's own mapping is read-write, and every `SegmentReader` in these tests opens an
/// independent read-only mapping of the same file, which is what makes the doorbell wait
/// exercise the placement a real consumer gets rather than the writer's own address.
fn file_segment(
    tag: &str,
    level_capacity: u32,
    event_capacity: u32,
) -> (std::path::PathBuf, Arc<SegmentRegion>, SegmentWriter) {
    let layout = SegmentLayout::new(2, 2, level_capacity, event_capacity, 16).unwrap();
    let path = temp_segment_path(tag);
    let region = Arc::new(SegmentRegion::create_file(&path, layout.region_size()).unwrap());
    let writer = SegmentWriter::create(
        Arc::clone(&region),
        SegmentConfig::new(layout, 0x2026_0902_0000_0003_dead_beef_cafe_f00d, 1),
    )
    .unwrap();
    (path, region, writer)
}

/// A parked reader — its own read-only mapping of a real file-backed segment, waiting on
/// whichever doorbell placement this host declared — wakes on a publish from another thread.
///
/// The assertion is "not a timeout" rather than "woken by the wake syscall specifically",
/// exactly as the writer-side doorbell test already establishes: [`SegmentReader::
/// wait_for_publication`]'s own pre-park recheck of the generation is what closes the
/// lost-wake window, so a publish landing before the parked thread's syscall registers is
/// caught there instead, and both are the property under test.
#[test]
fn shm_wait_for_publication_wakes_a_parked_reader_on_a_real_publish() {
    let (path, _region, mut writer) = file_segment("wake", 8, 8);
    let handle = writer.install(&market()).unwrap();
    let opened = Arc::new(SegmentRegion::open_file(&path).unwrap());
    let reader = SegmentReader::attach(opened).unwrap();
    let before = reader.publication_generation();

    let outcome = std::thread::scope(|scope| {
        let parked = scope.spawn(|| {
            reader.wait_for_publication(before, Duration::ZERO, Some(Duration::from_secs(10)))
        });
        std::thread::sleep(Duration::from_millis(50));
        let mut book = OrderBook::new(market());
        book.apply_snapshot(&snapshot(0)).unwrap();
        writer.publish(handle, &book.publish(), 0).unwrap();
        parked.join().expect("the parked thread joins")
    });

    match outcome.expect("wait_for_publication must not fault") {
        WaitOutcome::Changed(generation) => {
            assert_eq!(generation, reader.publication_generation());
        }
        WaitOutcome::TimedOut(_) => panic!("the park timed out instead of waking"),
    }

    let _ = std::fs::remove_file(format!("{}.doorbell", path.display()));
    let _ = std::fs::remove_file(&path);
}

/// The bounded spin phase alone observes a generation that already changed before the call
/// started, and returns well inside the spin budget without ever resolving the doorbell.
#[test]
fn shm_wait_for_publication_spin_phase_observes_an_already_published_generation() {
    let (region, mut writer) = segment(4, 4);
    let handle = writer.install(&market()).unwrap();
    let reader = SegmentReader::attach(Arc::clone(&region)).unwrap();
    let before = reader.publication_generation();

    let mut book = OrderBook::new(market());
    book.apply_snapshot(&snapshot(0)).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();

    let started = Instant::now();
    let outcome = reader
        .wait_for_publication(
            before,
            Duration::from_secs(2),
            Some(Duration::from_millis(100)),
        )
        .expect("a heap-backed segment always resolves its own header doorbell");
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "the spin phase should have caught the change without parking: took {:?}",
        started.elapsed()
    );
    assert_eq!(
        outcome,
        WaitOutcome::Changed(reader.publication_generation())
    );
}

/// A parked wait that nobody wakes still ends at its own deadline, reporting the generation
/// it last observed.
#[test]
fn shm_wait_for_publication_times_out_with_no_publish() {
    let (region, writer) = segment(4, 4);
    let reader = SegmentReader::attach(Arc::clone(&region)).unwrap();
    let before = reader.publication_generation();

    let started = Instant::now();
    let outcome = reader
        .wait_for_publication(before, Duration::ZERO, Some(Duration::from_millis(50)))
        .expect("a heap-backed segment always resolves its own header doorbell");
    assert!(started.elapsed() >= Duration::from_millis(40));
    assert_eq!(outcome, WaitOutcome::TimedOut(before));
    drop(writer);
}

/// Each state publication appends exactly one dirty-index entry, naming the market that
/// changed and the revision it advertised — decoded through [`SegmentReader::next_dirty`]
/// rather than the raw layout offsets, which is what a consumer actually sees. Neither book
/// carries an applied snapshot: this test is about the dirty ring naming the right directory
/// entry and carrying the right (constant) revision along, not about book content.
#[test]
fn shm_next_dirty_delivers_one_entry_per_state_publication_naming_the_right_market() {
    let (region, mut writer) = segment(4, 4);
    let alpha = writer.install(&market()).unwrap();
    let beta = writer.install(&other_market()).unwrap();
    let reader = SegmentReader::attach(Arc::clone(&region)).unwrap();
    let mut cursor = reader.dirty_cursor();
    assert_eq!(cursor.position(), 0);

    let alpha_book = OrderBook::new(market());
    let alpha_published = alpha_book.publish();
    writer.publish(alpha, &alpha_published, 0).unwrap();

    let beta_book = OrderBook::new(other_market());
    let beta_published = beta_book.publish();
    writer.publish(beta, &beta_published, 0).unwrap();

    match reader.next_dirty(&mut cursor) {
        DirtyPoll::Delivered {
            directory_index,
            book_revision,
        } => {
            assert_eq!(directory_index, alpha.entry_index());
            assert_eq!(book_revision, alpha_published.revision());
        }
        other => panic!("expected the alpha entry delivered, got {other:?}"),
    }
    assert_eq!(cursor.position(), 1);
    match reader.next_dirty(&mut cursor) {
        DirtyPoll::Delivered {
            directory_index,
            book_revision,
        } => {
            assert_eq!(directory_index, beta.entry_index());
            assert_eq!(book_revision, beta_published.revision());
        }
        other => panic!("expected the beta entry delivered, got {other:?}"),
    }
    assert_eq!(reader.next_dirty(&mut cursor), DirtyPoll::Idle);
}

/// A writer that laps the segment's single dirty-index ring past a cursor's expectation
/// reports the declared full-rescan signal, never silence, and the cursor recovers on its
/// own — never sticky, and never a second full-ring scan.
#[test]
fn shm_next_dirty_reports_a_rescan_when_the_writer_laps_the_ring() {
    let (region, mut writer) = segment(4, 4);
    let handle = writer.install(&market()).unwrap();
    let reader = SegmentReader::attach(Arc::clone(&region)).unwrap();
    let mut cursor = reader.dirty_cursor();
    let book = OrderBook::new(market());

    writer.publish(handle, &book.publish(), 0).unwrap();
    assert!(matches!(
        reader.next_dirty(&mut cursor),
        DirtyPoll::Delivered { .. }
    ));
    assert_eq!(cursor.position(), 1);

    // The segment's dirty ring is created at capacity 16 (`segment`'s own layout); 17 more
    // publications lap it past the position this cursor still expects.
    for _ in 0..17 {
        writer.publish(handle, &book.publish(), 0).unwrap();
    }

    assert_eq!(reader.next_dirty(&mut cursor), DirtyPoll::Rescan);
    match reader.next_dirty(&mut cursor) {
        DirtyPoll::Delivered { .. } | DirtyPoll::Idle => {}
        DirtyPoll::Rescan => panic!("a rescan must not repeat once the cursor has rebased"),
    }
}

/// The arrival stamp round-trips through every surface a consumer reads it from: the state
/// snapshot, a delivered mutation, and a delivered resolution — and 0 reports absence on
/// every one of them.
#[test]
fn shm_arrival_stamps_are_readable_through_snapshot_and_both_delivery_kinds() {
    let (region, mut writer) = segment(16, 16);
    let handle = writer.install(&market()).unwrap();
    let reader = SegmentReader::attach(Arc::clone(&region)).unwrap();
    let mut book = OrderBook::new(market());
    book.apply_snapshot(&snapshot(0)).unwrap();

    let state_arrival = 1_756_800_000_111_222_333_u64;
    writer
        .publish(handle, &book.publish(), state_arrival)
        .unwrap();
    assert_eq!(
        reader.read(handle).unwrap().arrival_time_nanos(),
        Some(state_arrival)
    );

    let (_, mut stream) = reader.attach_stream(handle).unwrap();

    let mutation_arrival = 1_756_800_000_444_555_666_u64;
    let commit = book.apply_source_delta(&delta(1)).unwrap();
    assert_eq!(commit.mutations().len(), 1);
    writer
        .publish(handle, &book.publish(), mutation_arrival)
        .unwrap();
    for record in commit.mutations() {
        writer
            .publish_mutation(
                handle,
                commit.revision(),
                record.cursor(),
                record.mutation(),
                mutation_arrival,
            )
            .unwrap();
    }
    match stream.poll().unwrap() {
        EventPoll::Delivered(RetainedEvent::Mutation(event)) => {
            assert_eq!(event.arrival_time_nanos(), Some(mutation_arrival));
        }
        other => panic!("expected a delivered mutation, got {other:?}"),
    }

    let resolution_arrival = 1_756_800_000_777_888_999_u64;
    let resolution_cursor = book.note_stream_event().unwrap();
    writer
        .publish_resolution(
            handle,
            book.revision(),
            &resolution_cursor,
            &resolution("Yes", 1, "clob", RESOLUTION_DATE),
            resolution_arrival,
        )
        .unwrap();
    writer
        .publish(handle, &book.publish(), resolution_arrival)
        .unwrap();
    match stream.poll().unwrap() {
        EventPoll::Delivered(RetainedEvent::Resolution(event)) => {
            assert_eq!(event.arrival_time_nanos(), Some(resolution_arrival));
        }
        other => panic!("expected a delivered resolution, got {other:?}"),
    }

    writer.publish(handle, &book.publish(), 0).unwrap();
    assert_eq!(reader.read(handle).unwrap().arrival_time_nanos(), None);
}

/// The boundary between delivery and overrun is exactly one lap, and a deeper lap rebases
/// the cursor exactly as far.
///
/// The slot a cursor at position `P` reads is `P mod capacity`, and it holds the largest
/// published `P + k * capacity`; the cursor is overrun precisely when `k >= 1`. So one
/// publication short of a full lap must still deliver `P` itself, one full lap must be the
/// declared rescan, and two laps must rebase two laps ahead rather than one. Computed from
/// the segment's own declared capacity rather than from a hand-counted number of rounds.
#[test]
fn shm_next_dirty_overrun_boundary_is_one_lap_and_rebases_by_the_lap_it_observed() {
    let (region, mut writer) = segment(4, 4);
    let handle = writer.install(&market()).unwrap();
    let reader = SegmentReader::attach(Arc::clone(&region)).unwrap();
    let capacity = u64::from(reader.geometry().layout().dirty_capacity());
    let book = OrderBook::new(market());
    let base = reader.dirty_cursor();
    assert_eq!(base.position(), 0, "the ring starts empty");

    for _ in 0..capacity {
        writer.publish(handle, &book.publish(), 0).unwrap();
    }
    let mut cursor = base;
    assert!(
        matches!(reader.next_dirty(&mut cursor), DirtyPoll::Delivered { .. }),
        "one publication short of a full lap must still deliver the cursor's own position"
    );
    assert_eq!(cursor.position(), 1);

    writer.publish(handle, &book.publish(), 0).unwrap();
    let mut cursor = base;
    assert_eq!(
        reader.next_dirty(&mut cursor),
        DirtyPoll::Rescan,
        "exactly one lap past the cursor is an overrun"
    );
    assert_eq!(
        cursor.position(),
        capacity + 1,
        "the rebase must land one past the position that overran the cursor"
    );

    for _ in 0..capacity {
        writer.publish(handle, &book.publish(), 0).unwrap();
    }
    let mut cursor = base;
    assert_eq!(reader.next_dirty(&mut cursor), DirtyPoll::Rescan);
    assert_eq!(
        cursor.position(),
        2 * capacity + 1,
        "two laps must rebase by two laps, not by one"
    );

    let mut cursor = reader.dirty_cursor();
    assert_eq!(
        cursor.position(),
        2 * capacity + 1,
        "head discovery must agree with the rebase a rescan computed"
    );
    assert_eq!(reader.next_dirty(&mut cursor), DirtyPoll::Idle);
}

/// Head discovery against a writer that never stops publishing terminates, stays inside the
/// positions the writer has actually reached, and never yields a cursor that decodes a
/// market this segment does not hold — and every entry it does deliver carries exactly the
/// market and revision that entry's absolute ring position must hold, not merely a position
/// that decoded to *some* installed market.
///
/// The writer alternates two installed markets — `alpha` on even publications, `beta` on
/// odd — and advances whichever book it is about to publish by exactly one revision first
/// (`OrderBook::apply_snapshot` advances the revision on every call, per its own contract).
/// So absolute position `p`'s market and revision are pure functions of `p` alone: even `p`
/// is `alpha` at revision `p / 2 + 1`, odd `p` is `beta` at revision `(p - 1) / 2 + 1` — a
/// single global counter, since every `writer.publish` call, regardless of which market it
/// names, advances the ring's absolute position by exactly one. A torn delivery that paired
/// the right position with a stale or foreign payload — the right slot, wrong content —
/// would fail this prediction; a repeated, unchanging revision on one market alone could
/// never have told the two cases apart.
///
/// The scan is one best-effort pass with no retry, so its answer may trail the writer's true
/// tip — that is the documented behaviour, and this pins the two properties that matter
/// instead: it always ends, and it never invents. Both sides are bounded twice over, by a
/// round count and by wall time, so a defect here fails rather than hangs.
#[test]
fn shm_dirty_head_discovery_survives_a_writer_publishing_underneath_it() {
    const WRITER_DEADLINE: Duration = Duration::from_secs(5);
    const SCAN_WINDOW: Duration = Duration::from_millis(300);
    /// Kept far below the point at which `levels`' own price arithmetic (`400 - step * 3`)
    /// would underflow, so an unbounded round count run through it forever stays safe.
    const CONTENT_CYCLE: usize = 64;

    let (region, mut writer) = segment(16, 16);
    let alpha = writer.install(&market()).unwrap();
    let beta = writer.install(&other_market()).unwrap();
    let reader = SegmentReader::attach(Arc::clone(&region)).unwrap();
    let published = Arc::new(AtomicU64::new(0));
    let scanning = Arc::new(AtomicBool::new(true));

    let expected_at = |position: u64| -> (u32, u64) {
        if position.is_multiple_of(2) {
            (alpha.entry_index(), position / 2 + 1)
        } else {
            (beta.entry_index(), (position - 1) / 2 + 1)
        }
    };

    let (scans, highest) = std::thread::scope(|scope| {
        let counter = Arc::clone(&published);
        let publishing = Arc::clone(&scanning);
        // The publisher runs for exactly as long as the scanner does, rather than for a round
        // count guessed against it: a round budget that ran out early would leave the back of
        // the scan window racing nobody and passing on an empty room, and one that did not
        // would burn a core past the window for no gain. The wall-clock deadline stays as the
        // safety net that keeps a defect a failure rather than a hang.
        let publisher = scope.spawn(move || {
            let mut alpha_book = OrderBook::new(market());
            let mut beta_book = OrderBook::new(other_market());
            let started = Instant::now();
            let mut rounds = 0_u64;
            while publishing.load(Ordering::Acquire) && started.elapsed() < WRITER_DEADLINE {
                // Counted before the publication rather than after it, so the counter is
                // always an upper bound on the positions a scanning thread can observe.
                counter.store(rounds + 1, Ordering::Release);
                let step = (rounds as usize) % CONTENT_CYCLE;
                if rounds.is_multiple_of(2) {
                    alpha_book.apply_snapshot(&snapshot(step)).unwrap();
                    writer.publish(alpha, &alpha_book.publish(), 0).unwrap();
                } else {
                    beta_book
                        .apply_snapshot(&snapshot_for(other_market(), step))
                        .unwrap();
                    writer.publish(beta, &beta_book.publish(), 0).unwrap();
                }
                rounds += 1;
            }
            rounds
        });

        let started = Instant::now();
        let mut scans = 0_u64;
        let mut highest = 0_u64;
        while started.elapsed() < SCAN_WINDOW {
            let mut cursor = reader.dirty_cursor();
            let position = cursor.position();
            let started_rounds = published.load(Ordering::Acquire);
            assert!(
                position <= started_rounds,
                "head discovery reported position {position} past the {started_rounds} \
                 publications the writer had begun"
            );
            highest = highest.max(position);
            if let DirtyPoll::Delivered {
                directory_index,
                book_revision,
            } = reader.next_dirty(&mut cursor)
            {
                let (expected_index, expected_revision) = expected_at(position);
                assert_eq!(
                    directory_index, expected_index,
                    "position {position} delivered directory index {directory_index}, \
                     expected {expected_index}"
                );
                assert_eq!(
                    book_revision, expected_revision,
                    "position {position} delivered revision {book_revision}, expected \
                     {expected_revision}"
                );
            }
            scans += 1;
        }
        scanning.store(false, Ordering::Release);
        let rounds = publisher.join().expect("the publishing thread joins");
        assert!(rounds > 0, "the writer published nothing at all");
        assert!(
            highest > 0,
            "the scanner never observed a published position, so nothing was raced"
        );
        (scans, highest)
    });

    assert!(
        scans > 0,
        "no head discovery completed inside {SCAN_WINDOW:?}"
    );
    assert!(
        highest <= published.load(Ordering::Acquire),
        "the highest head observed outran every position the writer ever published"
    );
}

/// `target/<profile>/examples/<name>`, derived from this test binary's own location, exactly
/// as `tests/cross_process_contracts.rs`'s own helper of the same name derives one — `cargo
/// test` builds every example alongside the test binaries, so this is always the example
/// this run just compiled from the current source tree.
fn example_binary(name: &str) -> std::path::PathBuf {
    let mut path = std::env::current_exe().expect("test binary path");
    let _ = path.pop();
    if path.ends_with("deps") {
        let _ = path.pop();
    }
    path.push("examples");
    path.push(if cfg!(windows) {
        format!("{name}.exe")
    } else {
        name.to_owned()
    });
    assert!(
        path.is_file(),
        "example `{name}` is not built at {path:?}; run `cargo build --example {name}`"
    );
    path
}

/// The value of a `"key: value..."` report line `print_report` (`examples/latency_probe.rs`)
/// emits, parsed as the leading integer of its value — e.g. `"rescans: 190\n"` yields `190`.
/// Asserts the line exists and parses, so a report-format change fails loudly here rather
/// than silently skipping the property this test exists to check.
fn parse_report_counter(report: &str, key: &str) -> u64 {
    let prefix = format!("{key}: ");
    let line = report
        .lines()
        .find(|line| line.starts_with(&prefix))
        .unwrap_or_else(|| panic!("no {key:?} line in latency_probe's report:\n{report}"));
    let value = line[prefix.len()..]
        .split_whitespace()
        .next()
        .unwrap_or_else(|| panic!("{key:?} line has no value: {line:?}"));
    value
        .parse()
        .unwrap_or_else(|error| panic!("{key:?} value {value:?} did not parse as u64: {error}"))
}

/// S7A-C2-2: `drain_dirty` in `examples/latency_probe.rs` used to loop back into
/// `next_dirty` after handling a `Rescan` instead of returning. A full-directory recovery
/// walk takes longer than an empty poll, so a writer that never stops publishing into a
/// ring far smaller than its own publish rate laps the ring again before that walk
/// completes — the unfixed loop then received another `Rescan` on its very next poll, and
/// kept going, doing many full-directory recovery passes inside what should have been one
/// `drain_dirty` call.
///
/// This drives the actual shipped binary — not a copy of its control flow — against a real
/// file-backed segment with a capacity-4 dirty ring, 1,024 real installed markets so the
/// recovery scan spends its time on genuine state reads spread across memory the hot writer
/// below never touches, and a writer thread publishing into one hot market as fast as it
/// can for the whole run.
///
/// The control property that distinguishes the two: fixed, every `drain_dirty` call that a
/// wake triggers handles at most one `Rescan` before returning, so cumulative `rescans` can
/// never run meaningfully ahead of cumulative `wakes` (observed on this machine: 149-159
/// rescans against 150-163 wakes, always `rescans <= wakes`). Unfixed, a single wake's
/// `drain_dirty` call can absorb dozens of consecutive rescans before it happens to escape
/// to `Idle` (observed: 170-190 rescans against 3-5 wakes, a 35x-63x ratio) — both counters
/// are printed in the process's own report, so this reads them back rather than inferring
/// anything from wall-clock exit time.
///
/// Wall-clock exit time cannot see this bug: `run`'s outer loop rechecks its `--seconds`
/// deadline only after `drain_dirty` returns, so a late-but-eventual escape and a prompt one
/// both end the process shortly after the deadline either way — on this fast, many-core
/// development machine, the unfixed loop was observed to self-resolve within roughly one
/// scheduler quantum's worth of wasted scanning rather than hanging outright, so exit time
/// alone would not have told the two apart. The wasted-rescan ratio does. `KILL_BOUND`
/// stays only as a hang guard for a machine where escape is rarer still: the child is
/// polled and killed past a generous bound rather than joined unboundedly, so a genuine
/// hang fails this test instead of the suite.
#[test]
fn latency_probe_bounds_wasted_rescans_per_wake_under_a_writer_that_keeps_lapping() {
    /// Real installed markets sharing the directory the recovery scan walks, so that scan
    /// spends its time on genuine state reads spread across memory the hot writer below
    /// never touches — not on cheap not-installed skips that a fast writer's own cache
    /// traffic could throttle into looking cheaper than a real fleet's recovery pass is.
    const INSTALLED_MARKETS: u32 = 1024;
    const WRITER_DEADLINE: Duration = Duration::from_secs(30);
    const PROBE_SECONDS: u64 = 1;
    const KILL_BOUND: Duration = Duration::from_secs(10);
    /// Fixed behaviour observed on the development machine never exceeded `rescans ==
    /// wakes`; the unfixed loop was observed at 35x-63x. This sits an order of magnitude
    /// above the fixed ceiling and nearly two below the unfixed floor.
    const MAX_RESCANS_PER_WAKE: u64 = 10;

    let layout = SegmentLayout::new(INSTALLED_MARKETS, INSTALLED_MARKETS, 4, 4, 4).unwrap();
    let path = temp_segment_path("latency-probe-rescan");
    let region = Arc::new(SegmentRegion::create_file(&path, layout.region_size()).unwrap());
    let mut writer = SegmentWriter::create(
        Arc::clone(&region),
        SegmentConfig::new(layout, 0x2026_0902_0000_0002_dead_beef_cafe_f00d, 4),
    )
    .unwrap();

    let handle = writer.install(&market()).unwrap();
    writer
        .publish(handle, &OrderBook::new(market()).publish(), 0)
        .unwrap();
    for index in 0..INSTALLED_MARKETS - 1 {
        let other = market_n(index);
        let other_handle = writer.install(&other).unwrap();
        writer
            .publish(other_handle, &OrderBook::new(other).publish(), 0)
            .unwrap();
    }

    let running = Arc::new(AtomicBool::new(true));
    let publishing = Arc::clone(&running);
    let publisher = std::thread::spawn(move || {
        let published = OrderBook::new(market()).publish();
        let started = Instant::now();
        while publishing.load(Ordering::Acquire) && started.elapsed() < WRITER_DEADLINE {
            writer.publish(handle, &published, 0).unwrap();
        }
    });

    let mut command = std::process::Command::new(example_binary("latency_probe"));
    let _ = command
        .arg("--shm")
        .arg(&path)
        .args(["--mode", "spin"])
        .args(["--seconds", &PROBE_SECONDS.to_string()])
        .args(["--label", "shared_memory_contracts rescan-liveness proof"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let mut child = command.spawn().expect("spawn latency_probe");

    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll latency_probe") {
            break Some(status);
        }
        if started.elapsed() > KILL_BOUND {
            let _ = child.kill();
            let _ = child.wait();
            break None;
        }
        std::thread::sleep(Duration::from_millis(20));
    };

    running.store(false, Ordering::Release);
    publisher.join().expect("the publishing thread joins");
    let _ = std::fs::remove_file(&path);

    let mut stdout = String::new();
    if let Some(mut handle) = child.stdout.take() {
        let _ = std::io::Read::read_to_string(&mut handle, &mut stdout);
    }
    let mut stderr = String::new();
    if let Some(mut handle) = child.stderr.take() {
        let _ = std::io::Read::read_to_string(&mut handle, &mut stderr);
    }

    let status = status.unwrap_or_else(|| {
        panic!(
            "latency_probe did not exit within {KILL_BOUND:?} of its own --seconds \
             {PROBE_SECONDS} deadline under a writer that kept lapping a capacity-4 dirty \
             ring\nstdout: {stdout}\nstderr: {stderr}"
        )
    });
    assert!(
        status.success(),
        "latency_probe exited with {status:?}\n{stderr}"
    );

    let wakes = parse_report_counter(&stdout, "wakes");
    let rescans = parse_report_counter(&stdout, "rescans");
    assert!(
        rescans <= wakes * MAX_RESCANS_PER_WAKE + MAX_RESCANS_PER_WAKE,
        "rescans ({rescans}) ran {:.1}x ahead of wakes ({wakes}); S7A-C2-2 (an unfixed \
         Rescan handler that loops back into next_dirty without returning) lets a single \
         wake's drain_dirty call absorb many consecutive rescans instead of at most one\n\
         {stdout}",
        rescans as f64 / wakes.max(1) as f64
    );
}

/// A file-backed segment forced to keep its doorbell in a sibling page, with both files opened
/// the way each has to be to be usable at all: the segment read-only, because no consumer may
/// write a cell of this ABI, and the page read-write, because the platform's wait primitive
/// parks only on a mapping that accepts writes.
///
/// That read-write page descriptor is exactly what `pmwsd` will not hand a consumer — a
/// writable mapping carries a writable length, so the descriptor is a truncation capability
/// over a file the daemon's own writer stores through — so these are the channel's and the
/// reader's capabilities under test, not the daemon's behaviour.
fn page_segment(
    tag: &str,
) -> (
    std::path::PathBuf,
    Arc<SegmentRegion>,
    SegmentWriter,
    std::fs::File,
    std::fs::File,
) {
    let layout = SegmentLayout::new(2, 2, 8, 8, 16).unwrap();
    let path = temp_segment_path(tag);
    let region = Arc::new(SegmentRegion::create_file(&path, layout.region_size()).unwrap());
    let writer = SegmentWriter::create(
        Arc::clone(&region),
        SegmentConfig {
            doorbell: DoorbellPlacement::ForcePage,
            ..SegmentConfig::new(layout, 0x2026_0902_0000_0006_dead_beef_cafe_f00d, 1)
        },
    )
    .unwrap();
    assert_eq!(writer.doorbell_feature_bit(), FEATURE_DOORBELL_PAGE);
    let page_path = std::path::PathBuf::from(format!("{}.doorbell", path.display()));
    let segment = std::fs::File::open(&path).unwrap();
    let page = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&page_path)
        .unwrap();
    (path, region, writer, segment, page)
}

/// A page-doorbell segment crosses a socket as two descriptors, and the consumer that receives
/// them reads the book through the first and parks on the second.
///
/// This is the channel's and the reader's capability, not a transfer `pmwsd` performs:
/// `pmwsd` deliberately serves the segment descriptor alone, because a page descriptor is a
/// truncation capability over a file its writer stores through. What is pinned here is that
/// [`SegmentReader::attach_with_doorbell`]'s page parameter works end to end for a sender that
/// does have a page to give — a same-process pair here — so the degraded no-page path proven
/// below is a choice rather than the only thing that functions. The placement is forced rather
/// than probed because which one a segment takes is the platform's answer and not a knob: a
/// host that keeps the doorbell in the header would otherwise never run this path at all.
#[test]
fn shm_a_page_doorbell_segment_crosses_a_socket_as_two_working_descriptors() {
    let (path, _region, mut writer, segment, page) = page_segment("transfer-page");
    let handle = writer.install(&market()).unwrap();
    let mut book = OrderBook::new(market());
    book.apply_snapshot(&snapshot(0)).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();

    let (daemon, consumer) = std::os::unix::net::UnixStream::pair().unwrap();
    let sent = send_with_fds(
        std::os::fd::AsFd::as_fd(&daemon),
        b"{\"result\":\"attached\"}\n",
        &[
            std::os::fd::AsFd::as_fd(&segment),
            std::os::fd::AsFd::as_fd(&page),
        ],
    )
    .unwrap();
    assert_eq!(sent, 22);
    let mut buffer = [0_u8; 64];
    let (_read, mut received) = recv_with_fds(
        std::os::fd::AsFd::as_fd(&consumer),
        &mut buffer,
        MAX_TRANSFERRED_DESCRIPTORS,
    )
    .unwrap();
    assert_eq!(received.len(), 2, "segment first, doorbell page second");

    let received_page = received.remove(1);
    let received_segment = received.remove(0);
    let mapped = Arc::new(SegmentRegion::open_read_only_from_fd(received_segment).unwrap());
    assert!(
        !mapped.is_writable(),
        "a consumer's mapping of a transferred segment accepts no store"
    );
    let reader = SegmentReader::attach_with_doorbell(mapped, Some(received_page)).unwrap();
    let resolved = reader.resolve(&market()).unwrap();
    assert_eq!(
        reader.read(resolved).unwrap().revision(),
        book.publish().revision(),
        "the received descriptor carries the book the writer published"
    );

    let before = reader.publication_generation();
    let outcome = std::thread::scope(|scope| {
        let parked = scope.spawn(|| {
            reader.wait_for_publication(before, Duration::ZERO, Some(Duration::from_secs(10)))
        });
        std::thread::sleep(Duration::from_millis(50));
        book.apply_snapshot(&snapshot(1)).unwrap();
        writer.publish(handle, &book.publish(), 0).unwrap();
        parked.join().expect("the parked thread joins")
    });
    assert!(
        matches!(outcome, Ok(WaitOutcome::Changed(_))),
        "the transferred page is the doorbell this consumer parks on: {outcome:?}"
    );

    let _ = std::fs::remove_file(format!("{}.doorbell", path.display()));
    let _ = std::fs::remove_file(&path);
}

/// A page-doorbell segment transferred without its page reads perfectly and cannot park.
///
/// This is what every `pmwsd` attach of a page-placement segment looks like, and what a sender
/// that could not open the sibling page hands over. The degraded behaviour is the one
/// [`SegmentReader::attach`] already chose for a page it cannot open: attachment succeeds,
/// every read works, a spinning consumer is unaffected, and only a park fails — with the typed
/// fault, never a panic and never a silent forever-wait.
#[test]
fn shm_a_page_segment_without_its_page_reads_but_cannot_park() {
    let (path, _region, mut writer, segment, _page) = page_segment("transfer-no-page");
    let handle = writer.install(&market()).unwrap();
    let mut book = OrderBook::new(market());
    book.apply_snapshot(&snapshot(0)).unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();

    let (daemon, consumer) = std::os::unix::net::UnixStream::pair().unwrap();
    let _sent = send_with_fds(
        std::os::fd::AsFd::as_fd(&daemon),
        b"{}\n",
        &[std::os::fd::AsFd::as_fd(&segment)],
    )
    .unwrap();
    let mut buffer = [0_u8; 64];
    let (_read, mut received) = recv_with_fds(
        std::os::fd::AsFd::as_fd(&consumer),
        &mut buffer,
        MAX_TRANSFERRED_DESCRIPTORS,
    )
    .unwrap();
    assert_eq!(received.len(), 1);

    let mapped = Arc::new(SegmentRegion::open_read_only_from_fd(received.remove(0)).unwrap());
    let reader = SegmentReader::attach_with_doorbell(mapped, None).unwrap();
    let resolved = reader.resolve(&market()).unwrap();
    assert_eq!(
        reader.read(resolved).unwrap().revision(),
        book.publish().revision(),
        "a segment with no page beside it still reads"
    );
    let spun = reader.wait_for_publication(0, Duration::from_millis(5), Some(Duration::ZERO));
    assert!(
        matches!(spun, Ok(WaitOutcome::Changed(_))),
        "a spinning consumer never resolves the doorbell and so never sees its absence: {spun:?}"
    );
    let parked = reader.wait_for_publication(
        reader.publication_generation(),
        Duration::ZERO,
        Some(Duration::from_millis(50)),
    );
    assert!(
        matches!(parked, Err(WaitFault::DoorbellUnavailable(_))),
        "a park with no page is the typed fault, surfaced on the first wait: {parked:?}"
    );

    let _ = std::fs::remove_file(format!("{}.doorbell", path.display()));
    let _ = std::fs::remove_file(&path);
}

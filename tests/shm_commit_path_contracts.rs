#![forbid(unsafe_code)]

//! Shared-memory publication contracts for the real `limitless::supervisor` driven against
//! the scripted controlled peer.
//!
//! The segment is published on the book's own ordered commit path, not through an
//! in-process consumer of it. These tests script the failure that distinction exists for: a
//! diagnostic [`BookObserver`] attached to the same book with a one-delivery ring, never
//! drained, so it is overrun while the book keeps committing. A shared-memory consumer
//! attached to the segment must still receive every mutation position contiguously, and
//! every loss it can be told about must be an explicit one.
//!
//! One test here installs no segment at all: it is the control for what the segment's fixed
//! cells are allowed to narrow, and it belongs beside the case it is contrasted with.

mod support;

use pm_ws::limitless::supervisor::{
    BookSegment, Stopper, Supervisor, SupervisorConfig, SupervisorNotice, SupervisorStats, VENUE,
};
use pm_ws::{
    AuthorityState, BookObserver, BookSnapshot, ContinuityReason, DeliveryPath, EventPoll,
    EventStream, MarketHandle, MarketRef, MutationContinuity, MutationCursor, NativeIdentifierKind,
    NativeMarketKey, ObserverRecvError, ReadFault, ReplicaRole, ResolutionEvent, RetainedEvent,
    SegmentConfig, SegmentLayout, SegmentReader, SegmentRegion, SegmentWriter, StreamDelivery,
    StreamFault, Venue, WriterError,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use support::controlled_peer::{ControlledPeer, PeerConfig, PeerConnection};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;

const SLUG: &str = "btc-up-or-down-5-min-1788172500";
const STEP_TIMEOUT: Duration = Duration::from_secs(10);
const RUN_CAP: Duration = Duration::from_secs(120);
const POLL_INTERVAL: Duration = Duration::from_millis(1);
/// One delivery of mutation history for the diagnostic observer: the smallest ring a book
/// can have, so a consumer that reads nothing is overrun by the second mutation committed.
const STARVED_OBSERVER_CAPACITY: usize = 1;
const SEGMENT_LEVEL_CAPACITY: u32 = 16;
const SEGMENT_EVENT_CAPACITY: u32 = 64;
/// Snapshots the peer serves. The first is the book's base and derives nothing; each later
/// one restates both prices at a new quantity, so it derives exactly two mutations.
const SNAPSHOTS: usize = 9;
const MUTATIONS_PER_SNAPSHOT: u64 = 2;

fn market() -> MarketRef {
    MarketRef::new(
        Venue::new(VENUE).expect("the venue name is valid"),
        NativeMarketKey::new(NativeIdentifierKind::slug(), SLUG).expect("the slug is valid"),
    )
}

/// A peer whose announced heartbeat cadence outlasts any step here, so nothing in these
/// tests can be disturbed by a heartbeat deadline.
fn patient_peer() -> PeerConfig {
    PeerConfig {
        ping_interval_ms: 60_000,
        ping_timeout_ms: 60_000,
        ..PeerConfig::default()
    }
}

/// One publishing connection, a compressed reconnect clock, and a diagnostic mutation ring
/// deliberately too small to hold this run's history.
fn test_config(endpoint: String) -> SupervisorConfig {
    SupervisorConfig {
        endpoint,
        market: SLUG.to_owned(),
        setup_timeout: Duration::from_secs(10),
        replicas: 1,
        observer_capacity: STARVED_OBSERVER_CAPACITY,
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(40),
        exhausted_backoff: Duration::from_millis(40),
        max_recovery_attempts: 2,
        fenced_linger: Duration::from_secs(10),
        resubscribe_window: Duration::from_secs(30),
        ..SupervisorConfig::default()
    }
}

fn segment_path(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "pm-ws-commit-path-{name}-{}-{}.seg",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is after the epoch")
            .as_nanos()
    ));
    path
}

/// A freshly formatted single-market segment: the writer half handed to the supervisor, the
/// region kept so this test can attach to it as an ordinary reader would.
struct Segment {
    region: Arc<SegmentRegion>,
    book: BookSegment,
    path: PathBuf,
}

fn open_segment(name: &str, levels: u32) -> Segment {
    open_segment_with_events(name, levels, SEGMENT_EVENT_CAPACITY)
}

/// The same segment with a ring of `events` deliveries per market, for a test that must lap
/// one. Ring depths are powers of two, which is what lets a position map to a slot by
/// masking.
fn open_segment_with_events(name: &str, levels: u32, events: u32) -> Segment {
    let path = segment_path(name);
    let layout = SegmentLayout::new(1, 1, levels, events, 16).expect("a valid segment layout");
    let region = Arc::new(
        SegmentRegion::create_file(&path, layout.region_size()).expect("create the segment file"),
    );
    let mut writer = SegmentWriter::create(
        Arc::clone(&region),
        SegmentConfig::new(layout, 0x2026_0901_0000_0003_0000_0000_0000_0001, 1),
    )
    .expect("format the segment");
    let handle = writer.install(&market()).expect("install the market");
    Segment {
        region,
        book: BookSegment::new(writer, handle),
        path,
    }
}

struct Running {
    starved: BookObserver,
    stop: Stopper,
    handle: JoinHandle<SupervisorStats>,
}

/// Starts a supervisor publishing into `segment`, with a diagnostic observer attached and
/// never read.
///
/// The observer is attached after the segment so this run's first published revision is
/// already in the segment when the caller attaches a stream to it.
fn start(endpoint: String, segment: BookSegment) -> Running {
    let mut supervisor = Supervisor::new(test_config(endpoint)).expect("a valid configuration");
    supervisor
        .publish_into(segment)
        .expect("the initial revision publishes into a segment sized for it");
    let starved = supervisor.attach();
    let stop = supervisor.stopper();
    let handle = tokio::spawn(async move {
        let stats = supervisor.run_until(Instant::now() + RUN_CAP).await;
        assert_eq!(
            supervisor.segment_failure(),
            None,
            "a segment sized for this book refuses nothing it is given"
        );
        stats
    });
    Running {
        starved,
        stop,
        handle,
    }
}

/// Waits until the segment's published state satisfies `predicate`, or panics naming what
/// it was waiting for and what it last read.
///
/// Read faults are this reader's own bound expiring against a live writer, so they are
/// retried rather than reported.
async fn await_state(
    reader: &SegmentReader,
    handle: MarketHandle,
    what: &str,
    predicate: impl Fn(&BookSnapshot) -> bool,
) -> BookSnapshot {
    let deadline = Instant::now() + STEP_TIMEOUT;
    let mut last = None;
    loop {
        if let Ok(snapshot) = reader.read(handle) {
            if predicate(&snapshot) {
                return snapshot;
            }
            last = Some(snapshot);
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}; last read {last:?}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Serves snapshot `step`: both prices fixed, both quantities moved, so every snapshot
/// after the first derives exactly [`MUTATIONS_PER_SNAPSHOT`] level changes.
async fn send_snapshot(peer: &mut PeerConnection, step: usize) {
    let bid = (1_000 + step).to_string();
    let ask = (2_000 + step).to_string();
    peer.send_orderbook(SLUG, &[("0.300", &bid)], &[("0.700", &ask)], None)
        .await;
}

/// Reads every mutation the ring currently holds, up to `expected`, and returns their
/// cursors in delivery order.
///
/// A poll that reports a continuity loss fails the test naming it: this consumer was
/// attached before the first commit and the ring is deeper than this run's history, so the
/// only way it can lose anything is the gap this whole contract exists to rule out.
async fn drain(stream: &mut EventStream, expected: usize) -> Vec<MutationCursor> {
    let deadline = Instant::now() + STEP_TIMEOUT;
    let mut cursors = Vec::new();
    while cursors.len() < expected {
        match stream.poll() {
            Ok(EventPoll::Delivered(event)) => cursors.push(event.cursor().clone()),
            Ok(EventPoll::Idle) => {
                assert!(
                    Instant::now() < deadline,
                    "timed out after {} of {expected} mutations; the stream is idle at {:?}",
                    cursors.len(),
                    stream.cursor()
                );
                tokio::time::sleep(POLL_INTERVAL).await;
            }
            Err(StreamFault::ContinuityLost { reason }) => panic!(
                "the shared-memory consumer lost continuity at {:?} after {} of {expected} \
                 mutations: {reason:?}",
                stream.cursor(),
                cursors.len()
            ),
            Err(StreamFault::Read(fault)) => {
                assert!(
                    Instant::now() < deadline,
                    "timed out on a repeated read fault at {:?}: {fault:?}",
                    stream.cursor()
                );
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        }
    }
    cursors
}

/// This test pins that a shared-memory consumer's delivery is independent of any in-process
/// consumer of the same book: a diagnostic observer that falls behind must never stall the
/// segment's mutation ring.
///
/// Here the diagnostic observer is overrun by construction — a one-delivery ring, never
/// read, against sixteen committed mutations — and the shared-memory consumer must still
/// receive positions 0..16 contiguously, as many as the book itself says it derived.
#[tokio::test]
async fn shared_memory_receives_every_mutation_while_the_diagnostic_observer_is_overrun() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let segment = open_segment("contiguous", SEGMENT_LEVEL_CAPACITY);
    let reader = SegmentReader::attach(Arc::clone(&segment.region)).expect("attach a reader");
    let path = segment.path.clone();
    let mut running = start(peer.endpoint(), segment.book);

    let handle = reader.resolve(&market()).expect("the market is installed");
    let (attached, mut stream) = reader.attach_stream(handle).expect("attach a stream");
    assert_eq!(
        attached.revision(),
        0,
        "the segment carries the book's initial revision before any commit"
    );
    assert_eq!(stream.cursor(), &MutationCursor::new(0, 0));

    let mut connection = peer.next_connection().await;
    let request = connection.complete_handshake().await;
    assert_eq!(request.slugs, vec![SLUG.to_owned()]);

    for step in 0..SNAPSHOTS {
        send_snapshot(&mut connection, step).await;
        let revision = step as u64 + 1;
        let _ = await_state(&reader, handle, "the snapshot to commit", |snapshot| {
            snapshot.revision() >= revision
        })
        .await;
    }

    let expected = MUTATIONS_PER_SNAPSHOT * (SNAPSHOTS as u64 - 1);
    let cursors = drain(&mut stream, expected as usize).await;
    let contiguous: Vec<MutationCursor> =
        (0..expected).map(|at| MutationCursor::new(0, at)).collect();
    assert_eq!(
        cursors, contiguous,
        "every position the book committed reaches the ring, in order and without a gap"
    );

    let published = await_state(&reader, handle, "the last snapshot to commit", |snapshot| {
        snapshot.revision() == SNAPSHOTS as u64
    })
    .await;
    assert_eq!(
        published.continuity(),
        &MutationContinuity::Intact {
            epoch: 0,
            next_position: expected,
        },
        "published state names the next position, which is exactly where the drained \
         stream now stands"
    );
    assert_eq!(stream.cursor(), &MutationCursor::new(0, expected));

    let starved = running.starved.try_recv();
    assert!(
        matches!(
            starved,
            Err(ObserverRecvError::ContinuityLost {
                reason: ContinuityReason::Overrun,
                ..
            })
        ),
        "the diagnostic observer must really have been overrun for this to prove anything, \
         but it reported {starved:?}"
    );

    running.stop.stop();
    let stats = tokio::time::timeout(STEP_TIMEOUT, running.handle)
        .await
        .expect("the supervisor run ends after stop")
        .expect("the supervisor task completes");
    assert_eq!(stats.snapshots_applied, SNAPSHOTS as u64);
    assert_eq!(
        stats.mutations_derived, expected,
        "the ring received exactly as many mutations as the book derived"
    );
    let _ = std::fs::remove_file(&path);
}

/// An evidence-based book loss derives no mutation, so it reaches a shared-memory consumer
/// as published state — and the stream over that state ends explicitly rather than idling
/// on a position the writer will never reach.
#[tokio::test]
async fn an_evidence_based_loss_reaches_the_segment_as_state_and_ends_the_stream_explicitly() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let segment = open_segment("loss", SEGMENT_LEVEL_CAPACITY);
    let reader = SegmentReader::attach(Arc::clone(&segment.region)).expect("attach a reader");
    let path = segment.path.clone();
    let running = start(peer.endpoint(), segment.book);

    let handle = reader.resolve(&market()).expect("the market is installed");
    let (_, mut stream) = reader.attach_stream(handle).expect("attach a stream");

    let mut connection = peer.next_connection().await;
    let _ = connection.complete_handshake().await;
    send_snapshot(&mut connection, 0).await;
    let live = await_state(&reader, handle, "the base snapshot to commit", |snapshot| {
        snapshot.revision() >= 1
    })
    .await;
    assert_eq!(live.authority(), &AuthorityState::Live);
    assert_eq!(stream.poll(), Ok(EventPoll::Idle));

    drop(connection);
    let stale = await_state(&reader, handle, "the book to report its loss", |snapshot| {
        matches!(snapshot.authority(), AuthorityState::Stale(_))
    })
    .await;
    assert!(
        matches!(
            stale.continuity(),
            MutationContinuity::Lost { epoch: 0, .. }
        ),
        "the loss is published as state, carrying the epoch it broke in: {:?}",
        stale.continuity()
    );
    assert!(
        matches!(stream.poll(), Err(StreamFault::ContinuityLost { .. })),
        "a stream over a lost book is told so, never left idling"
    );

    running.stop.stop();
    let _ = tokio::time::timeout(STEP_TIMEOUT, running.handle)
        .await
        .expect("the supervisor run ends after stop")
        .expect("the supervisor task completes");
    let _ = std::fs::remove_file(&path);
}

/// A refused publication is fatal for the run, because a segment that stopped advancing
/// while the daemon kept running would look live to every consumer attached to it.
///
/// The segment here is one level deep and the venue's first snapshot is two, which is the
/// only refusal a caller can script: a segment sized from the supervisor's own accepted
/// depth cannot be too small for a book it accepted.
#[tokio::test]
async fn a_refused_shared_memory_publication_ends_the_run_loudly() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let segment = open_segment("refused", 1);
    let reader = SegmentReader::attach(Arc::clone(&segment.region)).expect("attach a reader");
    let path = segment.path.clone();

    let mut supervisor =
        Supervisor::new(test_config(peer.endpoint())).expect("a valid configuration");
    supervisor
        .publish_into(segment.book)
        .expect("the empty initial revision fits any segment");
    let handle = reader.resolve(&market()).expect("the market is installed");

    let run = tokio::spawn(async move {
        let stats = supervisor.run_until(Instant::now() + RUN_CAP).await;
        (stats, supervisor.segment_failure().cloned())
    });

    let mut connection = peer.next_connection().await;
    let _ = connection.complete_handshake().await;
    send_snapshot(&mut connection, 0).await;

    let (stats, failure) = tokio::time::timeout(STEP_TIMEOUT, run)
        .await
        .expect("a refused publication ends the run without waiting for the deadline")
        .expect("the supervisor task completes");
    assert_eq!(
        failure,
        Some(WriterError::LevelCapacityExceeded {
            levels: 2,
            capacity: 1
        }),
        "the run reports exactly what refused it"
    );
    assert_eq!(
        stats.snapshots_applied, 1,
        "the book accepted the snapshot; only its publication was refused"
    );
    let published = reader.read(handle).expect("the segment is still readable");
    assert_eq!(
        published.revision(),
        0,
        "nothing is published past the refusal, so no consumer is told a revision the \
         segment does not hold"
    );
    let _ = std::fs::remove_file(&path);
}

/// A supervisor installs at most one segment for its whole lifetime. A second offer must be
/// refused without disturbing the first: consumers already attached to it would otherwise
/// stop receiving commits the moment a second segment took over publication, a silent gap
/// with no explicit signal anywhere.
///
/// The refused segment is checked too — it must never have been written to, which is what
/// pins the guard as running before any trial publish rather than after one.
#[tokio::test]
async fn a_second_publish_into_is_refused_and_the_first_segment_stays_live() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let segment_a = open_segment("second-refused-a", SEGMENT_LEVEL_CAPACITY);
    let segment_b = open_segment("second-refused-b", SEGMENT_LEVEL_CAPACITY);
    let reader_a = SegmentReader::attach(Arc::clone(&segment_a.region)).expect("attach a reader");
    let reader_b = SegmentReader::attach(Arc::clone(&segment_b.region)).expect("attach a reader");
    let path_a = segment_a.path.clone();
    let path_b = segment_b.path.clone();

    let mut supervisor =
        Supervisor::new(test_config(peer.endpoint())).expect("a valid configuration");
    supervisor
        .publish_into(segment_a.book)
        .expect("the initial revision publishes into a segment sized for it");
    assert_eq!(
        supervisor.publish_into(segment_b.book),
        Err(WriterError::SegmentAlreadyInstalled),
        "a supervisor that already installed a segment refuses a second one"
    );

    let handle_a = reader_a
        .resolve(&market())
        .expect("the market is installed");
    let handle_b = reader_b
        .resolve(&market())
        .expect("the market is installed");
    let (attached, mut stream) = reader_a.attach_stream(handle_a).expect("attach a stream");
    assert_eq!(
        attached.revision(),
        0,
        "the installed segment carries the book's initial revision before any commit"
    );
    assert_eq!(stream.cursor(), &MutationCursor::new(0, 0));

    let stop = supervisor.stopper();
    let handle = tokio::spawn(async move {
        let stats = supervisor.run_until(Instant::now() + RUN_CAP).await;
        assert_eq!(
            supervisor.segment_failure(),
            None,
            "a segment sized for this book refuses nothing it is given"
        );
        stats
    });

    let mut connection = peer.next_connection().await;
    let request = connection.complete_handshake().await;
    assert_eq!(request.slugs, vec![SLUG.to_owned()]);

    for step in 0..SNAPSHOTS {
        send_snapshot(&mut connection, step).await;
        let revision = step as u64 + 1;
        let _ = await_state(&reader_a, handle_a, "the snapshot to commit", |snapshot| {
            snapshot.revision() >= revision
        })
        .await;
    }

    let expected = MUTATIONS_PER_SNAPSHOT * (SNAPSHOTS as u64 - 1);
    let cursors = drain(&mut stream, expected as usize).await;
    let contiguous: Vec<MutationCursor> =
        (0..expected).map(|at| MutationCursor::new(0, at)).collect();
    assert_eq!(
        cursors, contiguous,
        "the installed segment's stream keeps receiving every committed position \
         contiguously across the refused second installation"
    );

    assert_eq!(
        reader_b.read(handle_b),
        Err(ReadFault::NoPublishedState),
        "the refused segment carries no published state at all; the guard runs before any \
         trial publish"
    );

    stop.stop();
    let stats = tokio::time::timeout(STEP_TIMEOUT, handle)
        .await
        .expect("the supervisor run ends after stop")
        .expect("the supervisor task completes");
    assert_eq!(stats.snapshots_applied, SNAPSHOTS as u64);
    assert_eq!(
        stats.mutations_derived, expected,
        "the ring received exactly as many mutations as the book derived"
    );
    let _ = std::fs::remove_file(&path_a);
    let _ = std::fs::remove_file(&path_b);
}

/// A latched publication failure is preserved for the life of the supervisor: a second
/// segment offered afterward is refused, and [`Supervisor::segment_failure`] must keep
/// reporting the original failure rather than being erased by the refusal or replaced by
/// it.
#[tokio::test]
async fn a_second_publish_into_after_a_latched_failure_is_refused_and_the_failure_stands() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let segment = open_segment("second-after-latched", 1);
    let reader = SegmentReader::attach(Arc::clone(&segment.region)).expect("attach a reader");
    let path = segment.path.clone();

    let mut supervisor =
        Supervisor::new(test_config(peer.endpoint())).expect("a valid configuration");
    supervisor
        .publish_into(segment.book)
        .expect("the empty initial revision fits any segment");
    let handle = reader.resolve(&market()).expect("the market is installed");

    let run = tokio::spawn(async move {
        let stats = supervisor.run_until(Instant::now() + RUN_CAP).await;
        (stats, supervisor)
    });

    let mut connection = peer.next_connection().await;
    let _ = connection.complete_handshake().await;
    send_snapshot(&mut connection, 0).await;

    let (stats, mut supervisor) = tokio::time::timeout(STEP_TIMEOUT, run)
        .await
        .expect("a refused publication ends the run without waiting for the deadline")
        .expect("the supervisor task completes");
    let original_failure = supervisor.segment_failure().cloned();
    assert_eq!(
        original_failure,
        Some(WriterError::LevelCapacityExceeded {
            levels: 2,
            capacity: 1
        }),
        "the run reports exactly what refused it"
    );
    assert_eq!(
        stats.snapshots_applied, 1,
        "the book accepted the snapshot; only its publication was refused"
    );

    let second = open_segment("second-after-latched-b", SEGMENT_LEVEL_CAPACITY);
    let reader_b = SegmentReader::attach(Arc::clone(&second.region)).expect("attach a reader");
    let path_b = second.path.clone();
    assert_eq!(
        supervisor.publish_into(second.book),
        Err(WriterError::SegmentAlreadyInstalled),
        "a supervisor whose installed segment already latched a failure still refuses a \
         second segment"
    );
    assert_eq!(
        supervisor.segment_failure(),
        original_failure.as_ref(),
        "the latched failure is exactly what it was before the refused second installation"
    );

    let handle_b = reader_b
        .resolve(&market())
        .expect("the market is installed");
    assert_eq!(
        reader_b.read(handle_b),
        Err(ReadFault::NoPublishedState),
        "the refused segment carries no published state at all"
    );
    let published = reader
        .read(handle)
        .expect("the original segment is still readable");
    assert_eq!(
        published.revision(),
        0,
        "nothing further is published into the original segment past its latched failure"
    );

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&path_b);
}

const RESOLUTION_DATE: &str = "2026-09-01T13:11:02.813Z";
const RESOLUTION_TAP_CAPACITY: usize = 64;

/// Reads `expected` deliveries from the segment ring in order, failing on any loss.
///
/// A loss here would be the contract this file exists for breaking: this consumer attached
/// before the first commit and the ring is deeper than the run's history.
async fn drain_deliveries(stream: &mut EventStream, expected: usize) -> Vec<RetainedEvent> {
    let deadline = Instant::now() + STEP_TIMEOUT;
    let mut events = Vec::new();
    while events.len() < expected {
        match stream.poll() {
            Ok(EventPoll::Delivered(event)) => events.push(event),
            Ok(EventPoll::Idle) => {
                assert!(
                    Instant::now() < deadline,
                    "timed out after {} of {expected} deliveries at {:?}",
                    events.len(),
                    stream.cursor()
                );
                tokio::time::sleep(POLL_INTERVAL).await;
            }
            Err(StreamFault::ContinuityLost { reason }) => {
                panic!("the shared-memory consumer lost continuity: {reason:?}")
            }
            Err(StreamFault::Read(fault)) => {
                assert!(
                    Instant::now() < deadline,
                    "timed out on a repeated read fault: {fault:?}"
                );
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        }
    }
    events
}

fn expect_mutation(event: &RetainedEvent, position: u64) {
    let RetainedEvent::Mutation(mutation) = event else {
        panic!("expected a mutation at position {position}, got {event:?}");
    };
    assert_eq!(mutation.cursor(), &MutationCursor::new(0, position));
}

fn expect_resolution(event: &RetainedEvent, position: u64) -> ResolutionEvent {
    let RetainedEvent::Resolution(resolution) = event else {
        panic!("expected a resolution at position {position}, got {event:?}");
    };
    assert_eq!(resolution.cursor(), &MutationCursor::new(0, position));
    resolution.clone()
}

/// A venue resolution is forwarded on the book's own ordered lane, advances the state slot's
/// stream boundary past itself, and changes nothing else about the book.
///
/// The ring holds the run's mutations, the resolution between them, and the mutations of the
/// update that arrived after it, at contiguous positions. Authority stays
/// [`AuthorityState::Live`], the revision the resolution names is the one that was current
/// when it arrived, and the update after the resolution is applied and delivered exactly as
/// the one before it: lifecycle and book state are independent.
///
/// The state slot is republished carrying the *same* `commit_time` the book's own last commit
/// stamped, byte for byte. Both halves of that are the contract. The boundary must advance,
/// because it is what every attachment starts from: a consumer attaching after a delivered
/// resolution must start strictly after it, and a boundary that stood still while the ring
/// advanced would eventually be lapped and wedge every new attachment. The stamp must not
/// move, because it means "when this book revision was stamped", and a resolution commits no
/// revision — restamping it would make a book that has not moved look freshly committed to a
/// consumer keying freshness on it.
#[tokio::test]
async fn a_resolution_is_ordered_with_the_book_and_freezes_nothing() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let segment = open_segment("resolution", SEGMENT_LEVEL_CAPACITY);
    let reader = SegmentReader::attach(Arc::clone(&segment.region)).expect("attach a reader");
    let path = segment.path.clone();
    let running = start(peer.endpoint(), segment.book);

    let handle = reader.resolve(&market()).expect("the market is installed");
    let (_, mut stream) = reader.attach_stream(handle).expect("attach a stream");

    let mut connection = peer.next_connection().await;
    let _ = connection.complete_handshake().await;
    send_snapshot(&mut connection, 0).await;
    let _ = await_state(&reader, handle, "the base snapshot", |snapshot| {
        snapshot.revision() >= 1
    })
    .await;
    send_snapshot(&mut connection, 1).await;
    let before = await_state(&reader, handle, "the first update", |snapshot| {
        snapshot.revision() >= 2
    })
    .await;
    let ordered_after = before.revision();
    let generation_before = reader.publication_generation();
    assert!(
        before.commit_time_nanos().is_some(),
        "the commit this resolution is ordered after carries a stamp, so carrying it forward \
         is a statement about a real value"
    );

    connection
        .send_market_resolved(SLUG, "CLOB", "NO", 1, RESOLUTION_DATE)
        .await;

    let delivered = drain_deliveries(&mut stream, MUTATIONS_PER_SNAPSHOT as usize + 1).await;
    for position in 0..MUTATIONS_PER_SNAPSHOT {
        expect_mutation(&delivered[position as usize], position);
    }
    let resolution = expect_resolution(
        &delivered[MUTATIONS_PER_SNAPSHOT as usize],
        MUTATIONS_PER_SNAPSHOT,
    );
    assert_eq!(resolution.market(), &market());
    assert_eq!(resolution.revision(), ordered_after);
    assert_eq!(resolution.winning_outcome(), "NO");
    assert_eq!(resolution.winning_index(), 1);
    assert_eq!(resolution.market_type(), "CLOB");
    assert_eq!(resolution.resolution_date(), RESOLUTION_DATE);
    assert_eq!(resolution.delivery_path(), &DeliveryPath::MarketFeed);

    let resolved = await_state(
        &reader,
        handle,
        "the state boundary past the resolution",
        |snapshot| {
            snapshot.continuity()
                == &MutationContinuity::Intact {
                    epoch: 0,
                    next_position: MUTATIONS_PER_SNAPSHOT + 1,
                }
        },
    )
    .await;
    assert_eq!(
        resolved.revision(),
        ordered_after,
        "a resolution occupies a delivery position and commits no revision of its own"
    );
    assert_eq!(
        resolved.commit_time_nanos(),
        before.commit_time_nanos(),
        "a book that did not move is never restamped as freshly committed"
    );
    assert_eq!(resolved.authority(), &AuthorityState::Live);
    assert!(
        reader.publication_generation() > generation_before,
        "the slot write itself still advances the publication generation"
    );

    let (attached, mut late) = reader
        .attach_stream(handle)
        .expect("attach a stream after the resolution");
    assert_eq!(
        attached.continuity(),
        &MutationContinuity::Intact {
            epoch: 0,
            next_position: MUTATIONS_PER_SNAPSHOT + 1
        }
    );
    assert_eq!(
        late.cursor(),
        &MutationCursor::new(0, MUTATIONS_PER_SNAPSHOT + 1)
    );
    assert_eq!(
        late.poll(),
        Ok(EventPoll::Idle),
        "an attachment taken after a delivered resolution starts strictly after it and never \
         replays it"
    );

    send_snapshot(&mut connection, 2).await;
    let after = await_state(
        &reader,
        handle,
        "the update that arrived after the resolution",
        |snapshot| snapshot.revision() >= 3,
    )
    .await;
    assert_eq!(
        after.authority(),
        &AuthorityState::Live,
        "a resolution never touches book authority"
    );
    assert_eq!(
        after.continuity(),
        &MutationContinuity::Intact {
            epoch: 0,
            next_position: 2 * MUTATIONS_PER_SNAPSHOT + 1
        }
    );

    for consumer in [&mut stream, &mut late] {
        let events = drain_deliveries(consumer, MUTATIONS_PER_SNAPSHOT as usize).await;
        for offset in 0..MUTATIONS_PER_SNAPSHOT {
            expect_mutation(
                &events[offset as usize],
                MUTATIONS_PER_SNAPSHOT + 1 + offset,
            );
        }
        assert_eq!(consumer.poll(), Ok(EventPoll::Idle));
    }

    running.stop.stop();
    let stats = tokio::time::timeout(STEP_TIMEOUT, running.handle)
        .await
        .expect("the supervisor run ends after stop")
        .expect("the supervisor task completes");
    assert_eq!(stats.events_resolved, 1);
    assert_eq!(stats.snapshots_applied, 3);
    let _ = std::fs::remove_file(&path);
}

/// The venue's repeated delivery of one resolution is reproduced, not deduplicated.
///
/// `docs/limitless.md` records this venue publishing one resolution as several
/// byte-identical frames. Reproducing venue-reported data means forwarding each arrival: the
/// daemon never decides which of them was the real one, so three arrivals occupy three
/// stream positions.
#[tokio::test]
async fn three_identical_resolutions_reach_the_ring_three_times() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let segment = open_segment("repeated", SEGMENT_LEVEL_CAPACITY);
    let reader = SegmentReader::attach(Arc::clone(&segment.region)).expect("attach a reader");
    let path = segment.path.clone();
    let running = start(peer.endpoint(), segment.book);

    let handle = reader.resolve(&market()).expect("the market is installed");
    let (_, mut stream) = reader.attach_stream(handle).expect("attach a stream");

    let mut connection = peer.next_connection().await;
    let _ = connection.complete_handshake().await;
    send_snapshot(&mut connection, 0).await;
    let _ = await_state(&reader, handle, "the base snapshot", |snapshot| {
        snapshot.revision() >= 1
    })
    .await;

    for _ in 0..3 {
        connection
            .send_market_resolved(SLUG, "CLOB", "NO", 1, RESOLUTION_DATE)
            .await;
    }
    let events = drain_deliveries(&mut stream, 3).await;
    for (position, event) in events.iter().enumerate() {
        let resolution = expect_resolution(event, position as u64);
        assert_eq!(resolution.winning_outcome(), "NO");
        assert_eq!(resolution.revision(), 1);
    }
    assert_eq!(stream.poll(), Ok(EventPoll::Idle));

    running.stop.stop();
    let stats = tokio::time::timeout(STEP_TIMEOUT, running.handle)
        .await
        .expect("the supervisor run ends after stop")
        .expect("the supervisor task completes");
    assert_eq!(stats.events_resolved, 3);
    let _ = std::fs::remove_file(&path);
}

/// A ring shallow enough that a handful of resolutions laps it. Every ring depth is a power
/// of two.
const BURST_EVENT_CAPACITY: u32 = 4;

/// A resolution-only burst that laps the ring overruns the consumer it outran, and leaves
/// every later attachment able to attach.
///
/// This is what the state slot's boundary is for. The burst commits no revision, so the only
/// thing that moves the boundary past those positions is the resolution's own republish of
/// state. Without it the boundary stands still at the position the ring is busy overwriting,
/// and [`SegmentReader::attach_stream`]'s coherence probe — which rereads state and retries
/// whenever the slot it probes already holds a later cursor — never settles: every new
/// attachment fails [`ReadFault::Contended`] for the rest of the run, on a segment that is
/// otherwise perfectly healthy. A permanent attach failure is not a loss any consumer can be
/// told about, which is why it must be impossible by construction rather than reported.
///
/// The consumer that *was* attached across the burst is a different case and keeps the
/// ordinary ring contract: it is lapped, told so explicitly with
/// [`ContinuityReason::Overrun`], and recovers by reattaching past the burst.
#[tokio::test]
async fn a_resolution_burst_that_laps_the_ring_never_wedges_a_later_attachment() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let segment = open_segment_with_events(
        "resolution-burst",
        SEGMENT_LEVEL_CAPACITY,
        BURST_EVENT_CAPACITY,
    );
    let reader = SegmentReader::attach(Arc::clone(&segment.region)).expect("attach a reader");
    let path = segment.path.clone();

    let (sender, mut tap) = mpsc::channel(RESOLUTION_TAP_CAPACITY);
    let mut supervisor = Supervisor::new(SupervisorConfig {
        observer_capacity: 64,
        ..test_config(peer.endpoint())
    })
    .expect("a valid configuration")
    .with_diagnostics(sender);
    supervisor
        .publish_into(segment.book)
        .expect("the empty initial revision fits any segment");
    let stop = supervisor.stopper();
    let handle = reader.resolve(&market()).expect("the market is installed");
    let (_, mut lapped) = reader.attach_stream(handle).expect("attach a stream");
    assert_eq!(lapped.cursor(), &MutationCursor::new(0, 0));
    let run = tokio::spawn(async move {
        let stats = supervisor.run_until(Instant::now() + RUN_CAP).await;
        (stats, supervisor.segment_failure().cloned())
    });

    let mut connection = peer.next_connection().await;
    let _ = connection.complete_handshake().await;
    send_snapshot(&mut connection, 0).await;
    let _ = await_state(&reader, handle, "the base snapshot", |snapshot| {
        snapshot.revision() >= 1
    })
    .await;

    let burst = u64::from(BURST_EVENT_CAPACITY) + 1;
    for _ in 0..burst {
        connection
            .send_market_resolved(SLUG, "CLOB", "NO", 1, RESOLUTION_DATE)
            .await;
    }
    for _ in 0..burst {
        await_notice(&mut tap, "a resolution on the tap", |notice| {
            matches!(notice, SupervisorNotice::Event(event)
                if matches!(event, pm_ws::limitless::LimitlessEvent::MarketResolved(_)))
            .then_some(())
        })
        .await;
    }

    let (fresh_state, mut fresh) = reader
        .attach_stream(handle)
        .expect("a fresh attachment after a resolution-only burst is coherent");
    assert_eq!(
        fresh_state.continuity(),
        &MutationContinuity::Intact {
            epoch: 0,
            next_position: burst
        },
        "the boundary tracks the ring's tip through a burst that commits no revision"
    );
    assert_eq!(fresh.cursor(), &MutationCursor::new(0, burst));
    assert_eq!(fresh.poll(), Ok(EventPoll::Idle));

    assert_eq!(
        lapped.poll(),
        Err(StreamFault::ContinuityLost {
            reason: ContinuityReason::Overrun
        }),
        "the consumer the ring lapped is told, explicitly and never partially"
    );
    let resumed = lapped
        .reattach()
        .expect("the overrun consumer reattaches past the burst");
    assert_eq!(
        resumed.continuity(),
        &MutationContinuity::Intact {
            epoch: 0,
            next_position: burst
        }
    );
    assert_eq!(lapped.cursor(), &MutationCursor::new(0, burst));

    send_snapshot(&mut connection, 1).await;
    for consumer in [&mut fresh, &mut lapped] {
        let events = drain_deliveries(consumer, MUTATIONS_PER_SNAPSHOT as usize).await;
        for offset in 0..MUTATIONS_PER_SNAPSHOT {
            expect_mutation(&events[offset as usize], burst + offset);
        }
        assert_eq!(consumer.poll(), Ok(EventPoll::Idle));
    }

    stop.stop();
    let (stats, failure) = tokio::time::timeout(STEP_TIMEOUT, run)
        .await
        .expect("the supervisor run ends after stop")
        .expect("the supervisor task completes");
    assert_eq!(
        failure, None,
        "a ring that wraps is not a refused publication"
    );
    assert_eq!(stats.events_resolved, burst);
    assert_eq!(stats.snapshots_applied, 2);
    let _ = std::fs::remove_file(&path);
}

/// A two-connection run with a diagnostic tap, publishing into `segment`.
struct RunningPair {
    stop: Stopper,
    handle: JoinHandle<SupervisorStats>,
    tap: mpsc::Receiver<SupervisorNotice>,
}

fn start_pair(endpoint: String, segment: BookSegment) -> RunningPair {
    let (sender, tap) = mpsc::channel(RESOLUTION_TAP_CAPACITY);
    let mut supervisor = Supervisor::new(SupervisorConfig {
        replicas: 2,
        observer_capacity: 64,
        ..test_config(endpoint)
    })
    .expect("a valid configuration")
    .with_diagnostics(sender);
    supervisor
        .publish_into(segment)
        .expect("the initial revision publishes into a segment sized for it");
    let stop = supervisor.stopper();
    let handle = tokio::spawn(async move { supervisor.run_until(Instant::now() + RUN_CAP).await });
    RunningPair { stop, handle, tap }
}

/// Waits for the next tap notice `select` accepts, failing with `what` on timeout.
async fn await_notice<T>(
    tap: &mut mpsc::Receiver<SupervisorNotice>,
    what: &str,
    select: impl Fn(&SupervisorNotice) -> Option<T>,
) -> T {
    let deadline = Instant::now() + STEP_TIMEOUT;
    loop {
        let notice = tokio::time::timeout_at(deadline, tap.recv())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
            .unwrap_or_else(|| panic!("the diagnostic tap closed while waiting for {what}"));
        if let Some(found) = select(&notice) {
            return found;
        }
    }
}

/// Accepts both connections and returns them ordered `(primary, standby)` by the role the
/// daemon announced for each Engine.IO session id.
async fn connect_pair(
    peer: &mut ControlledPeer,
    tap: &mut mpsc::Receiver<SupervisorNotice>,
) -> (PeerConnection, PeerConnection) {
    let mut first = peer.next_connection().await;
    let _ = first.complete_handshake().await;
    let mut second = peer.next_connection().await;
    let _ = second.complete_handshake().await;
    let mut roles = std::collections::BTreeMap::new();
    while roles.len() < 2 {
        let (sid, replica) =
            await_notice(tap, "a connection announcement", |notice| match notice {
                SupervisorNotice::Connected { sid, replica, .. } => {
                    Some((sid.clone(), replica.clone()))
                }
                _ => None,
            })
            .await;
        let _ = roles.insert(sid, replica);
    }
    let role_of = |connection: &PeerConnection| {
        roles
            .get(&connection.engine_sid())
            .cloned()
            .expect("the daemon announced a role for every accepted connection")
    };
    match (role_of(&first), role_of(&second)) {
        (ReplicaRole::PublishingPrimary, ReplicaRole::HotStandby) => (first, second),
        (ReplicaRole::HotStandby, ReplicaRole::PublishingPrimary) => (second, first),
        (left, right) => panic!("expected one primary and one standby, got {left:?} and {right:?}"),
    }
}

/// A standby's copy of a resolution is counted and tapped, and reaches no consumer.
///
/// One publishing primary owns every consumer lane. A standby holds a second copy of the
/// venue's stream and publishes nothing from it, so its copy of the same venue report is
/// diagnostic evidence and never a delivery: forwarding both would put one venue event on
/// the stream twice under two different positions, which no consumer could tell from the
/// venue genuinely reporting twice.
#[tokio::test]
async fn a_standby_resolution_is_counted_and_tapped_but_reaches_no_consumer() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let segment = open_segment("standby-resolution", SEGMENT_LEVEL_CAPACITY);
    let reader = SegmentReader::attach(Arc::clone(&segment.region)).expect("attach a reader");
    let path = segment.path.clone();
    let mut running = start_pair(peer.endpoint(), segment.book);

    let handle = reader.resolve(&market()).expect("the market is installed");
    let (_, mut stream) = reader.attach_stream(handle).expect("attach a stream");
    let (mut primary, mut standby) = connect_pair(&mut peer, &mut running.tap).await;

    send_snapshot(&mut primary, 0).await;
    let _ = await_state(&reader, handle, "the base snapshot", |snapshot| {
        snapshot.revision() >= 1
    })
    .await;

    standby
        .send_market_resolved(SLUG, "CLOB", "NO", 1, RESOLUTION_DATE)
        .await;
    primary
        .send_market_resolved(SLUG, "CLOB", "NO", 1, RESOLUTION_DATE)
        .await;
    for _ in 0..2 {
        await_notice(&mut running.tap, "a resolution on the tap", |notice| {
            matches!(notice, SupervisorNotice::Event(event)
                if matches!(event, pm_ws::limitless::LimitlessEvent::MarketResolved(_)))
            .then_some(())
        })
        .await;
    }

    let events = drain_deliveries(&mut stream, 1).await;
    let _ = expect_resolution(&events[0], 0);
    assert_eq!(
        stream.poll(),
        Ok(EventPoll::Idle),
        "both arrivals were processed, and only the primary's occupies a position"
    );

    running.stop.stop();
    let stats = tokio::time::timeout(STEP_TIMEOUT, running.handle)
        .await
        .expect("the supervisor run ends after stop")
        .expect("the supervisor task completes");
    assert_eq!(
        stats.events_resolved, 2,
        "every slot's arrival is counted, whichever role carried it"
    );
    let _ = std::fs::remove_file(&path);
}

/// A venue resolution text wider than its fixed cell is retained and counted, and reaches
/// neither delivery lane — while a segment is installed.
///
/// The vocabulary a resolution is recorded in is wider than the cells the segment stores it
/// in, so the venue can name a winner this ABI cannot carry. Truncating would name a
/// different winner, and refusing it at the segment writer would be worse still: the
/// in-process lane would have delivered a resolution at a position the ring was then denied,
/// every attached shared-memory consumer would poll that unwritten slot forever, and a value
/// the venue chose would end the run. It is judged before a position is allocated instead —
/// counted under its own decode-failure key, delivered to neither lane, retained in full
/// because what the venue reported is true whatever this daemon can carry — and the run goes
/// on. The position it did not take is the next mutation's.
#[tokio::test]
async fn a_resolution_text_the_segment_cannot_carry_is_retained_counted_and_never_delivered() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let segment = open_segment("overlong-resolution", SEGMENT_LEVEL_CAPACITY);
    let reader = SegmentReader::attach(Arc::clone(&segment.region)).expect("attach a reader");
    let path = segment.path.clone();

    let mut supervisor = Supervisor::new(SupervisorConfig {
        observer_capacity: 64,
        ..test_config(peer.endpoint())
    })
    .expect("a valid configuration");
    supervisor
        .publish_into(segment.book)
        .expect("the empty initial revision fits any segment");
    let mut observer = supervisor.attach();
    let stop = supervisor.stopper();
    let handle = reader.resolve(&market()).expect("the market is installed");
    let (_, mut stream) = reader.attach_stream(handle).expect("attach a stream");
    let run = tokio::spawn(async move {
        let stats = supervisor.run_until(Instant::now() + RUN_CAP).await;
        (
            stats,
            supervisor.segment_failure().cloned(),
            supervisor.latest_resolution().cloned(),
        )
    });

    let mut connection = peer.next_connection().await;
    let _ = connection.complete_handshake().await;
    send_snapshot(&mut connection, 0).await;
    let _ = await_state(&reader, handle, "the base snapshot", |snapshot| {
        snapshot.revision() >= 1
    })
    .await;
    let overlong = "N".repeat(65);
    connection
        .send_market_resolved(SLUG, "CLOB", &overlong, 1, RESOLUTION_DATE)
        .await;
    send_snapshot(&mut connection, 1).await;

    let after = await_state(
        &reader,
        handle,
        "the update that arrived after the refused resolution",
        |snapshot| snapshot.revision() >= 2,
    )
    .await;
    assert_eq!(
        after.authority(),
        &AuthorityState::Live,
        "a resolution this daemon cannot carry still never touches book authority"
    );
    assert_eq!(
        after.continuity(),
        &MutationContinuity::Intact {
            epoch: 0,
            next_position: MUTATIONS_PER_SNAPSHOT
        },
        "the unrepresentable resolution consumed no stream position"
    );

    let events = drain_deliveries(&mut stream, MUTATIONS_PER_SNAPSHOT as usize).await;
    for position in 0..MUTATIONS_PER_SNAPSHOT {
        expect_mutation(&events[position as usize], position);
    }
    assert_eq!(
        stream.poll(),
        Ok(EventPoll::Idle),
        "the ring holds the update's mutations and nothing else"
    );

    for position in 0..MUTATIONS_PER_SNAPSHOT {
        let delivery = tokio::time::timeout(STEP_TIMEOUT, observer.recv())
            .await
            .expect("the in-process observer is not left waiting")
            .expect("this attachment loses nothing");
        let StreamDelivery::Mutation(delivery) = delivery else {
            panic!("the in-process lane received a resolution the ring never got: {delivery:?}");
        };
        assert_eq!(delivery.cursor(), &MutationCursor::new(0, position));
    }

    stop.stop();
    let (stats, failure, retained) = tokio::time::timeout(STEP_TIMEOUT, run)
        .await
        .expect("the supervisor run ends after stop")
        .expect("the supervisor task completes");
    assert_eq!(
        failure, None,
        "no venue value reaches the segment writer's refusal, so the run is never ended by one"
    );
    assert_eq!(stats.events_resolved, 1);
    assert_eq!(stats.snapshots_applied, 2);
    assert_eq!(
        stats.decode_failures.get("resolution:Unrepresentable"),
        Some(&1),
        "the arrival is counted under its own key, apart from a value the domain types reject"
    );
    let retained = retained.expect("the resolution is retained whatever the lanes could carry");
    assert_eq!(retained.winner().text_value(), Some(overlong.as_str()));
    assert_eq!(retained.winning_index(), 1);
    let _ = std::fs::remove_file(&path);
}

/// The other half of the fork above: with no segment installed, the same venue report reaches
/// the in-process lane in full.
///
/// The shared-memory cells are why that report is dropped when a segment is publishing — both
/// lanes must carry one identical sequence, so a report the ring cannot hold reaches neither.
/// A run publishing into no segment has no such second lane to keep in step, and an
/// in-process delivery carries the venue's text in a `String` that no fixed cell bounds.
/// Narrowing it there would deny the run's only consumer a domain-valid venue report for a
/// reason that does not apply to it. Nothing is counted as a failure, because nothing failed;
/// the position is consumed like any other delivery's, so the next mutation follows
/// contiguously.
#[tokio::test]
async fn a_resolution_text_no_segment_could_carry_reaches_a_run_that_publishes_into_none() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut supervisor = Supervisor::new(SupervisorConfig {
        observer_capacity: 64,
        ..test_config(peer.endpoint())
    })
    .expect("a valid configuration");
    let mut observer = supervisor.attach();
    let stop = supervisor.stopper();
    let run = tokio::spawn(async move {
        let stats = supervisor.run_until(Instant::now() + RUN_CAP).await;
        (stats, supervisor.latest_resolution().cloned())
    });

    let mut connection = peer.next_connection().await;
    let _ = connection.complete_handshake().await;
    send_snapshot(&mut connection, 0).await;
    let overlong = "N".repeat(65);
    connection
        .send_market_resolved(SLUG, "CLOB", &overlong, 1, RESOLUTION_DATE)
        .await;
    send_snapshot(&mut connection, 1).await;

    let delivery = tokio::time::timeout(STEP_TIMEOUT, observer.recv())
        .await
        .expect("the in-process observer is not left waiting")
        .expect("this attachment loses nothing");
    let StreamDelivery::Resolution(delivery) = delivery else {
        panic!("the resolution the shm cells could not carry was withheld anyway: {delivery:?}");
    };
    assert_eq!(delivery.cursor(), &MutationCursor::new(0, 0));
    assert_eq!(
        delivery.resolution().winner().text_value(),
        Some(overlong.as_str()),
        "the venue's own text crosses the in-process lane whole"
    );
    assert_eq!(delivery.resolution().winning_index(), 1);
    for position in 1..=MUTATIONS_PER_SNAPSHOT {
        let delivery = tokio::time::timeout(STEP_TIMEOUT, observer.recv())
            .await
            .expect("the in-process observer is not left waiting")
            .expect("this attachment loses nothing");
        let StreamDelivery::Mutation(delivery) = delivery else {
            panic!("expected a mutation at position {position}, got {delivery:?}");
        };
        assert_eq!(delivery.cursor(), &MutationCursor::new(0, position));
    }

    stop.stop();
    let (stats, retained) = tokio::time::timeout(STEP_TIMEOUT, run)
        .await
        .expect("the supervisor run ends after stop")
        .expect("the supervisor task completes");
    assert_eq!(stats.events_resolved, 1);
    assert_eq!(stats.snapshots_applied, 2);
    assert_eq!(
        stats.decode_failures.get("resolution:Unrepresentable"),
        None,
        "a report this run delivered in full is not a failure of any kind"
    );
    let retained = retained.expect("the resolution is retained as it is in every other run");
    assert_eq!(retained.winner().text_value(), Some(overlong.as_str()));
}

/// Wall-clock nanoseconds since the Unix epoch, for bracketing a real socket-read arrival
/// this test caused, against the stamp the segment reports for it.
fn wall_clock_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the clock is after the epoch")
        .as_nanos()
        .try_into()
        .expect("the current time fits a u64 nanosecond count")
}

fn mutation_arrival(event: &RetainedEvent) -> Option<u64> {
    let RetainedEvent::Mutation(mutation) = event else {
        panic!("expected a mutation, got {event:?}");
    };
    mutation.arrival_time_nanos()
}

fn resolution_arrival(event: &RetainedEvent) -> Option<u64> {
    let RetainedEvent::Resolution(resolution) = event else {
        panic!("expected a resolution, got {event:?}");
    };
    resolution.arrival_time_nanos()
}

/// A commit driven by a real venue frame stamps its state slot and every mutation the ring
/// received in that same round with one identical, nonzero, wall-clock arrival: the instant
/// this run's connection read the socket frame that produced the commit.
///
/// The base snapshot derives no mutation, so the bracketed commit is the second snapshot,
/// which derives [`MUTATIONS_PER_SNAPSHOT`].
#[tokio::test]
async fn a_committed_frame_stamps_state_and_its_mutations_with_the_same_nonzero_arrival() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let segment = open_segment("arrival-commit", SEGMENT_LEVEL_CAPACITY);
    let reader = SegmentReader::attach(Arc::clone(&segment.region)).expect("attach a reader");
    let path = segment.path.clone();
    let running = start(peer.endpoint(), segment.book);

    let handle = reader.resolve(&market()).expect("the market is installed");
    let (_, mut stream) = reader.attach_stream(handle).expect("attach a stream");

    let mut connection = peer.next_connection().await;
    let _ = connection.complete_handshake().await;
    send_snapshot(&mut connection, 0).await;
    let _ = await_state(&reader, handle, "the base snapshot", |snapshot| {
        snapshot.revision() >= 1
    })
    .await;

    let before = wall_clock_nanos();
    send_snapshot(&mut connection, 1).await;
    let published = await_state(
        &reader,
        handle,
        "the second snapshot to commit",
        |snapshot| snapshot.revision() >= 2,
    )
    .await;
    let after = wall_clock_nanos();

    let state_arrival = published
        .arrival_time_nanos()
        .expect("a commit driven by a venue frame stamps a nonzero state arrival");
    assert!(
        (before..=after).contains(&state_arrival),
        "the state arrival {state_arrival} must fall inside [{before}, {after}]"
    );

    let events = drain_deliveries(&mut stream, MUTATIONS_PER_SNAPSHOT as usize).await;
    for (position, event) in events.iter().enumerate() {
        expect_mutation(event, position as u64);
        assert_eq!(
            mutation_arrival(event),
            Some(state_arrival),
            "every mutation this commit derived carries the same arrival as the state it \
             produced"
        );
    }

    running.stop.stop();
    let _ = tokio::time::timeout(STEP_TIMEOUT, running.handle)
        .await
        .expect("the supervisor run ends after stop")
        .expect("the supervisor task completes");
    let _ = std::fs::remove_file(&path);
}

/// A resolution's own ring entry carries the wall-clock arrival of the resolution frame
/// itself, and the state republish that follows it — which advances the stream boundary but
/// commits no revision — carries forward the arrival the book's last commit stamped rather
/// than adopting the resolution's: a book that has not moved is never reported as having
/// just arrived.
#[tokio::test]
async fn a_resolution_republish_carries_the_previous_arrival_not_its_own() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let segment = open_segment("arrival-resolution", SEGMENT_LEVEL_CAPACITY);
    let reader = SegmentReader::attach(Arc::clone(&segment.region)).expect("attach a reader");
    let path = segment.path.clone();
    let running = start(peer.endpoint(), segment.book);

    let handle = reader.resolve(&market()).expect("the market is installed");
    let (_, mut stream) = reader.attach_stream(handle).expect("attach a stream");

    let mut connection = peer.next_connection().await;
    let _ = connection.complete_handshake().await;
    send_snapshot(&mut connection, 0).await;
    let _ = await_state(&reader, handle, "the base snapshot", |snapshot| {
        snapshot.revision() >= 1
    })
    .await;
    send_snapshot(&mut connection, 1).await;
    let before = await_state(&reader, handle, "the first update", |snapshot| {
        snapshot.revision() >= 2
    })
    .await;
    let committed_arrival = before
        .arrival_time_nanos()
        .expect("the commit this resolution is ordered after carries a real arrival");

    let resolution_window_start = wall_clock_nanos();
    connection
        .send_market_resolved(SLUG, "CLOB", "NO", 1, RESOLUTION_DATE)
        .await;

    let delivered = drain_deliveries(&mut stream, MUTATIONS_PER_SNAPSHOT as usize + 1).await;
    let resolution_window_end = wall_clock_nanos();
    let resolution_event = &delivered[MUTATIONS_PER_SNAPSHOT as usize];
    let resolution_own_arrival = resolution_arrival(resolution_event)
        .expect("a resolution driven by a venue frame stamps a nonzero arrival");
    assert!(
        (resolution_window_start..=resolution_window_end).contains(&resolution_own_arrival),
        "the resolution's own arrival {resolution_own_arrival} must fall inside \
         [{resolution_window_start}, {resolution_window_end}]"
    );

    let resolved = await_state(
        &reader,
        handle,
        "the state boundary past the resolution",
        |snapshot| {
            snapshot.continuity()
                == &MutationContinuity::Intact {
                    epoch: 0,
                    next_position: MUTATIONS_PER_SNAPSHOT + 1,
                }
        },
    )
    .await;
    assert_eq!(
        resolved.arrival_time_nanos(),
        Some(committed_arrival),
        "the state republish past a resolution carries the previous commit's arrival, never \
         the resolution's own"
    );

    running.stop.stop();
    let _ = tokio::time::timeout(STEP_TIMEOUT, running.handle)
        .await
        .expect("the supervisor run ends after stop")
        .expect("the supervisor task completes");
    let _ = std::fs::remove_file(&path);
}

/// An evidence-based continuity loss derives no mutation and is driven by nothing the venue
/// sent, so the state it publishes carries a zero arrival even though the book's last real
/// commit had a nonzero one.
#[tokio::test]
async fn an_evidence_based_loss_publishes_state_with_a_zero_arrival() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let segment = open_segment("arrival-loss", SEGMENT_LEVEL_CAPACITY);
    let reader = SegmentReader::attach(Arc::clone(&segment.region)).expect("attach a reader");
    let path = segment.path.clone();
    let running = start(peer.endpoint(), segment.book);

    let handle = reader.resolve(&market()).expect("the market is installed");

    let mut connection = peer.next_connection().await;
    let _ = connection.complete_handshake().await;
    send_snapshot(&mut connection, 0).await;
    let live = await_state(&reader, handle, "the base snapshot to commit", |snapshot| {
        snapshot.revision() >= 1
    })
    .await;
    assert!(
        live.arrival_time_nanos().is_some(),
        "the base commit was driven by a real venue frame"
    );

    drop(connection);
    let stale = await_state(&reader, handle, "the book to report its loss", |snapshot| {
        matches!(snapshot.authority(), AuthorityState::Stale(_))
    })
    .await;
    assert_eq!(
        stale.arrival_time_nanos(),
        None,
        "an evidence-based loss is not driven by any venue frame, so its state carries no \
         arrival"
    );

    running.stop.stop();
    let _ = tokio::time::timeout(STEP_TIMEOUT, running.handle)
        .await
        .expect("the supervisor run ends after stop")
        .expect("the supervisor task completes");
    let _ = std::fs::remove_file(&path);
}

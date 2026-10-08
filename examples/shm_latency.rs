//! Commit-to-reader latency across two processes, reported as a distribution.
//!
//! The parent creates a segment file, spawns itself as the measuring child, then publishes
//! revisions at a fixed interval. Each publication stamps `commit_time` (wall clock, since
//! two processes share no monotonic origin); the child busy-polls the segment and, on every
//! revision change, differences its own wall clock against that stamp. A sample is kept
//! when the difference is non-negative and below `IMPLAUSIBLE_NANOS`; a backwards or absurd
//! one — what a clock adjustment between the two reads produces — is discarded and counted
//! rather than clamped, because a silently clamped set is a worse artifact than an honest
//! one with a discard count. A zero difference is kept, not discarded: the host's realtime
//! clock has its own quantum, and dropping the samples landing inside it would bias the
//! distribution against exactly the fastest reads.
//!
//! `--events` measures the other surface the same way: the child attaches a mutation stream
//! and differences its own wall clock against each delivered event's own `commit_time`,
//! under exactly the same discard-don't-clamp rule.
//!
//! Usage: `shm_latency [--samples <n>] [--interval-micros <n>] [--events]`.

use pm_ws::{
    BoundedLevels, BoundedSourceEvidence, Candidate, ConnectionIdentity, DecimalGrammar, Level,
    LevelCapacity, LocalMonotonicTimestamp, Origin, Price, Provenance, ProvenanceInput, Quantity,
    ReplicaRole, Representation, Side, SourceEvidenceCapacity,
};
use pm_ws::{
    DEFAULT_DIRTY_CAPACITY, DEFAULT_EVENT_CAPACITY, EventPoll, MarketHandle, MarketRef,
    NativeIdentifierKind, NativeMarketKey, OrderBook, ReadFault, SegmentConfig, SegmentLayout,
    SegmentReader, SegmentRegion, SegmentWriter, StreamFault, Venue,
};
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const SLUG: &str = "shm-latency-bench";
const IMPLAUSIBLE_NANOS: u64 = 1_000_000_000;

/// The fewest samples a reported distribution may rest on. A p99.9 over a handful of
/// samples is a number without a meaning, so the bench fails rather than printing one.
const MIN_REPORTABLE_SAMPLES: usize = 1_000;

fn main() {
    let mut samples = 20_000_usize;
    let mut interval = 50_u64;
    let mut measure: Option<PathBuf> = None;
    let mut events = false;
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        if flag == "--events" {
            events = true;
            continue;
        }
        let value = args.next().unwrap_or_default();
        match flag.as_str() {
            "--samples" => {
                samples = value.parse().expect("--samples");
                assert!(samples > 0, "--samples must be positive");
            }
            "--interval-micros" => interval = value.parse().expect("--interval-micros"),
            "--measure" => measure = Some(PathBuf::from(value)),
            other => panic!("unknown flag {other}"),
        }
    }
    match measure {
        Some(path) => measure_role(&path, samples, events),
        None => publish_role(samples, interval, events),
    }
}

/// The measuring core: busy-poll the segment and difference the reader's clock against the
/// writer's stamp on every revision change.
fn measure_role(path: &std::path::Path, samples: usize, events: bool) {
    let reader = attach(path);
    let handle = loop {
        if let Some(handle) = reader.resolve(&market()) {
            break handle;
        }
    };
    if events {
        let (deltas, discarded) = measure_events(&reader, handle, samples);
        report("commit-to-event", deltas, discarded, samples);
        return;
    }
    let mut deltas = Vec::with_capacity(samples);
    let mut discarded = 0_u64;
    let mut last = 0_u64;
    let deadline = Instant::now() + Duration::from_secs(60);
    while deltas.len() < samples && Instant::now() < deadline {
        match reader.read(handle) {
            Ok(book) if book.revision() != last => {
                last = book.revision();
                let observed = now_nanos();
                match book.commit_time_nanos() {
                    Some(stamped)
                        if observed >= stamped && observed - stamped < IMPLAUSIBLE_NANOS =>
                    {
                        deltas.push(observed - stamped);
                    }
                    _ => discarded += 1,
                }
            }
            Ok(_)
            | Err(
                ReadFault::NoPublishedState
                | ReadFault::Contended { .. }
                | ReadFault::WriterStalled { .. },
            ) => {}
            Err(other) => panic!("read: {other:?}"),
        }
    }
    report("commit-to-reader", deltas, discarded, samples);
}

/// Busy-polls one market's retained-event ring, differencing the reader's clock against
/// each delivered event's own stamp under the same discard rule as the state path.
///
/// An overrun is not a measurement failure: the ring wraps, so a measuring child that falls
/// behind reattaches and keeps sampling rather than reporting a distribution it stopped
/// filling.
fn measure_events(reader: &SegmentReader, handle: MarketHandle, samples: usize) -> (Vec<u64>, u64) {
    let mut stream = loop {
        if let Ok((_, stream)) = reader.attach_stream(handle) {
            break stream;
        }
    };
    let mut deltas = Vec::with_capacity(samples);
    let mut discarded = 0_u64;
    let deadline = Instant::now() + Duration::from_secs(60);
    while deltas.len() < samples && Instant::now() < deadline {
        match stream.poll() {
            Ok(EventPoll::Delivered(event)) => {
                let observed = now_nanos();
                match event.commit_time_nanos() {
                    Some(stamped)
                        if observed >= stamped && observed - stamped < IMPLAUSIBLE_NANOS =>
                    {
                        deltas.push(observed - stamped);
                    }
                    _ => discarded += 1,
                }
            }
            Ok(EventPoll::Idle) | Err(StreamFault::Read(_)) => {}
            Err(StreamFault::ContinuityLost { .. }) => {
                let _ = stream.reattach();
            }
        }
    }
    (deltas, discarded)
}

fn report(label: &str, mut deltas: Vec<u64>, discarded: u64, samples: usize) {
    assert!(
        deltas.len() >= samples.min(MIN_REPORTABLE_SAMPLES),
        "collected {} samples, too few to report a distribution",
        deltas.len()
    );
    deltas.sort_unstable();
    println!(
        "{label} n={} discarded={discarded} p50={}ns p99={}ns p99.9={}ns min={}ns max={}ns",
        deltas.len(),
        quantile(&deltas, 500),
        quantile(&deltas, 990),
        quantile(&deltas, 999),
        deltas.first().copied().unwrap_or(0),
        deltas.last().copied().unwrap_or(0),
    );
}

fn publish_role(samples: usize, interval_micros: u64, events: bool) {
    let mut path = std::env::temp_dir();
    path.push(format!("pm-ws-latency-{}.seg", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let layout = SegmentLayout::new(1, 1, 8, DEFAULT_EVENT_CAPACITY, DEFAULT_DIRTY_CAPACITY)
        .expect("layout");
    let region = Arc::new(SegmentRegion::create_file(&path, layout.region_size()).expect("map"));
    let mut writer = SegmentWriter::create(Arc::clone(&region), SegmentConfig::new(layout, 1, 1))
        .expect("format");
    let handle = writer.install(&market()).expect("install");
    let mut book = OrderBook::new(market());
    let mut command = Command::new(std::env::current_exe().expect("self"));
    let _ = command
        .arg("--measure")
        .arg(&path)
        .arg("--samples")
        .arg(samples.to_string());
    if events {
        let _ = command.arg("--events");
    }
    let mut child = command.spawn().expect("spawn measuring child");
    let interval = Duration::from_micros(interval_micros);
    for step in 0..samples + samples / 4 {
        let commit = book.apply_snapshot(&candidate(step)).expect("snapshot");
        writer.publish(handle, &book.publish(), 0).expect("publish");
        for record in commit.mutations() {
            writer
                .publish_mutation(
                    handle,
                    commit.revision(),
                    record.cursor(),
                    record.mutation(),
                    0,
                )
                .expect("publish mutation");
        }
        std::thread::sleep(interval);
    }
    let status = child.wait().expect("child exits");
    let _ = std::fs::remove_file(&path);
    assert!(status.success(), "the measuring child failed: {status}");
}

fn attach(path: &std::path::Path) -> SegmentReader {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(region) = SegmentRegion::open_file(path)
            && let Ok(reader) = SegmentReader::attach(Arc::new(region))
        {
            return reader;
        }
        assert!(Instant::now() < deadline, "segment never became readable");
    }
}

/// The `permille`-th value of a sorted sample set, by nearest-rank; 0 for an empty set.
fn quantile(sorted: &[u64], permille: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (sorted.len() * permille).div_ceil(1000).max(1) - 1;
    sorted[rank.min(sorted.len() - 1)]
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_nanos()).unwrap_or(u64::MAX)
        })
}

fn market() -> MarketRef {
    MarketRef::new(
        Venue::new("limitless").expect("venue"),
        NativeMarketKey::new(NativeIdentifierKind::slug(), SLUG).expect("key"),
    )
}

fn candidate(step: usize) -> Candidate {
    let grammar = DecimalGrammar::new(18, 30, true, false).expect("grammar");
    let levels = vec![
        Level::new(
            Side::Bid,
            Price::parse(&format!("0.{:03}", 100 + step % 300), grammar).expect("price"),
            Quantity::parse(&format!("{}", 1_000 + step % 997), grammar).expect("quantity"),
        ),
        Level::new(
            Side::Ask,
            Price::parse(&format!("0.{:03}", 600 + step % 300), grammar).expect("price"),
            Quantity::parse(&format!("{}", 2_000 + step % 991), grammar).expect("quantity"),
        ),
    ];
    Candidate::snapshot(
        Provenance::new(ProvenanceInput {
            market: market(),
            outcome: None,
            native_family: "bench".into(),
            source_timestamp: None,
            source_evidence: BoundedSourceEvidence::new(
                [],
                SourceEvidenceCapacity::new(0).expect("capacity"),
            )
            .expect("evidence"),
            daemon_generation: 1,
            connection: ConnectionIdentity::new("bench", 1).expect("connection"),
            subscription_generation: 1,
            receive_position: step as u64,
            commit_position: step as u64,
            local_receive_time: LocalMonotonicTimestamp::new(step as u64),
            local_commit_time: LocalMonotonicTimestamp::new(step as u64),
            replica: ReplicaRole::PublishingPrimary,
            representation: Representation::VenueNative,
            origin: Origin::SourceReported,
            local_revision: 0,
            continuity_epoch: 0,
        })
        .expect("provenance"),
        BoundedLevels::new(levels, LevelCapacity::new(8).expect("capacity")).expect("levels"),
    )
    .expect("candidate")
}

//! Cross-process contracts for the resolution delivery lane: a venue-reported
//! `marketResolved` must reach every binding surface, and `event_object`
//! (`src/ffi/napi.rs`) must materialize an event for every delivery kind, not only a
//! mutation, without wedging the stream.
//!
//! Every child here is spawned, and attaches, before the events under test are written:
//! `attach` starts a stream at the segment's current tip and never replays history published
//! before it, so a consumer spawned after the fact would see only the final state and no
//! events at all.
//!
//! [`node_binding_reports_mutations_then_a_resolution_then_mutations_in_order`] is placed
//! first: it is the first Node consumer this crate ever spawns as a cross-process contract,
//! and it pins the standing contract that a resolution delivery must not wedge the child's
//! next `nextEvent` call.

mod support;

use pm_ws::limitless::supervisor::{BookSegment, Supervisor, SupervisorConfig, VENUE};
use pm_ws::*;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use support::controlled_peer::{ControlledPeer, PeerConfig};
use support::observed_frames::{
    OBSERVED_MARKET_RESOLVED_OWN_ROOM, OBSERVED_ORDERBOOK_UPDATE_LAST_BEFORE_RESOLUTION,
    OBSERVED_RESOLUTION_MARKET_SLUG,
};
use tokio::time::Instant as TokioInstant;

const ATTACH_TIMEOUT: Duration = Duration::from_secs(5);

const BINDING_SLUG: &str = "btc-up-or-down-5-min-1788299400";

fn binding_market() -> MarketRef {
    MarketRef::new(
        Venue::new("limitless").unwrap(),
        NativeMarketKey::new(NativeIdentifierKind::slug(), BINDING_SLUG).unwrap(),
    )
}

fn binding_grammar() -> DecimalGrammar {
    DecimalGrammar::new(18, 30, true, false).unwrap()
}

fn binding_provenance(step: usize) -> Provenance {
    Provenance::new(ProvenanceInput {
        market: binding_market(),
        outcome: None,
        native_family: "orderbookUpdate".into(),
        source_timestamp: None,
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
        representation: Representation::VenueNative,
        origin: Origin::SourceReported,
        local_revision: 0,
        continuity_epoch: 0,
    })
    .unwrap()
}

/// The book's base snapshot: a bid and an ask, established with zero derived mutations.
fn binding_base_snapshot() -> Candidate {
    let levels = vec![
        Level::new(
            Side::Bid,
            Price::parse("0.300", binding_grammar()).unwrap(),
            Quantity::parse("1000000", binding_grammar()).unwrap(),
        ),
        Level::new(
            Side::Ask,
            Price::parse("0.700", binding_grammar()).unwrap(),
            Quantity::parse("2000000", binding_grammar()).unwrap(),
        ),
    ];
    Candidate::snapshot(
        binding_provenance(0),
        BoundedLevels::new(levels, LevelCapacity::new(8).unwrap()).unwrap(),
    )
    .unwrap()
}

/// A source delta touching only the ask quantity, so every call derives exactly one
/// mutation.
fn binding_ask_delta(step: usize, quantity: usize) -> Candidate {
    let levels = vec![Level::new(
        Side::Ask,
        Price::parse("0.700", binding_grammar()).unwrap(),
        Quantity::parse(&quantity.to_string(), binding_grammar()).unwrap(),
    )];
    Candidate::source_delta(
        binding_provenance(step),
        BoundedLevels::new(levels, LevelCapacity::new(8).unwrap()).unwrap(),
    )
    .unwrap()
}

/// Builds one venue-reported resolution, mirroring `Supervisor::build_resolution`'s shape
/// closely enough to publish through the same `SegmentWriter::publish_resolution` a
/// supervisor uses, without needing a live connection to produce one.
fn market_resolution(
    market: MarketRef,
    revision: u64,
    epoch: u64,
    outcome: &str,
    market_type: &str,
    winning_index: u32,
    resolution_date: &str,
) -> MarketResolution {
    let provenance = Provenance::new(ProvenanceInput {
        market,
        outcome: None,
        native_family: "marketResolved".into(),
        source_timestamp: Some(SourceTimestamp::new(resolution_date).unwrap()),
        source_evidence: BoundedSourceEvidence::new([], SourceEvidenceCapacity::new(0).unwrap())
            .unwrap(),
        daemon_generation: 1,
        connection: ConnectionIdentity::new("ws.limitless.exchange", 1).unwrap(),
        subscription_generation: 1,
        receive_position: 0,
        commit_position: 0,
        local_receive_time: LocalMonotonicTimestamp::new(0),
        local_commit_time: LocalMonotonicTimestamp::new(0),
        replica: ReplicaRole::PublishingPrimary,
        representation: Representation::VenueNative,
        origin: Origin::SourceReported,
        local_revision: revision,
        continuity_epoch: epoch,
    })
    .unwrap();
    let observation = ResolutionObservation::new(
        provenance,
        NativeOutcome::venue_defined(outcome).unwrap(),
        NativeLabel::new(market_type).unwrap(),
        DeliveryPath::MarketFeed,
    )
    .unwrap();
    MarketResolution::new(
        observation,
        winning_index,
        SourceTimestamp::new(resolution_date).unwrap(),
    )
}

fn segment_path(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "pm-ws-resolution-binding-{name}-{}-{:?}.seg",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    path
}

/// `target/<profile>/<cdylib name>`, derived from this test binary's own location, so the
/// child spawned below always loads the cdylib this very test run just built from the
/// current source tree.
fn cdylib_path() -> PathBuf {
    let mut path = std::env::current_exe().expect("test binary path");
    let _ = path.pop();
    if path.ends_with("deps") {
        let _ = path.pop();
    }
    let name = if cfg!(target_os = "macos") {
        "libpm_ws.dylib"
    } else if cfg!(windows) {
        "pm_ws.dll"
    } else {
        "libpm_ws.so"
    };
    path.push(name);
    path
}

fn build_cdylib() -> PathBuf {
    let build = Command::new("cargo")
        .args(["build", "--lib", "--locked"])
        .status()
        .expect("run cargo build --lib");
    assert!(build.success(), "cargo build --lib failed");
    let lib_path = cdylib_path();
    assert!(
        lib_path.is_file(),
        "cdylib not found at {lib_path:?} after cargo build --lib"
    );
    lib_path
}

fn expected_python_mutation_line(revision: u64, record: &MutationRecord) -> String {
    let mutation = record.mutation();
    let coordinate = mutation.replacement().or_else(|| mutation.old()).unwrap();
    let half = |level: Option<&Level>| {
        level.map_or_else(
            || "none".to_owned(),
            |level| format!("{}", level.quantity().value()),
        )
    };
    format!(
        "mutation revision={revision} cursor={}:{} origin=sourceReported side={:?} price={} qty={}->{}",
        record.cursor().epoch(),
        record.cursor().position(),
        coordinate.side(),
        coordinate.price().value(),
        half(mutation.old()),
        half(mutation.replacement()),
    )
}

fn expected_node_mutation_line(revision: u64, record: &MutationRecord) -> String {
    let mutation = record.mutation();
    let coordinate = mutation.replacement().or_else(|| mutation.old()).unwrap();
    let side = match coordinate.side() {
        Side::Bid => "bid",
        Side::Ask => "ask",
    };
    let half = |level: Option<&Level>| {
        level.map_or_else(
            || "none".to_owned(),
            |level| format!("{}", level.quantity().value()),
        )
    };
    format!(
        "mutation revision={revision} cursor={}:{} origin=sourceReported side={side} price={} qty={}->{}",
        record.cursor().epoch(),
        record.cursor().position(),
        coordinate.price().value(),
        half(mutation.old()),
        half(mutation.replacement()),
    )
}

/// The `resolution ...` line `examples/bbo.py` and `examples/bbo.ts` both print — the two
/// renderers were written to the same shape, so one expectation covers both languages.
#[allow(clippy::too_many_arguments)]
fn expected_resolution_line(
    revision: u64,
    epoch: u64,
    position: u64,
    origin: &str,
    outcome: &str,
    index: u32,
    market_type: &str,
    date: &str,
    path: &str,
) -> String {
    format!(
        "resolution revision={revision} cursor={epoch}:{position} origin={origin} outcome={outcome} index={index} type={market_type} date={date} path={path}"
    )
}

/// One spawned `bbo.py`/`bbo.ts` child, with a background thread draining its stdout into a
/// shared, lock-protected line buffer so the caller can observe lines as they arrive without
/// risking a full pipe buffer stalling the child.
struct SpawnedChild {
    child: Child,
    lines: Arc<Mutex<Vec<String>>>,
    collector: std::thread::JoinHandle<()>,
}

fn spawn_binding_child(
    mut command: Command,
    segment: &Path,
    market: &str,
    lib_path: &Path,
) -> SpawnedChild {
    let mut child = command
        .arg("--segment")
        .arg(segment)
        .args(["--market", market])
        .args(["--seconds", "5"])
        .arg("--events")
        .env("PMWS_LIB", lib_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| panic!("spawn failed: {error}"));
    let lines = Arc::new(Mutex::new(Vec::new()));
    let collector = {
        let lines = Arc::clone(&lines);
        let stdout = child.stdout.take().expect("child stdout");
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                lines.lock().expect("stdout lines").push(line);
            }
        })
    };
    SpawnedChild {
        child,
        lines,
        collector,
    }
}

/// Waits, without blocking the async runtime, until `spawned` has printed at least one line
/// — proof its `attach` completed and its retained-event cursor is anchored at the
/// segment's current tip.
async fn await_attached(name: &str, spawned: &SpawnedChild) {
    let deadline = Instant::now() + ATTACH_TIMEOUT;
    while spawned.lines.lock().expect("stdout lines").is_empty() {
        assert!(
            Instant::now() < deadline,
            "{name} never attached to the segment"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// Waits until `spawned` has printed a line `matches` accepts, and returns every line it had
/// printed at that moment.
///
/// Polled rather than awaited on the child's exit, because the line under test is printed
/// while the child is still running and the assertion it feeds is about ordering against
/// lines printed earlier.
async fn await_line(
    name: &str,
    spawned: &SpawnedChild,
    what: &str,
    matches: impl Fn(&str) -> bool,
) -> Vec<String> {
    let deadline = Instant::now() + ATTACH_TIMEOUT;
    loop {
        let lines = spawned.lines.lock().expect("stdout lines").clone();
        if lines.iter().any(|line| matches(line.as_str())) {
            return lines;
        }
        assert!(
            Instant::now() < deadline,
            "{name} never printed {what}: {lines:?}"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// Asserts `lines` carries `wanted` only after the last line starting with `after`.
fn assert_line_follows(name: &str, lines: &[String], after: &str, wanted: impl Fn(&str) -> bool) {
    let last_before = lines
        .iter()
        .rposition(|line| line.starts_with(after))
        .unwrap_or_else(|| {
            panic!("{name} never printed a line starting with {after:?}: {lines:?}")
        });
    let found = lines
        .iter()
        .position(|line| wanted(line.as_str()))
        .unwrap_or_else(|| panic!("{name} never printed the line under test: {lines:?}"));
    assert!(
        found > last_before,
        "{name} printed the line under test before the last {after:?} line: {lines:?}"
    );
}

/// Waits for `spawned` to exit and returns its exit status and every stdout line it printed.
fn finish_binding_child(name: &str, spawned: SpawnedChild) -> (ExitStatus, Vec<String>) {
    let SpawnedChild {
        mut child,
        lines,
        collector,
    } = spawned;
    let status = child
        .wait()
        .unwrap_or_else(|error| panic!("{name} exits: {error}"));
    collector.join().expect("stdout collector");
    let mut stderr = String::new();
    if let Some(mut handle) = child.stderr.take() {
        let _ = handle.read_to_string(&mut stderr);
    }
    let lines = lines.lock().expect("stdout lines").clone();
    assert!(status.success(), "{name} failed: {stderr}\n{lines:?}");
    (status, lines)
}

fn assert_ordered(name: &str, lines: &[String], before: &str, resolution: &str, after: &str) {
    let before_index = lines
        .iter()
        .position(|line| line == before)
        .unwrap_or_else(|| {
            panic!("{name} never printed the mutation before the resolution: {lines:?}")
        });
    let resolution_index = lines
        .iter()
        .position(|line| line == resolution)
        .unwrap_or_else(|| panic!("{name} never printed the resolution: {lines:?}"));
    let after_index = lines
        .iter()
        .position(|line| line == after)
        .unwrap_or_else(|| {
            panic!("{name} never printed the mutation after the resolution: {lines:?}")
        });
    assert!(
        before_index < resolution_index && resolution_index < after_index,
        "{name} printed the three deliveries out of order: {lines:?}"
    );
}

struct BindingRun {
    lines: Vec<String>,
    mutation_before_python: String,
    mutation_before_node: String,
    resolution_line: String,
    mutation_after_python: String,
    mutation_after_node: String,
}

/// Spawns `command` against a fresh segment, waits for it to attach at the base revision,
/// then writes one mutation, one venue-reported resolution, and one more mutation into the
/// ring while it runs — the shape every consumer under test here reads.
///
/// `OrderBook::note_stream_event` is the same call `BookWriter::publish_resolution` makes in
/// the real supervisor path: it draws the next ring position without deriving a mutation, so
/// the resolution and the mutations around it share one contiguous cursor stream, exactly as
/// a real supervisor run produces.
async fn run_binding(name: &str, command: Command) -> BindingRun {
    let path = segment_path(name);
    let layout = SegmentLayout::new(1, 1, 8, 16, 16).unwrap();
    let region = Arc::new(SegmentRegion::create_file(&path, layout.region_size()).unwrap());
    let mut writer = SegmentWriter::create(
        Arc::clone(&region),
        SegmentConfig::new(layout, 0x2026_0901_0000_0007_0000_0000_0000_0001, 1),
    )
    .unwrap();
    let handle = writer.install(&binding_market()).unwrap();
    let mut book = OrderBook::new(binding_market());

    let base = book.apply_snapshot(&binding_base_snapshot()).unwrap();
    assert!(base.mutations().is_empty());
    writer.publish(handle, &book.publish(), 0).unwrap();

    let lib_path = build_cdylib();
    let spawned = spawn_binding_child(command, &path, BINDING_SLUG, &lib_path);
    await_attached(name, &spawned).await;

    let before = book
        .apply_source_delta(&binding_ask_delta(1, 2_000_001))
        .unwrap();
    assert_eq!(before.mutations().len(), 1);
    for record in before.mutations() {
        writer
            .publish_mutation(
                handle,
                before.revision(),
                record.cursor(),
                record.mutation(),
                0,
            )
            .unwrap();
    }
    writer.publish(handle, &book.publish(), 0).unwrap();
    let mutation_before_python =
        expected_python_mutation_line(before.revision(), &before.mutations()[0]);
    let mutation_before_node =
        expected_node_mutation_line(before.revision(), &before.mutations()[0]);
    tokio::time::sleep(Duration::from_millis(20)).await;

    let resolution_cursor = book.note_stream_event().unwrap();
    let resolution_revision = book.revision();
    let resolution = market_resolution(
        binding_market(),
        resolution_revision,
        resolution_cursor.epoch(),
        "YES",
        "CLOB-test-market",
        7,
        "2026-01-01T00:00:00.000Z",
    );
    writer
        .publish_resolution(
            handle,
            resolution_revision,
            &resolution_cursor,
            &resolution,
            0,
        )
        .unwrap();
    writer.publish(handle, &book.publish(), 0).unwrap();
    let resolution_line = expected_resolution_line(
        resolution_revision,
        resolution_cursor.epoch(),
        resolution_cursor.position(),
        "sourceReported",
        "YES",
        7,
        "CLOB-test-market",
        "2026-01-01T00:00:00.000Z",
        "marketFeed",
    );
    tokio::time::sleep(Duration::from_millis(20)).await;

    let after = book
        .apply_source_delta(&binding_ask_delta(2, 2_000_002))
        .unwrap();
    assert_eq!(after.mutations().len(), 1);
    for record in after.mutations() {
        writer
            .publish_mutation(
                handle,
                after.revision(),
                record.cursor(),
                record.mutation(),
                0,
            )
            .unwrap();
    }
    writer.publish(handle, &book.publish(), 0).unwrap();
    let mutation_after_python =
        expected_python_mutation_line(after.revision(), &after.mutations()[0]);
    let mutation_after_node = expected_node_mutation_line(after.revision(), &after.mutations()[0]);

    let (_, lines) = finish_binding_child(name, spawned);
    let _ = std::fs::remove_file(&path);

    BindingRun {
        lines,
        mutation_before_python,
        mutation_before_node,
        resolution_line,
        mutation_after_python,
        mutation_after_node,
    }
}

/// The node binding refuses to run against a library it cannot load, instead of proceeding.
///
/// `libraryPath` searches `target/release` before `target/debug` and neither build carries a
/// stamp in its filename, so an artifact left behind by an older build is loaded by name
/// alone, which would silently serve wrong behaviour. The gate's happy path is
/// exercised by every node child in this file, each pinned through `PMWS_LIB` to the cdylib
/// this run just built; this pins the other side, that a `PMWS_LIB` naming no usable library
/// ends the process loudly and names the path it tried. A genuinely stale-but-loadable
/// artifact is not cheap to manufacture here, so the version comparison itself is covered by
/// the missing-export and mismatch branches being the only ways past `loadNative`.
#[test]
fn the_node_binding_refuses_a_library_it_cannot_load() {
    const MISSING: &str = "/nonexistent/pm-ws-stale-artifact-for-tests";
    let output = Command::new("node")
        .args([
            "--input-type=module",
            "-e",
            "await import('./bindings/node/pmws.ts')",
        ])
        .env("PMWS_LIB", MISSING)
        .output()
        .expect("run node");
    assert!(
        !output.status.success(),
        "the binding imported cleanly against a library that does not exist"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(MISSING),
        "the failure never named the library it tried to load: {stderr}"
    );
}

/// The first Node cross-process contract this crate spawns. It pins that `event_object`
/// materializes a kind-3 (resolution) event and that the mutation delivered right after the
/// resolution stays readable on the next `nextEvent` call.
#[tokio::test]
async fn node_binding_reports_mutations_then_a_resolution_then_mutations_in_order() {
    let mut command = Command::new("node");
    command.arg("examples/bbo.ts");
    let run = run_binding("node", command).await;
    assert!(
        run.lines
            .first()
            .is_some_and(|line| line.starts_with("bbo revision=")),
        "node printed no initial bbo line: {:?}",
        run.lines
    );
    assert_ordered(
        "node",
        &run.lines,
        &run.mutation_before_node,
        &run.resolution_line,
        &run.mutation_after_node,
    );
}

#[tokio::test]
async fn python_binding_reports_mutations_then_a_resolution_then_mutations_in_order() {
    let mut command = Command::new("python3");
    command.arg("examples/bbo.py");
    let run = run_binding("python", command).await;
    assert!(
        run.lines
            .first()
            .is_some_and(|line| line.starts_with("bbo revision=")),
        "python printed no initial bbo line: {:?}",
        run.lines
    );
    assert_ordered(
        "python",
        &run.lines,
        &run.mutation_before_python,
        &run.resolution_line,
        &run.mutation_after_python,
    );
}

const STEP_TIMEOUT: Duration = Duration::from_secs(10);
const RUN_CAP: Duration = Duration::from_secs(120);

fn scenario_market() -> MarketRef {
    MarketRef::new(
        Venue::new(VENUE).unwrap(),
        NativeMarketKey::new(
            NativeIdentifierKind::slug(),
            OBSERVED_RESOLUTION_MARKET_SLUG,
        )
        .unwrap(),
    )
}

fn scenario_config(endpoint: String) -> SupervisorConfig {
    SupervisorConfig {
        endpoint,
        market: OBSERVED_RESOLUTION_MARKET_SLUG.to_owned(),
        setup_timeout: Duration::from_secs(10),
        replicas: 1,
        observer_capacity: 64,
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(40),
        exhausted_backoff: Duration::from_millis(40),
        max_recovery_attempts: 2,
        fenced_linger: Duration::from_secs(10),
        resubscribe_window: Duration::from_secs(30),
        ..SupervisorConfig::default()
    }
}

fn patient_peer() -> PeerConfig {
    PeerConfig {
        ping_interval_ms: 60_000,
        ping_timeout_ms: 60_000,
        ..PeerConfig::default()
    }
}

struct ScenarioSegment {
    region: Arc<SegmentRegion>,
    book: BookSegment,
    path: PathBuf,
}

fn open_scenario_segment(name: &str) -> ScenarioSegment {
    let path = segment_path(name);
    let layout = SegmentLayout::new(1, 1, 16, 64, 16).expect("a valid segment layout");
    let region = Arc::new(
        SegmentRegion::create_file(&path, layout.region_size()).expect("create the segment file"),
    );
    let mut writer = SegmentWriter::create(
        Arc::clone(&region),
        SegmentConfig::new(layout, 0x2026_0901_0000_0008_0000_0000_0000_0001, 1),
    )
    .expect("format the segment");
    let handle = writer
        .install(&scenario_market())
        .expect("install the market");
    ScenarioSegment {
        region,
        book: BookSegment::new(writer, handle),
        path,
    }
}

async fn await_state(
    reader: &SegmentReader,
    handle: MarketHandle,
    what: &str,
    predicate: impl Fn(&BookSnapshot) -> bool,
) -> BookSnapshot {
    let deadline = TokioInstant::now() + STEP_TIMEOUT;
    let mut last = None;
    loop {
        if let Ok(snapshot) = reader.read(handle) {
            if predicate(&snapshot) {
                return snapshot;
            }
            last = Some(snapshot);
        }
        assert!(
            TokioInstant::now() < deadline,
            "timed out waiting for {what}; last read {last:?}"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Waits until the in-process latest-state surface carries `revision`, failing on any
/// delivery or loss this replay must not produce.
///
/// That surface coalesces, so a single wake can carry several published revisions at once and
/// the first one to reach `revision` is the answer. The book update replayed after the
/// resolutions derives no mutation — it restates the same levels — so latest state is the
/// only in-process surface that can report it, and reporting it is what proves a resolution
/// froze nothing.
async fn await_published(observer: &mut BookObserver, revision: u64) -> Arc<PublishedBook> {
    let deadline = TokioInstant::now() + STEP_TIMEOUT;
    loop {
        match tokio::time::timeout_at(deadline, observer.next_event()).await {
            Ok(Ok(ObserverEvent::Published(published))) if published.revision() >= revision => {
                return published;
            }
            Ok(Ok(ObserverEvent::Published(_))) => {}
            Ok(Ok(event)) => {
                panic!("expected only latest-state wakes past the resolutions, got {event:?}")
            }
            Ok(Err(error)) => panic!("the in-process observer lost continuity: {error:?}"),
            Err(_) => panic!("timed out waiting for revision {revision} on the state surface"),
        }
    }
}

/// Drains `expected` deliveries from the in-process observer, in order, failing on any loss
/// this replay must not produce.
async fn drain_observer(observer: &mut BookObserver, expected: usize) -> Vec<StreamDelivery> {
    let deadline = TokioInstant::now() + STEP_TIMEOUT;
    let mut deliveries = Vec::new();
    while deliveries.len() < expected {
        match tokio::time::timeout_at(deadline, observer.recv()).await {
            Ok(Ok(delivery)) => deliveries.push(delivery),
            Ok(Err(error)) => panic!(
                "the in-process observer lost continuity after {} of {expected} deliveries: {error:?}",
                deliveries.len()
            ),
            Err(_) => panic!(
                "timed out after {} of {expected} deliveries",
                deliveries.len()
            ),
        }
    }
    deliveries
}

/// An end-to-end proof test: a real `Supervisor` behind a `BookSegment`, driven by the
/// controlled peer replaying the OBSERVED `wire_observed.rs` frames verbatim — the venue's
/// own bytes, not a hand-written approximation of them.
///
/// Sequence: the observed btc `orderbookUpdate` (establishes the book, deriving no
/// mutation — the first accepted snapshot never does), the observed `marketResolved` sent
/// three times exactly as the venue delivered it, then the observed `orderbookUpdate` again.
/// The second `orderbookUpdate` is byte-identical to the first, so it derives no mutation
/// either — `OrderBook::apply_snapshot`'s own documented checkpoint-refresh case — but the
/// revision still advances and authority stays Live, which is the post-resolution acceptance
/// this test exists to prove: a resolution never freezes the book.
///
/// The python3 and node consumers are spawned as soon as the base snapshot is live, before
/// any resolution is sent, so their attach anchors at the empty ring and they observe every
/// later delivery — exactly the ordering [`run_binding`] establishes for the binding tests
/// above.
///
/// Every surface must show that post-resolution acceptance, not just the Rust-side segment
/// read: the in-process observer reports revision 2 on its latest-state surface, and both
/// children print a `bbo revision=2` line after their resolution lines. The children see it
/// because the shared-memory state slot is left alone by a resolution and advances at the next
/// commit — which is exactly this replayed update.
#[tokio::test]
async fn s7_resolution_forwarding_reaches_the_observer_and_every_binding_byte_exact() {
    let lib_path = build_cdylib();
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let segment = open_scenario_segment("s7-scenario");
    let reader = SegmentReader::attach(Arc::clone(&segment.region)).expect("attach a reader");
    let path = segment.path.clone();

    let mut supervisor =
        Supervisor::new(scenario_config(peer.endpoint())).expect("a valid configuration");
    supervisor
        .publish_into(segment.book)
        .expect("the initial revision publishes into a segment sized for it");
    let mut observer = supervisor.attach();
    let stop = supervisor.stopper();
    let run = tokio::spawn(async move {
        let stats = supervisor.run_until(TokioInstant::now() + RUN_CAP).await;
        assert_eq!(
            supervisor.segment_failure(),
            None,
            "a segment sized for this book refuses nothing it is given"
        );
        stats
    });

    let handle = reader
        .resolve(&scenario_market())
        .expect("the market is installed");

    let mut connection = peer.next_connection().await;
    let request = connection.complete_handshake().await;
    assert_eq!(
        request.slugs,
        vec![OBSERVED_RESOLUTION_MARKET_SLUG.to_owned()]
    );

    connection
        .send_raw(OBSERVED_ORDERBOOK_UPDATE_LAST_BEFORE_RESOLUTION)
        .await;
    let base = await_state(
        &reader,
        handle,
        "the observed base snapshot to commit",
        |snapshot| snapshot.revision() >= 1,
    )
    .await;
    assert_eq!(base.revision(), 1);
    assert_eq!(base.authority(), &AuthorityState::Live);

    let mut python_command = Command::new("python3");
    python_command.arg("examples/bbo.py");
    let python_spawned = spawn_binding_child(
        python_command,
        &path,
        OBSERVED_RESOLUTION_MARKET_SLUG,
        &lib_path,
    );
    let mut node_command = Command::new("node");
    node_command.arg("examples/bbo.ts");
    let node_spawned = spawn_binding_child(
        node_command,
        &path,
        OBSERVED_RESOLUTION_MARKET_SLUG,
        &lib_path,
    );
    await_attached("python3", &python_spawned).await;
    await_attached("node", &node_spawned).await;

    for _ in 0..3 {
        connection.send_raw(OBSERVED_MARKET_RESOLVED_OWN_ROOM).await;
    }

    let deliveries = drain_observer(&mut observer, 3).await;
    let mutations: Vec<&StreamDelivery> = deliveries
        .iter()
        .filter(|delivery| matches!(delivery, StreamDelivery::Mutation(_)))
        .collect();
    assert!(
        mutations.is_empty(),
        "the observed base snapshot establishes the book and derives no mutation, so nothing \
         precedes the three resolutions: {deliveries:?}"
    );
    for (position, delivery) in deliveries.iter().enumerate() {
        let StreamDelivery::Resolution(resolution) = delivery else {
            panic!("expected a resolution at position {position}, got {delivery:?}");
        };
        assert_eq!(
            resolution.cursor(),
            &MutationCursor::new(0, position as u64),
            "the three resolutions occupy contiguous positions"
        );
        assert_eq!(resolution.revision(), 1);
        assert_eq!(resolution.resolution().winner().text_value(), Some("NO"));
        assert_eq!(resolution.resolution().winning_index(), 1);
        assert_eq!(resolution.resolution().native_label().as_str(), "CLOB");
        assert_eq!(
            resolution.resolution().resolution_date().as_lexeme(),
            "2026-09-01T13:11:02.813Z"
        );
        assert_eq!(
            resolution.resolution().delivery_path(),
            &DeliveryPath::MarketFeed
        );
    }

    let third_resolution =
        |line: &str| line.starts_with("resolution ") && line.contains("cursor=0:2");
    let _drained = await_line(
        "python3",
        &python_spawned,
        "its third resolution line",
        third_resolution,
    )
    .await;
    let _drained = await_line(
        "node",
        &node_spawned,
        "its third resolution line",
        third_resolution,
    )
    .await;

    connection
        .send_raw(OBSERVED_ORDERBOOK_UPDATE_LAST_BEFORE_RESOLUTION)
        .await;
    let accepted = await_state(
        &reader,
        handle,
        "the post-resolution orderbookUpdate to be accepted",
        |snapshot| snapshot.revision() >= 2,
    )
    .await;
    assert_eq!(
        accepted.revision(),
        2,
        "a resolution never freezes the book: the venue's next update is accepted normally"
    );
    assert_eq!(accepted.authority(), &AuthorityState::Live);

    let published = await_published(&mut observer, 2).await;
    assert_eq!(
        published.revision(),
        2,
        "the in-process lane reports the post-resolution revision on its latest-state surface"
    );
    assert_eq!(published.authority(), &AuthorityState::Live);

    let post_resolution = |line: &str| line.starts_with("bbo revision=2 ");
    let python_live = await_line(
        "python3",
        &python_spawned,
        "a post-resolution bbo line",
        post_resolution,
    )
    .await;
    assert_line_follows("python3", &python_live, "resolution ", post_resolution);
    let node_live = await_line(
        "node",
        &node_spawned,
        "a post-resolution bbo line",
        post_resolution,
    )
    .await;
    assert_line_follows("node", &node_live, "resolution ", post_resolution);

    stop.stop();
    let stats = tokio::time::timeout(STEP_TIMEOUT, run)
        .await
        .expect("the supervisor run ends after stop")
        .expect("the supervisor task completes");
    assert_eq!(stats.events_resolved, 3);
    assert_eq!(stats.snapshots_applied, 2);

    let (_, python_lines) = finish_binding_child("python3", python_spawned);
    let (_, node_lines) = finish_binding_child("node", node_spawned);
    let _ = std::fs::remove_file(&path);

    let expected_resolution_lines: Vec<String> = (0..3)
        .map(|position| {
            expected_resolution_line(
                1,
                0,
                position,
                "sourceReported",
                "NO",
                1,
                "CLOB",
                "2026-09-01T13:11:02.813Z",
                "marketFeed",
            )
        })
        .collect();

    let python_resolutions: Vec<&String> = python_lines
        .iter()
        .filter(|line| line.starts_with("resolution "))
        .collect();
    assert_eq!(
        python_resolutions.len(),
        3,
        "python printed {} resolution lines, not 3: {python_lines:?}",
        python_resolutions.len()
    );
    for (line, want) in python_resolutions
        .iter()
        .zip(expected_resolution_lines.iter())
    {
        assert_eq!(**line, *want, "python printed a mismatched resolution line");
    }

    let node_resolutions: Vec<&String> = node_lines
        .iter()
        .filter(|line| line.starts_with("resolution "))
        .collect();
    assert_eq!(
        node_resolutions.len(),
        3,
        "node printed {} resolution lines, not 3: {node_lines:?}",
        node_resolutions.len()
    );
    for (line, want) in node_resolutions
        .iter()
        .zip(expected_resolution_lines.iter())
    {
        assert_eq!(**line, *want, "node printed a mismatched resolution line");
    }
}

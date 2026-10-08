//! A second process reads the published segment: the compiled Rust example and the
//! stdlib-only Python reader both track the writer's revisions over a mapped file.

use pm_ws::*;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const SLUG: &str = "btc-up-or-down-5-min-1788188100";
const REVISIONS: usize = 20;
const CHILD_SECONDS: &str = "2";

fn market() -> MarketRef {
    MarketRef::new(
        Venue::new("limitless").unwrap(),
        NativeMarketKey::new(NativeIdentifierKind::slug(), SLUG).unwrap(),
    )
}

fn grammar() -> DecimalGrammar {
    DecimalGrammar::new(18, 30, true, false).unwrap()
}

fn provenance(step: usize) -> Provenance {
    Provenance::new(ProvenanceInput {
        market: market(),
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

/// Revision `step + 1` of one book, with a bid and an ask nobody else has, so a printed
/// BBO line identifies exactly which revision produced it.
fn candidate(step: usize) -> Candidate {
    let levels = vec![
        Level::new(
            Side::Bid,
            Price::parse(&format!("0.{:03}", 300 + step), grammar()).unwrap(),
            Quantity::parse(&format!("{}", 1_000_000 + step), grammar()).unwrap(),
        ),
        Level::new(
            Side::Ask,
            Price::parse(&format!("0.{:03}", 700 - step), grammar()).unwrap(),
            Quantity::parse(&format!("{}", 2_000_000 + step), grammar()).unwrap(),
        ),
    ];
    Candidate::snapshot(
        provenance(step),
        BoundedLevels::new(levels, LevelCapacity::new(8).unwrap()).unwrap(),
    )
    .unwrap()
}

fn expected_bbo(book: &PublishedBook) -> String {
    let level = |side: Side| {
        let matching: Vec<&Level> = book
            .canonical_levels()
            .iter()
            .filter(|level| level.side() == side)
            .collect();
        let chosen = if side == Side::Bid {
            matching.last()
        } else {
            matching.first()
        };
        chosen.map_or_else(
            || "-".to_owned(),
            |level| format!("{}@{}", level.price().value(), level.quantity().value()),
        )
    };
    format!(
        "bbo revision={} authority={:?} best_bid={} best_ask={}",
        book.revision(),
        book.authority(),
        level(Side::Bid),
        level(Side::Ask)
    )
}

/// `target/<profile>/examples/<name>`, derived from this test binary's own location.
fn example_binary(name: &str) -> PathBuf {
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

fn segment_path(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "pm-ws-{name}-{}-{:?}.seg",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    path
}

/// Runs `command` as a reader of a fresh segment while publishing [`REVISIONS`] revisions
/// into it, and returns the child's stdout lines together with the books published.
fn drive(name: &str, mut command: Command) -> (Vec<String>, Vec<PublishedBook>) {
    let path = segment_path(name);
    let layout = SegmentLayout::new(2, 2, 8, 8, 16).unwrap();
    let region = Arc::new(SegmentRegion::create_file(&path, layout.region_size()).unwrap());
    let mut writer = SegmentWriter::create(
        Arc::clone(&region),
        SegmentConfig::new(layout, 0x2026_0901_0000_0002_0000_0000_0000_0001, 1),
    )
    .unwrap();
    let handle = writer.install(&market()).unwrap();
    let mut book = OrderBook::new(market());
    let mut published = Vec::new();

    book.apply_snapshot(&candidate(0)).unwrap();
    published.push(book.publish());
    writer.publish(handle, &published[0], 0).unwrap();

    let mut child = command
        .arg("--segment")
        .arg(&path)
        .arg("--market")
        .arg(SLUG)
        .arg("--seconds")
        .arg(CHILD_SECONDS)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| panic!("spawn {name}: {error}"));

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
    let spawned = Instant::now();
    while lines.lock().expect("stdout lines").is_empty() {
        assert!(
            spawned.elapsed() < Duration::from_secs(5),
            "{name} never attached to the segment"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    for step in 1..REVISIONS {
        book.apply_snapshot(&candidate(step)).unwrap();
        published.push(book.publish());
        writer.publish(handle, &published[step], 0).unwrap();
        std::thread::sleep(Duration::from_millis(10));
    }

    let status = child.wait().expect("child exits");
    collector.join().expect("stdout collector");
    let mut stderr = String::new();
    if let Some(mut handle) = child.stderr.take() {
        let _ = handle.read_to_string(&mut stderr);
    }
    let _ = std::fs::remove_file(&path);
    let lines = lines.lock().expect("stdout lines").clone();
    assert!(status.success(), "{name} failed: {stderr}\n{lines:?}");
    (lines, published)
}

/// Asserts the reader attached, tracked revisions forward without ever going backwards,
/// reached the final published revision, and printed the exact BBO of every revision it
/// reported. Latest state coalesces by contract, so a skipped intermediate revision is
/// correct; a wrong or stale BBO for a reported revision is not.
fn assert_tracks(name: &str, lines: &[String], published: &[PublishedBook]) {
    assert!(
        lines
            .first()
            .is_some_and(|line| line.starts_with("attached instance=0x")),
        "{name} printed no attach line: {lines:?}"
    );
    let bbo: Vec<&String> = lines
        .iter()
        .filter(|line| line.starts_with("bbo "))
        .collect();
    assert!(
        bbo.len() >= 3,
        "{name} printed only {} bbo lines: {lines:?}",
        bbo.len()
    );
    let expected: Vec<String> = published.iter().map(expected_bbo).collect();
    let mut last = 0_u64;
    for line in &bbo {
        let revision: u64 = line
            .split_whitespace()
            .find_map(|field| field.strip_prefix("revision="))
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(|| panic!("{name} printed an unparsable line: {line}"));
        assert!(revision > last, "{name} went backwards: {line}");
        last = revision;
        assert_eq!(
            **line,
            expected[revision as usize - 1],
            "{name} printed a BBO that is not revision {revision}"
        );
    }
    assert_eq!(
        last, REVISIONS as u64,
        "{name} never reached the last published revision"
    );
}

/// A run that never receives a book update still exposes a readable segment.
///
/// `--shm` publishes the book's current state once as the segment is installed, before any
/// commit, so a quiet market is a readable revision 0 rather than an indefinite "nothing
/// published". The endpoint is unreachable on purpose: this run receives nothing.
#[test]
fn shm_daemon_publishes_the_initial_revision_on_a_market_with_no_updates() {
    let path = segment_path("initial");
    let status = Command::new(env!("CARGO_BIN_EXE_pmws-run"))
        .args(["--market", SLUG])
        .args(["--endpoint", "ws://127.0.0.1:1/unreachable"])
        .args(["--seconds", "1"])
        .arg("--shm")
        .arg(&path)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("daemon runs");
    assert!(
        status.status.success(),
        "daemon failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );

    let region = Arc::new(SegmentRegion::open_file(&path).expect("map the daemon's segment"));
    let reader = SegmentReader::attach(region).expect("attach");
    let market = MarketRef::new(
        Venue::new(pm_ws::limitless::supervisor::VENUE).unwrap(),
        NativeMarketKey::new(NativeIdentifierKind::slug(), SLUG).unwrap(),
    );
    let handle = reader.resolve(&market).expect("market installed");
    let snapshot = reader.read(handle).expect("initial revision readable");
    assert_eq!(snapshot.revision(), 0);
    assert_eq!(snapshot.authority(), &AuthorityState::Synchronizing);
    assert_eq!(snapshot.publication(), None);
    assert!(snapshot.levels().is_empty());
    assert_eq!(
        reader.geometry().layout().level_capacity() as usize,
        pm_ws::limitless::supervisor::MAX_BOOK_LEVELS,
        "the segment must be sized from the supervisor's accepted depth"
    );
    let _ = std::fs::remove_file(&path);
}

#[test]
fn shm_rust_example_reader_tracks_published_revisions_across_processes() {
    let (lines, published) = drive("rust", Command::new(example_binary("reader")));
    assert_tracks("rust reader", &lines, &published);
}

#[test]
fn shm_python_reader_tracks_published_revisions_across_processes() {
    let mut command = Command::new("python3");
    let _ = command.arg("examples/reader.py");
    let (lines, published) = drive("python", command);
    assert_tracks("python reader", &lines, &published);
}

/// The line `examples/reader.py --events` prints for one delivered mutation.
///
/// Built here from the mutation the writer committed, so a byte of drift between the two
/// implementations of the event slot fails the comparison rather than printing plausible
/// nonsense.
fn expected_event(revision: u64, record: &MutationRecord) -> String {
    let mutation = record.mutation();
    let coordinate = mutation.replacement().or_else(|| mutation.old()).unwrap();
    let half = |level: Option<&Level>| {
        level.map_or_else(
            || "-".to_owned(),
            |level| format!("{}", level.quantity().value()),
        )
    };
    format!(
        "event epoch={} position={} revision={revision} origin={:?} side={:?} price={} old={} new={}",
        record.cursor().epoch(),
        record.cursor().position(),
        mutation.provenance().origin(),
        coordinate.side(),
        coordinate.price().value(),
        half(mutation.old()),
        half(mutation.replacement()),
    )
}

/// The independent Python reader follows the retained-event ring of a live segment.
///
/// It attaches to a coherent `(revision, cursor)` pair, then classifies every slot it finds
/// against that cursor itself — no Rust helper takes part, which is the whole point of the
/// artifact. Every event it prints must be the mutation the writer committed at that
/// position, in order, with no gap.
#[test]
fn shm_python_reader_follows_the_retained_event_ring_across_processes() {
    let path = segment_path("python-events");
    let layout = SegmentLayout::new(1, 1, 8, 256, 16).unwrap();
    let region = Arc::new(SegmentRegion::create_file(&path, layout.region_size()).unwrap());
    let mut writer = SegmentWriter::create(
        Arc::clone(&region),
        SegmentConfig::new(layout, 0x2026_0901_0000_0003_0000_0000_0000_0001, 1),
    )
    .unwrap();
    let handle = writer.install(&market()).unwrap();
    let mut book = OrderBook::new(market());
    let base = book.apply_snapshot(&candidate(0)).unwrap();
    assert!(base.mutations().is_empty());
    writer.publish(handle, &book.publish(), 0).unwrap();

    let mut child = Command::new("python3")
        .arg("examples/reader.py")
        .arg("--segment")
        .arg(&path)
        .args(["--market", SLUG])
        .args(["--seconds", CHILD_SECONDS])
        .arg("--events")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn python3");

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
    let spawned = Instant::now();
    while lines.lock().expect("stdout lines").len() < 2 {
        assert!(
            spawned.elapsed() < Duration::from_secs(5),
            "the python reader never attached a stream"
        );
        std::thread::sleep(Duration::from_millis(2));
    }

    let mut expected = Vec::new();
    for step in 1..REVISIONS {
        let commit = book.apply_snapshot(&candidate(step)).unwrap();
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
            expected.push(expected_event(commit.revision(), record));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(expected.len() > 40, "only {} mutations", expected.len());

    let status = child.wait().expect("child exits");
    collector.join().expect("stdout collector");
    let mut stderr = String::new();
    if let Some(mut handle) = child.stderr.take() {
        let _ = handle.read_to_string(&mut stderr);
    }
    let _ = std::fs::remove_file(&path);
    let lines = lines.lock().expect("stdout lines").clone();
    assert!(
        status.success(),
        "python reader failed: {stderr}\n{lines:?}"
    );

    assert!(
        lines
            .get(1)
            .is_some_and(|line| line == "attached_stream epoch=0 position=0"),
        "the python reader attached at the wrong cursor: {lines:?}"
    );
    let delivered: Vec<&String> = lines
        .iter()
        .filter(|line| line.starts_with("event epoch="))
        .collect();
    assert!(
        !lines.iter().any(|line| line.contains("continuity_lost")),
        "the python reader lost continuity on a ring it was never lapped in: {lines:?}"
    );
    assert!(
        delivered.len() >= 40,
        "the python reader delivered only {} events: {lines:?}",
        delivered.len()
    );
    for (line, want) in delivered.iter().zip(expected.iter()) {
        assert_eq!(
            **line, *want,
            "the python reader decoded a different mutation"
        );
    }
}

/// `target/<profile>/<cdylib name>`, derived from this test binary's own location, exactly
/// as [`example_binary`] derives an example's path -- so the child below always loads the
/// cdylib this very test run just built from the current source tree, never a stale one a
/// previous `cargo build --release` left behind.
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

/// `bindings/python/pmws.py`'s camelCase rendering of an [`Origin`] -- the one place this
/// ABI's text deliberately differs from every other in-process surface's `{:?}` rendering.
/// Written as an exhaustive match, so a future [`Origin`] variant fails to compile here
/// rather than silently rendering as the wrong word.
fn python_origin_text(origin: &Origin) -> String {
    match origin {
        Origin::SourceReported => "sourceReported".to_owned(),
        Origin::NormalizedFromSource => "normalizedFromSource".to_owned(),
        Origin::LocallyDerived(Derivation::SnapshotDiff) => {
            "locallyDerived(snapshotDiff)".to_owned()
        }
    }
}

/// The line `examples/bbo.py --events` prints for one delivered mutation.
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
        "mutation revision={revision} cursor={}:{} origin={} side={:?} price={} qty={}->{}",
        record.cursor().epoch(),
        record.cursor().position(),
        python_origin_text(mutation.provenance().origin()),
        coordinate.side(),
        coordinate.price().value(),
        half(mutation.old()),
        half(mutation.replacement()),
    )
}

/// One source-delta candidate touching the same ask coordinate every time, so every commit
/// here produces exactly one mutation and the book's best ask changes with it.
fn ask_quantity_delta(step: usize, quantity: usize) -> Candidate {
    let levels = vec![Level::new(
        Side::Ask,
        Price::parse("0.700", grammar()).unwrap(),
        Quantity::parse(&quantity.to_string(), grammar()).unwrap(),
    )];
    Candidate::source_delta(
        provenance(step),
        BoundedLevels::new(levels, LevelCapacity::new(8).unwrap()).unwrap(),
    )
    .unwrap()
}

/// The Python binding reads both latest state and the retained-event ring through the C-ABI
/// helper, and recovers correctly from a scripted overrun.
///
/// A four-slot event ring: large enough that a handful of gently-paced controlled mutations
/// never wrap it, small enough that a burst fired with no pacing at all reliably does. The
/// child's `PMWS_LIB` is pinned to the cdylib this test run just built, so a pass here proves
/// the binding against the source tree under test, not whatever a stale release build left in
/// `target/release`.
#[test]
fn shm_python_binding_reads_books_and_mutations_through_the_helper() {
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

    let path = segment_path("python-binding");
    let layout = SegmentLayout::new(1, 1, 8, 4, 16).unwrap();
    let region = Arc::new(SegmentRegion::create_file(&path, layout.region_size()).unwrap());
    let mut writer = SegmentWriter::create(
        Arc::clone(&region),
        SegmentConfig::new(layout, 0x2026_0901_0000_0004_0000_0000_0000_0001, 1),
    )
    .unwrap();
    let handle = writer.install(&market()).unwrap();
    let mut book = OrderBook::new(market());

    let mut step = 0_usize;
    let mut next_step = || {
        let value = step;
        step += 1;
        value
    };

    let established = book.apply_snapshot(&candidate(next_step())).unwrap();
    assert!(established.mutations().is_empty());
    let mut published = vec![book.publish()];
    writer.publish(handle, &published[0], 0).unwrap();

    let mut child = Command::new("python3")
        .arg("examples/bbo.py")
        .arg("--segment")
        .arg(&path)
        .args(["--market", SLUG])
        .args(["--seconds", "2"])
        .arg("--events")
        .env("PMWS_LIB", &lib_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn python3");

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
    let spawned = Instant::now();
    while lines.lock().expect("stdout lines").is_empty() {
        assert!(
            spawned.elapsed() < Duration::from_secs(5),
            "the python binding never attached to the segment"
        );
        std::thread::sleep(Duration::from_millis(2));
    }

    let mut expected_mutations = Vec::new();
    let mut quantity = 3_000_000_usize;
    for _ in 0..3 {
        quantity += 1;
        let commit = book
            .apply_source_delta(&ask_quantity_delta(next_step(), quantity))
            .unwrap();
        assert_eq!(commit.mutations().len(), 1, "one coordinate per delta");
        published.push(book.publish());
        writer
            .publish(handle, published.last().unwrap(), 0)
            .unwrap();
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
            expected_mutations.push(expected_python_mutation_line(commit.revision(), record));
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    for _ in 0..64 {
        quantity += 1;
        let commit = book
            .apply_source_delta(&ask_quantity_delta(next_step(), quantity))
            .unwrap();
        assert_eq!(commit.mutations().len(), 1);
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
    published.push(book.publish());
    writer
        .publish(handle, published.last().unwrap(), 0)
        .unwrap();

    let status = child.wait().expect("child exits");
    collector.join().expect("stdout collector");
    let mut stderr = String::new();
    if let Some(mut handle) = child.stderr.take() {
        let _ = handle.read_to_string(&mut stderr);
    }
    let _ = std::fs::remove_file(&path);
    let lines = lines.lock().expect("stdout lines").clone();
    assert!(
        status.success(),
        "python binding failed: {stderr}\n{lines:?}"
    );

    assert!(
        lines
            .first()
            .is_some_and(|line| line.starts_with("bbo revision=")),
        "the python binding printed no initial bbo line: {lines:?}"
    );

    let expected_bbo_by_revision: HashMap<u64, String> = published
        .iter()
        .map(|book| (book.revision(), expected_bbo(book)))
        .collect();
    let bbo_lines: Vec<&String> = lines
        .iter()
        .filter(|line| line.starts_with("bbo "))
        .collect();
    assert!(bbo_lines.len() >= 3, "too few bbo lines: {lines:?}");
    let mut last_revision = 0_u64;
    for line in &bbo_lines {
        let revision: u64 = line
            .split_whitespace()
            .find_map(|field| field.strip_prefix("revision="))
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(|| panic!("unparsable bbo line: {line}"));
        assert!(revision > last_revision, "bbo went backwards: {line}");
        last_revision = revision;
        let expected = expected_bbo_by_revision
            .get(&revision)
            .unwrap_or_else(|| panic!("bbo line named a revision never published: {line}"));
        assert_eq!(
            **line, *expected,
            "bbo line does not match revision {revision}"
        );
    }

    let mutation_lines: Vec<&String> = lines
        .iter()
        .filter(|line| line.starts_with("mutation "))
        .collect();
    assert!(
        mutation_lines.len() >= expected_mutations.len(),
        "expected at least {} mutation lines, got {}: {lines:?}",
        expected_mutations.len(),
        mutation_lines.len()
    );
    for (line, want) in mutation_lines.iter().zip(expected_mutations.iter()) {
        assert_eq!(
            **line, *want,
            "the python binding decoded a different mutation"
        );
    }

    let loss_index = lines
        .iter()
        .position(|line| line == "continuity_lost reason=Overrun")
        .unwrap_or_else(|| panic!("no overrun was reported: {lines:?}"));
    let reattach_line = lines
        .get(loss_index + 1)
        .unwrap_or_else(|| panic!("no reattach line followed the overrun: {lines:?}"));
    let reattach_revision: u64 = reattach_line
        .strip_prefix("reattach revision=")
        .unwrap_or_else(|| panic!("malformed reattach line: {reattach_line}"))
        .parse()
        .unwrap_or_else(|_| panic!("unparsable reattach line: {reattach_line}"));
    assert!(
        expected_bbo_by_revision.contains_key(&reattach_revision),
        "the reattach line named a revision never published: {reattach_line}"
    );
}

/// A second market on the same segment, so a session-local index and a segment directory
/// index can be told apart by a binding test: a session that resolves only this one holds
/// index 0 for directory index 1.
const SECOND_SLUG: &str = "btc-up-or-down-5-min-1788188400";

fn second_market() -> MarketRef {
    MarketRef::new(
        Venue::new("limitless").unwrap(),
        NativeMarketKey::new(NativeIdentifierKind::slug(), SECOND_SLUG).unwrap(),
    )
}

/// A two-market file-backed segment with the cdylib this run just built beside it.
///
/// Both markets are installed and neither is published: a binding child establishes its own
/// dirty cursor on an empty ring, and the caller publishes afterwards so the entry it is
/// waiting for is one the cursor can actually reach.
fn binding_segment(
    tag: &str,
) -> (
    PathBuf,
    PathBuf,
    Arc<SegmentRegion>,
    SegmentWriter,
    MarketHandle,
) {
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

    let path = segment_path(tag);
    let layout = SegmentLayout::new(2, 2, 8, 16, 16).unwrap();
    let region = Arc::new(SegmentRegion::create_file(&path, layout.region_size()).unwrap());
    let mut writer = SegmentWriter::create(
        Arc::clone(&region),
        SegmentConfig::new(layout, 0x2026_0902_0000_0005_0000_0000_0000_0001, 1),
    )
    .unwrap();
    let _first = writer.install(&market()).unwrap();
    let second = writer.install(&second_market()).unwrap();
    assert_eq!(second.entry_index(), 1);
    (path, lib_path, region, writer, second)
}

/// Removes a segment file and whatever doorbell page its placement created beside it.
fn remove_segment(path: &std::path::Path) {
    let _ = std::fs::remove_file(format!("{}.doorbell", path.display()));
    let _ = std::fs::remove_file(path);
}

/// Runs a binding child to completion and returns whether it succeeded, its stdout lines,
/// and its stderr.
///
/// `republish` is what makes a dirty entry reachable for a child that wants one: the child's
/// cursor is created lazily at the ring's head on its first poll, so an entry published
/// before the child started can never be delivered to it, and a steady trickle during its
/// life can. `None` publishes nothing at all, which is what a child measuring its own park
/// needs — a publication would release the wait it is timing. Both loops are bounded — the
/// publishing loop by `PUBLISH_ROUNDS`, the wait by `CHILD_DEADLINE` and a kill — so nothing
/// here can hang.
fn run_binding_child(
    mut command: Command,
    writer: &mut SegmentWriter,
    republish: Option<(MarketHandle, &PublishedBook)>,
) -> (bool, Vec<String>, String) {
    const PUBLISH_ROUNDS: usize = 60;
    const PUBLISH_INTERVAL: Duration = Duration::from_millis(50);
    const CHILD_DEADLINE: Duration = Duration::from_secs(60);

    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn the binding child");
    let stdout = child.stdout.take().expect("child stdout");
    let stderr = child.stderr.take().expect("child stderr");
    let lines = Arc::new(Mutex::new(Vec::new()));
    let collector = {
        let lines = Arc::clone(&lines);
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                lines.lock().expect("stdout lines").push(line);
            }
        })
    };
    let errors = std::thread::spawn(move || {
        let mut text = String::new();
        let _ = BufReader::new(stderr).read_to_string(&mut text);
        text
    });

    let mut exited = None;
    for _ in 0..PUBLISH_ROUNDS {
        if let Ok(Some(status)) = child.try_wait() {
            exited = Some(status);
            break;
        }
        if let Some((handle, book)) = republish {
            writer.publish(handle, book, 0).unwrap();
        }
        std::thread::sleep(PUBLISH_INTERVAL);
    }
    let started = Instant::now();
    while exited.is_none() {
        match child.try_wait() {
            Ok(Some(status)) => exited = Some(status),
            Ok(None) if started.elapsed() >= CHILD_DEADLINE => {
                let _ = child.kill();
                exited = Some(child.wait().expect("reap the killed child"));
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(20)),
            Err(error) => panic!("waiting on the binding child failed: {error}"),
        }
    }

    collector.join().expect("stdout collector joins");
    let stderr = errors.join().expect("stderr collector joins");
    let lines = Arc::into_inner(lines)
        .expect("the collector released the lines")
        .into_inner()
        .expect("stdout lines");
    (
        exited.expect("the child was reaped").success(),
        lines,
        stderr,
    )
}

/// The Python binding's `Segment.wait` holds the session lock for its whole native call, so a
/// `close()` racing a parked wait cannot free the session out from under it.
///
/// The observable is the ordering, not a crash: an unlocked wait lets `close()` return
/// immediately while the native call is still parked on an address inside the mapping it just
/// unmapped, and a locked one makes `close()` block for what the wait has left to run. The
/// child asserts nothing itself beyond staying alive — it prints, and this test judges.
#[test]
fn shm_python_binding_serializes_a_wait_against_a_concurrent_close() {
    const SCRIPT: &str = r#"
import sys, threading, time
sys.path.insert(0, "bindings/python")
import pmws

segment = pmws.Segment(sys.argv[1])
generation = segment.publication_generation()
parked = threading.Event()
outcome = []


def waiter():
    parked.set()
    try:
        outcome.append("returned %r" % (segment.wait(generation, timeout_ms=1000),))
    except BaseException as error:
        outcome.append("raised %r" % (error,))


thread = threading.Thread(target=waiter)
thread.start()
assert parked.wait(5), "the waiter never started"
time.sleep(0.2)
began = time.monotonic()
segment.close()
blocked_ms = int((time.monotonic() - began) * 1000)
thread.join(10)
print("waiter %s" % outcome[0], flush=True)
print("close_blocked_ms %d" % blocked_ms, flush=True)
print("waiter_alive %s" % thread.is_alive(), flush=True)
segment.close()
"#;

    let (path, lib_path, _region, mut writer, _handle) = binding_segment("python-wait-close");
    let mut command = Command::new("python3");
    let _ = command
        .args(["-c", SCRIPT])
        .arg(&path)
        .env("PMWS_LIB", &lib_path);

    // Nothing is published during this child's life: the parked wait must reach its own
    // timeout rather than being released by a publication, which is what makes the blocked
    // interval a measurement of the lock and not of the writer.
    let (success, lines, stderr) = run_binding_child(command, &mut writer, None);
    remove_segment(&path);

    assert!(
        success,
        "the python child failed: {lines:?}\nstderr: {stderr}"
    );
    assert_eq!(
        lines.first().map(String::as_str),
        Some("waiter returned None"),
        "the parked wait did not end at its own timeout: {lines:?}\nstderr: {stderr}"
    );
    assert_eq!(
        lines.get(2).map(String::as_str),
        Some("waiter_alive False"),
        "the waiting thread never finished: {lines:?}"
    );
    let blocked: u64 = lines
        .get(1)
        .and_then(|line| line.strip_prefix("close_blocked_ms "))
        .unwrap_or_else(|| panic!("no close_blocked_ms line: {lines:?}"))
        .parse()
        .unwrap_or_else(|_| panic!("unparsable close_blocked_ms line: {lines:?}"));
    assert!(
        blocked >= 500,
        "close() returned after {blocked} ms while a wait with ~800 ms left was parked, so it \
         freed the session under the native call"
    );
}

/// The Python binding refuses a wait argument its narrowing would otherwise corrupt, and
/// carries the segment directory index a dirty entry names back to the market it resolved.
#[test]
fn shm_python_binding_checks_wait_arguments_and_carries_a_directory_index() {
    const SCRIPT: &str = r#"
import sys, time
sys.path.insert(0, "bindings/python")
import pmws

segment = pmws.Segment(sys.argv[1])
market = segment.resolve("limitless", "slug", sys.argv[2])
print("session_index %d" % market._index, flush=True)
print("directory_index %d" % market.directory_index, flush=True)

deadline = time.monotonic() + 30
entry = None
while entry is None and time.monotonic() < deadline:
    try:
        entry = segment.next_dirty()
    except pmws.PmwsDirtyRescan:
        continue
print("dirty_index %s" % (entry[0] if entry else "none"), flush=True)


def refused(label, call):
    try:
        call()
    except Exception as error:
        print("%s %s" % (label, type(error).__name__), flush=True)
        return
    print("%s ACCEPTED" % label, flush=True)


refused("spin_negative", lambda: segment.wait(0, spin_micros=-1, timeout_ms=0))
refused("spin_over", lambda: segment.wait(0, spin_micros=pmws.PMWS_MAX_SPIN_MICROS + 1, timeout_ms=0))
refused("timeout_over", lambda: segment.wait(0, timeout_ms=2 ** 40))
refused("timeout_negative", lambda: segment.wait(0, timeout_ms=-5))
refused("generation_over", lambda: segment.wait(2 ** 64, timeout_ms=0))
refused("spin_not_integer", lambda: segment.wait(0, spin_micros=1.5, timeout_ms=0))
segment.close()
"#;

    let (path, lib_path, _region, mut writer, handle) = binding_segment("python-wait-args");
    let book = OrderBook::new(second_market()).publish();
    let mut command = Command::new("python3");
    let _ = command
        .args(["-c", SCRIPT])
        .arg(&path)
        .arg(SECOND_SLUG)
        .env("PMWS_LIB", &lib_path);

    let (success, lines, stderr) = run_binding_child(command, &mut writer, Some((handle, &book)));
    remove_segment(&path);

    assert!(
        success,
        "the python child failed: {lines:?}\nstderr: {stderr}"
    );
    assert_eq!(
        lines,
        vec![
            "session_index 0".to_owned(),
            "directory_index 1".to_owned(),
            "dirty_index 1".to_owned(),
            "spin_negative ValueError".to_owned(),
            "spin_over ValueError".to_owned(),
            "timeout_over ValueError".to_owned(),
            "timeout_negative ValueError".to_owned(),
            "generation_over ValueError".to_owned(),
            "spin_not_integer TypeError".to_owned(),
        ],
        "stderr: {stderr}"
    );
}

/// The Node binding makes the same two guarantees: every `wait` argument is checked before
/// the native narrowing can wrap it, and a resolved market carries the directory index a
/// dirty entry names.
#[test]
fn shm_node_binding_checks_wait_arguments_and_carries_a_directory_index() {
    const SCRIPT: &str = r#"
const pmws = await import('./bindings/node/pmws.ts');
const segment = new pmws.Segment(process.argv[1]);
const market = segment.resolve("limitless", "slug", process.argv[2]);
console.log(`session_index ${market.index}`);
console.log(`directory_index ${market.directoryIndex}`);

const deadline = Date.now() + 30000;
let entry = null;
while (entry === null && Date.now() < deadline) {
  try {
    entry = segment.nextDirty();
  } catch (error) {
    if (error.code !== "PMWS_DIRTY_RESCAN") {
      throw error;
    }
  }
}
console.log(`dirty_index ${entry === null ? "none" : entry.directoryIndex}`);

const refused = (label, call) => {
  try {
    call();
    console.log(`${label} ACCEPTED`);
  } catch (error) {
    console.log(`${label} ${error.constructor.name}`);
  }
};

refused("spin_negative", () => segment.wait(0n, { spinMicros: -1, timeoutMs: 0 }));
refused("spin_over", () => segment.wait(0n, { spinMicros: pmws.MAX_SPIN_MICROS + 1, timeoutMs: 0 }));
refused("timeout_over", () => segment.wait(0n, { timeoutMs: 2 ** 40 }));
refused("timeout_negative", () => segment.wait(0n, { timeoutMs: -5 }));
refused("generation_over", () => segment.wait(1n << 64n, { timeoutMs: 0 }));
refused("generation_not_bigint", () => segment.wait(0, { timeoutMs: 0 }));
refused("spin_not_integer", () => segment.wait(0n, { spinMicros: 1.5, timeoutMs: 0 }));
segment.close();
"#;

    let (path, lib_path, _region, mut writer, handle) = binding_segment("node-wait-args");
    let book = OrderBook::new(second_market()).publish();
    let mut command = Command::new("node");
    let _ = command
        .args(["--input-type=module", "-e", SCRIPT, "--"])
        .arg(&path)
        .arg(SECOND_SLUG)
        .env("PMWS_LIB", &lib_path);

    let (success, lines, stderr) = run_binding_child(command, &mut writer, Some((handle, &book)));
    remove_segment(&path);

    assert!(
        success,
        "the node child failed: {lines:?}\nstderr: {stderr}"
    );
    assert_eq!(
        lines,
        vec![
            "session_index 0".to_owned(),
            "directory_index 1".to_owned(),
            "dirty_index 1".to_owned(),
            "spin_negative RangeError".to_owned(),
            "spin_over RangeError".to_owned(),
            "timeout_over RangeError".to_owned(),
            "timeout_negative RangeError".to_owned(),
            "generation_over RangeError".to_owned(),
            "generation_not_bigint TypeError".to_owned(),
            "spin_not_integer TypeError".to_owned(),
        ],
        "stderr: {stderr}"
    );
}

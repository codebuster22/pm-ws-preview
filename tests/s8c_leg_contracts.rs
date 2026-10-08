#![forbid(unsafe_code)]

//! Contracts for the two pm-ws legs of the S8c comparative harness,
//! `examples/embedded_live.rs` and `examples/matched_consumer.rs`, driven against the
//! scripted controlled peer.
//!
//! Both legs read live venue traffic in a real run, so the one thing no unit test can reach is
//! the path that matters most: a venue frame off the wire, decoded, given provenance, applied
//! to a book, delivered to a consumer that is not the ingesting thread — in the embedded leg
//! a strategy thread, in the shm leg another process reading a segment — then stamped,
//! digested, and written as an `.obs` row. That whole path is what these drive, against a
//! local peer that speaks the venue's Engine.IO/Socket.IO dialect: no venue traffic, and no
//! part of either leg stubbed out.
//!
//! The digest asserted in both is the same one, computed with an independent FNV-1a
//! implementation over the bytes `bench/sdk-harness/README.md` specifies for this book, so it
//! proves each leg's canonicalization and hashing rather than restating them. That the two
//! legs agree on it is the property the whole harness rests on: the embedded leg digests a
//! `PublishedBook`'s levels in process, the shm leg digests the same levels after they have
//! crossed the segment ABI, and a matcher pairs their observations by that value alone.
//!
//! It is the digest of the *last* book state the peer sends, which the coalescing
//! latest-state surface guarantees each leg observes: intermediate states may be coalesced
//! away by design, the final one cannot be.

mod support;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use support::controlled_peer::{ControlledPeer, PeerConfig};
use tokio::time::Instant;

/// The two markets this contract pins: one that keeps producing, and one the venue resolves.
const LIVE_MARKET: &str = "btc-up-or-down-5-min-1788172500";
const RESOLVED_MARKET: &str = "eth-up-or-down-5-min-1788172500";

/// How many book states the peer sends for [`LIVE_MARKET`]. Several, so the run has more than
/// one revision to coalesce, and the last one is unambiguous.
const UPDATE_ROUNDS: u64 = 12;

/// The quantity the last round carries: the rounds run `100 + i` and the assertion below is
/// pinned to the last of them.
const FINAL_QUANTITY: u64 = 100 + UPDATE_ROUNDS - 1;

/// FNV-1a 64 over `B0.53:111;|A0.6:200;`, the serialization
/// `bench/sdk-harness/README.md` specifies for the final book below, computed with an
/// implementation that is not this repository's.
const FINAL_DIGEST: &str = "1e4a783334eab99b";

/// How long this leg is given to run before it writes its log and exits. The peer's whole
/// script lands in the first moments of it; the rest is slack for a loaded machine.
const RUN_SECONDS: u64 = 10;

/// How long the leg is given to exit after its own duration elapses.
const EXIT_TIMEOUT: Duration = Duration::from_secs(60);

/// How long the shm leg runs. Longer than the embedded leg's: it spends its first moments
/// leasing its set through the control channel before it observes anything.
const SHM_RUN_SECONDS: u64 = 15;

/// How long one setup step — a daemon's socket, a consumer's leases — is given before this
/// contract gives up on it.
const STEP_TIMEOUT: Duration = Duration::from_secs(30);

const POLL_INTERVAL: Duration = Duration::from_millis(100);

static NEXT_PATH: AtomicU32 = AtomicU32::new(0);

/// A peer whose Engine.IO heartbeat is far longer than this run, so nothing here depends on
/// heartbeat timing.
fn patient_peer() -> PeerConfig {
    PeerConfig {
        ping_interval_ms: 60_000,
        ping_timeout_ms: 60_000,
        ..PeerConfig::default()
    }
}

/// A short absolute path under `/tmp`, short because a Unix domain socket address carries
/// about a hundred bytes.
fn temp_path(extension: &str) -> PathBuf {
    let sequence = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
    PathBuf::from(format!(
        "/tmp/pmws-s8c-{}-{sequence}.{extension}",
        std::process::id()
    ))
}

/// `target/<profile>`, derived from this test binary's own location the way the
/// cross-process suite derives the cdylib's.
fn profile_root() -> PathBuf {
    let mut path = std::env::current_exe().expect("test binary path");
    let _ = path.pop();
    if path.ends_with("deps") {
        let _ = path.pop();
    }
    path
}

/// `target/<profile>/examples/embedded_live`, where cargo uplifts a plain example binary.
fn example_path(name: &str) -> PathBuf {
    let mut path = profile_root();
    path.push("examples");
    path.push(name);
    path
}

/// The cargo argument that selects the profile this test binary was itself built under, so
/// the example this contract runs is built the same way. The dev profile's directory is
/// named `debug` and is cargo's default, so it names no argument at all.
fn profile_arguments() -> Vec<String> {
    let profile = profile_root()
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "debug".to_owned());
    match profile.as_str() {
        "debug" => Vec::new(),
        "release" => vec!["--release".to_owned()],
        other => vec!["--profile".to_owned(), other.to_owned()],
    }
}

/// Names of the examples already built by this test process.
static BUILT_EXAMPLES: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Builds `name` as a plain example binary, once per test process however many contracts
/// drive it.
///
/// An example carrying its own `#[test]`s is a test target: `cargo test` builds its test
/// harness into `target/<profile>/examples/<name>-<hash>` and never the plain binary this
/// contract has to execute, so a checkout whose only build is a test run has nothing at
/// [`example_path`]. Building it here is what makes the contract answer for its own inputs
/// on a fresh checkout; where a build already exists it costs a cargo fingerprint check.
fn build_example(name: &str) {
    let mut built = BUILT_EXAMPLES
        .lock()
        .expect("the example build ledger is not poisoned");
    if built.iter().any(|done| done == name) {
        return;
    }
    let output = Command::new(env!("CARGO"))
        .args(["build", "--locked", "--example", name])
        .args(profile_arguments())
        .output()
        .unwrap_or_else(|error| panic!("cargo build --example {name} could not run: {error}"));
    assert!(
        output.status.success(),
        "cargo build --example {name} failed ({}):\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    built.push(name.to_owned());
}

/// The example binary this contract drives, built if this checkout has not built it yet.
fn example_binary(name: &str) -> PathBuf {
    build_example(name);
    let path = example_path(name);
    assert!(
        path.is_file(),
        "{} is missing after `cargo build --example {name}` reported success",
        path.display()
    );
    path
}

fn field<'a>(report: &'a str, key: &str) -> &'a str {
    report
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{key}: ")))
        .unwrap_or_else(|| panic!("the report has no {key} line:\n{report}"))
}

fn count(report: &str, key: &str) -> u64 {
    field(report, key)
        .parse()
        .unwrap_or_else(|_| panic!("{key} is not a count:\n{report}"))
}

/// The `.obs` rows for `slug`, as `(t_obs_ns, digest, seq, revision)`.
fn obs_rows(log: &str, slug: &str) -> Vec<(u64, String, u64, u64)> {
    log.lines()
        .filter_map(|line| line.strip_prefix("obs "))
        .filter_map(|row| {
            let mut fields = row.split_whitespace();
            let market = fields.next()?;
            if market != slug {
                return None;
            }
            let stamp = fields.next()?.parse().ok()?;
            let digest = fields.next()?.to_owned();
            let seq = fields.next()?.parse().ok()?;
            let revision = fields.next()?.strip_prefix("rev=")?.parse().ok()?;
            Some((stamp, digest, seq, revision))
        })
        .collect()
}

fn header<'a>(log: &'a str, key: &str) -> &'a str {
    log.lines()
        .find_map(|line| line.strip_prefix(&format!("# {key}: ")))
        .unwrap_or_else(|| panic!("the log has no {key} header:\n{log}"))
}

fn write_slugs(path: &Path, slugs: &[&str]) {
    std::fs::write(path, format!("{}\n", slugs.join("\n"))).expect("the slug file writes");
}

/// Both pm-ws legs observe the same venue books and agree on their content digests.
///
/// One test rather than two, run one leg after the other, because each leg's half stands up a
/// controlled peer and a client process of its own — the shm half a `pmwsd` besides — and the
/// peer's accept deadline is a fixture constant. Running the two halves concurrently inside
/// one test binary would put that deadline in competition with this contract's own load, on
/// top of the other test binaries `cargo test` already runs beside it.
#[tokio::test]
async fn the_two_pm_ws_legs_observe_venue_books_and_agree_on_their_digests() {
    the_embedded_leg_observes_venue_books_and_logs_their_digests().await;
    the_shm_leg_leases_a_set_and_logs_the_digests_it_reads_from_the_segment().await;
}

/// The embedded leg reads venue frames off a real connection, applies them to one book per
/// market, observes the published states on a thread of its own, and writes the harness's
/// observation log — closing a market's recording on the venue's own resolution report.
async fn the_embedded_leg_observes_venue_books_and_logs_their_digests() {
    let binary = example_binary("embedded_live");
    let slugs = temp_path("slugs");
    let observations = temp_path("obs");
    let _removed = std::fs::remove_file(&observations);
    write_slugs(&slugs, &[LIVE_MARKET, RESOLVED_MARKET]);

    let mut peer = ControlledPeer::start(patient_peer()).await;
    let child = Command::new(&binary)
        .arg("--slugs")
        .arg(&slugs)
        .arg("--obs-out")
        .arg(&observations)
        .arg("--label")
        .arg("controlled peer, s8c embedded contract")
        .arg("--endpoint")
        .arg(peer.endpoint())
        .arg("--seconds")
        .arg(RUN_SECONDS.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the embedded leg starts");

    let mut connection = peer.next_connection().await;
    let subscription = connection.complete_handshake().await;
    let mut subscribed = subscription.slugs.clone();
    subscribed.sort();
    assert_eq!(
        subscribed,
        {
            let mut expected = vec![LIVE_MARKET.to_owned(), RESOLVED_MARKET.to_owned()];
            expected.sort();
            expected
        },
        "the leg subscribed its whole pinned set on one connection"
    );

    for round in 0..UPDATE_ROUNDS {
        let quantity = (100 + round).to_string();
        connection
            .send_orderbook(
                LIVE_MARKET,
                &[("0.53", quantity.as_str())],
                &[("0.60", "200")],
                Some(round + 1),
            )
            .await;
        connection
            .send_orderbook(
                RESOLVED_MARKET,
                &[("0.41", "10")],
                &[("0.59", "20")],
                Some(round + 1),
            )
            .await;
    }
    connection
        .send_market_resolved(
            RESOLVED_MARKET,
            "single",
            "Yes",
            0,
            "2026-09-06T00:00:00.000Z",
        )
        .await;

    let waiter = tokio::task::spawn_blocking(move || child.wait_with_output());
    let output = tokio::time::timeout(EXIT_TIMEOUT, waiter)
        .await
        .expect("the embedded leg exits inside its own duration plus slack")
        .expect("the wait task completes")
        .expect("the embedded leg's output is readable");
    let report = String::from_utf8_lossy(&output.stdout).into_owned();
    assert!(
        output.status.success(),
        "the embedded leg exited {:?}:\n{report}\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );

    assert_eq!(field(&report, "leg"), "rust-embedded");
    assert_eq!(count(&report, "connections"), 1);
    assert_eq!(count(&report, "markets_requested"), 2);
    assert_eq!(
        count(&report, "orderbook_updates"),
        UPDATE_ROUNDS * 2,
        "every venue book this peer sent reached the leg:\n{report}"
    );
    assert_eq!(
        count(&report, "applied"),
        UPDATE_ROUNDS * 2,
        "every venue book was normalized and committed to its own book:\n{report}"
    );
    assert_eq!(count(&report, "apply_failures"), 0, "{report}");
    assert_eq!(count(&report, "candidate_failures"), 0, "{report}");
    assert_eq!(count(&report, "provenance_failures"), 0, "{report}");
    assert_eq!(count(&report, "frames_unrouted"), 0, "{report}");
    assert_eq!(
        count(&report, "resolutions_closed"),
        1,
        "the venue's resolution closed exactly the market it named:\n{report}"
    );
    assert!(
        count(&report, "obs_rows") >= 2,
        "the strategy thread observed nothing:\n{report}"
    );
    assert_eq!(count(&report, "obs_dropped"), 0, "{report}");
    assert_eq!(
        count(&report, "markets_observed"),
        2,
        "both markets were observed before one of them resolved:\n{report}"
    );

    let log = std::fs::read_to_string(&observations).expect("the observation log is written");
    assert!(log.starts_with("# pmws-obs v1\n"), "{log}");
    assert_eq!(header(&log, "leg"), "rust-embedded");
    assert_eq!(header(&log, "clock"), "epoch_ns");
    assert_eq!(header(&log, "size"), "2");
    assert_eq!(header(&log, "dropped"), "0");

    let live = obs_rows(&log, LIVE_MARKET);
    assert!(!live.is_empty(), "no row for the live market:\n{log}");
    for (index, (_stamp, _digest, seq, _revision)) in live.iter().enumerate() {
        assert_eq!(
            u64::try_from(index).expect("a small index"),
            *seq,
            "the per-market sequence numbers each market from zero:\n{log}"
        );
    }
    let revisions: Vec<u64> = live.iter().map(|row| row.3).collect();
    assert!(
        revisions.windows(2).all(|pair| pair[0] < pair[1]),
        "the leg recorded a revision twice or out of order: {revisions:?}"
    );
    let (final_stamp, final_digest, _seq, final_revision) =
        live.last().expect("a last row").clone();
    assert_eq!(
        final_digest, FINAL_DIGEST,
        "the last observed book digests to the value an independent FNV-1a computes for \
         B0.53:{FINAL_QUANTITY};|A0.6:200;"
    );
    assert_eq!(
        final_revision, UPDATE_ROUNDS,
        "the last observed revision is the last book the peer sent"
    );
    assert!(
        final_stamp > 1_700_000_000_000_000_000,
        "the observation stamp is not epoch nanoseconds: {final_stamp}"
    );

    let resolved = obs_rows(&log, RESOLVED_MARKET);
    assert!(
        !resolved.is_empty(),
        "the resolved market was never observed before it resolved:\n{log}"
    );

    drop(connection);
    let _removed = std::fs::remove_file(&observations);
    let _removed = std::fs::remove_file(&slugs);
}

/// The daemon's own configuration for this contract: both markets pinned so the shard has a
/// venue connection from the first instant, a small delivery geometry so segment creation
/// costs milliseconds rather than tens of megabytes of zero fill, and a dirty ring wide
/// enough that this run's publications never lap it — the leg handles a lap, but this
/// contract is about the ordinary path.
fn write_daemon_config(endpoint: &str, socket: &Path) -> PathBuf {
    let path = temp_path("toml");
    let document = format!(
        "control_socket = {:?}\nendpoint = {endpoint:?}\n\
         markets = [{LIVE_MARKET:?}, {RESOLVED_MARKET:?}]\n\
         lease_ttl_ms = 0\nlevel_capacity = 64\nmarkets_per_shard = 8\n\
         [delivery]\nsegment_slots = 64\nevent_capacity = 16\ndirty_capacity = 256\n",
        socket.display().to_string()
    );
    std::fs::write(&path, document).expect("the daemon config is written");
    path
}

/// A `pmwsd` child that is always cleaned up, so a failing assertion never leaks a daemon
/// holding the socket path.
struct Daemon {
    child: Option<Child>,
    socket: PathBuf,
    config: PathBuf,
    log: PathBuf,
}

impl Daemon {
    async fn start(endpoint: &str) -> Self {
        let socket = temp_path("sock");
        let config = write_daemon_config(endpoint, socket.as_path());
        let log = temp_path("daemon-log");
        let child = Command::new(env!("CARGO_BIN_EXE_pmwsd"))
            .arg("--config")
            .arg(&config)
            .stdout(Stdio::null())
            .stderr(Stdio::from(
                File::create(&log).expect("the daemon log is created"),
            ))
            .spawn()
            .expect("pmwsd starts");
        let daemon = Self {
            child: Some(child),
            socket,
            config,
            log,
        };
        daemon.await_socket().await;
        daemon
    }

    /// Waits for the socket file alone, never for an answer on it: an answer costs a
    /// `pmwsctl` process, and the seconds that takes in a debug build are seconds the daemon
    /// spends waiting for the controlled peer to accept its connection.
    async fn await_socket(&self) {
        let deadline = Instant::now() + STEP_TIMEOUT;
        while !self.socket.exists() {
            assert!(
                Instant::now() < deadline,
                "the daemon never created {}",
                self.socket.display()
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// How many of the daemon's markets a consumer session currently holds a lease on.
    fn leased_markets(&self) -> usize {
        let output = Command::new(env!("CARGO_BIN_EXE_pmwsctl"))
            .arg("--socket")
            .arg(&self.socket)
            .arg("status")
            .output()
            .expect("pmwsctl runs");
        if !output.status.success() {
            return 0;
        }
        let Ok(status) = serde_json::from_slice::<serde_json::Value>(&output.stdout) else {
            return 0;
        };
        status["markets"].as_array().map_or(0, |rows| {
            rows.iter()
                .filter(|row| row["leases"].as_u64().unwrap_or(0) > 0)
                .count()
        })
    }

    /// Waits until `wanted` of the daemon's markets are leased, which is what says the
    /// consumer has finished its lease conversation and is observing.
    async fn await_leases(&self, wanted: usize) {
        let deadline = Instant::now() + STEP_TIMEOUT;
        loop {
            let leased = self.leased_markets();
            if leased >= wanted {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "only {leased} of {wanted} markets were leased before the deadline"
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _killed = child.kill();
            let _reaped = child.wait();
            let log = std::fs::read_to_string(&self.log).unwrap_or_default();
            if !log.trim().is_empty() {
                eprintln!("daemon log:\n{log}");
            }
        }
        let _removed = std::fs::remove_file(&self.socket);
        let _removed = std::fs::remove_file(&self.config);
        let _removed = std::fs::remove_file(&self.log);
    }
}

/// The shm leg leases its whole pinned set on one control session, observes every revision
/// the daemon publishes into the segment it was handed, and digests the same book content the
/// embedded leg digests in process.
async fn the_shm_leg_leases_a_set_and_logs_the_digests_it_reads_from_the_segment() {
    let binary = example_binary("matched_consumer");
    let slugs = temp_path("slugs");
    let observations = temp_path("obs");
    write_slugs(&slugs, &[LIVE_MARKET, RESOLVED_MARKET]);

    let mut peer = ControlledPeer::start(patient_peer()).await;
    let daemon = Daemon::start(peer.endpoint().as_str()).await;
    let mut connection = peer.next_connection().await;
    let subscription = connection.complete_handshake().await;
    assert_eq!(
        subscription.slugs.len(),
        2,
        "the daemon subscribed both pinned markets on one shard"
    );

    let child = Command::new(&binary)
        .arg("--control")
        .arg(&daemon.socket)
        .arg("--slugs")
        .arg(&slugs)
        .arg("--obs-out")
        .arg(&observations)
        .arg("--label")
        .arg("controlled peer, s8c shm contract")
        .arg("--seconds")
        .arg(SHM_RUN_SECONDS.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the shm leg starts");
    daemon.await_leases(2).await;

    for round in 0..UPDATE_ROUNDS {
        let quantity = (100 + round).to_string();
        connection
            .send_orderbook(
                LIVE_MARKET,
                &[("0.53", quantity.as_str())],
                &[("0.60", "200")],
                Some(round + 1),
            )
            .await;
        connection
            .send_orderbook(
                RESOLVED_MARKET,
                &[("0.41", "10")],
                &[("0.59", (20 + round).to_string().as_str())],
                Some(round + 1),
            )
            .await;
    }

    let waiter = tokio::task::spawn_blocking(move || child.wait_with_output());
    let output = tokio::time::timeout(EXIT_TIMEOUT, waiter)
        .await
        .expect("the shm leg exits inside its own duration plus slack")
        .expect("the wait task completes")
        .expect("the shm leg's output is readable");
    let report = String::from_utf8_lossy(&output.stdout).into_owned();
    assert!(
        output.status.success(),
        "the shm leg exited {:?}:\n{report}\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );

    assert_eq!(field(&report, "leg"), "rust-shm");
    assert_eq!(
        count(&report, "markets_leased"),
        2,
        "both markets were leased on one session:\n{report}"
    );
    assert_eq!(count(&report, "lease_attach_failures"), 0, "{report}");
    assert_eq!(
        count(&report, "lease_foreign_segment"),
        0,
        "one shard's markets all resolve to one segment:\n{report}"
    );
    assert_eq!(count(&report, "lease_renew_failures"), 0, "{report}");
    assert_eq!(
        count(&report, "unleased_states"),
        0,
        "the leg read a state for a market it did not lease, which means its own slug lookup \
         does not name the markets the segment carries:\n{report}"
    );
    assert_eq!(
        count(&report, "markets_observed"),
        2,
        "both leased markets produced an observation:\n{report}"
    );
    assert!(
        count(&report, "obs_rows") >= 2,
        "the leg observed nothing through the segment:\n{report}"
    );
    assert!(
        count(&report, "daemon_samples_kept") >= 1,
        "no observation carried both of the daemon's own stamps:\n{report}"
    );

    let log = std::fs::read_to_string(&observations).expect("the observation log is written");
    assert_eq!(header(&log, "leg"), "rust-shm");
    assert_eq!(header(&log, "size"), "2");
    let live = obs_rows(&log, LIVE_MARKET);
    assert!(!live.is_empty(), "no row for the live market:\n{log}");
    let (_stamp, final_digest, _seq, _revision) = live.last().expect("a last row").clone();
    assert_eq!(
        final_digest, FINAL_DIGEST,
        "the last book read out of the segment digests to the value an independent FNV-1a \
         computes for B0.53:{FINAL_QUANTITY};|A0.6:200; -- the same value the embedded leg \
         computes in process for the same book"
    );
    let revisions: Vec<u64> = live.iter().map(|row| row.3).collect();
    assert!(
        revisions.windows(2).all(|pair| pair[0] < pair[1]),
        "the leg recorded a revision twice or out of order: {revisions:?}"
    );

    drop(connection);
    let _removed = std::fs::remove_file(&observations);
    let _removed = std::fs::remove_file(&slugs);
}

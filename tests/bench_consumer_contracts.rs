#![forbid(unsafe_code)]

//! Contracts for the S8 full-fleet benchmark consumers, `examples/bench_consumer.py` and
//! `examples/bench_consumer.ts`, driven against the real `pmwsd` and the scripted controlled
//! peer.
//!
//! One test per consumer, and each one drives the whole shape a benchmark run has: the
//! consumer leases its anchor market through the control channel, the runner appends a second
//! market to the slug file while it runs and the consumer leases that one mid-run *on that
//! same session* rather than opening a second one, venue updates produce arrival-stamped
//! publications the consumer samples, and the venue resolves the second market so the
//! consumer releases exactly that market's lease and keeps the anchor's. `pmwsctl status`
//! exposes no count of open control sessions, and `attachments` cannot stand in for one --
//! `lease()` re-runs the same descriptor-transfer conversation `connect()` does (see the
//! attachments assertion below) -- so the one-session shape itself is not independently
//! provable from this daemon's status surface; `leases` still proves the release it always
//! has, one session or many.
//!
//! Everything is gated on events rather than on the clock: the consumers are run with
//! `--until-resolutions 1`, so the measurement loop ends when the resolution arrives rather
//! than at a deadline, and with `--hold-until`, so the process stays alive holding its
//! remaining leases until this test has finished inspecting them. Nothing here waits a fixed
//! interval for something to have happened.
//!
//! Three distributions are reported and all three are pinned here: `daemon_latency`
//! (`commit_time - arrival_time`, two daemon stamps on one clock), `consumer_latency`
//! (`observation - commit_time`) and the end-to-end latency this file has always checked.
//! Their percentiles are withheld at this sample count, so what is asserted is that each
//! group is present, that each measured something, and that the daemon's own half — which
//! involves no consumer clock — rejected nothing. The per-sample identity
//! `daemon + consumer == end_to_end` is pinned on the arithmetic itself, in
//! `examples/latency_probe.rs`'s unit tests, where the raw samples are reachable; percentiles
//! of the three distributions do not add, because each is ordered independently.
//!
//! The clock domain is proven by `samples_kept`. A consumer whose observation clock is not
//! the daemon's `arrival_time` domain — CLOCK_REALTIME nanoseconds since the epoch — produces
//! deltas of order 10^18 ns, every one of which the discard rule rejects, and keeps nothing.
//! A single kept sample is therefore only possible from a consumer reading the same clock.

mod support;

use pm_ws::limitless::shard::MarketStatus;
use pm_ws::{MarketRow, ShardReport};
use serde::Deserialize;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use support::controlled_peer::{ControlledPeer, PeerConfig};
use tokio::time::Instant;

const ANCHOR_MARKET: &str = "btc-up-or-down-5-min-1788172500";
const CHURN_MARKET: &str = "eth-up-or-down-5-min-1788172500";
const STEP_TIMEOUT: Duration = Duration::from_secs(30);
/// Longer than the consumers' own `--seconds` cap, so a run that never observes its
/// resolution still writes the report that says what it did see, rather than failing with an
/// empty file and nothing to read.
const REPORT_TIMEOUT: Duration = Duration::from_secs(75);
const POLL_INTERVAL: Duration = Duration::from_millis(25);
const PATIENT_HEARTBEAT_MS: u64 = 60_000;
/// Enough updates that a run has several revisions to sample without depending on how many
/// of them a single wake happens to coalesce.
const UPDATE_ROUNDS: u64 = 12;
/// How long one resolution is given to reach the consumer before another copy is sent.
const RESOLUTION_RESEND_INTERVAL: Duration = Duration::from_millis(400);
/// How many copies of one market's resolution this contract will send before giving up.
const RESOLUTION_RESENDS: u32 = 60;

static NEXT_PATH: AtomicU32 = AtomicU32::new(0);

fn patient_peer() -> PeerConfig {
    PeerConfig {
        ping_interval_ms: PATIENT_HEARTBEAT_MS,
        ping_timeout_ms: PATIENT_HEARTBEAT_MS,
        ..PeerConfig::default()
    }
}

/// A short absolute path under `/tmp`, short because a Unix domain socket address carries
/// about a hundred bytes.
fn temp_path(extension: &str) -> PathBuf {
    let pid = std::process::id();
    let sequence = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
    PathBuf::from(format!("/tmp/pmwsb-t{pid}-{sequence}.{extension}"))
}

fn lock_path(socket: &Path) -> PathBuf {
    let mut name = socket.as_os_str().to_owned();
    name.push(".lock");
    PathBuf::from(name)
}

/// The benchmark's own daemon shape: the anchor market pinned and nothing else, so the shard
/// has a venue connection from the first instant and every other subscription in the run
/// exists because a consumer leased it. A small delivery geometry keeps the segment's creation
/// to milliseconds rather than tens of megabytes of zero fill.
///
/// The anchor is pinned for the same reason `bench/run_fleet_bench.sh` pins each half's first
/// market: a consumer's reader session is a `connect()` on it and cannot be closed without
/// unmapping the segment the reader is parked on, so its lease is the one lease a run never
/// releases. An operator pin dominates a lease, which makes that market's demand the
/// operator's and leaves every leased market's demand purely a consumer's.
fn write_config(endpoint: &str, socket: &Path) -> PathBuf {
    let path = temp_path("toml");
    let document = format!(
        "control_socket = {:?}\nendpoint = {endpoint:?}\nmarkets = [{ANCHOR_MARKET:?}]\n\
         lease_ttl_ms = 0\nlevel_capacity = 64\n\
         [delivery]\nsegment_slots = 128\nevent_capacity = 16\ndirty_capacity = 16\n",
        socket.display().to_string()
    );
    std::fs::write(&path, document).expect("the test config is written");
    path
}

/// A `pmwsd` child process that is always cleaned up, so a failing assertion never leaks a
/// daemon holding the socket path.
struct Daemon {
    child: Option<Child>,
    socket: PathBuf,
    config: PathBuf,
    log: PathBuf,
}

impl Daemon {
    async fn start(config: PathBuf, socket: PathBuf) -> Self {
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

    /// Waits for the socket file alone, never for an answer on it.
    ///
    /// An answer costs a `pmwsctl` process, and the seconds that takes in a debug build are
    /// seconds the daemon spends waiting for the controlled peer to accept its connection:
    /// the peer accepts only inside `next_connection`, so a caller that spends that time
    /// before calling it hands the first connection to a client that has already given up on
    /// it.
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

    async fn terminate(mut self) -> i32 {
        let pid = self.child.as_ref().expect("the daemon is running").id();
        let signalled = Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status()
            .expect("kill runs");
        assert!(signalled.success(), "SIGTERM was delivered");
        let mut child = self.child.take().expect("the daemon is running");
        let status = tokio::task::spawn_blocking(move || child.wait().expect("the daemon exits"))
            .await
            .expect("the wait completes");
        status.code().unwrap_or(-1)
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _killed = child.kill();
            let _reaped = child.wait();
            let log = std::fs::read_to_string(&self.log).unwrap_or_default();
            eprintln!("daemon log:\n{log}");
            let _removed = std::fs::remove_file(&self.socket);
        }
        let _removed = std::fs::remove_file(&self.config);
        let _removed = std::fs::remove_file(lock_path(self.socket.as_path()));
        let _removed = std::fs::remove_file(&self.log);
    }
}

/// A consumer child that is always cleaned up, however the test leaves.
struct Consumer {
    child: Option<Child>,
    report: PathBuf,
    errors: PathBuf,
    slugs: PathBuf,
    sentinel: PathBuf,
}

impl Drop for Consumer {
    /// Kills a consumer still running, which is exactly the case where the test failed, and
    /// prints what it had said on its standard error first: a consumer that refused to start
    /// says why there and nowhere else, and this is the only chance to read it before the
    /// file goes.
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _killed = child.kill();
            let _reaped = child.wait();
            let errors = std::fs::read_to_string(&self.errors).unwrap_or_default();
            let report = std::fs::read_to_string(&self.report).unwrap_or_default();
            eprintln!("consumer stderr:\n{errors}\nconsumer stdout:\n{report}");
        }
        for path in [&self.report, &self.errors, &self.slugs, &self.sentinel] {
            let _removed = std::fs::remove_file(path);
        }
    }
}

async fn control(socket: &Path, arguments: &[&str]) -> (i32, String) {
    let socket = socket.to_path_buf();
    let arguments: Vec<String> = arguments.iter().map(|value| (*value).to_owned()).collect();
    tokio::task::spawn_blocking(move || {
        let output = Command::new(env!("CARGO_BIN_EXE_pmwsctl"))
            .arg("--socket")
            .arg(&socket)
            .args(&arguments)
            .output()
            .expect("pmwsctl runs");
        (
            output.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&output.stdout).into_owned(),
        )
    })
    .await
    .expect("the pmwsctl invocation completes")
}

/// What `pmwsctl status` prints, narrowed to what these contracts read. Unknown keys are
/// ignored, so a field added elsewhere in the status answer never breaks this file.
#[derive(Debug, Deserialize)]
struct StatusOutput {
    shards: Vec<ShardReport>,
    markets: Vec<MarketRow>,
}

impl StatusOutput {
    fn row(&self, slug: &str) -> Option<&MarketRow> {
        self.markets.iter().find(|row| row.market.slug == slug)
    }

    fn leases(&self, slug: &str) -> u32 {
        self.row(slug).map_or(0, |row| row.leases)
    }
}

async fn status(socket: &Path) -> StatusOutput {
    let (code, stdout) = control(socket, &["status"]).await;
    assert_eq!(code, 0, "status succeeds: {stdout}");
    serde_json::from_str(&stdout).expect("status prints the documented JSON")
}

/// Polls `status` for at most `within`, answering `None` rather than panicking when the
/// predicate never holds. The bounded half of [`await_status`], for a step whose stimulus the
/// caller repeats rather than waits out.
async fn poll_status(
    socket: &Path,
    within: Duration,
    predicate: impl Fn(&StatusOutput) -> bool,
) -> Option<StatusOutput> {
    let deadline = Instant::now() + within;
    loop {
        let seen = status(socket).await;
        if predicate(&seen) {
            return Some(seen);
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn await_status(
    socket: &Path,
    what: &str,
    predicate: impl Fn(&StatusOutput) -> bool,
) -> StatusOutput {
    let deadline = Instant::now() + STEP_TIMEOUT;
    loop {
        let seen = status(socket).await;
        if predicate(&seen) {
            return seen;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}; last saw {seen:?}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// `target/<profile>/libpm_ws.<ext>`, the cdylib this test run built, derived from this test
/// binary's own location the way the cross-process suite derives it.
fn cdylib_path() -> PathBuf {
    let mut path = std::env::current_exe().expect("test binary path");
    let _ = path.pop();
    if path.ends_with("deps") {
        let _ = path.pop();
    }
    path.push(if cfg!(target_os = "macos") {
        "libpm_ws.dylib"
    } else {
        "libpm_ws.so"
    });
    path
}

async fn build_cdylib() -> PathBuf {
    let built = tokio::task::spawn_blocking(|| {
        Command::new("cargo")
            .args(["build", "--lib", "--locked"])
            .status()
            .expect("cargo build --lib runs")
    })
    .await
    .expect("the build completes");
    assert!(built.success(), "cargo build --lib failed");
    let library = cdylib_path();
    assert!(library.is_file(), "the cdylib is at {}", library.display());
    library
}

fn append_slug(path: &Path, slug: &str) {
    let mut file = OpenOptions::new()
        .append(true)
        .open(path)
        .expect("the slug file opens for append");
    writeln!(file, "{slug}").expect("the slug is appended");
}

/// Waits for the consumer to have written its whole report, which the last key it prints
/// marks. The consumer writes the report in one synchronous write before it starts holding,
/// so a file containing that key contains all of it.
async fn await_report(path: &Path) -> String {
    let deadline = Instant::now() + REPORT_TIMEOUT;
    loop {
        let text = std::fs::read_to_string(path).unwrap_or_default();
        if text.contains("slugs_added_midrun:") {
            return text;
        }
        assert!(
            Instant::now() < deadline,
            "the consumer never finished its report; saw {text:?}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

fn field<'a>(report: &'a str, key: &str) -> &'a str {
    report
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{key}: ")))
        .unwrap_or_else(|| panic!("the report carries no {key} line:\n{report}"))
}

fn count(report: &str, key: &str) -> u64 {
    field(report, key)
        .parse()
        .unwrap_or_else(|_| panic!("{key} is not a count:\n{report}"))
}

fn milliseconds(report: &str, key: &str) -> f64 {
    field(report, key)
        .parse()
        .unwrap_or_else(|_| panic!("{key} is not a millisecond duration:\n{report}"))
}

/// Every key a benchmark report must carry for the S8 table to be assembled from it.
const REQUIRED_KEYS: [&str; 37] = [
    "label",
    "consumer",
    "runtime",
    "mode",
    "spin_budget_us",
    "clock",
    "lease_ttl_ms",
    "renew_interval_ms",
    "duration_seconds",
    "anchor_market",
    "markets_leased_peak",
    "markets_held",
    "markets_seen",
    "samples_kept",
    "wakes",
    "wakes_per_second",
    "timeouts",
    "dirty_delivered",
    "rescans",
    "samples_skipped",
    "samples_discarded",
    "samples_after_control",
    "transient_event_faults",
    "resolutions_observed",
    "slugs_added_midrun",
    "anchor_renewals",
    "anchor_renew_failures",
    "control_stall_count",
    "control_stall_total_ms",
    "control_stall_max_ms",
    "daemon_latency",
    "daemon_samples_kept",
    "daemon_samples_discarded",
    "consumer_latency",
    "consumer_samples_kept",
    "consumer_samples_discarded",
    "end_to_end",
];

/// Drives one benchmark consumer through a whole run and returns its report.
///
/// The daemon starts with nothing pinned, so the anchor market is subscribed because the
/// consumer leased it and the churn market because the consumer leased it mid-run — the two
/// halves of what the S8 benchmark claims to exercise. The venue then resolves the churn
/// market, and this function asserts the release against `pmwsctl status` **while the
/// consumer is still running**, which is the only moment the released market and the still-
/// held anchor can be told apart: exiting would release both.
async fn drive_consumer(name: &str, mut command: Command) -> String {
    let library = build_cdylib().await;
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let socket = temp_path("sock");
    let config = write_config(peer.endpoint().as_str(), socket.as_path());
    let daemon = Daemon::start(config, socket.clone()).await;

    let mut connection = peer.next_connection().await;
    let mut subscribed = connection.complete_handshake().await.slugs;
    let mut rounds = 0;
    while !subscribed.iter().any(|slug| slug == ANCHOR_MARKET) {
        rounds += 1;
        assert!(
            rounds < 8,
            "the daemon never subscribed its pinned anchor market"
        );
        subscribed = connection.expect_resubscription(STEP_TIMEOUT).await.slugs;
    }
    connection
        .send_orderbook(ANCHOR_MARKET, &[("0.61", "12")], &[("0.62", "13")], Some(1))
        .await;
    let _live = await_status(socket.as_path(), "the pinned anchor market live", |seen| {
        seen.row(ANCHOR_MARKET)
            .is_some_and(|row| row.market.status == MarketStatus::Live)
    })
    .await;

    let slugs = temp_path("slugs");
    std::fs::write(&slugs, format!("{ANCHOR_MARKET}\n")).expect("the slug file is written");
    let sentinel = temp_path("done");
    let report_path = temp_path("report");
    let errors_path = temp_path("stderr");

    let consumer = Consumer {
        child: Some(
            command
                .arg("--control")
                .arg(&socket)
                .arg("--slugs")
                .arg(&slugs)
                .arg("--seconds")
                .arg("40")
                .arg("--label")
                .arg(format!("{name} deterministic contract, controlled peer"))
                .arg("--lease-ttl-ms")
                .arg("1000")
                .arg("--rescan-ms")
                .arg("100")
                .arg("--until-resolutions")
                .arg("1")
                .arg("--hold-until")
                .arg(&sentinel)
                .env("PMWS_LIB", &library)
                .stdout(Stdio::from(
                    File::create(&report_path).expect("the report file is created"),
                ))
                .stderr(Stdio::from(
                    File::create(&errors_path).expect("the stderr file is created"),
                ))
                .spawn()
                .unwrap_or_else(|error| panic!("{name} starts: {error}")),
        ),
        report: report_path.clone(),
        errors: errors_path.clone(),
        slugs: slugs.clone(),
        sentinel: sentinel.clone(),
    };

    let _attached = await_status(socket.as_path(), "the consumer's anchor lease", |seen| {
        seen.leases(ANCHOR_MARKET) >= 1
    })
    .await;

    append_slug(slugs.as_path(), CHURN_MARKET);
    // A change to the daemon's desired set dials the whole new set on a fresh connection
    // rather than amending the one in flight, so the consumer's mid-run lease is observed as
    // a new connection here and not as a resubscription on the old one.
    let mut connection = peer.next_connection().await;
    let subscribed = connection.complete_handshake().await.slugs;
    assert!(
        subscribed.iter().any(|slug| slug == CHURN_MARKET)
            && subscribed.iter().any(|slug| slug == ANCHOR_MARKET),
        "{name}'s mid-run lease dialled the whole set: {subscribed:?}"
    );
    connection
        .send_orderbook(CHURN_MARKET, &[("0.31", "20")], &[("0.32", "21")], Some(2))
        .await;
    let _leased = await_status(
        socket.as_path(),
        "the churn market leased and live",
        |seen| {
            seen.leases(CHURN_MARKET) >= 1
                && seen
                    .row(CHURN_MARKET)
                    .is_some_and(|row| row.market.status == MarketStatus::Live)
        },
    )
    .await;

    for round in 0..UPDATE_ROUNDS {
        let size = format!("{}", 30 + round);
        connection
            .send_orderbook(
                ANCHOR_MARKET,
                &[("0.61", size.as_str())],
                &[("0.62", "13")],
                Some(10 + round),
            )
            .await;
        connection
            .send_orderbook(
                CHURN_MARKET,
                &[("0.31", size.as_str())],
                &[("0.32", "21")],
                Some(100 + round),
            )
            .await;
    }

    // The resolution is sent until the consumer acts on it, rather than once. The venue does
    // the same -- one market's `marketResolved` was observed three times byte-identically
    // inside 200 ms -- and a consumer's event cursor starts where the stream was when it
    // attached, so a single copy sent in the millisecond before the consumer resolved this
    // market would be a copy it can never read. Repeating removes that race from the test
    // without making it wait a fixed interval for anything.
    let mut sent = 0;
    let released = loop {
        connection
            .send_market_resolved(CHURN_MARKET, "CLOB", "YES", 0, "2026-01-01T00:00:00.000Z")
            .await;
        sent += 1;
        if let Some(seen) = poll_status(socket.as_path(), RESOLUTION_RESEND_INTERVAL, |seen| {
            seen.leases(CHURN_MARKET) == 0 && seen.leases(ANCHOR_MARKET) >= 1
        })
        .await
        {
            break seen;
        }
        assert!(
            sent < RESOLUTION_RESENDS,
            "{name} never released the resolved market's lease while holding its anchor's, \
             after {sent} resolutions"
        );
    };

    let report = await_report(report_path.as_path()).await;
    // `attachments` cannot tell one session leasing two markets from two sessions leasing one
    // each: `lease_market` (`src/ffi/mod.rs`) re-runs the same `ControlRequest::Attach`
    // conversation `connect()` uses, takes a fresh descriptor for the already-mapped segment,
    // and drops it -- so the daemon's `attach()` handler calls `record_attachment` on that
    // `lease()` exactly as it would on a second `connect()`. What this pins instead: this run
    // makes exactly the one connect plus one lease it is meant to and nothing more -- no
    // retried attach, no accidental extra connection -- scoped to the shard the anchor and
    // churn market actually share.
    let anchor_shard = released
        .row(ANCHOR_MARKET)
        .map(|row| row.shard)
        .expect("the anchor market has a shard");
    let attachments = released
        .shards
        .iter()
        .find(|shard| shard.shard == anchor_shard)
        .map_or(0, |shard| shard.attachments);
    assert_eq!(
        attachments, 2,
        "{name} made a different number of descriptor transfers than the one connect plus \
         one lease this run drives: {released:?}"
    );

    std::fs::write(&sentinel, b"done").expect("the sentinel is written");
    let mut consumer = consumer;
    let child = consumer.child.take().expect("the consumer is running");
    let finished = tokio::task::spawn_blocking(move || child.wait_with_output())
        .await
        .expect("the wait completes")
        .expect("the consumer exits");
    let errors = std::fs::read_to_string(&errors_path).unwrap_or_default();
    assert!(
        finished.status.success(),
        "{name} exited {:?}\nstderr: {errors}\nreport:\n{report}",
        finished.status.code()
    );

    let code = daemon.terminate().await;
    assert_eq!(code, 0, "the daemon served the whole run");
    report
}

fn assert_benchmark_report(name: &str, report: &str) {
    for key in REQUIRED_KEYS {
        assert!(
            report
                .lines()
                .any(|line| line.starts_with(&format!("{key}: "))),
            "{name}'s report carries no {key} line:\n{report}"
        );
    }
    assert_eq!(
        field(report, "mode"),
        "parked",
        "{name} ran parked:\n{report}"
    );
    assert_eq!(
        field(report, "spin_budget_us"),
        "0",
        "{name} parked with no spin budget:\n{report}"
    );
    assert_eq!(field(report, "anchor_market"), ANCHOR_MARKET);
    assert!(
        count(report, "wakes") >= 1,
        "{name} never woke on a publication:\n{report}"
    );
    assert!(
        count(report, "samples_kept") >= 1,
        "{name} kept no sample, so its observation clock is not the daemon's arrival-stamp \
         domain (or nothing was published):\n{report}"
    );
    for prefix in ["", "daemon_", "consumer_"] {
        assert!(
            report.contains(&format!("{prefix}percentiles: withheld, only ")),
            "{name} printed {prefix}percentiles below the 1000-sample floor:\n{report}"
        );
    }
    assert!(
        count(report, "daemon_samples_kept") >= 1,
        "{name} measured no daemon latency, though every state this run published carries \
         both an arrival and a commit stamp:\n{report}"
    );
    assert!(
        count(report, "consumer_samples_kept") >= 1,
        "{name} measured no consumer latency:\n{report}"
    );
    assert_eq!(
        count(report, "daemon_samples_discarded"),
        0,
        "{name} rejected a daemon-side difference, which needs no consumer clock at all:\n\
         {report}"
    );
    assert_eq!(
        count(report, "slugs_added_midrun"),
        1,
        "{name} did not pick up the market appended while it ran:\n{report}"
    );
    assert!(
        count(report, "resolutions_observed") >= 1,
        "{name} never observed the resolution:\n{report}"
    );
    assert_eq!(
        count(report, "leases_released_on_resolution"),
        1,
        "{name} released the one resolved market's lease exactly once, however many copies of \
         its resolution the venue sent:\n{report}"
    );
    assert_eq!(
        count(report, "anchor_resolutions_unreleased"),
        0,
        "{name} resolved its anchor, which this contract does not drive:\n{report}"
    );
    assert_eq!(
        count(report, "lease_renew_failures"),
        0,
        "{name} failed a lease renewal:\n{report}"
    );
    assert_eq!(
        count(report, "lease_attach_failures"),
        0,
        "{name} failed to attach a market:\n{report}"
    );
    assert_eq!(
        count(report, "anchor_renew_failures"),
        0,
        "{name} failed to renew its own reader session:\n{report}"
    );
    assert_eq!(
        field(report, "renew_interval_ms"),
        "333",
        "{name} renews at a third of the TTL it was told:\n{report}"
    );
    assert_eq!(
        count(report, "markets_seen"),
        2,
        "{name} sampled both of its markets:\n{report}"
    );
    // This run makes exactly two control-plane conversations on its one session beyond the
    // initial connect: leasing the churn market mid-run, and releasing it on resolution.
    // Neither the anchor's connect() nor any renew() is a lease()/release() conversation, so
    // this is an exact count here, not a lower bound.
    assert_eq!(
        count(report, "control_stall_count"),
        2,
        "{name} made a different number of lease()/release() conversations than the one lease \
         and one release this contract drives:\n{report}"
    );
    assert!(
        milliseconds(report, "control_stall_max_ms") > 0.0,
        "{name} reported a zero-duration control-plane conversation:\n{report}"
    );
    assert!(
        milliseconds(report, "control_stall_total_ms")
            >= milliseconds(report, "control_stall_max_ms"),
        "{name}'s total control-stall time is less than its own longest single stall:\n{report}"
    );
    // The mid-run lease is followed by several more update rounds for both markets before the
    // resolution, so at least one sample lands after it and is diverted here; the
    // resolution-triggered release happens right before this run's `--until-resolutions 1`
    // ends the loop, so it is not guaranteed to be followed by a further sample and is not
    // counted on to push this past 1.
    assert!(
        count(report, "samples_after_control") >= 1,
        "{name} never diverted a sample taken while its own control-plane conversation was \
         still blocking the measurement thread:\n{report}"
    );
    // `markets_leased_peak` is pinned at its correct value for this run's shape (anchor plus
    // one concurrently-held lease, never three markets with a release between two leases), so
    // this cannot by itself distinguish a `leased` set (Python's `self.leased`, TypeScript's
    // `leased`) that prunes on release from one that does not double-count a peak after a
    // release; only reading `_lease`/`leaseMarket` and the release paths proves that.
    assert_eq!(
        count(report, "markets_leased_peak"),
        2,
        "{name} did not report the anchor plus its one concurrently-held lease as the peak:\n\
         {report}"
    );
}

/// The Python benchmark consumer leases through the control channel, samples arrival-stamped
/// state, leases a market appended mid-run, and releases exactly the resolved market's lease.
#[tokio::test]
async fn the_python_bench_consumer_measures_leases_and_releases_on_resolution() {
    let mut command = Command::new("python3");
    command.arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/bench_consumer.py"));
    let report = drive_consumer("bench_consumer.py", command).await;
    assert_eq!(field(&report, "consumer"), "bench_consumer.py");
    assert_benchmark_report("bench_consumer.py", &report);
}

/// The TypeScript benchmark consumer does the same, through the same control channel and the
/// same segment, and its derived observation clock lands in the daemon's arrival-stamp domain.
#[tokio::test]
async fn the_node_bench_consumer_measures_leases_and_releases_on_resolution() {
    let mut command = Command::new("node");
    command.arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/bench_consumer.ts"));
    let report = drive_consumer("bench_consumer.ts", command).await;
    assert_eq!(field(&report, "consumer"), "bench_consumer.ts");
    assert!(
        count(&report, "clock_derivations") >= 1,
        "the node consumer reported no clock calibration:\n{report}"
    );
    assert_benchmark_report("bench_consumer.ts", &report);
}

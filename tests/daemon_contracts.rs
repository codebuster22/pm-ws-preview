#![forbid(unsafe_code)]

//! Operator-surface contracts for the real `pmwsd` and `pmwsctl` binaries, driven against
//! the scripted controlled peer.
//!
//! The daemon runs as a separate process here, exactly as an operator runs it, and every
//! command goes through the compiled `pmwsctl` over a real Unix domain socket. What is
//! proven is therefore the shipped surface: the configuration document, the control
//! protocol, the exit codes, and the venue traffic the peer sees on the wire.

mod support;

use pm_ws::limitless::shard::{
    MarketOutcome, MarketRejection, MarketStatus, Shard, ShardConfig, SubscriptionState,
};
use pm_ws::{
    Attachment, ControlRequest, ControlResponse, DaemonStatus, DoorbellLocation,
    MAX_CONTROL_LINE_BYTES, MAX_TRANSFERRED_DESCRIPTORS, MarketRef, MarketRow,
    NativeIdentifierKind, NativeMarketKey, SegmentReader, SegmentRegion, ShardReport, Venue,
    recv_with_fds,
};
use serde::Deserialize;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;
use support::controlled_peer::{ControlledPeer, PeerConfig};
use tokio::time::Instant;

const MARKET_A: &str = "btc-up-or-down-5-min-1788172500";
const MARKET_B: &str = "eth-up-or-down-5-min-1788172500";
const MARKET_C: &str = "sol-up-or-down-5-min-1788172500";
const STEP_TIMEOUT: Duration = Duration::from_secs(20);
const POLL_INTERVAL: Duration = Duration::from_millis(25);
const PATIENT_HEARTBEAT_MS: u64 = 60_000;
/// Longer than the daemon's default 500 ms command floor, so a command that was going to be
/// emitted has certainly been emitted by the time a negative assertion gives up waiting for it.
const NO_COMMAND_WINDOW: Duration = Duration::from_millis(900);
/// The configured command floor, less the scheduling slack between the instant a command is
/// authorized and the instant its bytes reach the peer.
const PACING_FLOOR: Duration = Duration::from_millis(450);

const EXIT_REJECTED: i32 = 3;
/// A market count above one status page, so a full answer has to be paged.
const PAGED_MARKETS: usize = 512;
/// A loopback address nothing listens on, so a daemon under test never reaches a venue.
const UNROUTABLE_ENDPOINT: &str = "ws://127.0.0.1:1/socket.io/?EIO=4&transport=websocket";

/// The lock file a daemon holds beside its control socket.
fn lock_path(socket: &Path) -> PathBuf {
    let mut name = socket.as_os_str().to_owned();
    name.push(".lock");
    PathBuf::from(name)
}

static NEXT_PATH: AtomicU32 = AtomicU32::new(0);

fn patient_peer() -> PeerConfig {
    PeerConfig {
        ping_interval_ms: PATIENT_HEARTBEAT_MS,
        ping_timeout_ms: PATIENT_HEARTBEAT_MS,
        ..PeerConfig::default()
    }
}

/// A short absolute path under `/tmp`. Short because a Unix domain socket address carries
/// about a hundred bytes, and a scratch directory path alone can exceed that.
fn temp_path(extension: &str) -> PathBuf {
    let pid = std::process::id();
    let sequence = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
    PathBuf::from(format!("/tmp/pmwsd-t{pid}-{sequence}.{extension}"))
}

fn write_config(endpoint: &str, socket: &Path, markets: &[&str]) -> PathBuf {
    write_config_with(endpoint, socket, markets, "")
}

/// The delivery geometry every test here runs under unless it declares its own.
///
/// Small on purpose: a segment's region is zero-filled at creation, so a test daemon under
/// the shipped `common` profile would write tens of mebibytes per shard before it served its
/// first command. What these tests prove — routing, the control protocol, exit codes — is
/// independent of the geometry, and the profiles' own arithmetic is checked in `daemon.rs`.
/// One test deliberately runs under the shipped defaults instead.
const TEST_DELIVERY: &str = "level_capacity = 64\n[delivery]\nsegment_slots = 128\nevent_capacity = 16\ndirty_capacity = 16\n";

fn write_config_with(endpoint: &str, socket: &Path, markets: &[&str], extra: &str) -> PathBuf {
    let path = temp_path("toml");
    let listed = markets
        .iter()
        .map(|slug| format!("{slug:?}"))
        .collect::<Vec<_>>()
        .join(", ");
    let delivery = if extra.contains("[delivery]") {
        ""
    } else {
        TEST_DELIVERY
    };
    let document = format!(
        "control_socket = {:?}\nendpoint = {endpoint:?}\nmarkets = [{listed}]\n{extra}{delivery}",
        socket.display().to_string()
    );
    std::fs::write(&path, document).expect("the test config is written");
    path
}

/// The delivery geometry a test needing `markets` markets on one shard declares.
fn wide_delivery(markets: usize) -> String {
    let slots = markets.next_power_of_two();
    format!(
        "level_capacity = 64\nmarkets_per_shard = {markets}\n[delivery]\nsegment_slots = {slots}\nevent_capacity = 16\ndirty_capacity = 16\n"
    )
}

/// A `pmwsd` child process that is always cleaned up.
///
/// A failing assertion unwinds through this, so the drop kills the daemon and unlinks its
/// socket: a leaked daemon would hold the path and make every later run fail on "another
/// daemon is listening".
struct Daemon {
    child: Option<Child>,
    socket: PathBuf,
    config: PathBuf,
}

impl Daemon {
    async fn start(config: PathBuf, socket: PathBuf) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_pmwsd"))
            .arg("--config")
            .arg(&config)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("pmwsd starts");
        let daemon = Self {
            child: Some(child),
            socket,
            config,
        };
        daemon.await_socket().await;
        daemon
    }

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

    fn pid(&self) -> u32 {
        self.child.as_ref().expect("the daemon is running").id()
    }

    /// Kills the daemon outright, leaving whatever it had on disk exactly as a crash would.
    async fn abandon(mut self) {
        let mut child = self.child.take().expect("the daemon is running");
        let _killed = child.kill();
        let _reaped = tokio::task::spawn_blocking(move || child.wait())
            .await
            .expect("the wait completes");
    }

    /// Sends SIGTERM and waits for the exit status, which is what proves the shutdown path
    /// rather than the kill path.
    async fn terminate(mut self) -> i32 {
        let pid = self.pid();
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
            let _removed = std::fs::remove_file(&self.socket);
        }
        let _removed = std::fs::remove_file(&self.config);
        let _removed = std::fs::remove_file(lock_path(self.socket.as_path()));
    }
}

/// One `pmwsctl` invocation: its exit code and its standard output.
struct Answer {
    code: i32,
    stdout: String,
}

async fn control(socket: &Path, arguments: &[&str]) -> Answer {
    let socket = socket.to_path_buf();
    let arguments: Vec<String> = arguments.iter().map(|value| (*value).to_owned()).collect();
    tokio::task::spawn_blocking(move || {
        let output = Command::new(env!("CARGO_BIN_EXE_pmwsctl"))
            .arg("--socket")
            .arg(&socket)
            .args(&arguments)
            .output()
            .expect("pmwsctl runs");
        Answer {
            code: output.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        }
    })
    .await
    .expect("the pmwsctl invocation completes")
}

/// What `pmwsctl status` prints: the daemon's own report plus the resident memory this tool
/// measured for it.
#[derive(Debug, Deserialize)]
struct StatusOutput {
    pid: u32,
    rss_kib: Option<u64>,
    answers_abandoned: u64,
    attachments_refused: u64,
    shards: Vec<ShardReport>,
    markets: Vec<MarketRow>,
}

impl StatusOutput {
    fn market(&self, slug: &str) -> Option<&pm_ws::limitless::shard::MarketReport> {
        self.markets
            .iter()
            .map(|row| &row.market)
            .find(|report| report.slug == slug)
    }
}

/// Waits until a daemon on `socket` answers a command, which is what proves it is serving
/// rather than merely that a socket file exists.
async fn await_serving(socket: &Path) {
    let deadline = Instant::now() + STEP_TIMEOUT;
    loop {
        if control(socket, &["status"]).await.code == 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "no daemon answered on {} within {STEP_TIMEOUT:?}",
            socket.display()
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn status(socket: &Path) -> StatusOutput {
    let answered = control(socket, &["status"]).await;
    assert_eq!(answered.code, 0, "status succeeds: {}", answered.stdout);
    serde_json::from_str(&answered.stdout).expect("status prints the documented JSON")
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

fn outcomes(answer: &Answer) -> Vec<MarketOutcome> {
    serde_json::from_str(&answer.stdout).expect("a command prints one outcome per market")
}

fn owned(slugs: &[&str]) -> Vec<String> {
    slugs.iter().map(|slug| (*slug).to_owned()).collect()
}

#[tokio::test]
async fn the_daemon_serves_its_configured_set_and_an_operator_grows_and_shrinks_it() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let socket = temp_path("sock");
    let config = write_config(
        peer.endpoint().as_str(),
        socket.as_path(),
        &[MARKET_A, MARKET_B],
    );
    let daemon = Daemon::start(config, socket.clone()).await;

    let mut connection = peer.next_connection().await;
    assert_eq!(
        connection.complete_handshake().await.slugs,
        owned(&[MARKET_A, MARKET_B]),
        "the configured set is subscribed in one command"
    );
    connection
        .send_orderbook(MARKET_A, &[("0.51", "10")], &[("0.52", "11")], Some(1))
        .await;
    connection
        .send_orderbook(MARKET_B, &[("0.31", "20")], &[("0.32", "21")], Some(2))
        .await;

    let live = await_status(socket.as_path(), "both configured markets live", |seen| {
        [MARKET_A, MARKET_B].iter().all(|slug| {
            seen.market(slug)
                .is_some_and(|report| report.status == MarketStatus::Live)
        })
    })
    .await;
    assert_eq!(
        live.pid,
        daemon.pid(),
        "status names the daemon it came from"
    );
    assert!(
        live.rss_kib.is_some_and(|kib| kib > 0),
        "status carries resident memory for that pid"
    );
    assert_eq!(live.shards.len(), 1);
    assert!(
        live.shards[0].connected,
        "connection plus subscription evidence"
    );
    assert_eq!(live.shards[0].desired, 2);
    assert!(
        live.shards[0].queue_age.samples > 0,
        "queue age is sampled once book events flow"
    );
    assert!(live.shards[0].queue_age.p99_micros <= live.shards[0].queue_age.max_micros);
    assert_eq!(
        live.market(MARKET_A).map(|report| report.subscription),
        Some(SubscriptionState::Established)
    );

    let added = control(socket.as_path(), &["add", MARKET_C]).await;
    assert_eq!(added.code, 0, "{}", added.stdout);
    assert_eq!(outcomes(&added)[0].status, MarketStatus::Accepted);
    let mut grown_connection = peer.next_connection().await;
    assert_eq!(
        grown_connection.complete_handshake().await.slugs,
        owned(&[MARKET_A, MARKET_B, MARKET_C]),
        "a runtime add dials the new set whole on a fresh connection"
    );
    for (slug, bid) in [(MARKET_A, "0.51"), (MARKET_B, "0.31"), (MARKET_C, "0.71")] {
        grown_connection
            .send_orderbook(slug, &[(bid, "30")], &[("0.92", "31")], Some(3))
            .await;
    }
    let grown = await_status(
        socket.as_path(),
        "every market live on the new set",
        |seen| {
            [MARKET_A, MARKET_B, MARKET_C].iter().all(|slug| {
                seen.market(slug)
                    .is_some_and(|report| report.status == MarketStatus::Live)
            })
        },
    )
    .await;
    assert_eq!(grown.shards[0].desired, 3);

    let repeated = control(socket.as_path(), &["add", MARKET_C]).await;
    assert_eq!(repeated.code, 0, "{}", repeated.stdout);
    assert_eq!(
        outcomes(&repeated)[0].status,
        MarketStatus::Live,
        "a repeated add answers the market's current state"
    );
    peer.expect_no_connection(NO_COMMAND_WINDOW).await;

    let removed = control(socket.as_path(), &["remove", MARKET_B]).await;
    assert_eq!(removed.code, 0, "{}", removed.stdout);
    assert_eq!(outcomes(&removed)[0].status, MarketStatus::Removed);
    let mut shrunk_connection = peer.next_connection().await;
    assert_eq!(
        shrunk_connection.complete_handshake().await.slugs,
        owned(&[MARKET_A, MARKET_C]),
        "a runtime remove dials what is left"
    );
    shrunk_connection
        .send_orderbook(MARKET_A, &[("0.51", "40")], &[("0.52", "41")], Some(4))
        .await;
    let shrunk = await_status(socket.as_path(), "the removed market gone", |seen| {
        seen.market(MARKET_B).is_none()
            && seen
                .market(MARKET_A)
                .is_some_and(|report| report.status == MarketStatus::Live)
    })
    .await;
    assert_eq!(shrunk.shards[0].desired, 2);
    assert_eq!(
        shrunk.answers_abandoned, 0,
        "no control client left an answer unread"
    );

    let rejected = control(socket.as_path(), &["add", ""]).await;
    assert_eq!(
        rejected.code, EXIT_REJECTED,
        "a slug that is not an identifier exits nonzero: {}",
        rejected.stdout
    );

    let code = daemon.terminate().await;
    assert_eq!(code, 0, "a signalled daemon shuts down cleanly");
    assert!(
        !socket.exists(),
        "shutdown removes the control socket it created"
    );
}

/// A shard's own market capacity, small enough that a two-shard daemon is one market per
/// shard and every command has to be routed.
const SINGLE_MARKET_SHARDS: &str = "markets_per_shard = 1\n";

#[tokio::test]
async fn a_batch_naming_one_market_twice_gives_it_one_book_on_one_shard() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let socket = temp_path("sock");
    let config = write_config_with(
        peer.endpoint().as_str(),
        socket.as_path(),
        &[MARKET_A, MARKET_B],
        SINGLE_MARKET_SHARDS,
    );
    let daemon = Daemon::start(config, socket.clone()).await;

    let mut one = peer.next_connection().await;
    let one_slugs = one.complete_handshake().await.slugs;
    let mut other = peer.next_connection().await;
    let other_slugs = other.complete_handshake().await.slugs;
    let (_first, mut second) = if one_slugs == owned(&[MARKET_A]) {
        assert_eq!(other_slugs, owned(&[MARKET_B]));
        (one, other)
    } else {
        assert_eq!(
            one_slugs,
            owned(&[MARKET_B]),
            "the partition gives each shard exactly its own market; arrival order is scheduling"
        );
        assert_eq!(other_slugs, owned(&[MARKET_A]));
        (other, one)
    };

    let removed = control(socket.as_path(), &["remove", MARKET_B]).await;
    assert_eq!(removed.code, 0, "{}", removed.stdout);
    assert!(second.read_text_frame(STEP_TIMEOUT).await.is_none());

    let doubled = control(socket.as_path(), &["add", MARKET_C, MARKET_C, ""]).await;
    assert_eq!(
        doubled.code, EXIT_REJECTED,
        "the invalid identifier in the batch is rejected: {}",
        doubled.stdout
    );
    let answered = outcomes(&doubled);
    assert_eq!(answered.len(), 3);
    assert_eq!(answered[0].slug, MARKET_C);
    assert_eq!(answered[0].status, MarketStatus::Accepted);
    assert_eq!(
        answered[1].status,
        MarketStatus::Accepted,
        "a market named twice earns one answer, repeated"
    );
    assert_eq!(
        answered[2].status,
        MarketStatus::Rejected(MarketRejection::InvalidIdentifier),
        "an identifier no shard accepts consumes no room"
    );

    let mut rejoined = peer.next_connection().await;
    assert_eq!(
        rejoined.complete_handshake().await.slugs,
        owned(&[MARKET_C]),
        "the market named twice is subscribed on exactly one shard, once"
    );
    let settled = await_status(socket.as_path(), "the added market present", |seen| {
        seen.market(MARKET_C).is_some()
    })
    .await;
    assert_eq!(
        settled.markets.len(),
        2,
        "one book for the market named twice, not two"
    );
    assert_eq!(settled.shards[0].desired, 1);
    assert_eq!(settled.shards[1].desired, 1);

    let code = daemon.terminate().await;
    assert_eq!(code, 0);
}

#[tokio::test]
async fn a_status_answer_pages_a_full_shard_without_truncating_it() {
    let socket = temp_path("sock");
    let slugs: Vec<String> = (0..PAGED_MARKETS)
        .map(|index| format!("paged-market-{index:04}"))
        .collect();
    let listed: Vec<&str> = slugs.iter().map(String::as_str).collect();
    let config = write_config_with(
        UNROUTABLE_ENDPOINT,
        socket.as_path(),
        &listed,
        wide_delivery(PAGED_MARKETS).as_str(),
    );
    let daemon = Daemon::start(config, socket.clone()).await;

    let seen = status(socket.as_path()).await;
    assert_eq!(
        seen.markets.len(),
        PAGED_MARKETS,
        "every market is reported, however many pages that takes"
    );
    assert_eq!(seen.shards.len(), 1);
    assert!(seen.rss_kib.is_some_and(|kib| kib > 0));
    assert_eq!(
        seen.market(slugs[0].as_str()).map(|row| row.revision),
        Some(0)
    );
    assert!(seen.market(slugs[PAGED_MARKETS - 1].as_str()).is_some());

    let code = daemon.terminate().await;
    assert_eq!(code, 0);
}

#[tokio::test]
async fn a_client_that_never_reads_its_answer_does_not_wedge_the_control_task() {
    let socket = temp_path("sock");
    let config = write_config_with(UNROUTABLE_ENDPOINT, socket.as_path(), &[MARKET_A], "");
    let daemon = Daemon::start(config, socket.clone()).await;

    let mut silent = std::os::unix::net::UnixStream::connect(socket.as_path())
        .expect("a control client connects");
    silent
        .write_all(b"{\"command\":\"status\"}\n")
        .expect("the request is sent");
    silent.flush().expect("the request is flushed");

    let answered = status(socket.as_path()).await;
    assert_eq!(answered.markets.len(), 1, "the control task still serves");

    let oversize = {
        let mut stream = std::os::unix::net::UnixStream::connect(socket.as_path())
            .expect("a control client connects");
        let mut line = vec![b'x'; 70_000];
        line.push(b'\n');
        let _written = stream.write_all(&line);
        let _flushed = stream.flush();
        let mut reply = String::new();
        let _read = BufReader::new(stream).read_line(&mut reply);
        reply
    };
    assert!(
        oversize.contains("exceeds") || oversize.is_empty(),
        "an over-long request line is refused typed, got {oversize:?}"
    );

    drop(silent);
    let code = daemon.terminate().await;
    assert_eq!(code, 0, "a signalled daemon still shuts down cleanly");
    assert!(!socket.exists());
}

#[tokio::test]
async fn a_second_daemon_refuses_to_start_while_the_first_holds_the_control_lock() {
    let socket = temp_path("sock");
    let config = write_config_with(UNROUTABLE_ENDPOINT, socket.as_path(), &[MARKET_A], "");
    let daemon = Daemon::start(config.clone(), socket.clone()).await;

    let refused = Command::new(env!("CARGO_BIN_EXE_pmwsd"))
        .arg("--config")
        .arg(&config)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("pmwsd runs");
    assert!(
        !refused.success(),
        "a second daemon must not take a socket the first is serving"
    );
    let still = status(socket.as_path()).await;
    assert_eq!(still.pid, daemon.pid(), "the first daemon still owns it");

    let code = daemon.terminate().await;
    assert_eq!(code, 0);
    assert!(
        !socket.exists(),
        "the daemon that created the socket is the one that removes it"
    );
    let _removed = std::fs::remove_file(lock_path(socket.as_path()));
}

#[tokio::test]
async fn a_daemon_replaces_a_socket_its_predecessor_abandoned() {
    let socket = temp_path("sock");
    let config = write_config_with(UNROUTABLE_ENDPOINT, socket.as_path(), &[MARKET_A], "");
    let abandoned = Daemon::start(config, socket.clone()).await;
    await_serving(socket.as_path()).await;
    abandoned.abandon().await;
    assert!(socket.exists(), "the killed daemon left its socket behind");

    let replacement = write_config_with(UNROUTABLE_ENDPOINT, socket.as_path(), &[MARKET_A], "");
    let successor = Daemon::start(replacement, socket.clone()).await;
    await_serving(socket.as_path()).await;
    let serving = status(socket.as_path()).await;
    assert_eq!(
        serving.pid,
        successor.pid(),
        "the successor serves the socket its predecessor abandoned"
    );
    let code = successor.terminate().await;
    assert_eq!(code, 0);
    assert!(!socket.exists());
    let _removed = std::fs::remove_file(lock_path(socket.as_path()));
}

#[tokio::test]
async fn an_unreachable_daemon_is_a_typed_nonzero_answer() {
    let socket = temp_path("sock");
    let answered = control(socket.as_path(), &["status"]).await;
    assert_ne!(answered.code, 0);
    assert!(answered.stdout.is_empty());
}

#[tokio::test]
async fn a_control_socket_path_that_is_not_a_socket_is_refused() {
    let socket = temp_path("sock");
    std::fs::write(&socket, b"not a socket").expect("the blocking file is written");
    let config = write_config(
        "ws://127.0.0.1:1/socket.io/?EIO=4&transport=websocket",
        socket.as_path(),
        &[MARKET_A],
    );
    let status = Command::new(env!("CARGO_BIN_EXE_pmwsd"))
        .arg("--config")
        .arg(&config)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("pmwsd runs");
    assert!(
        !status.success(),
        "a non-socket path is refused, not unlinked"
    );
    assert!(
        socket.exists(),
        "the daemon must never unlink a path it cannot prove is a dead socket"
    );
    let _removed = std::fs::remove_file(&socket);
    let _removed = std::fs::remove_file(&config);
}

#[tokio::test]
async fn a_configuration_the_daemon_refuses_never_starts_it() {
    let socket = temp_path("sock");
    let config = temp_path("toml");
    std::fs::write(
        &config,
        format!(
            "control_socket = {:?}\nmarket = [\"typo\"]\n",
            socket.display().to_string()
        ),
    )
    .expect("the test config is written");
    let status = Command::new(env!("CARGO_BIN_EXE_pmwsd"))
        .arg("--config")
        .arg(&config)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("pmwsd runs");
    assert_eq!(status.code(), Some(2), "a misconfiguration exits as usage");
    assert!(!socket.exists(), "nothing was bound");
    let _removed = std::fs::remove_file(&config);
}

/// Two shards behind one daemon: routing, per-shard batching, and the daemon's own view of
/// how much room each shard has left.
///
/// `markets_per_shard = 1` is the smallest configuration that produces two shards, so every
/// command in this test has to be routed rather than broadcast, and one command naming a
/// market on each shard is what proves the answers come back attached to the slugs that were
/// asked about.
#[tokio::test]
async fn a_two_shard_daemon_routes_each_market_to_the_shard_that_holds_it() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let socket = temp_path("sock");
    let config = write_config_with(
        peer.endpoint().as_str(),
        socket.as_path(),
        &[MARKET_A, MARKET_B],
        "markets_per_shard = 1\n",
    );
    let daemon = Daemon::start(config, socket.clone()).await;

    let mut one = peer.next_connection().await;
    let one_slugs = one.complete_handshake().await.slugs;
    let mut other = peer.next_connection().await;
    let other_slugs = other.complete_handshake().await.slugs;
    let (mut first, mut second) = if one_slugs == owned(&[MARKET_A]) {
        assert_eq!(
            other_slugs,
            owned(&[MARKET_B]),
            "the partition gives each shard exactly its own market"
        );
        (one, other)
    } else {
        assert_eq!(
            one_slugs,
            owned(&[MARKET_B]),
            "the partition gives each shard exactly its own market; arrival order is scheduling"
        );
        assert_eq!(other_slugs, owned(&[MARKET_A]));
        (other, one)
    };
    first
        .send_orderbook(MARKET_A, &[("0.51", "10")], &[("0.52", "11")], Some(1))
        .await;
    second
        .send_orderbook(MARKET_B, &[("0.31", "20")], &[("0.32", "21")], Some(2))
        .await;
    let spread = await_status(socket.as_path(), "both shards live", |seen| {
        seen.shards.len() == 2
            && [MARKET_A, MARKET_B].iter().all(|slug| {
                seen.market(slug)
                    .is_some_and(|report| report.status == MarketStatus::Live)
            })
    })
    .await;
    assert_eq!(spread.shards[0].desired, 1);
    assert_eq!(spread.shards[1].desired, 1);

    let full = control(socket.as_path(), &["add", MARKET_C]).await;
    assert_eq!(
        full.code, EXIT_REJECTED,
        "no shard has room, and a daemon never opens a connection its configuration did not authorize"
    );
    assert_eq!(
        outcomes(&full)[0].status,
        MarketStatus::Rejected(MarketRejection::CapacityExceeded)
    );

    let removed = control(socket.as_path(), &["remove", MARKET_B]).await;
    assert_eq!(removed.code, 0, "{}", removed.stdout);
    assert_eq!(outcomes(&removed)[0].status, MarketStatus::Removed);
    assert!(
        second.read_text_frame(STEP_TIMEOUT).await.is_none(),
        "a shard with nothing left subscribed gives up its socket"
    );

    let mixed = control(socket.as_path(), &["add", MARKET_C, MARKET_A]).await;
    assert_eq!(mixed.code, 0, "{}", mixed.stdout);
    let answered = outcomes(&mixed);
    assert_eq!(
        answered[0].slug, MARKET_C,
        "answers come back in request order across shards"
    );
    assert_eq!(answered[0].status, MarketStatus::Accepted);
    assert_eq!(answered[1].slug, MARKET_A);
    assert_eq!(
        answered[1].status,
        MarketStatus::Live,
        "the market the other shard already holds answers its own current state"
    );

    let mut rejoined = peer.next_connection().await;
    assert_eq!(
        rejoined.complete_handshake().await.slugs,
        owned(&[MARKET_C]),
        "the freed shard dials again carrying only what it was given"
    );
    rejoined
        .send_orderbook(MARKET_C, &[("0.71", "30")], &[("0.72", "31")], Some(3))
        .await;
    let rebalanced = await_status(socket.as_path(), "the re-filled shard live", |seen| {
        seen.market(MARKET_C)
            .is_some_and(|report| report.status == MarketStatus::Live)
    })
    .await;
    assert_eq!(rebalanced.shards[0].desired, 1);
    assert_eq!(rebalanced.shards[1].desired, 1);
    assert!(
        rebalanced.market(MARKET_B).is_none(),
        "the removed market's book is gone with its shard's connection"
    );

    let refused = control(socket.as_path(), &["add", "another-market"]).await;
    assert_eq!(
        refused.code, EXIT_REJECTED,
        "the daemon's room is back where it started, so a third market is refused again"
    );

    let code = daemon.terminate().await;
    assert_eq!(code, 0);
}

/// Two shards on one venue endpoint, which is the daemon's own configuration: one aggregate
/// command floor, honored across both, measured at the wire.
///
/// Both connections are accepted and both handshakes are driven to the same point before
/// either is released, so the two subscriptions are authorized and written as close together
/// as this harness can make them — the convergence a pacer that metered decision points
/// rather than writes would let through. What is timestamped is the arrival of the
/// `subscribe_market_prices` frames themselves, not a TCP accept, so what is proven is the
/// spacing of the bytes the venue sees. The reissues that follow are submitted one after the
/// other, so their order on the wire is fixed and the interval between them is the pacer's
/// rather than the test's.
#[tokio::test]
async fn two_shards_on_one_endpoint_never_command_the_venue_inside_the_configured_floor() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let mut shards = Vec::new();
    let mut handles = Vec::new();
    for markets in [vec![MARKET_A.to_owned()], vec![MARKET_B.to_owned()]] {
        let shard = Shard::new(ShardConfig {
            endpoint: peer.endpoint(),
            markets,
            setup_timeout: Duration::from_secs(10),
            resubscribe_window: Duration::from_secs(30),
            ..ShardConfig::default()
        })
        .expect("the shard configuration is valid");
        handles.push(shard.handle());
        shards.push(shard);
    }
    let stoppers: Vec<_> = shards.iter().map(Shard::stopper).collect();
    let tasks: Vec<_> = shards
        .into_iter()
        .map(|mut shard| {
            tokio::spawn(async move {
                shard
                    .run_until(Some(Instant::now() + STEP_TIMEOUT * 6))
                    .await
            })
        })
        .collect();

    let mut first = peer.next_connection().await;
    let mut second = peer.next_connection().await;
    first.complete_namespace().await;
    second.complete_namespace().await;

    let (opened, first_frame, followed) = {
        let one = first.expect_unacknowledged_resubscription(STEP_TIMEOUT);
        let other = second.expect_unacknowledged_resubscription(STEP_TIMEOUT);
        tokio::pin!(one, other);
        tokio::select! {
            request = &mut one => (request, Instant::now(), other.await),
            request = &mut other => (request, Instant::now(), one.await),
        }
    };
    let establish_gap = Instant::now().saturating_duration_since(first_frame);
    let mut subscribed = [opened.slugs, followed.slugs];
    subscribed.sort();
    assert_eq!(
        subscribed,
        [owned(&[MARKET_A]), owned(&[MARKET_B])],
        "the two shards between them subscribe exactly their own markets; which frame lands first is scheduling"
    );
    assert!(
        establish_gap >= PACING_FLOOR,
        "two establishing subscription frames landed {establish_gap:?} apart on the wire, inside the configured floor"
    );

    handles[0]
        .add(owned(&[MARKET_C]))
        .await
        .expect("the first shard is running");
    let mut third = peer.next_connection().await;
    third.complete_namespace().await;
    let _replacement = third
        .expect_unacknowledged_resubscription(STEP_TIMEOUT)
        .await;
    let first_replacement = Instant::now();
    handles[1]
        .add(owned(&[MARKET_C]))
        .await
        .expect("the second shard is running");
    let mut fourth = peer.next_connection().await;
    fourth.complete_namespace().await;
    let _replacement = fourth
        .expect_unacknowledged_resubscription(STEP_TIMEOUT)
        .await;
    let replacement_gap = Instant::now().saturating_duration_since(first_replacement);
    assert!(
        replacement_gap >= PACING_FLOOR,
        "two shards' replacement subscriptions landed {replacement_gap:?} apart, inside the configured floor"
    );

    for stopper in &stoppers {
        stopper.stop();
    }
    for task in tasks {
        let _stats = task.await.expect("the shard task completes");
    }
}

/// A daemon unlinks the socket it created and never one that replaced it.
///
/// The path is taken over while the daemon is still serving, which is what a successor that
/// found the socket abandoned would do to it, so the shutdown meets a path naming a
/// different inode than the one it bound. Without the identity check that shutdown would
/// unlink a live daemon's socket and leave the operator with a path nothing answers on.
#[tokio::test]
async fn a_daemon_leaves_a_socket_it_did_not_create_in_place() {
    let socket = temp_path("sock");
    let config = write_config_with(UNROUTABLE_ENDPOINT, socket.as_path(), &[MARKET_A], "");
    let daemon = Daemon::start(config, socket.clone()).await;
    await_serving(socket.as_path()).await;
    let bound = std::fs::symlink_metadata(socket.as_path())
        .map(|metadata| metadata.ino())
        .expect("the daemon bound a socket");

    std::fs::remove_file(socket.as_path()).expect("the daemon's socket is unlinked under it");
    let successor = std::os::unix::net::UnixListener::bind(socket.as_path())
        .expect("another listener takes the path");
    let taken = std::fs::symlink_metadata(socket.as_path())
        .map(|metadata| metadata.ino())
        .expect("the replacement socket exists");
    assert_ne!(bound, taken, "the path names a different socket now");

    let code = daemon.terminate().await;
    assert_eq!(code, 0, "the daemon still shuts down cleanly");
    let after = std::fs::symlink_metadata(socket.as_path())
        .map(|metadata| metadata.ino())
        .expect("the replacement socket outlives the daemon that did not create it");
    assert_eq!(
        after, taken,
        "shutdown unlinks the socket this daemon bound and never a successor's"
    );

    drop(successor);
    let _removed = std::fs::remove_file(socket.as_path());
    let _removed = std::fs::remove_file(lock_path(socket.as_path()));
}

/// Sends one raw control line and reads the one line that comes back.
///
/// The protocol rather than the tool, so a test can walk paging by hand and see what a
/// concurrent command does to a walk `pmwsctl` would have finished in one call.
async fn raw_control(socket: &Path, line: String) -> String {
    let socket = socket.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut stream =
            std::os::unix::net::UnixStream::connect(&socket).expect("a control client connects");
        stream
            .set_read_timeout(Some(STEP_TIMEOUT))
            .expect("the read is bounded");
        stream.write_all(line.as_bytes()).expect("the line is sent");
        stream.flush().expect("the line is flushed");
        let mut reply = String::new();
        let _read = BufReader::new(stream).read_line(&mut reply);
        reply
    })
    .await
    .expect("the control exchange completes")
}

/// Connects to `socket` and reads the one line the daemon answers, without ever writing a
/// request.
///
/// Proof for a connection refused at accept time, before the daemon has read anything this
/// client sent: writing a request first would race the daemon's own refusal-and-close against
/// this client's write, which a refused connection may already have closed its read side of.
async fn refused_connection(socket: &Path) -> String {
    let socket = socket.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let stream = std::os::unix::net::UnixStream::connect(&socket)
            .expect("the daemon accepts the connection before refusing it");
        stream
            .set_read_timeout(Some(STEP_TIMEOUT))
            .expect("the read is bounded");
        let mut reply = String::new();
        let _read = BufReader::new(stream).read_line(&mut reply);
        reply
    })
    .await
    .expect("the refusal conversation completes")
}

#[derive(Debug, Deserialize)]
struct StatusPage {
    status: DaemonStatus,
}

/// Paging by key cursor reports every market that was present throughout the walk exactly
/// once, whatever was added or removed between two pages.
///
/// The removal is deliberately of a market on the first page: under an offset cursor that
/// shifts every later row one place forward, so the row that moved into the offset the
/// caller is about to ask for is never reported at all. A key cursor names a market, so the
/// second page resumes from the same place whatever the set did in between.
#[tokio::test]
async fn a_membership_change_between_status_pages_omits_no_market_that_stayed() {
    let socket = temp_path("sock");
    let slugs: Vec<String> = (0..PAGED_MARKETS)
        .map(|index| format!("paged-market-{index:04}"))
        .collect();
    let listed: Vec<&str> = slugs.iter().map(String::as_str).collect();
    let config = write_config_with(
        UNROUTABLE_ENDPOINT,
        socket.as_path(),
        &listed,
        wide_delivery(PAGED_MARKETS).as_str(),
    );
    let daemon = Daemon::start(config, socket.clone()).await;
    await_serving(socket.as_path()).await;

    let mut seen: Vec<String> = Vec::new();
    let mut after: Option<String> = None;
    let mut removed_yet = false;
    loop {
        let request = match after.as_deref() {
            None => "{\"command\":\"status\"}\n".to_owned(),
            Some(cursor) => format!("{{\"command\":\"status\",\"after\":{cursor:?}}}\n"),
        };
        let page: StatusPage =
            serde_json::from_str(raw_control(socket.as_path(), request).await.trim_end())
                .expect("a status page is the documented JSON");
        assert!(
            !page.status.markets.is_empty(),
            "a page that says more follows must carry rows"
        );
        after = page
            .status
            .markets
            .last()
            .map(|row| row.market.slug.clone());
        seen.extend(
            page.status
                .markets
                .iter()
                .map(|row| row.market.slug.clone()),
        );
        if !removed_yet {
            let removed = control(socket.as_path(), &["remove", slugs[0].as_str()]).await;
            assert_eq!(removed.code, 0, "{}", removed.stdout);
            removed_yet = true;
        }
        if !page.status.more {
            break;
        }
    }

    for slug in &slugs[1..] {
        assert_eq!(
            seen.iter().filter(|held| *held == slug).count(),
            1,
            "a market present throughout the walk is reported exactly once, {slug} was not"
        );
    }

    let code = daemon.terminate().await;
    assert_eq!(code, 0);
}

/// A request line one byte past the cap is refused as over-long even though it ends in a
/// newline: the cap counts the newline, so the byte the reader allowed beyond it is already
/// one too many.
///
/// The refusal costs the request and not the connection: the rest of the over-long line is
/// discarded and the next request on the same session is served. That matters now that a
/// session carries market leases — dropping a consumer's markets over one malformed line
/// would answer a framing mistake with a venue unsubscription.
#[tokio::test]
async fn a_request_line_one_byte_past_the_cap_is_refused() {
    let socket = temp_path("sock");
    let config = write_config_with(UNROUTABLE_ENDPOINT, socket.as_path(), &[MARKET_A], "");
    let daemon = Daemon::start(config, socket.clone()).await;
    await_serving(socket.as_path()).await;

    let mut over = "x".repeat(MAX_CONTROL_LINE_BYTES);
    over.push('\n');
    assert_eq!(over.len(), MAX_CONTROL_LINE_BYTES + 1);
    let refused = raw_control(socket.as_path(), over.clone()).await;
    assert!(
        refused.contains("exceeds"),
        "an over-long line is refused as such, got {refused:?}"
    );

    let resumed = {
        let path = socket.clone();
        tokio::task::spawn_blocking(move || {
            let mut stream =
                std::os::unix::net::UnixStream::connect(&path).expect("a control client connects");
            stream
                .set_read_timeout(Some(STEP_TIMEOUT))
                .expect("the read is bounded");
            stream.write_all(over.as_bytes()).expect("the line is sent");
            stream
                .write_all(b"{\"command\":\"status\"}\n")
                .expect("the next request is sent");
            stream.flush().expect("both are flushed");
            let mut reader = BufReader::new(stream);
            let mut first = String::new();
            let _read = reader.read_line(&mut first);
            let mut second = String::new();
            let _read = reader.read_line(&mut second);
            (first, second)
        })
        .await
        .expect("the control exchange completes")
    };
    assert!(
        resumed.0.contains("exceeds"),
        "the over-long line is refused first, got {:?}",
        resumed.0
    );
    assert!(
        resumed.1.contains("\"result\":\"status\""),
        "a framing error costs the request that carried it and not the session: {:?}",
        resumed.1
    );

    let still = status(socket.as_path()).await;
    assert_eq!(still.markets.len(), 1, "the control task still serves");

    let code = daemon.terminate().await;
    assert_eq!(code, 0);
}

/// A batch that names an unusable identifier before a valid one still lands the valid one.
///
/// Planning is per unique validated slug, so the invalid name consumes no provisional room:
/// were it counted, the market behind it on a shard with one place left would be refused for
/// a capacity that was never really spent.
#[tokio::test]
async fn an_invalid_identifier_ahead_of_a_valid_one_costs_it_no_room() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let socket = temp_path("sock");
    let config = write_config_with(
        peer.endpoint().as_str(),
        socket.as_path(),
        &[MARKET_A],
        "markets_per_shard = 2\n",
    );
    let daemon = Daemon::start(config, socket.clone()).await;
    let mut first = peer.next_connection().await;
    assert_eq!(first.complete_handshake().await.slugs, owned(&[MARKET_A]));

    let mixed = control(socket.as_path(), &["add", "", MARKET_B, MARKET_B]).await;
    assert_eq!(
        mixed.code, EXIT_REJECTED,
        "the batch carried an identifier no shard accepts: {}",
        mixed.stdout
    );
    let answered = outcomes(&mixed);
    assert_eq!(answered.len(), 3);
    assert_eq!(
        answered[0].status,
        MarketStatus::Rejected(MarketRejection::InvalidIdentifier)
    );
    assert_eq!(
        answered[1].status,
        MarketStatus::Accepted,
        "the valid market behind it still found the room it needed"
    );
    assert_eq!(answered[2].status, MarketStatus::Accepted);

    let mut second = peer.next_connection().await;
    assert_eq!(
        second.complete_handshake().await.slugs,
        owned(&[MARKET_A, MARKET_B]),
        "the market named twice joins the set once, on the shard that had room"
    );
    let settled = await_status(socket.as_path(), "the added market present", |seen| {
        seen.market(MARKET_B).is_some()
    })
    .await;
    assert_eq!(settled.markets.len(), 2, "one book for the market, not two");

    let code = daemon.terminate().await;
    assert_eq!(code, 0);
}

/// A shard that stops feeding ends the daemon.
///
/// A shard task runs for as long as the daemon does, so its completion is never ordinary: a
/// daemon that went on answering `status` for markets nothing is maintaining would be
/// reporting state that had stopped being true. The socket goes with it, so an operator's
/// next command fails to connect rather than reaching a daemon that cannot serve it.
#[tokio::test]
async fn a_shard_that_stops_feeding_ends_the_daemon_and_takes_the_socket_with_it() {
    let socket = temp_path("sock");
    let config = write_config_with(UNROUTABLE_ENDPOINT, socket.as_path(), &[MARKET_A], "");
    let mut child = Command::new(env!("CARGO_BIN_EXE_pmwsd"))
        .arg("--config")
        .arg(&config)
        .arg("--kill-shard-after")
        .arg("1500")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("pmwsd starts");

    let status = tokio::task::spawn_blocking(move || child.wait().expect("the daemon exits"))
        .await
        .expect("the wait completes");
    assert_eq!(
        status.code(),
        Some(3),
        "a daemon whose shard stopped feeding exits non-zero"
    );
    assert!(
        !socket.exists(),
        "the socket goes with the daemon, so the next command fails to connect"
    );

    let _removed = std::fs::remove_file(&config);
    let _removed = std::fs::remove_file(lock_path(socket.as_path()));
}

/// Every shard owns a segment of its own, named for this daemon instance, and `pmwsctl
/// status` names it.
///
/// The names are what a consumer will be handed, so what is proven here is that they are
/// distinct per shard, carry one instance identity, name files that really exist, and are
/// gone when the daemon is — a segment left behind would be mapped by a later consumer and
/// read as live.
#[tokio::test]
async fn a_two_shard_daemon_creates_one_segment_per_shard_and_status_names_them() {
    let peer = ControlledPeer::start(patient_peer()).await;
    let socket = temp_path("sock");
    let config = write_config_with(
        peer.endpoint().as_str(),
        socket.as_path(),
        &[MARKET_A, MARKET_B],
        "markets_per_shard = 1\n",
    );
    let daemon = Daemon::start(config, socket.clone()).await;
    await_serving(socket.as_path()).await;

    let seen = status(socket.as_path()).await;
    assert_eq!(seen.shards.len(), 2);
    let names: Vec<String> = seen
        .shards
        .iter()
        .map(|shard| {
            shard
                .segment
                .clone()
                .expect("every shard publishes into a segment of its own")
        })
        .collect();
    assert_ne!(names[0], names[1], "one segment per shard, named apart");
    for (index, name) in names.iter().enumerate() {
        assert!(
            name.starts_with("pmws-") && name.ends_with(&format!("-{index}.seg")),
            "a segment name carries the instance identity and the shard it serves: {name}"
        );
        assert!(
            Path::new("/tmp").join(name).exists(),
            "status names a segment that exists: {name}"
        );
        assert_eq!(
            seen.shards[index].segment_markets, 1,
            "each shard installed the one market it holds"
        );
    }
    let instance: Vec<&str> = names.iter().map(|name| &name[5..37]).collect();
    assert_eq!(
        instance[0], instance[1],
        "both segments carry the one instance identity a consumer checks the header against"
    );

    let code = daemon.terminate().await;
    assert_eq!(code, 0);
    for name in &names {
        assert!(
            !Path::new("/tmp").join(name).exists(),
            "the daemon unlinks the segments it created: {name}"
        );
        assert!(
            !Path::new("/tmp").join(format!("{name}.doorbell")).exists(),
            "and the doorbell page beside them: {name}"
        );
    }
}

/// Shutdown unlinks the segment files this daemon created and never a file that took one of
/// their paths, exactly as it treats the control socket.
///
/// Both cases ride one daemon so the assertion is a comparison rather than two separate runs:
/// shard 0's segment is replaced while the daemon is still serving — which is what an operator
/// clearing a directory, or a successor instance, would do to it — and shard 1's is left
/// alone. Without the identity check the shutdown would remove whatever the path resolved to,
/// which for a replaced path is somebody else's file.
#[tokio::test]
async fn a_daemon_leaves_a_segment_file_it_did_not_create_in_place() {
    let peer = ControlledPeer::start(patient_peer()).await;
    let socket = temp_path("sock");
    let config = write_config_with(
        peer.endpoint().as_str(),
        socket.as_path(),
        &[MARKET_A, MARKET_B],
        "markets_per_shard = 1\n",
    );
    let daemon = Daemon::start(config, socket.clone()).await;
    await_serving(socket.as_path()).await;

    let seen = status(socket.as_path()).await;
    let paths: Vec<PathBuf> = seen
        .shards
        .iter()
        .map(|shard| {
            Path::new("/tmp").join(
                shard
                    .segment
                    .clone()
                    .expect("every shard publishes into a segment of its own"),
            )
        })
        .collect();
    let created = std::fs::symlink_metadata(paths[0].as_path())
        .map(|metadata| metadata.ino())
        .expect("the daemon created shard 0's segment");

    std::fs::remove_file(paths[0].as_path()).expect("shard 0's segment is unlinked under it");
    std::fs::write(paths[0].as_path(), b"not this daemon's segment")
        .expect("something else takes the path");
    let taken = std::fs::symlink_metadata(paths[0].as_path())
        .map(|metadata| metadata.ino())
        .expect("the replacement exists");
    assert_ne!(created, taken, "the path names a different file now");

    let code = daemon.terminate().await;
    assert_eq!(code, 0, "the daemon still shuts down cleanly");
    let after = std::fs::symlink_metadata(paths[0].as_path())
        .map(|metadata| metadata.ino())
        .expect("the replacement outlives the daemon that did not create it");
    assert_eq!(
        after, taken,
        "shutdown unlinks the segment this daemon created and never a file that replaced it"
    );
    assert!(
        !paths[1].exists(),
        "the segment still naming what this daemon created is removed: {}",
        paths[1].display()
    );

    let _removed = std::fs::remove_file(paths[0].as_path());
}

/// The shipped defaults are a configuration a daemon actually starts under: one document
/// naming only a socket and a market set produces a segment of exactly the size the common
/// profile's arithmetic says it does.
#[tokio::test]
async fn the_shipped_delivery_defaults_start_a_daemon_and_size_its_segment() {
    let socket = temp_path("sock");
    let path = temp_path("toml");
    std::fs::write(
        &path,
        format!(
            "control_socket = {:?}\nendpoint = {UNROUTABLE_ENDPOINT:?}\nmarkets = [{MARKET_A:?}]\n",
            socket.display().to_string()
        ),
    )
    .expect("the test config is written");
    let daemon = Daemon::start(path, socket.clone()).await;
    await_serving(socket.as_path()).await;

    let seen = status(socket.as_path()).await;
    let name = seen.shards[0]
        .segment
        .clone()
        .expect("the default configuration publishes into a segment");
    let file = Path::new("/tmp").join(&name);
    let bytes = std::fs::metadata(&file).expect("the segment exists").len();
    let expected = pm_ws::daemon::DaemonConfig::parse(&format!(
        "control_socket = {:?}\nmarkets = []\n",
        socket.display().to_string()
    ))
    .expect("the default document is valid")
    .delivery
    .layout
    .region_size();
    assert_eq!(
        bytes as usize, expected,
        "the file is the region the common profile's geometry implies"
    );

    let code = daemon.terminate().await;
    assert_eq!(code, 0);
    assert!(!file.exists());
}

/// One attach conversation over the real control socket, exactly as a consumer's own client
/// makes it: one request line out, one answer line and its descriptors back on one message.
///
/// The descriptors are owned here, so a refused attach that carries none is observable as an
/// empty vector rather than as a leaked open file.
struct Transfer {
    response: ControlResponse,
    descriptors: Vec<OwnedFd>,
}

async fn attach(socket: &Path, market: &str) -> Transfer {
    connect(socket, market).await.transfer
}

/// One consumer's control session: the attach conversation, with the connection held open
/// exactly as a consumer's own client holds it.
///
/// The connection is the lease. Dropping this releases every market this session leased, which
/// is what a consumer exiting — or crashing — does, so a test that wants the release simply
/// drops it.
struct Consumer {
    /// `None` once this session has been disconnected, so a test can close it before the end
    /// of the scope it lives in.
    stream: Option<std::os::unix::net::UnixStream>,
    transfer: Transfer,
}

impl Consumer {
    /// Sends one `renew` and returns what came back, keeping the session open.
    async fn renew(&mut self) -> ControlResponse {
        let mut stream = self.stream.take().expect("the session is open");
        let (stream, response) = tokio::task::spawn_blocking(move || {
            let request =
                pm_ws::encode_line(&ControlRequest::Renew).expect("a renew request encodes");
            stream
                .write_all(request.as_bytes())
                .expect("the request is written");
            let mut line = String::new();
            let mut reader = BufReader::new(
                stream
                    .try_clone()
                    .expect("the session's own descriptor duplicates"),
            );
            let read = reader.read_line(&mut line).expect("the answer arrives");
            assert!(read > 0, "the daemon answered the renewal");
            let response = serde_json::from_str(line.trim_end()).unwrap_or_else(|error| {
                panic!("the answer is a control response: {error}: {line}")
            });
            (stream, response)
        })
        .await
        .expect("the renew conversation completes");
        self.stream = Some(stream);
        response
    }

    /// Sends one `release` for `market` on this session and returns what came back, keeping
    /// the session open.
    async fn release(&mut self, market: &str) -> ControlResponse {
        let mut stream = self.stream.take().expect("the session is open");
        let market = market.to_owned();
        let (stream, response) = tokio::task::spawn_blocking(move || {
            let request = pm_ws::encode_line(&ControlRequest::Release { market })
                .expect("a release request encodes");
            stream
                .write_all(request.as_bytes())
                .expect("the request is written");
            let mut line = String::new();
            let mut reader = BufReader::new(
                stream
                    .try_clone()
                    .expect("the session's own descriptor duplicates"),
            );
            let read = reader.read_line(&mut line).expect("the answer arrives");
            assert!(read > 0, "the daemon answered the release");
            let response = serde_json::from_str(line.trim_end()).unwrap_or_else(|error| {
                panic!("the answer is a control response: {error}: {line}")
            });
            (stream, response)
        })
        .await
        .expect("the release conversation completes");
        self.stream = Some(stream);
        response
    }

    /// Sends one `attach` for another market on this same session and returns what came
    /// back, discarding any descriptor that arrives with it.
    ///
    /// Leases are per session rather than per attach, so this is what puts a second lease on
    /// a session that already holds one: a test that wants a session leasing several markets
    /// at once, so it can release one of them and observe that the others survive, has no
    /// other way to attach past the first market a session opens on.
    async fn attach_more(&mut self, market: &str) -> ControlResponse {
        let mut stream = self.stream.take().expect("the session is open");
        let market = market.to_owned();
        let (stream, response) = tokio::task::spawn_blocking(move || {
            let request = pm_ws::encode_line(&ControlRequest::Attach { market })
                .expect("an attach request encodes");
            stream
                .write_all(request.as_bytes())
                .expect("the request is written");
            let mut buffer = vec![0_u8; MAX_CONTROL_LINE_BYTES];
            let (mut filled, _descriptors) = recv_with_fds(
                stream.as_fd(),
                buffer.as_mut_slice(),
                MAX_TRANSFERRED_DESCRIPTORS,
            )
            .expect("the answer arrives");
            while filled < buffer.len() && !buffer[..filled].contains(&b'\n') {
                match stream.read(&mut buffer[filled..]) {
                    Ok(0) => break,
                    Ok(read) => filled += read,
                    Err(error) => panic!("the answer line did not finish: {error}"),
                }
            }
            let line = std::str::from_utf8(&buffer[..filled])
                .expect("the answer is utf-8")
                .trim_end()
                .to_owned();
            let response = serde_json::from_str(line.as_str()).unwrap_or_else(|error| {
                panic!("the answer is a control response: {error}: {line}")
            });
            (stream, response)
        })
        .await
        .expect("the attach conversation completes");
        self.stream = Some(stream);
        response
    }

    /// Closes the session, releasing every lease it held.
    fn disconnect(&mut self) {
        self.stream = None;
    }

    /// Pipelines `status` requests until the socket will not take another whole one, and
    /// reads none of the answers — a consumer whose process is wedged with its connection
    /// still open, which is the one death a closed socket cannot report.
    ///
    /// The session is left open here on purpose: only the daemon can end it, and whether it
    /// does is the contract under test. The burst is self-limiting — it stops when the
    /// daemon has stopped reading, which is what happens once the daemon is stalled writing
    /// an answer this client will never take — and capped besides, so it is bounded whatever
    /// the daemon does. A short write ends it rather than sending half a request line.
    async fn pipeline_unread(&mut self) -> usize {
        let stream = self.stream.take().expect("the session is open");
        let (stream, sent) = tokio::task::spawn_blocking(move || {
            stream
                .set_nonblocking(true)
                .expect("the burst never blocks on the daemon");
            let request = b"{\"command\":\"status\"}\n";
            let mut stream = stream;
            let mut sent = 0;
            for _ in 0..MAX_PIPELINED_REQUESTS {
                match stream.write(request) {
                    Ok(written) if written == request.len() => sent += 1,
                    Ok(_) | Err(_) => break,
                }
            }
            stream
                .set_nonblocking(false)
                .expect("the session goes back to blocking reads");
            (stream, sent)
        })
        .await
        .expect("the burst completes");
        self.stream = Some(stream);
        sent
    }
}

/// Opens one control session and attaches it to `market`, leaving the session open.
async fn connect(socket: &Path, market: &str) -> Consumer {
    let socket = socket.to_path_buf();
    let market = market.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut stream =
            std::os::unix::net::UnixStream::connect(&socket).expect("the daemon accepts");
        stream
            .set_read_timeout(Some(STEP_TIMEOUT))
            .expect("the read is bounded");
        let request = pm_ws::encode_line(&ControlRequest::Attach { market })
            .expect("an attach request encodes");
        stream
            .write_all(request.as_bytes())
            .expect("the request is written");
        let mut buffer = vec![0_u8; MAX_CONTROL_LINE_BYTES];
        let (mut filled, descriptors) = recv_with_fds(
            stream.as_fd(),
            buffer.as_mut_slice(),
            MAX_TRANSFERRED_DESCRIPTORS,
        )
        .expect("the answer arrives");
        while filled < buffer.len() && !buffer[..filled].contains(&b'\n') {
            match stream.read(&mut buffer[filled..]) {
                Ok(0) => break,
                Ok(read) => filled += read,
                Err(error) => panic!("the answer line did not finish: {error}"),
            }
        }
        let line = std::str::from_utf8(&buffer[..filled])
            .expect("the answer is utf-8")
            .trim_end()
            .to_owned();
        let response = serde_json::from_str(line.as_str())
            .unwrap_or_else(|error| panic!("the answer is a control response: {error}: {line}"));
        Consumer {
            stream: Some(stream),
            transfer: Transfer {
                response,
                descriptors,
            },
        }
    })
    .await
    .expect("the attach conversation completes")
}

/// The answer's own promise, checked against the descriptors that arrived with it.
///
/// One descriptor, whatever the doorbell placement says: the sibling page is a writable-length
/// object and its descriptor is a truncation capability the daemon keeps to itself, so the
/// placement decides what a consumer may do with the segment and never what rides the message.
fn promised(attached: &Transfer) -> &Attachment {
    let ControlResponse::Attached { attachment } = &attached.response else {
        panic!("the daemon attached this market: {:?}", attached.response);
    };
    assert_eq!(
        attached.descriptors.len(),
        usize::from(attachment.descriptors),
        "the transfer carries exactly the descriptors the answer promised"
    );
    assert_eq!(
        attachment.descriptors, 1,
        "an accepted attach transfers the segment's descriptor and nothing else, under either \
         doorbell placement"
    );
    attachment
}

/// A reader over a received transfer, validated the way a consumer must validate one:
/// read-only mapping, header and trailer first, then the identity the answer promised.
fn reader_from(attached: Transfer, promise: &Attachment) -> SegmentReader {
    let mut descriptors = attached.descriptors;
    let segment = descriptors.remove(0);
    let region = SegmentRegion::open_read_only_from_fd(segment)
        .expect("the transferred descriptor maps read-only");
    let reader = SegmentReader::attach_with_doorbell(Arc::new(region), None)
        .expect("the transferred segment validates");
    let expected = u128::from_str_radix(promise.instance_id.as_str(), 16)
        .expect("the promised instance identity is hexadecimal");
    assert_eq!(
        reader.geometry().daemon_instance_id(),
        expected,
        "the header declares the daemon instance the answer promised"
    );
    assert_eq!(
        reader.geometry().segment_generation(),
        promise.segment_generation,
        "the header declares the generation the answer promised"
    );
    reader
}

fn market_ref(slug: &str) -> MarketRef {
    MarketRef::new(
        Venue::new("limitless").expect("the venue name is valid"),
        NativeMarketKey::new(NativeIdentifierKind::slug(), slug).expect("the slug is valid"),
    )
}

/// The best bid and ask a received segment reports for `slug`, as `price@quantity` text.
fn bbo_through(reader: &SegmentReader, slug: &str) -> (String, String) {
    let handle = reader
        .resolve(&market_ref(slug))
        .expect("the segment's directory carries this market");
    let snapshot = reader.read(handle).expect("the state slot reads");
    let text = |level: Option<&pm_ws::Level>| {
        level.map_or_else(
            || "-".to_owned(),
            |level| format!("{}@{}", level.price().value(), level.quantity().value()),
        )
    };
    (text(snapshot.best_bid()), text(snapshot.best_ask()))
}

/// Waits for one market's best bid and ask to reach `expected`, so a test never races the
/// writer's own publication of a frame the venue has only just sent.
async fn await_bbo(reader: &SegmentReader, slug: &str, expected: (&str, &str)) -> bool {
    let deadline = Instant::now() + STEP_TIMEOUT;
    let expected = (expected.0.to_owned(), expected.1.to_owned());
    while Instant::now() < deadline {
        if bbo_through(reader, slug) == expected {
            return true;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    false
}

/// A consumer attaches by descriptor transfer, reads the market it asked for through the
/// descriptor it was handed, and never names a segment path.
///
/// The refusal rides the same test because it shares one daemon: an identifier no shard would
/// take is answered in the protocol's existing per-market vocabulary and carries no descriptor
/// at all. A well-formed slug this daemon does not hold is no longer a refusal — it is a lease
/// that creates the demand, which
/// [`an_attach_leases_an_unheld_market_and_its_disconnect_releases_it`] proves. `status` then
/// shows both sides — two transfers on the shard, one refusal daemon-wide — which is what
/// makes an accepted attach and a refused one observable to an operator rather than only to
/// the caller.
#[tokio::test]
async fn a_consumer_attaches_by_descriptor_transfer_and_reads_the_market_it_asked_for() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let socket = temp_path("sock");
    let config = write_config(
        peer.endpoint().as_str(),
        socket.as_path(),
        &[MARKET_A, MARKET_B],
    );
    let daemon = Daemon::start(config, socket.clone()).await;
    let mut connection = peer.next_connection().await;
    assert_eq!(
        connection.complete_handshake().await.slugs,
        owned(&[MARKET_A, MARKET_B])
    );
    connection
        .send_orderbook(MARKET_A, &[("0.51", "10")], &[("0.52", "11")], Some(1))
        .await;
    connection
        .send_orderbook(MARKET_B, &[("0.31", "20")], &[("0.32", "21")], Some(2))
        .await;
    let _live = await_status(socket.as_path(), "both markets live", |seen| {
        [MARKET_A, MARKET_B].iter().all(|slug| {
            seen.market(slug)
                .is_some_and(|report| report.status == MarketStatus::Live)
        })
    })
    .await;

    let first = attach(socket.as_path(), MARKET_A).await;
    let promise = promised(&first).clone();
    assert_eq!(promise.shard, 0);
    let named = status(socket.as_path()).await;
    assert_eq!(
        named.shards[0].segment.as_deref(),
        Some(promise.segment.as_str()),
        "the answer names the same segment `status` does"
    );
    let reader = reader_from(first, &promise);
    assert_eq!(
        bbo_through(&reader, MARKET_A),
        ("0.51@10".to_owned(), "0.52@11".to_owned()),
        "the descriptor carries this daemon's live book for the market that was asked for"
    );

    let parked = reader.clone();
    let generation = parked.publication_generation();
    let woken = tokio::task::spawn_blocking(move || {
        parked.wait_for_publication(generation, Duration::ZERO, Some(STEP_TIMEOUT))
    });
    connection
        .send_orderbook(MARKET_A, &[("0.53", "14")], &[("0.54", "15")], Some(3))
        .await;
    let outcome = woken.await.expect("the parked wait completes");
    match promise.doorbell {
        DoorbellLocation::InHeader => assert!(
            matches!(outcome, Ok(pm_ws::WaitOutcome::Changed(_))),
            "a header doorbell rides the transferred read-only mapping, so a transferred \
             attachment parks on it and is woken by the daemon's next publication: {outcome:?}"
        ),
        DoorbellLocation::Page => assert!(
            matches!(outcome, Err(pm_ws::WaitFault::DoorbellUnavailable(_))),
            "a page doorbell is never transferred — its descriptor would let a consumer \
             truncate a file the daemon stores through — so a transferred attachment answers \
             the typed unavailable fault rather than parking: {outcome:?}"
        ),
    }
    let waited = await_bbo(&reader, MARKET_A, ("0.53@14", "0.54@15")).await;
    assert!(
        waited,
        "the consumer reads the publication either way: a park that cannot happen never costs \
         it the segment"
    );

    let second = attach(socket.as_path(), MARKET_B).await;
    let promise_b = promised(&second).clone();
    assert_eq!(
        promise_b.shard, promise.shard,
        "both markets live on the one shard, so both attach to its segment"
    );
    let reader_b = reader_from(second, &promise_b);
    assert_eq!(
        bbo_through(&reader_b, MARKET_B),
        ("0.31@20".to_owned(), "0.32@21".to_owned()),
        "a second attach to the same shard is served independently"
    );
    assert_eq!(
        bbo_through(&reader_b, MARKET_A),
        ("0.53@14".to_owned(), "0.54@15".to_owned()),
        "one attachment covers every market in that shard's segment, at its latest state"
    );

    let invalid = attach(socket.as_path(), "").await;
    assert!(
        matches!(
            &invalid.response,
            ControlResponse::Markets { markets }
                if markets[0].status
                    == MarketStatus::Rejected(MarketRejection::InvalidIdentifier)
        ),
        "an unusable identifier is refused by the same predicate every command uses: {:?}",
        invalid.response
    );
    assert!(
        invalid.descriptors.is_empty(),
        "a refused attach transfers nothing"
    );

    let counted = status(socket.as_path()).await;
    assert_eq!(
        counted.shards[0].attachments, 2,
        "both transfers are counted against the segment that served them"
    );
    assert_eq!(
        counted.attachments_refused, 1,
        "the refusal is counted daemon-wide, where a refused market belongs to no shard"
    );
    assert_eq!(counted.answers_abandoned, 0);

    drop(reader);
    drop(reader_b);
    let code = daemon.terminate().await;
    assert_eq!(code, 0);
}

/// A stand-in transfer, for taking the real one out of a session that stays open. Carries no
/// descriptor, so nothing is closed and nothing is leaked by leaving it behind.
fn discarded() -> Transfer {
    Transfer {
        response: ControlResponse::Busy {
            message: String::new(),
        },
        descriptors: Vec::new(),
    }
}

/// What `status` says about one market's demand: whether an operator pinned it, and how many
/// sessions lease it.
fn demand(status: &StatusOutput, slug: &str) -> Option<(bool, u32)> {
    status
        .markets
        .iter()
        .find(|row| row.market.slug == slug)
        .map(|row| (row.pinned, row.leases))
}

/// A consumer's attach creates the venue demand, its disconnect ends it, and a second attach
/// brings the market back.
///
/// The whole lease lifecycle on one market nothing else wants: the attach subscribes it at
/// the venue, the consumer reads the book that arrives through the descriptor it was handed,
/// closing the connection unsubscribes it, and attaching again resubscribes it and delivers a
/// fresh book on the delivery entry it was retired from. Every step is read off the wire the
/// peer sees — the subscription payloads name the set before and after — rather than from a
/// daemon-side counter, and `status` shows the demand that produced them.
///
/// The last step is the one lease churn depends on: demand that comes and goes and comes back
/// is the ordinary case for consumers, and a market that could only ever be added once per
/// segment generation would serve the first consumer and refuse every later one.
#[tokio::test]
async fn an_attach_leases_an_unheld_market_and_its_disconnect_releases_it() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let socket = temp_path("sock");
    let config = write_config(peer.endpoint().as_str(), socket.as_path(), &[MARKET_A]);
    let daemon = Daemon::start(config, socket.clone()).await;

    let mut connection = peer.next_connection().await;
    assert_eq!(
        connection.complete_handshake().await.slugs,
        owned(&[MARKET_A]),
        "the configured set is subscribed first"
    );
    connection
        .send_orderbook(MARKET_A, &[("0.51", "10")], &[("0.52", "11")], Some(1))
        .await;
    await_status(socket.as_path(), "the configured market live", |seen| {
        seen.market(MARKET_A)
            .is_some_and(|report| report.status == MarketStatus::Live)
    })
    .await;

    let mut consumer = connect(socket.as_path(), MARKET_B).await;
    let promise = promised(&consumer.transfer).clone();
    let mut leased = peer.next_connection().await;
    assert_eq!(
        leased.complete_handshake().await.slugs,
        owned(&[MARKET_A, MARKET_B]),
        "a lease on a market nothing held subscribes it at the venue"
    );
    leased
        .send_orderbook(MARKET_B, &[("0.31", "20")], &[("0.32", "21")], Some(2))
        .await;
    let held = await_status(socket.as_path(), "the leased market live", |seen| {
        seen.market(MARKET_B)
            .is_some_and(|report| report.status == MarketStatus::Live)
    })
    .await;
    assert_eq!(
        demand(&held, MARKET_B),
        Some((false, 1)),
        "the leased market is held by one session and by no operator"
    );
    assert_eq!(
        demand(&held, MARKET_A),
        Some((true, 0)),
        "the configured market is the operator's, leased by nobody"
    );

    let reader = reader_from(
        std::mem::replace(&mut consumer.transfer, discarded()),
        &promise,
    );
    assert!(
        await_bbo(&reader, MARKET_B, ("0.31@20", "0.32@21")).await,
        "the consumer reads the market its own lease brought into the daemon"
    );

    consumer.disconnect();
    let mut released = peer.next_connection().await;
    assert_eq!(
        released.complete_handshake().await.slugs,
        owned(&[MARKET_A]),
        "the last lease going takes the market off the venue"
    );
    let gone = await_status(socket.as_path(), "the leased market gone", |seen| {
        seen.market(MARKET_B).is_none()
    })
    .await;
    assert_eq!(demand(&gone, MARKET_A), Some((true, 0)));
    assert_eq!(gone.answers_abandoned, 0);

    let mut again = connect(socket.as_path(), MARKET_B).await;
    let resumed = promised(&again.transfer).clone();
    assert_eq!(
        resumed.shard, promise.shard,
        "the market comes back on the shard, and the segment, it left"
    );
    let mut relet = peer.next_connection().await;
    assert_eq!(
        relet.complete_handshake().await.slugs,
        owned(&[MARKET_A, MARKET_B]),
        "demand that comes back subscribes the market again"
    );
    relet
        .send_orderbook(MARKET_B, &[("0.41", "30")], &[("0.42", "31")], Some(3))
        .await;
    let reader_again = reader_from(
        std::mem::replace(&mut again.transfer, discarded()),
        &resumed,
    );
    assert!(
        await_bbo(&reader_again, MARKET_B, ("0.41@30", "0.42@31")).await,
        "a market whose lease came back is live again on the entry it was retired from"
    );

    again.disconnect();
    drop(reader);
    drop(reader_again);
    let code = daemon.terminate().await;
    assert_eq!(code, 0);
}

/// Operator pins and consumer leases hold the same market independently, and each survives
/// the other's death.
///
/// Both halves of "operator pins dominate" in one daemon, because they are one rule seen from
/// two sides: a pinned market does not leave when its last lease dies, and an operator's
/// `remove` takes away only the operator's own ownership — the market stays while a consumer
/// still leases it, and the operator is told the state it is actually in rather than that it
/// was removed. The venue sees a subscription change only when aggregate demand reaches zero.
#[tokio::test]
async fn an_operator_pin_and_a_consumer_lease_hold_a_market_independently() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let socket = temp_path("sock");
    let config = write_config(peer.endpoint().as_str(), socket.as_path(), &[MARKET_A]);
    let daemon = Daemon::start(config, socket.clone()).await;

    let mut connection = peer.next_connection().await;
    assert_eq!(
        connection.complete_handshake().await.slugs,
        owned(&[MARKET_A])
    );

    let added = control(socket.as_path(), &["add", MARKET_B]).await;
    assert_eq!(added.code, 0, "{}", added.stdout);
    assert_eq!(outcomes(&added)[0].status, MarketStatus::Accepted);
    let mut pinned = peer.next_connection().await;
    assert_eq!(
        pinned.complete_handshake().await.slugs,
        owned(&[MARKET_A, MARKET_B])
    );
    pinned
        .send_orderbook(MARKET_B, &[("0.31", "20")], &[("0.32", "21")], Some(1))
        .await;
    await_status(socket.as_path(), "the pinned market live", |seen| {
        seen.market(MARKET_B)
            .is_some_and(|report| report.status == MarketStatus::Live)
    })
    .await;

    let mut consumer = connect(socket.as_path(), MARKET_B).await;
    let _promise = promised(&consumer.transfer);
    let both = await_status(socket.as_path(), "the pinned market leased too", |seen| {
        demand(seen, MARKET_B) == Some((true, 1))
    })
    .await;
    assert_eq!(both.shards[0].desired, 2, "one market, one subscription");
    peer.expect_no_connection(NO_COMMAND_WINDOW).await;

    consumer.disconnect();
    let unleased = await_status(socket.as_path(), "the lease gone", |seen| {
        demand(seen, MARKET_B) == Some((true, 0))
    })
    .await;
    assert_eq!(
        unleased
            .market(MARKET_B)
            .map(|report| report.status.clone()),
        Some(MarketStatus::Live),
        "an operator's pin outlives every lease on it"
    );
    peer.expect_no_connection(NO_COMMAND_WINDOW).await;

    let mut second = connect(socket.as_path(), MARKET_B).await;
    let _promise = promised(&second.transfer);
    await_status(socket.as_path(), "the pinned market leased again", |seen| {
        demand(seen, MARKET_B) == Some((true, 1))
    })
    .await;

    let unpinned = control(socket.as_path(), &["remove", MARKET_B]).await;
    assert_eq!(unpinned.code, 0, "{}", unpinned.stdout);
    assert_eq!(
        outcomes(&unpinned)[0].status,
        MarketStatus::Live,
        "removing the pin on a market consumers still lease answers what it is, never `removed`"
    );
    let leased_only = await_status(socket.as_path(), "the pin gone", |seen| {
        demand(seen, MARKET_B) == Some((false, 1))
    })
    .await;
    assert_eq!(
        leased_only
            .market(MARKET_B)
            .map(|report| report.status.clone()),
        Some(MarketStatus::Live),
        "a lease outlives the operator's own ownership of the market"
    );
    peer.expect_no_connection(NO_COMMAND_WINDOW).await;

    second.disconnect();
    let mut released = peer.next_connection().await;
    assert_eq!(
        released.complete_handshake().await.slugs,
        owned(&[MARKET_A]),
        "with the pin gone, the last lease's death is what unsubscribes the market"
    );
    await_status(socket.as_path(), "the market gone", |seen| {
        seen.market(MARKET_B).is_none()
    })
    .await;

    let code = daemon.terminate().await;
    assert_eq!(code, 0);
}

/// A session leasing several markets releases one without disturbing the others or itself:
/// the released market's demand ends at the venue, the kept market's demand and the
/// session's own liveness (a later `renew` still answers for it) survive, and the session's
/// eventual close still releases whatever it has left.
#[tokio::test]
async fn a_session_release_drops_one_lease_and_keeps_its_others_and_the_session_alive() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let socket = temp_path("sock");
    let config = write_config(peer.endpoint().as_str(), socket.as_path(), &[MARKET_A]);
    let daemon = Daemon::start(config, socket.clone()).await;

    let mut connection = peer.next_connection().await;
    assert_eq!(
        connection.complete_handshake().await.slugs,
        owned(&[MARKET_A])
    );

    let mut consumer = connect(socket.as_path(), MARKET_B).await;
    let mut leased_b = peer.next_connection().await;
    assert_eq!(
        leased_b.complete_handshake().await.slugs,
        owned(&[MARKET_A, MARKET_B]),
        "the session's first lease subscribes the market it names"
    );

    let attached_c = consumer.attach_more(MARKET_C).await;
    assert!(
        matches!(&attached_c, ControlResponse::Attached { .. }),
        "a second market on the same session is a second lease: {attached_c:?}"
    );
    let mut leased_c = peer.next_connection().await;
    assert_eq!(
        leased_c.complete_handshake().await.slugs,
        owned(&[MARKET_A, MARKET_B, MARKET_C]),
        "the session's second lease subscribes it too"
    );

    let before = await_status(socket.as_path(), "both leases held", |seen| {
        demand(seen, MARKET_B) == Some((false, 1)) && demand(seen, MARKET_C) == Some((false, 1))
    })
    .await;
    assert_eq!(demand(&before, MARKET_A), Some((true, 0)));

    let released = consumer.release(MARKET_B).await;
    assert_eq!(
        released,
        ControlResponse::Released { leases: 1 },
        "one lease released off two leaves the session holding one"
    );

    let mut unsubscribed = peer.next_connection().await;
    assert_eq!(
        unsubscribed.complete_handshake().await.slugs,
        owned(&[MARKET_A, MARKET_C]),
        "the released market alone leaves the venue's subscription"
    );
    let after = await_status(socket.as_path(), "only the released lease gone", |seen| {
        seen.market(MARKET_B).is_none()
    })
    .await;
    assert_eq!(
        demand(&after, MARKET_C),
        Some((false, 1)),
        "the kept lease is untouched by the release of the other"
    );
    assert_eq!(demand(&after, MARKET_A), Some((true, 0)));

    let renewed = consumer.renew().await;
    assert_eq!(
        renewed,
        ControlResponse::Renewed { leases: 1 },
        "the session is still alive and still answers for the lease it kept"
    );

    consumer.disconnect();
    let mut closed = peer.next_connection().await;
    assert_eq!(
        closed.complete_handshake().await.slugs,
        owned(&[MARKET_A]),
        "closing the session still releases whatever lease it had left"
    );
    await_status(socket.as_path(), "the kept market gone too", |seen| {
        seen.market(MARKET_C).is_none()
    })
    .await;

    let code = daemon.terminate().await;
    assert_eq!(code, 0);
}

/// Releasing a lease on a market an operator has also pinned drops only the lease: the
/// market stays exactly as live as it was, and the venue sees no traffic from the release.
#[tokio::test]
async fn a_release_of_a_pinned_and_leased_market_drops_only_the_lease() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let socket = temp_path("sock");
    let config = write_config(peer.endpoint().as_str(), socket.as_path(), &[MARKET_A]);
    let daemon = Daemon::start(config, socket.clone()).await;

    let mut connection = peer.next_connection().await;
    assert_eq!(
        connection.complete_handshake().await.slugs,
        owned(&[MARKET_A])
    );

    let added = control(socket.as_path(), &["add", MARKET_B]).await;
    assert_eq!(added.code, 0, "{}", added.stdout);
    let mut pinned = peer.next_connection().await;
    assert_eq!(
        pinned.complete_handshake().await.slugs,
        owned(&[MARKET_A, MARKET_B])
    );
    pinned
        .send_orderbook(MARKET_B, &[("0.31", "20")], &[("0.32", "21")], Some(1))
        .await;
    await_status(socket.as_path(), "the pinned market live", |seen| {
        seen.market(MARKET_B)
            .is_some_and(|report| report.status == MarketStatus::Live)
    })
    .await;

    let mut consumer = connect(socket.as_path(), MARKET_B).await;
    let _promise = promised(&consumer.transfer);
    await_status(socket.as_path(), "the pinned market leased too", |seen| {
        demand(seen, MARKET_B) == Some((true, 1))
    })
    .await;
    peer.expect_no_connection(NO_COMMAND_WINDOW).await;

    let released = consumer.release(MARKET_B).await;
    assert_eq!(
        released,
        ControlResponse::Released { leases: 0 },
        "the session held one lease on the pinned market and now holds none"
    );
    peer.expect_no_connection(NO_COMMAND_WINDOW).await;

    let after = await_status(
        socket.as_path(),
        "the lease gone, the pin holding",
        |seen| demand(seen, MARKET_B) == Some((true, 0)),
    )
    .await;
    assert_eq!(
        after.market(MARKET_B).map(|report| report.status.clone()),
        Some(MarketStatus::Live),
        "a release never reports removed while an operator's pin holds the market"
    );

    consumer.disconnect();
    let code = daemon.terminate().await;
    assert_eq!(code, 0);
}

/// A release is idempotent — repeating it, or releasing a market this session never held —
/// answers the session's current lease count rather than an error, and never touches a lease
/// another session holds on the same market.
#[tokio::test]
async fn a_release_is_idempotent_and_never_disturbs_another_sessions_lease() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let socket = temp_path("sock");
    let config = write_config(peer.endpoint().as_str(), socket.as_path(), &[MARKET_A]);
    let daemon = Daemon::start(config, socket.clone()).await;

    let mut connection = peer.next_connection().await;
    assert_eq!(
        connection.complete_handshake().await.slugs,
        owned(&[MARKET_A])
    );

    let mut first = connect(socket.as_path(), MARKET_B).await;
    let mut leased = peer.next_connection().await;
    assert_eq!(
        leased.complete_handshake().await.slugs,
        owned(&[MARKET_A, MARKET_B])
    );

    let mut second = connect(socket.as_path(), MARKET_B).await;
    let _promise = promised(&second.transfer);
    peer.expect_no_connection(NO_COMMAND_WINDOW).await;

    let both = await_status(socket.as_path(), "two sessions lease the market", |seen| {
        demand(seen, MARKET_B) == Some((false, 2))
    })
    .await;
    assert_eq!(demand(&both, MARKET_A), Some((true, 0)));

    let first_release = first.release(MARKET_B).await;
    assert_eq!(
        first_release,
        ControlResponse::Released { leases: 0 },
        "the first session's own lease is the one it released"
    );
    peer.expect_no_connection(NO_COMMAND_WINDOW).await;
    await_status(socket.as_path(), "one of two leases gone", |seen| {
        demand(seen, MARKET_B) == Some((false, 1))
    })
    .await;

    let repeated = first.release(MARKET_B).await;
    assert_eq!(
        repeated,
        ControlResponse::Released { leases: 0 },
        "releasing a market this session no longer holds is idempotent, not an error"
    );
    peer.expect_no_connection(NO_COMMAND_WINDOW).await;

    let unheld = first.release(MARKET_C).await;
    assert_eq!(
        unheld,
        ControlResponse::Released { leases: 0 },
        "releasing a market this session never leased is idempotent too"
    );
    peer.expect_no_connection(NO_COMMAND_WINDOW).await;

    let still_held = await_status(
        socket.as_path(),
        "the other session's lease untouched",
        |seen| demand(seen, MARKET_B) == Some((false, 1)),
    )
    .await;
    assert_eq!(
        still_held.market(MARKET_C),
        None,
        "a release of a market never held creates no demand record for it"
    );

    let second_renewed = second.renew().await;
    assert_eq!(
        second_renewed,
        ControlResponse::Renewed { leases: 1 },
        "the second session's own lease was never touched by the first session's releases"
    );

    first.disconnect();
    peer.expect_no_connection(NO_COMMAND_WINDOW).await;
    second.disconnect();
    let mut gone = peer.next_connection().await;
    assert_eq!(
        gone.complete_handshake().await.slugs,
        owned(&[MARKET_A]),
        "the last lease's own session closing is what finally unsubscribes the market"
    );

    let code = daemon.terminate().await;
    assert_eq!(code, 0);
}

/// A release naming an identifier no shard would accept is refused exactly as an attach
/// refuses one, and is never counted as an attach refusal or applied as a release of
/// anything.
#[tokio::test]
async fn a_release_of_an_invalid_market_identifier_is_refused_like_an_attach() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let socket = temp_path("sock");
    let config = write_config(peer.endpoint().as_str(), socket.as_path(), &[MARKET_A]);
    let daemon = Daemon::start(config, socket.clone()).await;

    let mut connection = peer.next_connection().await;
    assert_eq!(
        connection.complete_handshake().await.slugs,
        owned(&[MARKET_A])
    );

    let mut consumer = connect(socket.as_path(), MARKET_B).await;
    let mut leased = peer.next_connection().await;
    leased.complete_handshake().await;

    let refused = consumer.release("").await;
    assert!(
        matches!(
            &refused,
            ControlResponse::Markets { markets }
                if markets[0].status == MarketStatus::Rejected(MarketRejection::InvalidIdentifier)
        ),
        "an unusable identifier is refused by the same predicate an attach uses: {refused:?}"
    );

    let after = await_status(socket.as_path(), "the real lease untouched", |seen| {
        demand(seen, MARKET_B) == Some((false, 1))
    })
    .await;
    assert_eq!(
        after.attachments_refused, 0,
        "a release refusal is never counted as an attach refusal"
    );

    consumer.disconnect();
    let code = daemon.terminate().await;
    assert_eq!(code, 0);
}

/// A daemon configured with `max_control_sessions = 1` accepts one control session and
/// refuses the next with the same typed `busy` answer every session past the cap earns, now
/// at an operator-declared cap rather than the compiled-in default.
#[tokio::test]
async fn a_daemon_configured_with_one_control_session_refuses_a_second() {
    let socket = temp_path("sock");
    let config = write_config_with(
        UNROUTABLE_ENDPOINT,
        socket.as_path(),
        &[MARKET_A],
        "max_control_sessions = 1\n",
    );
    let daemon = Daemon::start(config, socket.clone()).await;
    await_serving(socket.as_path()).await;

    let mut consumer = connect(socket.as_path(), MARKET_B).await;
    let _promise = promised(&consumer.transfer);

    let refusal = refused_connection(socket.as_path()).await;
    let response: ControlResponse = serde_json::from_str(refusal.trim_end())
        .unwrap_or_else(|error| panic!("the refusal is a control response: {error}: {refusal}"));
    assert!(
        matches!(response, ControlResponse::Busy { .. }),
        "a connection past the configured cap is refused rather than queued: {response:?}"
    );

    consumer.disconnect();
    let code = daemon.terminate().await;
    assert_eq!(code, 0);
}

/// A lease TTL, configured in milliseconds, so a whole expiry fits inside a test step.
const LEASE_TTL_MS: u64 = 1_000;
/// How often the surviving session renews: well inside [`LEASE_TTL_MS`], so a renewal that is
/// late by a scheduling hiccup still lands in time.
const RENEW_INTERVAL: Duration = Duration::from_millis(200);
/// Long enough for a silent session to pass [`LEASE_TTL_MS`] with room to spare.
const TTL_WINDOW: Duration = Duration::from_millis(3_000);

/// Under a configured lease TTL a silent session loses its leases and a renewing one keeps
/// them.
///
/// The TTL covers the one failure a closed socket cannot report: a peer whose connection is
/// open and whose process is wedged. Both sessions here are open the whole time and neither
/// closes, so what separates them is only that one keeps saying so. The renewing session's
/// market must still be there at the end — an expiry that took both would satisfy "the silent
/// one expired" while being useless.
///
/// What the expiry is read from is the desired set rather than the market list: this peer
/// accepts no further connection, so the venue never reconciles the removal and the shard
/// rightly keeps the book until it does. The market being out of the desired set is the whole
/// of what a released lease does.
#[tokio::test]
async fn a_silent_session_expires_under_the_lease_ttl_and_a_renewing_one_survives() {
    let mut peer = ControlledPeer::start(patient_peer()).await;
    let socket = temp_path("sock");
    let config = write_config_with(
        peer.endpoint().as_str(),
        socket.as_path(),
        &[MARKET_A],
        format!("lease_ttl_ms = {LEASE_TTL_MS}\n").as_str(),
    );
    let daemon = Daemon::start(config, socket.clone()).await;

    let mut connection = peer.next_connection().await;
    assert_eq!(
        connection.complete_handshake().await.slugs,
        owned(&[MARKET_A])
    );

    let mut silent = connect(socket.as_path(), MARKET_B).await;
    let _promise = promised(&silent.transfer);
    let mut renewing = connect(socket.as_path(), MARKET_C).await;
    let _promise = promised(&renewing.transfer);
    await_status(socket.as_path(), "both leases held", |seen| {
        demand(seen, MARKET_B) == Some((false, 1)) && demand(seen, MARKET_C) == Some((false, 1))
    })
    .await;

    let deadline = Instant::now() + TTL_WINDOW;
    let mut renewals = 0u32;
    let expired = loop {
        let answered = renewing.renew().await;
        assert!(
            matches!(answered, ControlResponse::Renewed { leases } if leases == 1),
            "a renewal answers how many leases the session holds: {answered:?}"
        );
        renewals += 1;
        let seen = status(socket.as_path()).await;
        let released = seen
            .market(MARKET_B)
            .is_none_or(|report| report.status == MarketStatus::Removed);
        if released {
            break seen;
        }
        assert!(
            Instant::now() < deadline,
            "the silent session never expired inside {TTL_WINDOW:?}; last saw {seen:?}"
        );
        tokio::time::sleep(RENEW_INTERVAL).await;
    };
    assert!(
        renewals > 1,
        "the renewing session outlived at least one TTL's worth of renewals"
    );
    assert_eq!(
        demand(&expired, MARKET_C),
        Some((false, 1)),
        "the renewing session keeps every lease it renewed"
    );
    assert_eq!(
        expired.shards[0].desired, 2,
        "the expired lease left the desired set and the renewed one did not"
    );
    assert_eq!(
        demand(&expired, MARKET_A),
        Some((true, 0)),
        "an operator's pin is not a lease and no TTL touches it"
    );

    silent.disconnect();
    renewing.disconnect();
    let code = daemon.terminate().await;
    assert_eq!(code, 0);
}

/// How long a second session's one command may wait behind a session that never stops
/// sending. Generous next to the work behind one `status`: what it bounds is the control
/// task's fairness, never the daemon's speed.
const FAIR_TURN_BOUND: Duration = Duration::from_secs(3);

/// One `status` request line: the answer a saturating session asks for when all it has to be
/// is busy.
const STATUS_REQUEST: &str = "{\"command\":\"status\"}\n";

/// How long a pipelining client that never reads its answers may hold the control task.
///
/// One of `pmwsd`'s own `CONTROL_WRITE_TIMEOUT` — five seconds — plus room for the daemon to
/// notice and for a `status` to be answered around it. The number is what the contract costs
/// rather than slack: a daemon that closes such a session spends that bound once, and a
/// daemon that leaves it open spends it again for every request already queued behind the
/// first, which no bound this test could name would survive.
const WEDGED_CLIENT_BOUND: Duration = Duration::from_secs(12);

/// The most requests one wedged-client burst offers the daemon. A cap rather than a target:
/// the burst stops as soon as the socket will not take another whole request, which is what
/// happens once the daemon has stopped reading its side.
const MAX_PIPELINED_REQUESTS: usize = 50_000;

/// A control client that pipelines one request line as fast as the daemon will read them and
/// reads every answer it gets.
///
/// It exists to hold the control task busy for the whole of an assertion, which is the only
/// state in which the scheduler's fairness is observable at all. It reads its answers on
/// purpose: a client that did not would be testing the write-timeout path instead, and what
/// is under test here is only which session the daemon picks next. The line is the caller's
/// because what a saturating session should *ask* differs: an answer that is only work, or
/// one the daemon also counts where another session can see the count.
struct ChattySession {
    stop: Arc<AtomicBool>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl ChattySession {
    /// Connects and starts sending `request`, which must be one complete control line ending
    /// in a newline. Both threads carry socket timeouts, so neither can outlive the daemon it
    /// is talking to however that daemon ends.
    fn start(socket: &Path, request: &'static str) -> Self {
        let stream = std::os::unix::net::UnixStream::connect(socket).expect("the daemon accepts");
        stream
            .set_write_timeout(Some(STEP_TIMEOUT))
            .expect("the writes are bounded");
        stream
            .set_read_timeout(Some(POLL_INTERVAL))
            .expect("the reads are bounded");
        let mut reading = stream
            .try_clone()
            .expect("the session's own descriptor duplicates");
        let mut writing = stream;
        let stop = Arc::new(AtomicBool::new(false));
        let writer_stop = Arc::clone(&stop);
        let reader_stop = Arc::clone(&stop);
        let writer = std::thread::spawn(move || {
            while !writer_stop.load(Ordering::Relaxed) {
                if writing.write_all(request.as_bytes()).is_err() {
                    return;
                }
            }
        });
        let reader = std::thread::spawn(move || {
            let mut sink = [0_u8; 4096];
            while !reader_stop.load(Ordering::Relaxed) {
                match reading.read(&mut sink) {
                    Ok(0) => return,
                    Ok(_) => {}
                    Err(error)
                        if error.kind() == std::io::ErrorKind::WouldBlock
                            || error.kind() == std::io::ErrorKind::TimedOut => {}
                    Err(_) => return,
                }
            }
        });
        Self {
            stop,
            threads: vec![writer, reader],
        }
    }

    async fn stop(self) {
        self.stop.store(true, Ordering::Relaxed);
        for thread in self.threads {
            let _joined = tokio::task::spawn_blocking(move || thread.join())
                .await
                .expect("the join completes");
        }
    }
}

/// Sends one raw control line on a connection of its own and reads the answer, giving up
/// after `bound` rather than waiting on a daemon that never gets to it. `None` is that
/// deadline passing, which is what starvation looks like from a client.
async fn raw_request_within(socket: &Path, line: &str, bound: Duration) -> Option<String> {
    let socket = socket.to_path_buf();
    let line = line.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut stream =
            std::os::unix::net::UnixStream::connect(&socket).expect("a control client connects");
        stream
            .set_read_timeout(Some(bound))
            .expect("the read is bounded");
        stream.write_all(line.as_bytes()).expect("the line is sent");
        stream.flush().expect("the line is flushed");
        let mut reply = String::new();
        match BufReader::new(stream).read_line(&mut reply) {
            Ok(read) if read > 0 => Some(reply),
            _ => None,
        }
    })
    .await
    .expect("the control exchange completes")
}

/// A session that never stops sending does not starve another session's one command.
///
/// The control task serves one request at a time, so fairness is entirely a question of which
/// session it picks next and how long it stays there. The chatty session connects first — the
/// lower index, which is what a lowest-index-first scan favours — and reads every answer, so
/// nothing it does is a write stall: what it does is stay readable forever. Three rounds,
/// because the property under test is that every turn comes around, not that one lucky
/// command got through.
#[tokio::test]
async fn a_session_that_never_stops_sending_does_not_starve_another_session() {
    let socket = temp_path("sock");
    let config = write_config_with(UNROUTABLE_ENDPOINT, socket.as_path(), &[MARKET_A], "");
    let daemon = Daemon::start(config, socket.clone()).await;
    await_serving(socket.as_path()).await;

    let chatty = ChattySession::start(socket.as_path(), STATUS_REQUEST);
    tokio::time::sleep(POLL_INTERVAL * 8).await;

    for round in 0..3 {
        let answered = raw_request_within(
            socket.as_path(),
            "{\"command\":\"status\"}\n",
            FAIR_TURN_BOUND,
        )
        .await
        .unwrap_or_else(|| {
            panic!("round {round}: no answer inside {FAIR_TURN_BOUND:?} behind a chatty session")
        });
        let page: StatusPage = serde_json::from_str(answered.trim_end())
            .unwrap_or_else(|error| panic!("round {round}: {error}: {answered}"));
        assert!(
            page.status
                .markets
                .iter()
                .any(|row| row.market.slug == MARKET_A),
            "round {round}: the answer is this daemon's own status: {answered}"
        );
    }

    chatty.stop().await;
    let code = daemon.terminate().await;
    assert_eq!(code, 0);
}

/// A client that pipelines requests and reads none of the answers loses its session and every
/// lease on it, and the daemon goes on serving everything else.
///
/// The one failure a bounded write alone does not answer. The bound stops one answer; what
/// ends the conversation is the session going with it, because a peer that is not reading has
/// nothing more to be told and its next queued request would only spend the same bound again.
/// The lease is the observable: this consumer's market is held by nothing else, so its
/// leaving the daemon's set is the release itself, seen from the venue-facing side.
#[tokio::test]
async fn a_pipelining_client_that_never_reads_loses_its_session_and_its_leases() {
    let socket = temp_path("sock");
    let config = write_config_with(UNROUTABLE_ENDPOINT, socket.as_path(), &[MARKET_A], "");
    let daemon = Daemon::start(config, socket.clone()).await;
    await_serving(socket.as_path()).await;

    let mut wedged = connect(socket.as_path(), MARKET_B).await;
    let _promise = promised(&wedged.transfer);
    await_status(
        socket.as_path(),
        "the wedged session's lease held",
        |seen| demand(seen, MARKET_B) == Some((false, 1)),
    )
    .await;

    let sent = wedged.pipeline_unread().await;
    assert!(
        sent > 1,
        "the burst queued more than one request behind the answer the daemon cannot write"
    );

    let started = Instant::now();
    let released = await_status(
        socket.as_path(),
        "the wedged session's lease released",
        |seen| demand(seen, MARKET_B).is_none(),
    )
    .await;
    let waited = started.elapsed();
    assert!(
        waited < WEDGED_CLIENT_BOUND,
        "the wedged session held its lease for {waited:?}, past {WEDGED_CLIENT_BOUND:?}"
    );
    assert!(
        released.answers_abandoned >= 1,
        "the daemon counted the answer it gave up writing: {released:?}"
    );
    assert_eq!(
        demand(&released, MARKET_A),
        Some((true, 0)),
        "the operator's own market is untouched by another session's death"
    );

    wedged.disconnect();
    let code = daemon.terminate().await;
    assert_eq!(code, 0);
}

/// A consumer's closed socket releases its leases promptly even while another session is
/// saturating the control task.
///
/// A closed connection is readable — that is how the daemon learns of it — so a scan that
/// never reaches it is a lease held by a process that has exited. The chatty session connects
/// first here for the same reason as above: it is the session a lowest-index-first scan would
/// serve forever.
#[tokio::test]
async fn a_closed_consumer_releases_its_leases_while_another_session_stays_busy() {
    let socket = temp_path("sock");
    let config = write_config_with(UNROUTABLE_ENDPOINT, socket.as_path(), &[MARKET_A], "");
    let daemon = Daemon::start(config, socket.clone()).await;
    await_serving(socket.as_path()).await;

    let chatty = ChattySession::start(socket.as_path(), STATUS_REQUEST);
    tokio::time::sleep(POLL_INTERVAL * 8).await;

    let mut consumer = connect(socket.as_path(), MARKET_B).await;
    let _promise = promised(&consumer.transfer);
    await_status(socket.as_path(), "the consumer's lease held", |seen| {
        demand(seen, MARKET_B) == Some((false, 1))
    })
    .await;

    consumer.disconnect();
    let started = Instant::now();
    let _released = await_status(
        socket.as_path(),
        "the closed consumer's lease released",
        |seen| demand(seen, MARKET_B).is_none(),
    )
    .await;
    let waited = started.elapsed();
    assert!(
        waited < FAIR_TURN_BOUND,
        "a closed consumer's leases took {waited:?} to release behind a busy session"
    );

    chatty.stop().await;
    let code = daemon.terminate().await;
    assert_eq!(code, 0);
}

/// `pmwsd`'s own `CONTROL_REQUESTS_PER_DISPATCH`: how many answers one session is given
/// before the scan moves on.
const REQUESTS_PER_DISPATCH: u64 = 4;

/// How many sessions saturate the control task beside the measured one. Two, so the rotation
/// under test has the three ready sessions a two-session test cannot tell apart from one.
const SATURATING_SESSIONS: usize = 2;

/// How many answers the measured session reads. Ten turns' worth at
/// [`REQUESTS_PER_DISPATCH`], so the gaps between turns are sampled repeatedly rather than
/// once.
const MEASURED_ANSWERS: usize = 40;

/// An attach the daemon refuses without reaching a shard, and counts. The empty slug is not a
/// venue-native identifier, so the answer is a per-market rejection and one more
/// `attachments_refused`.
const REFUSED_ATTACH: &str = "{\"command\":\"attach\",\"market\":\"\"}\n";

/// What separates two turns of one session is the *other* sessions' whole budgets, not one
/// budget.
///
/// Three sessions, all continuously ready: two saturating ones that pipeline refusable
/// attaches and read their answers, and one measured session that pipelines status requests
/// and reads them back. Every refused attach increments the daemon's own
/// `attachments_refused`, and every status answer carries that counter as it stood when the
/// answer was served — so the difference between two consecutive status answers *is* the
/// number of answers the other two sessions were given in between, counted by the daemon
/// rather than timed by this test.
///
/// The bound that difference must respect is `CONTROL_REQUESTS_PER_DISPATCH * (sessions - 1)`:
/// each other ready session is served at most one turn of at most four answers before the
/// scan wraps back. The test also insists that some gap exceeds four, which is the part that
/// no two-session test can show: with three ready sessions a turn waits behind eight answers,
/// and a contract claiming four would be describing a daemon serving one other session.
///
/// Nothing else may connect while the gaps are being read — `pmwsctl` would be a fourth
/// session and a fourth budget — so the whole measurement is taken from the measured
/// session's own answers.
#[tokio::test]
async fn three_ready_sessions_rotate_within_the_budget_the_session_count_implies() {
    let socket = temp_path("sock");
    let config = write_config_with(UNROUTABLE_ENDPOINT, socket.as_path(), &[MARKET_A], "");
    let daemon = Daemon::start(config, socket.clone()).await;
    await_serving(socket.as_path()).await;

    let saturating: Vec<ChattySession> = (0..SATURATING_SESSIONS)
        .map(|_| ChattySession::start(socket.as_path(), REFUSED_ATTACH))
        .collect();
    tokio::time::sleep(POLL_INTERVAL * 8).await;

    let measured = socket.clone();
    let gaps = tokio::task::spawn_blocking(move || rotation_gaps(measured.as_path()))
        .await
        .expect("the rotation completes");
    for session in saturating {
        session.stop().await;
    }

    let ceiling = REQUESTS_PER_DISPATCH * SATURATING_SESSIONS as u64;
    assert!(
        gaps.iter().all(|gap| *gap <= ceiling),
        "no turn waits behind more than {ceiling} answers with three ready sessions: {gaps:?}"
    );
    assert!(
        gaps.iter().any(|gap| *gap > REQUESTS_PER_DISPATCH),
        "with three ready sessions a turn waits behind more than one session's own budget of \
         {REQUESTS_PER_DISPATCH}: {gaps:?}"
    );

    let code = daemon.terminate().await;
    assert_eq!(code, 0);
}

/// Runs the three-session rotation and returns, for each pair of consecutive answers the
/// measured session received, how many answers the other two sessions were given in between.
///
/// Blocking on purpose: three real sessions, and the ordering under test is the daemon's own
/// answer order, which nothing here may perturb by connecting again.
fn rotation_gaps(socket: &Path) -> Vec<u64> {
    let mut measured =
        std::os::unix::net::UnixStream::connect(socket).expect("the measured session connects");
    measured
        .set_read_timeout(Some(STEP_TIMEOUT))
        .expect("the reads are bounded");
    measured
        .set_write_timeout(Some(STEP_TIMEOUT))
        .expect("the writes are bounded");
    let requests = "{\"command\":\"status\"}\n".repeat(MEASURED_ANSWERS);
    measured
        .write_all(requests.as_bytes())
        .expect("the measured session's whole backlog is sent");
    measured.flush().expect("the backlog is flushed");

    let mut answers = BufReader::new(measured);
    let mut refusals = Vec::with_capacity(MEASURED_ANSWERS);
    for index in 0..MEASURED_ANSWERS {
        let mut line = String::new();
        let read = answers
            .read_line(&mut line)
            .unwrap_or_else(|error| panic!("answer {index}: {error}"));
        assert!(
            read > 0,
            "answer {index} of {MEASURED_ANSWERS} never arrived"
        );
        let page: StatusPage = serde_json::from_str(line.trim_end())
            .unwrap_or_else(|error| panic!("answer {index}: {error}: {line}"));
        refusals.push(page.status.attachments_refused);
    }
    refusals.windows(2).map(|pair| pair[1] - pair[0]).collect()
}

/// How long a session that says nothing waits before the daemon's own opening deadline drops
/// it: `pmwsd`'s `CONTROL_READ_TIMEOUT` of five seconds, plus room for the sweep that deadline
/// wakes to run and for the assertions after it.
const OPENING_DEADLINE_WINDOW: Duration = Duration::from_secs(7);

/// A daemon runs under the largest lease TTL its own configuration accepts.
///
/// The arithmetic, not the expiry. Every pass of the control loop computes a TTL deadline
/// from the last request a session completed, and every sweep compares one against now — so a
/// TTL a monotonic clock could not carry that far would fail on the first completed request,
/// not at the far end of a year. The silent connection is what makes the sweep run inside a
/// test: its own opening deadline fires, and the sweep that fires with it evaluates the
/// consumer session's ceiling-TTL deadline beside it.
///
/// What must survive is the lease: a ceiling TTL expires nothing a year short of a year.
#[tokio::test]
async fn a_daemon_runs_under_the_largest_lease_ttl_its_configuration_accepts() {
    let socket = temp_path("sock");
    let config = write_config_with(
        UNROUTABLE_ENDPOINT,
        socket.as_path(),
        &[MARKET_A],
        format!("lease_ttl_ms = {}\n", pm_ws::daemon::MAX_LEASE_TTL_MS).as_str(),
    );
    let daemon = Daemon::start(config, socket.clone()).await;
    await_serving(socket.as_path()).await;

    let mut consumer = connect(socket.as_path(), MARKET_B).await;
    let _promise = promised(&consumer.transfer);
    await_status(socket.as_path(), "the consumer's lease held", |seen| {
        demand(seen, MARKET_B) == Some((false, 1))
    })
    .await;

    let silent =
        std::os::unix::net::UnixStream::connect(socket.as_path()).expect("the daemon accepts");
    tokio::time::sleep(OPENING_DEADLINE_WINDOW).await;
    drop(silent);

    let alive = status(socket.as_path()).await;
    assert_eq!(
        demand(&alive, MARKET_B),
        Some((false, 1)),
        "a ceiling TTL expires nothing while the session that holds the lease is open"
    );
    assert_eq!(demand(&alive, MARKET_A), Some((true, 0)));

    consumer.disconnect();
    let code = daemon.terminate().await;
    assert_eq!(
        code, 0,
        "the daemon served its whole run under the ceiling TTL"
    );
}

/// `target/<profile>/libpm_ws.<ext>`, the cdylib this very test run built, derived from this
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

/// The Python binding's `Segment.connect` reaches a live daemon's market end to end.
///
/// The proof is the whole path in one process boundary crossing: the child names only the
/// control socket and a slug, the C ABI does the conversation, and what comes back is the
/// book this test's controlled venue published seconds earlier. Nothing in the child knows a
/// segment path, and there is none it could have opened.
#[tokio::test]
async fn a_python_consumer_connects_through_the_control_socket_and_reads_the_market() {
    const CHILD: &str = r#"
import sys
sys.path.insert(0, sys.argv[1])
import pmws
segment = pmws.Segment.connect(sys.argv[2], sys.argv[3])
market = segment.resolve("limitless", "slug", sys.argv[3])
state = market.read_state()
level = lambda side: "-" if state.best(side) is None else f"{state.best(side).price}@{state.best(side).quantity}"
print(f"connected shard_markets={segment.info.directory_capacity > 0}")
print(f"bbo {level('Bid')} {level('Ask')}")
segment.renew()
print("renewed")
segment.release(sys.argv[3])
print("released")
segment.close()
"#;
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

    let mut peer = ControlledPeer::start(patient_peer()).await;
    let socket = temp_path("sock");
    let config = write_config(peer.endpoint().as_str(), socket.as_path(), &[MARKET_A]);
    let daemon = Daemon::start(config, socket.clone()).await;
    let mut connection = peer.next_connection().await;
    assert_eq!(
        connection.complete_handshake().await.slugs,
        owned(&[MARKET_A])
    );
    connection
        .send_orderbook(MARKET_A, &[("0.61", "12")], &[("0.62", "13")], Some(1))
        .await;
    let _live = await_status(socket.as_path(), "the market live", |seen| {
        seen.market(MARKET_A)
            .is_some_and(|report| report.status == MarketStatus::Live)
    })
    .await;

    let bindings = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("bindings/python");
    let socket_argument = socket.clone();
    let output = tokio::task::spawn_blocking(move || {
        Command::new("python3")
            .args(["-c", CHILD])
            .arg(bindings)
            .arg(&socket_argument)
            .arg(MARKET_A)
            .env("PMWS_LIB", library)
            .output()
            .expect("python3 runs")
    })
    .await
    .expect("the child completes");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        output.status.success(),
        "the python consumer failed: {stdout}{stderr}"
    );
    assert_eq!(
        stdout.lines().collect::<Vec<&str>>(),
        vec![
            "connected shard_markets=True",
            "bbo 0.61@12 0.62@13",
            "renewed",
            "released"
        ],
        "stderr: {stderr}"
    );

    let served = status(socket.as_path()).await;
    assert_eq!(
        served.shards[0].attachments, 1,
        "the daemon counts the transfer it served the python consumer"
    );
    let code = daemon.terminate().await;
    assert_eq!(code, 0);
}

/// The Node binding's `Segment.connect` reaches the same live daemon's market the Python one
/// does, through the same C ABI export and the same descriptor transfer.
///
/// The two runtimes are checked against each other rather than against a constant alone: a
/// binding that mis-decodes a level would still print *a* book, and only agreement with the
/// other runtime and with what the venue published makes the reading right.
#[tokio::test]
async fn a_node_consumer_connects_through_the_control_socket_and_reads_the_market() {
    const CHILD: &str = r#"
const pmws = await import('./bindings/node/pmws.ts');
const segment = pmws.Segment.connect(process.argv[1], process.argv[2]);
const market = segment.resolve("limitless", "slug", process.argv[2]);
const state = market.readState();
const level = (side) => {
  const best = state.best(side);
  return best === null ? "-" : `${best.price.text}@${best.quantity.text}`;
};
console.log(`bbo ${level("bid")} ${level("ask")}`);
segment.renew();
console.log("renewed");
segment.release(process.argv[2]);
console.log("released");
segment.close();
"#;
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

    let mut peer = ControlledPeer::start(patient_peer()).await;
    let socket = temp_path("sock");
    let config = write_config(peer.endpoint().as_str(), socket.as_path(), &[MARKET_A]);
    let daemon = Daemon::start(config, socket.clone()).await;
    let mut connection = peer.next_connection().await;
    assert_eq!(
        connection.complete_handshake().await.slugs,
        owned(&[MARKET_A])
    );
    connection
        .send_orderbook(MARKET_A, &[("0.61", "12")], &[("0.62", "13")], Some(1))
        .await;
    let _live = await_status(socket.as_path(), "the market live", |seen| {
        seen.market(MARKET_A)
            .is_some_and(|report| report.status == MarketStatus::Live)
    })
    .await;

    let socket_argument = socket.clone();
    let output = tokio::task::spawn_blocking(move || {
        Command::new("node")
            .args(["--input-type=module", "-e", CHILD, "--"])
            .arg(&socket_argument)
            .arg(MARKET_A)
            .env("PMWS_LIB", library)
            .output()
            .expect("node runs")
    })
    .await
    .expect("the child completes");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        output.status.success(),
        "the node consumer failed: {stdout}{stderr}"
    );
    assert_eq!(
        stdout.lines().collect::<Vec<&str>>(),
        vec!["bbo 0.61@12 0.62@13", "renewed", "released"],
        "the node consumer reads the book the python one does; stderr: {stderr}"
    );
    let code = daemon.terminate().await;
    assert_eq!(code, 0);
}

/// `target/<profile>/examples/latency_probe`, the example binary this test run built,
/// derived from this test binary's own location the way `cdylib_path` derives the cdylib's.
///
/// Cargo does not build an example ahead of an integration test the way it builds every
/// `[[bin]]` target automatically (`CARGO_BIN_EXE_pmwsd` and its siblings arrive already
/// built and named), so this test builds it explicitly before running it.
fn latency_probe_path() -> PathBuf {
    let mut path = std::env::current_exe().expect("test binary path");
    let _ = path.pop();
    if path.ends_with("deps") {
        let _ = path.pop();
    }
    path.push("examples");
    path.push(if cfg!(windows) {
        "latency_probe.exe"
    } else {
        "latency_probe"
    });
    path
}

/// The built `latency_probe` example, built once however many tests here run it.
///
/// The build is a `cargo` invocation from inside a test, so two tests asking for it would
/// otherwise queue behind each other on cargo's own build lock for no reason. `LazyLock`
/// makes the first asker build it and every later one take the path it produced.
static LATENCY_PROBE: std::sync::LazyLock<PathBuf> = std::sync::LazyLock::new(|| {
    let built = Command::new("cargo")
        .args(["build", "--example", "latency_probe", "--locked"])
        .status()
        .expect("cargo build --example latency_probe runs");
    assert!(
        built.success(),
        "cargo build --example latency_probe failed"
    );
    let path = latency_probe_path();
    assert!(
        path.is_file(),
        "the example binary is at {}",
        path.display()
    );
    path
});

/// [`LATENCY_PROBE`] from an async test, off the runtime's own thread: the first caller runs
/// a whole `cargo build` inside this.
async fn built_latency_probe() -> PathBuf {
    tokio::task::spawn_blocking(|| LATENCY_PROBE.clone())
        .await
        .expect("the example build completes")
}

/// `examples/latency_probe.rs`'s `--control`/`--market` mode attaches to a live shard's
/// segment by descriptor transfer through the control socket — the same conversation a
/// production consumer's C ABI drives — and its measurement loop runs to completion and
/// prints the documented report over that attachment exactly as it does over a `--shm` path
/// attachment.
///
/// The probe's market is deliberately **not** in the daemon's configured set, so the probe's
/// own lease is the only thing holding it: the attach is what subscribes it at the venue, and
/// the probe's control session is what keeps it there. A probe that dropped that session
/// after taking its descriptor would spend its run watching a market the daemon had already
/// removed — a segment that stays mapped and readable and simply stops changing — so what
/// this asserts is that the market is still leased mid-run, that the run kept its session
/// alive across at least one renewal, that samples were actually taken, and that the market
/// goes when the probe exits.
///
/// Bounded throughout: the probe's own `--seconds` ends its run, and this test's outer
/// `tokio::time::timeout` ends the test itself if the child somehow does not, so a
/// regression here fails loudly rather than hanging the suite.
#[tokio::test]
async fn the_latency_probe_attaches_by_descriptor_transfer_and_reports() {
    let binary = built_latency_probe().await;

    let mut peer = ControlledPeer::start(patient_peer()).await;
    let socket = temp_path("sock");
    let config = write_config(peer.endpoint().as_str(), socket.as_path(), &[MARKET_A]);
    let daemon = Daemon::start(config, socket.clone()).await;
    let mut connection = peer.next_connection().await;
    assert_eq!(
        connection.complete_handshake().await.slugs,
        owned(&[MARKET_A])
    );
    connection
        .send_orderbook(MARKET_A, &[("0.51", "10")], &[("0.52", "11")], Some(1))
        .await;
    let _live = await_status(socket.as_path(), "the market live", |seen| {
        seen.market(MARKET_A)
            .is_some_and(|report| report.status == MarketStatus::Live)
    })
    .await;

    let outcome = tokio::time::timeout(Duration::from_secs(60), async {
        let socket_argument = socket.clone();
        let child = Command::new(&binary)
            .arg("--control")
            .arg(&socket_argument)
            .arg("--market")
            .arg(MARKET_B)
            .arg("--seconds")
            .arg(PROBE_SECONDS.to_string())
            .arg("--label")
            .arg("daemon_contracts descriptor-transfer harness")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("latency_probe spawns");

        let mut leased = peer.next_connection().await;
        assert_eq!(
            leased.complete_handshake().await.slugs,
            owned(&[MARKET_A, MARKET_B]),
            "the probe's own attach is what subscribes its market"
        );
        tokio::time::sleep(PROBE_SETTLE).await;
        assert_eq!(
            demand(&status(socket.as_path()).await, MARKET_B),
            Some((false, 1)),
            "the probe holds its lease past the attach that took it"
        );
        for revision in 2..PROBE_UPDATES {
            tokio::time::sleep(Duration::from_millis(400)).await;
            leased
                .send_orderbook(
                    MARKET_A,
                    &[("0.51", "10")],
                    &[("0.52", "11")],
                    Some(revision),
                )
                .await;
            leased
                .send_orderbook(
                    MARKET_B,
                    &[("0.31", "20")],
                    &[("0.32", "21")],
                    Some(revision),
                )
                .await;
            if revision % 6 == 0 {
                let seen = status(socket.as_path()).await;
                assert_eq!(
                    demand(&seen, MARKET_B),
                    Some((false, 1)),
                    "the probe's market stays leased for the whole of its run"
                );
            }
        }

        tokio::task::spawn_blocking(move || child.wait_with_output().expect("latency_probe exits"))
            .await
            .expect("the wait completes")
    })
    .await
    .expect("the latency probe finished inside the test's own bound");

    let stdout = String::from_utf8_lossy(&outcome.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&outcome.stderr).into_owned();
    assert!(
        outcome.status.success(),
        "the latency probe failed: {stdout}{stderr}"
    );
    let lines: Vec<&str> = stdout.lines().collect();
    assert!(
        lines.iter().any(|line| line.starts_with("mode:")),
        "stdout: {stdout}"
    );
    assert!(
        lines.iter().any(|line| line.starts_with("wakes:")),
        "stdout: {stdout}"
    );
    assert!(
        reported(&lines, "samples_kept: ") > 0,
        "the probe sampled the market its own lease held: {stdout}"
    );
    assert!(
        reported(&lines, "lease_renewals: ") >= 1,
        "the probe renewed the session holding its lease at least once: {stdout}{stderr}"
    );
    assert_eq!(
        reported(&lines, "lease_renew_failures: "),
        0,
        "every renewal reached the daemon: {stdout}{stderr}"
    );

    let mut released = peer.next_connection().await;
    assert_eq!(
        released.complete_handshake().await.slugs,
        owned(&[MARKET_A]),
        "the probe exiting closes its session, and the last lease going takes the market off \
         the venue"
    );
    await_status(socket.as_path(), "the probe's market released", |seen| {
        demand(seen, MARKET_B).is_none()
    })
    .await;

    let code = daemon.terminate().await;
    assert_eq!(code, 0);
}

/// How long the probe measures for: past `examples/latency_probe.rs`'s own five-second
/// renewal cadence, so the run crosses a renewal rather than merely holding a socket open,
/// and comfortably past the last update this test feeds it — the venue connection the updates
/// ride is fenced when the probe exits and its market is released, so the updates must finish
/// first.
const PROBE_SECONDS: u64 = 12;
/// How long the test waits after the probe's attach before asking whether the market is
/// still leased. Long enough for a released lease to have reached the venue as a
/// resubscription, so what it reads is the daemon's settled demand rather than a race.
const PROBE_SETTLE: Duration = Duration::from_millis(1_500);
/// One book revision per 400 ms, ending well inside [`PROBE_SECONDS`].
///
/// Every round feeds both markets, not only the probe's. A shard that resubscribed and then
/// heard nothing about a market it had asked for treats the silence as a failed recovery and
/// reconnects — inside `ShardConfig`'s own five-second resubscribe window — which would take
/// the venue connection these updates ride out from under the test.
const PROBE_UPDATES: u64 = 18;

/// The TTL the renewal proof runs its daemon under: the shortest a configuration may declare,
/// so a probe renewing on a cadence of its own choosing would have had to choose one under a
/// third of a second — and the shipped one before this contract existed was five seconds.
const PROBE_TTL_MS: u64 = pm_ws::daemon::MIN_LEASE_TTL_MS;

/// How long the renewal proof's probe measures for: several whole TTLs, so a lease that is
/// still held at the end survived expiry rather than merely not having reached it yet.
const PROBE_TTL_SECONDS: u64 = 6;

/// How long the renewal proof watches the lease for, after the attach that took it: three
/// TTLs, inside the run above.
const LEASE_WATCH: Duration = Duration::from_millis(PROBE_TTL_MS * 3);

/// How often it asks.
const LEASE_WATCH_INTERVAL: Duration = Duration::from_millis(250);

/// The fewest acknowledged renewals a run across three TTLs may report. A third of the TTL is
/// the cadence the probe derives, so a healthy run reports many more; this is the count below
/// which the lease could not have been held on renewals at all.
const MIN_ACKNOWLEDGED_RENEWALS: u64 = 2;

/// Under a configured lease TTL the probe renews inside the deadline its own attach answer
/// declared, and the market only its lease holds stays held for the whole run.
///
/// The deadline is the daemon's to state and the consumer's to meet: a probe that renewed on a
/// fixed cadence of its own would keep a market alive under a long TTL and lose one under any
/// TTL shorter than that cadence — while its segment stays mapped, readable, and silently
/// unmaintained, which is the failure that looks most like success. So this daemon runs at the
/// shortest TTL a configuration may declare, the probe's market is deliberately not in the
/// configured set — its own lease is the only demand on it — and the run lasts several whole
/// TTLs.
///
/// What is asserted is the market's continuous presence rather than the renewal count alone:
/// counting renewals proves lines were sent and acknowledged, and only the demand the daemon
/// reports proves they arrived in time. The venue endpoint is unroutable on purpose, because
/// nothing here is about a venue: an attach leases a market and a shard holds it whether or
/// not a connection to the venue exists.
#[tokio::test]
async fn the_latency_probe_renews_inside_the_lease_ttl_its_attachment_declared() {
    let binary = built_latency_probe().await;
    let socket = temp_path("sock");
    let config = write_config_with(
        UNROUTABLE_ENDPOINT,
        socket.as_path(),
        &[MARKET_A],
        format!("lease_ttl_ms = {PROBE_TTL_MS}\n").as_str(),
    );
    let daemon = Daemon::start(config, socket.clone()).await;
    await_serving(socket.as_path()).await;

    let outcome = tokio::time::timeout(Duration::from_secs(60), async {
        let child = Command::new(&binary)
            .arg("--control")
            .arg(socket.as_path())
            .arg("--market")
            .arg(MARKET_B)
            .arg("--seconds")
            .arg(PROBE_TTL_SECONDS.to_string())
            .arg("--label")
            .arg("daemon_contracts short-ttl renewal harness")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("latency_probe spawns");

        let _held = await_status(socket.as_path(), "the probe's lease held", |seen| {
            demand(seen, MARKET_B) == Some((false, 1))
        })
        .await;

        let watch_until = Instant::now() + LEASE_WATCH;
        let mut polls = 0_u32;
        while Instant::now() < watch_until {
            tokio::time::sleep(LEASE_WATCH_INTERVAL).await;
            assert_eq!(
                demand(&status(socket.as_path()).await, MARKET_B),
                Some((false, 1)),
                "the probe renews inside the {PROBE_TTL_MS} ms TTL its attach answer declared, \
                 so the market its lease alone holds is still held"
            );
            polls += 1;
        }
        assert!(
            polls >= 3,
            "the lease was watched across {LEASE_WATCH:?}, which is more than two whole TTLs"
        );

        tokio::task::spawn_blocking(move || child.wait_with_output().expect("latency_probe exits"))
            .await
            .expect("the wait completes")
    })
    .await
    .expect("the latency probe finished inside the test's own bound");

    let stdout = String::from_utf8_lossy(&outcome.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&outcome.stderr).into_owned();
    assert!(
        outcome.status.success(),
        "the latency probe failed: {stdout}{stderr}"
    );
    let lines: Vec<&str> = stdout.lines().collect();
    assert!(
        reported(&lines, "lease_renewals: ") >= MIN_ACKNOWLEDGED_RENEWALS,
        "the daemon acknowledged the renewals that kept the lease: {stdout}{stderr}"
    );
    assert_eq!(
        reported(&lines, "lease_renew_failures: "),
        0,
        "every renewal exchange completed: {stdout}{stderr}"
    );

    await_status(socket.as_path(), "the probe's market released", |seen| {
        demand(seen, MARKET_B).is_none()
    })
    .await;

    let code = daemon.terminate().await;
    assert_eq!(code, 0);
}

/// One `name: value` line of the probe's report, as the number it carries.
fn reported(lines: &[&str], name: &str) -> u64 {
    let line = lines
        .iter()
        .find(|line| line.starts_with(name))
        .unwrap_or_else(|| panic!("the report carries a {name:?} line: {lines:?}"));
    line[name.len()..]
        .trim()
        .parse()
        .unwrap_or_else(|error| panic!("{name:?} carries a number: {error}: {line}"))
}

#![forbid(unsafe_code)]

//! The `/metrics` endpoint as a monitoring server meets it: the real `pmwsd` binary, a real
//! TCP connection, and the exposition document it answers with.
//!
//! Every daemon here runs against an endpoint nothing listens on, so what is proven is the
//! endpoint itself — its configuration, its refusals, and the numbers it reports about a
//! daemon that is serving — and no venue traffic is involved in proving it.

use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::time::Instant;

const MARKET_A: &str = "btc-up-or-down-5-min-1788172500";
const MARKET_B: &str = "eth-up-or-down-5-min-1788172500";
const STEP_TIMEOUT: Duration = Duration::from_secs(20);
const POLL_INTERVAL: Duration = Duration::from_millis(25);
/// A loopback address nothing listens on, so a daemon under test never reaches a venue.
const UNROUTABLE_ENDPOINT: &str = "ws://127.0.0.1:1/socket.io/?EIO=4&transport=websocket";
/// How long a scrape connection waits on the daemon before the test fails instead of hanging.
const SCRAPE_TIMEOUT: Duration = Duration::from_secs(10);
/// The daemon's own `MAX_METRICS_CONNECTIONS`, which is not a public constant: a connection
/// past this many is closed unread, and a test that held fewer would prove nothing.
const CONNECTION_CAP: usize = 4;
/// One byte past the daemon's `METRICS_HEAD_LIMIT`.
///
/// Exactly one past it on purpose: the daemon stops reading when what it holds is *over* the
/// limit, so a head of this length is one it has consumed whole. Nothing is left unread on
/// the socket it then answers and closes, which is what keeps the refusal readable rather
/// than a reset.
const OVERSIZED_HEAD_BYTES: usize = 8193;

/// The delivery geometry every daemon here runs under.
///
/// Small on purpose: a segment's region is zero-filled at creation, and what these tests
/// prove is independent of its geometry.
const TEST_DELIVERY: &str = "level_capacity = 64\n[delivery]\nsegment_slots = 128\nevent_capacity = 16\ndirty_capacity = 16\n";

static NEXT_PATH: AtomicU32 = AtomicU32::new(0);

/// A short absolute path under `/tmp`, unique to this process and this call.
fn temp_path(extension: &str) -> PathBuf {
    let pid = std::process::id();
    let sequence = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
    PathBuf::from(format!("/tmp/pmwsm-t{pid}-{sequence}.{extension}"))
}

/// The lock file a daemon holds beside its control socket.
fn lock_path(socket: &Path) -> PathBuf {
    let mut name = socket.as_os_str().to_owned();
    name.push(".lock");
    PathBuf::from(name)
}

fn write_config(socket: &Path, markets: &[&str], extra: &str) -> PathBuf {
    let path = temp_path("toml");
    let listed = markets
        .iter()
        .map(|slug| format!("{slug:?}"))
        .collect::<Vec<_>>()
        .join(", ");
    let document = format!(
        "control_socket = {:?}\nendpoint = {UNROUTABLE_ENDPOINT:?}\nmarkets = [{listed}]\n{extra}{TEST_DELIVERY}",
        socket.display().to_string()
    );
    std::fs::write(&path, document).expect("the test config is written");
    path
}

/// A `pmwsd` child process that is always cleaned up, so a failing assertion never leaves a
/// daemon holding the socket a later run needs.
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

/// The fields of `pmwsctl status` these tests read. The rest of the document is ignored.
#[derive(Debug, Deserialize)]
struct StatusOutput {
    pid: u32,
    metrics_listen: Option<String>,
}

/// Waits until a daemon on `socket` answers a command, which is what proves it is serving
/// rather than that a socket file exists.
async fn status(socket: &Path) -> StatusOutput {
    let deadline = Instant::now() + STEP_TIMEOUT;
    loop {
        let answered = control(socket, &["status"]).await;
        if answered.code == 0 {
            return serde_json::from_str(answered.stdout.as_str())
                .expect("pmwsctl status prints the document these tests read");
        }
        assert!(
            Instant::now() < deadline,
            "no daemon answered on {} within {STEP_TIMEOUT:?}",
            socket.display()
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// The address the daemon reports its metrics endpoint on, which for a configured port `0`
/// is the only place the chosen port is knowable.
async fn metrics_address(socket: &Path) -> String {
    status(socket)
        .await
        .metrics_listen
        .expect("a daemon configured with a metrics endpoint reports where it bound")
}

/// One HTTP exchange with the endpoint: the request as written, the answer as read to the
/// close the daemon promises.
///
/// Read to end-of-stream rather than by `Content-Length`, because `Connection: close` is what
/// the answer declares; the socket's own timeouts are what turn a daemon that never answers
/// into a failure rather than a hang.
async fn request(address: &str, head: &str) -> String {
    request_within(address, head, SCRAPE_TIMEOUT).await
}

/// One HTTP exchange with a deadline of the caller's choosing, for a request the daemon is
/// expected to answer only after a deadline of its own.
async fn request_within(address: &str, head: &str, patience: Duration) -> String {
    let address = address.to_owned();
    let head = head.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut stream = TcpStream::connect(address.as_str()).expect("the endpoint accepts");
        stream
            .set_read_timeout(Some(patience))
            .expect("the read deadline is set");
        stream
            .set_write_timeout(Some(patience))
            .expect("the write deadline is set");
        stream
            .write_all(head.as_bytes())
            .expect("the request is sent");
        stream.flush().expect("the request is flushed");
        let mut answer = Vec::new();
        let _read = stream.read_to_end(&mut answer);
        String::from_utf8_lossy(answer.as_slice()).into_owned()
    })
    .await
    .expect("the exchange completes")
}

async fn scrape(address: &str) -> String {
    request(address, "GET /metrics HTTP/1.0\r\nHost: pmwsd\r\n\r\n").await
}

/// The value of one unlabelled sample in an exposition document.
fn sample(document: &str, name: &str) -> Option<u64> {
    document
        .lines()
        .filter(|line| !line.starts_with('#'))
        .find_map(|line| {
            line.strip_prefix(name)?
                .strip_prefix(' ')?
                .trim()
                .parse()
                .ok()
        })
}

/// A configured endpoint serves this daemon's own numbers, and says how they were measured.
///
/// The port is discovered from `pmwsctl status` rather than pinned in the configuration, so
/// this test never fails because another process holds a port, and the discovery itself is
/// what deliverable the daemon reports its resolved address for.
#[tokio::test]
async fn a_configured_endpoint_serves_this_daemons_numbers_over_a_real_connection() {
    let socket = temp_path("sock");
    let config = write_config(
        socket.as_path(),
        &[MARKET_A],
        "metrics_listen = \"127.0.0.1:0\"\n",
    );
    let daemon = Daemon::start(config, socket.clone()).await;
    let reported = status(socket.as_path()).await;
    let address = reported
        .metrics_listen
        .clone()
        .expect("the daemon reports where it bound");
    assert!(
        address.starts_with("127.0.0.1:") && !address.ends_with(":0"),
        "port 0 is bound to a port the operating system chose, and that port is reported: {address}"
    );

    let answer = scrape(address.as_str()).await;
    assert!(
        answer.starts_with("HTTP/1.0 200 OK\r\n"),
        "a scrape is answered: {answer}"
    );
    assert!(
        answer.contains("Content-Type: text/plain; version=0.0.4\r\n"),
        "the answer names the exposition format: {answer}"
    );
    let body = answer
        .split_once("\r\n\r\n")
        .expect("the answer has a body")
        .1;
    for family in [
        "pmws_shard_frames_seen",
        "pmws_shard_events",
        "pmws_shard_snapshots_applied",
        "pmws_shard_mutations_derived",
        "pmws_shard_resolutions_forwarded",
        "pmws_shard_overload_drops",
        "pmws_shard_continuity_losses",
        "pmws_shard_connection_attempts",
        "pmws_shard_markets_dropped",
        "pmws_shard_queue_depth_max",
        "pmws_shard_queue_age_samples",
        "pmws_shard_queue_age_last_micros",
        "pmws_shard_queue_age_max_micros",
        "pmws_shard_queue_age_p50_micros",
        "pmws_shard_queue_age_p99_micros",
        "pmws_shard_publish_latency_samples",
        "pmws_shard_publish_latency_last_micros",
        "pmws_shard_publish_latency_max_micros",
        "pmws_shard_publish_latency_p50_micros",
        "pmws_shard_publish_latency_p99_micros",
        "pmws_shard_publish_latency_p999_micros",
        "pmws_shard_connected",
        "pmws_shard_reconciling",
        "pmws_shard_segment_attachments",
        "pmws_shard_desired_markets",
        "pmws_shard_decode_failures",
        "pmws_answers_abandoned",
        "pmws_attachments_refused",
        "pmws_markets",
        "pmws_markets_pinned",
        "pmws_market_leases",
        "pmws_pid",
    ] {
        assert!(
            body.contains(format!("# TYPE {family} ").as_str()),
            "{family} declares its type: {body}"
        );
    }
    assert!(
        body.contains("pmws_shard_queue_age_samples{shard=\"0\"} "),
        "the queue-age distribution carries the sample count behind it: {body}"
    );
    assert_valid_exposition(body);
    assert_eq!(
        sample(body, "pmws_pid"),
        Some(u64::from(reported.pid)),
        "the document reports the daemon it was scraped from: {body}"
    );
    assert_eq!(
        sample(body, "pmws_markets_pinned"),
        Some(1),
        "the configured market is reported as the pin it is: {body}"
    );

    let added = control(socket.as_path(), &["add", MARKET_B]).await;
    assert_eq!(added.code, 0, "the market is added");
    let grown = scrape(address.as_str()).await;
    assert_eq!(
        sample(&grown, "pmws_markets_pinned"),
        Some(2),
        "a scrape reports demand as it is now, not as it was at startup: {grown}"
    );

    assert_eq!(daemon.terminate().await, 0, "the daemon shuts down cleanly");
}

/// A daemon told nothing about metrics binds nothing and says so.
#[tokio::test]
async fn a_daemon_with_no_metrics_address_reports_none() {
    let socket = temp_path("sock");
    let config = write_config(socket.as_path(), &[MARKET_A], "");
    let daemon = Daemon::start(config, socket.clone()).await;

    assert_eq!(
        status(socket.as_path()).await.metrics_listen,
        None,
        "an unconfigured endpoint is absent from the answer, never an address"
    );

    assert_eq!(daemon.terminate().await, 0, "the daemon shuts down cleanly");
}

/// A request that is not `GET /metrics` is refused, and the daemon goes on serving.
///
/// The point is the second half: a hostile or merely confused scraper must cost one closed
/// connection, and nothing about the control plane behind it.
#[tokio::test]
async fn a_request_that_is_not_the_metrics_get_is_refused_and_costs_the_control_plane_nothing() {
    let socket = temp_path("sock");
    let config = write_config(
        socket.as_path(),
        &[MARKET_A],
        "metrics_listen = \"127.0.0.1:0\"\n",
    );
    let daemon = Daemon::start(config, socket.clone()).await;
    let address = metrics_address(socket.as_path()).await;

    for head in [
        "GET /admin HTTP/1.0\r\n\r\n",
        "POST /metrics HTTP/1.0\r\n\r\n",
        "nonsense\r\n\r\n",
    ] {
        let answer = request(address.as_str(), head).await;
        assert!(
            !answer.starts_with("HTTP/1.0 200"),
            "{head:?} is not a scrape and is not answered as one: {answer}"
        );
        assert!(
            answer.starts_with("HTTP/1.0 4"),
            "{head:?} is refused by status line: {answer}"
        );
    }

    assert_eq!(
        control(socket.as_path(), &["status"]).await.code,
        0,
        "the daemon goes on answering control commands"
    );
    let answer = scrape(address.as_str()).await;
    assert!(
        answer.starts_with("HTTP/1.0 200 OK\r\n"),
        "and goes on answering scrapes: {answer}"
    );

    assert_eq!(daemon.terminate().await, 0, "the daemon shuts down cleanly");
}

/// One `pmwsd` startup that is expected to fail: its exit code and what it said.
fn refused_start(config: &Path) -> (i32, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_pmwsd"))
        .arg("--config")
        .arg(config)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("pmwsd runs");
    (
        output.status.code().unwrap_or(-1),
        String::from_utf8_lossy(output.stderr.as_slice()).into_owned(),
    )
}

/// A metrics address that is not one a listener can bind is a startup refusal naming the key.
///
/// A host name is refused with it: resolving one is blocking I/O the daemon does none of, so
/// the document must carry the address itself.
#[tokio::test]
async fn a_metrics_address_that_is_not_an_address_refuses_startup() {
    let socket = temp_path("sock");
    let config = write_config(
        socket.as_path(),
        &[MARKET_A],
        "metrics_listen = \"localhost:9090\"\n",
    );
    let (code, message) = refused_start(config.as_path());

    assert_eq!(code, 2, "a misconfiguration exits as usage");
    assert!(
        message.contains("metrics_listen"),
        "the refusal names the key an operator has to fix: {message}"
    );
    assert!(!socket.exists(), "nothing was bound");
    let _removed = std::fs::remove_file(&config);
}

/// A key this daemon does not define is a refusal, not a default.
#[tokio::test]
async fn a_misspelled_metrics_key_refuses_startup_rather_than_serving_nothing() {
    let socket = temp_path("sock");
    let config = write_config(
        socket.as_path(),
        &[MARKET_A],
        "metrics_listen_address = \"127.0.0.1:0\"\n",
    );
    let (code, message) = refused_start(config.as_path());

    assert_eq!(code, 2, "a misconfiguration exits as usage");
    assert!(
        message.contains("metrics_listen_address"),
        "the refusal names the key that was not understood: {message}"
    );
    assert!(!socket.exists(), "nothing was bound");
    let _removed = std::fs::remove_file(&config);
}

/// An address something else already holds ends startup loudly.
///
/// A daemon that started anyway would look healthy and be unscrapeable, which is the one
/// failure a monitoring endpoint must not have.
#[tokio::test]
async fn a_metrics_address_already_in_use_ends_startup() {
    let held = std::net::TcpListener::bind("127.0.0.1:0").expect("the test holds a port");
    let address = held.local_addr().expect("the held port is known");
    let socket = temp_path("sock");
    let config = write_config(
        socket.as_path(),
        &[MARKET_A],
        format!("metrics_listen = \"{address}\"\n").as_str(),
    );
    let (code, message) = refused_start(config.as_path());

    assert_ne!(code, 0, "a metrics address that cannot be bound is fatal");
    assert!(
        message.contains("metrics_listen"),
        "the refusal names the key an operator has to fix: {message}"
    );
    assert!(
        !socket.exists(),
        "the control socket goes with the failed startup"
    );
    let _removed = std::fs::remove_file(&config);
    let _removed = std::fs::remove_file(lock_path(socket.as_path()));
}

/// Whether a name is one the exposition format allows a family or a label to carry.
fn is_legal_name(name: &str, colons: bool) -> bool {
    let legal = |character: char, first: bool| {
        character.is_ascii_alphabetic()
            || character == '_'
            || (colons && character == ':')
            || (!first && character.is_ascii_digit())
    };
    let mut characters = name.chars();
    characters.next().is_some_and(|first| legal(first, true))
        && characters.all(|character| legal(character, false))
}

/// Asserts one sample's label set is a legal one, and answers nothing: a malformed set is a
/// failure here rather than a value a caller has to inspect.
fn assert_legal_labels(labels: &str, line: &str) {
    if labels.is_empty() {
        return;
    }
    for pair in labels.split(',') {
        let (name, value) = pair
            .split_once('=')
            .unwrap_or_else(|| panic!("a label is a name and a value: {line}"));
        assert!(
            is_legal_name(name, false),
            "{name} is not a label name the format allows: {line}"
        );
        assert!(
            value.len() >= 2 && value.starts_with('"') && value.ends_with('"'),
            "a label value is quoted: {line}"
        );
        let mut characters = value[1..value.len() - 1].chars();
        while let Some(character) = characters.next() {
            match character {
                '\\' => assert!(
                    matches!(characters.next(), Some('\\' | '"' | 'n')),
                    "a backslash in a label value escapes one of the three characters the \
                     format reserves: {line}"
                ),
                '"' | '\n' => panic!("a label value carries {character:?} unescaped: {line}"),
                _ => {}
            }
        }
    }
}

/// Asserts a scrape body is a document a strict exposition parser accepts.
///
/// Written against the format rather than against this daemon's families: every sample line
/// parses as a legal name, an optional legal label set and an integer value; every family
/// declares its help and its type exactly once and does so before its first sample; and no
/// series appears twice. A renderer that repeated a family's metadata, emitted a sample it
/// never declared, or wrote a value as anything but an integer produces a document a scraper
/// refuses whole, and that refusal is invisible to a test that only looks for names.
fn assert_valid_exposition(body: &str) {
    let mut helped: BTreeMap<&str, usize> = BTreeMap::new();
    let mut typed: BTreeMap<&str, usize> = BTreeMap::new();
    let mut series: BTreeSet<&str> = BTreeSet::new();
    for line in body.lines() {
        assert!(!line.is_empty(), "the document carries no blank line");
        if let Some(rest) = line.strip_prefix("# HELP ") {
            let (name, help) = rest
                .split_once(' ')
                .unwrap_or_else(|| panic!("a HELP line names a family and describes it: {line}"));
            assert!(is_legal_name(name, true), "{name} is not a legal name");
            assert!(!help.is_empty(), "{name} declares help text");
            *helped.entry(name).or_default() += 1;
            continue;
        }
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            let (name, kind) = rest
                .split_once(' ')
                .unwrap_or_else(|| panic!("a TYPE line names a family and its type: {line}"));
            assert!(is_legal_name(name, true), "{name} is not a legal name");
            assert!(
                matches!(
                    kind,
                    "counter" | "gauge" | "histogram" | "summary" | "untyped"
                ),
                "{kind} is not a type the format defines: {line}"
            );
            *typed.entry(name).or_default() += 1;
            continue;
        }
        assert!(!line.starts_with('#'), "an unrecognised comment: {line}");

        let (identity, value) = line
            .rsplit_once(' ')
            .unwrap_or_else(|| panic!("a sample is a series and a value: {line}"));
        assert!(
            value.parse::<u64>().is_ok(),
            "every value this daemon reports is a whole number, never a float: {line}"
        );
        let (name, labels) = match identity.split_once('{') {
            Some((name, labels)) => (
                name,
                labels
                    .strip_suffix('}')
                    .unwrap_or_else(|| panic!("a label set is closed: {line}")),
            ),
            None => (identity, ""),
        };
        assert!(
            is_legal_name(name, true),
            "{name} is not a family name the format allows: {line}"
        );
        assert_legal_labels(labels, line);
        assert_eq!(
            helped.get(name).copied(),
            Some(1),
            "{name} declares its help exactly once, before its first sample"
        );
        assert_eq!(
            typed.get(name).copied(),
            Some(1),
            "{name} declares its type exactly once, before its first sample"
        );
        assert!(series.insert(identity), "{identity} is sampled twice");
    }
    for (name, count) in &helped {
        assert_eq!(*count, 1, "{name} declares its help more than once");
        assert_eq!(
            typed.get(name).copied(),
            Some(1),
            "{name} declares help and no type"
        );
    }
    for name in typed.keys() {
        assert!(
            helped.contains_key(name),
            "{name} declares a type and no help"
        );
    }
    assert!(
        helped.len() > 20,
        "the document carries the families this daemon reports"
    );
}

/// A request head one byte past what the daemon will hold, with no blank line to end it.
fn oversized_head() -> String {
    let framing = "GET /metrics HTTP/1.0\r\nX-Pad: \r\n";
    let head = format!(
        "GET /metrics HTTP/1.0\r\nX-Pad: {}\r\n",
        "a".repeat(OVERSIZED_HEAD_BYTES - framing.len())
    );
    assert_eq!(head.len(), OVERSIZED_HEAD_BYTES);
    head
}

/// A connection that sends nothing and reads until the daemon closes it.
async fn read_until_closed(address: &str) -> String {
    let address = address.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut stream = TcpStream::connect(address.as_str()).expect("the endpoint accepts");
        stream
            .set_read_timeout(Some(SCRAPE_TIMEOUT))
            .expect("the read deadline is set");
        let mut answer = Vec::new();
        let _read = stream.read_to_end(&mut answer);
        String::from_utf8_lossy(answer.as_slice()).into_owned()
    })
    .await
    .expect("the exchange completes")
}

/// A connection that opens, sends a request it never finishes, and is held by the caller.
fn unfinished_request(address: &str) -> TcpStream {
    let mut stream = TcpStream::connect(address).expect("the endpoint accepts");
    stream
        .set_write_timeout(Some(SCRAPE_TIMEOUT))
        .expect("the write deadline is set");
    stream
        .write_all(b"GET /metrics HTTP/1.0\r\n")
        .expect("the partial head is sent");
    stream
}

/// A request head past the limit is refused, and costs the endpoint one connection.
///
/// The head is one byte past the limit, so the daemon has consumed all of it by the time it
/// refuses: what is proven is the refusal a scraper reads, and that the endpoint and the
/// control plane behind it go on serving after it.
///
/// The answer must also arrive well inside the read deadline. An endpoint that had lost its
/// size limit would hold this same head — it ends in no blank line — and refuse it when that
/// deadline expired, with the same status line; the only thing separating the bound that
/// fired is when the refusal came.
#[tokio::test]
async fn a_request_head_past_the_limit_is_refused_and_costs_one_connection() {
    let socket = temp_path("sock");
    let config = write_config(
        socket.as_path(),
        &[MARKET_A],
        "metrics_listen = \"127.0.0.1:0\"\n",
    );
    let daemon = Daemon::start(config, socket.clone()).await;
    let address = metrics_address(socket.as_path()).await;

    let started = Instant::now();
    let answer = request(address.as_str(), oversized_head().as_str()).await;
    let waited = started.elapsed();
    assert!(
        answer.starts_with("HTTP/1.0 400 Bad Request\r\n"),
        "a head past the limit is refused rather than buffered: {answer}"
    );
    assert!(
        waited < Duration::from_secs(3),
        "the size limit refused it, not the read deadline that would have refused it later: \
         {waited:?}"
    );

    assert_eq!(
        control(socket.as_path(), &["status"]).await.code,
        0,
        "the daemon goes on answering control commands"
    );
    let scraped = scrape(address.as_str()).await;
    assert!(
        scraped.starts_with("HTTP/1.0 200 OK\r\n"),
        "and goes on answering scrapes: {scraped}"
    );

    assert_eq!(daemon.terminate().await, 0, "the daemon shuts down cleanly");
}

/// A head that never ends is refused at the daemon's own read deadline, not held forever.
///
/// The connection sends a request line and no blank line, then reads. The answer proves both
/// halves of the bound: it arrives, so the daemon gave up on its own rather than waiting for
/// a peer that never speaks again, and it does not arrive at once, so what ended the wait was
/// the deadline and not the parser.
#[tokio::test]
async fn a_head_that_never_ends_is_refused_at_the_read_deadline() {
    let socket = temp_path("sock");
    let config = write_config(
        socket.as_path(),
        &[MARKET_A],
        "metrics_listen = \"127.0.0.1:0\"\n",
    );
    let daemon = Daemon::start(config, socket.clone()).await;
    let address = metrics_address(socket.as_path()).await;

    let started = Instant::now();
    let answer = request_within(
        address.as_str(),
        "GET /metrics HTTP/1.0\r\nHost: pmwsd\r\n",
        STEP_TIMEOUT,
    )
    .await;
    let waited = started.elapsed();

    assert!(
        answer.starts_with("HTTP/1.0 400 Bad Request\r\n"),
        "an unfinished head is refused: {answer}"
    );
    assert!(
        waited >= Duration::from_secs(1),
        "the refusal came from the read deadline, not from a parse that ended the request \
         early: {waited:?}"
    );

    assert_eq!(
        control(socket.as_path(), &["status"]).await.code,
        0,
        "the daemon served control throughout"
    );
    assert_eq!(daemon.terminate().await, 0, "the daemon shuts down cleanly");
}

/// A connection past the cap is closed unread, and the cap comes back with the connections
/// that held it.
///
/// The holders open first and never finish their requests, so each one is accepted and holds
/// its place; the connection after them is accepted and closed without an answer, which is
/// the contract — closed unread rather than queued behind connections nothing is serving.
/// Dropping the holders is what proves the cap is a bound and not a leak.
#[tokio::test]
async fn a_connection_past_the_cap_is_closed_unread_and_the_cap_returns() {
    let socket = temp_path("sock");
    let config = write_config(
        socket.as_path(),
        &[MARKET_A],
        "metrics_listen = \"127.0.0.1:0\"\n",
    );
    let daemon = Daemon::start(config, socket.clone()).await;
    let address = metrics_address(socket.as_path()).await;

    let holders: Vec<TcpStream> = (0..CONNECTION_CAP)
        .map(|_| unfinished_request(address.as_str()))
        .collect();

    let refused = read_until_closed(address.as_str()).await;
    assert!(
        refused.is_empty(),
        "a connection past the cap is closed without an answer: {refused:?}"
    );
    assert_eq!(
        control(socket.as_path(), &["status"]).await.code,
        0,
        "a capped endpoint costs the control plane nothing"
    );

    drop(holders);
    let deadline = Instant::now() + STEP_TIMEOUT;
    loop {
        let answer = scrape(address.as_str()).await;
        if answer.starts_with("HTTP/1.0 200 OK\r\n") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the cap came back with the connections that held it: {answer}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }

    assert_eq!(daemon.terminate().await, 0, "the daemon shuts down cleanly");
}

/// A scrape of a daemon running more than one shard is a document a strict parser accepts.
///
/// Two shards rather than one because that is where a renderer that repeated a family's
/// metadata per shard would show it, and a document with a family declared twice is refused
/// whole by a scraper — for every family in it, not only the repeated one.
#[tokio::test]
async fn a_scrape_of_a_multi_shard_daemon_parses_strictly() {
    let socket = temp_path("sock");
    let config = write_config(
        socket.as_path(),
        &[MARKET_A, MARKET_B],
        "metrics_listen = \"127.0.0.1:0\"\nmarkets_per_shard = 1\n",
    );
    let daemon = Daemon::start(config, socket.clone()).await;
    let address = metrics_address(socket.as_path()).await;

    let answer = scrape(address.as_str()).await;
    assert!(
        answer.starts_with("HTTP/1.0 200 OK\r\n"),
        "a scrape is answered: {answer}"
    );
    let body = answer
        .split_once("\r\n\r\n")
        .expect("the answer has a body")
        .1;
    assert_valid_exposition(body);
    for shard in ["0", "1"] {
        assert!(
            body.contains(format!("pmws_shard_frames_seen{{shard=\"{shard}\"}} ").as_str()),
            "every shard carries its own sample of a per-shard family: {body}"
        );
        assert!(
            body.contains(
                format!("pmws_shard_publish_latency_samples{{shard=\"{shard}\"}} 0").as_str()
            ),
            "a daemon that has published nothing a frame drove reports the publish-latency \
             family with no samples behind it, never a zero latency: {body}"
        );
        for percentile in ["p50", "p99", "p999"] {
            assert!(
                body.contains(
                    format!(
                        "pmws_shard_publish_latency_{percentile}_micros{{shard=\"{shard}\"}} 0"
                    )
                    .as_str()
                ),
                "an unmeasured publish latency reports zeroes beside a zero sample count: \
                 {body}"
            );
        }
    }
    for help in body
        .lines()
        .filter(|line| line.starts_with("# HELP pmws_shard_publish_latency_"))
    {
        assert!(
            !help.contains("consumer-ready"),
            "the publish-latency families measure to publication complete inside this \
             daemon and must not advertise a consumer-side boundary they do not measure: \
             {help}"
        );
        assert!(
            help.contains("publication"),
            "and each says what it does measure: {help}"
        );
    }

    assert_eq!(daemon.terminate().await, 0, "the daemon shuts down cleanly");
}

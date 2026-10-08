//! `pmwsctl` — the local operator tool for a running `pmwsd`.
//!
//! Connects to the daemon's Unix domain socket, sends one line of JSON, prints the one line
//! that comes back. Commands are idempotent desired-state operations: `add` and `remove`
//! submit what the caller wants subscribed and answer per market, and `status` reports what
//! the daemon holds.
//!
//! Output is JSON rather than a table so that an operator can pipe it and a test can assert
//! on parsed fields instead of on formatting.

use pm_ws::limitless::shard::MarketStatus;
use pm_ws::{
    ControlRequest, ControlResponse, MAX_CONTROL_LINE_BYTES, MarketRow, ShardReport, encode_line,
};
use serde::Serialize;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

/// Where the daemon listens unless `--socket` says otherwise.
const DEFAULT_SOCKET: &str = "/tmp/pmwsd.sock";

/// How long to wait for the daemon's answer. A daemon that has accepted the connection
/// answers from its control task; this bounds the wait rather than describing it.
const REPLY_TIMEOUT: Duration = Duration::from_secs(10);

/// The most status pages this tool will ask for before giving up.
///
/// A daemon answers `more` truthfully and every page advances the cursor, so this is not how
/// paging ends; it is what stops a daemon that answered wrongly from making this tool loop.
/// It is far above what any permitted configuration can produce.
const MAX_STATUS_PAGES: u32 = 64;

/// The command succeeded.
const EXIT_OK: i32 = 0;
/// The daemon could not be reached, or the conversation failed.
const EXIT_UNREACHABLE: i32 = 1;
/// The command line was not a command.
const EXIT_USAGE: i32 = 2;
/// At least one named market was rejected.
const EXIT_REJECTED: i32 = 3;
/// A bounded queue was full. Nothing was applied and the command may be retried.
const EXIT_BUSY: i32 = 4;

fn main() {
    let invocation = match parse_args(std::env::args()) {
        Ok(invocation) => invocation,
        Err(message) => {
            eprintln!("pmwsctl: {message}");
            eprintln!(
                "usage: pmwsctl [--socket <path>] add <slugs...> | remove <slugs...> | status"
            );
            std::process::exit(EXIT_USAGE);
        }
    };
    let answered = match invocation.request {
        ControlRequest::Status { .. } => collect_status(invocation.socket.as_path()),
        _ => exchange(invocation.socket.as_path(), &invocation.request),
    };
    match answered {
        Ok(response) => std::process::exit(report(response)),
        Err(message) => {
            eprintln!("pmwsctl: {message}");
            std::process::exit(EXIT_UNREACHABLE);
        }
    }
}

struct Invocation {
    socket: PathBuf,
    request: ControlRequest,
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Invocation, String> {
    let _binary = args.next();
    let mut socket = PathBuf::from(DEFAULT_SOCKET);
    let mut command = None;
    let mut markets = Vec::new();
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--socket" => {
                socket = PathBuf::from(args.next().ok_or("--socket requires a value")?);
            }
            other if other.starts_with("--") => return Err(format!("unknown flag {other}")),
            other if command.is_none() => command = Some(other.to_owned()),
            other => markets.push(other.to_owned()),
        }
    }
    let command = command.ok_or("a command is required")?;
    let request = match command.as_str() {
        "add" if markets.is_empty() => return Err("add requires at least one market".to_owned()),
        "remove" if markets.is_empty() => {
            return Err("remove requires at least one market".to_owned());
        }
        "add" => ControlRequest::Add { markets },
        "remove" => ControlRequest::Remove { markets },
        "status" if markets.is_empty() => ControlRequest::Status { after: None },
        "status" => return Err("status takes no market".to_owned()),
        other => return Err(format!("unknown command {other}")),
    };
    Ok(Invocation { socket, request })
}

/// Sends one request and reads one response.
///
/// The read is bounded by [`MAX_CONTROL_LINE_BYTES`] and by [`REPLY_TIMEOUT`], so a daemon
/// that accepts the connection and then says nothing costs a timeout rather than a hang.
fn exchange(socket: &Path, request: &ControlRequest) -> Result<ControlResponse, String> {
    let stream = UnixStream::connect(socket)
        .map_err(|error| format!("cannot reach the daemon on {}: {error}", socket.display()))?;
    stream
        .set_read_timeout(Some(REPLY_TIMEOUT))
        .map_err(|error| error.to_string())?;
    stream
        .set_write_timeout(Some(REPLY_TIMEOUT))
        .map_err(|error| error.to_string())?;
    let line = encode_line(request).map_err(|error| error.to_string())?;
    let mut writer = &stream;
    writer
        .write_all(line.as_bytes())
        .map_err(|error| format!("cannot send the command: {error}"))?;
    writer
        .flush()
        .map_err(|error| format!("cannot send the command: {error}"))?;
    let mut reply = String::new();
    let mut reader = BufReader::new((&stream).take(MAX_CONTROL_LINE_BYTES as u64 + 1));
    let read = reader
        .read_line(&mut reply)
        .map_err(|error| format!("no answer from the daemon: {error}"))?;
    if read == 0 {
        return Err("the daemon closed the connection without answering".to_owned());
    }
    if !reply.ends_with('\n') || reply.len() > MAX_CONTROL_LINE_BYTES {
        return Err(format!(
            "the daemon's answer exceeded {MAX_CONTROL_LINE_BYTES} bytes or carried no newline"
        ));
    }
    serde_json::from_str(reply.trim_end())
        .map_err(|error| format!("unreadable answer from the daemon: {error}"))
}

/// Collects every page of a `status` answer into one.
///
/// Paging is the protocol's, not the operator's: this walks it and hands back a single
/// picture of the daemon. The cursor carried into each request is the last slug the previous
/// page reported, so a market added or removed mid-walk cannot shift a row past the position
/// this tool is about to ask for. Shard summaries come from the first page, market rows
/// accumulate in the order the daemon reports them, and a page that is not a status answer —
/// busy, or an error — is handed back as it is.
fn collect_status(socket: &Path) -> Result<ControlResponse, String> {
    let mut shards: Vec<ShardReport> = Vec::new();
    let mut markets: Vec<MarketRow> = Vec::new();
    let mut pid = 0u32;
    let mut abandoned = 0u64;
    let mut attachments_refused = 0u64;
    let mut metrics_listen: Option<String> = None;
    let mut after: Option<String> = None;
    for _page in 0..MAX_STATUS_PAGES {
        let answer = exchange(
            socket,
            &ControlRequest::Status {
                after: after.clone(),
            },
        )?;
        let ControlResponse::Status { status } = answer else {
            return Ok(answer);
        };
        pid = status.pid;
        abandoned = status.answers_abandoned;
        attachments_refused = status.attachments_refused;
        metrics_listen = status.metrics_listen;
        if after.is_none() {
            shards = status.shards;
        }
        after = status
            .markets
            .last()
            .map(|row| row.market.slug.clone())
            .or(after);
        markets.extend(status.markets);
        if !status.more || after.is_none() {
            break;
        }
    }
    Ok(ControlResponse::Status {
        status: pm_ws::DaemonStatus {
            pid,
            more: false,
            shards,
            markets,
            answers_abandoned: abandoned,
            attachments_refused,
            metrics_listen,
        },
    })
}

/// The `status` output: what the daemon reported, plus what this process measured about it.
#[derive(Serialize)]
struct StatusOutput {
    pid: u32,
    /// Resident set size in kibibytes, as the operating system reports it for `pid`, or
    /// absent when it could not be read.
    rss_kib: Option<u64>,
    /// Control answers the daemon gave up writing because a client stopped reading.
    answers_abandoned: u64,
    /// Consumer attach requests the daemon refused, over the run.
    attachments_refused: u64,
    /// The address the daemon's metrics endpoint is bound to, or absent when it serves none.
    ///
    /// The resolved address, so a daemon configured with port `0` is scrapeable by reading
    /// this.
    metrics_listen: Option<String>,
    shards: Vec<ShardReport>,
    markets: Vec<MarketRow>,
}

fn report(response: ControlResponse) -> i32 {
    match response {
        ControlResponse::Markets { markets } => {
            print_json(&markets);
            if markets
                .iter()
                .any(|outcome| matches!(outcome.status, MarketStatus::Rejected(_)))
            {
                EXIT_REJECTED
            } else {
                EXIT_OK
            }
        }
        ControlResponse::Status { status } => {
            print_json(&StatusOutput {
                pid: status.pid,
                rss_kib: resident_kib(status.pid),
                answers_abandoned: status.answers_abandoned,
                attachments_refused: status.attachments_refused,
                metrics_listen: status.metrics_listen,
                shards: status.shards,
                markets: status.markets,
            });
            EXIT_OK
        }
        ControlResponse::Busy { message } => {
            eprintln!("pmwsctl: {message}");
            EXIT_BUSY
        }
        ControlResponse::Error { message } => {
            eprintln!("pmwsctl: {message}");
            EXIT_UNREACHABLE
        }
        ControlResponse::Attached { attachment } => {
            eprintln!(
                "pmwsctl: the daemon answered an attachment for shard {} that this tool never \
                 asked for",
                attachment.shard
            );
            EXIT_UNREACHABLE
        }
        ControlResponse::Released { leases } => {
            eprintln!(
                "pmwsctl: the daemon released a lease, leaving {leases} leases this tool never \
                 asked to hold; leases belong to a consumer's own connection"
            );
            EXIT_UNREACHABLE
        }
        ControlResponse::Renewed { leases } => {
            eprintln!(
                "pmwsctl: the daemon renewed {leases} leases this tool never asked to renew; \
                 leases belong to a consumer's own connection"
            );
            EXIT_UNREACHABLE
        }
    }
}

fn print_json<T: Serialize>(value: &T) {
    match serde_json::to_string_pretty(value) {
        Ok(text) => println!("{text}"),
        Err(error) => eprintln!("pmwsctl: cannot render the answer: {error}"),
    }
}

/// The daemon's resident memory, read from the operating system by this process.
///
/// `ps -o rss=` reports kibibytes on both macOS and Linux, which is why the field carries its
/// unit in its name. The measurement is taken here, in a short-lived tool, rather than in the
/// daemon: reading process accounting is blocking I/O, and the daemon's update path admits
/// none. On a host where `ps` is absent or refuses, `status` still succeeds and this field
/// is simply absent rather than guessed.
fn resident_kib(pid: u32) -> Option<u64> {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p"])
        .arg(pid.to_string())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()?.trim().parse().ok()
}

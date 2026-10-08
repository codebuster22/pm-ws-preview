//! Book-free upstream ingestion and metrics-only qualification before local IPC selection.

pub mod control;
pub mod demand;
mod metrics;
mod reconcile;
mod routing;
#[cfg(test)]
mod tests;

use crate::native::gate::{GateDecision, GateFault, GateLimits, SingleSourceGate};
use crate::native::{NativeBatch, NativeEvent, NativeFamily, NativeSource, NativeVenue};
use metrics::{CpuTimings, ReceiverMetrics, Timings};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::io::{Read, Write};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::SyncSender;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Bytes;

pub(crate) const MAX_INPUT_BYTES: usize = 1_048_576;

/// Selection rows assigned to one venue connection; the last chunk may hold fewer.
const ROWS_PER_CONNECTION: usize = 100;

/// Subscription coordinates one frozen selection may name across both venues. A Polymarket
/// condition counts two, one per CLOB token. A selection naming more is refused before any
/// connection opens, which keeps the frozen bound equal to the control plane's own
/// `DemandLimits::max_targets`.
const MAX_SELECTED_TARGETS: usize = 4_096;

/// Bounded demo tape depth per shard; the oldest witness is dropped on overflow.
const RECENT_CAPACITY: usize = 48;

/// Bounded stdout tape queue depth; a line offered to a full queue is dropped and counted.
const TAPE_CHANNEL_DEPTH: usize = 2_048;

/// Maximum source-frame bytes copied onto one tape line before truncation.
const TAPE_TEXT_BYTES: usize = 600;

/// Serve-mode delay before one failed peer is reconciled again.
const PEER_RESPAWN_DELAY: Duration = Duration::from_secs(5);

/// Nanoseconds of continuous full readiness that open the measured window on any run.
const WINDOW_READINESS_HOLD_NS: u64 = 30_000_000_000;

/// Nanoseconds from the process origin after which `--serve` opens the measured window even
/// without full readiness.
const SERVE_WINDOW_TIMEOUT_NS: u64 = 90_000_000_000;

/// One `--tape` line: identifiers, sizes, the handoff interval and the bounded source text.
///
/// The tape thread serializes it; the update path only offers the struct. Source text reaches
/// stdout on this type alone and never a report, a snapshot or any other file.
#[derive(Serialize)]
struct TapeLine {
    t_ns: u64,
    venue: &'static str,
    stream: String,
    family: &'static str,
    market: Option<String>,
    events: usize,
    bytes: usize,
    handoff_ns: u64,
    generation: u64,
    text: String,
}

/// Bounded handoff from one admitting worker to the single stdout tape thread.
#[derive(Clone)]
struct TapeSender {
    lines: SyncSender<TapeLine>,
    dropped: Arc<AtomicU64>,
}

impl TapeSender {
    /// Offers one line without blocking: a full or closed queue drops it and counts the drop.
    fn offer(&self, line: TapeLine) {
        if self.lines.try_send(line).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Copies the longest valid UTF-8 prefix of at most [`TAPE_TEXT_BYTES`] bytes of `frame` and
/// appends `\u{2026}` when anything was cut, including a frame that is not text at all. Only
/// the prefix is ever validated, so the cost is bounded by [`TAPE_TEXT_BYTES`], not by the frame.
fn truncate_tape_frame(frame: &[u8]) -> String {
    let prefix = &frame[..frame.len().min(TAPE_TEXT_BYTES)];
    let valid = match std::str::from_utf8(prefix) {
        Ok(text) => text,
        Err(error) => std::str::from_utf8(&prefix[..error.valid_up_to()]).unwrap_or_default(),
    };
    if valid.len() == frame.len() {
        return valid.to_owned();
    }
    let mut result = String::with_capacity(valid.len() + '\u{2026}'.len_utf8());
    result.push_str(valid);
    result.push('\u{2026}');
    result
}

/// Owns stdout while `--tape` is on and writes one JSON line per admitted batch.
///
/// The thread ends when every sender is dropped or stdout stops accepting writes; afterwards
/// offered lines fill the queue and count as drops. No worker thread touches stdout.
fn spawn_tape(
    dropped: Arc<AtomicU64>,
) -> Result<(TapeSender, std::thread::JoinHandle<()>), String> {
    let (lines, received) = std::sync::mpsc::sync_channel(TAPE_CHANNEL_DEPTH);
    let handle = std::thread::Builder::new()
        .name("tape".into())
        .spawn(move || {
            let mut out = std::io::BufWriter::new(std::io::stdout());
            for line in received {
                if serde_json::to_writer(&mut out, &line).is_err()
                    || out.write_all(b"\n").is_err()
                    || out.flush().is_err()
                {
                    break;
                }
            }
        })
        .map_err(|error| format!("tape thread: {error}"))?;
    Ok((TapeSender { lines, dropped }, handle))
}

/// Why the measured window opens on this tick, or `None` while it stays shut.
///
/// Full readiness held for [`WINDOW_READINESS_HOLD_NS`] opens it on every run and wins ties;
/// `--serve` additionally opens it [`SERVE_WINDOW_TIMEOUT_NS`] after the process origin.
fn window_open_cause(
    serve: bool,
    ready: bool,
    ready_since: Option<u64>,
    now_ns: u64,
) -> Option<&'static str> {
    if ready && now_ns.saturating_sub(ready_since.unwrap_or(now_ns)) >= WINDOW_READINESS_HOLD_NS {
        return Some("readiness");
    }
    (serve && now_ns >= SERVE_WINDOW_TIMEOUT_NS).then_some("timeout")
}

/// A concrete venue-native subscription coordinate, not a book descriptor.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct NativeTarget {
    pub market: Arc<str>,
    pub asset: Option<Arc<str>>,
    pub amm: bool,
}

/// One socket generation's bounded assignment and connection rail.
#[derive(Clone)]
pub(crate) struct PeerConfig {
    pub venue: NativeVenue,
    pub endpoint: String,
    pub slot: u16,
    pub generation: u64,
    pub targets: Vec<NativeTarget>,
    pub lifecycle: bool,
    pub min_command_interval: Duration,
    /// True only under `--serve`: an acknowledgement covering a strict subset of this
    /// generation's targets establishes coverage for the acknowledged markets and keeps the
    /// connection instead of rejecting the venue. A measurement run never tolerates it.
    pub tolerate_partial_ack: bool,
}

/// Distinct transport, schema, admission, and operator terminal outcomes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PeerEnd {
    ConnectFailed,
    ConnectTimedOut,
    WriteFailed,
    HeartbeatTimeout,
    SocketClosed,
    ReadFailed,
    DecodeFailed,
    RoutingFailed,
    AdmissionFailed,
    ControlOverload,
    VenueRejected,
    Stopped,
}

struct Window {
    origin: Instant,
    start_ns: AtomicU64,
    end_ns: AtomicU64,
}

#[derive(Clone, Copy)]
pub(crate) struct StageStamp {
    pub(crate) wall_ns: u64,
    pub(crate) cpu_ns: Option<u64>,
}

#[derive(Clone, Serialize)]
struct CpuClockCalibration {
    paired_reads: usize,
    minimum_nonzero_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    max_ns: u64,
}

#[derive(Clone, Serialize)]
struct CpuTimingReport {
    clock: &'static str,
    calibration: CpuClockCalibration,
    read_failures: u64,
    nonmonotonic_spans: u64,
}

#[allow(unsafe_code)]
fn thread_cpu_ns() -> Option<u64> {
    let mut value = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: value is writable timespec storage and the calling thread owns its clock.
    if unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut value) } != 0 {
        return None;
    }
    u64::try_from(value.tv_sec)
        .ok()?
        .checked_mul(1_000_000_000)?
        .checked_add(u64::try_from(value.tv_nsec).ok()?)
}

fn calibrate_cpu_clock() -> Result<CpuTimingReport, String> {
    let mut overheads = Vec::with_capacity(10_000);
    let mut minimum_increment = u64::MAX;
    for _ in 0..10_000 {
        let wall = Instant::now();
        let start = thread_cpu_ns().ok_or("thread CPU clock unavailable")?;
        let end = thread_cpu_ns().ok_or("thread CPU clock unavailable")?;
        let increment = end
            .checked_sub(start)
            .ok_or("thread CPU clock is nonmonotonic")?;
        if increment != 0 {
            minimum_increment = minimum_increment.min(increment);
        }
        overheads.push(u64::try_from(wall.elapsed().as_nanos()).unwrap_or(u64::MAX));
    }
    overheads.sort_unstable();
    let at = |percent: usize| overheads[(overheads.len() - 1) * percent / 100];
    Ok(CpuTimingReport {
        clock: "CLOCK_THREAD_CPUTIME_ID",
        calibration: CpuClockCalibration {
            paired_reads: overheads.len(),
            minimum_nonzero_ns: if minimum_increment == u64::MAX {
                0
            } else {
                minimum_increment
            },
            p95_ns: at(95),
            p99_ns: at(99),
            max_ns: *overheads.last().unwrap_or(&0),
        },
        read_failures: 0,
        nonmonotonic_spans: 0,
    })
}

impl Window {
    fn now_ns(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    fn contains(&self, receive_ns: u64) -> bool {
        let start = self.start_ns.load(Ordering::Acquire);
        let end = self.end_ns.load(Ordering::Acquire);
        start != 0 && receive_ns >= start && (end == 0 || receive_ns < end)
    }
}

struct ShardState {
    venue: NativeVenue,
    stream: Arc<str>,
    gate: SingleSourceGate,
    next_sequence: u64,
    peers: Vec<PeerState>,
    received_messages: u64,
    decoded_events: u64,
    malformed_messages: u64,
    routing_rejections: u64,
    control_counts: BTreeMap<&'static str, u64>,
    faults: BTreeMap<&'static str, u64>,
    receiver: ReceiverMetrics,
    measured_gate: [u64; 4],
    connections: Vec<ConnectionMetrics>,
    recent: Option<VecDeque<RecentBatch>>,
    pending_tape_frame: Option<Bytes>,
}

struct PeerState {
    generation: u64,
    accepting: bool,
    connected: bool,
    subscription_sent: bool,
    last_heartbeat_ns: Option<u64>,
    partial_ack: bool,
    coverage: BTreeMap<Arc<str>, bool>,
    target_markets: BTreeMap<Arc<str>, Arc<str>>,
}

/// A socket's synchronous, single-owner admission handoff. It exposes no user callback.
#[derive(Clone)]
pub(crate) struct ConnectionContext {
    state: Rc<RefCell<ShardState>>,
    window: Arc<Window>,
    index: usize,
    slot: u16,
    generation: u64,
    source_name: Arc<str>,
    cpu_timing: bool,
    tape: Option<TapeSender>,
}

impl ConnectionContext {
    pub(crate) fn now_ns(&self) -> u64 {
        self.window.now_ns()
    }

    pub(crate) fn stamp(&self) -> StageStamp {
        StageStamp {
            wall_ns: self.now_ns(),
            cpu_ns: self.cpu_timing.then(thread_cpu_ns).flatten(),
        }
    }

    /// Holds one frame's bytes for the next [`ConnectionContext::admit`] line.
    ///
    /// A no-op when `--tape` is off. Only the reference count moves here, on the update path;
    /// the bounded copy of at most [`TAPE_TEXT_BYTES`] bytes plus the truncation mark is made
    /// after the audited stamp, when the line is offered. The next call replaces the handle,
    /// and admission consumes it.
    pub(crate) fn tape_frame(&self, frame: &Bytes) {
        if self.tape.is_none() {
            return;
        }
        self.state.borrow_mut().pending_tape_frame = Some(frame.clone());
    }

    pub(crate) fn heartbeat_origin(&self) -> Instant {
        self.window.origin
    }

    pub(crate) fn heartbeat_timeout(&self, witness: HeartbeatTimeoutWitness) {
        let mut state = self.state.borrow_mut();
        let connection = &mut state.connections[self.index];
        if connection.heartbeat_timeout.is_none() {
            connection.heartbeat_timeout = Some(witness);
        }
    }

    pub(crate) fn message_received(&self) {
        let mut state = self.state.borrow_mut();
        state.received_messages = state.received_messages.saturating_add(1);
        state.connections[self.index].received_messages += 1;
    }

    pub(crate) fn malformed(&self, kind: &'static str) {
        {
            let mut state = self.state.borrow_mut();
            state.malformed_messages = state.malformed_messages.saturating_add(1);
            state.connections[self.index].malformed_messages += 1;
        }
        self.fault(kind);
    }

    #[cfg(test)]
    pub(crate) fn accounting(&self) -> (u64, u64, u64, u64) {
        let state = self.state.borrow();
        (
            state.received_messages,
            state.malformed_messages,
            state.decoded_events,
            state.gate.counters().admitted,
        )
    }

    #[cfg(test)]
    pub(crate) fn health(&self) -> (bool, bool, Option<u64>, usize) {
        let state = self.state.borrow();
        let peer = &state.peers[self.index];
        (
            peer.connected,
            peer.subscription_sent,
            peer.last_heartbeat_ns,
            peer.coverage.values().filter(|covered| **covered).count(),
        )
    }

    #[cfg(test)]
    pub(crate) fn controls(&self, kind: &str) -> u64 {
        self.state
            .borrow()
            .control_counts
            .get(kind)
            .copied()
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub(crate) fn partial_acknowledged(&self) -> bool {
        self.state.borrow().peers[self.index].partial_ack
    }

    #[cfg(test)]
    pub(crate) fn heartbeat_timeout_cause(&self) -> Option<HeartbeatTimeoutCause> {
        self.state.borrow().connections[self.index]
            .heartbeat_timeout
            .as_ref()
            .map(|witness| witness.cause)
    }

    pub(crate) fn source(&self, received: StageStamp) -> NativeSource {
        let state = self.state.borrow();
        NativeSource {
            stream: state.stream.clone(),
            source: self.source_name.clone(),
            slot: self.slot,
            generation: self.generation,
            stream_generation: state.gate.generation(),
            sequence: state.next_sequence,
            received_ns: received.wall_ns,
            validated_ns: 0,
        }
    }

    pub(crate) fn control(&self, kind: &'static str) {
        let mut state = self.state.borrow_mut();
        if state.control_counts.len() < 32 || state.control_counts.contains_key(kind) {
            let count = state.control_counts.entry(kind).or_default();
            *count = count.saturating_add(1);
        }
    }

    pub(crate) fn fault(&self, kind: &'static str) {
        let mut state = self.state.borrow_mut();
        if state.faults.len() < 32 || state.faults.contains_key(kind) {
            let count = state.faults.entry(kind).or_default();
            *count = count.saturating_add(1);
        }
    }

    pub(crate) fn coverage(&self, native_id: &str) {
        let mut state = self.state.borrow_mut();
        let peer = &mut state.peers[self.index];
        if peer.accepting
            && peer.generation == self.generation
            && let Some(covered) = peer.coverage.get_mut(native_id)
        {
            *covered = true;
        }
    }

    /// Records that this generation's subscription was acknowledged for a strict subset of its
    /// targets. The unacknowledged targets stay uncovered, so per-target evidence keeps telling
    /// the truth; the connection is kept, and its health then rests on the markets that were
    /// acknowledged. Fenced to the current, accepting generation like every other evidence
    /// write.
    pub(crate) fn partial_acknowledgement(&self) {
        let mut state = self.state.borrow_mut();
        let peer = &mut state.peers[self.index];
        if peer.accepting && peer.generation == self.generation {
            peer.partial_ack = true;
        }
    }

    pub(crate) fn connected(&self) {
        let mut state = self.state.borrow_mut();
        let peer = &mut state.peers[self.index];
        if peer.accepting && peer.generation == self.generation {
            peer.connected = true;
        }
    }

    pub(crate) fn subscription_sent(&self) {
        let mut state = self.state.borrow_mut();
        let peer = &mut state.peers[self.index];
        if peer.accepting && peer.generation == self.generation {
            peer.subscription_sent = true;
        }
    }

    pub(crate) fn heartbeat_received(&self) {
        let mut state = self.state.borrow_mut();
        let peer = &mut state.peers[self.index];
        if peer.accepting && peer.generation == self.generation {
            peer.last_heartbeat_ns = Some(self.now_ns());
        }
    }

    pub(crate) fn admit(
        &self,
        mut batch: NativeBatch,
        received: StageStamp,
        decoded: StageStamp,
        validated: StageStamp,
    ) -> Result<(), PeerEnd> {
        let mut state = self.state.borrow_mut();
        let frame = state.pending_tape_frame.take();
        state.decoded_events = state
            .decoded_events
            .saturating_add(batch.events.len() as u64);
        if batch.source.received_ns != received.wall_ns
            || !state.peers[self.index].accepting
            || state.peers[self.index].generation != self.generation
            || batch.source.generation != self.generation
            || batch.source.slot != self.slot
            || batch.source.stream != state.stream
            || batch.source.source != self.source_name
            || batch.source.received_ns > decoded.wall_ns
            || decoded.wall_ns > validated.wall_ns
            || validated.wall_ns > self.now_ns()
        {
            return Err(PeerEnd::AdmissionFailed);
        }
        for event in &batch.events {
            if !route_valid(event, &state.peers[self.index]) {
                state.routing_rejections += 1;
                return Err(PeerEnd::RoutingFailed);
            }
        }
        batch.source.sequence = state.next_sequence;
        batch.source.validated_ns = validated.wall_ns;
        let measured = self.window.contains(batch.source.received_ns);
        for event in &batch.events {
            state.connections[self.index].arrivals_all[metrics::family_index(event.family)] += 1;
            if measured {
                state.connections[self.index].arrivals_measured
                    [metrics::family_index(event.family)] += 1;
            }
        }
        let received_ns = received.wall_ns;
        let gate_start = self.stamp();
        let before = gate_counts(&state.gate);
        let decision = state.gate.admit(batch);
        for (index, after) in gate_counts(&state.gate).into_iter().enumerate() {
            let delta = after.saturating_sub(before[index]);
            state.connections[self.index].gate_all[index] += delta;
            if measured {
                state.connections[self.index].gate_measured[index] += delta;
                state.measured_gate[index] = state.measured_gate[index].saturating_add(delta);
            }
        }
        match decision {
            GateDecision::Fault(fault) => {
                let kind = match fault {
                    GateFault::StaleSource => "stale_source",
                    GateFault::Capacity => "admission_capacity",
                    GateFault::InvalidEvent => "invalid_native_event",
                };
                drop(state);
                self.fault(kind);
                Err(PeerEnd::AdmissionFailed)
            }
            GateDecision::Admit(batch) => {
                for event in &batch.events {
                    if matches!(
                        event.family,
                        NativeFamily::LimitlessMarketResolved
                            | NativeFamily::PolymarketMarketResolved
                    ) && let Some(selected_target_index) =
                        event.market.as_ref().and_then(|market| {
                            let peer = &state.peers[self.index];
                            match state.venue {
                                NativeVenue::Limitless => {
                                    peer.coverage.keys().position(|key| key == market)
                                }
                                NativeVenue::Polymarket => peer
                                    .target_markets
                                    .iter()
                                    .position(|(_, condition)| condition == market),
                            }
                        })
                    {
                        *state
                            .control_counts
                            .entry("selected_target_resolved")
                            .or_default() += 1;
                        state.connections[self.index]
                            .first_selected_resolution
                            .get_or_insert(selected_target_index);
                    }
                    state.connections[self.index].published_all
                        [metrics::family_index(event.family)] += 1;
                    if measured {
                        state.connections[self.index].published_measured
                            [metrics::family_index(event.family)] += 1;
                    }
                }
                state.next_sequence = state
                    .next_sequence
                    .checked_add(1)
                    .ok_or(PeerEnd::AdmissionFailed)?;
                let observed = self.stamp();
                if measured {
                    state.receiver.observe(&batch);
                    let audited = self.stamp();
                    state.receiver.record(
                        &batch,
                        Timings {
                            decode: decoded.wall_ns.saturating_sub(received_ns),
                            validate: gate_start.wall_ns.saturating_sub(decoded.wall_ns),
                            gate: observed.wall_ns.saturating_sub(gate_start.wall_ns),
                            receiver: audited.wall_ns.saturating_sub(observed.wall_ns),
                            total: observed.wall_ns.saturating_sub(received_ns),
                            audited_total: audited.wall_ns.saturating_sub(received_ns),
                            cpu: self.cpu_timing.then(|| {
                                CpuTimings::from_stamps(
                                    received.cpu_ns,
                                    decoded.cpu_ns,
                                    gate_start.cpu_ns,
                                    observed.cpu_ns,
                                    audited.cpu_ns,
                                )
                            }),
                        },
                    );
                }
                if (state.recent.is_some() || self.tape.is_some())
                    && let Some(first) = batch.events.first()
                {
                    let family = metrics::family_name(first.family);
                    let market = first.market.as_ref().map(|market| market.to_string());
                    let handoff_ns = observed.wall_ns.saturating_sub(received_ns);
                    let events = batch.events.len();
                    if let Some(recent) = state.recent.as_mut() {
                        if recent.len() == RECENT_CAPACITY {
                            recent.pop_front();
                        }
                        recent.push_back(RecentBatch {
                            received_ns,
                            family,
                            market: market.clone(),
                            events,
                            input_bytes: batch.input_bytes,
                            handoff_ns,
                            generation: self.generation,
                        });
                    }
                    if let Some(tape) = &self.tape {
                        tape.offer(TapeLine {
                            t_ns: received_ns,
                            venue: venue_name(state.venue),
                            stream: state.stream.to_string(),
                            family,
                            market,
                            events,
                            bytes: batch.input_bytes,
                            handoff_ns,
                            generation: self.generation,
                            text: frame
                                .as_deref()
                                .map(truncate_tape_frame)
                                .unwrap_or_default(),
                        });
                    }
                }
                Ok(())
            }
        }
    }
}

fn route_valid(event: &NativeEvent, peer: &PeerState) -> bool {
    match event.family {
        NativeFamily::LimitlessOrderbookUpdate | NativeFamily::LimitlessNewPriceData => event
            .market
            .as_ref()
            .is_some_and(|market| peer.coverage.contains_key(market)),
        NativeFamily::PolymarketBook
        | NativeFamily::PolymarketPriceChange
        | NativeFamily::PolymarketLastTradePrice
        | NativeFamily::PolymarketTickSizeChange
        | NativeFamily::PolymarketBestBidAsk => event.market.as_ref().is_some_and(|market| {
            peer.target_markets
                .values()
                .any(|expected| expected == market)
                && event.assets.iter().any(|asset| {
                    peer.target_markets
                        .get(asset)
                        .is_some_and(|expected| expected == market)
                })
                && event.assets.iter().all(|asset| {
                    peer.target_markets
                        .get(asset)
                        .is_none_or(|expected| expected == market)
                })
        }),
        _ => true,
    }
}

#[derive(Clone, Deserialize, Serialize)]
struct LimitlessSelection {
    slug: String,
    end_epoch: Option<serde_json::Number>,
}

#[derive(Clone, Deserialize, Serialize)]
struct PolymarketSelection {
    condition_id: String,
    clob_token_ids: Vec<String>,
    end_epoch: Option<serde_json::Number>,
}

#[derive(Clone, Deserialize, Serialize)]
struct Selection {
    limitless: Vec<LimitlessSelection>,
    polymarket: Vec<PolymarketSelection>,
}

fn gate_counts(gate: &SingleSourceGate) -> [u64; 4] {
    let c = gate.counters();
    [c.received, c.admitted, c.stale, c.overload]
}

#[derive(Clone)]
struct StreamPlan {
    venue: NativeVenue,
    index: usize,
    targets: Vec<NativeTarget>,
    endpoint: String,
}

#[derive(Default, Clone, Serialize)]
struct WorkerProgress {
    ready_targets: usize,
    required_targets: usize,
    healthy_connections: usize,
    required_connections: usize,
    activity: [u64; 2],
    faults: u64,
    resolved_targets: u64,
}

#[derive(Serialize)]
struct ShardReport {
    venue: &'static str,
    stream: String,
    ready_targets: usize,
    required_targets: usize,
    received_messages: u64,
    decoded_events: u64,
    malformed_messages: u64,
    routing_rejections: u64,
    gate_all: [u64; 4],
    gate_measured: [u64; 4],
    controls: BTreeMap<&'static str, u64>,
    faults: BTreeMap<&'static str, u64>,
    receiver: metrics::ReceiverReport,
    connections: Vec<ConnectionMetrics>,
    health: Vec<ConnectionHealth>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    targets: Vec<TargetHealth>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    recent: Vec<RecentBatch>,
}

/// Per-target subscription evidence for the live demo: the venue-native coverage key, the market
/// it belongs to and whether the current generation has evidenced it. Identifiers only.
#[derive(Clone, Serialize)]
struct TargetHealth {
    id: String,
    market: String,
    covered: bool,
}

/// Identifier-only witness of one admitted batch: venue-native market id, family label, sizes and
/// the receive-to-handoff interval in nanoseconds. No source payload field can enter this type.
#[derive(Clone, Serialize)]
struct RecentBatch {
    received_ns: u64,
    family: &'static str,
    market: Option<String>,
    events: usize,
    input_bytes: usize,
    handoff_ns: u64,
    generation: u64,
}

#[derive(Serialize)]
struct ConnectionHealth {
    generation: u64,
    connected: bool,
    subscription_sent: bool,
    subscription_evidence: &'static str,
    evidenced_targets: usize,
    requested_targets: usize,
    last_heartbeat_ns: Option<u64>,
}

#[derive(Clone, Default, Serialize)]
struct ConnectionMetrics {
    received_messages: u64,
    malformed_messages: u64,
    arrivals_all: [u64; 15],
    arrivals_measured: [u64; 15],
    published_all: [u64; 15],
    published_measured: [u64; 15],
    gate_all: [u64; 4],
    gate_measured: [u64; 4],
    /// Zero-based first selected resolution's index in sorted active subscription coordinates.
    /// Limitless ranks slug keys; Polymarket ranks asset-token keys and picks the first matching
    /// condition.
    /// The enclosing `connections` array index identifies the connection that observed it.
    first_selected_resolution: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    heartbeat_timeout: Option<HeartbeatTimeoutWitness>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HeartbeatTimeoutCause {
    DeadlineElapsed,
    LatePongProcessed,
}

/// First timeout per connection; optional timestamps are nanoseconds from the common process origin.
#[derive(Clone, Serialize)]
pub(crate) struct HeartbeatTimeoutWitness {
    pub(crate) generation: u64,
    pub(crate) observed_ns: u64,
    pub(crate) cause: HeartbeatTimeoutCause,
    pub(crate) ping_enqueued_ns: Option<u64>,
    pub(crate) writer_started_ns: Option<u64>,
    pub(crate) write_completed_ns: Option<u64>,
    pub(crate) receipt_observed_ns: Option<u64>,
    pub(crate) deadline_ns: Option<u64>,
    pub(crate) pong_processed_ns: Option<u64>,
    pub(crate) last_ws_message_received_ns: Option<u64>,
}

fn venue_name(venue: NativeVenue) -> &'static str {
    match venue {
        NativeVenue::Limitless => "limitless",
        NativeVenue::Polymarket => "polymarket",
    }
}

fn peer_state(targets: &[NativeTarget]) -> PeerState {
    PeerState {
        generation: 1,
        accepting: true,
        connected: false,
        subscription_sent: false,
        last_heartbeat_ns: None,
        partial_ack: false,
        coverage: targets
            .iter()
            .map(|target| {
                (
                    target
                        .asset
                        .clone()
                        .unwrap_or_else(|| target.market.clone()),
                    false,
                )
            })
            .collect(),
        target_markets: targets
            .iter()
            .filter_map(|target| {
                target
                    .asset
                    .clone()
                    .map(|asset| (asset, target.market.clone()))
            })
            .collect(),
    }
}

fn shard_state(plan: &StreamPlan, recent: bool) -> Rc<RefCell<ShardState>> {
    Rc::new(RefCell::new(ShardState {
        venue: plan.venue,
        stream: Arc::from(format!("{}-{}", venue_name(plan.venue), plan.index)),
        gate: SingleSourceGate::new(
            GateLimits {
                bytes: 64 * 1_048_576,
                max_input_bytes: MAX_INPUT_BYTES,
                max_value_nodes: 262_144,
                max_value_depth: 20,
                ..GateLimits::default()
            },
            1,
        ),
        next_sequence: 1,
        peers: vec![peer_state(&plan.targets)],
        received_messages: 0,
        decoded_events: 0,
        malformed_messages: 0,
        routing_rejections: 0,
        control_counts: BTreeMap::new(),
        faults: BTreeMap::new(),
        receiver: ReceiverMetrics::new(),
        measured_gate: [0; 4],
        connections: vec![ConnectionMetrics::default()],
        recent: recent.then(|| VecDeque::with_capacity(RECENT_CAPACITY)),
        pending_tape_frame: None,
    }))
}

fn progress(states: &[Rc<RefCell<ShardState>>]) -> WorkerProgress {
    let mut result = WorkerProgress::default();
    for state in states {
        let state = state.borrow();
        for peer in &state.peers {
            result.required_targets += peer.coverage.len();
            result.ready_targets += peer.coverage.values().filter(|value| **value).count();
            if !peer.coverage.is_empty() {
                result.required_connections += 1;
                result.healthy_connections += usize::from(
                    peer.connected
                        && peer.subscription_sent
                        && peer.last_heartbeat_ns.is_some()
                        && (state.venue == NativeVenue::Polymarket
                            || peer.coverage.values().all(|covered| *covered)
                            || (peer.partial_ack
                                && peer.coverage.values().any(|covered| *covered))),
                );
            }
        }
        result.activity[match state.venue {
            NativeVenue::Limitless => 0,
            NativeVenue::Polymarket => 1,
        }] += state.receiver.activity_events;
        result.resolved_targets += state
            .control_counts
            .get("selected_target_resolved")
            .copied()
            .unwrap_or(0);
        result.faults += state.faults.values().sum::<u64>()
            + state.malformed_messages
            + state.routing_rejections
            + state.gate.counters().overload
            + state.receiver.generation_errors
            + state.receiver.member_order_errors
            + state.receiver.sequence_errors;
    }
    result
}

fn shard_report(state: &ShardState) -> ShardReport {
    ShardReport {
        venue: venue_name(state.venue),
        stream: state.stream.to_string(),
        ready_targets: state
            .peers
            .iter()
            .map(|peer| peer.coverage.values().filter(|value| **value).count())
            .sum(),
        required_targets: state.peers.iter().map(|peer| peer.coverage.len()).sum(),
        received_messages: state.received_messages,
        decoded_events: state.decoded_events,
        malformed_messages: state.malformed_messages,
        routing_rejections: state.routing_rejections,
        gate_all: gate_counts(&state.gate),
        gate_measured: state.measured_gate,
        controls: state.control_counts.clone(),
        faults: state.faults.clone(),
        receiver: state.receiver.snapshot(state.venue),
        connections: state.connections.clone(),
        health: state
            .peers
            .iter()
            .map(|peer| ConnectionHealth {
                generation: peer.generation,
                connected: peer.connected,
                subscription_sent: peer.subscription_sent,
                subscription_evidence: match state.venue {
                    NativeVenue::Limitless => "acknowledged",
                    NativeVenue::Polymarket => "observed_data",
                },
                evidenced_targets: peer.coverage.values().filter(|covered| **covered).count(),
                requested_targets: peer.coverage.len(),
                last_heartbeat_ns: peer.last_heartbeat_ns,
            })
            .collect(),
        targets: if state.recent.is_some() {
            state
                .peers
                .iter()
                .flat_map(|peer| {
                    peer.coverage.iter().map(|(id, covered)| TargetHealth {
                        id: id.to_string(),
                        market: peer.target_markets.get(id).unwrap_or(id).to_string(),
                        covered: *covered,
                    })
                })
                .collect()
        } else {
            Vec::new()
        },
        recent: state
            .recent
            .as_ref()
            .map(|tape| tape.iter().cloned().collect())
            .unwrap_or_default(),
    }
}

/// Interim shard reports published for the supervisory thread to serialize.
///
/// The worker overwrites the slot every `interval_ticks` of its one-second timer; an unread
/// report is replaced, never queued.
struct SnapshotSink {
    slot: Arc<Mutex<Option<Vec<ShardReport>>>>,
    interval_ticks: u64,
}

/// Shared handles one worker needs beyond its own plans.
struct WorkerChannels {
    window: Arc<Window>,
    progress: Arc<Mutex<WorkerProgress>>,
    router: Arc<routing::AssignmentRouter>,
    snapshots: Option<SnapshotSink>,
    tape: Option<TapeSender>,
    serve: bool,
}

/// Reconciles one shard's peer to its end, clearing its evidence when it failed.
///
/// Answers the shard index and whether the end was a fault rather than an operator stop. A
/// `delay` defers the start, which `--serve` uses to retry a shard the venue or admission
/// ended; the peer's own attempt ledger bounds how often that retry reaches the venue.
async fn run_shard_peer(
    shard: usize,
    config: PeerConfig,
    context: ConnectionContext,
    subscription: watch::Receiver<Vec<NativeTarget>>,
    mut stop: watch::Receiver<bool>,
    delay: Option<Duration>,
) -> (usize, bool) {
    if let Some(delay) = delay {
        if *stop.borrow() {
            return (shard, false);
        }
        tokio::select! { _ = tokio::time::sleep(delay) => {}, _ = stop.changed() => {} }
    }
    let result = reconcile::run_peer(config, context.clone(), subscription, stop).await;
    match result {
        Err(reason) if reason != PeerEnd::Stopped => {
            context.fault("peer_ended");
            let mut state = context.state.borrow_mut();
            let peer = &mut state.peers[context.index];
            peer.connected = false;
            peer.subscription_sent = false;
            peer.partial_ack = false;
            peer.coverage
                .values_mut()
                .for_each(|covered| *covered = false);
            (shard, true)
        }
        _ => (shard, false),
    }
}

async fn worker(
    plans: Vec<StreamPlan>,
    channels: WorkerChannels,
    mut desired: watch::Receiver<Vec<demand::TargetRef>>,
    mut stop: watch::Receiver<bool>,
    cpu_timing: bool,
) -> Vec<ShardReport> {
    let WorkerChannels {
        window,
        progress: shared,
        router,
        snapshots,
        tape,
        serve,
    } = channels;
    let states: Vec<_> = plans
        .iter()
        .map(|plan| shard_state(plan, snapshots.is_some()))
        .collect();
    let mut tasks = tokio::task::JoinSet::new();
    let mut assignments = Vec::new();
    let mut seeds = Vec::new();
    for (plan, state) in plans.iter().zip(&states) {
        let targets = router.desired(plan.venue, plan.index, &desired.borrow());
        let (assignment, subscription) = watch::channel(targets);
        let config = PeerConfig {
            venue: plan.venue,
            endpoint: plan.endpoint.clone(),
            slot: plan.index as u16,
            generation: 1,
            targets: plan.targets.clone(),
            lifecycle: plan.index == 0,
            min_command_interval: Duration::from_millis(500),
            tolerate_partial_ack: serve,
        };
        let context = ConnectionContext {
            state: state.clone(),
            window: window.clone(),
            index: 0,
            slot: config.slot,
            generation: config.generation,
            source_name: Arc::from("single-source"),
            cpu_timing,
            tape: tape.clone(),
        };
        seeds.push((config.clone(), context.clone()));
        tasks.spawn_local(run_shard_peer(
            assignments.len(),
            config,
            context,
            subscription,
            stop.clone(),
            None,
        ));
        assignments.push(assignment);
    }
    let mut timer = tokio::time::interval(Duration::from_secs(1));
    let mut ticks = 0u64;
    loop {
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() { break; }
            }
            changed = desired.changed() => {
                if changed.is_err() { break; }
                let targets = desired.borrow_and_update();
                for (plan, assignment) in plans.iter().zip(&assignments) {
                    let next = router.desired(plan.venue, plan.index, &targets);
                    if *assignment.borrow() != next {
                        let _ = assignment.send(next);
                    }
                }
            }
            _ = timer.tick() => {
                ticks = ticks.saturating_add(1);
                if let Ok(mut value) = shared.lock() { *value = progress(&states); }
                if let Some(sink) = &snapshots
                    && ticks.is_multiple_of(sink.interval_ticks)
                    && let Ok(mut slot) = sink.slot.lock()
                {
                    *slot = Some(states.iter().map(|state| shard_report(&state.borrow())).collect());
                }
            }
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                match result {
                    Err(_) => { for state in &states { state.borrow_mut().faults.insert("peer_panic",1); } }
                    Ok((shard, true)) if serve => {
                        let (template, context) = &seeds[shard];
                        let mut config = template.clone();
                        config.generation = context.state.borrow().peers[context.index].generation;
                        context.control("peer_respawned");
                        tasks.spawn_local(run_shard_peer(
                            shard,
                            config,
                            context.clone(),
                            assignments[shard].subscribe(),
                            stop.clone(),
                            Some(PEER_RESPAWN_DELAY),
                        ));
                    }
                    Ok(_) => {}
                }
            }
        }
    }
    while let Some(result) = tasks.join_next().await {
        if result.is_err() {
            for state in &states {
                state.borrow_mut().faults.insert("peer_panic", 1);
            }
        }
    }
    if let Ok(mut value) = shared.lock() {
        *value = progress(&states);
    }
    states
        .iter()
        .map(|state| shard_report(&state.borrow()))
        .collect()
}

#[derive(Serialize)]
struct Invocation {
    selection: std::path::PathBuf,
    output: std::path::PathBuf,
    min_seconds: u64,
    max_seconds: u64,
    min_events: u64,
    workers: usize,
    diagnostic: bool,
    cpu_timing: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    control_socket: Option<std::path::PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot: Option<std::path::PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot_seconds: Option<u64>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    serve: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    tape: bool,
}

impl Invocation {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut result = Self {
            selection: Default::default(),
            output: Default::default(),
            min_seconds: 900,
            max_seconds: 7_200,
            min_events: 10_000,
            workers: 2,
            diagnostic: false,
            cpu_timing: false,
            control_socket: None,
            snapshot: None,
            snapshot_seconds: None,
            serve: false,
            tape: false,
        };
        let mut args = args;
        while let Some(arg) = args.next() {
            if arg == "--diagnostic" {
                result.diagnostic = true;
                continue;
            }
            if arg == "--cpu-timing" {
                result.cpu_timing = true;
                continue;
            }
            if arg == "--serve" {
                result.serve = true;
                result.diagnostic = true;
                continue;
            }
            if arg == "--tape" {
                result.tape = true;
                continue;
            }
            let value = args
                .next()
                .ok_or_else(|| format!("missing value for {arg}"))?;
            match arg.as_str() {
                "--selection" => result.selection = value.into(),
                "--output" => result.output = value.into(),
                "--min-seconds" => {
                    result.min_seconds = value.parse().map_err(|_| "invalid min-seconds")?
                }
                "--max-seconds" => {
                    result.max_seconds = value.parse().map_err(|_| "invalid max-seconds")?
                }
                "--min-events" => {
                    result.min_events = value.parse().map_err(|_| "invalid min-events")?
                }
                "--workers" => result.workers = value.parse().map_err(|_| "invalid workers")?,
                "--control-socket" => result.control_socket = Some(value.into()),
                "--snapshot" => result.snapshot = Some(value.into()),
                "--snapshot-seconds" => {
                    result.snapshot_seconds =
                        Some(value.parse().map_err(|_| "invalid snapshot-seconds")?)
                }
                _ => return Err(format!("unknown upstream option {arg}")),
            }
        }
        if result.selection.as_os_str().is_empty() || result.output.as_os_str().is_empty() {
            return Err("usage: pmwsd upstream --selection <descriptor-json> --output <metrics-json> [--control-socket <path>] [--workers 2] [--min-seconds 900] [--max-seconds 7200] [--min-events 10000] [--diagnostic] [--cpu-timing] [--serve] [--snapshot <path>] [--snapshot-seconds 2] [--tape]".into());
        }
        if result.workers == 0
            || result.workers > 8
            || result.min_seconds == 0
            || result.max_seconds < result.min_seconds
            || result.max_seconds > 14_400
            || result.min_events == 0
            || (!result.diagnostic && (result.min_seconds < 900 || result.min_events < 10_000))
            || result
                .snapshot_seconds
                .is_some_and(|value| value == 0 || value > 60)
        {
            return Err("upstream limits violate the declared qualification bounds".into());
        }
        Ok(result)
    }

    /// Seconds between interim snapshots; two unless `--snapshot-seconds` narrows it to 1..=60.
    fn snapshot_interval(&self) -> u64 {
        self.snapshot_seconds.unwrap_or(2)
    }
}

fn expiry(number: &Option<serde_json::Number>) -> Option<u64> {
    let text = number.as_ref()?.to_string();
    let (whole, fractional) = text.split_once('.').unwrap_or((&text, ""));
    if !fractional.bytes().all(|byte| byte == b'0') {
        return None;
    }
    whole.parse().ok()
}

/// Turns one frozen selection into one stream plan per connection.
///
/// Accepts any non-empty selection; either venue may contribute no rows. Each venue's rows are
/// chunked [`ROWS_PER_CONNECTION`] rows to a connection, in file order, and each chunk becomes
/// one [`StreamPlan`] whose index names its stream. A Polymarket condition is one row and
/// contributes two subscription coordinates, one per CLOB token.
///
/// Fails when the selection is empty, when it exceeds [`MAX_SELECTED_TARGETS`] coordinates,
/// when an identifier is empty or longer than 1024 bytes, when a market, condition or token
/// repeats, when a condition does not carry exactly two tokens, when `end_epoch` is absent or
/// not a whole number of seconds, or when a row's remaining lifetime is too short: a
/// measurement run requires every row to outlive `--max-seconds` by 210 seconds, while
/// `--serve` requires only that the row has not already expired, because a served run does not
/// end on a selected target resolving.
fn plans(selection: Selection, invocation: &Invocation) -> Result<Vec<StreamPlan>, String> {
    if selection.limitless.is_empty() && selection.polymarket.is_empty() {
        return Err("the selection names no market".into());
    }
    let total = selection
        .polymarket
        .len()
        .checked_mul(2)
        .and_then(|tokens| tokens.checked_add(selection.limitless.len()))
        .ok_or("selection target count overflows")?;
    if total > MAX_SELECTED_TARGETS {
        return Err(format!(
            "the selection names {total} targets; at most {MAX_SELECTED_TARGETS} are accepted (a Polymarket condition counts two)"
        ));
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| "invalid host wall clock")?
        .as_secs();
    let deadline = if invocation.serve {
        now + 1
    } else {
        now + invocation.max_seconds + 210
    };
    let mut seen = std::collections::BTreeSet::new();
    let mut result = Vec::new();
    for (index, rows) in selection.limitless.chunks(ROWS_PER_CONNECTION).enumerate() {
        let mut targets = Vec::new();
        for row in rows {
            if row.slug.is_empty()
                || row.slug.len() > 1_024
                || !seen.insert(row.slug.clone())
                || expiry(&row.end_epoch).is_none_or(|end| end < deadline)
            {
                return Err("Limitless selection contains invalid/duplicate targets or insufficient remaining lifetime".into());
            }
            targets.push(NativeTarget {
                market: Arc::from(row.slug.as_str()),
                asset: None,
                amm: false,
            });
        }
        result.push(StreamPlan {
            venue: NativeVenue::Limitless,
            index,
            targets,
            endpoint: "wss://ws.limitless.exchange/socket.io/?EIO=4&transport=websocket".into(),
        });
    }
    seen.clear();
    let mut tokens = std::collections::BTreeSet::new();
    for (index, rows) in selection.polymarket.chunks(ROWS_PER_CONNECTION).enumerate() {
        let mut targets = Vec::new();
        for row in rows {
            if row.condition_id.is_empty()
                || row.condition_id.len() > 1_024
                || !seen.insert(row.condition_id.clone())
                || row.clob_token_ids.len() != 2
                || expiry(&row.end_epoch).is_none_or(|end| end < deadline)
            {
                return Err("Polymarket selection contains invalid/duplicate conditions or insufficient remaining lifetime".into());
            }
            for token in &row.clob_token_ids {
                if token.is_empty() || token.len() > 1_024 || !tokens.insert(token.clone()) {
                    return Err("Polymarket selection contains invalid/duplicate token IDs".into());
                }
                targets.push(NativeTarget {
                    market: Arc::from(row.condition_id.as_str()),
                    asset: Some(Arc::from(token.as_str())),
                    amm: false,
                });
            }
        }
        result.push(StreamPlan {
            venue: NativeVenue::Polymarket,
            index,
            targets,
            endpoint: crate::polymarket::DEFAULT_ENDPOINT.into(),
        });
    }
    Ok(result)
}

#[derive(Serialize)]
struct RunReport<'a> {
    schema: &'static str,
    phase: &'static str,
    qualified: bool,
    reason: &'static str,
    invocation: &'a Invocation,
    receive_clock: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    cpu_timing: Option<&'a CpuTimingReport>,
    measured_start_ns: u64,
    measured_end_ns: u64,
    stage_names: [&'static str; 6],
    gate_count_names: [&'static str; 4],
    shards: Vec<&'a ShardReport>,
    capacities: CapacityReport,
    consumed_selection: &'a Selection,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot: Option<SnapshotInfo>,
}

/// Liveness envelope present only on interim snapshots, never on the terminal report.
#[derive(Serialize)]
struct SnapshotInfo {
    elapsed_ns: u64,
    serve: bool,
    interval_seconds: u64,
    tape_dropped: u64,
    window_opened_by: Option<&'static str>,
}

/// Replaces `path` with one serialized report, renaming a sibling temporary into place.
///
/// Runs on the supervisory thread only: no worker and no admission path ever serializes.
fn write_snapshot(path: &std::path::Path, report: &RunReport<'_>) -> Result<(), String> {
    let bytes = serde_json::to_vec(report).map_err(|error| format!("snapshot encode: {error}"))?;
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(".tmp");
    let temporary = std::path::PathBuf::from(temporary);
    std::fs::write(&temporary, &bytes).map_err(|error| format!("snapshot write: {error}"))?;
    std::fs::rename(&temporary, path).map_err(|error| format!("snapshot rename: {error}"))
}

#[derive(Serialize)]
struct CapacityReport {
    shards: usize,
    connections_per_shard: usize,
    max_input_bytes: usize,
    admission_bytes_per_shard: usize,
    atomic_events: usize,
    typed_nodes: usize,
    typed_depth: usize,
    allocator: &'static str,
    declared_data_bytes: usize,
}

const STAGE_NAMES: [&str; 6] = [
    "json_decode",
    "typed_validation",
    "admission_gate",
    "observer_audit",
    "receive_to_typed_handoff",
    "receive_to_audited_observation",
];
const GATE_COUNT_NAMES: [&str; 4] = ["received", "admitted", "stale", "overload"];

/// Declares this run's bounds: `shards` is the planned connection count, and the declared data
/// bytes are that many admission buffers.
fn capacities(shards: usize) -> CapacityReport {
    CapacityReport {
        shards,
        connections_per_shard: 1,
        max_input_bytes: MAX_INPUT_BYTES,
        admission_bytes_per_shard: 64 * 1_048_576,
        atomic_events: 4_096,
        typed_nodes: 262_144,
        typed_depth: 20,
        allocator: "system; bounded transient batches; no identity or payload history",
        declared_data_bytes: shards.saturating_mul(64 * 1_048_576),
    }
}

fn calibrate_clock() -> String {
    let mut minimum = u128::MAX;
    let mut maximum = 0;
    for _ in 0..10_000 {
        let start = Instant::now();
        let elapsed = start.elapsed().as_nanos();
        if elapsed != 0 {
            minimum = minimum.min(elapsed);
        }
        maximum = maximum.max(elapsed);
    }
    format!(
        "std::time::Instant; common process origin; nanoseconds; 10000 idle paired reads: minimum_nonzero_ns={}, maximum_ns={maximum}; overhead not subtracted",
        if minimum == u128::MAX { 0 } else { minimum }
    )
}

/// Runs fixed-workload upstream qualification and writes only terminal numeric metrics.
///
/// Configuration/file work occurs off the update path. This is not a production IPC consumer
/// or a production consumer transport. Controller changes invalidate this fixed-workload run.
///
/// `--snapshot <path>` additionally republishes the same report shape every
/// `--snapshot-seconds` (default 2, bounds 1..=60) while the run is live, and `--serve` keeps a
/// diagnostic run alive across faults, resolutions and health loss until an operator signal or
/// `--max-seconds` since the process origin, respawning one failed peer after
/// [`PEER_RESPAWN_DELAY`] and opening the measured window at [`SERVE_WINDOW_TIMEOUT_NS`] when
/// full readiness never arrives. `--tape` writes one JSON line per admitted batch to stdout
/// through a bounded queue, dropping lines a slow reader cannot keep up with. No flag qualifies
/// a run.
pub async fn run_cli(args: impl Iterator<Item = String>) -> Result<(), String> {
    let invocation = Invocation::parse(args)?;
    let mut cpu_timing = invocation
        .cpu_timing
        .then(calibrate_cpu_clock)
        .transpose()?;
    if invocation.output.exists() {
        return Err("metrics output already exists; choose a new path".into());
    }
    let mut bytes = Vec::new();
    std::fs::File::open(&invocation.selection)
        .map_err(|error| format!("selection read: {error}"))?
        .take(4 * 1_048_576 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("selection read: {error}"))?;
    if bytes.len() > 4 * 1_048_576 {
        return Err("selection file exceeds 4 MiB".into());
    }
    let selection: Selection =
        serde_json::from_slice(&bytes).map_err(|error| format!("selection schema: {error}"))?;
    let plans = plans(selection.clone(), &invocation)?;
    let planned_targets: usize = plans.iter().map(|plan| plan.targets.len()).sum();
    let planned_connections = plans.len();
    let router = Arc::new(routing::AssignmentRouter::new(&plans));
    let initial = router.initial();
    let (desired, mut demand_updates) = watch::channel(initial.clone());
    let control_socket = invocation
        .control_socket
        .as_ref()
        .map(|path| routing::ControlSocket::bind(path))
        .transpose()?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .map_err(|error| format!("interrupt handler: {error}"))?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|error| format!("termination handler: {error}"))?;
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&invocation.output)
        .map_err(|error| format!("metrics output: {error}"))?;
    let window = Arc::new(Window {
        origin: Instant::now(),
        start_ns: AtomicU64::new(0),
        end_ns: AtomicU64::new(0),
    });
    let (stop, receiver) = watch::channel(false);
    let (_control_guard, mut control_task) = match control_socket {
        Some((guard, listener)) => (
            Some(guard),
            Some(tokio::spawn(control::serve(
                listener,
                initial.clone(),
                desired.clone(),
                receiver.clone(),
            ))),
        ),
        None => (None, None),
    };
    let receive_clock = calibrate_clock();
    let mut groups = vec![Vec::new(); invocation.workers];
    for (index, plan) in plans.into_iter().enumerate() {
        groups[index % invocation.workers].push(plan);
    }
    let mut workers = Vec::new();
    let mut statuses = Vec::new();
    let snapshot_path = invocation.snapshot.clone();
    let snapshot_interval = invocation.snapshot_interval();
    let mut snapshot_slots = Vec::new();
    let mut snapshot_latest: Vec<Option<Vec<ShardReport>>> =
        (0..invocation.workers).map(|_| None).collect();
    let mut snapshot_reported = false;
    let tape_dropped = Arc::new(AtomicU64::new(0));
    let (tape, tape_thread) = if invocation.tape {
        let (sender, handle) = spawn_tape(tape_dropped.clone())?;
        (Some(sender), Some(handle))
    } else {
        (None, None)
    };
    for (index, group) in groups.into_iter().enumerate() {
        let shared = Arc::new(Mutex::new(WorkerProgress::default()));
        statuses.push(shared.clone());
        let window = window.clone();
        let receiver = receiver.clone();
        let desired = desired.subscribe();
        let router = router.clone();
        let cpu_timing_enabled = cpu_timing.is_some();
        let tape_for_worker = tape.clone();
        let snapshots = snapshot_path.as_ref().map(|_| {
            let slot = Arc::new(Mutex::new(None));
            snapshot_slots.push(slot.clone());
            SnapshotSink {
                slot,
                interval_ticks: snapshot_interval,
            }
        });
        let spawned = std::thread::Builder::new()
            .name(format!("native-{index}"))
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| format!("worker runtime: {error}"))?;
                let local = tokio::task::LocalSet::new();
                Ok::<_, String>(runtime.block_on(local.run_until(worker(
                    group,
                    WorkerChannels {
                        window,
                        progress: shared,
                        router,
                        snapshots,
                        tape: tape_for_worker,
                        serve: invocation.serve,
                    },
                    desired,
                    receiver,
                    cpu_timing_enabled,
                ))))
            });
        match spawned {
            Ok(worker) => workers.push(worker),
            Err(error) => {
                let _ = stop.send(true);
                for worker in workers {
                    let _ = worker.join();
                }
                if let Some(task) = control_task.take() {
                    let _ = task.await;
                }
                return Err(format!("worker spawn: {error}"));
            }
        }
    }
    let mut ready_since = None;
    let mut window_opened_by = None;
    let mut ticks = 0u64;
    let mut reason;
    let mut qualified = false;
    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            _ = interrupt.recv() => { reason = "operator_interrupt"; break; }
            _ = terminate.recv() => { reason = "operator_terminate"; break; }
            change = demand_updates.changed() => {
                if change.is_err() || *demand_updates.borrow_and_update() != initial {
                    reason = "frozen_selection_changed";
                    break;
                }
                continue;
            }
        }
        ticks += 1;
        let values: Vec<_> = statuses
            .iter()
            .map(|value| value.lock().map(|value| value.clone()).unwrap_or_default())
            .collect();
        let ready: usize = values.iter().map(|value| value.ready_targets).sum();
        let required: usize = values.iter().map(|value| value.required_targets).sum();
        let healthy: usize = values.iter().map(|value| value.healthy_connections).sum();
        let connections: usize = values.iter().map(|value| value.required_connections).sum();
        let faults: u64 = values.iter().map(|value| value.faults).sum();
        let activity = [0, 1].map(|i| values.iter().map(|value| value.activity[i]).sum::<u64>());
        let now = window.now_ns();
        let start = window.start_ns.load(Ordering::Acquire);
        if control_task.as_ref().is_some_and(|task| task.is_finished()) {
            reason = "controller_ended";
            break;
        }
        if workers.iter().any(|worker| worker.is_finished()) {
            reason = "worker_ended";
            break;
        }
        if ticks.is_multiple_of(10) {
            eprintln!(
                "upstream elapsed={}s evidence={}/{} healthy_connections={}/{} activity={:?} faults={}",
                ticks, ready, required, healthy, connections, activity, faults
            );
        }
        if let Some(path) = &snapshot_path {
            for (index, slot) in snapshot_slots.iter().enumerate() {
                if let Ok(mut value) = slot.lock()
                    && let Some(reports) = value.take()
                {
                    snapshot_latest[index] = Some(reports);
                }
            }
            if ticks.is_multiple_of(snapshot_interval)
                && snapshot_latest.iter().all(Option::is_some)
            {
                let live = RunReport {
                    schema: "pm-ws-native-upstream-v2",
                    phase: "A",
                    qualified: false,
                    reason: "snapshot",
                    invocation: &invocation,
                    receive_clock: &receive_clock,
                    cpu_timing: cpu_timing.as_ref(),
                    measured_start_ns: start,
                    measured_end_ns: 0,
                    stage_names: STAGE_NAMES,
                    gate_count_names: GATE_COUNT_NAMES,
                    shards: snapshot_latest.iter().flatten().flatten().collect(),
                    capacities: capacities(planned_connections),
                    consumed_selection: &selection,
                    snapshot: Some(SnapshotInfo {
                        elapsed_ns: now,
                        serve: invocation.serve,
                        interval_seconds: snapshot_interval,
                        tape_dropped: tape_dropped.load(Ordering::Relaxed),
                        window_opened_by,
                    }),
                };
                if let Err(error) = write_snapshot(path, &live)
                    && !snapshot_reported
                {
                    snapshot_reported = true;
                    eprintln!("upstream snapshot unavailable: {error}");
                }
            }
        }
        if faults != 0 && !invocation.serve {
            reason = "observed_fault";
            break;
        }
        if !invocation.serve && values.iter().any(|value| value.resolved_targets != 0) {
            reason = "selected_target_resolved";
            break;
        }
        let ready_now = required == planned_targets
            && connections == planned_connections
            && healthy == connections;
        if ready_now {
            if ready_since.is_none() {
                ready_since = Some(now);
            }
        } else if start != 0 && !invocation.serve {
            reason = "connection_health_lost";
            break;
        } else if start == 0 {
            ready_since = None;
        }
        if start == 0
            && let Some(cause) = window_open_cause(invocation.serve, ready_now, ready_since, now)
        {
            window.start_ns.store(now, Ordering::Release);
            window_opened_by = Some(cause);
            eprintln!("upstream measured window started by {cause}");
        }
        if !invocation.serve && start == 0 && now >= 180_000_000_000 {
            reason = "readiness_timeout";
            break;
        }
        if invocation.serve {
            if now >= invocation.max_seconds * 1_000_000_000 {
                reason = "serve_max_seconds";
                break;
            }
        } else if start != 0 {
            let elapsed = now.saturating_sub(start);
            if elapsed >= invocation.min_seconds * 1_000_000_000
                && activity.iter().all(|value| *value >= invocation.min_events)
            {
                qualified = !invocation.diagnostic;
                reason = if qualified {
                    "healthy_floors_reached"
                } else {
                    "diagnostic_only"
                };
                break;
            }
            if elapsed >= invocation.max_seconds * 1_000_000_000 {
                reason = "insufficient_samples";
                break;
            }
        }
    }
    let operator_stop = matches!(reason, "operator_interrupt" | "operator_terminate");
    let end = window.now_ns();
    window.end_ns.store(end, Ordering::Release);
    let _ = stop.send(true);
    if let Some(task) = control_task.take()
        && !matches!(task.await, Ok(Ok(())))
    {
        qualified = false;
        reason = "controller_failed";
    }
    let mut shards = Vec::new();
    let mut worker_failure = None;
    for worker in workers {
        match worker.join() {
            Ok(Ok(reports)) => shards.extend(reports),
            Ok(Err(error)) => worker_failure = worker_failure.or(Some(error)),
            Err(_) => {
                worker_failure =
                    worker_failure.or_else(|| Some("native worker panicked".to_owned()))
            }
        }
    }
    drop(tape);
    if let Some(handle) = tape_thread {
        let _ = handle.join();
    }
    if let Some(error) = worker_failure {
        return Err(error);
    }
    if shards.iter().any(|shard| {
        shard
            .controls
            .get("selected_target_resolved")
            .copied()
            .unwrap_or(0)
            != 0
    }) {
        qualified = false;
        reason = "selected_target_resolved";
    }
    if let Some(cpu_timing) = &mut cpu_timing {
        cpu_timing.read_failures = shards.iter().fold(0u64, |total, shard| {
            total.saturating_add(shard.receiver.cpu_missing_reads)
        });
        cpu_timing.nonmonotonic_spans = shards.iter().fold(0u64, |total, shard| {
            total.saturating_add(shard.receiver.cpu_nonmonotonic_spans)
        });
    }
    if shards.iter().any(|shard| {
        shard.receiver.events != shard.gate_measured[1]
            || shard.receiver.sequence_errors != 0
            || shard.receiver.generation_errors != 0
            || shard.receiver.member_order_errors != 0
            || !shard.faults.is_empty()
    }) {
        if qualified {
            reason = "terminal_accounting_fault";
        }
        qualified = false;
    }
    let report = RunReport {
        schema: "pm-ws-native-upstream-v2",
        phase: "A",
        qualified,
        reason,
        invocation: &invocation,
        receive_clock: &receive_clock,
        cpu_timing: cpu_timing.as_ref(),
        measured_start_ns: window.start_ns.load(Ordering::Acquire),
        measured_end_ns: if window.start_ns.load(Ordering::Acquire) == 0 {
            0
        } else {
            end
        },
        stage_names: STAGE_NAMES,
        gate_count_names: GATE_COUNT_NAMES,
        shards: shards.iter().collect(),
        capacities: capacities(planned_connections),
        consumed_selection: &selection,
        snapshot: None,
    };
    serde_json::to_writer_pretty(file, &report)
        .map_err(|error| format!("metrics serialization: {error}"))?;
    if report.qualified || (invocation.serve && operator_stop) {
        Ok(())
    } else {
        Err(format!("upstream not qualified: {}", report.reason))
    }
}

#[cfg(test)]
pub(crate) fn test_context(targets: Vec<NativeTarget>) -> ConnectionContext {
    let venue = if targets.iter().any(|target| target.asset.is_some()) {
        NativeVenue::Polymarket
    } else {
        NativeVenue::Limitless
    };
    let peer = peer_state(&targets);
    ConnectionContext {
        state: Rc::new(RefCell::new(ShardState {
            venue,
            stream: Arc::from("test"),
            gate: SingleSourceGate::new(GateLimits::default(), 1),
            next_sequence: 1,
            peers: vec![peer],
            received_messages: 0,
            decoded_events: 0,
            malformed_messages: 0,
            routing_rejections: 0,
            control_counts: BTreeMap::new(),
            faults: BTreeMap::new(),
            receiver: ReceiverMetrics::new(),
            measured_gate: [0; 4],
            connections: vec![ConnectionMetrics::default()],
            recent: None,
            pending_tape_frame: None,
        })),
        window: Arc::new(Window {
            origin: Instant::now(),
            start_ns: AtomicU64::new(1),
            end_ns: AtomicU64::new(0),
        }),
        index: 0,
        slot: 0,
        generation: 1,
        source_name: Arc::from("single-source"),
        cpu_timing: false,
        tape: None,
    }
}

#[cfg(test)]
mod resolution_witness_tests {
    use super::*;

    fn target(market: &str, asset: Option<&str>) -> NativeTarget {
        NativeTarget {
            market: Arc::from(market),
            asset: asset.map(Arc::from),
            amm: false,
        }
    }

    fn resolved_batch(
        context: &ConnectionContext,
        venue: NativeVenue,
        family: NativeFamily,
        market: &str,
    ) -> NativeBatch {
        let received = context.stamp();
        NativeBatch {
            source: context.source(received),
            input_bytes: 1,
            events: vec![NativeEvent {
                venue,
                family,
                family_name: None,
                market: Some(Arc::from(market)),
                assets: Vec::new(),
                identity: None,
                payload: crate::native::NativePayload::from_document(
                    crate::native::document::NativeDocument::parse(
                        br#""payload-not-retained""#,
                        crate::wire::lexical::LexicalLimits::venue_payload(),
                        crate::DecimalGrammar::new(18, 30, false, false).unwrap(),
                    )
                    .unwrap(),
                ),
                member_index: 0,
            }],
        }
    }

    #[test]
    fn first_selected_resolution_reports_its_coordinate_without_retaining_payload() {
        let context = test_context(vec![
            target("selected-coordinate", None),
            target("first-coordinate", None),
        ]);
        for _ in 0..2 {
            let batch = resolved_batch(
                &context,
                NativeVenue::Limitless,
                NativeFamily::LimitlessMarketResolved,
                "selected-coordinate",
            );
            let received = StageStamp {
                wall_ns: batch.source.received_ns,
                cpu_ns: None,
            };
            assert_eq!(
                context.admit(
                    batch,
                    received,
                    context.stamp(),
                    StageStamp {
                        wall_ns: context.now_ns(),
                        cpu_ns: None
                    },
                ),
                Ok(())
            );
        }
        let state = context.state.borrow();
        assert_eq!(state.receiver.events, 2);
        assert_eq!(
            state.control_counts.get("selected_target_resolved"),
            Some(&2)
        );
        let report = shard_report(&state);
        assert_eq!(report.connections[0].first_selected_resolution, Some(1));
        let encoded = serde_json::to_string(&report).unwrap();
        assert!(!encoded.contains("selected-coordinate"));
        assert!(!encoded.contains("payload-not-retained"));
    }

    #[test]
    fn polymarket_resolution_uses_the_first_sorted_asset_coordinate() {
        let context = test_context(vec![
            target("condition-b", Some("token-z")),
            target("condition-b", Some("token-y")),
            target("condition-a", Some("token-x")),
            target("condition-a", Some("token-w")),
        ]);
        let batch = resolved_batch(
            &context,
            NativeVenue::Polymarket,
            NativeFamily::PolymarketMarketResolved,
            "condition-b",
        );
        let received = StageStamp {
            wall_ns: batch.source.received_ns,
            cpu_ns: None,
        };
        assert_eq!(
            context.admit(
                batch,
                received,
                context.stamp(),
                StageStamp {
                    wall_ns: context.now_ns(),
                    cpu_ns: None
                },
            ),
            Ok(())
        );
        assert_eq!(
            shard_report(&context.state.borrow()).connections[0].first_selected_resolution,
            Some(2)
        );
    }
}

#[cfg(test)]
mod heartbeat_witness_tests {
    use super::*;

    fn targets() -> Vec<NativeTarget> {
        vec![NativeTarget {
            market: Arc::from("m"),
            asset: Some(Arc::from("a")),
            amm: false,
        }]
    }

    #[test]
    fn timeout_witness_is_first_failure_and_survives_fencing() {
        let context = test_context(targets());
        context.heartbeat_timeout(witness(HeartbeatTimeoutCause::DeadlineElapsed));
        {
            let mut state = context.state.borrow_mut();
            state.peers[0].connected = false;
            state.peers[0].accepting = false;
        }
        context.heartbeat_timeout(witness(HeartbeatTimeoutCause::LatePongProcessed));
        context.state.borrow_mut().peers[0] = peer_state(&targets());
        let state = context.state.borrow();
        let witness = state.connections[0].heartbeat_timeout.as_ref().unwrap();
        assert!(matches!(
            witness.cause,
            HeartbeatTimeoutCause::DeadlineElapsed
        ));
        assert_eq!(witness.generation, 1);
        assert!(witness.deadline_ns.is_some());
        assert!(witness.pong_processed_ns.is_none());
    }

    #[test]
    fn late_pong_witness_serializes_numeric_fields_without_payload() {
        let context = test_context(targets());
        context.heartbeat_timeout(witness(HeartbeatTimeoutCause::LatePongProcessed));
        let encoded = serde_json::to_value(shard_report(&context.state.borrow())).unwrap();
        let witness = &encoded["connections"][0]["heartbeat_timeout"];
        assert_eq!(witness["cause"], "late_pong_processed");
        assert!(witness["generation"].is_u64());
        assert!(witness["pong_processed_ns"].is_u64());
        assert!(witness["last_ws_message_received_ns"].is_null());
        assert!(!encoded.to_string().contains("payload"));
    }

    #[test]
    fn successful_heartbeat_has_no_failure_witness() {
        let context = test_context(targets());
        let encoded = serde_json::to_value(shard_report(&context.state.borrow())).unwrap();
        assert!(encoded["connections"][0].get("heartbeat_timeout").is_none());
    }

    fn witness(cause: HeartbeatTimeoutCause) -> HeartbeatTimeoutWitness {
        HeartbeatTimeoutWitness {
            generation: 1,
            observed_ns: 10,
            cause,
            ping_enqueued_ns: Some(1),
            writer_started_ns: Some(2),
            write_completed_ns: Some(3),
            receipt_observed_ns: Some(4),
            deadline_ns: Some(5),
            pong_processed_ns: matches!(cause, HeartbeatTimeoutCause::LatePongProcessed)
                .then_some(6),
            last_ws_message_received_ns: None,
        }
    }
}

//! The local control protocol between the `pmwsd` daemon and the `pmwsctl` operator tool.
//!
//! One request per line, one response per line, JSON in both directions, over a Unix domain
//! socket. Line-delimited because it is the smallest framing that both a `tokio` reader and
//! a three-line shell pipeline can speak, and because a control conversation is one command
//! and one answer rather than a stream.
//!
//! Commands are idempotent desired-state operations, as `docs/design.md` "Subscription
//! control" requires: the caller submits what it wants subscribed, never a sequence of
//! mutations, and repeating a command it already sent changes nothing. Every answer is
//! per market, so a batch that is partly rejected reports which part.
//!
//! Nothing in this protocol carries market data. Controllers submit desired market sets;
//! they never inject book state.

use crate::limitless::shard::{
    MarketOutcome, MarketReport, PoolState, PublishLatencySummary, QueueAgeSummary,
};
use serde::{Deserialize, Serialize};

/// How many market rows one `status` page carries.
///
/// A status answer is one line, and a line is bounded by [`MAX_CONTROL_LINE_BYTES`]; a
/// deployment of a few hundred markets would exceed that in a single answer. Paging keeps
/// every line comfortably inside the cap — a full page of rows is a few tens of kilobytes
/// against a 64 KiB bound — whatever the market count, so the cap never has to be raised to
/// match the deployment and a truncated answer is never mistaken for a short one.
pub const STATUS_PAGE_MARKETS: usize = 128;

/// The most bytes one control request line may occupy, including its newline.
///
/// A request is a command name and a batch of slugs; this bounds the batch rather than
/// describing a typical one. A reader stops at this many bytes rather than growing a buffer
/// for whatever a local client sends, so a client that never sends a newline costs a bounded
/// allocation and a refused request.
pub const MAX_CONTROL_LINE_BYTES: usize = 65_536;

/// What an operator or a consumer asks of the daemon.
///
/// `add` and `remove` name venue-native market slugs; the daemon answers with one
/// [`MarketOutcome`] per slug in the order they were given. `status` takes no argument.
///
/// Two kinds of caller share this protocol, and the difference is the connection rather
/// than the vocabulary. An operator sends one command and closes. A consumer holds the
/// connection open: an [`Self::Attach`] on it takes a lease on the market, a session may hold
/// many such leases at once, and [`Self::Release`] drops one of them without closing the
/// session or disturbing the others. Every lease a connection still holds is released when it
/// closes. That makes the socket the outer bound on every lease's lifetime, which the
/// operating system reports whether the consumer exited cleanly or crashed, while an explicit
/// release lets a session shed a market it no longer wants without giving up the rest.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
pub enum ControlRequest {
    /// Add these markets to the daemon's pinned desired set.
    Add { markets: Vec<String> },
    /// Remove these markets from the daemon's pinned desired set.
    ///
    /// Only the pin: a market consumer sessions still lease stays, and the answer for it is
    /// the state it is actually in rather than
    /// [`crate::limitless::shard::MarketStatus::Removed`], which would say the market had
    /// gone when it had not.
    Remove { markets: Vec<String> },
    /// Report the daemon's identity, its shards, and one page of the markets they hold.
    ///
    /// `after` is the last slug the caller has already seen, and the answer carries the next
    /// [`STATUS_PAGE_MARKETS`] rows whose slug sorts after it. It is the market's own
    /// identifier rather than an offset because the set changes underneath a walk: an offset
    /// names a position in a list that shrinks when a market is removed, so the row that
    /// moved into that position is never reported, and a market present throughout the walk
    /// can be silently omitted. A key cursor names a market, so a page boundary is stable
    /// whatever else was added or removed between two requests.
    ///
    /// The first page carries no cursor, and it is the only page carrying the shard
    /// summaries. [`DaemonStatus::more`] says whether another page follows.
    Status {
        #[serde(default)]
        after: Option<String>,
    },
    /// Hand this caller the shared-memory segment carrying `market`, as file descriptors on
    /// this same connection.
    ///
    /// The one request of this protocol whose answer is not only a line: the daemon replies
    /// with [`ControlResponse::Attached`] and, in the same message, the segment's own
    /// descriptor — opened read-only by the daemon when it created the file, and never
    /// re-opened by name to serve a request. Possession of the descriptor is the authorization
    /// (`docs/notes/shared-memory-model.md` §4.2), so the daemon checks the connected peer's
    /// effective user before it transfers anything, and a caller never needs, and never
    /// receives, a path it could race. The sibling doorbell page's descriptor is never
    /// transferred; [`Attachment::descriptors`] says why.
    ///
    /// It **is** a subscription, for as long as this connection lasts. Attaching takes a
    /// lease on `market`: a market no session leases and no operator pinned is added to the
    /// daemon's desired set to serve this attach, and released when the last lease on it
    /// goes. Leases are counted per session, so a session that attaches to the same market
    /// twice holds one lease on it, and [`Self::Release`] — or the session's own close — is
    /// what ends it. Operator pins dominate: a pinned market never leaves on a lease's death.
    ///
    /// The descriptor is served as soon as the daemon holds the market, which is before the
    /// venue has necessarily said anything about it. That is the same attachment a quiet
    /// market has always produced: the segment carries a slot that reports the book as
    /// synchronizing until its first venue base lands, and the consumer reads it. Nothing
    /// here waits on a venue.
    Attach { market: String },
    /// Give up this session's lease on `market`, without closing the session or touching any
    /// other lease it holds.
    ///
    /// Idempotent: releasing a market this session never leased, or releasing it a second
    /// time, answers [`ControlResponse::Released`] rather than an error — the same
    /// caller-cannot-wedge-open-or-close-twice guarantee [`Self::Attach`] already gives on
    /// the way in. Only this session's own lease is touched, so another session's lease on
    /// the same market, and an operator's pin on it, are both untouched: the market leaves
    /// the desired set only once nothing at all wants it any more. `market` is validated
    /// exactly as [`Self::Attach`] validates it — an identifier no shard would accept is
    /// refused as [`ControlResponse::Markets`] carrying
    /// [`crate::limitless::shard::MarketRejection::InvalidIdentifier`], never applied as a
    /// release.
    Release { market: String },
    /// Refresh this connection's leases, so a session that is holding markets but has
    /// nothing to ask for does not expire under the configured lease TTL.
    ///
    /// Any request on a session renews it; this is the one that does nothing else. A daemon
    /// configured with no TTL still answers it, because a client cannot see the daemon's
    /// configuration and renewing is not an error there.
    Renew,
}

/// What the daemon answers.
///
/// [`Self::Busy`] is retryable and means the command was never applied: a bounded control
/// queue was full, so nothing changed. [`Self::Error`] is not retryable by itself — it names
/// a request the daemon will refuse the same way again.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum ControlResponse {
    /// One outcome per requested market, in request order.
    Markets { markets: Vec<MarketOutcome> },
    /// The daemon's current state.
    Status { status: DaemonStatus },
    /// A bounded queue was full. Nothing was applied; the same request may be retried.
    Busy { message: String },
    /// The request was refused, with the reason.
    Error { message: String },
    /// One [`ControlRequest::Attach`]: what the descriptors that arrive with this line are.
    Attached { attachment: Attachment },
    /// One [`ControlRequest::Release`]: how many market leases this session holds after it.
    Released { leases: u32 },
    /// One [`ControlRequest::Renew`]: how many market leases this session now holds.
    Renewed { leases: u32 },
}

/// What a consumer is promised about the descriptors riding the answer that carries this.
///
/// Every field is something the consumer must check the mapped segment against before it
/// reads a byte of it (`docs/notes/shared-memory-model.md` §4.4 step 3): the header it
/// validates has to declare this same daemon instance and this same segment generation, or
/// the descriptor names some other segment and the consumer must reattach. The line is a
/// promise; the header is the authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Attachment {
    /// Which shard holds this market, matching [`ShardReport::shard`].
    pub shard: usize,
    /// The segment's file name, as [`ShardReport::segment`] reports it. Diagnostic: the
    /// consumer reads through the descriptor and never opens this name.
    pub segment: String,
    /// The formatting daemon instance's 128-bit identity, as 32 lower-case hexadecimal
    /// digits — the segment header's own `daemon_instance_id`, written as text because JSON
    /// numbers do not carry 128 bits.
    pub instance_id: String,
    /// The segment generation the header must declare.
    pub segment_generation: u64,
    /// Where this segment's doorbell lives, which is what decides whether this attachment can
    /// be parked on at all.
    ///
    /// [`DoorbellLocation::InHeader`] reaches the doorbell through the read-only mapping the
    /// transferred descriptor produces, so parking works. [`DoorbellLocation::Page`] puts it
    /// in a sibling file this channel never transfers (see [`Self::descriptors`]), so an
    /// attachment made this way spins or polls; its first park is the typed
    /// `WaitFault::DoorbellUnavailable` a reader whose page failed to open already gets.
    pub doorbell: DoorbellLocation,
    /// How many descriptors ride this answer: always `1`, the segment's own, opened read-only
    /// by the daemon.
    ///
    /// A sibling doorbell page is never transferred, whatever [`Self::doorbell`] names. The
    /// wait primitive parks only on a writable mapping, and a writable mapping carries a
    /// writable *length*: a descriptor for that page is a truncation capability over an object
    /// the daemon's own writer stores through, and the store after a truncation is a `SIGBUS`
    /// that ends ingestion. Until this ABI has a sealed or otherwise non-resizable shared
    /// object on every platform it targets, the page stays behind the daemon's own file
    /// permissions and is reached only by a same-user consumer that opens it by name.
    ///
    /// It is still on the wire, and still checked, because it is the promise the arriving
    /// message is validated against: a reply carrying any other number of descriptors is a
    /// transfer that did not keep its promise and is refused rather than partly adopted.
    pub descriptors: u8,
    /// The lease TTL this daemon expires silent sessions under, in milliseconds, or `0` for a
    /// daemon that expires none.
    ///
    /// The renewal deadline is part of the attach contract rather than a guess each consumer
    /// makes, because the consumer is the only side that can miss it and the daemon is the
    /// only side that knows it. A fixed client cadence is wrong for every TTL shorter than
    /// itself: the daemon measures silence from the request it just answered, so a consumer
    /// renewing more slowly than the configured TTL loses the leases it is still using, and
    /// goes on reading a segment nothing maintains. A consumer therefore renews inside a
    /// fraction of this — [`crate::daemon::MIN_LEASE_TTL_MS`] is what makes such a fraction
    /// large enough to be schedulable — and a `0` here says no renewal is needed at all.
    ///
    /// Required, not optional: an answer without it comes from a daemon that predates the
    /// contract and cannot say what its consumers owe it, which is a typed decoding refusal
    /// at the client rather than a silent fall back to a cadence that may be too slow.
    pub lease_ttl_ms: u64,
}

/// Where an attached segment's doorbell cell lives, mirroring the feature bit its header
/// declares.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DoorbellLocation {
    /// Inside the segment's own header, reachable through the consumer's read-only mapping.
    InHeader,
    /// In the sibling page beside the segment, which a consumer needs a second descriptor for
    /// because the platform's wait primitive will not park on a read-only mapping.
    Page,
}

/// The daemon as an operator sees it.
///
/// `pid` is reported so the caller can attribute resident memory to this process without the
/// daemon doing any measurement of its own: `pmwsctl` reads the operating system's own
/// accounting for that pid. Keeping it on the caller's side is what keeps blocking process
/// inspection off the daemon entirely.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DaemonStatus {
    pub pid: u32,
    /// Whether a further page follows this one, to be asked for with the last slug below as
    /// the cursor.
    pub more: bool,
    /// The shard summaries, carried on the first page only.
    pub shards: Vec<ShardReport>,
    /// This page's market rows, in slug order across every shard, so a cursor names one
    /// position in one sequence however many shards produced it.
    pub markets: Vec<MarketRow>,
    /// Control answers this daemon gave up writing because the client stopped reading.
    ///
    /// Counted rather than logged: the control task shares a runtime with nothing that
    /// blocks, and a print on that thread would be exactly the blocking write this counter
    /// exists to report. It is a run total, not a health verdict.
    pub answers_abandoned: u64,
    /// [`ControlRequest::Attach`] requests this daemon refused, for every reason it refuses
    /// one: a peer running as another user, a market it does not hold, an unusable
    /// identifier, or a shard publishing into no segment.
    ///
    /// Daemon-wide rather than per shard, because the two refusals that matter most —
    /// a foreign peer and an unknown market — belong to no shard. A run total, counted for
    /// the same reason [`Self::answers_abandoned`] is: the refusal itself is already a typed
    /// answer to the caller, and this is how an operator sees that it happened at all.
    pub attachments_refused: u64,
    /// The address this daemon's metrics endpoint is bound to, or `None` for a daemon
    /// serving none.
    ///
    /// The resolved address rather than the configured one, which is what makes a
    /// configuration naming port `0` usable: the operating system chose the port and this is
    /// the only place it is reported. Absent from the answer entirely when no endpoint is
    /// bound, so a daemon serving none costs the line nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics_listen: Option<String>,
}

/// One market's row in a status page: the shard that holds it, what that shard says about
/// it, and why the daemon is holding it at all.
///
/// `pinned` and `leases` are the two halves of aggregate demand, reported separately rather
/// than as one total, because they answer different operator questions: whether an operator
/// command put this market here, and how many consumer sessions would have to go before it
/// left. A market with neither — a removal the venue has not reconciled yet — is on its way
/// out.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MarketRow {
    pub shard: usize,
    pub market: MarketReport,
    /// Whether an operator command holds this market in the desired set.
    pub pinned: bool,
    /// How many control sessions hold a lease on it. Sessions, never attachments: one
    /// session that attached to the same market twice is one lease.
    pub leases: u32,
}

/// One shard's line in [`DaemonStatus`].
///
/// `connected` is connection health plus subscription evidence — an established connection
/// with its subscription on the wire — and never a frame rate: a shard whose markets are
/// silent is as connected as a busy one.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ShardReport {
    pub shard: usize,
    /// Whether this shard's *publishing* connection has its subscription on the wire. A
    /// standby's subscription never makes a book reachable, so it does not answer this.
    pub connected: bool,
    pub reconciling: bool,
    /// How many venue connection roles this shard runs, publishing role included, or `None`
    /// for a shard running the default single connection.
    ///
    /// Absent rather than 1 at the default, together with the two fields below: a daemon
    /// running no standby writes the shard summary it wrote before redundancy was
    /// configurable, byte for byte, so a client built against that shape keeps reading it.
    /// Absence therefore means "no redundancy configured", which is a different fact from a
    /// standby that is configured and currently agreeing on nothing.
    ///
    /// With [`Self::standbys_established`] this is the shard's connection topology — one
    /// publishing role, `replicas - 1` standbys, and how many of those are currently on the
    /// wire. The roles themselves are structural rather than per-shard facts, so they are
    /// named by these counts rather than by a row each: a status page is capped at one
    /// control line, and a row per connection per shard would spend that cap on repeating
    /// the same two role names. The per-connection detail, including each connection's
    /// generation and the venue's own session identifier for it, is on the shard's own
    /// `limitless::shard::ShardStatus`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<usize>,
    /// How many of this shard's standby connections currently hold an established
    /// subscription, or `None` for a shard running no standby.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub standbys_established: Option<usize>,
    /// How many of this shard's desired markets the standby best placed to take over
    /// currently agrees with the published book on: the promotion coverage a loss of the
    /// publishing connection would find right now. `None` for a shard running no standby.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub standby_agreeing_markets: Option<usize>,
    /// What this shard's redundant connections are doing about publishing — active-active
    /// through the venue-key gate, or one publishing primary with hot standbys and why —
    /// or `None` for a shard running the default single connection.
    ///
    /// A distinct line from the counts above rather than a flag on them: whether a shard is
    /// publishing across its sockets, and whether it stopped, is the fact an operator reads
    /// those counts differently in the light of.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<PoolState>,
    pub desired: usize,
    /// The file name of the shared-memory segment this shard publishes into, or `None` for a
    /// shard publishing into none.
    ///
    /// The name alone rather than the path: the directory is configuration the operator
    /// already holds, and the name is the part that is not — it carries the daemon
    /// instance's random identity, which is what makes a name from a previous instance fail
    /// to resolve.
    pub segment: Option<String>,
    /// How many markets hold a live directory entry in that segment.
    pub segment_markets: usize,
    /// How many consumers this daemon has handed this segment's descriptor to over the run.
    ///
    /// Transfers, not live attachments: a consumer that maps the segment and exits is never
    /// subtracted, because nothing it holds is registered anywhere in the daemon — that is
    /// the whole point of possession being the authorization. It is what makes an accepted
    /// attach observable to an operator.
    pub attachments: u64,
    pub queue_age: QueueAgeSummary,
    /// What this shard's publications cost, from a frame's arrival to the moment its
    /// publication was complete.
    ///
    /// Both readings are monotonic ([`std::time::Instant`]) and taken in the daemon: one at
    /// the socket read that produced the revision, one the moment the segment writer returned
    /// — slot stable, dirty entry posted, doorbell rung, generation advanced, wake posted. It
    /// is not the difference of the state slot's own wall-clock `(commit, arrival)` stamps
    /// that a consumer in another process reads: those end at the commit stamp taken before
    /// the slot is staged, and they move with the system clock. A consumer-side split derived
    /// from them is expected to read lower than this.
    ///
    /// Absent from the answer entirely for a shard that has published nothing measurable, so
    /// a line from a daemon whose shards are quiet is byte-identical to what a daemon without
    /// this metric wrote, and absence reads as "unmeasured" rather than as zero latency.
    #[serde(default, skip_serializing_if = "PublishLatencySummary::is_unmeasured")]
    pub publish_latency: PublishLatencySummary,
}

/// Encodes one protocol value as the single line that carries it, newline included.
///
/// Fails only if the value cannot be represented as JSON, which for these types cannot
/// happen; the error is returned rather than panicked on so a caller can answer a client
/// instead of dying.
pub fn encode_line<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    let mut line = serde_json::to_string(value)?;
    line.push('\n');
    Ok(line)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AuthorityReason;
    use crate::daemon::MAX_SEGMENT_FILE_NAME_BYTES;
    use crate::limitless::shard::{MarketReport, MarketStatus, SubscriptionState};

    /// The longest market slug this bound assumes a venue issues.
    ///
    /// Limitless slugs are about thirty bytes (`docs/limitless.md`); this is four times
    /// that. The identifier surface itself accepts more, so what the assertion below proves
    /// is that [`STATUS_PAGE_MARKETS`] is sized with room to spare for the identifiers this
    /// daemon actually carries, not that no identifier could ever overflow a page.
    const ASSUMED_MAX_SLUG_BYTES: usize = 128;

    /// The shard count this test sizes the widest page against: the twenty-shard full-fleet
    /// deployment, the largest shard count this project has an actual deployment shape for.
    ///
    /// Shard summaries ride the first status page whole and are not paginated the way market
    /// rows are (`Sessions::status` in `src/bin/pmwsd.rs`). Shard count itself is fleet-sized
    /// — it follows the configured market list with no ceiling — so this constant is this
    /// test's own sizing assumption and not a bound the daemon enforces anywhere: a
    /// deployment configuring more shards than this is accepted, and its `status` answer is
    /// untested by the assertion below. Measured directly (with a temporary local harness,
    /// not a command this tree carries): twenty shards plus a full 128-row market page encode
    /// to 59,627 of the 64 KiB (65,536-byte) line cap, and thirty-two shards already cross it
    /// at 67,451 bytes, so this is proof for a deployment shape that exists rather than
    /// headroom for an arbitrary fleet. Shard summaries would need their own paging cursor to
    /// stay proven past this shard count; this test does not claim that they do.
    const ASSUMED_SHARDS_FOR_LINE_SIZING: usize = 20;

    /// The widest answer a full status page can carry: every row at the assumed slug length,
    /// every counter at its widest decimal form, and the longest status a market can report.
    fn widest_page() -> ControlResponse {
        let markets = (0..STATUS_PAGE_MARKETS)
            .map(|index| MarketRow {
                shard: usize::MAX,
                pinned: true,
                leases: u32::MAX,
                market: MarketReport {
                    slug: format!("{index:0>ASSUMED_MAX_SLUG_BYTES$}"),
                    subscription: SubscriptionState::Established,
                    status: MarketStatus::Stale(AuthorityReason::RecoveryBaseUnavailable),
                    revision: u64::MAX,
                    continuity_epoch: u64::MAX,
                },
            })
            .collect();
        let shards = (0..ASSUMED_SHARDS_FOR_LINE_SIZING)
            .map(|_| ShardReport {
                shard: usize::MAX,
                connected: true,
                reconciling: true,
                replicas: Some(usize::MAX),
                standbys_established: Some(usize::MAX),
                standby_agreeing_markets: Some(usize::MAX),
                pool: Some(PoolState::Armed),
                desired: usize::MAX,
                segment: Some("s".repeat(MAX_SEGMENT_FILE_NAME_BYTES)),
                segment_markets: usize::MAX,
                attachments: u64::MAX,
                queue_age: QueueAgeSummary {
                    samples: u64::MAX,
                    last_micros: u64::MAX,
                    max_micros: u64::MAX,
                    p50_micros: u64::MAX,
                    p99_micros: u64::MAX,
                },
                publish_latency: PublishLatencySummary {
                    samples: u64::MAX,
                    last_micros: u64::MAX,
                    max_micros: u64::MAX,
                    p50_micros: u64::MAX,
                    p99_micros: u64::MAX,
                    p999_micros: u64::MAX,
                },
            })
            .collect();
        ControlResponse::Status {
            status: DaemonStatus {
                pid: u32::MAX,
                more: true,
                shards,
                markets,
                answers_abandoned: u64::MAX,
                attachments_refused: u64::MAX,
                metrics_listen: Some(
                    "[ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255%4294967295]:65535".to_owned(),
                ),
            },
        }
    }

    #[test]
    fn a_full_status_page_stays_inside_the_control_line_cap() {
        let line = encode_line(&widest_page()).expect("a status answer is representable");
        assert!(
            line.len() < MAX_CONTROL_LINE_BYTES,
            "a full page of {STATUS_PAGE_MARKETS} rows encodes to {} bytes, past the {MAX_CONTROL_LINE_BYTES}-byte line cap",
            line.len()
        );
        assert!(line.ends_with('\n'), "a protocol line carries its newline");
    }

    /// The exact line a shard summary encoded to before the `replicas` key existed, for the
    /// values [`shard_report`] carries.
    ///
    /// Written out rather than derived, because what it pins is the wire itself: field
    /// presence and field order, not the values a struct comparison would check. A client
    /// that parses this shape must keep parsing a default daemon's answer unchanged.
    const SHARD_LINE_BEFORE_REPLICAS: &str = concat!(
        r#"{"shard":3,"connected":true,"reconciling":false,"desired":5,"#,
        r#""segment":"pmws-3-abcdef","segment_markets":5,"attachments":2,"#,
        r#""queue_age":{"samples":1,"last_micros":2,"max_micros":3,"p50_micros":4,"p99_micros":5}}"#,
    );

    /// One shard summary at the default topology, or at `replicas` roles when given more.
    fn shard_report(replicas: usize) -> ShardReport {
        let redundant = replicas > 1;
        ShardReport {
            shard: 3,
            connected: true,
            reconciling: false,
            replicas: redundant.then_some(replicas),
            standbys_established: redundant.then(|| replicas.saturating_sub(1)),
            standby_agreeing_markets: redundant.then_some(4),
            pool: redundant.then_some(PoolState::Armed),
            desired: 5,
            segment: Some("pmws-3-abcdef".to_owned()),
            segment_markets: 5,
            attachments: 2,
            queue_age: QueueAgeSummary {
                samples: 1,
                last_micros: 2,
                max_micros: 3,
                p50_micros: 4,
                p99_micros: 5,
            },
            publish_latency: PublishLatencySummary::default(),
        }
    }

    #[test]
    fn a_default_topology_shard_summary_encodes_to_the_line_that_preceded_replicas() {
        let line = encode_line(&shard_report(1)).expect("a shard summary is representable");
        assert_eq!(line, format!("{SHARD_LINE_BEFORE_REPLICAS}\n"));
    }

    #[test]
    fn the_line_that_preceded_replicas_still_decodes_into_a_default_topology_summary() {
        let decoded: ShardReport =
            serde_json::from_str(SHARD_LINE_BEFORE_REPLICAS).expect("the prior shape still parses");
        assert_eq!(decoded, shard_report(1));
    }

    #[test]
    fn a_shard_running_standbys_names_its_topology_on_the_wire() {
        let line = encode_line(&shard_report(3)).expect("a shard summary is representable");
        for key in [
            r#""replicas":3"#,
            r#""standbys_established":2"#,
            r#""standby_agreeing_markets":4"#,
        ] {
            assert!(line.contains(key), "{line} carries {key}");
        }
    }
}

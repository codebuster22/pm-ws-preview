# Native-event design

## Purpose and scope

`pm-ws` is an ultra-low-latency Rust WebSocket ingestor and local event distributor for Limitless and
Polymarket. It owns connections, subscription demand, exact decoding, bounded in-memory retention, and typed
delivery to Rust, Python, and TypeScript. All event families on the selected market-data feeds, open or keyed, are in scope,
not only snapshots and deltas. Authenticated account/order feeds and unrelated public channels are not
implicitly included.

This document is the target contract. The tree implements the connection-to-admission path for both venues plus
a metrics-only in-process receiver: each admitted batch is observed for delivery coordinates and stage timings,
then dropped, and no event leaves the daemon process. No local transport, consumer API, or foreign-language
binding exists. The upstream cost of that path for the 50–100 band's workload is measured and accepted; the
[qualification report](../bench/reports/native-upstream-flat-packed-100-m1.md) holds its numbers, host, and
provenance.

In scope:

- Every event family on the selected Limitless and Polymarket market-data feeds, open or keyed, including trades,
  prices, metadata, and lifecycle.
- Venue framing, heartbeat, acknowledgement, error, and subscription-control handling, kept distinguishable from
  public source events and from local status.
- Externally supplied target sets, operator pins, reference-counted client leases, and batch commands.
- Disjoint per-connection target sets, one owning connection per market, generation-fenced reconciliation,
  pacing, and recovery.
- Exact full-event decode, validation, source fields, native identities, and provenance.
- Bounded count-and-byte retention, independent consumers, explicit gaps, and typed local delivery.
- Complete event accounting, stage latency, queue age, CPU, and memory evidence.

Out of scope:

- Order-book construction, latest-book publication, snapshot-derived diffs, economic normalization, trading
  policy, and any separate downstream book product.
- Market discovery and selection, remote distribution, and durable payload archives.
- Authenticated account/order feeds and unrelated public channels a venue happens to offer.

Venues are added in-tree through one venue trait each adapter implements, declaring its transport and decode entry
points, its families, its identity form, its health-evidence rule, its subscription wording, its constants and its
credential fields; every venue distinction in the core is an exhaustive match with no silent fall-through, and the
core stays venue-agnostic. A venue is first-class in the
readers: each reference consumer types every venue's families, so a venue addition changes the daemon and all three
readers together and ships as a minor release. More memory and CPU are acceptable when measured to improve speed
and robustness within explicit resource limits.

## Invariants

- Decode source prices and quantities exactly; no floating point or rounding in delivered data.
- Reproduce every valid native-event arrival, including repeats, same-value updates, and older-looking source
  timestamps or versions. Do not invent changes or discard events by economic equality or identity.
- One admission/publication owner per local stream. No consumer reads a book or holds a writer lock.
- Every queue, retention log, payload, and diagnostic buffer is bounded and has a declared overflow outcome. A
  slow or failed consumer never blocks ingestion.
- Public identity is venue-native and venue-scoped; local handles and sequences are not venue IDs.
- Each current market is owned by exactly one venue WebSocket; one WebSocket can carry many markets. Every valid
  event arrival from that connection is published.
- Connection-generation fences reject a retired source before admission. Reconnect continuity is explicitly a
  gap boundary, never inferred from availability.
- Source snapshots, source deltas, lifecycle events, and local control/failure records remain distinct.
- Loss is explicit. Malformed input, overload, retention overrun, disconnect, and generation change are not
  interchangeable reasons.
- Quiet subscribed targets are not failed or unsubscribed merely because no data arrived.
- Ingestion is WebSocket-only. Recovery is resubscribe then reconnect; REST recovery requires explicit opt-in
  and remains outside the update path.
- No blocking I/O, persistence, payload logging, or consumer backpressure on the update path.
- Controllers supply desired native target sets, never market data.

Changing an invariant needs an owner decision, a regression test that fails without the change, and one bounded
adversarial review.

## Data path and ownership

```text
one owning WebSocket per market (many markets per WebSocket)
  → complete application message → exact decode and full validation
  → one generation-fenced admission gate → complete typed upstream handoff
  → selected bounded local transport + retention (target; not in the tree)
  → independent Rust / Python / TypeScript readers (target; not in the tree)
```

Stages name measurement boundaries, not mandatory threads or queues. Keep a direct path where ownership permits
it. Any handoff added for isolation must justify its cost with queue-age and service-time evidence. Socket tasks
never run user callbacks.

Ownership preserves connection message order and source array order. Every valid arrival reaches the single
admission owner; preserve its original receive stamp. Local publication order is admission order, not a claim of
exchange chronology or absolute earliest kernel-byte arrival. Parallel decoding must not silently reorder one
socket's events.

A connection can own many native targets
([ADR-0003](adr/0003-one-owning-socket-per-market-every-arrival-published.md)). No thread per market is
required. Startup sizing is in scope; dynamic migration, automatic live resharding, and child daemons need
evidence and a separate decision.

The upstream handoff contains complete validated native events/batches, exact source values, source and local
provenance, and stream/generation/sequence boundaries. It does not expose a book, a transport-specific loan, or
a chosen shared-memory layout. A logical handoff is not a mandatory queue or copy. The metrics-only receiver
takes this same interface; consumer code never runs as an ingestion callback. Event meaning is fixed
independently of transport selection; final byte layout, allocation, and public ABI are chosen afterward.

## Complete native events

An application message means the complete WebSocket message after transport reassembly, TLS, and venue framing;
a network packet or WebSocket fragment is not the delivery unit.

A supported native event is retained with all its source fields and arrays, source order, native identifiers,
exact numbers, and provenance. Typed decoding is not a projection onto only changed levels. Preserve zero sizes,
empty snapshots, unchanged levels, and metadata-only updates. Do not sort, complement, merge, trim depth, or
infer order actions.

The in-process native path constructs one owned document with contiguous source storage and preorder value nodes
([ADR-0005](adr/0005-decode-once-into-a-source-preserving-document.md)). Unescaped strings and JSON-number
lexemes reference that source storage; escaped text is decoded into document-owned storage. Exact JSON numbers
are parsed during the scan ([ADR-0002](adr/0002-exact-decimals-never-floating-point.md)); venue validation
promotes economic strings without changing their decoded source spelling. Polymarket array members share the
document but retain separate event roots. Limitless retains the complete event argument array, including the
name and every extra argument. One scanner serves both native application documents and lexical
transport/control decoding.

Consumers access immutable typed views: fields, source-order child iterators, exact decimals, and source text.
Retaining one view's payload retains its entire source document. No book read, writer lock, or second JSON parse
is required. Admission checks every event subtree and charges each distinct document allocation once, including
actual buffer capacity, plus event metadata and unused batch-vector capacity. Parsing may transiently use more
memory than the retained admission cap, but input, depth and container limits bound it; oversize input is
rejected before copying it. This is neither a zero-allocation promise nor a selected cross-process layout/ABI.

Each schema includes bounded handling for source extensions: preserve their typed values without consumer JSON
reparsing, or reject an unsupported required shape explicitly. Unknown families use a bounded typed native
envelope if complete validation/routing is possible; otherwise emit an explicit unsupported-protocol fault with
the affected continuity scope. Never silently omit source fields or skip families while claiming complete
delivery. Unknown input cannot allocate arbitrary market subscriptions or unbounded schemas/metric labels.
Negotiated/enabled families and unsupported observations are inspectable.

Validate the entire application data message before admitting any member. A malformed member of a top-level JSON
array rejects the entire array; no earlier member escapes. A valid array is emitted in source order as one
committed batch with original member indices and boundaries. Repeated identities and repeated or conflicting
content are independent valid arrivals; no identity comparison, payload hashing, or content witness runs on the
admission path.

A `price_change` and all of its `price_changes` entries are one native event. Individual entries are never
independently suppressed or published. A received batch stays with one publication owner; it is not split into
cross-shard transactions. Interest filtering is daemon-side and selects whole events, not subsets of an event's levels or
assets. Unexpected routing identity is a typed validation failure, never permission to allocate unbounded new
subscriptions.

Each committed batch is wholly readable or unavailable. Count and byte reservations must fit the complete
admitted batch before commit. Oversize input or insufficient ingress capacity produces explicit local loss,
never a truncated event or invisible partial batch.

Every documented family on the selected feeds is required, including trade, price, metadata, and
lifecycle events. Required optional features must be enabled explicitly. Each family needs validation, routing,
and a deterministic test; live counts are reported separately per family. Zero live observations do not imply
support or failure. See the inventories in [Limitless](limitless.md) and [Polymarket](polymarket.md); this
document does not maintain a second incomplete shortlist.

Handle heartbeat, acknowledgements, subscription errors, and other protocol controls through their venue rail.
Preserve their declared source/control meaning and expose relevant health/subscription observations; do not
reinterpret transport pings as market changes. Venue-native system/error events with source payloads follow
their documented delivery policy. Global lifecycle/metadata notifications use bounded venue-scoped routing, not
implicit subscription to new markets. All-events support does not override externally controlled demand.

## Identity and admission

Venue-native identity remains complete source data: Limitless timestamps and versions and Polymarket timestamps
and hashes are delivered unchanged where supplied. It is not a local deduplication, ordering, conflict, or
freshness key. The daemon publishes an event with the same identity and equal content again, and also publishes
an event with the same identity but different content
([ADR-0003](adr/0003-one-owning-socket-per-market-every-arrival-published.md)). Consumers that need
interpretation make that policy downstream.

The admission owner validates batch shape and exact values, checks bounded input and value limits, and rejects
only a retired connection generation or invalid/over-capacity batch. It keeps neither native identities nor
event payloads after returning the admitted batch.

## Local transport, retention, and consumer reads

No local transport, retention log, or consumer API exists in the tree; this section is the contract a candidate
must satisfy, and no candidate is selected
([ADR-0007](adr/0007-local-transport-chosen-by-measurement-not-assumed.md)). Local-only delivery is required and
shared memory is not; an existing implementation whose measured behavior fits the contract is preferred to a new
one.

Decode JSON once per received message, then publish binary typed data. Python and TypeScript use the Rust-owned
reader helper for complete reads and schema validation; they do not parse venue JSON or implement low-level transport
synchronization. Language object conversion still costs time and belongs inside consumer latency measurements.
Control-plane JSON is unrelated to the market-data decode-once guarantee. Each reference consumer types every
venue's families, and a venue addition extends all three.

The chosen transport needs a versioned encoding or layout with explicit numeric widths/scales, lengths, schema
compatibility, instance/stream generations, and source/local provenance. Specify byte order, alignment, offsets,
commit markers, and fragmentation where its actual representation needs them. Process-local pointers and
language-owned objects cannot serve as cross-process payloads. Prices and quantities remain exact integers plus
scale across all languages, including values too large for a JavaScript Number. No custom mapped ABI is frozen
before the comparison.

Candidate evaluation includes Unix-domain sockets and an existing shared-memory pub/sub implementation; other
local options need a concrete reason to enter the shortlist. Compare the same complete event meaning and account
for encoding, copies, notifications, framing, conversion, auxiliary processes, memory, and CPU. Every candidate
meets the same observable delivery contract.

Retention is a bounded log per publication stream/shard, not a latest-book cache. Configure both retained
native-event count (for example 100 or 1,000) and encoded bytes, plus a maximum atomic batch size. Count means
complete native events, not levels. Byte pressure can shorten the count window; hot targets share their shard's
retention budget. Capacity arithmetic reports total memory including in-flight messages, logs, consumers, and
diagnostics. No unlimited slow-consumer promise
([ADR-0011](adr/0011-bounded-everything-drop-never-backpressure.md)).

Delivery is daemon-side and by lease
([ADR-0015](adr/0015-per-target-delivery-is-daemon-side-by-lease-on-per-shard-streams.md)). Each shard publishes one
stream and a target is a routing key, not a stream: a published batch is delivered to a session only when it names a
target that session leases, whole and at most once per session, so a session leasing one asset of a Polymarket
condition receives the complete `price_change` with the sibling asset's entries and a session leasing both receives it
once. A lifecycle event also reaches every session leasing its venue's lifecycle feed; one naming nothing leased is
retained and counted, and delivered to nobody. Control records, gaps and generation changes reach every session
attached to their stream. An unknown-family envelope reaches nobody and is counted; its payload is never re-parsed to
invent a target, so an unknown family such as Limitless `oraclePriceData`, which names no target the daemon can trust,
reaches no consumer until it is documented and typed, and is visible only as a `status` count. Admission appends the
encoded batch once to the stream's retention, advances the stream sequence, resolves the named targets through one
dense index to the interested sessions and leaves a descriptor per session; the per-session writes happen off the
admission path, so admission cost is independent of the total target count and of uninterested sessions, and a slow
session delays only itself. Over a byte-stream transport the kernel copies once per consumer from the retention slot;
a shared-memory candidate may read the single copy.

Each delivered record carries its stream's sequence, which a session sees with holes because batches for other
targets are skipped, and each delivery's frame carries, beside the record, a contiguous delivery sequence for
that attachment, so the reader proves local loss by contiguity and never trusts a gap record alone. A local
egress loss and an upstream continuity boundary are distinct gap kinds, and only the first ever shows as a
delivery-sequence jump. The record bytes are identical for every transport candidate, and the per-delivery
fields sit in the frame outside the record, so the record itself is identical for every attachment; the record
header is fixed before the comparison.

A session attaches once, on the data socket, naming its session and a start per stream: `head`, the first batch
admitted after the attach; `oldest`, the oldest retained batch; or `cursor`, the batch after one the reader completed
earlier. Attachment returns the actual start cursor and generation per stream. A batch is delivered to a lease only if
it was admitted at or after that lease's origin: a lease held at attach has its origin at the attachment's start
position, and a lease added later has it at the stream head when the lease is accepted, so a later lease never
receives retained batches the attachment has not yet reached. Only whole batch boundaries are valid starts. Reads
advance the cursor only after the complete batch is copied and validated. The next batch being written does not force
a read of any latest-book state before an already committed batch can be consumed. A transport may reclaim old storage
without waiting for readers. A stale cursor reports expected and oldest available positions and the affected stream;
continuation requires explicit acknowledgement/reattachment, not silent rebasing.

Notifications are coalescible wake hints, not event storage. Poll committed cursors until caught up. A transient
read failure leaves the cursor unchanged and is retried to an independent bounded deadline even if no new
notification arrives. Retry exhaustion, writer unavailability, generation change, and retention overrun are
different outcomes. Consumers may spin or park; neither changes event content or loss semantics.

These are consumer semantics, not a requirement to implement a particular ring. Record what a candidate library
provides and what a small adapter must supply. If retention/cursor, resource, or isolation semantics do not fit,
surface the mismatch for an owner decision; do not silently amend the contract or build a framework solely to
force library compatibility.

A candidate that reuses storage while a delayed reader copies it states its publication and reuse protocol
before anything relies on it: a torn successful read is never a permitted outcome, and a complete validated
record, a retry that does not advance the cursor, or an explicit retention/generation gap are the only results.
A memory-model argument justifies that protocol; a stress-test pass does not, and neither does a fixed number of
spare slots or a guessed safe reuse delay. Readers validate schema compatibility, layout, byte order, checked
offset arithmetic, alignment, and instance/stream generation before touching a record, and cursor or sequence
wrap is checked rather than silently recreating an old valid position. Data mappings are read-only to consumers,
and cleanup removes only an object still proven to be the one this instance created, never a successor's.
Byte-stream candidates owe the same contract through complete framing, bounded per-client buffering, and tested
partial-I/O and disconnect behavior.

## Late attachment and downstream books

Native-event readers need no book base and may receive deltas immediately. pm-ws does not wait for a snapshot
to start publishing valid deltas. A late reader's starting position says what history it has, not that it has a
synchronized book.

A downstream book builder decides whether to use a complete retained source snapshot plus an unbroken tail, or
wait for a later source snapshot. Base discovery/assembly belongs to that consumer, not an implicit book API in
`pm-ws` ([ADR-0001](adr/0001-publish-complete-native-events-never-books.md)). Observing a snapshot earlier in a
generation does not prove it is still retained or that no later source data was missed.

For a downstream Polymarket book, a `book` fully replaces that asset's book, and following `price_change`
entries operate on that replacement. This is interpretation of source events, not work done by pm-ws. No
fixed snapshot cadence or snapshot-only-after-trades assumption is made. Limitless delivers source snapshots,
not locally derived changes. Neither rail can reconstruct individual order actions that the venue did not
publish.

Adding a reader never forces a healthy shared subscription to resubscribe. Any requested recovery is an explicit
control operation with its effect on existing readers visible.

## Subscription demand and control

For each native target: `wanted = operator_pinned || active_client_lease_count > 0`.

The operator pins, unpins and replaces, and drains and undrains a list of targets or a whole venue; a client
session leases and releases; `status` reports. Operator pins survive consumer disconnects. `replace` makes the
pinned set exactly the list given, so repeating an owned set changes and resubscribes nothing. A session is one
client's control connection and is not resumable: a consumer whose control connection drops opens a new session and
leases again, which the reader library does for it. A session can own many individually releasable leases, and
releasing one lease leaves its other leases intact. A lease lives exactly as long as its session's control
connection: it ends through `release` or through that connection closing, never through time, so there is no
renewal, no time-to-live and no expiry sweep, and a session's end releases only its own leases
([ADR-0013](adr/0013-demand-is-pins-and-connection-bound-leases.md)). A hung consumer holds its leases until it
dies or the operator drains them, and `status` shows them. Unsubscribe only when aggregate demand becomes zero, or
on an explicit operator drain or reconfiguration.

The controller supplies complete venue-native identity and the daemon never discovers: a Limitless slug or, for an
AMM market, its address, and a Polymarket condition ID with its asset. A selection names an AMM market by that same
address. Selection, databases and market discovery stay external; the discovery tool gives an operator the same
identities a controller holds. Commands accept concrete native identifiers in batches, including externally
discovered all-market lists. A batch is atomic: one bad member rejects the whole batch, nothing is applied, and the
reply names that member. A command returns desired-state acceptance promptly, and its reply carries acceptance and
the desired-set revision only; subscription reconciliation and achieved per-target coverage are reported separately
and are read from `status`.

The outcomes a caller sees are distinct codes: `invalid_request`, `unknown_venue`, `invalid_target`, `capacity`,
`drained`, `frozen`, `busy`, which invites a retry, `stopping`, and `credential_required`, which refuses a lease
naming a target at a venue whose credential state is `absent`, `unusable` or `rejected`; an operator pin at such a
venue is accepted. A framing failure is `invalid_request` wherever a reply is still possible. A daemon at its
session limit answers `capacity` before closing the connection, and a peer that does not share the daemon's
effective user id is closed silently.

`status` is the stream directory. Unfiltered it returns counts, per-stream connection health and per-session rows, and
counts of lifecycle events and unknown-family envelopes retained but delivered to nobody. Per-target rows, carrying
venue, market, asset, stream, pinned, lease count, drained, coverage observed and generation, are returned for a named
target list or for a whole venue, in bounded pages with a page token; the row's stream is the target's own owning
stream, and a lifecycle arrival for it may also appear on the stream carrying the venue's lifecycle feed. Every reply
and every `status` page carries the desired-set revision and the session id.

The data path is a second local socket beside the control socket, bound the same way. A session opens exactly
one data attachment on it, named by its session id; the attachment receives exactly what the session's leases
entitle it to, lives exactly as long as the session's control connection, and a second attachment for the same
session is refused `invalid_request`. The total attachment buffer budget is a startup limit, and an attach that
would exceed it is refused `capacity`.

A venue's lifecycle feed is a coordinate every verb accepts beside a target, `{venue, lifecycle}`: a session
leases it to receive every lifecycle event on that venue, an operator pins, drains or replaces it, and `status`
lists it. Each adapter declares how its venue's feed is obtained. Where the venue commands it independently, as
Limitless does, the adapter subscribes it on one owning connection only while a pin or lease holds it. Where the
venue couples it to a required family, as Polymarket's `custom_feature_enabled` couples `new_market` to
`market_resolved` and `best_bid_ask`, the adapter always requests it and the feed is gated at delivery, so a
lifecycle event naming nothing leased reaches nobody and is counted in `status`. The lifecycle events a market's
own subscription delivers reach that market's lessees either way. A session holding both the feed and a market's
lease receives each arrival once; a lifecycle event the venue sends on two paths is two arrivals, both delivered.

A drained target is unsubscribed regardless of pins and leases and refuses new leases with `drained` until it is
undrained. A session that held a lease on a target while it was drained learns from the reply to its next
command.

A qualification run holds its desired set frozen: any command that would change it is refused with `frozen` and
the run's report counts the refusals, while `status` and leases on already-pinned targets keep working.

Reconcile the current desired set, never replay obsolete command history. A runtime set change is an in-connection
subscription transition on both venues, a whole-set replacement on Limitless and subscribe and unsubscribe
operations on Polymarket, never a reconnect, which stays recovery. Each transition is serialized and
generation-fenced. Late acknowledgements or data from retired assignments cannot reactivate removed or re-added
targets. If a protocol cannot distinguish pre/post-transition data, use a fresh connection generation and expose
the boundary. A set change need not invalidate unrelated subscriptions when the venue supplies sufficient evidence.
A Polymarket market and asset pairing that admission finds wrong is a per-target fault shown in `status`, and a
disqualifier while the desired set is frozen.

An owning connection can cover many markets and their assets, and a market is owned by exactly one such
connection. Hard connection/attempt budgets include temporary replacement overlap. Do not equate an open
socket, a sent command, a venue acknowledgement, observed target coverage, and health.

The control socket defaults to `~/.pm-ws/run/control.sock` and is overridable. It is bound under umask 0077, so it is
owner-only from its first instant, and a peer must share the daemon's effective user id. Consumers run as the daemon's
user, through a shared volume in containers; group access is outside v1. Limits are startup configuration and none is
hot-changeable: sessions, default 128; total targets, default 32,768; and the control-socket request line, default 8
MiB, sized to hold a replace at the top load band. There is no per-session lease cap. The total attachment buffer
budget and the per-stream retention count and bytes are startup limits too. Demand changes at runtime through the
control plane, and venue credentials change through the `pmwsd credential` subcommands and the control plane; the
limits, the socket path, the metrics address, the log level, the connection count per venue, the spread rule and venue
endpoints change only across a restart.

A venue credential is one file per venue, at `~/.pm-ws/configs/<venue>.key`: a JSON object keyed by the field names
that venue's adapter declares, mode 0600, owned by the daemon's effective user, in a directory of mode 0700,
bounded at 64 KiB. Wider permissions or a failed parse make the credential `unusable`. An environment such as
Kalshi's demo host is endpoint configuration, never a second file
([ADR-0012](adr/0012-authenticated-market-data-feeds-in-scope.md)).

Each adapter declares its own credential fields: name, one-line hint, whether the field is secret, whether it is
single- or multi-line, and the parse rule it runs at connect. Kalshi declares a key id and a private key PEM;
predict.fun declares an API key; Limitless and Polymarket declare none. The daemon core renders these declarations
as prompts, as flags with the hints in `--help`, and as validation, and never understands a credential itself.

`pmwsd credential set <venue>` takes every declared field as `--<field> <value>`, `--<field> @<path>` reading a
file, or `--<field> -` reading stdin; it prompts for a missing field when stdin is a terminal and exits 2
otherwise. The adapter's parser runs at capture; a failing field names itself under the usage exit code and never
echoes a value. The file is written atomically, through a temporary file and a rename, at mode 0600.

When a daemon answers at the control socket, `credential set` sends a `credential` command naming the venue; the
daemon re-reads the file and runs one verification handshake, spending one attempt against the attempt ledger,
closed once the venue accepts it, and replies `accepted` or `rejected`, an outcome `status` also shows. `pmwsd
credential verify <venue>` runs the same handshake on demand. With no daemon reachable the file is stored and
reported unverified, exit 0. A venue that checks a credential later than the handshake declares its own
verification step in its adapter; Kalshi and predict.fun both check at the handshake. A qualification run refuses
the `credential` command as `frozen`, counted in its report, and the file stays stored.

The adapter reads the credential file at every connect, so rotation never forces a healthy connection to reconnect:
a new credential applies starting at that connection's next connect. `pmwsd credential clear <venue>` removes the
file and wakes the daemon; live connections on that venue stay up until they drop on their own or demand frees
them, because a consumer already attached to them would otherwise be cut off by the removal. The venue then shows
`absent`, makes no new attempts, and refuses new leases.

A handshake refused with HTTP 401 or 403, or a venue message after connect that names authentication (a Kalshi
authentication code, a Limitless authentication challenge), is the fault `credential_rejected` for that venue. That
venue makes no further attempts until `credential set` or `credential verify` names it again, `verify` being the
operator's re-attempt for an open feed; one refusal is enough, with no retry count and no timer, and the spent
attempt stays spent. A 429 stays venue pushback and every other connect fault stays transport loss; the rule
follows the venue's own answer, not whether the feed is keyed, so an open feed that starts answering 401 stops the
same way.

`status` carries a per-venue credential state: `open` for a feed that needs none, `present` for a usable file not
yet proven by a handshake, `accepted`, `absent`, `unusable`, and `rejected`.

At startup, a keyed venue named in the frozen selection with an `absent` or `unusable` credential refuses the start
with exit 2 and a message naming the venue and the path, never the content.

Etiquette gains no credential dimension: one daemon holds one credential per venue, so per credential is per venue
already. A verification handshake and a refused handshake each spend an attempt like any other.

The effective-user-id check is the only authority boundary, so the CLI is the operator's tool and the reader
libraries expose only `lease`, `release` and `status`. One binary carries the daemon and that CLI: the control
verbs are the `pmwsd` subcommands `pin`, `unpin`, `replace`, `drain`, `undrain`, `status`, `credential set`,
`credential verify` and `credential clear`, each taking a flag that names the socket, with exit codes 0 ok, 1
unreachable, 2 usage, 3 rejected and 4 busy; `credential set` exits 0 when the file is stored, whether verified or
not, and 3 when the venue rejects it or a qualification run refuses it as frozen.

Control operations, filesystem work, descriptor setup, discovery, and diagnostics scraping run outside the
update path. Local attachment is permission-checked; secrets and writable or truncatable daemon backing objects
are not handed to readers. Socket endpoints also require explicit permissions and bounded client resources.
A transport library does not remove these deployment obligations.

## Failure, recovery, and lifecycle

Feed health is connectivity, heartbeat, subscription evidence, local continuity, and observed target coverage
([ADR-0006](adr/0006-feed-health-is-evidence-not-time.md)). Venue lifecycle data remains separate. An observed
resolution is delivered, not an instruction to invent unsubscription or trading policy. Normal silence is
activity telemetry only.

For Limitless, health requires a live heartbeat and the acknowledged current market set. For Polymarket,
subscription commands are sent but not venue-confirmed; health requires the sent set and separate observed asset
coverage. Neither venue treats a quiet subscribed market as unhealthy. Connection generation advances before
reconnect work can admit data, so retired work cannot cross an explicit reconnect-gap boundary.

Connection loss, known dropped input, rejected required data, or a subscription boundary reports a stream gap.
If loss cannot be localized, widen its reported scope conservatively. A generation fence prevents a retired task
from publishing across that gap.

Loss reporting cannot itself depend on room in a full data queue. A bounded sticky health/gap state and
generation boundary must make loss visible before subsequent data can appear continuous. Ordinary valid arrivals
may resume with explicit continuity qualification; pm-ws does not rebuild a book to recover.

Recovery respects venue pacing and budgets: reconcile/resubscribe first, reconnect if needed, restore the newest
desired set, and discard work from retired generations
([ADR-0004](adr/0004-websocket-only-ingestion-rest-recovery-opt-in.md)). A venue's refusal of a credential is a
fault and not a recovery case: it ends that venue's attempts until the operator acts, never a reconnect loop.
Heartbeat and operation deadlines are allowed; an invented market-update cadence is not. A fresh snapshot does not
retroactively repair missing event history.

Shutdown exposes an explicit terminal cursor/generation boundary and drains or reports every
accepted-but-uncommitted batch. Restart creates a new instance/stream generation, never adopts an uncertain old
writer position. Slow/crashed clients cannot postpone shutdown or ingestion.

## Operations and release

The daemon serves metrics only when `--metrics <addr>` names an address; the flag is off by default and carries no
default port. The endpoint answers Prometheus text format at `/metrics` from a minimal in-tree responder on the
tokio listener the daemon already runs, one GET per connection over a bounded request line, so no dependency is
added, and the documentation recommends a loopback address. It exposes per-venue connection health and evidence
coverage, faults by reason, admission and drop counters, retention depth, credential state, control sessions and
leases, the six stage latencies as histograms read from the bounded buckets already kept, `log_dropped`, and
resident memory. A scrape reads snapshots off the update path.

Logs are JSON lines on stderr, one object per line carrying `ts` in RFC 3339 UTC with milliseconds, `level` as one
of `info`, `warn` and `error`, `event` as a fixed snake_case name, and then named fields. Only transitions are
written, connect, subscribed, evidence reached, a fault by reason, a credential state change, a control session
opening and closing, and the shutdown reason, plus a ten-second `status` event while metrics are off, and never a
venue payload or a credential value. A shard never writes a line: it pushes a fixed-size record into a bounded ring
of 256 records that one writer task drains off the update path, and a full ring drops the new record and counts it
as `log_dropped` ([ADR-0011](adr/0011-bounded-everything-drop-never-backpressure.md)). The ring depth is a startup
constant. `--log-level` defaults to `info`, the `status` event stops when metrics are on, and serde_json renders
the lines without a logging crate.

A release carries three binaries. aarch64-apple-darwin is built on the M1 and needs macOS 11 or later.
x86_64-unknown-linux-gnu and aarch64-unknown-linux-gnu are built inside the `manylinux_2_34` containers,
AlmaLinux 9 with glibc 2.34, through Docker on the M1, arm64 natively and x86_64 under emulation, or on the WSL
box once it carries Docker. The declared glibc floor of 2.34 is a minimum rather than a host version and costs
nothing at run time, so one Linux binary runs on Ubuntu 22.04 and 24.04, Debian 12 and Amazon Linux 2023 alike.
deny.toml and about.toml name aarch64-unknown-linux-gnu beside the targets they already carry. There is no musl
build and no x86_64 macOS binary. One multi-architecture image, `ghcr.io/codebuster22/pm-ws:<version>` for amd64
and arm64, holds the single `pmwsd` binary on a minimal glibc base with `~/.pm-ws` as its one volume, so the
control socket, the key files and the readers' attachment share it; `docker buildx` builds it on the M1 from the
Linux binaries through a Dockerfile kept in the tree and excluded from the crate. The two supported production
runs are the bare binary under systemd and the image under Compose, and the README documents both; the tree ships
neither a unit file nor a compose file.

The first release is 1.0.0. It promises the operator surface, the CLI and its exit codes, the control protocol,
the file locations and the metrics and log shapes, together with the published record schema; the Rust library's
public modules carry no stability promise and the README says so. The readers ship as `pm-ws-client`, on PyPI importing as `pm_ws_client`, on npm unscoped, and as a
Rust crate for the Rust reader; all three ship at the daemon's version in lockstep from the same release, and no
placeholder publish holds a name. Python wheels are built in the same `manylinux_2_34` containers plus macOS
arm64.

The release gates are ordered local commands: `./check` green on the M1 and on the WSL box; `cargo deny check`
and `cargo audit` green; `cargo package --list` matching the expected file list of sources, the Cargo files,
README, LICENSE, NOTICE and the third-party notices; the security pass done; and the required ladder rows and the
soak run of the measurement section present under `bench/reports/`. No EC2 host, row or run gates
any release, and there is no CI for v1: the gates are these commands run on the release machines. The security
pass is one bounded adversarial review cycle of the built release candidate before 1.0.0, at most two cycles,
against a fixed checklist of the umask bind, the effective-user-id check and the directory preconditions, the
request-line and batch-parser bounds, the key-file rule, the capture path never echoing a value, the atomic
write, the 401 and 403 handling, every `unsafe` block, the metrics bind, and log redaction, where a finding
without user-visible consequence is a note. A "Release" section in the README holds the ordered checklist, the
gates, then the builds, then the publish: tag `v1.0.0`, the GitHub Release carrying the three binaries and their
sha256 sums, `cargo publish`, the image push, `npm publish` and the PyPI upload. One script `./release-build`
produces the three binaries, their sums and the image into `dist/`, and the owner runs each publishing command
from the owner's own registry accounts.

## Measurement and acceptance

The primary latency boundary is the complete application message becoming available to user-space receive code
through the consumer having the complete usable typed event. It excludes venue processing, WAN transit,
kernel/socket buffering, and transport/TLS work already done before that receive timestamp. It is not
NIC-to-consumer or trading-action latency. Name the excluded work; zero admission loss does not imply zero
socket backlog.

Use a verified same-host monotonic clock domain across processes and name its resolution. Do not subtract
unrelated process-relative clocks or server timestamps to claim software latency. Preserve per-event receive,
decode, validation, admission, upstream handoff, encoding, queue, commit, wake/poll, and conversion stamps;
multiple events from a message share its receive stamp. Stages may be fused, but do not add queues, decoding
passes, or object copies just to create timers, and no encoding, handoff, or wake interval may fall between
unmeasured stage boundaries. Bound diagnostics, and compare instrumented against uninstrumented cost before a
production latency claim.

The achievable latency is discovered by a ladder of qualification runs at each load band's top: 10, 50, 100, 250,
500, 1000 and 10000 markets per venue, a market being a Limitless condition-bearing leaf slug, never a group
container, or a Polymarket condition with both outcome assets
([ADR-0010](adr/0010-latency-target-judged-by-tails.md)). A venue whose active population falls short of a band's
top runs at its full population, the report records the count, and the band above is not run for that venue. Each
band runs one workload per shape, each venue alone and all venues together and never pairs, so three shapes while
two venues exist; every row is quoted and the all-venues row carries the hero figure. A workload is frozen before
launch: a selection chosen most active first by a documented listing field, Limitless `volume` and Polymarket
`volume24hr`, ranked in the discovery store with the field and value recorded, whose targets outlive the run's
ceiling; a declared connection and shard layout; and frozen capacities. The run refuses any command that would
change its desired set and reports the refusals it made; a demand change that reaches the rail disqualifies it. All
targets stay subscribed concurrently; sent subscription state and observed target coverage are recorded separately
from configured targets and open sockets, and quiet targets are not faulty.

The upstream report keeps six stages: `json_decode`, `typed_validation`, `admission_gate`, `observer_audit`,
`receive_to_typed_handoff`, and `receive_to_audited_observation`. `json_decode` includes scanning,
native-document construction, exact JSON-number parsing and, for Limitless, Socket.IO framing and extraction;
venue schema/routing validation and economic-string promotion belong to `typed_validation`. Compare the same
total handoff boundary, never a reduction produced by moving work across a stamp. The upstream observer audits
delivery coordinates, generation, sequence, and member order with bounded metrics; it does not retain native
identities or hash complete source payloads.

A qualification run counts only continuously healthy measured time, starting 30 seconds after every target is
ready on healthy connections, with a readiness deadline that scales with the declared layout. Every band needs
at least 900 such seconds and at least 10,000 accepted book-data arrivals from each venue present, counting
Limitless `orderbookUpdate` and Polymarket `book` and `price_change` arrivals rather than unique identities;
controls, metadata, trades, levels, and derived events cannot pad that floor. A band whose arrival rate cannot
reach 10,000 within the four-hour ceiling runs to the ceiling and qualifies at 1,000; its rows withhold p99.9
and every row prints its sample count. Families are reported separately and never floored separately. The
floors are the daemon's invocation, echoed in the report; the validator checks the report against that echo and
holds no floor of its own. A reconnect, coverage drop, gap, stale or overload admission, malformed or rejected
message, resolved or expired target, demand change, or correctness fault disqualifies the run at every band. An
unqualified band is attributed by cause before anything else: a local cause is a defect fixed before the re-run;
a venue-initiated cause changes the placement within etiquette and re-runs; the band is the venue's ceiling only
when the permitted layouts are exhausted, and every attempt stays in the ladder table with its cause. A rare
family is certified by deterministic coverage, not by a fabricated live sample count.

A release requires ladder rows on both release machines: every band the ladder runs and every workload shape on the
M1, which is the production machine and carries the hero figure, and the all-venues shape at every band on the WSL
box, whose single-venue shapes are optional. The two machines never run against a venue at the same time, so the
etiquette numbers stay per machine. A release also requires one 24-hour soak run on the M1, all venues, the
release binary, at the highest band that qualified on the M1, on mains power with sleep prevented for the run and
the power state recorded; it gates 1.0.0 and every later release. A soak is production-shaped: faults, reconnects
and gaps are counted and attributed, never fatal, so the harness carries a soak mode that lifts the four-hour
ceiling. The soak proves endurance and the ladder proves latency, and neither stands in for the other.

A venue addition merges to local main on an acceptance run: the all-venues workload at the highest band the new
venue qualified, on the M1 with the Rust reference consumer's full path, watched live by the owner. The venue-alone
rows at every band the venue can populate and the all-venues rows at every band, both on the M1, come before it;
the box's all-venues rows and a fresh all-venues soak run are the next release's gates rather than merge
conditions. The fixture probe comes before the first row: one connection, one frame per documented family, a byte
cap and a deadline, authorized once for every venue addition, its frames curated into one file per venue under
`tests/support/` and its session recorded in the venue document.

Thread-CPU timing is an opt-in mode pairing CPU-clock readings with the same stage boundaries, retaining raw
intervals in the bounded tail witnesses. Its paired-read overhead is calibrated against wall clock before any
venue socket opens and is never subtracted from event latency. Failed sampled reads and nonmonotonic intervals
are reported explicitly: missing CPU evidence is unknown, not zero. Elapsed minus CPU distinguishes neither
preemption from blocking nor kernel/socket backlog. The diagnostic mode never qualifies a run.

Report per venue, family, transport, language, and mode: count, p50, p95, p99, p99.9, max, and the counts and
fractions above 100 µs, 250 µs, and 1 ms, with bounded dimensions. Decisions use p95 and p99
([ADR-0010](adr/0010-latency-target-judged-by-tails.md)); a p50 field stays descriptive and cannot establish a
tail improvement or an acceptance. Quantiles read from a histogram are upper bounds, and 10,000 events give only
about ten top-0.1% observations, not a worst-case guarantee. Every event above 1 ms stays accounted for even
when bounded numeric witness storage fills; a passing p99 does not excuse millisecond tails. Correlate stages on
the same events and never subtract independent percentiles. Report paired stage distributions, queue depth,
bytes and oldest age, retry episodes from first fault to success or deadline with the affected event's latency,
drops, generation/sequence/member-order faults, CPU, and memory. Failed runs and unmatched samples remain
visible. Every row also names its band, workload shape, market count per venue, connection layout and selection
policy, its machine, the measured window in UTC, per-venue messages received and events decoded, per-family
arrivals and their rate, and peak and end resident memory; memory is reported, never gated, and a figure is
never compared across machines. The ladder table is rendered from the run reports, never written by hand.

The hero figure, one per band for each venue and book-data family, is the p99 from complete local receive to the
Rust reference consumer's complete usable typed event on the all-venues workload: daemon, transport and Rust
reader, the package as shipped. Receive-to-handoff is the rail's component beneath it, measured once and not re-run
per transport; TypeScript on Bun and Python are their own rows, binding cost included. The [qualification
report](../bench/reports/native-upstream-flat-packed-100-m1.md) is the 50–100 band's upstream measurement on the
host it names. Its Polymarket snapshot population limits tail precision and its snapshot p99 already exceeds the
100–250 µs hypothesis at the upstream boundary; its rare millisecond handoffs with little thread CPU are evidence
of non-CPU delay, not of its cause. No hero figure exists while no consumer path exists to measure it, and neither
healthy accounting nor a generated decoder speedup establishes production consumer latency.

Only numeric histograms, bounded identifier and timing witnesses, digests, configuration, and build and host
provenance may be persisted ([ADR-0008](adr/0008-no-venue-payload-persistence.md)). No observed WebSocket frames,
venue JSON, decoded books, or binary event bodies reach disk; bounded in-memory events are allowed. The curated
observed frames under `tests/support/`, one file per venue, each venue addition taking its frames with the fixture
probe, and owner-authorized bounded probe samples are the only retained venue bytes; other deterministic
correctness tests use authored or generated protocol fixtures. Controlled synthetic load supplements local
transport selection with reproducible bursts, saturation, and slow or crashed readers; it never substitutes for
healthy live proof, and it persists generator seeds and parameters, never generated or captured payloads.

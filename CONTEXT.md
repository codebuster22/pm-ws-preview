# pm-ws

pm-ws is a Rust daemon for ultra-low-latency prediction-market WebSocket ingestion and local distribution of complete, venue-native events. This glossary fixes the words its code, its documents and its decisions use for venues, events, ownership, demand, delivery and measurement.

## Language

The _Avoid_ lists bind this project's own prose; where a venue contract quotes the venue's own wording, the venue's word stands.

### Venues and markets

**Venue**:
A prediction-market platform whose market-data WebSocket feed — open or keyed — pm-ws ingests, currently Limitless and Polymarket. Each venue owns its transport, protocol, event schemas, subscription wording and recovery.
_Avoid_: exchange, provider

**Market**:
One venue-native tradable question, named by a Limitless slug or address, or by a Polymarket condition ID.
_Avoid_: instrument, symbol, ticker

**Asset**:
One of the two outcomes of a Polymarket condition, named by its `asset_id`. A Limitless market carries no separate asset.
_Avoid_: token, outcome token

**CLOB market / AMM market**:
The two Limitless market mechanisms: a CLOB market is named by slug and carries `orderbookUpdate`, an AMM market is named by address and carries `newPriceData`.

**Leaf market**:
A Limitless market that carries its own condition ID and expiry, as opposed to a group container that only lists child markets.
_Avoid_: physical market

**Target**:
The subscription coordinate pm-ws acts on: a venue, a market, and an asset where the venue requires one.
_Avoid_: symbol, key

**Selection**:
The frozen set of targets chosen outside the daemon for one run, carrying each target's venue-native identifiers and expiry.
_Avoid_: universe, watchlist, market list

### Events

**Application message**:
One complete WebSocket message after transport reassembly, TLS and venue framing. It is the unit a venue delivers and it may carry several native events.
_Avoid_: frame, packet

**Native event**:
One complete, fully validated venue application event, retaining all its source fields, source order, exact numbers and provenance.
_Avoid_: tick, quote

**Family**:
The venue's own name for a kind of native event — `orderbookUpdate`, `book`, `price_change` and their siblings. An undocumented or unrecognized name is an unknown family, retained as a bounded envelope under its source name rather than projected onto a known one.
_Avoid_: event type, message type

**Batch**:
Every native event of one application message, admitted atomically in source order with each member's original index. A malformed member rejects the whole batch.
_Avoid_: bundle, transaction

**Source snapshot**:
A native event in which the venue supplies complete state for a market or asset, such as a Limitless `orderbookUpdate` or a Polymarket `book`.
_Avoid_: base, image, full book

**Source delta**:
A native event in which the venue supplies a change, such as a Polymarket `price_change`. pm-ws never derives one.
_Avoid_: diff, patch, incremental update

**Lifecycle event**:
A native event reporting a market's creation or resolution, delivered as supplied and never read as an instruction to change subscription or trading policy.

**Lifecycle feed**:
A venue's feed of lifecycle events for every market on the venue, which a session leases or an operator pins as a whole beside individual targets.
_Avoid_: firehose, all-markets channel

**Control record**:
A bounded record of protocol control rather than market data: a venue source-control event such as Limitless `system` or `exception`, or a transport-control witness such as a heartbeat or an outgoing subscription command.
_Avoid_: metadata event, housekeeping

**Arrival**:
One valid receipt of a native event on its owning connection. Repeated identities, equal content and conflicting content are each separate arrivals and each one publishes.
_Avoid_: duplicate, redelivery

### Identity, provenance and continuity

**Native identity**:
The venue-supplied equality material an event carries — timestamps, versions, hashes — delivered unchanged as data and never used locally to suppress, order or reconcile.
_Avoid_: dedup key, sequence number, message ID

**Exact decimal**:
An integer coefficient and a decimal scale, parsed from a source number so that no value is rounded or widened. Precision the representation cannot hold rejects the event instead.
_Avoid_: float, double, price value

**Lexeme**:
The source spelling of a number, preserved beside its exact decimal so a reader sees the venue's own digits.
_Avoid_: raw string, number token

**Provenance**:
The routing and local origin travelling with a batch: stream, source, slot, connection generation, stream generation, sequence, and receive and validation stamps.
_Avoid_: metadata, header

**Connection generation**:
The label of one socket assignment. A replacement connection or a subscription transition advances it, and a retired generation can never publish.

**Generation fence**:
The admission check that rejects a batch carrying a retired connection generation, so a late arrival from a retired assignment cannot cross a continuity boundary.

**Gap**:
An explicit continuity boundary published where events were or may have been lost — a reconnect, a rejected required event, overload, or a subscription transition. Its reported scope widens conservatively when loss cannot be localized. A local loss on one data attachment and an upstream boundary are distinct kinds: the first is also provable from the delivery sequence, the second only from the record.

### Ingestion and ownership

**Shard**:
One venue's unit of publication ownership: the single admission owner and the stream it publishes. A shard's targets are disjoint from every other shard's.
_Avoid_: partition, worker, thread

**Stream**:
One publication order owned by one venue shard. Publication order is admission order, not a claim about venue chronology.
_Avoid_: channel, topic

**Rail**:
One venue's path from socket to the upstream handoff, covering its transport, decoding, subscription and recovery.
_Avoid_: pipeline, lane

**Adapter**:
The venue-specific code inside a rail that owns protocol framing, family decoding, routing and subscription wording and declares its families, constants and credential fields. Venues are added in-tree and the core stays venue-agnostic.
_Avoid_: driver, plugin, connector

**Venue addition**:
The bounded in-tree sequence that brings one venue from documentation to a released adapter with typed support in every reference consumer, merged on an acceptance run; a feature of a minor release, never a runtime mechanism.
_Avoid_: plugin, integration, onboarding

**Venue document**:
The per-venue contract that pins the venue-declared transport and data, the selected families and their policy, health evidence, atomicity, etiquette and credential facts beside the owner-provided target-contract subsections; venue decisions live there rather than in a decision record.
_Avoid_: venue spec, integration guide, runbook

**Venue credential**:
A per-venue secret an adapter presents when it connects to a keyed market-data feed, with fields that adapter declares. It lives outside the tree and the update path, is read only by that venue's adapter and read again at every connect, is captured and verified by an operator command, and is never logged or handed to a reader. A venue's refusal of it stops that venue's attempts until the operator acts.
_Avoid_: token, auth

**Keyed feed**:
A selected venue feed whose handshake requires a venue credential before it admits data; a feed needing none is an open feed. Nothing after the handshake differs between the two.

**Owning connection**:
The one venue WebSocket that currently carries a market. One such connection carries many markets, and no market has two.
_Avoid_: peer, pool, replica

**Placement**:
The assignment of a target to an owning connection, and the opening and retiring of connections as demand changes. Moving a live target between connections is a subscription transition and publishes a gap.
_Avoid_: load balancing, pooling

**Admission**:
The single-owner decision that publishes a validated batch onto a stream or rejects it. The admission gate that makes it retains neither payloads nor native identities afterwards.
_Avoid_: enqueue, intake

**Fault**:
An explicit outcome that names its own reason and carries no market data, at the handshake (a rejected credential) or at admission (a stale source, exceeded capacity, an invalid event), each kept distinct from every other reason.
_Avoid_: error, failure

**Overload**:
Bounded capacity exceeded, producing explicit local loss and a gap. A slow or absent reader is never allowed to slow ingestion instead.
_Avoid_: congestion, throttling; "backpressure" names only the rejected behavior, never the outcome.

**Handoff**:
The transport-independent point where an admitted batch leaves upstream. Everything before it is upstream, everything after it downstream.
_Avoid_: delivery point, egress

**Receiver**:
Whatever takes the handoff inside the daemon, as distinct from a consumer reading published events downstream of it.
_Avoid_: consumer, sink

### Demand and control

**Demand**:
The aggregate wanted state of a target: wanted while an operator pin holds it or at least one client lease counts it. The desired set is the targets demand currently resolves to, and a target is unsubscribed only when its demand reaches zero or an operator drains it.
_Avoid_: subscription list, watchlist

**Operator pin**:
A demand claim on a target or a lifecycle feed held by the operator. It survives client disconnects, is released only by an explicit operator change, and by itself delivers to no session.

**Client lease**:
A releasable demand claim on a target or a lifecycle feed, owned by one client session and ending with its release or with the end of that session's control connection, never by time. Releasing one lease leaves that session's others intact, a new lease never forces a healthy subscription to resubscribe, and a session's leases are exactly what its data attachment receives.
_Avoid_: ticket, keepalive, subscription

**Drain**:
The operator's override that unsubscribes a target regardless of pins and leases and refuses new leases on it until the operator lifts it.
_Avoid_: blacklist, block, mute

**Session**:
One client's connection to the control plane, and the owner of the leases taken under it. Its end releases only its own leases, and it is not resumable: a reconnecting client is a new session.
_Avoid_: client, connection

**Control socket**:
The local socket on which the daemon takes demand and answers status. It carries commands and status only, never market data, and is neither the venue connection nor the data path to consumers.
_Avoid_: admin port, API, RPC

**Controller**:
A local client that submits desired sets over the control plane and never injects market data.
_Avoid_: publisher, admin client

**Reconciliation**:
Bringing an owning connection's subscribed set to the current desired set — Limitless by replacing the whole set, Polymarket by adding and removing incrementally — serialized, paced and generation-fenced.
_Avoid_: sync, replay, diffing

**Recovery**:
The WebSocket-only response to a broken or doubtful subscription: resubscribe, then reconnect if that is not enough. REST recovery is opt-in and never on the update path.
_Avoid_: failover, resync, refetch

**Heartbeat**:
The venue protocol's own liveness exchange, distinct from market activity: Limitless sends a server ping the client answers, and Polymarket answers a client ping. Its absence past a venue-guaranteed deadline is evidence; market silence is not.
_Avoid_: keepalive, liveness probe

**Acknowledgement**:
A venue's own statement that it accepted a subscription. Limitless returns one as a `system` event naming the markets that connection now carries; Polymarket publishes none, so a successful write is never read as one.
_Avoid_: confirmation, ACK

**Coverage**:
Observed evidence that a target's own data arrived on its owning connection, reported separately from the commands that requested the subscription.

**Stream directory**:
The per-target answer the control plane gives: which stream's sequence and gaps scope a target's own data, while a lifecycle arrival for it may also reach a session on the stream carrying its venue's lifecycle feed, and whether it is pinned, leased, drained and covered. A consumer attaches its session, never a stream, and reads the directory to interpret positions and gaps.
_Avoid_: registry, catalog, routing table

**Health**:
Connectivity, heartbeat, subscription evidence, local continuity and coverage taken together. Market activity is not an input, so a quiet subscribed target stays healthy.
_Avoid_: freshness, staleness, uptime

### Local delivery (target)

**Local transport**:
The same-host mechanism that will carry admitted batches from the handoff to consumers. None is selected; candidates are judged on measured fit against the delivery contract.
_Avoid_: IPC, transport layer, bus

**Consumer**:
A downstream reader of published events, in Rust, Python or TypeScript. None exists in the tree; the target is that it receives complete typed events without parsing venue JSON again, and builds for itself any book, action policy or economic normalization it needs.
_Avoid_: subscriber, downstream service

**Delivery**:
The daemon-side selection and copy of a published batch to each session entitled to it: a batch naming targets reaches the sessions leasing any of them, a lifecycle event also reaches the sessions leasing its venue's lifecycle feed, and a control record or gap reaches every session attached to its stream. A batch is delivered whole and at most once per session, each arrival on its own, and a session never receives an event whose only named targets it did not lease, so a consumer never filters.
_Avoid_: fan-out, broadcast, consumer-side filtering

**Data attachment**:
A session's single connection on which it receives delivered batches, separate from the control socket, opened with a start position per stream and living exactly as long as the session's control connection.
_Avoid_: subscription, data client, feed handle

**Delivery sequence**:
A contiguous count stamped on each delivery to one data attachment, carried beside the record so a reader proves it lost nothing locally. It is never a position to resume from.
_Avoid_: sequence number, message ID

**Reference consumer**:
The minimal reader in each supported language — Rust, TypeScript on Bun, and Python — that attaches to the local transport to prove delivery and measure tails per load band, typing every venue's families. It is the acceptance instrument, not a product.
_Avoid_: dummy consumer, example client, SDK

**Book**:
An order book assembled from source snapshots and deltas. It is a consumer's own construction; pm-ws publishes the events and builds none.
_Avoid_: latest state, price ladder, depth cache

**Retention**:
Target design: a bounded per-stream log of published batches, sized in both events and bytes, that a consumer reads at its own pace. It is a log, not a slot the newest state overwrites.
_Avoid_: cache, latest state, buffer

**Cursor**:
A consumer's position in a stream's retention, valid only at batch boundaries and advanced only after a complete batch is read and validated. A position no longer retained reports the expected and oldest available positions instead of rebasing silently.
_Avoid_: offset, read pointer

### Measurement

**Workload**:
The frozen measurement configuration a qualification run is judged on: its shape (the venues present: each venue alone or all venues together, never a pair), a frozen selection of a stated size per venue chosen by a recorded selection policy, a declared connection and shard layout, and frozen capacities. A qualification run refuses any demand change rather than changing the workload.
_Avoid_: benchmark config, test load

**Ladder**:
The ordered progression of qualification runs across load bands by which the achievable latency is discovered rather than fixed in advance. Its results are one generated ladder table, never a hand-kept record.
_Avoid_: benchmark suite, test matrix

**Load band**:
One rung of the ladder: a market-count range per venue, such as fifty to one hundred markets, whose workloads are judged together so the achievable latency is known per band rather than as one number.
_Avoid_: tier, scale level

**Stage**:
A named measurement boundary on an event's path, reported as a distribution over the same events rather than as an average.
_Avoid_: phase, step, span

**Qualification run**:
A live measurement run over one workload — a frozen selection, a declared connection layout and frozen capacities — counting only continuously healthy measured time. A reconnect, coverage drop, gap, resolved or expired target, demand change or correctness fault disqualifies it at every band; the run refuses a demand change before it can reach the rail, so that disqualifier is enforced by refusal rather than by ending the run.
_Avoid_: benchmark run, test run

**Soak run**:
A production-shaped run of the release binary on a named machine for a fixed duration, under the operator's demand at a qualified load band, in which faults, reconnects and gaps are counted and attributed rather than ending the run; it proves endurance while the ladder proves latency.
_Avoid_: stress test, long benchmark, endurance test

**Acceptance run**:
A qualification run of the all-venues workload at the highest load band a venue being added has qualified, on the production machine with the Rust reference consumer's full path, watched live by the owner; its qualified row is the condition for merging a venue addition.
_Avoid_: owner-present probe, demo run, sign-off run

**Hero figure**:
The figure published per load band for each venue and book-data family, taken from the all-venues workload on a named machine: the p99 from complete local receive to the Rust reference consumer's complete typed event. Reader rows in other languages and the receive-to-handoff component sit beside it, never in its place.
_Avoid_: headline latency, benchmark result, latency target

**Selection policy**:
The recorded rule by which a workload's markets were chosen from the discovered population, such as most active first by a named venue listing field.
_Avoid_: market filter, sort order

**Witness**:
A bounded retained timing or identifier sample kept to explain an outcome. It is never a venue payload.
_Avoid_: trace, capture

**Observed frame**:
Curated venue bytes taken from a live session, for a venue addition by the fixture probe, and kept as a decoder regression fixture, one set per venue. These and owner-authorized bounded probe samples are the only venue bytes the tree retains.
_Avoid_: recording, replay file

**Fixture probe**:
One bounded live connection a venue addition opens to take one observed frame per documented family, under a byte cap and a deadline, authorized once for every venue addition; the frames are curated into that venue's regression fixtures and nothing else is kept.
_Avoid_: capture session, recording, sampling run

**Release gate**:
A condition a release must show before it is published, as a command's outcome or a generated report, never as a claim: the check green on each release machine, dependency policy and advisories clean, the package list clean, the security pass done, the required ladder rows and the soak present.
_Avoid_: milestone, sign-off, definition of done

**Release machine**:
A named machine on which the release gates run and whose ladder rows the release quotes; the soak run belongs to the one that is also the production machine. A machine that is not a release machine may produce rows but never gates.
_Avoid_: build box, CI runner, reference hardware

### Venue traffic etiquette

**Etiquette**:
This project's own conservative limits on venue traffic — connection attempts, command spacing and REST selection — chosen by the owner rather than published by a venue.
_Avoid_: rate limit, quota

**Attempt ledger**:
The process-wide rolling record that spends connection attempts against the etiquette budget and names when the next one comes free.

**Command pacer**:
The process-wide, per-endpoint reservation that spaces subscription-bearing commands on the wire.
_Avoid_: throttle, rate limiter

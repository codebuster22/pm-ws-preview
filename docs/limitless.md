# Limitless venue contract

This document records the behavior the Limitless adapter may rely on, the behavior it must verify, and the behavior it must not assume.

It separates official documentation, inspected SDK behavior, user-confirmed domain behavior, observations, and unverified assumptions.

## Rules

- Official documentation and official SDK source are preferred over examples from third parties.
- A documentation example does not establish an ordering, delivery, precision, or recovery guarantee unless the text says so.
- Observed production behavior remains an observation until the venue documents it.
- User-confirmed matching semantics are recorded as domain knowledge and protected by conformance tests.
- Venue changes update this document before adapter behavior changes.
- Credentials, authenticated payloads, and sensitive captures are never committed.

## Transport

Documented:

- Endpoint: `wss://ws.limitless.exchange`.
- Socket.IO namespace: `/markets`.
- WebSocket transport is used without polling fallback.
- The server initiates the Socket.IO/Engine.IO heartbeat and the client responds with pong.
- Clients must not add a custom application-level PING loop.

Adapter consequence:

The Limitless rail owns Socket.IO framing and lifecycle. Socket.IO messages must not leak into the venue-agnostic core. Engine.IO opening values such as negotiated ping interval, ping timeout, and maximum payload are session inputs to validate and enforce, not constants copied from examples.

Connection health follows Socket.IO session and heartbeat behavior. An assigned market remains live during arbitrary order-book silence while the Socket.IO connection and its subscription evidence remain healthy and no known message loss has occurred. The adapter must not create a per-market inactivity timeout that the venue does not document.

Observed 2026-08-31, one 60 s public connection subscribed to CLOB market `eth-up-or-down-daily-1788105600`:

- The Engine.IO `open` packet carried `pingInterval` 25000, `pingTimeout` 60000, `maxPayload` 1000000, `upgrades` []. A server ping arrived roughly every 25 s; the client's pong kept the session healthy for the full 60 s capture. These are observed values for one session on one connection, not a policy; the adapter still reads them from the wire per connection rather than assuming them.
- The `/markets` namespace connect ack (`40/markets,{"sid":...}`) carries its own `sid`, distinct from the Engine.IO session `sid` carried in the `open` packet.

## Public CLOB subscription

Documented:

- `subscribe_market_prices` accepts CLOB market slugs.
- A new subscription call replaces the connection's previous subscription set.
- Subscriptions must be restored after reconnect.

Adapter consequence:

The adapter maintains a complete desired set per connection and serializes reconciliation. Core subscription commands do not write directly to the socket. If live conformance cannot establish an acknowledgment and frame boundary that separates the old set from the replacement set, reconciliation uses a fresh connection generation and fresh books before publication.

Observed 2026-08-31, via one `GET https://api.limitless.exchange/markets/active` (counted against the REST etiquette below): recurring up-or-down CLOB markets follow the slug pattern `<asset>-up-or-down-<cadence>-<start-epoch>` with cadences `5-min`, `15-min`, `hourly`, `daily`; the start epoch is UTC-aligned to the cadence boundary, the reported `deadline` is start plus the cadence in milliseconds, and future markets are created ahead of their window. The endpoint listed 25 active markets with `slug`, `deadline`, and `status` fields; `?limit=200` was rejected with HTTP 400. The owner designates the `btc-up-or-down-5-min-*` series as the preferred live-verification target because of its update frequency; slug construction from the clock is a convenience for selecting targets, never market policy.

Observed 2026-09-02 (six probing lookups, counted against the REST etiquette below): `/markets/active` is paginated at 25 rows per page via `?page=N`; `totalMarketsCount` is the live open-market count, not a lifetime aggregate (856 earlier in the day, 822 at probe time), and the observation that one bare `GET` "listed 25 active markets" was page 1 only. At probe time the open set spanned 33 pages (page 33 partial with 22 rows, a page past the end answering zero rows with `totalMarketsCount: 0`). Later pages mix `tradeType` values (`clob` and `amm`), so an orderbook fleet is the `clob` subset of a full page walk, not one page. `?limit=50`, `?limit=100`, and `?pageSize=100` each answered zero rows rather than a larger page, so page-walking is the only bulk mechanism observed. Newly listed recurring markets were observed arriving on page 1, so a page-1 re-fetch suffices for new-listing polling; a full sweep costs one call per page against the operator REST default below.

Observed 2026-09-06 (the S8 full-fleet discovery sweeps and runs, counted against the REST etiquette below): `/markets/active` answers HTTP 403 to `Python-urllib`'s default user-agent while accepting `curl`'s default and a truthful tool-identifying one, so `bench/discover_markets.py` sends `pm-ws-bench-discover/1.0 (operator market selection)`. The open set spanned 23 pages at 546-558 CLOB markets across the day's sweeps (the 33-page/~820 observation above was four days earlier — page count follows the set). The first full-fleet live runs, both named hosts, each held 546-547 pinned CLOB markets over 20 shards for a 900 s window: every pinned shard kept one socket for the whole window, the overflow shard cycled 10-12 sockets via subscription replacement as mid-run leases came and went (~31 connection attempts per run against the 280/day-per-shard ledger), and no pushback of any kind was observed — no 429, no unexpected close, no throttling signal.

## Order-book event

Documented:

- Event name: `orderbookUpdate`.
- The event is keyed by `marketSlug`.
- The nested order book contains complete bid and ask arrays.
- Bids are ordered highest price first and asks lowest price first.
- An event timestamp is present.

Field names re-retrieved on 2026-08-26 and recorded in [the checked claims](#checked-claims) below:

- The payload is `{ "marketSlug": string, "orderbook": { "bids": [{ "price": number, "size": number }], "asks": [...] }, "timestamp": string }`.
- `timestamp` is an ISO-8601 string rather than an epoch number.
- `price` and `size` are JSON numbers, and the documentation instructs callers to coerce defensively to preserve decimal precision.
- `marketCreated` and `marketResolved` identify the market with `slug`, not `marketSlug`, and carry ISO-8601 `createdAt` and `resolutionDate` respectively.

SDK implementation evidence, not a documented public-feed guarantee:

- The pinned official Rust SDK requires a book `tokenId` and associates it with the YES token.
- The current public event example omits `tokenId`; live conformance must determine whether it exists, what it identifies, and whether it is stable across reconnects. Observed 2026-08-31 (below): present on every event this session; identity and stability across reconnects and markets remain open.

Observed 2026-08-31, one 60 s public connection, one active market, 17 `orderbookUpdate` events, 0 decode failures:

- `orderbook.tokenId` was present on all 17 events, a 77-digit decimal string, unchanged across the session.
- Every level object also carries `side` (`"BUY"` on bids, `"SELL"` on asks), redundant with array position.
- `orderbook` also carries `adjustedMidpoint`, `midpoint`, `maxSpread`, and `minSize` as JSON numbers. Their semantics are not established. Observed `minSize` (100000000) did not bound the smallest resting size seen (asks as small as 1441000), and observed `maxSpread` (0.035) did not bound the observed best-bid/best-ask spread (0.207 = 0.219 − 0.012). `midpoint` matched the arithmetic mean of best bid and best ask; `adjustedMidpoint` did not. None of these fields are treated as a constraint on, or correction to, the book.
- The event object carries a top-level integer `version` (e.g. `7861372`) alongside the documented ISO-8601 `timestamp`. It is monotone per connection-session (observed below) with unestablished scope and no ordering meaning — see [Ordering and redundant connections](#ordering-and-redundant-connections).
- Precision observed: prices at up to 3 decimal places within [0.001, 0.999]; sizes as plain integer lexemes (e.g. `100000000`), consistent with a fixed scaled unit whose scale is not verified.
- Ordering observed: bids strictly descending and asks strictly ascending in all 17 events, matching the documented order.
- The same market room also delivered an undocumented event `oraclePriceData`, roughly once per second, independent of `orderbookUpdate`: `{marketAddress: null, marketSlug, source: "pyth-pro", timestamp (epoch milliseconds, number), value (decimal number)}`. The adapter preserves it as `LimitlessEvent::Unknown`, never as book data.
- On this active market, `subscribe_market_prices` was followed within about 1 s by a complete `orderbookUpdate` book (10 bids, 20 asks). See [Recovery](#recovery) — the idle-market case remains open.

Adapter consequence:

- A valid event is normalized as an atomic candidate replacement, not as a venue delta.
- Level mutations are derived by comparing accepted snapshots.
- Derived differences state old and new depth but do not infer adds, cancels, fills, or intermediate actions.
- A known lost `orderbookUpdate` breaks derived-mutation continuity. The last book is stale until a later complete update or explicit resubscription snapshot restores current-state authority.

User-confirmed domain behavior:

- The opposite outcome book is the side-swapped price complement of the canonical outcome book, with identical depth.
- Both views identify the same liquidity and must not be counted independently.

Conformance requirement:

Normalize both available outcome representations into the same coordinates, compare deterministic state digests, measure skew, and degrade the adapter if the invariant fails.

## Ordering and redundant connections

Documented:

- The public order-book event includes a timestamp.

Not documented:

- A monotonic per-market sequence.
- A globally comparable version across connections.
- A checksum or state hash with ordering meaning.
- Monotonicity or uniqueness guarantees for the event timestamp.
- A per-market event cadence that could make silence evidence of loss.
- Any meaning, scope, or monotonicity for a top-level integer `version`, observed on the wire but not documented (below).

Observed 2026-08-31: a top-level integer `version` (e.g. `7861372`) is present on every `orderbookUpdate` this session. At that time its scope and monotonicity were not established; the second same-day session (below) established strict per-connection-session monotonicity, while scope and ordering meaning remain unassumed. See [Order-book event](#order-book-event).

Observed 2026-08-31 (second session, one connection, one active 5-minute market, full 300 s window, 1023 `orderbookUpdate` events): `version` was present on every event and strictly increased in arrival order (526135 to 564536), with 502 steps larger than one. A concurrent two-connection session on another market received near-identical version streams on both connections. The counter is therefore monotone per connection-session but non-contiguous per market: it cannot detect per-market gaps, its scope remains unestablished, and it still carries no ordering meaning.

Observed 2026-09-01 (three dedicated conformance sessions with `--log-versions` instrumentation tying each accepted `orderbookUpdate`'s `version` to the committed book's canonical content digest):

- Same market, two concurrent connections (primary plus hot standby), one full 5-minute window (`btc-up-or-down-5-min-1788216000`, 782 accepted updates): both connections received identical 391-key streams (range 1993584 to 2002185), zero inversions, zero duplicates, identical gap structure; across the 232 book states observed by both connections, the per-connection key sets per digest differed on none. Key-to-content agreement across connections was exact for the whole window.
- Live reconnect (`btc-up-or-down-5-min-1788216300`, publishing connection killed at t=100 s): the counter continued across the replacement connection (last key 2007630, first key 2007696) rather than resetting. The reconnected session began with one adjacent duplicate key (2007696) whose two deliveries carried byte-identical content — the same redundant equal-content delivery pattern recorded for `marketResolved`. Per connection-session the counter is therefore non-decreasing rather than strictly increasing, and everywhere it was observed, equal keys carried equal content.
- Two markets on two concurrent connections (`btc-up-or-down-hourly-1788213600` and `eth-up-or-down-hourly-1788213600`, 120 s): both streams drew from one interleaved numeric region (btc 2013164 to 2016604 inside eth 2013048 to 2017202), each monotone with zero inversions. The counter's scope is venue-global, and per market it remains non-contiguous, so it still cannot detect per-market gaps.

These remain observations — `version` is still undocumented upstream. Together they are the S4b conformance basis for a version-gated active-active pool on this venue: duplicate keys are dedup evidence (equal content observed), a key exceeding the last published key identifies newer state on any healthy connection to the same market, and because the ordering rests on observation rather than documentation, pooled publishing requires a live inversion tripwire that degrades to primary/standby on the first observed violation.

Adapter consequence:

Arrival order across redundant connections is not authoritative. Publishing across redundant connections is by `version` key on the 2026-09-01 conformance basis above — first arrival whose key passes the last published key, never by arrival order — with the live tripwire degrading back to one publishing primary on the first observed violation. A connection not publishing under that licence maintains independent shadow state instead, and redundant streams are compared through a deterministic normalized book digest that excludes connection metadata and timestamps. Which of the two a rail runs is `docs/design.md`'s decision and not a venue fact: the single-market rail opts in per run, the daemon's shard rail runs the key gate wherever this declaration admits it.

Without a sequence, checksum, or documented cadence, the adapter cannot prove that the upstream omitted an event while Socket.IO heartbeat remains healthy. Redundant divergence can reveal some discrepancies, but an inactivity timer cannot establish continuity loss.

## Market lifecycle events

Documented:

- `subscribe_market_lifecycle` subscribes to public market creation and resolution events.
- `unsubscribe_market_lifecycle` removes that lifecycle subscription.
- `marketCreated` identifies a newly visible market by native slug and includes venue-native descriptive and grouping metadata.
- `marketResolved` identifies a market by native slug and reports the winning outcome, winning index, market type, and resolution timestamp.
- `marketResolved` is also sent to existing per-market room subscribers.

Adapter consequence:

- Resolution is published as a venue report with the native slug, winning outcome, winning index, and source timestamp preserved.
- Resolution does not close, freeze, clear, or otherwise alter the local order book.
- Book updates received after resolution continue through normal validation and application.
- Resolution does not cause automatic unsubscription.
- The adapter must not claim stronger finality than the documented event.
- Resolution coverage for ordinary book subscriptions follows the subscribed market rooms. Venue-wide lifecycle coverage is a separately requested data family.
- Lifecycle and per-market delivery paths may overlap. Each accepted arrival preserves its delivery path. Unless live evidence finds a stable event identity, equal content from both paths is not claimed to be exactly deduplicated.
- Redundancy handling must not suppress distinct events from the publishing source merely because their content is equal.

Observed 2026-08-31: `marketResolved` for a subscribed market's own room arrived three times with byte-identical content within one second (resolution timestamp 2026-08-31T16:00:27.015Z). Redundant equal-content deliveries are real; each accepted arrival was reproduced. The book received before resolution stayed live through it.

Observed 2026-09-01: `marketResolved` for `btc-up-or-down-5-min-1788267900` arrived on the market's own room three times byte-identically within 200 ms (resolution timestamp 2026-09-01T13:11:02.813Z, ~63 s after the window's nominal 13:10:00Z deadline), reproducing the redundancy pattern on a second market class. Identity, layer by layer: the payload carries exactly the five documented keys (`slug`, `type`, `winningOutcome`, `winningIndex`, `resolutionDate`) — no id, no `version`, no sequence — and no Socket.IO acknowledgment id appeared on any event frame in the session, so the three arrivals are indistinguishable at every layer and no delivery identity exists. The event itself is naturally keyed by `(slug, resolutionDate)`; that names the resolution report, not the delivery, and whether the venue can re-report or amend a resolution under the same or a different key is undocumented, so equal content across overlapping paths is still not claimed to be exactly deduplicated. After resolution this 5-minute market's per-market flow stopped entirely within the observed session: no further `orderbookUpdate` (last at 13:09:45.277Z) and no further `oraclePriceData`. The resolution frame and the last preceding book update are pinned byte-exact in `tests/wire_observed.rs`.

Observed 2026-09-02 (daemon-scale sessions over the venue's full open market list): one
`GET /markets/active` answered 25 open markets spanning several 5-minute series (`btc-`, `eth-`,
`sol-up-or-down-5-min-<epoch>` among them). A single connection subscribed to all 25 carried the
whole set without venue error or throttling; across one 480-second window the daemon accepted
2,010 book snapshots and 36 `marketResolved` deliveries as the 5-minute windows rolled, every
resolution on the market's own room, flow stopping per market after its resolution exactly as
pinned above. Book depth on the 5-minute series stayed within a handful of levels in every
sampled window (best sides carrying 2-5 levels); no observed book approached the scale
profile's declared 256-level depth, and none approached `MAX_BOOK_LEVELS`.

The current required daemon scope includes `marketResolved`. `marketCreated` is pinned here for the future subscription-selection boundary but is not required for command-driven explicit subscriptions.

## Recovery

Required design behavior:

1. Mark the book stale when authority is lost.
2. Attempt venue-native resubscription for the affected market.
3. Await and validate a complete authoritative snapshot.
4. Reassign or reconnect the stream when resubscription fails.
5. Replace the book atomically and emit recovery continuity.

Observed 2026-08-31: on an active market, `subscribe_market_prices` was followed within about 1 s by a complete `orderbookUpdate` book (10 bids, 20 asks), for one 60 s session on one connection.

Observed 2026-08-31 (evening): one public connection subscribed to a quiet daily market held a single 3600 s session: 22 `orderbookUpdate` events with silent stretches of many minutes, server pings roughly every 25 s answered throughout, no venue-side disconnect, and the local book stayed live for the full hour. The market resolved at its deadline during the session; delivery continued normally around the resolution. This settled that an initial subscribe on a quiet (not fully idle) market returns a complete book and that hour-long silences never stale a healthy session; it did not settle live resubscribe (none was exercised) nor subscribe on a fully idle market — this one still produced 22 updates.

Unverified assumption:

Whether resubscribe (not just initial subscribe) produces an immediate complete book, and whether subscribe or resubscribe produces one at all when the market is idle.

REST is not part of normal ingestion. It may be enabled only as an explicit recovery fallback if WebSocket conformance shows that subscription recovery cannot establish authority.

After bounded WebSocket attempts fail to produce a base, the book remains stale with `recovery_base_unavailable`. The adapter does not promote a divergent standby or reuse an old book merely to regain live status.

## Precision

Documented payload examples and the official SDK use JSON numeric price and size fields. The public event documentation does not state a maximum decimal precision.

Adapter requirements:

- Preserve the lexical JSON number and parse it exactly rather than converting through floating point.
- Reject negative size, out-of-range price, malformed decimal, invalid ordering, and unrepresentable precision.
- Record a schema or precision violation and mark the affected candidate invalid.
- Never silently round authoritative market data.

## Adapter profile — pre-live assumptions

Except where a stronger basis is named, each statement below is a working assumption consolidated from the retired Sprint 2 adapter work and is unverified against a live socket. The first live connection confirms or corrects it here.

1. Native market identity is slug-shaped. The canonical subscription key is the venue's CLOB market slug, and the core treats its value as opaque.
2. Subscription is complete-set replacement. The adapter holds one aggregate desired set per connection and reissues it whole; it never replays a history of individual subscription commands.
3. Source timestamps carry no ordering guarantee. Stronger basis: this is a documented absence, recorded under [Ordering and redundant connections](#ordering-and-redundant-connections) — no retrieved Limitless page documents a monotonic per-market sequence, a cross-connection version, or timestamp monotonicity.
4. The public feed is unauthenticated. Neither `subscribe_market_prices` nor `subscribe_market_lifecycle` presents a credential. Partially observed 2026-08-31: `subscribe_market_prices` succeeded (`system` ack "Successfully subscribed to market price updates") with no credential in the captured frames of that session; `subscribe_market_lifecycle` was not exercised.
5. Recovery is resubscribe-then-reconnect. Per-target resubscription is attempted first on a usable connection; full connection replacement is the fallback, not the default.
6. One physical book per market, with the opposite outcome view derived as its side-swapped price complement over identical depth. Stronger basis: user-confirmed domain knowledge from the project owner, recorded under [Order-book event](#order-book-event) and in the appendix.

## What the first live connection must settle

This is the test plan for the first live contact, merged from the formerly separate feed-contract, vendor-assumption, and SDK-study question lists.

| Assumption | Current basis | Validation | Consequence if false |
|---|---|---|---|
| Limitless sends an authoritative full book after a market subscription or resubscription. | User experience and intended recovery model; not explicitly guaranteed in the reviewed public documentation. Partially confirmed 2026-08-31: initial subscribe on an active market produced a complete book (10 bids, 20 asks) within about 1 s, for one 60 s session. Live resubscribe remains untested; a quiet market's initial subscribe returned a complete book (3600 s session under Recovery), and a fully idle market remains untested. | Live subscribe, unsubscribe, and resubscribe conformance tests across idle and active markets. | WebSocket-only recovery needs a different adapter procedure or an explicitly enabled REST fallback. |
| Limitless mirrored outcome views contain identical underlying liquidity. | User-confirmed matching-engine behavior. | Subscribe to both representations where possible, normalize them to one coordinate system, and compare deterministic book digests. | The adapter must retain independent physical books or quarantine divergence. |
| A Limitless event timestamp is useful for freshness but not a globally comparable cross-connection sequence. | No documented global ordering guarantee was found. | Compare redundant live streams and monitor inversions; continue treating timestamp as non-authoritative unless documentation changes. | Active-active arrival-order merge remains unsafe. |
| Reissuing the complete desired subscription set is safe and idempotent. | Public documentation says subscription calls replace the previous set. | Repeated subscription and concurrent control conformance tests. | The adapter needs stricter acknowledgment and generation handling. |
| Venue decimal precision fits the selected fixed-point representation. | Current examples and SDK behavior fit within practical fixed precision. | Retained-frame decoder tests and a live precision tripwire. | Reject affected data and revise the shared-state schema deliberately; never silently round. |
| A live Limitless book contains enough outcome identity to validate any configured token alias. | Public documentation keys the book by slug and omits `tokenId`; the pinned official SDK requires one. Observed 2026-08-31: `tokenId` present on all 17 `orderbookUpdate` events, one active market, one connection; not yet verified across reconnects or other markets. | Capture live payloads across initial subscription and reconnect, then compare with versioned external descriptors. | Slug and venue-defined sides remain canonical; token aliases stay unavailable or conflicting rather than being guessed or fetched through hidden REST. |

Protocol and event questions still open:

- Whether Engine.IO opening values are stable across sessions and markets (observed once, 2026-08-31, above), and server behavior when pong is late or absent.
- Whether pings can arrive during namespace setup and whether other packets can precede the namespace acknowledgment.
- Exact `system` or acknowledgment behavior for successful, partial, empty, rejected, and oversized market sets.
- The causal boundary between replacement sets and subsequent book frames.
- Whether subscribe, remove/re-add, and reconnect produce a complete snapshot for an idle market.
- Maximum identifiers and markets per command/connection, rate limits, payload limits, compression, and fragmentation behavior.
- Whether resolution delivered through both paths is byte-identical and whether any stable event identity exists.
- Behavior for malformed, unsupported, and future Socket.IO event shapes.
- Whether re-emitting an unchanged set returns fresh books.

## Live venue etiquette

Amended 2026-09-05 under the owner directives of 2026-09-03. The numeric ceilings this section carried since 2026-08-27 — at most 4 concurrent connections, 300 connection attempts per day, 2 commands per second sustained, 60 REST requests per hour — are retired as venue policy. They were self-imposed live-test safety ceilings from the Sprint 2 authorization record, and a 2026-09-05 sweep of the venue documentation set and Terms of Service (appendix) confirms no venue document places any of them. What the venue actually publishes, how this project responds to venue signals, and which numbers are this project's own choices are recorded separately below. Venue-signal response is the primary safety mechanism; a limit the venue is observed to enforce live is recorded here as documented by observation and then encoded.

### Venue-documented limits

Everything the venue publishes that bounds a public-data consumer, per the 2026-09-05 sweep:

- REST rate limits exist, but no numeric threshold is published. Exceeding one returns HTTP `429 Too Many Requests`, optionally carrying `Retry-After` in seconds; remaining allowance is visible at runtime in the `x-ratelimit-remaining` response header; higher published limits or a dedicated quota are by arrangement with the venue (`help@limitless.network`).
- The venue's own `429` guidance: respect `Retry-After` when present; otherwise back off exponentially starting at 1 second, doubling per retry; throttle at the source; never retry `400` or `401`.
- No retrieved venue document places a limit on concurrent WebSocket connections, connection attempts, WebSocket command rate, markets per subscription, or payload size. The WebSocket guides, the API reference, and all four SDK documentation sets were searched on 2026-09-05. These are unestablished, not permitted-by-silence: the first observed enforcement becomes a documented-by-observation fact here.
- The Terms of Service contain no clause addressing automated or programmatic access to public data — no API, bot, scraping, or crawling language anywhere in the document (verified in the retrieved bytes, appendix). Its conduct clauses prohibit market manipulation, fraud (including exploiting technical vulnerabilities), and criminal activity, and the venue reserves sole-discretion suspension and termination at any time.

### Venue-signal responses

The primary safety mechanism. These stand whatever the configured numbers are:

- Abort immediately, and tell the owner, on: venue throttling past this project's own abort threshold of more than 3 rejections in 10 minutes; any unexpected authentication challenge; any credential or otherwise sensitive payload appearing on the wire; or any sign of impact on the venue.
- On REST `429`: honor `Retry-After` when present, otherwise exponential backoff from 1 second doubling per retry — the venue's own published procedure.
- On any observed venue throttling or pushback during a ramp: descend — reduce connections or rate until the signal clears — and record the observed bound above as documented by observation.

### Operator-chosen defaults

This project's own numbers, conservative by default and operator-configurable; none is venue policy:

- Connection capacity is fleet-sized: shard count follows the configured market list, each shard peaking at one live socket plus one fenced socket still draining, with `max_venue_connections` an optional operator-declared cap (owner 2026-09-03: an artificial refusal serves nobody aiming to be the go-to connector). The first full-fleet live run starts at 20 connections with abort conditions armed, descending on any venue pushback (owner 2026-09-03: start high and come down as we hit problems).
- Sustained command pacing defaults to one subscription-bearing command per 500 ms per endpoint (`min_command_interval_ms`).
- The connection-attempt ledger admits 280 attempts per rolling day by default (`daily_connection_attempt_budget`).
- Read-only public REST lookup is used only to select target markets, at a working default of no more than 60 requests per hour. It is never an ingestion or recovery path.

### Scope and capture rules

Unchanged, and not subject to configuration:

- Public, unauthenticated market data only. Never connect to an authenticated channel, a trading channel, or an order or position stream.
- Captures are written to a disposable directory outside this repository. Credentials, cookies, and tokens are structurally redacted before anything is retained. Public venue-native identifiers such as `tokenId` are kept as-is.

## Appendix: sources

### Official documentation

| Source | Purpose |
|---|---|
| [Documentation index](https://docs.limitless.exchange/llms.txt) | Discover the current official documentation surface. |
| [WebSocket integration](https://docs.limitless.exchange/developers/quickstart/websocket) | Connection, authentication modes, subscription examples, events, and reconnect guidance. |
| [WebSocket events](https://docs.limitless.exchange/developers/websocket-events) | Current event reference and order-book payload. |
| [Rust SDK WebSocket documentation](https://docs.limitless.exchange/developers/sdk/rust/websocket) | Official Rust client surface, subscriptions, handlers, and reconnect claims. |
| [Rust SDK market documentation](https://docs.limitless.exchange/developers/sdk/rust/markets) | Slug, outcome token metadata, and public market lookup surface. |
| [Developer documentation](https://docs.limitless.exchange/) | Product and API entry point. |
| [Market SDK documentation](https://docs.limitless.exchange/developers/sdk/typescript/markets) | Market identifiers and order-book retrieval semantics. |
| [API reference introduction](https://docs.limitless.exchange/api-reference/introduction) | Rate-limit behavior: `429`, `Retry-After`, backoff guidance, quota contact. |
| [Terms of Service](https://docs.limitless.exchange/user-guide/terms-of-service) | Conduct rules; checked for automated-access clauses. |

### Checked claims

Each claim below was verified present in the bytes retrieved over plain HTTPS `GET` on the date shown. No response body is retained in this repository.

| Checked heading | Checked claim | Retrieved |
|---|---|---|
| Connection | `URL: wss://ws.limitless.exchange`; `Namespace: /markets`; `Transport: WebSocket only (no polling fallback)`. | 2026-08-26 |
| Connection | `No client PING required. The server runs the Socket.IO heartbeat (server-initiated ping / client pong) automatically. Do not send your own PING frames.` | 2026-08-26 |
| `orderbookUpdate` | `marketSlug` string CLOB market slug; `orderbook.bids` highest price first; `orderbook.asks` lowest price first; `timestamp` string ISO-8601 event timestamp; `price` and `size` are JSON numbers, coerce defensively to preserve decimal precision. | 2026-08-26 |
| `marketCreated` | Emitted when a new market is funded and visible; hidden markets excluded; fields `slug`, `title`, `type`, optional `groupSlug`, optional `categoryIds`, ISO-8601 `createdAt`. | 2026-08-26 |
| `marketResolved` | Emitted when a market resolves; sent to both `market_lifecycle` subscribers and existing `market:{slug}` room subscribers; fields `slug`, `type`, `winningOutcome`, `winningIndex`, ISO-8601 `resolutionDate`. | 2026-08-26 |
| Subscribing to market lifecycle events | `subscribe_market_lifecycle` requires no authentication; `unsubscribe_market_lifecycle` removes it. | 2026-08-26 |
| Subscribing to market prices | `Emit subscribe_market_prices with market identifiers. Subscriptions replace previous ones, so include all markets you want in a single call.`; CLOB markets use `marketSlugs`. | 2026-08-26 |
| Rate limits ([`api-reference/introduction.md`](https://docs.limitless.exchange/api-reference/introduction.md)) | `The API enforces rate limits. When you exceed a limit, requests return HTTP 429 Too Many Requests.`; `Respect Retry-After — when present on a 429 response, wait for that many seconds before retrying.`; `Exponential backoff — if no Retry-After header is returned, back off starting at 1 second and double on each retry.`; `Never retry 400 or 401`; `For higher published limits or a dedicated quota, contact help@limitless.network`. No numeric threshold appears anywhere on the page. | 2026-09-05 |
| Reading rate-limit state ([`developers/sdk/rust/error-handling.md`](https://docs.limitless.exchange/developers/sdk/rust/error-handling.md)) | The `*_with_raw` response surface exposes `rate-limit metadata` headers, read as `x-ratelimit-remaining` in the page's own example. | 2026-09-05 |
| Terms of Service ([`user-guide/terms-of-service.md`](https://docs.limitless.exchange/user-guide/terms-of-service.md)) | `Last Updated: June 19, 2026`; operated by `Street Chow Inc., a Panamanian company`; prohibits market manipulation (`wash trading, spoofing, layering, or coordinated trading`), fraud (`attempting to hack or compromise the Platform's security, exploiting technical vulnerabilities`), and criminal activity; reserves sole-discretion suspension and termination `at any time, with or without prior notice, for any reason`. Verified absent from the same bytes: any occurrence of API, bot, script, scraping, or crawling language, and any automated-access clause — the sole match for that pattern set is `automatically deducted` in the transaction-fees clause. | 2026-09-05 |

### Official TypeScript SDK snapshot

| Field | Value |
|---|---|
| Package | `@limitless-exchange/sdk` |
| Package version | `1.1.0` |
| Repository snapshot commit | `aa827e46d262c44ba11ffc7254d9445ca2272db8` |
| Repository | [limitless-exchange-ts-sdk](https://github.com/limitless-labs-group/limitless-exchange-ts-sdk/tree/aa827e46d262c44ba11ffc7254d9445ca2272db8) |

The inspected snapshot is design evidence, not a project dependency. Development must pin an upstream source or vendored snapshot deliberately. Observed 2026-09-06: the repository default branch advanced one merge past the pinned snapshot (to `1d5b3a7c44af1bfba97ce7b5bd8c2f859cdfeeba`, "feat/1.1.0-additional-ext", including a "fix orderbook dto" commit) with the published npm version unchanged at `1.1.0`.

### Official Python SDK snapshot

| Field | Value |
|---|---|
| Package | `limitless-sdk` (PyPI) |
| Package version | `1.1.0`, released 2026-08-11 |
| Repository snapshot commit | `a26ea4b3ffd29bba3e7b3b3bd5d721aa17a57935` |
| Repository | [limitless-sdk](https://github.com/limitless-labs-group/limitless-sdk/tree/a26ea4b3ffd29bba3e7b3b3bd5d721aa17a57935) |
| Python requirement | `>=3.8` |
| WebSocket transport | `python-socketio>=5.11.0` (`limitless_sdk/websocket/client.py` imports `socketio.AsyncClient`) |

Recorded 2026-09-06 from the PyPI project page, the repository at the snapshot commit, and [`developers/sdk/python/websocket.md`](https://docs.limitless.exchange/developers/sdk/python/websocket). Subscription surface: `WebSocketClient.subscribe("subscribe_market_prices", {"marketSlugs": [...]})` with `async def` handlers registered via `@ws_client.on("orderbookUpdate")`; auto-reconnect defaults on with `reconnect_delay` 1.0 s, capped exponential (`reconnection_delay_max = min(reconnect_delay * 32, 60)`), and a `max_reconnect_attempts` setting. A fourth official SDK exists for Go (`github.com/limitless-labs-group/limitless-exchange-go-sdk`), not inspected.

### Official Rust SDK snapshot

| Field | Value |
|---|---|
| Package | `limitless-exchange-rust-sdk` |
| Package version | `1.1.0` |
| Repository snapshot commit | `b1ad2e108e96f05b615d513d3f32237a775177f8` |
| Commit date | 2026-08-11 |
| Repository | [limitless-exchange-rust-sdk](https://github.com/limitless-labs-group/limitless-exchange-rust-sdk/tree/b1ad2e108e96f05b615d513d3f32237a775177f8) |
| `src/websocket.rs` SHA-256 | `13acb731f99f73c7a5d497a284a66165fd09cd54e7a166b208cb4e1d1e928a4a` |
| `src/markets.rs` SHA-256 | `59087f04cc908400716aac22c6d69aedbbb80b77abc34424086a8a5b363d1299` |
| `Cargo.toml` SHA-256 | `7530f75cf22741f901c5ebbaeffa95a1c09366becbd2b2ab245548b3791d3c86` |


The SDK snapshot is a reference and conformance input, not a dependency candidate for the daemon. Verified 2026-09-06: the repository default branch still points at the pinned snapshot commit. Whether the crate is published to the crate registry, and at which version, is unestablished (registry pages were not directly retrievable); anything consuming this SDK pins the git commit above.

### Domain knowledge supplied by the project owner

Polymarket and Limitless expose mirrored binary outcome liquidity because their matching systems support complementary, merge, and split paths. The adapter may use one physical canonical book with two outcome views, subject to automated live invariant checks.

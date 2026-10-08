# Limitless venue contract

This is the current contract for the Limitless native-event rail. "Venue-declared"
means the linked official documentation says it; "target contract" is behavior
provided by the project owner. The rail implements the ingestion, subscription, and
recovery contract below; the delivery and retention contract awaits a consumer transport.

## Scope

### Target contract — owner-provided

- The product is an ultra-low-latency, decode-once WebSocket feed of native events. It
  preserves each accepted source event with venue-native identity and provenance.
- It does not build an economic book, derive diffs, normalize complementary
  outcomes, or publish a shared latest-book state. `orderbookUpdate` is a native
  event, not an instruction to maintain a local book.
- Prices, quantities, depths, timestamps, versions, and all other numeric fields
  are decoded from their original JSON lexemes into exact bounded representations.
  Float conversion and rounding are forbidden. An unrepresentable required field
  rejects that event and records the reason.
- Native identity is venue-scoped. A routing handle or connection generation is not
  public identity. Lifecycle is reported as supplied and never changes feed or
  subscription policy by inference.
- The selected public feeds are market prices and market lifecycle. Every
  documented application event received from those subscriptions is decoded and
  accounted for; publication follows the explicit source policy below.
  Authenticated positions/order events are out of scope. The separate public
  `subscribe_unrealized_pnl` feed and its `unrealizedPnlProjectionChanged` REST
  refetch hint are inventoried but not selected.

## Venue-declared transport and data

- Public endpoint: `wss://ws.limitless.exchange`, Socket.IO namespace `/markets`,
  WebSocket transport only. The server owns the Engine.IO heartbeat; the client
  responds with pong and sends no application PING.
- `subscribe_market_prices` accepts AMM `marketAddresses` and CLOB `marketSlugs`;
  a request replaces the connection's prior set. Subscriptions are not retained
  after reconnect. `subscribe_market_lifecycle` and
  `unsubscribe_market_lifecycle` independently control the public lifecycle feed.
- `orderbookUpdate` has `marketSlug`, complete bid and ask arrays, and an event
  timestamp. Bids are highest-first and asks lowest-first. Prices and sizes are
  JSON numbers, so the decoder preserves their lexical number tokens.
- The current [market-data reference](https://docs.limitless.exchange/developers/websocket/market-data)
  describes `orderbookUpdate` as coalesced full snapshots. It says a valid
  (re)subscription receives an initial snapshot, including an empty snapshot;
  invalid or resolved markets are exceptions. A backend failover may reset
  `version`, and database fallback may emit `version: 0`. It describes `version`
  as a per-book publisher sequence that increases while that publisher is active,
  rather than a contiguous or cross-connection sequence.
- `marketCreated` and `marketResolved` are native lifecycle families. Their
  documented market identity is `slug`, while `orderbookUpdate` uses `marketSlug`.
- The server can emit `marketResolved` both to lifecycle subscribers and to the
  corresponding per-market room. The documentation declares neither a shared
  event ID nor equality semantics across those paths. The payload has no delivery-path
  marker; an overlapping connection cannot identify which room caused an arrival.
- The market room also carries `oraclePriceData` (observed 2026-08-31: `source` `pyth-pro`,
  a floating-point `value`, `marketAddress` null), which the venue does not document. The
  rail admits it as a bounded unknown envelope with its family name preserved; it is not
  routed and not economic data.

No venue-declared cadence, total cross-connection ordering, quorum rule, or
cross-replica delivery identity is assumed.

## Selected public family policy

The decoder validates the complete documented shape and retains all documented
fields, optional-field presence, array order, and exact numeric lexemes. Routing
uses only the listed native identity. The publication policy is explicit:

| Family or control | Native routing and validation | Publication policy |
| --- | --- | --- |
| `orderbookUpdate` | `marketSlug`; validate the complete bids/asks snapshot, `timestamp`, and `version` | Every valid arrival |
| `newPriceData` | `marketAddress`; preserve both Yes/No `updatedPrices`, `blockNumber`, and `timestamp` | Every valid arrival on the owning connection |
| `marketCreated` | `slug`; preserve the complete lifecycle object, including optional and array fields | Every valid arrival on the lifecycle connection |
| `marketResolved` | `slug`; preserve the complete resolution object | Every valid arrival, including equal repeats |
| `system` | Connection generation; preserve the full acknowledgement, including returned `markets` | Connection-local source-control record |
| `exception` | Connection generation; preserve the full error object | Source-control/fault record; no invented connection-spanning key |
| `oraclePriceData` and any other undocumented event | No native identity; the complete envelope is retained as an unknown family with its name | Every valid arrival on the owning connection, unrouted |
| Subscription commands | Connection generation and complete requested target set | Bounded outgoing control record; `system` is evidence, not a synthesized acknowledgement |
| Socket.IO connect/disconnect and Engine.IO ping/pong | Connection generation and local receive/send stamps | Bounded transport-control witnesses, not market-data events |

The current workload subscribes one connection to market data and lifecycle; the target is
that the lifecycle feed is subscribed on one owning connection only while a pin or lease on
`{limitless, lifecycle}` holds it, while `marketResolved` for a subscribed market reaches that
market's lessees through the market's own room regardless. If the venue repeats a lifecycle
event, each valid arrival is preserved. There is no local content or identity suppression and
no claim of once-only delivery.

An initial market-data snapshot follows a `system` acknowledgement for valid,
unresolved CLOB slugs. The returned `markets` set is reconciliation evidence; a
successful socket write alone is not. There is no generic unsubscribe command:
shrinking the replacement set handles market prices, while lifecycle and
unrealized-PnL subscriptions have their documented explicit unsubscribe events.

The native adapter also preserves registration `system` notifications without a `markets`
field. These are not subscription acknowledgements and establish no target coverage. The
existing registration fixture and native connection tests exercise that distinction.

An unrecognized event name or an incompatible extension on a selected feed is
never silently discarded or partially projected into a known family. Admit it as
a bounded typed native envelope only when its discriminator, size limit, and full
payload preservation are supported; otherwise emit an explicit unsupported-family
or unsupported-schema fault and mark the affected continuity gap. This rule does
not promise a schema the venue has not documented.

## Subscription ownership and recovery

### Target contract — owner-provided

- Operator pins and reference-counted client leases form the desired set. A
  controller can change demand but cannot inject market data. Subscription
  ownership serializes each connection's complete-set replacement.
- A new lease only changes demand, and it ends with its session's control
  connection and is never renewed. It does not automatically resubscribe an
  otherwise healthy connection. Reconciliation is paced and fenced by connection
  generation; a replacement subscription or reconnect invalidates late frames
  from the retired generation.
- Recovery is WebSocket-only: resubscribe, then reconnect when necessary. REST
  recovery is opt-in outside the ingestion path. A transport loss, decoder loss,
  generation violation, or local queue overflow creates an explicit gap/recovery
  state; silence from a subscribed market does not.
- The native feed may begin without a snapshot. Starting cursor/generation and
  observed source families are metadata, not a base-authority guarantee or a
  reason to delay valid event publication.

## Single-source delivery and health

Every market has exactly one owning WebSocket. A WebSocket can subscribe to many markets;
there are no replica pools and no event deduplication. Publish every valid arrival in that
connection's order, including identical repeats, older-looking timestamps, and reused native
hashes or versions with different content. Those source fields are data, not suppression keys.
No content hash, equality cache, or conflict fence runs on this path.

A replacement connection starts only after the previous generation stops. Retired generations
cannot publish, and reconnect exposes an explicit continuity boundary: WebSocket-only recovery
cannot promise to replay events missed while disconnected. A later source snapshot remains a
complete native event; any downstream book builder decides how to establish its recovery base.

Transport health is evidence from the connection and its heartbeat, independent of market
activity. A quiet market does not time out, lose its subscription, or trigger a reconnect.
Subscription state is reported separately from transport liveness and never invented.

Limitless subscription confirmation requires the returned system.markets set to match the
requested targets. Registration notifications without markets do not confirm a subscription.
The acknowledgement has a bounded deadline; after confirmation, market silence is harmless.
An incompatible acknowledgement is still delivered as a complete native system event before
the subscription fault. Missing and unexpected target counts remain payload-free diagnostics.
Engine.IO server PINGs and successful client PONG writes supply heartbeat evidence. Missing
heartbeat or a failed write causes an explicit failure and paced reconnect.

## Message atomicity

Validate the complete application message before publishing any member. A malformed or
over-capacity member rejects the entire batch. Publish all valid complete events in source
order, preserving member indices, exact source values, extensions, and full arrays. Never
split a multi-event application message, or a single event's arrays, into separately published parts.

## Delivery, retention, and observability

### Target contract — owner-provided

- Decode once, validate once, then hand the native event to bounded stages. No
  consumer, disk operation, or queue may backpressure WebSocket ingestion.
  Overflow drops only according to the configured bounded policy, records the
  loss/gap and generation, and initiates the recovery rail where applicable.
- Consumer retention has independent event and byte limits. Admission keeps no event history.
- A native-event reader attaches at the head, a retained batch boundary, or a
  valid explicit cursor. Its start position does not assert a synchronized book.
  A downstream book builder independently chooses a retained source base plus
  an unbroken tail, or waits for a later source snapshot. pm-ws provides no
  book-building/automatic-base API and does not resubscribe for a new reader.
- Metrics include connections, subscriptions, generation fences, decode/validation
  failures, accepted arrivals, queue loss, retention loss,
  recovery, and latency/queue-age distributions. Venue payloads are not written
  to disk.

## Live venue etiquette

External discovery must distinguish group containers from their condition-bearing child
markets. The active listing contains both; a child may itself report `marketType: group`.
For a qualification workload, select leaf slugs with their own condition ID and expiry,
not parent containers. This selection validation is outside the ingestion path.

The [active-market reference](https://docs.limitless.exchange/api-reference/markets/browse-active)
caps listing pages at 25 entries; larger `limit` values are rejected, not truncated. Each listed
market carries `volume`, `openInterest` and `liquidity`; the venue documents no most-active
ordering, so a selection policy ranks those fields locally. Respect the separate request budget
below when walking pages or refreshing expired selections.

These are retained project controls, not venue-published WebSocket limits. The
connection-attempt ledger and the per-endpoint command pacer are implemented in
`src/etiquette.rs`:

- The initial Limitless full-fleet envelope is 20 connections; descend on venue pushback.
  The accepted 50–100 band run used one Limitless socket; each workload declares its
  connection layout within this envelope. Budget replacement overlap within the envelope
  and pace transitions.
- Subscription-bearing commands are paced at one per 500 ms per endpoint.
- The connection-attempt ledger permits 280 attempts per rolling day.
- Public REST market selection is capped at 60 requests per hour and is never
  ingestion or ordinary recovery.
- On REST `429`, honor `Retry-After`; otherwise back off from one second and
  double per retry. Never retry `400` or `401`. Abort and notify the owner after
  more than three rejections in ten minutes, an authentication challenge,
  sensitive payload, or any sign of venue impact.

## Credential

The selected feed is an open feed: the adapter declares no venue credential fields, and no
credential is captured for Limitless. A handshake refused with an authentication challenge
or with HTTP 401 or 403 is the fault `credential_rejected`, which ends this venue's
attempts until the operator acts. The authenticated positions and order-events feeds stay
outside this contract.

## Sources and open boundaries

- [Limitless WebSocket market data](https://docs.limitless.exchange/developers/websocket/market-data)
  — current snapshot, version-reset, and subscription behavior.
- [Limitless WebSocket overview](https://docs.limitless.exchange/developers/websocket/overview)
  — endpoint, namespace, heartbeat, public/authenticated event inventory, and
  subscription replacement/unsubscribe behavior.
- [Limitless market lifecycle](https://docs.limitless.exchange/developers/websocket/market-lifecycle)
  — lifecycle subscription, payloads, and dual delivery of resolution events.
- [Limitless unrealized PnL](https://docs.limitless.exchange/developers/websocket/unrealized-pnl)
  — separate public leaderboard-invalidation subscription excluded from the
  selected feeds.
- [Limitless WebSocket integration](https://docs.limitless.exchange/developers/quickstart/websocket)
  — connection flow and `system` acknowledgement example.
- [Limitless API introduction](https://docs.limitless.exchange/api-reference/introduction)
  — REST `429` response guidance.

The venue has not declared a per-market cadence, replica ordering, total event
identity for lifecycle families, WebSocket capacity, command-rate limit, or a
cross-connection ordering meaning for timestamps/versions. Those remain
conformance boundaries, not runtime assumptions.

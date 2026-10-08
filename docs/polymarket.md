# Polymarket venue contract

This is the current contract for the Polymarket native-event rail. "Venue-declared"
means an official document says it; "target contract" is behavior supplied by the
project owner. The rail implements the ingestion, subscription, and recovery contract
below; the delivery and retention contract awaits a consumer transport.

## Scope

### Target contract — owner-provided

- The product is an ultra-low-latency, decode-once WebSocket feed of native events. It
  does not build an economic book, derive diffs, normalize Yes/No economics, or
  publish a shared latest-book state. `book` and `price_change` remain distinct
  native families.
- Decode decimal strings and every numeric field exactly into bounded
  representations; never use floating point or round. Preserve source lexemes
  and native provenance. Invalid or unrepresentable required data rejects that
  event with a reason.
- Public identity is venue-scoped and native: condition ID (`market`) plus outcome
  token ID (`asset_id`) where supplied, and native event family. Gamma IDs and
  slugs are descriptive aliases, not feed keys; internal handles are never public
  identity.
- NegRisk, outcome complementing, lifecycle interpretation, and book authority
  are outside this feed. Native lifecycle and market-data events are reproduced,
  not used to infer policy.
- The selected public feed is the CLOB market channel, with custom features
  enabled so all seven documented market-channel families are requested. The
  authenticated user channel, separate public sports channel, and unrelated RTDS
  feeds are outside this selected-feed contract.

## Venue-declared transport and data

- The public market endpoint is `wss://ws-subscriptions-clob.polymarket.com/ws/market`.
  It subscribes by `assets_ids` with `type: "market"`; later `subscribe` and
  `unsubscribe` operations modify the connection's set. The market channel needs
  no authentication. Send `PING` every 10 seconds; the server replies `PONG`.
- `book` is a full aggregated orderbook for one asset and includes `market`,
  `asset_id`, bids, asks, `timestamp`, and `hash`. `price_change` contains one
  `market`, one `timestamp`, and an ordered `price_changes` array whose entries
  include `asset_id`, price, size, side, hash, best bid, and best ask.
- `tick_size_change`, `last_trade_price`, and, when `custom_feature_enabled` is
  requested, `best_bid_ask`, `new_market`, and `market_resolved`, are separate
  native families.
- `new_market` takes no market identifier and, by the owner's statement, reports every
  market created on the venue; the venue does not document its scope, and the fixture probe
  confirms it. It is a property of the market channel rather than a separate command:
  `custom_feature_enabled` enables it together with `market_resolved` and `best_bid_ask`.
- The official [agent WebSocket guide](https://github.com/Polymarket/agent-skills/blob/main/websocket.md)
  says a `book` is emitted on subscription and that a `price_change` with size
  `"0"` removes a level. This supports source-family interpretation; it does not
  establish a cadence, ordering proof, or universal recovery guarantee.

The venue does not declare a monotonic sequence, timestamp uniqueness/ordering,
hash algorithm/ordering meaning, cross-connection identity, arrival cadence, or
WebSocket capacity/rate limit. None is inferred.

## Selected public family policy

The decoder validates the complete documented event and retains every documented
field, null/optional-field presence, nested object, array order, and exact numeric
lexeme. Routing uses only native IDs actually supplied by that family:

| Family or control | Native routing and validation | Publication policy |
| --- | --- | --- |
| `book` | `market` + `asset_id`; validate complete ordered bids/asks, `timestamp`, and `hash` | Every valid arrival |
| `price_change` | `market`; validate the whole ordered `price_changes` list and every entry, then route member assets within the atomic event | Every valid complete event |
| `last_trade_price` | `market` + `asset_id`; preserve price, size, fee, side, timestamp, and transaction hash | Every valid arrival on the owning connection |
| `tick_size_change` | `market` + `asset_id`; preserve old/new tick sizes and timestamp | Every valid arrival on the owning connection |
| `best_bid_ask` | `market` + `asset_id`; preserve best bid, best ask, spread, and timestamp | Every valid arrival on the owning connection |
| `new_market` | `market` and documented asset IDs; preserve the complete object, nested `event_message`, and arrays | Every valid arrival on the owning connection |
| `market_resolved` | `market` and documented asset IDs; preserve the complete resolution object | Every valid arrival on the owning connection |
| Subscription request/update | Connection generation, operation, and complete requested asset set | Bounded outgoing control record; no documented application acknowledgement |
| `PING`/`PONG`, connect, and disconnect | Connection generation and local receive/send stamps | Bounded transport-control witnesses, not market-data events |

The current market-channel reference documents subscription request/update
messages and `PONG`, but no application acknowledgement family. Do not infer
acceptance from a successful write. Matching native events and connection-local
protocol evidence are observations, not proof that an entire requested set was
accepted.

An unrecognized `event_type`, a non-event response, or an incompatible extension
on this selected channel is never silently discarded or partially projected into
a known family. Admit it as a bounded typed native/control envelope only when its
discriminator, size limit, and full payload preservation are supported; otherwise
emit an explicit unsupported-family or unsupported-schema fault and mark the
affected continuity gap. This does not manufacture a schema guarantee.

## Demand, generations, and recovery

### Target contract — owner-provided

- Operator pins plus reference-counted client leases form desired demand.
  Controllers never inject market data. Per-connection subscription ownership
  serializes reconciliation and is paced.
- A new lease changes desired demand only, and it ends with its session's
  control connection and is never renewed. It does not automatically resubscribe
  a healthy connection. Every transition is generation-fenced. When native protocol evidence cannot
  distinguish old from new assignment data, use a fresh connection generation.
  Frames from retired assignments cannot cross the fence.
- Recovery is WebSocket-only: resubscribe, then reconnect. REST recovery is an
  explicit opt-in and not on the ingestion path. Transport loss, parser/decoder
  loss, queue overflow, or a generation violation records a distinct gap/recovery
  reason. Quiet subscribed assets do not become stale: no cadence is declared.
- A native feed can start without a `book`. Starting cursor/generation and
  observed source families are metadata, not a base-authority guarantee or a
  reason to delay valid deltas. No synthetic book is emitted.

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

Polymarket subscription writes establish sent-but-unconfirmed requests, not acknowledgements.
The public feed documents no subscription ACK. Observed target events are separate evidence;
absence of those events cannot distinguish a quiet market from an unconfirmed subscription.
A successfully written PING must receive a corresponding PONG before the heartbeat deadline;
market-data traffic never satisfies that deadline. Both outcome tokens of a condition are
assigned to the same connection and complete price_changes arrays remain atomic. The adapter
requests `custom_feature_enabled` on every market connection, so `market_resolved` for a
subscribed market always reaches that market's lessees, and the venue-wide `new_market`
events it brings are delivered only to sessions leasing `{polymarket, lifecycle}` (ADR-0015).

Heartbeat diagnostics retain only the first timeout's bounded local timing witness per
connection, including its source generation, across recovery. PING enqueue, writer start and
completion, receipt processing, deadline, last WebSocket receive, and PONG processing remain
distinct. A PONG-processing timestamp is not a kernel arrival timestamp. The witness separates
deadline expiry from late PONG processing without changing health or replay guarantees.

Observed diagnostic pairs reused timestamp and the full ordered asset/hash list while changing
prices and sizes. Both arrivals publish; neither is an admission fault. The narrow
owner-authorized saved samples remain verifiable with bench/verify_polymarket_samples.py;
ordinary runs remain metrics-only and never persist venue payloads.

## Message atomicity

Validate the complete application message before publishing any member. A malformed or
over-capacity member rejects the entire batch. Publish all valid complete events in source
order, preserving member indices, exact source values, extensions, and full arrays. Never
split a price_changes event into separately published outcome entries.

## Delivery, retention, and observability

### Target contract — owner-provided

- Decode and validate once before bounded handoff. Slow consumers, persistence,
  and disk I/O cannot backpressure ingestion. Overflow follows the configured
  bounded policy, records loss and stream generation, and invokes recovery where
  applicable.
- Consumer retention has independent event and byte limits. Admission keeps no event history.
- A native-event reader attaches at the head, a retained batch boundary, or a
  valid explicit cursor. Its start position does not assert a synchronized book.
  A downstream book builder independently chooses a retained source base plus
  an unbroken tail, or waits for a later source snapshot. pm-ws provides no
  book-building/automatic-base API and does not resubscribe for a new reader.
- Record metrics and bounded identifier/timing witnesses, not payloads:
  connection/subscription outcomes, generation
  fences, decode/validation failures, accepted arrivals,
  queue/retention loss, recovery, and latency/queue-age distributions. Do not
  write venue payloads to disk.

## Live venue etiquette

Use the conservative endpoint pacing, rolling attempt/REST budgets, pushback and abort
rules in [Limitless live etiquette](limitless.md#live-venue-etiquette) as project controls
for this rail too, not as Polymarket-published limits; the attempt ledger and command
pacer are implemented in `src/etiquette.rs`. The accepted 50–100 band run used one
Polymarket socket; each workload declares its connection layout. Account for replacement
overlap; public REST discovery remains external to ingestion and must stay within its
budget. The [Gamma market
object](https://docs.polymarket.com/api-reference/markets/list-markets) carries
`volume24hr`, `volume1wk` and `liquidityNum`; the keyset listing declares no ordering, so
a selection policy ranks those fields locally.

## Credential

The selected market channel is an open feed: it needs no authentication, and the adapter
declares no venue credential fields, so no credential is captured for Polymarket. A handshake
refused with HTTP 401 or 403 is the fault `credential_rejected`, which ends this venue's
attempts until the operator acts. The authenticated user channel stays outside this contract.

## Sources and open boundaries

- [Polymarket market channel](https://docs.polymarket.com/api-reference/wss/market)
  — endpoint, subscription operations, heartbeat, all seven public market-channel
  event schemas, and custom-feature gating.
- [Polymarket real-time data](https://docs.polymarket.com/market-data/realtime-data)
  — current market-stream inventory and the distinct RTDS reference-price/comment
  streams excluded from this contract.
- [Polymarket user channel](https://docs.polymarket.com/api-reference/wss/user)
  — authenticated order/trade feed excluded from this contract.
- [Polymarket sports channel](https://docs.polymarket.com/api-reference/wss/sports)
  — separate public sports-results feed excluded from this contract.
- [Polymarket agent WebSocket guide](https://github.com/Polymarket/agent-skills/blob/main/websocket.md)
  — official repository guidance for subscription `book` and zero-size levels.
- [Polymarket Gamma markets](https://docs.polymarket.com/api-reference/markets/list-markets)
  — market object fields a selection policy may rank; the keyset listing declares no ordering.

Whether every subscription produces a `book`, partial subscription failure, and the ordering
of source snapshots versus deltas remain venue conformance questions. Neither local sequence
numbers nor timestamps/hashes establish an exchange-wide ordering or loss-detection guarantee.

# Prediction-market data-feed design

This is the single design document for the daemon: what it must do, the invariants it may not trade away, and the decisions that stay settled until named evidence reopens them. Venue facts live in `limitless.md`.

## Purpose

The system provides a robust, scalable, low-memory, ultra-low-latency connection between prediction-market venues and reactive strategy consumers. It owns venue connectivity, subscriptions, recovery, faithful local order-book projections, required venue events, health, metrics, and local delivery.

The primary deployment is a standalone Rust daemon usable from Rust, Python, TypeScript, and other languages. Its internal boundaries must allow the same engine to run inside a Rust process later.

Limitless is the first venue. The architecture is venue-agnostic from the beginning.

## Invariants

- Venue events retain their source meaning. Normalization may rename or re-coordinate data but may not invent business semantics.
- Venue lifecycle observations and local feed authority are independent.
- Public identity is venue-scoped and venue-native.
- Authoritative monetary state uses exact decimal or integer representations, never floating point.
- Latest-state access and reactive event delivery are coordinated but serve different consumer needs.
- Every queue and retained event window is bounded.
- Local processing order is not represented as venue order unless the venue contract supplies that guarantee.

## Confirmed choices

| Choice | Consequence |
|---|---|
| Standalone daemon first, embedded-compatible boundaries later | Local cross-language consumers use the standalone service; the core is not tied to inter-process delivery. |
| Normalized interface by default with a venue-native escape hatch | Cross-venue consumers share economic semantics without losing access to venue detail. |
| Venue-scoped native identity | Public identifiers are the venue's own market, token, ticker, address, slug, or side identifiers. No stable synthetic cross-venue market ID is created. |
| Venue-faithful event handling | Resolution and other lifecycle reports do not gate book ingestion or create local market policy. |
| One physical book for verified mirrored outcomes | Outcome views alias liquidity and cannot be counted as independent depth. |
| NegRisk grouping is outside the daemon | Every constituent market is processed independently. |
| Pinned subscriptions and client leases coexist | Infrastructure subscriptions survive client restarts while dynamic consumers share demand. |
| Latest-state and mutation APIs coexist | Consumers choose freshness-oriented or continuity-oriented reaction semantics. |
| A Rust-owned C-ABI helper is the foreign-runtime path | Python and TypeScript consumers reach shared state through the helper cdylib for validation, attachment, latest-state reads, and mutation consumption; foreign runtimes never implement atomics or ABI traversal. |
| Key-gated active-active is the default redundant topology, primary with hot standbys is its fallback (owner 2026-09-09) | A deployment that configures redundancy publishes the first arrival past a book's last venue key, whichever connection carried it, wherever the venue's recorded key declaration admits it; where it does not, and for the rest of the process once the live tripwire has fired, one primary publishes and the rest are hot standbys. Arrival order alone selects no history under either, so neither can roll authoritative state backward. |
| WebSocket-first recovery | REST is excluded from normal ingestion and is optional only for recovery. |
| Command-driven concrete subscription sets | Operators submit typed native identifiers in batches, directly or from a local list. Database and semantic-selector controllers remain external to the feed daemon. |
| MIT (owner 2026-09-08) | `LICENSE` governs use: standard MIT, copyright Chaain Labs, no Commons Clause or other sale restriction. |

## Goals

- Maintain authoritative local books for up to 50,000 binary markets (a sizing capability; live proofs stay bounded by venue reality), with defaults tuned for the common deployment below 100 markets — three named sizing profiles, one architecture: `common` (the default: full book depth, generous retention, sub-100 markets), `hot` (a handful of markets at the deepest retention), and `scale` (the 50,000-market envelope, which buys market count with declared book depth).
- Deliver same-host consumer updates from socket arrival to consumer-observable at p99 ≤ 50 µs, reported as distributions on a named host, for up to 50 concurrent consumers in parked or spinning mode.
- Support multiple markets per connection and configurable redundant connections per market.
- Allow strategies to react to the latest book or to exact accepted level mutations.
- Publish venue-reported market resolutions without using lifecycle state to alter unrelated feed behavior.
- Avoid repeated JSON parsing, broker hops, and duplicate book construction in every local strategy.
- Bound memory and latency under overload.
- Make subscription, recovery, replica health, consumer lag, and stage latency observable.

## Prioritized consumption scenarios

When a decision trades one consumer shape against another, this order names the winner. It records what the delivery surfaces are optimized for, not who may connect.

1. A broad monitoring consumer — grading, risk, recording — one process attached to most or all resident markets. It parks on the coalesced doorbell and learns what changed from the bounded changed-market feed; fan-out and wake cost are optimized for this shape first.
2. A narrow latency-critical strategy — a few markets, spinning or spin-then-park, entitled to the microsecond floor; it scans only its own interest set and needs no changed-market feed.
3. Scale capability — the 50,000-market, 50-consumer envelope as designed headroom: capacity arithmetic is checked by the resource-envelope calculation, and live proofs stay bounded by venue reality (owner decision 2026-09-01: no synthetic load proofs; live verification subscribes to the venue's full open market list).

The delivery contract is identical in every scenario; only wake mode and sizing differ.

## Non-goals

- Trading, order signing, position management, or strategy execution.
- NegRisk group construction or cross-market synthetic liquidity.
- Durable storage of every market-data event on the ingestion path.
- Exactly-once delivery to arbitrary slow consumers.
- Hiding venue-specific facts that advanced strategies legitimately require.
- Inferring resolution, finality, market relationships, subscription policy, or trading meaning that the venue did not report.
- A synthetic global identifier hierarchy spanning venues.
- Direct PostgreSQL, ClickHouse, or other application-database ownership inside the feed daemon.

## System context

```text
Prediction-market venues
          |
          v
Venue-specific connection rails
          |
          v
Validation and canonicalization
          |
          v
Single-writer market-data projections
          |
          +----------------------+------------------+
          |                      |                  |
          v                      v                  v
Shared latest state      Reactive book events   Required venue events
          |                      |                  |
          +----------------------+------------------+
                                 v
                  Rust / Python / TypeScript consumers
```

The control plane manages desired subscriptions, leases, pinned markets, connection placement, health queries, and explicit recovery commands. It remains separate from the market-data hot path.

The ultra-low-latency contract applies to consumers on the same host. Remote distribution is provided by an external consumer or bridge that reads the same daemon surfaces and chooses its own network or broker protocol. Remote transport failures and backpressure cannot enter authoritative ingestion.

## Operating model

The primary deployment is one supervised standalone daemon serving local consumers. It may own many venue connections and many book shards. Process boundaries are introduced only when measured capacity, venue isolation, or operational failure domains require them.

Normal ingestion is WebSocket-only. Control operations, metrics export, local client attachment, and optional recovery mechanisms remain outside the market-data hot path.

The daemon must calculate its requested resource envelope before accepting production traffic. It refuses configurations that cannot fit hard connection, file-descriptor, memory, shared-state, queue, or consumer limits safely.

## Venue fidelity

The daemon is a faithful projection and distribution layer, not a market-policy engine. It performs only the mechanical work needed to decode, validate, order, reproduce, and distribute venue data according to each documented contract.

Venue-reported lifecycle and local feed authority are independent dimensions. A market may be reported as resolved while its book feed remains live. If the venue sends a book snapshot or update after a resolution event, the adapter continues to validate and process it. The daemon does not infer closure, unsubscribe automatically, reject later data, or promote a venue report to stronger finality than the source states.

Derived information is explicitly identified as derived. A local book difference between snapshots, a complementary outcome view, and a top-of-book calculated from committed depth must not be represented as a source-reported venue event.

The required event scope is deliberately narrow: order-book data, market-resolution data, and protocol events necessary for subscription, continuity, recovery, and health. Other public venue events can be added behind the same adapter boundary when a concrete consumer requirement justifies them. The daemon is not a blind raw-frame relay.

## Identity

External identity is the venue plus the venue's native identifier. The daemon does not mint a stable universal market ID, rewrite venue identifiers into a common string convention, or infer relationships by parsing identifier text.

Each venue adapter chooses its canonical subscription key and records any native aliases needed for commands, payload correlation, and consumer output. Outcome identity remains scoped to its native market. Where a venue has separate token identifiers they are preserved; where it identifies a binary side within one market, the native market and side remain the identity.

Compact handles may be used internally for routing and shared-state indexing. Such handles are process-local, may change after restart, and are never accepted as durable configuration or exposed as cross-process business identifiers. The consumer-visible mapping always includes the venue-native identity.

If several supplied aliases resolve to the same native feed target or mirrored physical book, the adapter consolidates their demand without erasing which aliases consumers requested. Replica coverage applies to the physical venue subscription target, not separately to two mirrored outcome views.

A controller may attach a versioned market descriptor containing documented native aliases, outcome labels, or token references needed before the feed supplies them. The descriptor is control-plane metadata, not authoritative market data. It retains its source and version, cannot override contradictory feed evidence, and produces an explicit conflict when it disagrees with a native event. The daemon does not silently query REST to complete identity metadata.

Commands identify the kind of native key explicitly. The daemon must not guess an identifier kind from its characters, length, prefix, or numeric appearance.

A venue may expose several aliases for one market. Its adapter selects the canonical subscription key and may resolve documented aliases at the control boundary. Events always include the canonical key and may include the source alias that appeared on the wire.

## Venue rails and normalized access

Each venue owns its transport protocol, connection lifecycle, authentication, native identifiers, payload parsing, ordering evidence, subscription semantics, and recovery behavior. The core receives canonical book operations and lifecycle events rather than raw venue messages.

Strategies use a normalized economic interface by default. A venue-native interface exposes additional identifiers and protocol metadata without weakening the normalized contract.

## Binary-market book model

A market has two outcome views, stable venue-scoped references, the latest venue-reported lifecycle observations, and one or more venue-native market aliases. A separate native token identifier is preserved when it exists; otherwise a view is referenced by its native market and side rather than by a daemon-minted outcome ID.

For venues where both outcome books are exact mirrors, one physical liquidity book backs two views:

```text
Outcome 0 bid at price p  <-> Outcome 1 ask at price 1-p
Outcome 0 ask at price p  <-> Outcome 1 bid at price 1-p
```

The two views must expose the same underlying liquidity identity. Consumers must be able to determine that mirrored levels are aliases rather than independent liquidity.

The adapter is responsible for proving and continuously checking the venue's mirror invariant. A venue that does not satisfy it may use a different physical encoding without changing the strategy-facing economic concepts.

NegRisk membership does not change the book model. Every constituent is processed as an independent binary market. Group constraints, conversions, and cross-market arbitrage belong outside the daemon.

## Exact numeric semantics

Wire decimals are parsed from their lexical representation without passing through floating point. Prices, quantities, sizes, and notional values preserve the precision reported by the venue when that precision is representable by the accepted contract.

Each accepted value retains enough scale information to be interpreted exactly. The daemon does not assume that every venue uses the same tick, quantity scale, settlement unit, or number of decimal places.

Every venue field has one canonical exact economic coordinate for equality, ordering, complementation, and state digesting. Lexically different exact representations of the same value, such as `0.5`, `0.50`, and `5e-1`, compare equal and encode identically in authoritative state. Original wire lexemes remain available as provenance and fixture evidence but do not create false book changes or replica divergence. Exponent notation is accepted when it can be represented exactly within the field's bounded grammar; an adapter may prohibit it only when the pinned venue contract does.

The adapter validates:

- The venue-defined price domain.
- Tick and quantity-step compatibility when the venue defines them.
- Non-negative depth and permitted zero semantics.
- Duplicate levels and side ordering according to the source contract.
- Representable precision and numeric bounds.

Invalid or unrepresentable authoritative data is rejected explicitly. It is never silently rounded, clamped, wrapped, or converted to an approximate binary floating-point value.

Complementary prices are calculated exactly in the venue's settlement coordinate. A nominal binary payoff of one does not authorize assumptions about wire scale, fees, rounding, or valid ticks that the venue did not document.

## Provenance envelope

Every normalized state change or consumer event carries sufficient provenance to answer what the source reported and what the daemon derived. The logical envelope includes, when applicable:

- Venue and canonical native market reference.
- Native outcome, token, or side reference.
- Native event family or message type.
- Source timestamp exactly as reported.
- Source sequence, version, checksum, hash, or event identifier exactly as reported.
- Local monotonic receive and commit timestamps.
- Connection and replica role that supplied the publishing event.
- Whether the payload is source-reported, normalized, or locally derived.
- Local state revision and continuity status.

Absent source ordering evidence remains absent. A local revision orders accepted commits inside the daemon; it does not manufacture a venue sequence or prove the order of events that arrived through unrelated connections.

## Authoritative state and mutations

Every mutable book has one writer. Venue events for the same physical book are serialized through that owner.

The engine produces two coordinated consumer surfaces:

- Latest-state notification: a consumer learns that a book changed and reads the newest consistent state. Intermediate versions may be coalesced.
- Mutation delivery: a consumer receives accepted price-level changes with old quantity, new quantity, side, price, book version, provenance, and continuity state.

A source-reported delta remains distinguishable from a difference derived by comparing complete snapshots. A snapshot difference describes the state transition between two received books; it does not claim to identify individual orders, fills, or cancellations.

Slow consumers never block the writer. If mutation retention is exceeded, the consumer is told that continuity was lost and must restart from the authoritative latest state.

Market resolution is a separate retained observation and reactive event. It records what the venue reported, including the native winning outcome and source metadata. It does not mutate the book, advance book continuity, or gate later book processing. Repeated or changed venue reports remain observable rather than being silently converted into a local finality policy.

A venue adapter classifies an order-book message as a complete candidate snapshot, a source delta, or another documented book operation. Validation and ordering occur before authoritative state changes.

A complete snapshot is committed atomically. A consumer never observes half of a replacement. A source delta is applied only when the adapter can establish the required base and continuity. When continuity cannot be established, the affected projection becomes stale and follows venue-specific recovery.

For a snapshot-only source, level differences between consecutive accepted snapshots are derived mutations. They state the old and new aggregated quantities but do not imply orders, trades, cancellations, or intermediate states.

An accepted message that leaves the economic book unchanged may produce no book mutation. This does not permit the daemon to suppress a separately requested native source event solely because its content matches an earlier event.

## Book authority and venue lifecycle are independent

Book-feed authority records whether the daemon can stand behind its current local projection:

```text
Unsubscribed -> Subscribing -> Synchronizing -> Live
                                      ^           |
                                      |           v
                                  Recovering <- Stale
```

Transitions and available recovery paths remain adapter-specific, but consumers receive the resulting authority state and reason.

Venue lifecycle is the latest observation reported by the venue. It is not collapsed into the feed-authority state machine. A venue may report created, active, closed, resolved, or other states, and later continue sending book data. The daemon records both facts without making one gate the other.

## Resolution events

The required normalized resolution observation contains, when supplied by the venue:

- Canonical native market reference.
- Native winning outcome, token, side, or index.
- Venue-reported lifecycle or resolution label.
- Venue-reported resolution timestamp.
- Source ordering or event identity evidence.
- Local receive and commit metadata.
- The complete venue-native resolution fields available through the native interface.

The normalized term means only that the venue reported a resolution. It does not claim legal, oracle, challenge-period, settlement, or payout finality unless the venue contract explicitly provides that meaning.

Resolution processing does not clear, freeze, close, or replace the order book. It does not unsubscribe the market. Book events received afterward follow their normal protocol validation.

## Consumer surfaces

The standalone daemon maintains books once and exposes them to local consumers through shared state plus reactive notifications.

The consumer API remains event-driven. Blocking, asynchronous, and latency-focused polling styles are consumer policies over the same event contract.

The standalone daemon exposes coordinated local surfaces:

- Latest state for a consistent, freshness-oriented view of a book and its current metadata.
- Book-change notifications that may coalesce multiple intermediate revisions.
- Bounded book-mutation delivery for consumers that need each retained level change.
- Bounded venue-event delivery for resolution and later explicitly supported event families, with explicit continuity-loss behavior.
- Venue-native access for fields that have no stable normalized meaning.

Consumers select market references and event families. No consumer receives every event merely because it is attached to the daemon.

Python and TypeScript consumers reach these surfaces through the Rust-owned helper, which performs validation, resolution, attachment, and cursor management on their behalf; a foreign runtime never traverses the shared layout itself.

A state notification tells the consumer that a newer revision exists; it is not a copy of the whole book. A latency-sensitive consumer wakes, reads the current shared state, verifies a stable revision, and reacts. Coalescing is safe for this surface because its contract is newest state rather than every transition. The wake-up is a coalesced per-segment doorbell a consumer waits on without write access to shared state; spinning, spin-then-park, and parked waiting are consumer policies over the same contract. Parked waiting needs a doorbell the consumer's own mapping reaches. Where the platform places the doorbell in a sibling page rather than in the segment header, a consumer that attached by descriptor transfer spins or polls — that page is a writable-length object, so its descriptor is a truncation capability the daemon does not hand out — and parked waiting is reached by opening the segment by name as the same user. A bounded changed-market feed lets a consumer attached to many markets learn what changed without scanning every book; overrunning that feed is an explicit rescan signal, never silent omission.

Mutation consumers retain an independent cursor in a bounded event window. Each entry identifies the affected state revision. If the writer overtakes a cursor, the consumer receives a continuity-loss result rather than partial history and restarts from current state.

## Race-free attachment

A consumer must not miss the transition between reading initial state and beginning event consumption. Attachment therefore establishes a coherent state revision and event cursor as one logical operation:

1. Validate protocol and shared-state compatibility.
2. Resolve requested venue-native references to current process-local mappings.
3. Obtain a consistent current state revision and corresponding event position.
4. Begin event consumption strictly after that position.
5. Re-read state if the retained event window was overtaken during attachment.

The same behavior applies to Rust, Python, TypeScript, and other clients. Language libraries may expose blocking, asynchronous, callback, or polling styles, but they must not change continuity semantics.

## Slow and failed consumers

A consumer never holds a lock required by feed ingestion and never applies backpressure to a venue reader or book writer. Consumer state, cursors, leases, and notification capacity are independently bounded.

On lag, the daemon distinguishes:

- A coalesced state notification, which requires only a latest-state read.
- A lost mutation window, which explicitly breaks mutation continuity.
- A lost non-durable venue event, which is reported according to that event surface's continuity contract.

Disconnecting or crashing releases every consumer resource that connection held. Its subscription leases expire according to control-plane policy without affecting operator-pinned demand.

## Subscription control

The daemon combines two sources of desired subscriptions:

- Operator-pinned subscriptions remain active independently of strategy processes.
- Client leases are reference counted so multiple consumers share one venue subscription. A
  consumer session may hold many leases at once, one per market it has acquired. A lease
  expires when its consumer disconnects, stops renewing it, or explicitly releases it — an
  explicit release drops only that one lease, leaving the session open and every other lease
  it holds untouched. Releasing, and disconnecting or expiring, are otherwise the same
  operation from aggregate demand's point of view: whichever one takes the last lease off a
  market, and no operator pin holds it either, is what makes the market unwanted.

Only transitions in aggregate demand change venue subscriptions. A new consumer of an already-live market immediately receives the current book rather than forcing a venue resubscription.

Subscription state is reconciled as desired state rather than as an untracked sequence of commands. Venue adapters translate that desired state into the venue's native subscription operations.

The operator-facing path is command driven. A command can add, remove, or atomically replace a batch of explicitly typed venue-native market or outcome identifiers. The same batch may be supplied inline or read from a local list document; both forms produce the same desired-set operation.

The daemon does not read application databases directly. A sportsbook, trading system, or data provider may query PostgreSQL, ClickHouse, or another catalog externally and submit the resulting concrete batch. This keeps selection latency, credentials, schema changes, and database failure outside feed ingestion.

Commands are idempotent desired-state operations. Repeating the same owned set does not create duplicate demand. Results distinguish:

- Rejected input, such as an invalid identifier kind or unsupported data family.
- Accepted desired state.
- Venue reconciliation in progress.
- Live authoritative state established.
- Failed or stale state with a specific reason.

When aggregate demand reaches zero, the adapter reconciles venue unsubscription and publication transitions to unsubscribed. Existing shared state is marked unavailable for live use before its storage can be reclaimed or reused. Consumers observe the transition and cannot mistake retained bytes for a current book. Process-local handles are not reused within a daemon generation in a way that lets a stale reader address a different market.

Each reconciliation has a generation. A late acknowledgment from an older generation cannot overwrite newer desired state. Venue adapters respect whether the venue replaces a complete set, adds and removes individual targets, or uses subscription identifiers.

For a venue whose command replaces the complete subscription set, one serialized transition owns the boundary between the old and new sets. The adapter must prove which acknowledgments and market frames belong before and after that boundary. If the protocol provides no sufficient causal evidence, replacement occurs on a fresh connection generation: the old generation becomes publication-ineligible, and every retained target obtains a new authoritative base before returning live. A late old-room frame can never repopulate a removed or re-added target.

## Operator command families

The control contract supports these behavioral command families without prescribing CLI syntax or transport encoding:

| Command family | Required outcome |
|---|---|
| Add subscriptions | Add a concrete batch to the caller's pinned set or lease ownership. |
| Remove subscriptions | Remove only the caller's ownership and reconcile aggregate demand. |
| Replace subscriptions | Atomically replace the caller's complete concrete set. |
| Acquire, renew, and release leases | Maintain dynamic demand with explicit expiry semantics; a release drops one lease without ending the session or its other leases. |
| Inspect | Return desired, assigned, synchronizing, live, stale, failed, and replica-coverage state using native identities. |
| Recover | Request venue-specific recovery for explicit targets without inventing a recovery result. |
| Reconcile | Reapply current desired state to selected connections or a venue rail. |
| Reconnect | Replace a selected connection while preserving desired assignments and continuity reporting. |
| Change replica policy | Reconcile requested coverage subject to hard capacity and venue constraints. |
| Drain | Stop accepting new demand and expose shutdown progress. |

Commands return promptly with control acceptance and expose asynchronous progress to live or failed state. A slow venue acknowledgment cannot block the control plane or masquerade as a live subscription.

## Startup

Startup proceeds as an observable state transition:

1. Validate configuration, identifier kinds, replica policy, hard capacities, and local access policy.
2. Establish a new daemon generation so stale local readers cannot mistake old shared state for the current process.
3. Start control and health surfaces without declaring feed readiness.
4. Load operator-pinned concrete desired subscriptions.
5. Establish venue connections and reconcile their assigned subscription sets.
6. Synchronize each requested book according to its venue contract.
7. Declare each market live independently when its authority requirements are satisfied.
8. Declare service readiness according to the configured required coverage policy.

## Readiness and liveness

Process liveness means the daemon control loop and health surface can make progress. It does not claim that every venue or book is healthy.

Service readiness is policy driven. At minimum it reports:

- Whether all required venue rails are operational.
- Desired targets, live targets, stale targets, and failed targets.
- Requested and achieved replica coverage.
- Oldest queue age and consumer-delivery pressure.
- Whether control commands can be accepted.

Market readiness is separate and exposes book authority, recovery reason, primary connection, subscription evidence, achieved replica coverage, and last accepted source activity as telemetry. Venue-reported lifecycle observations and time since the last market update do not determine readiness.

The system applies timeouts to operations that have an expected deadline: connection establishment, transport or protocol heartbeat, authentication, subscription acknowledgment where available, recovery snapshot acquisition, control leases, and shutdown. It does not invent a deadline for ordinary market activity.

## Connection lifecycle

Transport lifecycle and subscription lifecycle are coordinated but distinct:

```text
Stopped -> Connecting -> Handshaking -> Protocol ready -> Draining -> Stopped
              ^              |                |
              |              +----> Failed <---+
              |                       |
              +------------------- Backoff
```

`Handshaking` includes the venue-required TCP, TLS, WebSocket, Socket.IO or other protocol establishment, and authentication steps. `Protocol ready` means the connection heartbeat and command protocol can operate; it does not claim that every assigned subscription is synchronized.

Each connection generation owns its read loop, heartbeat state, pending acknowledgments, and assigned subscription reconciliation. Frames and acknowledgments from an obsolete generation cannot mutate current desired state or books.

A connection is healthy when its transport and venue protocol satisfy their documented liveness behavior, reads and writes make progress when required, and no fatal protocol condition exists. Market-level message silence while heartbeat remains healthy does not degrade the connection.

## Connection failure detection

A connection failure can be established by:

- A close, read, write, TLS, WebSocket, or protocol-session error.
- Failure to satisfy the venue's documented ping, pong, heartbeat, or keepalive deadline.
- Authentication or session expiration that invalidates further messages.
- A fatal framing or protocol error whose continuity scope cannot be isolated.
- An operator command to replace the connection.

Absence of book updates from one or all subscribed markets is not sufficient while the documented heartbeat is healthy. The daemon may expose last transport activity and last market activity for diagnosis without interpreting them as failure.

TCP and WebSocket provide ordered delivery on a viable connection, but application continuity can still be lost through parser failure, bounded-queue overflow, process scheduling overload, adapter rejection, or venue behavior. The daemon uses explicit evidence from those boundaries rather than an inactivity heuristic.

The daemon cannot prove that an upstream service omitted an event when the venue supplies no sequence, checksum, replay boundary, or per-market cadence and every observable connection signal remains healthy. Redundant-state comparison may expose some omissions, but silence alone cannot. Health and provenance must communicate this evidence limit rather than replace it with a guessed timeout.

For a complete-snapshot feed, a known missed message breaks mutation continuity and leaves the prior state stale until another authoritative snapshot is accepted. The later complete snapshot can restore current-state authority without claiming that the missing intermediate mutations were recovered.

## Reconnect lifecycle

On established connection failure:

1. Close the failed connection generation and make late work from it ineligible for publication.
2. Mark each affected source assignment unavailable.
3. Promote an independently authoritative standby for each target where possible.
4. Mark only targets without another authoritative source stale.
5. Enter bounded reconnect backoff with jitter and venue-aware rate limits.
6. Establish a new transport and protocol generation, including fresh authentication when required.
7. Reconcile the current desired subscription set rather than replaying an obsolete command history.
8. Await the venue-defined acknowledgment and authoritative recovery base for each assignment.
9. Make recovered targets live independently and rebuild requested standby coverage.
10. Reset failure backoff only after a configured stable-connection interval so flapping does not create a tight loop.

Reconnect re-resolves connection prerequisites such as current credentials and endpoint resolution according to venue policy. It does not assume that an ordering sequence or book base survives a new session unless the venue contract guarantees it.

When the connection remains healthy and the venue supports per-target subscription changes, recovery first resubscribes or removes and re-adds only the affected target. Full connection replacement is the fallback, not the default response to every book problem.

## Redundancy, promotion, and divergence

The topology supports both multiple markets per connection and multiple connections per market:

```text
market -> requested replica coverage -> assigned connections
connection -> capacity and load -> assigned markets
```

The initial operating configuration may assign one market to one connection. Increasing markets per connection or replica coverage must not change the book, strategy, or control contracts.

Whether redundant streams may be applied to one book from several connections follows what the venue's own frame key is evidence of, in three tiers. No tier admits interleaving by arrival time.

A venue that documents a cross-connection ordering key permits deterministic active-active application. No supported venue documents one.

A venue whose key is only observed monotone within a connection-session may be opted into pooled active-active publishing by an operator, and only against a conformance basis recorded in that venue's contract document: cross-connection key-to-content agreement, key continuity across reconnect, and equal keys carrying equal content. A pool then publishes the first arrival whose key passes the last published key, whichever connection carried it, drops an equal key as a duplicate and a lower key as cross-connection skew, and never rolls the published key back. It publishes by key and never by arrival, so an arrival the book has already passed is dropped rather than applied.

A pool that has lost the book's authority is the one exception, and it is not a publication. The venue's answer to a resubscribe is observed to be the same frame again, carrying the key the book already holds, so a stale pool that treated that as nothing but a duplicate would wait for a higher key a quiet market need never produce. An arrival at exactly the published key whose content the pool holds exact evidence for is therefore installed as a recovery base under the ordinary contract — a new continuity epoch, no diffs across the gap — and the published key does not move, because nothing newer than it has been published.

Because that basis is observation rather than documentation, such a pool carries a live tripwire of four conditions. Three contradict the recorded observations: a connection whose own key stream inverts, a replacement connection whose first key falls below the last published key, and one key naming two different book states. The fourth is the pool losing the input it runs on — the venue ceasing to supply a key it can order, or an arrival whose content evidence it cannot keep — because a condition a pool cannot check is not a condition it may keep publishing under. An arrival that is both a content mismatch and a backward step is reported as the mismatch, which names two contents where an ordering violation names only two keys. Any of the four withdraws the pool's licence for the rest of the process and hands the book back to one publishing primary with hot standbys.

Equal-key content is compared exactly, never through a fingerprint, because a fingerprint collision would mask the one observation the licence depends on never happening. Exactness costs storage, so the comparison is scoped to a bounded window of the newest observed keys — sixty-four, plus a total-byte ceiling — and an arrival whose key the window no longer holds is judged by key alone. This is deliberate rather than a gap left open: a venue key stream need not be contiguous, so a key the window never held and a key it has dropped cannot be told apart, and answering either as a violation would degrade healthy pools on ordinary sparse streams. The window is sized against observed behaviour, where cross-socket skew was adjacent-key — orders of magnitude inside it — so a mismatch older than the window is undetectable by construction and, on the recorded evidence, not a case that arises. The hand-back replaces no published state and opens no continuity epoch: what was published was never in question, only the licence to keep choosing arrivals by key. The licence does not re-arm within the process, because the evidence granting it is recorded before the run and a run that has just contradicted it cannot re-record it. Which connection published each arrival, how many arrivals were dropped as duplicates and as skew, and whether the tripwire still stands are consumer-visible coverage.

Without a documented key or a recorded conformance basis, one primary publishes authoritative changes while hot standbys maintain independent shadow state and validate deterministic book equality.

Both of the daemon's rails carry that topology. The single-market rail runs one market over its configured replica ladder, and opts into a pool by its own explicit flag. The `pmwsd` shard rail runs a whole market set over one connection per role, configured by the daemon document's `replicas` key: 1 — the default — for one publishing connection per shard, and one redundant connection per replica beyond that, bounded by the same supported ladder depth of four. Every role subscribes its shard's whole desired set.

On that rail the two topologies are not two configurations. `replicas > 1` is a pool wherever the venue's recorded key declaration admits pooled publishing, and one publishing primary with hot standbys wherever it does not; an operator asks for redundancy, and the venue's own declaration decides what redundancy can honestly be. Because the shard's gate is per market — this venue's counter is venue-global and non-contiguous per market, so one market's keys cannot be ordered against another's — a shard holds one gate per market and one licence for the set: the tripwire's conditions are facts about the venue's key rather than about one book, so the first arrival contradicting the basis withdraws the licence for every market at once and the shard runs primary-with-standbys for the rest of the process. The hand-back is the same non-event the single-market rail's is for every book the publishing connection's own session has been seen to stand at — no state replaced, no epoch opened, because the venue's key orders that session's own stream and its next frame therefore cannot stand behind what the book holds. Every other book is told it lost its authority, and for one of two distinct reasons: a book that connection does not carry has lost the last source that could publish it, while a book it carries but stands below is a source change with no venue ordering proof, which the promotion rules below answer the same way — stale until an authoritative recovery base is installed atomically, never by applying the next thing that arrives over state it cannot be shown to be newer than. Standbys shadow throughout: while a pool is armed each socket's own arrivals keep its slot's shadow current whether the gate published them, deduplicated them or dropped them as skew, so the topology handed back to is warm at the instant it is handed back to.

Losing one socket of an armed shard pool asks no promotion question. Each market keeps its authority for as long as any established socket still carries it, and one that has just lost its last such socket is reported exactly as the single-source path reports it, recovery attempt included, because for that market this was the single source. A surviving socket is moved into the publishing slot — which carries the shard's subscription bookkeeping, its whole-set reissue, and the topology a withdrawal hands back to — and nothing about any book changes with it. Venue-reported resolutions stay the publishing slot's alone: the key declaration is for the book family, the venue's resolution frames carry no key at all, and forwarding one from every socket would multiply the venue's single report by the socket count.

Under primary-with-standbys each standby keeps a per-market shadow book that no consumer reads and that is never counted as a market's liquidity. The difference between the rails is arity, not policy: one socket carries a whole set, so losing a shard's publishing connection asks the promotion question once per market, against one shared definition of canonical equality, and the same set can answer it both ways at once. Markets whose shadow agrees carry their authority across untouched; markets that diverged or hold no comparable history are refused and recover through the shard's ordinary resubscribe-then-reconnect rail on the connection that took over. Agreement is judged against everything the shard has already been handed, including frames a standby delivered before the loss was declared but which had not yet reached its shadow, so no arrival observed before a takeover can replay over a published book after it. A publishing connection that ended holding a loss it could not report answers the question for the whole set at once: two replicas agreeing after a known local loss say only that neither holds what went missing, so every market that held authority is told it was lost rather than promoted over it. A deliberate replacement — a subscription-set change, which retires every role's connection — asks no promotion question, because no source survives it to switch to. Redundancy multiplies a deployment's venue sockets, and both the connection cap and the startup descriptor preflight budget for the resulting `2 * replicas` peak per shard; it does not multiply connection attempts, because each role's reconnect ladder is paced by the replica count against one shared attempt ledger and a role vacated by a promotion refills on that ladder like any other.

The promotion rules that follow govern every topology holding a publishing primary, which is every topology except a pool that still holds its licence. A pool has no primary to promote and asks no promotion question: losing one socket costs coverage, and the surviving sockets keep publishing the same book. Every socket of an armed pool is an authoritative source, so a known local loss on any of them — a frame received and dropped, a decode failure, an overflowed queue — is a loss of the book's continuity and not merely of one replica's shadow.

On source promotion, equal canonical state permits a source switch without a state replacement. A divergent standby cannot become live merely because the primary failed. Without venue ordering proof that selects one history, the target remains synchronizing or stale until an authoritative venue recovery base is obtained and installed atomically. The resulting continuity loss is explicit, and authoritative book versions remain monotonic across source changes.

Redundancy improves connection-failure tolerance but does not protect against correlated venue failure or identical bad data from the same upstream service.

Market count is not the sole load measure. Assignment considers observed bytes, messages, snapshot size, update work, queue age, reconnect cost, and any venue-enforced subscription limit.

Content equality across separate replicas is comparison evidence, not permission to interleave their events. Native source events are deduplicated only when the venue supplies an identity with defined equality or the active-source policy proves that they are redundant copies. Equal payload content alone must not suppress distinct events from the publishing source.

Replica divergence is feed-health evidence. It does not authorize the daemon to decide which venue-reported market outcome or business state is true.

Recovery of the former primary does not trigger automatic failback. A later authority change follows the same eligibility and canonical-state equality rules as any other promotion, preventing source-role churn from rolling state backward.

The reasoning that fixes this rule, carried verbatim from the accepted divergent-standby decision:

> A primary and standby can both have viable transports yet hold different economic state. When the venue provides no ordering key comparable across connections, local arrival time, timestamp proximity, or availability pressure cannot prove which history is authoritative. Treating a divergent standby as a replacement can roll a book backward or publish a state assembled from unproven history.

A standby is eligible for immediate promotion only when it has independent continuity evidence and its canonical economic state equals the last authoritative state.

If the states diverge and no venue ordering proof selects a history, the candidate remains non-authoritative. The target is synchronizing or stale until a venue-specific recovery procedure obtains a fresh authoritative base. That base is installed atomically with explicit mutation-continuity loss. A recovered former primary receives no automatic failback preference.

Canonical equality uses the adapter-defined exact economic coordinate. Original decimal lexemes, connection-local timing, and other provenance do not create economic inequality, while genuine depth or metadata differences remain observable as divergence evidence.

## Recovery and book authority

A book is live while the system has evidence that its current-state claim remains authoritative. Evidence is lost through events such as loss of its publishing connection without promotion, a detected protocol gap, a known dropped frame or internal update, failed candidate validation that breaks continuity, subscription loss, or unrecoverable local overload.

Time since the last market update is not authority evidence. A quiet market may remain unchanged indefinitely while its connection heartbeat, protocol session, and subscription remain healthy. Last-update age is useful activity telemetry but must not trigger generic staleness, recovery, resubscription, or reconnection. A venue-specific inactivity rule is permitted only when the pinned venue contract explicitly guarantees a per-market message cadence.

Replica divergence degrades redundancy and triggers investigation or standby recovery. It does not by itself stale a still-authoritative primary unless venue ordering, checksum, or other protocol evidence shows that the primary has lost authority.

Recovery is adapter-specific and WebSocket-first:

1. Mark the affected book stale.
2. Resubscribe the affected market when supported.
3. Await an authoritative subscription snapshot.
4. Move or reconnect the owning stream when resubscription does not recover.
5. Install recovered state atomically and mark the book live.

REST is not used for normal ingestion. A venue may expose it as an explicitly enabled recovery capability when WebSocket recovery cannot re-establish authority.

Recovery is bounded. If the configured WebSocket attempts are exhausted and no explicitly enabled fallback produces an authoritative base, the target remains stale with a terminal `recovery_base_unavailable` reason until a later automatic retry window or an operator recovery request. The daemon never promotes an unverified candidate merely to restore availability.

Staleness is a statement about whether a local feed projection can be trusted. It is not a statement about whether the venue market is active, closed, resolved, or tradable.

Book authority never uses a generic `last update age` threshold. A book can be healthy and inactive. Connection health uses the venue's actual transport and heartbeat contract, while subscription health uses acknowledgment, assignment, and recovery evidence. These signals are displayed separately so operators can distinguish a quiet market from a dead socket.

## Malformed data and continuity failures

Failures remain distinguishable:

| Failure | Scope | Required behavior |
|---|---|---|
| Malformed transport or protocol frame | Connection or message | Record the protocol reason; follow adapter continuity rules. |
| Invalid book value or precision | Affected candidate state | Reject the candidate; preserve the last valid state and establish whether authority is lost. |
| Venue sequence gap | Target or subscription stream | Mark affected projections stale and recover from a valid base. |
| Local queue overflow | Precisely affected targets where knowable | Break continuity explicitly; never hide the drop. |
| Consumer lag | One consumer and event surface | Preserve ingestion; report lost consumer continuity. |
| Replica divergence | One replicated target or group | Preserve source isolation; compare, promote, or recover without interleaving. |
| Connection loss | Assigned targets | Promote or recover only the affected assignments. |
| Venue outage | Venue rail | Isolate the venue and keep unrelated venues operating. |
| Market inactivity with healthy heartbeat and subscription | No failure | Keep the book live; expose activity age only as telemetry. |

An identical business event after a prior lifecycle report is not malformed. In particular, post-resolution book data follows normal validation.

## Overload and fairness

All internal queues and consumer event buffers are bounded. Queue age is the primary overload signal because a short queue of old market data is still unsafe.

The system does not preserve an ever-growing backlog of obsolete updates. When freshness can no longer be maintained, it marks affected state stale, isolates the failure, and recovers authoritative state.

One hot market must not cause unbounded delay across unrelated books. Connection and book placement must support isolation and rebalancing based on measured work rather than market count alone.

When work exceeds sustainable capacity:

- Consumer backpressure is isolated first.
- Obsolete coalescible notifications may collapse to newest state.
- Mutation loss is explicit.
- Affected books become stale when authoritative continuity is lost.
- Recovery is scheduled within bounded concurrency.
- Unaffected markets and venues continue where possible.

## Shutdown and restart

Graceful shutdown stops accepting new leases and control mutations, reports draining state, ends publication at an explicit local revision boundary, closes venue subscriptions and connections, and invalidates the daemon generation for attached readers.

The daemon does not claim durable continuity across process restart. Books recover from venue-defined authoritative bases. Consumers detect a new generation, discard process-local handles and cursors, resolve native identities again, and attach to new state.

Venue resolution observations are delivered reactively and retained in the running market state, but are not a durable grading ledger. A downstream system requiring audit durability owns that persistence unless a separate reliable sink is specified later.

## Configuration and hard limits

Every deployment declares hard capacities for venue connections, requested subscriptions, physical books, standby replicas, book depth or encoded state size, shard queues, retained mutations, event delivery, consumers, leases, and diagnostic capture.

The delivery capacities are declared under `[delivery]`: `profile` names one of the three sizing profiles above and supplies every key left unset, and `directory`, `segment_slots`, `event_capacity` and `dirty_capacity` — with the top-level `markets_per_shard`, `level_capacity` and `observer_capacity` — override the profile's own figures one at a time. A profile is a named point in the trade between market count, book depth, and retained history, not a floor: the geometry a set of keys implies is validated at startup, and one that no segment can have is a named refusal rather than a running daemon with a truncated book.

Configuration changes that fit existing contracts may be reconciled while running. A change that alters shared-state compatibility, numeric representation, or another contract generation requires a controlled restart and new daemon generation.

## Health and observability

The system exposes health at process, venue, connection, replica, market, book, shard, and consumer levels. Metrics cover bytes, messages, parsing, normalization, book commits, mutation publication, queue age, queue depth, stale state, recovery, replica divergence, consumer lag, CPU, and memory.

Latency metrics use local monotonic clocks for local stages. Source timestamps are retained separately and never subtracted from a local clock to claim processing latency without a verified synchronization method.

No single average is accepted as evidence. Reports include sample count, throughput, P50, P95, P99, P99.9 where sample volume supports it, maximum, queue age, CPU, and memory.

Per-message logging is disabled on the hot path. Diagnostic payload capture is bounded, sampled or explicitly targeted, redacts secrets, and remains separate from authoritative processing.

## Latency stage boundaries

Local latency is measured at explicit boundaries:

```text
first application byte or socket read-ready
        -> TLS, WebSocket, and venue-envelope assembly
        -> complete venue message available
        -> decoded and parsed
        -> validated and normalized
        -> queued for owning writer
        -> authoritative state committed
        -> consumer notification published
        -> consumer observes the revision
```

## Memory attribution

Memory is reported as both resident process memory and attributed logical capacity:

```text
base runtime
+ venue connections and decode buffers
+ canonical physical books
+ independent standby books
+ routing and native-identity mappings
+ bounded shard queues
+ shared latest-state capacity
+ retained mutation and event windows
+ consumer cursors, leases, and notification buffers
+ bounded recovery and diagnostics state
```

Memory per market is not reported without depth, numeric scale, replica factor, buffer policy, and consumer configuration. Mirrored views share physical liquidity and are not counted as two independent books.

## Standalone and embedded boundaries

Standalone mode owns process lifecycle, local access control, shared state, reactive wake-up, client cleanup, and the command surface. It is the first production deployment model.

Embedded Rust mode reuses the same venue rails, validation, book ownership, resolution handling, desired-set reconciliation, provenance, continuity, and metrics semantics. It replaces only the cross-process delivery and control boundary with in-process access. Embedded mode must not fork the core model or create a second venue implementation.

## Security and isolation

The daemon never broadens credential scope beyond the venue's connection requirement. A public-data rail uses unauthenticated access where available and least-privilege connection credentials where the venue requires authentication even for market data. Secrets remain inside the venue rail and never enter shared memory, consumer events, metrics, logs, or client-visible configuration.

Malformed venue input, malformed client commands, and incompatible shared-state readers fail closed without corrupting authoritative state.

## Execution host and platform sequencing

Feature implementation and acceptance run on the named current Apple M1 `aarch64-apple-darwin` macOS profile below. Linux and other-machine execution does not retroactively turn macOS evidence into Linux evidence.

The named host profile is:

| Field | Value |
|---|---|
| Host class | MacBook Pro |
| Processor | Apple M1, 8 cores: 4 performance and 4 efficiency |
| Memory | 16 GB |
| Operating system at profile capture | macOS 26.5.2, build 25F84 |
| Open-file soft limit at profile capture | 2,560 |
| Open-file hard limit at profile capture | Unlimited |
| Profile captured | 2026-08-25 |

## Venue extension and compatibility

Every venue rail classifies supported events as book input, required public event, transport/control event, optional native event, or unsupported event. Adding an optional native event must not require changing the book model or existing normalized contracts.

Unknown events are observable through adapter health and conformance tooling. They are not silently interpreted, and raw payloads do not enter authoritative book state.

Observed raw venue frames are retained as decoder regression tests. A captured payload is evidence of what the wire actually produced, and the decoder is re-run against every retained capture whenever the adapter changes.

Shared state and event contracts carry an explicit compatibility generation. A consumer must reject an incompatible layout or semantic version rather than reading ambiguous memory.

## Revisit these decisions when

- Standalone-first: Measurements show that the process boundary prevents a required Rust latency target that cannot be met through the standalone delivery surface.
- Normalized interface with a native escape hatch: A venue cannot represent its economically meaningful state through the normalized concepts without loss or misleading synthesis.
- Venue-native identity: A verified cross-venue standard provides stable identity semantics that every supported venue can adopt without lossy mapping or daemon-owned reconciliation.
- Shared state plus reactive delivery: Measured local process-boundary overhead prevents a required strategy latency target, or a supported platform cannot provide the necessary consistent shared-state and notification behavior.
- Key-gated active-active with primary-and-hot-standbys as its fallback: A venue supplies and documents a cross-connection ordering key that permits deterministic active-active application, and conformance tests verify it, which retires the live tripwire rather than the topology.
- Divergent standby requires recovery: A venue documents a cross-connection ordering or signed-state mechanism that deterministically selects one divergent history, and conformance evidence validates it across disconnect, reconnect, and failback scenarios.

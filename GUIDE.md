# pm-ws preview guide

Two frozen builds of pm-ws, each on its own branch and tag of this repository: v1, the Limitless
order-book daemon with same-host Python, TypeScript and Rust consumers, and v2, the native-event
rail for Limitless and Polymarket with no consumer yet. Every command here runs from a clone of
the branch it belongs to. Where a run's real output is shown, it was pasted from a live run on
the date given; everything else shows the format a run prints.

## 1. What each version is

pm-ws is a Rust daemon that reads prediction-market WebSocket feeds. It decodes every price and quantity exactly into scaled integers, never floats. Each market has exactly one current WebSocket owner, and one socket carries many markets. It places no orders and makes no trading decisions. The two builds go different distances along the path from socket to reader.

| | v1 | v2 |
| --- | --- | --- |
| State | Frozen preview of the v1 line (branch and tag `v1`) | Frozen preview of the native-event rail (branch and tag `v2`) |
| Venues | Limitless | Limitless and Polymarket |
| Output | Order books and level mutations, published through shared memory | Typed native events, taken by an in-process receiver that only counts and times them. `--tape` prints a view on stdout |
| Who can read it | Python, TypeScript and Rust consumers on the same host | No process outside the daemon. You read the tape, the snapshot and the report |
| Binaries | `pmwsd`, `pmwsctl`, `pmws-run` | `pmwsd upstream` |
| Configuration | `pmwsd.toml`, rendered by `setup.sh` from `config/pmwsd.toml.in` | One selection JSON file per run, passed with `--selection` |
| Changing markets | `pmwsctl add` and `pmwsctl remove` while the daemon runs | Edit the selection file and restart |
| Monitoring | `pmwsctl status` and Prometheus metrics | Status line on stderr every 10 s, snapshot JSON, final report JSON |
| Platform | Linux, macOS, WSL2 on Windows | Linux, macOS, WSL2 on Windows |

### 1.1 Five terms

The definitions follow the [glossary in the v2 tree](https://github.com/codebuster22/pm-ws-preview/blob/v2/CONTEXT.md).

- **Target.** The subscription coordinate pm-ws acts on: a venue, a market, and an asset where the venue requires one. A selection file lists targets, and a Polymarket condition counts as two. This is a v2 term. v1 speaks of markets.
- **Shard.** One venue's unit of publication ownership. It is the single admission owner of its markets and publishes one ordered stream. Both previews run 100 markets to a shard. v1's rendered `pmwsd.toml` pins `markets_per_shard = 100` (the daemon's own default is 128), and `pmwsd` prints one `shard <index>: ...` line per shard when it stops. v2 uses 100 selection rows per connection, per venue. The word stream is a v2 term, and the tape's `stream` field names it.
- **Connection generation.** The label of one socket assignment. A replacement connection or a subscription transition advances it. A batch from a retired generation is rejected before admission, so a late arrival cannot cross a gap. v1's `pmws-run` prints it as `generation=<n>` in its `connected` and `fenced` lines. The v2 tape carries it as `generation`.
- **Handoff.** The point where an admitted batch leaves the daemon's upstream half. Everything before it is upstream and everything after it is downstream. This is a v2 term. The tape's `handoff_ns` is the time from receipt to this point.
- **Coverage.** Observed evidence that a target's own data arrived on its owning connection. It is kept apart from the fact that a subscribe command was sent. This is a v2 term. The status line shows it as `evidence=<ready>/<required>`, and the snapshot marks each target with `covered`. The evidence is the venue's acknowledgement for Limitless and observed data for Polymarket.

## 2. Install

### 2.1 Prerequisites

- Linux or macOS. On Windows, use WSL2.
- rustup. The toolchain pinned in each tree's `rust-toolchain.toml` (Rust 1.98.0) installs itself on the first build.
- A C compiler (`cc`).
- Python 3.12 or newer.
- Node 22.18 or newer, for the v1 TypeScript consumer only.
- curl, for refreshing the v2 selections only.

`setup.sh` stops with a message if rustup, cargo, `cc` or `python3` is missing. A missing curl (v2) or a missing or old Node (v1) is only a warning.

### 2.2 Clone and build

```sh
git clone --branch v1 --depth 1 https://github.com/codebuster22/pm-ws-preview.git pm-ws-v1
git clone --branch v2 --depth 1 https://github.com/codebuster22/pm-ws-preview.git pm-ws-v2
```

Tarballs of the same trees:

- <https://github.com/codebuster22/pm-ws-preview/archive/refs/tags/v1.tar.gz>
- <https://github.com/codebuster22/pm-ws-preview/archive/refs/tags/v2.tar.gz>

In each clone, build and then run the setup script:

```sh
cargo build --release --locked
./setup.sh
```

`setup.sh` takes a few seconds. It runs no `cargo build`, no `cargo test` and no network calls; the only checks it runs are the instant self-tests listed below. Run it after the build. Run before the build, it still prepares the tree and then prints `build first: cargo build --release --locked   (then run ./setup.sh again)`.

**v1 `setup.sh`:**

- It checks the tools and creates `run/`.
- It renders `pmwsd.toml` (four seeded markets) and `pmwsd.empty.toml` (no markets) from `config/` with this clone's absolute path.
- It sets the control socket to `run/pmwsd.sock`.
- If the three binaries exist, it runs `python3 examples/reader.py --self-test` (which prints `self-test ok`) and loads the Python binding, printing `python binding ok`. When `node` is on PATH it also loads the Node binding and prints `node binding ok`; if that load fails it warns instead.
- It ends by printing the binary paths and the next commands.
- Run it again after moving the clone.

**v2 `setup.sh`:**

- It checks the tools and creates `runs/`.
- If `target/release/pmwsd` exists, it checks that running `pmwsd` with no arguments exits 2 (the usage exit) and prints `pmwsd usage exit ok`.
- It runs the self-tests of `bench/report_upstream.py`, `bench/run_upstream.py`, `bench/discover_sports.py`, `bench/tape_summary.py` and `bench/snapshot_summary.py`.
- It ends by printing the binary path and the next commands.

### 2.3 What writes where

Everything a preview does stays inside its clone.

| Path in the clone | Written by | Content |
| --- | --- | --- |
| `target/` | `cargo build` | Build output, both trees |
| `pmwsd.toml`, `pmwsd.empty.toml` | v1 `setup.sh` | Rendered configs |
| `run/` | v1 `setup.sh`, then `pmwsd` | Control socket `run/pmwsd.sock`, its `.lock` file, shared-memory segments `pmws-<instance>-<shard>.seg` |
| `runs/` | v2 `setup.sh` creates it | The files you name with `--output` and `--snapshot` |

- **v1 segment files.** `pmwsd` removes them on a clean stop. `pmws-run` writes nothing unless you give it `--shm <path>`, which creates a one-market segment file at that path, or `--record <path>` (see **Venue payloads** below).
- **v1 socket exception.** If `<clone>/run/pmwsd.sock` would be longer than 100 bytes, `setup.sh` puts the control socket and its `.lock` file at `/tmp/pmwsd-v1.sock` and says so. These are the only files a preview writes outside the clone.
- **v2 control socket.** `pmwsd upstream` opens one only if you pass `--control-socket <path>`. Keep the path inside the clone.
- **v2 snapshots.** A snapshot is written to `<path>.tmp` and then renamed over `<path>`.
- **Caches.** rustup and cargo keep their toolchain and crate caches in your home directory, as they always do.
- **Venue payloads.** No command in this guide writes a venue payload to disk. Reports and snapshots hold counters, histograms and the consumed selection, never event content. The v2 tape goes to stdout only. v1's `pmws-run` also has an optional `--record <path>` flag that appends every inbound frame to the file you name. This guide never uses it.

## 3. Run v1

### 3.1 Configuration

`./setup.sh` renders two files in the clone root from templates in `config/`. Edit a template, then run `./setup.sh` again.

| Rendered file | Template | Markets |
| --- | --- | --- |
| `pmwsd.toml` | [`config/pmwsd.toml.in`](https://github.com/codebuster22/pm-ws-preview/blob/v1/config/pmwsd.toml.in) | the four seeds |
| `pmwsd.empty.toml` | [`config/pmwsd.empty.toml.in`](https://github.com/codebuster22/pm-ws-preview/blob/v1/config/pmwsd.empty.toml.in) | none |

Both files set `metrics_listen = "127.0.0.1:9090"` and `markets_per_shard = 100`. The control socket is `run/pmwsd.sock` and the delivery directory is `run/`, both written as absolute paths. If the rendered socket path, the clone path plus `/run/pmwsd.sock`, is over 100 bytes, `setup.sh` puts the socket at `/tmp/pmwsd-v1.sock` instead and says so. Its `.lock` file goes next to it.

The seeded file has this shape:

```toml
control_socket = "<clone>/run/pmwsd.sock"
metrics_listen = "127.0.0.1:9090"
markets_per_shard = 100
markets = [
  "<seed-slug-1>",
  "<seed-slug-2>",
  "<seed-slug-3>",
  "<seed-slug-4>",
]

[delivery]
directory = "<clone>/run"
```

The seeds run until 31 December 2026. An unknown key is refused and `pmwsd` exits 2. The keys are defined in [`src/daemon.rs`](https://github.com/codebuster22/pm-ws-preview/blob/v1/src/daemon.rs).

| Key | Default | Meaning |
| --- | --- | --- |
| `control_socket` | required | Unix socket `pmwsctl` connects to. Absolute path, in an existing directory. |
| `endpoint` | the Limitless WebSocket URL | Venue endpoint. Must be a `ws://` or `wss://` URL. |
| `markets` | empty | Pinned market slugs. When empty, the daemon opens no venue connection until a market is added. |
| `markets_per_shard` | the delivery profile's capacity (128 for `common`) | Most markets one shard carries. Each shard has its own venue connection. |
| `metrics_listen` | unset | Numeric `host:port` for the metrics endpoint. When unset, nothing is bound. A host name is refused. |
| `replicas` | 1 | Venue connections per shard. Allowed 1 to 4. |
| `lease_ttl_ms` | 0 | Idle time before a control session loses its leases. 0 means no limit. Otherwise 1000 to 31536000000. |
| `max_control_sessions` | 256 | Control connections accepted at once. Further connections are refused. 0 is refused. |
| `min_command_interval_ms` | 500 | Minimum gap between subscription commands sent to the venue. 0 means no pacing. Up to 60000. |
| `daily_connection_attempt_budget` | 280 | Most venue connection attempts per day, shared by all shards. Up to 1000000. |
| `[delivery] directory` | the control socket's directory | Where each shard's shared-memory segment file is created. Absolute path, must exist. |
| `[delivery] profile` | `common` | `common`, `hot` or `scale`. Sets segment capacities for the keys left unset. |

### 3.2 Start and stop

```sh
./target/release/pmwsd --config pmwsd.toml
```

The daemon runs in the foreground. It prints one line once every shard and segment exists:

```text
pmwsd pid=<pid> shards=<n> markets=<n> socket=<path> metrics=<addr>
```

The `metrics=<addr>` part appears only when `metrics_listen` is set. After this line the daemon prints nothing until it stops. Use `pmwsctl status` or the metrics endpoint to watch it. Each shard creates one segment file in the delivery directory. A clean stop removes it.

From a run on 9 October 2026 with the seeded file. The home directory is elided from the path:

```text
pmwsd pid=46848 shards=1 markets=4 socket=/…/pm-ws-v1/run/pmwsd.sock metrics=127.0.0.1:9090
```

Stop it with Ctrl-C or `SIGTERM`. It prints one line per shard, then exits 0:

```text
shard <index>: connections=<n> subscriptions=<n> snapshots=<n> resolutions=<n> losses=<n> unrouted=<n> queue_age_max_us=<n> queue_age_p99_us=<n>
```

The same run after about two minutes, during which the connection dropped twice:

```text
shard 0: connections=3 subscriptions=3 snapshots=13 resolutions=0 losses=8 unrouted=0 queue_age_max_us=228 queue_age_p99_us=228
```

| Exit code | Meaning |
| --- | --- |
| 0 | Orderly stop after `SIGINT` or `SIGTERM`. |
| 1 | Startup failed after the configuration loaded. Examples: another daemon is listening on the socket, the socket cannot be bound or locked, a segment cannot be created, a shard refused its configuration, or the configuration needs more file descriptors than the process may open. |
| 2 | Bad command line, or the configuration failed to load or validate. |
| 3 | A shard stopped. stderr shows `pmwsd: shard <index> stopped feeding; its markets are no longer maintained` or `pmwsd: shard <index> did not survive: <error>`. |

After exit 3, the lost shard prints no stats line. The other shards do.

### 3.3 Status, add and remove

Run `pmwsctl` in a second terminal while the daemon is up:

```sh
./target/release/pmwsctl --socket run/pmwsd.sock status
./target/release/pmwsctl --socket run/pmwsd.sock add <slug>...
./target/release/pmwsctl --socket run/pmwsd.sock remove <slug>...
```

Without `--socket`, `pmwsctl` looks for `/tmp/pmwsd.sock`. Pass the path that `setup.sh` printed. `add` and `remove` need at least one slug. `status` takes none.

Every answer is pretty-printed JSON on stdout. Errors go to stderr as `pmwsctl: <message>`.

| Exit code | Meaning |
| --- | --- |
| 0 | The command succeeded. |
| 1 | The daemon could not be reached, it answered with an error, or its answer was unexpected. |
| 2 | Bad command line. |
| 3 | At least one named market was rejected. |
| 4 | A bounded queue was full. Nothing was applied. Retry. |

`add` and `remove` change the pinned market set while the daemon runs. They print one object per slug, in request order:

```json
[ { "slug": "<slug>", "status": "<status>" } ]
```

`status` is one of `accepted`, `reconciling`, `live`, `removed`, `{"rejected": "<reason>"}` or `{"stale": "<reason>"}`. A rejection reason is `invalid_identifier`, `capacity_exceeded` or `delivery_unavailable`. A stale reason is `Gap`, `Disconnect`, `SubscriptionLost`, `LocalLoss`, `OrderingUnknown`, `Overload`, `ReplicaDivergence` or `recovery_base_unavailable`.

Adding a fifth market to the running daemon, then removing it. Four seconds after the `add`, `status` showed the market `established` and `live`:

```json
[
  {
    "slug": "opensea-fdv-above-dollar3b-one-day-after-launch-1764857000239",
    "status": "accepted"
  }
]
[
  {
    "slug": "opensea-fdv-above-dollar3b-one-day-after-launch-1764857000239",
    "status": "removed"
  }
]
```

`status` returns `pid`, `rss_kib`, `answers_abandoned`, `attachments_refused`, `metrics_listen`, `shards` and `markets`. A shard's `reconciling` field and a market's `market.subscription` field show a change taking effect. `subscription` is one of `desired`, `subscribing`, `established` or `removing`. Each market row is `shard`, `pinned`, `leases` and a nested `market` object holding `slug`, `subscription`, `status`, `revision` and `continuity_epoch`.

`status` from the same run on Apple M1, 8 CPUs, trimmed to one shard and one market:

```json
{
  "pid": 46848,
  "rss_kib": 64064,
  "answers_abandoned": 0,
  "attachments_refused": 0,
  "metrics_listen": "127.0.0.1:9090",
  "shards": [
    {
      "shard": 0,
      "connected": true,
      "reconciling": false,
      "desired": 4,
      "segment": "pmws-e6baeb9ae9b6c688fdad6336bc7397c6-0.seg",
      "segment_markets": 4,
      "attachments": 0,
      "queue_age": {
        "samples": 6,
        "last_micros": 176,
        "max_micros": 216,
        "p50_micros": 216,
        "p99_micros": 216
      },
      "publish_latency": {
        "samples": 4,
        "last_micros": 198,
        "max_micros": 269,
        "p50_micros": 256,
        "p99_micros": 269,
        "p999_micros": 269
      }
    }
  ],
  "markets": [
    {
      "shard": 0,
      "market": {
        "slug": "will-trump-acquire-greenland-before-2027-1768930762585",
        "subscription": "established",
        "status": "live",
        "revision": 1,
        "continuity_epoch": 0
      },
      "pinned": true,
      "leases": 0
    }
  ]
}
```

To start with no markets, use the empty file and add them by hand:

```sh
./target/release/pmwsd --config pmwsd.empty.toml
./target/release/pmwsctl --socket run/pmwsd.sock add <slug>
```

The startup line then shows `markets=0`.

### 3.4 Metrics

Both rendered files set `metrics_listen`, so the endpoint is on:

```sh
curl 127.0.0.1:9090/metrics
```

The address is the value of `metrics_listen`. If port 9090 is taken, change it in the template and run `./setup.sh` again. The endpoint answers `GET /metrics` only. Another target or method gets a 404, an unreadable or oversized request head gets a 400, and a scrape that collides with one already in flight gets a 503. It has no authentication, so keep it on a loopback address.

Per-shard lines look like this:

```text
<name>{shard="<n>"} <value>
```

These names are worth watching:

| Metric | Meaning |
| --- | --- |
| `pmws_shard_connected` | 1 when the shard's publishing connection is established with its subscription on the wire. |
| `pmws_shard_frames_seen` | Venue frames received. |
| `pmws_shard_continuity_losses` | Times a book lost continuity. |
| `pmws_shard_queue_age_p99_micros` | 99th-percentile sampled ingest queue age, in microseconds, as a bucket upper bound. |
| `pmws_markets` | Markets the daemon holds for a book, across every shard. Daemon-wide: this line carries no `shard` label (the per-shard count is `pmws_shard_markets`). |

Six of the lines from the same run on Apple M1, 8 CPUs, about two minutes in:

```text
pmws_shard_connected{shard="0"} 1
pmws_shard_frames_seen{shard="0"} 15
pmws_shard_continuity_losses{shard="0"} 4
pmws_shard_queue_age_p99_micros{shard="0"} 216
pmws_shard_markets{shard="0"} 5
pmws_markets 5
```

## 4. Study the book

### 4.1 pmws-run --print-book

`pmws-run` opens one WebSocket to Limitless for one market, rebuilds the book and prints what it sees. It needs no daemon. It writes no file unless given `--shm` or `--record`.

```sh
./target/release/pmws-run --market <slug> --seconds 60 --print-book
```

`<slug>` is a Limitless market slug, for example one of the four seeds in `config/pmwsd.toml.in`.

`--seconds` defaults to 60 and accepts up to 86400. Without `--print-book` you still get the connection and event lines and the summary. `--print-book` adds the `book`, `mutation`, `resolution` and `summary book` lines. `--log-versions` adds one `dedup` line per accepted book update.

| Exit code | Meaning |
|---|---|
| 0 | The run reached `--seconds` and printed the summary. |
| 1 | The run failed. The message is on stderr. |
| 2 | Bad command line. The message is on stderr. |

The lines below are grouped by kind, not by the order a run prints them. Notice lines and book lines come from two sources. The `orderbookUpdate` line and the `book` and `mutation` lines it causes can appear in either order.

**connected.** Printed after each successful connection.

```
connected generation=<generation> replica=<replica> sid=<sid> ping_interval_ms=<ms> ping_timeout_ms=<ms> max_payload_bytes=<bytes> subscription_generation=<n>
```

`generation` numbers the connection. A reconnect gets a new one. The `sid`, ping and payload fields are the handshake values the venue returned.

**orderbookUpdate.** One line for each venue update frame.

```
orderbookUpdate slug=<slug> bids=<count> asks=<count> best_bid=<price>@<qty> best_ask=<price>@<qty> ts=<timestamp>
```

`bids` and `asks` count the levels in the frame. `best_bid` and `best_ask` are the first level the venue listed on each side, or `-` if there is none. Prices and quantities are exact decimal text.

Two more event lines use the same stream:

```
marketResolved slug=<slug> type=<type> winning_outcome=<outcome> winning_index=<index> resolution_date=<date>
unknown event name=<name>
```

**book.** Printed once at start, before `connected`, and again after each new revision.

```
book revision=<R> authority=<authority> continuity_epoch=<E> canonical_bids=[<levels>] canonical_asks=[<levels>] derived_complement_bids=[<levels>] derived_complement_asks=[<levels>]
book revision=<R> authority=<authority> continuity_epoch=<E> canonical_bids=[<levels>] canonical_asks=[<levels>] derived_complement=unavailable
```

`<levels>` is `<price>@<qty>,<price>@<qty>,...` with at most three levels, best first. Bids start at the highest price. Asks start at the lowest.

- `revision` is the book's revision number. It rises as the run publishes new book states.
- `authority` is the book's authority state: `Unsubscribed`, `Subscribing`, `Synchronizing`, `Live`, `Recovering` or `Stale(<reason>)`.
- `continuity_epoch` names the stretch of unbroken history the book belongs to.
- The canonical lists are the book as the venue reports it.
- The derived complement lists are the complement of that book, derived locally. When it cannot be derived the line says `derived_complement=unavailable`.

**mutation.** One line for each level change in the book.

```
mutation revision=<R> epoch=<E> position=<P> side=<Bid|Ask> price=<price> qty=<old>-><new>
```

`epoch` and `position` are the cursor of the change in the book's event stream. `qty` shows the level's quantity before and after. `none` means the level is absent on that side of the change.

**resolution.** One line when the venue reports the market resolved.

```
resolution revision=<R> epoch=<E> position=<P> winner=<outcome> index=<index> type=<type> date=<date>
```

`revision` is the book revision the resolution was ordered after. A resolution does not change the book.

**Lines printed only when something happens.** A run given `--pool` also prints `source pooled` and `pool_degraded` lines here, and a `summary pool` line in the summary; this guide does not cover `--pool`.

```
continuity_loss continuity=<continuity> authority=<authority>
book continuity_lost reason=<reason> missed=<count>
fenced generation=<generation>
reconnecting generation=<generation> replica=<replica> delay_ms=<ms>
resubscribing generation=<generation> replica=<replica>
recovery_base_unavailable attempts=<n>
source publishing primary=<connection> standbys=[<list>] standby_capacity=<n>
source recovering replica=<connection>
source none
```

`<connection>` is `<id>#<generation>`. A `fenced` line means events from a retired connection generation are rejected before they reach the book. After `book continuity_lost` the run prints a fresh `book` line for the state it resumed from.

**dedup.** Printed only with `--log-versions`.

```
dedup market=<market> position=<position> generation=<generation> replica=<replica> semantics=<label> key=<key> digest=<digest>
```

`key` is the venue's version value for the update, `none` if the frame had none, or `invalid:<reason>`.

**summary.** Printed at the end, in this order.

```
summary frames_seen=<n> connection_attempts=<n> fenced_generations=<n> fenced_events=<n>
summary events orderbookUpdate=<n> marketResolved=<n> unknown=<n>
summary diagnostics_dropped=<n> overload_drops=<n>
summary decode_failures=none
summary decode_failure reason=<reason> count=<n>
summary book snapshots_applied=<n> mutations_derived=<n> continuity_losses=<n> recovery_base_unavailable=<n> observer_continuity_losses=<n>
```

- The first three lines count frames, connection attempts, fences, events by kind and drops.
- The run prints `decode_failures=none`, or one `decode_failure` line for each reason.
- `summary book` appears only with `--print-book`. It counts snapshots applied and mutations derived from them. It also counts continuity losses, once in the supervisor and once in this run's own book observer.

The source for every format above is [src/main.rs](https://github.com/codebuster22/pm-ws-preview/blob/v1/src/main.rs).

A 60-second run on 9 October 2026 against the seeded Greenland market. About half a minute in, the connection dropped; the run reconnected, resubscribed and opened continuity epoch 1. Nothing is cut:

```text
book revision=0 authority=Synchronizing continuity_epoch=0 canonical_bids=[] canonical_asks=[] derived_complement_bids=[] derived_complement_asks=[]
connected generation=1 replica=PublishingPrimary sid=BHIdKiZA8BsjNoE2AAHD ping_interval_ms=25000 ping_timeout_ms=60000 max_payload_bytes=1000000 subscription_generation=1
unknown event name=system
unknown event name=system
source publishing primary=limitless-markets#1 standbys=[] standby_capacity=0
orderbookUpdate slug=will-trump-acquire-greenland-before-2027-1768930762585 bids=2 asks=5 best_bid=0.003@863898323 best_ask=0.899@12362000 ts=2026-10-08T21:32:01.199Z
book revision=1 authority=Live continuity_epoch=0 canonical_bids=[0.003@863898323,0.001@5000000000] canonical_asks=[0.899@12362000,0.9@91872889,0.95@100000000] derived_complement_bids=[0.101@12362000,0.1@91872889,0.05@100000000] derived_complement_asks=[0.997@863898323,0.999@5000000000]
book revision=2 authority=Stale(Disconnect) continuity_epoch=0 canonical_bids=[0.003@863898323,0.001@5000000000] canonical_asks=[0.899@12362000,0.9@91872889,0.95@100000000] derived_complement_bids=[0.101@12362000,0.1@91872889,0.05@100000000] derived_complement_asks=[0.997@863898323,0.999@5000000000]
continuity_loss continuity=Reconnect authority=Disconnect
source none
reconnecting generation=2 replica=PublishingPrimary delay_ms=1235
source recovering replica=limitless-markets#2
connected generation=2 replica=PublishingPrimary sid=LXMkcoAqfCgr4Iy6AAY9 ping_interval_ms=25000 ping_timeout_ms=60000 max_payload_bytes=1000000 subscription_generation=1
unknown event name=system
resubscribing generation=2 replica=PublishingPrimary
book revision=3 authority=Live continuity_epoch=1 canonical_bids=[0.003@863898323,0.001@5000000000] canonical_asks=[0.899@12362000,0.9@91872889,0.95@100000000] derived_complement_bids=[0.101@12362000,0.1@91872889,0.05@100000000] derived_complement_asks=[0.997@863898323,0.999@5000000000]
unknown event name=system
source publishing primary=limitless-markets#2 standbys=[] standby_capacity=0
orderbookUpdate slug=will-trump-acquire-greenland-before-2027-1768930762585 bids=2 asks=5 best_bid=0.003@863898323 best_ask=0.899@12362000 ts=2026-10-08T21:32:28.205Z
unknown event name=system
book revision=4 authority=Live continuity_epoch=1 canonical_bids=[0.003@863898323,0.001@5000000000] canonical_asks=[0.899@12362000,0.9@91872889,0.95@100000000] derived_complement_bids=[0.101@12362000,0.1@91872889,0.05@100000000] derived_complement_asks=[0.997@863898323,0.999@5000000000]
orderbookUpdate slug=will-trump-acquire-greenland-before-2027-1768930762585 bids=2 asks=5 best_bid=0.003@863898323 best_ask=0.899@12362000 ts=2026-10-08T21:32:28.723Z
summary frames_seen=10 connection_attempts=2 fenced_generations=0 fenced_events=0
summary events orderbookUpdate=3 marketResolved=0 unknown=5
summary diagnostics_dropped=0 overload_drops=0
summary decode_failures=none
summary book snapshots_applied=3 mutations_derived=0 continuity_losses=1 recovery_base_unavailable=0 observer_continuity_losses=0
```

### 4.2 The consumers bbo.py and bbo.ts

Both consumers read a segment file that a running `pmwsd` publishes. Start the daemon first, in another terminal:

```sh
./target/release/pmwsd --config pmwsd.toml
```

The daemon prints `pmwsd pid=<pid> shards=<n> markets=<n> socket=<path> metrics=<addr>` once its segments exist. The ` metrics=<addr>` suffix is there only because `pmwsd.toml` sets `metrics_listen`. It then prints nothing until it stops. The consumers must run on the same host, as the same user as the daemon, from a clone that has run `cargo build --release --locked`. `bbo.ts` additionally needs Node 22.18 or newer. They attach read-only.

Each daemon start creates new segment files. List them:

```sh
ls run/
```

A segment is named `pmws-<instance>-<shard>.seg`. `<instance>` is 32 hex digits and `<shard>` is the shard index. The market must be in the daemon's set, and it must be in the segment you name.

```sh
python3 examples/bbo.py --segment run/pmws-<instance>-0.seg --market <slug> --events
node examples/bbo.ts --segment run/pmws-<instance>-0.seg --market <slug>
```

`<slug>` is one of the markets in `pmwsd.toml`. Node runs the `.ts` file directly with no build step.

| Flag | Default | Meaning |
|---|---|---|
| `--segment <path>` | required | The segment file. |
| `--market <slug>` | required | The market's venue-native key. |
| `--venue <name>` | `limitless` | Venue of the market. |
| `--kind <kind>` | `slug` | Kind of the market key. |
| `--seconds <n>` | `10` | Run time. Must be finite, above 0 and at most 86400. |
| `--events` | off | Also print `mutation` and `resolution` lines and recovery lines. |

Both files accept the same flags, and [bbo.py](https://github.com/codebuster22/pm-ws-preview/blob/v1/examples/bbo.py) and [bbo.ts](https://github.com/codebuster22/pm-ws-preview/blob/v1/examples/bbo.ts) print the same line shapes.

**bbo.** The first line prints after attach. Another prints each time the book revision changes.

```
bbo revision=<R> authority=<authority> best_bid=<price>@<qty> best_ask=<price>@<qty>
```

A missing side prints `-`. `authority` is one of `Unsubscribed`, `Subscribing`, `Synchronizing`, `Live`, `Recovering` or `Stale(<reason>)`. The reason is one of `Gap`, `Disconnect`, `SubscriptionLost`, `LocalLoss`, `OrderingUnknown`, `Overload`, `ReplicaDivergence` or `RecoveryBaseUnavailable`.

**mutation and resolution.** Printed only with `--events`.

```
mutation revision=<R> cursor=<epoch>:<position> origin=<origin> side=<side> price=<price> qty=<old>-><new>
resolution revision=<R> cursor=<epoch>:<position> origin=<origin> outcome=<outcome> index=<index> type=<type> date=<date> path=<path>
```

- `none` in `qty` means the level is absent on that side of the change.
- In `bbo.py`, `side` is `Bid` or `Ask`. In `bbo.ts` it is lowercase.
- In `bbo.py`, `origin` is `sourceReported`, `normalizedFromSource` or `locallyDerived(snapshotDiff)`. In `bbo.ts` it is `sourceReported`, `normalizedFromSource`, `snapshotDiff` or `unknown`.
- In `bbo.py`, `path` is `marketFeed`, `lifecycleFeed` or `resolutionFeed`. In `bbo.ts` it is one of those or `unknown`.

**Continuity.** Printed only with `--events`. If the mutation stream loses continuity, the consumer prints both lines and goes on.

```
continuity_lost reason=<reason>
reattach revision=<R>
```

`reason` is one of `Overrun`, `Gap`, `LocalLoss`, `Reconnect`, `RecoveryBase` or `SyncDivergence`. The second line shows the revision the consumer resumed from.

**Errors.** If the market never appears in the segment before `--seconds` ends, the consumer prints this to stderr and exits 1:

```
error: market not installed in the segment
```

A missing or invalid flag exits 2 in `bbo.py` and 1 in `bbo.ts`, with a usage line. A normal run exits 0 when `--seconds` ends.

Both consumers against the daemon above, on the Greenland seed. No mutation arrived in their windows, so each printed only its opening line:

```text
$ python3 examples/bbo.py --segment run/pmws-e6baeb9ae9b6c688fdad6336bc7397c6-0.seg --market will-trump-acquire-greenland-before-2027-1768930762585 --seconds 20 --events
bbo revision=1 authority=Live best_bid=0.003@863898323 best_ask=0.899@12362000
$ node examples/bbo.ts --segment run/pmws-e6baeb9ae9b6c688fdad6336bc7397c6-0.seg --market will-trump-acquire-greenland-before-2027-1768930762585 --seconds 10
bbo revision=1 authority=Live best_bid=0.003@863898323 best_ask=0.899@12362000
```

### 4.3 Your own consumer

Save the two files below in the root of your clone. Each opens the segment, resolves one market by slug, attaches, prints the best bid and ask once, then prints every event as it arrives. Reference consumers: [examples/bbo.py](https://github.com/codebuster22/pm-ws-preview/blob/v1/examples/bbo.py) and [examples/bbo.ts](https://github.com/codebuster22/pm-ws-preview/blob/v1/examples/bbo.ts).

`follow.py`:

```python
import sys
import pmws

path, slug = sys.argv[1], sys.argv[2]


def level_text(level):
    return "-" if level is None else f"{level.price}@{level.quantity}"


with pmws.Segment(path) as segment:
    state, stream = segment.resolve("limitless", "slug", slug).attach()
    print(f"revision={state.revision} authority={state.authority} "
          f"best_bid={level_text(state.best('Bid'))} best_ask={level_text(state.best('Ask'))}", flush=True)
    generation = segment.publication_generation()
    while True:
        try:
            event = stream.next_event()
        except pmws.PmwsContinuityLost as loss:
            state = stream.reattach()
            print(f"continuity_lost reason={loss.reason} reattach revision={state.revision}", flush=True)
            continue
        if event is None:
            changed = segment.wait(generation, spin_micros=200, timeout_ms=1000)
            generation = generation if changed is None else changed
        elif isinstance(event, pmws.Resolution):
            print(f"resolution revision={event.revision} outcome={event.winning_outcome}", flush=True)
        else:
            print(f"mutation revision={event.revision} side={event.side} price={event.price} "
                  f"qty={event.old_quantity}->{event.new_quantity}", flush=True)
```

Calls used, each as `bindings/python/pmws.py` declares it:

- `pmws.Segment(path)` opens a segment file. It works as a context manager whose exit calls `close()`.
- `segment.resolve(venue, kind, key)` returns a `Market`.
- `market.attach()` returns `(state, stream)`, a `BookState` and an `EventStream`.
- `state.best(side)` takes `"Bid"` or `"Ask"` and returns a `Level` or `None`.
- `segment.publication_generation()` returns the current generation as an int.
- `stream.next_event()` returns a `Mutation`, a `Resolution` or `None`. It raises `pmws.PmwsContinuityLost`, which has `.reason`.
- `stream.reattach()` returns the resumed `BookState`.
- `segment.wait(last_generation, spin_micros=0, timeout_ms=None)` returns the new generation, or `None` on timeout.

Run it from the clone root. Take a concrete `<market-slug>` from `config/pmwsd.toml`:

```sh
PYTHONPATH=bindings/python python3 follow.py run/pmws-<instance>-0.seg <market-slug>
```

`follow.ts`:

```ts
import { Segment, PmwsContinuityLost, type Event, type Level } from "./bindings/node/pmws.ts";

const [path, slug] = process.argv.slice(2) as [string, string];

const levelText = (level: Level | null) =>
  level === null ? "-" : `${level.price.text}@${level.quantity.text}`;

const segment = new Segment(path);
try {
  const { state, stream } = segment.resolve("limitless", "slug", slug).attach();
  console.log(
    `revision=${state.revision} authority=${state.authority} ` +
      `best_bid=${levelText(state.best("bid"))} best_ask=${levelText(state.best("ask"))}`,
  );
  let generation = segment.publicationGeneration();
  for (;;) {
    let event: Event | null;
    try {
      event = stream.nextEvent();
    } catch (error) {
      if (!(error instanceof PmwsContinuityLost)) throw error;
      const resumed = stream.reattach();
      console.log(`continuity_lost reason=${error.reason} reattach revision=${resumed.revision}`);
      continue;
    }
    if (event === null) {
      generation = segment.wait(generation, { spinMicros: 200, timeoutMs: 1000 }) ?? generation;
    } else if (event.kind === "resolution") {
      console.log(`resolution revision=${event.revision} outcome=${event.winningOutcome}`);
    } else {
      const old = event.oldQuantity?.text ?? "None";
      const next = event.newQuantity?.text ?? "None";
      console.log(`mutation revision=${event.revision} side=${event.side} price=${event.price.text} qty=${old}->${next}`);
    }
  }
} finally {
  segment.close();
}
```

Calls used, each as `bindings/node/pmws.ts` exports it:

- `new Segment(path)` opens a segment file. `segment.close()` frees it.
- `segment.resolve(venue, kind, key)` returns a `Market`.
- `market.attach()` returns `{ state, stream }`.
- `state.best("bid" | "ask")` returns `Level | null`. A price or quantity is a `Decimal`, and `.text` is its printable form.
- `segment.publicationGeneration()` returns a `bigint`.
- `stream.nextEvent()` returns `Event | null`. It throws `PmwsContinuityLost`, which has `.reason`. `event.kind` is `"mutation"` or `"resolution"`.
- `stream.reattach()` returns the resumed `State`.
- `segment.wait(lastGeneration, { spinMicros, timeoutMs })` returns a `bigint`, or `null` on timeout.

Run it from the clone root with Node 22.18 or newer. Node runs the `.ts` file directly. Take the slug from `config/pmwsd.toml` as above:

```sh
node follow.ts run/pmws-<instance>-0.seg <market-slug>
```

Both scripts print these line shapes until you stop them with Ctrl-C:

```
revision=<revision> authority=<authority> best_bid=<price>@<qty>|- best_ask=<price>@<qty>|-
mutation revision=<revision> side=<side> price=<price> qty=<old>-><new>
resolution revision=<revision> outcome=<winning_outcome>
continuity_lost reason=<reason> reattach revision=<revision>
```

Against the daemon above. `follow.py` ran for 20 s after a fresh start; `follow.ts` ran later in the longer run, after two reconnects had moved the revision to 3:

```text
$ PYTHONPATH=bindings/python python3 follow.py run/pmws-e6baeb9ae9b6c688fdad6336bc7397c6-0.seg will-trump-acquire-greenland-before-2027-1768930762585
revision=1 authority=Live best_bid=0.003@863898323 best_ask=0.899@12362000
$ node follow.ts run/pmws-e6baeb9ae9b6c688fdad6336bc7397c6-0.seg will-trump-acquire-greenland-before-2027-1768930762585
revision=3 authority=Live best_bid=0.003@863898323 best_ask=0.899@12362000
```

What the scripts leave out:

- Neither binding can pin a market, and neither has a subscribe call; a binding creates venue demand only by leasing (`Segment.connect` and `Segment.lease`). `resolve` raises an error with code `PMWS_STATUS_MARKET_NOT_FOUND` (Python) or `PMWS_MARKET_NOT_FOUND` (TypeScript) when the segment does not hold the market.
- To add a market, run `pmwsctl --socket run/pmwsd.sock add <slug>...`. Or use `Segment.connect(control_socket, market)`, which asks the daemon for the market and leases it (see 4.5).
- `bbo.py` retries `resolve` until the market appears. `follow.py` does not.
- Neither script retries transient errors on `attach()`. See 4.5.

### 4.4 Field reference

Python names come first and TypeScript names follow after a slash where they differ. Python sides are `Bid` and `Ask`. TypeScript sides are `bid` and `ask`. TypeScript revisions, cursors, generations, timestamps and `syncDivergences` are `bigint`.

**Book state** (`BookState` / `State`), returned by `attach()`, `read_state()` / `readState()` and `reattach()`:

| Field | Type | Meaning |
|---|---|---|
| `revision` | int | Book revision of this state. A new value means a new published state. |
| `authority` | str | `Unsubscribed`, `Subscribing`, `Synchronizing`, `Live`, `Recovering` or `Stale(<reason>)`. Stale reasons: `Gap`, `Disconnect`, `SubscriptionLost`, `LocalLoss`, `OrderingUnknown`, `Overload`, `ReplicaDivergence`, `RecoveryBaseUnavailable`. |
| `cursor_epoch`, `cursor_position` / `cursorEpoch`, `cursorPosition` | int | Event-stream cursor that goes with this state. |
| `continuity_intact` / `continuityIntact` | bool | Whether the state sits on intact continuity. |
| `continuity_reason` / `continuityReason` | str or None | `None` when intact. Otherwise `Overrun`, `Gap`, `LocalLoss`, `Reconnect`, `RecoveryBase` or `SyncDivergence`. |
| `sync_divergences` / `syncDivergences` | int | Count of sync divergences recorded for the book. |
| `levels` | list of `Level` | Every level, ascending by `(side, price)`. Each has `side`, `price`, `quantity`. |
| `best(side)` | `Level` or None | Best bid is the last matching bid level. Best ask is the first matching ask level. Levels with exactly zero quantity are skipped. |
| `commit_time`, `arrival_time` / `commitTime`, `arrivalTime` | int or None | Timestamps. `None` (`null` in TypeScript) when absent. |
| `origin`, `representation`, `native_family` / `nativeFamily` | str or None | Provenance of the publication. `None` until something is published. In TypeScript `representation` is a number. |

The binding has no separate bid and ask lists. Filter `levels` on `side`.

**Mutation** (`Mutation` / `MutationEvent`), returned by `next_event()` / `nextEvent()`:

| Field | Type | Meaning |
|---|---|---|
| `revision` | int | Book revision of the mutation. |
| `cursor_epoch`, `cursor_position` / `cursor.epoch`, `cursor.position` | int | Place of the event in the stream. `bbo.py` prints it as `<epoch>:<position>`. |
| `origin` | str | Python: `sourceReported`, `normalizedFromSource` or `locallyDerived(snapshotDiff)`. TypeScript: `sourceReported`, `normalizedFromSource`, `snapshotDiff` or `unknown`. |
| `side` | str | `Bid` or `Ask` (TypeScript: `bid`, `ask` or `unknown`). |
| `price` | `ExactDecimal` | Price of the level. |
| `old_quantity`, `new_quantity` / `oldQuantity`, `newQuantity` | `ExactDecimal` or None | Quantity before and after. `None` (`undefined` in TypeScript) when that half is absent. |
| `representation`, `native_family` / `nativeFamily` | str | `venueNative` or `normalized`, and the native family name. In TypeScript `representation` is a number. |
| `daemon_generation`, `subscription_generation` / `daemonGeneration`, `subscriptionGeneration` | int | Daemon and subscription generations stamped on the event. |
| `commit_time`, `arrival_time` / `commitTime`, `arrivalTime` | int or None | Timestamps. |
| `kind` | str | TypeScript only. `"mutation"`. |

**Resolution** (`Resolution` / `ResolutionEvent`) shares `cursor_*`, `origin`, `representation`, the generations and the timestamps with a mutation:

| Field | Type | Meaning |
|---|---|---|
| `revision` | int | The book revision this resolution is ordered after. It commits none of its own. |
| `winning_index` / `winningIndex` | int | Index of the winning outcome. |
| `winning_outcome` / `winningOutcome` | str | The venue's own text, verbatim. |
| `market_type` / `marketType` | str | The venue's own text, verbatim. |
| `resolution_date` / `resolutionDate` | str | The venue's own text, verbatim. Never parsed into a number. |
| `delivery_path` / `deliveryPath` | str | `marketFeed`, `lifecycleFeed` or `resolutionFeed` (TypeScript also `unknown`). |
| `kind` | str | TypeScript only. `"resolution"`. |

`arrival_time` is documented in the TypeScript binding as wall-clock nanoseconds since the Unix epoch. The bindings do not state a unit for `commit_time`.

**ExactDecimal.** Every price and quantity is an exact decimal. Nothing is a float.

| Part | Python `ExactDecimal` | TypeScript `Decimal` |
|---|---|---|
| `coefficient` | signed int, 128-bit range | `bigint` |
| `scale` | int, the count of digits after the decimal point | `number` |
| `text` | str, rendered by the library | `string`, rendered by the library |

The value is `coefficient` divided by 10 to the power `scale`. To print one, use `str(x)` or `x.text` in Python and `x.text` in TypeScript. The binding never builds `text` locally. Python `==` compares coefficient and scale, so the same number at two scales is not equal. Do not convert to float.

### 4.5 Consumer rules

- **Same operating-system user.** Run the consumer as the user that runs `pmwsd`. `Segment.connect(control_socket, market)` is refused when the caller is another user. Python raises `PmwsError` with code `PMWS_STATUS_ATTACH_REFUSED`. TypeScript throws an `Error` with `code` `PMWS_ATTACH_REFUSED`. The peer check sits on top of the segment file's own permissions, which apply when you open by path too.
- **Attachment is read-only.** A `Segment` is a read-only view of the daemon's segment. `connect` receives a read-only descriptor. Neither binding has a call that writes to the segment.
- **`connect` takes a lease.** The lease lasts while the segment is open. A market that nothing else holds or pins is subscribed at the venue to serve it. `close()`, or the process exiting, releases the lease. A segment from `connect` may not be able to park in `wait()`. It can raise `PMWS_STATUS_DOORBELL_UNAVAILABLE` (TypeScript: `PMWS_DOORBELL_UNAVAILABLE`). Poll `publication_generation()` instead, or spin with `spin_micros`. A segment opened by path can park.
- **`PmwsContinuityLost`.** `next_event()` raises it when the stream can no longer prove it delivered every mutation in order. `reason` is one of `Overrun`, `Gap`, `LocalLoss`, `Reconnect`, `RecoveryBase` or `SyncDivergence`. The error repeats on every later call until you call `reattach()`. Treat the `BookState` that `reattach()` returns as the book. Discard any book you built from earlier mutations and read the levels again from it.
- **`PmwsDirtyRescan`.** `next_dirty()` raises it when the dirty-index ring lapped your cursor. It is not sticky, because the cursor has already moved to the ring's head. Read every market you follow once, with `read_state()` and by draining `next_event()`. Then carry on with `next_dirty()`. The revision in a dirty entry only lets you skip a state read. Poll the event stream on every entry.
- **A slow consumer loses mutations.** The daemon never waits for a consumer. The retained mutations per market are bounded (`segment.info.event_capacity`). A consumer that falls behind is overtaken and gets `PmwsContinuityLost` with reason `Overrun`. It never slows ingestion.
- **Retry transient reads.** `attach()`, `read_state()` and `next_event()` can raise `PMWS_STATUS_CONTENDED`, `PMWS_STATUS_WRITER_STALLED` or `PMWS_STATUS_NO_PUBLISHED_STATE`. Retry the call. In TypeScript, `isTransient(error)` tests for these. See `retry_transient` in [examples/bbo.py](https://github.com/codebuster22/pm-ws-preview/blob/v1/examples/bbo.py).
- **Threads.** A Python `Segment` serializes its calls with one lock. `wait()` holds that lock for the whole call, so `close()` blocks until the wait ends. Pass a finite `timeout_ms`. In TypeScript, `wait()` blocks the event loop for its full duration.
- **`PMWS_LIB`.** Set this environment variable to the path of the compiled library to override where the binding looks. Without it, both bindings look for `target/release/libpm_ws.dylib` (macOS) or `libpm_ws.so` (Linux), then the same name under `target/debug`, inside the clone. If neither exists, Python raises `FileNotFoundError` and TypeScript throws an `Error`. The library must come from the same tree as the daemon. Both bindings check FFI version 8 and ABI version 5 and refuse anything else. Sources: [pmws.py](https://github.com/codebuster22/pm-ws-preview/blob/v1/bindings/python/pmws.py) and [pmws.ts](https://github.com/codebuster22/pm-ws-preview/blob/v1/bindings/node/pmws.ts).

## 5. Run v2

### 5.1 The selection file

The daemon discovers no markets. A selection file names them, and the run keeps exactly that set.

```text
{
  "limitless": [
    { "slug": "<limitless-market-slug>", "end_epoch": <unix-seconds> }
  ],
  "polymarket": [
    {
      "condition_id": "<condition-id>",
      "clob_token_ids": ["<token-id-1>", "<token-id-2>"],
      "end_epoch": <unix-seconds>
    }
  ]
}
```

The daemon ignores every other field. The seeded files carry `title`, `game_start`, `tag`, `generated_at`, `valid_until` and `note` for people to read.

The daemon enforces these rules when it loads the file. A violation is refused before any connection opens, and the daemon exits 2.

| Rule | Detail |
| --- | --- |
| Keys | `limitless` and `polymarket` must both be present. Either list may be empty, but not both. |
| Size | The file is at most 4 MiB. |
| Targets | At most 4096. A Limitless row counts one. A Polymarket condition counts two, one per token. |
| Connections | Rows go 100 to a connection, per venue, in file order. A Limitless connection carries up to 100 markets. A Polymarket connection carries up to 100 conditions, which is 200 tokens. |
| Identifiers | Non-empty, at most 1024 bytes. Slugs, condition ids and token ids never repeat. A condition has exactly two token ids. |
| `end_epoch` | Required, as a whole number of Unix seconds. |
| Lifetime, `--serve` | `end_epoch` must be later than now. One expired row blocks the run. |
| Lifetime, measurement run | `end_epoch` must be at least now plus `--max-seconds` plus 210 s. |

The seeded files:

| File | Contents | Valid until |
| --- | --- | --- |
| [`selections/limitless.json`](https://github.com/codebuster22/pm-ws-preview/blob/v2/selections/limitless.json) | Four long-dated Limitless markets | 31 December 2026 |
| [`selections/nfl-ncaa.json`](https://github.com/codebuster22/pm-ws-preview/blob/v2/selections/nfl-ncaa.json) | Polymarket NFL and college-football conditions | The `valid_until` header in the file |
| [`selections/mixed.json`](https://github.com/codebuster22/pm-ws-preview/blob/v2/selections/mixed.json) | Both of the above | The `valid_until` header in the file |

`valid_until` is the earliest market end in the file. Market ends are the venue's end dates, not kickoff times.

[`bench/discover_sports.py`](https://github.com/codebuster22/pm-ws-preview/blob/v2/bench/discover_sports.py) rebuilds the Polymarket files. It needs `curl`. It sends `GET https://gamma-api.polymarket.com/events` for one tag at a time. The default tags are `nfl` and `cfb`. It asks for the newest listings first and keeps at most four markets per event, so one game's props do not fill the file. It makes at most three requests per tag, half a second apart. It writes descriptors only.

```sh
python3 bench/discover_sports.py --output selections/nfl-ncaa.json --min-days 10 --max-conditions 200
python3 bench/discover_sports.py --output selections/mixed.json --min-days 10 --merge-limitless selections/limitless.json
python3 bench/discover_sports.py --prune selections/nfl-ncaa.json --output selections/nfl-ncaa.json
```

| Flag | Effect |
| --- | --- |
| `--output <path>` | Required. The file to write. |
| `--min-days <n>` | Keep markets that end at least this many days from now. Default 5. |
| `--per-event <n>` | Markets kept per event, in the venue's order. Default 4. |
| `--max-conditions <n>` | Keep the first n conditions: upcoming games by start time, then the rest. Default 200. Each condition is two targets. |
| `--tag <slug>` | Gamma tag to fetch. Repeatable. |
| `--pages <n>` | Pages of 100 events per tag. Default 3. |
| `--merge-limitless <file>` | Copy that file's Limitless rows into the output. |
| `--prune <file>` | Copy a selection without rows that end within an hour. It makes no network request. |

The venue allows about 60 requests an hour, so the script makes at most three requests per tag, half a second apart, and stops after three HTTP pushbacks. It ends by printing `<path>: <n> limitless, <n> polymarket, valid until <date>`.

### 5.2 The serve recipe

```sh
RUN=runs/$(date +%Y%m%d-%H%M%S); mkdir -p "$RUN"
./target/release/pmwsd upstream --selection selections/nfl-ncaa.json --output "$RUN/report.json" \
  --serve --snapshot "$RUN/snapshot.json" --snapshot-seconds 5 --workers 2 \
  --min-seconds 1 --min-events 1 --max-seconds 3600 --tape | python3 bench/tape_summary.py
```

| Flag | Effect |
| --- | --- |
| `--selection <path>` | The selection file from 5.1. |
| `--output <path>` | Where the final report goes. The path must not exist. Use a new `RUN` directory each time. The daemon refuses an existing path. |
| `--serve` | A long-running run. It waives the `--min-seconds` and `--min-events` floors. Rows only need to be unexpired. A failed connection is retried after five seconds. Faults, resolved markets and lost health do not end the run. |
| `--snapshot <path>` | A live summary file, rewritten while the run goes (5.5). |
| `--snapshot-seconds <n>` | Seconds between rewrites. 1 to 60, default 2. |
| `--workers <n>` | Worker threads that own the connections. 1 to 8, default 2. |
| `--min-seconds`, `--min-events` | Floors for a measurement run. A serve run does not use them to decide when to stop. `--serve` waives the 900-second and 10,000-event minimums these flags otherwise carry. All that is left is `--min-seconds` at least 1, `--min-events` at least 1, and `--max-seconds` not below `--min-seconds`. |
| `--max-seconds <n>` | The serve run ends this many seconds after the process starts. At most 14400. It must not be below `--min-seconds`. |
| `--tape` | Print one JSON line per admitted batch to stdout (5.4). |

Three more flags exist. `--diagnostic` waives the floors and `--serve` sets it. `--cpu-timing` adds CPU timing to the report. `--control-socket <path>` opens the control socket (5.7).

Run the binary with no arguments to see the usage. It exits 2.

```text
usage: pmwsd upstream --selection <descriptor-json> --output <metrics-json> [--control-socket <path>]
  [--workers 2] [--min-seconds 900] [--max-seconds 7200] [--min-events 10000]
  [--diagnostic] [--cpu-timing] [--serve] [--snapshot <path>] [--snapshot-seconds 2] [--tape]
```

Stop the run with Ctrl-C or SIGTERM. The daemon then writes the report and exits 0. If the run reaches `--max-seconds`, it writes the report and exits 2. That is normal for a serve run. A SIGKILL leaves no report, only the last snapshot.

### 5.3 Reading the stderr line

The daemon prints nothing else to stderr during a run. The tape goes to stdout, so the status lines stay on your terminal. Every ten seconds:

```text
upstream elapsed=<n>s evidence=<a>/<b> healthy_connections=<c>/<d> activity=[<l>, <p>] faults=<n>
```

| Field | Meaning |
| --- | --- |
| `elapsed` | Seconds since the daemon started. |
| `evidence=<a>/<b>` | Targets with subscription evidence, out of all selected targets. For Limitless, the venue acknowledged the market. For Polymarket, a data message arrived for the token. |
| `healthy_connections=<c>/<d>` | Connections that are up, subscribed and have shown a heartbeat, out of all connections. A Limitless connection must also have all its targets acknowledged. In a serve run, an acknowledgement for only some of its markets still counts. |
| `activity=[<l>, <p>]` | Events since the process started. `<l>` counts Limitless `orderbookUpdate` events. `<p>` counts Polymarket `book` and `price_change` events. Other families are not in this count. |
| `faults` | One total of the fault counters, malformed messages, routing rejections, admission overloads, generation errors, member-order errors and sequence errors. Zero is the healthy value. The names are in the snapshot. |

A quiet market is not a fault. `evidence` rises as subscriptions are acknowledged or first data arrives.

Two other lines can appear once each:

```text
upstream measured window started by readiness
upstream measured window started by timeout
```

`readiness` means every planned connection was healthy (5.3) for 30 s in a row. `timeout` happens only in a serve run, 90 s after start, when readiness has not held. Per-family counts and latencies in the report and snapshot cover events inside this window only.

```text
upstream snapshot unavailable: <error>
```

This line appears at most once, if a snapshot cannot be written. A run that exits 2 ends with `pmwsd upstream: <error>`. For a finished run the error reads `upstream not qualified: <reason>` (5.6).

### 5.4 The tape

`--tape` prints one JSON line to stdout for each admitted batch. A batch is one received message. Pipe the tape. Do not redirect it to a file, because `text` holds source content and the daemon keeps no venue payload on disk.

```text
{"t_ns":<n>,"venue":"polymarket","stream":"polymarket-0","family":"price_change","market":"<condition-id>","events":<n>,"bytes":<n>,"handoff_ns":<n>,"generation":<n>,"text":"…"}
```

| Field | Meaning |
| --- | --- |
| `t_ns` | Receive time in nanoseconds on the daemon's clock, counted from process start. |
| `venue` | `limitless` or `polymarket`. |
| `stream` | `<venue>-<index>`, one per connection. |
| `family` | The family of the batch's first event, such as `orderbookUpdate`, `book` or `price_change`. |
| `market` | The venue-native market id of the first event: the slug for Limitless, the condition id for Polymarket. `null` if the event carries none. |
| `events` | Events in the batch. |
| `bytes` | Size of the source message in bytes. |
| `handoff_ns` | Nanoseconds from receive to typed handoff. |
| `generation` | The connection generation. It changes on reconnect. |
| `text` | The source message cut at 600 bytes, with `…` appended when something was cut. |

One batch makes one line. `family` and `market` come from the first event only, so a batch with several events is counted under its first event's family.

The tape thread owns stdout. Lines wait in a queue of 2048. If the queue is full, or stdout stops accepting writes, the new line is dropped and counted. Ingestion never waits. The drops show only as `tape_dropped` in a snapshot (5.5).

[`bench/tape_summary.py`](https://github.com/codebuster22/pm-ws-preview/blob/v2/bench/tape_summary.py) reads the tape on stdin. It writes no file.

| Flag | Effect |
| --- | --- |
| `--interval <s>` | Seconds between tables. Default 10. A table prints when the next line arrives, so a silent feed prints nothing. |
| `--follow <market-id>` | Echo that market's lines, with text. Repeatable. |
| `--peek <n>` | Echo the first n lines unchanged. |

Each table has one row per venue and family:

```text
tape last <n>s: lines=<a> events=<b> bytes=<c>
  <venue>    <family>             lines=<n>       events=<n>       bytes=<n>
tape since start: lines=<a> events=<b> bytes=<c>
  <venue>    <family>             lines=<n>       events=<n>       bytes=<n>
tape at end of input: lines=<a> events=<b> bytes=<c>
```

`--follow` lines look like this:

```text
<venue> <family> <market> events=<n> bytes=<n> handoff_us=<x> gen=<n> text=<text>
```

Input that is not a tape line passes through unchanged.

Five minutes on 9 October 2026 with `selections/mixed.json`: one Limitless socket with 4 markets and two Polymarket sockets with 100 conditions each, on Apple M1, 8 CPUs. The last 10-second table and the end-of-input table:

```text
tape last 10s: lines=257 events=257 bytes=133478
  polymarket best_bid_ask         lines=74      events=74       bytes=21213
  polymarket price_change         lines=183     events=183      bytes=112265
tape at end of input: lines=9259 events=9657 bytes=5273179
  limitless  marketCreated        lines=2       events=2        bytes=358
  limitless  marketResolved       lines=5       events=5        bytes=875
  limitless  orderbookUpdate      lines=4       events=4        bytes=1874
  limitless  system               lines=3       events=3        bytes=492
  polymarket best_bid_ask         lines=2748    events=2748     bytes=787406
  polymarket book                 lines=2       events=400      bytes=298455
  polymarket new_market           lines=104     events=104      bytes=264580
  polymarket price_change         lines=6391    events=6391     bytes=3919139
```

### 5.5 Snapshot and report

`--snapshot <path>` rewrites a file every `--snapshot-seconds`, once every worker has published. The daemon writes `<path>.tmp` and renames it, so a reader sees a whole file. `--output` is written once, when the run ends. Both use the schema `pm-ws-native-upstream-v2`.

A snapshot differs from a report in four ways:

- Its `reason` is `snapshot` and `qualified` is `false`.
- It has a `snapshot` block: `elapsed_ns`, `serve`, `interval_seconds`, `tape_dropped` and `window_opened_by`. The last is `null` until the window opens.
- Each shard has `targets`, with a `covered` flag per target, and `recent`, the last 48 batches. These hold identifiers, sizes and times, never event content.
- It has no end time. `measured_end_ns` is 0.

[`bench/snapshot_summary.py`](https://github.com/codebuster22/pm-ws-preview/blob/v2/bench/snapshot_summary.py) prints either file.

```sh
python3 bench/snapshot_summary.py "$RUN/snapshot.json"
python3 bench/snapshot_summary.py "$RUN/report.json"
```

```text
<path>: schema=<schema> reason=<reason> qualified=<true|false>
  snapshot after <n>s serve=<true|false> window_opened_by=<readiness|timeout|None> tape_dropped=<n>
  measured window: <n>s
shard <i> <venue> <stream> generation=<n> connected=<true|false> covered=<a>/<b> received=<n> decoded=<n> faults=<json> controls=<json>
  family                   events    p50 µs    p99 µs      max µs
  <family>                 <n>       ≤<n>      ≤<n>        <x>
```

The report of the five-minute `selections/mixed.json` run above, on Apple M1, 8 CPUs. It ended at `--max-seconds`, so its reason is `serve_max_seconds`. One Polymarket socket logged three failed connection attempts before it stayed up:

```text
runs/20261009-030730/report.json: schema=pm-ws-native-upstream-v2 reason=serve_max_seconds qualified=false
  measured window: 245s
shard 0 limitless limitless-0 generation=2 connected=false covered=4/4 received=27 decoded=14 faults={} controls={"engineio_open": 1, "engineio_ping": 11, "engineio_pong_queued": 11, "namespace_connect": 1}
  family                   events    p50 µs    p99 µs      max µs
  marketCreated                 2        ≤6       ≤25      24.333
  marketResolved                3       ≤17       ≤44      43.917
shard 1 polymarket polymarket-1 generation=2 connected=false covered=200/200 received=4206 decoded=4405 faults={} controls={"ping_sent": 30, "pong": 30}
  family                   events    p50 µs    p99 µs      max µs
  price_change               2376       ≤27      ≤117    1075.709
  best_bid_ask                684       ≤14       ≤84     914.000
  new_market                   43       ≤59      ≤282     281.167
shard 2 polymarket polymarket-0 generation=5 connected=false covered=200/200 received=5039 decoded=5238 faults={"connect_failed": 3} controls={"ping_sent": 29, "pong": 29}
  family                   events    p50 µs    p99 µs      max µs
  price_change               2578       ≤28      ≤122     604.167
  best_bid_ask               1490       ≤13       ≤71   10389.375
  new_market                   43       ≤55      ≤130     129.208
```

| Part | Meaning |
| --- | --- |
| `reason`, `qualified` | Why the file was written. `qualified` is `true` only for a measurement run that met its floors. |
| `snapshot after` | Snapshots only. Seconds elapsed, the mode, how the window opened (`None` until it does), and tape lines dropped. |
| `measured window` | Final reports show its length. It reads `not opened` if the window never opened. A snapshot with an open window omits this line. |
| `shard` | One connection. `generation` and `connected` are its latest state. `covered` is targets with evidence over targets required. |
| `received`, `decoded` | Messages received and events decoded on that connection. |
| `faults`, `controls` | JSON maps from counter name to count, `{}` when empty. Fault names include `peer_ended` and `admission_fenced`. `controls` names include the venues' protocol records, such as `engineio_ping`, `websocket_ping`, `websocket_pong` and `pong`, plus `peer_respawned` in a serve run and `selected_target_resolved` when a selected market resolves. |
| family rows | Events inside the window, then p50, p99 and the maximum of receive-to-typed-handoff. Quantiles are histogram upper bounds in microseconds, in 1 µs buckets below 1 ms and doubling above. `overflow` means the last bucket. The maximum is exact. Families with no events are left out. |

### 5.6 Exit codes and reasons

| Code | When |
| --- | --- |
| 0 | A serve run stopped by Ctrl-C or SIGTERM. A measurement run that qualified. |
| 2 | Everything else: a serve run that hit `--max-seconds`, a bad flag or selection, an existing `--output`, and any run that ended for another reason. |

Every run that gets as far as ending writes its report before it exits. The `reason` field says why it ended.

| Reason | Meaning |
| --- | --- |
| `operator_interrupt` | Ctrl-C. |
| `operator_terminate` | SIGTERM. |
| `serve_max_seconds` | A serve run reached `--max-seconds`. |
| `frozen_selection_changed` | The desired market set changed, or the control channel closed (5.7). |
| `controller_ended` | The control task stopped before the run did. |
| `controller_failed` | The control task ended with an error. |
| `worker_ended` | A worker thread stopped before the run did. |
| `selected_target_resolved` | A selected market resolved. A measurement run stops on it. A serve run keeps going, but its report carries this reason if any selected market resolved. |
| `observed_fault` | Measurement run only: a fault was counted. |
| `connection_health_lost` | Measurement run only: after the window opened, readiness was lost. |
| `readiness_timeout` | Measurement run only: the window had not opened after 180 s. |
| `healthy_floors_reached` | Measurement run: the window lasted `--min-seconds` and both venues reached `--min-events`. Qualified, exit 0. |
| `diagnostic_only` | The same floors, reached with `--diagnostic`. Not qualified, exit 2. |
| `insufficient_samples` | The window reached `--max-seconds` without meeting the floors. |
| `terminal_accounting_fault` | A run that met its floors had a fault or an accounting mismatch at the end. |

The floors need both venues. A selection with one venue cannot meet them and ends as `insufficient_samples`.

The [v2 README](https://github.com/codebuster22/pm-ws-preview/blob/v2/README.md) shows how to wrap a measurement run with `bench/run_upstream.py`.

### 5.7 Changing markets

Edit the selection file and start a new run with a new `RUN` directory. The daemon cannot add or remove markets while it runs.

`--control-socket <path>` opens a Unix socket. It takes one JSON line per request and answers with one JSON line. Keep the path short, because Unix socket paths are limited to about 100 bytes. The line `{"command":"status"}` returns these fields:

| Field | Meaning |
| --- | --- |
| `ok` | `true` or `false`. |
| `code` | `ok`, `invalid`, `busy` or `stopped`. |
| `revision` | A count that rises when the desired set changes. |
| `desired` | The number of desired targets. |
| `changed` | `false` for `status`. |
| `ready` | Always `false`. |

`status` carries no health, counts or faults. Use the stderr line, the tape or the snapshot for those.

The socket also accepts `add`, `remove`, `replace`, `lease`, `release` and `renew`. Any request that changes the desired set ends the run with `frozen_selection_changed` and exit 2, even in a serve run. A request that changes nothing is harmless. The socket is therefore a status check in practice.

## 6. Numbers

Percentiles are histogram upper bounds, written with "≤". A maximum is one exact observation. No averages are reported.

### 6.1 v1

All v1 figures are from runs on 7 October 2026, one per machine.

| Figure | Value | Machine |
| --- | --- | --- |
| Workload | 300 Limitless markets on 3 sockets of 100 markets, one shared-memory segment per socket | Apple M1, 8 CPUs, and 32-core Ubuntu 24.04 under WSL2 |
| Connections up | 3 of 3 | Apple M1, 8 CPUs |
| Markets held | 300 of 300 | Apple M1, 8 CPUs |
| Resident memory | about 220 MB, including the three mapped segments | Apple M1, 8 CPUs |
| Resident memory | about 185 MB, including the three mapped segments | 32-core Ubuntu 24.04 under WSL2 |
| CPU | about 0.2% of one core at 300 books, over a 5 s window | Apple M1, 8 CPUs |

That run captured no events-per-second figure and no exact p99 or maximum latency for v1; its only latency note was that p99 queue age, from venue frame to published book, stayed well under a millisecond.

### 6.2 v2

Qualification figures come from [bench/reports/native-upstream-flat-packed-100-m1.md](https://github.com/codebuster22/pm-ws-preview/blob/v2/bench/reports/native-upstream-flat-packed-100-m1.md). The scale rows are from a run on 7 October 2026.

| Figure | Value | Machine |
| --- | --- | --- |
| Qualification workload | 100 Limitless markets and 100 Polymarket conditions (200 tokens), two sockets | Apple M1, 8 CPUs |
| Measured time | 4619.124 s | Apple M1, 8 CPUs |
| Limitless `orderbookUpdate`, receive to typed handoff | p99 ≤158 µs, maximum 502.042 µs | Apple M1, 8 CPUs |
| Polymarket `price_change`, receive to typed handoff | p99 ≤111 µs, maximum 32570.333 µs | Apple M1, 8 CPUs |
| Polymarket `book`, receive to typed handoff | p99 ≤628 µs, maximum 902.541 µs | Apple M1, 8 CPUs |
| Scale workload | 2,500 targets (500 Limitless markets, 1,000 Polymarket conditions as 2,000 tokens) on 15 sockets | 32-core Ubuntu 24.04 under WSL2 |
| Scale CPU | 0.03 of one core | recorded on both machines at this workload |
| Scale memory | about 70 MiB resident | 32-core Ubuntu 24.04 under WSL2 |

The v2 figures are upstream costs. They run from the socket read of a complete message to a typed, validated event at the handoff. They are not end-to-end consumer latency. v1 queue age and v2 handoff time are different stages. Figures are never compared across machines.

## 7. Venue behaviour

### 7.1 Limitless

Full contracts: [v2 docs/limitless.md](https://github.com/codebuster22/pm-ws-preview/blob/v2/docs/limitless.md) and [v1 docs/limitless.md](https://github.com/codebuster22/pm-ws-preview/blob/v1/docs/limitless.md).

- **Transport.** `wss://ws.limitless.exchange`, Socket.IO namespace `/markets`, WebSocket only, no polling fallback.
- **Heartbeat.** The server drives the Engine.IO heartbeat. It sends a ping and pm-ws answers with a pong. pm-ws sends no application ping. In one observed connection the server pinged about every 25 s. pm-ws reads the heartbeat values from each connection and does not assume them.
- **Health.** Transport health comes from the connection and its heartbeat. A quiet market does not time out, lose its subscription or trigger a reconnect. The venue declares no per-market cadence.
- **Subscription.** `subscribe_market_prices` takes CLOB market slugs in `marketSlugs`. Each call replaces the connection's whole set, so one call carries every market. The venue does not retain subscriptions across a reconnect. `subscribe_market_lifecycle` and `unsubscribe_market_lifecycle` control the lifecycle feed. Both feeds are open and use no credential.
- **Acknowledgement.** The venue answers with a `system` event that lists the markets it accepted. pm-ws counts a subscription as confirmed only when that `markets` set matches the request. A `system` notification without `markets` confirms nothing. A complete `orderbookUpdate` snapshot then follows for each valid, unresolved CLOB market.
- **Known omission.** In a run on 7 October 2026 the venue left some in-play match markets (the "vs" slugs) out of the acknowledgement and sent no error. The venue does not document this. In v2 `--serve` mode the partial acknowledgement is recorded, the socket is kept, and only the acknowledged markets count as covered. Outside `--serve` it is a subscription fault. Leaving match markets out of a selection avoids it.

| Family | Identity | Notes |
| --- | --- | --- |
| `orderbookUpdate` | `marketSlug` | Complete bid and ask arrays, bids highest first, asks lowest first. Carries `timestamp` and `version`. Prices and sizes are JSON numbers, decoded exactly. |
| `newPriceData` | `marketAddress` | Yes and No `updatedPrices`, `blockNumber`, `timestamp`. |
| `marketCreated` | `slug` | Lifecycle feed. |
| `marketResolved` | `slug` | Lifecycle feed and the market's own room. |
| `system`, `exception` | connection generation | Acknowledgements and errors, kept as source-control records. |
| `oraclePriceData` | none | Not documented by the venue. Seen in market rooms. Kept as an unknown family under its own name. |

Every valid arrival is published, including repeats. An unknown event is kept as a bounded envelope or reported as an explicit unsupported fault. It is never skipped silently. v1 builds its books from `orderbookUpdate`. One market has one socket in both builds; v1's optional replica pool (`--pool`, `replicas > 1`) is the only path that drops an arrival, and it drops only an equal or lower `version` key arriving on a second socket for the same market.

- **Version.** The venue describes `version` as a per-book publisher sequence. It is not contiguous and not comparable across connections. A backend failover can reset it, and a database fallback can send 0. pm-ws keeps it as source data. v2 never uses it to drop or reorder events; in v1 it is the key the optional replica pool gates on, and nothing else reads it.
- **Resolution.** `marketResolved` carries `slug`, `type`, `winningOutcome`, `winningIndex` and `resolutionDate`. The venue sends it to lifecycle subscribers and to the market's own room, so a subscribed market receives it without the lifecycle feed. In observed runs the payload held only those five keys, and the same resolution arrived three times, byte-identical, within one second. pm-ws publishes each arrival; at one socket per market neither build deduplicates. v1 reports the resolution and leaves the book unchanged. Venue-wide lifecycle needs its own subscription.
- **REST.** pm-ws uses public REST only to select markets. `/markets/active` returns at most 25 entries per page, and a larger `limit` is rejected, not truncated. The listing holds group containers and their child markets, so select leaf markets that have their own condition ID and expiry. In one observed sweep the endpoint answered HTTP 403 to Python's default `urllib` user agent, so send a truthful tool-identifying one. The venue publishes no numeric REST limit. On HTTP 429, honor `Retry-After`. Without it, back off from 1 s and double per retry. Never retry 400 or 401. The connection-attempt budget and command spacing are in 7.3.

### 7.2 Polymarket

Full contract: [v2 docs/polymarket.md](https://github.com/codebuster22/pm-ws-preview/blob/v2/docs/polymarket.md).

- **Transport.** `wss://ws-subscriptions-clob.polymarket.com/ws/market`. A plain WebSocket on the CLOB market channel. No authentication.
- **Subscription.** Subscribe by token ids with `assets_ids` and `type: "market"`. Later `subscribe` and `unsubscribe` messages change the set. pm-ws requests `custom_feature_enabled` on every connection, which adds `best_bid_ask`, `new_market` and `market_resolved`. Both outcome tokens of a condition go on the same connection.
- **No acknowledgement.** The venue documents none, so a successful write proves nothing. Coverage is shown only by data arriving for a token. A quiet token and an unconfirmed subscription look the same until data arrives.
- **Heartbeat.** pm-ws sends `PING` every 10 s and the server replies `PONG`. A written `PING` must get its `PONG` before the heartbeat deadline. Market data does not satisfy that deadline.
- **First message.** The venue's guide says a `book` is emitted on subscription. In the 7 October 2026 run the first message on each socket was about 770 KB, at 100 conditions (200 tokens) per socket. It produced the largest handoff times in that run. After it the venue sent small deltas.

| Family | Identity | Notes |
| --- | --- | --- |
| `book` | `market` + `asset_id` | Full aggregated book for one asset: bids, asks, `timestamp`, `hash`. |
| `price_change` | `market` | One `timestamp` and an ordered `price_changes` array. Each entry has `asset_id`, price, size, side, hash, best bid and best ask. The venue's guide says size `"0"` removes a level. |
| `last_trade_price` | `market` + `asset_id` | Price, size, fee, side, timestamp, transaction hash. |
| `tick_size_change` | `market` + `asset_id` | Old and new tick size, timestamp. |
| `best_bid_ask` | `market` + `asset_id` | Needs `custom_feature_enabled`. Best bid, best ask, spread, timestamp. |
| `new_market` | `market` and asset ids | Needs `custom_feature_enabled`. Not scoped to the subscribed set: it reports every market created on the venue. The venue does not document that scope. It was observed. |
| `market_resolved` | `market` and asset ids | Needs `custom_feature_enabled`. The complete resolution object. |

- **Atomic messages.** A `price_change` message is validated whole and published whole. It is never split into one event per asset.
- **Resolution.** `market_resolved` reaches a subscribed market because pm-ws requests custom features. pm-ws publishes the complete object for each valid arrival. It does not change subscriptions or health because of it.
- **Ordering.** The venue declares no sequence number, timestamp ordering, hash meaning or cadence. Message pairs have been seen with the same timestamp and the same asset and hash list but different prices and sizes. pm-ws publishes both.
- **Start-up faults.** In a run on 7 October 2026 on Apple M1, 8 CPUs, some Polymarket sockets had failed handshakes and failed reads at start. Each recovered on a later attempt.

### 7.3 Limits pm-ws applies to itself

Neither venue documents a limit on WebSocket connections, connection attempts, command rate or markets per subscription. These limits are pm-ws's own choices. They apply to both venues. The ledger and pacer are in [src/etiquette.rs](https://github.com/codebuster22/pm-ws-preview/blob/v2/src/etiquette.rs).

| Limit | Value | Notes |
| --- | --- | --- |
| Rows per connection | 100 selection rows | v2. A Limitless row is one market. A Polymarket row is one condition, which is two tokens. |
| Targets per process | 4,096 | v2. |
| Connections | initial envelope of 20 | Reduced on venue pushback. v1 also has an optional cap, `max_venue_connections`. |
| Connection attempts | 280 per rolling day | One process-wide ledger. In v2 it counts both venues, so a 15-socket launch spends 15 at once. A failed handshake spends one. v1 setting: `daily_connection_attempt_budget`. |
| Command spacing | one subscription-bearing command per 500 ms per endpoint | v1 setting: `min_command_interval_ms`. |
| Public REST | 60 requests per hour | Market selection only. Never ingestion or recovery. |
| Frame size | 1 MiB maximum | v2. |
| Admission | 64 MiB per shard | v2. In the 15-socket scale run, one shard per socket, the declared ceiling is 960 MiB. It is a limit the gate refuses to exceed, not an allocation. |

Every queue is bounded and states its overflow behavior. A slow consumer never blocks ingestion. A malformed or over-capacity member rejects its whole message batch.

Order of recovery after a loss:

1. Resubscribe on the existing connection when it is usable.
2. Reconnect if that fails. The attempt is paced and spends one from the ledger. A replacement connection starts only after the previous generation stops.
3. The new connection has a new generation. The fence rejects anything from the retired one.
4. The gap is exposed as a continuity boundary. Missed events are not replayed.
5. REST recovery is off unless an operator opts in. It is never on the ingestion path.

Both selected feeds are open, so neither preview captures a credential. The venue contracts state the rule for a keyed feed: a handshake refused with an authentication challenge, HTTP 401 or HTTP 403 is the fault `credential_rejected`, which ends that venue's attempts until the operator acts. Neither preview build implements it.

## 8. Limits and what is not built

### 8.1 v1

- Limitless is the only venue. There is no Polymarket support and no cross-venue view.
- It rebuilds books from the venue's snapshots and updates. It normalizes no economics and places no orders.
- On a gap, the daemon resubscribes or reconnects. Readers are told about the gap:
  - Python raises `PmwsContinuityLost` with a reason. It stays raised until the reader calls `reattach()`.
  - The TypeScript reader exposes the same condition as error code `PMWS_CONTINUITY_LOST`.
  - `pmws-run` prints `continuity_loss`, `fenced`, `reconnecting` and `resubscribing` lines.
- Consumers read shared memory on the same host. There is no network API.
- `Segment.connect` in the Python and TypeScript readers must run as the same OS user as the daemon. Otherwise the daemon refuses the attach.
- Both bindings load the `libpm_ws` library built from the same tree. They use the path in `PMWS_LIB` when it is set, and otherwise look in `target/release`, then `target/debug`. They are not installable packages.
- The TypeScript consumer needs Node 22.18 or newer, because Node runs the `.ts` file directly.
- The daemon calls no REST endpoint on the update path and keeps no history on disk.
- `pmwsd` prints one start line and, when it stops, one summary line per shard. It logs nothing per event. Use `pmwsctl status`, the metrics, or `pmws-run` to see activity.
- `pmws-run` follows one market per run and ends after `--seconds` (default 60).
- `pmwsctl` defaults to the socket `/tmp/pmwsd.sock`. Pass `--socket run/pmwsd.sock` to reach the daemon that `setup.sh` configured.
- Metrics are on one port. The rendered `pmwsd.toml` sets `metrics_listen = "127.0.0.1:9090"`; the key itself has no default. The value must be a numeric host and port, because resolving a host name would be blocking I/O on the ingestion thread. The endpoint serves `GET /metrics` over plain HTTP and nothing else. If the key is absent, no endpoint exists.
- The four seeded markets run until 31 December 2026. After that date, or if one resolves early, replace them in `config/pmwsd.toml.in` and run `./setup.sh` again.

### 8.2 v2

- There is no local transport, no consumer API, and no Python or TypeScript binding. The rail ends in a metrics-only in-process receiver, so no process other than the daemon reads an event.
- It builds no order book, derives no diffs, normalizes no economics, and makes no trading decisions.
- The tape is a display aid, not a transport:
  - It writes one JSON line per admitted batch to stdout.
  - Its `text` field is cut at 600 bytes.
  - Its `family` and `market` fields describe only the first event of a batch.
  - Its stdout queue holds at most 2048 pending lines. When that queue is full, the new line is dropped and ingestion is never blocked. The drop count appears only as `tape_dropped` in snapshot files.
  - Pipe it into a viewer such as `bench/tape_summary.py`. Do not redirect it to a file.
- A selection change needs a restart. A selection holds at most 4096 targets, and the file may be at most 4 MiB. Each row needs a whole-second `end_epoch`.
- The control socket is status-only in practice:
  - `{"command":"status"}` returns the revision and the desired-target count. It carries no health or event data.
  - Any `add`, `remove`, `replace`, `lease` or `release` that changes the desired set ends the run with exit 2, in every mode. A request that changes nothing does not.
  - There is no control channel unless you pass `--control-socket`.
- A `--serve` run needs every selection row to end in the future. One expired row blocks the run.
  - The four seeded Limitless markets in `selections/limitless.json` run until 31 December 2026.
  - The NFL and college-football files are generated by [`bench/discover_sports.py`](https://github.com/codebuster22/pm-ws-preview/blob/v2/bench/discover_sports.py), which stamps each file with its own `valid_until` (the earliest row end). Regenerate with `python3 bench/discover_sports.py --output selections/nfl-ncaa.json --min-days 10`, or prune expired rows with `--prune`.
- Exit code 2 when `--max-seconds` is reached is normal for a serve run. Under `--serve`, Ctrl-C or SIGTERM exits 0; without `--serve`, a signal exits 2 too. Without `--serve`, a run exits 2 unless it qualifies, and qualifying needs events from both venues. Read `reason` in the final report. The report is written before the daemon exits. `--max-seconds` may not exceed 14400.
- `--output` must not exist before the run. A killed process (SIGKILL) leaves no final report, only the last snapshot.
- Faults appear as a count (`faults=<n>`) in the status line, with no line per fault. Counts by family exist. Byte counts by family do not, because bytes are recorded per batch.
- `bench/run_upstream.py` and `bench/report_upstream.py` validate only the 100 Limitless plus 100 Polymarket workload on two connections. `run_upstream.py` never passes `--serve`, `--snapshot` or `--tape`.

Both versions are previews with no stability promise.

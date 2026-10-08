# pm-ws preview guide

Two frozen builds of pm-ws, each on its own branch and tag of this repository: v1, the Limitless order-book daemon with same-host Python, TypeScript and Rust consumers, and v2, the native-event rail for Limitless and Polymarket with no consumer yet. Every command runs from a clone of the branch it belongs to. Output marked with a date was pasted from a live run; everything else shows the format a run prints.

## 1. What each version is

pm-ws is a Rust daemon that reads prediction-market WebSocket feeds. It decodes every price and quantity exactly into scaled integers, never floats. Each market has exactly one current WebSocket owner, and one socket carries many markets. It places no orders and makes no trading decisions.

| | v1 | v2 |
| --- | --- | --- |
| Venues | Limitless | Limitless and Polymarket |
| Output | Order books and level mutations, published through shared memory | Typed native events, taken by an in-process receiver that only counts and times them. `--tape` prints a view on stdout |
| Who can read it | Python, TypeScript and Rust consumers on the same host | No process outside the daemon. You read the tape, the snapshot and the report |
| Binaries | `pmwsd`, `pmwsctl`, `pmws-run` | `pmwsd upstream` |
| Configuration | `pmwsd.toml`, rendered by `setup.sh` from `config/pmwsd.toml.in` | One selection JSON file per run, passed with `--selection` |
| Changing markets | `pmwsctl add` and `pmwsctl remove` while the daemon runs | Edit the selection file and restart |
| Monitoring | `pmwsctl status` and Prometheus metrics | Status line on stderr every 10 s, snapshot JSON, final report JSON |

Five terms, defined as in the [glossary in the v2 tree](https://github.com/codebuster22/pm-ws-preview/blob/v2/CONTEXT.md):

- **Target.** The subscription coordinate pm-ws acts on: a venue, a market, and an asset where the venue requires one. A Polymarket condition counts as two. v1 speaks of markets.
- **Shard.** One venue's unit of publication ownership, the single admission owner of its markets. Both previews run 100 markets to a shard; v2 calls the order a shard publishes a stream.
- **Connection generation.** The label of one socket assignment. A reconnect advances it, and a batch from a retired generation is rejected before admission.
- **Handoff.** The point where an admitted batch leaves the daemon's upstream half. The v2 tape's `handoff_ns` is the time from receipt to this point.
- **Coverage.** Observed evidence that a target's own data arrived on its owning connection: the venue's acknowledgement for Limitless, observed data for Polymarket.

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

Tarballs of the same trees: <https://github.com/codebuster22/pm-ws-preview/archive/refs/tags/v1.tar.gz> and <https://github.com/codebuster22/pm-ws-preview/archive/refs/tags/v2.tar.gz>.

In each clone, build and then run the setup script:

```sh
cargo build --release --locked
./setup.sh
```

`setup.sh` takes a few seconds and runs no build, no tests and no network call. In v1 it creates `run/`, renders `pmwsd.toml` (four seeded markets) and `pmwsd.empty.toml` (no markets) from `config/` with this clone's absolute path, loads the Python binding and, when `node` is on PATH, the Node binding, and prints the next commands. Run it again after moving the clone. In v2 it creates `runs/`, checks that `pmwsd` with no arguments exits 2, runs the self-tests of the five `bench/` scripts this guide uses (`discover_sports.py`, `tape_summary.py`, `snapshot_summary.py`, `run_upstream.py`, `report_upstream.py`) and prints the serve recipe.

### 2.3 What writes where

Everything a preview does stays inside its clone.

| Path in the clone | Written by | Content |
| --- | --- | --- |
| `target/` | `cargo build` | Build output, both trees |
| `pmwsd.toml`, `pmwsd.empty.toml` | v1 `setup.sh` | Rendered configs |
| `run/` | v1 `setup.sh`, then `pmwsd` | Control socket `run/pmwsd.sock`, its `.lock` file, shared-memory segments `pmws-<instance>-<shard>.seg`, which `pmwsd` removes on a clean stop |
| `runs/` | v2 `setup.sh` creates it | The files you name with `--output` and `--snapshot` |

One exception: if `<clone>/run/pmwsd.sock` would be longer than 100 bytes, v1 `setup.sh` puts the control socket and its `.lock` file at `/tmp/pmwsd-v1.sock` and says so. No command in this guide writes a venue payload to disk. Reports and snapshots hold counters, histograms and the consumed selection, never event content, and the v2 tape goes to stdout only. (`pmws-run --record <path>` would append every inbound frame to a file; this guide never uses it.)

## 3. Run v1

### 3.1 Configuration

`./setup.sh` renders `pmwsd.toml` and `pmwsd.empty.toml` from [`config/pmwsd.toml.in`](https://github.com/codebuster22/pm-ws-preview/blob/v1/config/pmwsd.toml.in) and [`config/pmwsd.empty.toml.in`](https://github.com/codebuster22/pm-ws-preview/blob/v1/config/pmwsd.empty.toml.in). Edit a template, then run `./setup.sh` again. Both set `metrics_listen = "127.0.0.1:9090"`, `markets_per_shard = 100`, and the control socket `run/pmwsd.sock` and delivery directory `run/` as absolute paths. The seeded markets run until 31 December 2026.

The keys are defined in [`src/daemon.rs`](https://github.com/codebuster22/pm-ws-preview/blob/v1/src/daemon.rs); an unknown key is refused and `pmwsd` exits 2. Beyond the four above: `markets` (the pinned slugs), `endpoint` (the venue URL), `replicas` (connections per shard, 1 to 4), `min_command_interval_ms` (500, the gap between subscription commands), `daily_connection_attempt_budget` (280, shared by all shards), `lease_ttl_ms`, `max_control_sessions`, `max_venue_connections` (the optional connection cap of 7.3) and `[delivery] profile`.

### 3.2 Start and stop

```sh
./target/release/pmwsd --config pmwsd.toml
```

The daemon runs in the foreground, prints one line once every shard and segment exists, then prints nothing until it stops. From a run on 9 October 2026 with the seeded file, home directory elided:

```text
pmwsd pid=46848 shards=1 markets=4 socket=/…/pm-ws-v1/run/pmwsd.sock metrics=127.0.0.1:9090
```

Stop it with Ctrl-C or `SIGTERM`. It prints one line per shard and exits 0. The same run after about two minutes, during which the connection dropped twice:

```text
shard 0: connections=3 subscriptions=3 snapshots=13 resolutions=0 losses=8 unrouted=0 queue_age_max_us=228 queue_age_p99_us=228
```

Exit 1 means startup failed after the configuration loaded (another daemon on the socket, a segment that cannot be created), 2 a bad command line or configuration, 3 a shard stopped; stderr names the shard.

### 3.3 Status, add and remove

Run `pmwsctl` in a second terminal while the daemon is up. Without `--socket` it looks for `/tmp/pmwsd.sock`, so pass the path `setup.sh` printed:

```sh
./target/release/pmwsctl --socket run/pmwsd.sock status
./target/release/pmwsctl --socket run/pmwsd.sock add <slug>...
./target/release/pmwsctl --socket run/pmwsd.sock remove <slug>...
```

Every answer is pretty-printed JSON on stdout; errors go to stderr as `pmwsctl: <message>`. Exit 0 is success, 1 the daemon could not be reached or answered with an error, 2 a bad command line, 3 at least one market was rejected, 4 a bounded queue was full and nothing was applied.

`add` and `remove` print one object per slug with a `status` of `accepted`, `reconciling`, `live`, `removed`, `{"rejected": "<reason>"}` or `{"stale": "<reason>"}`. Adding a fifth market to the running daemon, then removing it; four seconds after the `add`, `status` showed it `established` and `live`:

```json
[ { "slug": "opensea-fdv-above-dollar3b-one-day-after-launch-1764857000239", "status": "accepted" } ]
[ { "slug": "opensea-fdv-above-dollar3b-one-day-after-launch-1764857000239", "status": "removed" } ]
```

`status` returns the daemon's `pid`, `rss_kib`, `metrics_listen`, one entry per shard and one per market. A market's `subscription` is `desired`, `subscribing`, `established` or `removing`. From the same run on Apple M1, 8 CPUs, trimmed to one shard and one market:

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
      "queue_age": { "samples": 6, "last_micros": 176, "max_micros": 216, "p50_micros": 216, "p99_micros": 216 },
      "publish_latency": { "samples": 4, "last_micros": 198, "max_micros": 269, "p50_micros": 256, "p99_micros": 269, "p999_micros": 269 }
    }
  ],
  "markets": [
    {
      "shard": 0,
      "market": { "slug": "will-trump-acquire-greenland-before-2027-1768930762585", "subscription": "established", "status": "live", "revision": 1, "continuity_epoch": 0 },
      "pinned": true,
      "leases": 0
    }
  ]
}
```

To start with no markets, run `pmwsd --config pmwsd.empty.toml` and add them with `pmwsctl add`.

### 3.4 Metrics

```sh
curl 127.0.0.1:9090/metrics
```

The endpoint answers `GET /metrics` only and has no authentication, so keep it on a loopback address. If port 9090 is taken, change `metrics_listen` in the template and run `./setup.sh` again. Per-shard lines carry a `shard` label.

| Metric | Meaning |
| --- | --- |
| `pmws_shard_connected` | 1 when the shard's publishing connection is established with its subscription on the wire. |
| `pmws_shard_frames_seen` | Venue frames received. |
| `pmws_shard_continuity_losses` | Times a book lost continuity. |
| `pmws_shard_queue_age_p99_micros` | 99th-percentile sampled ingest queue age, in microseconds, as a bucket upper bound. |
| `pmws_markets` | Markets the daemon holds, across every shard. The per-shard count is `pmws_shard_markets`. |

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

`pmws-run` opens one WebSocket to Limitless for one market, rebuilds the book and prints what it sees. It needs no daemon and writes no file unless given `--shm` or `--record`.

```sh
./target/release/pmws-run --market <slug> --seconds 60 --print-book
```

`<slug>` is a Limitless market slug, for example one of the four seeds in `config/pmwsd.toml.in`. `--seconds` defaults to 60 and accepts up to 86400. Without `--print-book` you get the connection and event lines and the summary; `--print-book` adds the `book`, `mutation`, `resolution` and `summary book` lines. Exit 0 means the run reached `--seconds`, 1 that it failed, 2 a bad command line.

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

How to read it:

- `connected` prints after each successful connection. `generation` numbers the connection; the `sid`, ping and payload fields are the handshake values the venue returned.
- `orderbookUpdate` is one venue frame. `bids` and `asks` count its levels; `best_bid` and `best_ask` are the first level the venue listed on each side, or `-`. Prices and quantities are exact decimal text.
- `book` prints once at start and after each new revision. `authority` is `Unsubscribed`, `Subscribing`, `Synchronizing`, `Live`, `Recovering` or `Stale(<reason>)`; `continuity_epoch` names a stretch of unbroken history. The canonical lists are the book as the venue reports it, at most three levels a side, best first; the derived complement is computed locally.
- `mutation revision=<R> epoch=<E> position=<P> side=<Bid|Ask> price=<price> qty=<old>-><new>` prints for each level change; `resolution` prints when the venue reports the market resolved and does not change the book. Neither occurred in this run.
- `continuity_loss`, `reconnecting`, `resubscribing`, `fenced` and the `source` lines print only when something happens. A `fenced` line means events from a retired connection generation were rejected before they reached the book.
- The `summary` lines count frames, connection attempts, fences, events by kind, drops and decode failures; `summary book` counts snapshots applied, mutations derived and continuity losses.

The source for every line is [src/main.rs](https://github.com/codebuster22/pm-ws-preview/blob/v1/src/main.rs).

### 4.2 The consumers bbo.py and bbo.ts

Both consumers read a segment file that a running `pmwsd` publishes. Start the daemon first, in another terminal, then list `run/` for the segment name `pmws-<instance>-<shard>.seg`. The consumers must run on the same host, as the same user as the daemon, from a clone that has been built. They attach read-only. Node runs the `.ts` file directly, with no build step.

```sh
python3 examples/bbo.py --segment run/pmws-<instance>-0.seg --market <slug> --events
node examples/bbo.ts --segment run/pmws-<instance>-0.seg --market <slug>
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--segment <path>` | required | The segment file. |
| `--market <slug>` | required | The market's venue-native key. It must be in the daemon's set and in this segment. |
| `--venue <name>`, `--kind <kind>` | `limitless`, `slug` | Venue and key kind. |
| `--seconds <n>` | `10` | Run time, at most 86400. |
| `--events` | off | Also print `mutation`, `resolution` and continuity lines. |

[bbo.py](https://github.com/codebuster22/pm-ws-preview/blob/v1/examples/bbo.py) and [bbo.ts](https://github.com/codebuster22/pm-ws-preview/blob/v1/examples/bbo.ts) print the same line shapes. The first `bbo` line prints after attach and another each time the book revision changes; `mutation` and `resolution` print only with `--events`; if the mutation stream loses continuity, the consumer prints `continuity_lost reason=<reason>` and `reattach revision=<R>` and goes on.

```text
bbo revision=<R> authority=<authority> best_bid=<price>@<qty> best_ask=<price>@<qty>
mutation revision=<R> cursor=<epoch>:<position> origin=<origin> side=<side> price=<price> qty=<old>-><new>
resolution revision=<R> cursor=<epoch>:<position> origin=<origin> outcome=<outcome> index=<index> type=<type> date=<date> path=<path>
```

A missing side prints `-`; `none` in `qty` means the level is absent on that side of the change. If the market never appears in the segment before `--seconds` ends, the consumer prints `error: market not installed in the segment` and exits 1. A normal run exits 0.

Both consumers against the daemon of 3.2, on the Greenland seed. No mutation arrived in their windows, so each printed only its opening line:

```text
$ python3 examples/bbo.py --segment run/pmws-e6baeb9ae9b6c688fdad6336bc7397c6-0.seg --market will-trump-acquire-greenland-before-2027-1768930762585 --seconds 20 --events
bbo revision=1 authority=Live best_bid=0.003@863898323 best_ask=0.899@12362000
$ node examples/bbo.ts --segment run/pmws-e6baeb9ae9b6c688fdad6336bc7397c6-0.seg --market will-trump-acquire-greenland-before-2027-1768930762585 --seconds 10
bbo revision=1 authority=Live best_bid=0.003@863898323 best_ask=0.899@12362000
```

### 4.3 Writing your own

Copy `bbo.py` or `bbo.ts` inside `examples/`: both find the binding by a path relative to their own location, so a copy in the clone root cannot import it. Both sit on the binding in [`bindings/python/pmws.py`](https://github.com/codebuster22/pm-ws-preview/blob/v1/bindings/python/pmws.py) or [`bindings/node/pmws.ts`](https://github.com/codebuster22/pm-ws-preview/blob/v1/bindings/node/pmws.ts). The calls, in Python names (TypeScript uses camelCase and `bigint` for revisions, cursors and generations):

- `pmws.Segment(path)` opens a segment read-only; `segment.resolve(venue, kind, key)` returns a `Market`, and `market.attach()` returns `(state, stream)`.
- `pmws.Segment.connect(control_socket, market)` opens the segment that serves one market through the daemon's control socket and holds a lease on it; `segment.close()` or process exit releases the lease.
- `state.revision`, `state.authority`, `state.levels` (every level, each with `side`, `price`, `quantity`) and `state.best(side)`, which returns a `Level` or `None`.
- `stream.next_event()` returns a `Mutation` (`revision`, `side`, `price`, `old_quantity`, `new_quantity`, `origin`), a `Resolution` (`revision`, `winning_outcome`, `winning_index`, `market_type`, `resolution_date`, the last three the venue's own text) or `None`.
- `segment.wait(last_generation, spin_micros=0, timeout_ms=None)` parks until the daemon publishes; `segment.publication_generation()` gives the value to pass it.
- Every price and quantity is an `ExactDecimal` (`coefficient`, `scale`, `text`). The value is `coefficient` divided by 10 to the power `scale`. Nothing is a float; print `text`, do not convert.

Rules:

- **Same user, read-only.** Run the consumer as the user that runs `pmwsd`; `Segment.connect(control_socket, market)` is refused otherwise. Neither binding has a call that writes to the segment.
- **`connect` takes a lease.** A market that nothing else holds or pins is subscribed at the venue to serve it. Neither binding can pin a market; pinning is `pmwsctl add`.
- **Continuity.** `next_event()` raises `PmwsContinuityLost` (reason `Overrun`, `Gap`, `LocalLoss`, `Reconnect`, `RecoveryBase` or `SyncDivergence`) when the stream can no longer prove it delivered every mutation in order, and keeps raising it until you call `reattach()`. Treat the state `reattach()` returns as the book and discard what you built from earlier mutations.
- **A slow consumer loses mutations.** The daemon never waits for a consumer. Retained mutations per market are bounded; a consumer that falls behind gets `Overrun`. Ingestion never slows.
- **Retry transient reads.** `attach()`, `read_state()` and `next_event()` can raise `PMWS_STATUS_CONTENDED`, `PMWS_STATUS_WRITER_STALLED` or `PMWS_STATUS_NO_PUBLISHED_STATE`; retry the call. See `retry_transient` in `bbo.py` and `isTransient` in the TypeScript binding.
- **`PMWS_LIB`.** Both bindings load the library built from the same tree, from `target/release` then `target/debug` inside the clone, or from the path in this variable. They check FFI version 8 and ABI version 5 and refuse anything else.

## 5. Run v2

### 5.1 The selection file

The daemon discovers no markets. A selection file names them, and the run keeps exactly that set. Every other field is ignored; the seeded files carry `title`, `game_start`, `tag`, `generated_at`, `valid_until` and `note` for people to read.

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

Rules, checked when the file loads; a violation exits 2 before any connection opens:

| Rule | Detail |
| --- | --- |
| Keys | `limitless` and `polymarket` must both be present. Either list may be empty, but not both. |
| Targets | At most 4096, and the file at most 4 MiB. A Limitless row counts one, a Polymarket condition two. |
| Connections | Rows go 100 to a connection, per venue, in file order: up to 100 Limitless markets, or 100 conditions (200 tokens). |
| Identifiers | Non-empty, at most 1024 bytes, never repeated. A condition has exactly two token ids. |
| `end_epoch` | Required, whole Unix seconds. With `--serve` it must be later than now, and one expired row blocks the run. For a measurement run it must be at least now plus `--max-seconds` plus 210 s. |

| File | Contents | Valid until |
| --- | --- | --- |
| [`selections/limitless.json`](https://github.com/codebuster22/pm-ws-preview/blob/v2/selections/limitless.json) | Four long-dated Limitless markets | 31 December 2026 |
| [`selections/nfl-ncaa.json`](https://github.com/codebuster22/pm-ws-preview/blob/v2/selections/nfl-ncaa.json) | Polymarket NFL and college-football conditions | The `valid_until` header in the file |
| [`selections/mixed.json`](https://github.com/codebuster22/pm-ws-preview/blob/v2/selections/mixed.json) | Both of the above | The `valid_until` header in the file |

`valid_until` is the earliest market end in the file; market ends are the venue's end dates, not kickoff times. [`bench/discover_sports.py`](https://github.com/codebuster22/pm-ws-preview/blob/v2/bench/discover_sports.py) rebuilds the Polymarket files through `curl` from `GET https://gamma-api.polymarket.com/events`, one tag at a time (default `nfl` and `cfb`), newest listings first, at most four markets per event, upcoming games ranked first. The venue allows about 60 requests an hour; the script makes at most three per tag, half a second apart, and stops after three pushbacks. It writes descriptors only.

```sh
python3 bench/discover_sports.py --output selections/nfl-ncaa.json --min-days 10 --max-conditions 200
python3 bench/discover_sports.py --output selections/mixed.json --min-days 10 --merge-limitless selections/limitless.json
python3 bench/discover_sports.py --prune selections/nfl-ncaa.json --output selections/nfl-ncaa.json
```

`--min-days` keeps markets that end at least that many days out (default 5), `--per-event` caps markets per event (default 4), `--max-conditions` caps the file (default 200), `--tag` is repeatable, and `--prune` copies a selection without rows that end within an hour, with no network request.

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
| `--output <path>` | The final report. The path must not exist, so use a new `RUN` directory each time. |
| `--serve` | A long-running run. A run without this flag is a measurement run, which must meet the floors below to qualify and stops on the first fault. With `--serve`, rows only need to be unexpired, a failed connection is retried after five seconds, and faults, resolved markets and lost health do not end the run. |
| `--snapshot <path>`, `--snapshot-seconds <n>` | A live summary file, rewritten every n seconds (1 to 60, default 2). See 5.5. |
| `--workers <n>` | Worker threads that own the connections. 1 to 8, default 2. |
| `--min-seconds`, `--min-events` | Floors for a measurement run. `--serve` waives the 900-second and 10,000-event minimums; 1 and 1 are the lowest values accepted. |
| `--max-seconds <n>` | The run ends this many seconds after start. At most 14400. |
| `--tape` | One JSON line per admitted batch on stdout. See 5.4. |

`--diagnostic`, `--cpu-timing` and `--control-socket <path>` (5.7) also exist. Run the binary with no arguments for the usage; it exits 2. Stop the run with Ctrl-C or SIGTERM: the daemon writes the report and exits 0. If the run reaches `--max-seconds`, it writes the report and exits 2, which is normal for a serve run. A SIGKILL leaves no report, only the last snapshot.

### 5.3 The stderr line

Every ten seconds on stderr (the tape goes to stdout):

```text
upstream elapsed=<n>s evidence=<a>/<b> healthy_connections=<c>/<d> activity=[<l>, <p>] faults=<n>
```

| Field | Meaning |
| --- | --- |
| `evidence=<a>/<b>` | Targets with subscription evidence, out of all selected. For Limitless the venue acknowledged the market; for Polymarket a data message arrived for the token. |
| `healthy_connections=<c>/<d>` | Connections that are up, subscribed and have shown a heartbeat. A Limitless connection must also have all its targets acknowledged. In a serve run, a Limitless acknowledgement for only some of its markets still counts. |
| `activity=[<l>, <p>]` | Events since start: Limitless `orderbookUpdate`, and Polymarket `book` plus `price_change`. |
| `faults` | One total of the fault counters: malformed messages, routing rejections, admission overloads, generation, member-order and sequence errors. Zero is healthy. The names are in the snapshot. |

A quiet market is not a fault. Once, the daemon also prints `upstream measured window started by readiness` (every connection healthy for 30 s in a row) or `... by timeout` (serve runs only, 90 s after start, when readiness has not held). Per-family counts and latencies in the report and snapshot cover events inside this window only. A run that exits 2 ends with `pmwsd upstream: <error>`, for a finished run `upstream not qualified: <reason>` (5.6).

### 5.4 The tape

`--tape` prints one JSON line to stdout for each admitted batch, which is one received message. Pipe it; do not redirect it to a file, because `text` holds source content and the daemon keeps no venue payload on disk.

```text
{"t_ns":<n>,"venue":"polymarket","stream":"polymarket-0","family":"price_change","market":"<condition-id>","events":<n>,"bytes":<n>,"handoff_ns":<n>,"generation":<n>,"text":"…"}
```

| Field | Meaning |
| --- | --- |
| `t_ns`, `handoff_ns` | Receive time in nanoseconds from process start, and nanoseconds from receive to typed handoff. |
| `venue`, `stream`, `generation` | `limitless` or `polymarket`; `<venue>-<index>`, one per connection; the connection generation, which changes on reconnect. |
| `family`, `market` | Family and venue-native market id of the batch's first event (slug or condition id, `null` if none). |
| `events`, `bytes` | Events in the batch and size of the source message. |
| `text` | The source message cut at 600 bytes, with `…` appended when cut. |

The tape thread owns stdout and queues at most 2048 lines; a full queue drops the new line and counts it as `tape_dropped` in the snapshot. Ingestion never waits. [`bench/tape_summary.py`](https://github.com/codebuster22/pm-ws-preview/blob/v2/bench/tape_summary.py) reads the tape on stdin and writes no file: a table per venue and family every `--interval` seconds (default 10), `--follow <market-id>` to echo one market's lines with text, `--peek <n>` to echo the first n lines unchanged.

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

`--snapshot <path>` rewrites a file every `--snapshot-seconds`, written to `<path>.tmp` and renamed so a reader sees a whole file. `--output` is written once, when the run ends. Both use the schema `pm-ws-native-upstream-v2`. A snapshot has `reason` `snapshot`, a `snapshot` block (`elapsed_ns`, `serve`, `interval_seconds`, `tape_dropped`, `window_opened_by`) and, per shard, a `covered` flag per target and the last 48 batches as identifiers, sizes and times, never event content. [`bench/snapshot_summary.py`](https://github.com/codebuster22/pm-ws-preview/blob/v2/bench/snapshot_summary.py) prints either file:

```sh
python3 bench/snapshot_summary.py "$RUN/snapshot.json"
python3 bench/snapshot_summary.py "$RUN/report.json"
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
| `measured window` | Its length in a final report, `not opened` if the window never opened. |
| `shard` line | One connection: latest `generation` and `connected`, `covered` targets with evidence over targets required, messages `received`, events `decoded`, and the fault and protocol-control counters as JSON maps. |
| family rows | Events inside the window, then p50, p99 and the maximum of receive-to-typed-handoff. Quantiles are histogram upper bounds in microseconds, 1 µs buckets below 1 ms and doubling above. The maximum is exact. |

### 5.6 Exit codes and reasons

Exit 0 is a serve run stopped by Ctrl-C or SIGTERM, or a measurement run that qualified. Everything else exits 2. Every run that gets as far as ending writes its report first, and the report's `reason` says why:

| Reason | Meaning |
| --- | --- |
| `operator_interrupt`, `operator_terminate` | Ctrl-C, SIGTERM. |
| `serve_max_seconds` | A serve run reached `--max-seconds`. |
| `selected_target_resolved` | A selected market resolved. A measurement run stops on it; a serve run keeps going and carries the reason. |
| `frozen_selection_changed` | The desired set changed through the control socket (5.7). |
| `healthy_floors_reached` | Measurement run: the window lasted `--min-seconds` and both venues reached `--min-events`. Qualified, exit 0. |
| `insufficient_samples`, `observed_fault`, `connection_health_lost`, `readiness_timeout` | Measurement run: the floors were not met, a fault was counted, readiness was lost after the window opened, or the window had not opened after 180 s. |

The floors need both venues; a one-venue selection ends as `insufficient_samples`. The [v2 README](https://github.com/codebuster22/pm-ws-preview/blob/v2/README.md) shows how to wrap a measurement run with `bench/run_upstream.py`.

### 5.7 Changing markets

Edit the selection file and start a new run with a new `RUN` directory. The daemon cannot add or remove markets while it runs: `--control-socket <path>` opens a Unix socket that answers `{"command":"status"}` with the selection `revision` and `desired` count, and any `add`, `remove`, `replace`, `lease` or `release` that changes the set ends the run with `frozen_selection_changed` and exit 2, even in a serve run. Keep the path short; Unix socket paths are limited to about 100 bytes.

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
| Scale workload | 2,500 targets (500 Limitless markets, 1,000 Polymarket conditions as 2,000 tokens) on 15 sockets | Apple M1, 8 CPUs, and 32-core Ubuntu 24.04 under WSL2 |
| Scale CPU | 0.03 of one core | recorded on both machines at this workload |
| Scale memory | about 70 MiB resident | 32-core Ubuntu 24.04 under WSL2 |

The v2 figures are upstream costs. They run from the socket read of a complete message to a typed, validated event at the handoff. They are not end-to-end consumer latency. v1 queue age and v2 handoff time are different stages. Figures are never compared across machines.


## 7. Venue behaviour

### 7.1 Limitless

Full contracts: [v2 docs/limitless.md](https://github.com/codebuster22/pm-ws-preview/blob/v2/docs/limitless.md) and [v1 docs/limitless.md](https://github.com/codebuster22/pm-ws-preview/blob/v1/docs/limitless.md).

- **Transport.** `wss://ws.limitless.exchange`, Socket.IO namespace `/markets`, WebSocket only. The server drives the Engine.IO heartbeat (about every 25 s in one observed connection); pm-ws answers its pings and sends none of its own. Both feeds are open and use no credential.
- **Subscription.** `subscribe_market_prices` takes CLOB market slugs in `marketSlugs`, and each call replaces the connection's whole set. The venue does not retain subscriptions across a reconnect. `subscribe_market_lifecycle` controls the lifecycle feed.
- **Acknowledgement.** The venue answers with a `system` event listing the markets it accepted, and pm-ws counts a subscription as confirmed only when that set matches the request. A complete `orderbookUpdate` snapshot then follows for each valid, unresolved market. In a run on 7 October 2026 the venue left some in-play match markets (the "vs" slugs) out of the acknowledgement and sent no error; `--serve` records the partial acknowledgement and keeps the socket, outside `--serve` it is a subscription fault.
- **Health.** A quiet market does not time out, lose its subscription or trigger a reconnect. The venue declares no per-market cadence.

| Family | Identity | Notes |
| --- | --- | --- |
| `orderbookUpdate` | `marketSlug` | Complete bid and ask arrays, bids highest first, asks lowest first, with `timestamp` and `version`. Prices and sizes are JSON numbers, decoded exactly. |
| `newPriceData` | `marketAddress` | Yes and No `updatedPrices`, `blockNumber`, `timestamp`. |
| `marketCreated`, `marketResolved` | `slug` | Lifecycle feed; `marketResolved` also reaches the market's own room. |
| `system`, `exception` | connection generation | Acknowledgements and errors, kept as source-control records. |
| `oraclePriceData` | none | Not documented by the venue. Kept as an unknown family under its own name. |

Every valid arrival is published, including repeats: in observed runs the same `marketResolved` arrived three times, byte-identical, within a second, and pm-ws published each. `version` is the venue's per-book publisher sequence, not contiguous and not comparable across connections; pm-ws keeps it as source data. v2 never uses it to drop or reorder; in v1 it is the key the optional replica pool (`--pool`, `replicas > 1`) gates on, and that pool is the only path that drops an arrival: an equal or lower `version` arriving on a second socket for the same market. pm-ws uses public REST only to select markets: `/markets/active` returns at most 25 entries a page, and in one observed sweep answered HTTP 403 to Python's default user agent.

### 7.2 Polymarket

Full contract: [v2 docs/polymarket.md](https://github.com/codebuster22/pm-ws-preview/blob/v2/docs/polymarket.md).

- **Transport.** `wss://ws-subscriptions-clob.polymarket.com/ws/market`, a plain WebSocket on the CLOB market channel, no authentication. pm-ws sends `PING` every 10 s; a written `PING` must get its `PONG` before the heartbeat deadline, and market data does not satisfy it.
- **Subscription.** By token ids with `assets_ids` and `type: "market"`; later `subscribe` and `unsubscribe` messages change the set. pm-ws requests `custom_feature_enabled` on every connection, which adds `best_bid_ask`, `new_market` and `market_resolved`. Both outcome tokens of a condition go on the same connection.
- **No acknowledgement.** The venue documents none, so coverage is shown only by data arriving for a token. A quiet token and an unconfirmed subscription look the same until data arrives.
- **First message.** The venue's guide says a `book` is emitted on subscription. In the 7 October 2026 run the first message on each socket was about 770 KB at 100 conditions a socket, and it produced the largest handoff times in that run. Some sockets also had failed handshakes and reads at start; each recovered on a later attempt.

| Family | Identity | Notes |
| --- | --- | --- |
| `book` | `market` + `asset_id` | Full aggregated book for one asset: bids, asks, `timestamp`, `hash`. |
| `price_change` | `market` | One `timestamp` and an ordered `price_changes` array; each entry has `asset_id`, price, size, side, hash, best bid and best ask. Validated and published whole, never split per asset. |
| `last_trade_price` | `market` + `asset_id` | Price, size, fee, side, timestamp, transaction hash. |
| `tick_size_change` | `market` + `asset_id` | Old and new tick size, timestamp. |
| `best_bid_ask` | `market` + `asset_id` | Needs `custom_feature_enabled`. Best bid, best ask, spread, timestamp. |
| `new_market` | `market` and asset ids | Needs `custom_feature_enabled`. Not scoped to the subscribed set: it reports every market created on the venue. Observed, not documented. |
| `market_resolved` | `market` and asset ids | Needs `custom_feature_enabled`. The complete resolution object. |

The venue declares no sequence number, timestamp ordering, hash meaning or cadence. Message pairs have been seen with the same timestamp, asset and hash list but different prices and sizes; pm-ws publishes both.

### 7.3 Limits pm-ws applies to itself

Neither venue documents a limit on connections, connection attempts, command rate or markets per subscription. These are pm-ws's own choices, applied to both venues; the ledger and pacer are in [src/etiquette.rs](https://github.com/codebuster22/pm-ws-preview/blob/v2/src/etiquette.rs).

| Limit | Value | Notes |
| --- | --- | --- |
| Rows per connection | 100 selection rows | v2. A Limitless row is one market, a Polymarket row one condition (two tokens). |
| Targets per process | 4,096 | v2. |
| Connections | initial envelope of 20 | Reduced on venue pushback. v1 also has an optional cap, `max_venue_connections`. |
| Connection attempts | 280 per rolling day | One process-wide ledger across both venues; a failed handshake spends one. v1 setting: `daily_connection_attempt_budget`. |
| Command spacing | one subscription-bearing command per 500 ms per endpoint | v1 setting: `min_command_interval_ms`. |
| Public REST | 60 requests per hour | Market selection only. Never ingestion or recovery. |
| Frame size | 1 MiB maximum | v2. |
| Admission | 64 MiB per shard | v2. A limit the gate refuses to exceed, not an allocation. |

Recovery after a loss: resubscribe on the existing connection when it is usable; otherwise reconnect, paced and spending one from the ledger, with a new generation whose fence rejects anything from the retired one. The gap is exposed as a continuity boundary and missed events are not replayed. REST recovery is off unless an operator opts in. Both selected feeds are open, so neither preview captures a credential; the venue contracts state the rule for a keyed feed (`credential_rejected` on an authentication challenge, HTTP 401 or 403), which neither preview implements.

## 8. Limits and what is not built

**v1**

- Limitless is the only venue. It rebuilds books from the venue's snapshots and updates, normalizes no economics and places no orders.
- Consumers read shared memory on the same host, as the same OS user as the daemon. There is no network API, and the bindings are not installable packages.
- On a gap, the daemon resubscribes or reconnects and readers are told: `PmwsContinuityLost` in Python, error code `PMWS_CONTINUITY_LOST` in TypeScript, `continuity_loss` and `fenced` lines in `pmws-run`.
- `pmwsd` prints one start line and one summary line per shard when it stops, nothing per event. `pmws-run` follows one market per run.
- The metrics endpoint serves `GET /metrics` over plain HTTP on the one numeric address in `metrics_listen`, and does not exist when the key is absent.
- The four seeded markets run until 31 December 2026. After that date, or if one resolves early, replace them in `config/pmwsd.toml.in` and run `./setup.sh` again.

**v2**

- There is no local transport, no consumer API and no Python or TypeScript binding. The rail ends in a metrics-only in-process receiver. It builds no order book, derives no diffs, normalizes no economics and makes no trading decisions.
- The tape is a display aid, not a transport: `text` is cut at 600 bytes, `family` and `market` describe the first event of a batch, and a full stdout queue drops lines rather than block ingestion.
- A selection change needs a restart, and a `--serve` run needs every row to end in the future. The seeded Limitless markets run until 31 December 2026; the sports files carry their own `valid_until`, and `discover_sports.py --prune` drops expired rows.
- Exit 2 at `--max-seconds` is normal for a serve run; the report is written before the daemon exits, and `--output` must not exist beforehand. A SIGKILL leaves no report, only the last snapshot.
- Faults appear as a count in the status line, with no line per fault. Byte counts exist per batch, not per family.
- `bench/run_upstream.py` and `bench/report_upstream.py` validate only the 100 Limitless plus 100 Polymarket workload on two connections, and `run_upstream.py` never passes `--serve`, `--snapshot` or `--tape`.

Both versions are previews with no stability promise.

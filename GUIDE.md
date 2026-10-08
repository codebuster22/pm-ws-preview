# pm-ws preview guide

This guide walks through the two preview builds in this repository: v1, a Limitless order-book daemon with same-host Python, TypeScript and Rust consumers, and v2, a native-event rail for Limitless and Polymarket with no consumer yet. It shows how to install each build, run it, and read what it prints. Every command runs from a clone of the branch it belongs to. Output marked with a date is pasted from a live run on that date; everything else shows the format a run prints.

- [Overview](#overview)
- [Installation](#installation)
- [v1: the order-book daemon](#v1-the-order-book-daemon)
  - [Starting the daemon](#starting-the-daemon)
  - [Controlling the daemon with pmwsctl](#controlling-the-daemon-with-pmwsctl)
  - [Metrics](#metrics)
  - [Watching one book with pmws-run](#watching-one-book-with-pmws-run)
  - [Reading the book from Python and TypeScript](#reading-the-book-from-python-and-typescript)
  - [Writing your own consumer](#writing-your-own-consumer)
- [v2: the native-event rail](#v2-the-native-event-rail)
  - [Writing a selection](#writing-a-selection)
  - [Serving a selection](#serving-a-selection)
  - [The status line](#the-status-line)
  - [The tape](#the-tape)
  - [Snapshots and the report](#snapshots-and-the-report)
  - [Exit codes and reasons](#exit-codes-and-reasons)

## Overview

pm-ws is a Rust daemon that reads prediction-market WebSocket feeds. It decodes every price and quantity exactly, into scaled integers rather than floats. Each market has exactly one current WebSocket owner, and one socket carries many markets. The daemon places no orders and makes no trading decisions.

The two builds go different distances along the path from socket to reader:

| | v1 | v2 |
| --- | --- | --- |
| Venues | Limitless | Limitless and Polymarket |
| Output | Order books and level mutations, published through shared memory | Typed native events, taken by an in-process receiver that counts and times them. `--tape` prints a view on stdout |
| Who can read it | Python, TypeScript and Rust consumers on the same host | No process outside the daemon. You read the tape, the snapshot and the report |
| Binaries | `pmwsd`, `pmwsctl`, `pmws-run` | `pmwsd upstream` |
| Configuration | `pmwsd.toml`, rendered by `setup.sh` from `config/pmwsd.toml.in` | One selection JSON file per run, passed with `--selection` |
| Changing markets | `pmwsctl add` and `pmwsctl remove` while the daemon runs | Edit the selection file and restart |
| Monitoring | `pmwsctl status` and Prometheus metrics | A status line on stderr every 10 s, a snapshot file, a final report |

Each branch's README lists what that build does and does not do. The venue contracts, with every event family, are in the v2 tree: [docs/limitless.md](https://github.com/codebuster22/pm-ws-preview/blob/v2/docs/limitless.md) and [docs/polymarket.md](https://github.com/codebuster22/pm-ws-preview/blob/v2/docs/polymarket.md).

A few terms recur below. They follow the [glossary in the v2 tree](https://github.com/codebuster22/pm-ws-preview/blob/v2/CONTEXT.md).

- A **target** is the subscription coordinate pm-ws acts on: a venue, a market, and an asset where the venue requires one. A Polymarket condition counts as two targets. v1 speaks of markets.
- A **shard** is one venue's unit of publication ownership. It owns admission for its markets and publishes one ordered stream. Both builds run 100 markets to a shard.
- A **connection generation** labels one socket assignment. A reconnect advances it, and a batch from a retired generation is rejected before admission.
- The **handoff** is the point where an admitted batch leaves the daemon's upstream half. The v2 tape's `handoff_ns` is the time from receipt to that point.
- **Coverage** is the evidence that a target is carried by its owning connection: the venue's acknowledgement for Limitless, observed data for Polymarket.

## Installation

You need:

- Linux or macOS. On Windows, use WSL2.
- rustup. The toolchain pinned in each tree's `rust-toolchain.toml` (Rust 1.98.0) installs itself on the first build.
- A C compiler (`cc`).
- Python 3.12 or newer.
- Node 22.18 or newer, for the v1 TypeScript consumer only.
- curl, for refreshing the v2 selections only.

Clone the branch you want. Each clone is self-contained:

```
$ git clone --branch v1 --depth 1 https://github.com/codebuster22/pm-ws-preview.git pm-ws-v1
$ git clone --branch v2 --depth 1 https://github.com/codebuster22/pm-ws-preview.git pm-ws-v2
```

Tarballs of the same trees are at <https://github.com/codebuster22/pm-ws-preview/archive/refs/tags/v1.tar.gz> and <https://github.com/codebuster22/pm-ws-preview/archive/refs/tags/v2.tar.gz>.

In each clone, build the release binaries, then run the setup script:

```
$ cargo build --release --locked
$ ./setup.sh
```

`setup.sh` takes a few seconds. It runs no build, no tests and no network call, and it stops with a message if rustup, cargo, `cc` or `python3` is missing. A missing curl (v2) or a missing or old Node (v1) is only a warning.

In the v1 clone, `setup.sh` creates `run/`, renders `pmwsd.toml` (four seeded markets) and `pmwsd.empty.toml` (no markets) from the templates in `config/` with the clone's absolute path and, once the three binaries are built, loads the Python binding and, when `node` is on PATH, the Node binding, then prints the commands to run next. Run it again after moving the clone. In the v2 clone, it creates `runs/` and, once `pmwsd` is built, checks that `pmwsd` with no arguments exits 2, runs the self-tests of five `bench/` scripts (`discover_sports.py`, `tape_summary.py`, `snapshot_summary.py`, `run_upstream.py`, `report_upstream.py`), and prints the serve recipe.

Everything a build writes stays inside its clone:

| Path in the clone | Written by | Content |
| --- | --- | --- |
| `target/` | `cargo build` | Build output, both trees |
| `pmwsd.toml`, `pmwsd.empty.toml` | v1 `setup.sh` | Rendered configuration |
| `run/` | v1 `setup.sh`, then `pmwsd` | Control socket `run/pmwsd.sock`, its `.lock` file, and the shared-memory segments `pmws-<instance>-<shard>.seg`, which `pmwsd` removes on a clean stop |
| `runs/` | v2 `setup.sh` creates it | The files you name with `--output` and `--snapshot` |

There is one exception. If `<clone>/run/pmwsd.sock` would be longer than 100 bytes, the limit on Unix socket paths, v1 `setup.sh` puts the control socket and its `.lock` file at `/tmp/pmwsd-v1.sock` and says so.

No command in this guide writes a venue payload to disk. Reports and snapshots hold counters, histograms and the consumed selection, never event content, and the v2 tape goes to stdout only. v1's `pmws-run` has a `--record <path>` flag that appends every inbound frame to a file; this guide does not use it.

## v1: the order-book daemon

v1 subscribes to Limitless markets, rebuilds each order book from the venue's snapshots and updates, and publishes the books and their level changes through shared-memory segments that consumers on the same host attach to.

`setup.sh` renders `pmwsd.toml` and `pmwsd.empty.toml` from [config/pmwsd.toml.in](https://github.com/codebuster22/pm-ws-preview/blob/v1/config/pmwsd.toml.in) and [config/pmwsd.empty.toml.in](https://github.com/codebuster22/pm-ws-preview/blob/v1/config/pmwsd.empty.toml.in). To change the markets, edit a template and run `./setup.sh` again. Both files set `metrics_listen = "127.0.0.1:9090"` and `markets_per_shard = 100`, and write the control socket `run/pmwsd.sock` and the delivery directory `run/` as absolute paths. The seeded markets run until 31 December 2026.

The other keys are `markets` (the pinned slugs), `endpoint` (the venue URL), `replicas` (connections per shard, 1 to 4), `min_command_interval_ms` (500, the gap between subscription commands), `daily_connection_attempt_budget` (280, shared by all shards), `max_venue_connections` (an optional cap), `lease_ttl_ms`, `max_control_sessions` and `[delivery] profile`. They are defined in [src/daemon.rs](https://github.com/codebuster22/pm-ws-preview/blob/v1/src/daemon.rs). An unknown key is refused, and `pmwsd` exits 2.

### Starting the daemon

Start the daemon with the seeded configuration:

```
$ ./target/release/pmwsd --config pmwsd.toml
```

It runs in the foreground and prints one line once every shard and its segment exist. After that it prints nothing until it stops. Here is the line from a run on 9 October 2026, with the home directory elided:

```
pmwsd pid=46848 shards=1 markets=4 socket=/…/pm-ws-v1/run/pmwsd.sock metrics=127.0.0.1:9090
```

Stop it with Ctrl-C or `SIGTERM`. It prints one line per shard and exits 0. The same run after about two minutes, during which the connection dropped twice:

```
shard 0: connections=3 subscriptions=3 snapshots=13 resolutions=0 losses=8 unrouted=0 queue_age_max_us=228 queue_age_p99_us=228
```

Exit code 1 means startup failed after the configuration loaded, for example because another daemon holds the socket or a segment cannot be created. Exit code 2 is a bad command line or configuration. Exit code 3 means a shard stopped; stderr names it.

To start with no markets and add them by hand, use `pmwsd.empty.toml` instead.

### Controlling the daemon with pmwsctl

While the daemon runs, use `pmwsctl` from a second terminal. Without `--socket` it looks for `/tmp/pmwsd.sock`, so pass the path that `setup.sh` printed:

```
$ ./target/release/pmwsctl --socket run/pmwsd.sock status
$ ./target/release/pmwsctl --socket run/pmwsd.sock add <slug>...
$ ./target/release/pmwsctl --socket run/pmwsd.sock remove <slug>...
```

Every answer is pretty-printed JSON on stdout. Errors go to stderr as `pmwsctl: <message>`. Exit code 0 is success; 1 means the daemon could not be reached or answered with an error; 2 is a bad command line; 3 means at least one market was rejected; 4 means a bounded queue was full and nothing was applied, so retry.

`add` and `remove` change the pinned market set while the daemon runs. Each prints one object per slug, with a `status` of `accepted`, `reconciling`, `live`, `removed`, `{"rejected": "<reason>"}` or `{"stale": "<reason>"}`. Here a fifth market is added to the running daemon and then removed. Four seconds after the `add`, `status` showed it `established` and `live`:

```json
[ { "slug": "opensea-fdv-above-dollar3b-one-day-after-launch-1764857000239", "status": "accepted" } ]
[ { "slug": "opensea-fdv-above-dollar3b-one-day-after-launch-1764857000239", "status": "removed" } ]
```

`status` returns the daemon's `pid`, `rss_kib` and `metrics_listen`, then one entry per shard and one per market. A market's `subscription` is `desired`, `subscribing`, `established` or `removing`. This is the answer from the same run, on an Apple M1 with 8 CPUs, trimmed to one shard and one market:

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

### Metrics

Both rendered configurations set `metrics_listen`, so the daemon serves Prometheus metrics:

```
$ curl 127.0.0.1:9090/metrics
```

The endpoint answers `GET /metrics` only and has no authentication, so keep it on a loopback address. If port 9090 is taken, change `metrics_listen` in the template and run `./setup.sh` again. Per-shard metrics carry a `shard` label. The ones worth watching:

| Metric | Meaning |
| --- | --- |
| `pmws_shard_connected` | 1 when the shard's publishing connection is established with its subscription on the wire |
| `pmws_shard_frames_seen` | Venue frames received |
| `pmws_shard_continuity_losses` | Times a book lost continuity |
| `pmws_shard_queue_age_p99_micros` | 99th-percentile sampled ingest queue age in microseconds, as a bucket upper bound |
| `pmws_markets` | Markets the daemon holds, across every shard. The per-shard count is `pmws_shard_markets` |

Six lines from the same run, about two minutes in:

```
pmws_shard_connected{shard="0"} 1
pmws_shard_frames_seen{shard="0"} 15
pmws_shard_continuity_losses{shard="0"} 4
pmws_shard_queue_age_p99_micros{shard="0"} 216
pmws_shard_markets{shard="0"} 5
pmws_markets 5
```

### Watching one book with pmws-run

`pmws-run` opens one WebSocket to Limitless for one market, rebuilds its book and prints what it sees. It needs no daemon and writes no file unless given `--shm <path>`, which creates a one-market segment file at that path, or `--record <path>`. Pass a Limitless market slug, for example one of the four seeds in `config/pmwsd.toml.in`:

```
$ ./target/release/pmws-run --market <slug> --seconds 60 --print-book
```

`--seconds` defaults to 60 and accepts up to 86400. Without `--print-book` you get the connection and event lines and the summary; `--print-book` adds the `book`, `mutation`, `resolution` and `summary book` lines. The run exits 0 when it reaches `--seconds`, 1 if it fails, 2 on a bad command line.

This is a complete 60-second run on 9 October 2026 against the seeded Greenland market. About half a minute in, the connection dropped; the run reconnected, resubscribed and opened continuity epoch 1:

```
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

Reading from the top:

- `connected` prints after each successful connection. `generation` numbers the connection; the `sid`, ping and payload fields are the handshake values the venue returned.
- `orderbookUpdate` is one venue frame. `bids` and `asks` count its levels, and `best_bid` and `best_ask` are the first level the venue listed on each side, or `-` when a side is empty. Prices and quantities are exact decimal text.
- `book` prints once at start and again after each new revision. `authority` is one of `Unsubscribed`, `Subscribing`, `Synchronizing`, `Live`, `Recovering` and `Stale(<reason>)`. `continuity_epoch` names a stretch of unbroken history. The canonical lists show the book as the venue reports it, at most three levels a side, best first; the derived complement is computed locally.
- `mutation revision=<R> epoch=<E> position=<P> side=<Bid|Ask> price=<price> qty=<old>-><new>` prints for each level change, and `resolution` prints when the venue reports the market resolved. A resolution does not change the book. Neither line occurred in this run.
- `continuity_loss`, `reconnecting`, `resubscribing`, `fenced` and the `source` lines print only when something happens. `fenced` means events from a retired connection generation were rejected before they reached the book.
- The `summary` lines count frames, connection attempts, fences, events by kind, drops and decode failures. `summary book` counts snapshots applied, mutations derived and continuity losses.

The source for every line is [src/main.rs](https://github.com/codebuster22/pm-ws-preview/blob/v1/src/main.rs).

### Reading the book from Python and TypeScript

[examples/bbo.py](https://github.com/codebuster22/pm-ws-preview/blob/v1/examples/bbo.py) and [examples/bbo.ts](https://github.com/codebuster22/pm-ws-preview/blob/v1/examples/bbo.ts) attach to a segment that a running `pmwsd` publishes and print the best bid and ask. They must run on the same host, as the same user as the daemon, from a clone that has been built. They attach read-only. Node runs the `.ts` file directly, with no build step.

Start the daemon in one terminal, then list `run/` for the segment name, `pmws-<instance>-<shard>.seg`. Run a consumer against it with a market from `pmwsd.toml`:

```
$ python3 examples/bbo.py --segment run/pmws-<instance>-0.seg --market <slug> --events
$ node examples/bbo.ts --segment run/pmws-<instance>-0.seg --market <slug>
```

Both take the same flags:

| Flag | Default | Meaning |
| --- | --- | --- |
| `--segment <path>` | required | The segment file |
| `--market <slug>` | required | The market's venue-native key. It must be in the daemon's set and in this segment |
| `--venue <name>`, `--kind <kind>` | `limitless`, `slug` | Venue and key kind |
| `--seconds <n>` | `10` | Run time, at most 86400 |
| `--events` | off | Also print `mutation`, `resolution` and continuity lines |

Both print the same line shapes. The first `bbo` line prints after attach, and another each time the book revision changes. `mutation` and `resolution` print only with `--events`. With `--events`, if the mutation stream loses continuity, the consumer prints `continuity_lost reason=<reason>` and `reattach revision=<R>`, then goes on:

```
bbo revision=<R> authority=<authority> best_bid=<price>@<qty> best_ask=<price>@<qty>
mutation revision=<R> cursor=<epoch>:<position> origin=<origin> side=<side> price=<price> qty=<old>-><new>
resolution revision=<R> cursor=<epoch>:<position> origin=<origin> outcome=<outcome> index=<index> type=<type> date=<date> path=<path>
```

A missing side prints `-`. `none` in `qty` means the level is absent on that side of the change. If the market never appears in the segment before `--seconds` ends, the consumer prints `error: market not installed in the segment` and exits 1. A normal run exits 0.

Both consumers against the daemon started above, on the Greenland seed. No mutation arrived in their windows, so each printed only its opening line:

```
$ python3 examples/bbo.py --segment run/pmws-e6baeb9ae9b6c688fdad6336bc7397c6-0.seg --market will-trump-acquire-greenland-before-2027-1768930762585 --seconds 20 --events
bbo revision=1 authority=Live best_bid=0.003@863898323 best_ask=0.899@12362000
$ node examples/bbo.ts --segment run/pmws-e6baeb9ae9b6c688fdad6336bc7397c6-0.seg --market will-trump-acquire-greenland-before-2027-1768930762585 --seconds 10
bbo revision=1 authority=Live best_bid=0.003@863898323 best_ask=0.899@12362000
```

### Writing your own consumer

Start from `bbo.py` or `bbo.ts` and keep your copy inside `examples/`: both files find the binding by a path relative to their own location, so a copy in the clone root cannot import it. The bindings are [bindings/python/pmws.py](https://github.com/codebuster22/pm-ws-preview/blob/v1/bindings/python/pmws.py) and [bindings/node/pmws.ts](https://github.com/codebuster22/pm-ws-preview/blob/v1/bindings/node/pmws.ts). The calls you need, in their Python names (TypeScript uses camelCase, and `bigint` for revisions, cursors and generations):

- `pmws.Segment(path)` opens a segment read-only. `segment.resolve(venue, kind, key)` returns a `Market`, and `market.attach()` returns `(state, stream)`.
- `pmws.Segment.connect(control_socket, market)` opens the segment that serves one market through the daemon's control socket and holds a lease on it. `segment.close()` or process exit releases the lease.
- `state.revision`, `state.authority`, `state.levels` (every level, each with `side`, `price` and `quantity`) and `state.best(side)`, which returns a `Level` or `None`.
- `stream.next_event()` returns a `Mutation` (`revision`, `side`, `price`, `old_quantity`, `new_quantity`, `origin`), a `Resolution` (`revision`, `winning_index`, `winning_outcome`, `market_type`, `resolution_date`, the last three being the venue's own text) or `None`.
- `segment.wait(last_generation, spin_micros=0, timeout_ms=None)` parks until the daemon publishes. `segment.publication_generation()` gives the value to pass it.
- Every price and quantity is an `ExactDecimal` with `coefficient`, `scale` and `text`. The value is `coefficient` divided by 10 to the power `scale`. Print `text`; do not convert to a float.

Six rules apply to any consumer:

- **Same user, read-only.** Run the consumer as the user that runs `pmwsd`; `Segment.connect` is refused otherwise. Neither binding has a call that writes to the segment.
- **`connect` takes a lease.** A market that nothing else holds or pins is subscribed at the venue to serve it. Neither binding can pin a market; pinning is `pmwsctl add`.
- **Continuity.** `next_event()` raises `PmwsContinuityLost`, with a `reason` of `Overrun`, `Gap`, `LocalLoss`, `Reconnect`, `RecoveryBase` or `SyncDivergence`, when the stream can no longer prove it delivered every mutation in order. It keeps raising until you call `reattach()`. Treat the state that `reattach()` returns as the book and discard what you built from earlier mutations.
- **A slow consumer loses mutations.** The daemon never waits for a consumer. Retained mutations per market are bounded, and a consumer that falls behind gets `Overrun`. Ingestion never slows.
- **Retry transient reads.** `attach()`, `read_state()` and `next_event()` can raise `PMWS_STATUS_CONTENDED`, `PMWS_STATUS_WRITER_STALLED` or `PMWS_STATUS_NO_PUBLISHED_STATE`. Retry the call, as `retry_transient` in `bbo.py` and `isTransient` in the TypeScript binding do.
- **`PMWS_LIB`.** Both bindings load the library built from the same tree, from `target/release` then `target/debug` inside the clone, or from the path in this environment variable. They check FFI version 8 and ABI version 5 and refuse anything else.

## v2: the native-event rail

v2 subscribes to Limitless and Polymarket, decodes the events on the selected feeds into typed events, and hands them to an in-process receiver that counts and times them. It builds no book and has no consumer API. You watch a run through its status line, its tape, its snapshots and its final report.

### Writing a selection

The daemon discovers no markets. A selection file names them, and the run keeps exactly that set:

```
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

The daemon ignores every other field. The seeded files carry `title`, `game_start`, `tag`, `generated_at`, `valid_until` and `note` for people to read. The daemon checks these rules when the file loads, and a violation exits 2 before any connection opens:

| Rule | Detail |
| --- | --- |
| Keys | `limitless` and `polymarket` must both be present. Either list may be empty, but not both |
| Targets | At most 4096, and the file at most 4 MiB. A Limitless row counts one, a Polymarket condition two |
| Connections | Rows go 100 to a connection, per venue, in file order: up to 100 Limitless markets, or 100 conditions (200 tokens) |
| Identifiers | Non-empty, at most 1024 bytes, never repeated. A condition has exactly two token ids |
| `end_epoch` | Required, in whole Unix seconds. With `--serve` it must be later than now, and one expired row blocks the run. For a measurement run it must be at least now plus `--max-seconds` plus 210 s |

Three selections ship with the tree:

| File | Contents | Valid until |
| --- | --- | --- |
| [selections/limitless.json](https://github.com/codebuster22/pm-ws-preview/blob/v2/selections/limitless.json) | Four long-dated Limitless markets | 31 December 2026 |
| [selections/nfl-ncaa.json](https://github.com/codebuster22/pm-ws-preview/blob/v2/selections/nfl-ncaa.json) | Polymarket NFL and college-football conditions | The `valid_until` header in the file |
| [selections/mixed.json](https://github.com/codebuster22/pm-ws-preview/blob/v2/selections/mixed.json) | Both of the above | The `valid_until` header in the file |

`valid_until` is the earliest market end in the file. Market ends are the venue's end dates, not kickoff times.

To rebuild the Polymarket files, run [bench/discover_sports.py](https://github.com/codebuster22/pm-ws-preview/blob/v2/bench/discover_sports.py). It fetches `GET https://gamma-api.polymarket.com/events` through curl, one tag at a time (`nfl` and `cfb` by default), newest listings first, keeps at most four markets per event, and ranks upcoming games first. It writes descriptors only. The venue allows about 60 requests an hour; the script makes at most three per tag, half a second apart, and stops after three pushbacks.

```
$ python3 bench/discover_sports.py --output selections/nfl-ncaa.json --min-days 10 --max-conditions 200
$ python3 bench/discover_sports.py --output selections/mixed.json --min-days 10 --merge-limitless selections/limitless.json
$ python3 bench/discover_sports.py --prune selections/nfl-ncaa.json --output selections/nfl-ncaa.json
```

`--min-days` keeps markets that end at least that many days out (default 5). `--per-event` caps markets per event (default 4). `--max-conditions` caps the conditions kept, each worth two targets (default 200). `--tag` is repeatable. `--prune` copies a selection without the rows that end within an hour and makes no network request.

### Serving a selection

A serve run keeps a selection subscribed for as long as you let it and writes a snapshot while it goes. This is the recipe `setup.sh` prints:

```
$ RUN=runs/$(date +%Y%m%d-%H%M%S); mkdir -p "$RUN"
$ ./target/release/pmwsd upstream --selection selections/nfl-ncaa.json --output "$RUN/report.json" \
    --serve --snapshot "$RUN/snapshot.json" --snapshot-seconds 5 --workers 2 \
    --min-seconds 1 --min-events 1 --max-seconds 3600 --tape | python3 bench/tape_summary.py
```

| Flag | Effect |
| --- | --- |
| `--selection <path>` | The selection file |
| `--output <path>` | The final report. The path must not exist, so use a new `RUN` directory each time |
| `--serve` | A long-running run. A run without this flag is a measurement run, which must meet the floors below to qualify and stops on the first fault. With `--serve`, rows only need to be unexpired, a failed connection is retried after five seconds, and faults, resolved markets and lost health do not end the run |
| `--snapshot <path>`, `--snapshot-seconds <n>` | A live summary file, rewritten every n seconds (1 to 60, default 2) |
| `--workers <n>` | Worker threads that own the connections. 1 to 8, default 2 |
| `--min-seconds`, `--min-events` | Floors for a measurement run. `--serve` waives the 900-second and 10,000-event minimums; 1 and 1 are the lowest values accepted |
| `--max-seconds <n>` | A serve run ends this many seconds after the process starts; a measurement run this many seconds after the measured window opens. At most 14400, and never below `--min-seconds` |
| `--tape` | One JSON line per admitted batch on stdout |

`--diagnostic`, `--cpu-timing` and `--control-socket <path>` also exist. Run the binary with no arguments to see the usage; it exits 2.

Stop the run with Ctrl-C or `SIGTERM`. A serve run writes the report and exits 0; a measurement run stopped this way writes its report and exits 2. If a serve run reaches `--max-seconds` first, it writes the report and exits 2, which is normal for a serve run. A `SIGKILL` leaves no report, only the last snapshot.

To change the markets, edit the selection and start a new run with a new `RUN` directory. The daemon cannot add or remove markets while it runs. `--control-socket <path>` opens a Unix socket that answers `{"command":"status"}` with the selection `revision` and `desired` count, but any `add`, `remove`, `replace`, `lease` or `release` that changes the set ends the run with the reason `frozen_selection_changed` and exit 2, even in a serve run. Keep the path short; Unix socket paths are limited to about 100 bytes.

### The status line

Every ten seconds the daemon prints one line on stderr. The tape goes to stdout, so the two do not mix:

```
upstream elapsed=<n>s evidence=<a>/<b> healthy_connections=<c>/<d> activity=[<l>, <p>] faults=<n>
```

| Field | Meaning |
| --- | --- |
| `evidence=<a>/<b>` | Targets with subscription evidence, out of all selected. For Limitless the venue acknowledged the market; for Polymarket a data message arrived for the token |
| `healthy_connections=<c>/<d>` | Connections that are up, subscribed and have shown a heartbeat. A Limitless connection must also have all its targets acknowledged. In a serve run, an acknowledgement for only some of its markets still counts |
| `activity=[<l>, <p>]` | Events since start: Limitless `orderbookUpdate`, and Polymarket `book` plus `price_change` |
| `faults` | One total of the fault counters: connection faults such as `connect_failed`, malformed messages, routing rejections, admission overloads, and generation, member-order and sequence errors. Zero means no fault of any kind; a failed connection attempt counts one, as in the report under Snapshots. The names are in the snapshot |

A quiet market is not a fault. Once per run the daemon also prints `upstream measured window started by readiness`, when every connection has been healthy for 30 s in a row, or `upstream measured window started by timeout`, in serve runs only, 90 s after start when readiness has not held. Per-family counts and latencies in the report and the snapshot cover events inside this window only. A run that exits 2 ends with `pmwsd upstream: <error>`; for a finished run the error reads `upstream not qualified: <reason>`.

### The tape

`--tape` prints one JSON line to stdout for each admitted batch, which is one received message. Pipe it into a viewer. Do not redirect it to a file: `text` holds source content, and the daemon keeps no venue payload on disk.

```
{"t_ns":<n>,"venue":"polymarket","stream":"polymarket-0","family":"price_change","market":"<condition-id>","events":<n>,"bytes":<n>,"handoff_ns":<n>,"generation":<n>,"text":"…"}
```

| Field | Meaning |
| --- | --- |
| `t_ns`, `handoff_ns` | Receive time in nanoseconds from process start, and nanoseconds from receive to typed handoff |
| `venue`, `stream`, `generation` | `limitless` or `polymarket`; `<venue>-<index>`, one per connection; the connection generation, which changes on reconnect |
| `family`, `market` | Family and venue-native market id of the batch's first event: a slug or a condition id, `null` if the event carries none |
| `events`, `bytes` | Events in the batch, and the size of the source message |
| `text` | The source message cut at 600 bytes, with `…` appended when cut |

The tape thread owns stdout and queues at most 2048 lines. When the queue is full, the new line is dropped and counted as `tape_dropped` in the snapshot. Ingestion never waits.

[bench/tape_summary.py](https://github.com/codebuster22/pm-ws-preview/blob/v2/bench/tape_summary.py) reads the tape on stdin and writes no file. Every `--interval` seconds (default 10) it prints a `tape last <n>s` table and a `tape since start` table, one row per venue and family. A table prints only when the next line arrives, so a silent feed prints nothing. `--follow <market-id>` echoes one market's lines with their text, and `--peek <n>` echoes the first n lines unchanged.

Here are the last 10-second table and the end-of-input table from five minutes on 9 October 2026 with `selections/mixed.json`, on an Apple M1 with 8 CPUs: one Limitless socket with 4 markets and two Polymarket sockets with 100 conditions each.

```
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

### Snapshots and the report

`--snapshot <path>` rewrites a file every `--snapshot-seconds`. The daemon writes `<path>.tmp` and renames it, so a reader always sees a whole file. `--output` is written once, when the run ends. Both use the schema `pm-ws-native-upstream-v2`. A snapshot has the `reason` `snapshot`, a `snapshot` block (`elapsed_ns`, `serve`, `interval_seconds`, `tape_dropped`, `window_opened_by`) and, per shard, a `covered` flag per target and the last 48 batches as identifiers, sizes and times, never event content.

[bench/snapshot_summary.py](https://github.com/codebuster22/pm-ws-preview/blob/v2/bench/snapshot_summary.py) prints either file:

```
$ python3 bench/snapshot_summary.py "$RUN/snapshot.json"
$ python3 bench/snapshot_summary.py "$RUN/report.json"
```

This is the report of the five-minute run above. It ended at `--max-seconds`, so its reason is `serve_max_seconds`. One Polymarket socket logged three failed connection attempts before it stayed up:

```
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
| `reason`, `qualified` | Why the file was written. `qualified` is `true` only for a measurement run that met its floors |
| `measured window` | Its length in a final report, or `not opened` if the window never opened |
| `shard` line | One connection: its latest `generation` and `connected` state, `covered` targets with evidence over targets required, messages `received`, events `decoded`, and the fault and protocol-control counters as JSON maps |
| family rows | Events inside the window, then the p50, p99 and maximum of receive-to-typed-handoff. Quantiles are histogram upper bounds in microseconds, in 1 µs buckets below 1 ms and doubling above. The maximum is exact |

The figures are upstream costs, from the socket read of a complete message to a typed, validated event at the handoff, not end-to-end consumer latency. A full measurement report from an Apple M1, 100 Limitless markets and 100 Polymarket conditions over a 4619 s measured window, is in [bench/reports/native-upstream-flat-packed-100-m1.md](https://github.com/codebuster22/pm-ws-preview/blob/v2/bench/reports/native-upstream-flat-packed-100-m1.md).

### Exit codes and reasons

Exit code 0 means a serve run stopped by Ctrl-C or `SIGTERM`, or a measurement run that qualified. Everything else exits 2. Every run that gets as far as ending writes its report first, and the report's `reason` says why it ended:

| Reason | Meaning |
| --- | --- |
| `operator_interrupt`, `operator_terminate` | Ctrl-C, `SIGTERM` |
| `serve_max_seconds` | A serve run reached `--max-seconds` |
| `selected_target_resolved` | A selected market resolved. A measurement run stops on it; a serve run keeps going and carries the reason |
| `frozen_selection_changed` | The desired set changed through the control socket, or the control channel closed |
| `healthy_floors_reached` | Measurement run: the window lasted `--min-seconds` and both venues reached `--min-events`. Qualified, exit 0 |
| `insufficient_samples`, `observed_fault`, `connection_health_lost`, `readiness_timeout` | Measurement run: the floors were not met, a fault was counted, readiness was lost after the window opened, or the window had not opened after 180 s |

The floors need both venues; a one-venue selection ends as `insufficient_samples`. The [v2 README](https://github.com/codebuster22/pm-ws-preview/blob/v2/README.md) shows how to wrap a measurement run with `bench/run_upstream.py`.

# pm-ws preview guide

This guide covers the two preview builds in this repository: v1, a Limitless order-book daemon with same-host Python, TypeScript and Rust consumers, and v2, a native-event rail for Limitless and Polymarket with no consumer yet. Every command runs from a clone of the branch it belongs to. Output marked with a date is pasted from a live run on that date; everything else shows the shape a run prints. The fastest path is Installation, then the first three v1 sections, then the first four v2 sections. The rest is reference.

- [Overview](#overview)
- [Terms](#terms)
- [Installation](#installation)
- [v1: the order-book daemon](#v1-the-order-book-daemon)
  - [A first look at a book](#a-first-look-at-a-book)
  - [Starting the daemon](#starting-the-daemon)
  - [Reading the book from Python and TypeScript](#reading-the-book-from-python-and-typescript)
  - [Controlling the daemon with pmwsctl](#controlling-the-daemon-with-pmwsctl)
  - [Metrics](#metrics)
  - [Writing your own consumer](#writing-your-own-consumer)
- [v2: the native-event rail](#v2-the-native-event-rail)
  - [Serving a selection](#serving-a-selection)
  - [The status line](#the-status-line)
  - [The tape](#the-tape)
  - [Snapshots and the report](#snapshots-and-the-report)
  - [Writing a selection](#writing-a-selection)
  - [Measurement runs](#measurement-runs)

## Overview

pm-ws is a Rust daemon that reads prediction-market WebSocket feeds. It decodes every price and quantity exactly, into scaled integers rather than floats. Each market has exactly one current WebSocket owner, and one socket carries many markets. The daemon places no orders and makes no trading decisions.

The two builds go different distances along the path from socket to reader:

| | v1 | v2 |
| --- | --- | --- |
| Venues | Limitless | Limitless and Polymarket |
| Output | Order books and level mutations, published through shared memory | Typed native events, taken by an in-process receiver that counts and times them. A status line on stderr reports coverage and faults, and `--tape` prints one JSON line per received message on stdout |
| Who can read it | Python, TypeScript and Rust consumers on the same host | No process outside the daemon. You read the tape, the snapshot and the report |
| Binaries | `pmwsd`, `pmwsctl`, `pmws-run` | `pmwsd upstream` |
| Configuration | `pmwsd.toml`, rendered by `setup.sh` from `config/pmwsd.toml.in` | One selection JSON file per run, passed with `--selection` |

The venue contracts, with every event family, are in the v2 tree: [docs/limitless.md](https://github.com/codebuster22/pm-ws-preview/blob/v2/docs/limitless.md) and [docs/polymarket.md](https://github.com/codebuster22/pm-ws-preview/blob/v2/docs/polymarket.md).

## Terms

These are the words the commands and outputs below use.

- A **venue** is Limitless or Polymarket.
- A **market identifier** is the venue's own name for a market. Limitless uses a slug such as `will-trump-acquire-greenland-before-2027-1768930762585`. Polymarket uses a condition id with two token ids, one per outcome. The v1 consumers take the identifier with `--market` and its kind with `--kind` (`slug`).
- A **target** (v2) is one subscription coordinate: a venue, a market, and an asset where the venue needs one. A Polymarket condition is two targets. v1 speaks of markets.
- A **selection** (v2) is the JSON file that names the targets of one run.
- A **shard** is one venue's unit of publication ownership: the single admission owner of its markets and the one ordered stream it publishes. v1 puts 100 markets on a shard (`markets_per_shard = 100`); v2 puts 100 selection rows on a connection, per venue. v1 prints `shard 0:` when it stops; a v2 report has one `shard` line per connection.
- A **connection generation** labels one socket assignment. A reconnect advances it, and data from a retired generation is rejected.
- A **segment** (v1) is the shared-memory file a shard publishes, `run/pmws-<instance>-<shard>.seg`. Consumers attach to it read-only.
- A **book revision** (v1) counts the published states of one book. Its **authority** is the book's state: `Live`, `Stale(<reason>)`, `Synchronizing` and so on. A **continuity epoch** names a stretch of unbroken history; a loss starts a new one. A **mutation** is one level change derived from a venue update.
- A **batch** (v2) is one received message. It holds one or more **events**, and a **family** is the venue's own name for a kind of event, such as `orderbookUpdate` or `price_change`.
- The **handoff** (v2) is where an admitted batch leaves the daemon's upstream half. `handoff_ns` measures receipt to that point.
- **Coverage** (v2) is the evidence that a target is carried by its connection: the venue's acknowledgement for Limitless, observed data for Polymarket.
- A **serve run** (v2, `--serve`) keeps a selection subscribed until you stop it or `--max-seconds` passes. A **measurement run** (no `--serve`) must meet event and time floors to qualify.
- A **setting** (v1) is one entry in `pmwsd.toml`, such as `markets` or `metrics_listen`.

## Installation

You need:

- Linux or macOS. On Windows, use WSL2.
- rustup. The toolchain pinned in each tree's `rust-toolchain.toml` (Rust 1.98.0) installs itself on the first build.
- A C compiler (`cc`).
- Python 3.12 or newer.
- Node 22.18 or newer, for the v1 TypeScript consumer only.
- curl, for the v1 metrics check and for refreshing the v2 selections.

Clone the branch you want. Each clone is self-contained:

```
$ git clone --branch v1 --depth 1 https://github.com/codebuster22/pm-ws-preview.git pm-ws-v1
$ git clone --branch v2 --depth 1 https://github.com/codebuster22/pm-ws-preview.git pm-ws-v2
```

In each clone, build the release binaries, then run the setup script:

```
$ cargo build --release --locked
$ ./setup.sh
```

`setup.sh` takes a few seconds and makes no network call. It runs no `cargo build` and no `cargo test`; its only checks are instant self-tests. It renders the v1 configuration, or checks the v2 binary and scripts, and prints the commands to run next. Run it again after moving a clone, because the v1 configuration holds absolute paths.

Everything a build writes stays inside its clone: `target/`, the rendered v1 configuration, the v1 `run/` directory (the control socket, its `.lock` file, and the segments `pmws-<instance>-<shard>.seg`, which `pmwsd` removes on a clean stop) and the v2 `runs/` directory for the files you name with `--output` and `--snapshot`. One exception: if `<clone>/run/pmwsd.sock` would be longer than 100 bytes, v1 `setup.sh` puts the control socket and its `.lock` file at `/tmp/pmwsd-v1.sock` and says so. No command in this guide writes a venue payload to disk: reports and snapshots hold counters, histograms and the consumed selection, never event content, and the v2 tape goes to stdout only. The one flag that would write venue frames, `pmws-run --record <path>`, is not used here.

## v1: the order-book daemon

v1 subscribes to Limitless markets, rebuilds each order book from the venue's snapshots and updates, and publishes the books and their level changes through shared-memory segments that consumers on the same host attach to.

`setup.sh` renders `pmwsd.toml` (four seeded markets) and `pmwsd.empty.toml` (no markets) from the templates in `config/`. The seeded markets run until 31 December 2026. To change the markets, edit [config/pmwsd.toml.in](https://github.com/codebuster22/pm-ws-preview/blob/v1/config/pmwsd.toml.in) and run `./setup.sh` again. Every setting is defined in [src/daemon.rs](https://github.com/codebuster22/pm-ws-preview/blob/v1/src/daemon.rs); a setting the daemon does not know is refused as an unknown field, and `pmwsd` exits 2.

### A first look at a book

`pmws-run` opens one WebSocket to Limitless for one market, rebuilds its book and prints what it sees. It needs no daemon and, as run here, writes no file. Pass a market slug, for example one of the four seeds in `config/pmwsd.toml.in`:

```
$ ./target/release/pmws-run --market <slug> --seconds 60 --print-book
```

Without `--print-book` you get the connection and event lines and the summary; `--print-book` adds the `book`, `mutation`, `resolution` and `summary book` lines. This is a complete 60-second run on 9 October 2026 against the seeded Greenland market; the `ts` values in the venue lines are UTC. About half a minute in, the connection dropped; the run reconnected, resubscribed and opened continuity epoch 1:

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

Three things to read in it:

- `orderbookUpdate` is one venue frame. `bids` and `asks` count its levels; `best_bid` and `best_ask` are the first level the venue listed on each side. Prices and quantities are exact decimal text.
- `book` prints once at start and after each new revision. The canonical lists show the book as the venue reports it, at most three levels a side, best first. The derived complement is computed locally; the venue did not send it.
- `summary book` shows `snapshots_applied=3` and `mutations_derived=0`: the three snapshots carried the same levels, so no mutation was derived. The `source` lines report which connection is publishing; they are not errors.

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

Exit code 1 means startup failed after the configuration loaded, for example because another daemon holds the socket. Exit code 2 is a bad command line or configuration. Exit code 3 means a shard stopped; stderr names it.

### Reading the book from Python and TypeScript

[examples/bbo.py](https://github.com/codebuster22/pm-ws-preview/blob/v1/examples/bbo.py) and [examples/bbo.ts](https://github.com/codebuster22/pm-ws-preview/blob/v1/examples/bbo.ts) attach to a segment that a running `pmwsd` publishes and print the best bid and ask. They must run on the same host, as the same user as the daemon, from a clone that has been built. Node runs the `.ts` file directly, with no build step.

With the daemon running in another terminal, list `run/` for the segment name, then run a consumer against it with a market from `pmwsd.toml`:

```
$ python3 examples/bbo.py --segment run/pmws-<instance>-0.seg --market <slug> --events
$ node examples/bbo.ts --segment run/pmws-<instance>-0.seg --market <slug>
```

`--seconds` sets the run time and defaults to 10. `--events` also prints `mutation`, `resolution` and continuity lines. The market must be in the daemon's set and in this segment; otherwise the consumer prints `error: market not installed in the segment` and exits 1.

Both print the same line shapes. The first `bbo` line prints after attach, and another each time the book revision changes. With `--events`, a `mutation` line prints for each level change, a `resolution` line when the venue resolves the market, and if the mutation stream loses continuity the consumer prints `continuity_lost reason=<reason>` and `reattach revision=<R>`, then goes on:

```
bbo revision=<R> authority=<authority> best_bid=<price>@<qty> best_ask=<price>@<qty>
mutation revision=<R> cursor=<epoch>:<position> origin=<origin> side=<side> price=<price> qty=<old>-><new>
resolution revision=<R> cursor=<epoch>:<position> origin=<origin> outcome=<outcome> index=<index> type=<type> date=<date> path=<path>
```

A missing side prints `-`. `none` in `qty` means the level is absent on that side of the change.

Both consumers against the daemon started above, on the Greenland seed. No mutation arrived in their windows, so each printed only its opening line:

```
$ python3 examples/bbo.py --segment run/pmws-e6baeb9ae9b6c688fdad6336bc7397c6-0.seg --market will-trump-acquire-greenland-before-2027-1768930762585 --seconds 20 --events
bbo revision=1 authority=Live best_bid=0.003@863898323 best_ask=0.899@12362000
$ node examples/bbo.ts --segment run/pmws-e6baeb9ae9b6c688fdad6336bc7397c6-0.seg --market will-trump-acquire-greenland-before-2027-1768930762585 --seconds 10
bbo revision=1 authority=Live best_bid=0.003@863898323 best_ask=0.899@12362000
```

### Controlling the daemon with pmwsctl

While the daemon runs, `pmwsctl` from a second terminal shows its state and changes the pinned market set. Without `--socket` it looks for `/tmp/pmwsd.sock`, so pass the path that `setup.sh` printed:

```
$ ./target/release/pmwsctl --socket run/pmwsd.sock status
$ ./target/release/pmwsctl --socket run/pmwsd.sock add <slug>...
$ ./target/release/pmwsctl --socket run/pmwsd.sock remove <slug>...
```

Every answer is pretty-printed JSON on stdout; errors go to stderr. Exit code 3 means at least one market was rejected, and 4 means a bounded queue was full and nothing was applied, so retry.

`add` and `remove` print one object per slug with a `status` of `accepted`, `reconciling`, `live`, `removed`, `{"rejected": "<reason>"}` or `{"stale": "<reason>"}`. Here a fifth market is added to the running daemon and then removed. Four seconds after the `add`, `status` showed it `established` and `live`:

```json
[ { "slug": "opensea-fdv-above-dollar3b-one-day-after-launch-1764857000239", "status": "accepted" } ]
[ { "slug": "opensea-fdv-above-dollar3b-one-day-after-launch-1764857000239", "status": "removed" } ]
```

`status` returns one entry per shard and one per market. A market's `subscription` is `desired`, `subscribing`, `established` or `removing`; `pinned` is true while the configuration file or `pmwsctl add` holds the market, and `leases` counts the consumer sessions that hold it. The latency figures are the daemon's own queue age and publish time, in microseconds; the percentiles are histogram upper bounds, `last_micros` and `max_micros` are exact, and none of them is end-to-end delivery latency. This is the answer from the same run, on an Apple M1 with 8 CPUs, trimmed to one shard and one market:

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

Both rendered configurations set `metrics_listen = "127.0.0.1:9090"`, so the daemon serves Prometheus metrics. If the port is taken, change the setting in the template and run `./setup.sh` again. The endpoint has no authentication, so keep it on a loopback address.

```
$ curl 127.0.0.1:9090/metrics
```

Six lines from the same run, about two minutes in. `pmws_shard_queue_age_p99_micros` is a histogram upper bound, not an exact measurement:

```
pmws_shard_connected{shard="0"} 1
pmws_shard_frames_seen{shard="0"} 15
pmws_shard_continuity_losses{shard="0"} 4
pmws_shard_queue_age_p99_micros{shard="0"} 216
pmws_shard_markets{shard="0"} 5
pmws_markets 5
```

### Writing your own consumer

Start from `bbo.py` or `bbo.ts` and keep your copy inside `examples/`: both files find the binding by a path relative to their own location, so an unmodified copy in the clone root will not find it. The bindings are [bindings/python/pmws.py](https://github.com/codebuster22/pm-ws-preview/blob/v1/bindings/python/pmws.py) and [bindings/node/pmws.ts](https://github.com/codebuster22/pm-ws-preview/blob/v1/bindings/node/pmws.ts), and each call is documented where it is declared. Every price and quantity is an exact decimal with `coefficient`, `scale` and `text` (`ExactDecimal` in Python, `Decimal` in TypeScript); print `text`, and do not convert to a float.

Three rules apply to any consumer:

- **Same user, read-only.** Run the consumer as the user that runs `pmwsd`. Neither binding has a call that writes to the segment, and neither can pin a market; pinning is `pmwsctl add`.
- **Continuity.** `next_event()` raises `PmwsContinuityLost` when the stream can no longer prove it delivered every mutation in order, and keeps raising until you call `reattach()`. Treat the state that `reattach()` returns as the book and discard what you built from earlier mutations.
- **A slow consumer loses mutations.** The daemon never waits for a consumer. Retained mutations per market are bounded, and a consumer that falls behind gets a continuity loss with the reason `Overrun`. Ingestion never slows.

## v2: the native-event rail

v2 subscribes to Limitless and Polymarket, decodes the events on the selected feeds into typed events, and hands them to an in-process receiver that counts and times them. It builds no book and has no consumer API. You watch a run through its status line, its tape, its snapshots and its final report.

### Serving a selection

Three selections ship with the tree. A serve run needs every row in the file to end in the future, so check the date before you pick one:

| File | Contents | Valid until |
| --- | --- | --- |
| [selections/limitless.json](https://github.com/codebuster22/pm-ws-preview/blob/v2/selections/limitless.json) | Four long-dated Limitless markets | 31 December 2026 |
| [selections/nfl-ncaa.json](https://github.com/codebuster22/pm-ws-preview/blob/v2/selections/nfl-ncaa.json) | Polymarket NFL and college-football conditions | The `valid_until` header in the file |
| [selections/mixed.json](https://github.com/codebuster22/pm-ws-preview/blob/v2/selections/mixed.json) | Both of the above | The `valid_until` header in the file |

`valid_until` is the earliest market end in the file; market ends are the venue's end dates, not kickoff times. When a file has expired, [Writing a selection](#writing-a-selection) shows how to prune or rebuild it.

This is the recipe `setup.sh` prints. It serves the selection for up to an hour, writes a snapshot every five seconds, and pipes the tape through its viewer:

```
$ RUN=runs/$(date +%Y%m%d-%H%M%S); mkdir -p "$RUN"
$ ./target/release/pmwsd upstream --selection selections/nfl-ncaa.json --output "$RUN/report.json" \
    --serve --snapshot "$RUN/snapshot.json" --snapshot-seconds 5 --workers 2 \
    --min-seconds 1 --min-events 1 --max-seconds 3600 --tape | python3 bench/tape_summary.py
```

| Flag | Effect |
| --- | --- |
| `--output <path>` | The final report. The path must not exist, so use a new `RUN` directory each time |
| `--serve` | Keep going: a failed connection is retried after five seconds, and faults, resolved markets and lost health do not end the run. Without it the run is a measurement run, which stops on the first fault |
| `--snapshot <path>` | A live summary file, rewritten every `--snapshot-seconds` |
| `--max-seconds <n>` | A serve run ends this many seconds after the process starts. At most 14400 |
| `--tape` | One JSON line per admitted batch on stdout |
| `--workers <n>` | Worker threads that own the connections. 1 to 8, default 2 |

Stop the run with Ctrl-C or `SIGTERM`. A serve run writes the report and exits 0. If it reaches `--max-seconds` first, it writes the report and exits 2, which is normal for a serve run; the report's `reason` is then `serve_max_seconds`. A `SIGKILL` leaves no report, only the last snapshot. To change the markets, edit the selection and start a new run with a new `RUN` directory; the daemon cannot add or remove markets while it runs.

### The status line

Every ten seconds the daemon prints one line on stderr. The tape goes to stdout, so the two do not mix:

```
upstream elapsed=<n>s evidence=<a>/<b> healthy_connections=<c>/<d> activity=[<l>, <p>] faults=<n>
```

| Field | Meaning |
| --- | --- |
| `evidence=<a>/<b>` | Targets with subscription evidence, out of all selected. For Limitless the venue acknowledged the market; for Polymarket a data message arrived for the token |
| `healthy_connections=<c>/<d>` | Connections that are up, subscribed and have shown a heartbeat. A Limitless connection must also have all its targets acknowledged; in a serve run, an acknowledgement for only some of its markets still counts |
| `activity=[<l>, <p>]` | Events since start: Limitless `orderbookUpdate`, and Polymarket `book` plus `price_change` |
| `faults` | One total of the fault counters: connection faults such as `connect_failed`, malformed messages, routing rejections, admission overloads, and generation, member-order and sequence errors. A failed connection attempt counts one, as in the report under [Snapshots and the report](#snapshots-and-the-report). The names are in the snapshot |

A quiet market is not a fault. Once per run the daemon also prints `upstream measured window started by readiness`, when every connection has been healthy for 30 s in a row, or, in a serve run only, `upstream measured window started by timeout`, 90 s after start when readiness has not held. Per-family counts and latencies in the report and the snapshot cover events inside this window only. A finished run that exits 2 ends with `pmwsd upstream: upstream not qualified: <reason>`.

### The tape

`--tape` prints one JSON line to stdout for each admitted batch. Pipe it into a viewer. Do not redirect it to a file: `text` holds source content, and the daemon keeps no venue payload on disk.

```
{"t_ns":<n>,"venue":"polymarket","stream":"polymarket-0","family":"price_change","market":"<condition-id>","events":<n>,"bytes":<n>,"handoff_ns":<n>,"generation":<n>,"text":"…"}
```

| Field | Meaning |
| --- | --- |
| `t_ns`, `handoff_ns` | Receive time in nanoseconds from process start, and nanoseconds from receive to typed handoff |
| `stream`, `generation` | `<venue>-<index>`, one per connection, and the connection generation, which changes on reconnect |
| `family`, `market` | Family and market identifier of the batch's first event; `market` is `null` if the event carries none. A batch with several events is counted under its first event's family |
| `events`, `bytes` | Events in the batch, and the size of the source message |
| `text` | The source message cut at 600 bytes, with `…` appended when cut |

The tape queues at most 2048 lines for stdout. When the queue is full, the new line is dropped and counted as `tape_dropped` in the snapshot; ingestion never waits.

[bench/tape_summary.py](https://github.com/codebuster22/pm-ws-preview/blob/v2/bench/tape_summary.py) reads the tape on stdin and writes no file. Every `--interval` seconds (default 10) it prints a `tape last <n>s` table and a `tape since start` table, one row per venue and family, and when its input ends it prints the since-start totals once more as `tape at end of input`. A table prints only when the next line arrives, so a silent feed prints nothing. `--follow <market-id>` echoes one market's lines with their text.

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

`--snapshot <path>` rewrites a file every `--snapshot-seconds`. The daemon writes `<path>.tmp` and renames it, so a reader always sees a whole file. `--output` is written once, when the run ends. Both hold counters, histograms and the consumed selection, never event content. [bench/snapshot_summary.py](https://github.com/codebuster22/pm-ws-preview/blob/v2/bench/snapshot_summary.py) prints either file:

```
$ python3 bench/snapshot_summary.py "$RUN/snapshot.json"
$ python3 bench/snapshot_summary.py "$RUN/report.json"
```

The figures are upstream costs, from the socket read of a complete message to a typed, validated event at the handoff. They are not end-to-end consumer latency. This is the report of the five-minute run above. It ended at `--max-seconds`, so its reason is `serve_max_seconds`. One Polymarket socket logged three failed connection attempts before it stayed up:

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

Other reasons a serve run can carry: `operator_interrupt` and `operator_terminate` for Ctrl-C and `SIGTERM`, and `selected_target_resolved` when a selected market resolved during the run; a serve run keeps going and carries that reason in its report.

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

The daemon ignores every other field; the seeded files carry `title`, `game_start`, `tag`, `generated_at`, `valid_until` and `note` for people to read. Both the `limitless` and `polymarket` lists must be present, and either may be empty but not both. Rows go 100 to a connection, per venue, in file order, so a 200-condition file makes two Polymarket shards. `end_epoch` is required, in whole Unix seconds; with `--serve` it must be later than now, and one expired row makes the daemon exit 2 before any connection opens.

To rebuild the Polymarket files, run [bench/discover_sports.py](https://github.com/codebuster22/pm-ws-preview/blob/v2/bench/discover_sports.py). It fetches `GET https://gamma-api.polymarket.com/events` through curl, one tag at a time (`nfl` and `cfb` by default), keeps at most four markets per event, ranks upcoming games first, and writes descriptors only. The venue allows about 60 requests an hour; the script makes at most three per tag, half a second apart, and stops after three pushbacks. `--min-days <n>` keeps only markets whose end is at least that many days away (default 5), and `--max-conditions <n>` caps the conditions written (default 200, two targets each). `--prune` copies a selection without the rows that end within an hour and makes no network request.

```
$ python3 bench/discover_sports.py --output selections/nfl-ncaa.json --min-days 10 --max-conditions 200
$ python3 bench/discover_sports.py --output selections/mixed.json --min-days 10 --merge-limitless selections/limitless.json
$ python3 bench/discover_sports.py --prune selections/nfl-ncaa.json --output selections/nfl-ncaa.json
```

### Measurement runs

A run without `--serve` is a measurement run. It needs every row to end at least `--max-seconds` plus 210 s in the future, opens its measured window only on readiness, stops on the first fault, and ends with `--max-seconds` counted from the moment the window opens. It qualifies, with the reason `healthy_floors_reached` and exit code 0, when the window lasted `--min-seconds` (at least 900) and both venues reached `--min-events` (at least 10,000); a selection with one venue cannot qualify and ends as `insufficient_samples`. Interrupting a measurement run exits 2 with `operator_interrupt`. The [v2 README](https://github.com/codebuster22/pm-ws-preview/blob/v2/README.md) shows how to wrap one with `bench/run_upstream.py`, and a full measurement report from an Apple M1, 100 Limitless markets and 100 Polymarket conditions over a 4619 s measured window, is in [bench/reports/native-upstream-flat-packed-100-m1.md](https://github.com/codebuster22/pm-ws-preview/blob/v2/bench/reports/native-upstream-flat-packed-100-m1.md).

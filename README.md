# pm-ws v2 preview

Ultra-low-latency Rust WebSocket ingestion of complete native prediction-market events from
Limitless and Polymarket. Each market has exactly one current WebSocket owner, one socket
carries many markets, and each received application message is decoded once into exact typed
data and handed off whole. This branch is a frozen preview of the native-event rail; it has no
consumer API yet, so what you see is the daemon's own accounting and its event tape.

The guide on the `main` branch of this repository walks through everything below, including
what each printed line means:
<https://github.com/codebuster22/pm-ws-preview/blob/main/GUIDE.md>
An agent asked to build, run or report on this clone should follow
<https://github.com/codebuster22/pm-ws-preview/blob/main/GUIDE-FOR-AGENTS.md>.

## Build

Needs rustup (the pinned toolchain installs itself from `rust-toolchain.toml`), a C compiler,
Python 3.12 or newer, and curl for refreshing the selections. Linux or macOS; WSL2 on Windows.

```sh
cargo build --release --locked
./setup.sh
```

`setup.sh` takes a few seconds: it checks the tools, creates `runs/`, and runs the scripts'
self-tests once the daemon is built.

## Quick start

One serve run on the seeded selection, one hour at most, with the event tape summarized every
ten seconds (stop it earlier with Ctrl-C; the daemon then exits 0, and exit 2 at the hour is
normal):

```sh
RUN=runs/$(date +%Y%m%d-%H%M%S); mkdir -p "$RUN"
./target/release/pmwsd upstream --selection selections/nfl-ncaa.json --output "$RUN/report.json" \
  --serve --snapshot "$RUN/snapshot.json" --snapshot-seconds 5 --workers 2 \
  --min-seconds 1 --min-events 1 --max-seconds 3600 --tape | python3 bench/tape_summary.py
```

While it runs, the daemon prints one status line to stderr every ten seconds, and in another
terminal `python3 bench/snapshot_summary.py "$RUN/snapshot.json"` shows coverage, faults and
per-family handoff latency. `--output` must not exist before the run; the final report is
written there when the run ends. Adding or removing markets means editing the selection file
and restarting the run.

The daemon's full usage, printed when it runs with no arguments:

```text
pmwsd upstream --selection <descriptor-json> --output <metrics-json> [--control-socket <path>]
  [--workers 2] [--min-seconds 900] [--max-seconds 7200] [--min-events 10000]
  [--diagnostic] [--cpu-timing] [--serve] [--snapshot <path>] [--snapshot-seconds 2] [--tape]
```

## Seeded selections

`selections/nfl-ncaa.json` names Polymarket NFL and college-football conditions,
`selections/limitless.json` four long-dated Limitless markets, and `selections/mixed.json` both.
Each file's `valid_until` is the earliest market end in it; one expired row blocks a `--serve`
run. After that date, or to refresh earlier, regenerate from the venue's listing (about three
requests per tag, descriptors only):

```sh
python3 bench/discover_sports.py --output selections/nfl-ncaa.json --min-days 10 --max-conditions 200
python3 bench/discover_sports.py --output selections/mixed.json --min-days 10 --merge-limitless selections/limitless.json
python3 bench/discover_sports.py --prune selections/nfl-ncaa.json --output selections/nfl-ncaa.json
```

Pick selections whose markets have at least five days left. `--prune` drops rows ending within
the hour without touching the network. A selection may name up to 4096 targets, a Polymarket
condition counting as two.

## What writes where

Everything stays inside the clone: `target/` for the build and `runs/` for the reports and
snapshots you name with `--output` and `--snapshot`. The tape goes to stdout only. The daemon
persists no venue payload: reports and snapshots carry counters, histograms and the consumed
selection, never event content. rustup and cargo keep their own caches in your home directory.

## What it does

- Decodes every documented family on the selected feeds: Limitless `orderbookUpdate`,
  `newPriceData`, `marketCreated`, `marketResolved`, with `system` and `exception` kept as
  source-control records; Polymarket `book`, `price_change`, `last_trade_price`,
  `tick_size_change`, `best_bid_ask`, `new_market`, `market_resolved`. An undocumented family is
  admitted as a bounded unknown envelope under its source name, never skipped silently.
- Parses venue decimals exactly into scaled integers; no floating point in any source price,
  size, timestamp or version.
- Publishes every valid arrival, including repeats and same-value updates, as one atomic batch
  per application message, fenced by connection generation so a reconnect exposes its gap.
- Bounds admission by event count and bytes. Ingestion is WebSocket-only; recovery is
  resubscribe, then reconnect.

## What it does not do

There is no local transport, no consumer API, and no Python or TypeScript binding in this
line yet: the rail ends in a metrics-only in-process receiver, so no process other than the
daemon reads an event, and the tape exists to show what arrives. It builds no order book,
derives no diffs, normalizes no economics, and makes no trading decisions. The
[qualification run](bench/reports/native-upstream-flat-packed-100-m1.md) records the measured
upstream cost on an Apple M1; the guide repeats the few numbers that matter.

## Measurement runs

`bench/run_upstream.py` wraps the daemon for a qualification run and records a build receipt;
`bench/report_upstream.py` validates and renders the report. `--run` is what opens live venue
connections:

```sh
python3 bench/run_upstream.py --build-only
python3 bench/run_upstream.py --selection selections/nfl-ncaa.json --output-dir runs/measure --run
python3 bench/report_upstream.py --input runs/measure/report.json --manifest runs/measure/metadata.json --output runs/measure/report.md
```

## Verify

```sh
./check
```

Runs formatting, clippy with warnings denied, and the test suite.

## Documents

[Glossary](CONTEXT.md) defines the project's terms. [Design](docs/design.md) owns event
atomicity, identity, admission, and consumer semantics. [Decisions](docs/adr/) record what was
decided and why. [Limitless](docs/limitless.md) and [Polymarket](docs/polymarket.md) pin venue
facts, family coverage, and live etiquette.

## Licence

MIT, Chaain Labs; see [LICENSE](LICENSE). Third-party licence texts are in
[THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md).

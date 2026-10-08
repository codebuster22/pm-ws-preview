# pm-ws v1 preview

A prediction-market order-book daemon for Limitless. One process keeps authoritative order books
for a pinned set of binary markets from the venue's WebSocket feed and publishes latest state plus
every level mutation to same-host consumers through shared memory. Python, TypeScript and Rust
consumers attach read-only; every price and quantity is an exact scaled decimal, never a float.

This branch is a frozen preview of the v1 line. The guide on the `main` branch of this
repository walks through everything below, including what each printed line means:
<https://github.com/codebuster22/pm-ws-preview/blob/main/GUIDE.md>
An agent asked to build, run or report on this clone should follow
<https://github.com/codebuster22/pm-ws-preview/blob/main/GUIDE-FOR-AGENTS.md>.

## Build

Needs rustup (the pinned toolchain installs itself from `rust-toolchain.toml`), a C compiler,
Python 3.12 or newer, and Node 22.18 or newer for the TypeScript consumer. Linux or macOS;
WSL2 on Windows.

```sh
cargo build --release --locked
./setup.sh
```

`setup.sh` takes a few seconds. It checks the tools, creates `run/`, and renders `pmwsd.toml`
(four seeded markets) and `pmwsd.empty.toml` (no markets) from `config/` with this clone's
absolute path. Run it again after moving the clone.

## Quick start

One live market, no daemon, for sixty seconds:

```sh
./target/release/pmws-run --market opensea-fdv-above-dollar500m-one-day-after-launch-1764857001399 --seconds 60 --print-book
```

The daemon, its control tool, its metrics, and the consumers (`ls run/` shows the segment file
once the daemon is up):

```sh
./target/release/pmwsd --config pmwsd.toml
./target/release/pmwsctl --socket run/pmwsd.sock status
curl 127.0.0.1:9090/metrics
python3 examples/bbo.py --segment run/pmws-<instance>-0.seg --market <slug> --events
node examples/bbo.ts --segment run/pmws-<instance>-0.seg --market <slug>
```

`pmwsctl add <slug>...` and `pmwsctl remove <slug>...` change the pinned market set while the
daemon runs. If port 9090 is taken, change `metrics_listen` in `config/pmwsd.toml.in` and rerun
`./setup.sh`.

## Seeded markets

The four markets in `config/pmwsd.toml.in` run until 31 December 2026. If you run after that
date, or one resolves early, replace them with Limitless markets that have at least five days
left (`docs/limitless.md` describes discovery) and run `./setup.sh` again.

## What writes where

Everything stays inside the clone: `target/` for the build, `run/` for the control socket, its
`.lock` file and the shared-memory segments, and the two rendered toml files. The one exception
is a clone path too long for a Unix socket (over 100 bytes): `setup.sh` then places the socket
at `/tmp/pmwsd-v1.sock` and says so. `pmws-run` writes nothing unless given `--shm` or
`--record`. rustup and cargo keep their own caches in your home directory.

## What it does and does not do

It connects to Limitless, subscribes to the pinned markets, rebuilds each book from the venue's
snapshots and updates, detects gaps and resubscribes or reconnects, and publishes books and
mutations to consumers without blocking on them. It builds no cross-venue view, normalizes no
economics, places no orders, calls no REST endpoint on the update path, and persists nothing.
Limitless is the only venue in this line.

## Verify

```sh
./check
```

Runs formatting, clippy with warnings denied, and the test suite.

## Licence

MIT, Chaain Labs; see `LICENSE`. Third-party notices are in `THIRD-PARTY-NOTICES.md`.

# pm-ws preview

pm-ws is a Rust daemon for ultra-low-latency prediction-market WebSocket ingestion and local
distribution. It keeps one WebSocket per market owner, decodes venue messages exactly (scaled
integers, never floats), and hands complete events to same-host consumers. This repository holds
two preview builds of it, each on its own branch and tag, so they can be cloned, built and run
without anything else.

| | v1 | v2 |
| --- | --- | --- |
| What it is | Order-book daemon with same-host consumers | Native-event rail, no consumer yet |
| Venues | Limitless | Limitless and Polymarket |
| Output | Order books and level mutations in shared memory | Typed native events, observed in-process; a stdout tape shows them |
| Who can read it | Python, TypeScript and Rust consumers | Nobody outside the daemon yet; tape, snapshot and report only |
| Binaries | `pmwsd`, `pmwsctl`, `pmws-run` | `pmwsd upstream` |
| Configuration | `pmwsd.toml` rendered by `setup.sh` | A selection JSON file per run |
| Changing markets | `pmwsctl add` / `remove` while running | Edit the selection file and restart |
| Monitoring | `pmwsctl status`, Prometheus metrics | Status line every 10 s, snapshot JSON, final report |

[GUIDE.md](GUIDE.md) explains both: install, run, read the output, change markets.
[GUIDE-FOR-AGENTS.md](GUIDE-FOR-AGENTS.md) gives a coding agent the same path as ordered steps with
completion criteria.

## Prerequisites

Linux or macOS; WSL2 on Windows. rustup (Rust 1.98.0 installs itself from each tree's
`rust-toolchain.toml`), a C compiler, Python 3.12 or newer, Node 22.18 or newer for the v1
TypeScript consumer, and curl for the v2 selection refresh.

## Get a build

```sh
git clone --branch v1 --depth 1 https://github.com/codebuster22/pm-ws-preview.git pm-ws-v1
git clone --branch v2 --depth 1 https://github.com/codebuster22/pm-ws-preview.git pm-ws-v2
```

Tarballs: <https://github.com/codebuster22/pm-ws-preview/archive/refs/tags/v1.tar.gz> and
<https://github.com/codebuster22/pm-ws-preview/archive/refs/tags/v2.tar.gz>. Then, in each
clone, `cargo build --release --locked` and `./setup.sh`; each tree's README has its quick
start.

## What writes where

Everything a preview does stays inside its clone: the build under `target/`, v1's control
socket, lock file, rendered configs and shared-memory segments under `run/`, v2's reports and
snapshots under `runs/`. The one exception is a v1 clone path too long for a Unix socket (over
100 bytes): `setup.sh` then uses `/tmp/pmwsd-v1.sock` and says so. rustup and cargo keep their
toolchain and crate caches in your home directory as usual. No venue payload is ever written to
disk.

## Licence

MIT, Chaain Labs; see [LICENSE](LICENSE).

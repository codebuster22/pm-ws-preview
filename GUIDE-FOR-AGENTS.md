# Guide for agents

Read this when you are asked to build, run, check or report on a clone of this repository. It gives the order of work and the condition that ends each step. [GUIDE.md](https://github.com/codebuster22/pm-ws-preview/blob/main/GUIDE.md) on the `main` branch is the authority for every command and every printed line. It is in neither branch clone, so open it at that URL; every pointer below is a section of that page. This file adds what an unattended run needs.

## What you are holding

Two frozen builds of pm-ws, a Rust daemon that reads prediction-market WebSocket feeds, each on its own branch: `v1` (Limitless order books with same-host Python, TypeScript and Rust consumers) and `v2` (a native-event rail for Limitless and Polymarket, with no consumer). A clone is one branch. The branch README is the quick start, and `setup.sh` prints the exact next commands for that clone, so run it and use what it prints.

## Rules for every step

- **Self-contained.** Everything you run reads and writes inside the clone. Put logs under `run/` (v1) or `runs/` (v2); both are ignored by git.
- **Venue payloads stay off disk.** The daemon's own files hold counters, histograms and the consumed selection, never event content. The v2 tape goes to its viewer through a pipe (`| python3 bench/tape_summary.py`) and nowhere else, and `pmws-run` runs without `--record`.
- **Budgeted connections.** Every process spends from a budget of 280 venue connection attempts per rolling day. Run one daemon or run at a time, start it deliberately, and stop it with `SIGINT` so it exits cleanly: a clean stop writes the v2 report and removes the v1 segments.
- **Shipped data.** Use the seeded markets in `pmwsd.toml` and the shipped files in `selections/`. Regenerate a selection only after its `valid_until` has passed, with the commands under [Writing a selection](https://github.com/codebuster22/pm-ws-preview/blob/main/GUIDE.md#writing-a-selection).
- **Same user.** Run consumers as the user that runs the daemon.
- **Report lines, not payloads.** Quote the daemon's own lines (startup, status, summary, tables) verbatim, and leave out any `text` field from the tape.

## Steps

### 1. Identify the branch

`git rev-parse --abbrev-ref HEAD` prints `v1` or `v2`. In a tarball, `src/bin/pmwsctl.rs` exists only on v1. Done when you know which of the two paths under step 4 applies.

### 2. Build

```
$ cargo build --release --locked
```

Done when `target/release/pmwsd` exists, and on v1 also `target/release/pmwsctl` and `target/release/pmws-run`. A toolchain download on the first build is expected.

### 3. Set up

```
$ ./setup.sh
```

Done when it prints `next` followed by the commands to run; on v1 it also prints `python binding ok`. `build first` means step 2 did not finish. A `setup:` line on stderr names a missing tool.

### 4a. v1: book, daemon, consumer

1. **One book, no daemon.** Run the `pmws-run` line that `setup.sh` printed (60 s). Done when the output ends with the `summary` lines and the exit code is 0. Report the last `book` line and the `summary` lines. A `continuity_loss` followed by `reconnecting` and a new `connected` line is a venue disconnect handled by the run, not a failure. Reading: [A first look at a book](https://github.com/codebuster22/pm-ws-preview/blob/main/GUIDE.md#a-first-look-at-a-book).
2. **Daemon.** Start it detached, logging inside the clone:

   ```
   $ nohup ./target/release/pmwsd --config pmwsd.toml > run/pmwsd.log 2>&1 &
   ```

   Done when `run/pmwsd.log` holds the line `pmwsd pid=<pid> shards=1 markets=4 ...` and `run/` holds one `pmws-<instance>-0.seg` file. Keep the pid for the clean stop below.
3. **Status.** Run `./target/release/pmwsctl --socket run/pmwsd.sock status`, with the socket path `setup.sh` printed. Done when every market shows `"subscription": "established"` and `"status": "live"`; allow up to 30 s after start. Report the shard entry. Reading: [Controlling the daemon with pmwsctl](https://github.com/codebuster22/pm-ws-preview/blob/main/GUIDE.md#controlling-the-daemon-with-pmwsctl).
4. **Consumer.** Run `python3 examples/bbo.py --segment run/pmws-<instance>-0.seg --market <slug> --seconds 20 --events` with a slug from `pmwsd.toml`, and the same with `node examples/bbo.ts` when `node --version` is 22.18 or newer. Done when each prints a `bbo revision=<n> authority=Live ...` line and exits 0. A quiet market prints only that line. Reading: [Reading the book from Python and TypeScript](https://github.com/codebuster22/pm-ws-preview/blob/main/GUIDE.md#reading-the-book-from-python-and-typescript).
5. **Clean stop.** `kill -INT <pid>`. Done when `run/pmwsd.log` ends with one `shard 0:` line and `run/` holds no `.seg` file.

### 4b. v2: serve, watch, report

1. **Check the selection.** `python3 -c 'import json; print(json.load(open("selections/nfl-ncaa.json"))["valid_until"])'`. Done when the date is after today. Otherwise prune it with the `--prune` command under [Writing a selection](https://github.com/codebuster22/pm-ws-preview/blob/main/GUIDE.md#writing-a-selection) and check again. `selections/limitless.json` runs until 31 December 2026 and needs no check.
2. **Serve for five minutes.** Run the daemon detached, with the status line in a log and the tape through its viewer, and note the run directory, because `$RUN` is gone once this shell exits:

   ```
   $ RUN=runs/$(date +%Y%m%d-%H%M%S); mkdir -p "$RUN"
   $ nohup sh -c "./target/release/pmwsd upstream --selection selections/nfl-ncaa.json --output '$RUN/report.json' \
       --serve --snapshot '$RUN/snapshot.json' --snapshot-seconds 5 --workers 2 \
       --min-seconds 1 --min-events 1 --max-seconds 300 --tape 2> '$RUN/pmwsd.log' \
       | python3 bench/tape_summary.py > '$RUN/tape.log'" > /dev/null 2>&1 &
   ```

   While it runs, `$RUN/pmwsd.log` gains one `upstream elapsed=` line every 10 s; `evidence` and `healthy_connections` reach their full counts within about a minute. Done when `$RUN/tape.log` ends with a `tape at end of input` table, `$RUN/pmwsd.log` ends with `pmwsd upstream: upstream not qualified: serve_max_seconds`, and `$RUN/report.json` exists. That line is the normal end of a serve run; the daemon exits 2, which the pipe and the detached shell hide, so judge the run by the three files. Reading: [Serving a selection](https://github.com/codebuster22/pm-ws-preview/blob/main/GUIDE.md#serving-a-selection), [The tape](https://github.com/codebuster22/pm-ws-preview/blob/main/GUIDE.md#the-tape) and [The status line](https://github.com/codebuster22/pm-ws-preview/blob/main/GUIDE.md#the-status-line).
3. **Report the run.** `python3 bench/snapshot_summary.py runs/<run>/report.json`, where `<run>` is the directory the previous step created (`ls runs/` lists it). Done when it prints one `shard` line per connection with `covered=<n>/<n>` at the full count, followed by family rows. Report that output and the `tape at end of input` table. Reading: [Snapshots and the report](https://github.com/codebuster22/pm-ws-preview/blob/main/GUIDE.md#snapshots-and-the-report).

### 5. Report

Give the branch and commit (`git rev-parse --short HEAD`), the operating system and CPU count, the exit codes of steps 2 and 3, and the lines named above, verbatim. On v2, the latency columns are upstream costs in microseconds, from socket read to typed handoff, with p50 and p99 as histogram upper bounds and the maximum exact. On v1, `queue_age` and `publish_latency` in the status shard entry are the daemon's own queue age and publish time in microseconds, with the percentiles as histogram upper bounds and `last_micros` and `max_micros` exact. Present each as that, and never as end-to-end latency.

## When something is off

| You see | It means |
| --- | --- |
| v2 exits 2 at start, before any `upstream elapsed=` line | A selection rule failed, most often an expired row. See [Writing a selection](https://github.com/codebuster22/pm-ws-preview/blob/main/GUIDE.md#writing-a-selection) |
| `error: market not installed in the segment` | The slug is not in `pmwsd.toml`, or not in the segment you named |
| `pmwsd` exits 1 at start | Startup failed after the configuration loaded; stderr names what. A held control socket or lock means another daemon is running: find it with `pgrep -f 'pmwsd --config'` and stop it with `SIGINT`. `Address already in use` on `127.0.0.1:9090` means the metrics port is taken: change `metrics_listen` in `config/pmwsd.toml.in` and run `./setup.sh` again |
| `setup:` says the clone path is too long for a socket | The control socket is at `/tmp/pmwsd-v1.sock`; use the path `setup.sh` printed |
| `faults=<n>` above 0 while `healthy_connections` is full | Failed connection attempts count as faults. The report's `faults={...}` map names them |
| `continuity_loss`, then `reconnecting`, then `connected` with a new `generation` | The venue closed the connection and the run recovered. The new `continuity_epoch` marks the gap |

## Pointers

- [GUIDE.md](https://github.com/codebuster22/pm-ws-preview/blob/main/GUIDE.md): every command, every printed line, and the terms the outputs use.
- The branch README: the quick start, and what that build does and does not do.
- `docs/limitless.md` and `docs/polymarket.md` in the v2 tree: the venue contracts, family by family.
- `CONTEXT.md` in the v2 tree: the glossary.

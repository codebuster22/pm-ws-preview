# Guide for agents

Read this when you are asked to build, run, check or report on a clone of this repository. It gives the order of work and the condition that ends each step. [GUIDE.md](GUIDE.md) is the authority for every command and every printed line; this file points into it and adds what an unattended run needs.

## What you are holding

Two frozen builds of pm-ws, a Rust daemon that reads prediction-market WebSocket feeds, each on its own branch: `v1` (Limitless order books with same-host Python, TypeScript and Rust consumers) and `v2` (a native-event rail for Limitless and Polymarket, with no consumer). A clone is one branch. The branch README is the quick start, and `setup.sh` prints the exact next commands for that clone, so run it and use what it prints.

## Rules for every step

- **Self-contained.** Everything you run reads and writes inside the clone. Put logs under `run/` (v1) or `runs/` (v2); both are ignored by git.
- **Venue payloads stay off disk.** The daemon's own files hold counters and histograms only. The v2 tape goes to its viewer through a pipe (`| python3 bench/tape_summary.py`) and nowhere else, and `pmws-run --record` stays unused.
- **Budgeted connections.** Every process spends from a budget of 280 venue connection attempts per rolling day. Run one daemon or run at a time, start it deliberately, and stop it with `SIGINT` so it exits cleanly: a clean stop writes the v2 report and removes the v1 segments.
- **Shipped data.** Use the seeded markets in `pmwsd.toml` and the shipped files in `selections/`. Regenerate a selection only after its `valid_until` has passed, with the commands under [Writing a selection](GUIDE.md#writing-a-selection).
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

1. **One book, no daemon.** Run the `pmws-run` line that `setup.sh` printed (60 s). Done when the output ends with the `summary` lines and the exit code is 0. Report the last `book` line and the `summary` lines. A `continuity_loss` followed by `reconnecting` and a new `connected` line is a venue disconnect handled by the run, not a failure. Reading: [A first look at a book](GUIDE.md#a-first-look-at-a-book).
2. **Daemon.** Start it detached, logging inside the clone:

   ```
   $ nohup ./target/release/pmwsd --config pmwsd.toml > run/pmwsd.log 2>&1 &
   ```

   Done when `run/pmwsd.log` holds the line `pmwsd pid=<pid> shards=1 markets=4 ...` and `run/` holds one `pmws-<instance>-0.seg` file. Keep the pid for step 5.
3. **Status.** Run `./target/release/pmwsctl --socket run/pmwsd.sock status`, with the socket path `setup.sh` printed. Done when every market shows `"subscription": "established"` and `"status": "live"`; allow up to 30 s after start. Report the shard entry. Reading: [Controlling the daemon with pmwsctl](GUIDE.md#controlling-the-daemon-with-pmwsctl).
4. **Consumer.** Run `python3 examples/bbo.py --segment run/pmws-<instance>-0.seg --market <slug> --seconds 20 --events` with a slug from `pmwsd.toml`, and the same with `node examples/bbo.ts` when `node --version` is 22.18 or newer. Done when each prints a `bbo revision=<n> authority=Live ...` line and exits 0. A quiet market prints only that line. Reading: [Reading the book from Python and TypeScript](GUIDE.md#reading-the-book-from-python-and-typescript).
5. **Clean stop.** `kill -INT <pid>`. Done when `run/pmwsd.log` ends with one `shard 0:` line and `run/` holds no `.seg` file.

### 4b. v2: serve, watch, report

1. **Check the selection.** `python3 -c 'import json; print(json.load(open("selections/nfl-ncaa.json"))["valid_until"])'`. Done when the date is after today. Otherwise prune it with the `--prune` command under [Writing a selection](GUIDE.md#writing-a-selection) and check again. `selections/limitless.json` runs until 31 December 2026 and needs no check.
2. **Serve for five minutes.** Take the recipe `setup.sh` printed, put `--max-seconds 300` in place of `3600`, and run it detached with its viewer:

   ```
   $ RUN=runs/$(date +%Y%m%d-%H%M%S); mkdir -p "$RUN"
   $ nohup sh -c "./target/release/pmwsd upstream --selection selections/nfl-ncaa.json --output '$RUN/report.json' \
       --serve --snapshot '$RUN/snapshot.json' --snapshot-seconds 5 --workers 2 \
       --min-seconds 1 --min-events 1 --max-seconds 300 --tape 2> '$RUN/pmwsd.log' \
       | python3 bench/tape_summary.py > '$RUN/tape.log'" > /dev/null 2>&1 &
   ```

   While it runs, `$RUN/pmwsd.log` gains one `upstream elapsed=` line every 10 s; `evidence` and `healthy_connections` reach their full counts within about a minute. Done when `$RUN/tape.log` ends with a `tape at end of input` table, `$RUN/pmwsd.log` ends with `pmwsd upstream: upstream not qualified: serve_max_seconds`, and `$RUN/report.json` exists. That line, and the daemon's exit code 2, are the normal end of a serve run. Reading: [The status line](GUIDE.md#the-status-line).
3. **Report the run.** `python3 bench/snapshot_summary.py "$RUN/report.json"`. Done when it prints one `shard` line per connection with `covered=<n>/<n>` at the full count, followed by family rows. Report that output and the `tape at end of input` table. Reading: [Snapshots and the report](GUIDE.md#snapshots-and-the-report).

### 5. Report

Give the branch and commit (`git rev-parse --short HEAD`), the operating system and CPU count, the exit codes of steps 2 and 3, and the lines named above, verbatim. Latency columns are upstream costs, histogram upper bounds in microseconds, from socket read to typed handoff; present them as that, and never as end-to-end latency.

## When something is off

| You see | It means |
| --- | --- |
| v2 exits 2 at start, before any `upstream elapsed=` line | A selection rule failed, most often an expired row. See [Writing a selection](GUIDE.md#writing-a-selection) |
| `error: market not installed in the segment` | The slug is not in `pmwsd.toml`, or not in the segment you named |
| `pmwsd` exits 1 at start | Another daemon holds `run/pmwsd.sock`. Find it with `pgrep -f 'pmwsd --config'` and stop it with `SIGINT` |
| `setup:` says the clone path is too long for a socket | The control socket is at `/tmp/pmwsd-v1.sock`; use the path `setup.sh` printed |
| `faults=<n>` above 0 while `healthy_connections` is full | Failed connection attempts count as faults. The report's `faults={...}` map names them |
| `continuity_loss`, then `reconnecting`, then `connected` with a new `generation` | The venue closed the connection and the run recovered. The new `continuity_epoch` marks the gap |

## Pointers

- [GUIDE.md](GUIDE.md): every command, every printed line, and the terms the outputs use.
- The branch README: the quick start, and what that build does and does not do.
- `docs/limitless.md` and `docs/polymarket.md` in the v2 tree: the venue contracts, family by family.
- `CONTEXT.md` in the v2 tree: the glossary.

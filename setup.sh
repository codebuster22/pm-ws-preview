#!/bin/sh
# Prepares this clone to run the v2 preview: checks the tools, creates runs/, and, once the
# daemon is built, runs the instant self-checks. Takes a few seconds. No build, no tests, no
# network.
set -eu
cd "$(dirname "$0")"

fail() { echo "setup: $*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || fail "$1 not found; $2"; }

need rustup "install it from https://rustup.rs (the pinned toolchain then installs itself)"
need cargo "install it from https://rustup.rs"
need cc "install a C compiler (build-essential, clang, or the Xcode command line tools)"
need python3 "install Python 3.12 or newer"
command -v curl >/dev/null 2>&1 || echo "setup: curl not found; bench/discover_sports.py needs it to refresh the selections" >&2

mkdir -p runs

if [ -x target/release/pmwsd ]; then
  status=0
  ./target/release/pmwsd >/dev/null 2>&1 || status=$?
  [ "$status" = 2 ] || fail "pmwsd without arguments exited $status, expected 2 (usage)"
  echo "pmwsd usage exit ok"
  python3 bench/report_upstream.py --self-test
  python3 bench/run_upstream.py --self-test && echo "run_upstream self-test: ok"
  python3 bench/discover_sports.py --self-test
  python3 bench/tape_summary.py --self-test
  python3 bench/snapshot_summary.py --self-test
  echo "binary: target/release/pmwsd"
  echo "next (one serve run on the seeded NFL and college-football selection, one hour at most):"
  echo '  RUN=runs/$(date +%Y%m%d-%H%M%S); mkdir -p "$RUN"'
  echo '  ./target/release/pmwsd upstream --selection selections/nfl-ncaa.json --output "$RUN/report.json" --serve --snapshot "$RUN/snapshot.json" --snapshot-seconds 5 --workers 2 --min-seconds 1 --min-events 1 --max-seconds 3600 --tape | python3 bench/tape_summary.py'
  echo '  python3 bench/snapshot_summary.py "$RUN/snapshot.json"'
else
  echo "build first: cargo build --release --locked   (then run ./setup.sh again)"
fi

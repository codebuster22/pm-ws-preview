#!/bin/sh
# Prepares this clone to run the v1 preview: checks the tools, creates run/, and renders
# pmwsd.toml and pmwsd.empty.toml from config/*.toml.in with this clone's absolute path.
# Takes a few seconds. No build, no tests, no network. Rerun it after moving the clone.
set -eu
cd "$(dirname "$0")"
REPO=$(pwd -P)

fail() { echo "setup: $*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || fail "$1 not found; $2"; }

need rustup "install it from https://rustup.rs (the pinned toolchain then installs itself)"
need cargo "install it from https://rustup.rs"
need cc "install a C compiler (build-essential, clang, or the Xcode command line tools)"
need python3 "install Python 3.12 or newer"
if command -v node >/dev/null 2>&1; then
  NODE_OK=$(node -e 'const [a,b]=process.versions.node.split(".").map(Number); process.stdout.write(a>22||(a===22&&b>=18)?"1":"0")')
  [ "$NODE_OK" = 1 ] || echo "setup: node $(node --version) is older than 22.18; the TypeScript consumer needs Node 22.18+ (type stripping)" >&2
else
  echo "setup: node not found; the TypeScript consumer (examples/bbo.ts) needs Node 22.18+. Python and Rust consumers are unaffected." >&2
fi

mkdir -p run
SOCKET="$REPO/run/pmwsd.sock"
if [ ${#SOCKET} -gt 100 ]; then
  SOCKET=/tmp/pmwsd-v1.sock
  echo "setup: the clone path is too long for a Unix socket (100 bytes); the control socket and its .lock file go to $SOCKET, the only files written outside the clone"
fi
ESC_REPO=$(printf '%s' "$REPO" | sed 's/[&|\\]/\\&/g')
ESC_SOCKET=$(printf '%s' "$SOCKET" | sed 's/[&|\\]/\\&/g')
render() { sed -e "s|__REPO__|$ESC_REPO|g" -e "s|__SOCKET__|$ESC_SOCKET|g" "$1" > "$2"; }
render config/pmwsd.toml.in pmwsd.toml
render config/pmwsd.empty.toml.in pmwsd.empty.toml
echo "setup: wrote pmwsd.toml (four seeded markets) and pmwsd.empty.toml (no markets)"
echo "setup: control socket $SOCKET; shared-memory segments under $REPO/run"

if [ -x target/release/pmwsd ] && [ -x target/release/pmwsctl ] && [ -x target/release/pmws-run ]; then
  python3 examples/reader.py --self-test
  python3 -c 'import sys; sys.path.insert(0, "bindings/python"); import pmws; print("python binding ok")'
  if command -v node >/dev/null 2>&1; then
    node --no-warnings -e "import('./bindings/node/pmws.ts').then(()=>console.log('node binding ok'))" \
      || echo "setup: the node binding did not load (needs Node 22.18+); examples/bbo.ts will not run" >&2
  fi
  echo "binaries: target/release/pmwsd target/release/pmwsctl target/release/pmws-run"
  echo "next:"
  echo "  ./target/release/pmws-run --market opensea-fdv-above-dollar500m-one-day-after-launch-1764857001399 --seconds 60 --print-book"
  echo "  ./target/release/pmwsd --config pmwsd.toml"
  echo "  ./target/release/pmwsctl --socket $SOCKET status"
else
  echo "build first: cargo build --release --locked   (then run ./setup.sh again)"
fi

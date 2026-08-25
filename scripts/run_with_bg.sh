#!/usr/bin/env bash
# Start a background process, run a command, always kill the process.
# Frees the port first so a leftover demo cannot block the next one.
#
# Usage:
#   scripts/run_with_bg.sh 8080 "GIN_MODE=release src/gate/gate" -- cmd...
set -euo pipefail

if [[ $# -lt 4 ]]; then
  echo "usage: $0 PORT START_CMD -- COMMAND [args...]" >&2
  exit 2
fi

PORT="$1"
shift
START_CMD="$1"
shift
if [[ "$1" != "--" ]]; then
  echo "expected -- before the payload command" >&2
  exit 2
fi
shift

free_port() {
  if command -v lsof >/dev/null 2>&1; then
    lsof -ti:"$PORT" | xargs -r kill 2>/dev/null || true
  elif command -v fuser >/dev/null 2>&1; then
    fuser -k "${PORT}/tcp" 2>/dev/null || true
  fi
}

PID=""
cleanup() {
  if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
    kill "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
  fi
  free_port
}
trap cleanup EXIT INT TERM

free_port
sleep 0.2

# shellcheck disable=SC2086
eval "$START_CMD" &
PID=$!

for _ in $(seq 1 40); do
  if curl -sf "http://localhost:${PORT}/health" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$PID" 2>/dev/null; then
    echo "background process on :$PORT died before becoming healthy" >&2
    exit 1
  fi
  sleep 0.25
done

if ! curl -sf "http://localhost:${PORT}/health" >/dev/null 2>&1; then
  echo "timed out waiting for http://localhost:${PORT}/health" >&2
  exit 1
fi

"$@"

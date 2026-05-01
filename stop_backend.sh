#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$PROJECT_ROOT/data/app/backend.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "[info] backend is not running"
  exit 0
fi

PID="$(cat "$PID_FILE")"
if [[ -z "$PID" ]]; then
  echo "[warn] pid file is empty, removing it"
  rm -f "$PID_FILE"
  exit 0
fi

if kill -0 "$PID" 2>/dev/null; then
  kill "$PID"
  sleep 1
  if kill -0 "$PID" 2>/dev/null; then
    echo "[warn] backend still running, sending SIGKILL"
    kill -9 "$PID"
  fi
  echo "[ok] backend stopped: pid=$PID"
else
  echo "[warn] process not found, removing stale pid file"
fi

rm -f "$PID_FILE"

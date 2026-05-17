#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY_BIN="$PROJECT_ROOT/.venv/bin/python"
PID_DIR="$PROJECT_ROOT/data/app"
PID_FILE="$PID_DIR/worker.pid"
LOG_FILE="$PID_DIR/worker.log"
WORKER_ID="${RANKING_WORKER_ID:-}"
POLL_INTERVAL="${RANKING_WORKER_POLL_INTERVAL:-2.0}"
STALE_TIMEOUT="${RANKING_WORKER_STALE_TIMEOUT_SECONDS:-900}"

mkdir -p "$PID_DIR"

if [[ ! -x "$PY_BIN" ]]; then
  echo "[fatal] missing python venv: $PY_BIN"
  echo "Please create it first: cd $PROJECT_ROOT && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"
  exit 1
fi

if [[ -f "$PID_FILE" ]]; then
  EXISTING_PID="$(cat "$PID_FILE")"
  if [[ -n "$EXISTING_PID" ]] && kill -0 "$EXISTING_PID" 2>/dev/null; then
    echo "[info] worker already running: pid=$EXISTING_PID"
    exit 0
  fi
  rm -f "$PID_FILE"
fi

CMD=("$PY_BIN" -m ib_qlib_pipeline.webapi.worker --poll-interval "$POLL_INTERVAL" --stale-timeout-seconds "$STALE_TIMEOUT")
if [[ -n "$WORKER_ID" ]]; then
  CMD+=(--worker-id "$WORKER_ID")
fi

echo "[info] starting worker"
nohup "${CMD[@]}" >>"$LOG_FILE" 2>&1 &
NEW_PID=$!
echo "$NEW_PID" >"$PID_FILE"
sleep 2

if kill -0 "$NEW_PID" 2>/dev/null; then
  echo "[ok] worker started: pid=$NEW_PID"
  echo "[ok] log: $LOG_FILE"
else
  echo "[fatal] worker failed to start"
  echo "[hint] check log: $LOG_FILE"
  rm -f "$PID_FILE"
  exit 1
fi

#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY_BIN="$PROJECT_ROOT/.venv/bin/python"
PID_DIR="$PROJECT_ROOT/data/app"
PID_FILE="$PID_DIR/backend.pid"
LOG_FILE="$PID_DIR/backend.log"
HOST="${RANKING_API_HOST:-0.0.0.0}"
PORT="${RANKING_API_PORT:-8000}"

mkdir -p "$PID_DIR"

if [[ ! -x "$PY_BIN" ]]; then
  echo "[fatal] missing python venv: $PY_BIN"
  echo "Please create it first: cd $PROJECT_ROOT && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"
  exit 1
fi

if [[ -f "$PID_FILE" ]]; then
  EXISTING_PID="$(cat "$PID_FILE")"
  if [[ -n "$EXISTING_PID" ]] && kill -0 "$EXISTING_PID" 2>/dev/null; then
    echo "[info] backend already running: pid=$EXISTING_PID"
    echo "[info] docs: http://127.0.0.1:$PORT/docs"
    exit 0
  fi
  rm -f "$PID_FILE"
fi

echo "[info] starting backend on ${HOST}:${PORT}"
nohup env RANKING_API_HOST="$HOST" RANKING_API_PORT="$PORT" \
  "$PY_BIN" "$PROJECT_ROOT/run_backend.py" >>"$LOG_FILE" 2>&1 &
NEW_PID=$!
echo "$NEW_PID" >"$PID_FILE"
sleep 2

if kill -0 "$NEW_PID" 2>/dev/null; then
  echo "[ok] backend started: pid=$NEW_PID"
  echo "[ok] docs: http://127.0.0.1:$PORT/docs"
  echo "[ok] log: $LOG_FILE"
else
  echo "[fatal] backend failed to start"
  echo "[hint] check log: $LOG_FILE"
  rm -f "$PID_FILE"
  exit 1
fi

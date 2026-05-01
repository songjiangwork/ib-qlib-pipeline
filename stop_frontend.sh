#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$PROJECT_ROOT/data/app/frontend.pid"
PORT="${FRONTEND_PORT:-9991}"

find_port_pid() {
  lsof -tiTCP:"$PORT" -sTCP:LISTEN 2>/dev/null | head -n 1
}

stop_pid() {
  local pid="$1"
  if [[ -z "$pid" ]]; then
    return 0
  fi
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    sleep 1
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
      sleep 1
    fi
    echo "[ok] frontend stopped: pid=$pid"
  fi
}

STOPPED=0

if [[ -f "$PID_FILE" ]]; then
  PID="$(cat "$PID_FILE")"
  if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
    stop_pid "$PID"
    STOPPED=1
  else
    echo "[warn] process from pid file not found, checking port $PORT"
  fi
fi

rm -f "$PID_FILE"

PORT_PID="$(find_port_pid || true)"
if [[ -n "${PORT_PID:-}" ]]; then
  stop_pid "$PORT_PID"
  STOPPED=1
fi

if [[ "$STOPPED" -eq 0 ]]; then
  echo "[info] frontend is not running"
fi

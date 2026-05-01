#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FRONTEND_DIR="$PROJECT_ROOT/frontend"
PID_DIR="$PROJECT_ROOT/data/app"
PID_FILE="$PID_DIR/frontend.pid"
LOG_FILE="$PID_DIR/frontend.log"
PORT="${FRONTEND_PORT:-9991}"
BACKEND_PORT="${FRONTEND_BACKEND_PORT:-8001}"
PROXY_FILE="$FRONTEND_DIR/proxy.conf.json"

mkdir -p "$PID_DIR"

if [[ ! -d "$FRONTEND_DIR/node_modules" ]]; then
  echo "[fatal] frontend dependencies are missing"
  echo "Run: cd $FRONTEND_DIR && npm install"
  exit 1
fi

find_port_pid() {
  lsof -tiTCP:"$PORT" -sTCP:LISTEN 2>/dev/null | head -n 1
}

if [[ -f "$PID_FILE" ]]; then
  EXISTING_PID="$(cat "$PID_FILE")"
  if [[ -n "$EXISTING_PID" ]] && kill -0 "$EXISTING_PID" 2>/dev/null; then
    echo "[info] frontend already running: pid=$EXISTING_PID"
    echo "[info] url: http://127.0.0.1:$PORT"
    exit 0
  fi
  rm -f "$PID_FILE"
fi

PORT_PID="$(find_port_pid || true)"
if [[ -n "${PORT_PID:-}" ]]; then
  echo "[warn] port $PORT is already in use by pid=$PORT_PID"
  echo "[warn] stopping stale frontend process on port $PORT"
  kill "$PORT_PID" 2>/dev/null || true
  sleep 2
  if kill -0 "$PORT_PID" 2>/dev/null; then
    echo "[warn] pid=$PORT_PID still alive, sending SIGKILL"
    kill -9 "$PORT_PID" 2>/dev/null || true
    sleep 1
  fi
fi

PORT_PID="$(find_port_pid || true)"
if [[ -n "${PORT_PID:-}" ]]; then
  echo "[fatal] port $PORT is still in use by pid=$PORT_PID"
  exit 1
fi

echo "[info] starting frontend on 0.0.0.0:$PORT"
cat >"$PROXY_FILE" <<EOF
{
  "/api": {
    "target": "http://127.0.0.1:${BACKEND_PORT}",
    "secure": false,
    "changeOrigin": true,
    "logLevel": "info"
  }
}
EOF

nohup env FRONTEND_PORT="$PORT" FRONTEND_BACKEND_PORT="$BACKEND_PORT" npm --prefix "$FRONTEND_DIR" start >"$LOG_FILE" 2>&1 &
NEW_PID=$!
echo "$NEW_PID" >"$PID_FILE"
sleep 3

if kill -0 "$NEW_PID" 2>/dev/null; then
  echo "[ok] frontend started: pid=$NEW_PID"
  echo "[ok] url: http://127.0.0.1:$PORT"
  echo "[ok] backend proxy: http://127.0.0.1:$BACKEND_PORT"
  echo "[ok] log: $LOG_FILE"
else
  echo "[fatal] frontend failed to start"
  echo "[hint] check log: $LOG_FILE"
  rm -f "$PID_FILE"
  exit 1
fi

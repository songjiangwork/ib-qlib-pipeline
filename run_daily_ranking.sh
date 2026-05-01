#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY_BIN="$PROJECT_ROOT/.venv/bin/python"

if [[ ! -x "$PY_BIN" ]]; then
  echo "[fatal] missing python venv: $PY_BIN"
  echo "Please create it first: cd $PROJECT_ROOT && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"
  exit 1
fi

exec "$PY_BIN" "$PROJECT_ROOT/oneclick_daily_ranking.py" "$@"


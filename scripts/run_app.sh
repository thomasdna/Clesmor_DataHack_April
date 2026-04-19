#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ ! -d ".venv" ]]; then
  echo "Missing .venv. Create it first: python3.11 -m venv .venv && ./.venv/bin/pip install -r requirements.txt"
  exit 1
fi

exec "$REPO_ROOT/.venv/bin/streamlit" run "$REPO_ROOT/app/app.py" "$@"


#!/usr/bin/env bash
# run_pipeline.sh - run phishfindr via module mode so imports resolve
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1
# use venv python if available
if [ -x "./venv/bin/python3" ]; then
  exec ./venv/bin/python3 -m phishfindr "$@"
elif [ -x "./venv/bin/python" ]; then
  exec ./venv/bin/python -m phishfindr "$@"
else
  exec python3 -m phishfindr "$@"
fi

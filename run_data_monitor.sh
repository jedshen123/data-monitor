#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

export MCP_URL="${MCP_URL:-http://54.226.190.74:8000/mcp}"
export DATABASE_ID="${DATABASE_ID:-1}"
export SEND_DINGTALK="${SEND_DINGTALK:-false}"
export LOG_LEVEL="${LOG_LEVEL:-INFO}"

exec "${PYTHON_BIN}" "${SCRIPT_DIR}/data_monitor.py" "$@"

#!/usr/bin/env bash
set -euo pipefail

PORT="${PORT:-3999}"
HOST="${HOST:-127.0.0.1}"

MODE="basic"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-15}"
CONNECT_TIMEOUT_SECONDS="${CONNECT_TIMEOUT_SECONDS:-2}"
HTTP_MAX_TIME_SECONDS="${HTTP_MAX_TIME_SECONDS:-20}"

usage() {
  cat <<'USAGE'
usage: smoke_test.sh [--real-agent] [--timeout SECONDS]

Options:
  --real-agent         Run an end-to-end flow using the claude-code agent.
  --timeout SECONDS    Timeout for waiting on agent events (default: 15 or $TIMEOUT_SECONDS).
USAGE
}

curl_json() {
  # Usage: curl_json <method> <url> [data]
  local method="$1"
  local url="$2"
  local data="${3:-}"

  if [[ -n "$data" ]]; then
    curl -fsS \
      --connect-timeout "$CONNECT_TIMEOUT_SECONDS" \
      --max-time "$HTTP_MAX_TIME_SECONDS" \
      -X "$method" "$url" \
      -H "Content-Type: application/json" \
      -d "$data"
  else
    curl -fsS \
      --connect-timeout "$CONNECT_TIMEOUT_SECONDS" \
      --max-time "$HTTP_MAX_TIME_SECONDS" \
      -X "$method" "$url"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --real-agent)
      MODE="real-agent"
      shift
      ;;
    --timeout)
      TIMEOUT_SECONDS="${2:-}"
      if [[ -z "$TIMEOUT_SECONDS" ]]; then
        echo "error: --timeout requires a value" >&2
        exit 2
      fi
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

# Use a temp data dir by default so we don't touch ./data
if [[ -z "${DATA_DIR:-}" ]]; then
  DATA_DIR="/tmp/acp-gateway-smoke-$$"
  CLEANUP_DATA_DIR=1
else
  CLEANUP_DATA_DIR=0
fi

SERVER_BIN="${SERVER_BIN:-./target/release/acp-gateway}"

if [[ ! -x "$SERVER_BIN" ]]; then
  echo "error: $SERVER_BIN not found or not executable" >&2
  echo "hint: run: cargo build --release" >&2
  exit 1
fi

mkdir -p "$DATA_DIR"

cleanup() {
  if [[ -n "${PID:-}" ]] && kill -0 "$PID" >/dev/null 2>&1; then
    kill "$PID" >/dev/null 2>&1 || true
    # give it a moment to exit
    sleep 0.1 || true
  fi
  if [[ "$CLEANUP_DATA_DIR" -eq 1 ]]; then
    rm -rf "$DATA_DIR" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

LOG_FILE="$DATA_DIR/server.log"

PORT="$PORT" DATA_DIR="$DATA_DIR" "$SERVER_BIN" >"$LOG_FILE" 2>&1 &
PID=$!

echo "server pid: $PID"
echo "data dir: $DATA_DIR"
echo "log file: $LOG_FILE"

# wait for server readiness via /health
for _ in $(seq 1 50); do
  if curl -fsS "http://$HOST:$PORT/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done

echo "--- GET /health"
curl -fsS --connect-timeout "$CONNECT_TIMEOUT_SECONDS" --max-time "$HTTP_MAX_TIME_SECONDS" "http://$HOST:$PORT/health"; echo

echo "--- GET /api/agents"
curl -fsS --connect-timeout "$CONNECT_TIMEOUT_SECONDS" --max-time "$HTTP_MAX_TIME_SECONDS" "http://$HOST:$PORT/api/agents"; echo

echo "--- GET /v1/models"
curl -fsS --connect-timeout "$CONNECT_TIMEOUT_SECONDS" --max-time "$HTTP_MAX_TIME_SECONDS" "http://$HOST:$PORT/v1/models"; echo

echo "--- POST /v1/chat/completions (unknown model; expect 400)"
# curl -f treats 4xx as error, so we capture status separately
STATUS=$(curl -sS -o "$DATA_DIR/chat_unknown.json" -w "%{http_code}" \
  --connect-timeout "$CONNECT_TIMEOUT_SECONDS" \
  --max-time "$HTTP_MAX_TIME_SECONDS" \
  -X POST "http://$HOST:$PORT/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -d '{"model":"nonexistent-model-xyz","messages":[{"role":"user","content":"hello"}],"stream":false}')

echo "status: $STATUS"
cat "$DATA_DIR/chat_unknown.json"; echo

if [[ "$STATUS" != "400" ]]; then
  echo "error: expected HTTP 400 for unknown model" >&2
  echo "--- server log (last 200 lines)" >&2
  tail -n 200 "$LOG_FILE" >&2 || true
  exit 1
fi

if [[ "$MODE" == "real-agent" ]]; then
  if ! command -v claude >/dev/null 2>&1; then
    echo "error: --real-agent requires 'claude' in PATH" >&2
    exit 1
  fi
  if ! command -v python3 >/dev/null 2>&1; then
    echo "error: --real-agent requires python3 (used for minimal JSON parsing)" >&2
    exit 1
  fi

  echo "--- real-agent: POST /api/sessions (claude-code)"
  if ! curl_json POST "http://$HOST:$PORT/api/sessions" '{"agent":"claude-code","cwd":"/tmp","mcp_servers":[]}' \
    >"$DATA_DIR/create.json"; then
    echo "error: create session request failed" >&2
    echo "--- server log (last 200 lines)" >&2
    tail -n 200 "$LOG_FILE" >&2 || true
    exit 1
  fi

  SESSION_ID=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["session_id"])' <"$DATA_DIR/create.json")
  if [[ -z "$SESSION_ID" ]]; then
    echo "error: failed to parse session_id from create response" >&2
    cat "$DATA_DIR/create.json" >&2 || true
    exit 1
  fi
  echo "session_id: $SESSION_ID"

  echo "--- real-agent: POST /api/sessions/:id/prompt"
  if ! curl_json POST "http://$HOST:$PORT/api/sessions/$SESSION_ID/prompt" '{"content":"Reply with exactly: gateway real-agent smoke ok"}' \
    >/dev/null; then
    echo "error: prompt request failed" >&2
    echo "--- server log (last 200 lines)" >&2
    tail -n 200 "$LOG_FILE" >&2 || true
    exit 1
  fi

  echo "--- real-agent: poll /api/sessions/:id/events (timeout=${TIMEOUT_SECONDS}s)"
  deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
  total="0"
  while [[ $(date +%s) -lt $deadline ]]; do
    curl -fsS \
      --connect-timeout "$CONNECT_TIMEOUT_SECONDS" \
      --max-time "$HTTP_MAX_TIME_SECONDS" \
      "http://$HOST:$PORT/api/sessions/$SESSION_ID/events?from=0&limit=50" \
      >"$DATA_DIR/events.json"
    total=$(python3 -c 'import json,sys; print(int(json.load(sys.stdin).get("total", 0)))' <"$DATA_DIR/events.json")
    if [[ "$total" -gt 0 ]]; then
      break
    fi
    sleep 0.5
  done

  if [[ "$total" -le 0 ]]; then
    echo "error: timed out waiting for events" >&2
    echo "--- events response" >&2
    cat "$DATA_DIR/events.json" >&2 || true
    echo "--- server log (last 200 lines)" >&2
    tail -n 200 "$LOG_FILE" >&2 || true
    exit 1
  fi

  echo "ok: received $total event(s)"

  echo "--- real-agent: DELETE /api/sessions/:id"
  if ! curl -fsS \
    --connect-timeout "$CONNECT_TIMEOUT_SECONDS" \
    --max-time "$HTTP_MAX_TIME_SECONDS" \
    -X DELETE "http://$HOST:$PORT/api/sessions/$SESSION_ID" >/dev/null; then
    echo "error: delete request failed" >&2
    echo "--- server log (last 200 lines)" >&2
    tail -n 200 "$LOG_FILE" >&2 || true
    exit 1
  fi
fi

echo "ok: smoke test passed"

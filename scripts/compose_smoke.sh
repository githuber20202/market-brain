#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

test -f .env || { echo "ERROR: .env missing"; exit 2; }
grep -q '^EXECUTION_ACTIONS_ALLOWED=false$' .env || { echo "ERROR: automatic execution must remain disabled"; exit 2; }
grep -q '^DIRECT_ACCOUNT_ACCESS_ALLOWED=false$' .env || { echo "ERROR: direct account access must remain disabled"; exit 2; }

PROJECT_NAME="${COMPOSE_SMOKE_PROJECT_NAME:-market-brain-smoke}"
PORT="${COMPOSE_SMOKE_PORT:-18080}"
export API_BIND_PORT="$PORT"
compose=(docker compose -p "$PROJECT_NAME")
shadow_report_host=""
cleanup() {
  if [[ -n "$shadow_report_host" ]]; then
    rm -f -- "$shadow_report_host"
  fi
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

cleanup
"${compose[@]}" up -d --build postgres nats api

health=""
for _ in $(seq 1 60); do
  if health=$(curl -fsS "http://127.0.0.1:${PORT}/health" 2>/dev/null); then
    break
  fi
  sleep 1
done
if [[ -z "$health" ]]; then
  "${compose[@]}" ps
  "${compose[@]}" logs api postgres nats || true
  echo "COMPOSE_SMOKE=FAIL API_HEALTH_TIMEOUT"
  exit 1
fi

HEALTH_JSON="$health" python - <<'PY'
import json, os
payload = json.loads(os.environ['HEALTH_JSON'])
assert payload['status'] == 'ok', payload
assert payload['architecture'] == 'BROKERLESS_EVENT_SOURCED', payload
assert payload['execution_actions_allowed'] is False, payload
assert payload['direct_account_access_allowed'] is False, payload
PY

replay=$(curl -fsS "http://127.0.0.1:${PORT}/admin/replay-check")
REPLAY_JSON="$replay" python - <<'PY'
import json, os
payload = json.loads(os.environ['REPLAY_JSON'])
assert payload['ok'] is True, payload
assert payload['differences'] == [], payload
PY

"${compose[@]}" run --rm api python -m market_brain.ops.preflight --offline
echo "PREFLIGHT_OFFLINE=PASS"

shadow_report_output=$("${compose[@]}" run --rm api python scripts/shadow_report.py)
printf '%s\n' "$shadow_report_output"
while IFS= read -r line; do
  if [[ "$line" =~ ^/app/reports/(shadow_[0-9]{4}-W[0-9]{2}\.md)$ ]]; then
    shadow_report_host="reports/${BASH_REMATCH[1]}"
  fi
done <<< "$shadow_report_output"
if [[ -z "$shadow_report_host" || ! -s "$shadow_report_host" ]]; then
  echo "COMPOSE_SMOKE=FAIL SHADOW_REPORT_NOT_PERSISTED"
  exit 1
fi
grep -q '^# Shadow report:' "$shadow_report_host"
echo "SHADOW_REPORT_SMOKE=PASS path=$shadow_report_host"
rm -f -- "$shadow_report_host"
shadow_report_host=""

echo "COMPOSE_SMOKE=PASS"

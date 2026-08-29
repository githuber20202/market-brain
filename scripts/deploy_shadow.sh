#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
test -f .env || { echo "ERROR: .env missing"; exit 2; }
grep -q '^EXECUTION_ACTIONS_ALLOWED=false$' .env || { echo "ERROR: automatic execution must remain disabled"; exit 2; }
grep -q '^DIRECT_ACCOUNT_ACCESS_ALLOWED=false$' .env || { echo "ERROR: direct account access must remain disabled"; exit 2; }
grep -q '^ALPACA_API_KEY=.\+' .env || { echo "ERROR: market-data key missing"; exit 2; }
grep -q '^ALPACA_API_SECRET=.\+' .env || { echo "ERROR: market-data secret missing"; exit 2; }
python scripts/validate_runtime.py
docker compose config >/dev/null
docker compose up -d --build
docker compose --profile live up -d stream-worker
echo "Brokerless shadow runtime started. No financial-account access or automatic execution exists."


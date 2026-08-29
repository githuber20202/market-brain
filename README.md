# MARKET BRAIN V4 — Brokerless Advisory Runtime

Market Brain runs in Shadow mode using delayed public market data. It produces deterministic advisory plans and alerts, never executes, modifies, or cancels broker orders, and is not financial advice.

## Current status

This repository is CI-validated engineering software and is **not declared production READY**. Production/advisory readiness additionally requires an always-on deployment, credentialed live market-data operation, configured alert delivery, current reconciliation, and multi-session shadow validation.

## Runtime components

- FastAPI advisory API.
- PostgreSQL event/materialized state store, with in-memory development fallback.
- Alpaca market-data adapters.
- NATS market-event transport.
- Resilient stream-worker with reconnect/staleness handling and throttled status writes.
- Cached PositionMonitor with persistent `triggered_at`, deterministic exit evaluation, and alert dedupe.
- Webhook/Telegram alert dispatcher when configured.

## Runtime-authoritative configuration

`config/` contains only files consumed during startup/runtime:

- `00-V4_MANIFEST.json` — loaded by `market_brain.settings`; startup fails closed if the config set drifts.
- `02-RISK_ENVELOPE.json` — the single source for trade-risk, daily-loss, position-notional, concurrent-position, and automatic-execution defaults.
- `schema.sql` — validated by `market_brain.settings` at startup and mounted into PostgreSQL initialization by Docker Compose.

There is no `05-RUNTIME_CONFIG.json`. Environment variables may override Settings fields, including `MAX_POSITION_NOTIONAL_PCT` and `MAX_CONCURRENT_POSITIONS`; the example env file intentionally does not duplicate their numeric defaults.

## Risk behavior

New system-managed entries are constrained by the active risk envelope. The same Settings values are used by sizing, activation, trigger advisory sizing, and confirmed-fill hard guards. Manual/imported holdings remain truth even when the user already holds more positions than the entry envelope permits; that state blocks additional managed entries rather than hiding holdings.

## Plan construction

`POST /plans` fetches price/bar data server-side. Opening Range uses 1–5 consecutive closed one-minute bars beginning at 09:30 ET. Missing/non-consecutive bars fail closed. `TRIGGER_HIT` is advisory only; `POST /plans/{plan_id}/activate` is the separate deterministic activation step.

## Operating loop

`TRIGGER_HIT` / `BUY_NOW` → enter the order and protective stop manually in the broker → `POST /fills/confirm` with `stop_order_placed=true` → monitor advisory alerts → `POST /positions/{position_id}/exit` after a user-confirmed exit → `POST /reconcile` daily.

## Actual API endpoints

- `GET /health`
- `GET /policy`
- `GET /admin/replay-check`
- `GET /alerts?undelivered=true`
- `POST /screen`
- `POST /wallet/seed`
- `GET /wallet`
- `POST /plans`
- `POST /plans/{plan_id}/activate`
- `POST /plans/{plan_id}/release`
- `POST /fills/confirm`
- `GET /positions`
- `POST /positions/import`
- `POST /reconcile`
- `POST /positions/{position_id}/protect`
- `POST /positions/{position_id}/evaluate`
- `POST /positions/{position_id}/exit`

## Not wired yet

- `data/universe/03-WATCH_UNIVERSE.csv` and `data/universe/04-MARKET_CORE_UNIVERSE.csv` are retained reference universes and are **not loaded by runtime yet**.

## Not implemented

- Slow Brain / fundamentals pipeline.
- Backtest framework.
- Operator console.
- Universe loader.
- Server-derived retest validation; `retest_valid` is still manual input to `POST /plans/{plan_id}/activate`.

## Validation

```bash
python -m pytest -q
python -m pytest -m postgres -q --strict-markers
python scripts/validate_runtime.py
cp .env.example .env
docker compose config -q
./scripts/compose_smoke.sh
```

The compose smoke starts a fresh isolated PostgreSQL + NATS + API stack, waits for `/health`, confirms account/execution access remain disabled, verifies clean replay, and tears the stack down. It does not start the live stream-worker.

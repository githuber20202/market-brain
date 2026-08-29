CREATE TABLE IF NOT EXISTS decision_events (
  event_id UUID PRIMARY KEY,
  event_type TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  payload JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_decision_events_aggregate ON decision_events(aggregate_id, occurred_at);

CREATE TABLE IF NOT EXISTS trade_plans (
  plan_id UUID PRIMARY KEY,
  symbol TEXT NOT NULL,
  status TEXT NOT NULL,
  plan_json JSONB NOT NULL,
  triggered_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE trade_plans ADD COLUMN IF NOT EXISTS triggered_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS risk_wallet (
  singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
  wallet_json JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS reservations (
  plan_id UUID PRIMARY KEY,
  reservation_json JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS position_twin (
  position_id UUID PRIMARY KEY,
  symbol TEXT NOT NULL,
  position_json JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS alerts (
  alert_id UUID PRIMARY KEY,
  kind TEXT NOT NULL,
  payload_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  delivered_at TIMESTAMPTZ,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  next_attempt_at TIMESTAMPTZ
);
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_alerts_undelivered ON alerts(delivered_at, created_at);
CREATE INDEX IF NOT EXISTS idx_alerts_next_attempt ON alerts(delivered_at, next_attempt_at, created_at);

CREATE TABLE IF NOT EXISTS counterfactual_outcomes (
  plan_id UUID PRIMARY KEY,
  outcome_json JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS liquidity_profiles (
  symbol TEXT PRIMARY KEY,
  adv20 DOUBLE PRECISION NOT NULL,
  close DOUBLE PRECISION NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,
  refreshed_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS runtime_status (
  key TEXT PRIMARY KEY,
  value_json JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);


CREATE TABLE IF NOT EXISTS intraday_bars (
  symbol TEXT NOT NULL,
  session_date DATE NOT NULL,
  minute_ts TIMESTAMPTZ NOT NULL,
  source TEXT NOT NULL,
  open DOUBLE PRECISION NOT NULL,
  high DOUBLE PRECISION NOT NULL,
  low DOUBLE PRECISION NOT NULL,
  close DOUBLE PRECISION NOT NULL,
  volume DOUBLE PRECISION,
  vwap DOUBLE PRECISION,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(symbol, session_date, minute_ts, source)
);
CREATE INDEX IF NOT EXISTS idx_intraday_bars_symbol_session
  ON intraday_bars(symbol, session_date, minute_ts);

CREATE TABLE IF NOT EXISTS shadow_trades (
  trade_id UUID PRIMARY KEY,
  plan_id UUID NOT NULL UNIQUE,
  symbol TEXT NOT NULL,
  status TEXT NOT NULL,
  trade_json JSONB NOT NULL,
  opened_at TIMESTAMPTZ NOT NULL,
  closed_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_shadow_trades_status_opened
  ON shadow_trades(status, opened_at);

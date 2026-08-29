from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = ROOT / "config"
DATA_DIR = ROOT / "data"
MANIFEST_PATH = CONFIG_DIR / "00-V4_MANIFEST.json"
RISK_ENVELOPE_PATH = CONFIG_DIR / "02-RISK_ENVELOPE.json"
SCHEMA_PATH = CONFIG_DIR / "schema.sql"


def _load_json(path: Path, label: str) -> dict:
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"{label}_INVALID") from exc
    if not isinstance(payload, dict):
        raise TypeError(f"{label}_INVALID")
    return payload


def _load_runtime_contract() -> tuple[dict, dict]:
    manifest = _load_json(MANIFEST_PATH, "V4_MANIFEST")
    canonical = manifest.get("canonical_files")
    if not isinstance(canonical, list) or not all(isinstance(row, str) for row in canonical):
        raise RuntimeError("V4_MANIFEST_CANONICAL_FILES_INVALID")
    expected = {"00-V4_MANIFEST.json", *canonical}
    try:
        actual = {path.name for path in CONFIG_DIR.iterdir() if path.is_file()}
    except OSError as exc:
        raise RuntimeError("CONFIG_DIRECTORY_UNREADABLE") from exc
    if actual != expected:
        raise RuntimeError(
            f"CONFIG_SET_MISMATCH missing={sorted(expected-actual)} extra={sorted(actual-expected)}"
        )

    risk = _load_json(RISK_ENVELOPE_PATH, "RISK_ENVELOPE")
    required_risk = {
        "max_trade_risk_pct",
        "max_daily_loss_pct",
        "max_position_notional_pct",
        "max_concurrent_positions",
        "automatic_execution_allowed",
    }
    if set(risk) != required_risk:
        raise RuntimeError("RISK_ENVELOPE_KEYS_INVALID")
    if risk["automatic_execution_allowed"] is not False:
        raise RuntimeError("AUTOMATIC_EXECUTION_FORBIDDEN")

    try:
        schema = SCHEMA_PATH.read_text()
    except OSError as exc:
        raise RuntimeError("SCHEMA_CONFIG_INVALID") from exc
    required_tables = (
        "decision_events",
        "trade_plans",
        "risk_wallet",
        "reservations",
        "position_twin",
        "alerts",
        "runtime_status",
        "liquidity_profiles",
        "intraday_bars",
        "shadow_trades",
    )
    missing_tables = [name for name in required_tables if f"TABLE IF NOT EXISTS {name}" not in schema]
    if missing_tables:
        raise RuntimeError(f"SCHEMA_CONTRACT_MISSING={missing_tables}")
    return manifest, risk


V4_MANIFEST, RISK_ENVELOPE = _load_runtime_contract()


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    execution_actions_allowed: bool = bool(RISK_ENVELOPE["automatic_execution_allowed"])
    direct_account_access_allowed: bool = False
    strategy_speculative_enabled: bool = False

    data_plan: Literal["keyless_delayed", "free", "plus"] = "keyless_delayed"
    discovery_feed: str = "iex"
    decision_feed: str = "iex"
    historical_feed: str = "sip"
    historical_lag_minutes: int = 16
    alpaca_api_key: str | None = None
    alpaca_api_secret: str | None = None
    alpaca_data_base_url: str = "https://data.alpaca.markets"
    alpaca_stream_url: str = "wss://stream.data.alpaca.markets/v2/iex"
    rest_calls_per_minute: int = 200
    rest_safe_calls_per_minute: int = 180
    rest_batch_symbols: int = 100
    keyless_calls_per_minute: int = 120
    keyless_request_interval_seconds: float = 0.5
    keyless_retry_attempts: int = 3
    stream_max_symbols: int = 30
    universe_dir: Path = DATA_DIR / "universe"
    quality_path: Path = DATA_DIR / "quality.csv"
    market_calendar_path: Path = DATA_DIR / "market_calendar.csv"
    plans_per_run: int = 5
    radar_poll_seconds: float = 5.0
    run_mode: Literal["shadow", "live"] = "shadow"

    postgres_dsn: str | None = None
    nats_url: str | None = None
    webhook_url: str | None = None
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None
    alert_poll_seconds: float = 2.0
    alert_max_attempts: int = 6

    max_trade_risk_pct: float = float(RISK_ENVELOPE["max_trade_risk_pct"])
    max_daily_loss_pct: float = float(RISK_ENVELOPE["max_daily_loss_pct"])
    max_position_notional_pct: float = float(RISK_ENVELOPE["max_position_notional_pct"])
    max_concurrent_positions: int = int(RISK_ENVELOPE["max_concurrent_positions"])
    plan_ttl_seconds: int = 300
    reservation_ttl_seconds: int = 120
    max_market_data_age_seconds: float = 15.0
    max_quote_age_seconds: float = 5.0
    intraday_opening_range_minutes: int = 5
    retest_touch_tolerance_pct: float = 0.15
    retest_invalidation_buffer_r: float = 0.25
    retest_window_minutes: int = 30
    intraday_backfill_interval_seconds: float = 300.0
    min_adv: float = 2_000_000.0
    min_adv_keyless: float = 5_000_000.0
    max_delayed_age_minutes: float = 20.0
    min_price: float = 5.0
    max_spread_bps: float = 20.0
    iex_mid_tolerance_pct: float = 0.75
    stream_stale_seconds: float = 30.0
    stream_stale_alert_seconds: float = 120.0
    stream_subscription_refresh_seconds: float = 15.0
    status_write_interval_seconds: float = 1.0
    monitor_cache_refresh_seconds: float = 5.0
    monitor_min_interval_seconds: float = 1.0
    failed_breakout_buffer_r: float = 0.25
    alert_repeat_minutes: float = 5.0
    reconciliation_max_age_hours: float = 24.0
    fill_risk_tolerance: float = 0.20
    stop_tolerance_pct: float = 0.10
    authoritative_source_ids: str = "ALPACA_IEX,ALPACA_SIP,MARKET_QUORUM"

    @model_validator(mode="after")
    def enforce_boundaries(self) -> Settings:
        if self.execution_actions_allowed:
            raise ValueError("AUTOMATIC_EXECUTION_FORBIDDEN")
        if self.direct_account_access_allowed:
            raise ValueError("DIRECT_ACCOUNT_ACCESS_FORBIDDEN")
        if self.data_plan == "free":
            self.discovery_feed = "iex"
            self.decision_feed = "iex"
            self.historical_feed = "sip"
            self.alpaca_stream_url = "wss://stream.data.alpaca.markets/v2/iex"
            self.historical_lag_minutes = max(16, self.historical_lag_minutes)
            if self.stream_max_symbols > 30:
                raise ValueError("FREE_PLAN_STREAM_CAP_EXCEEDED")
            if self.rest_calls_per_minute > 200:
                raise ValueError("FREE_PLAN_REST_LIMIT_EXCEEDED")
            if self.rest_safe_calls_per_minute > 180:
                raise ValueError("FREE_PLAN_REST_SAFE_LIMIT_EXCEEDED")
        elif self.data_plan == "keyless_delayed":
            self.discovery_feed = "yahoo"
            self.decision_feed = "yahoo"
            self.historical_feed = "yahoo"
        if self.historical_lag_minutes < 0:
            raise ValueError("INVALID_HISTORICAL_LAG")
        if self.rest_calls_per_minute <= 0:
            raise ValueError("INVALID_REST_CALL_LIMIT")
        if self.rest_safe_calls_per_minute <= 0 or self.rest_safe_calls_per_minute > self.rest_calls_per_minute:
            raise ValueError("INVALID_REST_SAFE_CALL_LIMIT")
        if self.rest_batch_symbols <= 0:
            raise ValueError("INVALID_REST_BATCH_SIZE")
        if self.keyless_calls_per_minute <= 0:
            raise ValueError("INVALID_KEYLESS_CALL_LIMIT")
        if self.keyless_request_interval_seconds < 0.5:
            raise ValueError("KEYLESS_REQUEST_INTERVAL_TOO_LOW")
        if self.keyless_retry_attempts <= 0:
            raise ValueError("INVALID_KEYLESS_RETRY_ATTEMPTS")
        if self.stream_max_symbols <= 0:
            raise ValueError("INVALID_STREAM_SYMBOL_CAP")
        if self.plans_per_run <= 0 or self.plans_per_run > self.stream_max_symbols:
            raise ValueError("INVALID_PLANS_PER_RUN")
        if self.radar_poll_seconds <= 0:
            raise ValueError("INVALID_RADAR_POLL_SECONDS")
        if self.stream_stale_alert_seconds <= 0:
            raise ValueError("INVALID_STREAM_STALE_ALERT_SECONDS")
        if self.max_trade_risk_pct <= 0 or self.max_trade_risk_pct > 1.0:
            raise ValueError("INVALID_MAX_TRADE_RISK")
        if self.max_daily_loss_pct <= 0 or self.max_daily_loss_pct > 2.0:
            raise ValueError("INVALID_MAX_DAILY_LOSS")
        if self.max_position_notional_pct <= 0 or self.max_position_notional_pct > 100.0:
            raise ValueError("INVALID_MAX_POSITION_NOTIONAL")
        if self.max_concurrent_positions <= 0:
            raise ValueError("INVALID_MAX_CONCURRENT_POSITIONS")
        if self.fill_risk_tolerance < 0 or self.fill_risk_tolerance > 1.0:
            raise ValueError("INVALID_FILL_RISK_TOLERANCE")
        if self.stop_tolerance_pct < 0 or self.stop_tolerance_pct > 5.0:
            raise ValueError("INVALID_STOP_TOLERANCE")
        if self.alert_poll_seconds <= 0:
            raise ValueError("INVALID_ALERT_POLL_SECONDS")
        if self.alert_max_attempts <= 0:
            raise ValueError("INVALID_ALERT_MAX_ATTEMPTS")
        if self.reconciliation_max_age_hours <= 0:
            raise ValueError("INVALID_RECONCILIATION_MAX_AGE")
        if self.status_write_interval_seconds <= 0:
            raise ValueError("INVALID_STATUS_WRITE_INTERVAL")
        if self.monitor_cache_refresh_seconds <= 0:
            raise ValueError("INVALID_MONITOR_CACHE_REFRESH")
        if self.monitor_min_interval_seconds <= 0:
            raise ValueError("INVALID_MONITOR_MIN_INTERVAL")
        if self.max_quote_age_seconds <= 0:
            raise ValueError("INVALID_MAX_QUOTE_AGE_SECONDS")
        if self.intraday_opening_range_minutes <= 0 or self.intraday_opening_range_minutes > 30:
            raise ValueError("INVALID_INTRADAY_OPENING_RANGE_MINUTES")
        if self.retest_touch_tolerance_pct <= 0 or self.retest_touch_tolerance_pct > 5.0:
            raise ValueError("INVALID_RETEST_TOUCH_TOLERANCE_PCT")
        if self.retest_invalidation_buffer_r < 0 or self.retest_invalidation_buffer_r > 1.0:
            raise ValueError("INVALID_RETEST_INVALIDATION_BUFFER_R")
        if self.retest_window_minutes <= 0 or self.retest_window_minutes > 120:
            raise ValueError("INVALID_RETEST_WINDOW_MINUTES")
        if self.intraday_backfill_interval_seconds <= 0:
            raise ValueError("INVALID_INTRADAY_BACKFILL_INTERVAL")
        if self.min_adv <= 0:
            raise ValueError("INVALID_MIN_ADV")
        if self.min_adv_keyless <= 0:
            raise ValueError("INVALID_MIN_ADV_KEYLESS")
        if self.max_delayed_age_minutes <= 0:
            raise ValueError("INVALID_MAX_DELAYED_AGE_MINUTES")
        if self.min_price <= 0:
            raise ValueError("INVALID_MIN_PRICE")
        if self.max_spread_bps <= 0:
            raise ValueError("INVALID_MAX_SPREAD_BPS")
        if self.iex_mid_tolerance_pct <= 0 or self.iex_mid_tolerance_pct > 10.0:
            raise ValueError("INVALID_IEX_MID_TOLERANCE_PCT")
        if self.failed_breakout_buffer_r < 0 or self.failed_breakout_buffer_r > 1.0:
            raise ValueError("INVALID_FAILED_BREAKOUT_BUFFER_R")
        if self.alert_repeat_minutes <= 0:
            raise ValueError("INVALID_ALERT_REPEAT_MINUTES")
        return self


settings = Settings()

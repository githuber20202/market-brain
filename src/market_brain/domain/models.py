from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4


def utc_now() -> datetime:
    return datetime.now(UTC)


class StrategyLane(StrEnum):
    CORE_MOMENTUM = "CORE_MOMENTUM"
    EVENT_MOMENTUM = "EVENT_MOMENTUM"
    SPECULATIVE = "SPECULATIVE"


class SignalState(StrEnum):
    DISCOVERED = "DISCOVERED"
    WATCH = "WATCH"
    ARMED = "ARMED"
    BUY_NOW = "BUY_NOW"
    EXPIRED = "EXPIRED"
    INVALID = "INVALID"
    NO_TRADE = "NO_TRADE"


class PositionAction(StrEnum):
    HOLD = "HOLD"
    TRIM = "TRIM"
    TAKE_PROFIT = "TAKE_PROFIT"
    SELL_NOW = "SELL_NOW"
    UNKNOWN_POSITION = "UNKNOWN_POSITION"
    PLACE_STOP_NOW = "PLACE_STOP_NOW"
    RECONCILE_REQUIRED = "RECONCILE_REQUIRED"


class ProtectionState(StrEnum):
    PROTECTED = "PROTECTED"
    UNPROTECTED = "UNPROTECTED"


class ReconciliationState(StrEnum):
    RECONCILED = "RECONCILED"
    UNRECONCILED = "UNRECONCILED"
    UNRECONCILED_MISSING_AT_BROKER = "UNRECONCILED_MISSING_AT_BROKER"


class PlanStatus(StrEnum):
    ACTIVE = "ACTIVE"
    RESERVED = "RESERVED"
    FILLED = "FILLED"
    EXPIRED = "EXPIRED"
    CANCELLED = "CANCELLED"


class ShadowTradeStatus(StrEnum):
    OPEN = "OPEN"
    TP1 = "TP1"
    STOPPED = "STOPPED"
    TP2 = "TP2"
    TIME_STOP = "TIME_STOP"


class IntradayStructureState(StrEnum):
    BUILDING_OR = "BUILDING_OR"
    ARMED = "ARMED"
    BREAKOUT_SEEN = "BREAKOUT_SEEN"
    RETEST_VALID = "RETEST_VALID"
    INVALID = "INVALID"


@dataclass(slots=True)
class EvidenceCard:
    evidence_type: str
    summary: str
    source: str
    published_at: datetime
    confidence: float
    expires_at: datetime
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class QualityProfile:
    symbol: str
    score: float
    tier: str
    risk_multiplier: float
    as_of: datetime
    evidence: list[EvidenceCard] = field(default_factory=list)


@dataclass(slots=True)
class IntradayStructure:
    symbol: str
    session_date: str
    state: IntradayStructureState = IntradayStructureState.BUILDING_OR
    opening_range_high: float | None = None
    opening_range_low: float | None = None
    opening_bars_seen: int = 0
    running_vwap: float | None = None
    cumulative_volume: float = 0.0
    cumulative_vwap_notional: float = 0.0
    last_bar_at: datetime | None = None
    last_close: float | None = None
    breakout_at: datetime | None = None
    retest_at: datetime | None = None
    reasons: list[str] = field(default_factory=list)
    updated_at: datetime = field(default_factory=utc_now)


@dataclass(slots=True)
class IntradayBarRecord:
    symbol: str
    session_date: str
    minute_ts: datetime
    source: str
    open: float
    high: float
    low: float
    close: float
    volume: float | None = None
    vwap: float | None = None

    def as_market_bar(self) -> dict[str, Any]:
        return {
            "t": self.minute_ts.isoformat(),
            "o": self.open,
            "h": self.high,
            "l": self.low,
            "c": self.close,
            "v": self.volume,
            "vw": self.vwap,
            "source": self.source,
        }


@dataclass(slots=True)
class LiquidityProfile:
    symbol: str
    adv20: float
    close: float
    as_of: datetime
    refreshed_at: datetime = field(default_factory=utc_now)


@dataclass(slots=True)
class MarketSnapshot:
    symbol: str
    last: float
    prior_close: float | None = None
    bid: float | None = None
    ask: float | None = None
    volume: float | None = None
    avg_volume: float | None = None
    vwap: float | None = None
    open_price: float | None = None
    high: float | None = None
    low: float | None = None
    opening_range_high: float | None = None
    retest_low: float | None = None
    atr_1m: float | None = None
    sector_return_pct: float | None = None
    benchmark_return_pct: float | None = None
    catalyst_verified: bool = False
    catalyst_strength: float = 0.0
    data_age_seconds: float | None = None
    source_id: str | None = None
    delay_minutes: float | None = None
    fetched_at: datetime | None = None
    authoritative: bool = False
    halted: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class FeatureVector:
    symbol: str
    price_return_pct: float | None
    gap_pct: float | None
    relative_volume: float | None
    spread_pct: float | None
    distance_from_vwap_pct: float | None
    relative_strength_pct: float | None
    catalyst_strength: float
    liquidity_ok: bool
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ScoreCard:
    symbol: str
    catalyst_or_continuation: float
    price_momentum: float
    volume_liquidity: float
    relative_strength_sector: float
    entry_invalidation_structure: float
    risk_reward: float
    total: float
    discovery_total: float
    reasons: list[str] = field(default_factory=list)


@dataclass(slots=True)
class TradePlan:
    symbol: str
    lane: StrategyLane
    entry_trigger: float
    entry_zone_high: float
    stop: float | None
    tp1: float | None
    tp2: float | None
    max_spread_pct: float
    max_slippage_pct: float
    created_at: datetime
    expires_at: datetime
    quality_risk_multiplier: float
    plan_id: str = field(default_factory=lambda: str(uuid4()))
    status: PlanStatus = PlanStatus.ACTIVE
    reasons: list[str] = field(default_factory=list)
    evidence_hash: str = ""
    source_ids: list[str] = field(default_factory=list)
    triggered_at: datetime | None = None

    @property
    def risk_per_share(self) -> float:
        return self.entry_trigger - self.stop


@dataclass(slots=True)
class WalletState:
    capital_base: float
    cash_available: float
    daily_realized_loss: float = 0.0
    open_risk: float = 0.0
    reserved_cash: float = 0.0
    version: int = 1
    as_of: datetime = field(default_factory=utc_now)


@dataclass(slots=True)
class Reservation:
    plan_id: str
    quantity: int
    reserved_cash: float
    reserved_risk: float
    expires_at: datetime


@dataclass(slots=True)
class AlertRecord:
    kind: str
    payload: dict[str, Any]
    alert_id: str = field(default_factory=lambda: str(uuid4()))
    created_at: datetime = field(default_factory=utc_now)
    delivered_at: datetime | None = None
    attempts: int = 0
    last_error: str | None = None
    next_attempt_at: datetime | None = None


@dataclass(slots=True)
class ActivationDecision:
    plan_id: str
    symbol: str
    state: SignalState
    reasons: list[str]
    quantity: int = 0
    entry: float | None = None
    stop: float | None = None
    tp1: float | None = None
    tp2: float | None = None
    order_ticket: dict[str, Any] | None = None
    alert_id: str | None = None


@dataclass(slots=True)
class PositionState:
    position_id: str
    plan_id: str
    symbol: str
    quantity: int
    remaining_quantity: int
    average_fill: float
    stop: float | None
    tp1: float | None
    tp2: float | None
    opened_at: datetime
    time_stop_at: datetime | None
    realized_pnl: float = 0.0
    managed: bool = True
    source: str = "FILL_ACK"
    closed_at: datetime | None = None
    protection: ProtectionState = ProtectionState.UNPROTECTED
    broker_stop_price: float | None = None
    broker_order_ref: str | None = None
    protected_quantity: int = 0
    reconciliation_state: ReconciliationState = ReconciliationState.UNRECONCILED
    last_reconciled_at: datetime | None = None

    @property
    def entry_avg(self) -> float:
        return self.average_fill

    @entry_avg.setter
    def entry_avg(self, value: float) -> None:
        self.average_fill = value


@dataclass(slots=True)
class ShadowTrade:
    trade_id: str
    plan_id: str
    symbol: str
    setup: str
    quantity: int
    trigger: float
    fill: float
    stop: float
    tp1: float
    tp2: float
    opened_at: datetime
    time_stop_at: datetime
    status: ShadowTradeStatus = ShadowTradeStatus.OPEN
    remaining_fraction: float = 1.0
    tp1_taken: bool = False
    realized_r: float = 0.0
    exit_legs: list[dict[str, Any]] = field(default_factory=list)
    last_bar_at: datetime | None = None
    closed_at: datetime | None = None

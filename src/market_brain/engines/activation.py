from __future__ import annotations

from datetime import UTC, datetime

from market_brain.domain.models import (
    ActivationDecision,
    MarketSnapshot,
    SignalState,
    TradePlan,
    WalletState,
)
from market_brain.engines.wallet import size_from_wallet
from market_brain.settings import settings as runtime_settings


def activate_plan(
    plan: TradePlan,
    snapshot: MarketSnapshot,
    wallet: WalletState,
    *,
    retest_valid: bool,
    above_vwap: bool,
    max_data_age_seconds: float | None = None,
    max_trade_risk_pct: float | None = None,
    max_daily_loss_pct: float | None = None,
    max_position_notional_pct: float | None = None,
    now: datetime | None = None,
) -> ActivationDecision:
    data_age_limit = (
        runtime_settings.max_market_data_age_seconds
        if max_data_age_seconds is None
        else max_data_age_seconds
    )
    timestamp = now or datetime.now(UTC)
    reasons: list[str] = []
    if timestamp > plan.expires_at:
        return ActivationDecision(plan.plan_id, plan.symbol, SignalState.EXPIRED, ["PLAN_EXPIRED"])
    if snapshot.halted:
        return ActivationDecision(plan.plan_id, plan.symbol, SignalState.INVALID, ["HALTED"])
    if snapshot.symbol != plan.symbol:
        return ActivationDecision(plan.plan_id, plan.symbol, SignalState.INVALID, ["SYMBOL_MISMATCH"])
    if not snapshot.authoritative:
        reasons.append("AUTHORITATIVE_MARKET_FEED_REQUIRED")
    if snapshot.data_age_seconds is None or snapshot.data_age_seconds > data_age_limit:
        reasons.append("MARKET_DATA_STALE")
    if snapshot.bid is None or snapshot.ask is None or snapshot.ask < snapshot.bid:
        reasons.append("BBO_MISSING")
    else:
        spread_pct = (snapshot.ask - snapshot.bid) / snapshot.last * 100.0
        if spread_pct > plan.max_spread_pct:
            reasons.append("SPREAD_TOO_WIDE")
    if not above_vwap:
        reasons.append("ABOVE_VWAP_REQUIRED")
    if not retest_valid:
        reasons.append("RETEST_REQUIRED")
    if snapshot.last < plan.entry_trigger:
        reasons.append("TRIGGER_NOT_REACHED")
    if snapshot.last > plan.entry_zone_high:
        reasons.append("NO_CHASE_ENTRY_ZONE_EXCEEDED")

    if reasons:
        state = SignalState.NO_TRADE if "NO_CHASE_ENTRY_ZONE_EXCEEDED" in reasons else SignalState.ARMED
        return ActivationDecision(plan.plan_id, plan.symbol, state, reasons)

    sized = size_from_wallet(
        wallet,
        plan,
        max_trade_risk_pct=max_trade_risk_pct,
        max_daily_loss_pct=max_daily_loss_pct,
        max_position_notional_pct=max_position_notional_pct,
    )
    if not sized.allowed:
        return ActivationDecision(plan.plan_id, plan.symbol, SignalState.WATCH, sized.reasons)

    return ActivationDecision(
        plan.plan_id,
        plan.symbol,
        SignalState.BUY_NOW,
        [],
        quantity=sized.quantity,
        entry=plan.entry_trigger,
        stop=plan.stop,
        tp1=plan.tp1,
        tp2=plan.tp2,
    )


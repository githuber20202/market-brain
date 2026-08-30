from __future__ import annotations

from dataclasses import dataclass
from math import floor

from market_brain.domain.models import TradePlan, WalletState
from market_brain.settings import settings as runtime_settings


@dataclass(slots=True)
class SizeResult:
    allowed: bool
    quantity: int
    risk_dollars: float
    cash_required: float
    reasons: list[str]


def size_from_wallet(
    wallet: WalletState,
    plan: TradePlan,
    *,
    entry_price: float | None = None,
    max_trade_risk_pct: float | None = None,
    max_daily_loss_pct: float | None = None,
    max_position_notional_pct: float | None = None,
) -> SizeResult:
    trade_risk_pct = (
        runtime_settings.max_trade_risk_pct
        if max_trade_risk_pct is None
        else max_trade_risk_pct
    )
    daily_loss_pct = (
        runtime_settings.max_daily_loss_pct
        if max_daily_loss_pct is None
        else max_daily_loss_pct
    )
    position_notional_pct = (
        runtime_settings.max_position_notional_pct
        if max_position_notional_pct is None
        else max_position_notional_pct
    )
    if wallet.capital_base <= 0 or wallet.cash_available <= 0:
        return SizeResult(False, 0, 0.0, 0.0, ["WALLET_NOT_SEEDED"])
    sizing_entry = plan.entry_zone_high if entry_price is None else entry_price
    risk_per_share = plan.risk_per_share if entry_price is None else entry_price - plan.stop
    if risk_per_share <= 0 or sizing_entry <= 0:
        return SizeResult(False, 0, 0.0, 0.0, ["INVALID_PLAN_RISK"])

    daily_cap = wallet.capital_base * daily_loss_pct / 100.0
    remaining_daily = daily_cap - wallet.daily_realized_loss - wallet.open_risk
    if remaining_daily <= 0:
        return SizeResult(False, 0, 0.0, 0.0, ["DAILY_RISK_LIMIT_REACHED"])

    trade_budget = wallet.capital_base * trade_risk_pct / 100.0
    budget = min(trade_budget, remaining_daily) * plan.quality_risk_multiplier
    available_cash = max(0.0, wallet.cash_available - wallet.reserved_cash)
    max_position_notional = wallet.capital_base * position_notional_pct / 100.0
    by_risk = floor(budget / risk_per_share)
    by_cash = floor(available_cash / sizing_entry)
    by_position_notional = floor(max_position_notional / sizing_entry)
    quantity = max(0, min(by_risk, by_cash, by_position_notional))
    if quantity <= 0:
        return SizeResult(False, 0, 0.0, 0.0, ["NO_CAPACITY"])

    risk_dollars = round(quantity * risk_per_share, 2)
    cash_required = round(quantity * sizing_entry, 2)
    return SizeResult(True, quantity, risk_dollars, cash_required, [])

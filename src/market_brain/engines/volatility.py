from __future__ import annotations

from market_brain.domain.models import LiquidityProfile, MarketSnapshot

ATR_PERIOD = 14


def wilder_atr(
    rows: list[tuple[float, float, float]],
    *,
    period: int = ATR_PERIOD,
) -> float | None:
    """Return Wilder ATR from ordered (high, low, close) daily rows."""
    if period <= 0 or len(rows) < period + 1:
        return None
    true_ranges: list[float] = []
    previous_close = rows[0][2]
    for high, low, close in rows[1:]:
        if high <= 0 or low <= 0 or close <= 0 or high < low:
            return None
        true_ranges.append(
            max(
                high - low,
                abs(high - previous_close),
                abs(low - previous_close),
            )
        )
        previous_close = close
    if len(true_ranges) < period:
        return None
    atr = sum(true_ranges[:period]) / period
    for true_range in true_ranges[period:]:
        atr = ((atr * (period - 1)) + true_range) / period
    return atr


def apply_volatility_context(
    snapshot: MarketSnapshot,
    profile: LiquidityProfile | None,
) -> MarketSnapshot:
    snapshot.atr14 = profile.atr14 if profile is not None else None
    snapshot.atr14_pct = (
        snapshot.atr14 / snapshot.last * 100.0
        if snapshot.atr14 is not None and snapshot.last > 0
        else (profile.atr14_pct if profile is not None else None)
    )
    snapshot.remaining_atr = None
    snapshot.remaining_atr_pct = None
    if (
        snapshot.atr14 is not None
        and snapshot.atr14 > 0
        and snapshot.prior_close is not None
        and snapshot.prior_close > 0
        and snapshot.last > 0
    ):
        # Consume the true range already traveled today. This catches both
        # upside extension and large intraday/downside whipsaws before a long entry.
        used_range = abs(snapshot.last - snapshot.prior_close)
        if (
            snapshot.high is not None
            and snapshot.low is not None
            and snapshot.high > 0
            and snapshot.low > 0
            and snapshot.high >= snapshot.low
        ):
            used_range = max(
                snapshot.high - snapshot.low,
                abs(snapshot.high - snapshot.prior_close),
                abs(snapshot.low - snapshot.prior_close),
            )
        snapshot.remaining_atr = max(0.0, snapshot.atr14 - used_range)
        snapshot.remaining_atr_pct = snapshot.remaining_atr / snapshot.last * 100.0
    return snapshot


def volatility_gate_reason(
    snapshot: MarketSnapshot,
    *,
    min_atr_pct: float,
) -> str | None:
    if snapshot.atr14 is None or snapshot.atr14_pct is None:
        return "ATR_MISSING"
    if snapshot.atr14 <= 0 or snapshot.atr14_pct < min_atr_pct:
        return "ATR_TOO_LOW"
    if snapshot.remaining_atr is None:
        return "ATR_REMAINING_MISSING"
    if snapshot.remaining_atr <= 0:
        return "ATR_EXHAUSTED"
    return None


def target_atr_budget_reason(
    snapshot: MarketSnapshot,
    *,
    target: float,
    multiplier: float,
) -> str | None:
    if snapshot.remaining_atr is None:
        return "ATR_REMAINING_MISSING"
    distance_to_target = max(0.0, target - snapshot.last)
    if distance_to_target > snapshot.remaining_atr * multiplier:
        return "TARGET_EXCEEDS_ATR_BUDGET"
    return None

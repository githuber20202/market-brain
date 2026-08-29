from __future__ import annotations

from market_brain.domain.models import LiquidityProfile, MarketSnapshot
from market_brain.settings import Settings


def apply_iex_liquidity_gate(
    snapshot: MarketSnapshot,
    profile: LiquidityProfile | None,
    cfg: Settings,
) -> list[str]:
    if snapshot.source_id != "ALPACA_IEX":
        return []

    reasons: list[str] = []
    if profile is None:
        reasons.append("LIQUIDITY_PROFILE_MISSING")
    elif profile.adv20 < cfg.min_adv:
        reasons.append("ADV_TOO_LOW")

    if snapshot.last < cfg.min_price:
        reasons.append("PRICE_TOO_LOW")

    if (
        snapshot.data_age_seconds is None
        or snapshot.data_age_seconds < 0
        or snapshot.data_age_seconds > cfg.max_quote_age_seconds
    ):
        reasons.append("QUOTE_STALE")

    bid = snapshot.bid
    ask = snapshot.ask
    if bid is None or ask is None or bid <= 0 or ask < bid:
        reasons.append("SPREAD_TOO_WIDE")
    else:
        mid = (bid + ask) / 2.0
        spread_bps = ((ask - bid) / mid) * 10_000.0 if mid > 0 else float("inf")
        if spread_bps > cfg.max_spread_bps:
            reasons.append("SPREAD_TOO_WIDE")
        if mid <= 0 or abs(snapshot.last - mid) / mid * 100.0 > cfg.iex_mid_tolerance_pct:
            reasons.append("PRICE_MID_SANITY_FAILED")

    snapshot.authoritative = not reasons
    return ["LIQUIDITY_GATE_PASS"] if snapshot.authoritative else list(dict.fromkeys(reasons))


def apply_keyless_liquidity_gate(
    snapshot: MarketSnapshot,
    profile: LiquidityProfile | None,
    cfg: Settings,
) -> list[str]:
    if not _is_keyless_source(snapshot.source_id):
        return []

    reasons: list[str] = []
    if profile is None:
        reasons.append("LIQUIDITY_PROFILE_MISSING")
    elif profile.adv20 < cfg.min_adv_keyless:
        reasons.append("ADV_TOO_LOW")
    if snapshot.last < cfg.min_price:
        reasons.append("PRICE_TOO_LOW")
    if (
        snapshot.delay_minutes is None
        or snapshot.delay_minutes < 0
        or snapshot.delay_minutes > cfg.max_delayed_age_minutes
    ):
        reasons.append("DELAYED_DATA_STALE")

    bid = snapshot.bid
    ask = snapshot.ask
    if bid is not None and ask is not None:
        if bid <= 0 or ask < bid:
            reasons.append("SPREAD_TOO_WIDE")
        else:
            mid = (bid + ask) / 2.0
            spread_bps = ((ask - bid) / mid) * 10_000.0 if mid > 0 else float("inf")
            if spread_bps > cfg.max_spread_bps:
                reasons.append("SPREAD_TOO_WIDE")

    high = snapshot.metadata.get("last_bar_high")
    low = snapshot.metadata.get("last_bar_low")
    try:
        range_pct = (float(high) - float(low)) / snapshot.last * 100.0
    except (TypeError, ValueError, ZeroDivisionError):
        range_pct = None
    if range_pct is None or range_pct < 0:
        reasons.append("KEYLESS_BAR_RANGE_MISSING")
    elif range_pct > cfg.keyless_max_bar_range_pct:
        reasons.append("KEYLESS_BAR_RANGE_TOO_WIDE")
    if snapshot.metadata.get("price_cross_check") == "FAIL":
        reasons.append("PRICE_CROSS_CHECK_FAILED")

    snapshot.authoritative = not reasons
    return ["LIQUIDITY_GATE_PASS"] if snapshot.authoritative else list(dict.fromkeys(reasons))


def _is_keyless_source(source_id: str | None) -> bool:
    return bool(source_id and ("YAHOO" in source_id or "CBOE_DELAYED" in source_id))

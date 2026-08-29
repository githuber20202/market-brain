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


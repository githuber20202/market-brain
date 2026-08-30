from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from market_brain.domain.models import FeatureVector, LiquidityProfile, MarketSnapshot

EASTERN = ZoneInfo("America/New_York")
SESSION_MINUTES = 390.0
MIN_ELAPSED_FRACTION = 0.05


def _pct(a: float | None, b: float | None) -> float | None:
    if a is None or b in (None, 0):
        return None
    return (a / b - 1.0) * 100.0


def price_return_pct(snapshot: MarketSnapshot) -> float | None:
    return _pct(snapshot.last, snapshot.prior_close)


def elapsed_session_fraction(now: datetime) -> float:
    local = now.astimezone(EASTERN)
    opened = local.replace(hour=9, minute=30, second=0, microsecond=0)
    elapsed_minutes = (local - opened).total_seconds() / 60.0
    return min(1.0, max(MIN_ELAPSED_FRACTION, elapsed_minutes / SESSION_MINUTES))


def apply_ranking_context(
    snapshot: MarketSnapshot,
    profile: LiquidityProfile | None,
    *,
    benchmark_return_pct: float | None,
    now: datetime,
) -> MarketSnapshot:
    fraction = elapsed_session_fraction(now)
    snapshot.avg_volume = profile.adv20 * fraction if profile is not None else None
    snapshot.benchmark_return_pct = benchmark_return_pct
    snapshot.metadata = {
        **snapshot.metadata,
        "expected_volume_fraction": fraction,
        "expected_volume_so_far": snapshot.avg_volume,
        "avg_volume_source": "LIQUIDITY_PROFILE_ADV20" if profile else None,
        "liquidity_profile_as_of": profile.as_of.isoformat() if profile else None,
        "benchmark_symbol": "SPY",
        "benchmark_return_pct": benchmark_return_pct,
    }
    return snapshot


def compute_features(snapshot: MarketSnapshot) -> FeatureVector:
    spread_pct = None
    if (
        snapshot.bid is not None
        and snapshot.ask is not None
        and snapshot.last > 0
        and snapshot.ask >= snapshot.bid
    ):
        spread_pct = ((snapshot.ask - snapshot.bid) / snapshot.last) * 100.0

    relative_volume = None
    if snapshot.volume is not None and snapshot.avg_volume not in (None, 0):
        relative_volume = snapshot.volume / snapshot.avg_volume

    price_return = price_return_pct(snapshot)
    gap = _pct(snapshot.open_price, snapshot.prior_close)
    distance_vwap = _pct(snapshot.last, snapshot.vwap)

    relative_strength = None
    if price_return is not None:
        anchor = (
            snapshot.sector_return_pct
            if snapshot.sector_return_pct is not None
            else snapshot.benchmark_return_pct
        )
        if anchor is not None:
            relative_strength = price_return - anchor

    keyless = bool(
        snapshot.source_id
        and ("YAHOO" in snapshot.source_id or "CBOE_DELAYED" in snapshot.source_id)
    )
    liquidity_ok = (
        snapshot.authoritative
        if keyless
        else bool(spread_pct is not None and spread_pct <= 0.25)
    )
    catalyst = snapshot.catalyst_strength if snapshot.catalyst_verified else 0.0

    return FeatureVector(
        symbol=snapshot.symbol,
        price_return_pct=price_return,
        gap_pct=gap,
        relative_volume=relative_volume,
        spread_pct=spread_pct,
        distance_from_vwap_pct=distance_vwap,
        relative_strength_pct=relative_strength,
        catalyst_strength=max(0.0, min(1.0, catalyst)),
        liquidity_ok=liquidity_ok,
        evidence={
            "source_id": snapshot.source_id,
            "data_age_seconds": snapshot.data_age_seconds,
            "delay_minutes": snapshot.delay_minutes,
            "fetched_at": snapshot.fetched_at,
            "authoritative": snapshot.authoritative,
            "halted": snapshot.halted,
        },
    )

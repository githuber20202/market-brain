from __future__ import annotations

from market_brain.domain.models import FeatureVector, MarketSnapshot


def _pct(a: float | None, b: float | None) -> float | None:
    if a is None or b in (None, 0):
        return None
    return (a / b - 1.0) * 100.0


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

    price_return = _pct(snapshot.last, snapshot.prior_close)
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

    liquidity_ok = bool(spread_pct is not None and spread_pct <= 0.25)
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
            "authoritative": snapshot.authoritative,
            "halted": snapshot.halted,
        },
    )


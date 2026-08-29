from __future__ import annotations

from market_brain.domain.models import FeatureVector, ScoreCard


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def score_features(
    features: FeatureVector,
    *,
    structure_score: float = 0.0,
    rr_score: float = 0.0,
) -> ScoreCard:
    catalyst = 20.0 * features.catalyst_strength

    momentum = 0.0
    if features.price_return_pct is not None:
        momentum += _clamp(abs(features.price_return_pct) / 3.0, 0.0, 1.0) * 14.0
    if features.distance_from_vwap_pct is not None and features.distance_from_vwap_pct > 0:
        momentum += _clamp(features.distance_from_vwap_pct / 1.0, 0.0, 1.0) * 6.0

    volume = 0.0
    if features.relative_volume is not None:
        volume = _clamp((features.relative_volume - 0.5) / 2.5, 0.0, 1.0) * 20.0

    relative = 0.0
    if features.relative_strength_pct is not None:
        relative = _clamp((features.relative_strength_pct + 0.5) / 2.5, 0.0, 1.0) * 15.0

    structure = _clamp(structure_score, 0.0, 15.0)
    risk_reward = _clamp(rr_score, 0.0, 10.0)
    discovery_raw = catalyst + momentum + volume + relative
    total = discovery_raw + structure + risk_reward
    discovery_total = discovery_raw / 75.0 * 100.0

    reasons: list[str] = []
    if not features.liquidity_ok:
        reasons.append("LIQUIDITY_WEAK")
    if features.catalyst_strength == 0:
        reasons.append("NO_VERIFIED_CATALYST")
    if features.relative_strength_pct is not None and features.relative_strength_pct < 0:
        reasons.append("RELATIVE_STRENGTH_NEGATIVE")

    return ScoreCard(
        symbol=features.symbol,
        catalyst_or_continuation=round(catalyst, 2),
        price_momentum=round(momentum, 2),
        volume_liquidity=round(volume, 2),
        relative_strength_sector=round(relative, 2),
        entry_invalidation_structure=round(structure, 2),
        risk_reward=round(risk_reward, 2),
        total=round(total, 2),
        discovery_total=round(discovery_total, 2),
        reasons=reasons,
    )


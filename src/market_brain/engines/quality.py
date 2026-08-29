from __future__ import annotations

from datetime import datetime

from market_brain.domain.models import QualityProfile, StrategyLane


def classify_quality(symbol: str, score: float, as_of: datetime) -> QualityProfile:
    bounded = max(0.0, min(100.0, score))
    if bounded >= 80:
        tier, multiplier = "A", 1.0
    elif bounded >= 65:
        tier, multiplier = "B", 0.75
    elif bounded >= 50:
        tier, multiplier = "C", 0.5
    else:
        tier, multiplier = "D", 0.0
    return QualityProfile(symbol, bounded, tier, multiplier, as_of)


def lane_risk_multiplier(
    lane: StrategyLane,
    profile: QualityProfile,
    catalyst_strength: float,
    speculative_enabled: bool = False,
) -> float:
    if lane == StrategyLane.CORE_MOMENTUM:
        return profile.risk_multiplier
    if lane == StrategyLane.EVENT_MOMENTUM:
        if catalyst_strength < 0.8 or profile.score < 35:
            return 0.0
        return min(0.5, max(0.25, profile.risk_multiplier))
    if lane == StrategyLane.SPECULATIVE:
        return 0.25 if speculative_enabled and profile.score >= 35 else 0.0
    return 0.0


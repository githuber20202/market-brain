from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from datetime import datetime, timedelta

from market_brain.domain.models import (
    MarketSnapshot,
    QualityProfile,
    ScoreCard,
    StrategyLane,
    TradePlan,
    utc_now,
)
from market_brain.engines.quality import lane_risk_multiplier


class PlanBuildError(ValueError):
    pass


def build_trade_plan(
    *,
    snapshot: MarketSnapshot,
    score: ScoreCard,
    quality: QualityProfile,
    lane: StrategyLane,
    plan_ttl_seconds: int = 300,
    speculative_enabled: bool = False,
    now: datetime | None = None,
) -> TradePlan:
    if snapshot.halted:
        raise PlanBuildError("HALTED")
    if score.total < 65:
        raise PlanBuildError("SCORE_BELOW_PLAN_THRESHOLD")
    if snapshot.opening_range_high is None or snapshot.retest_low is None:
        raise PlanBuildError("STRUCTURE_DATA_MISSING")

    entry = snapshot.opening_range_high
    stop = snapshot.retest_low
    if stop >= entry:
        raise PlanBuildError("INVALID_STRUCTURE")

    risk = entry - stop
    if risk <= 0:
        raise PlanBuildError("INVALID_RISK")

    multiplier = lane_risk_multiplier(
        lane,
        quality,
        snapshot.catalyst_strength if snapshot.catalyst_verified else 0.0,
        speculative_enabled,
    )
    if multiplier <= 0:
        raise PlanBuildError("QUALITY_OR_LANE_BLOCKED")

    extension = min(risk * 0.25, entry * 0.003)
    created_at = now or utc_now()
    evidence_payload = {
        "snapshot": asdict(snapshot),
        "score": asdict(score),
        "quality": asdict(quality),
        "lane": str(lane),
    }
    evidence_hash = hashlib.sha256(
        json.dumps(evidence_payload, sort_keys=True, default=str, separators=(",", ":")).encode()
    ).hexdigest()
    return TradePlan(
        symbol=snapshot.symbol,
        lane=lane,
        entry_trigger=round(entry, 4),
        entry_zone_high=round(entry + extension, 4),
        stop=round(stop, 4),
        tp1=round(entry + risk * 1.5, 4),
        tp2=round(entry + risk * 2.0, 4),
        max_spread_pct=0.25,
        max_slippage_pct=0.30,
        created_at=created_at,
        expires_at=created_at + timedelta(seconds=plan_ttl_seconds),
        quality_risk_multiplier=multiplier,
        reasons=[f"QUALITY_TIER_{quality.tier}", f"LANE_{lane}"],
        evidence_hash=evidence_hash,
        source_ids=[snapshot.source_id] if snapshot.source_id else [],
    )


from __future__ import annotations

import pytest

from market_brain.domain.models import MarketSnapshot
from market_brain.engines.premarket import CatalystAssessment, score_premarket_candidate


def _catalyst() -> CatalystAssessment:
    return CatalystAssessment(
        verified=True,
        negative=False,
        score=20.0,
        category="EARNINGS_GUIDANCE",
        headline="Company raises guidance",
        publisher="Reuters",
        published_at="2026-09-25T11:00:00+00:00",
        url="https://example.test/news",
        source_id="TEST",
        reason_codes=("DIRECT_NEWS_MATCH", "TRUSTED_PUBLISHER", "CATALYST_CLASSIFIED"),
    )


def _snapshot(last: float, prior_close: float, atr14: float) -> MarketSnapshot:
    return MarketSnapshot(
        symbol="AMD",
        last=last,
        prior_close=prior_close,
        volume=2_000_000,
        avg_volume=10_000_000,
        vwap=last - 1.0,
        high=last,
        low=prior_close,
        atr14=atr14,
        atr14_pct=atr14 / last * 100.0,
        authoritative=True,
        metadata={
            "premarket_high": last,
            "premarket_return_15m_percent": 0.8,
            "premarket_lower_highs_count": 0,
        },
    )


def test_amd_like_breakout_is_fresh_context_not_rejected():
    result = score_premarket_candidate(
        _snapshot(last=640.0, prior_close=629.005, atr14=25.3508682779),
        adv20=15_000_000,
        benchmark_return_pct=0.2,
        sector_return_pct=0.4,
        catalyst=_catalyst(),
        minimum_price=5.0,
        minimum_adv=5_000_000,
        finalist_score=65.0,
        reference_high_52w=630.795,
    )

    assert result["metrics"]["reference_high_state"] == "FRESH_BREAKOUT"
    assert result["metrics"]["distance_to_reference_high_percent"] == pytest.approx(1.4593, abs=1e-4)
    assert result["metrics"]["breakout_extension_atr"] == pytest.approx(0.3631, abs=1e-4)
    assert "REFERENCE_HIGH_FRESH_BREAKOUT" in result["reason_codes"]
    assert result["ranking_allowed"] is True


def test_extended_reference_high_is_context_not_a_hard_block():
    result = score_premarket_candidate(
        _snapshot(last=660.0, prior_close=629.005, atr14=25.0),
        adv20=15_000_000,
        benchmark_return_pct=0.2,
        sector_return_pct=0.4,
        catalyst=_catalyst(),
        minimum_price=5.0,
        minimum_adv=5_000_000,
        finalist_score=65.0,
        reference_high_52w=630.795,
    )

    assert result["metrics"]["reference_high_state"] == "EXTENDED_BREAKOUT"
    assert result["metrics"]["breakout_extension_atr"] > 0.5
    assert "REFERENCE_HIGH_EXTENDED_BREAKOUT" in result["reason_codes"]
    assert result["ranking_allowed"] is True

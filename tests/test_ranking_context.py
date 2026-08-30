from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from market_brain.domain.models import LiquidityProfile, MarketSnapshot
from market_brain.engines.features import apply_ranking_context
from market_brain.ledger.store import InMemoryEventStore
from market_brain.orchestration.screener import MarketScreener

EASTERN = ZoneInfo("America/New_York")


def _at(hour: int, minute: int) -> datetime:
    return datetime(2026, 8, 28, hour, minute, tzinfo=EASTERN)


def test_expected_volume_uses_opening_floor_and_midday_fraction():
    profile = LiquidityProfile("TEST", 1_000_000, 100.0, _at(9, 0))
    opening = MarketSnapshot("TEST", 101.0)
    midday = MarketSnapshot("TEST", 101.0)

    apply_ranking_context(
        opening,
        profile,
        benchmark_return_pct=0.0,
        now=_at(9, 35),
    )
    apply_ranking_context(
        midday,
        profile,
        benchmark_return_pct=0.0,
        now=_at(12, 45),
    )

    assert opening.avg_volume == pytest.approx(50_000)
    assert opening.metadata["expected_volume_fraction"] == pytest.approx(0.05)
    assert midday.avg_volume == pytest.approx(500_000)
    assert midday.metadata["expected_volume_fraction"] == pytest.approx(0.5)


class SnapshotProvider:
    async def snapshots(self, _symbols, *, decision=False):
        assert decision is False
        return [
            MarketSnapshot("TEST", 102.0, prior_close=100.0, volume=1_000_000),
            MarketSnapshot("SPY", 101.0, prior_close=100.0, volume=5_000_000),
        ]


@pytest.mark.asyncio
async def test_screener_injects_spy_benchmark_before_feature_scoring():
    store = InMemoryEventStore()
    await store.save_liquidity_profile(
        LiquidityProfile("TEST", 2_000_000, 100.0, _at(9, 0))
    )
    await store.save_liquidity_profile(
        LiquidityProfile("SPY", 10_000_000, 100.0, _at(9, 0))
    )

    result = await MarketScreener(SnapshotProvider(), store=store).screen(
        ["TEST", "SPY"],
        top_n=2,
        now=_at(12, 45),
        structure_score=15.0,
        rr_score=10.0,
    )
    test_row = next(row for row in result if row["snapshot"]["symbol"] == "TEST")

    assert test_row["snapshot"]["avg_volume"] == pytest.approx(1_000_000)
    assert test_row["snapshot"]["benchmark_return_pct"] == pytest.approx(1.0)
    assert test_row["features"]["relative_volume"] == pytest.approx(1.0)
    assert test_row["features"]["relative_strength_pct"] == pytest.approx(1.0)

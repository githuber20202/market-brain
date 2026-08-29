from datetime import UTC, datetime, timedelta

import pytest

from market_brain.domain.models import LiquidityProfile, MarketSnapshot
from market_brain.engines.liquidity import apply_iex_liquidity_gate
from market_brain.ledger.store import InMemoryEventStore
from market_brain.orchestration.service import DecisionService
from market_brain.settings import Settings


def profile(now: datetime, adv20: float = 3_000_000) -> LiquidityProfile:
    return LiquidityProfile(
        symbol="TEST", adv20=adv20, close=100.0,
        as_of=now - timedelta(days=1), refreshed_at=now,
    )


def snapshot(age: float = 1.0, last: float = 100.0, bid: float = 99.99, ask: float = 100.01):
    return MarketSnapshot(
        symbol="TEST", last=last, bid=bid, ask=ask, data_age_seconds=age,
        source_id="ALPACA_IEX", authoritative=False,
    )


def test_liquidity_gate_threshold_reasons():
    cfg = Settings()
    now = datetime.now(UTC)
    row = snapshot()
    assert apply_iex_liquidity_gate(row, profile(now), cfg) == ["LIQUIDITY_GATE_PASS"]
    assert row.authoritative is True

    low_adv = snapshot()
    assert "ADV_TOO_LOW" in apply_iex_liquidity_gate(low_adv, profile(now, 100_000), cfg)
    wide = snapshot(bid=99.0, ask=101.0)
    assert "SPREAD_TOO_WIDE" in apply_iex_liquidity_gate(wide, profile(now), cfg)
    cheap = snapshot(last=4.0, bid=3.99, ask=4.01)
    assert "PRICE_TOO_LOW" in apply_iex_liquidity_gate(cheap, profile(now), cfg)


class HistoricalProvider:
    configured = True

    def __init__(self):
        self.calls = 0

    async def bars(self, symbol, timeframe, start, end):
        self.calls += 1
        base = datetime(2026, 7, 1, tzinfo=UTC)
        return [
            {"t": (base + timedelta(days=index)).isoformat(), "v": 2_000_000 + index, "c": 100 + index / 10}
            for index in range(25)
        ]


@pytest.mark.asyncio
async def test_liquidity_profile_refreshes_once_per_day():
    cfg = Settings()
    provider = HistoricalProvider()
    store = InMemoryEventStore()
    service = DecisionService(store, cfg=cfg, market_data=provider)
    now = datetime(2026, 8, 29, 14, 0, tzinfo=UTC)
    first = await service.ensure_liquidity_profile("TEST", now=now)
    second = await service.ensure_liquidity_profile("TEST", now=now + timedelta(hours=1))
    assert provider.calls == 1
    assert first.adv20 == second.adv20
    assert first.close == pytest.approx(102.4)
    await service.ensure_liquidity_profile("TEST", now=now + timedelta(days=1))
    assert provider.calls == 2


from datetime import UTC, datetime

import pytest

from market_brain.domain.models import MarketSnapshot, StrategyLane
from market_brain.engines.quality import classify_quality
from market_brain.ledger.replay import rebuild_state, replay_check
from market_brain.ledger.store import InMemoryEventStore
from market_brain.orchestration.service import DecisionService
from tests.retest_helpers import activate_with_server_retest


class FakeProvider:
    async def snapshot(self, symbol: str, decision: bool = False):
        return MarketSnapshot(symbol=symbol,last=100.8,bid=100.79,ask=100.81,vwap=100.0,data_age_seconds=1.0,
            source_id="ALPACA_SIP",authoritative=True)


@pytest.mark.asyncio
async def test_rebuild_state_matches_materialized_lifecycle_inmemory():
    store = InMemoryEventStore()
    service = DecisionService(store, market_data=FakeProvider())
    await service.seed_wallet(10_000,10_000)
    snap = MarketSnapshot(symbol="TEST",last=100.8,prior_close=98.0,bid=100.79,ask=100.81,volume=2_000_000,
        avg_volume=1_000_000,vwap=100.0,open_price=99.0,opening_range_high=100.8,retest_low=100.0,
        benchmark_return_pct=0.5,catalyst_verified=True,catalyst_strength=0.9)
    quality = classify_quality("TEST",90,datetime.now(UTC))
    plan,_ = await service.build_plan(snap,quality,StrategyLane.CORE_MOMENTUM,15,10)
    decision = await activate_with_server_retest(service, plan.plan_id)
    position = await service.confirm_fill(plan.plan_id,fill_price=plan.entry_trigger,quantity=decision.quantity,
        stop_order_placed=True,stop_order_price=plan.stop)
    rebuilt = rebuild_state(await store.read_events())
    assert rebuilt["wallet"] == await store.get_wallet()
    assert rebuilt["positions"][position.position_id] == await store.get_position(position.position_id)
    assert await replay_check(store) == []


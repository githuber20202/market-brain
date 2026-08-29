from datetime import UTC, datetime, timedelta

import pytest

from market_brain.domain.models import MarketSnapshot, StrategyLane
from market_brain.engines.quality import classify_quality
from market_brain.ledger.store import InMemoryEventStore
from market_brain.orchestration.service import DecisionService
from tests.retest_helpers import activate_with_server_retest


class FakeProvider:
    def __init__(self, snapshot: MarketSnapshot):
        self.value = snapshot

    async def snapshot(self, symbol: str, decision: bool = False) -> MarketSnapshot:
        self.value.symbol = symbol
        return self.value


def market_snapshot(**overrides) -> MarketSnapshot:
    values = {
        "symbol": "TEST",
        "last": 100.8,
        "prior_close": 98.0,
        "bid": 100.79,
        "ask": 100.81,
        "volume": 2_000_000,
        "avg_volume": 1_000_000,
        "vwap": 100.0,
        "open_price": 99.0,
        "opening_range_high": 100.8,
        "retest_low": 100.0,
        "benchmark_return_pct": 0.5,
        "catalyst_verified": True,
        "catalyst_strength": 0.9,
        "data_age_seconds": 1.0,
        "source_id": "ALPACA_SIP",
        "authoritative": True,
    }
    values.update(overrides)
    return MarketSnapshot(**values)


async def prepared_service():
    store = InMemoryEventStore()
    provider = FakeProvider(market_snapshot())
    service = DecisionService(store, market_data=provider)
    await service.seed_wallet(10_000, 10_000)
    quality = classify_quality("TEST", 90, datetime.now(UTC))
    plan, _ = await service.build_plan(
        market_snapshot(), quality, StrategyLane.CORE_MOMENTUM, 15, 10
    )
    provider.value = market_snapshot(last=plan.entry_trigger)
    decision = await activate_with_server_retest(service, plan.plan_id)
    assert decision.state == "BUY_NOW"
    return service, store, provider, plan


@pytest.mark.asyncio
async def test_expired_plan_releases_reservation_before_activation_result():
    service, store, _provider, plan = await prepared_service()
    plan.expires_at = datetime.now(UTC) - timedelta(seconds=1)
    await store.save_plan(plan)

    decision = await activate_with_server_retest(service, plan.plan_id)

    assert decision.state == "EXPIRED"
    assert await store.get_reservation(plan.plan_id) is None
    wallet = await store.get_wallet()
    assert wallet is not None
    assert wallet.reserved_cash == 0
    assert wallet.open_risk == 0


@pytest.mark.asyncio
async def test_sweep_expired_releases_expired_reservation():
    service, store, _provider, plan = await prepared_service()
    reservation = await store.get_reservation(plan.plan_id)
    assert reservation is not None
    reservation.expires_at = datetime.now(UTC) - timedelta(seconds=1)
    await store.save_reservation(reservation)

    result = await service.sweep_expired()

    assert result["released_reservations"] == 1
    assert await store.get_reservation(plan.plan_id) is None
    wallet = await store.get_wallet()
    assert wallet is not None
    assert wallet.reserved_cash == 0
    assert wallet.open_risk == 0


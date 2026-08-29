from datetime import UTC, datetime

import pytest

from market_brain.domain.models import MarketSnapshot, StrategyLane
from market_brain.engines.quality import classify_quality
from market_brain.ledger.store import InMemoryEventStore
from market_brain.orchestration.service import DecisionService
from tests.retest_helpers import activate_with_server_retest


class FakeProvider:
    def __init__(self, market_snapshot):
        self.market_snapshot = market_snapshot
        self.calls = []

    async def snapshot(self, symbol: str, decision: bool = False):
        self.calls.append((symbol, decision))
        self.market_snapshot.symbol = symbol
        return self.market_snapshot


def snapshot(**overrides):
    base = {
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
        "opening_range_low": 99.6,
        "retest_low": 100.0,
        "benchmark_return_pct": 0.5,
        "catalyst_verified": True,
        "catalyst_strength": 0.9,
        "data_age_seconds": 1.0,
        "source_id": "AUTH",
        "authoritative": True,
    }
    base.update(overrides)
    return MarketSnapshot(**base)


@pytest.mark.asyncio
async def test_full_brokerless_lifecycle_buy_hold_sell():
    store = InMemoryEventStore()
    provider = FakeProvider(snapshot())
    service = DecisionService(store, market_data=provider)
    await service.seed_wallet(10_000, 10_000)
    quality = classify_quality("TEST", 90, datetime.now(UTC))
    plan, _ = await service.build_plan(snapshot(), quality, StrategyLane.CORE_MOMENTUM, 15, 10)
    provider.market_snapshot = snapshot(last=plan.entry_trigger)
    decision = await activate_with_server_retest(service, plan.plan_id)
    assert decision.state == "BUY_NOW"
    position = await service.confirm_fill(
        plan.plan_id,
        fill_price=plan.entry_trigger,
        quantity=decision.quantity,
        stop_order_placed=True,
        stop_order_price=plan.stop,
    )
    assert position.remaining_quantity == decision.quantity
    assert await service.evaluate_position(position.position_id, last=plan.entry_trigger + 0.2) == "HOLD"
    assert await service.evaluate_position(position.position_id, last=plan.stop) == "SELL_NOW"
    closed = await service.confirm_exit(position.position_id, exit_price=plan.stop, quantity=position.remaining_quantity)
    assert closed.closed_at is not None


@pytest.mark.asyncio
async def test_fill_above_reservation_is_rejected():
    provider = FakeProvider(snapshot())
    service = DecisionService(InMemoryEventStore(), market_data=provider)
    await service.seed_wallet(10_000, 10_000)
    quality = classify_quality("TEST", 90, datetime.now(UTC))
    plan, _ = await service.build_plan(snapshot(), quality, StrategyLane.CORE_MOMENTUM, 15, 10)
    provider.market_snapshot = snapshot(last=plan.entry_trigger)
    decision = await activate_with_server_retest(service, plan.plan_id)
    with pytest.raises(ValueError, match="QUANTITY_EXCEEDS_RESERVATION"):
        await service.confirm_fill(plan.plan_id, fill_price=plan.entry_trigger, quantity=decision.quantity + 1)


@pytest.mark.asyncio
async def test_fill_outside_plan_is_rejected():
    provider = FakeProvider(snapshot())
    service = DecisionService(InMemoryEventStore(), market_data=provider)
    await service.seed_wallet(10_000, 10_000)
    quality = classify_quality("TEST", 90, datetime.now(UTC))
    plan, _ = await service.build_plan(snapshot(), quality, StrategyLane.CORE_MOMENTUM, 15, 10)
    provider.market_snapshot = snapshot(last=plan.entry_trigger)
    decision = await activate_with_server_retest(service, plan.plan_id)
    with pytest.raises(ValueError, match="FILL_OUTSIDE_PLAN"):
        await service.confirm_fill(plan.plan_id, fill_price=plan.entry_zone_high * 1.01, quantity=decision.quantity)


@pytest.mark.asyncio
async def test_external_trade_is_invisible_and_fails_closed():
    provider = FakeProvider(snapshot())
    service = DecisionService(InMemoryEventStore(), market_data=provider)
    action = await service.evaluate_position("unknown", last=100)
    assert action == "UNKNOWN_POSITION"

@pytest.mark.asyncio
async def test_activation_is_idempotent_and_does_not_double_reserve():
    store = InMemoryEventStore()
    provider = FakeProvider(snapshot())
    service = DecisionService(store, market_data=provider)
    await service.seed_wallet(10_000, 10_000)
    quality = classify_quality("TEST", 90, datetime.now(UTC))
    plan, _ = await service.build_plan(snapshot(), quality, StrategyLane.CORE_MOMENTUM, 15, 10)
    provider.market_snapshot = snapshot(last=plan.entry_trigger)
    first = await activate_with_server_retest(service, plan.plan_id)
    wallet_after_first = await store.get_wallet()
    second = await activate_with_server_retest(service, plan.plan_id)
    wallet_after_second = await store.get_wallet()
    assert first.quantity == second.quantity
    assert wallet_after_first.reserved_cash == wallet_after_second.reserved_cash
    assert "EXISTING_CAPACITY_RESERVATION" in second.reasons


@pytest.mark.asyncio
async def test_reservation_release_restores_wallet_capacity():
    store = InMemoryEventStore()
    provider = FakeProvider(snapshot())
    service = DecisionService(store, market_data=provider)
    await service.seed_wallet(10_000, 10_000)
    quality = classify_quality("TEST", 90, datetime.now(UTC))
    plan, _ = await service.build_plan(snapshot(), quality, StrategyLane.CORE_MOMENTUM, 15, 10)
    provider.market_snapshot = snapshot(last=plan.entry_trigger)
    await activate_with_server_retest(service, plan.plan_id)
    reserved = await store.get_wallet()
    assert reserved.reserved_cash > 0
    assert await service.release_reservation(plan.plan_id)
    released = await store.get_wallet()
    assert released.reserved_cash == 0
    assert released.open_risk == 0

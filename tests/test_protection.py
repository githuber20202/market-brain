from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

import market_brain.api.main as api_main
from market_brain.domain.models import (
    MarketSnapshot,
    PlanStatus,
    PositionAction,
    ProtectionState,
    Reservation,
    StrategyLane,
)
from market_brain.engines.quality import classify_quality
from market_brain.ledger.store import InMemoryEventStore
from market_brain.orchestration.service import DecisionService
from market_brain.settings import Settings
from tests.retest_helpers import activate_with_server_retest


def snapshot() -> MarketSnapshot:
    return MarketSnapshot(
        symbol="TEST",
        last=100.8,
        prior_close=98.0,
        bid=100.79,
        ask=100.81,
        volume=2_000_000,
        avg_volume=1_000_000,
        vwap=100.0,
        open_price=99.0,
        opening_range_high=100.8,
        opening_range_low=99.6,
        retest_low=100.0,
        benchmark_return_pct=0.5,
        catalyst_verified=True,
        catalyst_strength=0.9,
        data_age_seconds=1.0,
        source_id="ALPACA_SIP",
        authoritative=True,
    )


class FakeProvider:
    async def snapshot(self, symbol: str, decision: bool = False) -> MarketSnapshot:
        assert decision is True
        snap = snapshot()
        snap.symbol = symbol
        return snap


async def reserved_service(quantity: int = 10):
    store = InMemoryEventStore()
    service = DecisionService(store, cfg=Settings(), market_data=FakeProvider())
    await service.seed_wallet(10_000, 10_000)
    quality = classify_quality("TEST", 90, datetime.now(UTC))
    plan, _ = await service.build_plan(snapshot(), quality, StrategyLane.CORE_MOMENTUM, 15, 10)
    plan.status = PlanStatus.RESERVED
    await store.save_plan(plan)
    reservation = Reservation(
        plan_id=plan.plan_id,
        quantity=quantity,
        reserved_cash=round(quantity * plan.entry_zone_high, 2),
        reserved_risk=round(quantity * plan.risk_per_share, 2),
        expires_at=datetime.now(UTC) + timedelta(minutes=2),
    )
    wallet = await store.get_wallet()
    assert wallet is not None
    wallet.reserved_cash = reservation.reserved_cash
    wallet.open_risk = reservation.reserved_risk
    await store.save_wallet(wallet)
    await store.save_reservation(reservation)
    return service, store, plan


@pytest.mark.asyncio
async def test_buy_now_includes_order_ticket_and_event():
    store = InMemoryEventStore()
    service = DecisionService(store, market_data=FakeProvider())
    await service.seed_wallet(10_000, 10_000)
    quality = classify_quality("TEST", 90, datetime.now(UTC))
    plan, _ = await service.build_plan(snapshot(), quality, StrategyLane.CORE_MOMENTUM, 15, 10)
    decision = await activate_with_server_retest(service, plan.plan_id)
    assert decision.order_ticket is not None
    assert decision.order_ticket["side"] == "BUY"
    assert decision.order_ticket["stop_price"] == plan.stop
    assert any(event.event_type == "BUY_NOW_EMITTED" for event in store.events)


@pytest.mark.asyncio
async def test_fill_with_plan_stop_is_protected():
    service, _store, plan = await reserved_service()
    position = await service.confirm_fill(
        plan.plan_id, fill_price=plan.entry_trigger, quantity=10,
        stop_order_placed=True, stop_order_price=plan.stop, broker_order_ref="stop-1"
    )
    assert position.protection == ProtectionState.PROTECTED
    assert position.broker_stop_price == plan.stop
    assert position.broker_order_ref == "stop-1"


@pytest.mark.asyncio
async def test_fill_without_stop_creates_unprotected_and_evaluate_place_stop_now():
    service, store, plan = await reserved_service()
    position = await service.confirm_fill(plan.plan_id, fill_price=plan.entry_trigger, quantity=10)
    assert position.protection == ProtectionState.UNPROTECTED
    assert any(event.event_type == "POSITION_UNPROTECTED" for event in store.events)
    assert await service.evaluate_position(position.position_id, last=plan.entry_trigger + 0.1) == PositionAction.PLACE_STOP_NOW


@pytest.mark.asyncio
async def test_looser_stop_returns_422(monkeypatch):
    service, _store, plan = await reserved_service()
    monkeypatch.setattr(api_main, "service", service)
    with TestClient(api_main.app) as client:
        response = client.post(
            "/fills/confirm",
            json={
                "plan_id": plan.plan_id, "fill_price": plan.entry_trigger, "quantity": 10,
                "stop_order_placed": True, "stop_order_price": plan.stop * 0.99,
            },
        )
    assert response.status_code == 422
    assert response.json()["detail"] == "STOP_LOOSER_THAN_PLAN"


@pytest.mark.asyncio
async def test_tighter_stop_updates_twin_and_recalculates_risk():
    service, store, plan = await reserved_service()
    tighter = plan.stop + (plan.entry_trigger - plan.stop) * 0.25
    position = await service.confirm_fill(
        plan.plan_id, fill_price=plan.entry_trigger, quantity=10,
        stop_order_placed=True, stop_order_price=tighter, broker_order_ref="tight"
    )
    wallet = await store.get_wallet()
    assert wallet is not None
    assert position.stop == pytest.approx(tighter)
    assert wallet.open_risk == pytest.approx((position.entry_avg - tighter) * position.quantity)


@pytest.mark.asyncio
async def test_protect_moves_unprotected_to_protected():
    service, store, plan = await reserved_service()
    position = await service.confirm_fill(plan.plan_id, fill_price=plan.entry_trigger, quantity=10)
    protected = await service.protect_position(
        position.position_id, stop_order_price=plan.stop, broker_order_ref="later-stop"
    )
    assert protected.protection == ProtectionState.PROTECTED
    assert protected.broker_order_ref == "later-stop"
    assert any(event.event_type == "STOP_ORDER_CONFIRMED" for event in store.events)


@pytest.mark.asyncio
async def test_unprotected_below_stop_returns_sell_now():
    service, _store, plan = await reserved_service()
    position = await service.confirm_fill(plan.plan_id, fill_price=plan.entry_trigger, quantity=10)
    assert position.protection == ProtectionState.UNPROTECTED
    assert await service.evaluate_position(position.position_id, last=plan.stop) == PositionAction.SELL_NOW


@pytest.mark.asyncio
async def test_protect_cannot_widen_tightened_twin_stop():
    service, store, plan = await reserved_service()
    tighter = plan.stop + (plan.entry_trigger - plan.stop) * 0.25
    position = await service.confirm_fill(
        plan.plan_id,
        fill_price=plan.entry_trigger,
        quantity=10,
        stop_order_placed=True,
        stop_order_price=tighter,
        broker_order_ref="tight-stop",
    )
    with pytest.raises(ValueError, match="STOP_LOOSER_THAN_CURRENT"):
        await service.protect_position(position.position_id, stop_order_price=plan.stop)
    unchanged = await store.get_position(position.position_id)
    assert unchanged is not None
    assert unchanged.stop == pytest.approx(tighter)
    assert unchanged.broker_stop_price == pytest.approx(tighter)
    assert unchanged.broker_order_ref == "tight-stop"


@pytest.mark.asyncio
async def test_additional_fill_cannot_widen_current_stop():
    service, _store, plan = await reserved_service(quantity=10)
    tighter = plan.stop + (plan.entry_trigger - plan.stop) * 0.25
    await service.confirm_fill(
        plan.plan_id,
        fill_price=plan.entry_trigger,
        quantity=6,
        stop_order_placed=True,
        stop_order_price=tighter,
        broker_order_ref="tight-stop",
    )
    with pytest.raises(ValueError, match="STOP_LOOSER_THAN_CURRENT"):
        await service.confirm_fill(
            plan.plan_id,
            fill_price=plan.entry_trigger,
            quantity=4,
            stop_order_placed=True,
            stop_order_price=plan.stop,
            broker_order_ref="looser-stop",
        )


@pytest.mark.asyncio
async def test_partial_protection_is_preserved_until_full_protect():
    service, _store, plan = await reserved_service(quantity=10)
    tighter = plan.stop + (plan.entry_trigger - plan.stop) * 0.25
    position = await service.confirm_fill(
        plan.plan_id,
        fill_price=plan.entry_trigger,
        quantity=6,
        stop_order_placed=True,
        stop_order_price=tighter,
        broker_order_ref="first-stop",
    )
    assert position.protected_quantity == 6
    position = await service.confirm_fill(
        plan.plan_id,
        fill_price=plan.entry_trigger,
        quantity=4,
        stop_order_placed=False,
    )
    assert position.protection == ProtectionState.UNPROTECTED
    assert position.protected_quantity == 6
    assert position.broker_stop_price == pytest.approx(tighter)
    assert position.broker_order_ref == "first-stop"
    assert position.stop == pytest.approx(tighter)

    protected = await service.protect_position(
        position.position_id,
        stop_order_price=tighter,
        broker_order_ref="full-stop",
    )
    assert protected.protection == ProtectionState.PROTECTED
    assert protected.protected_quantity == 10
    assert protected.broker_order_ref == "full-stop"


@pytest.mark.asyncio
async def test_place_stop_now_has_priority_over_reconcile_required():
    service, store, plan = await reserved_service()
    position = await service.confirm_fill(plan.plan_id, fill_price=plan.entry_trigger, quantity=10)
    position.tp1 = None
    await store.save_position(position)
    assert (
        await service.evaluate_position(position.position_id, last=plan.entry_trigger + 0.1)
        == PositionAction.PLACE_STOP_NOW
    )

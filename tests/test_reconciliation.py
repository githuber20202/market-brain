from datetime import UTC, datetime, timedelta

import pytest

from market_brain.domain.models import (
    PositionAction,
    PositionState,
    ProtectionState,
    ReconciliationState,
)
from market_brain.engines.position import evaluate_position
from market_brain.ledger.store import InMemoryEventStore
from market_brain.orchestration.service import DecisionService
from market_brain.settings import Settings


@pytest.mark.asyncio
async def test_import_position_creates_manual_twin_and_updates_wallet():
    store = InMemoryEventStore()
    service = DecisionService(store, cfg=Settings())
    await service.seed_wallet(10_000, 10_000)

    position = await service.import_position(
        symbol="abc",
        quantity=10,
        average_fill=100.0,
        stop_order_price=95.0,
        broker_order_ref="stop-1",
    )

    wallet = await store.get_wallet()
    assert wallet is not None
    assert position.symbol == "ABC"
    assert position.source == "MANUAL_IMPORT"
    assert position.protection == ProtectionState.PROTECTED
    assert position.protected_quantity == 10
    assert position.reconciliation_state == ReconciliationState.RECONCILED
    assert position.last_reconciled_at is not None
    assert wallet.cash_available == pytest.approx(9000.0)
    assert wallet.open_risk == pytest.approx(50.0)


@pytest.mark.asyncio
async def test_reconcile_matching_holding_marks_reconciled():
    store = InMemoryEventStore()
    service = DecisionService(store, cfg=Settings())
    await service.seed_wallet(10_000, 10_000)
    position = await service.import_position(
        symbol="ABC", quantity=10, average_fill=100.0, stop_order_price=95.0
    )
    position.reconciliation_state = ReconciliationState.UNRECONCILED
    position.last_reconciled_at = None
    await store.save_position(position)

    result = await service.reconcile_holdings([{"symbol": "ABC", "quantity": 10}])
    saved = await store.get_position(position.position_id)

    assert result["reconciled_symbols"] == ["ABC"]
    assert saved is not None
    assert saved.reconciliation_state == ReconciliationState.RECONCILED
    assert saved.last_reconciled_at is not None


@pytest.mark.asyncio
async def test_reconcile_quantity_mismatch_marks_unreconciled_and_emits_event():
    store = InMemoryEventStore()
    service = DecisionService(store, cfg=Settings())
    await service.seed_wallet(10_000, 10_000)
    position = await service.import_position(
        symbol="ABC", quantity=10, average_fill=100.0, stop_order_price=95.0
    )

    result = await service.reconcile_holdings([{"symbol": "ABC", "quantity": 9}])
    saved = await store.get_position(position.position_id)

    assert saved is not None
    assert saved.reconciliation_state == ReconciliationState.UNRECONCILED
    assert result["mismatches"][0]["reason"] == "QUANTITY_MISMATCH"
    assert any(event.event_type == "RECONCILIATION_MISMATCH" for event in store.events)


@pytest.mark.asyncio
async def test_reconcile_missing_broker_position_marks_missing():
    store = InMemoryEventStore()
    service = DecisionService(store, cfg=Settings())
    await service.seed_wallet(10_000, 10_000)
    position = await service.import_position(
        symbol="ABC", quantity=10, average_fill=100.0, stop_order_price=95.0
    )

    result = await service.reconcile_holdings([])
    saved = await store.get_position(position.position_id)

    assert saved is not None
    assert saved.reconciliation_state == ReconciliationState.UNRECONCILED_MISSING_AT_BROKER
    assert result["mismatches"][0]["reason"] == "MISSING_AT_BROKER"


@pytest.mark.asyncio
async def test_reconcile_unknown_holding_emits_event_without_creating_twin():
    store = InMemoryEventStore()
    service = DecisionService(store, cfg=Settings())
    await service.seed_wallet(10_000, 10_000)

    result = await service.reconcile_holdings([{"symbol": "XYZ", "quantity": 5}])

    assert await store.list_positions() == []
    assert result["unknown_holdings"] == [{"symbol": "XYZ", "quantity": 5}]
    assert any(event.event_type == "UNKNOWN_HOLDING" for event in store.events)


def _position(**overrides) -> PositionState:
    base = {
        "position_id": "p1",
        "plan_id": "plan1",
        "symbol": "ABC",
        "quantity": 10,
        "remaining_quantity": 10,
        "average_fill": 100.0,
        "stop": 95.0,
        "tp1": 107.5,
        "tp2": 110.0,
        "opened_at": datetime.now(UTC) - timedelta(hours=2),
        "time_stop_at": datetime.now(UTC) + timedelta(hours=1),
        "protection": ProtectionState.PROTECTED,
        "protected_quantity": 10,
        "reconciliation_state": ReconciliationState.RECONCILED,
        "last_reconciled_at": datetime.now(UTC) - timedelta(hours=25),
    }
    base.update(overrides)
    return PositionState(**base)


def test_reconciliation_age_requires_reconcile_before_targets():
    position = _position()
    assert (
        evaluate_position(position, last=106.0, reconciliation_max_age_hours=24.0)
        == PositionAction.RECONCILE_REQUIRED
    )


def test_sell_now_precedes_reconcile_required():
    position = _position()
    assert (
        evaluate_position(position, last=95.0, reconciliation_max_age_hours=24.0)
        == PositionAction.SELL_NOW
    )


def test_place_stop_now_precedes_reconcile_required():
    position = _position(
        protection=ProtectionState.UNPROTECTED,
        protected_quantity=0,
    )
    assert (
        evaluate_position(position, last=100.0, reconciliation_max_age_hours=24.0)
        == PositionAction.PLACE_STOP_NOW
    )


def test_reconcile_required_precedes_time_stop():
    position = _position(
        time_stop_at=datetime.now(UTC) - timedelta(minutes=1),
    )
    assert (
        evaluate_position(position, last=99.0, reconciliation_max_age_hours=24.0)
        == PositionAction.RECONCILE_REQUIRED
    )


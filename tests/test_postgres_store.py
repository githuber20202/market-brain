from dataclasses import asdict
from datetime import UTC, datetime, timedelta

import pytest

from market_brain.domain.models import (
    AlertRecord,
    MarketSnapshot,
    PositionState,
    ProtectionState,
    ReconciliationState,
    ShadowTrade,
    ShadowTradeStatus,
    StrategyLane,
    TradePlan,
)
from market_brain.engines.quality import classify_quality
from market_brain.ledger.events import LedgerEvent
from market_brain.ledger.replay import replay_check
from market_brain.orchestration.service import DecisionService
from tests.retest_helpers import activate_with_server_retest

pytestmark = pytest.mark.postgres


class FakeProvider:
    def __init__(self):
        self.last = 100.8

    async def snapshot(self, symbol: str, decision: bool = False) -> MarketSnapshot:
        return MarketSnapshot(
            symbol=symbol,last=self.last,prior_close=98.0,bid=self.last-0.01,ask=self.last+0.01,
            volume=2_000_000,avg_volume=1_000_000,vwap=100.0,open_price=99.0,
            opening_range_high=100.8,retest_low=100.0,benchmark_return_pct=0.5,
            catalyst_verified=True,catalyst_strength=0.9,data_age_seconds=1.0,
            source_id="ALPACA_SIP",authoritative=True,
        )


def planning_snapshot():
    return MarketSnapshot(symbol="TEST",last=100.8,prior_close=98.0,bid=100.79,ask=100.81,volume=2_000_000,
        avg_volume=1_000_000,vwap=100.0,open_price=99.0,opening_range_high=100.8,retest_low=100.0,
        benchmark_return_pct=0.5,catalyst_verified=True,catalyst_strength=0.9)


async def activated_service(pg_store):
    provider = FakeProvider()
    service = DecisionService(pg_store, market_data=provider)
    await service.seed_wallet(10_000, 10_000)
    quality = classify_quality("TEST", 90, datetime.now(UTC))
    plan, _ = await service.build_plan(planning_snapshot(), quality, StrategyLane.CORE_MOMENTUM, 15, 10)
    provider.last = plan.entry_trigger
    decision = await activate_with_server_retest(service, plan.plan_id)
    return service, plan, decision


@pytest.mark.asyncio
async def test_postgres_full_lifecycle(pg_store):
    service, plan, decision = await activated_service(pg_store)
    position = await service.confirm_fill(plan.plan_id, fill_price=plan.entry_trigger, quantity=decision.quantity,
        stop_order_placed=True, stop_order_price=plan.stop, broker_order_ref="stop-1")
    assert await service.evaluate_position(position.position_id, last=plan.entry_trigger + 0.1) == "HOLD"
    closed = await service.confirm_exit(position.position_id, exit_price=plan.entry_trigger + 0.1, quantity=position.remaining_quantity)
    assert closed.closed_at is not None


@pytest.mark.asyncio
async def test_postgres_position_round_trip_all_safety_fields(pg_store):
    now = datetime.now(UTC)
    position = PositionState(position_id="11111111-1111-1111-1111-111111111111", plan_id="manual-test", symbol="ABC",
        quantity=10,remaining_quantity=10,average_fill=100.0,stop=96.0,tp1=106.0,tp2=108.0,opened_at=now,
        time_stop_at=now+timedelta(minutes=30),protection=ProtectionState.PROTECTED,broker_stop_price=96.0,
        broker_order_ref="broker-stop",protected_quantity=10,reconciliation_state=ReconciliationState.RECONCILED,
        last_reconciled_at=now)
    await pg_store.save_position(position)
    loaded = await pg_store.get_position(position.position_id)
    assert loaded == position


@pytest.mark.asyncio
async def test_postgres_outbox_round_trip_next_attempt_at(pg_store):
    next_at = datetime.now(UTC) + timedelta(seconds=30)
    alert = AlertRecord(kind="SELL_NOW", payload={"symbol":"ABC"}, next_attempt_at=next_at)
    await pg_store.save_alert(alert)
    loaded = await pg_store.get_alert(alert.alert_id)
    assert loaded is not None
    assert loaded.kind == "SELL_NOW"
    assert loaded.next_attempt_at == next_at


@pytest.mark.asyncio
async def test_postgres_sweep_expired(pg_store):
    service, plan, _ = await activated_service(pg_store)
    reservation = await pg_store.get_reservation(plan.plan_id)
    assert reservation is not None
    reservation.expires_at = datetime.now(UTC) - timedelta(seconds=1)
    await pg_store.save_reservation(reservation)
    result = await service.sweep_expired()
    assert result["released_reservations"] >= 1
    assert await pg_store.get_reservation(plan.plan_id) is None


@pytest.mark.asyncio
async def test_postgres_reconcile_holdings(pg_store):
    service = DecisionService(pg_store)
    await service.seed_wallet(10_000, 10_000)
    position = await service.import_position(symbol="ABC",quantity=10,average_fill=100.0,stop_order_price=95.0)
    result = await service.reconcile_holdings([{"symbol":"ABC","quantity":10}])
    saved = await pg_store.get_position(position.position_id)
    assert result["reconciled_symbols"] == ["ABC"]
    assert saved is not None and saved.reconciliation_state == ReconciliationState.RECONCILED


@pytest.mark.asyncio
async def test_postgres_confirm_fill_rolls_back_on_mid_operation_exception(pg_store, monkeypatch):
    service, plan, decision = await activated_service(pg_store)
    before_wallet = await pg_store.get_wallet()
    before_reservation = await pg_store.get_reservation(plan.plan_id)
    original = pg_store.save_position

    async def fail_after_first_write(position):
        await original(position)
        raise RuntimeError("INJECTED_AFTER_FIRST_WRITE")

    monkeypatch.setattr(pg_store, "save_position", fail_after_first_write)
    with pytest.raises(RuntimeError, match="INJECTED_AFTER_FIRST_WRITE"):
        await service.confirm_fill(plan.plan_id, fill_price=plan.entry_trigger, quantity=decision.quantity,
            stop_order_placed=True, stop_order_price=plan.stop)
    monkeypatch.setattr(pg_store, "save_position", original)
    assert await pg_store.list_positions() == []
    assert await pg_store.get_wallet() == before_wallet
    assert await pg_store.get_reservation(plan.plan_id) == before_reservation
    assert not any(event.event_type == "FILL_CONFIRMED" for event in await pg_store.read_events())


@pytest.mark.asyncio
async def test_postgres_replay_matches_materialized_after_lifecycle(pg_store):
    service, plan, decision = await activated_service(pg_store)
    position = await service.confirm_fill(plan.plan_id, fill_price=plan.entry_trigger, quantity=decision.quantity,
        stop_order_placed=True, stop_order_price=plan.stop)
    await service.confirm_exit(position.position_id, exit_price=plan.entry_trigger + 0.2, quantity=position.remaining_quantity)
    assert await replay_check(pg_store) == []


@pytest.mark.asyncio
async def test_postgres_replay_detects_manual_position_row_change(pg_store):
    service, plan, decision = await activated_service(pg_store)
    position = await service.confirm_fill(plan.plan_id, fill_price=plan.entry_trigger, quantity=decision.quantity,
        stop_order_placed=True, stop_order_price=plan.stop)
    assert await replay_check(pg_store) == []
    assert pg_store.pool is not None
    async with pg_store.pool.acquire() as connection:
        await connection.execute(
            "UPDATE position_twin SET position_json=jsonb_set(position_json,'{remaining_quantity}','999'::jsonb) WHERE position_id=$1",
            position.position_id,
        )
    diffs = await replay_check(pg_store)
    assert f"position:{position.position_id}" in diffs


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_postgres_runtime_status_round_trip(pg_store):
    await pg_store.set_runtime_status("stream_connected", True)
    await pg_store.set_runtime_status("subscribed_symbols", ["AAPL", "SPY"])
    status = await pg_store.get_runtime_status()
    assert status["stream_connected"] is True
    assert status["subscribed_symbols"] == ["AAPL", "SPY"]



@pytest.mark.postgres
@pytest.mark.asyncio
async def test_postgres_plan_triggered_at_round_trip(pg_store):
    now = datetime.now(UTC)
    plan = TradePlan(
        symbol="ABC",
        lane=StrategyLane.CORE_MOMENTUM,
        entry_trigger=100.0,
        entry_zone_high=100.2,
        stop=98.0,
        tp1=103.0,
        tp2=104.0,
        max_spread_pct=0.25,
        max_slippage_pct=0.30,
        created_at=now,
        expires_at=now + timedelta(minutes=5),
        quality_risk_multiplier=1.0,
        triggered_at=now,
    )
    await pg_store.save_plan(plan)
    loaded = await pg_store.get_plan(plan.plan_id)
    assert loaded is not None
    assert loaded.triggered_at == now


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_postgres_shadow_trade_round_trip_and_event_replay(pg_store):
    now = datetime.now(UTC)
    trade = ShadowTrade(
        trade_id="11111111-1111-1111-1111-111111111111",
        plan_id="22222222-2222-2222-2222-222222222222",
        symbol="ABC",
        setup="CORE_MOMENTUM",
        quantity=5,
        trigger=100.0,
        fill=100.1,
        stop=98.0,
        tp1=103.0,
        tp2=104.0,
        opened_at=now,
        time_stop_at=now + timedelta(minutes=30),
        status=ShadowTradeStatus.OPEN,
    )
    await pg_store.save_shadow_trade(trade)
    await pg_store.append(
        LedgerEvent(
            "SHADOW_TRADE_OPENED",
            trade.trade_id,
            {"shadow_trade": asdict(trade)},
            occurred_at=now,
        )
    )

    assert await pg_store.get_shadow_trade(trade.plan_id) == trade
    assert await pg_store.list_shadow_trades() == [trade]
    assert await replay_check(pg_store) == []

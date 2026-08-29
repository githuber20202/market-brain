from datetime import UTC, datetime, timedelta

import pytest

from market_brain.domain.models import (
    MarketSnapshot,
    ProtectionState,
    ReconciliationState,
    StrategyLane,
)
from market_brain.engines.quality import classify_quality
from market_brain.ledger.store import InMemoryEventStore
from market_brain.orchestration.service import DecisionService
from market_brain.runtime.position_monitor import PositionMonitor
from market_brain.settings import Settings


class FakeProvider:
    async def snapshot(self, symbol: str, decision: bool = False):
        raise AssertionError("monitor test must not call decision provider")


async def build_position(*, stop=95.0):
    store = InMemoryEventStore()
    cfg = Settings(
        nats_url=None,
        monitor_min_interval_seconds=1,
        monitor_cache_refresh_seconds=5,
        failed_breakout_buffer_r=0.25,
        alert_repeat_minutes=5,
    )
    service = DecisionService(store, cfg=cfg, market_data=FakeProvider())
    await service.seed_wallet(10_000, 10_000)
    quality = classify_quality("TEST", 90, datetime.now(UTC))
    snapshot = MarketSnapshot(
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
        retest_low=100.0,
        benchmark_return_pct=0.5,
        catalyst_verified=True,
        catalyst_strength=0.9,
    )
    plan, _ = await service.build_plan(
        snapshot,
        quality,
        StrategyLane.CORE_MOMENTUM,
        15,
        10,
    )
    position = await service.import_position(
        symbol="TEST",
        quantity=10,
        average_fill=100.0,
        stop_order_price=stop,
        broker_order_ref="stop-1",
    )
    position.plan_id = plan.plan_id
    position.stop = stop
    position.tp1 = 107.5
    position.tp2 = 110.0
    position.protection = ProtectionState.PROTECTED
    position.protected_quantity = position.remaining_quantity
    position.reconciliation_state = ReconciliationState.RECONCILED
    position.last_reconciled_at = datetime.now(UTC)
    await store.save_position(position)
    return store, service, cfg, plan, position


def alerts(store, kind):
    return [row for row in store.alerts.values() if row.kind == kind]


async def loaded_monitor(service, cfg):
    monitor = PositionMonitor(service, cfg=cfg)
    await monitor.refresh_cache()
    return monitor


@pytest.mark.asyncio
async def test_stop_crossed_emits_one_sell_now_alert():
    store, service, cfg, _plan, position = await build_position(stop=95.0)
    monitor = await loaded_monitor(service, cfg)
    now = datetime.now(UTC)
    await monitor.handle_message(
        "market.market_trade.test",
        {"p": 94.9, "t": now.isoformat()},
        now=now,
    )
    assert len(alerts(store, "SELL_NOW")) == 1
    events = [
        e for e in store.events
        if e.event_type == "POSITION_EVALUATED" and e.aggregate_id == position.position_id
    ]
    assert len(events) == 1
    assert events[-1].payload["action"] == "SELL_NOW"


@pytest.mark.asyncio
async def test_repeated_same_action_does_not_emit_duplicate_alert_or_event():
    store, service, cfg, _plan, position = await build_position(stop=95.0)
    monitor = await loaded_monitor(service, cfg)
    now = datetime.now(UTC)
    await monitor.handle_message(
        "market.market_trade.test", {"p": 94.9, "t": now.isoformat()}, now=now
    )
    later = now + timedelta(seconds=2)
    await monitor.handle_message(
        "market.market_trade.test", {"p": 94.8, "t": later.isoformat()}, now=later
    )
    assert len(alerts(store, "SELL_NOW")) == 1
    events = [
        e for e in store.events
        if e.event_type == "POSITION_EVALUATED" and e.aggregate_id == position.position_id
    ]
    assert len(events) == 1


@pytest.mark.asyncio
async def test_sell_now_repeats_after_five_minutes():
    store, service, cfg, _plan, _position = await build_position(stop=95.0)
    monitor = await loaded_monitor(service, cfg)
    now = datetime.now(UTC)
    await monitor.handle_message(
        "market.market_trade.test", {"p": 94.9, "t": now.isoformat()}, now=now
    )
    later = now + timedelta(minutes=5, seconds=1)
    await monitor.handle_message(
        "market.market_trade.test", {"p": 94.8, "t": later.isoformat()}, now=later
    )
    assert len(alerts(store, "SELL_NOW")) == 2


@pytest.mark.asyncio
async def test_trigger_hit_is_persisted_and_not_reemitted_after_restart():
    store = InMemoryEventStore()
    cfg = Settings(
        nats_url=None,
        alpaca_stream_url="wss://stream.data.alpaca.markets/v2/sip",
    )
    service = DecisionService(store, cfg=cfg)
    await service.seed_wallet(10_000, 10_000)
    quality = classify_quality("TEST", 90, datetime.now(UTC))
    snapshot = MarketSnapshot(
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
        retest_low=100.0,
        benchmark_return_pct=0.5,
        catalyst_verified=True,
        catalyst_strength=0.9,
    )
    plan, _ = await service.build_plan(
        snapshot, quality, StrategyLane.CORE_MOMENTUM, 15, 10
    )
    monitor = await loaded_monitor(service, cfg)
    now = datetime.now(UTC)
    await monitor.handle_message(
        "market.market_trade.test",
        {"p": plan.entry_trigger - 0.1, "t": now.isoformat()},
        now=now,
    )
    crossing = now + timedelta(seconds=1)
    await monitor.handle_message(
        "market.market_trade.test",
        {"p": plan.entry_trigger + 0.01, "t": crossing.isoformat()},
        now=crossing,
    )
    saved = await store.get_plan(plan.plan_id)
    assert saved is not None and saved.triggered_at == crossing
    assert len(alerts(store, "TRIGGER_HIT")) == 1

    restarted = await loaded_monitor(service, cfg)
    later = crossing + timedelta(seconds=1)
    await restarted.handle_message(
        "market.market_trade.test",
        {"p": plan.entry_trigger - 0.1, "t": later.isoformat()},
        now=later,
    )
    await restarted.handle_message(
        "market.market_trade.test",
        {"p": plan.entry_trigger + 0.02, "t": (later + timedelta(seconds=1)).isoformat()},
        now=later + timedelta(seconds=1),
    )
    assert len(alerts(store, "TRIGGER_HIT")) == 1


@pytest.mark.asyncio
async def test_trigger_freshness_is_feed_agnostic_and_uses_quote_age():
    store = InMemoryEventStore()
    cfg = Settings(nats_url=None, data_plan="free", max_quote_age_seconds=5)
    service = DecisionService(store, cfg=cfg)
    monitor = PositionMonitor(service, cfg=cfg)
    now = datetime.now(UTC)
    assert cfg.alpaca_stream_url.endswith("/iex")
    assert monitor._fresh_quote({"t": now.isoformat()}, now) is True
    stale = now - timedelta(seconds=8)
    assert monitor._fresh_quote({"t": stale.isoformat()}, now) is False
    assert monitor._fresh_quote({}, now) is False


@pytest.mark.asyncio
async def test_hold_never_generates_alert():
    store, service, cfg, plan, position = await build_position(stop=95.0)
    position.plan_id = plan.plan_id
    await store.save_position(position)
    monitor = await loaded_monitor(service, cfg)
    now = datetime.now(UTC)
    await monitor.handle_message("market.bar_closed_1m.test", {"vw": 99.0}, now=now)
    await monitor.handle_message(
        "market.market_trade.test", {"p": 101.0, "t": now.isoformat()}, now=now
    )
    assert not alerts(store, "HOLD")
    actionable = [
        a for a in store.alerts.values()
        if a.kind in {
            "SELL_NOW", "PLACE_STOP_NOW", "RECONCILE_REQUIRED", "TRIM", "TAKE_PROFIT"
        }
    ]
    assert actionable == []


@pytest.mark.asyncio
async def test_failed_breakout_buffer_01r_below_trigger_holds():
    store, service, cfg, plan, position = await build_position(stop=99.0)
    position.average_fill = plan.entry_trigger
    position.stop = plan.stop
    position.tp1 = plan.entry_trigger + (plan.entry_trigger - plan.stop) * 1.5
    position.tp2 = plan.entry_trigger + (plan.entry_trigger - plan.stop) * 2.0
    await store.save_position(position)
    monitor = await loaded_monitor(service, cfg)
    now = datetime.now(UTC)
    await monitor.handle_message(
        "market.bar_closed_1m.test", {"vw": plan.entry_trigger + 1.0}, now=now
    )
    r_value = plan.entry_trigger - plan.stop
    last = plan.entry_trigger - 0.1 * r_value
    await monitor.handle_message(
        "market.market_trade.test", {"p": last, "t": now.isoformat()}, now=now
    )
    events = [e for e in store.events if e.event_type == "POSITION_EVALUATED"]
    assert events[-1].payload["action"] == "HOLD"
    assert events[-1].payload["failed_breakout"] is False


@pytest.mark.asyncio
async def test_failed_breakout_buffer_03r_and_below_vwap_sells():
    store, service, cfg, plan, position = await build_position(stop=99.0)
    position.average_fill = plan.entry_trigger
    position.stop = plan.stop
    position.tp1 = plan.entry_trigger + (plan.entry_trigger - plan.stop) * 1.5
    position.tp2 = plan.entry_trigger + (plan.entry_trigger - plan.stop) * 2.0
    await store.save_position(position)
    monitor = await loaded_monitor(service, cfg)
    now = datetime.now(UTC)
    await monitor.handle_message(
        "market.bar_closed_1m.test", {"vw": plan.entry_trigger + 1.0}, now=now
    )
    r_value = plan.entry_trigger - plan.stop
    last = plan.entry_trigger - 0.3 * r_value
    await monitor.handle_message(
        "market.market_trade.test", {"p": last, "t": now.isoformat()}, now=now
    )
    events = [e for e in store.events if e.event_type == "POSITION_EVALUATED"]
    assert events[-1].payload["action"] == "SELL_NOW"
    assert events[-1].payload["failed_breakout"] is True


class CountingStore(InMemoryEventStore):
    def __init__(self):
        super().__init__()
        self.calls: dict[str, int] = {}

    def _count(self, name: str) -> None:
        self.calls[name] = self.calls.get(name, 0) + 1

    async def list_positions(self):
        self._count("list_positions")
        return await super().list_positions()

    async def list_plans(self):
        self._count("list_plans")
        return await super().list_plans()

    async def get_position(self, position_id):
        self._count("get_position")
        return await super().get_position(position_id)

    async def get_wallet(self):
        self._count("get_wallet")
        return await super().get_wallet()

    async def append(self, event):
        self._count("append")
        return await super().append(event)

    async def save_alert(self, alert):
        self._count("save_alert")
        return await super().save_alert(alert)


@pytest.mark.asyncio
async def test_1000_unknown_symbol_ticks_touch_store_zero_times():
    store = CountingStore()
    cfg = Settings(nats_url=None, monitor_cache_refresh_seconds=5)
    service = DecisionService(store, cfg=cfg)
    monitor = PositionMonitor(service, cfg=cfg)
    await monitor.refresh_cache()
    store.calls.clear()
    now = datetime.now(UTC)
    for index in range(1000):
        await monitor.handle_message(
            "market.market_trade.unknown",
            {"p": 100.0 + index / 10000, "t": now.isoformat()},
            now=now,
        )
    assert sum(store.calls.values()) == 0


@pytest.mark.asyncio
async def test_1000_position_ticks_within_5_seconds_use_at_most_two_list_calls():
    base_store, _service, cfg, plan, position = await build_position(stop=95.0)
    store = CountingStore()
    for saved_plan in await base_store.list_plans():
        await store.save_plan(saved_plan)
    for saved_position in await base_store.list_positions():
        await store.save_position(saved_position)
    wallet = await base_store.get_wallet()
    assert wallet is not None
    await store.save_wallet(wallet)
    service = DecisionService(store, cfg=cfg, market_data=FakeProvider())
    monitor = PositionMonitor(service, cfg=cfg)
    await monitor.refresh_cache()
    now = datetime.now(UTC)
    for index in range(1000):
        tick_time = now + timedelta(milliseconds=index * 4)
        await monitor.handle_message(
            "market.market_trade.test",
            {"p": plan.entry_trigger + 0.2, "t": tick_time.isoformat()},
            now=tick_time,
        )
    list_calls = store.calls.get("list_positions", 0) + store.calls.get("list_plans", 0)
    assert list_calls <= 2
    assert position.position_id in store.positions


@pytest.mark.asyncio
async def test_service_hook_invalidates_symbol_cache_immediately():
    _store, service, cfg, _plan, _position = await build_position(stop=95.0)
    monitor = await loaded_monitor(service, cfg)
    assert "TEST" in monitor._positions_by_symbol
    await service._notify_state_change("TEST")
    assert "TEST" not in monitor._positions_by_symbol
    assert "TEST" not in monitor._plans_by_symbol


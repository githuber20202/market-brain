import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from market_brain.domain.models import PositionState, ProtectionState
from market_brain.ledger.events import LedgerEvent
from market_brain.ledger.store import InMemoryEventStore
from market_brain.runtime.stream_worker import (
    ResilientStreamWorker,
    desired_symbols,
    select_subscription_symbols,
)


class FakeStream:
    def __init__(self, messages=None):
        self.messages = list(messages or [])
        self.connected = 0
        self.closed = 0
        self.subscribed = []
        self.unsubscribed = []

    async def connect(self): self.connected += 1
    async def subscribe(self, symbols): self.subscribed.append(list(symbols))
    async def unsubscribe(self, symbols): self.unsubscribed.append(list(symbols))
    async def recv(self):
        if not self.messages: return None
        item = self.messages.pop(0)
        if isinstance(item, Exception): raise item
        return item
    def parse(self, raw):
        if raw == "bad": raise ValueError("bad message")
        return [LedgerEvent("MARKET_QUOTE", "SPY", {"T": "q", "S": "SPY"})]
    async def close(self): self.closed += 1


@pytest.mark.asyncio
async def test_disconnect_reconnects_and_resubscribes(monkeypatch):
    store = InMemoryEventStore(); monkeypatch.setenv("STREAM_SYMBOLS", "SPY")
    streams = [FakeStream([None]), FakeStream([None])]; calls = 0
    def factory():
        nonlocal calls
        stream = streams[calls]; calls += 1; return stream
    async def fake_sleep(_delay):
        if calls >= 2: worker._stop.set()
    worker = ResilientStreamWorker(store=store, stream_factory=factory, sleep=fake_sleep, rng=lambda: 0.5)
    await worker.run()
    assert calls == 2
    assert streams[0].subscribed[0] == ["SPY"]
    assert streams[1].subscribed[0] == ["SPY"]


@pytest.mark.asyncio
async def test_stale_connection_reconnects(monkeypatch):
    store = InMemoryEventStore(); monkeypatch.setenv("STREAM_SYMBOLS", "SPY")
    class StaleStream(FakeStream):
        async def recv(self): await asyncio.sleep(1)
    streams = [StaleStream(), FakeStream([None])]; calls = 0
    def factory():
        nonlocal calls
        stream = streams[calls]; calls += 1; return stream
    async def fake_sleep(_delay):
        if calls >= 2: worker._stop.set()
    worker = ResilientStreamWorker(store=store, stream_factory=factory, stale_seconds=0.01, refresh_seconds=60, sleep=fake_sleep, rng=lambda: 0.5)
    await worker.run(); assert calls == 2


@pytest.mark.asyncio
async def test_malformed_message_is_swallowed(monkeypatch):
    store = InMemoryEventStore(); monkeypatch.setenv("STREAM_SYMBOLS", "SPY")
    stream = FakeStream(["bad", None])
    worker = ResilientStreamWorker(store=store, stream_factory=lambda: stream)
    with pytest.raises(ConnectionError): await worker._consume_connection(stream)
    assert stream.connected == 1
    status = await store.get_runtime_status()
    assert status["stream_last_message_at"] is not None
    assert datetime.fromisoformat(status["stream_connected_since"]).tzinfo is not None


@pytest.mark.asyncio
async def test_open_position_changes_dynamic_subscriptions(monkeypatch):
    store = InMemoryEventStore(); monkeypatch.setenv("STREAM_SYMBOLS", "SPY")
    assert await desired_symbols(store) == {"SPY"}
    now = datetime.now(UTC)
    await store.save_position(PositionState(position_id="p1", plan_id="manual-p1", symbol="AAPL", quantity=1, remaining_quantity=1, average_fill=100.0, stop=95.0, tp1=107.5, tp2=110.0, opened_at=now, time_stop_at=now+timedelta(minutes=30), protection=ProtectionState.PROTECTED, broker_stop_price=95.0, protected_quantity=1))
    assert await desired_symbols(store) == {"AAPL", "SPY"}


@pytest.mark.asyncio
async def test_dynamic_position_change_calls_subscribe(monkeypatch):
    store = InMemoryEventStore()
    monkeypatch.setenv("STREAM_SYMBOLS", "SPY")
    stream = FakeStream()
    worker = ResilientStreamWorker(store=store, stream_factory=lambda: stream)
    await worker._sync_subscriptions(stream)
    assert stream.subscribed == [["SPY"]]

    now = datetime.now(UTC)
    await store.save_position(
        PositionState(
            position_id="p-dynamic",
            plan_id="manual-dynamic",
            symbol="AAPL",
            quantity=1,
            remaining_quantity=1,
            average_fill=100.0,
            stop=95.0,
            tp1=107.5,
            tp2=110.0,
            opened_at=now,
            time_stop_at=now + timedelta(minutes=30),
            protection=ProtectionState.PROTECTED,
            broker_stop_price=95.0,
            protected_quantity=1,
        )
    )
    await worker._sync_subscriptions(stream)
    assert stream.subscribed[-1] == ["AAPL"]


@pytest.mark.asyncio
async def test_stream_events_are_published_to_nats_contract():
    class Publisher:
        def __init__(self):
            self.rows = []

        async def publish(self, subject, payload):
            self.rows.append((subject, payload))

    publisher = Publisher()
    worker = ResilientStreamWorker(store=InMemoryEventStore(), publisher=publisher)
    await worker._publish(
        [LedgerEvent("MARKET_QUOTE", "AAPL", {"T": "q", "S": "AAPL", "bp": 100.0})]
    )
    assert publisher.rows[0][0] == "market.market_quote.aapl"
    assert b'"S":"AAPL"' in publisher.rows[0][1]



@pytest.mark.asyncio
async def test_last_message_status_write_is_throttled(monkeypatch):
    class CountingStatusStore(InMemoryEventStore):
        def __init__(self):
            super().__init__()
            self.last_message_writes = 0

        async def set_runtime_status(self, key, value):
            if key == "stream_last_message_at":
                self.last_message_writes += 1
            await super().set_runtime_status(key, value)

    class BurstStream(FakeStream):
        async def recv(self):
            if self.messages:
                return self.messages.pop(0)
            return None

    store = CountingStatusStore()
    monkeypatch.setenv("STREAM_SYMBOLS", "SPY")
    stream = BurstStream(["ok"] * 100 + [None])
    worker = ResilientStreamWorker(
        store=store,
        stream_factory=lambda: stream,
        status_write_interval_seconds=1.0,
    )
    with pytest.raises(ConnectionError):
        await worker._consume_connection(stream)
    assert store.last_message_writes == 1



@pytest.mark.asyncio
async def test_subscription_cap_prioritizes_positions_then_plans_then_watchlist():
    from types import SimpleNamespace

    from market_brain.domain.models import PlanStatus

    store = InMemoryEventStore()
    now = datetime.now(UTC)
    await store.save_position(
        PositionState(
            position_id="priority-pos",
            plan_id="manual-priority",
            symbol="AAPL",
            quantity=1,
            remaining_quantity=1,
            average_fill=100.0,
            stop=95.0,
            tp1=107.5,
            tp2=110.0,
            opened_at=now,
            time_stop_at=now + timedelta(minutes=30),
            protection=ProtectionState.PROTECTED,
            broker_stop_price=95.0,
            protected_quantity=1,
        )
    )
    store.plans["priority-plan"] = SimpleNamespace(
        status=PlanStatus.ACTIVE,
        symbol="MSFT",
    )
    selection = await select_subscription_symbols(
        store,
        cap=3,
        watchlist=["SPY", "QQQ", "NVDA"],
    )
    assert selection.selected == ["AAPL", "MSFT", "SPY"]
    assert selection.dropped == ["QQQ", "NVDA"]


@pytest.mark.asyncio
async def test_worker_persists_subscription_cap_and_dropped_symbols(monkeypatch):
    store = InMemoryEventStore()
    monkeypatch.setenv("STREAM_SYMBOLS", "SPY,QQQ,NVDA")
    stream = FakeStream()
    worker = ResilientStreamWorker(
        store=store,
        stream_factory=lambda: stream,
        stream_max_symbols=2,
    )
    await worker._sync_subscriptions(stream)
    status = await store.get_runtime_status()
    assert status["subscription_cap"] == 2
    assert status["subscribed_symbols"] == ["QQQ", "SPY"]
    assert status["dropped_symbols"] == ["NVDA"]


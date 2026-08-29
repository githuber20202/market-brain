from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

import market_brain.api.main as api_main
from market_brain.domain.models import AlertRecord, MarketSnapshot, StrategyLane
from market_brain.engines.quality import classify_quality
from market_brain.ledger.store import InMemoryEventStore
from market_brain.orchestration.service import DecisionService
from market_brain.settings import Settings
from tests.retest_helpers import activate_with_server_retest


class FakeProvider:
    async def snapshot(self, symbol: str, decision: bool = False) -> MarketSnapshot:
        assert decision is True
        return MarketSnapshot(
            symbol=symbol,
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


def planning_snapshot() -> MarketSnapshot:
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
    )


@pytest.mark.asyncio
async def test_inmemory_alert_round_trip_and_undelivered_listing():
    store = InMemoryEventStore()
    alert = AlertRecord(kind="BUY_NOW", payload={"symbol": "TEST"})
    await store.save_alert(alert)

    loaded = await store.get_alert(alert.alert_id)
    pending = await store.list_undelivered()

    assert loaded is alert
    assert [row.alert_id for row in pending] == [alert.alert_id]


@pytest.mark.asyncio
async def test_inmemory_mark_delivered_removes_from_undelivered():
    store = InMemoryEventStore()
    alert = AlertRecord(kind="SELL_NOW", payload={"symbol": "TEST"})
    await store.save_alert(alert)

    delivered = await store.mark_delivered(alert.alert_id)

    assert delivered is not None
    assert delivered.delivered_at is not None
    assert delivered.attempts == 1
    assert delivered.next_attempt_at is None
    assert await store.list_undelivered() == []


@pytest.mark.asyncio
async def test_inmemory_mark_failed_stays_undelivered():
    store = InMemoryEventStore()
    alert = AlertRecord(kind="PLACE_STOP_NOW", payload={"symbol": "TEST"})
    await store.save_alert(alert)
    retry_at = datetime.now(UTC) + timedelta(seconds=1)

    failed = await store.mark_failed(alert.alert_id, "temporary", retry_at)

    assert failed is not None
    assert failed.delivered_at is None
    assert failed.attempts == 1
    assert failed.last_error == "temporary"
    assert failed.next_attempt_at == retry_at
    assert [row.alert_id for row in await store.list_undelivered()] == [alert.alert_id]


@pytest.mark.asyncio
async def test_buy_now_creates_alert_and_returns_alert_id():
    store = InMemoryEventStore()
    service = DecisionService(store, market_data=FakeProvider())
    await service.seed_wallet(10_000, 10_000)
    quality = classify_quality("TEST", 90, datetime.now(UTC))
    plan, _ = await service.build_plan(
        planning_snapshot(), quality, StrategyLane.CORE_MOMENTUM, 15, 10
    )

    decision = await activate_with_server_retest(service, plan.plan_id)

    assert decision.state == "BUY_NOW"
    assert decision.alert_id is not None
    alert = await store.get_alert(decision.alert_id)
    assert alert is not None
    assert alert.kind == "BUY_NOW"
    assert alert.payload["plan_id"] == plan.plan_id
    emitted = [event for event in store.events if event.event_type == "BUY_NOW_EMITTED"]
    assert emitted[-1].payload["alert_id"] == decision.alert_id
    shadow = await store.get_shadow_trade(plan.plan_id)
    assert shadow is not None
    assert shadow.fill == pytest.approx(plan.entry_trigger * 1.001, abs=0.0001)
    assert [event.event_type for event in store.events][-1] == "SHADOW_TRADE_OPENED"


@pytest.mark.asyncio
async def test_live_label_mode_stays_brokerless_and_does_not_open_shadow_trade():
    store = InMemoryEventStore()
    service = DecisionService(
        store,
        cfg=Settings(run_mode="live"),
        market_data=FakeProvider(),
    )
    await service.seed_wallet(10_000, 10_000)
    quality = classify_quality("TEST", 90, datetime.now(UTC))
    plan, _ = await service.build_plan(
        planning_snapshot(), quality, StrategyLane.CORE_MOMENTUM, 15, 10
    )

    decision = await activate_with_server_retest(service, plan.plan_id)

    assert decision.state == "BUY_NOW"
    assert decision.order_ticket is not None
    assert await store.get_shadow_trade(plan.plan_id) is None
    assert not any(event.event_type == "SHADOW_TRADE_OPENED" for event in store.events)


def test_alerts_endpoint_lists_undelivered(monkeypatch):
    store = InMemoryEventStore()
    service = DecisionService(store)
    alert = AlertRecord(kind="RECONCILE_REQUIRED", payload={"symbol": "TEST"})

    import asyncio

    asyncio.run(store.save_alert(alert))
    monkeypatch.setattr(api_main, "service", service)

    with TestClient(api_main.app) as client:
        response = client.get("/alerts?undelivered=true")

    assert response.status_code == 200
    assert response.json()[0]["alert_id"] == alert.alert_id
    assert "next_attempt_at" in response.json()[0]

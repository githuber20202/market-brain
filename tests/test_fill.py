from dataclasses import asdict
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

import market_brain.api.main as api_main
from market_brain.domain.models import MarketSnapshot, PlanStatus, Reservation, StrategyLane
from market_brain.engines.quality import classify_quality
from market_brain.ledger.store import InMemoryEventStore
from market_brain.orchestration.service import DecisionService
from market_brain.settings import Settings


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
        retest_low=100.0,
        benchmark_return_pct=0.5,
        catalyst_verified=True,
        catalyst_strength=0.9,
    )


async def reserved_service(*, quantity: int = 10, cfg: Settings | None = None):
    store = InMemoryEventStore()
    service = DecisionService(store, cfg=cfg or Settings())
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
async def test_overspend_returns_422_and_wallet_unchanged(monkeypatch):
    service, store, plan = await reserved_service(quantity=10)
    wallet = await store.get_wallet()
    assert wallet is not None
    wallet.cash_available = 10.0
    await store.save_wallet(wallet)
    before = asdict(wallet)

    monkeypatch.setattr(api_main, "service", service)
    with TestClient(api_main.app) as client:
        response = client.post(
            "/fills/confirm",
            json={"plan_id": plan.plan_id, "fill_price": plan.entry_trigger, "quantity": 1},
        )

    assert response.status_code == 422
    assert response.json()["detail"] == "FILL_EXCEEDS_CASH_AVAILABLE"
    after = await store.get_wallet()
    assert after is not None
    assert asdict(after) == before


@pytest.mark.asyncio
async def test_fill_risk_above_tolerance_returns_422_with_allowed_quantity(monkeypatch):
    cfg = Settings(max_trade_risk_pct=0.02, fill_risk_tolerance=0.20)
    service, _store, plan = await reserved_service(quantity=3, cfg=cfg)
    fill_price = plan.entry_trigger + plan.risk_per_share * 0.24

    monkeypatch.setattr(api_main, "service", service)
    with TestClient(api_main.app) as client:
        response = client.post(
            "/fills/confirm",
            json={"plan_id": plan.plan_id, "fill_price": fill_price, "quantity": 3},
        )

    assert response.status_code == 422
    assert "FILL_RISK_EXCEEDS_TOLERANCE" in response.json()["detail"]
    assert "allowed_quantity=2" in response.json()["detail"]


@pytest.mark.asyncio
async def test_two_partial_fills_accumulate_entry_avg_and_recalculate_targets():
    service, store, plan = await reserved_service(quantity=10)
    first_price = plan.entry_trigger
    second_price = plan.entry_trigger + 0.1

    first = await service.confirm_fill(plan.plan_id, fill_price=first_price, quantity=6)
    assert first.quantity == 6
    assert first.entry_avg == pytest.approx(first_price)

    second = await service.confirm_fill(plan.plan_id, fill_price=second_price, quantity=4)
    expected_avg = (first_price * 6 + second_price * 4) / 10
    expected_r = expected_avg - plan.stop

    assert second.position_id == first.position_id
    assert second.quantity == 10
    assert second.remaining_quantity == 10
    assert second.entry_avg == pytest.approx(expected_avg)
    assert second.tp1 == pytest.approx(round(expected_avg + expected_r * 1.5, 4))
    assert second.tp2 == pytest.approx(round(expected_avg + expected_r * 2.0, 4))
    filled_plan = await store.get_plan(plan.plan_id)
    assert filled_plan is not None
    assert filled_plan.status == PlanStatus.FILLED
    assert await store.get_reservation(plan.plan_id) is None


@pytest.mark.asyncio
async def test_third_fill_after_full_quantity_is_rejected(monkeypatch):
    service, _store, plan = await reserved_service(quantity=10)
    await service.confirm_fill(plan.plan_id, fill_price=plan.entry_trigger, quantity=6)
    await service.confirm_fill(plan.plan_id, fill_price=plan.entry_trigger, quantity=4)

    monkeypatch.setattr(api_main, "service", service)
    with TestClient(api_main.app) as client:
        response = client.post(
            "/fills/confirm",
            json={"plan_id": plan.plan_id, "fill_price": plan.entry_trigger, "quantity": 1},
        )

    assert response.status_code == 422
    assert response.json()["detail"] == "PLAN_ALREADY_FILLED"


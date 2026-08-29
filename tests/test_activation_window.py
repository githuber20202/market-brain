from datetime import UTC, datetime, timedelta

import pytest

from market_brain.domain.models import (
    MarketSnapshot,
    PlanStatus,
    SignalState,
    StrategyLane,
    TradePlan,
)
from market_brain.engines.intraday import structure_key
from market_brain.ledger.store import InMemoryEventStore
from market_brain.orchestration.service import DecisionService
from market_brain.settings import Settings


class ActivationProvider:
    async def snapshot(self, symbol: str, decision: bool = False) -> MarketSnapshot:
        assert decision is True
        return MarketSnapshot(
            symbol=symbol,
            last=100.25,
            prior_close=98.0,
            bid=100.24,
            ask=100.26,
            vwap=99.80,
            data_age_seconds=1.0,
            source_id="AUTH",
            authoritative=True,
        )


def _plan() -> TradePlan:
    created_at = datetime(2026, 8, 28, 13, 50, tzinfo=UTC)
    return TradePlan(
        symbol="TEST",
        lane=StrategyLane.CORE_MOMENTUM,
        entry_trigger=100.0,
        entry_zone_high=100.50,
        stop=99.0,
        tp1=101.5,
        tp2=102.0,
        max_spread_pct=0.25,
        max_slippage_pct=0.30,
        created_at=created_at,
        expires_at=created_at + timedelta(minutes=30),
        quality_risk_multiplier=0.5,
        plan_id="activation-window-plan",
    )


@pytest.mark.asyncio
async def test_trigger_extends_activation_through_retest_window() -> None:
    store = InMemoryEventStore()
    cfg = Settings(retest_window_minutes=30)
    service = DecisionService(store, cfg=cfg, market_data=ActivationProvider())
    plan = _plan()
    await store.save_plan(plan)
    await service.seed_wallet(100_000, 100_000, now=plan.created_at)

    triggered_at = datetime(2026, 8, 28, 14, 15, tzinfo=UTC)
    assert await service.record_trigger_hit(
        plan.plan_id,
        last=100.10,
        triggered_at=triggered_at,
        source="TEST_BAR",
    )

    extended = await store.get_plan(plan.plan_id)
    assert extended is not None
    assert extended.expires_at == datetime(2026, 8, 28, 14, 50, tzinfo=UTC)
    trigger_event = next(row for row in store.events if row.event_type == "TRIGGER_HIT")
    assert trigger_event.payload["extended_expires_at"] == extended.expires_at.isoformat()

    await store.set_runtime_status(
        structure_key(plan.symbol, "2026-08-28"),
        {
            "symbol": plan.symbol,
            "session_date": "2026-08-28",
            "state": "RETEST_VALID",
            "reasons": ["SERVER_RETEST_VALID"],
        },
    )
    decision = await service.activate(
        plan.plan_id,
        now=datetime(2026, 8, 28, 14, 35, tzinfo=UTC),
    )
    assert decision.state == SignalState.BUY_NOW
    assert sum(row.event_type == "BUY_NOW_EMITTED" for row in store.events) == 1


@pytest.mark.asyncio
async def test_untriggered_plan_keeps_original_expiry() -> None:
    store = InMemoryEventStore()
    service = DecisionService(store, cfg=Settings(), market_data=ActivationProvider())
    plan = _plan()
    original_expiry = plan.expires_at
    await store.save_plan(plan)

    result = await service.sweep_expired(now=original_expiry + timedelta(seconds=1))

    expired = await store.get_plan(plan.plan_id)
    assert result["expired_plans"] == 1
    assert expired is not None
    assert expired.expires_at == original_expiry
    assert expired.status == PlanStatus.EXPIRED
    assert not any(row.event_type == "TRIGGER_HIT" for row in store.events)

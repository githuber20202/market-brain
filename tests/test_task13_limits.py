import json
from datetime import UTC, datetime, timedelta
from math import floor
from pathlib import Path

import pytest

from market_brain.domain.models import (
    MarketSnapshot,
    PlanStatus,
    Reservation,
    StrategyLane,
    TradePlan,
    WalletState,
)
from market_brain.engines.quality import classify_quality
from market_brain.engines.wallet import size_from_wallet
from market_brain.ledger.store import InMemoryEventStore
from market_brain.orchestration.service import DecisionService
from market_brain.settings import Settings
from tests.retest_helpers import activate_with_server_retest

ROOT = Path(__file__).resolve().parents[1]


class FakeProvider:
    def __init__(self, market_snapshot):
        self.market_snapshot = market_snapshot

    async def snapshot(self, symbol: str, decision: bool = False):
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


def risk_config():
    return json.loads((ROOT / "config/02-RISK_ENVELOPE.json").read_text())


def test_settings_defaults_are_loaded_from_risk_envelope(monkeypatch):
    monkeypatch.delenv("MAX_POSITION_NOTIONAL_PCT", raising=False)
    monkeypatch.delenv("MAX_CONCURRENT_POSITIONS", raising=False)
    cfg = Settings(_env_file=None)
    risk = risk_config()
    assert cfg.max_position_notional_pct == risk["max_position_notional_pct"]
    assert cfg.max_concurrent_positions == risk["max_concurrent_positions"]


def test_environment_can_override_risk_envelope(monkeypatch):
    monkeypatch.setenv("MAX_POSITION_NOTIONAL_PCT", "22")
    monkeypatch.setenv("MAX_CONCURRENT_POSITIONS", "3")
    cfg = Settings(_env_file=None)
    assert cfg.max_position_notional_pct == 22
    assert cfg.max_concurrent_positions == 3


def test_sizing_uses_configured_position_notional_limit():
    cfg = Settings(_env_file=None)
    plan = TradePlan(
        symbol="TEST",
        lane=StrategyLane.CORE_MOMENTUM,
        entry_trigger=100.0,
        entry_zone_high=100.0,
        stop=99.99,
        tp1=100.015,
        tp2=100.02,
        max_spread_pct=0.5,
        max_slippage_pct=0.1,
        created_at=datetime.now(UTC),
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
        quality_risk_multiplier=1.0,
    )
    wallet = WalletState(10_000, 10_000)
    result = size_from_wallet(wallet, plan)
    max_notional = wallet.capital_base * cfg.max_position_notional_pct / 100.0
    assert result.allowed
    assert result.quantity * plan.entry_zone_high <= max_notional + 1e-9
    assert (result.quantity + 1) * plan.entry_zone_high > max_notional


@pytest.mark.asyncio
async def test_next_position_is_blocked_at_configured_concurrency_limit():
    cfg = Settings(_env_file=None)
    store = InMemoryEventStore()
    provider = FakeProvider(snapshot())
    service = DecisionService(store, cfg=cfg, market_data=provider)
    await service.seed_wallet(10_000, 10_000)
    for index in range(cfg.max_concurrent_positions):
        await service.import_position(
            symbol=f"P{index}",
            quantity=1,
            average_fill=10.0,
            stop_order_price=9.0,
        )
    quality = classify_quality("TEST", 90, datetime.now(UTC))
    plan, _ = await service.build_plan(
        snapshot(), quality, StrategyLane.CORE_MOMENTUM, 15, 10
    )
    provider.market_snapshot = snapshot(last=plan.entry_trigger)
    decision = await activate_with_server_retest(service, plan.plan_id)
    assert decision.state == "WATCH"
    assert decision.reasons == ["MAX_CONCURRENT_POSITIONS_REACHED"]
    assert await store.get_reservation(plan.plan_id) is None


@pytest.mark.asyncio
async def test_confirm_fill_hard_rejects_configured_notional_limit():
    cfg = Settings(_env_file=None)
    store = InMemoryEventStore()
    service = DecisionService(store, cfg=cfg, market_data=FakeProvider(snapshot()))
    wallet = await service.seed_wallet(10_000, 10_000)
    quality = classify_quality("TEST", 90, datetime.now(UTC))
    plan, _ = await service.build_plan(
        snapshot(), quality, StrategyLane.CORE_MOMENTUM, 15, 10
    )
    plan.status = PlanStatus.RESERVED
    max_notional = wallet.capital_base * cfg.max_position_notional_pct / 100.0
    quantity = floor(max_notional / plan.entry_trigger) + 1
    reservation = Reservation(
        plan_id=plan.plan_id,
        quantity=quantity,
        reserved_cash=round(quantity * plan.entry_zone_high, 2),
        reserved_risk=round(quantity * plan.risk_per_share, 2),
        expires_at=datetime.now(UTC) + timedelta(minutes=1),
    )
    wallet.reserved_cash = reservation.reserved_cash
    wallet.open_risk = reservation.reserved_risk
    await store.save_wallet(wallet)
    await store.save_plan(plan)
    await store.save_reservation(reservation)
    with pytest.raises(ValueError, match="POSITION_NOTIONAL_LIMIT_EXCEEDED"):
        await service.confirm_fill(
            plan.plan_id,
            fill_price=plan.entry_trigger,
            quantity=quantity,
            stop_order_placed=True,
            stop_order_price=plan.stop,
        )


@pytest.mark.asyncio
async def test_manual_truth_import_is_not_hidden_by_concurrency_limit():
    cfg = Settings(_env_file=None)
    store = InMemoryEventStore()
    service = DecisionService(store, cfg=cfg, market_data=FakeProvider(snapshot()))
    await service.seed_wallet(10_000, 10_000)
    total = cfg.max_concurrent_positions + 1
    for index in range(total):
        await service.import_position(
            symbol=f"M{index}",
            quantity=1,
            average_fill=10.0,
            stop_order_price=9.0,
        )
    assert len(await store.list_positions()) == total


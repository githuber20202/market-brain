from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

import market_brain.api.main as api_main
from market_brain.domain.models import (
    IntradayStructureState,
    MarketSnapshot,
    StrategyLane,
)
from market_brain.engines.intraday import (
    compute_structure,
    new_intraday_structure,
    update_intraday_structure,
)
from market_brain.engines.quality import classify_quality
from market_brain.ledger.store import InMemoryEventStore, PostgresEventStore
from market_brain.orchestration.service import DecisionService
from market_brain.runtime.position_monitor import PositionMonitor
from market_brain.settings import Settings

EASTERN = ZoneInfo("America/New_York")


def bar(day, minute, *, high, low, close, volume=1000, vwap=100.0):
    stamp = datetime.combine(day, datetime.min.time(), EASTERN).replace(hour=9, minute=30) + timedelta(minutes=minute)
    return {
        "t": stamp.astimezone(UTC).isoformat(),
        "o": close,
        "h": high,
        "l": low,
        "c": close,
        "v": volume,
        "vw": vwap,
    }


def build_or(cfg, now):
    day = now.astimezone(EASTERN).date()
    structure = new_intraday_structure("TEST", now)
    rows = [
        bar(day, 0, high=100.2, low=99.6, close=100.0, vwap=99.9),
        bar(day, 1, high=100.4, low=99.8, close=100.2, vwap=100.0),
        bar(day, 2, high=100.5, low=99.9, close=100.3, vwap=100.1),
        bar(day, 3, high=100.7, low=100.0, close=100.5, vwap=100.2),
        bar(day, 4, high=100.8, low=100.1, close=100.6, vwap=100.3),
    ]
    for row in rows:
        structure = update_intraday_structure(structure, row, cfg, now=now)
    assert structure.state == IntradayStructureState.ARMED
    assert structure.opening_range_high == 100.8
    assert structure.opening_range_low == 99.6
    assert structure.running_vwap is not None
    return structure, day


def test_clean_breakout_without_pullback_is_not_retest_valid():
    cfg = Settings()
    now = datetime.now(UTC)
    structure, day = build_or(cfg, now)
    structure = update_intraday_structure(
        structure, bar(day, 5, high=101.2, low=100.75, close=101.0, vwap=100.5), cfg, now=now
    )
    assert structure.state == IntradayStructureState.BREAKOUT_SEEN
    structure = update_intraday_structure(
        structure, bar(day, 6, high=101.4, low=101.05, close=101.3, vwap=100.7), cfg, now=now
    )
    assert structure.state == IntradayStructureState.BREAKOUT_SEEN
    assert structure.reasons == ["RETEST_WINDOW_OPEN"]


def test_following_bar_touch_and_close_above_orh_is_valid():
    cfg = Settings()
    now = datetime.now(UTC)
    structure, day = build_or(cfg, now)
    structure = update_intraday_structure(
        structure, bar(day, 5, high=101.2, low=100.75, close=101.0, vwap=100.5), cfg, now=now
    )
    structure = update_intraday_structure(
        structure, bar(day, 6, high=101.0, low=100.70, close=100.9, vwap=100.6), cfg, now=now
    )
    assert structure.state == IntradayStructureState.RETEST_VALID
    assert structure.reasons == ["SERVER_RETEST_VALID"]


def test_touch_but_close_below_orh_is_not_valid():
    cfg = Settings()
    now = datetime.now(UTC)
    structure, day = build_or(cfg, now)
    structure = update_intraday_structure(
        structure, bar(day, 5, high=101.2, low=100.75, close=101.0, vwap=100.5), cfg, now=now
    )
    structure = update_intraday_structure(
        structure, bar(day, 6, high=100.9, low=100.70, close=100.75, vwap=100.6), cfg, now=now
    )
    assert structure.state == IntradayStructureState.ARMED
    assert structure.reasons == ["RETEST_CLOSE_BELOW_ORH"]


def test_bar_below_orh_minus_point25r_invalidates_structure():
    cfg = Settings()
    now = datetime.now(UTC)
    structure, day = build_or(cfg, now)
    structure = update_intraday_structure(
        structure, bar(day, 5, high=101.2, low=100.75, close=101.0, vwap=100.5), cfg, now=now
    )
    structure = update_intraday_structure(
        structure, bar(day, 6, high=100.8, low=100.49, close=100.7, vwap=100.6), cfg, now=now
    )
    assert structure.state == IntradayStructureState.INVALID
    assert "RETEST_INVALIDATED_BELOW_ORH_BUFFER" in structure.reasons


class SnapshotProvider:
    configured = False

    def __init__(self, snapshot):
        self.market_snapshot = snapshot

    async def snapshot(self, symbol, decision=False):
        self.market_snapshot.symbol = symbol
        return self.market_snapshot


async def valid_service():
    cfg = Settings(data_plan="plus", decision_feed="sip")
    provider = SnapshotProvider(
        MarketSnapshot(
            symbol="TEST", last=100.8, bid=100.79, ask=100.81, vwap=100.5,
            data_age_seconds=1.0, source_id="ALPACA_SIP", authoritative=True,
        )
    )
    store = InMemoryEventStore()
    service = DecisionService(store, cfg=cfg, market_data=provider)
    await service.seed_wallet(10_000, 10_000)
    quality = classify_quality("TEST", 90, datetime.now(UTC))
    planning = MarketSnapshot(
        symbol="TEST", last=100.8, prior_close=98.0, bid=100.79, ask=100.81,
        volume=2_000_000, avg_volume=1_000_000, vwap=100.0, open_price=99.0,
        opening_range_high=100.8, retest_low=100.0, benchmark_return_pct=0.5,
        catalyst_verified=True, catalyst_strength=0.9,
    )
    plan, _ = await service.build_plan(planning, quality, StrategyLane.CORE_MOMENTUM, 15, 10)
    provider.market_snapshot.last = plan.entry_trigger
    provider.market_snapshot.bid = plan.entry_trigger - 0.01
    provider.market_snapshot.ask = plan.entry_trigger + 0.01
    now = datetime.now(UTC)
    day = now.astimezone(EASTERN).date()
    opening_rows = [
        bar(day, 0, high=100.2, low=99.6, close=100.0, vwap=99.9),
        bar(day, 1, high=100.4, low=99.8, close=100.2, vwap=100.0),
        bar(day, 2, high=100.5, low=99.9, close=100.3, vwap=100.1),
        bar(day, 3, high=100.7, low=100.0, close=100.5, vwap=100.2),
        bar(day, 4, high=100.8, low=100.1, close=100.6, vwap=100.3),
    ]
    for row in opening_rows:
        await service.record_intraday_bar("TEST", row, now=now)
    await service.record_intraday_bar(
        "TEST", bar(day, 5, high=101.2, low=100.75, close=101.0, vwap=100.5), now=now
    )
    await service.record_intraday_bar(
        "TEST", bar(day, 6, high=101.0, low=100.70, close=100.9, vwap=100.6), now=now
    )
    return store, service, plan


@pytest.mark.asyncio
async def test_activate_uses_server_retest_and_records_server_source():
    store, service, plan = await valid_service()
    decision = await service.activate(plan.plan_id)
    assert decision.state == "BUY_NOW"
    events = [e for e in store.events if e.event_type == "PLAN_EVALUATED" and e.aggregate_id == plan.plan_id]
    assert events[-1].payload["retest_valid"] is True
    assert events[-1].payload["retest_valid_source"] == "SERVER"
    assert events[-1].payload["retest_state"] == "RETEST_VALID"


def test_activate_http_body_is_empty_and_manual_retest_is_rejected():
    client = TestClient(api_main.app)
    assert client.post("/plans/not-found/activate").status_code == 200
    response = client.post("/plans/not-found/activate", json={"retest_valid": True})
    assert response.status_code == 422


class TrackingStore(InMemoryEventStore):
    def __init__(self):
        super().__init__()
        self.depth = 0
        self.saved_inside = False
        self.event_inside = False

    @asynccontextmanager
    async def transaction(self):
        self.depth += 1
        try:
            yield self
        finally:
            self.depth -= 1

    async def save_liquidity_profile(self, profile):
        self.saved_inside = self.depth > 0
        await super().save_liquidity_profile(profile)

    async def append(self, event):
        if event.event_type == "LIQUIDITY_PROFILE_REFRESHED":
            self.event_inside = self.depth > 0
        await super().append(event)


class LiquidityProvider:
    configured = True

    def __init__(self, store, now):
        self.store = store
        self.now = now

    async def bars(self, symbol, timeframe, start, end):
        assert self.store.depth == 0
        return [
            {"t": (self.now - timedelta(days=25-index)).isoformat(), "v": 3_000_000 + index, "c": 100 + index / 10}
            for index in range(25)
        ]


@pytest.mark.asyncio
async def test_liquidity_http_fetch_happens_before_short_persistence_transaction():
    store = TrackingStore()
    now = datetime.now(UTC)
    service = DecisionService(store, market_data=LiquidityProvider(store, now))
    profile = await service.refresh_liquidity_profile("TEST", now=now)
    assert profile.adv20 > 0
    assert store.saved_inside is True
    assert store.event_inside is True


@pytest.mark.asyncio
async def test_monitor_restart_uses_persisted_intraday_structure():
    store, service, plan = await valid_service()
    monitor = PositionMonitor(service, cfg=service.cfg)
    await monitor.refresh_cache()
    status_before = await store.get_runtime_status()
    restarted = PositionMonitor(service, cfg=service.cfg)
    await restarted.refresh_cache()
    status_after = await store.get_runtime_status()
    assert status_after == status_before
    structure = await service.get_intraday_structure(plan.symbol)
    assert structure is not None
    assert structure.state == IntradayStructureState.RETEST_VALID


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_intraday_runtime_status_survives_new_postgres_store(pg_store):
    now = datetime.now(UTC)
    cfg = Settings()
    structure, _day = build_or(cfg, now)
    key = f"intraday_structure:{structure.session_date}:TEST"
    await pg_store.set_runtime_status(key, asdict(structure))
    other = PostgresEventStore(pg_store.dsn)
    try:
        payload = (await other.get_runtime_status())[key]
        assert payload["symbol"] == "TEST"
        assert payload["state"] == "ARMED"
    finally:
        await other.close()


def test_retest_seven_minutes_after_breakout_is_valid():
    cfg = Settings(retest_window_minutes=30)
    now = datetime.now(UTC)
    structure, day = build_or(cfg, now)
    structure = update_intraday_structure(
        structure, bar(day, 5, high=101.2, low=100.75, close=101.0, vwap=100.5), cfg, now=now
    )
    for minute in range(6, 12):
        structure = update_intraday_structure(
            structure, bar(day, minute, high=101.5, low=101.1, close=101.3, vwap=100.7), cfg, now=now
        )
        assert structure.state == IntradayStructureState.BREAKOUT_SEEN
    structure = update_intraday_structure(
        structure, bar(day, 12, high=101.2, low=100.72, close=100.95, vwap=100.65), cfg, now=now
    )
    assert structure.state == IntradayStructureState.RETEST_VALID


def test_retest_after_thirty_one_minutes_expires_window():
    cfg = Settings(retest_window_minutes=30)
    now = datetime.now(UTC)
    structure, day = build_or(cfg, now)
    structure = update_intraday_structure(
        structure, bar(day, 5, high=101.2, low=100.75, close=101.0, vwap=100.5), cfg, now=now
    )
    structure = update_intraday_structure(
        structure, bar(day, 36, high=101.3, low=100.72, close=100.95, vwap=100.6), cfg, now=now
    )
    assert structure.state == IntradayStructureState.ARMED
    assert structure.reasons == ["RETEST_WINDOW_EXPIRED"]


def test_compute_structure_is_deterministic_and_sip_wins_same_minute():
    cfg = Settings()
    now = datetime.now(UTC)
    day = now.astimezone(EASTERN).date()
    rows = [
        bar(day, 0, high=100.2, low=99.6, close=100.0, vwap=99.9),
        bar(day, 1, high=100.4, low=99.8, close=100.2, vwap=100.0),
        bar(day, 2, high=100.5, low=99.9, close=100.3, vwap=100.1),
        bar(day, 3, high=100.7, low=100.0, close=100.5, vwap=100.2),
        {**bar(day, 4, high=100.75, low=100.1, close=100.55, vwap=100.25), "source": "IEX"},
        {**bar(day, 4, high=100.8, low=100.05, close=100.6, vwap=100.3), "source": "SIP"},
    ]
    first = compute_structure("TEST", day.isoformat(), rows, cfg, now=now)
    second = compute_structure("TEST", day.isoformat(), list(reversed(rows)), cfg, now=now)
    assert asdict(first) == asdict(second)
    assert first.opening_range_high == 100.8
    assert first.opening_range_low == 99.6


class KeyOnlyStore(InMemoryEventStore):
    async def get_runtime_status(self):
        raise AssertionError("full runtime_status scan is forbidden on intraday hot path")


@pytest.mark.asyncio
async def test_intraday_hot_path_uses_runtime_status_key_only():
    store = KeyOnlyStore()
    service = DecisionService(store)
    now = datetime.now(UTC)
    day = now.astimezone(EASTERN).date()
    opening_rows = [
        bar(day, 0, high=100.2, low=99.6, close=100.0, vwap=99.9),
        bar(day, 1, high=100.4, low=99.8, close=100.2, vwap=100.0),
        bar(day, 2, high=100.5, low=99.9, close=100.3, vwap=100.1),
        bar(day, 3, high=100.7, low=100.0, close=100.5, vwap=100.2),
        bar(day, 4, high=100.8, low=100.1, close=100.6, vwap=100.3),
    ]
    for row in opening_rows:
        await service.record_intraday_bar("TEST", row, now=now)
    loaded = await service.get_intraday_structure("TEST", now=now)
    assert loaded is not None
    assert loaded.state == IntradayStructureState.ARMED
    updated = await service.record_intraday_bar(
        "TEST", bar(day, 5, high=101.2, low=100.75, close=101.0, vwap=100.5), now=now
    )
    assert updated.state == IntradayStructureState.BREAKOUT_SEEN


from __future__ import annotations

import json
import subprocess
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

import market_brain.runtime.state as state_module
from market_brain.alerts.dispatcher import AlertDispatcher
from market_brain.alerts.sink import GitHubIssueSink
from market_brain.domain.models import (
    AlertRecord,
    LiquidityProfile,
    MarketSnapshot,
    ShadowTradeStatus,
    StrategyLane,
    TradePlan,
)
from market_brain.ledger.events import LedgerEvent
from market_brain.ledger.store import InMemoryEventStore
from market_brain.orchestration.service import DecisionService
from market_brain.orchestration.universe import NyseMarketCalendar
from market_brain.runtime.batch import BatchRuntime
from market_brain.runtime.shadow import ShadowEvaluator
from market_brain.runtime.state import publish_state_branch, restore_state
from market_brain.settings import Settings
from scripts.batch_gate import should_run


class FakeScheduler:
    def __init__(self):
        self.calendar = NyseMarketCalendar({}, {2026})
        self.calls: list[datetime] = []
        self.completed: set[datetime] = set()
        self.universe = ()

    def validate_startup(self, *, now):
        del now

    async def run_pending(self, *, now):
        minute = now.replace(second=0, microsecond=0)
        self.calls.append(now)
        if minute in self.completed:
            return None
        self.completed.add(minute)
        return {
            "status": "COMPLETED",
            "scheduled_for": minute.isoformat(),
        }

    async def run_slot(self, slot, *, now):
        minute = slot.replace(second=0, microsecond=0)
        if minute in self.completed:
            return None
        self.calls.append(now)
        self.completed.add(minute)
        return {"status": "COMPLETED", "scheduled_for": minute.isoformat()}

    async def mark_missed(self, slot, *, now):
        del now
        minute = slot.replace(second=0, microsecond=0)
        return {"status": "MISSED", "scheduled_for": minute.isoformat()}


class FakeShadow:
    def validate_startup(self, *, now):
        del now

    async def run_pending(self, *, now):
        del now
        return 2

    async def evaluate_now(self, *, now):
        del now
        return 2


class FakeService:
    async def sweep_expired(self, *, now=None):
        del now
        return {"expired_plans": 0, "released_reservations": 0}


class FakeDispatcher:
    def __init__(self):
        self.calls = 0

    async def dispatch_once(self, *, now):
        del now
        self.calls += 1
        return 0


class FakeIssueSink:
    async def aclose(self):
        return None


class FakeProvider:
    async def aclose(self):
        return None


class PlanWatchProvider:
    configured = True

    def __init__(self, bars, *, current_last: float = 100.55):
        self.bars = list(bars)
        self.current_last = current_last

    async def snapshot(self, symbol: str, decision: bool = False):
        del decision
        return MarketSnapshot(
            symbol=symbol,
            last=self.current_last,
            prior_close=98.0,
            bid=100.50,
            ask=100.60,
            vwap=99.80,
            data_age_seconds=60.0,
            source_id="YAHOO_DELAYED",
            delay_minutes=1.0,
            authoritative=True,
            metadata={
                "last_bar_high": 100.6,
                "last_bar_low": 100.2,
                "price_cross_check": "PASS",
            },
        )

    async def bars_batch(self, symbols, timeframe, start, end):
        del timeframe
        return {
            symbol: [
                row
                for row in self.bars
                if start <= datetime.fromisoformat(row["t"]) < end
            ]
            for symbol in symbols
        }

    async def aclose(self):
        return None


class FakeDigest:
    async def create(self, *, now, run_id):
        return AlertRecord(
            kind="DAILY_DIGEST",
            payload={
                "session_date": now.date().isoformat(),
                "text": f"digest {run_id}",
            },
            created_at=now,
        )


def _runtime(tmp_path: Path):
    store = InMemoryEventStore()
    scheduler = FakeScheduler()
    runtime = BatchRuntime(
        store=store,
        service=FakeService(),
        provider=FakeProvider(),
        scheduler=scheduler,
        shadow=FakeShadow(),
        digest=FakeDigest(),
        dispatcher=FakeDispatcher(),
        issue_sink=FakeIssueSink(),
        cfg=Settings(run_mode="live"),
        output_dir=tmp_path / "reports",
        state_dir=tmp_path / "state",
    )
    return runtime, scheduler


@pytest.mark.asyncio
async def test_batch_fails_closed_before_radar_on_replay_difference(tmp_path):
    runtime, scheduler = _runtime(tmp_path)
    now = datetime(2026, 8, 28, 13, 50, tzinfo=UTC)
    await runtime.store.save_plan(
        TradePlan(
            symbol="SPY",
            lane=StrategyLane.CORE_MOMENTUM,
            entry_trigger=100.0,
            entry_zone_high=100.1,
            stop=99.0,
            tp1=101.5,
            tp2=102.0,
            max_spread_pct=0.25,
            max_slippage_pct=0.30,
            created_at=now,
            expires_at=now.replace(hour=14),
            quality_risk_multiplier=0.5,
        )
    )

    with pytest.raises(RuntimeError, match="STATE_INTEGRITY"):
        await runtime.run("radar", now=now)

    assert scheduler.calls == []
    alerts = await runtime.store.list_alerts()
    assert alerts[-1].kind == "STATE_INTEGRITY"


@pytest.mark.asyncio
async def test_batch_radar_marks_old_slots_missed_and_runs_latest_with_real_now(tmp_path):
    runtime, scheduler = _runtime(tmp_path)
    now = datetime(2026, 8, 28, 18, 50, tzinfo=UTC)

    first = await runtime.run("radar", now=now)
    second = await runtime.run("radar", now=now)

    assert first["due_slots"] == 11
    assert len(first["runs"]) == 1
    assert first["missed_slots"] == 10
    assert second["runs"] == []
    assert scheduler.calls == [now]
    latest = json.loads((tmp_path / "state" / "latest.json").read_text())
    assert latest["mode"] == "radar"


@pytest.mark.asyncio
async def test_batch_digest_catches_up_after_1620_and_writes_report(tmp_path):
    runtime, _scheduler = _runtime(tmp_path)

    result = await runtime.run(
        "digest",
        now=datetime(2026, 8, 28, 20, 27, tzinfo=UTC),
    )

    assert result["status"] == "COMPLETED"
    report = Path(result["report"])
    assert report.name == "digest_2026-08-28.md"
    assert "# Shadow digest: 2026-08-28" in report.read_text()


@pytest.mark.asyncio
async def test_weekly_batch_refreshes_quality_into_state(tmp_path, monkeypatch):
    runtime, scheduler = _runtime(tmp_path)
    scheduler.universe = (
        SimpleNamespace(
            symbol="FULL", instrument_type="EQUITY", ranking_eligible=True
        ),
        SimpleNamespace(
            symbol="PART", instrument_type="EQUITY", ranking_eligible=True
        ),
        SimpleNamespace(symbol="SPY", instrument_type="ETF", ranking_eligible=True),
        SimpleNamespace(
            symbol="CHG", instrument_type="UNRESOLVED", ranking_eligible=False
        ),
    )
    calls: dict[str, object] = {}

    async def fake_quality(
        symbols,
        *,
        output_path,
        now,
        quality_source,
        skipped_instruments,
    ):
        calls["quality"] = (
            symbols,
            output_path,
            now,
            quality_source,
            skipped_instruments,
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("symbol,quality_score,as_of,source,partial\n")
        return {"status": "COMPLETED", "rows": 2}

    async def fake_replay(**kwargs):
        calls["replay"] = kwargs
        return tmp_path / "reports" / "replay.md"

    async def fake_shadow(*_args, **kwargs):
        calls["shadow"] = kwargs
        return tmp_path / "reports" / "shadow.md"

    monkeypatch.setattr("scripts.quality_refresh.refresh_quality", fake_quality)
    monkeypatch.setattr("scripts.replay_report.create_replay_report", fake_replay)
    monkeypatch.setattr("scripts.shadow_report.create_shadow_report", fake_shadow)
    now = datetime(2026, 8, 28, 21, 30, tzinfo=UTC)

    result = await runtime.run("weekly", now=now)

    assert result["quality"] == {"status": "COMPLETED", "rows": 2}
    assert calls["quality"] == (
        ["FULL", "PART"],
        tmp_path / "state" / "quality.csv",
        now,
        "yahoo",
        [
            {"symbol": "CHG", "instrument_type": "UNRESOLVED"},
            {"symbol": "SPY", "instrument_type": "ETF"},
        ],
    )
    assert calls["replay"]["symbols"] == ["FULL", "PART", "SPY"]
    latest = json.loads((tmp_path / "state" / "latest.json").read_text())
    assert latest["quality"] == {"status": "COMPLETED", "rows": 2}


def _plan_watch_bar(minute, *, high, low, close):
    stamp = datetime(2026, 8, 28, 13, 30, tzinfo=UTC) + timedelta(minutes=minute)
    return {
        "t": stamp.isoformat(),
        "o": close,
        "h": high,
        "l": low,
        "c": close,
        "v": 10_000,
        "vw": 99.8,
    }


@pytest.mark.asyncio
async def test_batch_plan_watch_full_shadow_path_is_idempotent(tmp_path):
    calendar_path = tmp_path / "calendar.csv"
    calendar_path.write_text(
        "date,status,open_time,close_time,source\n"
        "2026-09-07,CLOSED,,,NYSE\n"
        "2027-01-01,CLOSED,,,NYSE\n"
    )
    cfg = Settings(market_calendar_path=calendar_path, run_mode="shadow")
    store = InMemoryEventStore()
    bars = [
        _plan_watch_bar(0, high=99.6, low=99.0, close=99.4),
        _plan_watch_bar(1, high=99.7, low=99.2, close=99.5),
        _plan_watch_bar(2, high=99.8, low=99.3, close=99.6),
        _plan_watch_bar(3, high=99.9, low=99.4, close=99.7),
        _plan_watch_bar(4, high=100.0, low=99.5, close=99.8),
        _plan_watch_bar(5, high=100.6, low=99.9, close=100.4),
        _plan_watch_bar(6, high=100.4, low=99.95, close=100.2),
    ]
    provider = PlanWatchProvider(bars)
    service = DecisionService(store, cfg=cfg, market_data=provider)
    created_at = datetime(2026, 8, 28, 13, 34, tzinfo=UTC)
    plan = TradePlan(
        symbol="SPY",
        lane=StrategyLane.CORE_MOMENTUM,
        entry_trigger=100.0,
        entry_zone_high=100.75,
        stop=99.0,
        tp1=101.5,
        tp2=102.0,
        max_spread_pct=0.25,
        max_slippage_pct=0.30,
        created_at=created_at,
        expires_at=created_at + timedelta(minutes=30),
        quality_risk_multiplier=0.5,
        plan_id="batch-plan",
    )
    await store.save_plan(plan)
    await store.append(
        LedgerEvent(
            "PLAN_ISSUED",
            plan.plan_id,
            {"plan": asdict(plan)},
            occurred_at=created_at,
        )
    )
    await store.save_liquidity_profile(
        LiquidityProfile(
            symbol="SPY",
            adv20=10_000_000,
            close=98.0,
            as_of=created_at - timedelta(days=1),
            refreshed_at=created_at,
        )
    )
    shadow = ShadowEvaluator(
        store,
        cfg=cfg,
        backfill=service.backfill_intraday_structures,
    )
    first_now = datetime(2026, 8, 28, 13, 40, tzinfo=UTC)
    shadow.validate_startup(now=first_now)
    runtime = BatchRuntime(
        store=store,
        service=service,
        provider=provider,
        scheduler=FakeScheduler(),
        shadow=shadow,
        digest=FakeDigest(),
        dispatcher=FakeDispatcher(),
        issue_sink=FakeIssueSink(),
        cfg=cfg,
        output_dir=tmp_path / "reports",
        state_dir=tmp_path / "state",
    )

    assert await runtime._ensure_shadow_wallet(first_now) is True
    assert await runtime._ensure_shadow_wallet(first_now) is False
    first = await runtime._run_plan_watch(first_now)

    assert first["trigger_hits"] == 1
    assert first["activation_rejected"] == 0, await store.get_runtime_status_key(
        f"activation_rejected:{plan.plan_id}"
    )
    assert first["buy_now"] == 1, first
    assert first["reservations_released"] == 1
    assert await store.get_reservation(plan.plan_id) is None
    assert await store.get_shadow_trade(plan.plan_id) is not None
    assert sum(event.event_type == "WALLET_SEEDED" for event in store.events) == 1
    assert sum(event.event_type == "BUY_NOW_EMITTED" for event in store.events) == 1
    assert sum(
        event.event_type == "SHADOW_RESERVATION_RELEASED" for event in store.events
    ) == 1

    provider.bars.append(
        _plan_watch_bar(11, high=100.7, low=98.8, close=99.0)
    )
    second_now = datetime(2026, 8, 28, 13, 44, tzinfo=UTC)
    second = await runtime._run_plan_watch(second_now)
    assert second["buy_now"] == 0
    assert sum(event.event_type == "BUY_NOW_EMITTED" for event in store.events) == 1
    assert await shadow.evaluate_now(now=second_now) == 1
    trade = await store.get_shadow_trade(plan.plan_id)
    assert trade is not None and trade.status == ShadowTradeStatus.STOPPED


@pytest.mark.asyncio
async def test_batch_shadow_activates_at_retest_bar_not_delayed_detection_price(tmp_path):
    calendar_path = tmp_path / "calendar.csv"
    calendar_path.write_text(
        "date,status,open_time,close_time,source\n"
        "2026-09-07,CLOSED,,,NYSE\n"
        "2027-01-01,CLOSED,,,NYSE\n"
    )
    cfg = Settings(market_calendar_path=calendar_path, run_mode="shadow")
    store = InMemoryEventStore()
    bars = [
        _plan_watch_bar(0, high=99.6, low=99.0, close=99.4),
        _plan_watch_bar(1, high=99.7, low=99.2, close=99.5),
        _plan_watch_bar(2, high=99.8, low=99.3, close=99.6),
        _plan_watch_bar(3, high=99.9, low=99.4, close=99.7),
        _plan_watch_bar(4, high=100.0, low=99.5, close=99.8),
        _plan_watch_bar(36, high=100.7, low=100.1, close=100.5),
        _plan_watch_bar(37, high=100.35, low=99.95, close=100.2),
    ]
    provider = PlanWatchProvider(bars, current_last=102.0)
    service = DecisionService(store, cfg=cfg, market_data=provider)
    created_at = datetime(2026, 8, 28, 13, 50, tzinfo=UTC)
    plan = TradePlan(
        symbol="SPY",
        lane=StrategyLane.CORE_MOMENTUM,
        entry_trigger=100.0,
        entry_zone_high=100.3,
        stop=99.0,
        tp1=101.5,
        tp2=102.0,
        max_spread_pct=0.25,
        max_slippage_pct=0.30,
        created_at=created_at,
        expires_at=created_at + timedelta(minutes=30),
        quality_risk_multiplier=0.5,
        plan_id="retest-basis-plan",
    )
    await store.save_plan(plan)
    await store.append(
        LedgerEvent("PLAN_ISSUED", plan.plan_id, {"plan": asdict(plan)}, occurred_at=created_at)
    )
    await store.save_liquidity_profile(
        LiquidityProfile(
            symbol="SPY",
            adv20=10_000_000,
            close=98.0,
            as_of=created_at - timedelta(days=1),
            refreshed_at=created_at,
        )
    )
    shadow = ShadowEvaluator(store, cfg=cfg, backfill=service.backfill_intraday_structures)
    detected_at = datetime(2026, 8, 28, 14, 15, tzinfo=UTC)
    shadow.validate_startup(now=detected_at)
    runtime = BatchRuntime(
        store=store,
        service=service,
        provider=provider,
        scheduler=FakeScheduler(),
        shadow=shadow,
        digest=FakeDigest(),
        dispatcher=FakeDispatcher(),
        issue_sink=FakeIssueSink(),
        cfg=cfg,
        output_dir=tmp_path / "reports",
        state_dir=tmp_path / "state",
    )
    await runtime._ensure_shadow_wallet(detected_at)

    result = await runtime._run_plan_watch(detected_at)

    assert result["buy_now"] == 1, result
    trade = await store.get_shadow_trade(plan.plan_id)
    assert trade is not None
    assert trade.opened_at == datetime(2026, 8, 28, 14, 7, 59, tzinfo=UTC)
    assert trade.fill == pytest.approx(100.3002)
    emitted = next(event for event in store.events if event.event_type == "BUY_NOW_EMITTED")
    context = emitted.payload["activation_context"]
    assert context["activation_basis"] == "RETEST_BAR"
    assert context["current_price"] == 102.0
    assert context["virtual_entry"] == pytest.approx(100.3002)
    alert = next(row for row in await store.list_alerts() if row.kind == "BUY_NOW")
    assert "virtual_entry=100.3002" in alert.payload["text"]
    assert "current_price=102.0000" in alert.payload["text"]

    provider.bars.append(_plan_watch_bar(38, high=100.25, low=98.8, close=99.0))
    await service.backfill_intraday_structures(["SPY"], now=detected_at)
    assert await shadow.evaluate_now(now=detected_at) == 1
    trade = await store.get_shadow_trade(plan.plan_id)
    assert trade is not None and trade.status == ShadowTradeStatus.STOPPED
    assert trade.last_bar_at == datetime(2026, 8, 28, 14, 8, tzinfo=UTC)


@pytest.mark.asyncio
async def test_shadow_retest_close_above_entry_zone_is_no_chase(tmp_path):
    calendar_path = tmp_path / "calendar.csv"
    calendar_path.write_text(
        "date,status,open_time,close_time,source\n"
        "2026-09-07,CLOSED,,,NYSE\n"
        "2027-01-01,CLOSED,,,NYSE\n"
    )
    cfg = Settings(market_calendar_path=calendar_path, run_mode="shadow")
    store = InMemoryEventStore()
    bars = [
        _plan_watch_bar(0, high=99.6, low=99.0, close=99.4),
        _plan_watch_bar(1, high=99.7, low=99.2, close=99.5),
        _plan_watch_bar(2, high=99.8, low=99.3, close=99.6),
        _plan_watch_bar(3, high=99.9, low=99.4, close=99.7),
        _plan_watch_bar(4, high=100.0, low=99.5, close=99.8),
        _plan_watch_bar(36, high=100.8, low=100.1, close=100.6),
        _plan_watch_bar(37, high=100.6, low=99.95, close=100.4),
    ]
    provider = PlanWatchProvider(bars, current_last=100.1)
    service = DecisionService(store, cfg=cfg, market_data=provider)
    created_at = datetime(2026, 8, 28, 13, 50, tzinfo=UTC)
    plan = TradePlan(
        symbol="SPY", lane=StrategyLane.CORE_MOMENTUM,
        entry_trigger=100.0, entry_zone_high=100.3, stop=99.0,
        tp1=101.5, tp2=102.0, max_spread_pct=0.25, max_slippage_pct=0.30,
        created_at=created_at, expires_at=created_at + timedelta(minutes=30),
        quality_risk_multiplier=0.5, plan_id="retest-no-chase",
    )
    await store.save_plan(plan)
    await store.append(
        LedgerEvent("PLAN_ISSUED", plan.plan_id, {"plan": asdict(plan)}, occurred_at=created_at)
    )
    await store.save_liquidity_profile(
        LiquidityProfile(
            symbol="SPY", adv20=10_000_000, close=98.0,
            as_of=created_at - timedelta(days=1), refreshed_at=created_at,
        )
    )
    detected_at = datetime(2026, 8, 28, 14, 15, tzinfo=UTC)
    await service.seed_wallet(100_000, 100_000, source="SHADOW_VIRTUAL", now=detected_at)
    await service.backfill_intraday_structures(["SPY"], now=detected_at)
    structure = await service.get_intraday_structure("SPY", now=detected_at)
    assert structure is not None and structure.state == "RETEST_VALID"

    decision = await service.activate_shadow_retest(
        plan.plan_id, structure=structure, detected_at=detected_at
    )

    assert decision.state != "BUY_NOW"
    assert "NO_CHASE_ENTRY_ZONE_EXCEEDED" in decision.reasons


@pytest.mark.asyncio
async def test_shadow_retest_must_follow_plan_trigger(tmp_path):
    calendar_path = tmp_path / "calendar.csv"
    calendar_path.write_text(
        "date,status,open_time,close_time,source\n"
        "2026-09-07,CLOSED,,,NYSE\n"
        "2027-01-01,CLOSED,,,NYSE\n"
    )
    cfg = Settings(market_calendar_path=calendar_path, run_mode="shadow")
    store = InMemoryEventStore()
    bars = [
        _plan_watch_bar(0, high=99.6, low=99.0, close=99.4),
        _plan_watch_bar(1, high=99.7, low=99.2, close=99.5),
        _plan_watch_bar(2, high=99.8, low=99.3, close=99.6),
        _plan_watch_bar(3, high=99.9, low=99.4, close=99.7),
        _plan_watch_bar(4, high=100.0, low=99.5, close=99.8),
        _plan_watch_bar(36, high=100.4, low=100.1, close=100.2),
        _plan_watch_bar(37, high=100.25, low=99.95, close=100.2),
    ]
    provider = PlanWatchProvider(bars, current_last=100.2)
    service = DecisionService(store, cfg=cfg, market_data=provider)
    created_at = datetime(2026, 8, 28, 14, 10, tzinfo=UTC)
    plan = TradePlan(
        symbol="SPY", lane=StrategyLane.CORE_MOMENTUM,
        entry_trigger=100.0, entry_zone_high=100.3, stop=99.0,
        tp1=101.5, tp2=102.0, max_spread_pct=0.25, max_slippage_pct=0.30,
        created_at=created_at, expires_at=created_at + timedelta(minutes=30),
        triggered_at=created_at + timedelta(minutes=1),
        quality_risk_multiplier=0.5, plan_id="retest-before-plan",
    )
    await store.save_plan(plan)
    detected_at = datetime(2026, 8, 28, 14, 15, tzinfo=UTC)
    await service.backfill_intraday_structures(["SPY"], now=detected_at)
    structure = await service.get_intraday_structure("SPY", now=detected_at)
    assert structure is not None and structure.state == "RETEST_VALID"

    decision = await service.activate_shadow_retest(
        plan.plan_id, structure=structure, detected_at=detected_at
    )

    assert decision.state == "INVALID"
    assert decision.reasons == ["RETEST_PRECEDES_PLAN_TRIGGER"]
    assert await store.get_shadow_trade(plan.plan_id) is None


@pytest.mark.asyncio
async def test_batch_activation_rejection_is_transition_only_and_labels_extension(tmp_path):
    runtime, _scheduler = _runtime(tmp_path)
    now = datetime(2026, 8, 28, 14, 0, tzinfo=UTC)

    assert await runtime._record_activation_rejected(
        "extended-plan",
        ["NO_CHASE_ENTRY_ZONE_EXCEEDED"],
        now,
    )
    assert not await runtime._record_activation_rejected(
        "extended-plan",
        ["NO_CHASE_ENTRY_ZONE_EXCEEDED"],
        now + timedelta(minutes=10),
    )

    rejected = [
        event
        for event in runtime.store.events
        if event.event_type == "ACTIVATION_REJECTED"
    ]
    assert len(rejected) == 1
    assert rejected[0].payload["reasons"] == [
        "EXTENDED",
        "NO_CHASE_ENTRY_ZONE_EXCEEDED",
    ]


@pytest.mark.asyncio
async def test_github_issue_sink_reuses_daily_issue_and_dispatcher_tags():
    requests: list[tuple[str, str, dict | None]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        requests.append((request.method, request.url.path, body))
        if request.method == "GET" and request.url.path.endswith("/labels/shadow"):
            return httpx.Response(404, json={})
        if request.method == "POST" and request.url.path.endswith("/labels"):
            return httpx.Response(201, json={"name": "shadow"})
        if request.method == "GET" and request.url.path.endswith("/issues"):
            return httpx.Response(200, json=[])
        if request.method == "POST" and request.url.path.endswith("/issues"):
            return httpx.Response(201, json={"number": 17})
        return httpx.Response(201, json={"id": 1})

    now = datetime(2026, 8, 28, 15, tzinfo=UTC)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        sink = GitHubIssueSink(
            "test-token",
            "githuber20202/market-brain",
            client,
            clock=lambda: now,
        )
        store = InMemoryEventStore()
        for text in ("BUY SPY", "SELL SPY"):
            await store.save_alert(AlertRecord(kind="BUY_NOW", payload={"text": text}))
        dispatcher = AlertDispatcher(
            store,
            [sink],
            run_mode="shadow",
            data_plan="keyless_delayed",
            redact_values=("test-token",),
        )

        assert await dispatcher.dispatch_once(now=now) == 2
        await store.save_alert(
            AlertRecord(
                kind="DAILY_DIGEST",
                payload={
                    "run_id": "daily_digest:2026-08-28",
                    "session_date": "2026-08-28",
                    "text": "daily digest",
                },
            )
        )
        assert await dispatcher.dispatch_once(now=now) == 1

    comments = [body for method, path, body in requests if path.endswith("/comments")]
    assert len(comments) == 3
    assert comments[0]["body"].startswith("@githuber20202\n\n[SHADOW][DELAYED]")
    assert sum(path.endswith("/issues") and method == "POST" for method, path, _ in requests) == 1
    assert any(
        method == "PATCH" and path.endswith("/issues/17") and body == {"state": "closed"}
        for method, path, body in requests
    )
    issue_body = next(
        body["body"]
        for method, path, body in requests
        if method == "POST" and path.endswith("/issues")
    )
    assert "Measurement only" in issue_body
    assert "test-token" not in json.dumps(requests)


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    ).stdout.strip()


def test_state_branch_force_push_is_parentless_and_restore_reads_dump(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    remote = tmp_path / "remote.git"
    repo.mkdir()
    _git(repo, "init")
    _git(repo, "config", "user.name", "Test")
    _git(repo, "config", "user.email", "test@example.invalid")
    _git(tmp_path, "init", "--bare", str(remote))
    _git(repo, "remote", "add", "origin", str(remote))
    (repo / "state" / "snapshots").mkdir(parents=True)
    (repo / "reports").mkdir()
    (repo / "state" / "market.dump").write_bytes(b"fixture-dump")
    (repo / "state" / "latest.json").write_text("{}\n")
    (repo / "reports" / "digest_2026-08-28.md").write_text("digest\n")

    first = publish_state_branch(repo, remote="origin")
    (repo / "state" / "latest.json").write_text('{"updated": true}\n')
    second = publish_state_branch(repo, remote="origin")

    assert first != second
    assert _git(repo, "rev-list", "--count", "shadow-state") == "1"
    assert _git(repo, "rev-list", "--parents", "-n", "1", "shadow-state") == second
    assert (
        _git(tmp_path, f"--git-dir={remote}", "show", "shadow-state:state/market.dump")
        == "fixture-dump"
    )

    clone = tmp_path / "clone"
    _git(tmp_path, "clone", "--branch", "shadow-state", str(remote), str(clone))
    calls: list[list[str]] = []
    original_run = state_module._run

    def fake_run(args, **kwargs):
        if args[0] == "pg_restore":
            calls.append(args)
            return subprocess.CompletedProcess(args, 0)
        return original_run(args, **kwargs)

    monkeypatch.setattr("market_brain.runtime.state._run", fake_run)
    assert restore_state(clone, "postgresql://fixture", ref="HEAD") is True
    assert (clone / "state" / "market.dump").read_bytes() == b"fixture-dump"
    assert calls[0][0] == "pg_restore"


def test_batch_gate_selects_only_real_et_schedule(tmp_path):
    calendar = tmp_path / "calendar.csv"
    calendar.write_text(
        "date,status,open_time,close_time,source\n"
        "2026-09-07,CLOSED,,,NYSE\n"
    )
    assert should_run(
        "radar", datetime(2026, 8, 28, 13, 50, tzinfo=UTC), calendar
    )
    assert should_run(
        "radar", datetime(2026, 8, 28, 13, 58, tzinfo=UTC), calendar
    )
    assert not should_run(
        "radar", datetime(2026, 8, 28, 13, 20, tzinfo=UTC), calendar
    )
    assert should_run(
        "radar", datetime(2026, 8, 28, 19, 20, tzinfo=UTC), calendar
    )
    assert not should_run(
        "radar", datetime(2026, 8, 28, 19, 21, tzinfo=UTC), calendar
    )
    assert should_run(
        "digest", datetime(2026, 8, 28, 20, 20, tzinfo=UTC), calendar
    )
    assert should_run(
        "digest", datetime(2026, 8, 28, 20, 41, tzinfo=UTC), calendar
    )
    assert not should_run(
        "digest", datetime(2026, 8, 29, 3, 59, tzinfo=UTC), calendar
    )
    assert not should_run(
        "radar", datetime(2026, 9, 7, 13, 50, tzinfo=UTC), calendar
    )
    assert should_run(
        "radar",
        datetime(2026, 8, 29, 13, 50, tzinfo=UTC),
        calendar,
        force=True,
    )


def test_shadow_session_owns_daily_schedule_and_legacy_jobs_are_manual_only():
    root = Path(__file__).resolve().parents[1]
    premarket = (root / ".github/workflows/premarket-prediction.yml").read_text()
    radar = (root / ".github/workflows/shadow-radar.yml").read_text()
    digest = (root / ".github/workflows/shadow-digest.yml").read_text()
    session = (root / ".github/workflows/shadow-session.yml").read_text()
    watchdog = (root / ".github/workflows/shadow-watchdog.yml").read_text()

    assert "schedule:" not in premarket
    assert "schedule:" not in radar
    assert "schedule:" not in digest
    assert "workflow_dispatch:" in premarket
    assert "workflow_dispatch:" in radar
    assert "workflow_dispatch:" in digest
    assert "--mode premarket" in premarket
    assert 'choices=("premarket", "radar", "digest")' in (
        root / "scripts/batch_gate.py"
    ).read_text()
    for workflow in (premarket, radar, digest):
        assert "group: market-brain-shadow-state" in workflow
        assert "contents: write" in workflow
        assert "issues: write" in workflow
        assert "secrets." not in workflow
        assert "BATCH_DURATION_SECONDS" in workflow
        assert "force:" in workflow
        assert "FORCE_GATE" in workflow
    assert 'args+=(--force --checkpoint "$FORCED_CHECKPOINT")' in premarket
    for workflow in (radar, digest):
        assert "args+=(--force)" in workflow

    assert 'cron: "0 6,7,8,9,10,11,12 * * 1-5"' in session
    assert 'cron: "30 6,7 * * 1-5"' in session
    assert "branches: [session-trigger]" in session
    assert 'paths:\n      - "triggers/*.json"' in session
    assert "group: market-brain-shadow-state" in session
    assert "cancel-in-progress: false" in session
    assert "contents: write" in session
    assert "issues: write" in session
    assert "actions: read" in session
    assert "--phase wait" in session
    assert "--phase a" in session
    assert "--phase b" in session
    assert "timeout-minutes: 345" in session
    assert "secrets." not in session

    assert 'cron: "5,35 12-20 * * 1-5"' in watchdog
    assert "actions: write" in watchdog
    assert "python scripts/session_watchdog.py" in watchdog

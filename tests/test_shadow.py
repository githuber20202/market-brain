from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from uuid import uuid4

import pytest

from market_brain.domain.models import IntradayBarRecord, ShadowTrade, ShadowTradeStatus
from market_brain.ledger.events import LedgerEvent
from market_brain.ledger.replay import rebuild_state, replay_check
from market_brain.ledger.store import InMemoryEventStore
from market_brain.runtime.shadow import ShadowEvaluator, shadow_metrics
from market_brain.settings import Settings


def _trade(*, opened_at: datetime, suffix: str = "") -> ShadowTrade:
    return ShadowTrade(
        trade_id=str(uuid4()),
        plan_id=str(uuid4()),
        symbol=f"TEST{suffix}",
        setup="CORE_MOMENTUM",
        quantity=10,
        trigger=99.9,
        fill=100.0,
        stop=99.0,
        tp1=101.0,
        tp2=102.0,
        opened_at=opened_at,
        time_stop_at=opened_at + timedelta(minutes=5),
    )


def _bar(trade: ShadowTrade, stamp: datetime, o: float, h: float, low: float, c: float):
    return IntradayBarRecord(
        symbol=trade.symbol,
        session_date="2026-08-28",
        minute_ts=stamp,
        source="SIP",
        open=o,
        high=h,
        low=low,
        close=c,
    )


async def _evaluate(trade: ShadowTrade, bars: list[IntradayBarRecord], *, finalize=False):
    store = InMemoryEventStore()
    await store.save_shadow_trade(trade)
    await store.append(
        LedgerEvent(
            "SHADOW_TRADE_OPENED",
            trade.trade_id,
            {"shadow_trade": asdict(trade)},
            occurred_at=trade.opened_at,
        )
    )
    for bar in bars:
        await store.save_intraday_bar(bar)
    await ShadowEvaluator(store).evaluate_session(date(2026, 8, 28), finalize=finalize)
    return store, await store.get_shadow_trade(trade.plan_id)


@pytest.mark.asyncio
async def test_shadow_stop_first_on_ambiguous_bar():
    opened = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
    trade = _trade(opened_at=opened)
    store, saved = await _evaluate(trade, [_bar(trade, opened, 100, 102, 98, 100)])

    assert saved is not None and saved.status == ShadowTradeStatus.STOPPED
    assert saved.realized_r == -1.0
    assert [row["reason"] for row in saved.exit_legs] == ["STOP"]
    assert [row.event_type for row in store.events][-1] == "SHADOW_TRADE_TRANSITIONED"


@pytest.mark.asyncio
async def test_shadow_tp1_then_tp2_uses_shared_replay_engine():
    opened = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
    trade = _trade(opened_at=opened)
    store, saved = await _evaluate(
        trade,
        [
            _bar(trade, opened, 100, 101, 100, 101),
            _bar(trade, opened + timedelta(minutes=1), 101, 102, 101, 102),
        ],
    )

    assert saved is not None and saved.status == ShadowTradeStatus.TP2
    assert [row["reason"] for row in saved.exit_legs] == ["TP1", "TP2"]
    assert saved.realized_r == 1.5
    transitions = [row for row in store.events if row.event_type == "SHADOW_TRADE_TRANSITIONED"]
    assert [row.payload["to"] for row in transitions] == ["TP1", "TP2"]


@pytest.mark.asyncio
async def test_shadow_time_stop_and_event_replay_match_materialized_state():
    opened = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
    trade = _trade(opened_at=opened)
    bar = _bar(trade, opened + timedelta(minutes=5), 99.5, 99.6, 99.4, 99.5)
    store, saved = await _evaluate(trade, [bar])

    assert saved is not None and saved.status == ShadowTradeStatus.TIME_STOP
    assert saved.realized_r == -0.5
    assert rebuild_state(store.events)["shadow_trades"][trade.plan_id] == saved
    assert await replay_check(store) == []


@pytest.mark.asyncio
async def test_shadow_end_of_day_finalizes_from_last_stored_sip_bar_without_new_data():
    opened = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
    trade = _trade(opened_at=opened)
    trade.time_stop_at = opened + timedelta(hours=8)
    bar = _bar(trade, opened, 100.2, 100.4, 100.1, 100.3)
    store, saved = await _evaluate(trade, [bar])
    assert saved is not None and saved.status == ShadowTradeStatus.OPEN

    await ShadowEvaluator(store).evaluate_session(date(2026, 8, 28), finalize=True)
    finalized = await store.get_shadow_trade(trade.plan_id)

    assert finalized is not None and finalized.status == ShadowTradeStatus.TIME_STOP
    assert finalized.exit_legs[-1]["price"] == 100.3


def test_shadow_metrics_include_no_trigger_and_setup_breakdown():
    opened = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
    win = _trade(opened_at=opened, suffix="W")
    win.status = ShadowTradeStatus.TP2
    win.realized_r = 1.5
    win.closed_at = opened + timedelta(minutes=10)
    loss = _trade(opened_at=opened + timedelta(minutes=1), suffix="L")
    loss.status = ShadowTradeStatus.STOPPED
    loss.realized_r = -1.0
    loss.closed_at = opened + timedelta(minutes=2)
    events = [
        LedgerEvent("BUY_NOW_EMITTED", win.plan_id, {}, occurred_at=opened),
        LedgerEvent("BUY_NOW_EMITTED", loss.plan_id, {}, occurred_at=opened),
        LedgerEvent("PLAN_EXPIRED", "never-triggered", {}, occurred_at=opened),
    ]

    result = shadow_metrics([win, loss], events, session_date=date(2026, 8, 28))

    assert result["signals"] == 2
    assert result["trades"] == 2
    assert result["no_trigger"] == 1
    assert result["hit_rate"] == 0.5
    assert result["expectancy_r"] == 0.25
    assert result["by_setup"]["CORE_MOMENTUM"]["trade_count"] == 2


@pytest.mark.asyncio
async def test_shadow_evaluator_fake_clock_runs_only_on_five_minute_market_slots(tmp_path):
    calendar = tmp_path / "calendar.csv"
    calendar.write_text(
        "date,status,open_time,close_time,source\n"
        "2026-09-07,CLOSED,,,NYSE\n"
        "2027-01-01,CLOSED,,,NYSE\n"
    )
    cfg = Settings(market_calendar_path=calendar)
    store = InMemoryEventStore()
    opened = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
    trade = _trade(opened_at=opened)
    await store.save_shadow_trade(trade)
    await store.append(
        LedgerEvent(
            "SHADOW_TRADE_OPENED",
            trade.trade_id,
            {"shadow_trade": asdict(trade)},
            occurred_at=opened,
        )
    )
    await store.save_intraday_bar(_bar(trade, opened, 100, 100.5, 99.5, 100.2))
    evaluator = ShadowEvaluator(store, cfg=cfg)
    slot = datetime(2026, 8, 28, 14, 0, tzinfo=UTC)
    evaluator.validate_startup(now=slot)

    assert await evaluator.run_pending(now=slot + timedelta(minutes=1)) == 0
    assert await evaluator.run_pending(now=slot) == 1
    assert await evaluator.run_pending(now=slot) == 0


def _calendar_path(tmp_path):
    calendar = tmp_path / "calendar.csv"
    calendar.write_text(
        "date,status,open_time,close_time,source\n"
        "2026-09-07,CLOSED,,,NYSE\n"
        "2027-01-01,CLOSED,,,NYSE\n"
    )
    return calendar


async def _store_open_trade(store, trade, bar=None):
    await store.save_shadow_trade(trade)
    await store.append(
        LedgerEvent(
            "SHADOW_TRADE_OPENED",
            trade.trade_id,
            {"shadow_trade": asdict(trade)},
            occurred_at=trade.opened_at,
        )
    )
    if bar is not None:
        await store.save_intraday_bar(bar)


@pytest.mark.asyncio
async def test_shadow_missed_end_slot_finalizes_at_1627(tmp_path):
    store = InMemoryEventStore()
    opened = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
    trade = _trade(opened_at=opened)
    trade.time_stop_at = opened + timedelta(hours=8)
    closing_bar = _bar(
        trade,
        datetime(2026, 8, 28, 19, 59, tzinfo=UTC),
        100.1,
        100.4,
        100.0,
        100.3,
    )
    await _store_open_trade(store, trade, closing_bar)
    evaluator = ShadowEvaluator(
        store,
        cfg=Settings(market_calendar_path=_calendar_path(tmp_path)),
    )
    now = datetime(2026, 8, 28, 20, 27, tzinfo=UTC)
    evaluator.validate_startup(now=now)

    assert await evaluator.run_pending(now=now) == 1
    saved = await store.get_shadow_trade(trade.plan_id)
    assert saved is not None and saved.status == ShadowTradeStatus.TIME_STOP
    assert saved.exit_legs[-1]["price"] == 100.3
    assert await store.get_runtime_status_key("shadow_finalize:2026-08-28") == {
        "status": "COMPLETED",
        "evaluated": 1,
        "at": "2026-08-28T16:27:00-04:00",
    }


@pytest.mark.asyncio
async def test_shadow_first_slot_catches_up_previous_session(tmp_path):
    store = InMemoryEventStore()
    opened = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
    trade = _trade(opened_at=opened)
    trade.time_stop_at = opened + timedelta(hours=8)
    closing_bar = _bar(
        trade,
        datetime(2026, 8, 28, 19, 59, tzinfo=UTC),
        100.1,
        100.4,
        100.0,
        100.25,
    )
    await _store_open_trade(store, trade, closing_bar)
    evaluator = ShadowEvaluator(
        store,
        cfg=Settings(market_calendar_path=_calendar_path(tmp_path)),
    )
    first_slot = datetime(2026, 8, 31, 13, 30, tzinfo=UTC)
    evaluator.validate_startup(now=first_slot)

    assert await evaluator.run_pending(now=first_slot) == 1
    saved = await store.get_shadow_trade(trade.plan_id)
    assert saved is not None and saved.status == ShadowTradeStatus.TIME_STOP
    assert saved.exit_legs[-1]["price"] == 100.25


@pytest.mark.asyncio
async def test_shadow_finalize_is_idempotent_after_missed_slot(tmp_path):
    store = InMemoryEventStore()
    opened = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
    trade = _trade(opened_at=opened)
    trade.time_stop_at = opened + timedelta(hours=8)
    closing_bar = _bar(
        trade,
        datetime(2026, 8, 28, 19, 59, tzinfo=UTC),
        100.0,
        100.2,
        99.8,
        100.1,
    )
    await _store_open_trade(store, trade, closing_bar)
    evaluator = ShadowEvaluator(
        store,
        cfg=Settings(market_calendar_path=_calendar_path(tmp_path)),
    )
    now = datetime(2026, 8, 28, 20, 27, tzinfo=UTC)
    evaluator.validate_startup(now=now)

    await evaluator.run_pending(now=now)
    transition_count = sum(
        event.event_type == "SHADOW_TRADE_TRANSITIONED" for event in store.events
    )
    assert await evaluator.run_pending(now=now + timedelta(minutes=1)) == 0
    assert sum(
        event.event_type == "SHADOW_TRADE_TRANSITIONED" for event in store.events
    ) == transition_count


@pytest.mark.asyncio
async def test_shadow_missing_bars_backfills_once_then_marks_pending(tmp_path):
    store = InMemoryEventStore()
    opened = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
    trade = _trade(opened_at=opened)
    await _store_open_trade(store, trade)
    calls = []

    async def backfill(symbols, *, now):
        calls.append((symbols, now))

    evaluator = ShadowEvaluator(
        store,
        cfg=Settings(market_calendar_path=_calendar_path(tmp_path)),
        backfill=backfill,
    )
    now = datetime(2026, 8, 28, 20, 27, tzinfo=UTC)
    evaluator.validate_startup(now=now)

    assert await evaluator.run_pending(now=now) == 0
    assert await evaluator.run_pending(now=now + timedelta(minutes=1)) == 0
    assert calls == [([trade.symbol], datetime(2026, 8, 28, 20, 20, tzinfo=UTC))]
    pending = await store.get_runtime_status_key(
        f"shadow_finalize_pending:{trade.plan_id}"
    )
    assert pending["status"] == "PENDING"
    assert pending["reason"] == "SIP_BARS_UNAVAILABLE"
    saved = await store.get_shadow_trade(trade.plan_id)
    assert saved is not None and saved.status == ShadowTradeStatus.OPEN

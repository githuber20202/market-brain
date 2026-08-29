from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from market_brain.domain.models import ShadowTrade, ShadowTradeStatus
from market_brain.ledger.events import LedgerEvent
from market_brain.ledger.store import InMemoryEventStore
from market_brain.orchestration.universe import NyseMarketCalendar
from market_brain.replay.engine import ReplayEngine
from scripts.replay_report import create_replay_report, last_trading_days
from scripts.shadow_report import create_shadow_report

FIXTURE = Path(__file__).parent / "fixtures" / "replay_bars.json"


class NetworkForbidden:
    async def bars_batch(self, *_args, **_kwargs):
        raise AssertionError("fixture report must not use the network")


@pytest.mark.asyncio
async def test_replay_report_fixture_writes_markdown_without_network(tmp_path):
    fixture = json.loads(FIXTURE.read_text())
    calendar = NyseMarketCalendar({}, {2026})
    path = await create_replay_report(
        days=1,
        symbols=["WIN", "LOSS"],
        calendar=calendar,
        engine=ReplayEngine(NetworkForbidden()),
        output_dir=tmp_path,
        now=datetime(2026, 8, 29, 12, tzinfo=UTC),
        fixture_bars={fixture["date"]: fixture["symbols"]},
    )

    text = path.read_text()
    assert path.name == "replay_2026-08-28_2026-08-28.md"
    assert "- Trades: 2" in text
    assert "- Hit rate: 50.00%" in text
    assert "| WIN | 1 | 100.00%" in text
    assert "| LOSS | 1 | 0.00%" in text


def test_last_trading_days_skips_weekend_and_holiday():
    calendar = NyseMarketCalendar(
        {date(2026, 9, 7): ("CLOSED", None)},
        {2026},
    )

    assert last_trading_days(calendar, days=2, before=date(2026, 9, 8)) == [
        date(2026, 9, 3),
        date(2026, 9, 4),
    ]


def _shadow_trade(opened: datetime, status: ShadowTradeStatus, realized_r: float):
    closed = opened + timedelta(minutes=10)
    return ShadowTrade(
        trade_id=str(uuid4()),
        plan_id=str(uuid4()),
        symbol="TEST",
        setup="CORE_MOMENTUM",
        quantity=5,
        trigger=100.0,
        fill=100.1,
        stop=99.0,
        tp1=102.0,
        tp2=103.0,
        opened_at=opened,
        time_stop_at=opened + timedelta(minutes=30),
        status=status,
        remaining_fraction=0.0,
        realized_r=realized_r,
        closed_at=closed,
    )


@pytest.mark.asyncio
async def test_shadow_report_writes_weekly_markdown_from_store(tmp_path):
    store = InMemoryEventStore()
    opened = datetime(2026, 8, 24, 14, tzinfo=UTC)
    trades = [
        _shadow_trade(opened, ShadowTradeStatus.TP2, 1.5),
        _shadow_trade(opened + timedelta(days=1), ShadowTradeStatus.STOPPED, -1.0),
    ]
    for trade in trades:
        await store.save_shadow_trade(trade)
        await store.append(
            LedgerEvent(
                "SHADOW_TRADE_OPENED",
                trade.trade_id,
                {"shadow_trade": asdict(trade)},
                occurred_at=trade.opened_at,
            )
        )
        await store.append(
            LedgerEvent("BUY_NOW_EMITTED", trade.plan_id, {}, occurred_at=trade.opened_at)
        )
    await store.append(
        LedgerEvent("PLAN_EXPIRED", "no-trigger", {}, occurred_at=opened)
    )

    path = await create_shadow_report(
        store,
        week_start=date(2026, 8, 24),
        output_dir=tmp_path,
    )

    text = path.read_text()
    assert path.name == "shadow_2026-W35.md"
    assert "- Signals: 2" in text
    assert "- Virtual trades: 2" in text
    assert "- No trigger: 1" in text
    assert "- Hit rate: 50.00%" in text
    assert "- Expectancy: 0.250 R" in text

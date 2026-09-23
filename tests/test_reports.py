from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from market_brain.orchestration.universe import NyseMarketCalendar
from market_brain.replay.engine import ReplayEngine
from scripts.replay_report import create_replay_report, last_trading_days

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
        fixture_scoring_context=fixture["scoring_context"],
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

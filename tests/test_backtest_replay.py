from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_brain.replay.engine import ReplayEngine, replay_summary, synthesize_bar_ticks

FIXTURE = Path(__file__).parent / "fixtures" / "replay_bars.json"


def _fixture():
    return json.loads(FIXTURE.read_text())


def test_synthesized_tick_order_follows_candle_direction():
    bullish = {"t": "2026-08-28T13:30:00+00:00", "o": 10, "h": 12, "l": 9, "c": 11}
    bearish = {"t": "2026-08-28T13:30:00+00:00", "o": 11, "h": 12, "l": 9, "c": 10}

    assert [tick.kind for tick in synthesize_bar_ticks(bullish)] == ["open", "low", "high", "close"]
    assert [tick.kind for tick in synthesize_bar_ticks(bearish)] == ["open", "high", "low", "close"]


@pytest.mark.asyncio
async def test_fixture_replay_is_deterministic_and_writes_report(tmp_path: Path):
    fixture = _fixture()
    engine = ReplayEngine(output_dir=tmp_path)

    first = await engine.run(fixture["date"], ["WIN", "LOSS"], bars_by_symbol=fixture["symbols"])
    second = await engine.run(fixture["date"], ["LOSS", "WIN"], bars_by_symbol=fixture["symbols"])

    assert first == second
    assert json.loads((tmp_path / "replay_2026-08-28.json").read_text()) == first
    assert first["trade_count"] == 2
    assert first["wins"] == 1
    assert first["hit_rate"] == 0.5


@pytest.mark.asyncio
async def test_bar_touching_stop_and_target_counts_as_stop():
    fixture = _fixture()
    report = await ReplayEngine().run(
        fixture["date"], ["LOSS"], bars_by_symbol=fixture["symbols"], write_report=False
    )

    trade = report["trades"][0]
    assert trade["exit_legs"] == [
        {
            "reason": "STOP",
            "price": trade["stop"],
            "fraction": 1.0,
            "at": "2026-08-28T13:38:00+00:00",
        }
    ]
    assert trade["r"] == -1.0


@pytest.mark.asyncio
async def test_trigger_bar_conflict_is_also_stop_first():
    fixture = _fixture()
    bars = [dict(row) for row in fixture["symbols"]["LOSS"][:8]]
    bars[-1].update({"o": 100.6, "h": 100.95, "l": 100.4, "c": 100.5})

    report = await ReplayEngine().run(
        fixture["date"], ["LOSS"], bars_by_symbol={"LOSS": bars}, write_report=False
    )

    assert report["trades"][0]["exit_legs"][0]["reason"] == "STOP"
    assert report["trades"][0]["r"] == -1.0


@pytest.mark.asyncio
async def test_tp1_and_tp2_are_half_and_half():
    fixture = _fixture()
    report = await ReplayEngine().run(
        fixture["date"], ["WIN"], bars_by_symbol=fixture["symbols"], write_report=False
    )

    trade = report["trades"][0]
    assert [leg["reason"] for leg in trade["exit_legs"]] == ["TP1", "TP2"]
    assert [leg["fraction"] for leg in trade["exit_legs"]] == [0.5, 0.5]
    assert trade["protection"] == "PROTECTED"
    assert trade["fill"] == pytest.approx(trade["trigger"] * 1.001, abs=0.0001)


@pytest.mark.asyncio
async def test_time_stop_uses_current_tick_price():
    fixture = _fixture()
    bars = [dict(row) for row in fixture["symbols"]["WIN"][:8]]
    start = datetime.fromisoformat(bars[-1]["t"]) + timedelta(minutes=1)
    for minute in range(31):
        stamp = start + timedelta(minutes=minute)
        bars.append(
            {
                "t": stamp.isoformat(),
                "o": 100.65,
                "h": 100.7,
                "l": 100.6,
                "c": 100.65,
                "v": 10000,
                "vw": 100.6,
            }
        )

    report = await ReplayEngine().run(
        fixture["date"], ["FLAT"], bars_by_symbol={"FLAT": bars}, write_report=False
    )

    trade = report["trades"][0]
    assert trade["exit_legs"][-1]["reason"] == "TIME_STOP"
    assert trade["exit_legs"][-1]["price"] == 100.65


class FakeSipProvider:
    def __init__(self, bars):
        self.bars = bars
        self.calls = []

    async def bars_batch(self, symbols, timeframe, start, end):
        self.calls.append((symbols, timeframe, start, end))
        return self.bars


@pytest.mark.asyncio
async def test_prior_session_fetches_one_minute_sip_pipeline_without_network():
    fixture = _fixture()
    provider = FakeSipProvider(fixture["symbols"])
    now = lambda: datetime(2026, 8, 29, 12, tzinfo=UTC)
    report = await ReplayEngine(provider, now=now).run(
        fixture["date"], ["WIN"], write_report=False
    )

    assert report["bar_feed"] == "SIP"
    assert provider.calls[0][0:2] == (["WIN"], "1Min")


@pytest.mark.asyncio
async def test_current_session_fails_closed_before_provider_call():
    fixture = _fixture()
    provider = FakeSipProvider(fixture["symbols"])
    now = lambda: datetime(2026, 8, 29, 12, tzinfo=UTC)

    with pytest.raises(ValueError, match="REPLAY_REQUIRES_PRIOR_SESSION"):
        await ReplayEngine(provider, now=now).run("2026-08-29", ["WIN"], write_report=False)
    assert provider.calls == []


def test_summary_reports_expectancy_and_peak_to_trough_drawdown():
    summary = replay_summary([{"r": 1.0}, {"r": -0.5}, {"r": -1.0}, {"r": 2.0}])

    assert summary == {
        "trade_count": 4,
        "wins": 2,
        "hit_rate": 0.5,
        "expectancy_r": 0.375,
        "max_drawdown_r": 1.5,
    }


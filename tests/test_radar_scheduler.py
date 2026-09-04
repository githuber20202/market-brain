from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from market_brain.domain.models import StrategyLane, TradePlan
from market_brain.engines.plan import PlanBuildError
from market_brain.ledger.store import InMemoryEventStore
from market_brain.orchestration.screener import ScreenResult
from market_brain.orchestration.universe import load_universe
from market_brain.providers.base import DataUnavailable, SkippedSymbol
from market_brain.runtime.daily_digest import DailyDigest
from market_brain.runtime.radar_scheduler import RadarScheduler
from market_brain.runtime.stream_worker import select_subscription_symbols

EASTERN = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parents[1]


class FakeScreener:
    def __init__(self, rows: list[dict]):
        self.rows = rows
        self.calls: list[tuple[list[str], int]] = []

    async def screen(self, symbols: list[str], top_n: int, **_kwargs):
        self.calls.append((symbols, top_n))
        return self.rows[:top_n]


class RecoveringScreener(FakeScreener):
    def __init__(self, rows: list[dict]):
        super().__init__(rows)
        self.unavailable = True

    async def screen(self, symbols: list[str], top_n: int, **_kwargs):
        self.calls.append((symbols, top_n))
        if self.unavailable:
            self.unavailable = False
            raise DataUnavailable(
                source_id="YAHOO_DELAYED",
                resource="chart:1m",
                symbol=symbols[0],
                error_type="HTTP_429",
            )
        return self.rows[:top_n]


class FakeService:
    def __init__(self):
        self.store = InMemoryEventStore()
        self.plan_calls: list[dict] = []

    async def refresh_liquidity_profiles_for_symbols(self, symbols, *, now):
        return {
            "session_date": now.astimezone(EASTERN).date().isoformat(),
            "refreshed": len(symbols),
            "failed": [],
        }

    async def build_plan_from_market(self, **kwargs):
        self.plan_calls.append(kwargs)
        now = kwargs["now"]
        plan = TradePlan(
            symbol=kwargs["symbol"],
            lane=kwargs["lane"],
            entry_trigger=100.0,
            entry_zone_high=100.1,
            stop=99.0,
            tp1=101.5,
            tp2=102.0,
            max_spread_pct=0.25,
            max_slippage_pct=0.30,
            created_at=now,
            expires_at=now + timedelta(minutes=5),
            quality_risk_multiplier=0.5,
        )
        await self.store.save_plan(plan)
        return plan, {"score": {"total": 75.0}}


class KeylessPreflightService(FakeService):
    def __init__(self):
        super().__init__()
        self.cfg = SimpleNamespace(data_plan="keyless_delayed")
        self.prepared: list[str] = []

    async def prepare_plan_market_data(self, symbol: str, *, now):
        del now
        self.prepared.append(symbol)
        if symbol == "MSFT":
            raise DataUnavailable(
                source_id="YAHOO_DELAYED",
                resource="chart:1m",
                symbol=symbol,
                error_type="HTTP_503",
            )


class KeylessFailureRatioService(FakeService):
    def __init__(self):
        super().__init__()
        self.cfg = SimpleNamespace(
            data_plan="keyless_delayed",
            keyless_max_failure_ratio=0.2,
        )


class RejectingService(FakeService):
    async def build_plan_from_market(self, **kwargs):
        self.plan_calls.append(kwargs)
        raise PlanBuildError("RISK_TOO_SMALL")


class PartialScreener(FakeScreener):
    def __init__(self, rows: list[dict], skipped: tuple[SkippedSymbol, ...]):
        super().__init__(rows)
        self.skipped = skipped

    async def screen(self, symbols: list[str], top_n: int, **_kwargs):
        self.calls.append((symbols, top_n))
        return ScreenResult(tuple(self.rows[:top_n]), self.skipped)


def _row(symbol: str, score: float = 90.0, *, catalyst: bool = False) -> dict:
    return {
        "snapshot": {
            "symbol": symbol,
            "catalyst_verified": catalyst,
            "catalyst_strength": 0.9 if catalyst else 0.0,
        },
        "score": {
            "catalyst_or_continuation": 0.0,
            "price_momentum": 20.0,
            "volume_liquidity": 15.0,
            "relative_strength_sector": 10.0,
            "entry_invalidation_structure": 15.0,
            "risk_reward": 10.0,
            "total": score,
            "discovery_total": score,
        },
    }


def _files(tmp_path: Path, *, symbols: tuple[str, ...], quality: tuple[str, ...] = ()):
    universe_dir = tmp_path / "universe"
    universe_dir.mkdir()
    universe_rows = "symbol,ranking_eligible\n" + "".join(
        f"{symbol},true\n" for symbol in symbols
    )
    (universe_dir / "universe.csv").write_text(universe_rows)
    quality_path = tmp_path / "quality.csv"
    quality_path.write_text(
        "symbol,quality_score,as_of\n"
        + "".join(f"{symbol},85,2026-08-01T00:00:00+00:00\n" for symbol in quality)
    )
    calendar_path = tmp_path / "market_calendar.csv"
    calendar_path.write_text(
        "date,status,open_time,close_time,source\n"
        "2026-09-07,CLOSED,,,NYSE\n"
        "2026-11-27,EARLY_CLOSE,09:30,13:00,NYSE\n"
        "2027-01-01,CLOSED,,,NYSE\n"
    )
    return universe_dir, quality_path, calendar_path


def _scheduler(tmp_path: Path, rows: list[dict], *, symbols=("AAPL",), quality=("AAPL",)):
    paths = _files(tmp_path, symbols=symbols, quality=quality)
    service = FakeService()
    screener = FakeScreener(rows)
    scheduler = RadarScheduler(
        service=service,
        screener=screener,
        universe_dir=paths[0],
        quality_path=paths[1],
        calendar_path=paths[2],
    )
    scheduler.validate_startup(now=datetime(2026, 8, 28, 9, 49, tzinfo=EASTERN))
    return scheduler, service, screener


def test_universe_loader_normalizes_and_rejects_duplicates(tmp_path: Path):
    universe_dir = tmp_path / "universe"
    universe_dir.mkdir()
    (universe_dir / "a.csv").write_text("symbol\naapl\n")
    (universe_dir / "b.csv").write_text("ticker\nAAPL\n")
    with pytest.raises(ValueError, match="UNIVERSE_DUPLICATE_SYMBOL=AAPL"):
        load_universe(universe_dir)


def test_docker_image_includes_runtime_data_and_report_scripts():
    dockerfile = (ROOT / "Dockerfile").read_text()
    assert "COPY data /app/data" in dockerfile
    assert "COPY scripts /app/scripts" in dockerfile


@pytest.mark.asyncio
async def test_fake_clock_fires_at_0950_once_and_creates_digest(tmp_path: Path):
    scheduler, service, screener = _scheduler(tmp_path, [_row("AAPL")])
    before = datetime(2026, 8, 28, 9, 49, tzinfo=EASTERN)
    slot = datetime(2026, 8, 28, 9, 50, tzinfo=EASTERN)

    assert await scheduler.run_pending(now=before) is None
    result = await scheduler.run_pending(now=slot)
    assert result is not None
    assert result["status"] == "COMPLETED"
    assert result["score_histogram"] == {
        "0-20": 0,
        "20-40": 0,
        "40-65": 0,
        "65+": 1,
    }
    assert result["candidates"][0]["score_components"]["total"] == 90.0
    assert len(result["rankings"]) == 1
    assert result["rankings"][0]["symbol"] == "AAPL"
    assert result["rankings"][0]["risk_reward"] == 10.0
    assert len(screener.calls) == 1
    assert len(service.plan_calls) == 1
    assert service.plan_calls[0]["lane"] == StrategyLane.CORE_MOMENTUM
    assert service.plan_calls[0]["quality"].evidence[0].source == "MANUAL"
    assert await scheduler.run_pending(now=slot.replace(second=30)) is None

    assert service.store.events[-1].event_type == "RADAR_RUN"
    alerts = await service.store.list_undelivered()
    assert alerts[-1].kind == "RADAR_DIGEST"
    assert alerts[-1].payload["candidates"][0]["levels"]["stop"] == 99.0


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["EDGAR_AUTO", "YAHOO_FUNDAMENTALS"])
async def test_radar_accepts_automated_quality_for_core_lane(
    tmp_path: Path,
    source: str,
):
    paths = _files(tmp_path, symbols=("AAPL",), quality=())
    paths[1].write_text(
        "symbol,quality_score,as_of,source,partial\n"
        f"AAPL,85,2026-08-28T00:00:00+00:00,{source},false\n"
    )
    service = FakeService()
    scheduler = RadarScheduler(
        service=service,
        screener=FakeScreener([_row("AAPL")]),
        universe_dir=paths[0],
        quality_path=paths[1],
        calendar_path=paths[2],
    )
    slot = datetime(2026, 8, 28, 9, 50, tzinfo=EASTERN)
    scheduler.validate_startup(now=slot)

    result = await scheduler.run_pending(now=slot)

    assert result is not None
    assert service.plan_calls[0]["lane"] == StrategyLane.CORE_MOMENTUM
    assert service.plan_calls[0]["quality"].evidence[0].source == source
    assert result["candidates"][0]["quality_source"] == source


@pytest.mark.asyncio
async def test_radar_treats_etf_company_quality_as_not_applicable(tmp_path: Path):
    paths = _files(tmp_path, symbols=("SPY",), quality=())
    (paths[0] / "universe.csv").write_text(
        "symbol,instrument_type,ranking_eligible\nSPY,ETF,true\n"
    )
    service = FakeService()
    scheduler = RadarScheduler(
        service=service,
        screener=FakeScreener([_row("SPY")]),
        universe_dir=paths[0],
        quality_path=paths[1],
        calendar_path=paths[2],
    )
    slot = datetime(2026, 8, 28, 9, 50, tzinfo=EASTERN)
    scheduler.validate_startup(now=slot)

    result = await scheduler.run_pending(now=slot)

    assert result is not None
    assert service.plan_calls[0]["lane"] == StrategyLane.CORE_MOMENTUM
    quality = service.plan_calls[0]["quality"]
    assert quality.tier == "NOT_APPLICABLE"
    assert quality.risk_multiplier == 1.0
    assert result["candidates"][0]["quality_source"] == "NOT_APPLICABLE_ETF"
    assert result["candidates"][0]["reason"] is None


@pytest.mark.asyncio
async def test_batch_run_slot_uses_real_now_and_missed_slot_is_persisted(tmp_path: Path):
    scheduler, service, _screener = _scheduler(tmp_path, [_row("AAPL")])
    slot = datetime(2026, 8, 28, 9, 50, tzinfo=EASTERN)
    actual = datetime(2026, 8, 28, 9, 58, tzinfo=EASTERN)

    result = await scheduler.run_slot(slot, now=actual)

    assert result is not None and result["status"] == "COMPLETED"
    assert service.plan_calls[0]["now"] == actual.astimezone(ZoneInfo("UTC"))

    missed_slot = datetime(2026, 8, 28, 10, 20, tzinfo=EASTERN)
    missed = await scheduler.mark_missed(
        missed_slot,
        now=datetime(2026, 8, 28, 10, 27, tzinfo=EASTERN),
    )
    assert missed["status"] == "MISSED"
    assert service.store.events[-1].event_type == "RADAR_RUN"
    assert service.store.events[-1].payload["status"] == "MISSED"


@pytest.mark.asyncio
async def test_weekend_and_holiday_make_no_provider_calls(tmp_path: Path):
    scheduler, _service, screener = _scheduler(tmp_path, [_row("AAPL")])
    weekend = datetime(2026, 8, 29, 9, 50, tzinfo=EASTERN)
    holiday = datetime(2026, 9, 7, 9, 50, tzinfo=EASTERN)

    assert await scheduler.run_pending(now=weekend) is None
    assert await scheduler.run_pending(now=holiday) is None
    assert screener.calls == []


@pytest.mark.asyncio
async def test_early_close_stops_after_1300(tmp_path: Path):
    scheduler, _service, screener = _scheduler(tmp_path, [_row("AAPL")])
    last_slot = datetime(2026, 11, 27, 12, 50, tzinfo=EASTERN)
    after_close = datetime(2026, 11, 27, 13, 20, tzinfo=EASTERN)

    assert await scheduler.run_pending(now=last_slot) is not None
    assert await scheduler.run_pending(now=after_close) is None
    assert len(screener.calls) == 1


@pytest.mark.asyncio
async def test_symbol_without_quality_gets_no_core_plan(tmp_path: Path):
    scheduler, service, _screener = _scheduler(
        tmp_path,
        [_row("MSFT")],
        symbols=("MSFT",),
        quality=(),
    )
    result = await scheduler.run_pending(
        now=datetime(2026, 8, 28, 9, 50, tzinfo=EASTERN)
    )

    assert result is not None
    assert service.plan_calls == []
    assert result["candidates"][0]["reason"] == "QUALITY_MISSING_CORE_BLOCKED"


@pytest.mark.asyncio
async def test_plan_floor_rejection_is_recorded_in_radar_run(tmp_path: Path):
    paths = _files(tmp_path, symbols=("AAPL",), quality=("AAPL",))
    service = RejectingService()
    scheduler = RadarScheduler(
        service=service,
        screener=FakeScreener([_row("AAPL")]),
        universe_dir=paths[0],
        quality_path=paths[1],
        calendar_path=paths[2],
    )
    slot = datetime(2026, 8, 28, 9, 50, tzinfo=EASTERN)
    scheduler.validate_startup(now=slot)

    result = await scheduler.run_pending(now=slot)

    assert result is not None
    assert result["candidates"][0]["reason"] == "RISK_TOO_SMALL"
    assert service.store.events[-1].event_type == "RADAR_RUN"
    assert service.store.events[-1].payload["candidates"][0]["reason"] == "RISK_TOO_SMALL"


@pytest.mark.asyncio
async def test_missing_quality_with_catalyst_uses_event_lane(tmp_path: Path):
    scheduler, service, _screener = _scheduler(
        tmp_path,
        [_row("MSFT", catalyst=True)],
        symbols=("MSFT",),
        quality=(),
    )
    await scheduler.run_pending(now=datetime(2026, 8, 28, 9, 50, tzinfo=EASTERN))

    assert service.plan_calls[0]["lane"] == StrategyLane.EVENT_MOMENTUM
    assert service.plan_calls[0]["quality"].tier == "UNRATED"


@pytest.mark.asyncio
async def test_scheduler_plans_flow_into_capped_stream_subscriptions(tmp_path: Path):
    scheduler, service, _screener = _scheduler(tmp_path, [_row("AAPL")])
    await scheduler.run_pending(now=datetime(2026, 8, 28, 9, 50, tzinfo=EASTERN))

    selection = await select_subscription_symbols(service.store, cap=30, watchlist=[])
    assert selection.selected == ["AAPL"]
    assert selection.dropped == []


@pytest.mark.asyncio
async def test_daily_digest_hook_fires_at_1615_et_once_on_market_day(tmp_path: Path):
    scheduler, service, screener = _scheduler(tmp_path, [])
    scheduler.daily_digest = DailyDigest(service.store)
    slot = datetime(2026, 8, 28, 16, 15, tzinfo=EASTERN)

    result = await scheduler.run_pending(now=slot)

    assert result is not None
    assert result["run_id"] == "daily_digest:2026-08-28"
    assert result["status"] == "COMPLETED"
    assert await scheduler.run_pending(now=slot.replace(second=30)) is None
    alerts = await service.store.list_undelivered()
    assert [alert.kind for alert in alerts] == ["DAILY_DIGEST"]
    assert screener.calls == []


@pytest.mark.asyncio
async def test_daily_digest_hook_does_not_fire_on_holiday(tmp_path: Path):
    scheduler, service, _screener = _scheduler(tmp_path, [])
    scheduler.daily_digest = DailyDigest(service.store)

    assert await scheduler.run_pending(
        now=datetime(2026, 9, 7, 16, 15, tzinfo=EASTERN)
    ) is None
    assert await service.store.list_undelivered() == []


@pytest.mark.asyncio
async def test_data_unavailable_slot_fails_closed_and_next_slot_catches_up(tmp_path: Path):
    paths = _files(tmp_path, symbols=("AAPL",), quality=("AAPL",))
    service = FakeService()
    screener = RecoveringScreener([_row("AAPL")])
    scheduler = RadarScheduler(
        service=service,
        screener=screener,
        universe_dir=paths[0],
        quality_path=paths[1],
        calendar_path=paths[2],
    )
    scheduler.validate_startup(now=datetime(2026, 8, 28, 9, 49, tzinfo=EASTERN))

    unavailable = await scheduler.run_pending(
        now=datetime(2026, 8, 28, 9, 50, tzinfo=EASTERN)
    )
    assert unavailable is not None
    assert unavailable["status"] == "DATA_UNAVAILABLE"
    assert unavailable["plan_ids"] == []
    assert service.plan_calls == []
    assert [event.event_type for event in service.store.events][-2:] == [
        "RADAR_RUN",
        "DATA_UNAVAILABLE",
    ]

    next_slot = datetime(2026, 8, 28, 10, 0, tzinfo=EASTERN)
    caught_up = await scheduler.run_pending(now=next_slot)
    current = await scheduler.run_pending(now=next_slot.replace(second=10))
    repeated = await scheduler.run_pending(now=next_slot.replace(second=20))

    assert caught_up is not None
    assert caught_up["scheduled_for"].endswith("09:50:00-04:00")
    assert caught_up["status"] == "COMPLETED"
    assert current is not None
    assert current["scheduled_for"].endswith("10:00:00-04:00")
    assert current["status"] == "COMPLETED"
    assert repeated is None
    assert len(service.plan_calls) == 2


@pytest.mark.asyncio
async def test_keyless_preflight_prevents_partial_plans_when_later_symbol_fails(tmp_path: Path):
    paths = _files(
        tmp_path,
        symbols=("AAPL", "MSFT"),
        quality=("AAPL", "MSFT"),
    )
    service = KeylessPreflightService()
    scheduler = RadarScheduler(
        service=service,
        screener=FakeScreener([_row("AAPL"), _row("MSFT")]),
        universe_dir=paths[0],
        quality_path=paths[1],
        calendar_path=paths[2],
    )
    scheduler.validate_startup(now=datetime(2026, 8, 28, 9, 49, tzinfo=EASTERN))

    result = await scheduler.run_pending(
        now=datetime(2026, 8, 28, 9, 50, tzinfo=EASTERN)
    )

    assert result is not None
    assert result["status"] == "DATA_UNAVAILABLE"
    assert service.prepared == ["AAPL", "MSFT"]
    assert service.plan_calls == []
    assert await service.store.list_plans() == []


@pytest.mark.asyncio
async def test_one_keyless_symbol_failure_completes_and_is_recorded(tmp_path: Path):
    symbols = ("SPY", "AAPL", "MSFT", "NVDA", "META")
    paths = _files(tmp_path, symbols=symbols, quality=("SPY",))
    service = KeylessFailureRatioService()
    screener = PartialScreener(
        [_row("SPY")],
        (SkippedSymbol("DELISTED", "HTTPStatusError"),),
    )
    scheduler = RadarScheduler(
        service=service,
        screener=screener,
        universe_dir=paths[0],
        quality_path=paths[1],
        calendar_path=paths[2],
    )
    scheduler.validate_startup(now=datetime(2026, 8, 28, 9, 49, tzinfo=EASTERN))

    result = await scheduler.run_pending(
        now=datetime(2026, 8, 28, 9, 50, tzinfo=EASTERN)
    )

    assert result is not None
    assert result["status"] == "COMPLETED"
    assert result["skipped_symbols"] == [
        {"symbol": "DELISTED", "error_type": "HTTPStatusError"}
    ]


@pytest.mark.asyncio
async def test_keyless_thirty_percent_failures_make_slot_unavailable(tmp_path: Path):
    symbols = ("SPY", "AAPL", "MSFT", "NVDA", "META", "AMZN", "GOOG", "AMD", "TSLA", "QQQ")
    paths = _files(tmp_path, symbols=symbols, quality=("SPY",))
    service = KeylessFailureRatioService()
    skipped = tuple(
        SkippedSymbol(symbol, "ReadTimeout") for symbol in ("AAPL", "MSFT", "NVDA")
    )
    scheduler = RadarScheduler(
        service=service,
        screener=PartialScreener([_row("SPY")], skipped),
        universe_dir=paths[0],
        quality_path=paths[1],
        calendar_path=paths[2],
    )
    scheduler.validate_startup(now=datetime(2026, 8, 28, 9, 49, tzinfo=EASTERN))

    result = await scheduler.run_pending(
        now=datetime(2026, 8, 28, 9, 50, tzinfo=EASTERN)
    )

    assert result is not None
    assert result["status"] == "DATA_UNAVAILABLE"
    assert result["error_type"] == "KEYLESS_FAILURE_RATIO_EXCEEDED"
    assert len(result["skipped_symbols"]) == 3

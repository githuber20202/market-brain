from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from market_brain.domain.models import IntradayBarRecord, IntradayStructureState
from market_brain.engines.intraday import compute_structure
from market_brain.ledger.store import InMemoryEventStore
from market_brain.orchestration.service import DecisionService
from market_brain.settings import Settings

EASTERN = ZoneInfo("America/New_York")


def at(day, minute):
    return (
        datetime.combine(day, datetime.min.time(), EASTERN).replace(hour=9, minute=30)
        + timedelta(minutes=minute)
    ).astimezone(UTC)


def row(day, minute, *, source="IEX", high=100.4, low=99.8, close=100.1):
    return {
        "t": at(day, minute).isoformat(),
        "o": 100.0,
        "h": high,
        "l": low,
        "c": close,
        "v": 10_000,
        "vw": 100.0,
        "source": source,
    }


def test_missing_iex_opening_minute_waits_for_sip_then_sip_backfill_arms():
    cfg = Settings()
    now = datetime.now(UTC)
    day = now.astimezone(EASTERN).date()
    live = [row(day, minute) for minute in (0, 1, 3, 4)]
    building = compute_structure("TEST", day.isoformat(), live, cfg, now=now)
    assert building.state == IntradayStructureState.BUILDING_OR
    assert building.reasons == ["OPENING_RANGE_WAITING_FOR_SIP"]
    sip = row(day, 2, source="SIP", high=100.8, low=99.7, close=100.3)
    armed = compute_structure(
        "TEST", day.isoformat(), [*live, sip], cfg, now=now,
        sip_confirmed_through=at(day, 5),
    )
    assert armed.state == IntradayStructureState.ARMED
    assert armed.opening_range_high == 100.8


def test_confirmed_sip_empty_opening_minute_is_invalid():
    cfg = Settings()
    now = datetime.now(UTC)
    day = now.astimezone(EASTERN).date()
    live = [row(day, minute) for minute in (0, 1, 3, 4)]
    structure = compute_structure(
        "TEST", day.isoformat(), live, cfg, now=now,
        sip_confirmed_through=at(day, 5),
    )
    assert structure.state == IntradayStructureState.INVALID
    assert structure.reasons == ["OPENING_RANGE_CONFIRMED_EMPTY"]


def test_store_preserves_iex_and_sip_same_minute_and_compute_prefers_sip():
    store = InMemoryEventStore()
    now = datetime.now(UTC)
    day = now.astimezone(EASTERN).date()
    stamp = at(day, 2)
    iex = IntradayBarRecord("TEST", day.isoformat(), stamp, "IEX", 100, 100.2, 99.9, 100.1, 1000, 100.0)
    sip = IntradayBarRecord("TEST", day.isoformat(), stamp, "SIP", 100, 101.0, 99.7, 100.4, 2000, 100.2)

    async def run():
        await store.save_intraday_bar(iex)
        await store.save_intraday_bar(sip)
        return await store.list_intraday_bars("TEST", day.isoformat())

    import asyncio

    stored = asyncio.run(run())
    assert [bar.source for bar in stored] == ["IEX", "SIP"]
    opening = [row(day, minute) for minute in (0, 1, 3, 4)]
    structure = compute_structure(
        "TEST", day.isoformat(), [*opening, *(bar.as_market_bar() for bar in stored)],
        Settings(), now=now, sip_confirmed_through=at(day, 5),
    )
    assert structure.opening_range_high == 101.0


class BackfillProvider:
    configured = True

    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    async def bars_batch(self, symbols, timeframe, start, end):
        self.calls.append((list(symbols), timeframe, start, end))
        return {symbol: list(self.rows.get(symbol, [])) for symbol in symbols}


@pytest.mark.asyncio
async def test_backfill_is_batched_and_advances_sip_coverage():
    now = datetime(2026, 8, 28, 15, 0, tzinfo=UTC)
    day = now.astimezone(EASTERN).date()
    rows = {
        "AAA": [{**row(day, minute), "source": "SIP"} for minute in range(5)],
        "BBB": [{**row(day, minute), "source": "SIP"} for minute in range(5)],
    }
    provider = BackfillProvider(rows)
    store = InMemoryEventStore()
    service = DecisionService(store, market_data=provider)
    result = await service.backfill_intraday_structures(["AAA", "BBB"], now=now)
    assert len(provider.calls) == 1
    assert provider.calls[0][0] == ["AAA", "BBB"]
    assert set(result) == {"AAA", "BBB"}
    for symbol in result:
        coverage = await store.get_runtime_status_key(
            f"intraday_sip_confirmed_through:{day.isoformat()}:{symbol}"
        )
        assert coverage["source"] == "SIP"


@pytest.mark.asyncio
async def test_backfill_derives_running_vwap_when_yahoo_bar_has_no_vwap():
    now = datetime(2026, 8, 28, 15, 0, tzinfo=UTC)
    day = now.astimezone(EASTERN).date()
    rows = [
        {key: value for key, value in row(day, minute).items() if key != "vw"}
        for minute in range(5)
    ]
    store = InMemoryEventStore()
    service = DecisionService(store, market_data=BackfillProvider({"AAA": rows}))

    result = await service.backfill_intraday_structures(["AAA"], now=now)

    assert result["AAA"].running_vwap == pytest.approx(100.1)


def test_backfill_setting_is_five_minutes():
    assert Settings().intraday_backfill_interval_seconds == 300

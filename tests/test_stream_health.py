from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from market_brain.ledger.store import InMemoryEventStore
from market_brain.runtime.stream_health import StreamStaleMonitor
from market_brain.settings import Settings


def _calendar(tmp_path):
    path = tmp_path / "calendar.csv"
    path.write_text(
        "date,status,open_time,close_time,source\n"
        "2026-09-07,CLOSED,,,NYSE\n"
        "2027-01-01,CLOSED,,,NYSE\n"
    )
    return path


@pytest.mark.asyncio
async def test_stream_stale_and_recovered_emit_once_per_transition(tmp_path):
    store = InMemoryEventStore()
    cfg = Settings(
        market_calendar_path=_calendar(tmp_path),
        stream_stale_alert_seconds=120,
    )
    monitor = StreamStaleMonitor(store, cfg=cfg)
    now = datetime(2026, 8, 28, 14, 0, tzinfo=UTC)
    monitor.validate_startup(now=now)
    await store.set_runtime_status(
        "stream_last_message_at", (now - timedelta(seconds=121)).isoformat()
    )

    assert await monitor.check(now=now) == "STREAM_STALE"
    assert await monitor.check(now=now + timedelta(seconds=1)) is None
    await store.set_runtime_status(
        "stream_last_message_at", (now + timedelta(seconds=2)).isoformat()
    )
    assert await monitor.check(now=now + timedelta(seconds=2)) == "STREAM_RECOVERED"
    assert await monitor.check(now=now + timedelta(seconds=3)) is None

    assert [row.event_type for row in store.events] == ["STREAM_STALE", "STREAM_RECOVERED"]
    assert [row.kind for row in await store.list_undelivered()] == [
        "STREAM_STALE",
        "STREAM_RECOVERED",
    ]
    assert (await store.get_runtime_status())["stream_stale"] is False


@pytest.mark.asyncio
async def test_stream_stale_monitor_is_quiet_outside_regular_session(tmp_path):
    store = InMemoryEventStore()
    cfg = Settings(market_calendar_path=_calendar(tmp_path))
    monitor = StreamStaleMonitor(store, cfg=cfg)
    weekend = datetime(2026, 8, 29, 14, 0, tzinfo=UTC)
    monitor.validate_startup(now=weekend)

    assert await monitor.check(now=weekend) is None
    assert store.events == []


@pytest.mark.asyncio
async def test_stream_stale_waits_full_threshold_after_market_open(tmp_path):
    store = InMemoryEventStore()
    cfg = Settings(
        market_calendar_path=_calendar(tmp_path),
        stream_stale_alert_seconds=120,
    )
    monitor = StreamStaleMonitor(store, cfg=cfg)
    open_time = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
    monitor.validate_startup(now=open_time)
    await store.set_runtime_status(
        "stream_last_message_at", (open_time - timedelta(days=1)).isoformat()
    )

    assert await monitor.check(now=open_time + timedelta(seconds=120)) is None
    assert await monitor.check(now=open_time + timedelta(seconds=121)) == "STREAM_STALE"

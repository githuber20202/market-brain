from datetime import UTC, datetime

import pytest

from market_brain.domain.models import IntradayBarRecord


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_postgres_intraday_bars_round_trip_preserves_sources(pg_store):
    stamp = datetime(2026, 8, 28, 13, 32, tzinfo=UTC)
    for source, high in (("IEX", 100.2), ("SIP", 100.8)):
        await pg_store.save_intraday_bar(
            IntradayBarRecord(
                symbol="TEST",
                session_date="2026-08-28",
                minute_ts=stamp,
                source=source,
                open=100.0,
                high=high,
                low=99.8,
                close=100.1,
                volume=1234.0,
                vwap=100.0,
            )
        )
    rows = await pg_store.list_intraday_bars("TEST", "2026-08-28")
    assert [(row.source, row.high) for row in rows] == [("IEX", 100.2), ("SIP", 100.8)]


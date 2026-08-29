import os
from pathlib import Path

import pytest
import pytest_asyncio

from market_brain.ledger.store import PostgresEventStore


@pytest_asyncio.fixture
async def pg_store():
    dsn = os.getenv("TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("TEST_POSTGRES_DSN not configured")
    store = PostgresEventStore(dsn)
    await store.connect()
    assert store.pool is not None
    schema = Path("config/schema.sql").read_text()
    async with store.pool.acquire() as connection:
        await connection.execute(schema)
        await connection.execute(
            "TRUNCATE TABLE decision_events,trade_plans,risk_wallet,reservations,"
            "position_twin,alerts,counterfactual_outcomes,liquidity_profiles,intraday_bars,shadow_trades,runtime_status RESTART IDENTITY CASCADE"
        )
    try:
        yield store
    finally:
        await store.close()

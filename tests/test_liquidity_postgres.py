from datetime import UTC, datetime, timedelta

import pytest

from market_brain.domain.models import LiquidityProfile


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_postgres_liquidity_profile_round_trip(pg_store):
    now = datetime.now(UTC)
    expected = LiquidityProfile(
        symbol="AAPL",
        adv20=3_250_000.5,
        close=201.25,
        as_of=now - timedelta(days=1),
        refreshed_at=now,
    )
    await pg_store.save_liquidity_profile(expected)
    restored = await pg_store.get_liquidity_profile("aapl")
    assert restored is not None
    assert restored.symbol == "AAPL"
    assert restored.adv20 == pytest.approx(expected.adv20)
    assert restored.close == pytest.approx(expected.close)
    assert restored.as_of == expected.as_of
    assert restored.refreshed_at == expected.refreshed_at
    listed = await pg_store.list_liquidity_profiles()
    assert [row.symbol for row in listed] == ["AAPL"]


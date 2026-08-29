from datetime import UTC, datetime, timedelta

import httpx
import pytest

from market_brain.ledger.store import InMemoryEventStore
from market_brain.providers.alpaca import AlpacaMarketData, DataPlanViolation
from market_brain.providers.rate_limit import TokenBucketRateLimiter
from market_brain.settings import Settings


def test_free_plan_forces_iex_live_and_delayed_sip_history():
    cfg = Settings(
        data_plan="free",
        discovery_feed="sip",
        decision_feed="sip",
        historical_feed="iex",
        historical_lag_minutes=1,
        alpaca_stream_url="wss://example.test/v2/sip",
    )
    assert cfg.discovery_feed == "iex"
    assert cfg.decision_feed == "iex"
    assert cfg.historical_feed == "sip"
    assert cfg.historical_lag_minutes == 16
    assert cfg.alpaca_stream_url.endswith("/v2/iex")
    assert cfg.stream_max_symbols == 30
    assert cfg.rest_calls_per_minute == 200
    assert cfg.rest_safe_calls_per_minute == 180


@pytest.mark.asyncio
async def test_historical_lag_is_enforced_on_end():
    fixed_now = datetime(2026, 8, 29, 15, 0, tzinfo=UTC)
    seen = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(dict(request.url.params))
        return httpx.Response(200, json={"bars": []})

    cfg = Settings(
        data_plan="free",
        alpaca_api_key="key",
        alpaca_api_secret="secret",
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = AlpacaMarketData(cfg, client, now=lambda: fixed_now)
        await provider.bars(
            "TEST",
            "1Min",
            fixed_now - timedelta(hours=1),
            fixed_now,
        )
    assert datetime.fromisoformat(seen[0]["end"]) == fixed_now - timedelta(minutes=16)
    assert seen[0]["feed"] == "sip"


@pytest.mark.asyncio
async def test_subscription_error_records_once_and_opens_fail_closed_circuit():
    fixed_now = datetime(2026, 8, 29, 15, 0, tzinfo=UTC)
    calls = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(403, json={"message": "subscription required for sip feed"})

    store = InMemoryEventStore()
    cfg = Settings(
        data_plan="free",
        alpaca_api_key="key",
        alpaca_api_secret="secret",
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = AlpacaMarketData(
            cfg,
            client,
            event_store=store,
            now=lambda: fixed_now,
        )
        for _ in range(2):
            with pytest.raises(DataPlanViolation, match="DATA_PLAN_VIOLATION"):
                await provider.bars(
                    "TEST",
                    "1Min",
                    fixed_now - timedelta(hours=1),
                    fixed_now,
                )
    assert calls == 1
    events = await store.read_events()
    violations = [event for event in events if event.event_type == "DATA_PLAN_VIOLATION"]
    assert len(violations) == 1
    assert violations[0].payload["feed"] == "sip"
    assert violations[0].payload["status_code"] == 403


@pytest.mark.asyncio
async def test_token_bucket_allows_180_burst_and_181st_waits():
    class Clock:
        value = 0.0

        def __call__(self):
            return self.value

    clock = Clock()
    waits = []

    async def fake_sleep(delay):
        waits.append(delay)
        clock.value += delay

    limiter = TokenBucketRateLimiter(180, clock=clock, sleep=fake_sleep)
    for _ in range(180):
        await limiter.acquire()
    assert waits == []
    await limiter.acquire()
    assert len(waits) == 1
    assert waits[0] == pytest.approx(1 / 3)
    for _ in range(19):
        await limiter.acquire()
    assert clock.value < 60


@pytest.mark.asyncio
async def test_snapshots_and_bars_batch_symbols_instead_of_one_call_per_symbol():
    calls = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.url.path, dict(request.url.params)))
        symbols = request.url.params.get("symbols", "").split(",")
        if request.url.path.endswith("/snapshots"):
            return httpx.Response(
                200,
                json={
                    "snapshots": {
                        symbol: {
                            "latestTrade": {"p": 10.0},
                            "latestQuote": {"bp": 9.99, "ap": 10.01, "t": datetime.now(UTC).isoformat()},
                        }
                        for symbol in symbols
                    }
                },
            )
        return httpx.Response(200, json={"bars": {symbol: [] for symbol in symbols}})

    cfg = Settings(
        data_plan="free",
        alpaca_api_key="key",
        alpaca_api_secret="secret",
        rest_batch_symbols=2,
    )
    symbols = ["A", "B", "C", "D", "E"]
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = AlpacaMarketData(cfg, client)
        await provider.snapshots(symbols)
        await provider.bars_batch(
            symbols,
            "1Day",
            datetime.now(UTC) - timedelta(days=10),
            datetime.now(UTC) - timedelta(days=1),
        )
    snapshot_calls = [row for row in calls if row[0].endswith("/snapshots")]
    bars_calls = [row for row in calls if row[0].endswith("/bars")]
    assert len(snapshot_calls) == 3
    assert len(bars_calls) == 3
    assert all(len(params["symbols"].split(",")) <= 2 for _, params in calls)


@pytest.mark.asyncio
async def test_bars_batch_follows_pagination_for_complete_session():
    calls = []

    async def handler(request: httpx.Request) -> httpx.Response:
        params = dict(request.url.params)
        calls.append(params)
        if "page_token" not in params:
            return httpx.Response(
                200,
                json={"bars": {"A": [{"t": "first"}]}, "next_page_token": "page-2"},
            )
        return httpx.Response(200, json={"bars": {"A": [{"t": "second"}]}})

    cfg = Settings(data_plan="free", alpaca_api_key="key", alpaca_api_secret="secret")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = AlpacaMarketData(cfg, client)
        result = await provider.bars_batch(
            ["A"],
            "1Min",
            datetime.now(UTC) - timedelta(days=2),
            datetime.now(UTC) - timedelta(days=1),
        )

    assert [row["t"] for row in result["A"]] == ["first", "second"]
    assert len(calls) == 2
    assert calls[1]["page_token"] == "page-2"


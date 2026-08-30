from __future__ import annotations

import json
from datetime import UTC, date, datetime
from itertools import pairwise

import httpx
import pytest

from market_brain.alerts.sink import GitHubIssueSink
from market_brain.providers.rate_limit import TokenBucketRateLimiter
from market_brain.providers.yahoo_replay import (
    YAHOO_REPLAY_SOURCE_ID,
    YahooReplayMarketData,
)
from market_brain.runtime.rehearsal import MutableClock, rehearsal_ticks
from market_brain.settings import Settings


def _chart_payload(interval: str) -> dict:
    if interval == "1d":
        timestamps = [1785429000, 1785515400]
        values = [90.0, 91.0]
    else:
        timestamps = [1787831940, 1787923800, 1787923860, 1787923920]
        values = [99.0, 100.0, 101.0, 102.0]
    return {
        "chart": {
            "result": [
                {
                    "meta": {"previousClose": 99.0},
                    "timestamp": timestamps,
                    "indicators": {
                        "quote": [
                            {
                                "open": values,
                                "high": [value + 0.5 for value in values],
                                "low": [value - 0.5 for value in values],
                                "close": values,
                                "volume": [100, 200, 300, 400],
                            }
                        ]
                    },
                }
            ],
            "error": None,
        }
    }


@pytest.mark.asyncio
async def test_yahoo_replay_slices_future_bars_caches_and_disables_cboe():
    requests: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(str(request.url))
        return httpx.Response(
            200,
            json=_chart_payload(request.url.params.get("interval", "1m")),
        )

    clock = MutableClock(datetime(2026, 8, 28, 13, 31, tzinfo=UTC))
    cfg = Settings(max_delayed_age_minutes=20)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = YahooReplayMarketData(
            date(2026, 8, 28),
            cfg,
            client,
            limiter=TokenBucketRateLimiter(1_000_000),
            now=clock.now,
        )
        first = await provider.snapshot("NVDA")
        assert first.source_id == YAHOO_REPLAY_SOURCE_ID
        assert first.last == 101.0
        assert first.prior_close == 99.0
        assert first.high == 101.5
        assert first.low == 99.5
        assert first.volume == 500.0
        assert first.bid is None and first.ask is None
        assert first.metadata["price_cross_check"] == "SKIP_REHEARSAL"
        assert first.metadata["cboe_error_type"] == "DISABLED_IN_REHEARSAL"
        assert first.metadata["last_bar_timestamp"] == "2026-08-28T13:31:00+00:00"
        assert provider.request_count == 1

        clock.set(datetime(2026, 8, 28, 13, 32, tzinfo=UTC))
        second = await provider.snapshot("NVDA")
        assert second.last == 102.0
        assert second.high == 102.5
        assert second.volume == 900.0
        assert provider.request_count == 1

        visible = await provider.bars(
            "NVDA",
            "1Min",
            datetime(2026, 8, 28, 13, 30, tzinfo=UTC),
            datetime(2026, 8, 28, 13, 33, tzinfo=UTC),
        )
        assert [row["c"] for row in visible] == [100.0, 101.0, 102.0]
        assert {row["source"] for row in visible} == {YAHOO_REPLAY_SOURCE_ID}

        await provider.bars(
            "NVDA",
            "1Day",
            datetime(2026, 7, 1, tzinfo=UTC),
            datetime(2026, 8, 28, tzinfo=UTC),
        )
        await provider.bars(
            "NVDA",
            "1Day",
            datetime(2026, 7, 1, tzinfo=UTC),
            datetime(2026, 8, 28, tzinfo=UTC),
        )
        assert provider.request_count == 2
        assert len(requests) == 2
        assert all("cdn.cboe.com" not in request for request in requests)


def test_rehearsal_ticks_cover_every_ten_minutes_through_1520():
    ticks = rehearsal_ticks(date(2026, 8, 28))
    assert len(ticks) == 34
    assert ticks[0].isoformat() == "2026-08-28T09:50:00-04:00"
    assert ticks[-1].isoformat() == "2026-08-28T15:20:00-04:00"
    assert all(
        (later - earlier).total_seconds() == 600
        for earlier, later in pairwise(ticks)
    )


@pytest.mark.asyncio
async def test_github_issue_sink_posts_one_rehearsal_comment_and_closes_issue():
    requests: list[tuple[str, str, dict | None]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        requests.append((request.method, request.url.path, body))
        if request.method == "GET" and request.url.path.endswith("/labels/shadow"):
            return httpx.Response(200, json={"name": "shadow"})
        if request.method == "GET" and request.url.path.endswith("/issues"):
            return httpx.Response(200, json=[])
        if request.method == "POST" and request.url.path.endswith("/issues"):
            return httpx.Response(201, json={"number": 28})
        return httpx.Response(200, json={"id": 1})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        sink = GitHubIssueSink(
            "test-token",
            "githuber20202/market-brain",
            client,
        )
        number = await sink.send_rehearsal_summary("2026-08-28", "CLEAN")

    assert number == 28
    issue = next(
        body
        for method, path, body in requests
        if method == "POST" and path.endswith("/issues")
    )
    assert issue["title"] == "Shadow rehearsal 2026-08-28"
    comments = [body for method, path, body in requests if path.endswith("/comments")]
    assert comments == [{"body": "@githuber20202\n\nCLEAN"}]
    assert any(
        method == "PATCH" and path.endswith("/issues/28") and body == {"state": "closed"}
        for method, path, body in requests
    )
    assert "test-token" not in json.dumps(requests)

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from market_brain.domain.models import LiquidityProfile
from market_brain.engines.liquidity import apply_keyless_liquidity_gate
from market_brain.ledger.events import LedgerEvent
from market_brain.ledger.store import InMemoryEventStore
from market_brain.orchestration.service import DecisionService
from market_brain.providers.base import DataUnavailable
from market_brain.providers.rate_limit import TokenBucketRateLimiter
from market_brain.providers.yahoo import YahooMarketData
from market_brain.runtime.daily_digest import DailyDigest
from market_brain.settings import Settings

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"
FIXED_NOW = datetime(2026, 8, 28, 13, 36, tzinfo=UTC)


def _fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def _transport(*, cboe_price: float = 100.55, yahoo_statuses: list[int] | None = None):
    calls: list[httpx.Request] = []
    statuses = list(yahoo_statuses or [])

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.host == "cdn.cboe.com":
            payload = _fixture("cboe_quote.json")
            payload["data"]["current_price"] = cboe_price
            return httpx.Response(200, json=payload)
        if statuses:
            status = statuses.pop(0)
            if status != 200:
                return httpx.Response(status, json={"error": "temporary"})
        filename = (
            "yahoo_chart_1d.json"
            if request.url.params.get("interval") == "1d"
            else "yahoo_chart_1m.json"
        )
        return httpx.Response(200, json=_fixture(filename))

    return httpx.MockTransport(handler), calls


def _cfg(**overrides) -> Settings:
    return Settings(
        data_plan="keyless_delayed",
        keyless_request_interval_seconds=0.5,
        **overrides,
    )


@pytest.mark.asyncio
async def test_yahoo_snapshot_has_full_provenance_and_cboe_cross_check():
    transport, calls = _transport()
    async with httpx.AsyncClient(transport=transport) as client:
        provider = YahooMarketData(
            _cfg(),
            client,
            limiter=TokenBucketRateLimiter(1000),
            now=lambda: FIXED_NOW,
        )
        snapshot = await provider.snapshot("test")

    assert snapshot.source_id == "YAHOO_DELAYED"
    assert snapshot.delay_minutes == pytest.approx(1.0)
    assert snapshot.fetched_at == FIXED_NOW
    assert snapshot.metadata["fetched_at"] == FIXED_NOW.isoformat()
    assert snapshot.metadata["price_cross_check"] == "PASS"
    assert snapshot.metadata["cboe_source_id"] == "CBOE_DELAYED"
    assert snapshot.authoritative is True
    assert [request.url.host for request in calls] == [
        "query2.finance.yahoo.com",
        "cdn.cboe.com",
    ]


@pytest.mark.asyncio
async def test_cboe_divergence_marks_snapshot_non_authoritative():
    transport, _calls = _transport(cboe_price=102.0)
    async with httpx.AsyncClient(transport=transport) as client:
        provider = YahooMarketData(
            _cfg(iex_mid_tolerance_pct=0.75),
            client,
            limiter=TokenBucketRateLimiter(1000),
            now=lambda: FIXED_NOW,
        )
        snapshot = await provider.snapshot("TEST")

    assert snapshot.authoritative is False
    assert snapshot.metadata["price_cross_check"] == "FAIL"
    profile = LiquidityProfile("TEST", 8_000_000, 100.0, FIXED_NOW)
    assert "PRICE_CROSS_CHECK_FAILED" in apply_keyless_liquidity_gate(
        snapshot, profile, provider.cfg
    )


@pytest.mark.asyncio
async def test_slot_cache_prevents_duplicate_yahoo_and_cboe_requests():
    transport, calls = _transport()
    async with httpx.AsyncClient(transport=transport) as client:
        provider = YahooMarketData(
            _cfg(),
            client,
            limiter=TokenBucketRateLimiter(1000),
            now=lambda: FIXED_NOW,
        )
        first = await provider.snapshot("TEST")
        second = await provider.snapshot("TEST")

    assert first.last == second.last
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_cboe_failure_is_optional_and_negatively_cached_for_slot():
    calls: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.host or "")
        if request.url.host == "cdn.cboe.com":
            return httpx.Response(503, json={})
        return httpx.Response(200, json=_fixture("yahoo_chart_1m.json"))

    async def fake_sleep(_seconds: float) -> None:
        return None

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = YahooMarketData(
            _cfg(),
            client,
            limiter=TokenBucketRateLimiter(1000),
            now=lambda: FIXED_NOW,
            sleep=fake_sleep,
        )
        first = await provider.snapshot("TEST")
        second = await provider.snapshot("TEST")

    assert first.last == second.last == 100.5
    assert first.metadata["cboe_error_type"] == "DataUnavailable"
    assert calls.count("query2.finance.yahoo.com") == 1
    assert calls.count("cdn.cboe.com") == 3


@pytest.mark.asyncio
async def test_yahoo_retries_429_with_backoff_then_succeeds():
    transport, calls = _transport(yahoo_statuses=[429, 200])
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    async with httpx.AsyncClient(transport=transport) as client:
        provider = YahooMarketData(
            _cfg(),
            client,
            limiter=TokenBucketRateLimiter(1000),
            now=lambda: FIXED_NOW,
            sleep=fake_sleep,
        )
        snapshot = await provider.snapshot("TEST")

    assert snapshot.last == 100.5
    assert sleeps == [1.0]
    assert len(calls) == 3


@pytest.mark.asyncio
async def test_yahoo_timeout_exhaustion_is_data_unavailable():
    calls = 0
    sleeps: list[float] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ReadTimeout("timeout", request=request)

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = YahooMarketData(
            _cfg(keyless_retry_attempts=3),
            client,
            limiter=TokenBucketRateLimiter(1000),
            now=lambda: FIXED_NOW,
            sleep=fake_sleep,
        )
        with pytest.raises(DataUnavailable, match="DATA_UNAVAILABLE") as caught:
            await provider.snapshot("TEST")

    assert calls == 3
    assert sleeps == [1.0, 2.0]
    assert caught.value.source_id == "YAHOO_DELAYED"
    assert caught.value.error_type == "ReadTimeout"


@pytest.mark.asyncio
async def test_yahoo_daily_route_builds_adv_liquidity_profile():
    transport, calls = _transport()
    async with httpx.AsyncClient(transport=transport) as client:
        provider = YahooMarketData(
            _cfg(),
            client,
            limiter=TokenBucketRateLimiter(1000),
            now=lambda: FIXED_NOW,
        )
        service = DecisionService(
            InMemoryEventStore(),
            cfg=provider.cfg,
            market_data=provider,
        )
        profile = await service.refresh_liquidity_profile("TEST", now=FIXED_NOW)

    assert profile.adv20 == pytest.approx(7_450_000)
    daily = [request for request in calls if request.url.params.get("interval") == "1d"]
    assert len(daily) == 1
    assert daily[0].url.params["range"] == "1y"


def test_keyless_liquidity_gate_uses_adv_age_and_last_bar_range():
    cfg = _cfg(min_adv_keyless=5_000_000, max_delayed_age_minutes=20)
    transport, _calls = _transport()
    del transport
    from market_brain.domain.models import MarketSnapshot

    snapshot = MarketSnapshot(
        symbol="TEST",
        last=100.0,
        source_id="YAHOO_DELAYED",
        delay_minutes=10.0,
        authoritative=True,
        metadata={
            "last_bar_high": 100.3,
            "last_bar_low": 99.8,
            "price_cross_check": "PASS",
        },
    )
    profile = LiquidityProfile("TEST", 6_000_000, 99.0, FIXED_NOW)
    assert apply_keyless_liquidity_gate(snapshot, profile, cfg) == [
        "LIQUIDITY_GATE_PASS"
    ]
    snapshot.delay_minutes = 21.0
    assert "DELAYED_DATA_STALE" in apply_keyless_liquidity_gate(snapshot, profile, cfg)
    snapshot.delay_minutes = 10.0
    low_adv = LiquidityProfile("TEST", 4_999_999, 99.0, FIXED_NOW)
    assert "ADV_TOO_LOW" in apply_keyless_liquidity_gate(snapshot, low_adv, cfg)


@pytest.mark.asyncio
async def test_digest_reports_latest_slot_data_availability():
    store = InMemoryEventStore()
    now = datetime(2026, 8, 28, 16, 15, tzinfo=UTC)
    await store.append(
        LedgerEvent(
            "RADAR_RUN",
            "radar:2026-08-28:0950",
            {"status": "DATA_UNAVAILABLE"},
            occurred_at=now - timedelta(hours=5),
        )
    )
    await store.append(
        LedgerEvent(
            "RADAR_RUN",
            "radar:2026-08-28:0950",
            {"status": "COMPLETED"},
            occurred_at=now - timedelta(hours=4),
        )
    )
    await store.append(
        LedgerEvent(
            "RADAR_RUN",
            "radar:2026-08-28:1020",
            {"status": "DATA_UNAVAILABLE"},
            occurred_at=now - timedelta(hours=3),
        )
    )

    alert = await DailyDigest(store).create(now=now)

    assert alert is not None
    assert alert.payload["data_availability"] == {
        "slots_ok": 1,
        "slots_unavailable": 1,
    }
    assert "Data availability: slots_ok=1 slots_unavailable=1" in alert.payload["text"]


def test_default_plan_is_keyless_and_alpaca_free_policy_is_unchanged():
    assert Settings().data_plan == "keyless_delayed"
    keyless = Settings(data_plan="keyless_delayed")
    assert keyless.discovery_feed == "yahoo"
    assert keyless.historical_feed == "yahoo"
    free = Settings(
        data_plan="free",
        discovery_feed="sip",
        decision_feed="sip",
        historical_feed="iex",
        historical_lag_minutes=1,
    )
    assert free.discovery_feed == "iex"
    assert free.decision_feed == "iex"
    assert free.historical_feed == "sip"
    assert free.historical_lag_minutes == 16


def test_data_probe_uses_only_public_keyless_providers():
    script = (ROOT / "scripts" / "data_probe.py").read_text()
    assert "YahooMarketData" in script
    assert "provider.cboe.quote" in script
    assert "ALPACA_API_KEY" not in script
    assert "TELEGRAM" not in script

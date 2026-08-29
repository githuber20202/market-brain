from datetime import UTC, datetime, timedelta

import httpx
import pytest
from fastapi.testclient import TestClient

import market_brain.api.main as api_main
from market_brain.domain.models import LiquidityProfile, MarketSnapshot, StrategyLane
from market_brain.engines.quality import classify_quality
from market_brain.ledger.store import InMemoryEventStore
from market_brain.orchestration.service import DecisionService
from market_brain.providers.alpaca import AlpacaMarketData
from market_brain.settings import Settings
from tests.retest_helpers import activate_with_server_retest


class FakeProvider:
    def __init__(self, market_snapshot: MarketSnapshot):
        self.market_snapshot = market_snapshot
        self.calls: list[tuple[str, bool]] = []

    async def snapshot(self, symbol: str, decision: bool = False) -> MarketSnapshot:
        self.calls.append((symbol, decision))
        self.market_snapshot.symbol = symbol
        return self.market_snapshot


def planning_snapshot() -> MarketSnapshot:
    return MarketSnapshot(
        symbol="TEST",
        last=100.8,
        prior_close=98.0,
        bid=100.79,
        ask=100.81,
        volume=2_000_000,
        avg_volume=1_000_000,
        vwap=100.0,
        open_price=99.0,
        opening_range_high=100.8,
        retest_low=100.0,
        benchmark_return_pct=0.5,
        catalyst_verified=True,
        catalyst_strength=0.9,
    )


async def make_service(provider) -> tuple[DecisionService, object]:
    service = DecisionService(InMemoryEventStore(), market_data=provider)
    await service.seed_wallet(10_000, 10_000)
    quality = classify_quality("TEST", 90, datetime.now(UTC))
    plan, _ = await service.build_plan(
        planning_snapshot(), quality, StrategyLane.CORE_MOMENTUM, 15, 10
    )
    return service, plan


def alpaca_transport(*, quote_timestamp: str | None, feed: str, last: float):
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["feed"] == feed
        quote = {"bp": last - 0.01, "ap": last + 0.01}
        if quote_timestamp is not None:
            quote["t"] = quote_timestamp
        return httpx.Response(
            200,
            json={
                "snapshots": {
                    "TEST": {
                        "latestTrade": {"p": last, "t": datetime.now(UTC).isoformat()},
                        "latestQuote": quote,
                        "dailyBar": {"vw": last - 0.2, "v": 2_000_000},
                        "prevDailyBar": {"c": 98.0},
                    }
                }
            },
        )

    return httpx.MockTransport(handler)


def test_activate_rejects_caller_source_id():
    client = TestClient(api_main.app)
    response = client.post(
        "/plans/not-important/activate",
        json={"retest_valid": True, "source_id": "ALPACA_SIP"},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_activate_uses_provider_snapshot_not_caller_snapshot():
    provider = FakeProvider(
        MarketSnapshot(
            symbol="IGNORED",
            last=100.8,
            bid=100.79,
            ask=100.81,
            vwap=100.0,
            data_age_seconds=1.0,
            source_id="ALPACA_SIP",
            authoritative=True,
        )
    )
    service, plan = await make_service(provider)
    provider.market_snapshot.last = plan.entry_trigger
    provider.market_snapshot.bid = plan.entry_trigger - 0.01
    provider.market_snapshot.ask = plan.entry_trigger + 0.01
    decision = await activate_with_server_retest(service, plan.plan_id)
    assert provider.calls == [("TEST", True)]
    assert decision.state == "BUY_NOW"


@pytest.mark.asyncio
async def test_quote_without_timestamp_is_armed_not_buy_now():
    cfg = Settings(data_plan="plus", alpaca_api_key="key", alpaca_api_secret="secret", decision_feed="sip")
    async with httpx.AsyncClient(
        transport=alpaca_transport(quote_timestamp=None, feed="sip", last=100.8)
    ) as client:
        provider = AlpacaMarketData(cfg, client)
        service, plan = await make_service(provider)
        decision = await activate_with_server_retest(service, plan.plan_id)
    assert decision.state == "ARMED"
    assert "AUTHORITATIVE_MARKET_FEED_REQUIRED" in decision.reasons
    assert "MARKET_DATA_STALE" in decision.reasons


@pytest.mark.asyncio
async def test_iex_liquidity_gate_pass_can_produce_buy_now():
    now = datetime.now(UTC)
    provider = FakeProvider(
        MarketSnapshot(
            symbol="TEST",
            last=100.8,
            bid=100.79,
            ask=100.81,
            vwap=100.0,
            data_age_seconds=1.0,
            source_id="ALPACA_IEX",
            authoritative=False,
        )
    )
    service, plan = await make_service(provider)
    provider.market_snapshot.last = plan.entry_trigger
    provider.market_snapshot.bid = plan.entry_trigger - 0.01
    provider.market_snapshot.ask = plan.entry_trigger + 0.01
    await service.store.save_liquidity_profile(
        LiquidityProfile(
            symbol="TEST",
            adv20=3_000_000,
            close=100.0,
            as_of=now - timedelta(days=1),
            refreshed_at=now,
        )
    )
    decision = await activate_with_server_retest(service, plan.plan_id)
    assert decision.state == "BUY_NOW"
    assert "LIQUIDITY_GATE_PASS" in decision.reasons
    evaluated = [row for row in await service.store.read_events() if row.event_type == "PLAN_EVALUATED"]
    assert "LIQUIDITY_GATE_PASS" in evaluated[-1].payload["reasons"]


@pytest.mark.asyncio
async def test_iex_low_adv_is_armed_with_reason():
    now = datetime.now(UTC)
    provider = FakeProvider(
        MarketSnapshot(
            symbol="TEST", last=100.8, bid=100.79, ask=100.81, vwap=100.0,
            data_age_seconds=1.0, source_id="ALPACA_IEX", authoritative=False,
        )
    )
    service, plan = await make_service(provider)
    provider.market_snapshot.last = plan.entry_trigger
    provider.market_snapshot.bid = plan.entry_trigger - 0.01
    provider.market_snapshot.ask = plan.entry_trigger + 0.01
    await service.store.save_liquidity_profile(
        LiquidityProfile(
            symbol="TEST", adv20=500_000, close=100.0,
            as_of=now - timedelta(days=1), refreshed_at=now,
        )
    )
    decision = await activate_with_server_retest(service, plan.plan_id)
    assert decision.state == "ARMED"
    assert "ADV_TOO_LOW" in decision.reasons


@pytest.mark.asyncio
async def test_iex_quote_eight_seconds_old_is_stale():
    now = datetime.now(UTC)
    provider = FakeProvider(
        MarketSnapshot(
            symbol="TEST", last=100.8, bid=100.79, ask=100.81, vwap=100.0,
            data_age_seconds=8.0, source_id="ALPACA_IEX", authoritative=False,
        )
    )
    service, plan = await make_service(provider)
    provider.market_snapshot.last = plan.entry_trigger
    provider.market_snapshot.bid = plan.entry_trigger - 0.01
    provider.market_snapshot.ask = plan.entry_trigger + 0.01
    await service.store.save_liquidity_profile(
        LiquidityProfile(
            symbol="TEST", adv20=3_000_000, close=100.0,
            as_of=now - timedelta(days=1), refreshed_at=now,
        )
    )
    decision = await activate_with_server_retest(service, plan.plan_id)
    assert decision.state == "ARMED"
    assert "QUOTE_STALE" in decision.reasons


@pytest.mark.asyncio
async def test_fresh_sip_snapshot_can_produce_buy_now():
    now = datetime.now(UTC).isoformat()
    cfg = Settings(data_plan="plus", alpaca_api_key="key", alpaca_api_secret="secret", decision_feed="sip")
    async with httpx.AsyncClient(
        transport=alpaca_transport(quote_timestamp=now, feed="sip", last=100.8)
    ) as client:
        provider = AlpacaMarketData(cfg, client)
        service, plan = await make_service(provider)
        decision = await activate_with_server_retest(service, plan.plan_id)
    assert decision.state == "BUY_NOW"


@pytest.mark.asyncio
async def test_existing_iex_reservation_re_evaluation_records_liquidity_reason():
    now = datetime.now(UTC)
    provider = FakeProvider(
        MarketSnapshot(
            symbol="TEST",
            last=100.8,
            bid=100.79,
            ask=100.81,
            vwap=100.0,
            data_age_seconds=1.0,
            source_id="ALPACA_IEX",
            authoritative=False,
        )
    )
    service, plan = await make_service(provider)
    provider.market_snapshot.last = plan.entry_trigger
    provider.market_snapshot.bid = plan.entry_trigger - 0.01
    provider.market_snapshot.ask = plan.entry_trigger + 0.01
    await service.store.save_liquidity_profile(
        LiquidityProfile(
            symbol="TEST",
            adv20=3_000_000,
            close=100.0,
            as_of=now - timedelta(days=1),
            refreshed_at=now,
        )
    )
    first = await activate_with_server_retest(service, plan.plan_id)
    assert first.state == "BUY_NOW"
    before = [
        row for row in await service.store.read_events()
        if row.event_type == "PLAN_EVALUATED"
    ]
    second = await activate_with_server_retest(service, plan.plan_id)
    assert second.state == "BUY_NOW"
    assert "LIQUIDITY_GATE_PASS" in second.reasons
    after = [
        row for row in await service.store.read_events()
        if row.event_type == "PLAN_EVALUATED"
    ]
    assert len(after) == len(before) + 1
    assert "LIQUIDITY_GATE_PASS" in after[-1].payload["reasons"]


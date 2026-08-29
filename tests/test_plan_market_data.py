from datetime import UTC, datetime

import httpx
import pytest
from fastapi.testclient import TestClient

import market_brain.api.main as api_main
from market_brain.domain.models import StrategyLane
from market_brain.engines.quality import classify_quality
from market_brain.ledger.store import InMemoryEventStore
from market_brain.orchestration.service import DecisionService
from market_brain.providers.alpaca import AlpacaMarketData
from market_brain.settings import Settings


def market_transport(seen: list[tuple[str, dict]]):
    bars = [
        {"t": "2026-08-28T13:30:00+00:00", "o": 99.0, "h": 100.2, "l": 98.9, "c": 100.0, "v": 1000},
        {"t": "2026-08-28T13:31:00+00:00", "o": 100.0, "h": 100.6, "l": 99.8, "c": 100.4, "v": 1200},
        {"t": "2026-08-28T13:32:00+00:00", "o": 100.4, "h": 100.8, "l": 100.1, "c": 100.7, "v": 1400},
        {"t": "2026-08-28T13:33:00+00:00", "o": 100.7, "h": 100.75, "l": 99.7, "c": 100.2, "v": 900},
    ]

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, dict(request.url.params)))
        if request.url.path.endswith('/v2/stocks/snapshots'):
            return httpx.Response(
                200,
                json={
                    "snapshots": {
                        "TEST": {
                            "latestTrade": {"p": 100.7, "t": "2026-08-28T13:34:00+00:00"},
                            "latestQuote": {"bp": 100.69, "ap": 100.71, "t": "2026-08-28T13:34:00+00:00"},
                            "dailyBar": {"o": 99.0, "h": 100.8, "l": 98.9, "vw": 100.0, "v": 2_000_000},
                            "prevDailyBar": {"c": 98.0},
                        }
                    }
                },
            )
        if request.url.path.endswith('/v2/stocks/TEST/bars'):
            return httpx.Response(200, json={"bars": bars})
        return httpx.Response(404)

    return httpx.MockTransport(handler)


def test_plans_rejects_snapshot_and_price_fields():
    with TestClient(api_main.app) as client:
        response = client.post(
            "/plans",
            json={
                "symbol": "TEST",
                "quality_score": 90,
                "quality_as_of": datetime.now(UTC).isoformat(),
                "lane": "CORE_MOMENTUM",
                "catalyst_verified": True,
                "catalyst_strength": 0.9,
                "structure_score": 15,
                "rr_score": 10,
                "last": 100.0,
                "snapshot": {"symbol": "TEST", "last": 100.0},
            },
        )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_alpaca_bars_uses_symbol_endpoint_one_minute_and_historical_feed():
    seen = []
    cfg = Settings(
        data_plan="free",
        alpaca_api_key="key",
        alpaca_api_secret="secret",
        discovery_feed="iex",
    )
    async with httpx.AsyncClient(transport=market_transport(seen)) as client:
        provider = AlpacaMarketData(cfg, client)
        rows = await provider.bars(
            "TEST",
            "1Min",
            datetime(2026, 8, 28, 13, 30, tzinfo=UTC),
            datetime(2026, 8, 28, 13, 34, tzinfo=UTC),
        )
    assert len(rows) == 4
    path, params = seen[-1]
    assert path.endswith('/v2/stocks/TEST/bars')
    assert params["timeframe"] == "1Min"
    assert params["feed"] == "sip"


@pytest.mark.asyncio
async def test_build_plan_from_market_fetches_snapshot_and_derives_structure():
    seen = []
    cfg = Settings(
        data_plan="free",
        alpaca_api_key="key",
        alpaca_api_secret="secret",
        discovery_feed="iex",
    )
    async with httpx.AsyncClient(transport=market_transport(seen)) as client:
        provider = AlpacaMarketData(cfg, client)
        original_snapshot = provider.snapshot

        async def enriched_snapshot(symbol: str, decision: bool = False):
            snapshot = await original_snapshot(symbol, decision=decision)
            snapshot.avg_volume = 1_000_000
            snapshot.benchmark_return_pct = 0.0
            return snapshot

        provider.snapshot = enriched_snapshot
        store = InMemoryEventStore()
        service = DecisionService(store, cfg=cfg, market_data=provider)
        quality = classify_quality("TEST", 90, datetime.now(UTC))
        plan, evidence = await service.build_plan_from_market(
            symbol="TEST",
            quality=quality,
            lane=StrategyLane.CORE_MOMENTUM,
            catalyst_verified=True,
            catalyst_strength=0.9,
            structure_score=15,
            rr_score=10,
            now=datetime(2026, 8, 28, 13, 34, 30, tzinfo=UTC),
        )
    assert plan.symbol == "TEST"
    assert plan.entry_trigger == 100.8
    assert plan.stop == 99.7
    assert evidence["features"]["catalyst_strength"] == 0.9
    paths = [row[0] for row in seen]
    assert any(path.endswith('/v2/stocks/snapshots') for path in paths)
    assert any(path.endswith('/v2/stocks/TEST/bars') for path in paths)


@pytest.mark.asyncio
async def test_opening_structure_requires_contiguous_bars_and_retest_bar():
    session_start = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
    closed_before = datetime(2026, 8, 28, 13, 34, tzinfo=UTC)
    with pytest.raises(ValueError, match="OPENING_BARS_NOT_CONTIGUOUS"):
        DecisionService._opening_structure(
            [
                {"t": "2026-08-28T13:31:00+00:00", "h": 100, "l": 99},
                {"t": "2026-08-28T13:32:00+00:00", "h": 101, "l": 99.5},
            ],
            session_start,
            closed_before,
        )
    with pytest.raises(ValueError, match="STRUCTURE_DATA_MISSING"):
        DecisionService._opening_structure(
            [{"t": "2026-08-28T13:30:00+00:00", "h": 100, "l": 99}],
            session_start,
            closed_before,
        )

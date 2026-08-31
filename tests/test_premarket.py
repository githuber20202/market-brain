from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from market_brain.domain.models import LiquidityProfile, MarketSnapshot
from market_brain.engines.premarket import assess_catalyst, score_premarket_candidate
from market_brain.ledger.events import LedgerEvent
from market_brain.ledger.store import InMemoryEventStore
from market_brain.providers.base import SnapshotBatch
from market_brain.providers.rate_limit import TokenBucketRateLimiter
from market_brain.providers.yahoo import YahooMarketData
from market_brain.runtime.premarket import PremarketFunnel
from market_brain.runtime.premarket_learning import PremarketLearningReviewer
from market_brain.settings import Settings
from scripts.batch_gate import premarket_checkpoint

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"
T30 = datetime(2026, 8, 28, 13, 0, tzinfo=UTC)


def _fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def _cfg(**overrides) -> Settings:
    return Settings(
        data_plan="keyless_delayed",
        keyless_request_interval_seconds=0.5,
        **overrides,
    )


@pytest.mark.asyncio
async def test_yahoo_premarket_snapshot_uses_current_premarket_and_skips_cboe():
    calls: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json=_fixture("yahoo_chart_premarket.json"))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = YahooMarketData(
            _cfg(),
            client,
            limiter=TokenBucketRateLimiter(1000),
            now=lambda: T30,
        )
        snapshot = await provider.premarket_snapshot("TEST")

    assert snapshot.last == 102.0
    assert snapshot.prior_close == 98.0
    assert snapshot.volume == 970_000
    assert snapshot.vwap is None
    assert snapshot.source_id == "YAHOO_PREMARKET_DELAYED"
    assert snapshot.delay_minutes == pytest.approx(1.0)
    assert snapshot.metadata["market_phase"] == "PREMARKET"
    assert snapshot.metadata["premarket_bars_count"] == 4
    assert snapshot.metadata["vwap_state"] == "MISSING"
    assert len(calls) == 1
    assert calls[0].url.host == "query2.finance.yahoo.com"
    assert calls[0].url.params["includePrePost"] == "true"


def test_catalyst_requires_direct_trusted_classified_headline():
    news = [
        {
            "title": "Company raises guidance after strong demand",
            "publisher": "Business Wire",
            "published_at": "2026-08-28T12:30:00+00:00",
            "url": "https://example.test/news",
            "source_id": "YAHOO_NEWS_SEARCH",
            "direct_symbol_match": True,
        }
    ]

    assessment = assess_catalyst(news, as_of=T30)

    assert assessment.verified is True
    assert assessment.negative is False
    assert assessment.category == "EARNINGS_GUIDANCE"
    assert assessment.score == 20.0


def test_premarket_deterioration_blocks_finalist_but_remains_watch():
    snapshot = _snapshot(
        "AAPL",
        last=101.0,
        prior_close=100.0,
        premarket_high=104.0,
        return_15m=-1.2,
        lower_highs=2,
    )
    catalyst = assess_catalyst(
        [
            {
                "title": "Company raises guidance after strong demand",
                "publisher": "Reuters",
                "published_at": "2026-08-28T12:30:00+00:00",
                "source_id": "YAHOO_NEWS_SEARCH",
                "direct_symbol_match": True,
            }
        ],
        as_of=T30,
    )

    result = score_premarket_candidate(
        snapshot,
        adv20=10_000_000,
        benchmark_return_pct=0.2,
        sector_return_pct=0.1,
        catalyst=catalyst,
        minimum_price=5.0,
        minimum_adv=5_000_000,
        finalist_score=65.0,
    )

    assert result["premarket_deterioration"]["confirmed"] is True
    assert result["premarket_deterioration"]["severe"] is True
    assert result["finalist_eligible"] is False
    assert result["status"] == "WATCH"
    assert "PREMARKET_DETERIORATION" in result["reason_codes"]


class FakePremarketProvider:
    def __init__(self) -> None:
        self.deteriorated = False

    async def external_movers(self):
        return []

    async def premarket_snapshots(self, symbols):
        rows = []
        for symbol in symbols:
            if symbol == "AAPL" and self.deteriorated:
                rows.append(
                    _snapshot(
                        symbol,
                        last=101.0,
                        prior_close=100.0,
                        premarket_high=104.0,
                        return_15m=-1.2,
                        lower_highs=2,
                    )
                )
            elif symbol == "SPY":
                rows.append(_snapshot(symbol, last=100.2, prior_close=100.0))
            else:
                rows.append(_snapshot(symbol))
        return SnapshotBatch(tuple(rows))

    async def news(self, symbol):
        if symbol != "AAPL":
            return []
        return [
            {
                "title": "Company raises guidance after strong demand",
                "publisher": "Business Wire",
                "published_at": "2026-08-28T12:30:00+00:00",
                "url": "https://example.test/news",
                "source_id": "YAHOO_NEWS_SEARCH",
                "direct_symbol_match": True,
            }
        ]


class FakeLiquidityService:
    def __init__(self, store):
        self.store = store

    async def refresh_liquidity_profiles_for_symbols(self, symbols, *, now):
        for symbol in symbols:
            await self.store.save_liquidity_profile(
                LiquidityProfile(symbol, 10_000_000, 100.0, now, refreshed_at=now)
            )
        return {"session_date": now.date().isoformat(), "refreshed": len(symbols), "failed": []}


@pytest.mark.asyncio
async def test_premarket_checkpoints_refresh_delta_deterioration_and_artifacts(tmp_path):
    universe_dir, calendar_path = _runtime_files(tmp_path)
    store = InMemoryEventStore()
    provider = FakePremarketProvider()
    funnel = PremarketFunnel(
        store=store,
        service=FakeLiquidityService(store),
        provider=provider,
        universe_dir=universe_dir,
        calendar_path=calendar_path,
        cfg=_cfg(),
        state_dir=tmp_path / "state",
    )

    t30 = await funnel.run("T-30", now=T30)
    provider.deteriorated = True
    t12 = await funnel.run("T-12", now=datetime(2026, 8, 28, 13, 18, tzinfo=UTC))

    assert t30["status"] == "COMPLETED"
    assert t30["coverage"]["audit_rows"] == 4
    assert t30["coverage"]["required"] == 4
    assert t30["finalists"][0] == "AAPL"
    assert len(t30["finalists"]) <= 2
    assert t12["status"] == "COMPLETED"
    assert t12["delta_state"] == "AVAILABLE"
    assert "AAPL" not in t12["finalists"]
    artifact = await store.get_runtime_status_key("premarket_artifact:2026-08-28:T-12")
    assert artifact["audit"][0]["symbol"] == "AAPL"
    assert artifact["audit"][0]["premarket_deterioration"]["confirmed"] is True
    assert artifact["numeric_execution_allowed"] is False
    assert artifact["ready_allowed"] is False
    assert len(await store.list_alerts()) == 2
    for path in (Path(t30["artifact_dir"]), Path(t12["artifact_dir"])):
        assert (path / "report.json").is_file()
        assert (path / "audit.jsonl").is_file()
        assert (path / "funnel.json").is_file()


def test_premarket_gate_tracks_new_york_dst_and_rejects_duplicates(tmp_path):
    _universe_dir, calendar_path = _runtime_files(tmp_path)

    assert premarket_checkpoint(
        datetime(2026, 8, 31, 13, 0, tzinfo=UTC), calendar_path
    ) == "T-30"
    assert premarket_checkpoint(
        datetime(2026, 8, 31, 13, 18, tzinfo=UTC), calendar_path
    ) == "T-12"
    assert premarket_checkpoint(
        datetime(2026, 8, 31, 13, 27, tzinfo=UTC), calendar_path
    ) == "T-3"
    assert premarket_checkpoint(
        datetime(2026, 8, 31, 14, 0, tzinfo=UTC), calendar_path
    ) is None
    assert premarket_checkpoint(
        datetime(2027, 1, 4, 14, 18, tzinfo=UTC), calendar_path
    ) == "T-12"


def test_production_universe_keeps_61_required_rows_and_mandatory_symbols():
    from market_brain.orchestration.universe import load_universe

    rows = load_universe(ROOT / "data" / "universe")
    required = {row.symbol for row in rows if row.audit_required}

    assert len(required) == 61
    assert {"MRNA", "MRVL"}.issubset(required)


class FakeLearningProvider:
    async def learning_bars(self, symbol):
        assert symbol == "AAPL"
        rows = []
        for minute in range(420):
            timestamp = T30 + timedelta(minutes=minute)
            close = 100.0 + minute / 100.0
            rows.append(
                {
                    "t": timestamp.isoformat(),
                    "h": close + 0.5,
                    "l": close - 0.5,
                    "c": close,
                }
            )
        return rows


@pytest.mark.asyncio
async def test_premarket_learning_review_measures_each_checkpoint_and_is_idempotent(
    tmp_path,
):
    _universe_dir, calendar_path = _runtime_files(tmp_path)
    store = InMemoryEventStore()
    for checkpoint, as_of in (
        ("T-30", T30),
        ("T-12", T30 + timedelta(minutes=18)),
        ("T-3", T30 + timedelta(minutes=27)),
    ):
        await store.append(
            LedgerEvent(
                "PREMARKET_RUN",
                f"premarket:2026-08-28:{checkpoint}",
                {
                    "checkpoint": checkpoint,
                    "as_of": as_of.isoformat(),
                    "top10_rows": [
                        {
                            "symbol": "AAPL",
                            "score": 72.0,
                            "status": "PREDICTION/WATCH",
                            "finalist_eligible": True,
                            "reason_codes": [],
                            "metrics": {"price": 100.0},
                        }
                    ],
                },
                occurred_at=as_of,
            )
        )
    reviewer = PremarketLearningReviewer(
        store=store,
        provider=FakeLearningProvider(),
        calendar_path=calendar_path,
        state_dir=tmp_path / "state",
    )

    result = await reviewer.review(
        now=datetime(2026, 8, 28, 20, 20, tzinfo=UTC)
    )
    duplicate = await reviewer.review(
        now=datetime(2026, 8, 28, 20, 21, tzinfo=UTC)
    )

    assert result["status"] == "COMPLETED"
    assert result["data_state"] == "COMPLETE"
    assert result["records"] == 3
    assert result["complete_records"] == 3
    assert duplicate["status"] == "ALREADY_COMPLETED"
    artifact = json.loads(Path(result["artifact_path"]).read_text())
    assert artifact["records"][0]["forward_returns_percent"]["5"] == 0.05
    assert artifact["records"][0]["mfe_percent"] == 4.69
    assert artifact["records"][0]["mae_percent"] == -0.5
    assert artifact["records"][0]["eod_return_percent"] == 4.19
    assert artifact["weights_change_permitted"] is False


def _snapshot(
    symbol: str,
    *,
    last: float = 104.0,
    prior_close: float = 100.0,
    premarket_high: float | None = None,
    return_15m: float = 1.0,
    lower_highs: int = 0,
) -> MarketSnapshot:
    high = premarket_high or last
    return MarketSnapshot(
        symbol=symbol,
        last=last,
        prior_close=prior_close,
        volume=1_000_000,
        vwap=last - 0.5,
        open_price=prior_close + 0.5,
        high=high,
        low=prior_close,
        data_age_seconds=60.0,
        source_id="YAHOO_PREMARKET_DELAYED",
        delay_minutes=1.0,
        fetched_at=T30,
        authoritative=True,
        metadata={
            "quote_timestamp": (T30 - timedelta(minutes=1)).isoformat(),
            "premarket_high": high,
            "premarket_return_15m_percent": return_15m,
            "premarket_lower_highs_count": lower_highs,
        },
    )


def _runtime_files(tmp_path: Path) -> tuple[Path, Path]:
    universe_dir = tmp_path / "universe"
    universe_dir.mkdir()
    (universe_dir / "universe.csv").write_text(
        "ticker,instrument_type,name,exchange,sector_proxy,audit_required,ranking_eligible\n"
        "AAPL,EQUITY,Apple,Nasdaq,SPY,true,true\n"
        "MRNA,EQUITY,Moderna,Nasdaq,SPY,true,true\n"
        "MRVL,EQUITY,Marvell,Nasdaq,SPY,true,true\n"
        "SPY,ETF,SPDR,NYSE Arca,SPY,true,true\n"
    )
    calendar_path = tmp_path / "market_calendar.csv"
    calendar_path.write_text(
        "date,status,open_time,close_time,source\n"
        "2026-09-07,CLOSED,,,NYSE\n"
        "2027-01-01,CLOSED,,,NYSE\n"
    )
    return universe_dir, calendar_path

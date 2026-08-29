from __future__ import annotations

import csv
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from market_brain.engines.quality_scorer import score_yahoo_fundamentals
from market_brain.ledger.store import InMemoryEventStore
from market_brain.orchestration.universe import load_manual_quality, load_universe
from market_brain.providers.keyless_http import USER_AGENT
from market_brain.providers.rate_limit import TokenBucketRateLimiter
from market_brain.providers.yahoo_fundamentals import (
    YAHOO_FUNDAMENTAL_TYPES,
    YahooFundamentals,
)
from market_brain.runtime.state import activate_quality_from_state
from market_brain.settings import Settings
from scripts.quality_refresh import refresh_quality

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"
NOW = datetime(2026, 8, 29, 18, 0, tzinfo=UTC)


def _fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


@pytest.mark.asyncio
async def test_yahoo_fundamentals_provider_parses_provenance_and_caches() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=_fixture("yahoo_fundamentals_full.json"))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = YahooFundamentals(
            client,
            limiter=TokenBucketRateLimiter(1_000_000),
            now=lambda: NOW,
        )
        first = await provider.fundamentals("full")
        second = await provider.fundamentals("FULL")

    assert first is second
    assert first.source_id == "YAHOO_FUNDAMENTALS"
    assert first.fetched_at == NOW
    assert len(first.series["annualTotalRevenue"]) == 3
    assert len(requests) == 1
    assert requests[0].headers["User-Agent"] == USER_AGENT
    assert requests[0].url.params["merge"] == "false"
    assert set(requests[0].url.params["type"].split(",")) == set(
        YAHOO_FUNDAMENTAL_TYPES
    )


def test_yahoo_quality_uses_shared_rubric_and_is_deterministic() -> None:
    full = YahooFundamentals(now=lambda: NOW)
    partial = YahooFundamentals(now=lambda: NOW)
    full_snapshot = _snapshot_from_fixture(full, "FULL", "yahoo_fundamentals_full.json")
    partial_snapshot = _snapshot_from_fixture(
        partial,
        "PART",
        "yahoo_fundamentals_partial.json",
    )

    first = score_yahoo_fundamentals(full_snapshot, as_of=NOW)
    second = score_yahoo_fundamentals(full_snapshot, as_of=NOW)
    incomplete = score_yahoo_fundamentals(partial_snapshot, as_of=NOW)

    assert first == second
    assert first.quality_score == 85
    assert first.source == "YAHOO_FUNDAMENTALS"
    assert first.partial is False
    assert first.metrics["revenue_growth_yoy"].points == 25
    assert first.metrics["operating_margin"].points == 25
    assert first.metrics["leverage"].points == 20
    assert first.metrics["fcf_margin"].points == 25
    assert first.dilution_penalty == 10
    assert incomplete.quality_score == 10
    assert incomplete.partial is True
    assert set(incomplete.missing_metrics) == {
        "operating_margin",
        "leverage",
        "fcf_margin",
        "dilution_yoy",
    }


@pytest.mark.asyncio
async def test_yahoo_quality_refresh_writes_provenance_and_partial(tmp_path: Path) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        fixture = (
            "yahoo_fundamentals_full.json"
            if request.url.path.endswith("/FULL")
            else "yahoo_fundamentals_partial.json"
        )
        return httpx.Response(200, json=_fixture(fixture))

    output = tmp_path / "quality.csv"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = YahooFundamentals(
            client,
            limiter=TokenBucketRateLimiter(1_000_000),
            now=lambda: NOW,
        )
        summary = await refresh_quality(
            ["FULL", "PART"],
            output_path=output,
            now=NOW,
            quality_source="yahoo",
            provider=provider,
            skipped_instruments=[{"symbol": "SPY", "instrument_type": "ETF"}],
        )

    with output.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert summary["source"] == "YAHOO_FUNDAMENTALS"
    assert summary["rows"] == 2
    assert summary["missing"] == []
    assert summary["skipped_instruments"] == [
        {"symbol": "SPY", "instrument_type": "ETF"}
    ]
    assert {row["source"] for row in rows} == {"YAHOO_FUNDAMENTALS"}
    assert rows[1]["partial"] == "true"


@pytest.mark.asyncio
async def test_yahoo_quality_state_is_accepted_and_still_expires(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    state = repo / "state"
    state.mkdir(parents=True)
    target = repo / "data" / "quality.csv"
    store = InMemoryEventStore()
    content = (
        "symbol,quality_score,as_of,source,partial\n"
        f"FULL,85,{NOW.isoformat()},YAHOO_FUNDAMENTALS,false\n"
    )
    (state / "quality.csv").write_text(content)

    ready = await activate_quality_from_state(repo, target, store, now=NOW)
    assert ready == {
        "status": "READY",
        "source": "YAHOO_FUNDAMENTALS",
        "checked_at": NOW.isoformat(),
        "as_of": NOW.isoformat(),
        "rows": 1,
    }
    target.unlink()
    stale = await activate_quality_from_state(
        repo,
        target,
        store,
        now=NOW + timedelta(days=15),
    )
    assert stale["status"] == "QUALITY_STALE"
    assert not target.exists()


def test_universe_marks_etfs_and_quality_loader_accepts_yahoo(tmp_path: Path) -> None:
    universe_dir = tmp_path / "universe"
    universe_dir.mkdir()
    (universe_dir / "symbols.csv").write_text(
        "symbol,instrument_type,ranking_eligible\n"
        "NVDA,EQUITY,true\n"
        "SPY,ETF,true\n"
    )
    entries = load_universe(universe_dir)
    assert [(entry.symbol, entry.instrument_type) for entry in entries] == [
        ("NVDA", "EQUITY"),
        ("SPY", "ETF"),
    ]
    quality = tmp_path / "quality.csv"
    quality.write_text(
        "symbol,quality_score,as_of,source,partial\n"
        f"NVDA,85,{NOW.isoformat()},YAHOO_FUNDAMENTALS,false\n"
    )
    assert load_manual_quality(quality)["NVDA"].source == "YAHOO_FUNDAMENTALS"
    assert Settings().quality_source == "yahoo"


def test_canonical_universe_quality_population_excludes_non_companies() -> None:
    entries = load_universe(ROOT / "data" / "universe")
    assert sum(entry.instrument_type == "EQUITY" for entry in entries) == 51
    assert sum(entry.instrument_type == "ETF" for entry in entries) == 9
    assert sum(entry.instrument_type == "UNRESOLVED" for entry in entries) == 1


def _snapshot_from_fixture(
    provider: YahooFundamentals,
    symbol: str,
    fixture: str,
):
    from market_brain.providers.yahoo_fundamentals import (
        YahooFundamentalsSnapshot,
        _parse_series,
    )

    payload = _fixture(fixture)
    return YahooFundamentalsSnapshot(
        symbol,
        _parse_series(payload["timeseries"]["result"]),
        provider.now(),
    )

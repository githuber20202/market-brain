from __future__ import annotations

import csv
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from market_brain.engines.quality_scorer import (
    _higher_is_better,
    _lower_is_better,
    _quarterly_series,
    score_companyfacts,
)
from market_brain.ledger.store import InMemoryEventStore
from market_brain.providers.edgar import EDGAR_USER_AGENT, EdgarCompanyFacts
from market_brain.providers.rate_limit import TokenBucketRateLimiter
from market_brain.runtime.state import activate_quality_from_state
from scripts.quality_refresh import refresh_quality

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"
NOW = datetime(2026, 8, 29, 18, 0, tzinfo=UTC)


def _fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


@pytest.mark.asyncio
async def test_edgar_provider_uses_policy_user_agent_rate_limit_and_cache() -> None:
    assert EDGAR_USER_AGENT == (
        "Market Brain shadow radar githuber20202@users.noreply.github.com"
    )
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("company_tickers.json"):
            return httpx.Response(200, json=_fixture("edgar_company_tickers.json"))
        filename = (
            "edgar_companyfacts_full.json"
            if request.url.path.endswith("CIK0000000001.json")
            else "edgar_companyfacts_partial.json"
        )
        return httpx.Response(200, json=_fixture(filename))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = EdgarCompanyFacts(
            client,
            limiter=TokenBucketRateLimiter(1_000_000),
        )
        first = await provider.companyfacts("full")
        assert await provider.companyfacts("FULL") is first
        await provider.companyfacts("PART")

    assert len(requests) == 3
    assert all(request.headers["User-Agent"] == EDGAR_USER_AGENT for request in requests)
    default_provider = EdgarCompanyFacts()
    assert default_provider.http.limiter.refill_per_second <= 10.0
    await default_provider.aclose()


@pytest.mark.asyncio
async def test_edgar_provider_retries_retryable_status_with_backoff() -> None:
    statuses = [503, 200]
    sleeps: list[float] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("company_tickers.json"):
            status = statuses.pop(0)
            return httpx.Response(
                status,
                json=_fixture("edgar_company_tickers.json") if status == 200 else {},
            )
        raise AssertionError("facts must not be requested")

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = EdgarCompanyFacts(
            client,
            limiter=TokenBucketRateLimiter(1_000_000),
            sleep=fake_sleep,
        )
        mapping = await provider.ticker_map()

    assert mapping["FULL"] == "0000000001"
    assert sleeps == [1.0]


def test_quality_score_is_deterministic_for_full_and_partial_facts() -> None:
    full_payload = _fixture("edgar_companyfacts_full.json")
    first = score_companyfacts("FULL", full_payload, as_of=NOW)
    second = score_companyfacts("FULL", full_payload, as_of=NOW)
    partial = score_companyfacts(
        "PART",
        _fixture("edgar_companyfacts_partial.json"),
        as_of=NOW,
    )

    assert first == second
    assert first.quality_score == 85
    assert first.partial is False
    assert first.metrics["revenue_growth_yoy"].points == 25
    assert first.metrics["operating_margin"].points == 25
    assert first.metrics["leverage"].points == 20
    assert first.metrics["fcf_margin"].points == 25
    assert first.dilution_penalty == 10
    assert partial.quality_score == 15
    assert partial.partial is True
    assert set(partial.missing_metrics) == {
        "operating_margin",
        "leverage",
        "fcf_margin",
        "dilution_yoy",
    }


def test_quarterly_series_derives_quarters_from_ytd_and_annual_facts() -> None:
    rows = [
        ("2025-03-31", 10, "10-Q"),
        ("2025-06-30", 25, "10-Q"),
        ("2025-09-30", 45, "10-Q"),
        ("2025-12-31", 70, "10-K"),
    ]
    facts = {
        "NetCashProvidedByUsedInOperatingActivities": {
            "units": {
                "USD": [
                    {
                        "start": "2025-01-01",
                        "end": end,
                        "val": value,
                        "filed": "2026-02-01",
                        "form": form,
                        "accn": end,
                    }
                    for end, value, form in rows
                ]
            }
        }
    }

    series = _quarterly_series(
        facts,
        ("NetCashProvidedByUsedInOperatingActivities",),
        "USD",
    )

    assert list(series.values()) == [10.0, 15.0, 20.0, 25.0]


@pytest.mark.parametrize(
    ("value", "expected"),
    [(0.20, 25), (0.10, 20), (0.05, 15), (0.0, 10), (-0.10, 5), (-0.11, 0)],
)
def test_higher_is_better_thresholds(value: float, expected: int) -> None:
    assert _higher_is_better(value, (0.20, 0.10, 0.05, 0.0, -0.10)) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [(0.0, 25), (1.0, 20), (2.0, 15), (3.0, 10), (4.0, 5), (4.01, 0)],
)
def test_lower_is_better_thresholds(value: float, expected: int) -> None:
    assert _lower_is_better(value, (0.0, 1.0, 2.0, 3.0, 4.0)) == expected


@pytest.mark.asyncio
async def test_quality_refresh_writes_edgar_csv_and_reports_missing(tmp_path: Path) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("company_tickers.json"):
            return httpx.Response(200, json=_fixture("edgar_company_tickers.json"))
        if request.url.path.endswith("CIK0000000001.json"):
            return httpx.Response(200, json=_fixture("edgar_companyfacts_full.json"))
        return httpx.Response(200, json=_fixture("edgar_companyfacts_partial.json"))

    output = tmp_path / "quality.csv"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = EdgarCompanyFacts(
            client,
            limiter=TokenBucketRateLimiter(1_000_000),
        )
        summary = await refresh_quality(
            ["FULL", "PART", "MISSING"],
            output_path=output,
            now=NOW,
            provider=provider,
        )

    with output.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert summary["rows"] == 2
    assert summary["partial"] == 1
    assert summary["missing"] == [
        {"symbol": "MISSING", "error_type": "EDGAR_CIK_NOT_FOUND"}
    ]
    assert rows[0] == {
        "symbol": "FULL",
        "quality_score": "85",
        "as_of": NOW.isoformat(),
        "source": "EDGAR_AUTO",
        "partial": "false",
    }
    assert rows[1]["partial"] == "true"


@pytest.mark.asyncio
async def test_radar_quality_state_copy_accepts_fresh_and_rejects_stale(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    state = repo / "state"
    state.mkdir(parents=True)
    target = repo / "data" / "quality.csv"
    store = InMemoryEventStore()
    content = (
        "symbol,quality_score,as_of,source,partial\n"
        f"FULL,85,{NOW.isoformat()},EDGAR_AUTO,false\n"
    )
    (state / "quality.csv").write_text(content)

    ready = await activate_quality_from_state(repo, target, store, now=NOW)
    assert ready["status"] == "READY"
    assert target.read_text() == content

    target.unlink()
    stale_now = NOW + timedelta(days=15)
    stale = await activate_quality_from_state(repo, target, store, now=stale_now)
    assert stale["status"] == "QUALITY_STALE"
    assert not target.exists()


def test_shadow_weekly_workflow_and_radar_quality_restore_are_configured() -> None:
    weekly = (ROOT / ".github/workflows/shadow-weekly.yml").read_text()
    radar = (ROOT / ".github/workflows/shadow-radar.yml").read_text()
    assert 'cron: "30 21 * * 5"' in weekly
    assert "workflow_dispatch:" in weekly
    assert "group: market-brain-shadow-state" in weekly
    assert "permissions:\n  contents: write\n  issues: write" in weekly
    assert "python -m market_brain.runtime.batch --mode weekly" in weekly
    assert "QUALITY_SOURCE: yahoo" in weekly
    assert "market_brain.runtime.state persist" in weekly
    assert "market_brain.runtime.state activate-quality" in radar

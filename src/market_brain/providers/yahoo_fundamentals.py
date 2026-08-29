from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx

from market_brain.providers.base import DataUnavailable
from market_brain.providers.keyless_http import KeylessJsonClient
from market_brain.providers.rate_limit import TokenBucketRateLimiter

YAHOO_FUNDAMENTALS_SOURCE_ID = "YAHOO_FUNDAMENTALS"
YAHOO_FUNDAMENTALS_BASE_URL = (
    "https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries"
)
YAHOO_FUNDAMENTAL_TYPES = (
    "annualTotalRevenue",
    "quarterlyTotalRevenue",
    "annualOperatingIncome",
    "quarterlyOperatingIncome",
    "annualTotalDebt",
    "annualCashAndCashEquivalents",
    "annualFreeCashFlow",
    "quarterlyFreeCashFlow",
    "annualDilutedAverageShares",
    "quarterlyDilutedAverageShares",
)


@dataclass(frozen=True, slots=True)
class FundamentalPoint:
    as_of: date
    value: float


@dataclass(frozen=True, slots=True)
class YahooFundamentalsSnapshot:
    symbol: str
    series: dict[str, tuple[FundamentalPoint, ...]]
    fetched_at: datetime
    source_id: str = YAHOO_FUNDAMENTALS_SOURCE_ID


class YahooFundamentals:
    def __init__(
        self,
        client: httpx.AsyncClient | None = None,
        *,
        limiter: TokenBucketRateLimiter | None = None,
        retry_attempts: int = 3,
        now: Callable[[], datetime] | None = None,
        sleep: Callable[[float], Any] = asyncio.sleep,
    ) -> None:
        self.now = now or (lambda: datetime.now(UTC))
        self.http = KeylessJsonClient(
            source_id=YAHOO_FUNDAMENTALS_SOURCE_ID,
            client=client,
            limiter=limiter or TokenBucketRateLimiter(120, burst_capacity=1),
            retry_attempts=retry_attempts,
            sleep=sleep,
        )
        self._cache: dict[str, YahooFundamentalsSnapshot] = {}

    async def fundamentals(self, symbol: str) -> YahooFundamentalsSnapshot:
        normalized = symbol.upper().strip()
        cached = self._cache.get(normalized)
        if cached is not None:
            return cached
        fetched_at = _aware(self.now())
        payload = await self.http.get_json(
            f"{YAHOO_FUNDAMENTALS_BASE_URL}/{normalized}",
            resource="fundamentals_timeseries",
            symbol=normalized,
            params={
                "type": ",".join(YAHOO_FUNDAMENTAL_TYPES),
                "period1": int((fetched_at - timedelta(days=3 * 366)).timestamp()),
                "period2": int(fetched_at.timestamp()),
                "merge": "false",
            },
        )
        rows = payload.get("timeseries", {}).get("result")
        if not isinstance(rows, list):
            raise DataUnavailable(
                source_id=YAHOO_FUNDAMENTALS_SOURCE_ID,
                resource="fundamentals_timeseries",
                symbol=normalized,
                error_type="YAHOO_FUNDAMENTALS_INVALID",
            )
        parsed = _parse_series(rows)
        if not any(parsed.values()):
            raise DataUnavailable(
                source_id=YAHOO_FUNDAMENTALS_SOURCE_ID,
                resource="fundamentals_timeseries",
                symbol=normalized,
                error_type="YAHOO_FUNDAMENTALS_EMPTY",
            )
        snapshot = YahooFundamentalsSnapshot(normalized, parsed, fetched_at)
        self._cache[normalized] = snapshot
        return snapshot

    async def aclose(self) -> None:
        await self.http.aclose()


def _parse_series(rows: list[Any]) -> dict[str, tuple[FundamentalPoint, ...]]:
    collected: dict[str, dict[date, float]] = {
        metric: {} for metric in YAHOO_FUNDAMENTAL_TYPES
    }
    for row in rows:
        if not isinstance(row, dict):
            continue
        meta = row.get("meta")
        raw_types = (
            meta.get("type") if isinstance(meta, dict) else None
        ) or row.get("type") or []
        metrics = raw_types if isinstance(raw_types, list) else [raw_types]
        for metric in metrics:
            if metric not in collected:
                continue
            points = row.get(metric)
            if not isinstance(points, list):
                continue
            for point in points:
                parsed = _point(point)
                if parsed is not None:
                    collected[metric][parsed.as_of] = parsed.value
    return {
        metric: tuple(FundamentalPoint(day, value) for day, value in sorted(values.items()))
        for metric, values in collected.items()
    }


def _point(value: Any) -> FundamentalPoint | None:
    if not isinstance(value, dict):
        return None
    try:
        as_of = date.fromisoformat(str(value["asOfDate"]))
        reported = value.get("reportedValue")
        raw = reported.get("raw") if isinstance(reported, dict) else value.get("raw")
        number = float(raw)
    except (KeyError, TypeError, ValueError):
        return None
    return FundamentalPoint(as_of, number)


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import httpx

from market_brain.providers.base import DataUnavailable
from market_brain.providers.keyless_http import KeylessJsonClient
from market_brain.providers.rate_limit import TokenBucketRateLimiter

EDGAR_SOURCE_ID = "SEC_EDGAR"
EDGAR_USER_AGENT = "market-brain/1.0 (github.com/githuber20202/market-brain)"
EDGAR_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
EDGAR_FACTS_BASE_URL = "https://data.sec.gov/api/xbrl/companyfacts"
EDGAR_REQUEST_INTERVAL_SECONDS = 0.15
EDGAR_CALLS_PER_MINUTE = int(60 / EDGAR_REQUEST_INTERVAL_SECONDS)


class EdgarCompanyFacts:
    def __init__(
        self,
        client: httpx.AsyncClient | None = None,
        *,
        limiter: TokenBucketRateLimiter | None = None,
        retry_attempts: int = 3,
        sleep: Callable[[float], Any] = asyncio.sleep,
    ) -> None:
        self.http = KeylessJsonClient(
            source_id=EDGAR_SOURCE_ID,
            client=client,
            limiter=limiter
            or TokenBucketRateLimiter(EDGAR_CALLS_PER_MINUTE, burst_capacity=1),
            retry_attempts=retry_attempts,
            user_agent=EDGAR_USER_AGENT,
            sleep=sleep,
        )
        self._ticker_cache: dict[str, str] | None = None
        self._facts_cache: dict[str, dict] = {}

    async def ticker_map(self) -> dict[str, str]:
        if self._ticker_cache is not None:
            return dict(self._ticker_cache)
        payload = await self.http.get_json(
            EDGAR_TICKERS_URL,
            resource="company_tickers",
            symbol="ALL",
        )
        mapping: dict[str, str] = {}
        for row in payload.values():
            if not isinstance(row, dict):
                continue
            ticker = str(row.get("ticker", "")).upper().strip()
            try:
                cik = f"{int(row['cik_str']):010d}"
            except (KeyError, TypeError, ValueError):
                continue
            if ticker:
                mapping[ticker] = cik
        if not mapping:
            raise DataUnavailable(
                source_id=EDGAR_SOURCE_ID,
                resource="company_tickers",
                symbol="ALL",
                error_type="EDGAR_TICKER_MAP_EMPTY",
            )
        self._ticker_cache = mapping
        return dict(mapping)

    async def companyfacts(self, symbol: str) -> dict:
        normalized = symbol.upper().strip()
        cached = self._facts_cache.get(normalized)
        if cached is not None:
            return cached
        cik = (await self.ticker_map()).get(normalized)
        if cik is None:
            raise DataUnavailable(
                source_id=EDGAR_SOURCE_ID,
                resource="companyfacts",
                symbol=normalized,
                error_type="EDGAR_CIK_NOT_FOUND",
            )
        payload = await self.http.get_json(
            f"{EDGAR_FACTS_BASE_URL}/CIK{cik}.json",
            resource="companyfacts",
            symbol=normalized,
        )
        facts = payload.get("facts")
        if not isinstance(facts, dict) or not isinstance(facts.get("us-gaap"), dict):
            raise DataUnavailable(
                source_id=EDGAR_SOURCE_ID,
                resource="companyfacts",
                symbol=normalized,
                error_type="EDGAR_COMPANYFACTS_INVALID",
            )
        self._facts_cache[normalized] = payload
        return payload

    async def aclose(self) -> None:
        await self.http.aclose()


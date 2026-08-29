from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import httpx

from market_brain.providers.base import DataUnavailable
from market_brain.providers.rate_limit import TokenBucketRateLimiter

USER_AGENT = "Market-Brain/1.0 (+https://github.com/githuber20202/market-brain)"
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class KeylessJsonClient:
    def __init__(
        self,
        *,
        source_id: str,
        client: httpx.AsyncClient | None,
        limiter: TokenBucketRateLimiter,
        retry_attempts: int,
        user_agent: str = USER_AGENT,
        sleep: Callable[[float], Any] = asyncio.sleep,
    ) -> None:
        self.source_id = source_id
        self.client = client
        self._owned_client: httpx.AsyncClient | None = None
        self.limiter = limiter
        self.retry_attempts = retry_attempts
        self.user_agent = user_agent
        self.sleep = sleep

    async def get_json(
        self,
        url: str,
        *,
        resource: str,
        symbol: str,
        params: dict[str, Any] | None = None,
    ) -> dict:
        last_error = "UNKNOWN"
        for attempt in range(self.retry_attempts):
            await self.limiter.acquire()
            client = self.client
            if client is None:
                if self._owned_client is None:
                    self._owned_client = httpx.AsyncClient(timeout=10.0)
                client = self._owned_client
            try:
                response = await client.get(
                    url,
                    params=params,
                    headers={"User-Agent": self.user_agent, "Accept": "application/json"},
                )
                if response.status_code not in RETRYABLE_STATUS:
                    response.raise_for_status()
                    payload = response.json()
                    if not isinstance(payload, dict):
                        raise TypeError("KEYLESS_RESPONSE_INVALID")
                    return payload
                last_error = f"HTTP_{response.status_code}"
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                last_error = type(exc).__name__
            except (httpx.HTTPStatusError, TypeError, ValueError) as exc:
                raise DataUnavailable(
                    source_id=self.source_id,
                    resource=resource,
                    symbol=symbol,
                    error_type=type(exc).__name__,
                ) from exc
            if attempt + 1 < self.retry_attempts:
                await self.sleep(float(2**attempt))
        raise DataUnavailable(
            source_id=self.source_id,
            resource=resource,
            symbol=symbol,
            error_type=last_error,
        )

    async def aclose(self) -> None:
        if self._owned_client is not None:
            await self._owned_client.aclose()
            self._owned_client = None

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import httpx

from market_brain.providers.base import DataUnavailable
from market_brain.providers.keyless_http import KeylessJsonClient
from market_brain.providers.rate_limit import TokenBucketRateLimiter
from market_brain.settings import Settings, settings

CBOE_SOURCE_ID = "CBOE_DELAYED"
CBOE_BASE_URL = "https://cdn.cboe.com/api/global/delayed_quotes/quotes"
EASTERN = ZoneInfo("America/New_York")


@dataclass(frozen=True, slots=True)
class DelayedQuote:
    symbol: str
    price: float
    bid: float | None
    ask: float | None
    quoted_at: datetime
    fetched_at: datetime
    source_id: str = CBOE_SOURCE_ID

    @property
    def delay_minutes(self) -> float:
        return max(0.0, (self.fetched_at - self.quoted_at).total_seconds() / 60.0)

    def delay_minutes_at(self, now: datetime) -> float:
        return max(0.0, (_aware(now) - self.quoted_at).total_seconds() / 60.0)


class CboeDelayedQuotes:
    def __init__(
        self,
        cfg: Settings = settings,
        client: httpx.AsyncClient | None = None,
        *,
        limiter: TokenBucketRateLimiter,
        now: Callable[[], datetime] | None = None,
        sleep=asyncio.sleep,
    ) -> None:
        self.cfg = cfg
        self.now = now or (lambda: datetime.now(UTC))
        self.http = KeylessJsonClient(
            source_id=CBOE_SOURCE_ID,
            client=client,
            limiter=limiter,
            retry_attempts=cfg.keyless_retry_attempts,
            sleep=sleep,
        )
        self._cache: dict[tuple[str, str], DelayedQuote] = {}
        self._failures: dict[tuple[str, str], str] = {}

    async def quote(self, symbol: str) -> DelayedQuote:
        normalized = symbol.upper().strip()
        key = (_slot_key(self.now()), normalized)
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        if key in self._failures:
            raise DataUnavailable(
                source_id=CBOE_SOURCE_ID,
                resource="quote",
                symbol=normalized,
                error_type=self._failures[key],
            )
        try:
            payload = await self.http.get_json(
                f"{CBOE_BASE_URL}/{normalized}.json",
                resource="quote",
                symbol=normalized,
            )
        except DataUnavailable as exc:
            self._failures[key] = exc.error_type
            raise
        raw = payload.get("data")
        if isinstance(raw, list):
            raw = raw[0] if raw else None
        if not isinstance(raw, dict):
            raise TypeError("CBOE_QUOTE_INVALID")
        fetched_at = _aware(self.now())
        price = _positive_float(raw.get("current_price"))
        quoted_at = _parse_cboe_time(raw.get("last_trade_time"))
        if price is None or quoted_at is None:
            raise ValueError("CBOE_QUOTE_INVALID")
        quote = DelayedQuote(
            symbol=normalized,
            price=price,
            bid=_positive_float(raw.get("bid")),
            ask=_positive_float(raw.get("ask")),
            quoted_at=quoted_at,
            fetched_at=fetched_at,
        )
        self._cache[key] = quote
        return quote

    async def aclose(self) -> None:
        await self.http.aclose()


def _parse_cboe_time(value) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        stamp = datetime.fromisoformat(value)
    except ValueError:
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=EASTERN)
    return stamp.astimezone(UTC)


def _positive_float(value) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _slot_key(value: datetime) -> str:
    stamp = _aware(value)
    minute = stamp.minute - stamp.minute % 5
    return stamp.replace(minute=minute, second=0, microsecond=0).isoformat()


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

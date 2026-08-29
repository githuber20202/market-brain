from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime

import httpx

from market_brain.domain.models import MarketSnapshot
from market_brain.providers.base import DataUnavailable
from market_brain.providers.cboe import CboeDelayedQuotes, DelayedQuote, _slot_key
from market_brain.providers.keyless_http import KeylessJsonClient
from market_brain.providers.rate_limit import TokenBucketRateLimiter
from market_brain.settings import Settings, settings

YAHOO_SOURCE_ID = "YAHOO_DELAYED"
YAHOO_CHART_BASE_URL = "https://query2.finance.yahoo.com/v8/finance/chart"


class YahooMarketData:
    def __init__(
        self,
        cfg: Settings = settings,
        client: httpx.AsyncClient | None = None,
        *,
        cboe_client: httpx.AsyncClient | None = None,
        limiter: TokenBucketRateLimiter | None = None,
        now: Callable[[], datetime] | None = None,
        sleep=asyncio.sleep,
    ) -> None:
        self.cfg = cfg
        self.now = now or (lambda: datetime.now(UTC))
        calls_per_minute = min(
            cfg.keyless_calls_per_minute,
            max(1, int(60.0 / cfg.keyless_request_interval_seconds)),
        )
        shared_limiter = limiter or TokenBucketRateLimiter(
            calls_per_minute,
            burst_capacity=1,
        )
        self.http = KeylessJsonClient(
            source_id=YAHOO_SOURCE_ID,
            client=client,
            limiter=shared_limiter,
            retry_attempts=cfg.keyless_retry_attempts,
            sleep=sleep,
        )
        self.cboe = CboeDelayedQuotes(
            cfg,
            cboe_client if cboe_client is not None else client,
            limiter=shared_limiter,
            now=self.now,
            sleep=sleep,
        )
        self._chart_cache: dict[tuple[str, str, str, str], dict] = {}

    @property
    def configured(self) -> bool:
        return True

    async def _chart(self, symbol: str, interval: str, range_value: str) -> dict:
        normalized = symbol.upper().strip()
        key = (_slot_key(self.now()), normalized, interval, range_value)
        cached = self._chart_cache.get(key)
        if cached is not None:
            return cached
        payload = await self.http.get_json(
            f"{YAHOO_CHART_BASE_URL}/{normalized}",
            resource=f"chart:{interval}",
            symbol=normalized,
            params={"interval": interval, "range": range_value},
        )
        chart = payload.get("chart")
        results = chart.get("result") if isinstance(chart, dict) else None
        row = results[0] if isinstance(results, list) and results else None
        if not isinstance(row, dict):
            raise DataUnavailable(
                source_id=YAHOO_SOURCE_ID,
                resource=f"chart:{interval}",
                symbol=normalized,
                error_type="YAHOO_CHART_INVALID",
            )
        self._chart_cache[key] = row
        return row

    async def snapshot(self, symbol: str, decision: bool = False) -> MarketSnapshot:
        del decision
        normalized = symbol.upper().strip()
        chart = await self._chart(normalized, "1m", "1d")
        bars = _chart_bars(chart)
        if not bars:
            raise DataUnavailable(
                source_id=YAHOO_SOURCE_ID,
                resource="snapshot",
                symbol=normalized,
                error_type="YAHOO_BARS_EMPTY",
            )
        fetched_at = _aware(self.now())
        latest = bars[-1]
        latest_at = datetime.fromisoformat(latest["t"])
        meta = chart.get("meta") if isinstance(chart.get("meta"), dict) else {}

        cboe_quote: DelayedQuote | None = None
        cboe_error: str | None = None
        try:
            cboe_quote = await self.cboe.quote(normalized)
        except (DataUnavailable, RuntimeError, TypeError, ValueError) as exc:
            cboe_error = type(exc).__name__

        yahoo_price = _positive_float(meta.get("regularMarketPrice"))
        fresh_cboe = (
            cboe_quote is not None
            and cboe_quote.delay_minutes <= self.cfg.max_delayed_age_minutes
        )
        if yahoo_price is not None:
            last = yahoo_price
            quote_source = YAHOO_SOURCE_ID
            quoted_at = latest_at
        elif fresh_cboe and cboe_quote is not None:
            last = cboe_quote.price
            quote_source = "CBOE_DELAYED_QUOTE_YAHOO_BARS"
            quoted_at = cboe_quote.quoted_at
        else:
            last = float(latest["c"])
            quote_source = YAHOO_SOURCE_ID
            quoted_at = latest_at

        delay_minutes = max(0.0, (fetched_at - quoted_at).total_seconds() / 60.0)
        divergence_pct = None
        price_consistent = True
        if yahoo_price is not None and fresh_cboe and cboe_quote is not None:
            divergence_pct = abs(yahoo_price - cboe_quote.price) / yahoo_price * 100.0
            price_consistent = divergence_pct <= self.cfg.iex_mid_tolerance_pct
        authoritative = (
            delay_minutes <= self.cfg.max_delayed_age_minutes and price_consistent
        )
        volumes = [float(row["v"]) for row in bars if row.get("v") is not None]
        total_volume = sum(volumes) if volumes else None
        vwap = _vwap(bars)
        return MarketSnapshot(
            symbol=normalized,
            last=last,
            prior_close=_positive_float(
                meta.get("previousClose", meta.get("chartPreviousClose"))
            ),
            bid=cboe_quote.bid if fresh_cboe and cboe_quote else None,
            ask=cboe_quote.ask if fresh_cboe and cboe_quote else None,
            volume=total_volume,
            vwap=vwap,
            open_price=float(bars[0]["o"]),
            high=max(float(row["h"]) for row in bars),
            low=min(float(row["l"]) for row in bars),
            data_age_seconds=delay_minutes * 60.0,
            source_id=quote_source,
            delay_minutes=delay_minutes,
            fetched_at=fetched_at,
            authoritative=authoritative,
            metadata={
                "fetched_at": fetched_at.isoformat(),
                "delay_minutes": delay_minutes,
                "quote_timestamp": quoted_at.isoformat(),
                "last_bar_timestamp": latest_at.isoformat(),
                "last_bar_high": latest["h"],
                "last_bar_low": latest["l"],
                "last_bar_close": latest["c"],
                "cboe_source_id": cboe_quote.source_id if cboe_quote else None,
                "cboe_quote_timestamp": (
                    cboe_quote.quoted_at.isoformat() if cboe_quote else None
                ),
                "cboe_delay_minutes": (
                    cboe_quote.delay_minutes if cboe_quote else None
                ),
                "cboe_error_type": cboe_error,
                "price_divergence_pct": divergence_pct,
                "price_cross_check": "PASS" if price_consistent else "FAIL",
            },
        )

    async def snapshots(self, symbols: list[str], *, decision: bool = False) -> list[MarketSnapshot]:
        output: list[MarketSnapshot] = []
        for symbol in _symbols(symbols):
            output.append(await self.snapshot(symbol, decision=decision))
        return output

    async def bars(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> list[dict]:
        interval, range_value = _timeframe(timeframe)
        normalized = symbol.upper().strip()
        chart = await self._chart(normalized, interval, range_value)
        lower = _aware(start)
        upper = _aware(end)
        if lower >= upper:
            raise ValueError("HISTORICAL_WINDOW_UNAVAILABLE")
        return [
            row
            for row in _chart_bars(chart)
            if lower <= datetime.fromisoformat(row["t"]) < upper
        ]

    async def bars_batch(
        self,
        symbols: list[str],
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> dict[str, list[dict]]:
        output: dict[str, list[dict]] = {}
        for symbol in _symbols(symbols):
            output[symbol] = await self.bars(symbol, timeframe, start, end)
        return output


def _chart_bars(chart: dict) -> list[dict]:
    timestamps = chart.get("timestamp")
    indicators = chart.get("indicators")
    quotes = indicators.get("quote") if isinstance(indicators, dict) else None
    quote = quotes[0] if isinstance(quotes, list) and quotes else None
    if not isinstance(timestamps, list) or not isinstance(quote, dict):
        return []
    output: list[dict] = []
    for index, raw_ts in enumerate(timestamps):
        try:
            volumes = quote.get("volume")
            raw_volume = volumes[index] if isinstance(volumes, list) else None
            row = {
                "t": datetime.fromtimestamp(int(raw_ts), UTC).isoformat(),
                "o": float(quote["open"][index]),
                "h": float(quote["high"][index]),
                "l": float(quote["low"][index]),
                "c": float(quote["close"][index]),
                "v": _optional_float(raw_volume),
                "source": YAHOO_SOURCE_ID,
            }
        except (IndexError, KeyError, TypeError, ValueError):
            continue
        if row["o"] <= 0 or row["h"] <= 0 or row["l"] <= 0 or row["c"] <= 0:
            continue
        output.append(row)
    return output


def _vwap(bars: list[dict]) -> float | None:
    weighted = 0.0
    volume = 0.0
    for row in bars:
        row_volume = row.get("v")
        if row_volume is None or row_volume <= 0:
            continue
        typical = (float(row["h"]) + float(row["l"]) + float(row["c"])) / 3.0
        weighted += typical * float(row_volume)
        volume += float(row_volume)
    return weighted / volume if volume > 0 else None


def _timeframe(value: str) -> tuple[str, str]:
    normalized = value.strip().lower()
    if normalized in {"1min", "1m"}:
        return "1m", "7d"
    if normalized in {"5min", "5m"}:
        return "5m", "7d"
    if normalized in {"1day", "1d"}:
        return "1d", "1y"
    raise ValueError("KEYLESS_TIMEFRAME_UNSUPPORTED")


def _symbols(values: list[str]) -> list[str]:
    return list(dict.fromkeys(value.upper().strip() for value in values if value.strip()))


def _positive_float(value) -> float | None:
    parsed = _optional_float(value)
    return parsed if parsed is not None and parsed > 0 else None


def _optional_float(value) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

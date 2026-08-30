from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

import httpx

from market_brain.domain.models import MarketSnapshot
from market_brain.providers.base import DataUnavailable, SkippedSymbol, SnapshotBatch
from market_brain.providers.keyless_http import KeylessJsonClient
from market_brain.providers.rate_limit import TokenBucketRateLimiter
from market_brain.providers.yahoo import (
    YAHOO_CHART_BASE_URL,
    _aware,
    _chart_bars,
    _positive_float,
    _symbols,
    _timeframe,
    _vwap,
)
from market_brain.settings import Settings, settings

EASTERN = ZoneInfo("America/New_York")
YAHOO_REPLAY_SOURCE_ID = "YAHOO_REPLAY"


class YahooReplayMarketData:
    """Serve one historical Yahoo chart as it would have looked at each simulated tick."""

    def __init__(
        self,
        session_date: date,
        cfg: Settings = settings,
        client: httpx.AsyncClient | None = None,
        *,
        limiter: TokenBucketRateLimiter | None = None,
        now: Callable[[], datetime] | None = None,
        sleep=asyncio.sleep,
    ) -> None:
        self.session_date = session_date
        self.cfg = cfg
        self.now = now or (lambda: datetime.now(UTC))
        calls_per_minute = min(
            cfg.keyless_calls_per_minute,
            max(1, int(60.0 / cfg.keyless_request_interval_seconds)),
        )
        self.http = KeylessJsonClient(
            source_id=YAHOO_REPLAY_SOURCE_ID,
            client=client,
            limiter=limiter
            or TokenBucketRateLimiter(calls_per_minute, burst_capacity=1),
            retry_attempts=cfg.keyless_retry_attempts,
            sleep=sleep,
        )
        self._charts: dict[tuple[str, str, str], dict] = {}

    @property
    def configured(self) -> bool:
        return True

    @property
    def request_count(self) -> int:
        return self.http.request_count

    async def _chart(self, symbol: str, interval: str, range_value: str) -> dict:
        normalized = symbol.upper().strip()
        key = (normalized, interval, range_value)
        cached = self._charts.get(key)
        if cached is not None:
            return cached
        payload = await self.http.get_json(
            f"{YAHOO_CHART_BASE_URL}/{normalized}",
            resource=f"replay_chart:{interval}",
            symbol=normalized,
            params={"interval": interval, "range": range_value},
        )
        chart = payload.get("chart")
        results = chart.get("result") if isinstance(chart, dict) else None
        row = results[0] if isinstance(results, list) and results else None
        if not isinstance(row, dict):
            raise DataUnavailable(
                source_id=YAHOO_REPLAY_SOURCE_ID,
                resource=f"replay_chart:{interval}",
                symbol=normalized,
                error_type="YAHOO_REPLAY_CHART_INVALID",
            )
        self._charts[key] = row
        return row

    def _visible_session_bars(self, chart: dict) -> list[dict]:
        simulated_now = _aware(self.now())
        return [
            {**row, "source": YAHOO_REPLAY_SOURCE_ID}
            for row in _chart_bars(chart)
            if datetime.fromisoformat(row["t"]).astimezone(EASTERN).date()
            == self.session_date
            and datetime.fromisoformat(row["t"]) <= simulated_now
        ]

    async def snapshot(self, symbol: str, decision: bool = False) -> MarketSnapshot:
        del decision
        normalized = symbol.upper().strip()
        chart = await self._chart(normalized, "1m", "7d")
        bars = self._visible_session_bars(chart)
        if not bars:
            raise DataUnavailable(
                source_id=YAHOO_REPLAY_SOURCE_ID,
                resource="replay_snapshot",
                symbol=normalized,
                error_type="YAHOO_REPLAY_BARS_EMPTY",
            )
        fetched_at = _aware(self.now())
        latest = bars[-1]
        latest_at = datetime.fromisoformat(latest["t"])
        all_bars = _chart_bars(chart)
        previous = [
            row
            for row in all_bars
            if datetime.fromisoformat(row["t"]).astimezone(EASTERN).date()
            < self.session_date
        ]
        meta = chart.get("meta") if isinstance(chart.get("meta"), dict) else {}
        prior_close = (
            float(previous[-1]["c"])
            if previous
            else _positive_float(meta.get("previousClose", meta.get("chartPreviousClose")))
        )
        delay_minutes = max(0.0, (fetched_at - latest_at).total_seconds() / 60.0)
        volumes = [float(row["v"]) for row in bars if row.get("v") is not None]
        return MarketSnapshot(
            symbol=normalized,
            last=float(latest["c"]),
            prior_close=prior_close,
            bid=None,
            ask=None,
            volume=sum(volumes) if volumes else None,
            vwap=_vwap(bars),
            open_price=float(bars[0]["o"]),
            high=max(float(row["h"]) for row in bars),
            low=min(float(row["l"]) for row in bars),
            data_age_seconds=delay_minutes * 60.0,
            source_id=YAHOO_REPLAY_SOURCE_ID,
            delay_minutes=delay_minutes,
            fetched_at=fetched_at,
            authoritative=delay_minutes <= self.cfg.max_delayed_age_minutes,
            metadata={
                "fetched_at": fetched_at.isoformat(),
                "delay_minutes": delay_minutes,
                "quote_timestamp": latest_at.isoformat(),
                "last_bar_timestamp": latest_at.isoformat(),
                "last_bar_high": latest["h"],
                "last_bar_low": latest["l"],
                "last_bar_close": latest["c"],
                "cboe_source_id": None,
                "cboe_quote_timestamp": None,
                "cboe_delay_minutes": None,
                "cboe_error_type": "DISABLED_IN_REHEARSAL",
                "price_divergence_pct": None,
                "price_cross_check": "SKIP_REHEARSAL",
                "replay_session": self.session_date.isoformat(),
            },
        )

    async def snapshots(
        self, symbols: list[str], *, decision: bool = False
    ) -> SnapshotBatch:
        output: list[MarketSnapshot] = []
        skipped: list[SkippedSymbol] = []
        for symbol in _symbols(symbols):
            try:
                output.append(await self.snapshot(symbol, decision=decision))
            except (DataUnavailable, RuntimeError, TypeError, ValueError) as exc:
                error_type = (
                    exc.error_type if isinstance(exc, DataUnavailable) else type(exc).__name__
                )
                skipped.append(SkippedSymbol(symbol=symbol, error_type=error_type))
        return SnapshotBatch(tuple(output), tuple(skipped))

    async def bars(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> list[dict]:
        interval, range_value = _timeframe(timeframe)
        chart = await self._chart(symbol, interval, range_value)
        lower = _aware(start)
        upper = _aware(end)
        if lower >= upper:
            raise ValueError("HISTORICAL_WINDOW_UNAVAILABLE")
        simulated_now = _aware(self.now())
        return [
            {**row, "source": YAHOO_REPLAY_SOURCE_ID}
            for row in _chart_bars(chart)
            if lower <= datetime.fromisoformat(row["t"]) < upper
            and (interval == "1d" or datetime.fromisoformat(row["t"]) <= simulated_now)
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

    async def aclose(self) -> None:
        await self.http.aclose()

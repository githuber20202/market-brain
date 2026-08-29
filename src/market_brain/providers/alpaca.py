from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from market_brain.domain.models import MarketSnapshot
from market_brain.ledger.events import LedgerEvent
from market_brain.providers.rate_limit import TokenBucketRateLimiter
from market_brain.settings import Settings, settings


class DataPlanViolation(RuntimeError):
    pass


class AlpacaMarketData:
    def __init__(
        self,
        cfg: Settings = settings,
        client: httpx.AsyncClient | None = None,
        *,
        event_store=None,
        limiter: TokenBucketRateLimiter | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.cfg = cfg
        self.client = client
        self.event_store = event_store
        self.limiter = limiter or TokenBucketRateLimiter(cfg.rest_safe_calls_per_minute)
        self.now = now or (lambda: datetime.now(UTC))
        self._blocked_entitlements: set[tuple[str, str]] = set()

    @property
    def configured(self) -> bool:
        return bool(self.cfg.alpaca_api_key and self.cfg.alpaca_api_secret)

    def _headers(self) -> dict[str, str]:
        if not self.configured:
            raise RuntimeError("MARKET_DATA_NOT_CONFIGURED")
        return {
            "APCA-API-KEY-ID": self.cfg.alpaca_api_key or "",
            "APCA-API-SECRET-KEY": self.cfg.alpaca_api_secret or "",
        }

    def _clamp_historical_end(self, end: datetime) -> datetime:
        end = _aware(end)
        if self.cfg.data_plan != "free":
            return end
        cutoff = _aware(self.now()) - timedelta(minutes=self.cfg.historical_lag_minutes)
        return min(end, cutoff)

    async def _record_data_plan_violation(
        self,
        *,
        resource: str,
        feed: str,
        status_code: int,
    ) -> None:
        if self.event_store is None:
            return
        await self.event_store.append(
            LedgerEvent(
                "DATA_PLAN_VIOLATION",
                f"{resource}:{feed}",
                {
                    "resource": resource,
                    "feed": feed,
                    "status_code": status_code,
                    "data_plan": self.cfg.data_plan,
                },
            )
        )

    async def _request_json(
        self,
        *,
        resource: str,
        path: str,
        params: dict[str, Any],
        feed: str,
    ) -> dict:
        entitlement_key = (resource, feed.lower())
        if entitlement_key in self._blocked_entitlements:
            raise DataPlanViolation("DATA_PLAN_VIOLATION")
        await self.limiter.acquire()
        owned = self.client is None
        client = self.client or httpx.AsyncClient(timeout=10.0)
        try:
            response = await client.get(
                f"{self.cfg.alpaca_data_base_url.rstrip('/')}{path}",
                headers=self._headers(),
                params=params,
            )
            if _subscription_required(response):
                self._blocked_entitlements.add(entitlement_key)
                await self._record_data_plan_violation(
                    resource=resource,
                    feed=feed,
                    status_code=response.status_code,
                )
                raise DataPlanViolation("DATA_PLAN_VIOLATION")
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise TypeError("MARKET_RESPONSE_INVALID")
            return payload
        finally:
            if owned:
                await client.aclose()

    async def snapshot(self, symbol: str, decision: bool = False) -> MarketSnapshot:
        snapshots = await self.snapshots([symbol], decision=decision)
        if not snapshots:
            raise RuntimeError("MARKET_SNAPSHOT_UNAVAILABLE")
        return snapshots[0]

    async def snapshots(self, symbols: list[str], *, decision: bool = False) -> list[MarketSnapshot]:
        normalized = _symbols(symbols)
        if not normalized:
            return []
        feed = self.cfg.decision_feed if decision else self.cfg.discovery_feed
        raw_by_symbol: dict[str, dict] = {}
        for batch in _batches(normalized, self.cfg.rest_batch_symbols):
            data = await self._request_json(
                resource="snapshots",
                path="/v2/stocks/snapshots",
                params={"symbols": ",".join(batch), "feed": feed},
                feed=feed,
            )
            raw = data.get("snapshots", data)
            if not isinstance(raw, dict):
                raise TypeError("MARKET_SNAPSHOTS_INVALID")
            for symbol in batch:
                row = raw.get(symbol)
                if isinstance(row, dict):
                    raw_by_symbol[symbol] = row

        out: list[MarketSnapshot] = []
        for symbol in normalized:
            row = raw_by_symbol.get(symbol, {})
            trade = row.get("latestTrade") or row.get("latest_trade") or {}
            quote = row.get("latestQuote") or row.get("latest_quote") or {}
            minute = row.get("minuteBar") or row.get("minute_bar") or {}
            daily = row.get("dailyBar") or row.get("daily_bar") or {}
            previous = row.get("prevDailyBar") or row.get("previous_daily_bar") or {}
            last = trade.get("p", trade.get("price", minute.get("c", minute.get("close"))))
            if last is None:
                continue
            quote_timestamp = quote.get("t") or quote.get("timestamp")
            feed_name = feed.lower()
            out.append(
                MarketSnapshot(
                    symbol=symbol,
                    last=float(last),
                    prior_close=_float(previous.get("c", previous.get("close"))),
                    bid=_float(quote.get("bp", quote.get("bid_price"))),
                    ask=_float(quote.get("ap", quote.get("ask_price"))),
                    volume=_float(daily.get("v", daily.get("volume"))),
                    vwap=_float(daily.get("vw", daily.get("vwap"))),
                    open_price=_float(daily.get("o", daily.get("open"))),
                    high=_float(daily.get("h", daily.get("high"))),
                    low=_float(daily.get("l", daily.get("low"))),
                    data_age_seconds=_age(quote_timestamp, self.now()),
                    source_id=f"ALPACA_{feed.upper()}",
                    authoritative=(
                        decision
                        and quote_timestamp is not None
                        and feed_name != "iex"
                    ),
                    metadata={"quote_timestamp": quote_timestamp, "feed": feed_name},
                )
            )
        return out

    async def bars(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> list[dict]:
        clamped_end = self._clamp_historical_end(end)
        if _aware(start) >= clamped_end:
            raise RuntimeError("HISTORICAL_WINDOW_UNAVAILABLE")
        data = await self._request_json(
            resource="bars",
            path=f"/v2/stocks/{symbol.upper()}/bars",
            params={
                "timeframe": timeframe,
                "start": _aware(start).isoformat(),
                "end": clamped_end.isoformat(),
                "feed": self.cfg.historical_feed,
                "limit": 1000,
                "adjustment": "raw",
            },
            feed=self.cfg.historical_feed,
        )
        rows = data.get("bars", [])
        if not isinstance(rows, list):
            raise TypeError("MARKET_BARS_INVALID")
        return [row for row in rows if isinstance(row, dict)]

    async def bars_batch(
        self,
        symbols: list[str],
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> dict[str, list[dict]]:
        normalized = _symbols(symbols)
        clamped_end = self._clamp_historical_end(end)
        if _aware(start) >= clamped_end:
            raise RuntimeError("HISTORICAL_WINDOW_UNAVAILABLE")
        output: dict[str, list[dict]] = {symbol: [] for symbol in normalized}
        for batch in _batches(normalized, self.cfg.rest_batch_symbols):
            page_token: str | None = None
            seen_tokens: set[str] = set()
            while True:
                params = {
                    "symbols": ",".join(batch),
                    "timeframe": timeframe,
                    "start": _aware(start).isoformat(),
                    "end": clamped_end.isoformat(),
                    "feed": self.cfg.historical_feed,
                    "limit": 1000,
                    "adjustment": "raw",
                }
                if page_token is not None:
                    params["page_token"] = page_token
                data = await self._request_json(
                    resource="bars",
                    path="/v2/stocks/bars",
                    params=params,
                    feed=self.cfg.historical_feed,
                )
                raw = data.get("bars", {})
                if not isinstance(raw, dict):
                    raise TypeError("MARKET_BARS_INVALID")
                for symbol in batch:
                    rows = raw.get(symbol, [])
                    if not isinstance(rows, list):
                        raise TypeError("MARKET_BARS_INVALID")
                    output[symbol].extend(row for row in rows if isinstance(row, dict))
                raw_token = data.get("next_page_token")
                if raw_token in (None, ""):
                    break
                if not isinstance(raw_token, str) or raw_token in seen_tokens:
                    raise TypeError("MARKET_BARS_PAGE_TOKEN_INVALID")
                seen_tokens.add(raw_token)
                page_token = raw_token
        return output

    async def historical_trades(
        self,
        symbols: list[str],
        start: datetime,
        end: datetime,
    ) -> dict[str, list[dict]]:
        return await self._historical_multi("trades", symbols, start, end)

    async def historical_quotes(
        self,
        symbols: list[str],
        start: datetime,
        end: datetime,
    ) -> dict[str, list[dict]]:
        return await self._historical_multi("quotes", symbols, start, end)

    async def _historical_multi(
        self,
        resource: str,
        symbols: list[str],
        start: datetime,
        end: datetime,
    ) -> dict[str, list[dict]]:
        normalized = _symbols(symbols)
        clamped_end = self._clamp_historical_end(end)
        if _aware(start) >= clamped_end:
            raise RuntimeError("HISTORICAL_WINDOW_UNAVAILABLE")
        output: dict[str, list[dict]] = {symbol: [] for symbol in normalized}
        for batch in _batches(normalized, self.cfg.rest_batch_symbols):
            data = await self._request_json(
                resource=resource,
                path=f"/v2/stocks/{resource}",
                params={
                    "symbols": ",".join(batch),
                    "start": _aware(start).isoformat(),
                    "end": clamped_end.isoformat(),
                    "feed": self.cfg.historical_feed,
                    "limit": 1000,
                },
                feed=self.cfg.historical_feed,
            )
            raw = data.get(resource, {})
            if not isinstance(raw, dict):
                raise TypeError(f"MARKET_{resource.upper()}_INVALID")
            for symbol in batch:
                rows = raw.get(symbol, [])
                if not isinstance(rows, list):
                    raise TypeError(f"MARKET_{resource.upper()}_INVALID")
                output[symbol].extend(row for row in rows if isinstance(row, dict))
        return output


def _symbols(symbols: list[str]) -> list[str]:
    return list(dict.fromkeys(symbol.strip().upper() for symbol in symbols if symbol.strip()))


def _batches(symbols: list[str], size: int):
    for index in range(0, len(symbols), size):
        yield symbols[index : index + size]


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _subscription_required(response: httpx.Response) -> bool:
    if response.status_code < 400 or response.status_code >= 500:
        return False
    text = response.text.lower()
    return any(
        marker in text
        for marker in (
            "subscription required",
            "subscription does not permit",
            "not subscribed",
            "entitlement",
            "upgrade your subscription",
        )
    )


def _float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _age(timestamp: str | None, now: datetime | None = None) -> float | None:
    if not timestamp:
        return None
    try:
        dt = datetime.fromisoformat(timestamp)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        reference = _aware(now or datetime.now(UTC))
        return max(0.0, (reference - dt.astimezone(UTC)).total_seconds())
    except ValueError:
        return None


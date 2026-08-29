from __future__ import annotations

import asyncio
import json
import logging
import os
import random
from dataclasses import dataclass
from datetime import UTC, datetime

import nats
from websockets.exceptions import WebSocketException

from market_brain.domain.models import PlanStatus
from market_brain.ledger.store import InMemoryEventStore, PostgresEventStore
from market_brain.providers.alpaca_stream import AlpacaStockStream
from market_brain.settings import settings

LOGGER = logging.getLogger(__name__)


def _watchlist_symbols() -> list[str]:
    return list(
        dict.fromkeys(
            symbol.strip().upper()
            for symbol in os.getenv("STREAM_SYMBOLS", "SPY,QQQ").split(",")
            if symbol.strip()
        )
    )


@dataclass(slots=True)
class SubscriptionSelection:
    selected: list[str]
    dropped: list[str]


async def select_subscription_symbols(
    store,
    *,
    cap: int | None = None,
    watchlist: list[str] | None = None,
) -> SubscriptionSelection:
    limit = cap or settings.stream_max_symbols
    priority: list[str] = []

    def add(symbol: str) -> None:
        normalized = symbol.upper()
        if normalized not in priority:
            priority.append(normalized)

    for position in await store.list_positions():
        if position.closed_at is None and position.remaining_quantity > 0:
            add(position.symbol)
    for plan in await store.list_plans():
        if plan.status in {PlanStatus.ACTIVE, PlanStatus.RESERVED}:
            add(plan.symbol)
    for symbol in watchlist if watchlist is not None else _watchlist_symbols():
        add(symbol)

    return SubscriptionSelection(priority[:limit], priority[limit:])


async def desired_symbols(store) -> set[str]:
    return set((await select_subscription_symbols(store)).selected)


class ResilientStreamWorker:
    def __init__(
        self,
        *,
        store,
        stream_factory=AlpacaStockStream,
        publisher=None,
        stale_seconds: float | None = None,
        refresh_seconds: float | None = None,
        status_write_interval_seconds: float | None = None,
        stream_max_symbols: int | None = None,
        sleep=asyncio.sleep,
        rng=random.random,
    ) -> None:
        self.store = store
        self.stream_factory = stream_factory
        self.publisher = publisher
        self.stale_seconds = stale_seconds or settings.stream_stale_seconds
        self.refresh_seconds = refresh_seconds or settings.stream_subscription_refresh_seconds
        self.status_write_interval_seconds = (
            status_write_interval_seconds or settings.status_write_interval_seconds
        )
        self.stream_max_symbols = stream_max_symbols or settings.stream_max_symbols
        self.sleep = sleep
        self.rng = rng
        self._stop = asyncio.Event()
        self._subscribed: set[str] = set()
        self._last_message_status_monotonic: float | None = None

    async def _write_status(self, key: str, value) -> None:
        await self.store.set_runtime_status(key, value)

    async def _sync_subscriptions(self, stream) -> None:
        selection = await select_subscription_symbols(
            self.store,
            cap=self.stream_max_symbols,
        )
        desired = set(selection.selected)
        add = desired - self._subscribed
        remove = self._subscribed - desired
        if add:
            await stream.subscribe(sorted(add))
        if remove:
            await stream.unsubscribe(sorted(remove))
        self._subscribed = desired
        await self._write_status("subscribed_symbols", sorted(self._subscribed))
        await self._write_status("subscription_cap", self.stream_max_symbols)
        await self._write_status("dropped_symbols", selection.dropped)

    async def _publish(self, events) -> None:
        if self.publisher is None:
            return
        for event in events:
            subject = f"market.{event.event_type.lower()}.{event.aggregate_id.lower()}"
            payload = json.dumps(
                event.payload,
                separators=(",", ":"),
                default=str,
            ).encode("utf-8")
            await self.publisher.publish(subject, payload)

    async def _consume_connection(self, stream) -> None:
        self._subscribed.clear()
        await stream.connect()
        await self._write_status("stream_connected", True)
        await self._write_status("stream_connected_since", datetime.now(UTC).isoformat())
        await self._sync_subscriptions(stream)
        loop = asyncio.get_running_loop()
        last_message_monotonic = loop.time()
        self._last_message_status_monotonic = None
        next_refresh = last_message_monotonic + self.refresh_seconds

        while not self._stop.is_set():
            now = loop.time()
            timeout = min(
                max(0.05, self.stale_seconds - (now - last_message_monotonic)),
                max(0.05, next_refresh - now),
            )
            try:
                raw = await asyncio.wait_for(stream.recv(), timeout=timeout)
            except TimeoutError:
                now = loop.time()
                if now - last_message_monotonic >= self.stale_seconds:
                    raise ConnectionError("STREAM_STALE")
                if now >= next_refresh:
                    await self._sync_subscriptions(stream)
                    next_refresh = now + self.refresh_seconds
                continue

            if raw is None:
                raise ConnectionError("STREAM_DISCONNECTED")
            last_message_monotonic = loop.time()
            if (
                self._last_message_status_monotonic is None
                or last_message_monotonic - self._last_message_status_monotonic
                >= self.status_write_interval_seconds
            ):
                await self._write_status(
                    "stream_last_message_at", datetime.now(UTC).isoformat()
                )
                self._last_message_status_monotonic = last_message_monotonic

            try:
                events = stream.parse(raw)
            except (TypeError, ValueError, UnicodeDecodeError) as exc:
                LOGGER.warning("stream_parse_error type=%s", type(exc).__name__)
                continue
            await self._publish(events)

            now = loop.time()
            if now >= next_refresh:
                await self._sync_subscriptions(stream)
                next_refresh = now + self.refresh_seconds

    async def run(self) -> None:
        delay = 1.0
        while not self._stop.is_set():
            stream = self.stream_factory()
            try:
                await self._consume_connection(stream)
                delay = 1.0
            except asyncio.CancelledError:
                raise
            except (ConnectionError, OSError, RuntimeError, TimeoutError, WebSocketException) as exc:
                LOGGER.warning("stream_connection_error type=%s", type(exc).__name__)
            finally:
                await self._write_status("stream_connected", False)
                try:
                    await stream.close()
                except (ConnectionError, OSError, RuntimeError, WebSocketException) as exc:
                    LOGGER.warning("stream_close_error type=%s", type(exc).__name__)

            if self._stop.is_set():
                break
            jitter = 0.8 + 0.4 * self.rng()
            await self.sleep(min(60.0, delay) * jitter)
            delay = min(60.0, delay * 2.0)

    async def stop(self) -> None:
        self._stop.set()


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    if not settings.nats_url:
        raise RuntimeError("NATS_URL_MISSING")
    store = (
        PostgresEventStore(settings.postgres_dsn)
        if settings.postgres_dsn
        else InMemoryEventStore()
    )
    connection = await nats.connect(settings.nats_url)
    try:
        await ResilientStreamWorker(store=store, publisher=connection).run()
    finally:
        await connection.drain()


if __name__ == "__main__":
    asyncio.run(main())


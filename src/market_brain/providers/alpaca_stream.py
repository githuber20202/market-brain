from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

import websockets

from market_brain.ledger.events import LedgerEvent
from market_brain.settings import Settings, settings


class AlpacaStockStream:
    def __init__(self, cfg: Settings = settings, websocket_factory=websockets.connect) -> None:
        self.cfg = cfg
        self.websocket_factory = websocket_factory
        self.ws = None
        self._symbols: set[str] = set()

    async def connect(self) -> None:
        if not self.cfg.alpaca_api_key or not self.cfg.alpaca_api_secret:
            raise RuntimeError("MARKET_STREAM_NOT_CONFIGURED")
        self.ws = await self.websocket_factory(
            self.cfg.alpaca_stream_url,
            ping_interval=20,
            ping_timeout=20,
        )
        await self.ws.send(
            json.dumps(
                {
                    "action": "auth",
                    "key": self.cfg.alpaca_api_key,
                    "secret": self.cfg.alpaca_api_secret,
                }
            )
        )
        raw = await self.ws.recv()
        auth = json.loads(raw)
        if isinstance(auth, dict):
            auth = [auth]
        if not isinstance(auth, list) or not any(
            isinstance(row, dict)
            and row.get("T") == "success"
            and row.get("msg") == "authenticated"
            for row in auth
        ):
            raise RuntimeError("MARKET_STREAM_AUTH_FAILED")

    async def subscribe(self, symbols: list[str]) -> None:
        if not symbols:
            return
        if self.ws is None:
            raise RuntimeError("STREAM_NOT_CONNECTED")
        normalized = sorted({symbol.upper() for symbol in symbols})
        await self.ws.send(
            json.dumps(
                {
                    "action": "subscribe",
                    "trades": normalized,
                    "quotes": normalized,
                    "bars": normalized,
                    "statuses": normalized,
                }
            )
        )
        self._symbols.update(normalized)

    async def unsubscribe(self, symbols: list[str]) -> None:
        if not symbols:
            return
        if self.ws is None:
            raise RuntimeError("STREAM_NOT_CONNECTED")
        normalized = sorted({symbol.upper() for symbol in symbols})
        await self.ws.send(
            json.dumps(
                {
                    "action": "unsubscribe",
                    "trades": normalized,
                    "quotes": normalized,
                    "bars": normalized,
                    "statuses": normalized,
                }
            )
        )
        self._symbols.difference_update(normalized)

    async def recv(self):
        if self.ws is None:
            raise RuntimeError("STREAM_NOT_CONNECTED")
        return await self.ws.recv()

    def parse(self, raw) -> list[LedgerEvent]:
        payload = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
        if isinstance(payload, dict):
            payload = [payload]
        if not isinstance(payload, list):
            raise TypeError("INVALID_STREAM_PAYLOAD")
        events: list[LedgerEvent] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            event = parse_message(item)
            if event is not None:
                events.append(event)
        return events

    async def close(self) -> None:
        if self.ws is not None:
            await self.ws.close()
            self.ws = None

    async def events(self, symbols: list[str]) -> AsyncIterator[LedgerEvent]:
        if not symbols:
            raise RuntimeError("STREAM_SYMBOLS_EMPTY")
        await self.connect()
        try:
            await self.subscribe(symbols)
            while True:
                raw = await self.recv()
                for event in self.parse(raw):
                    yield event
        finally:
            await self.close()


AlpacaStream = AlpacaStockStream


def parse_message(item: dict[str, Any]) -> LedgerEvent | None:
    mapping = {
        "t": "MARKET_TRADE",
        "q": "MARKET_QUOTE",
        "b": "BAR_CLOSED_1M",
        "s": "TRADING_STATUS",
        "l": "LULD",
    }
    event_type = mapping.get(item.get("T"))
    if event_type is None:
        return None
    return LedgerEvent(event_type, item.get("S") or "UNKNOWN", item)


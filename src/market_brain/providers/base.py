from __future__ import annotations

from datetime import datetime
from typing import Protocol

from market_brain.domain.models import MarketSnapshot


class MarketDataProvider(Protocol):
    async def snapshot(self, symbol: str, decision: bool = False) -> MarketSnapshot:
        ...

    async def bars(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> list[dict]:
        ...

    async def bars_batch(
        self,
        symbols: list[str],
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> dict[str, list[dict]]:
        ...


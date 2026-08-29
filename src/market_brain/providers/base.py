from __future__ import annotations

from datetime import datetime
from typing import Protocol

from market_brain.domain.models import MarketSnapshot


class DataUnavailable(RuntimeError):
    """A provider could not supply the required market data after safe retries."""

    def __init__(
        self,
        *,
        source_id: str,
        resource: str,
        symbol: str,
        error_type: str,
    ) -> None:
        super().__init__("DATA_UNAVAILABLE")
        self.source_id = source_id
        self.resource = resource
        self.symbol = symbol.upper()
        self.error_type = error_type


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

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from market_brain.domain.models import MarketSnapshot


@dataclass(frozen=True, slots=True)
class SkippedSymbol:
    symbol: str
    error_type: str


class DataUnavailable(RuntimeError):
    """A provider could not supply the required market data after safe retries."""

    def __init__(
        self,
        *,
        source_id: str,
        resource: str,
        symbol: str,
        error_type: str,
        reason_codes: Sequence[str] = (),
        skipped_symbols: Sequence[SkippedSymbol] = (),
    ) -> None:
        super().__init__("DATA_UNAVAILABLE")
        self.source_id = source_id
        self.resource = resource
        self.symbol = symbol.upper()
        self.error_type = error_type
        normalized_reasons = tuple(
            dict.fromkeys(str(reason) for reason in reason_codes if str(reason))
        )
        self.reason_codes = normalized_reasons or (error_type,)
        self.skipped_symbols = tuple(skipped_symbols)


@dataclass(frozen=True, slots=True)
class SnapshotBatch(Sequence[MarketSnapshot]):
    snapshots: tuple[MarketSnapshot, ...]
    skipped_symbols: tuple[SkippedSymbol, ...] = ()

    def __len__(self) -> int:
        return len(self.snapshots)

    def __iter__(self) -> Iterator[MarketSnapshot]:
        return iter(self.snapshots)

    def __getitem__(self, index):
        return self.snapshots[index]


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

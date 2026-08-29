from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import asdict, dataclass

from market_brain.engines.features import compute_features
from market_brain.engines.ranking import score_features
from market_brain.providers import build_market_data_provider
from market_brain.providers.base import SkippedSymbol


@dataclass(frozen=True, slots=True)
class ScreenResult(Sequence[dict]):
    rows: tuple[dict, ...]
    skipped_symbols: tuple[SkippedSymbol, ...] = ()

    def __len__(self) -> int:
        return len(self.rows)

    def __iter__(self) -> Iterator[dict]:
        return iter(self.rows)

    def __getitem__(self, index):
        return self.rows[index]


class MarketScreener:
    def __init__(self, provider=None):
        self.provider = provider or build_market_data_provider()

    async def screen(self, symbols: list[str], top_n: int = 10) -> ScreenResult:
        snapshots = await self.provider.snapshots(symbols, decision=False)
        skipped = tuple(getattr(snapshots, "skipped_symbols", ()))
        rows: list[dict] = []
        for snapshot in snapshots:
            features = compute_features(snapshot)
            score = score_features(features)
            rows.append(
                {
                    "snapshot": asdict(snapshot),
                    "features": asdict(features),
                    "score": asdict(score),
                    "state": "DISCOVERED",
                }
            )
        rows.sort(key=lambda row: row["score"]["discovery_total"], reverse=True)
        return ScreenResult(tuple(rows[:top_n]), skipped)

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import asdict, dataclass

from market_brain.engines.features import (
    apply_ranking_context,
    compute_features,
    price_return_pct,
)
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
    def __init__(self, provider=None, *, store=None):
        self.provider = provider or build_market_data_provider()
        self.store = store

    async def screen(
        self,
        symbols: list[str],
        top_n: int = 10,
        *,
        now=None,
        structure_score: float = 0.0,
        rr_score: float = 0.0,
    ) -> ScreenResult:
        snapshots = await self.provider.snapshots(symbols, decision=False)
        skipped = tuple(getattr(snapshots, "skipped_symbols", ()))
        profiles = {}
        if self.store is not None:
            profiles = {
                profile.symbol.upper(): profile
                for profile in await self.store.list_liquidity_profiles()
            }
        benchmark = next(
            (snapshot for snapshot in snapshots if snapshot.symbol.upper() == "SPY"),
            None,
        )
        benchmark_return = price_return_pct(benchmark) if benchmark is not None else None
        rows: list[dict] = []
        for snapshot in snapshots:
            if now is not None:
                apply_ranking_context(
                    snapshot,
                    profiles.get(snapshot.symbol.upper()),
                    benchmark_return_pct=benchmark_return,
                    now=now,
                )
            features = compute_features(snapshot)
            score = score_features(
                features,
                structure_score=structure_score,
                rr_score=rr_score,
            )
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

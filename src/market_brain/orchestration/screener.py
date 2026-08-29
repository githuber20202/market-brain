from __future__ import annotations

from dataclasses import asdict

from market_brain.engines.features import compute_features
from market_brain.engines.ranking import score_features
from market_brain.providers import build_market_data_provider


class MarketScreener:
    def __init__(self, provider=None):
        self.provider = provider or build_market_data_provider()

    async def screen(self, symbols: list[str], top_n: int = 10) -> list[dict]:
        snapshots = await self.provider.snapshots(symbols, decision=False)
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
        return rows[:top_n]

from __future__ import annotations

from market_brain.providers.alpaca import AlpacaMarketData
from market_brain.providers.yahoo import YahooMarketData
from market_brain.settings import Settings, settings


def build_market_data_provider(cfg: Settings = settings, *, event_store=None):
    if cfg.data_plan == "keyless_delayed":
        return YahooMarketData(cfg)
    return AlpacaMarketData(cfg, event_store=event_store)


__all__ = ["build_market_data_provider"]

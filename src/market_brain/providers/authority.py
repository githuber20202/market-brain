from __future__ import annotations

from market_brain.settings import Settings, settings


def source_is_authoritative(source_id: str | None, cfg: Settings = settings) -> bool:
    if not source_id:
        return False
    allowed = {item.strip().upper() for item in cfg.authoritative_source_ids.split(',') if item.strip()}
    return source_id.strip().upper() in allowed


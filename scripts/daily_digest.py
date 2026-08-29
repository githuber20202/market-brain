from __future__ import annotations

import asyncio
import json
from dataclasses import asdict

from market_brain.ledger.store import PostgresEventStore
from market_brain.runtime.daily_digest import DailyDigest
from market_brain.settings import settings


async def main() -> None:
    if not settings.postgres_dsn:
        raise RuntimeError("POSTGRES_DSN_MISSING")
    store = PostgresEventStore(settings.postgres_dsn)
    try:
        alert = await DailyDigest(store).create()
        print(json.dumps(asdict(alert) if alert else {"status": "ALREADY_CREATED"}, default=str))
    finally:
        close = getattr(store, "close", None)
        if close is not None:
            await close()


if __name__ == "__main__":
    asyncio.run(main())


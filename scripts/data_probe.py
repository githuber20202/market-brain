from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from market_brain.providers.yahoo import YahooMarketData
from market_brain.settings import Settings


async def async_main() -> int:
    now = datetime.now(UTC)
    provider = YahooMarketData(Settings(data_plan="keyless_delayed"))
    try:
        for symbol in ("SPY", "NVDA"):
            snapshot = await provider.snapshot(symbol)
            minute_bars = await provider.bars(
                symbol,
                "1Min",
                now - timedelta(days=7),
                now + timedelta(minutes=1),
            )
            daily_bars = await provider.bars(
                symbol,
                "1Day",
                now - timedelta(days=366),
                now + timedelta(days=1),
            )
            cboe = await provider.cboe.quote(symbol)
            print(
                " ".join(
                    (
                        f"symbol={symbol}",
                        f"source={snapshot.source_id}",
                        f"delay_minutes={snapshot.delay_minutes:.2f}",
                        f"bars_1m={len(minute_bars)}",
                        f"bars_1d={len(daily_bars)}",
                        f"last_timestamp={snapshot.metadata['last_bar_timestamp']}",
                        f"cboe_source={cboe.source_id}",
                        f"cboe_delay_minutes={cboe.delay_minutes_at(now):.2f}",
                    )
                )
            )
        print("KEYLESS_DATA_PROBE=PASS")
    finally:
        await provider.aclose()
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(async_main()))


if __name__ == "__main__":
    main()

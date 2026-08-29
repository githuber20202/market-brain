from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from market_brain.replay.engine import ReplayEngine


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", type=Path, required=True)
    args = parser.parse_args()
    fixture = json.loads(args.fixture.read_text())
    with TemporaryDirectory() as directory:
        report = await ReplayEngine(output_dir=Path(directory)).run(
            fixture["date"],
            sorted(fixture["symbols"]),
            bars_by_symbol=fixture["symbols"],
        )
    if report["trade_count"] == 0 or not any(row["r"] != 0 for row in report["trades"]):
        raise SystemExit("REPLAY_SMOKE=FAIL")
    print(f"REPLAY_SMOKE=PASS trades={report['trade_count']}")


if __name__ == "__main__":
    asyncio.run(main())


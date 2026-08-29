from __future__ import annotations

import argparse
import asyncio
import csv
import json
from datetime import date, timedelta
from pathlib import Path

from market_brain.orchestration.universe import load_universe, normalize_symbol
from market_brain.providers.alpaca import AlpacaMarketData
from market_brain.replay.engine import ReplayEngine, replay_summary
from market_brain.settings import ROOT


def load_symbols(path: Path) -> list[str]:
    if path.is_dir():
        return sorted(entry.symbol for entry in load_universe(path) if entry.ranking_eligible)
    try:
        handle = path.open(newline="", encoding="utf-8-sig")
    except OSError as exc:
        raise RuntimeError("REPLAY_UNIVERSE_UNREADABLE") from exc
    with handle:
        reader = csv.DictReader(handle)
        headers = set(reader.fieldnames or [])
        field = "symbol" if "symbol" in headers else "ticker" if "ticker" in headers else None
        if field is None:
            raise RuntimeError("REPLAY_UNIVERSE_SYMBOL_COLUMN_MISSING")
        symbols = [normalize_symbol(row.get(field, "")) for row in reader]
    if len(set(symbols)) != len(symbols):
        raise ValueError("REPLAY_UNIVERSE_DUPLICATE_SYMBOL")
    if not symbols:
        raise ValueError("REPLAY_UNIVERSE_EMPTY")
    return sorted(symbols)


async def run_range(start: date, end: date, symbols: list[str], output_dir: Path) -> dict:
    if end < start:
        raise ValueError("REPLAY_RANGE_INVALID")
    engine = ReplayEngine(AlpacaMarketData(), output_dir=output_dir)
    reports = []
    current = start
    while current <= end:
        reports.append(await engine.run(current, symbols))
        current += timedelta(days=1)
    trades = [trade for report in reports for trade in report["trades"]]
    trades.sort(key=lambda row: (row["entry_at"], row["symbol"]))
    summary = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "sessions": len(reports),
        "symbols": sorted(symbols),
        **replay_summary(trades),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"replay_range_{start.isoformat()}_{end.isoformat()}.json"
    path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    return summary


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--universe", type=Path, required=True)
    args = parser.parse_args()
    summary = await run_range(args.start, args.end, load_symbols(args.universe), ROOT / "reports")
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(main())


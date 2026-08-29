from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
from datetime import UTC, datetime
from pathlib import Path

from market_brain.engines.quality_scorer import EdgarQualityScore, score_companyfacts
from market_brain.orchestration.universe import load_universe
from market_brain.providers.base import DataUnavailable
from market_brain.providers.edgar import EdgarCompanyFacts
from market_brain.settings import DATA_DIR


async def refresh_quality(
    symbols: list[str],
    *,
    output_path: Path,
    now: datetime,
    provider: EdgarCompanyFacts | None = None,
) -> dict:
    timestamp = _aware(now)
    edgar = provider or EdgarCompanyFacts()
    owns_provider = provider is None
    scores: list[EdgarQualityScore] = []
    missing: list[dict[str, str]] = []
    try:
        await edgar.ticker_map()
        for symbol in _symbols(symbols):
            try:
                payload = await edgar.companyfacts(symbol)
                scores.append(
                    score_companyfacts(symbol, payload, as_of=timestamp)
                )
            except DataUnavailable as exc:
                missing.append(
                    {"symbol": symbol, "error_type": exc.error_type}
                )
    finally:
        if owns_provider:
            await edgar.aclose()

    _write_quality_csv(output_path, scores)
    summary = {
        "status": "COMPLETED",
        "as_of": timestamp.isoformat(),
        "universe": len(_symbols(symbols)),
        "rows": len(scores),
        "partial": sum(score.partial for score in scores),
        "complete": sum(not score.partial for score in scores),
        "missing": missing,
        "output": str(output_path),
    }
    print(f"QUALITY_REFRESH={json.dumps(summary, sort_keys=True)}")
    return summary


def _write_quality_csv(path: Path, scores: list[EdgarQualityScore]) -> None:
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=("symbol", "quality_score", "as_of", "source", "partial"),
        lineterminator="\n",
    )
    writer.writeheader()
    for score in scores:
        writer.writerow(
            {
                "symbol": score.symbol,
                "quality_score": score.quality_score,
                "as_of": score.as_of.isoformat(),
                "source": score.source,
                "partial": str(score.partial).lower(),
            }
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(buffer.getvalue(), encoding="utf-8")


def _symbols(values: list[str]) -> list[str]:
    return list(dict.fromkeys(value.upper().strip() for value in values if value.strip()))


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


async def async_main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--universe", type=Path, default=DATA_DIR / "universe")
    parser.add_argument("--output", type=Path, default=DATA_DIR / "quality.csv")
    parser.add_argument("--now", help="UTC/offset ISO timestamp; tests use only")
    args = parser.parse_args()
    now = datetime.fromisoformat(args.now) if args.now else datetime.now(UTC)
    symbols = [entry.symbol for entry in load_universe(args.universe)]
    await refresh_quality(symbols, output_path=args.output, now=now)
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(async_main()))


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
from datetime import UTC, datetime
from pathlib import Path

from market_brain.engines.quality_scorer import (
    QualityScore,
    score_companyfacts,
    score_yahoo_fundamentals,
)
from market_brain.orchestration.universe import load_universe
from market_brain.providers.base import DataUnavailable
from market_brain.providers.edgar import EdgarCompanyFacts
from market_brain.providers.yahoo_fundamentals import YahooFundamentals
from market_brain.settings import DATA_DIR, settings


async def refresh_quality(
    symbols: list[str],
    *,
    output_path: Path,
    now: datetime,
    quality_source: str = "yahoo",
    provider: EdgarCompanyFacts | YahooFundamentals | None = None,
    skipped_instruments: list[dict[str, str]] | None = None,
) -> dict:
    timestamp = _aware(now)
    selected_source = quality_source.strip().lower()
    if isinstance(provider, EdgarCompanyFacts):
        selected_source = "edgar"
    elif isinstance(provider, YahooFundamentals):
        selected_source = "yahoo"
    if selected_source not in {"edgar", "yahoo"}:
        raise ValueError("QUALITY_SOURCE_INVALID")
    quality_provider = provider or (
        EdgarCompanyFacts() if selected_source == "edgar" else YahooFundamentals()
    )
    owns_provider = provider is None
    scores: list[QualityScore] = []
    missing: list[dict[str, str]] = []
    try:
        if selected_source == "edgar":
            assert isinstance(quality_provider, EdgarCompanyFacts)
            await quality_provider.ticker_map()
        for symbol in _symbols(symbols):
            try:
                if selected_source == "edgar":
                    assert isinstance(quality_provider, EdgarCompanyFacts)
                    payload = await quality_provider.companyfacts(symbol)
                    score = score_companyfacts(symbol, payload, as_of=timestamp)
                else:
                    assert isinstance(quality_provider, YahooFundamentals)
                    snapshot = await quality_provider.fundamentals(symbol)
                    score = score_yahoo_fundamentals(snapshot, as_of=timestamp)
                scores.append(score)
            except DataUnavailable as exc:
                missing.append(
                    {"symbol": symbol, "error_type": exc.error_type}
                )
    finally:
        if owns_provider:
            await quality_provider.aclose()

    _write_quality_csv(output_path, scores)
    summary = {
        "status": "COMPLETED",
        "source": scores[0].source if scores else (
            "EDGAR_AUTO" if selected_source == "edgar" else "YAHOO_FUNDAMENTALS"
        ),
        "as_of": timestamp.isoformat(),
        "universe": len(_symbols(symbols)),
        "rows": len(scores),
        "partial": sum(score.partial for score in scores),
        "complete": sum(not score.partial for score in scores),
        "missing": missing,
        "skipped_instruments": skipped_instruments or [],
        "output": str(output_path),
    }
    print(f"QUALITY_REFRESH={json.dumps(summary, sort_keys=True)}")
    return summary


def _write_quality_csv(path: Path, scores: list[QualityScore]) -> None:
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
    parser.add_argument(
        "--source",
        choices=("edgar", "yahoo"),
        default=settings.quality_source,
    )
    parser.add_argument("--now", help="UTC/offset ISO timestamp; tests use only")
    args = parser.parse_args()
    now = datetime.fromisoformat(args.now) if args.now else datetime.now(UTC)
    universe = load_universe(args.universe)
    symbols = [entry.symbol for entry in universe if entry.instrument_type == "EQUITY"]
    skipped = [
        {"symbol": entry.symbol, "instrument_type": entry.instrument_type}
        for entry in universe
        if entry.instrument_type != "EQUITY"
    ]
    await refresh_quality(
        symbols,
        output_path=args.output,
        now=now,
        quality_source=args.source,
        skipped_instruments=skipped,
    )
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(async_main()))


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from market_brain.orchestration.universe import (
    EASTERN,
    NyseMarketCalendar,
    load_market_calendar,
    load_universe,
)
from market_brain.providers.alpaca import AlpacaMarketData
from market_brain.replay.engine import ReplayEngine, replay_summary
from market_brain.settings import ROOT, settings


def last_trading_days(
    calendar: NyseMarketCalendar,
    *,
    days: int,
    before: date,
) -> list[date]:
    if days <= 0:
        raise ValueError("REPLAY_REPORT_DAYS_INVALID")
    sessions: list[date] = []
    cursor = before - timedelta(days=1)
    while len(sessions) < days:
        if calendar.session_for(cursor) is not None:
            sessions.append(cursor)
        cursor -= timedelta(days=1)
    return sorted(sessions)


async def create_replay_report(
    *,
    days: int,
    symbols: list[str],
    calendar: NyseMarketCalendar,
    engine: ReplayEngine,
    output_dir: Path,
    now: datetime | None = None,
    fixture_bars: dict[str, dict[str, list[dict]]] | None = None,
) -> Path:
    timestamp = (now or datetime.now(UTC)).astimezone(EASTERN)
    sessions = last_trading_days(calendar, days=days, before=timestamp.date())
    reports = []
    for session_date in sessions:
        bars = None
        if fixture_bars is not None:
            bars = fixture_bars.get(session_date.isoformat(), {})
        reports.append(
            await engine.run(
                session_date,
                symbols,
                bars_by_symbol=bars,
                write_report=False,
            )
        )
    trades = [trade for report in reports for trade in report["trades"]]
    trades.sort(key=lambda row: (row["entry_at"], row["symbol"]))
    markdown = render_replay_markdown(sessions, symbols, trades)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"replay_{sessions[0].isoformat()}_{sessions[-1].isoformat()}.md"
    path.write_text(markdown, encoding="utf-8")
    return path


def render_replay_markdown(
    sessions: list[date],
    symbols: list[str],
    trades: list[dict],
) -> str:
    summary = replay_summary(trades)
    lines = [
        f"# Replay report: {sessions[0].isoformat()} to {sessions[-1].isoformat()}",
        "",
        f"- Trading sessions: {len(sessions)}",
        f"- Universe symbols: {len(symbols)}",
        f"- Trades: {summary['trade_count']}",
        f"- Hit rate: {summary['hit_rate']:.2%}",
        f"- Expectancy: {summary['expectancy_r']:.3f} R",
        f"- Max drawdown: {summary['max_drawdown_r']:.3f} R",
        "",
        "## By symbol",
        "",
        "| Symbol | Trades | Hit rate | Expectancy (R) | Max drawdown (R) |",
        "|---|---:|---:|---:|---:|",
    ]
    for symbol in sorted(symbols):
        rows = [row for row in trades if row["symbol"] == symbol]
        metrics = replay_summary(rows)
        lines.append(
            f"| {symbol} | {metrics['trade_count']} | {metrics['hit_rate']:.2%} | "
            f"{metrics['expectancy_r']:.3f} | {metrics['max_drawdown_r']:.3f} |"
        )
    lines.extend(
        [
            "",
            "## Trades",
            "",
            "| Entry (UTC) | Symbol | Fill | Stop | TP1 | TP2 | Exit legs | R |",
            "|---|---|---:|---:|---:|---:|---|---:|",
        ]
    )
    if not trades:
        lines.append("| — | — | — | — | — | — | no trades | — |")
    for trade in trades:
        legs = ", ".join(
            f"{row['reason']} {row['fraction']:.2f}@{row['price']:.2f}"
            for row in trade["exit_legs"]
        )
        lines.append(
            f"| {trade['entry_at']} | {trade['symbol']} | {trade['fill']:.2f} | "
            f"{trade['stop']:.2f} | {trade['tp1']:.2f} | {trade['tp2']:.2f} | "
            f"{legs} | {trade['r']:.3f} |"
        )
    return "\n".join(lines) + "\n"


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=20)
    args = parser.parse_args()
    timestamp = datetime.now(UTC).astimezone(EASTERN)
    calendar = load_market_calendar(
        settings.market_calendar_path,
        required_years={timestamp.year},
    )
    symbols = sorted(
        entry.symbol
        for entry in load_universe(settings.universe_dir)
        if entry.ranking_eligible
    )
    path = await create_replay_report(
        days=args.days,
        symbols=symbols,
        calendar=calendar,
        engine=ReplayEngine(AlpacaMarketData()),
        output_dir=ROOT / "reports",
    )
    print(path)


if __name__ == "__main__":
    asyncio.run(main())

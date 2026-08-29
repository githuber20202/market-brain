from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from market_brain.ledger.store import PostgresEventStore
from market_brain.orchestration.universe import EASTERN
from market_brain.runtime.shadow import shadow_metrics
from market_brain.settings import ROOT, settings


async def create_shadow_report(
    store,
    *,
    week_start: date,
    output_dir: Path,
) -> Path:
    week_end = week_start + timedelta(days=6)
    trades = [
        row
        for row in await store.list_shadow_trades()
        if week_start <= row.opened_at.astimezone(EASTERN).date() <= week_end
    ]
    events = [
        row
        for row in await store.read_events()
        if week_start <= row.occurred_at.astimezone(EASTERN).date() <= week_end
    ]
    metrics = shadow_metrics(trades, events)
    markdown = render_shadow_markdown(week_start, week_end, metrics)
    output_dir.mkdir(parents=True, exist_ok=True)
    iso_year, iso_week, _ = week_start.isocalendar()
    path = output_dir / f"shadow_{iso_year}-W{iso_week:02d}.md"
    path.write_text(markdown, encoding="utf-8")
    return path


def render_shadow_markdown(week_start: date, week_end: date, metrics: dict) -> str:
    lines = [
        f"# Shadow report: {week_start.isoformat()} to {week_end.isoformat()}",
        "",
        f"- Signals: {metrics['signals']}",
        f"- Virtual trades: {metrics['trades']}",
        f"- No trigger: {metrics['no_trigger']}",
        f"- Closed trades: {metrics['trade_count']}",
        f"- Hit rate: {metrics['hit_rate']:.2%}",
        f"- Expectancy: {metrics['expectancy_r']:.3f} R",
        f"- Max drawdown: {metrics['max_drawdown_r']:.3f} R",
        "",
        "## By setup",
        "",
        "| Setup | Trades | Closed | Hit rate | Expectancy (R) | Max drawdown (R) |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    if not metrics["by_setup"]:
        lines.append("| — | 0 | 0 | 0.00% | 0.000 | 0.000 |")
    for setup, row in metrics["by_setup"].items():
        lines.append(
            f"| {setup} | {row['trades']} | {row['trade_count']} | {row['hit_rate']:.2%} | "
            f"{row['expectancy_r']:.3f} | {row['max_drawdown_r']:.3f} |"
        )
    return "\n".join(lines) + "\n"


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--week", help="ISO week as YYYY-Www; defaults to the current ET week")
    args = parser.parse_args()
    local_date = datetime.now(UTC).astimezone(EASTERN).date()
    if args.week:
        try:
            year_text, week_text = args.week.split("-W", 1)
            week_start = date.fromisocalendar(int(year_text), int(week_text), 1)
        except (TypeError, ValueError) as exc:
            raise ValueError("SHADOW_REPORT_WEEK_INVALID") from exc
    else:
        week_start = local_date - timedelta(days=local_date.weekday())
    if not settings.postgres_dsn:
        raise RuntimeError("POSTGRES_DSN_MISSING")
    store = PostgresEventStore(settings.postgres_dsn)
    try:
        path = await create_shadow_report(
            store,
            week_start=week_start,
            output_dir=ROOT / "reports",
        )
        print(path)
    finally:
        await store.close()


if __name__ == "__main__":
    asyncio.run(main())

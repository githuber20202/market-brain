from __future__ import annotations

import argparse
import csv
import os
from datetime import UTC, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

EASTERN = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parents[1]


def is_market_session(day, calendar_path: Path) -> bool:
    if day.weekday() >= 5:
        return False
    with calendar_path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            if row.get("date") == day.isoformat():
                return row.get("status", "").upper() != "CLOSED"
    return True


def should_run(
    mode: str,
    now: datetime,
    calendar_path: Path,
    *,
    force: bool = False,
) -> bool:
    if force:
        return True
    local = now.astimezone(EASTERN)
    if not is_market_session(local.date(), calendar_path):
        return False
    minute = local.time().replace(second=0, microsecond=0)
    if mode == "radar":
        return time(9, 50) <= minute < time(15, 20)
    if mode == "digest":
        return time(16, 20) <= minute < time(23, 59)
    raise ValueError("BATCH_GATE_MODE_INVALID")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("radar", "digest"), required=True)
    parser.add_argument("--now", help="ISO timestamp for deterministic tests")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Bypass only this workflow gate; the batch still enforces calendar and due state",
    )
    args = parser.parse_args()
    now = datetime.fromisoformat(args.now) if args.now else datetime.now(UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    due = should_run(
        args.mode,
        now,
        ROOT / "data" / "market_calendar.csv",
        force=args.force,
    )
    value = "true" if due else "false"
    output = os.getenv("GITHUB_OUTPUT")
    if output:
        with Path(output).open("a", encoding="utf-8") as handle:
            handle.write(f"due={value}\n")
    forced = "true" if args.force else "false"
    print(
        f"BATCH_GATE mode={args.mode} due={value} forced={forced} "
        f"et={now.astimezone(EASTERN).isoformat()}"
    )


if __name__ == "__main__":
    main()

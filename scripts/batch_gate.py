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
        return time(9, 50) <= minute <= time(15, 20)
    if mode == "premarket":
        return premarket_checkpoint(now, calendar_path) is not None
    if mode == "digest":
        return time(16, 20) <= minute < time(23, 59)
    raise ValueError("BATCH_GATE_MODE_INVALID")


def premarket_checkpoint(
    now: datetime,
    calendar_path: Path,
    *,
    force: bool = False,
    forced_checkpoint: str | None = None,
) -> str | None:
    local = now.astimezone(EASTERN)
    if not is_market_session(local.date(), calendar_path):
        return None
    if force:
        if forced_checkpoint not in {"T-30", "T-12", "T-3"}:
            raise ValueError("PREMARKET_FORCED_CHECKPOINT_INVALID")
        return forced_checkpoint
    minute = local.time().replace(second=0, microsecond=0)
    if time(8, 55) <= minute < time(9, 12):
        return "T-30"
    if time(9, 12) <= minute < time(9, 24):
        return "T-12"
    if time(9, 24) <= minute < time(9, 30):
        return "T-3"
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("premarket", "radar", "digest"), required=True)
    parser.add_argument("--now", help="ISO timestamp for deterministic tests")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Bypass only this workflow gate; the batch still enforces calendar and due state",
    )
    parser.add_argument("--checkpoint", choices=("T-30", "T-12", "T-3"))
    args = parser.parse_args()
    now = datetime.fromisoformat(args.now) if args.now else datetime.now(UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    calendar_path = ROOT / "data" / "market_calendar.csv"
    checkpoint = None
    if args.mode == "premarket":
        checkpoint = premarket_checkpoint(
            now,
            calendar_path,
            force=args.force,
            forced_checkpoint=args.checkpoint,
        )
        due = checkpoint is not None
    else:
        if args.checkpoint:
            parser.error("--checkpoint requires --mode premarket")
        due = should_run(
            args.mode,
            now,
            calendar_path,
            force=args.force,
        )
    value = "true" if due else "false"
    output = os.getenv("GITHUB_OUTPUT")
    if output:
        with Path(output).open("a", encoding="utf-8") as handle:
            handle.write(f"due={value}\n")
            if checkpoint is not None:
                handle.write(f"checkpoint={checkpoint}\n")
    forced = "true" if args.force else "false"
    print(
        f"BATCH_GATE mode={args.mode} due={value} checkpoint={checkpoint} forced={forced} "
        f"et={now.astimezone(EASTERN).isoformat()}"
    )


if __name__ == "__main__":
    main()

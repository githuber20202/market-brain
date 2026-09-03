from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Any

RADAR_COLUMNS = (
    "run_id",
    "scheduled_for",
    "run_status",
    "rank",
    "symbol",
    "data_status",
    "catalyst_or_continuation",
    "price_momentum",
    "volume_liquidity",
    "relative_strength_sector",
    "entry_invalidation_structure",
    "risk_reward",
    "total",
    "discovery_total",
    "reasons",
    "last",
    "volume",
    "relative_volume",
    "plan_id",
)


def append_radar_csv(payload: dict[str, Any], reports_dir: Path) -> Path:
    scheduled_for = datetime.fromisoformat(str(payload["scheduled_for"]))
    directory = reports_dir / "radar"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{scheduled_for.date().isoformat()}.csv"
    run_id = str(payload["run_id"])
    if _contains_run(path, run_id):
        return path
    rows = payload.get("rankings")
    if not isinstance(rows, list) or not rows:
        raise RuntimeError("RADAR_RANKING_TABLE_MISSING")
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=RADAR_COLUMNS)
        if write_header:
            writer.writeheader()
        for ranking in rows:
            if not isinstance(ranking, dict):
                raise TypeError("RADAR_RANKING_ROW_INVALID")
            reasons = ranking.get("reasons")
            reason_text = (
                "|".join(str(value) for value in reasons)
                if isinstance(reasons, list)
                else str(reasons or "")
            )
            writer.writerow(
                {
                    "run_id": run_id,
                    "scheduled_for": payload["scheduled_for"],
                    "run_status": payload.get("status"),
                    **{key: ranking.get(key) for key in RADAR_COLUMNS[3:]},
                    "reasons": reason_text,
                }
            )
    _retain_ten_sessions(directory)
    return path


def _contains_run(path: Path, run_id: str) -> bool:
    if not path.exists():
        return False
    with path.open(newline="", encoding="utf-8") as handle:
        return any(row.get("run_id") == run_id for row in csv.DictReader(handle))


def _retain_ten_sessions(directory: Path) -> None:
    files = sorted(directory.glob("????-??-??.csv"))
    for stale in files[:-10]:
        stale.unlink()

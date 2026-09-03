from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, time, timedelta
from typing import Any

from market_brain.orchestration.universe import EASTERN

RADAR_OK = "COMPLETED"
RADAR_UNAVAILABLE = "DATA_UNAVAILABLE"
PREMARKET_CHECKPOINTS = ("T-30", "T-12", "T-3")


def expected_radar_slots(session_date: date) -> tuple[datetime, ...]:
    """Return the eleven discovery slots used by the existing radar policy."""
    first = datetime.combine(session_date, time(9, 50), EASTERN)
    return tuple(first + timedelta(minutes=30 * index) for index in range(11))


def coverage_for_events(events: Iterable[Any], session_date: date) -> dict[str, Any]:
    """Classify schedule evidence independently from which workflows happened to run."""
    date_text = session_date.isoformat()
    radar_latest: dict[str, dict[str, Any]] = {}
    premarket_latest: dict[str, dict[str, Any]] = {}
    for event in events:
        payload = event.payload if isinstance(event.payload, dict) else {}
        if event.event_type == "RADAR_RUN" and event.aggregate_id.startswith(
            f"radar:{date_text}:"
        ):
            radar_latest[event.aggregate_id] = payload
        elif event.event_type == "PREMARKET_RUN" and event.aggregate_id.startswith(
            f"premarket:{date_text}:"
        ):
            checkpoint = str(payload.get("checkpoint") or event.aggregate_id.rsplit(":", 1)[-1])
            if checkpoint in PREMARKET_CHECKPOINTS:
                premarket_latest[checkpoint] = payload

    radar_expected = [
        f"radar:{date_text}:{slot.strftime('%H%M')}"
        for slot in expected_radar_slots(session_date)
    ]
    radar_ok = 0
    radar_unavailable = 0
    radar_missed = 0
    radar_never_ran = 0
    incomplete: list[str] = []
    for run_id in radar_expected:
        row = radar_latest.get(run_id)
        if row is None:
            radar_never_ran += 1
            incomplete.append(f"{run_id}:NEVER_RAN")
            continue
        status = str(row.get("status") or "UNKNOWN")
        if status == RADAR_OK:
            radar_ok += 1
        elif status == RADAR_UNAVAILABLE:
            radar_unavailable += 1
            incomplete.append(f"{run_id}:DATA_UNAVAILABLE")
        else:
            radar_missed += 1
            incomplete.append(f"{run_id}:{status}")

    premarket_ok = 0
    premarket_missed = 0
    premarket_never_ran = 0
    for checkpoint in PREMARKET_CHECKPOINTS:
        run_id = f"premarket:{date_text}:{checkpoint}"
        row = premarket_latest.get(checkpoint)
        if row is None:
            premarket_never_ran += 1
            premarket_missed += 1
            incomplete.append(f"{run_id}:NEVER_RAN")
            continue
        status = str(row.get("status") or "UNKNOWN")
        if status == "COMPLETED":
            premarket_ok += 1
        else:
            premarket_missed += 1
            incomplete.append(f"{run_id}:{status}")

    attempts = len(radar_latest) + len(premarket_latest)
    if attempts == 0:
        session_status = "NEVER_RAN"
    elif incomplete:
        session_status = "INCOMPLETE"
    else:
        session_status = "COMPLETE"
    learning_status = "READY" if session_status == "COMPLETE" else "BLOCKED"
    return {
        "radar": {
            "expected": len(radar_expected),
            "ok": radar_ok,
            "unavailable": radar_unavailable,
            "missed": radar_missed,
            "never_ran": radar_never_ran,
        },
        "premarket": {
            "expected": len(PREMARKET_CHECKPOINTS),
            "ok": premarket_ok,
            "missed": premarket_missed,
            "never_ran": premarket_never_ran,
        },
        "session_status": session_status,
        "learning_status": learning_status,
        "incomplete_slots": incomplete,
    }


def coverage_line(coverage: dict[str, Any]) -> str:
    radar = coverage["radar"]
    premarket = coverage["premarket"]
    return (
        "Session coverage: "
        f"radar expected={radar['expected']} ok={radar['ok']} "
        f"unavailable={radar['unavailable']} missed={radar['missed']} "
        f"never_ran={radar['never_ran']}; "
        f"premarket expected={premarket['expected']} ok={premarket['ok']} "
        f"missed={premarket['missed']}"
    )

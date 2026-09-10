from datetime import UTC, date, datetime
from itertools import pairwise

from market_brain.ledger.events import LedgerEvent
from market_brain.runtime.coverage import (
    coverage_for_events,
    coverage_line,
    expected_radar_slots,
)


def _event(event_type: str, aggregate_id: str, status: str, **payload):
    return LedgerEvent(
        event_type,
        aggregate_id,
        {"status": status, **payload},
        occurred_at=datetime(2026, 9, 3, 20, 20, tzinfo=UTC),
    )


def test_expected_radar_slots_run_every_ten_minutes_through_1550():
    slots = expected_radar_slots(date(2026, 9, 3))

    assert len(slots) == 37
    assert slots[0].isoformat() == "2026-09-03T09:50:00-04:00"
    assert slots[-1].isoformat() == "2026-09-03T15:50:00-04:00"
    assert all(
        (later - earlier).total_seconds() == 600
        for earlier, later in pairwise(slots)
    )


def test_coverage_never_ran_is_not_reported_as_zero_successes():
    coverage = coverage_for_events([], date(2026, 9, 3))

    assert coverage["session_status"] == "NEVER_RAN"
    assert coverage["learning_status"] == "BLOCKED"
    assert coverage["radar"] == {
        "expected": 37,
        "ok": 0,
        "unavailable": 0,
        "missed": 0,
        "never_ran": 37,
    }
    assert coverage["premarket"]["missed"] == 3


def test_coverage_missing_slots_is_incomplete_and_blocked():
    events = [
        _event("PREMARKET_RUN", f"premarket:2026-09-03:{checkpoint}", "COMPLETED", checkpoint=checkpoint)
        for checkpoint in ("T-30", "T-12", "T-3")
    ]
    events.extend(
        _event("RADAR_RUN", f"radar:2026-09-03:{slot}", "COMPLETED")
        for slot in ("0950", "1000", "1010", "1020", "1030")
    )

    coverage = coverage_for_events(events, date(2026, 9, 3))

    assert coverage["radar"]["ok"] == 5
    assert coverage["radar"]["never_ran"] == 32
    assert coverage["session_status"] == "INCOMPLETE"
    assert coverage["learning_status"] == "BLOCKED"
    assert coverage_line(coverage) == (
        "Session coverage: radar expected=37 ok=5 unavailable=0 missed=0 "
        "never_ran=32; premarket expected=3 ok=3 missed=0"
    )


def test_coverage_unavailable_is_fail_closed():
    events = [
        _event("PREMARKET_RUN", f"premarket:2026-09-03:{checkpoint}", "COMPLETED", checkpoint=checkpoint)
        for checkpoint in ("T-30", "T-12", "T-3")
    ]
    events.extend(
        _event(
            "RADAR_RUN",
            f"radar:2026-09-03:{slot.strftime('%H%M')}",
            "DATA_UNAVAILABLE" if slot.time().isoformat() == "09:50:00" else "COMPLETED",
        )
        for slot in expected_radar_slots(date(2026, 9, 3))
    )

    coverage = coverage_for_events(events, date(2026, 9, 3))

    assert coverage["radar"]["unavailable"] == 1
    assert coverage["session_status"] == "INCOMPLETE"
    assert coverage["learning_status"] == "BLOCKED"


def test_planning_failures_do_not_erase_complete_discovery_learning_evidence():
    session_date = date(2026, 9, 4)
    blocked_slots = {"0950", "1020", "1050"}
    events = [
        LedgerEvent(
            "PREMARKET_RUN",
            f"premarket:{session_date.isoformat()}:{checkpoint}",
            {"status": "COMPLETED", "checkpoint": checkpoint},
            occurred_at=datetime(2026, 9, 4, 20, 20, tzinfo=UTC),
        )
        for checkpoint in ("T-30", "T-12", "T-3")
    ]
    events.extend(
        LedgerEvent(
            "RADAR_RUN",
            f"radar:{session_date.isoformat()}:{slot.strftime('%H%M')}",
            {
                "status": (
                    "MISSED" if slot.strftime("%H%M") in blocked_slots else "COMPLETED"
                ),
                "discovery_status": "COMPLETED",
                "planning_status": (
                    "BLOCKED_DATA_UNAVAILABLE"
                    if slot.strftime("%H%M") in blocked_slots
                    else "COMPLETED"
                ),
                "previous_status": (
                    "DATA_UNAVAILABLE" if slot.strftime("%H%M") in blocked_slots else None
                ),
            },
            occurred_at=datetime(2026, 9, 4, 20, 20, tzinfo=UTC),
        )
        for slot in expected_radar_slots(session_date)
    )

    coverage = coverage_for_events(events, session_date)

    assert coverage["radar"] == {
        "expected": 37,
        "ok": 37,
        "unavailable": 0,
        "missed": 0,
        "never_ran": 0,
    }
    assert coverage["planning"] == {
        "expected": 37,
        "ok": 34,
        "blocked": 3,
        "not_run": 0,
    }
    assert coverage["session_status"] == "COMPLETE"
    assert coverage["planning_status"] == "BLOCKED"
    assert coverage["learning_status"] == "READY"


def test_explicit_discovery_failure_overrides_legacy_completed_status():
    session_date = date(2026, 9, 4)
    events = [
        _event(
            "RADAR_RUN",
            f"radar:{session_date.isoformat()}:{slot.strftime('%H%M')}",
            "COMPLETED",
            discovery_status=(
                "DATA_UNAVAILABLE" if slot.strftime("%H%M") == "0950" else "COMPLETED"
            ),
        )
        for slot in expected_radar_slots(session_date)
    ]
    events.extend(
        _event(
            "PREMARKET_RUN",
            f"premarket:{session_date.isoformat()}:{checkpoint}",
            "COMPLETED",
            checkpoint=checkpoint,
        )
        for checkpoint in ("T-30", "T-12", "T-3")
    )

    coverage = coverage_for_events(events, session_date)

    assert coverage["radar"]["unavailable"] == 1
    assert coverage["session_status"] == "INCOMPLETE"
    assert coverage["learning_status"] == "BLOCKED"

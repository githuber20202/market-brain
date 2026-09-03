from datetime import UTC, date, datetime

from market_brain.ledger.events import LedgerEvent
from market_brain.runtime.coverage import coverage_for_events, coverage_line


def _event(event_type: str, aggregate_id: str, status: str, **payload):
    return LedgerEvent(
        event_type,
        aggregate_id,
        {"status": status, **payload},
        occurred_at=datetime(2026, 9, 3, 20, 20, tzinfo=UTC),
    )


def test_coverage_never_ran_is_not_reported_as_zero_successes():
    coverage = coverage_for_events([], date(2026, 9, 3))

    assert coverage["session_status"] == "NEVER_RAN"
    assert coverage["learning_status"] == "BLOCKED"
    assert coverage["radar"] == {
        "expected": 11,
        "ok": 0,
        "unavailable": 0,
        "missed": 0,
        "never_ran": 11,
    }
    assert coverage["premarket"]["missed"] == 3


def test_coverage_six_missing_is_incomplete_and_blocked():
    events = [
        _event("PREMARKET_RUN", f"premarket:2026-09-03:{checkpoint}", "COMPLETED", checkpoint=checkpoint)
        for checkpoint in ("T-30", "T-12", "T-3")
    ]
    events.extend(
        _event("RADAR_RUN", f"radar:2026-09-03:{slot}", "COMPLETED")
        for slot in ("0950", "1020", "1050", "1120", "1150")
    )

    coverage = coverage_for_events(events, date(2026, 9, 3))

    assert coverage["radar"]["ok"] == 5
    assert coverage["radar"]["never_ran"] == 6
    assert coverage["session_status"] == "INCOMPLETE"
    assert coverage["learning_status"] == "BLOCKED"
    assert coverage_line(coverage) == (
        "Session coverage: radar expected=11 ok=5 unavailable=0 missed=0 "
        "never_ran=6; premarket expected=3 ok=3 missed=0"
    )


def test_coverage_unavailable_is_fail_closed():
    events = [
        _event("PREMARKET_RUN", f"premarket:2026-09-03:{checkpoint}", "COMPLETED", checkpoint=checkpoint)
        for checkpoint in ("T-30", "T-12", "T-3")
    ]
    events.extend(
        _event(
            "RADAR_RUN",
            f"radar:2026-09-03:{slot}",
            "DATA_UNAVAILABLE" if slot == "0950" else "COMPLETED",
        )
        for slot in (
            "0950",
            "1020",
            "1050",
            "1120",
            "1150",
            "1220",
            "1250",
            "1320",
            "1350",
            "1420",
            "1450",
        )
    )

    coverage = coverage_for_events(events, date(2026, 9, 3))

    assert coverage["radar"]["unavailable"] == 1
    assert coverage["session_status"] == "INCOMPLETE"
    assert coverage["learning_status"] == "BLOCKED"

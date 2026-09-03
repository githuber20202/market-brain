import json
import subprocess
from datetime import UTC, datetime, timedelta

from scripts.session_gate import (
    active_session_runs,
    evaluate_session_gate,
    normalized_trigger,
)


def _calendar(tmp_path):
    path = tmp_path / "calendar.csv"
    path.write_text("date,status,open_time,close_time,source\n", encoding="utf-8")
    return path


def _lease(now: datetime, *, run_id: str = "100", minutes: int = 25):
    return {
        "session_id": "2026-09-03",
        "workflow_run_id": run_id,
        "acquired_at": now.isoformat(),
        "expires_at": (now + timedelta(minutes=minutes)).isoformat(),
    }


def test_gate_valid_lease_blocks_push_trigger_and_expired_lease_recovers(tmp_path):
    now = datetime(2026, 9, 3, 12, 30, tzinfo=UTC)
    blocked = evaluate_session_gate(
        now=now,
        calendar_path=_calendar(tmp_path),
        trigger=normalized_trigger("push"),
        workflow_run_id="200",
        force=False,
        lease=_lease(now),
        active_runs=[],
    )
    recovered = evaluate_session_gate(
        now=now,
        calendar_path=_calendar(tmp_path),
        trigger="schedule",
        workflow_run_id="200",
        force=False,
        lease=_lease(now, minutes=-1),
        active_runs=[],
    )

    assert (blocked.due, blocked.reason, blocked.trigger) == (False, "LEASE_HELD", "push")
    assert (recovered.due, recovered.reason) == (True, "DUE")


def test_gate_rejects_active_run_but_force_bypasses_only_the_gate(tmp_path):
    now = datetime(2026, 9, 3, 12, 30, tzinfo=UTC)
    active = evaluate_session_gate(
        now=now,
        calendar_path=_calendar(tmp_path),
        trigger="schedule",
        workflow_run_id="200",
        force=False,
        lease=None,
        active_runs=["100"],
    )
    forced = evaluate_session_gate(
        now=now,
        calendar_path=_calendar(tmp_path),
        trigger="dispatch",
        workflow_run_id="200",
        force=True,
        lease=_lease(now),
        active_runs=["100"],
    )

    assert (active.due, active.reason) == (False, "RUN_ACTIVE")
    assert (forced.due, forced.reason) == (True, "FORCE")


def test_active_run_query_excludes_current_run_and_prior_day():
    rows = [
        {"databaseId": 100, "status": "in_progress", "createdAt": "2026-09-03T12:00:00Z"},
        {"databaseId": 200, "status": "queued", "createdAt": "2026-09-03T12:01:00Z"},
        {"databaseId": 300, "status": "in_progress", "createdAt": "2026-09-02T12:00:00Z"},
        {"databaseId": 400, "status": "completed", "createdAt": "2026-09-03T12:00:00Z"},
    ]

    def runner(*_args, **_kwargs):
        return subprocess.CompletedProcess([], 0, stdout=json.dumps(rows), stderr="")

    active = active_session_runs(
        "githuber20202/market-brain",
        now=datetime(2026, 9, 3, 12, 30, tzinfo=UTC),
        current_run_id="100",
        runner=runner,
    )

    assert active == ["200"]

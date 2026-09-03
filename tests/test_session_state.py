import json
import subprocess
from datetime import UTC, datetime, timedelta

import pytest

from market_brain.runtime.session_state import (
    LEASE_MINUTES,
    inspect_lease,
    lease_held_by_other,
    renew_lease,
    verify_handoff,
    write_handoff,
    write_heartbeat,
)


def test_valid_lease_blocks_other_run_and_expired_lease_allows_recovery(tmp_path):
    now = datetime(2026, 9, 3, 12, 30, tzinfo=UTC)
    lease = renew_lease(
        tmp_path,
        now=now,
        session_id="2026-09-03",
        workflow_run_id="101",
    )

    assert lease["expires_at"] == (now + timedelta(minutes=LEASE_MINUTES)).isoformat()
    assert lease_held_by_other(
        lease,
        now=now + timedelta(minutes=24),
        session_id="2026-09-03",
        workflow_run_id="202",
    )
    assert not lease_held_by_other(
        lease,
        now=now + timedelta(minutes=25),
        session_id="2026-09-03",
        workflow_run_id="202",
    )
    assert not lease_held_by_other(
        lease,
        now=now,
        session_id="2026-09-03",
        workflow_run_id="101",
    )


@pytest.mark.parametrize(
    ("holder_status", "held_by_other", "reason"),
    [
        ("in_progress", True, "LEASE_HELD"),
        ("queued", True, "LEASE_HELD"),
        ("completed", False, "LEASE_STALE_HOLDER_DEAD"),
        ("cancelled", False, "LEASE_STALE_HOLDER_DEAD"),
    ],
)
def test_lease_checks_holder_run_liveness(
    tmp_path,
    holder_status,
    held_by_other,
    reason,
):
    now = datetime(2026, 9, 3, 12, 30, tzinfo=UTC)
    lease = renew_lease(
        tmp_path,
        now=now,
        session_id="2026-09-03",
        workflow_run_id="101",
    )

    def runner(args, **_kwargs):
        assert args[:3] == ["gh", "run", "view"]
        return subprocess.CompletedProcess(
            args,
            0,
            stdout=json.dumps({"status": holder_status}),
            stderr="",
        )

    inspection = inspect_lease(
        lease,
        now=now + timedelta(minutes=1),
        session_id="2026-09-03",
        workflow_run_id="202",
        repository="githuber20202/market-brain",
        runner=runner,
    )

    assert inspection.held_by_other is held_by_other
    assert inspection.reason == reason
    assert inspection.holder_run_id == "101"
    assert inspection.holder_status == holder_status


def test_lease_holder_query_failure_is_fail_closed(tmp_path):
    now = datetime(2026, 9, 3, 12, 30, tzinfo=UTC)
    lease = renew_lease(
        tmp_path,
        now=now,
        session_id="2026-09-03",
        workflow_run_id="101",
    )

    def runner(args, **_kwargs):
        return subprocess.CompletedProcess(args, 1, stdout="", stderr="unavailable")

    inspection = inspect_lease(
        lease,
        now=now,
        session_id="2026-09-03",
        workflow_run_id="202",
        repository="githuber20202/market-brain",
        runner=runner,
    )

    assert inspection.held_by_other
    assert inspection.reason == "LEASE_HOLDER_STATUS_UNKNOWN"


def test_valid_lease_without_holder_is_fail_closed(tmp_path):
    now = datetime(2026, 9, 3, 12, 30, tzinfo=UTC)
    lease = {
        "session_id": "2026-09-03",
        "acquired_at": now.isoformat(),
        "expires_at": (now + timedelta(minutes=25)).isoformat(),
    }

    inspection = inspect_lease(
        lease,
        now=now,
        session_id="2026-09-03",
        workflow_run_id="202",
        repository="githuber20202/market-brain",
    )

    assert inspection.held_by_other
    assert inspection.reason == "LEASE_HOLDER_INVALID"


def test_heartbeat_contains_only_public_session_fields(tmp_path):
    now = datetime(2026, 9, 3, 12, 30, tzinfo=UTC)
    heartbeat = write_heartbeat(
        tmp_path,
        now=now,
        session_id="2026-09-03",
        workflow_run_id="101",
        head_sha="abc123",
        phase="a",
        last_scheduled_tick="2026-09-03T09:00:00-04:00",
        last_completed_tick="2026-09-03T09:00:01-04:00",
        next_due_tick="2026-09-03T09:10:00-04:00",
        lease_expires_at="2026-09-03T13:00:00+00:00",
        policy_version="2026-09-02.1",
        consecutive_failures=2,
    )

    assert set(heartbeat) == {
        "session_id",
        "workflow_run_id",
        "head_sha",
        "phase",
        "last_scheduled_tick",
        "last_completed_tick",
        "next_due_tick",
        "lease_expires_at",
        "policy_version",
        "consecutive_failures",
        "as_of",
    }
    assert heartbeat["consecutive_failures"] == 2


def test_handoff_validates_restored_dump_and_fails_closed_on_mismatch(tmp_path):
    (tmp_path / "market.dump").write_bytes(b"phase-a-dump")
    handoff = write_handoff(
        tmp_path,
        session_id="2026-09-03",
        workflow_run_id="101",
        last_completed_tick="2026-09-03T12:50:00-04:00",
    )

    assert verify_handoff(tmp_path, session_id="2026-09-03") == handoff
    (tmp_path / "market.dump").write_bytes(b"different-dump")
    with pytest.raises(RuntimeError, match="HANDOFF_MISMATCH"):
        verify_handoff(tmp_path, session_id="2026-09-03")


def test_missing_handoff_is_allowed_for_phase_b_resume(tmp_path):
    assert verify_handoff(tmp_path, session_id="2026-09-03") is None

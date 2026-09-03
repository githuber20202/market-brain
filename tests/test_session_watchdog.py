import json
import subprocess
from datetime import UTC, datetime

from scripts.session_watchdog import run_watchdog


def test_watchdog_dispatches_when_lease_and_active_run_are_absent(tmp_path):
    data = tmp_path / "data"
    data.mkdir()
    (data / "market_calendar.csv").write_text(
        "date,status,open_time,close_time,source\n",
        encoding="utf-8",
    )
    calls: list[list[str]] = []

    def runner(args, **_kwargs):
        calls.append(args)
        if args[:2] == ["git", "show"]:
            return subprocess.CompletedProcess(args, 1, stdout="", stderr="missing")
        if args[:3] == ["gh", "run", "list"]:
            return subprocess.CompletedProcess(args, 0, stdout="[]", stderr="")
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    result = run_watchdog(
        now=datetime(2026, 9, 3, 12, 35, tzinfo=UTC),
        repo=tmp_path,
        repository="githuber20202/market-brain",
        workflow_run_id="900",
        runner=runner,
    )

    assert result == ("DISPATCHED", "RECOVERY_REQUIRED")
    assert any(args[:3] == ["gh", "workflow", "run"] for args in calls)


def test_watchdog_is_idle_when_another_run_is_active(tmp_path):
    data = tmp_path / "data"
    data.mkdir()
    (data / "market_calendar.csv").write_text(
        "date,status,open_time,close_time,source\n",
        encoding="utf-8",
    )
    rows = [
        {"databaseId": 901, "status": "queued", "createdAt": "2026-09-03T12:30:00Z"}
    ]
    calls: list[list[str]] = []

    def runner(args, **_kwargs):
        calls.append(args)
        if args[:2] == ["git", "show"]:
            return subprocess.CompletedProcess(args, 1, stdout="", stderr="missing")
        if args[:3] == ["gh", "run", "list"]:
            return subprocess.CompletedProcess(args, 0, stdout=json.dumps(rows), stderr="")
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    result = run_watchdog(
        now=datetime(2026, 9, 3, 12, 35, tzinfo=UTC),
        repo=tmp_path,
        repository="githuber20202/market-brain",
        workflow_run_id="900",
        runner=runner,
    )

    assert result == ("IDLE", "RUN_ACTIVE")
    assert not any(args[:3] == ["gh", "workflow", "run"] for args in calls)

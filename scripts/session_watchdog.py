from __future__ import annotations

import argparse
import os
import subprocess
from datetime import UTC, datetime, time
from pathlib import Path

from market_brain.runtime.session_state import LeaseInspection, inspect_lease
from scripts.batch_gate import EASTERN, ROOT, is_market_session
from scripts.session_gate import active_session_runs, state_lease


def watchdog_decision(
    *,
    now: datetime,
    calendar_path: Path,
    workflow_run_id: str,
    lease: dict | None,
    active_runs: list[str],
    lease_inspection: LeaseInspection | None = None,
) -> tuple[str, str]:
    local = _aware(now).astimezone(EASTERN)
    if not is_market_session(local.date(), calendar_path):
        return "IDLE", "MARKET_CLOSED"
    if local.time().replace(tzinfo=None) >= time(16, 20):
        return "IDLE", "WATCHDOG_WINDOW_CLOSED"
    lease_state = lease_inspection or inspect_lease(
        lease,
        now=now,
        session_id=local.date().isoformat(),
        workflow_run_id=workflow_run_id,
    )
    if lease_state.held_by_other:
        return "IDLE", lease_state.reason
    if active_runs:
        return "IDLE", "RUN_ACTIVE"
    reason = (
        "LEASE_STALE_HOLDER_DEAD"
        if lease_state.reason == "LEASE_STALE_HOLDER_DEAD"
        else "RECOVERY_REQUIRED"
    )
    return "DISPATCHED", reason


def run_watchdog(
    *,
    now: datetime,
    repo: Path,
    repository: str,
    workflow_run_id: str,
    runner=subprocess.run,
) -> tuple[str, str]:
    runner(
        ["git", "fetch", "origin", "shadow-state:refs/remotes/origin/shadow-state"],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
    )
    lease = state_lease(repo, runner=runner)
    lease_inspection = inspect_lease(
        lease,
        now=now,
        session_id=_aware(now).astimezone(EASTERN).date().isoformat(),
        workflow_run_id=workflow_run_id,
        repository=repository,
        runner=runner,
    )
    active = active_session_runs(
        repository,
        now=now,
        current_run_id=workflow_run_id,
        runner=runner,
    )
    action, reason = watchdog_decision(
        now=now,
        calendar_path=repo / "data" / "market_calendar.csv",
        workflow_run_id=workflow_run_id,
        lease=lease,
        active_runs=active,
        lease_inspection=lease_inspection,
    )
    if action == "DISPATCHED":
        runner(
            [
                "gh",
                "workflow",
                "run",
                "shadow-session.yml",
                "--repo",
                repository,
                "--ref",
                "main",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    local_now = _aware(now).astimezone(EASTERN).isoformat()
    print(f"WATCHDOG action={action} reason={reason} et={local_now}")
    return action, reason


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--now", help="ISO timestamp for deterministic tests")
    parser.add_argument("--repo", type=Path, default=ROOT)
    args = parser.parse_args()
    now = datetime.fromisoformat(args.now) if args.now else datetime.now(UTC)
    run_watchdog(
        now=now,
        repo=args.repo.resolve(),
        repository=os.getenv("GITHUB_REPOSITORY", "githuber20202/market-brain"),
        workflow_run_id=os.getenv("GITHUB_RUN_ID", "watchdog-local"),
    )


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


if __name__ == "__main__":
    main()

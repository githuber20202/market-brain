from __future__ import annotations

import argparse
import os
import subprocess
from datetime import UTC, datetime, time
from pathlib import Path

from market_brain.runtime.session_state import lease_held_by_other
from scripts.batch_gate import EASTERN, ROOT, is_market_session
from scripts.session_gate import active_session_runs, state_lease


def watchdog_decision(
    *,
    now: datetime,
    calendar_path: Path,
    workflow_run_id: str,
    lease: dict | None,
    active_runs: list[str],
) -> tuple[str, str]:
    local = _aware(now).astimezone(EASTERN)
    if not is_market_session(local.date(), calendar_path):
        return "IDLE", "MARKET_CLOSED"
    if local.time().replace(tzinfo=None) >= time(16, 20):
        return "IDLE", "WATCHDOG_WINDOW_CLOSED"
    if lease_held_by_other(
        lease,
        now=now,
        session_id=local.date().isoformat(),
        workflow_run_id=workflow_run_id,
    ):
        return "IDLE", "LEASE_HELD"
    if active_runs:
        return "IDLE", "RUN_ACTIVE"
    return "DISPATCHED", "RECOVERY_REQUIRED"


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
    print(f"WATCHDOG action={action} reason={reason} et={_aware(now).astimezone(EASTERN).isoformat()}")
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

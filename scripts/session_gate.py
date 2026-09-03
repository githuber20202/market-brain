from __future__ import annotations

import argparse
import json
import os
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime, time
from pathlib import Path
from typing import Any

from market_brain.runtime.session_state import LeaseInspection, inspect_lease
from scripts.batch_gate import EASTERN, ROOT, is_market_session

ACTIVE_STATUSES = {"in_progress", "queued", "waiting", "pending", "requested"}


@dataclass(frozen=True, slots=True)
class GateResult:
    due: bool
    reason: str
    et: datetime
    trigger: str


def state_lease(
    repo: Path,
    *,
    ref: str = "origin/shadow-state",
    runner=subprocess.run,
) -> dict[str, Any] | None:
    result = runner(
        ["git", "show", f"{ref}:state/lease.json"],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    try:
        payload = json.loads(result.stdout)
    except (TypeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def active_session_runs(
    repository: str,
    *,
    now: datetime,
    current_run_id: str,
    runner=subprocess.run,
) -> list[str]:
    result = runner(
        [
            "gh",
            "run",
            "list",
            "--repo",
            repository,
            "--workflow",
            "shadow-session.yml",
            "--limit",
            "100",
            "--json",
            "databaseId,status,createdAt",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    try:
        rows = json.loads(result.stdout)
    except (TypeError, json.JSONDecodeError) as exc:
        raise RuntimeError("SESSION_RUN_LIST_INVALID") from exc
    local_day = _aware(now).astimezone(EASTERN).date()
    active: list[str] = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict) or str(row.get("status")) not in ACTIVE_STATUSES:
            continue
        run_id = str(row.get("databaseId") or "")
        if not run_id or run_id == str(current_run_id):
            continue
        try:
            created = _aware(datetime.fromisoformat(str(row["createdAt"])))
        except (KeyError, ValueError):
            continue
        if created.astimezone(EASTERN).date() == local_day:
            active.append(run_id)
    return active


def trigger_contains_current_main(
    repo: Path,
    *,
    runner=subprocess.run,
) -> bool:
    try:
        fetched = runner(
            [
                "git",
                "fetch",
                "origin",
                "+main:refs/remotes/origin/main",
            ],
            cwd=repo,
            check=False,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    if fetched.returncode != 0:
        return False
    try:
        ancestor = runner(
            ["git", "merge-base", "--is-ancestor", "origin/main", "HEAD"],
            cwd=repo,
            check=False,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return ancestor.returncode == 0


def evaluate_session_gate(
    *,
    now: datetime,
    calendar_path: Path,
    trigger: str,
    workflow_run_id: str,
    force: bool,
    lease: dict[str, Any] | None,
    active_runs: list[str],
    lease_inspection: LeaseInspection | None = None,
    trigger_stale: bool = False,
) -> GateResult:
    local = _aware(now).astimezone(EASTERN)
    if force:
        return GateResult(True, "FORCE", local, trigger)
    if trigger == "push" and trigger_stale:
        return GateResult(False, "TRIGGER_STALE", local, trigger)
    if not is_market_session(local.date(), calendar_path):
        return GateResult(False, "MARKET_CLOSED", local, trigger)
    if local.time().replace(tzinfo=None) >= time(16, 35):
        return GateResult(False, "SESSION_WINDOW_CLOSED", local, trigger)
    session_id = local.date().isoformat()
    lease_state = lease_inspection or inspect_lease(
        lease,
        now=now,
        session_id=session_id,
        workflow_run_id=str(workflow_run_id),
    )
    if lease_state.held_by_other:
        return GateResult(False, lease_state.reason, local, trigger)
    if active_runs:
        return GateResult(False, "RUN_ACTIVE", local, trigger)
    reason = (
        "LEASE_STALE_HOLDER_DEAD"
        if lease_state.reason == "LEASE_STALE_HOLDER_DEAD"
        else "DUE"
    )
    return GateResult(True, reason, local, trigger)


def normalized_trigger(value: str) -> str:
    return {
        "workflow_dispatch": "dispatch",
        "schedule": "schedule",
        "push": "push",
    }.get(value, value or "unknown")


def emit_gate(result: GateResult, *, output_path: str | None = None) -> None:
    due = "true" if result.due else "false"
    if output_path:
        with Path(output_path).open("a", encoding="utf-8") as handle:
            handle.write(f"due={due}\n")
            handle.write(f"reason={result.reason}\n")
    print(
        f"SESSION_GATE due={due} reason={result.reason} "
        f"et={result.et.isoformat()} trigger={result.trigger}"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--now", help="ISO timestamp for deterministic tests")
    parser.add_argument("--trigger")
    parser.add_argument("--repo", type=Path, default=ROOT)
    args = parser.parse_args()
    now = datetime.fromisoformat(args.now) if args.now else datetime.now(UTC)
    repo = args.repo.resolve()
    subprocess.run(
        ["git", "fetch", "origin", "shadow-state:refs/remotes/origin/shadow-state"],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
    )
    repository = os.getenv("GITHUB_REPOSITORY", "githuber20202/market-brain")
    workflow_run_id = os.getenv("GITHUB_RUN_ID", "local")
    trigger = normalized_trigger(
        args.trigger or os.getenv("GITHUB_EVENT_NAME", "unknown")
    )
    trigger_stale = trigger == "push" and not trigger_contains_current_main(repo)
    lease = None if args.force else state_lease(repo)
    lease_inspection = (
        None
        if args.force
        else inspect_lease(
            lease,
            now=now,
            session_id=_aware(now).astimezone(EASTERN).date().isoformat(),
            workflow_run_id=workflow_run_id,
            repository=repository,
        )
    )
    runs = (
        []
        if args.force or trigger_stale
        else active_session_runs(
            repository,
            now=now,
            current_run_id=workflow_run_id,
        )
    )
    result = evaluate_session_gate(
        now=now,
        calendar_path=repo / "data" / "market_calendar.csv",
        trigger=trigger,
        workflow_run_id=workflow_run_id,
        force=args.force,
        lease=lease,
        active_runs=runs,
        lease_inspection=lease_inspection,
        trigger_stale=trigger_stale,
    )
    emit_gate(result, output_path=os.getenv("GITHUB_OUTPUT"))


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


if __name__ == "__main__":
    main()

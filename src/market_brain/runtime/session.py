from __future__ import annotations

import argparse
import asyncio
import os
import subprocess
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any

from market_brain.alerts.sink import GitHubIssueSink
from market_brain.domain.models import AlertRecord
from market_brain.ledger.events import LedgerEvent
from market_brain.orchestration.universe import EASTERN
from market_brain.runtime.batch import BatchRuntime, build_runtime
from market_brain.runtime.coverage import coverage_for_events, coverage_line
from market_brain.runtime.radar_scheduler import (
    DISCOVERY_END,
    DISCOVERY_INTERVAL,
    DISCOVERY_START,
    scheduled_slots,
)
from market_brain.runtime.session_state import (
    inspect_lease,
    load_policy_version,
    market_session_id,
    read_json,
    renew_lease,
    verify_handoff,
    write_handoff,
    write_heartbeat,
    write_json,
)
from market_brain.runtime.state import (
    activate_quality_from_state,
    persist_state,
    publish_state_branch,
    restore_database,
    restore_state_files,
)
from market_brain.settings import ROOT, Settings

HEARTBEAT_INTERVAL = timedelta(minutes=10)
RADAR_GRACE = timedelta(minutes=10)
PREMARKET_TARGETS = {
    "T-30": time(9, 0),
    "T-12": time(9, 18),
    "T-3": time(9, 27),
}
PREMARKET_WINDOW_ENDS = {
    "T-30": time(9, 12),
    "T-12": time(9, 24),
    "T-3": time(9, 30),
}
PHASE_STARTS = {"wait": time(0, 0), "a": time(7, 30), "b": time(13, 0)}
PHASE_ENDS = {"wait": time(7, 30), "a": time(13, 0), "b": time(16, 35)}
NEXT_PHASE = {"wait": "a", "a": "b", "b": "none"}


class RealClock:
    def now(self) -> datetime:
        return datetime.now(UTC)

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


@dataclass(frozen=True, slots=True)
class SessionTick:
    kind: str
    scheduled_for: datetime
    checkpoint: str | None = None


def select_phase(now: datetime) -> str:
    local = _aware(now).astimezone(EASTERN)
    minute = local.time().replace(second=0, microsecond=0)
    if minute < PHASE_ENDS["wait"]:
        return "wait"
    if minute < PHASE_ENDS["a"]:
        return "a"
    return "b"


def phase_window(session_date: date, phase: str) -> tuple[datetime, datetime]:
    if phase not in PHASE_STARTS:
        raise ValueError("SESSION_PHASE_INVALID")
    return (
        datetime.combine(session_date, PHASE_STARTS[phase], EASTERN),
        datetime.combine(session_date, PHASE_ENDS[phase], EASTERN),
    )


def session_ticks(session_date: date, phase: str) -> tuple[SessionTick, ...]:
    start, end = phase_window(session_date, phase)
    ticks: list[SessionTick] = []
    if phase == "a":
        ticks.extend(
            SessionTick(
                "premarket",
                datetime.combine(session_date, target, EASTERN),
                checkpoint,
            )
            for checkpoint, target in PREMARKET_TARGETS.items()
        )
    if phase in {"a", "b"}:
        radar = datetime.combine(session_date, DISCOVERY_START, EASTERN)
        radar_end = datetime.combine(session_date, DISCOVERY_END, EASTERN)
        while radar <= radar_end:
            if start <= radar < end:
                ticks.append(SessionTick("radar", radar))
            radar += DISCOVERY_INTERVAL
    if phase == "b":
        ticks.append(
            SessionTick(
                "digest",
                datetime.combine(session_date, time(16, 20), EASTERN),
            )
        )
    return tuple(sorted(ticks, key=lambda row: row.scheduled_for))


class SessionRunner:
    def __init__(
        self,
        runtime: BatchRuntime,
        *,
        state_dir: Path,
        phase: str,
        budget_minutes: int,
        workflow_run_id: str,
        head_sha: str,
        policy_version: str,
        persist: Callable[[bool], Awaitable[None]],
        clock: Any | None = None,
        repository: str | None = None,
        command_runner: Callable[..., Any] = subprocess.run,
    ) -> None:
        if phase not in {"auto", "wait", "a", "b"}:
            raise ValueError("SESSION_PHASE_INVALID")
        if budget_minutes <= 0:
            raise ValueError("SESSION_BUDGET_INVALID")
        self.runtime = runtime
        self.state_dir = state_dir
        self.requested_phase = phase
        self.budget_minutes = budget_minutes
        self.workflow_run_id = str(workflow_run_id)
        self.head_sha = head_sha
        self.policy_version = policy_version
        self.persist = persist
        self.clock = clock or RealClock()
        self.repository = repository
        self.command_runner = command_runner
        self.last_scheduled_tick: str | None = None
        self.last_completed_tick: str | None = None
        self.consecutive_failures = 0
        self._next_heartbeat: datetime | None = None
        self._has_full_checkpoint = False

    async def run(self) -> dict[str, Any]:
        started_at = _aware(self.clock.now())
        local_started = started_at.astimezone(EASTERN)
        session_id = market_session_id(started_at)
        phase = (
            select_phase(started_at)
            if self.requested_phase == "auto"
            else self.requested_phase
        )
        start, end = phase_window(local_started.date(), phase)
        budget_end = started_at + timedelta(minutes=self.budget_minutes)
        prior_heartbeat = read_json(self.state_dir / "heartbeat.json")
        if prior_heartbeat and prior_heartbeat.get("session_id") == session_id:
            self.last_scheduled_tick = prior_heartbeat.get("last_scheduled_tick")
            self.last_completed_tick = prior_heartbeat.get("last_completed_tick")
            prior_failures = prior_heartbeat.get("consecutive_failures", 0)
            if isinstance(prior_failures, int) and not isinstance(
                prior_failures, bool
            ) and prior_failures >= 0:
                self.consecutive_failures = prior_failures
        lease = read_json(self.state_dir / "lease.json")
        lease_state = inspect_lease(
            lease,
            now=started_at,
            session_id=session_id,
            workflow_run_id=self.workflow_run_id,
            repository=self.repository,
            runner=self.command_runner,
        )
        if lease_state.held_by_other:
            raise RuntimeError(f"SESSION_LEASE_HELD reason={lease_state.reason}")
        if lease_state.reason == "LEASE_STALE_HOLDER_DEAD":
            print(
                "SESSION_LEASE due=true reason=LEASE_STALE_HOLDER_DEAD "
                f"holder_run_id={lease_state.holder_run_id} "
                f"holder_status={lease_state.holder_status}"
            )

        if phase in {"a", "b"}:
            self.runtime.scheduler.validate_startup(now=started_at)
        if phase == "b":
            try:
                handoff = verify_handoff(self.state_dir, session_id=session_id)
            except RuntimeError:
                await self._record_failure(
                    phase=phase,
                    session_id=session_id,
                    now=started_at,
                    reason="HANDOFF_MISMATCH",
                )
                raise
            if handoff is None:
                print("HANDOFF_ABSENT reason=RESUME")
            else:
                print(
                    "HANDOFF_VERIFIED "
                    f"session_id={session_id} sha256={handoff['state_dump_sha256']}"
                )

        self._next_heartbeat = started_at
        if local_started >= end:
            await self._checkpoint(
                phase=phase,
                session_id=session_id,
                now=started_at,
                next_due=None,
                full=True,
            )
            print(f"SESSION_PHASE_SKIPPED phase={phase} reason=PHASE_PASSED")
            output = {
                "phase": phase,
                "ticks": 0,
                "next_phase": NEXT_PHASE[phase],
                "status": "SKIPPED",
            }
            print(
                f"SESSION_PHASE_END phase={phase} ticks=0 next_phase={NEXT_PHASE[phase]}"
            )
            return output

        if phase != "wait" and local_started < start:
            await self._checkpoint(
                phase=phase,
                session_id=session_id,
                now=started_at,
                next_due=start.isoformat(),
                full=True,
            )
            print(f"SESSION_PHASE_SKIPPED phase={phase} reason=PHASE_NOT_STARTED")
            print(
                f"SESSION_PHASE_END phase={phase} ticks=0 next_phase={NEXT_PHASE[phase]}"
            )
            return {
                "phase": phase,
                "ticks": 0,
                "next_phase": NEXT_PHASE[phase],
                "status": "SKIPPED",
            }

        await self._wait_until(
            min(start.astimezone(UTC), budget_end),
            phase=phase,
            session_id=session_id,
            phase_end=end,
            budget_end=budget_end,
            next_due=start.isoformat(),
        )
        now = _aware(self.clock.now())
        if now >= budget_end or now.astimezone(EASTERN) >= end:
            await self._checkpoint(
                phase=phase,
                session_id=session_id,
                now=now,
                next_due=start.isoformat(),
                full=True,
            )
            print(f"SESSION_PHASE_SKIPPED phase={phase} reason=BUDGET_OR_PHASE_END")
            print(
                f"SESSION_PHASE_END phase={phase} ticks=0 next_phase={NEXT_PHASE[phase]}"
            )
            return {
                "phase": phase,
                "ticks": 0,
                "next_phase": NEXT_PHASE[phase],
                "status": "SKIPPED",
            }

        missed = await self._mark_missed(now)
        pending = list(session_ticks(local_started.date(), phase))
        pending = [row for row in pending if not self._expired_tick(row, now)]
        tick_count = 0
        await self._checkpoint(
            phase=phase,
            session_id=session_id,
            now=now,
            next_due=self._next_due(pending),
            full=bool(missed),
        )

        try:
            while True:
                now = _aware(self.clock.now())
                local_now = now.astimezone(EASTERN)
                if now >= budget_end or local_now >= end:
                    break
                if self._next_heartbeat is not None and now >= self._next_heartbeat:
                    await self._checkpoint(
                        phase=phase,
                        session_id=session_id,
                        now=now,
                        next_due=self._next_due(pending),
                        full=False,
                    )
                    continue
                missed_now = await self._mark_missed(now)
                if missed_now:
                    await self._checkpoint(
                        phase=phase,
                        session_id=session_id,
                        now=now,
                        next_due=self._next_due(pending),
                        full=True,
                    )

                due = [row for row in pending if row.scheduled_for <= local_now]
                if due:
                    pending = [row for row in pending if row not in due]
                    actionable = [row for row in due if not self._expired_tick(row, now)]
                    tick = actionable[-1] if actionable else None
                    if tick is not None:
                        tick_count += 1
                        self.last_scheduled_tick = tick.scheduled_for.isoformat()
                        result: dict[str, Any] | None = None
                        failure_reason: str | None = None
                        try:
                            result = await self.runtime.run(
                                tick.kind,
                                now=now,
                                checkpoint=tick.checkpoint,
                            )
                            failure_reason = self._tick_result_failure(result)
                        except Exception as exc:  # noqa: BLE001 - isolate one scheduled tick
                            failure_reason = str(exc) or type(exc).__name__

                        if failure_reason is not None:
                            self.consecutive_failures += 1
                            await self._record_tick_failure(
                                tick=tick,
                                phase=phase,
                                session_id=session_id,
                                now=_aware(self.clock.now()),
                                failure_reason=failure_reason,
                            )
                            await self._checkpoint(
                                phase=phase,
                                session_id=session_id,
                                now=_aware(self.clock.now()),
                                next_due=self._next_due(pending),
                                full=True,
                            )
                            if tick.kind == "radar":
                                await self._print_coverage(local_started.date())
                            if self.consecutive_failures >= 3:
                                raise RuntimeError(
                                    "SESSION_CONSECUTIVE_TICK_FAILURES"
                                )
                            continue

                        self.consecutive_failures = 0
                        self.last_completed_tick = _aware(
                            self.clock.now()
                        ).isoformat()
                        await self._checkpoint(
                            phase=phase,
                            session_id=session_id,
                            now=_aware(self.clock.now()),
                            next_due=self._next_due(pending),
                            full=True,
                        )
                        if tick.kind == "radar":
                            await self._print_coverage(local_started.date())
                        continue

                next_tick = pending[0].scheduled_for.astimezone(UTC) if pending else None
                wake_at = min(
                    value
                    for value in (
                        next_tick,
                        self._next_heartbeat,
                        end.astimezone(UTC),
                        budget_end,
                    )
                    if value is not None
                )
                await self._wait_until(
                    wake_at,
                    phase=phase,
                    session_id=session_id,
                    phase_end=end,
                    budget_end=budget_end,
                    next_due=self._next_due(pending),
                )
        except Exception as exc:
            await self._record_failure(
                phase=phase,
                session_id=session_id,
                now=_aware(self.clock.now()),
                reason=str(exc) or type(exc).__name__,
            )
            raise

        finished = _aware(self.clock.now())
        await self._checkpoint(
            phase=phase,
            session_id=session_id,
            now=finished,
            next_due=None,
            full=True,
        )
        if phase == "a" and finished.astimezone(EASTERN) >= end:
            handoff = write_handoff(
                self.state_dir,
                session_id=session_id,
                workflow_run_id=self.workflow_run_id,
                last_completed_tick=self.last_completed_tick,
            )
            await self.persist(False)
            print(
                "HANDOFF_WRITTEN "
                f"session_id={session_id} sha256={handoff['state_dump_sha256']}"
            )
        print(
            f"SESSION_PHASE_END phase={phase} ticks={tick_count} "
            f"next_phase={NEXT_PHASE[phase]}"
        )
        return {
            "phase": phase,
            "ticks": tick_count,
            "next_phase": NEXT_PHASE[phase],
            "status": "COMPLETED",
        }

    async def _wait_until(
        self,
        target: datetime,
        *,
        phase: str,
        session_id: str,
        phase_end: datetime,
        budget_end: datetime,
        next_due: str | None,
    ) -> None:
        target = _aware(target)
        while _aware(self.clock.now()) < target:
            now = _aware(self.clock.now())
            if now >= budget_end or now.astimezone(EASTERN) >= phase_end:
                return
            if self._next_heartbeat is None or now >= self._next_heartbeat:
                await self._checkpoint(
                    phase=phase,
                    session_id=session_id,
                    now=now,
                    next_due=next_due,
                    full=not self._has_full_checkpoint,
                )
            seconds = min(60.0, max(0.0, (target - now).total_seconds()))
            if seconds == 0:
                return
            await self.clock.sleep(seconds)

    async def _checkpoint(
        self,
        *,
        phase: str,
        session_id: str,
        now: datetime,
        next_due: str | None,
        full: bool,
        workflow_status: str = "COMPLETED",
    ) -> None:
        timestamp = _aware(now)
        lease = renew_lease(
            self.state_dir,
            now=timestamp,
            session_id=session_id,
            workflow_run_id=self.workflow_run_id,
        )
        write_heartbeat(
            self.state_dir,
            now=timestamp,
            session_id=session_id,
            workflow_run_id=self.workflow_run_id,
            head_sha=self.head_sha,
            phase=phase,
            last_scheduled_tick=self.last_scheduled_tick,
            last_completed_tick=self.last_completed_tick,
            next_due_tick=next_due,
            lease_expires_at=lease["expires_at"],
            policy_version=self.policy_version,
            consecutive_failures=self.consecutive_failures,
        )
        events = await self.runtime.store.read_events()
        coverage = coverage_for_events(events, date.fromisoformat(session_id))
        latest_path = self.state_dir / "latest.json"
        latest = read_json(latest_path) or {}
        latest.update(
            {
                "updated_at": timestamp.isoformat(),
                "workflow_status": workflow_status,
                "session_status": coverage["session_status"],
                "planning_status": coverage["planning_status"],
                "learning_status": coverage["learning_status"],
                "session_coverage": coverage,
            }
        )
        write_json(latest_path, latest)
        await self.persist(full)
        self._has_full_checkpoint = self._has_full_checkpoint or full
        self._next_heartbeat = timestamp + HEARTBEAT_INTERVAL

    async def _mark_missed(self, now: datetime) -> int:
        if self.requested_phase == "wait":
            return 0
        timestamp = _aware(now)
        local = timestamp.astimezone(EASTERN)
        session_date = local.date()
        marked = 0
        if self.runtime.premarket is not None:
            for checkpoint, end_time in PREMARKET_WINDOW_ENDS.items():
                deadline = datetime.combine(session_date, end_time, EASTERN)
                if local < deadline:
                    continue
                run_id = f"premarket:{session_date.isoformat()}:{checkpoint}"
                status = await self.runtime.store.get_runtime_status_key(
                    f"premarket_run:{run_id}"
                )
                if not isinstance(status, dict) or status.get("status") not in {
                    "COMPLETED",
                    "MISSED",
                }:
                    await self.runtime.premarket.mark_missed(
                        checkpoint,
                        scheduled_for=datetime.combine(
                            session_date, PREMARKET_TARGETS[checkpoint], EASTERN
                        ),
                        now=timestamp,
                    )
                    marked += 1
        calendar = self.runtime.scheduler.calendar
        if calendar is None:
            return marked
        session = calendar.session_for(session_date)
        if session is None:
            return marked
        for slot in scheduled_slots(session):
            if slot + RADAR_GRACE >= local:
                continue
            run_id = f"radar:{session_date.isoformat()}:{slot.strftime('%H%M')}"
            status = await self.runtime.store.get_runtime_status_key(f"radar_run:{run_id}")
            if not isinstance(status, dict) or status.get("status") not in {
                "COMPLETED",
                "MISSED",
                "DATA_UNAVAILABLE",
                "FAILED",
            }:
                await self.runtime.scheduler.mark_missed(slot, now=timestamp)
                marked += 1
        return marked

    def _expired_tick(self, tick: SessionTick, now: datetime) -> bool:
        local = _aware(now).astimezone(EASTERN)
        if tick.kind == "premarket":
            assert tick.checkpoint is not None
            return local >= datetime.combine(
                tick.scheduled_for.date(),
                PREMARKET_WINDOW_ENDS[tick.checkpoint],
                EASTERN,
            )
        if tick.kind == "radar":
            return local > tick.scheduled_for + RADAR_GRACE
        return False

    @staticmethod
    def _next_due(pending: list[SessionTick]) -> str | None:
        return pending[0].scheduled_for.isoformat() if pending else None

    async def _print_coverage(self, session_date: date) -> None:
        coverage = coverage_for_events(await self.runtime.store.read_events(), session_date)
        print(
            "SESSION_COVERAGE "
            f"session_status={coverage['session_status']} "
            f"planning_status={coverage['planning_status']} "
            f"learning_status={coverage['learning_status']} "
            f"{coverage_line(coverage)}"
        )

    @staticmethod
    def _tick_result_failure(result: dict[str, Any] | None) -> str | None:
        if not isinstance(result, dict):
            return "SESSION_TICK_RESULT_INVALID"
        if result.get("status") == "FAILED":
            return str(result.get("error_type") or "SESSION_TICK_FAILED")
        runs = result.get("runs")
        if isinstance(runs, list):
            for row in runs:
                if isinstance(row, dict) and row.get("status") == "FAILED":
                    return str(row.get("error_type") or "SESSION_TICK_FAILED")
        return None

    async def _record_tick_failure(
        self,
        *,
        tick: SessionTick,
        phase: str,
        session_id: str,
        now: datetime,
        failure_reason: str,
    ) -> None:
        timestamp = _aware(now)
        aggregate_id = self._tick_aggregate_id(tick, session_id)
        payload: dict[str, Any] = {
            "session_date": session_id,
            "phase": phase,
            "tick_kind": tick.kind,
            "scheduled_for": tick.scheduled_for.isoformat(),
            "checkpoint": tick.checkpoint,
            "status": "FAILED",
            "reason": "SESSION_TICK_FAILED",
            "error_type": failure_reason,
            "failed_at": timestamp.isoformat(),
            "consecutive_failures": self.consecutive_failures,
        }
        async with self.runtime.store.transaction():
            await self.runtime.store.append(
                LedgerEvent(
                    "SESSION_TICK_FAILED",
                    aggregate_id,
                    payload,
                    occurred_at=timestamp,
                )
            )
            event_type, status_key = self._tick_status_target(tick, aggregate_id)
            existing = await self.runtime.store.get_runtime_status_key(status_key)
            if not isinstance(existing, dict) or existing.get("status") != "FAILED":
                specialized = {"run_id": aggregate_id, **payload}
                await self.runtime.store.append(
                    LedgerEvent(
                        event_type,
                        aggregate_id,
                        specialized,
                        occurred_at=timestamp,
                    )
                )
                await self.runtime.store.set_runtime_status(
                    status_key,
                    specialized,
                )
        alert = AlertRecord(
            kind="STATE_INTEGRITY",
            payload={
                **payload,
                "text": (
                    "SESSION_TICK_FAILED "
                    f"phase={phase} kind={tick.kind} "
                    f"scheduled_for={tick.scheduled_for.isoformat()} "
                    f"reason={failure_reason} "
                    f"consecutive_failures={self.consecutive_failures}"
                ),
            },
            created_at=timestamp,
        )
        await self.runtime.store.save_alert(alert)
        await self.runtime.dispatcher.dispatch_once(now=timestamp)
        print(alert.payload["text"])

    @staticmethod
    def _tick_aggregate_id(tick: SessionTick, session_id: str) -> str:
        if tick.kind == "premarket":
            return f"premarket:{session_id}:{tick.checkpoint}"
        return (
            f"{tick.kind}:{session_id}:"
            f"{tick.scheduled_for.astimezone(EASTERN).strftime('%H%M')}"
        )

    @staticmethod
    def _tick_status_target(
        tick: SessionTick,
        aggregate_id: str,
    ) -> tuple[str, str]:
        if tick.kind == "premarket":
            return "PREMARKET_RUN", f"premarket_run:{aggregate_id}"
        if tick.kind == "radar":
            return "RADAR_RUN", f"radar_run:{aggregate_id}"
        return "DAILY_DIGEST_FAILED", f"daily_digest_run:{tick.scheduled_for.date()}"

    async def _record_failure(
        self,
        *,
        phase: str,
        session_id: str,
        now: datetime,
        reason: str,
    ) -> None:
        timestamp = _aware(now)
        alert = AlertRecord(
            kind="STATE_INTEGRITY",
            payload={
                "session_date": session_id,
                "text": f"SESSION_FAILURE phase={phase} reason={reason}",
                "reason": reason,
            },
            created_at=timestamp,
        )
        await self.runtime.store.save_alert(alert)
        await self.runtime.dispatcher.dispatch_once(now=timestamp)
        try:
            await self._checkpoint(
                phase=phase,
                session_id=session_id,
                now=timestamp,
                next_due=None,
                full=True,
                workflow_status="FAILED",
            )
        except (OSError, RuntimeError, subprocess.SubprocessError):
            print("SESSION_FAILURE_PERSIST=FAILED")


async def _prepare_runtime(
    repo: Path,
    cfg: Settings,
    now: datetime,
    *,
    phase: str,
) -> BatchRuntime:
    await asyncio.to_thread(
        subprocess.run,
        ["git", "fetch", "origin", "shadow-state:refs/remotes/origin/shadow-state"],
        cwd=repo,
        check=False,
    )
    restored = await asyncio.to_thread(
        restore_state_files,
        repo,
        ref="origin/shadow-state",
    )
    effective_phase = select_phase(now) if phase == "auto" else phase
    if restored and effective_phase == "b":
        try:
            verify_handoff(repo / "state", session_id=market_session_id(now))
        except RuntimeError:
            await _publish_restore_failure(now, "HANDOFF_MISMATCH")
            raise
    if restored:
        await asyncio.to_thread(restore_database, repo, str(cfg.postgres_dsn))
    await asyncio.to_thread(
        subprocess.run,
        [
            "psql",
            str(cfg.postgres_dsn),
            "-v",
            "ON_ERROR_STOP=1",
            "-f",
            str(repo / "config" / "schema.sql"),
        ],
        cwd=repo,
        check=True,
    )
    runtime, _client = await build_runtime(cfg, now=now)
    await activate_quality_from_state(
        repo,
        cfg.quality_path,
        runtime.store,
        now=now,
    )
    return runtime


async def _publish_restore_failure(now: datetime, reason: str) -> None:
    sink = GitHubIssueSink(
        os.getenv("GITHUB_TOKEN"),
        os.getenv("GITHUB_REPOSITORY"),
        clock=lambda: now,
    )
    try:
        delivered = await sink.send(
            {
                "session_date": market_session_id(now),
                "text": f"SESSION_FAILURE phase=b reason={reason}",
                "reason": reason,
            }
        )
        print(f"HANDOFF_ALERT delivered={str(delivered).lower()}")
    finally:
        await sink.aclose()


def _head_sha(repo: Path) -> str:
    configured = os.getenv("GITHUB_SHA")
    if configured:
        return configured
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


async def async_main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=("auto", "wait", "a", "b"), default="auto")
    parser.add_argument("--budget-minutes", type=int, default=330)
    args = parser.parse_args()
    repo = ROOT
    cfg = Settings()
    if not cfg.postgres_dsn:
        raise RuntimeError("POSTGRES_DSN_MISSING")
    clock = RealClock()
    now = clock.now()
    runtime = await _prepare_runtime(repo, cfg, now, phase=args.phase)
    session_id = market_session_id(now)

    async def persist(full: bool) -> None:
        if full or not (repo / "state" / "market.dump").exists():
            await persist_state(repo, cfg.postgres_dsn or "", session_id, push=True)
        else:
            publish_state_branch(repo, remote="origin")

    runner = SessionRunner(
        runtime,
        state_dir=repo / "state",
        phase=args.phase,
        budget_minutes=args.budget_minutes,
        workflow_run_id=os.getenv("GITHUB_RUN_ID", f"local-{os.getpid()}"),
        head_sha=_head_sha(repo),
        policy_version=load_policy_version(repo),
        persist=persist,
        clock=clock,
        repository=os.getenv("GITHUB_REPOSITORY", "githuber20202/market-brain"),
    )
    try:
        await runner.run()
    finally:
        await runtime.close()
    return 0


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def main() -> None:
    raise SystemExit(asyncio.run(async_main()))


if __name__ == "__main__":
    main()

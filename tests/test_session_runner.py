from datetime import UTC, date, datetime, timedelta

import pytest

from market_brain.ledger.events import LedgerEvent
from market_brain.ledger.store import InMemoryEventStore
from market_brain.orchestration.universe import EASTERN, NyseMarketCalendar
from market_brain.runtime.session import SessionRunner, select_phase, session_ticks
from market_brain.runtime.session_state import write_handoff


class FakeClock:
    def __init__(self, now: datetime) -> None:
        self.current = now.astimezone(UTC)
        self.sleeps: list[float] = []

    def now(self) -> datetime:
        return self.current

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.current += timedelta(seconds=seconds)


class FakeScheduler:
    calendar = None

    def validate_startup(self, *, now: datetime) -> None:
        self.validated_at = now


class FakeDispatcher:
    async def dispatch_once(self, *, now: datetime) -> int:
        return 0


class FakeRuntime:
    def __init__(self) -> None:
        self.store = InMemoryEventStore()
        self.scheduler = FakeScheduler()
        self.premarket = None
        self.dispatcher = FakeDispatcher()
        self.calls: list[tuple[str, datetime, str | None]] = []

    async def run(self, mode: str, *, now: datetime, checkpoint: str | None = None):
        self.calls.append((mode, now, checkpoint))
        return {"status": "COMPLETED"}


class ResumeScheduler(FakeScheduler):
    def __init__(self, store) -> None:
        self.store = store
        self.calendar = NyseMarketCalendar({}, {2026, 2027})
        self.missed: list[str] = []

    async def mark_missed(self, slot, *, now):
        run_id = f"radar:{slot.date().isoformat()}:{slot.strftime('%H%M')}"
        payload = {"status": "MISSED", "scheduled_for": slot.isoformat()}
        self.missed.append(run_id)
        await self.store.append(LedgerEvent("RADAR_RUN", run_id, payload, occurred_at=now))
        await self.store.set_runtime_status(f"radar_run:{run_id}", payload)
        return payload


class ResumePremarket:
    def __init__(self, store) -> None:
        self.store = store
        self.missed: list[str] = []

    async def mark_missed(self, checkpoint, *, scheduled_for, now):
        run_id = f"premarket:{scheduled_for.date().isoformat()}:{checkpoint}"
        payload = {"status": "MISSED", "checkpoint": checkpoint}
        self.missed.append(checkpoint)
        await self.store.append(LedgerEvent("PREMARKET_RUN", run_id, payload, occurred_at=now))
        await self.store.set_runtime_status(f"premarket_run:{run_id}", payload)
        return payload


def test_phase_selection_and_tick_ownership_use_et():
    assert select_phase(datetime(2026, 9, 3, 11, 29, tzinfo=UTC)) == "wait"
    assert select_phase(datetime(2026, 9, 3, 11, 30, tzinfo=UTC)) == "a"
    assert select_phase(datetime(2026, 9, 3, 17, 0, tzinfo=UTC)) == "b"
    a_ticks = session_ticks(date(2026, 9, 3), "a")
    b_ticks = session_ticks(date(2026, 9, 3), "b")
    assert [row.checkpoint for row in a_ticks[:3]] == ["T-30", "T-12", "T-3"]
    assert a_ticks[-1].scheduled_for.time().isoformat() == "12:50:00"
    assert b_ticks[0].scheduled_for.time().isoformat() == "13:00:00"
    assert b_ticks[-1].kind == "digest"


@pytest.mark.asyncio
async def test_session_loop_fake_clock_runs_three_radar_ticks_and_digest(tmp_path):
    runtime = FakeRuntime()
    clock = FakeClock(datetime(2026, 9, 3, 15, 0, tzinfo=EASTERN))
    persisted: list[bool] = []

    async def persist(full: bool) -> None:
        persisted.append(full)
        if full:
            (tmp_path / "market.dump").write_bytes(b"fake-dump")

    runner = SessionRunner(
        runtime,
        state_dir=tmp_path,
        phase="b",
        budget_minutes=120,
        workflow_run_id="501",
        head_sha="abc123",
        policy_version="2026-09-02.1",
        persist=persist,
        clock=clock,
    )

    result = await runner.run()

    assert result == {
        "phase": "b",
        "ticks": 4,
        "next_phase": "none",
        "status": "COMPLETED",
    }
    assert [mode for mode, _now, _checkpoint in runtime.calls] == [
        "radar",
        "radar",
        "radar",
        "digest",
    ]
    assert all(seconds <= 60 for seconds in clock.sleeps)
    assert any(persisted)
    assert len(persisted) >= 10
    assert (tmp_path / "heartbeat.json").exists()
    assert (tmp_path / "lease.json").exists()


@pytest.mark.asyncio
async def test_phase_b_handoff_mismatch_alerts_and_fails_closed(tmp_path):
    runtime = FakeRuntime()
    clock = FakeClock(datetime(2026, 9, 3, 13, 0, tzinfo=EASTERN))
    (tmp_path / "market.dump").write_bytes(b"phase-a")
    write_handoff(
        tmp_path,
        session_id="2026-09-03",
        workflow_run_id="500",
        last_completed_tick="2026-09-03T12:50:00-04:00",
    )
    (tmp_path / "market.dump").write_bytes(b"tampered")

    async def persist(full: bool) -> None:
        if full:
            (tmp_path / "market.dump").write_bytes(b"failure-snapshot")

    runner = SessionRunner(
        runtime,
        state_dir=tmp_path,
        phase="b",
        budget_minutes=120,
        workflow_run_id="501",
        head_sha="abc123",
        policy_version="2026-09-02.1",
        persist=persist,
        clock=clock,
    )

    with pytest.raises(RuntimeError, match="HANDOFF_MISMATCH"):
        await runner.run()

    alerts = await runtime.store.list_alerts()
    assert alerts[-1].kind == "STATE_INTEGRITY"
    assert alerts[-1].payload["reason"] == "HANDOFF_MISMATCH"


@pytest.mark.asyncio
async def test_late_phase_a_resume_marks_passed_slots_without_catch_up(tmp_path):
    runtime = FakeRuntime()
    runtime.scheduler = ResumeScheduler(runtime.store)
    runtime.premarket = ResumePremarket(runtime.store)
    clock = FakeClock(datetime(2026, 9, 3, 11, 0, tzinfo=EASTERN))

    async def persist(full: bool) -> None:
        if full:
            (tmp_path / "market.dump").write_bytes(b"fake-dump")

    runner = SessionRunner(
        runtime,
        state_dir=tmp_path,
        phase="a",
        budget_minutes=1,
        workflow_run_id="700",
        head_sha="abc123",
        policy_version="2026-09-02.1",
        persist=persist,
        clock=clock,
    )

    result = await runner.run()

    assert result["ticks"] == 1
    assert runtime.premarket.missed == ["T-30", "T-12", "T-3"]
    assert runtime.scheduler.missed == [
        "radar:2026-09-03:0950",
        "radar:2026-09-03:1020",
    ]
    assert [mode for mode, _now, _checkpoint in runtime.calls] == ["radar"]

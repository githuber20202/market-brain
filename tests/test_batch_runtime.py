from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

import market_brain.runtime.state as state_module
from market_brain.alerts.dispatcher import AlertDispatcher
from market_brain.alerts.sink import GitHubIssueSink
from market_brain.domain.models import AlertRecord, StrategyLane, TradePlan
from market_brain.ledger.store import InMemoryEventStore
from market_brain.orchestration.universe import NyseMarketCalendar
from market_brain.runtime.batch import BatchRuntime
from market_brain.runtime.state import publish_state_branch, restore_state
from market_brain.settings import Settings
from scripts.batch_gate import should_run


class FakeScheduler:
    def __init__(self):
        self.calendar = NyseMarketCalendar({}, {2026})
        self.calls: list[datetime] = []
        self.completed: set[datetime] = set()
        self.universe = ()

    def validate_startup(self, *, now):
        del now

    async def run_pending(self, *, now):
        minute = now.replace(second=0, microsecond=0)
        self.calls.append(now)
        if minute in self.completed:
            return None
        self.completed.add(minute)
        return {
            "status": "COMPLETED",
            "scheduled_for": minute.isoformat(),
        }


class FakeShadow:
    def validate_startup(self, *, now):
        del now

    async def run_pending(self, *, now):
        del now
        return 2


class FakeService:
    async def sweep_expired(self):
        return {"expired_plans": 0, "released_reservations": 0}


class FakeDispatcher:
    def __init__(self):
        self.calls = 0

    async def dispatch_once(self, *, now):
        del now
        self.calls += 1
        return 0


class FakeIssueSink:
    async def aclose(self):
        return None


class FakeProvider:
    async def aclose(self):
        return None


class FakeDigest:
    async def create(self, *, now, run_id):
        return AlertRecord(
            kind="DAILY_DIGEST",
            payload={
                "session_date": now.date().isoformat(),
                "text": f"digest {run_id}",
            },
            created_at=now,
        )


def _runtime(tmp_path: Path):
    store = InMemoryEventStore()
    scheduler = FakeScheduler()
    runtime = BatchRuntime(
        store=store,
        service=FakeService(),
        provider=FakeProvider(),
        scheduler=scheduler,
        shadow=FakeShadow(),
        digest=FakeDigest(),
        dispatcher=FakeDispatcher(),
        issue_sink=FakeIssueSink(),
        cfg=Settings(),
        output_dir=tmp_path / "reports",
        state_dir=tmp_path / "state",
    )
    return runtime, scheduler


@pytest.mark.asyncio
async def test_batch_fails_closed_before_radar_on_replay_difference(tmp_path):
    runtime, scheduler = _runtime(tmp_path)
    now = datetime(2026, 8, 28, 13, 50, tzinfo=UTC)
    await runtime.store.save_plan(
        TradePlan(
            symbol="SPY",
            lane=StrategyLane.CORE_MOMENTUM,
            entry_trigger=100.0,
            entry_zone_high=100.1,
            stop=99.0,
            tp1=101.5,
            tp2=102.0,
            max_spread_pct=0.25,
            max_slippage_pct=0.30,
            created_at=now,
            expires_at=now.replace(hour=14),
            quality_risk_multiplier=0.5,
        )
    )

    with pytest.raises(RuntimeError, match="STATE_INTEGRITY"):
        await runtime.run("radar", now=now)

    assert scheduler.calls == []
    alerts = await runtime.store.list_alerts()
    assert alerts[-1].kind == "STATE_INTEGRITY"


@pytest.mark.asyncio
async def test_batch_radar_runs_all_eleven_due_slots_idempotently(tmp_path):
    runtime, scheduler = _runtime(tmp_path)
    now = datetime(2026, 8, 28, 18, 50, tzinfo=UTC)

    first = await runtime.run("radar", now=now)
    second = await runtime.run("radar", now=now)

    assert first["due_slots"] == 11
    assert len(first["runs"]) == 11
    assert second["runs"] == []
    assert {call.minute for call in scheduler.calls} <= {20, 50}
    latest = json.loads((tmp_path / "state" / "latest.json").read_text())
    assert latest["mode"] == "radar"


@pytest.mark.asyncio
async def test_batch_digest_catches_up_after_1620_and_writes_report(tmp_path):
    runtime, _scheduler = _runtime(tmp_path)

    result = await runtime.run(
        "digest",
        now=datetime(2026, 8, 28, 20, 27, tzinfo=UTC),
    )

    assert result["status"] == "COMPLETED"
    report = Path(result["report"])
    assert report.name == "digest_2026-08-28.md"
    assert "# Shadow digest: 2026-08-28" in report.read_text()


@pytest.mark.asyncio
async def test_github_issue_sink_reuses_daily_issue_and_dispatcher_tags():
    requests: list[tuple[str, str, dict | None]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        requests.append((request.method, request.url.path, body))
        if request.method == "GET" and request.url.path.endswith("/labels/shadow"):
            return httpx.Response(404, json={})
        if request.method == "POST" and request.url.path.endswith("/labels"):
            return httpx.Response(201, json={"name": "shadow"})
        if request.method == "GET" and request.url.path.endswith("/issues"):
            return httpx.Response(200, json=[])
        if request.method == "POST" and request.url.path.endswith("/issues"):
            return httpx.Response(201, json={"number": 17})
        return httpx.Response(201, json={"id": 1})

    now = datetime(2026, 8, 28, 15, tzinfo=UTC)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        sink = GitHubIssueSink(
            "test-token",
            "githuber20202/market-brain",
            client,
            clock=lambda: now,
        )
        store = InMemoryEventStore()
        for text in ("BUY SPY", "SELL SPY"):
            await store.save_alert(AlertRecord(kind="BUY_NOW", payload={"text": text}))
        dispatcher = AlertDispatcher(
            store,
            [sink],
            run_mode="shadow",
            data_plan="keyless_delayed",
            redact_values=("test-token",),
        )

        assert await dispatcher.dispatch_once(now=now) == 2

    comments = [body for method, path, body in requests if path.endswith("/comments")]
    assert len(comments) == 2
    assert comments[0]["body"].startswith("@githuber20202\n\n[SHADOW][DELAYED]")
    assert sum(path.endswith("/issues") and method == "POST" for method, path, _ in requests) == 1
    assert "test-token" not in json.dumps(requests)


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    ).stdout.strip()


def test_state_branch_force_push_is_parentless_and_restore_reads_dump(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    remote = tmp_path / "remote.git"
    repo.mkdir()
    _git(repo, "init")
    _git(repo, "config", "user.name", "Test")
    _git(repo, "config", "user.email", "test@example.invalid")
    _git(tmp_path, "init", "--bare", str(remote))
    _git(repo, "remote", "add", "origin", str(remote))
    (repo / "state" / "snapshots").mkdir(parents=True)
    (repo / "reports").mkdir()
    (repo / "state" / "market.dump").write_bytes(b"fixture-dump")
    (repo / "state" / "latest.json").write_text("{}\n")
    (repo / "reports" / "digest_2026-08-28.md").write_text("digest\n")

    first = publish_state_branch(repo, remote="origin")
    (repo / "state" / "latest.json").write_text('{"updated": true}\n')
    second = publish_state_branch(repo, remote="origin")

    assert first != second
    assert _git(repo, "rev-list", "--count", "shadow-state") == "1"
    assert _git(repo, "rev-list", "--parents", "-n", "1", "shadow-state") == second
    assert (
        _git(tmp_path, f"--git-dir={remote}", "show", "shadow-state:state/market.dump")
        == "fixture-dump"
    )

    clone = tmp_path / "clone"
    _git(tmp_path, "clone", "--branch", "shadow-state", str(remote), str(clone))
    calls: list[list[str]] = []
    original_run = state_module._run

    def fake_run(args, **kwargs):
        if args[0] == "pg_restore":
            calls.append(args)
            return subprocess.CompletedProcess(args, 0)
        return original_run(args, **kwargs)

    monkeypatch.setattr("market_brain.runtime.state._run", fake_run)
    assert restore_state(clone, "postgresql://fixture", ref="HEAD") is True
    assert (clone / "state" / "market.dump").read_bytes() == b"fixture-dump"
    assert calls[0][0] == "pg_restore"


def test_batch_gate_selects_only_real_et_schedule(tmp_path):
    calendar = tmp_path / "calendar.csv"
    calendar.write_text(
        "date,status,open_time,close_time,source\n"
        "2026-09-07,CLOSED,,,NYSE\n"
    )
    assert should_run(
        "radar", datetime(2026, 8, 28, 13, 50, tzinfo=UTC), calendar
    )
    assert not should_run(
        "radar", datetime(2026, 8, 28, 13, 20, tzinfo=UTC), calendar
    )
    assert should_run(
        "digest", datetime(2026, 8, 28, 20, 20, tzinfo=UTC), calendar
    )
    assert not should_run(
        "radar", datetime(2026, 9, 7, 13, 50, tzinfo=UTC), calendar
    )


def test_shadow_workflows_have_exact_schedule_permissions_and_concurrency():
    root = Path(__file__).resolve().parents[1]
    radar = (root / ".github/workflows/shadow-radar.yml").read_text()
    digest = (root / ".github/workflows/shadow-digest.yml").read_text()

    assert 'cron: "20,50 13-19 * * 1-5"' in radar
    assert 'cron: "20 20,21 * * 1-5"' in digest
    for workflow in (radar, digest):
        assert "group: market-brain-shadow-state" in workflow
        assert "contents: write" in workflow
        assert "issues: write" in workflow
        assert "secrets." not in workflow
        assert "BATCH_DURATION_SECONDS" in workflow

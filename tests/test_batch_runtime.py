from __future__ import annotations

import json
import subprocess
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

import market_brain.runtime.state as state_module
from market_brain.alerts.dispatcher import AlertDispatcher
from market_brain.alerts.sink import GitHubIssueSink
from market_brain.domain.models import (
    AlertRecord,
    LiquidityProfile,
    MarketSnapshot,
    StrategyLane,
    TradePlan,
)
from market_brain.ledger.events import LedgerEvent
from market_brain.ledger.store import InMemoryEventStore
from market_brain.orchestration.service import DecisionService
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

    async def run_slot(self, slot, *, now):
        minute = slot.replace(second=0, microsecond=0)
        if minute in self.completed:
            return None
        self.calls.append(now)
        self.completed.add(minute)
        return {"status": "COMPLETED", "scheduled_for": minute.isoformat()}

    async def mark_missed(self, slot, *, now):
        del now
        minute = slot.replace(second=0, microsecond=0)
        return {"status": "MISSED", "scheduled_for": minute.isoformat()}


class FakeService:
    async def sweep_expired(self, *, now=None):
        del now
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
async def test_batch_radar_marks_old_slots_missed_and_runs_latest_with_real_now(tmp_path):
    runtime, scheduler = _runtime(tmp_path)
    now = datetime(2026, 8, 28, 18, 50, tzinfo=UTC)

    first = await runtime.run("radar", now=now)
    second = await runtime.run("radar", now=now)

    assert first["due_slots"] == 31
    assert len(first["runs"]) == 1
    assert first["missed_slots"] == 30
    assert second["runs"] == []
    assert scheduler.calls == [now]
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
    assert "# Market digest: 2026-08-28" in report.read_text()


@pytest.mark.asyncio
async def test_weekly_batch_refreshes_quality_into_state(tmp_path, monkeypatch):
    runtime, scheduler = _runtime(tmp_path)
    scheduler.universe = (
        SimpleNamespace(
            symbol="FULL", instrument_type="EQUITY", ranking_eligible=True
        ),
        SimpleNamespace(
            symbol="PART", instrument_type="EQUITY", ranking_eligible=True
        ),
        SimpleNamespace(symbol="SPY", instrument_type="ETF", ranking_eligible=True),
        SimpleNamespace(
            symbol="CHG", instrument_type="UNRESOLVED", ranking_eligible=False
        ),
    )
    calls: dict[str, object] = {}

    async def fake_quality(
        symbols,
        *,
        output_path,
        now,
        quality_source,
        skipped_instruments,
    ):
        calls["quality"] = (
            symbols,
            output_path,
            now,
            quality_source,
            skipped_instruments,
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("symbol,quality_score,as_of,source,partial,ttm_net_income,profitability_pass\n")
        return {"status": "COMPLETED", "rows": 2}

    async def fake_replay(**kwargs):
        calls["replay"] = kwargs
        return tmp_path / "reports" / "replay.md"

    monkeypatch.setattr("scripts.quality_refresh.refresh_quality", fake_quality)
    monkeypatch.setattr("scripts.replay_report.create_replay_report", fake_replay)
    now = datetime(2026, 8, 28, 21, 30, tzinfo=UTC)

    result = await runtime.run("weekly", now=now)

    assert result["quality"] == {"status": "COMPLETED", "rows": 2}
    assert calls["quality"] == (
        ["FULL", "PART"],
        tmp_path / "state" / "quality.csv",
        now,
        "yahoo",
        [
            {"symbol": "CHG", "instrument_type": "UNRESOLVED"},
            {"symbol": "SPY", "instrument_type": "ETF"},
        ],
    )
    assert calls["replay"]["symbols"] == ["FULL", "PART", "SPY"]
    latest = json.loads((tmp_path / "state" / "latest.json").read_text())
    assert latest["quality"] == {"status": "COMPLETED", "rows": 2}


@pytest.mark.asyncio
async def test_github_issue_sink_reuses_daily_issue_and_dispatcher_tags():
    requests: list[tuple[str, str, dict | None]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        requests.append((request.method, request.url.path, body))
        if request.method == "GET" and request.url.path.endswith("/labels/market-core"):
            return httpx.Response(404, json={})
        if request.method == "POST" and request.url.path.endswith("/labels"):
            return httpx.Response(201, json={"name": "market-core"})
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
            data_plan="keyless_delayed",
            redact_values=("test-token",),
        )

        assert await dispatcher.dispatch_once(now=now) == 2
        await store.save_alert(
            AlertRecord(
                kind="DAILY_DIGEST",
                payload={
                    "run_id": "daily_digest:2026-08-28",
                    "session_date": "2026-08-28",
                    "text": "daily digest",
                },
            )
        )
        assert await dispatcher.dispatch_once(now=now) == 1

    comments = [body for method, path, body in requests if path.endswith("/comments")]
    assert len(comments) == 3
    assert comments[0]["body"].startswith("@githuber20202\n\n[DELAYED]")
    assert sum(path.endswith("/issues") and method == "POST" for method, path, _ in requests) == 1
    assert any(
        method == "PATCH" and path.endswith("/issues/17") and body == {"state": "closed"}
        for method, path, body in requests
    )
    issue_body = next(
        body["body"]
        for method, path, body in requests
        if method == "POST" and path.endswith("/issues")
    )
    assert "Manual decision support only" in issue_body
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
    assert _git(repo, "rev-list", "--count", "market-state") == "1"
    assert _git(repo, "rev-list", "--parents", "-n", "1", "market-state") == second
    assert (
        _git(tmp_path, f"--git-dir={remote}", "show", "market-state:state/market.dump")
        == "fixture-dump"
    )

    clone = tmp_path / "clone"
    _git(tmp_path, "clone", "--branch", "market-state", str(remote), str(clone))
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
    assert should_run(
        "radar", datetime(2026, 8, 28, 13, 58, tzinfo=UTC), calendar
    )
    assert not should_run(
        "radar", datetime(2026, 8, 28, 13, 20, tzinfo=UTC), calendar
    )
    assert should_run(
        "radar", datetime(2026, 8, 28, 19, 20, tzinfo=UTC), calendar
    )
    assert not should_run(
        "radar", datetime(2026, 8, 28, 19, 21, tzinfo=UTC), calendar
    )
    assert should_run(
        "digest", datetime(2026, 8, 28, 20, 20, tzinfo=UTC), calendar
    )
    assert should_run(
        "digest", datetime(2026, 8, 28, 20, 41, tzinfo=UTC), calendar
    )
    assert not should_run(
        "digest", datetime(2026, 8, 29, 3, 59, tzinfo=UTC), calendar
    )
    assert not should_run(
        "radar", datetime(2026, 9, 7, 13, 50, tzinfo=UTC), calendar
    )
    assert should_run(
        "radar",
        datetime(2026, 8, 29, 13, 50, tzinfo=UTC),
        calendar,
        force=True,
    )

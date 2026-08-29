from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import UTC, datetime, time, timedelta
from pathlib import Path

import httpx

from market_brain.alerts.dispatcher import AlertDispatcher
from market_brain.alerts.sink import GitHubIssueSink
from market_brain.domain.models import AlertRecord
from market_brain.ledger.replay import replay_check
from market_brain.ledger.store import PostgresEventStore
from market_brain.orchestration.screener import MarketScreener
from market_brain.orchestration.service import DecisionService
from market_brain.orchestration.universe import EASTERN
from market_brain.providers import build_market_data_provider
from market_brain.providers.rate_limit import TokenBucketRateLimiter
from market_brain.providers.yahoo import YahooMarketData
from market_brain.replay.engine import ReplayEngine
from market_brain.runtime.daily_digest import DailyDigest
from market_brain.runtime.radar_scheduler import RadarScheduler, scheduled_slots
from market_brain.runtime.shadow import ShadowEvaluator
from market_brain.settings import ROOT, Settings


class BatchRuntime:
    def __init__(
        self,
        *,
        store,
        service,
        provider,
        scheduler: RadarScheduler,
        shadow: ShadowEvaluator,
        digest: DailyDigest,
        dispatcher: AlertDispatcher,
        issue_sink: GitHubIssueSink,
        cfg: Settings,
        output_dir: Path,
        state_dir: Path | None = None,
    ) -> None:
        self.store = store
        self.service = service
        self.provider = provider
        self.scheduler = scheduler
        self.shadow = shadow
        self.digest = digest
        self.dispatcher = dispatcher
        self.issue_sink = issue_sink
        self.cfg = cfg
        self.output_dir = output_dir
        self.state_dir = state_dir or ROOT / "state"

    async def validate_state(self, *, now: datetime) -> None:
        differences = await replay_check(self.store)
        if not differences:
            print("STATE_INTEGRITY=PASS replay_check=[]")
            return
        alert = AlertRecord(
            kind="STATE_INTEGRITY",
            payload={
                "session_date": now.astimezone(EASTERN).date().isoformat(),
                "text": "STATE_INTEGRITY failed; radar was not run",
                "differences": differences,
            },
            created_at=now,
        )
        await self.store.save_alert(alert)
        await self.dispatcher.dispatch_once(now=now)
        print(f"STATE_INTEGRITY=FAIL replay_check={differences}")
        raise RuntimeError("STATE_INTEGRITY")

    async def run(self, mode: str, *, now: datetime) -> dict:
        timestamp = _aware(now)
        await self.validate_state(now=timestamp)
        self.scheduler.validate_startup(now=timestamp)
        self.shadow.validate_startup(now=timestamp)
        if mode == "radar":
            result = await self._run_radar(timestamp)
        elif mode == "digest":
            result = await self._run_digest(timestamp)
        elif mode == "weekly":
            result = await self._run_weekly(timestamp)
        else:
            raise ValueError("BATCH_MODE_INVALID")
        delivered = await self.dispatcher.dispatch_once(now=timestamp)
        result["alerts_delivered"] = delivered
        await self._write_latest(mode, timestamp, result)
        print(f"BATCH_RESULT={json.dumps(result, sort_keys=True, default=str)}")
        return result

    async def _run_radar(self, timestamp: datetime) -> dict:
        local = timestamp.astimezone(EASTERN)
        assert self.scheduler.calendar is not None
        session = self.scheduler.calendar.session_for(local.date())
        if session is None:
            return {"mode": "radar", "status": "NO_SESSION", "runs": []}
        due = [slot for slot in scheduled_slots(session) if slot <= local]
        runs: list[dict] = []
        for slot in due:
            for second in range(len(due) + 1):
                result = await self.scheduler.run_pending(
                    now=slot + timedelta(seconds=second)
                )
                if result is None:
                    break
                runs.append(result)
        shadow_count = await self.shadow.run_pending(now=timestamp)
        expired = await self.service.sweep_expired()
        return {
            "mode": "radar",
            "status": "COMPLETED",
            "due_slots": len(due),
            "runs": runs,
            "shadow_evaluated": shadow_count,
            "expired": expired,
        }

    async def _run_digest(self, timestamp: datetime) -> dict:
        local = timestamp.astimezone(EASTERN)
        assert self.scheduler.calendar is not None
        session = self.scheduler.calendar.session_for(local.date())
        if session is None:
            return {"mode": "digest", "status": "NO_SESSION"}
        scheduled = datetime.combine(local.date(), time(16, 20), EASTERN)
        if local < scheduled:
            return {"mode": "digest", "status": "NOT_DUE"}
        shadow_count = await self.shadow.run_pending(now=timestamp)
        alert = await self.digest.create(
            now=timestamp,
            run_id=f"daily_digest:{local.date().isoformat()}",
        )
        report = None
        if alert is not None:
            report = write_digest_report(alert.payload, self.output_dir)
        return {
            "mode": "digest",
            "status": "COMPLETED" if alert is not None else "ALREADY_COMPLETED",
            "shadow_evaluated": shadow_count,
            "report": str(report) if report else None,
        }

    async def _run_weekly(self, timestamp: datetime) -> dict:
        from scripts.replay_report import create_replay_report
        from scripts.shadow_report import create_shadow_report

        assert self.scheduler.calendar is not None
        symbols = sorted(entry.symbol for entry in self.scheduler.universe)
        replay_path = await create_replay_report(
            days=5,
            symbols=symbols,
            calendar=self.scheduler.calendar,
            engine=ReplayEngine(self.provider),
            output_dir=self.output_dir,
            now=timestamp,
        )
        local_date = timestamp.astimezone(EASTERN).date()
        week_start = local_date - timedelta(days=local_date.weekday())
        shadow_path = await create_shadow_report(
            self.store,
            week_start=week_start,
            output_dir=self.output_dir,
        )
        return {
            "mode": "weekly",
            "status": "COMPLETED",
            "replay_report": str(replay_path),
            "shadow_report": str(shadow_path),
            "quality": "TASK_25",
        }

    async def _write_latest(self, mode: str, timestamp: datetime, result: dict) -> None:
        runtime = await self.store.get_runtime_status()
        completed_slots = sorted(
            str(value.get("scheduled_for"))
            for key, value in runtime.items()
            if key.startswith("radar_run:")
            and isinstance(value, dict)
            and value.get("status") == "COMPLETED"
        )
        payload = {
            "updated_at": timestamp.isoformat(),
            "mode": mode,
            "status": result.get("status"),
            "last_completed_slot": completed_slots[-1] if completed_slots else None,
            "plans": len(await self.store.list_plans()),
            "alerts": len(await self.store.list_alerts()),
            "shadow_trades": len(await self.store.list_shadow_trades()),
        }
        self.state_dir.mkdir(parents=True, exist_ok=True)
        (self.state_dir / "latest.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    async def close(self) -> None:
        await self.issue_sink.aclose()
        close = getattr(self.provider, "aclose", None)
        if close is not None:
            await close()
        await self.store.close()


def write_digest_report(payload: dict, output_dir: Path) -> Path:
    session_date = str(payload["session_date"])
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"digest_{session_date}.md"
    path.write_text(
        f"# Shadow digest: {session_date}\n\n{payload['text']}\n",
        encoding="utf-8",
    )
    return path


def _fixture_provider(cfg: Settings, fixture_dir: Path, now: datetime):
    minute = json.loads((fixture_dir / "yahoo_chart_1m.json").read_text())
    daily = json.loads((fixture_dir / "yahoo_chart_1d.json").read_text())
    cboe = json.loads((fixture_dir / "cboe_quote.json").read_text())

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "cdn.cboe.com":
            return httpx.Response(200, json=cboe)
        payload = daily if request.url.params.get("interval") == "1d" else minute
        return httpx.Response(200, json=payload)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    provider = YahooMarketData(
        cfg,
        client,
        cboe_client=client,
        limiter=TokenBucketRateLimiter(1_000_000),
        now=lambda: now,
    )
    return provider, client


async def build_runtime(
    cfg: Settings,
    *,
    now: datetime,
    fixture_dir: Path | None = None,
) -> tuple[BatchRuntime, httpx.AsyncClient | None]:
    if not cfg.postgres_dsn:
        raise RuntimeError("POSTGRES_DSN_MISSING")
    store = PostgresEventStore(cfg.postgres_dsn)
    fixture_client = None
    if fixture_dir is None:
        provider = build_market_data_provider(cfg, event_store=store)
    else:
        provider, fixture_client = _fixture_provider(cfg, fixture_dir, now)
    service = DecisionService(store, cfg=cfg, market_data=provider)
    screener = MarketScreener(provider)
    scheduler = RadarScheduler(
        service=service,
        screener=screener,
        universe_dir=cfg.universe_dir,
        quality_path=cfg.quality_path,
        calendar_path=cfg.market_calendar_path,
        plans_per_run=cfg.plans_per_run,
    )
    shadow = ShadowEvaluator(store, cfg=cfg, backfill=service.backfill_intraday_structures)
    issue_sink = GitHubIssueSink(
        os.getenv("GITHUB_TOKEN"),
        os.getenv("GITHUB_REPOSITORY"),
        clock=lambda: now,
    )
    dispatcher = AlertDispatcher(
        store,
        [issue_sink],
        run_mode=cfg.run_mode,
        data_plan=cfg.data_plan,
        redact_values=(os.getenv("GITHUB_TOKEN", ""),),
    )
    runtime = BatchRuntime(
        store=store,
        service=service,
        provider=provider,
        scheduler=scheduler,
        shadow=shadow,
        digest=DailyDigest(store),
        dispatcher=dispatcher,
        issue_sink=issue_sink,
        cfg=cfg,
        output_dir=ROOT / "reports",
    )
    return runtime, fixture_client


async def async_main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("radar", "digest", "weekly"), required=True)
    parser.add_argument("--fixtures", type=Path)
    parser.add_argument("--now", help="UTC/offset ISO timestamp; test and smoke use only")
    args = parser.parse_args()
    now = _aware(datetime.fromisoformat(args.now)) if args.now else datetime.now(UTC)
    cfg = Settings()
    runtime, fixture_client = await build_runtime(cfg, now=now, fixture_dir=args.fixtures)
    try:
        await runtime.run(args.mode, now=now)
    finally:
        await runtime.close()
        if fixture_client is not None:
            await fixture_client.aclose()
    return 0


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def main() -> None:
    raise SystemExit(asyncio.run(async_main()))


if __name__ == "__main__":
    main()

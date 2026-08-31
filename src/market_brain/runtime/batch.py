from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import httpx

from market_brain.alerts.dispatcher import AlertDispatcher
from market_brain.alerts.sink import GitHubIssueSink
from market_brain.domain.models import (
    AlertRecord,
    IntradayStructureState,
    PlanStatus,
    ShadowTradeStatus,
    SignalState,
)
from market_brain.ledger.events import LedgerEvent
from market_brain.ledger.replay import replay_check
from market_brain.ledger.store import PostgresEventStore
from market_brain.orchestration.screener import MarketScreener
from market_brain.orchestration.service import DecisionService
from market_brain.orchestration.universe import EASTERN
from market_brain.providers import build_market_data_provider
from market_brain.providers.base import DataUnavailable
from market_brain.providers.rate_limit import TokenBucketRateLimiter
from market_brain.providers.yahoo import YahooMarketData
from market_brain.replay.engine import ReplayEngine
from market_brain.runtime.daily_digest import DailyDigest
from market_brain.runtime.premarket import PremarketFunnel
from market_brain.runtime.premarket_learning import PremarketLearningReviewer
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
        premarket: PremarketFunnel | None = None,
        premarket_learning: PremarketLearningReviewer | None = None,
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
        self.premarket = premarket
        self.premarket_learning = premarket_learning
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

    async def run(
        self,
        mode: str,
        *,
        now: datetime,
        checkpoint: str | None = None,
    ) -> dict:
        timestamp = _aware(now)
        await self.validate_state(now=timestamp)
        self.scheduler.validate_startup(now=timestamp)
        self.shadow.validate_startup(now=timestamp)
        wallet_seeded = await self._ensure_shadow_wallet(timestamp)
        if mode == "radar":
            result = await self._run_radar(timestamp)
        elif mode == "premarket":
            if checkpoint is None:
                raise ValueError("PREMARKET_CHECKPOINT_REQUIRED")
            if self.premarket is None:
                raise RuntimeError("PREMARKET_RUNTIME_NOT_CONFIGURED")
            result = await self.premarket.run(checkpoint, now=timestamp)
        elif mode == "digest":
            result = await self._run_digest(timestamp)
        elif mode == "weekly":
            result = await self._run_weekly(timestamp)
        else:
            raise ValueError("BATCH_MODE_INVALID")
        delivered = await self.dispatcher.dispatch_once(now=timestamp)
        result["alerts_delivered"] = delivered
        result["wallet_seeded"] = wallet_seeded
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
        missed: list[dict] = []
        if due:
            for slot in due[:-1]:
                run_id = f"radar:{slot.date().isoformat()}:{slot.strftime('%H%M')}"
                status = await self.store.get_runtime_status_key(f"radar_run:{run_id}")
                if not isinstance(status, dict) or status.get("status") not in {
                    "COMPLETED",
                    "MISSED",
                }:
                    missed.append(await self.scheduler.mark_missed(slot, now=timestamp))
            result = await self.scheduler.run_slot(due[-1], now=timestamp)
            if result is not None:
                runs.append(result)
        plan_watch = await self._run_plan_watch(timestamp)
        shadow_count = await self.shadow.evaluate_now(now=timestamp)
        expired = await self.service.sweep_expired(now=timestamp)
        return {
            "mode": "radar",
            "status": "COMPLETED",
            "due_slots": len(due),
            "runs": runs,
            "missed_slots": len(missed),
            "plan_watch": plan_watch,
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
        shadow_count = await self.shadow.evaluate_now(now=timestamp)
        learning = None
        if self.premarket_learning is not None:
            learning = await self.premarket_learning.review(now=timestamp)
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
            "premarket_learning": learning,
            "report": str(report) if report else None,
        }

    async def _run_weekly(self, timestamp: datetime) -> dict:
        from scripts.quality_refresh import refresh_quality
        from scripts.replay_report import create_replay_report
        from scripts.shadow_report import create_shadow_report

        assert self.scheduler.calendar is not None
        universe = sorted(self.scheduler.universe, key=lambda entry: entry.symbol)
        symbols = [entry.symbol for entry in universe if entry.ranking_eligible]
        quality_symbols = [
            entry.symbol for entry in universe if entry.instrument_type == "EQUITY"
        ]
        skipped_instruments = [
            {"symbol": entry.symbol, "instrument_type": entry.instrument_type}
            for entry in universe
            if entry.instrument_type != "EQUITY"
        ]
        quality = await refresh_quality(
            quality_symbols,
            output_path=self.state_dir / "quality.csv",
            now=timestamp,
            quality_source=self.cfg.quality_source,
            skipped_instruments=skipped_instruments,
        )
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
            "quality": quality,
        }

    async def _ensure_shadow_wallet(self, timestamp: datetime) -> bool:
        if self.cfg.run_mode != "shadow":
            return False
        if await self.store.get_wallet() is not None:
            return False
        await self.service.seed_wallet(
            self.cfg.shadow_capital_base,
            self.cfg.shadow_capital_base,
            source="SHADOW_VIRTUAL",
            now=timestamp,
        )
        await self.store.set_runtime_status(
            "shadow_wallet",
            {
                "mode": "virtual",
                "source": "SHADOW_VIRTUAL",
                "seeded_at": timestamp.isoformat(),
            },
        )
        return True

    async def _run_plan_watch(self, timestamp: datetime) -> dict:
        if self.cfg.run_mode != "shadow":
            return {"status": "SKIPPED_LIVE", "symbols": 0}
        plans = await self.store.list_plans()
        shadows = await self.store.list_shadow_trades()
        recovered_releases = 0
        for trade in shadows:
            if (
                await self.store.get_reservation(trade.plan_id) is not None
                and await self._release_shadow_reservation(trade.plan_id, timestamp)
            ):
                recovered_releases += 1
        if recovered_releases:
            plans = await self.store.list_plans()
        active_shadow = {
            row.symbol.upper()
            for row in shadows
            if row.status in {ShadowTradeStatus.OPEN, ShadowTradeStatus.TP1}
        }
        active_plans = [
            row
            for row in plans
            if row.status in {PlanStatus.ACTIVE, PlanStatus.RESERVED}
        ]
        symbols = sorted({row.symbol.upper() for row in active_plans} | active_shadow)
        backfill_failures: list[dict] = []
        failed_symbols: set[str] = set()
        for symbol in symbols:
            try:
                await self.service.backfill_intraday_structures([symbol], now=timestamp)
            except (DataUnavailable, OSError, RuntimeError, TypeError, ValueError) as exc:
                error_type = (
                    exc.error_type if isinstance(exc, DataUnavailable) else type(exc).__name__
                )
                failed_symbols.add(symbol)
                backfill_failures.append({"symbol": symbol, "error_type": error_type})

        triggers = 0
        retest_valid = 0
        activations = 0
        rejected = 0
        rejected_by_reason: dict[str, int] = {}
        released = recovered_releases
        session_date = timestamp.astimezone(EASTERN).date().isoformat()
        for plan in await self.store.list_plans():
            if (
                plan.status != PlanStatus.ACTIVE
                or plan.triggered_at is not None
                or plan.symbol.upper() in failed_symbols
            ):
                continue
            bars = await self.store.list_intraday_bars(plan.symbol, session_date)
            first_after = plan.created_at.astimezone(UTC).replace(
                second=0,
                microsecond=0,
            ) + timedelta(minutes=1)
            trigger_bar = next(
                (
                    row
                    for row in bars
                    if row.minute_ts >= first_after and row.high >= plan.entry_trigger
                ),
                None,
            )
            if trigger_bar is not None and await self.service.record_trigger_hit(
                plan.plan_id,
                last=trigger_bar.high,
                triggered_at=trigger_bar.minute_ts + timedelta(seconds=59),
                source="BATCH_SIP_BAR",
            ):
                triggers += 1

        for plan in await self.store.list_plans():
            if (
                plan.status != PlanStatus.ACTIVE
                or plan.triggered_at is None
                or timestamp > plan.expires_at
                or plan.symbol.upper() in failed_symbols
                or await self.store.get_shadow_trade(plan.plan_id) is not None
            ):
                continue
            structure = await self.service.get_intraday_structure(
                plan.symbol,
                now=timestamp,
            )
            if structure is None or structure.state in {
                IntradayStructureState.BUILDING_OR,
                IntradayStructureState.ARMED,
                IntradayStructureState.BREAKOUT_SEEN,
            }:
                continue
            if structure.state != IntradayStructureState.RETEST_VALID:
                reasons = [*structure.reasons] or ["RETEST_INVALID"]
                if await self._record_activation_rejected(
                    plan.plan_id,
                    reasons,
                    timestamp,
                ):
                    rejected += 1
                    self._count_rejection_reasons(rejected_by_reason, reasons)
                continue
            retest_valid += 1
            try:
                decision = await self.service.activate_shadow_retest(
                    plan.plan_id,
                    structure=structure,
                    detected_at=timestamp,
                )
            except (DataUnavailable, OSError, RuntimeError, TypeError, ValueError) as exc:
                error_type = (
                    exc.error_type if isinstance(exc, DataUnavailable) else type(exc).__name__
                )
                if await self._record_activation_rejected(
                    plan.plan_id,
                    [error_type],
                    timestamp,
                ):
                    rejected += 1
                    self._count_rejection_reasons(rejected_by_reason, [error_type])
                continue
            if decision.state != SignalState.BUY_NOW:
                reasons = decision.reasons or [str(decision.state)]
                if await self._record_activation_rejected(
                    plan.plan_id,
                    reasons,
                    timestamp,
                ):
                    rejected += 1
                    self._count_rejection_reasons(rejected_by_reason, reasons)
                continue
            activations += 1
            if await self._release_shadow_reservation(plan.plan_id, timestamp):
                released += 1
        return {
            "status": "COMPLETED",
            "symbols": len(symbols),
            "backfill_failures": backfill_failures,
            "trigger_hits": triggers,
            "retest_valid": retest_valid,
            "buy_now": activations,
            "activation_rejected": rejected,
            "activation_rejected_by_reason": dict(sorted(rejected_by_reason.items())),
            "reservations_released": released,
            "shadow_trades": len(await self.store.list_shadow_trades()),
        }

    async def _release_shadow_reservation(
        self,
        plan_id: str,
        timestamp: datetime,
    ) -> bool:
        async with self.store.transaction():
            released = await self.service.release_reservation(
                plan_id,
                reason="SHADOW_RESERVATION_RELEASED",
                now=timestamp,
            )
            if not released:
                return False
            await self.store.append(
                LedgerEvent(
                    "SHADOW_RESERVATION_RELEASED",
                    plan_id,
                    {"source": "SHADOW_VIRTUAL", "reason": "SHADOW_TRADE_OPENED"},
                    occurred_at=timestamp,
                )
            )
        return True

    async def _record_activation_rejected(
        self,
        plan_id: str,
        reasons: list[str],
        timestamp: datetime,
    ) -> bool:
        normalized = self._normalized_activation_reasons(reasons)
        key = f"activation_rejected:{plan_id}"
        previous = await self.store.get_runtime_status_key(key)
        if isinstance(previous, dict) and previous.get("reasons") == normalized:
            return False
        payload = {
            "plan_id": plan_id,
            "reasons": normalized,
            "at": timestamp.isoformat(),
        }
        async with self.store.transaction():
            await self.store.append(
                LedgerEvent(
                    "ACTIVATION_REJECTED",
                    plan_id,
                    payload,
                    occurred_at=timestamp,
                )
            )
            await self.store.set_runtime_status(key, payload)
        return True

    @staticmethod
    def _normalized_activation_reasons(reasons: list[str]) -> list[str]:
        expanded: list[str] = []
        for reason in reasons:
            value = str(reason)
            if value == "NO_CHASE_ENTRY_ZONE_EXCEEDED":
                expanded.append("EXTENDED")
            expanded.append(value)
        return list(dict.fromkeys(expanded))

    @classmethod
    def _count_rejection_reasons(
        cls,
        counts: dict[str, int],
        reasons: list[str],
    ) -> None:
        for reason in cls._normalized_activation_reasons(reasons):
            counts[reason] = counts.get(reason, 0) + 1

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
        if mode == "premarket":
            payload["checkpoint"] = result.get("checkpoint")
            payload["top10"] = result.get("top10", [])
            payload["finalists"] = result.get("finalists", [])
        if mode == "weekly":
            payload["quality"] = result.get("quality")
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


def _fixture_provider(
    cfg: Settings,
    fixture_dir: Path,
    now: datetime,
    *,
    fixture_profile: str | None = None,
):
    minute_file = (
        "yahoo_chart_batch_plan_watch.json"
        if fixture_profile == "batch-plan-watch"
        else (
            "yahoo_chart_premarket.json"
            if fixture_profile == "premarket"
            else "yahoo_chart_1m.json"
        )
    )
    minute = json.loads((fixture_dir / minute_file).read_text())
    daily = json.loads((fixture_dir / "yahoo_chart_1d.json").read_text())
    cboe = json.loads((fixture_dir / "cboe_quote.json").read_text())
    news = (
        json.loads((fixture_dir / "yahoo_search_news.json").read_text())
        if fixture_profile == "premarket"
        else None
    )
    screener = (
        json.loads((fixture_dir / "yahoo_screener_empty.json").read_text())
        if fixture_profile == "premarket"
        else None
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "cdn.cboe.com":
            return httpx.Response(200, json=cboe)
        if request.url.path.endswith("/v1/finance/search") and news is not None:
            payload = json.loads(json.dumps(news))
            rows = payload.get("news", [])
            if rows and isinstance(rows[0], dict):
                rows[0]["relatedTickers"] = [request.url.params.get("q", "TEST")]
            return httpx.Response(200, json=payload)
        if "/v1/finance/screener/" in request.url.path and screener is not None:
            return httpx.Response(200, json=screener)
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
    fixture_profile: str | None = None,
    provider_override=None,
    dispatcher_sinks: list | None = None,
    output_dir: Path | None = None,
    state_dir: Path | None = None,
) -> tuple[BatchRuntime, httpx.AsyncClient | None]:
    if not cfg.postgres_dsn:
        raise RuntimeError("POSTGRES_DSN_MISSING")
    store = PostgresEventStore(cfg.postgres_dsn)
    fixture_client = None
    if provider_override is not None:
        provider = provider_override
    elif fixture_dir is None:
        provider = build_market_data_provider(cfg, event_store=store)
    else:
        provider, fixture_client = _fixture_provider(
            cfg,
            fixture_dir,
            now,
            fixture_profile=fixture_profile,
        )
    service = DecisionService(store, cfg=cfg, market_data=provider)
    screener = MarketScreener(provider, store=store)
    scheduler = RadarScheduler(
        service=service,
        screener=screener,
        universe_dir=cfg.universe_dir,
        quality_path=cfg.quality_path,
        calendar_path=cfg.market_calendar_path,
        plans_per_run=cfg.plans_per_run,
    )
    runtime_state_dir = state_dir or ROOT / "state"
    premarket = PremarketFunnel(
        store=store,
        service=service,
        provider=provider,
        universe_dir=cfg.universe_dir,
        calendar_path=cfg.market_calendar_path,
        cfg=cfg,
        state_dir=runtime_state_dir,
    )
    premarket_learning = PremarketLearningReviewer(
        store=store,
        provider=provider,
        calendar_path=cfg.market_calendar_path,
        state_dir=runtime_state_dir,
    )
    shadow = ShadowEvaluator(store, cfg=cfg, backfill=service.backfill_intraday_structures)
    issue_sink = GitHubIssueSink(
        os.getenv("GITHUB_TOKEN"),
        os.getenv("GITHUB_REPOSITORY"),
        clock=lambda: now,
    )
    dispatcher = AlertDispatcher(
        store,
        dispatcher_sinks if dispatcher_sinks is not None else [issue_sink],
        run_mode=cfg.run_mode,
        data_plan=cfg.data_plan,
        redact_values=(os.getenv("GITHUB_TOKEN", ""),),
    )
    runtime = BatchRuntime(
        store=store,
        service=service,
        provider=provider,
        scheduler=scheduler,
        premarket=premarket,
        premarket_learning=premarket_learning,
        shadow=shadow,
        digest=DailyDigest(store),
        dispatcher=dispatcher,
        issue_sink=issue_sink,
        cfg=cfg,
        output_dir=output_dir or ROOT / "reports",
        state_dir=runtime_state_dir,
    )
    return runtime, fixture_client


async def async_main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=("premarket", "radar", "digest", "weekly", "rehearsal"),
        required=True,
    )
    parser.add_argument("--fixtures", type=Path)
    parser.add_argument(
        "--fixture-profile",
        choices=("batch-plan-watch", "premarket"),
    )
    parser.add_argument("--checkpoint", choices=("T-30", "T-12", "T-3"))
    parser.add_argument("--now", help="UTC/offset ISO timestamp; test and smoke use only")
    parser.add_argument("--session", help="NYSE session date for rehearsal (YYYY-MM-DD)")
    parser.add_argument(
        "--publish-issue",
        action="store_true",
        help="Post the clean rehearsal summary to a closed GitHub issue",
    )
    args = parser.parse_args()
    now = _aware(datetime.fromisoformat(args.now)) if args.now else datetime.now(UTC)
    cfg = Settings()
    if args.mode == "rehearsal":
        if not args.session:
            parser.error("--session is required with --mode rehearsal")
        from market_brain.providers.yahoo_replay import YahooReplayMarketData
        from market_brain.runtime.rehearsal import (
            MutableClock,
            RehearsalConsoleSink,
            rehearsal_ticks,
            run_rehearsal,
        )

        session_date = date.fromisoformat(args.session)
        first_tick = rehearsal_ticks(session_date)[0]
        clock = MutableClock(first_tick)
        provider = YahooReplayMarketData(session_date, cfg, now=clock.now)
        runtime, _fixture_client = await build_runtime(
            cfg,
            now=first_tick,
            provider_override=provider,
            dispatcher_sinks=[RehearsalConsoleSink()],
            output_dir=ROOT / "reports" / "rehearsal",
            state_dir=ROOT / "rehearsal-state",
        )
        try:
            await run_rehearsal(
                runtime,
                provider,
                session_date=session_date,
                clock=clock,
                publish_issue=args.publish_issue,
            )
        finally:
            await runtime.close()
        return 0
    if args.session or args.publish_issue:
        parser.error("--session and --publish-issue require --mode rehearsal")
    if args.mode == "premarket" and not args.checkpoint:
        parser.error("--checkpoint is required with --mode premarket")
    if args.mode != "premarket" and args.checkpoint:
        parser.error("--checkpoint requires --mode premarket")
    runtime, fixture_client = await build_runtime(
        cfg,
        now=now,
        fixture_dir=args.fixtures,
        fixture_profile=args.fixture_profile,
    )
    try:
        await runtime.run(args.mode, now=now, checkpoint=args.checkpoint)
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

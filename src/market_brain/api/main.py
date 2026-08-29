from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import UTC, datetime

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from market_brain.alerts.dispatcher import AlertDispatcher
from market_brain.alerts.sink import TelegramSink, WebhookSink
from market_brain.domain.models import AlertRecord, StrategyLane
from market_brain.engines.quality import classify_quality
from market_brain.ledger.events import LedgerEvent
from market_brain.ledger.replay import replay_check
from market_brain.ledger.store import InMemoryEventStore, PostgresEventStore
from market_brain.orchestration.screener import MarketScreener
from market_brain.orchestration.service import DecisionService
from market_brain.providers import build_market_data_provider
from market_brain.runtime.daily_digest import DailyDigest
from market_brain.runtime.position_monitor import PositionMonitor
from market_brain.runtime.radar_scheduler import RadarScheduler
from market_brain.runtime.shadow import ShadowEvaluator
from market_brain.runtime.stream_health import StreamStaleMonitor
from market_brain.settings import settings
from market_brain.version import __version__


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


store = PostgresEventStore(settings.postgres_dsn) if settings.postgres_dsn else InMemoryEventStore()
market_data = build_market_data_provider(event_store=store)
service = DecisionService(store, market_data=market_data)
screener = MarketScreener(market_data)
radar_scheduler = RadarScheduler(
    service=service,
    screener=screener,
    universe_dir=settings.universe_dir,
    quality_path=settings.quality_path,
    calendar_path=settings.market_calendar_path,
    plans_per_run=settings.plans_per_run,
    poll_seconds=settings.radar_poll_seconds,
    daily_digest=DailyDigest(service.store),
)
shadow_evaluator = ShadowEvaluator(
    service.store,
    backfill=service.backfill_intraday_structures,
)
stream_stale_monitor = StreamStaleMonitor(service.store)


def build_alert_sinks():
    return [
        WebhookSink(settings.webhook_url),
        TelegramSink(settings.telegram_bot_token, settings.telegram_chat_id),
    ]


def active_alert_sink_names() -> list[str]:
    return [sink.name for sink in build_alert_sinks() if sink.configured]


@asynccontextmanager
async def lifespan(_app: FastAPI):
    startup_at = datetime.now(UTC)
    radar_scheduler.validate_startup()
    shadow_evaluator.validate_startup(now=startup_at)
    await shadow_evaluator.catch_up(now=startup_at)
    stream_stale_monitor.validate_startup()
    differences = await replay_check(service.store)
    if differences:
        async with service.store.transaction():
            await service.store.append(
                LedgerEvent("REPLAY_MISMATCH", "materialized_state", {"differences": differences})
            )
            await service.store.save_alert(
                AlertRecord(
                    kind="RECONCILE_REQUIRED",
                    payload={
                        "action": "RECONCILE_REQUIRED",
                        "reason": "REPLAY_MISMATCH",
                        "differences": differences,
                    },
                )
            )
    await service.sweep_expired()
    await service.refresh_liquidity_profiles()
    dispatcher = AlertDispatcher(
        service.store,
        build_alert_sinks(),
        poll_seconds=settings.alert_poll_seconds,
        max_attempts=settings.alert_max_attempts,
        run_mode=settings.run_mode,
        data_plan=settings.data_plan,
    )
    task = asyncio.create_task(dispatcher.run())
    monitor = PositionMonitor(service)
    monitor_task = asyncio.create_task(monitor.run())
    radar_task = asyncio.create_task(radar_scheduler.run())
    shadow_task = asyncio.create_task(shadow_evaluator.run())
    stale_task = asyncio.create_task(stream_stale_monitor.run())
    try:
        yield
    finally:
        await stream_stale_monitor.stop()
        stale_task.cancel()
        try:
            await stale_task
        except asyncio.CancelledError:
            pass
        await shadow_evaluator.stop()
        shadow_task.cancel()
        try:
            await shadow_task
        except asyncio.CancelledError:
            pass
        await radar_scheduler.stop()
        radar_task.cancel()
        try:
            await radar_task
        except asyncio.CancelledError:
            pass
        await monitor.stop()
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
        await dispatcher.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        close = getattr(market_data, "aclose", None)
        if close is not None:
            await close()


app = FastAPI(
    title="Market Brain V4 Brokerless",
    version=__version__,
    lifespan=lifespan,
)


class ScreenRequest(StrictModel):
    symbols: list[str] = Field(min_length=1, max_length=500)
    top_n: int = Field(default=10, ge=1, le=50)


class WalletSeedRequest(StrictModel):
    capital_base: float = Field(gt=0)
    cash_available: float = Field(ge=0)


class PlanRequest(StrictModel):
    symbol: str = Field(min_length=1)
    quality_score: float = Field(ge=0, le=100)
    quality_as_of: datetime
    lane: StrategyLane = StrategyLane.CORE_MOMENTUM
    catalyst_verified: bool = False
    catalyst_strength: float = Field(default=0.0, ge=0, le=1)
    structure_score: float = Field(ge=0, le=15)
    rr_score: float = Field(ge=0, le=10)


class ActivateRequest(StrictModel):
    pass


class FillRequest(StrictModel):
    plan_id: str
    fill_price: float = Field(gt=0)
    quantity: int = Field(gt=0)
    time_stop_minutes: int = Field(default=30, ge=1, le=240)
    stop_order_placed: bool = False
    stop_order_price: float | None = Field(default=None, gt=0)
    broker_order_ref: str | None = None


class ProtectRequest(StrictModel):
    stop_order_price: float = Field(gt=0)
    broker_order_ref: str | None = None


class PositionImportRequest(StrictModel):
    symbol: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    average_fill: float = Field(gt=0)
    stop_order_price: float | None = Field(default=None, gt=0)
    broker_order_ref: str | None = None


class ReconcileHolding(StrictModel):
    symbol: str = Field(min_length=1)
    quantity: int = Field(gt=0)


class PositionEvaluateRequest(StrictModel):
    last: float = Field(gt=0)
    below_vwap: bool = False
    failed_breakout: bool = False


class ExitRequest(StrictModel):
    exit_price: float = Field(gt=0)
    quantity: int = Field(gt=0)


@app.get("/health")
async def health() -> dict:
    runtime = await service.store.get_runtime_status()
    last = runtime.get("stream_last_message_at")
    stream_age_seconds = None
    if isinstance(last, str):
        try:
            stamp = datetime.fromisoformat(last)
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=UTC)
            stream_age_seconds = max(0.0, (datetime.now(UTC) - stamp).total_seconds())
        except ValueError:
            stream_age_seconds = None
    return {
        "status": "ok",
        "version": __version__,
        "architecture": "BROKERLESS_EVENT_SOURCED",
        "direct_account_access_allowed": settings.direct_account_access_allowed,
        "execution_actions_allowed": settings.execution_actions_allowed,
        "data_plan": settings.data_plan,
        "run_mode": settings.run_mode,
        "decision_feed": settings.decision_feed,
        "historical_feed": settings.historical_feed,
        "historical_lag_minutes": settings.historical_lag_minutes,
        "active_alert_sinks": active_alert_sink_names(),
        "stream_connected": runtime.get("stream_connected", False),
        "stream_last_message_at": last,
        "subscribed_symbols": runtime.get("subscribed_symbols", []),
        "subscription_cap": runtime.get("subscription_cap", settings.stream_max_symbols),
        "dropped_symbols": runtime.get("dropped_symbols", []),
        "stream_age_seconds": stream_age_seconds,
        "stream_stale": runtime.get("stream_stale", False),
    }


@app.get("/policy")
def policy() -> dict:
    return {
        "market_data_only": True,
        "position_truth": "USER_CONFIRMED_EVENT_LEDGER",
        "capital_truth": "SOFTWARE_DEFINED_RISK_WALLET",
        "automatic_execution": False,
        "unknown_external_trades": "INVISIBLE_FAIL_CLOSED",
    }


@app.get("/admin/replay-check")
async def admin_replay_check() -> dict:
    differences = await replay_check(service.store)
    return {"ok": not differences, "differences": differences}


@app.get("/alerts")
async def alerts(undelivered: bool = True) -> list[dict]:
    if not undelivered:
        raise HTTPException(status_code=422, detail="ONLY_UNDELIVERED_ALERTS_SUPPORTED")
    return [asdict(row) for row in await service.store.list_undelivered()]


@app.post("/screen")
async def screen(req: ScreenRequest) -> dict:
    try:
        rows = await screener.screen([symbol.upper() for symbol in req.symbols], req.top_n)
        return {
            "mode": "BROKERLESS_DISCOVERY",
            "rows": list(rows),
            "skipped_symbols": [asdict(item) for item in rows.skipped_symbols],
        }
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.post("/plans/{plan_id}/release")
async def release_plan_capacity(plan_id: str) -> dict:
    released = await service.release_reservation(plan_id)
    return {"plan_id": plan_id, "released": released}


@app.post("/wallet/seed")
async def seed_wallet(req: WalletSeedRequest) -> dict:
    try:
        return asdict(await service.seed_wallet(req.capital_base, req.cash_available))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.get("/wallet")
async def wallet() -> dict:
    state = await service.store.get_wallet()
    if state is None:
        raise HTTPException(status_code=404, detail="WALLET_NOT_SEEDED")
    return asdict(state)


@app.post("/plans")
async def create_plan(req: PlanRequest) -> dict:
    try:
        profile = classify_quality(req.symbol.upper(), req.quality_score, req.quality_as_of)
        plan, evidence = await service.build_plan_from_market(
            symbol=req.symbol,
            quality=profile,
            lane=req.lane,
            catalyst_verified=req.catalyst_verified,
            catalyst_strength=req.catalyst_strength,
            structure_score=req.structure_score,
            rr_score=req.rr_score,
        )
        return {"plan": asdict(plan), **evidence}
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.post("/plans/{plan_id}/activate")
async def activate(plan_id: str, req: ActivateRequest | None = None) -> dict:
    try:
        decision = await service.activate(plan_id)
        return asdict(decision)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.post("/fills/confirm")
async def confirm_fill(req: FillRequest) -> dict:
    try:
        position = await service.confirm_fill(
            req.plan_id,
            fill_price=req.fill_price,
            quantity=req.quantity,
            time_stop_minutes=req.time_stop_minutes,
            stop_order_placed=req.stop_order_placed,
            stop_order_price=req.stop_order_price,
            broker_order_ref=req.broker_order_ref,
        )
        return asdict(position)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.get("/positions")
async def positions() -> list[dict]:
    return [asdict(row) for row in await service.store.list_positions()]


@app.post("/positions/import")
async def import_position(req: PositionImportRequest) -> dict:
    try:
        position = await service.import_position(
            symbol=req.symbol,
            quantity=req.quantity,
            average_fill=req.average_fill,
            stop_order_price=req.stop_order_price,
            broker_order_ref=req.broker_order_ref,
        )
        return asdict(position)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.post("/reconcile")
async def reconcile(holdings: list[ReconcileHolding]) -> dict:
    try:
        return await service.reconcile_holdings(
            [holding.model_dump() for holding in holdings]
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.post("/positions/{position_id}/protect")
async def protect_position(position_id: str, req: ProtectRequest) -> dict:
    try:
        position = await service.protect_position(
            position_id,
            stop_order_price=req.stop_order_price,
            broker_order_ref=req.broker_order_ref,
        )
        return asdict(position)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.post("/positions/{position_id}/evaluate")
async def position_evaluate(position_id: str, req: PositionEvaluateRequest) -> dict:
    action = await service.evaluate_position(
        position_id,
        last=req.last,
        below_vwap=req.below_vwap,
        failed_breakout=req.failed_breakout,
    )
    return {"position_id": position_id, "action": str(action)}


@app.post("/positions/{position_id}/exit")
async def confirm_exit(position_id: str, req: ExitRequest) -> dict:
    try:
        position = await service.confirm_exit(
            position_id,
            exit_price=req.exit_price,
            quantity=req.quantity,
        )
        return asdict(position)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

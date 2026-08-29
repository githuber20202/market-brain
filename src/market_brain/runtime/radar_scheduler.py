from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime, time, timedelta
from pathlib import Path

from market_brain.domain.models import (
    AlertRecord,
    EvidenceCard,
    QualityProfile,
    StrategyLane,
)
from market_brain.ledger.events import LedgerEvent
from market_brain.orchestration.universe import (
    EASTERN,
    ManualQuality,
    MarketSession,
    NyseMarketCalendar,
    UniverseEntry,
    load_manual_quality,
    load_market_calendar,
    load_universe,
)
from market_brain.providers.base import DataUnavailable

LOGGER = logging.getLogger(__name__)


class RadarScheduler:
    def __init__(
        self,
        *,
        service,
        screener,
        universe_dir: Path,
        quality_path: Path,
        calendar_path: Path,
        plans_per_run: int = 5,
        poll_seconds: float = 5.0,
        daily_digest=None,
        now: Callable[[], datetime] | None = None,
        sleep=asyncio.sleep,
    ) -> None:
        self.service = service
        self.screener = screener
        self.universe_dir = universe_dir
        self.quality_path = quality_path
        self.calendar_path = calendar_path
        self.plans_per_run = plans_per_run
        self.poll_seconds = poll_seconds
        self.daily_digest = daily_digest
        self.now = now or (lambda: datetime.now(UTC))
        self.sleep = sleep
        self.universe: tuple[UniverseEntry, ...] = ()
        self.quality: dict[str, ManualQuality] = {}
        self.calendar: NyseMarketCalendar | None = None
        self._attempted: set[tuple[str, str]] = set()
        self._stop = asyncio.Event()

    def validate_startup(self, *, now: datetime | None = None) -> None:
        timestamp = _aware(now or self.now()).astimezone(EASTERN)
        self.universe = load_universe(self.universe_dir)
        self.quality = load_manual_quality(self.quality_path)
        self.calendar = load_market_calendar(
            self.calendar_path,
            required_years={timestamp.year, timestamp.year + 1},
        )

    async def run_pending(self, *, now: datetime | None = None) -> dict | None:
        if self.calendar is None or not self.universe:
            raise RuntimeError("RADAR_SCHEDULER_NOT_VALIDATED")
        timestamp = _aware(now or self.now()).astimezone(EASTERN)
        session = self.calendar.session_for(timestamp.date())
        digest = await self._run_daily_digest_pending(timestamp, session)
        if digest is not None:
            return digest
        if session is None or not session.opens_at <= timestamp < session.closes_at:
            return None
        slot = _matching_slot(timestamp, session)
        if slot is None:
            return None
        catch_up_slot = await self._unavailable_catch_up_slot(session, slot)
        scheduled_slot = catch_up_slot or slot
        return await self._run_slot(
            scheduled_slot,
            timestamp=timestamp,
            attempt_slot=slot,
        )

    async def run_slot(
        self,
        slot: datetime,
        *,
        now: datetime | None = None,
    ) -> dict | None:
        if self.calendar is None or not self.universe:
            raise RuntimeError("RADAR_SCHEDULER_NOT_VALIDATED")
        scheduled_slot = _aware(slot).astimezone(EASTERN)
        timestamp = _aware(now or self.now()).astimezone(EASTERN)
        session = self.calendar.session_for(scheduled_slot.date())
        if (
            session is None
            or scheduled_slot not in scheduled_slots(session)
            or scheduled_slot > timestamp
        ):
            raise ValueError("RADAR_SLOT_INVALID")
        return await self._run_slot(
            scheduled_slot,
            timestamp=timestamp,
            attempt_slot=timestamp,
        )

    async def _run_slot(
        self,
        scheduled_slot: datetime,
        *,
        timestamp: datetime,
        attempt_slot: datetime,
    ) -> dict | None:
        run_id = (
            f"radar:{scheduled_slot.date().isoformat()}:{scheduled_slot.strftime('%H%M')}"
        )
        attempt_key = (run_id, attempt_slot.isoformat())
        if attempt_key in self._attempted:
            return None
        existing = await self.service.store.get_runtime_status_key(f"radar_run:{run_id}")
        if isinstance(existing, dict) and existing.get("status") in {
            "COMPLETED",
            "MISSED",
        }:
            self._attempted.add(attempt_key)
            return None
        self._attempted.add(attempt_key)
        await self.service.store.set_runtime_status(
            f"radar_run:{run_id}",
            {
                "status": "STARTED",
                "scheduled_for": scheduled_slot.isoformat(),
                "attempt_slot": attempt_slot.isoformat(),
                "started_at": timestamp.isoformat(),
            },
        )
        try:
            return await self._execute(run_id, scheduled_slot, timestamp)
        except DataUnavailable as exc:
            return await self._persist_result(
                run_id,
                scheduled_slot,
                status="DATA_UNAVAILABLE",
                candidates=[],
                plan_ids=[],
                error_type=exc.error_type,
                unavailable={
                    "source_id": exc.source_id,
                    "resource": exc.resource,
                    "symbol": exc.symbol,
                    "attempt_slot": attempt_slot.isoformat(),
                },
                skipped_symbols=[
                    {"symbol": item.symbol, "error_type": item.error_type}
                    for item in exc.skipped_symbols
                ],
            )
        except Exception as exc:
            LOGGER.exception("radar_run_failed run_id=%s", run_id)
            return await self._persist_result(
                run_id,
                scheduled_slot,
                status="FAILED",
                candidates=[],
                plan_ids=[],
                error_type=type(exc).__name__,
            )

    async def mark_missed(
        self,
        slot: datetime,
        *,
        now: datetime,
    ) -> dict:
        scheduled_slot = _aware(slot).astimezone(EASTERN)
        timestamp = _aware(now).astimezone(EASTERN)
        run_id = (
            f"radar:{scheduled_slot.date().isoformat()}:{scheduled_slot.strftime('%H%M')}"
        )
        key = f"radar_run:{run_id}"
        existing = await self.service.store.get_runtime_status_key(key)
        if isinstance(existing, dict) and existing.get("status") in {
            "COMPLETED",
            "MISSED",
        }:
            return existing
        payload = {
            "run_id": run_id,
            "status": "MISSED",
            "scheduled_for": scheduled_slot.isoformat(),
            "missed_at": timestamp.isoformat(),
            "previous_status": (
                existing.get("status") if isinstance(existing, dict) else None
            ),
            "universe_size": len(self.universe),
            "candidates": [],
            "plan_ids": [],
            "error_type": None,
            "data_unavailable": None,
            "skipped_symbols": [],
        }
        async with self.service.store.transaction():
            await self.service.store.append(LedgerEvent("RADAR_RUN", run_id, payload))
            await self.service.store.set_runtime_status(key, payload)
        return payload

    async def _unavailable_catch_up_slot(
        self,
        session: MarketSession,
        current_slot: datetime,
    ) -> datetime | None:
        for candidate in scheduled_slots(session):
            if candidate >= current_slot:
                break
            run_id = f"radar:{candidate.date().isoformat()}:{candidate.strftime('%H%M')}"
            status = await self.service.store.get_runtime_status_key(f"radar_run:{run_id}")
            if (
                isinstance(status, dict)
                and status.get("status") == "DATA_UNAVAILABLE"
                and (run_id, current_slot.isoformat()) not in self._attempted
            ):
                return candidate
        return None

    async def _execute(self, run_id: str, slot: datetime, timestamp: datetime) -> dict:
        symbols = [entry.symbol for entry in self.universe if entry.ranking_eligible]
        screen_result = await self.screener.screen(symbols, top_n=len(symbols))
        rows = list(screen_result)
        skipped = tuple(getattr(screen_result, "skipped_symbols", ()))
        cfg = getattr(self.service, "cfg", None)
        if getattr(cfg, "data_plan", None) == "keyless_delayed" and skipped:
            skipped_names = {item.symbol for item in skipped}
            failure_ratio = len(skipped) / len(symbols) if symbols else 1.0
            if "SPY" in skipped_names or failure_ratio > cfg.keyless_max_failure_ratio:
                raise DataUnavailable(
                    source_id="YAHOO_DELAYED",
                    resource="snapshots",
                    symbol="SPY" if "SPY" in skipped_names else "UNIVERSE",
                    error_type=(
                        "MARKET_ANCHOR_UNAVAILABLE"
                        if "SPY" in skipped_names
                        else "KEYLESS_FAILURE_RATIO_EXCEEDED"
                    ),
                    skipped_symbols=skipped,
                )
        if (
            getattr(cfg, "data_plan", None) == "keyless_delayed"
            and hasattr(self.service, "prepare_plan_market_data")
        ):
            instrument_types = {
                entry.symbol: entry.instrument_type for entry in self.universe
            }
            for row in rows[: self.plans_per_run]:
                snapshot = row.get("snapshot", {})
                symbol = str(snapshot.get("symbol", "")).upper()
                eligible = instrument_types.get(symbol) == "ETF" or symbol in self.quality or bool(
                    snapshot.get("catalyst_verified", False)
                )
                if not eligible:
                    continue
                try:
                    await self.service.prepare_plan_market_data(
                        symbol,
                        now=timestamp.astimezone(UTC),
                    )
                except DataUnavailable:
                    raise
                except (RuntimeError, ValueError, TypeError):
                    continue
        candidates: list[dict] = []
        plan_ids: list[str] = []
        instrument_types = {
            entry.symbol: entry.instrument_type for entry in self.universe
        }
        for row in rows[: self.plans_per_run]:
            snapshot = row.get("snapshot", {})
            score = row.get("score", {})
            symbol = str(snapshot.get("symbol", "")).upper()
            candidate = {
                "symbol": symbol,
                "rank_score": score.get("discovery_total"),
                "lane": None,
                "quality_source": None,
                "plan_id": None,
                "levels": None,
                "reason": None,
            }
            manual_quality = self.quality.get(symbol)
            catalyst_verified = bool(snapshot.get("catalyst_verified", False))
            catalyst_strength = float(snapshot.get("catalyst_strength", 0.0) or 0.0)
            if manual_quality is not None:
                quality = manual_quality.profile()
                lane = StrategyLane.CORE_MOMENTUM
                candidate["quality_source"] = manual_quality.source
            elif instrument_types.get(symbol) == "ETF":
                quality = QualityProfile(
                    symbol=symbol,
                    score=0.0,
                    tier="NOT_APPLICABLE",
                    risk_multiplier=1.0,
                    as_of=timestamp.astimezone(UTC),
                    evidence=[
                        EvidenceCard(
                            evidence_type="QUALITY_NOT_APPLICABLE",
                            summary="Company fundamentals quality is not applicable to ETFs",
                            source="INSTRUMENT_TYPE_ETF",
                            published_at=timestamp.astimezone(UTC),
                            confidence=1.0,
                            expires_at=datetime.max.replace(tzinfo=UTC),
                        )
                    ],
                )
                lane = StrategyLane.CORE_MOMENTUM
                candidate["quality_source"] = "NOT_APPLICABLE_ETF"
            elif catalyst_verified:
                quality = QualityProfile(
                    symbol=symbol,
                    score=35.0,
                    tier="UNRATED",
                    risk_multiplier=0.0,
                    as_of=timestamp.astimezone(UTC),
                )
                lane = StrategyLane.EVENT_MOMENTUM
                candidate["quality_source"] = "MISSING_EVENT_ONLY"
            else:
                candidate["reason"] = "QUALITY_MISSING_CORE_BLOCKED"
                candidates.append(candidate)
                continue
            candidate["lane"] = str(lane)
            try:
                plan, _evidence = await self.service.build_plan_from_market(
                    symbol=symbol,
                    quality=quality,
                    lane=lane,
                    catalyst_verified=catalyst_verified,
                    catalyst_strength=catalyst_strength,
                    structure_score=15.0,
                    rr_score=10.0,
                    now=timestamp.astimezone(UTC),
                )
            except DataUnavailable:
                raise
            except (RuntimeError, ValueError, TypeError) as exc:
                candidate["reason"] = str(exc) or type(exc).__name__
                candidates.append(candidate)
                continue
            candidate["plan_id"] = plan.plan_id
            candidate["levels"] = {
                "entry_trigger": plan.entry_trigger,
                "entry_zone_high": plan.entry_zone_high,
                "stop": plan.stop,
                "tp1": plan.tp1,
                "tp2": plan.tp2,
            }
            plan_ids.append(plan.plan_id)
            candidates.append(candidate)
        return await self._persist_result(
            run_id,
            slot,
            status="COMPLETED",
            candidates=candidates,
            plan_ids=plan_ids,
            skipped_symbols=[
                {"symbol": item.symbol, "error_type": item.error_type}
                for item in skipped
            ],
        )

    async def _persist_result(
        self,
        run_id: str,
        slot: datetime,
        *,
        status: str,
        candidates: list[dict],
        plan_ids: list[str],
        error_type: str | None = None,
        unavailable: dict | None = None,
        skipped_symbols: list[dict] | None = None,
    ) -> dict:
        payload = {
            "run_id": run_id,
            "status": status,
            "scheduled_for": slot.isoformat(),
            "universe_size": len(self.universe),
            "candidates": candidates,
            "plan_ids": plan_ids,
            "error_type": error_type,
            "data_unavailable": unavailable,
            "skipped_symbols": skipped_symbols or [],
        }
        async with self.service.store.transaction():
            await self.service.store.append(LedgerEvent("RADAR_RUN", run_id, payload))
            if status == "DATA_UNAVAILABLE":
                await self.service.store.append(
                    LedgerEvent("DATA_UNAVAILABLE", run_id, payload)
                )
            await self.service.store.save_alert(AlertRecord(kind="RADAR_DIGEST", payload=payload))
            await self.service.store.set_runtime_status(f"radar_run:{run_id}", payload)
        return payload

    async def _run_daily_digest_pending(
        self,
        timestamp: datetime,
        session: MarketSession | None,
    ) -> dict | None:
        if self.daily_digest is None or session is None:
            return None
        scheduled = datetime.combine(timestamp.date(), time(16, 15), EASTERN)
        if timestamp.replace(second=0, microsecond=0) != scheduled:
            return None
        run_id = f"daily_digest:{timestamp.date().isoformat()}"
        alert = await self.daily_digest.create(
            now=timestamp.astimezone(UTC),
            run_id=run_id,
        )
        if alert is None:
            return None
        return {
            "run_id": run_id,
            "status": "COMPLETED",
            "scheduled_for": scheduled.isoformat(),
            "alert_id": alert.alert_id,
        }

    async def run(self) -> None:
        self._stop.clear()
        while not self._stop.is_set():
            try:
                await self.run_pending()
            except asyncio.CancelledError:
                raise
            except (RuntimeError, ValueError, OSError) as exc:
                LOGGER.error("radar_scheduler_fail_closed error_type=%s", type(exc).__name__)
            await self.sleep(self.poll_seconds)

    async def stop(self) -> None:
        self._stop.set()


def scheduled_slots(session: MarketSession) -> tuple[datetime, ...]:
    slot = datetime.combine(session.session_date, time(9, 50), EASTERN)
    latest = min(
        datetime.combine(session.session_date, time(14, 50), EASTERN),
        session.closes_at,
    )
    slots: list[datetime] = []
    while slot <= latest and slot < session.closes_at:
        slots.append(slot)
        slot += timedelta(minutes=30)
    return tuple(slots)


def _matching_slot(timestamp: datetime, session: MarketSession) -> datetime | None:
    minute = timestamp.replace(second=0, microsecond=0)
    return minute if minute in scheduled_slots(session) else None


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

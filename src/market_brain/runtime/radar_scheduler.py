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

DISCOVERY_START = time(9, 50)
DISCOVERY_END = time(15, 50)
DISCOVERY_INTERVAL = timedelta(minutes=10)


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
                "discovery_status": "STARTED",
                "planning_status": "NOT_STARTED",
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
                discovery_status="DATA_UNAVAILABLE",
                planning_status="NOT_RUN",
                candidates=[],
                plan_ids=[],
                error_type=exc.error_type,
                unavailable={
                    "source_id": exc.source_id,
                    "resource": exc.resource,
                    "symbol": exc.symbol,
                    "reason_codes": list(exc.reason_codes),
                    "attempt_slot": attempt_slot.isoformat(),
                },
                skipped_symbols=[
                    {"symbol": item.symbol, "error_type": item.error_type}
                    for item in exc.skipped_symbols
                ],
                rankings=self._blank_rankings(
                    exc.error_type,
                    skipped_symbols={
                        item.symbol.upper(): item.error_type for item in exc.skipped_symbols
                    },
                ),
                occurred_at=timestamp,
            )
        except Exception as exc:
            LOGGER.exception("radar_run_failed run_id=%s", run_id)
            return await self._persist_result(
                run_id,
                scheduled_slot,
                status="FAILED",
                discovery_status="FAILED",
                planning_status="NOT_RUN",
                candidates=[],
                plan_ids=[],
                error_type=type(exc).__name__,
                rankings=self._blank_rankings(type(exc).__name__),
                occurred_at=timestamp,
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
        prior = existing if isinstance(existing, dict) else {}
        discovery_completed = prior.get("discovery_status") == "COMPLETED"
        prior_rankings = prior.get("rankings")
        payload = {
            "run_id": run_id,
            "status": "MISSED",
            "discovery_status": "COMPLETED" if discovery_completed else "MISSED",
            "planning_status": (
                str(prior.get("planning_status") or "BLOCKED_DATA_UNAVAILABLE")
                if discovery_completed
                else "NOT_RUN"
            ),
            "scheduled_for": scheduled_slot.isoformat(),
            "missed_at": timestamp.isoformat(),
            "previous_status": prior.get("status"),
            "universe_size": len(self.universe),
            "candidates": prior.get("candidates", []),
            "plan_ids": prior.get("plan_ids", []),
            "error_type": prior.get("error_type"),
            "data_unavailable": prior.get("data_unavailable"),
            "planning_failures": prior.get("planning_failures", []),
            "skipped_symbols": prior.get("skipped_symbols", []),
            "score_histogram": prior.get("score_histogram", _empty_score_histogram()),
            "liquidity_refresh": prior.get("liquidity_refresh"),
            "rankings": (
                prior_rankings
                if isinstance(prior_rankings, list) and prior_rankings
                else self._blank_rankings("RADAR_SLOT_MISSED")
            ),
        }
        async with self.service.store.transaction():
            await self.service.store.append(
                LedgerEvent("RADAR_RUN", run_id, payload, occurred_at=timestamp)
            )
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
        liquidity_refresh = await self.service.refresh_liquidity_profiles_for_symbols(
            symbols,
            now=timestamp.astimezone(UTC),
        )
        screen_result = await self.screener.screen(
            symbols,
            top_n=len(symbols),
            now=timestamp.astimezone(UTC),
            structure_score=15.0,
            rr_score=10.0,
        )
        rows = list(screen_result)
        score_histogram = _score_histogram(rows)
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
        if not rows:
            raise DataUnavailable(
                source_id=(
                    "YAHOO_DELAYED"
                    if getattr(cfg, "data_plan", None) == "keyless_delayed"
                    else "RADAR_DISCOVERY"
                ),
                resource="rankings",
                symbol="UNIVERSE",
                error_type="DISCOVERY_EVIDENCE_MISSING",
                skipped_symbols=skipped,
            )
        planning_failures: list[dict] = []
        prepared_snapshots: dict[str, object] = {}
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
                    prepared_snapshots[symbol] = await self.service.prepare_plan_market_data(
                        symbol,
                        now=timestamp.astimezone(UTC),
                    )
                except DataUnavailable as exc:
                    planning_failures.append(self._planning_failure(exc))
                except (RuntimeError, ValueError, TypeError):
                    continue
        candidates: list[dict] = []
        plan_ids: list[str] = []
        planning_failures_by_symbol = {
            str(row["symbol"]).upper(): row for row in planning_failures
        }
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
                "score_components": {
                    "catalyst": score.get("catalyst_or_continuation"),
                    "momentum": score.get("price_momentum"),
                    "volume": score.get("volume_liquidity"),
                    "relative": score.get("relative_strength_sector"),
                    "structure": score.get("entry_invalidation_structure"),
                    "rr": score.get("risk_reward"),
                    "total": score.get("total"),
                },
                "lane": None,
                "quality_source": None,
                "plan_id": None,
                "levels": None,
                "reason": None,
                "planning_status": "NOT_EVALUATED",
                "planning_reason_codes": [],
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
                candidate["planning_status"] = "BLOCKED_POLICY"
                candidates.append(candidate)
                continue
            candidate["lane"] = str(lane)
            if planning_failures:
                failure = planning_failures_by_symbol.get(symbol)
                if failure is None:
                    candidate["planning_status"] = "WITHHELD_FAIL_CLOSED"
                else:
                    candidate["reason"] = failure["error_type"]
                    candidate["planning_status"] = "DATA_UNAVAILABLE"
                    candidate["planning_reason_codes"] = failure["reason_codes"]
                candidates.append(candidate)
                continue
            try:
                prepared_snapshot = prepared_snapshots.get(symbol)
                if prepared_snapshot is not None and hasattr(self.service, "build_plan"):
                    prepared_snapshot.catalyst_verified = catalyst_verified
                    prepared_snapshot.catalyst_strength = catalyst_strength
                    plan, _evidence = await self.service.build_plan(
                        prepared_snapshot,
                        quality,
                        lane,
                        15.0,
                        10.0,
                        now=timestamp.astimezone(UTC),
                    )
                else:
                    plan, _evidence = await self.service.build_plan_from_market(
                        symbol=symbol,
                        quality=quality,
                        lane=lane,
                        catalyst_verified=catalyst_verified,
                        catalyst_strength=catalyst_strength,
                        structure_score=15.0,
                        rr_score=10.0,
                        benchmark_return_pct=snapshot.get("benchmark_return_pct"),
                        now=timestamp.astimezone(UTC),
                    )
            except DataUnavailable:
                raise
            except (RuntimeError, ValueError, TypeError) as exc:
                candidate["reason"] = str(exc) or type(exc).__name__
                candidate["planning_status"] = "REJECTED"
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
            candidate["planning_status"] = "COMPLETED"
            plan_ids.append(plan.plan_id)
            candidates.append(candidate)
        rankings = self._ranking_table(
            rows,
            candidates=candidates,
            skipped_symbols={item.symbol.upper(): item.error_type for item in skipped},
        )
        first_planning_failure = planning_failures[0] if planning_failures else None
        return await self._persist_result(
            run_id,
            slot,
            status="DATA_UNAVAILABLE" if planning_failures else "COMPLETED",
            discovery_status="COMPLETED",
            planning_status=(
                "BLOCKED_DATA_UNAVAILABLE" if planning_failures else "COMPLETED"
            ),
            planning_failures=planning_failures,
            candidates=candidates,
            plan_ids=plan_ids,
            error_type=(
                str(first_planning_failure["error_type"])
                if first_planning_failure is not None
                else None
            ),
            unavailable=(
                {
                    "scope": "PLANNING",
                    "source_id": first_planning_failure["source_id"],
                    "resource": first_planning_failure["resource"],
                    "symbol": first_planning_failure["symbol"],
                    "reason_codes": first_planning_failure["reason_codes"],
                    "failure_count": len(planning_failures),
                }
                if first_planning_failure is not None
                else None
            ),
            skipped_symbols=[
                {"symbol": item.symbol, "error_type": item.error_type}
                for item in skipped
            ],
            score_histogram=score_histogram,
            liquidity_refresh=liquidity_refresh,
            rankings=rankings,
            occurred_at=timestamp,
        )

    @staticmethod
    def _planning_failure(exc: DataUnavailable) -> dict:
        return {
            "symbol": exc.symbol,
            "source_id": exc.source_id,
            "resource": exc.resource,
            "error_type": exc.error_type,
            "reason_codes": list(exc.reason_codes),
        }

    def _ranking_table(
        self,
        rows: list[dict],
        *,
        candidates: list[dict],
        skipped_symbols: dict[str, str],
    ) -> list[dict]:
        candidates_by_symbol = {
            str(row.get("symbol") or "").upper(): row for row in candidates
        }
        ranked_by_symbol: dict[str, dict] = {}
        for rank, row in enumerate(rows, start=1):
            snapshot = row.get("snapshot") if isinstance(row.get("snapshot"), dict) else {}
            score = row.get("score") if isinstance(row.get("score"), dict) else {}
            features = row.get("features") if isinstance(row.get("features"), dict) else {}
            symbol = str(snapshot.get("symbol") or "").upper()
            if not symbol:
                continue
            candidate = candidates_by_symbol.get(symbol, {})
            reasons = score.get("reasons")
            if isinstance(reasons, (list, tuple)):
                normalized_reasons = [str(value) for value in reasons]
            elif reasons is None or reasons == "":
                normalized_reasons = []
            else:
                normalized_reasons = [str(reasons)]
            candidate_reason = candidate.get("reason")
            if candidate_reason:
                normalized_reasons.append(str(candidate_reason))
            ranked_by_symbol[symbol] = {
                "rank": rank,
                "symbol": symbol,
                "data_status": "OK",
                "catalyst_or_continuation": score.get("catalyst_or_continuation"),
                "price_momentum": score.get("price_momentum"),
                "volume_liquidity": score.get("volume_liquidity"),
                "relative_strength_sector": score.get("relative_strength_sector"),
                "entry_invalidation_structure": score.get(
                    "entry_invalidation_structure"
                ),
                "risk_reward": score.get("risk_reward"),
                "total": score.get("total"),
                "discovery_total": score.get("discovery_total"),
                "reasons": list(dict.fromkeys(normalized_reasons)),
                "last": snapshot.get("last"),
                "volume": snapshot.get("volume"),
                "relative_volume": features.get("relative_volume"),
                "plan_id": candidate.get("plan_id"),
            }
        output: list[dict] = []
        symbols = [entry.symbol.upper() for entry in self.universe if entry.ranking_eligible]
        for symbol in symbols:
            row = ranked_by_symbol.get(symbol)
            if row is not None:
                output.append(row)
                continue
            output.append(
                self._blank_ranking_row(
                    symbol,
                    skipped_symbols.get(symbol, "MARKET_SNAPSHOT_MISSING"),
                )
            )
        return sorted(
            output,
            key=lambda row: (
                row["rank"] is None,
                row["rank"] if row["rank"] is not None else 10**9,
                row["symbol"],
            ),
        )

    def _blank_rankings(
        self,
        reason: str,
        *,
        skipped_symbols: dict[str, str] | None = None,
    ) -> list[dict]:
        skipped = skipped_symbols or {}
        return [
            self._blank_ranking_row(
                entry.symbol.upper(),
                skipped.get(entry.symbol.upper(), reason),
            )
            for entry in self.universe
            if entry.ranking_eligible
        ]

    @staticmethod
    def _blank_ranking_row(symbol: str, reason: str) -> dict:
        return {
            "rank": None,
            "symbol": symbol,
            "data_status": "MISSING",
            "catalyst_or_continuation": None,
            "price_momentum": None,
            "volume_liquidity": None,
            "relative_strength_sector": None,
            "entry_invalidation_structure": None,
            "risk_reward": None,
            "total": None,
            "discovery_total": None,
            "reasons": [reason],
            "last": None,
            "volume": None,
            "relative_volume": None,
            "plan_id": None,
        }

    async def _persist_result(
        self,
        run_id: str,
        slot: datetime,
        *,
        status: str,
        discovery_status: str | None = None,
        planning_status: str | None = None,
        planning_failures: list[dict] | None = None,
        candidates: list[dict],
        plan_ids: list[str],
        error_type: str | None = None,
        unavailable: dict | None = None,
        skipped_symbols: list[dict] | None = None,
        score_histogram: dict[str, int] | None = None,
        liquidity_refresh: dict | None = None,
        rankings: list[dict] | None = None,
        occurred_at: datetime | None = None,
    ) -> dict:
        payload = {
            "run_id": run_id,
            "status": status,
            "discovery_status": discovery_status or status,
            "planning_status": planning_status or (
                "COMPLETED" if status == "COMPLETED" else "NOT_RUN"
            ),
            "scheduled_for": slot.isoformat(),
            "universe_size": len(self.universe),
            "candidates": candidates,
            "plan_ids": plan_ids,
            "error_type": error_type,
            "data_unavailable": unavailable,
            "planning_failures": planning_failures or [],
            "skipped_symbols": skipped_symbols or [],
            "score_histogram": score_histogram or _empty_score_histogram(),
            "liquidity_refresh": liquidity_refresh,
            "rankings": rankings or [],
        }
        async with self.service.store.transaction():
            await self.service.store.append(
                LedgerEvent(
                    "RADAR_RUN",
                    run_id,
                    payload,
                    occurred_at=occurred_at or slot,
                )
            )
            if status == "DATA_UNAVAILABLE":
                await self.service.store.append(
                    LedgerEvent(
                        "DATA_UNAVAILABLE",
                        run_id,
                        payload,
                        occurred_at=occurred_at or slot,
                    )
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
    slot = datetime.combine(session.session_date, DISCOVERY_START, EASTERN)
    latest = min(
        datetime.combine(session.session_date, DISCOVERY_END, EASTERN),
        session.closes_at,
    )
    slots: list[datetime] = []
    while slot <= latest and slot < session.closes_at:
        slots.append(slot)
        slot += DISCOVERY_INTERVAL
    return tuple(slots)


def _matching_slot(timestamp: datetime, session: MarketSession) -> datetime | None:
    minute = timestamp.replace(second=0, microsecond=0)
    return minute if minute in scheduled_slots(session) else None


def _empty_score_histogram() -> dict[str, int]:
    return {"0-20": 0, "20-40": 0, "40-65": 0, "65+": 0}


def _score_histogram(rows: list[dict]) -> dict[str, int]:
    output = _empty_score_histogram()
    for row in rows:
        try:
            total = float(row["score"]["total"])
        except (KeyError, TypeError, ValueError):
            continue
        if total < 20.0:
            output["0-20"] += 1
        elif total < 40.0:
            output["20-40"] += 1
        elif total < 65.0:
            output["40-65"] += 1
        else:
            output["65+"] += 1
    return output


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from math import ceil

from market_brain.domain.models import ShadowTrade, ShadowTradeStatus
from market_brain.ledger.events import LedgerEvent
from market_brain.orchestration.universe import EASTERN, NyseMarketCalendar, load_market_calendar
from market_brain.replay.engine import (
    VirtualTradeState,
    advance_virtual_trade_ticks,
    finalize_virtual_trade,
    replay_summary,
    synthesize_bar_ticks,
)
from market_brain.settings import Settings, settings

_ACTIVE = {ShadowTradeStatus.OPEN, ShadowTradeStatus.TP1}
LOGGER = logging.getLogger(__name__)
_TERMINAL = {
    ShadowTradeStatus.STOPPED,
    ShadowTradeStatus.TP2,
    ShadowTradeStatus.TIME_STOP,
}


class ShadowEvaluator:
    def __init__(
        self,
        store,
        *,
        cfg: Settings = settings,
        backfill=None,
        clock=lambda: datetime.now(UTC),
        sleep=asyncio.sleep,
    ) -> None:
        self.store = store
        self.cfg = cfg
        self.backfill = backfill
        self.clock = clock
        self.sleep = sleep
        self.calendar: NyseMarketCalendar | None = None
        self._stop = asyncio.Event()

    def validate_startup(self, *, now: datetime | None = None) -> None:
        local = _aware(now or self.clock()).astimezone(EASTERN)
        self.calendar = load_market_calendar(
            self.cfg.market_calendar_path,
            required_years={local.year, local.year + 1},
        )

    async def run_pending(self, *, now: datetime | None = None) -> int:
        if self.cfg.run_mode != "shadow":
            return 0
        if self.calendar is None:
            raise RuntimeError("SHADOW_EVALUATOR_NOT_VALIDATED")
        timestamp = _aware(now or self.clock()).astimezone(EASTERN)
        evaluated = await self.catch_up(now=timestamp)
        session = self.calendar.session_for(timestamp.date())
        if session is None:
            return evaluated
        minute = timestamp.replace(second=0, microsecond=0)
        regular_slot = (
            session.opens_at <= minute < session.closes_at
            and (minute.minute - session.opens_at.minute) % 5 == 0
        )
        end_slot = self._end_slot(session.closes_at)
        finalize_key = f"shadow_finalize:{session.session_date.isoformat()}"
        finalize_due = (
            minute >= end_slot
            and await self.store.get_runtime_status_key(finalize_key) is None
        )
        if not regular_slot and not finalize_due:
            return evaluated
        if finalize_due:
            return evaluated + await self._finalize_session(
                session.session_date,
                end_slot=end_slot,
                now=timestamp,
            )
        slot_key = f"shadow_eval:{minute.date().isoformat()}:{minute.strftime('%H%M')}"
        if await self.store.get_runtime_status_key(slot_key) is not None:
            return evaluated
        count = await self.evaluate_session(
            session.session_date,
            finalize=False,
        )
        await self.store.set_runtime_status(
            slot_key,
            {"status": "COMPLETED", "evaluated": count, "at": minute.isoformat()},
        )
        return evaluated + count

    async def evaluate_now(self, *, now: datetime | None = None) -> int:
        """Evaluate all newly stored bars regardless of cron start minute."""
        if self.cfg.run_mode != "shadow":
            return 0
        if self.calendar is None:
            raise RuntimeError("SHADOW_EVALUATOR_NOT_VALIDATED")
        timestamp = _aware(now or self.clock()).astimezone(EASTERN)
        evaluated = await self.catch_up(now=timestamp)
        session = self.calendar.session_for(timestamp.date())
        if session is None:
            return evaluated
        end_slot = self._end_slot(session.closes_at)
        if timestamp >= end_slot:
            return evaluated + await self._finalize_session(
                session.session_date,
                end_slot=end_slot,
                now=timestamp,
            )
        if timestamp < session.opens_at:
            return evaluated
        return evaluated + await self.evaluate_session(
            session.session_date,
            finalize=False,
        )

    async def catch_up(self, *, now: datetime | None = None) -> int:
        if self.cfg.run_mode != "shadow":
            return 0
        if self.calendar is None:
            raise RuntimeError("SHADOW_EVALUATOR_NOT_VALIDATED")
        timestamp = _aware(now or self.clock()).astimezone(EASTERN)
        session_dates = sorted(
            {
                row.opened_at.astimezone(EASTERN).date()
                for row in await self.store.list_shadow_trades()
                if row.status in _ACTIVE
            }
        )
        evaluated = 0
        for session_date in session_dates:
            if session_date >= timestamp.date():
                continue
            try:
                session = self.calendar.session_for(session_date)
            except RuntimeError:
                await self._mark_session_pending(
                    session_date,
                    reason="MARKET_CALENDAR_UNAVAILABLE",
                    now=timestamp,
                )
                continue
            if session is None:
                await self._mark_session_pending(
                    session_date,
                    reason="MARKET_SESSION_UNAVAILABLE",
                    now=timestamp,
                )
                continue
            end_slot = self._end_slot(session.closes_at)
            if timestamp < end_slot:
                continue
            evaluated += await self._finalize_session(
                session_date,
                end_slot=end_slot,
                now=timestamp,
            )
        return evaluated

    async def _finalize_session(
        self,
        session_date: date,
        *,
        end_slot: datetime,
        now: datetime,
    ) -> int:
        finalize_key = f"shadow_finalize:{session_date.isoformat()}"
        if await self.store.get_runtime_status_key(finalize_key) is not None:
            return 0
        active = [
            row
            for row in await self.store.list_shadow_trades()
            if row.status in _ACTIVE
            and row.opened_at.astimezone(EASTERN).date() == session_date
        ]
        for trade in active:
            bars = await self._stored_bars(trade, session_date)
            if not bars:
                await self._backfill_once(
                    trade,
                    session_date=session_date,
                    end_slot=end_slot,
                    now=now,
                )
        evaluated = await self.evaluate_session(session_date, finalize=True)
        remaining = [
            row
            for row in await self.store.list_shadow_trades()
            if row.status in _ACTIVE
            and row.opened_at.astimezone(EASTERN).date() == session_date
        ]
        if remaining:
            for trade in remaining:
                bars = await self._stored_bars(trade, session_date)
                pending_key = f"shadow_finalize_pending:{trade.plan_id}"
                existing = await self.store.get_runtime_status_key(pending_key)
                reason = (
                    existing.get("reason")
                    if isinstance(existing, dict) and existing.get("status") == "PENDING"
                    else "FINALIZE_INCOMPLETE" if bars else "SIP_BARS_UNAVAILABLE"
                )
                await self.store.set_runtime_status(
                    pending_key,
                    {
                        "status": "PENDING",
                        "reason": reason,
                        "session_date": session_date.isoformat(),
                        "symbol": trade.symbol,
                        "at": now.isoformat(),
                    },
                )
            return evaluated
        for trade in active:
            pending_key = f"shadow_finalize_pending:{trade.plan_id}"
            if await self.store.get_runtime_status_key(pending_key) is not None:
                await self.store.set_runtime_status(
                    pending_key,
                    {
                        "status": "RESOLVED",
                        "session_date": session_date.isoformat(),
                        "symbol": trade.symbol,
                        "at": now.isoformat(),
                    },
                )
        await self.store.set_runtime_status(
            finalize_key,
            {
                "status": "COMPLETED",
                "evaluated": evaluated,
                "at": now.isoformat(),
            },
        )
        return evaluated

    async def _backfill_once(
        self,
        trade: ShadowTrade,
        *,
        session_date: date,
        end_slot: datetime,
        now: datetime,
    ) -> None:
        key = f"shadow_finalize_backfill:{session_date.isoformat()}:{trade.symbol}"
        if await self.store.get_runtime_status_key(key) is not None:
            return
        await self.store.set_runtime_status(
            key,
            {"status": "ATTEMPTED", "at": now.isoformat()},
        )
        if self.backfill is None:
            await self.store.set_runtime_status(
                f"shadow_finalize_pending:{trade.plan_id}",
                {
                    "status": "PENDING",
                    "reason": "BACKFILL_NOT_CONFIGURED",
                    "session_date": session_date.isoformat(),
                    "symbol": trade.symbol,
                    "at": now.isoformat(),
                },
            )
            return
        try:
            await self.backfill([trade.symbol], now=end_slot.astimezone(UTC))
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            await self.store.set_runtime_status(
                key,
                {
                    "status": "FAILED",
                    "error": type(exc).__name__,
                    "at": now.isoformat(),
                },
            )
            await self.store.set_runtime_status(
                f"shadow_finalize_pending:{trade.plan_id}",
                {
                    "status": "PENDING",
                    "reason": "BACKFILL_FAILED",
                    "session_date": session_date.isoformat(),
                    "symbol": trade.symbol,
                    "at": now.isoformat(),
                },
            )

    async def _stored_bars(
        self,
        trade: ShadowTrade,
        session_date: date,
    ) -> list:
        return [
            row
            for row in await self.store.list_intraday_bars(
                trade.symbol,
                session_date.isoformat(),
            )
            if row.source.upper() == "SIP"
            and row.minute_ts + timedelta(seconds=59) >= trade.opened_at
        ]

    async def _mark_session_pending(
        self,
        session_date: date,
        *,
        reason: str,
        now: datetime,
    ) -> None:
        for trade in await self.store.list_shadow_trades():
            if (
                trade.status in _ACTIVE
                and trade.opened_at.astimezone(EASTERN).date() == session_date
            ):
                await self.store.set_runtime_status(
                    f"shadow_finalize_pending:{trade.plan_id}",
                    {
                        "status": "PENDING",
                        "reason": reason,
                        "session_date": session_date.isoformat(),
                        "symbol": trade.symbol,
                        "at": now.isoformat(),
                    },
                )

    def _end_slot(self, closes_at: datetime) -> datetime:
        lag_minutes = (
            self.cfg.keyless_confirmed_lag_minutes
            if self.cfg.data_plan == "keyless_delayed"
            else self.cfg.historical_lag_minutes
        )
        delay = 5 * ceil(lag_minutes / 5)
        return closes_at + timedelta(minutes=delay)

    async def evaluate_session(self, session_date: date, *, finalize: bool = False) -> int:
        evaluated = 0
        for trade in await self.store.list_shadow_trades():
            if trade.status not in _ACTIVE:
                continue
            if trade.opened_at.astimezone(EASTERN).date() != session_date:
                continue
            all_bars = await self._stored_bars(trade, session_date)
            bars = [
                row
                for row in all_bars
                if trade.last_bar_at is None or row.minute_ts > trade.last_bar_at
            ]
            if not bars and not (finalize and all_bars):
                continue
            state = _state_from_trade(trade)
            for bar in bars:
                ticks = tuple(
                    tick
                    for tick in synthesize_bar_ticks(bar.as_market_bar())
                    if tick.at >= trade.opened_at
                )
                transitions = advance_virtual_trade_ticks(
                    state,
                    ticks,
                    conflict_at=bar.minute_ts,
                )
                trade.last_bar_at = bar.minute_ts
                for transition in transitions:
                    previous = str(trade.status)
                    _apply_transition(trade, transition)
                    await self._persist(
                        trade,
                        "SHADOW_TRADE_TRANSITIONED",
                        {"from": previous, "to": str(trade.status)},
                    )
                if trade.status in _TERMINAL:
                    break
            if finalize and trade.status in _ACTIVE:
                last = all_bars[-1]
                transition = finalize_virtual_trade(
                    state,
                    last.close,
                    last.minute_ts + timedelta(seconds=59),
                )
                if transition is not None:
                    previous = str(trade.status)
                    _apply_transition(trade, transition)
                    await self._persist(
                        trade,
                        "SHADOW_TRADE_TRANSITIONED",
                        {"from": previous, "to": str(trade.status)},
                    )
            if trade.status in _ACTIVE:
                await self._persist(trade, "SHADOW_TRADE_EVALUATED", {})
            evaluated += 1
        return evaluated

    async def _persist(self, trade: ShadowTrade, event_type: str, extra: dict) -> None:
        occurred_at = trade.closed_at or trade.last_bar_at or trade.opened_at
        async with self.store.transaction():
            await self.store.save_shadow_trade(trade)
            await self.store.append(
                LedgerEvent(
                    event_type,
                    trade.trade_id,
                    {**extra, "shadow_trade": asdict(trade)},
                    occurred_at=occurred_at,
                )
            )

    async def run(self) -> None:
        self._stop.clear()
        while not self._stop.is_set():
            try:
                await self.run_pending()
            except (OSError, RuntimeError, TypeError, ValueError) as exc:
                LOGGER.error("shadow_evaluator_error type=%s", type(exc).__name__)
            await self.sleep(5.0)

    async def stop(self) -> None:
        self._stop.set()


def shadow_metrics(
    trades: list[ShadowTrade],
    events: list[LedgerEvent],
    *,
    session_date: date | None = None,
) -> dict:
    def in_scope(trade: ShadowTrade) -> bool:
        return session_date is None or trade.opened_at.astimezone(EASTERN).date() == session_date

    scoped = sorted((row for row in trades if in_scope(row)), key=lambda row: row.opened_at)
    closed = [row for row in scoped if row.status in _TERMINAL]
    event_scope = [
        event
        for event in events
        if session_date is None or _aware(event.occurred_at).astimezone(EASTERN).date() == session_date
    ]
    buy_ids = {event.aggregate_id for event in event_scope if event.event_type == "BUY_NOW_EMITTED"}
    all_buy_ids = {event.aggregate_id for event in events if event.event_type == "BUY_NOW_EMITTED"}
    expired_ids = {event.aggregate_id for event in event_scope if event.event_type == "PLAN_EXPIRED"}
    summary = replay_summary([{"r": row.realized_r} for row in closed])
    setups: dict[str, dict] = {}
    for setup in sorted({row.setup for row in scoped}):
        setup_rows = [row for row in scoped if row.setup == setup]
        setup_closed = [row for row in setup_rows if row.status in _TERMINAL]
        setups[setup] = {
            "trades": len(setup_rows),
            **replay_summary([{"r": row.realized_r} for row in setup_closed]),
        }
    return {
        "signals": len(buy_ids),
        "trades": len(scoped),
        "unfinalized": len(scoped) - len(closed),
        "no_trigger": len(expired_ids - all_buy_ids),
        **summary,
        "by_setup": setups,
    }


def _state_from_trade(trade: ShadowTrade) -> VirtualTradeState:
    return VirtualTradeState(
        fill=trade.fill,
        stop=trade.stop,
        tp1=trade.tp1,
        tp2=trade.tp2,
        opened_at=trade.opened_at,
        time_stop_at=trade.time_stop_at,
        remaining_fraction=trade.remaining_fraction,
        tp1_taken=trade.tp1_taken,
        status=str(trade.status),
        realized_r=trade.realized_r,
        exit_legs=[dict(row) for row in trade.exit_legs],
    )


def _apply_transition(trade: ShadowTrade, transition) -> None:
    trade.status = ShadowTradeStatus(transition.status)
    trade.remaining_fraction = transition.remaining_fraction
    trade.tp1_taken = transition.tp1_taken
    trade.realized_r = transition.realized_r
    trade.exit_legs = [dict(row) for row in transition.exit_legs]
    if trade.status in _TERMINAL:
        trade.closed_at = datetime.fromisoformat(trade.exit_legs[-1]["at"])


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

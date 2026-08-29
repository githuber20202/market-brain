from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta

import nats
from nats.errors import Error as NatsError

from market_brain.domain.models import AlertRecord, PlanStatus, PositionAction
from market_brain.ledger.events import LedgerEvent
from market_brain.settings import Settings, settings

LOGGER = logging.getLogger(__name__)
_ACTIONABLE = {
    "SELL_NOW",
    "PLACE_STOP_NOW",
    "RECONCILE_REQUIRED",
    "TAKE_PROFIT",
    "TRIM",
}
_REPEATABLE = {"SELL_NOW", "PLACE_STOP_NOW"}


class PositionMonitor:
    def __init__(
        self,
        service,
        *,
        cfg: Settings = settings,
        nats_connect=nats.connect,
        clock=lambda: datetime.now(UTC),
        sleep=asyncio.sleep,
    ) -> None:
        self.service = service
        self.store = service.store
        self.cfg = cfg
        self.nats_connect = nats_connect
        self.clock = clock
        self.sleep = sleep
        self._stop = asyncio.Event()
        self._cache_wakeup = asyncio.Event()
        self._cache_loaded = False
        self._cache_invalidated = True
        self._positions_by_symbol: dict[str, tuple] = {}
        self._plans_by_symbol: dict[str, tuple] = {}
        self._plans_by_id: dict[str, object] = {}
        self._structure_symbols: set[str] = set()
        self._vwap: dict[str, float] = {}
        self._last_price: dict[str, float] = {}
        self._last_eval_at: dict[str, datetime] = {}
        self._last_action: dict[str, str] = {}
        self._last_alert_at: dict[tuple[str, str], datetime] = {}
        self.service.register_state_change_hook(self.invalidate_cache)

    def invalidate_cache(self, symbol: str | None = None) -> None:
        if symbol is None:
            self._positions_by_symbol.clear()
            self._plans_by_symbol.clear()
            self._plans_by_id.clear()
            self._structure_symbols.clear()
        else:
            key = symbol.upper()
            self._positions_by_symbol.pop(key, None)
            self._plans_by_symbol.pop(key, None)
            stale_ids = [
                plan_id
                for plan_id, plan in self._plans_by_id.items()
                if getattr(plan, "symbol", "").upper() == key
            ]
            for plan_id in stale_ids:
                self._plans_by_id.pop(plan_id, None)
        self._cache_invalidated = True
        self._cache_wakeup.set()

    async def refresh_cache(self) -> None:
        positions = await self.store.list_positions()
        plans = await self.store.list_plans()
        by_position_symbol: dict[str, list] = {}
        for position in positions:
            if position.closed_at is None and position.remaining_quantity > 0:
                by_position_symbol.setdefault(position.symbol.upper(), []).append(position)
        by_plan_symbol: dict[str, list] = {}
        for plan in plans:
            if plan.status == PlanStatus.ACTIVE and plan.triggered_at is None:
                by_plan_symbol.setdefault(plan.symbol.upper(), []).append(plan)
        self._positions_by_symbol = {
            symbol: tuple(rows) for symbol, rows in by_position_symbol.items()
        }
        self._plans_by_symbol = {
            symbol: tuple(rows) for symbol, rows in by_plan_symbol.items()
        }
        self._plans_by_id = {plan.plan_id: plan for plan in plans}
        self._structure_symbols = {
            plan.symbol.upper()
            for plan in plans
            if plan.status in {PlanStatus.ACTIVE, PlanStatus.RESERVED}
        }
        self._cache_loaded = True
        self._cache_invalidated = False

    async def bootstrap_intraday_structures(self) -> None:
        if not self._structure_symbols:
            return
        try:
            await self.service.backfill_intraday_structures(
                sorted(self._structure_symbols), now=self.clock()
            )
        except (RuntimeError, ValueError, TypeError) as exc:
            LOGGER.warning("intraday_structure_bootstrap_error type=%s", type(exc).__name__)

    async def _backfill_loop(self) -> None:
        while not self._stop.is_set():
            await self.sleep(self.cfg.intraday_backfill_interval_seconds)
            if self._stop.is_set() or not self._structure_symbols:
                continue
            try:
                await self.service.backfill_intraday_structures(
                    sorted(self._structure_symbols), now=self.clock()
                )
            except (RuntimeError, ValueError, TypeError) as exc:
                LOGGER.warning("intraday_structure_backfill_error type=%s", type(exc).__name__)

    async def _cache_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self.refresh_cache()
            except (OSError, RuntimeError, ValueError) as exc:
                LOGGER.warning("position_monitor_cache_error type=%s", type(exc).__name__)
            self._cache_wakeup.clear()
            try:
                await asyncio.wait_for(
                    self._cache_wakeup.wait(),
                    timeout=self.cfg.monitor_cache_refresh_seconds,
                )
            except TimeoutError:
                pass

    @staticmethod
    def _symbol_from_subject(subject: str) -> str | None:
        parts = subject.split(".")
        if len(parts) < 3:
            return None
        return parts[-1].upper()

    @staticmethod
    def _price(payload: dict) -> float | None:
        if payload.get("p") is not None:
            return float(payload["p"])
        bid = payload.get("bp")
        ask = payload.get("ap")
        if bid is not None and ask is not None and float(bid) > 0 and float(ask) > 0:
            return (float(bid) + float(ask)) / 2.0
        if payload.get("c") is not None:
            return float(payload["c"])
        return None

    def _fresh_quote(self, payload: dict, now: datetime) -> bool:
        raw = payload.get("t")
        if not raw:
            return False
        try:
            stamp = datetime.fromisoformat(str(raw))
        except ValueError:
            return False
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=UTC)
        age = (now - stamp).total_seconds()
        return 0 <= age <= self.cfg.max_quote_age_seconds

    async def _queue_alert(self, kind: str, payload: dict) -> AlertRecord:
        alert = AlertRecord(kind=kind, payload=payload)
        async with self.store.transaction():
            await self.store.save_alert(alert)
        return alert

    def _failed_breakout(self, plan, last: float) -> bool:
        if plan is None or plan.stop is None:
            return False
        r_value = plan.entry_trigger - plan.stop
        if r_value <= 0:
            return False
        threshold = plan.entry_trigger - self.cfg.failed_breakout_buffer_r * r_value
        return last < threshold

    async def _handle_position(self, symbol: str, last: float, now: datetime) -> None:
        positions = self._positions_by_symbol.get(symbol, ())
        for position in positions:
            previous_eval = self._last_eval_at.get(position.position_id)
            if previous_eval is not None and (
                now - previous_eval
            ).total_seconds() < self.cfg.monitor_min_interval_seconds:
                continue
            self._last_eval_at[position.position_id] = now
            plan = self._plans_by_id.get(position.plan_id)
            vwap = self._vwap.get(symbol)
            below_vwap = vwap is not None and last < vwap
            failed_breakout = (
                position.opened_at <= now and self._failed_breakout(plan, last)
            )
            action = await self.service.evaluate_position(
                position.position_id,
                last=last,
                below_vwap=below_vwap,
                failed_breakout=failed_breakout,
                record_event=False,
                queue_alert=False,
            )
            action_name = str(action)
            previous_action = self._last_action.get(position.position_id)
            changed = previous_action != action_name
            if changed:
                await self.store.append(
                    LedgerEvent(
                        "POSITION_EVALUATED",
                        position.position_id,
                        {
                            "last": last,
                            "action": action_name,
                            "below_vwap": below_vwap,
                            "failed_breakout": failed_breakout,
                            "source": "POSITION_MONITOR",
                        },
                    )
                )
                self._last_action[position.position_id] = action_name

            if action_name == str(PositionAction.HOLD) or action_name not in _ACTIONABLE:
                continue
            alert_key = (position.position_id, action_name)
            last_alert = self._last_alert_at.get(alert_key)
            repeat_due = (
                action_name in _REPEATABLE
                and last_alert is not None
                and now - last_alert >= timedelta(minutes=self.cfg.alert_repeat_minutes)
            )
            if not changed and not repeat_due:
                continue
            await self._queue_alert(
                action_name,
                {
                    "action": action_name,
                    "position_id": position.position_id,
                    "symbol": position.symbol,
                    "last": last,
                },
            )
            self._last_alert_at[alert_key] = now

    async def _handle_trigger(
        self,
        symbol: str,
        last: float,
        payload: dict,
        now: datetime,
    ) -> None:
        plans = self._plans_by_symbol.get(symbol, ())
        if not plans or not self._fresh_quote(payload, now):
            return
        previous = self._last_price.get(symbol)
        self._last_price[symbol] = last
        if previous is None:
            return
        for plan in plans:
            if (
                plan.status != PlanStatus.ACTIVE
                or plan.triggered_at is not None
                or now > plan.expires_at
                or not (previous < plan.entry_trigger <= last)
            ):
                continue
            await self.service.record_trigger_hit(
                plan.plan_id,
                last=last,
                triggered_at=now,
                source="POSITION_MONITOR",
            )
            self._plans_by_symbol[symbol] = tuple(
                row for row in self._plans_by_symbol.get(symbol, ())
                if row.plan_id != plan.plan_id
            )

    async def handle_message(
        self,
        subject: str,
        payload: dict,
        *,
        now: datetime | None = None,
    ) -> None:
        timestamp = now or self.clock()
        symbol = self._symbol_from_subject(subject)
        if symbol is None:
            return
        if ".bar_closed_1m." in subject:
            raw_vwap = payload.get("vw")
            if raw_vwap is not None and float(raw_vwap) > 0:
                self._vwap[symbol] = float(raw_vwap)
            complete_structure_bar = (
                (payload.get("t") is not None or payload.get("timestamp") is not None)
                and payload.get("h", payload.get("high")) is not None
                and payload.get("l", payload.get("low")) is not None
                and payload.get("c", payload.get("close")) is not None
            )
            if self._cache_loaded and symbol in self._structure_symbols and complete_structure_bar:
                try:
                    await self.service.record_intraday_bar(
                        symbol, payload, now=timestamp, source="IEX"
                    )
                except (TypeError, ValueError) as exc:
                    LOGGER.warning("intraday_structure_bar_error type=%s", type(exc).__name__)
            return
        if not self._cache_loaded:
            return
        is_trade = ".market_trade." in subject
        has_position = bool(self._positions_by_symbol.get(symbol))
        has_plan = is_trade and bool(self._plans_by_symbol.get(symbol))
        if not has_position and not has_plan:
            return
        last = self._price(payload)
        if last is None:
            return
        if has_position:
            await self._handle_position(symbol, last, timestamp)
        if has_plan:
            await self._handle_trigger(symbol, last, payload, timestamp)

    async def run(self) -> None:
        if not self.cfg.nats_url:
            return
        await self.refresh_cache()
        await self.bootstrap_intraday_structures()
        cache_task = asyncio.create_task(self._cache_loop())
        backfill_task = asyncio.create_task(self._backfill_loop())
        try:
            while not self._stop.is_set():
                connection = None
                try:
                    connection = await self.nats_connect(self.cfg.nats_url)

                    async def callback(message):
                        try:
                            payload = json.loads(message.data.decode("utf-8"))
                            if isinstance(payload, dict):
                                await self.handle_message(message.subject, payload)
                        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
                            LOGGER.warning(
                                "position_monitor_message_error type=%s",
                                type(exc).__name__,
                            )

                    for subject in (
                        "market.market_trade.*",
                        "market.market_quote.*",
                        "market.bar_closed_1m.*",
                    ):
                        await connection.subscribe(subject, cb=callback)
                    await self._stop.wait()
                except asyncio.CancelledError:
                    raise
                except (NatsError, OSError, RuntimeError) as exc:
                    LOGGER.warning("position_monitor_nats_error type=%s", type(exc).__name__)
                    if not self._stop.is_set():
                        await self.sleep(1.0)
                finally:
                    if connection is not None:
                        try:
                            await connection.drain()
                        except (NatsError, OSError, RuntimeError):
                            pass
        finally:
            cache_task.cancel()
            backfill_task.cancel()
            for task in (cache_task, backfill_task):
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def stop(self) -> None:
        self._stop.set()

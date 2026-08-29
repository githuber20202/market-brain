from __future__ import annotations

import json
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import asdict
from datetime import date, datetime
from typing import Protocol

try:
    import asyncpg
except ModuleNotFoundError:  # optional until Postgres store is selected
    asyncpg = None  # type: ignore[assignment]

from market_brain.domain.models import (
    AlertRecord,
    IntradayBarRecord,
    LiquidityProfile,
    PositionState,
    Reservation,
    ShadowTrade,
    TradePlan,
    WalletState,
    utc_now,
)
from market_brain.ledger.events import LedgerEvent


def _json_obj(value):
    if isinstance(value, str):
        return json.loads(value)
    return dict(value)


class EventStore(Protocol):
    def transaction(self): ...
    async def read_events(self, after_event_id: str | None = None) -> list[LedgerEvent]: ...
    async def set_runtime_status(self, key: str, value) -> None: ...
    async def get_runtime_status_key(self, key: str): ...
    async def get_runtime_status(self) -> dict: ...
    async def append(self, event: LedgerEvent) -> None: ...
    async def save_plan(self, plan: TradePlan) -> None: ...
    async def get_plan(self, plan_id: str) -> TradePlan | None: ...
    async def list_plans(self) -> list[TradePlan]: ...
    async def save_wallet(self, wallet: WalletState) -> None: ...
    async def get_wallet(self) -> WalletState | None: ...
    async def save_reservation(self, reservation: Reservation) -> None: ...
    async def get_reservation(self, plan_id: str) -> Reservation | None: ...
    async def list_reservations(self) -> list[Reservation]: ...
    async def delete_reservation(self, plan_id: str) -> None: ...
    async def save_position(self, position: PositionState) -> None: ...
    async def get_position(self, position_id: str) -> PositionState | None: ...
    async def list_positions(self) -> list[PositionState]: ...
    async def replace_positions(self, positions: list[PositionState]) -> None: ...
    async def save_liquidity_profile(self, profile: LiquidityProfile) -> None: ...
    async def get_liquidity_profile(self, symbol: str) -> LiquidityProfile | None: ...
    async def list_liquidity_profiles(self) -> list[LiquidityProfile]: ...
    async def save_intraday_bar(self, bar: IntradayBarRecord) -> None: ...
    async def list_intraday_bars(self, symbol: str, session_date: str) -> list[IntradayBarRecord]: ...
    async def save_shadow_trade(self, trade: ShadowTrade) -> None: ...
    async def get_shadow_trade(self, plan_id: str) -> ShadowTrade | None: ...
    async def list_shadow_trades(self) -> list[ShadowTrade]: ...
    async def save_alert(self, alert: AlertRecord) -> None: ...
    async def get_alert(self, alert_id: str) -> AlertRecord | None: ...
    async def list_alerts(self) -> list[AlertRecord]: ...
    async def list_undelivered(self) -> list[AlertRecord]: ...
    async def prune_intraday_bars(self, keep_sessions: int) -> int: ...
    async def mark_delivered(self, alert_id: str) -> AlertRecord | None: ...
    async def mark_failed(
        self, alert_id: str, error: str, next_attempt_at: datetime | None
    ) -> AlertRecord | None: ...


class InMemoryEventStore:
    def __init__(self) -> None:
        self.events: list[LedgerEvent] = []
        self.plans: dict[str, TradePlan] = {}
        self.wallet: WalletState | None = None
        self.reservations: dict[str, Reservation] = {}
        self.positions: dict[str, PositionState] = {}
        self.liquidity_profiles: dict[str, LiquidityProfile] = {}
        self.intraday_bars: dict[tuple[str, str, datetime, str], IntradayBarRecord] = {}
        self.shadow_trades: dict[str, ShadowTrade] = {}
        self.alerts: dict[str, AlertRecord] = {}
        self.runtime_status: dict[str, object] = {}

    @asynccontextmanager
    async def transaction(self):
        yield self

    async def read_events(self, after_event_id: str | None = None) -> list[LedgerEvent]:
        if after_event_id is None:
            return list(self.events)
        for index, event in enumerate(self.events):
            if event.event_id == after_event_id:
                return list(self.events[index + 1 :])
        raise ValueError("EVENT_ID_NOT_FOUND")

    async def append(self, event: LedgerEvent) -> None:
        self.events.append(event)

    async def save_plan(self, plan: TradePlan) -> None:
        self.plans[plan.plan_id] = plan

    async def get_plan(self, plan_id: str) -> TradePlan | None:
        return self.plans.get(plan_id)

    async def list_plans(self) -> list[TradePlan]:
        return list(self.plans.values())

    async def save_wallet(self, wallet: WalletState) -> None:
        self.wallet = wallet

    async def get_wallet(self) -> WalletState | None:
        return self.wallet

    async def save_reservation(self, reservation: Reservation) -> None:
        self.reservations[reservation.plan_id] = reservation

    async def get_reservation(self, plan_id: str) -> Reservation | None:
        return self.reservations.get(plan_id)

    async def list_reservations(self) -> list[Reservation]:
        return list(self.reservations.values())

    async def delete_reservation(self, plan_id: str) -> None:
        self.reservations.pop(plan_id, None)

    async def save_position(self, position: PositionState) -> None:
        self.positions[position.position_id] = position

    async def get_position(self, position_id: str) -> PositionState | None:
        return self.positions.get(position_id)

    async def list_positions(self) -> list[PositionState]:
        return list(self.positions.values())

    async def replace_positions(self, positions: list[PositionState]) -> None:
        self.positions = {row.position_id: row for row in positions}

    async def save_liquidity_profile(self, profile: LiquidityProfile) -> None:
        self.liquidity_profiles[profile.symbol.upper()] = profile

    async def get_liquidity_profile(self, symbol: str) -> LiquidityProfile | None:
        return self.liquidity_profiles.get(symbol.upper())

    async def list_liquidity_profiles(self) -> list[LiquidityProfile]:
        return list(self.liquidity_profiles.values())

    async def save_intraday_bar(self, bar: IntradayBarRecord) -> None:
        key = (bar.symbol.upper(), bar.session_date, bar.minute_ts, bar.source.upper())
        self.intraday_bars[key] = bar

    async def list_intraday_bars(self, symbol: str, session_date: str) -> list[IntradayBarRecord]:
        rows = [
            bar
            for (stored_symbol, stored_date, _stamp, _source), bar in self.intraday_bars.items()
            if stored_symbol == symbol.upper() and stored_date == session_date
        ]
        return sorted(rows, key=lambda bar: (bar.minute_ts, bar.source))

    async def save_shadow_trade(self, trade: ShadowTrade) -> None:
        self.shadow_trades[trade.plan_id] = trade

    async def get_shadow_trade(self, plan_id: str) -> ShadowTrade | None:
        return self.shadow_trades.get(plan_id)

    async def list_shadow_trades(self) -> list[ShadowTrade]:
        return sorted(self.shadow_trades.values(), key=lambda row: (row.opened_at, row.trade_id))

    async def save_alert(self, alert: AlertRecord) -> None:
        self.alerts[alert.alert_id] = alert

    async def get_alert(self, alert_id: str) -> AlertRecord | None:
        return self.alerts.get(alert_id)

    async def list_alerts(self) -> list[AlertRecord]:
        return sorted(self.alerts.values(), key=lambda alert: alert.created_at)

    async def list_undelivered(self) -> list[AlertRecord]:
        return sorted(
            (alert for alert in self.alerts.values() if alert.delivered_at is None),
            key=lambda alert: alert.created_at,
        )

    async def prune_intraday_bars(self, keep_sessions: int) -> int:
        if keep_sessions <= 0:
            raise ValueError("KEEP_SESSIONS_INVALID")
        retained = sorted({key[1] for key in self.intraday_bars}, reverse=True)[:keep_sessions]
        retained_set = set(retained)
        before = len(self.intraday_bars)
        self.intraday_bars = {
            key: value
            for key, value in self.intraday_bars.items()
            if key[1] in retained_set
        }
        return before - len(self.intraday_bars)

    async def mark_delivered(self, alert_id: str) -> AlertRecord | None:
        alert = self.alerts.get(alert_id)
        if alert is None:
            return None
        alert.attempts += 1
        alert.delivered_at = utc_now()
        alert.last_error = None
        alert.next_attempt_at = None
        return alert

    async def mark_failed(
        self, alert_id: str, error: str, next_attempt_at: datetime | None
    ) -> AlertRecord | None:
        alert = self.alerts.get(alert_id)
        if alert is None:
            return None
        alert.attempts += 1
        alert.last_error = error
        alert.next_attempt_at = next_attempt_at
        return alert

    async def set_runtime_status(self, key: str, value) -> None:
        self.runtime_status[key] = value

    async def get_runtime_status_key(self, key: str):
        return self.runtime_status.get(key)

    async def get_runtime_status(self) -> dict:
        return dict(self.runtime_status)


class PostgresEventStore:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = None
        self._tx_connection: ContextVar = ContextVar(
            f"market_brain_pg_tx_{id(self)}", default=None
        )

    async def connect(self) -> None:
        if asyncpg is None:
            raise RuntimeError("ASYNCPG_NOT_INSTALLED")
        if self.pool is None:
            self.pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=5)

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()
            self.pool = None

    @asynccontextmanager
    async def _connection(self):
        current = self._tx_connection.get()
        if current is not None:
            yield current
            return
        await self.connect()
        assert self.pool is not None
        async with self.pool.acquire() as connection:
            yield connection

    @asynccontextmanager
    async def transaction(self):
        current = self._tx_connection.get()
        if current is not None:
            yield self
            return
        await self.connect()
        assert self.pool is not None
        async with self.pool.acquire() as connection:
            token = self._tx_connection.set(connection)
            try:
                async with connection.transaction():
                    yield self
            finally:
                self._tx_connection.reset(token)

    async def _execute(self, query: str, *args):
        async with self._connection() as connection:
            return await connection.execute(query, *args)

    async def _fetchrow(self, query: str, *args):
        async with self._connection() as connection:
            return await connection.fetchrow(query, *args)

    async def _fetch(self, query: str, *args):
        async with self._connection() as connection:
            return await connection.fetch(query, *args)

    async def read_events(self, after_event_id: str | None = None) -> list[LedgerEvent]:
        rows = await self._fetch(
            "SELECT event_id,event_type,aggregate_id,occurred_at,payload "
            "FROM decision_events ORDER BY occurred_at,event_id"
        )
        events = [
            LedgerEvent(
                event_id=str(row["event_id"]),
                event_type=str(row["event_type"]),
                aggregate_id=str(row["aggregate_id"]),
                occurred_at=row["occurred_at"],
                payload=_json_obj(row["payload"]),
            )
            for row in rows
        ]
        if after_event_id is None:
            return events
        for index, event in enumerate(events):
            if event.event_id == after_event_id:
                return events[index + 1 :]
        raise ValueError("EVENT_ID_NOT_FOUND")

    async def append(self, event: LedgerEvent) -> None:
        await self._execute(
            """INSERT INTO decision_events(event_id,event_type,aggregate_id,occurred_at,payload)
               VALUES($1,$2,$3,$4,$5::jsonb) ON CONFLICT (event_id) DO NOTHING""",
            event.event_id, event.event_type, event.aggregate_id, event.occurred_at,
            json.dumps(event.payload, default=str),
        )

    async def save_plan(self, plan: TradePlan) -> None:
        await self._execute(
            """INSERT INTO trade_plans(plan_id,symbol,status,plan_json,triggered_at) VALUES($1,$2,$3,$4::jsonb,$5)
               ON CONFLICT(plan_id) DO UPDATE SET status=EXCLUDED.status,plan_json=EXCLUDED.plan_json,
                 triggered_at=EXCLUDED.triggered_at,updated_at=now()""",
            plan.plan_id, plan.symbol, str(plan.status), json.dumps(asdict(plan), default=str),
            plan.triggered_at,
        )

    async def get_plan(self, plan_id: str) -> TradePlan | None:
        row = await self._fetchrow("SELECT plan_json FROM trade_plans WHERE plan_id=$1", plan_id)
        return _plan_from_json(_json_obj(row["plan_json"])) if row is not None else None

    async def list_plans(self) -> list[TradePlan]:
        rows = await self._fetch("SELECT plan_json FROM trade_plans")
        return [_plan_from_json(_json_obj(row["plan_json"])) for row in rows]

    async def save_wallet(self, wallet: WalletState) -> None:
        await self._execute(
            """INSERT INTO risk_wallet(singleton_id,wallet_json) VALUES(1,$1::jsonb)
               ON CONFLICT(singleton_id) DO UPDATE SET wallet_json=EXCLUDED.wallet_json""",
            json.dumps(asdict(wallet), default=str),
        )

    async def get_wallet(self) -> WalletState | None:
        row = await self._fetchrow("SELECT wallet_json FROM risk_wallet WHERE singleton_id=1")
        return _wallet_from_json(_json_obj(row["wallet_json"])) if row is not None else None

    async def save_reservation(self, reservation: Reservation) -> None:
        await self._execute(
            """INSERT INTO reservations(plan_id,reservation_json) VALUES($1,$2::jsonb)
               ON CONFLICT(plan_id) DO UPDATE SET reservation_json=EXCLUDED.reservation_json""",
            reservation.plan_id, json.dumps(asdict(reservation), default=str),
        )

    async def get_reservation(self, plan_id: str) -> Reservation | None:
        row = await self._fetchrow("SELECT reservation_json FROM reservations WHERE plan_id=$1", plan_id)
        return _reservation_from_json(_json_obj(row["reservation_json"])) if row is not None else None

    async def list_reservations(self) -> list[Reservation]:
        rows = await self._fetch("SELECT reservation_json FROM reservations")
        return [_reservation_from_json(_json_obj(row["reservation_json"])) for row in rows]

    async def delete_reservation(self, plan_id: str) -> None:
        await self._execute("DELETE FROM reservations WHERE plan_id=$1", plan_id)

    async def save_position(self, position: PositionState) -> None:
        await self._execute(
            """INSERT INTO position_twin(position_id,symbol,position_json) VALUES($1,$2,$3::jsonb)
               ON CONFLICT(position_id) DO UPDATE SET position_json=EXCLUDED.position_json""",
            position.position_id, position.symbol, json.dumps(asdict(position), default=str),
        )

    async def get_position(self, position_id: str) -> PositionState | None:
        row = await self._fetchrow("SELECT position_json FROM position_twin WHERE position_id=$1", position_id)
        return _position_from_json(_json_obj(row["position_json"])) if row is not None else None

    async def list_positions(self) -> list[PositionState]:
        rows = await self._fetch("SELECT position_json FROM position_twin ORDER BY updated_at")
        return [_position_from_json(_json_obj(row["position_json"])) for row in rows]

    async def replace_positions(self, positions: list[PositionState]) -> None:
        async with self.transaction():
            await self._execute("DELETE FROM position_twin")
            for position in positions:
                await self.save_position(position)

    async def save_liquidity_profile(self, profile: LiquidityProfile) -> None:
        await self._execute(
            """INSERT INTO liquidity_profiles(symbol,adv20,close,as_of,refreshed_at,updated_at)
               VALUES($1,$2,$3,$4,$5,now())
               ON CONFLICT(symbol) DO UPDATE SET adv20=EXCLUDED.adv20,close=EXCLUDED.close,
                 as_of=EXCLUDED.as_of,refreshed_at=EXCLUDED.refreshed_at,updated_at=now()""",
            profile.symbol.upper(), profile.adv20, profile.close, profile.as_of, profile.refreshed_at,
        )

    async def get_liquidity_profile(self, symbol: str) -> LiquidityProfile | None:
        row = await self._fetchrow(
            "SELECT symbol,adv20,close,as_of,refreshed_at FROM liquidity_profiles WHERE symbol=$1",
            symbol.upper(),
        )
        if row is None:
            return None
        return LiquidityProfile(
            symbol=str(row["symbol"]),
            adv20=float(row["adv20"]),
            close=float(row["close"]),
            as_of=row["as_of"],
            refreshed_at=row["refreshed_at"],
        )

    async def list_liquidity_profiles(self) -> list[LiquidityProfile]:
        rows = await self._fetch(
            "SELECT symbol,adv20,close,as_of,refreshed_at FROM liquidity_profiles ORDER BY symbol"
        )
        return [
            LiquidityProfile(
                symbol=str(row["symbol"]),
                adv20=float(row["adv20"]),
                close=float(row["close"]),
                as_of=row["as_of"],
                refreshed_at=row["refreshed_at"],
            )
            for row in rows
        ]

    async def save_intraday_bar(self, bar: IntradayBarRecord) -> None:
        await self._execute(
            """INSERT INTO intraday_bars(
                 symbol,session_date,minute_ts,source,open,high,low,close,volume,vwap,updated_at
               ) VALUES($1,$2::date,$3,$4,$5,$6,$7,$8,$9,$10,now())
               ON CONFLICT(symbol,session_date,minute_ts,source) DO UPDATE SET
                 open=EXCLUDED.open,high=EXCLUDED.high,low=EXCLUDED.low,close=EXCLUDED.close,
                 volume=EXCLUDED.volume,vwap=EXCLUDED.vwap,updated_at=now()""",
            bar.symbol.upper(), date.fromisoformat(bar.session_date), bar.minute_ts, bar.source.upper(),
            bar.open, bar.high, bar.low, bar.close, bar.volume, bar.vwap,
        )

    async def list_intraday_bars(self, symbol: str, session_date: str) -> list[IntradayBarRecord]:
        rows = await self._fetch(
            """SELECT symbol,session_date,minute_ts,source,open,high,low,close,volume,vwap
               FROM intraday_bars WHERE symbol=$1 AND session_date=$2::date
               ORDER BY minute_ts,source""",
            symbol.upper(), date.fromisoformat(session_date),
        )
        return [
            IntradayBarRecord(
                symbol=str(row["symbol"]),
                session_date=str(row["session_date"]),
                minute_ts=row["minute_ts"],
                source=str(row["source"]),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row["volume"]) if row["volume"] is not None else None,
                vwap=float(row["vwap"]) if row["vwap"] is not None else None,
            )
            for row in rows
        ]

    async def save_shadow_trade(self, trade: ShadowTrade) -> None:
        await self._execute(
            """INSERT INTO shadow_trades(trade_id,plan_id,symbol,status,trade_json,opened_at,closed_at)
               VALUES($1,$2,$3,$4,$5::jsonb,$6,$7)
               ON CONFLICT(plan_id) DO UPDATE SET status=EXCLUDED.status,
                 trade_json=EXCLUDED.trade_json,closed_at=EXCLUDED.closed_at,updated_at=now()""",
            trade.trade_id, trade.plan_id, trade.symbol, str(trade.status),
            json.dumps(asdict(trade), default=str), trade.opened_at, trade.closed_at,
        )

    async def get_shadow_trade(self, plan_id: str) -> ShadowTrade | None:
        row = await self._fetchrow("SELECT trade_json FROM shadow_trades WHERE plan_id=$1", plan_id)
        return _shadow_trade_from_json(_json_obj(row["trade_json"])) if row is not None else None

    async def list_shadow_trades(self) -> list[ShadowTrade]:
        rows = await self._fetch("SELECT trade_json FROM shadow_trades ORDER BY opened_at,trade_id")
        return [_shadow_trade_from_json(_json_obj(row["trade_json"])) for row in rows]

    async def save_alert(self, alert: AlertRecord) -> None:
        await self._execute(
            """INSERT INTO alerts(alert_id,kind,payload_json,created_at,delivered_at,attempts,last_error,next_attempt_at)
               VALUES($1,$2,$3::jsonb,$4,$5,$6,$7,$8)
               ON CONFLICT(alert_id) DO UPDATE SET kind=EXCLUDED.kind,payload_json=EXCLUDED.payload_json,
                 created_at=EXCLUDED.created_at,delivered_at=EXCLUDED.delivered_at,attempts=EXCLUDED.attempts,
                 last_error=EXCLUDED.last_error,next_attempt_at=EXCLUDED.next_attempt_at""",
            alert.alert_id, alert.kind, json.dumps(alert.payload, default=str), alert.created_at,
            alert.delivered_at, alert.attempts, alert.last_error, alert.next_attempt_at,
        )

    async def get_alert(self, alert_id: str) -> AlertRecord | None:
        row = await self._fetchrow("SELECT * FROM alerts WHERE alert_id=$1", alert_id)
        return _alert_from_row(row) if row is not None else None

    async def list_alerts(self) -> list[AlertRecord]:
        rows = await self._fetch("SELECT * FROM alerts ORDER BY created_at")
        return [_alert_from_row(row) for row in rows]

    async def list_undelivered(self) -> list[AlertRecord]:
        rows = await self._fetch("SELECT * FROM alerts WHERE delivered_at IS NULL ORDER BY created_at")
        return [_alert_from_row(row) for row in rows]

    async def prune_intraday_bars(self, keep_sessions: int) -> int:
        if keep_sessions <= 0:
            raise ValueError("KEEP_SESSIONS_INVALID")
        result = await self._execute(
            """DELETE FROM intraday_bars
               WHERE session_date NOT IN (
                 SELECT session_date FROM intraday_bars
                 GROUP BY session_date ORDER BY session_date DESC LIMIT $1
               )""",
            keep_sessions,
        )
        return int(str(result).rsplit(" ", 1)[-1])

    async def mark_delivered(self, alert_id: str) -> AlertRecord | None:
        row = await self._fetchrow(
            """UPDATE alerts SET delivered_at=COALESCE(delivered_at, now()),attempts=attempts+1,
               last_error=NULL,next_attempt_at=NULL WHERE alert_id=$1 RETURNING *""", alert_id
        )
        return _alert_from_row(row) if row is not None else None

    async def mark_failed(self, alert_id: str, error: str, next_attempt_at: datetime | None) -> AlertRecord | None:
        row = await self._fetchrow(
            """UPDATE alerts SET attempts=attempts+1,last_error=$2,next_attempt_at=$3
               WHERE alert_id=$1 RETURNING *""", alert_id, error, next_attempt_at
        )
        return _alert_from_row(row) if row is not None else None


    async def set_runtime_status(self, key: str, value) -> None:
        await self._execute(
            """INSERT INTO runtime_status(key,value_json,updated_at) VALUES($1,$2::jsonb,now())
               ON CONFLICT(key) DO UPDATE SET value_json=EXCLUDED.value_json, updated_at=now()""",
            key, json.dumps(value, default=str),
        )

    async def get_runtime_status_key(self, key: str):
        row = await self._fetchrow("SELECT value_json FROM runtime_status WHERE key=$1", key)
        if row is None:
            return None
        return _json_obj(row["value_json"])

    async def get_runtime_status(self) -> dict:
        rows = await self._fetch("SELECT key,value_json,updated_at FROM runtime_status")
        result = {}
        for row in rows:
            result[str(row["key"])] = _json_obj(row["value_json"])
        return result


def _dt(value):
    if value is None or isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def _plan_from_json(data: dict) -> TradePlan:
    from market_brain.domain.models import PlanStatus, StrategyLane

    return TradePlan(
        symbol=data["symbol"],
        lane=StrategyLane(data["lane"]),
        entry_trigger=float(data["entry_trigger"]),
        entry_zone_high=float(data["entry_zone_high"]),
        stop=float(data["stop"]) if data.get("stop") is not None else None,
        tp1=float(data["tp1"]) if data.get("tp1") is not None else None,
        tp2=float(data["tp2"]) if data.get("tp2") is not None else None,
        max_spread_pct=float(data["max_spread_pct"]),
        max_slippage_pct=float(data["max_slippage_pct"]),
        created_at=_dt(data["created_at"]),
        expires_at=_dt(data["expires_at"]),
        quality_risk_multiplier=float(data["quality_risk_multiplier"]),
        plan_id=data["plan_id"],
        status=PlanStatus(data.get("status", "ACTIVE")),
        reasons=list(data.get("reasons", [])),
        evidence_hash=str(data.get("evidence_hash", "")),
        source_ids=list(data.get("source_ids", [])),
        triggered_at=_dt(data.get("triggered_at")),
    )


def _wallet_from_json(data: dict) -> WalletState:
    return WalletState(
        capital_base=float(data["capital_base"]),
        cash_available=float(data["cash_available"]),
        daily_realized_loss=float(data.get("daily_realized_loss", 0.0)),
        open_risk=float(data.get("open_risk", 0.0)),
        reserved_cash=float(data.get("reserved_cash", 0.0)),
        version=int(data.get("version", 1)),
        as_of=_dt(data.get("as_of")),
    )


def _reservation_from_json(data: dict) -> Reservation:
    return Reservation(
        plan_id=data["plan_id"],
        quantity=int(data["quantity"]),
        reserved_cash=float(data["reserved_cash"]),
        reserved_risk=float(data["reserved_risk"]),
        expires_at=_dt(data["expires_at"]),
    )


def _position_from_json(data: dict) -> PositionState:
    from market_brain.domain.models import ProtectionState, ReconciliationState

    return PositionState(
        position_id=data["position_id"],
        plan_id=data["plan_id"],
        symbol=data["symbol"],
        quantity=int(data["quantity"]),
        remaining_quantity=int(data["remaining_quantity"]),
        average_fill=float(data["average_fill"]),
        stop=float(data["stop"]) if data.get("stop") is not None else None,
        tp1=float(data["tp1"]) if data.get("tp1") is not None else None,
        tp2=float(data["tp2"]) if data.get("tp2") is not None else None,
        opened_at=_dt(data["opened_at"]),
        time_stop_at=_dt(data.get("time_stop_at")),
        realized_pnl=float(data.get("realized_pnl", 0.0)),
        managed=bool(data.get("managed", True)),
        source=str(data.get("source", "FILL_ACK")),
        closed_at=_dt(data.get("closed_at")),
        protection=ProtectionState(data.get("protection", "UNPROTECTED")),
        broker_stop_price=(
            float(data["broker_stop_price"]) if data.get("broker_stop_price") is not None else None
        ),
        broker_order_ref=data.get("broker_order_ref"),
        protected_quantity=int(
            data.get(
                "protected_quantity",
                data["remaining_quantity"] if data.get("protection") == "PROTECTED" else 0,
            )
        ),
        reconciliation_state=ReconciliationState(
            data.get("reconciliation_state", "UNRECONCILED")
        ),
        last_reconciled_at=_dt(data.get("last_reconciled_at")),
    )


def _shadow_trade_from_json(data: dict) -> ShadowTrade:
    from market_brain.domain.models import ShadowTradeStatus

    return ShadowTrade(
        trade_id=str(data["trade_id"]),
        plan_id=str(data["plan_id"]),
        symbol=str(data["symbol"]),
        setup=str(data["setup"]),
        quantity=int(data["quantity"]),
        trigger=float(data["trigger"]),
        fill=float(data["fill"]),
        stop=float(data["stop"]),
        tp1=float(data["tp1"]),
        tp2=float(data["tp2"]),
        opened_at=_dt(data["opened_at"]),
        time_stop_at=_dt(data["time_stop_at"]),
        status=ShadowTradeStatus(data.get("status", "OPEN")),
        remaining_fraction=float(data.get("remaining_fraction", 1.0)),
        tp1_taken=bool(data.get("tp1_taken", False)),
        realized_r=float(data.get("realized_r", 0.0)),
        exit_legs=list(data.get("exit_legs", [])),
        last_bar_at=_dt(data.get("last_bar_at")),
        closed_at=_dt(data.get("closed_at")),
    )


def _alert_from_row(row) -> AlertRecord:
    payload = _json_obj(row["payload_json"])
    return AlertRecord(
        alert_id=str(row["alert_id"]),
        kind=str(row["kind"]),
        payload=dict(payload),
        created_at=row["created_at"],
        delivered_at=row["delivered_at"],
        attempts=int(row["attempts"]),
        last_error=row["last_error"],
        next_attempt_at=row["next_attempt_at"],
    )

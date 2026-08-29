from __future__ import annotations

import inspect
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from functools import wraps
from math import floor
from uuid import uuid4
from zoneinfo import ZoneInfo

from market_brain.domain.models import (
    ActivationDecision,
    AlertRecord,
    IntradayBarRecord,
    IntradayStructure,
    IntradayStructureState,
    LiquidityProfile,
    MarketSnapshot,
    PlanStatus,
    PositionState,
    ProtectionState,
    QualityProfile,
    ReconciliationState,
    Reservation,
    ShadowTrade,
    SignalState,
    StrategyLane,
    TradePlan,
    WalletState,
)
from market_brain.engines.activation import activate_plan
from market_brain.engines.features import compute_features
from market_brain.engines.intraday import (
    compute_structure,
    session_date_for,
    structure_from_dict,
    structure_key,
)
from market_brain.engines.liquidity import (
    _is_keyless_source,
    apply_iex_liquidity_gate,
    apply_keyless_liquidity_gate,
)
from market_brain.engines.plan import build_trade_plan
from market_brain.engines.position import evaluate_position
from market_brain.engines.ranking import score_features
from market_brain.engines.wallet import size_from_wallet
from market_brain.ledger.events import LedgerEvent
from market_brain.ledger.store import EventStore, InMemoryEventStore
from market_brain.providers.base import DataUnavailable, MarketDataProvider
from market_brain.replay.engine import SLIPPAGE_BPS, TIME_STOP_MINUTES
from market_brain.settings import Settings, settings


def transactional(method):
    @wraps(method)
    async def wrapped(self, *args, **kwargs):
        async with self.store.transaction():
            return await method(self, *args, **kwargs)

    return wrapped


class DecisionService:
    def __init__(
        self,
        store: EventStore | None = None,
        cfg: Settings = settings,
        market_data: MarketDataProvider | None = None,
    ):
        self.store = store or InMemoryEventStore()
        self.cfg = cfg
        self.market_data = market_data
        self._state_change_hooks = []

    def register_state_change_hook(self, hook) -> None:
        if hook not in self._state_change_hooks:
            self._state_change_hooks.append(hook)

    async def _notify_state_change(self, symbol: str | None) -> None:
        for hook in tuple(self._state_change_hooks):
            result = hook(symbol)
            if inspect.isawaitable(result):
                await result

    async def _open_shadow_trade(
        self,
        plan: TradePlan,
        decision: ActivationDecision,
        *,
        now: datetime | None = None,
    ) -> ShadowTrade | None:
        if self.cfg.run_mode != "shadow":
            return None
        existing = await self.store.get_shadow_trade(plan.plan_id)
        if existing is not None:
            return existing
        if plan.stop is None or plan.tp1 is None or plan.tp2 is None:
            raise RuntimeError("SHADOW_LEVELS_MISSING")
        opened_at = now or datetime.now(UTC)
        trade = ShadowTrade(
            trade_id=str(uuid4()),
            plan_id=plan.plan_id,
            symbol=plan.symbol,
            setup=str(plan.lane),
            quantity=decision.quantity,
            trigger=plan.entry_trigger,
            fill=round(plan.entry_trigger * (1.0 + SLIPPAGE_BPS / 10_000.0), 4),
            stop=plan.stop,
            tp1=plan.tp1,
            tp2=plan.tp2,
            opened_at=opened_at,
            time_stop_at=opened_at + timedelta(minutes=TIME_STOP_MINUTES),
        )
        await self.store.save_shadow_trade(trade)
        await self.store.append(
            LedgerEvent(
                "SHADOW_TRADE_OPENED",
                trade.trade_id,
                {"shadow_trade": asdict(trade)},
                occurred_at=opened_at,
            )
        )
        return trade

    @transactional
    async def seed_wallet(
        self,
        capital_base: float,
        cash_available: float,
        *,
        source: str | None = None,
        now: datetime | None = None,
    ) -> WalletState:
        if capital_base <= 0 or cash_available < 0 or cash_available > capital_base * 2:
            raise ValueError("INVALID_WALLET_SEED")
        timestamp = now or datetime.now(UTC)
        wallet = WalletState(
            capital_base=capital_base,
            cash_available=cash_available,
            as_of=timestamp,
        )
        await self.store.save_wallet(wallet)
        payload = asdict(wallet)
        if source is not None:
            payload["source"] = source
        await self.store.append(
            LedgerEvent("WALLET_SEEDED", "wallet", payload, occurred_at=timestamp)
        )
        return wallet

    @transactional
    async def reconcile_wallet(
        self,
        capital_base: float,
        cash_available: float,
        *,
        daily_realized_loss: float = 0.0,
    ) -> WalletState:
        if capital_base <= 0 or cash_available < 0 or daily_realized_loss < 0:
            raise ValueError("INVALID_WALLET_RECONCILIATION")
        positions = [
            row
            for row in await self.store.list_positions()
            if row.closed_at is None and row.remaining_quantity > 0
        ]
        open_risk = sum(
            max(0.0, (row.average_fill - row.stop) * row.remaining_quantity)
            for row in positions
        )
        wallet = WalletState(
            capital_base=capital_base,
            cash_available=cash_available,
            daily_realized_loss=daily_realized_loss,
            open_risk=round(open_risk, 2),
            reserved_cash=0.0,
        )
        await self.store.save_wallet(wallet)
        await self.store.append(LedgerEvent("WALLET_RECONCILED", "wallet", asdict(wallet)))
        return wallet

    @transactional
    async def import_position(
        self,
        *,
        symbol: str,
        quantity: int,
        average_fill: float,
        stop_order_price: float | None = None,
        broker_order_ref: str | None = None,
        opened_at: datetime | None = None,
        time_stop_minutes: int = 30,
    ) -> PositionState:
        wallet = await self.store.get_wallet()
        if wallet is None:
            raise ValueError("WALLET_NOT_SEEDED")
        if quantity <= 0 or average_fill <= 0:
            raise ValueError("INVALID_IMPORTED_POSITION")
        if stop_order_price is not None and stop_order_price <= 0:
            raise ValueError("INVALID_IMPORTED_STOP")
        for existing in await self.store.list_positions():
            if (
                existing.symbol.upper() == symbol.upper()
                and existing.closed_at is None
                and existing.remaining_quantity > 0
            ):
                raise ValueError("OPEN_POSITION_ALREADY_EXISTS_FOR_SYMBOL")

        now = datetime.now(UTC)
        opened = opened_at or now
        protected = stop_order_price is not None
        r_value = (average_fill - stop_order_price) if stop_order_price is not None else None
        tp1 = round(average_fill + r_value * 1.5, 4) if r_value is not None and r_value > 0 else None
        tp2 = round(average_fill + r_value * 2.0, 4) if r_value is not None and r_value > 0 else None
        position = PositionState(
            position_id=str(uuid4()),
            plan_id=f"manual-{uuid4()}",
            symbol=symbol.upper(),
            quantity=quantity,
            remaining_quantity=quantity,
            average_fill=average_fill,
            stop=stop_order_price,
            tp1=tp1,
            tp2=tp2,
            opened_at=opened,
            time_stop_at=opened + timedelta(minutes=time_stop_minutes),
            managed=True,
            source="MANUAL_IMPORT",
            protection=(ProtectionState.PROTECTED if protected else ProtectionState.UNPROTECTED),
            broker_stop_price=stop_order_price,
            broker_order_ref=(broker_order_ref if protected else None),
            protected_quantity=(quantity if protected else 0),
            reconciliation_state=ReconciliationState.RECONCILED,
            last_reconciled_at=now,
        )

        notional = average_fill * quantity
        position_risk = (
            max(0.0, (average_fill - stop_order_price) * quantity)
            if stop_order_price is not None
            else notional
        )
        wallet.cash_available -= notional
        wallet.open_risk += position_risk
        wallet.version += 1
        wallet.as_of = now
        await self.store.save_position(position)
        await self.store.save_wallet(wallet)
        await self.store.append(
            LedgerEvent("POSITION_IMPORTED", position.position_id, {"position": asdict(position), "wallet": asdict(wallet)})
        )
        if not protected:
            await self.store.append(
                LedgerEvent(
                    "POSITION_UNPROTECTED",
                    position.position_id,
                    {"symbol": position.symbol, "stop": None},
                )
            )
        return position

    @transactional
    async def reconcile_holdings(self, holdings: list[dict]) -> dict:
        now = datetime.now(UTC)
        declared: dict[str, int] = {}
        for row in holdings:
            symbol = str(row.get("symbol", "")).upper().strip()
            quantity = int(row.get("quantity", 0))
            if not symbol or quantity <= 0:
                raise ValueError("INVALID_RECONCILIATION_HOLDING")
            if symbol in declared:
                raise ValueError("DUPLICATE_RECONCILIATION_SYMBOL")
            declared[symbol] = quantity

        open_positions = [
            row
            for row in await self.store.list_positions()
            if row.closed_at is None and row.remaining_quantity > 0
        ]
        by_symbol: dict[str, list[PositionState]] = {}
        for position in open_positions:
            by_symbol.setdefault(position.symbol.upper(), []).append(position)

        mismatches: list[dict] = []
        reconciled_symbols: list[str] = []
        for symbol, positions in by_symbol.items():
            twin_quantity = sum(row.remaining_quantity for row in positions)
            declared_quantity = declared.get(symbol)
            if declared_quantity is None:
                for position in positions:
                    position.reconciliation_state = ReconciliationState.UNRECONCILED_MISSING_AT_BROKER
                    await self.store.save_position(position)
                detail = {
                    "symbol": symbol,
                    "twin_quantity": twin_quantity,
                    "declared_quantity": 0,
                    "reason": "MISSING_AT_BROKER",
                }
                mismatches.append(detail)
                await self.store.append(LedgerEvent("RECONCILIATION_MISMATCH", symbol, detail))
                continue
            if declared_quantity != twin_quantity:
                for position in positions:
                    position.reconciliation_state = ReconciliationState.UNRECONCILED
                    await self.store.save_position(position)
                detail = {
                    "symbol": symbol,
                    "twin_quantity": twin_quantity,
                    "declared_quantity": declared_quantity,
                    "reason": "QUANTITY_MISMATCH",
                }
                mismatches.append(detail)
                await self.store.append(LedgerEvent("RECONCILIATION_MISMATCH", symbol, detail))
                continue
            for position in positions:
                position.reconciliation_state = ReconciliationState.RECONCILED
                position.last_reconciled_at = now
                await self.store.save_position(position)
            reconciled_symbols.append(symbol)

        unknown_holdings: list[dict] = []
        for symbol, quantity in declared.items():
            if symbol in by_symbol:
                continue
            detail = {"symbol": symbol, "quantity": quantity}
            unknown_holdings.append(detail)
            await self.store.append(LedgerEvent("UNKNOWN_HOLDING", symbol, detail))

        result = {
            "reconciled_symbols": sorted(reconciled_symbols),
            "mismatches": mismatches,
            "unknown_holdings": unknown_holdings,
            "reconciled_at": now.isoformat(),
        }
        await self.store.append(
            LedgerEvent(
                "RECONCILIATION_COMPLETED",
                "portfolio",
                {"positions": [asdict(row) for row in await self.store.list_positions()]},
            )
        )
        return result

    @staticmethod
    def _opening_structure(bars: list[dict], session_start: datetime, closed_before: datetime) -> tuple[float, float, int]:
        parsed: list[tuple[datetime, float, float]] = []
        for row in bars:
            raw_ts = row.get("t") or row.get("timestamp")
            high = row.get("h", row.get("high"))
            low = row.get("l", row.get("low"))
            if raw_ts is None or high is None or low is None:
                continue
            try:
                stamp = datetime.fromisoformat(str(raw_ts))
                high_value = float(high)
                low_value = float(low)
            except (TypeError, ValueError):
                continue
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=UTC)
            stamp = stamp.astimezone(UTC)
            if session_start <= stamp < closed_before:
                parsed.append((stamp, high_value, low_value))
        parsed.sort(key=lambda row: row[0])
        if not parsed or parsed[0][0] != session_start:
            raise ValueError("OPENING_BARS_NOT_CONTIGUOUS")
        contiguous = [parsed[0]]
        for row in parsed[1:]:
            if row[0] - contiguous[-1][0] != timedelta(minutes=1):
                break
            contiguous.append(row)
        if len(contiguous) < 2:
            raise ValueError("STRUCTURE_DATA_MISSING")
        opening_count = min(5, len(contiguous) - 1)
        opening = contiguous[:opening_count]
        retest = contiguous[opening_count : opening_count + 3]
        if not retest:
            raise ValueError("STRUCTURE_DATA_MISSING")
        opening_range_high = max(row[1] for row in opening)
        retest_low = min(row[2] for row in retest)
        return opening_range_high, retest_low, opening_count

    async def refresh_liquidity_profile(
        self,
        symbol: str,
        *,
        now: datetime | None = None,
    ) -> LiquidityProfile:
        if self.market_data is None:
            raise RuntimeError("MARKET_DATA_PROVIDER_NOT_CONFIGURED")
        timestamp = now or datetime.now(UTC)
        eastern = ZoneInfo("America/New_York")
        local = timestamp.astimezone(eastern)
        end = local.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(UTC)
        start = end - timedelta(days=45)
        rows = await self.market_data.bars(symbol.upper(), "1Day", start, end)
        parsed: list[tuple[datetime, float, float]] = []
        for row in rows:
            raw_ts = row.get("t") or row.get("timestamp")
            raw_volume = row.get("v", row.get("volume"))
            raw_close = row.get("c", row.get("close"))
            if raw_ts is None or raw_volume is None or raw_close is None:
                continue
            try:
                stamp = datetime.fromisoformat(str(raw_ts))
                if stamp.tzinfo is None:
                    stamp = stamp.replace(tzinfo=UTC)
                volume = float(raw_volume)
                close = float(raw_close)
            except (TypeError, ValueError):
                continue
            if volume < 0 or close <= 0:
                continue
            parsed.append((stamp.astimezone(UTC), volume, close))
        parsed.sort(key=lambda row: row[0])
        if len(parsed) < 20:
            raise RuntimeError("LIQUIDITY_PROFILE_INSUFFICIENT_HISTORY")
        latest = parsed[-20:]
        profile = LiquidityProfile(
            symbol=symbol.upper(),
            adv20=sum(row[1] for row in latest) / 20.0,
            close=latest[-1][2],
            as_of=latest[-1][0],
            refreshed_at=timestamp,
        )
        async with self.store.transaction():
            await self.store.save_liquidity_profile(profile)
            await self.store.append(
                LedgerEvent(
                    "LIQUIDITY_PROFILE_REFRESHED",
                    profile.symbol,
                    {"profile": asdict(profile)},
                )
            )
        return profile

    async def ensure_liquidity_profile(
        self,
        symbol: str,
        *,
        now: datetime | None = None,
    ) -> LiquidityProfile:
        timestamp = now or datetime.now(UTC)
        existing = await self.store.get_liquidity_profile(symbol)
        eastern = ZoneInfo("America/New_York")
        if (
            existing is not None
            and existing.refreshed_at.astimezone(eastern).date()
            == timestamp.astimezone(eastern).date()
        ):
            return existing
        return await self.refresh_liquidity_profile(symbol, now=timestamp)

    async def _market_liquidity_reasons(
        self,
        snapshot: MarketSnapshot,
        *,
        now: datetime | None = None,
    ) -> list[str]:
        if snapshot.source_id != "ALPACA_IEX" and not _is_keyless_source(snapshot.source_id):
            return []
        try:
            profile = await self.ensure_liquidity_profile(snapshot.symbol, now=now)
        except DataUnavailable:
            raise
        except (RuntimeError, ValueError, TypeError):
            profile = None
        if snapshot.source_id == "ALPACA_IEX":
            return apply_iex_liquidity_gate(snapshot, profile, self.cfg)
        return apply_keyless_liquidity_gate(snapshot, profile, self.cfg)

    async def refresh_liquidity_profiles(self) -> dict[str, int]:
        if self.market_data is None or not getattr(self.market_data, "configured", False):
            return {"refreshed": 0, "failed": 0}
        symbols = {
            row.symbol.upper()
            for row in await self.store.list_plans()
            if row.status in {PlanStatus.ACTIVE, PlanStatus.RESERVED}
        }
        symbols.update(
            row.symbol.upper()
            for row in await self.store.list_positions()
            if row.closed_at is None and row.remaining_quantity > 0
        )
        refreshed = 0
        failed = 0
        for symbol in sorted(symbols):
            try:
                await self.ensure_liquidity_profile(symbol)
                refreshed += 1
            except (RuntimeError, ValueError, TypeError) as exc:
                failed += 1
                await self.store.append(
                    LedgerEvent(
                        "LIQUIDITY_PROFILE_REFRESH_FAILED",
                        symbol,
                        {"error_type": type(exc).__name__},
                    )
                )
        return {"refreshed": refreshed, "failed": failed}

    async def get_intraday_structure(
        self,
        symbol: str,
        *,
        now: datetime | None = None,
    ) -> IntradayStructure | None:
        timestamp = now or datetime.now(UTC)
        key = structure_key(symbol, session_date_for(timestamp))
        payload = await self.store.get_runtime_status_key(key)
        if not isinstance(payload, dict):
            return None
        try:
            return structure_from_dict(payload)
        except (KeyError, TypeError, ValueError):
            return None

    async def record_intraday_bar(
        self,
        symbol: str,
        bar: dict,
        *,
        now: datetime | None = None,
        source: str = "IEX",
    ) -> IntradayStructure:
        timestamp = now or datetime.now(UTC)
        record = self._intraday_bar_record(symbol, bar, source=source)
        key = structure_key(record.symbol, record.session_date)
        coverage_key = self._sip_coverage_key(record.symbol, record.session_date)
        async with self.store.transaction():
            previous_payload = await self.store.get_runtime_status_key(key)
            await self.store.save_intraday_bar(record)
            rows = await self.store.list_intraday_bars(record.symbol, record.session_date)
            coverage_raw = await self.store.get_runtime_status_key(coverage_key)
            coverage = self._coverage_timestamp(coverage_raw)
            structure = compute_structure(
                record.symbol,
                record.session_date,
                [row.as_market_bar() for row in rows],
                self.cfg,
                now=timestamp,
                sip_confirmed_through=coverage,
            )
            await self.store.set_runtime_status(key, asdict(structure))
            previous_state = previous_payload.get("state") if isinstance(previous_payload, dict) else None
            if previous_state != str(structure.state):
                await self.store.append(
                    LedgerEvent(
                        "INTRADAY_STRUCTURE_TRANSITION",
                        structure.symbol,
                        {
                            "session_date": structure.session_date,
                            "from_state": previous_state,
                            "to_state": str(structure.state),
                            "reasons": list(structure.reasons),
                            "last_bar_at": structure.last_bar_at.isoformat() if structure.last_bar_at else None,
                        },
                    )
                )
        return structure

    async def bootstrap_intraday_structure(
        self,
        symbol: str,
        *,
        now: datetime | None = None,
    ) -> IntradayStructure | None:
        timestamp = now or datetime.now(UTC)
        existing = await self.get_intraday_structure(symbol, now=timestamp)
        if self.market_data is not None and getattr(self.market_data, "configured", False):
            await self.backfill_intraday_structures([symbol], now=timestamp)
            refreshed = await self.get_intraday_structure(symbol, now=timestamp)
            if refreshed is not None:
                return refreshed
        return existing

    async def backfill_intraday_structures(
        self,
        symbols: list[str],
        *,
        now: datetime | None = None,
    ) -> dict[str, IntradayStructure]:
        if self.market_data is None or not getattr(self.market_data, "configured", False):
            return {}
        normalized = list(dict.fromkeys(symbol.upper() for symbol in symbols if symbol))
        if not normalized:
            return {}
        timestamp = now or datetime.now(UTC)
        eastern = ZoneInfo("America/New_York")
        local = timestamp.astimezone(eastern)
        session_start_local = local.replace(hour=9, minute=30, second=0, microsecond=0)
        session_start = session_start_local.astimezone(UTC)
        lag_minutes = (
            self.cfg.keyless_confirmed_lag_minutes
            if self.cfg.data_plan == "keyless_delayed"
            else self.cfg.historical_lag_minutes
        )
        lag_cutoff = timestamp - timedelta(minutes=lag_minutes)
        confirmed_through = lag_cutoff.replace(second=0, microsecond=0)
        if confirmed_through <= session_start:
            return {}
        batch_method = getattr(self.market_data, "bars_batch", None)
        if batch_method is None:
            raise RuntimeError("MARKET_DATA_BATCH_BARS_NOT_CONFIGURED")
        raw_by_symbol = await batch_method(
            normalized, "1Min", session_start, confirmed_through
        )
        session_date = session_date_for(timestamp)
        output: dict[str, IntradayStructure] = {}
        for symbol in normalized:
            key = structure_key(symbol, session_date)
            coverage_key = self._sip_coverage_key(symbol, session_date)
            raw_rows = raw_by_symbol.get(symbol, [])
            records = [self._intraday_bar_record(symbol, row, source="SIP") for row in raw_rows]
            async with self.store.transaction():
                previous_payload = await self.store.get_runtime_status_key(key)
                for record in records:
                    if record.session_date == session_date:
                        await self.store.save_intraday_bar(record)
                await self.store.set_runtime_status(
                    coverage_key, {"confirmed_through": confirmed_through.isoformat(), "source": "SIP"}
                )
                rows = await self.store.list_intraday_bars(symbol, session_date)
                structure = compute_structure(
                    symbol,
                    session_date,
                    [row.as_market_bar() for row in rows],
                    self.cfg,
                    now=timestamp,
                    sip_confirmed_through=confirmed_through,
                )
                await self.store.set_runtime_status(key, asdict(structure))
                previous_state = previous_payload.get("state") if isinstance(previous_payload, dict) else None
                if previous_state != str(structure.state):
                    await self.store.append(
                        LedgerEvent(
                            "INTRADAY_STRUCTURE_TRANSITION",
                            symbol,
                            {
                                "session_date": session_date,
                                "from_state": previous_state,
                                "to_state": str(structure.state),
                                "reasons": list(structure.reasons),
                                "source": "SIP_BACKFILL",
                            },
                        )
                    )
            output[symbol] = structure
        return output

    @staticmethod
    def _sip_coverage_key(symbol: str, session_date: str) -> str:
        return f"intraday_sip_confirmed_through:{session_date}:{symbol.upper()}"

    @staticmethod
    def _coverage_timestamp(payload) -> datetime | None:
        if not isinstance(payload, dict) or not payload.get("confirmed_through"):
            return None
        try:
            stamp = datetime.fromisoformat(str(payload["confirmed_through"]))
        except ValueError:
            return None
        return stamp if stamp.tzinfo is not None else stamp.replace(tzinfo=UTC)

    @staticmethod
    def _intraday_bar_record(symbol: str, bar: dict, *, source: str) -> IntradayBarRecord:
        raw_stamp = bar.get("t") or bar.get("timestamp")
        if raw_stamp is None:
            raise ValueError("INTRADAY_BAR_TIMESTAMP_MISSING")
        try:
            stamp = datetime.fromisoformat(str(raw_stamp))
        except ValueError as exc:
            raise ValueError("INTRADAY_BAR_TIMESTAMP_INVALID") from exc
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=UTC)
        stamp = stamp.astimezone(UTC).replace(second=0, microsecond=0)

        def required(short: str, long: str) -> float:
            raw = bar.get(short, bar.get(long))
            if raw is None:
                raise ValueError("INTRADAY_BAR_INCOMPLETE")
            return float(raw)

        open_price = required("o", "open")
        high = required("h", "high")
        low = required("l", "low")
        close = required("c", "close")
        if open_price <= 0 or high <= 0 or low <= 0 or close <= 0 or high < low:
            raise ValueError("INTRADAY_BAR_INVALID")
        volume_raw = bar.get("v", bar.get("volume"))
        vwap_raw = bar.get("vw", bar.get("vwap"))
        return IntradayBarRecord(
            symbol=symbol.upper(),
            session_date=session_date_for(stamp),
            minute_ts=stamp,
            source=source.upper(),
            open=open_price,
            high=high,
            low=low,
            close=close,
            volume=float(volume_raw) if volume_raw is not None else None,
            vwap=float(vwap_raw) if vwap_raw is not None else None,
        )

    async def server_retest(
        self,
        plan: TradePlan,
        *,
        now: datetime | None = None,
    ) -> tuple[bool, list[str], str]:
        structure = await self.get_intraday_structure(plan.symbol, now=now)
        if structure is None:
            return False, ["RETEST_STRUCTURE_MISSING"], "MISSING"
        state = str(structure.state)
        if structure.state == IntradayStructureState.RETEST_VALID:
            return True, ["SERVER_RETEST_VALID"], state
        if structure.state == IntradayStructureState.INVALID:
            return False, list(structure.reasons or ["RETEST_STRUCTURE_INVALID"]), state
        return False, list(structure.reasons or ["RETEST_REQUIRED"]), state

    async def build_plan_from_market(
        self,
        *,
        symbol: str,
        quality: QualityProfile,
        lane: StrategyLane,
        catalyst_verified: bool,
        catalyst_strength: float,
        structure_score: float,
        rr_score: float,
        now: datetime | None = None,
    ) -> tuple[TradePlan, dict]:
        snapshot = await self.prepare_plan_market_data(symbol, now=now)
        snapshot.catalyst_verified = catalyst_verified
        snapshot.catalyst_strength = catalyst_strength
        return await self.build_plan(
            snapshot,
            quality,
            lane,
            structure_score,
            rr_score,
            now=now,
        )

    async def prepare_plan_market_data(
        self,
        symbol: str,
        *,
        now: datetime | None = None,
    ) -> MarketSnapshot:
        if self.market_data is None:
            raise RuntimeError("MARKET_DATA_PROVIDER_NOT_CONFIGURED")
        normalized = symbol.upper().strip()
        if not normalized:
            raise ValueError("INVALID_SYMBOL")
        snapshot = await self.market_data.snapshot(normalized, decision=False)

        timestamp = now or datetime.now(UTC)
        liquidity_reasons = (
            await self._market_liquidity_reasons(snapshot, now=timestamp)
            if _is_keyless_source(snapshot.source_id)
            else []
        )
        blocking = [reason for reason in liquidity_reasons if reason != "LIQUIDITY_GATE_PASS"]
        if blocking:
            unavailable_reasons = {
                "LIQUIDITY_PROFILE_MISSING",
                "DELAYED_DATA_STALE",
                "KEYLESS_BAR_RANGE_MISSING",
                "KEYLESS_BAR_RANGE_TOO_WIDE",
                "PRICE_CROSS_CHECK_FAILED",
            }
            if any(reason in unavailable_reasons for reason in blocking):
                raise DataUnavailable(
                    source_id=snapshot.source_id or "KEYLESS_DELAYED",
                    resource="planning_snapshot",
                    symbol=normalized,
                    error_type=blocking[0],
                )
            raise ValueError(blocking[0])
        eastern = ZoneInfo("America/New_York")
        local = timestamp.astimezone(eastern)
        session_local = local.replace(hour=9, minute=30, second=0, microsecond=0)
        session_start = session_local.astimezone(UTC)
        closed_before = local.replace(second=0, microsecond=0).astimezone(UTC)
        if closed_before <= session_start:
            raise ValueError("STRUCTURE_DATA_MISSING")
        bars = await self.market_data.bars(
            normalized,
            "1Min",
            session_start,
            closed_before,
        )
        opening_high, retest_low, opening_count = self._opening_structure(
            bars,
            session_start,
            closed_before,
        )
        snapshot.opening_range_high = opening_high
        snapshot.retest_low = retest_low
        snapshot.metadata = {
            **snapshot.metadata,
            "planning_feed": snapshot.source_id,
            "opening_range_minutes": opening_count,
            "opening_range_session_start": session_start.isoformat(),
            "bars_count": len(bars),
        }
        return snapshot

    @transactional
    async def build_plan(
        self,
        snapshot: MarketSnapshot,
        quality: QualityProfile,
        lane: StrategyLane,
        structure_score: float,
        rr_score: float,
        *,
        now: datetime | None = None,
    ) -> tuple[TradePlan, dict]:
        features = compute_features(snapshot)
        score = score_features(
            features, structure_score=structure_score, rr_score=rr_score
        )
        plan = build_trade_plan(
            snapshot=snapshot,
            score=score,
            quality=quality,
            lane=lane,
            plan_ttl_seconds=(
                self.cfg.keyless_plan_ttl_seconds
                if self.cfg.data_plan == "keyless_delayed"
                else self.cfg.plan_ttl_seconds
            ),
            speculative_enabled=self.cfg.strategy_speculative_enabled,
            now=now,
        )
        await self.store.save_plan(plan)
        await self.store.append(
            LedgerEvent(
                "PLAN_ISSUED",
                plan.plan_id,
                {
                    "plan": asdict(plan),
                    "features": asdict(features),
                    "score": asdict(score),
                },
            )
        )
        return plan, {"features": asdict(features), "score": asdict(score)}

    @transactional
    async def sweep_expired(
        self,
        *,
        now: datetime | None = None,
    ) -> dict[str, int]:
        timestamp = now or datetime.now(UTC)
        expired_plans = 0
        released_reservations = 0

        for plan in await self.store.list_plans():
            if plan.status in {
                PlanStatus.FILLED,
                PlanStatus.CANCELLED,
                PlanStatus.EXPIRED,
            }:
                continue
            if timestamp <= plan.expires_at:
                continue
            if (
                await self.store.get_reservation(plan.plan_id) is not None
                and await self.release_reservation(
                    plan.plan_id,
                    reason="PLAN_EXPIRED",
                    now=timestamp,
                )
            ):
                released_reservations += 1
            plan.status = PlanStatus.EXPIRED
            await self.store.save_plan(plan)
            await self.store.append(
                LedgerEvent(
                    "PLAN_EXPIRED",
                    plan.plan_id,
                    {"expires_at": plan.expires_at.isoformat(), "plan": asdict(plan)},
                )
            )
            expired_plans += 1

        for reservation in await self.store.list_reservations():
            if (
                timestamp > reservation.expires_at
                and await self.release_reservation(
                    reservation.plan_id,
                    reason="RESERVATION_EXPIRED",
                    now=timestamp,
                )
            ):
                released_reservations += 1

        return {
            "expired_plans": expired_plans,
            "released_reservations": released_reservations,
        }

    def _order_ticket(self, plan: TradePlan, quantity: int, expires_at: datetime) -> dict:
        if plan.stop is None or plan.tp1 is None or plan.tp2 is None:
            raise ValueError("PLAN_PROTECTION_LEVELS_MISSING")
        text = (
            f"BUY {quantity} {plan.symbol} LMT {plan.entry_zone_high:.2f} | "
            f"STOP {plan.stop:.2f} GTC | TP1 {plan.tp1:.2f} TP2 {plan.tp2:.2f}"
        )
        return {
            "symbol": plan.symbol,
            "side": "BUY",
            "quantity": quantity,
            "limit_price": plan.entry_zone_high,
            "stop_price": plan.stop,
            "tp1": plan.tp1,
            "tp2": plan.tp2,
            "expires_at": expires_at.isoformat(),
            "text": text,
        }

    async def _queue_alert(self, kind: str, payload: dict) -> AlertRecord:
        alert = AlertRecord(kind=kind, payload=payload)
        await self.store.save_alert(alert)
        return alert

    @transactional
    async def record_trigger_hit(
        self,
        plan_id: str,
        *,
        last: float,
        triggered_at: datetime,
        source: str,
    ) -> bool:
        plan = await self.store.get_plan(plan_id)
        timestamp = triggered_at.astimezone(UTC)
        if (
            plan is None
            or plan.status != PlanStatus.ACTIVE
            or plan.triggered_at is not None
            or timestamp > plan.expires_at
        ):
            return False
        activation_deadline = timestamp + timedelta(
            minutes=self.cfg.retest_window_minutes + 5
        )
        plan.expires_at = max(plan.expires_at, activation_deadline)
        wallet = await self.store.get_wallet()
        quantity = 0
        if wallet is not None:
            sizing = size_from_wallet(
                wallet,
                plan,
                max_trade_risk_pct=self.cfg.max_trade_risk_pct,
                max_daily_loss_pct=self.cfg.max_daily_loss_pct,
                max_position_notional_pct=self.cfg.max_position_notional_pct,
            )
            if sizing.allowed:
                quantity = sizing.quantity
        ticket = {
            "symbol": plan.symbol,
            "side": "BUY",
            "quantity": quantity,
            "limit_price": plan.entry_zone_high,
            "stop_price": plan.stop,
            "tp1": plan.tp1,
            "tp2": plan.tp2,
            "expires_at": plan.expires_at.isoformat(),
            "text": (
                f"TRIGGER {plan.symbol} {plan.entry_trigger:.2f} | "
                f"LMT {plan.entry_zone_high:.2f} STOP {plan.stop:.2f} | "
                f"TP1 {plan.tp1:.2f} TP2 {plan.tp2:.2f} | QTY {quantity}"
            ),
        }
        alert = AlertRecord(
            kind="TRIGGER_HIT",
            payload={
                "action": "TRIGGER_HIT",
                "plan_id": plan.plan_id,
                "symbol": plan.symbol,
                "last": last,
                "source": source,
                "extended_expires_at": plan.expires_at.isoformat(),
                "order_ticket": ticket,
                "text": ticket["text"],
            },
            created_at=timestamp,
        )
        plan.triggered_at = timestamp
        await self.store.save_plan(plan)
        await self.store.save_alert(alert)
        await self.store.append(
            LedgerEvent(
                "TRIGGER_HIT",
                plan.plan_id,
                {
                    "alert_id": alert.alert_id,
                    "order_ticket": ticket,
                    "last": last,
                    "source": source,
                    "extended_expires_at": plan.expires_at.isoformat(),
                    "plan": asdict(plan),
                },
                occurred_at=timestamp,
            )
        )
        await self._notify_state_change(plan.symbol)
        return True

    async def activate(
        self,
        plan_id: str,
        *,
        now: datetime | None = None,
    ) -> ActivationDecision:
        timestamp = now or datetime.now(UTC)
        await self.sweep_expired(now=timestamp)
        plan = await self.store.get_plan(plan_id)
        if plan is None:
            return ActivationDecision(plan_id, "UNKNOWN", SignalState.INVALID, ["PLAN_NOT_FOUND"])
        if plan.status == PlanStatus.EXPIRED or timestamp > plan.expires_at:
            await self._notify_state_change(plan.symbol)
            return ActivationDecision(plan_id, plan.symbol, SignalState.EXPIRED, ["PLAN_EXPIRED"])
        if plan.status == PlanStatus.FILLED:
            return ActivationDecision(plan_id, plan.symbol, SignalState.INVALID, ["PLAN_ALREADY_FILLED"])
        wallet = await self.store.get_wallet()
        if wallet is None:
            return ActivationDecision(plan_id, plan.symbol, SignalState.WATCH, ["WALLET_NOT_SEEDED"])
        if self.market_data is None:
            raise RuntimeError("MARKET_DATA_PROVIDER_NOT_CONFIGURED")

        snapshot = await self.market_data.snapshot(plan.symbol, decision=True)
        liquidity_reasons = await self._market_liquidity_reasons(
            snapshot,
            now=timestamp,
        )
        retest_valid, retest_reasons, retest_state = await self.server_retest(
            plan,
            now=timestamp,
        )
        return await self._activate_with_context(
            plan_id,
            snapshot=snapshot,
            liquidity_reasons=liquidity_reasons,
            retest_valid=retest_valid,
            retest_reasons=retest_reasons,
            retest_state=retest_state,
            now=timestamp,
        )

    @transactional
    async def _activate_with_context(
        self,
        plan_id: str,
        *,
        snapshot: MarketSnapshot,
        liquidity_reasons: list[str],
        retest_valid: bool,
        retest_reasons: list[str],
        retest_state: str,
        now: datetime | None = None,
    ) -> ActivationDecision:
        timestamp = now or datetime.now(UTC)
        plan = await self.store.get_plan(plan_id)
        if plan is None:
            return ActivationDecision(plan_id, "UNKNOWN", SignalState.INVALID, ["PLAN_NOT_FOUND"])
        if plan.status == PlanStatus.EXPIRED or timestamp > plan.expires_at:
            if await self.store.get_reservation(plan_id) is not None:
                await self.release_reservation(
                    plan_id,
                    reason="PLAN_EXPIRED",
                    now=timestamp,
                )
            plan.status = PlanStatus.EXPIRED
            await self.store.save_plan(plan)
            await self._notify_state_change(plan.symbol)
            return ActivationDecision(plan_id, plan.symbol, SignalState.EXPIRED, ["PLAN_EXPIRED"])
        wallet = await self.store.get_wallet()
        if wallet is None:
            return ActivationDecision(plan_id, plan.symbol, SignalState.WATCH, ["WALLET_NOT_SEEDED"])
        if plan.status == PlanStatus.FILLED:
            return ActivationDecision(plan_id, plan.symbol, SignalState.INVALID, ["PLAN_ALREADY_FILLED"])
        open_positions = [
            row
            for row in await self.store.list_positions()
            if row.closed_at is None and row.remaining_quantity > 0
        ]
        same_plan_open = any(row.plan_id == plan_id for row in open_positions)
        if len(open_positions) >= self.cfg.max_concurrent_positions and not same_plan_open:
            if await self.store.get_reservation(plan_id) is not None:
                await self.release_reservation(
                    plan_id,
                    reason="MAX_CONCURRENT_POSITIONS_REACHED",
                    now=timestamp,
                )
            return ActivationDecision(
                plan_id, plan.symbol, SignalState.WATCH, ["MAX_CONCURRENT_POSITIONS_REACHED"]
            )

        above_vwap = snapshot.vwap is not None and snapshot.last >= snapshot.vwap
        common_reasons = list(dict.fromkeys([*liquidity_reasons, *retest_reasons]))
        existing = await self.store.get_reservation(plan_id)
        if existing is not None:
            if timestamp <= existing.expires_at:
                if not retest_valid or (
                    (snapshot.source_id == "ALPACA_IEX" or _is_keyless_source(snapshot.source_id))
                    and not snapshot.authoritative
                ):
                    decision = ActivationDecision(
                        plan_id,
                        plan.symbol,
                        SignalState.ARMED,
                        common_reasons,
                        entry=plan.entry_trigger,
                        stop=plan.stop,
                        tp1=plan.tp1,
                        tp2=plan.tp2,
                    )
                    await self.store.append(
                        LedgerEvent(
                            "PLAN_EVALUATED",
                            plan_id,
                            {
                                **asdict(decision),
                                "retest_valid": retest_valid,
                                "retest_valid_source": "SERVER",
                                "retest_state": retest_state,
                                "source_id": snapshot.source_id,
                                "data_age_seconds": snapshot.data_age_seconds,
                            },
                        )
                    )
                    return decision
                ticket = self._order_ticket(plan, existing.quantity, existing.expires_at)
                decision = ActivationDecision(
                    plan_id,
                    plan.symbol,
                    SignalState.BUY_NOW,
                    list(dict.fromkeys(["EXISTING_CAPACITY_RESERVATION", *common_reasons])),
                    quantity=existing.quantity,
                    entry=plan.entry_trigger,
                    stop=plan.stop,
                    tp1=plan.tp1,
                    tp2=plan.tp2,
                    order_ticket=ticket,
                )
                alert = await self._queue_alert(
                    "BUY_NOW",
                    {
                        "action": "BUY_NOW",
                        "plan_id": plan_id,
                        "symbol": plan.symbol,
                        "quantity": existing.quantity,
                        "entry": plan.entry_trigger,
                        "stop": plan.stop,
                        "tp1": plan.tp1,
                        "tp2": plan.tp2,
                        "expires_at": existing.expires_at.isoformat(),
                        "order_ticket": ticket,
                        "text": ticket["text"],
                    },
                )
                decision.alert_id = alert.alert_id
                await self.store.append(
                    LedgerEvent(
                        "PLAN_EVALUATED",
                        plan_id,
                        {
                            **asdict(decision),
                            "retest_valid": True,
                            "retest_valid_source": "SERVER",
                            "retest_state": retest_state,
                            "source_id": snapshot.source_id,
                            "data_age_seconds": snapshot.data_age_seconds,
                        },
                    )
                )
                await self.store.append(
                    LedgerEvent(
                        "BUY_NOW_EMITTED",
                        plan_id,
                        {
                            "alert_id": alert.alert_id,
                            "order_ticket": ticket,
                            "decision": asdict(decision),
                        },
                    )
                )
                await self._open_shadow_trade(plan, decision, now=timestamp)
                await self._notify_state_change(plan.symbol)
                return decision
            await self.release_reservation(
                plan_id,
                reason="RESERVATION_EXPIRED",
                now=timestamp,
            )
            wallet = await self.store.get_wallet()
            assert wallet is not None

        decision = activate_plan(
            plan,
            snapshot,
            wallet,
            retest_valid=retest_valid,
            above_vwap=above_vwap,
            max_data_age_seconds=(
                self.cfg.max_delayed_age_minutes * 60.0
                if _is_keyless_source(snapshot.source_id)
                else self.cfg.max_market_data_age_seconds
            ),
            max_trade_risk_pct=self.cfg.max_trade_risk_pct,
            max_daily_loss_pct=self.cfg.max_daily_loss_pct,
            max_position_notional_pct=self.cfg.max_position_notional_pct,
            now=timestamp,
        )
        decision.reasons = list(dict.fromkeys([*common_reasons, *decision.reasons]))
        event_payload = asdict(decision)
        event_payload.update(
            {
                "retest_valid": retest_valid,
                "retest_valid_source": "SERVER",
                "retest_state": retest_state,
                "source_id": snapshot.source_id,
                "data_age_seconds": snapshot.data_age_seconds,
            }
        )
        await self.store.append(LedgerEvent("PLAN_EVALUATED", plan_id, event_payload))
        if decision.state == SignalState.BUY_NOW:
            risk = decision.quantity * plan.risk_per_share
            cash = decision.quantity * plan.entry_zone_high
            reservation = Reservation(
                plan_id=plan_id,
                quantity=decision.quantity,
                reserved_cash=round(cash, 2),
                reserved_risk=round(risk, 2),
                expires_at=timestamp + timedelta(seconds=self.cfg.reservation_ttl_seconds),
            )
            wallet.reserved_cash += reservation.reserved_cash
            wallet.open_risk += reservation.reserved_risk
            wallet.version += 1
            wallet.as_of = timestamp
            plan.status = PlanStatus.RESERVED
            await self.store.save_wallet(wallet)
            await self.store.save_reservation(reservation)
            await self.store.save_plan(plan)
            await self.store.append(
                LedgerEvent(
                    "CAPACITY_RESERVED",
                    plan_id,
                    {"reservation": asdict(reservation), "wallet": asdict(wallet), "plan": asdict(plan)},
                )
            )
            ticket = self._order_ticket(plan, decision.quantity, reservation.expires_at)
            decision.order_ticket = ticket
            alert_payload = {
                "action": "BUY_NOW",
                "plan_id": plan_id,
                "symbol": plan.symbol,
                "quantity": decision.quantity,
                "entry": decision.entry,
                "stop": decision.stop,
                "tp1": decision.tp1,
                "tp2": decision.tp2,
                "expires_at": reservation.expires_at.isoformat(),
                "order_ticket": ticket,
                "text": ticket["text"],
            }
            alert = await self._queue_alert("BUY_NOW", alert_payload)
            decision.alert_id = alert.alert_id
            await self.store.append(
                LedgerEvent(
                    "BUY_NOW_EMITTED",
                    plan_id,
                    {
                        "alert_id": alert.alert_id,
                        "order_ticket": ticket,
                        "decision": asdict(decision),
                    },
                )
            )
            await self._open_shadow_trade(plan, decision, now=timestamp)
        await self._notify_state_change(plan.symbol)
        return decision

    @transactional
    async def release_reservation(
        self,
        plan_id: str,
        *,
        reason: str = "USER_RELEASED",
        now: datetime | None = None,
    ) -> bool:
        timestamp = now or datetime.now(UTC)
        reservation = await self.store.get_reservation(plan_id)
        wallet = await self.store.get_wallet()
        plan = await self.store.get_plan(plan_id)
        if reservation is None or wallet is None:
            return False
        wallet.reserved_cash = max(
            0.0, wallet.reserved_cash - reservation.reserved_cash
        )
        wallet.open_risk = max(0.0, wallet.open_risk - reservation.reserved_risk)
        wallet.version += 1
        wallet.as_of = timestamp
        if plan is not None and plan.status == PlanStatus.RESERVED:
            partial_position = next(
                (
                    row
                    for row in await self.store.list_positions()
                    if row.plan_id == plan_id and row.quantity > 0
                ),
                None,
            )
            if reason == "RESERVATION_EXPIRED" and partial_position is not None:
                plan.status = PlanStatus.FILLED
            else:
                plan.status = PlanStatus.ACTIVE
            await self.store.save_plan(plan)
        await self.store.save_wallet(wallet)
        await self.store.delete_reservation(plan_id)
        await self.store.append(
            LedgerEvent(
                "CAPACITY_RELEASED",
                plan_id,
                {"reason": reason, "reservation": asdict(reservation), "reservation_deleted": True, "wallet": asdict(wallet), "plan": asdict(plan) if plan is not None else None},
            )
        )
        return True

    @transactional
    async def confirm_fill(
        self,
        plan_id: str,
        *,
        fill_price: float,
        quantity: int,
        time_stop_minutes: int = 30,
        stop_order_placed: bool = False,
        stop_order_price: float | None = None,
        broker_order_ref: str | None = None,
    ) -> PositionState:
        await self.sweep_expired()
        plan = await self.store.get_plan(plan_id)
        wallet = await self.store.get_wallet()
        if plan is None or wallet is None:
            raise ValueError("FILL_CONFIRMATION_CONTEXT_MISSING")
        if plan.status == PlanStatus.FILLED:
            raise ValueError("PLAN_ALREADY_FILLED")
        reservation = await self.store.get_reservation(plan_id)
        if reservation is None:
            raise ValueError("FILL_CONFIRMATION_CONTEXT_MISSING")
        if plan.status != PlanStatus.RESERVED:
            raise ValueError("PLAN_NOT_RESERVED")
        if datetime.now(UTC) > reservation.expires_at:
            await self.release_reservation(plan_id, reason="RESERVATION_EXPIRED")
            raise ValueError("RESERVATION_EXPIRED")
        if quantity <= 0 or quantity > reservation.quantity:
            raise ValueError("QUANTITY_EXCEEDS_RESERVATION")
        max_fill = plan.entry_zone_high * (1.0 + plan.max_slippage_pct / 100.0)
        if fill_price < plan.entry_trigger or fill_price > max_fill:
            raise ValueError("FILL_OUTSIDE_PLAN")
        if plan.stop is None:
            raise ValueError("PLAN_STOP_MISSING")
        if stop_order_placed and stop_order_price is None:
            raise ValueError("STOP_ORDER_PRICE_REQUIRED")
        if not stop_order_placed and stop_order_price is not None:
            raise ValueError("STOP_ORDER_PRICE_WITHOUT_PLACEMENT")
        actual_cash = fill_price * quantity
        if actual_cash > wallet.cash_available + 1e-9:
            raise ValueError("FILL_EXCEEDS_CASH_AVAILABLE")
        if actual_cash > reservation.reserved_cash + 1e-9:
            raise ValueError("FILL_EXCEEDS_RESERVATION_CASH")

        open_positions = [
            row
            for row in await self.store.list_positions()
            if row.closed_at is None and row.remaining_quantity > 0
        ]
        existing = next(
            (row for row in open_positions if row.plan_id == plan_id),
            None,
        )
        if existing is None and len(open_positions) >= self.cfg.max_concurrent_positions:
            raise ValueError("MAX_CONCURRENT_POSITIONS_REACHED")
        if stop_order_price is not None:
            min_plan_stop = plan.stop * (1.0 - self.cfg.stop_tolerance_pct / 100.0)
            if (
                existing is not None
                and existing.stop is not None
                and stop_order_price < existing.stop - 1e-9
            ):
                raise ValueError("STOP_LOOSER_THAN_CURRENT")
            if stop_order_price < min_plan_stop - 1e-9:
                raise ValueError("STOP_LOOSER_THAN_PLAN")

        prior_quantity = existing.quantity if existing is not None else 0
        total_quantity = prior_quantity + quantity
        prior_value = existing.entry_avg * prior_quantity if existing is not None else 0.0
        entry_avg = (prior_value + fill_price * quantity) / total_quantity
        max_position_notional = (
            wallet.capital_base * self.cfg.max_position_notional_pct / 100.0
        )
        candidate_position_notional = entry_avg * total_quantity
        if candidate_position_notional > max_position_notional + 1e-9:
            raise ValueError("POSITION_NOTIONAL_LIMIT_EXCEEDED")
        if stop_order_placed:
            effective_stop = stop_order_price
        elif existing is not None and existing.stop is not None:
            effective_stop = existing.stop
        else:
            effective_stop = plan.stop
        assert effective_stop is not None
        candidate_total_risk = max(
            0.0, (entry_avg - effective_stop) * total_quantity
        )
        max_trade_risk = (
            wallet.capital_base
            * self.cfg.max_trade_risk_pct
            / 100.0
            * plan.quality_risk_multiplier
        )
        allowed_total_risk = max_trade_risk * (1.0 + self.cfg.fill_risk_tolerance)
        if candidate_total_risk > allowed_total_risk + 1e-9:
            existing_risk = 0.0
            if existing is not None and existing.stop is not None:
                existing_risk = max(
                    0.0,
                    (existing.entry_avg - existing.stop) * existing.quantity,
                )
            per_share = max(0.0, fill_price - effective_stop)
            remaining = max(0.0, allowed_total_risk - existing_risk)
            allowed_quantity = (
                floor((remaining + 1e-9) / per_share)
                if per_share > 0
                else reservation.quantity
            )
            raise ValueError(
                "FILL_RISK_EXCEEDS_TOLERANCE "
                f"allowed_quantity={allowed_quantity} "
                f"actual_risk={candidate_total_risk:.2f} "
                f"max_trade_risk={allowed_total_risk:.2f}"
            )

        now = datetime.now(UTC)
        old_position_risk = 0.0
        if existing is None:
            position = PositionState(
                position_id=str(uuid4()),
                plan_id=plan_id,
                symbol=plan.symbol,
                quantity=quantity,
                remaining_quantity=quantity,
                average_fill=fill_price,
                stop=effective_stop,
                tp1=None,
                tp2=None,
                opened_at=now,
                time_stop_at=now + timedelta(minutes=time_stop_minutes),
                protection=(
                    ProtectionState.PROTECTED
                    if stop_order_placed
                    else ProtectionState.UNPROTECTED
                ),
                broker_stop_price=(stop_order_price if stop_order_placed else None),
                broker_order_ref=(broker_order_ref if stop_order_placed else None),
                protected_quantity=(quantity if stop_order_placed else 0),
                reconciliation_state=ReconciliationState.RECONCILED,
                last_reconciled_at=now,
            )
        else:
            if existing.stop is not None:
                old_position_risk = max(
                    0.0,
                    (existing.entry_avg - existing.stop) * existing.quantity,
                )
            existing.average_fill = entry_avg
            existing.quantity = total_quantity
            existing.remaining_quantity += quantity
            existing.stop = effective_stop
            if stop_order_placed:
                existing.broker_stop_price = stop_order_price
                existing.broker_order_ref = broker_order_ref
                existing.protected_quantity = existing.remaining_quantity
            existing.protection = (
                ProtectionState.PROTECTED
                if existing.protected_quantity == existing.remaining_quantity
                else ProtectionState.UNPROTECTED
            )
            existing.reconciliation_state = ReconciliationState.RECONCILED
            existing.last_reconciled_at = now
            position = existing

        r_value = position.entry_avg - position.stop
        position.tp1 = round(position.entry_avg + r_value * 1.5, 4)
        position.tp2 = round(position.entry_avg + r_value * 2.0, 4)
        new_position_risk = max(
            0.0, (position.entry_avg - position.stop) * position.quantity
        )

        old_reserved_cash = reservation.reserved_cash
        old_reserved_risk = reservation.reserved_risk
        reservation.quantity -= quantity
        reservation.reserved_cash = round(
            reservation.quantity * plan.entry_zone_high, 2
        )
        reservation.reserved_risk = round(
            reservation.quantity * plan.risk_per_share, 2
        )
        wallet.reserved_cash -= old_reserved_cash - reservation.reserved_cash
        wallet.open_risk = (
            wallet.open_risk
            - old_reserved_risk
            + reservation.reserved_risk
            - old_position_risk
            + new_position_risk
        )
        wallet.cash_available -= actual_cash
        if (
            wallet.reserved_cash < -1e-9
            or wallet.open_risk < -1e-9
            or wallet.cash_available < -1e-9
        ):
            raise RuntimeError("WALLET_ACCOUNTING_INVARIANT_BROKEN")
        if abs(wallet.reserved_cash) < 1e-9:
            wallet.reserved_cash = 0.0
        if abs(wallet.open_risk) < 1e-9:
            wallet.open_risk = 0.0
        if abs(wallet.cash_available) < 1e-9:
            wallet.cash_available = 0.0
        wallet.version += 1
        wallet.as_of = now

        if reservation.quantity == 0:
            plan.status = PlanStatus.FILLED
            await self.store.delete_reservation(plan_id)
        else:
            plan.status = PlanStatus.RESERVED
            await self.store.save_reservation(reservation)
        await self.store.save_position(position)
        await self.store.save_wallet(wallet)
        await self.store.save_plan(plan)
        await self.store.append(
            LedgerEvent(
                "FILL_CONFIRMED",
                position.position_id,
                {
                    "fill_price": fill_price,
                    "fill_quantity": quantity,
                    "position": asdict(position),
                    "wallet": asdict(wallet),
                    "plan": asdict(plan),
                    "reservation": asdict(reservation) if reservation.quantity > 0 else None,
                    "reservation_deleted": reservation.quantity == 0,
                    "remaining_reservation_quantity": reservation.quantity,
                },
            )
        )
        if position.protection == ProtectionState.UNPROTECTED:
            await self.store.append(
                LedgerEvent(
                    "POSITION_UNPROTECTED",
                    position.position_id,
                    {"symbol": position.symbol, "stop": position.stop},
                )
            )
        await self._notify_state_change(position.symbol)
        return position

    @transactional
    async def protect_position(
        self,
        position_id: str,
        *,
        stop_order_price: float,
        broker_order_ref: str | None = None,
    ) -> PositionState:
        position = await self.store.get_position(position_id)
        wallet = await self.store.get_wallet()
        if position is None or wallet is None:
            raise ValueError("POSITION_OR_WALLET_MISSING")
        plan = await self.store.get_plan(position.plan_id)
        plan_stop = plan.stop if plan is not None else None
        reference_stop = position.stop if position.stop is not None else plan_stop
        if reference_stop is None:
            raise ValueError("POSITION_STOP_UNDEFINED")
        min_plan_stop = (
            plan_stop * (1.0 - self.cfg.stop_tolerance_pct / 100.0)
            if plan_stop is not None
            else reference_stop
        )
        min_stop = max(min_plan_stop, reference_stop)
        if stop_order_price < min_stop - 1e-9:
            if position.stop is not None and stop_order_price < position.stop - 1e-9:
                raise ValueError("STOP_LOOSER_THAN_CURRENT")
            raise ValueError("STOP_LOOSER_THAN_PLAN")
        old_risk = max(
            0.0,
            (position.entry_avg - reference_stop) * position.remaining_quantity,
        )
        new_risk = max(
            0.0,
            (position.entry_avg - stop_order_price) * position.remaining_quantity,
        )
        position.stop = stop_order_price
        position.broker_stop_price = stop_order_price
        position.broker_order_ref = broker_order_ref
        position.protected_quantity = position.remaining_quantity
        position.protection = ProtectionState.PROTECTED
        r_value = position.entry_avg - stop_order_price
        position.tp1 = round(position.entry_avg + r_value * 1.5, 4)
        position.tp2 = round(position.entry_avg + r_value * 2.0, 4)
        proposed_open_risk = wallet.open_risk - old_risk + new_risk
        if proposed_open_risk < -1e-9:
            raise RuntimeError("WALLET_ACCOUNTING_INVARIANT_BROKEN")
        wallet.open_risk = (
            0.0 if abs(proposed_open_risk) < 1e-9 else proposed_open_risk
        )
        wallet.version += 1
        wallet.as_of = datetime.now(UTC)
        await self.store.save_position(position)
        await self.store.save_wallet(wallet)
        await self.store.append(
            LedgerEvent(
                "STOP_ORDER_CONFIRMED",
                position_id,
                {
                    "stop_order_price": stop_order_price,
                    "broker_order_ref": broker_order_ref,
                    "position": asdict(position),
                    "wallet": asdict(wallet),
                },
            )
        )
        return position

    @transactional
    async def evaluate_position(
        self,
        position_id: str,
        *,
        last: float,
        below_vwap: bool = False,
        failed_breakout: bool = False,
        record_event: bool = True,
        queue_alert: bool = True,
    ):
        position = await self.store.get_position(position_id)
        action = evaluate_position(
            position,
            last=last,
            below_vwap=below_vwap,
            failed_breakout=failed_breakout,
            reconciliation_max_age_hours=self.cfg.reconciliation_max_age_hours,
        )
        if record_event:
            await self.store.append(
                LedgerEvent(
                    "POSITION_EVALUATED",
                    position_id,
                    {"last": last, "action": str(action)},
                )
            )
        action_name = str(action)
        if queue_alert and action_name in {
            "TRIM",
            "TAKE_PROFIT",
            "SELL_NOW",
            "PLACE_STOP_NOW",
            "RECONCILE_REQUIRED",
        }:
            await self._queue_alert(
                action_name,
                {
                    "action": action_name,
                    "position_id": position_id,
                    "symbol": position.symbol if position else None,
                    "last": last,
                },
            )
        return action

    @transactional
    async def confirm_exit(
        self, position_id: str, *, exit_price: float, quantity: int
    ) -> PositionState:
        position = await self.store.get_position(position_id)
        wallet = await self.store.get_wallet()
        if position is None or wallet is None:
            raise ValueError("POSITION_OR_WALLET_MISSING")
        if quantity <= 0 or quantity > position.remaining_quantity:
            raise ValueError("INVALID_EXIT_QUANTITY")

        pnl = (exit_price - position.average_fill) * quantity
        position.realized_pnl += pnl
        position.remaining_quantity -= quantity
        wallet.cash_available += exit_price * quantity
        wallet.open_risk = max(
            0.0,
            wallet.open_risk
            - max(
                0.0,
                (
                    position.average_fill
                    - (position.stop or position.average_fill)
                )
                * quantity,
            ),
        )
        if pnl < 0:
            wallet.daily_realized_loss += abs(pnl)
        if position.remaining_quantity == 0:
            position.closed_at = datetime.now(UTC)
        wallet.version += 1
        wallet.as_of = datetime.now(UTC)

        await self.store.save_position(position)
        await self.store.save_wallet(wallet)
        await self.store.append(
            LedgerEvent(
                "EXIT_CONFIRMED",
                position_id,
                {"exit_price": exit_price, "quantity": quantity, "pnl": pnl, "position": asdict(position), "wallet": asdict(wallet)},
            )
        )
        await self._notify_state_change(position.symbol)
        return position

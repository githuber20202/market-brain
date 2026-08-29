from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from market_brain.domain.models import (
    IntradayStructure,
    IntradayStructureState,
    MarketSnapshot,
    PositionAction,
    PositionState,
    ProtectionState,
    ReconciliationState,
    ScoreCard,
    StrategyLane,
    TradePlan,
    WalletState,
)
from market_brain.engines.activation import activate_plan
from market_brain.engines.intraday import compute_structure
from market_brain.engines.plan import PlanBuildError, build_trade_plan
from market_brain.engines.position import evaluate_position
from market_brain.engines.quality import classify_quality
from market_brain.settings import ROOT, Settings, settings

EASTERN = ZoneInfo("America/New_York")
SLIPPAGE_BPS = 10.0
TIME_STOP_MINUTES = 30
TP1_FRACTION = 0.5


@dataclass(frozen=True, slots=True)
class ReplayTick:
    at: datetime
    price: float
    kind: str


@dataclass(slots=True)
class VirtualTradeState:
    fill: float
    stop: float
    tp1: float
    tp2: float
    opened_at: datetime
    time_stop_at: datetime
    remaining_fraction: float = 1.0
    tp1_taken: bool = False
    status: str = "OPEN"
    realized_r: float = 0.0
    exit_legs: list[dict[str, Any]] | None = None

    def __post_init__(self) -> None:
        if self.fill <= self.stop:
            raise ValueError("REPLAY_FILL_RISK_INVALID")
        if self.exit_legs is None:
            self.exit_legs = []


@dataclass(frozen=True, slots=True)
class VirtualTradeTransition:
    status: str
    remaining_fraction: float
    tp1_taken: bool
    realized_r: float
    exit_legs: tuple[dict[str, Any], ...]


def synthesize_bar_ticks(bar: dict[str, Any]) -> tuple[ReplayTick, ...]:
    stamp = _bar_stamp(bar)
    open_price = _bar_price(bar, "o", "open")
    high = _bar_price(bar, "h", "high")
    low = _bar_price(bar, "l", "low")
    close = _bar_price(bar, "c", "close")
    if high < max(open_price, close) or low > min(open_price, close):
        raise ValueError("REPLAY_BAR_INVALID")
    middle = (("low", low), ("high", high)) if close >= open_price else (("high", high), ("low", low))
    path = (("open", open_price), *middle, ("close", close))
    offsets = (0, 20, 40, 59)
    return tuple(
        ReplayTick(at=stamp + timedelta(seconds=offset), price=price, kind=kind)
        for offset, (kind, price) in zip(offsets, path, strict=True)
    )


class ReplayEngine:
    def __init__(
        self,
        provider=None,
        *,
        cfg: Settings = settings,
        output_dir: Path | None = None,
        now=None,
    ) -> None:
        self.provider = provider
        self.cfg = cfg
        self.output_dir = output_dir or ROOT / "reports"
        self.now = now or (lambda: datetime.now(UTC))

    async def run(
        self,
        session_date: date | str,
        symbols: list[str],
        *,
        bars_by_symbol: dict[str, list[dict]] | None = None,
        write_report: bool = True,
    ) -> dict[str, Any]:
        day = date.fromisoformat(session_date) if isinstance(session_date, str) else session_date
        normalized = sorted(dict.fromkeys(symbol.strip().upper() for symbol in symbols if symbol.strip()))
        if not normalized:
            raise ValueError("REPLAY_SYMBOLS_EMPTY")
        if bars_by_symbol is None:
            if day >= self.now().astimezone(EASTERN).date():
                raise ValueError("REPLAY_REQUIRES_PRIOR_SESSION")
            if self.provider is None:
                raise RuntimeError("REPLAY_MARKET_DATA_PROVIDER_NOT_CONFIGURED")
            session_start = datetime.combine(day, time(9, 30), EASTERN).astimezone(UTC)
            session_end = datetime.combine(day, time(16, 0), EASTERN).astimezone(UTC)
            bars_by_symbol = await self.provider.bars_batch(
                normalized,
                "1Min",
                session_start,
                session_end,
            )

        trades: list[dict[str, Any]] = []
        for symbol in normalized:
            trade = self._run_symbol(day, symbol, bars_by_symbol.get(symbol, []))
            if trade is not None:
                trades.append(trade)
        trades.sort(key=lambda row: (row["entry_at"], row["symbol"]))
        report = {
            "date": day.isoformat(),
            "symbols": normalized,
            "bar_feed": "SIP",
            "timeframe": "1Min",
            "slippage_bps": SLIPPAGE_BPS,
            "trades": trades,
            **replay_summary(trades),
        }
        if write_report:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            path = self.output_dir / f"replay_{day.isoformat()}.json"
            path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        return report

    def _run_symbol(self, day: date, symbol: str, raw_bars: list[dict]) -> dict[str, Any] | None:
        bars = _session_bars(day, raw_bars)
        if not bars:
            return None
        plan: TradePlan | None = None
        structure: IntradayStructure | None = None
        plan_bar_index = -1
        for index in range(len(bars)):
            prefix = bars[: index + 1]
            structure = compute_structure(
                symbol,
                day.isoformat(),
                prefix,
                self.cfg,
                now=_bar_stamp(bars[index]) + timedelta(minutes=1),
                sip_confirmed_through=_bar_stamp(bars[index]) + timedelta(minutes=1),
            )
            if structure.state != IntradayStructureState.RETEST_VALID:
                continue
            try:
                plan = self._build_plan(symbol, day, structure, bars[index])
            except PlanBuildError:
                continue
            plan_bar_index = index
            break
        if plan is None or structure is None:
            return None

        wallet = WalletState(capital_base=100_000.0, cash_available=100_000.0)
        for index in range(plan_bar_index + 1, len(bars)):
            ticks = synthesize_bar_ticks(bars[index])
            for tick_index, tick in enumerate(ticks):
                snapshot = MarketSnapshot(
                    symbol=symbol,
                    last=tick.price,
                    bid=tick.price,
                    ask=tick.price,
                    vwap=tick.price,
                    data_age_seconds=0.0,
                    source_id="ALPACA_SIP",
                    authoritative=True,
                )
                decision = activate_plan(
                    plan,
                    snapshot,
                    wallet,
                    retest_valid=True,
                    above_vwap=True,
                    now=tick.at,
                )
                if str(decision.state) != "BUY_NOW":
                    continue
                fill = round(plan.entry_trigger * (1.0 + SLIPPAGE_BPS / 10_000.0), 4)
                return self._manage_trade(
                    symbol=symbol,
                    plan=plan,
                    fill=fill,
                    entry_at=tick.at,
                    quantity=decision.quantity,
                    bars=bars,
                    entry_bar_index=index,
                    remaining_entry_ticks=ticks[tick_index + 1 :],
                )
        return None

    def _build_plan(
        self,
        symbol: str,
        day: date,
        structure: IntradayStructure,
        retest_bar: dict,
    ) -> TradePlan:
        assert structure.opening_range_high is not None
        created_at = _bar_stamp(retest_bar) + timedelta(minutes=1)
        snapshot = MarketSnapshot(
            symbol=symbol,
            last=_bar_price(retest_bar, "c", "close"),
            opening_range_high=structure.opening_range_high,
            retest_low=_bar_price(retest_bar, "l", "low"),
            source_id="ALPACA_SIP",
            authoritative=True,
        )
        score = ScoreCard(
            symbol=symbol,
            catalyst_or_continuation=20.0,
            price_momentum=15.0,
            volume_liquidity=5.0,
            relative_strength_sector=0.0,
            entry_invalidation_structure=15.0,
            risk_reward=10.0,
            total=65.0,
            discovery_total=0.0,
            reasons=["REPLAY_STRUCTURE_VALIDATED"],
        )
        quality = classify_quality(symbol, 100.0, created_at)
        plan = build_trade_plan(
            snapshot=snapshot,
            score=score,
            quality=quality,
            lane=StrategyLane.CORE_MOMENTUM,
            plan_ttl_seconds=self.cfg.plan_ttl_seconds,
            speculative_enabled=False,
            now=created_at,
        )
        seed = f"{day.isoformat()}:{symbol}:{created_at.isoformat()}"
        plan.plan_id = "replay-" + hashlib.sha256(seed.encode()).hexdigest()[:20]
        plan.reasons.append("REPLAY_QUALITY_NEUTRAL")
        return plan

    def _manage_trade(
        self,
        *,
        symbol: str,
        plan: TradePlan,
        fill: float,
        entry_at: datetime,
        quantity: int,
        bars: list[dict],
        entry_bar_index: int,
        remaining_entry_ticks: tuple[ReplayTick, ...],
    ) -> dict[str, Any]:
        assert plan.stop is not None and plan.tp1 is not None and plan.tp2 is not None
        state = VirtualTradeState(
            fill=fill,
            stop=plan.stop,
            tp1=plan.tp1,
            tp2=plan.tp2,
            opened_at=entry_at,
            time_stop_at=entry_at + timedelta(minutes=TIME_STOP_MINUTES),
        )
        if remaining_entry_ticks:
            advance_virtual_trade_ticks(state, remaining_entry_ticks)
        for index in range(entry_bar_index + 1, len(bars)):
            if state.remaining_fraction <= 0:
                break
            advance_virtual_trade_bar(state, bars[index])

        if state.remaining_fraction > 0:
            last_tick = synthesize_bar_ticks(bars[-1])[-1]
            finalize_virtual_trade(state, last_tick.price, last_tick.at)
        return {
            "symbol": symbol,
            "plan_id": plan.plan_id,
            "entry_at": entry_at.isoformat(),
            "trigger": plan.entry_trigger,
            "fill": fill,
            "slippage_bps": SLIPPAGE_BPS,
            "quantity": quantity,
            "stop": plan.stop,
            "tp1": plan.tp1,
            "tp2": plan.tp2,
            "protection": str(ProtectionState.PROTECTED),
            "exit_legs": state.exit_legs,
            "r": state.realized_r,
        }


def advance_virtual_trade_bar(
    state: VirtualTradeState,
    bar: dict[str, Any],
) -> list[VirtualTradeTransition]:
    ticks = synthesize_bar_ticks(bar)
    return advance_virtual_trade_ticks(state, ticks, conflict_at=_bar_stamp(bar))


def advance_virtual_trade_ticks(
    state: VirtualTradeState,
    ticks: tuple[ReplayTick, ...],
    *,
    conflict_at: datetime | None = None,
) -> list[VirtualTradeTransition]:
    if not ticks or state.remaining_fraction <= 0:
        return []
    transitions: list[VirtualTradeTransition] = []
    next_target = state.tp2 if state.tp1_taken else state.tp1
    if min(tick.price for tick in ticks) <= state.stop and max(tick.price for tick in ticks) >= next_target:
        _transition(state, "STOPPED", "STOP", state.stop, state.remaining_fraction, conflict_at or ticks[0].at)
        transitions.append(_snapshot_transition(state))
        return transitions

    position = _virtual_position(state)
    for tick in ticks:
        if state.remaining_fraction <= 0:
            break
        action = evaluate_position(position, last=tick.price, now=tick.at)
        if action == PositionAction.SELL_NOW:
            if tick.price <= state.stop:
                _transition(state, "STOPPED", "STOP", state.stop, state.remaining_fraction, tick.at)
            else:
                _transition(state, "TIME_STOP", "TIME_STOP", tick.price, state.remaining_fraction, tick.at)
            transitions.append(_snapshot_transition(state))
            break
        if action == PositionAction.TAKE_PROFIT:
            _transition(state, "TP2", "TP2", state.tp2, state.remaining_fraction, tick.at)
            state.tp1_taken = True
            transitions.append(_snapshot_transition(state))
            break
        if action == PositionAction.TRIM and not state.tp1_taken:
            fraction = min(TP1_FRACTION, state.remaining_fraction)
            _transition(state, "TP1", "TP1", state.tp1, fraction, tick.at)
            state.tp1_taken = True
            position.remaining_quantity = 1
            transitions.append(_snapshot_transition(state))
    return transitions


def finalize_virtual_trade(
    state: VirtualTradeState,
    price: float,
    at: datetime,
) -> VirtualTradeTransition | None:
    if state.remaining_fraction <= 0:
        return None
    _transition(state, "TIME_STOP", "TIME_STOP", price, state.remaining_fraction, at)
    return _snapshot_transition(state)


def _virtual_position(state: VirtualTradeState) -> PositionState:
    return PositionState(
        position_id="virtual",
        plan_id="virtual",
        symbol="VIRTUAL",
        quantity=2,
        remaining_quantity=1 if state.tp1_taken else 2,
        average_fill=state.fill,
        stop=state.stop,
        tp1=state.tp1,
        tp2=state.tp2,
        opened_at=state.opened_at,
        time_stop_at=state.time_stop_at,
        protection=ProtectionState.PROTECTED,
        broker_stop_price=state.stop,
        protected_quantity=1 if state.tp1_taken else 2,
        reconciliation_state=ReconciliationState.RECONCILED,
        last_reconciled_at=state.opened_at,
        source="VIRTUAL_FILL",
    )


def _transition(
    state: VirtualTradeState,
    status: str,
    reason: str,
    price: float,
    fraction: float,
    at: datetime,
) -> None:
    assert state.exit_legs is not None
    _add_leg(state.exit_legs, reason, price, fraction, at)
    state.remaining_fraction = round(max(0.0, state.remaining_fraction - fraction), 6)
    state.status = status
    risk = state.fill - state.stop
    state.realized_r = round(
        sum(leg["fraction"] * (leg["price"] - state.fill) / risk for leg in state.exit_legs),
        6,
    )


def _snapshot_transition(state: VirtualTradeState) -> VirtualTradeTransition:
    assert state.exit_legs is not None
    return VirtualTradeTransition(
        status=state.status,
        remaining_fraction=state.remaining_fraction,
        tp1_taken=state.tp1_taken,
        realized_r=state.realized_r,
        exit_legs=tuple(dict(row) for row in state.exit_legs),
    )


def replay_summary(trades: list[dict[str, Any]]) -> dict[str, Any]:
    values = [float(row["r"]) for row in trades]
    equity = 0.0
    peak = 0.0
    drawdown = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    wins = sum(value > 0 for value in values)
    count = len(values)
    return {
        "trade_count": count,
        "wins": wins,
        "hit_rate": round(wins / count, 6) if count else 0.0,
        "expectancy_r": round(sum(values) / count, 6) if count else 0.0,
        "max_drawdown_r": round(drawdown, 6),
    }


def _session_bars(day: date, rows: list[dict]) -> list[dict]:
    start = datetime.combine(day, time(9, 30), EASTERN).astimezone(UTC)
    end = datetime.combine(day, time(16, 0), EASTERN).astimezone(UTC)
    output = [row for row in rows if start <= _bar_stamp(row) < end]
    output.sort(key=_bar_stamp)
    seen: set[datetime] = set()
    for row in output:
        stamp = _bar_stamp(row)
        if stamp in seen:
            raise ValueError("REPLAY_DUPLICATE_BAR")
        seen.add(stamp)
    return output


def _bar_stamp(bar: dict[str, Any]) -> datetime:
    raw = bar.get("t", bar.get("timestamp"))
    if raw is None:
        raise ValueError("REPLAY_BAR_TIMESTAMP_MISSING")
    try:
        stamp = datetime.fromisoformat(str(raw))
    except ValueError as exc:
        raise ValueError("REPLAY_BAR_TIMESTAMP_INVALID") from exc
    return stamp.replace(tzinfo=UTC) if stamp.tzinfo is None else stamp.astimezone(UTC)


def _bar_price(bar: dict[str, Any], short: str, long: str) -> float:
    raw = bar.get(short, bar.get(long))
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("REPLAY_BAR_INCOMPLETE") from exc
    if value <= 0:
        raise ValueError("REPLAY_BAR_INVALID")
    return value


def _add_leg(
    legs: list[dict[str, Any]],
    reason: str,
    price: float | None,
    fraction: float,
    at: datetime,
) -> None:
    assert price is not None
    legs.append(
        {
            "reason": reason,
            "price": round(price, 4),
            "fraction": round(fraction, 6),
            "at": at.isoformat(),
        }
    )

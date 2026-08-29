from __future__ import annotations

from datetime import UTC, datetime, timedelta

from market_brain.domain.models import (
    PositionAction,
    PositionState,
    ProtectionState,
    ReconciliationState,
)


def evaluate_position(
    position: PositionState | None,
    *,
    last: float,
    below_vwap: bool = False,
    failed_breakout: bool = False,
    reconciliation_max_age_hours: float = 24.0,
    now: datetime | None = None,
) -> PositionAction:
    if position is None or position.closed_at is not None or position.remaining_quantity <= 0:
        return PositionAction.UNKNOWN_POSITION
    if position.stop is not None and last <= position.stop:
        return PositionAction.SELL_NOW
    if failed_breakout and below_vwap:
        return PositionAction.SELL_NOW
    if position.protection == ProtectionState.UNPROTECTED:
        return PositionAction.PLACE_STOP_NOW
    if (
        not position.managed
        or position.stop is None
        or position.tp1 is None
        or position.tp2 is None
        or position.time_stop_at is None
    ):
        return PositionAction.RECONCILE_REQUIRED
    timestamp = now or datetime.now(UTC)
    if (
        position.reconciliation_state != ReconciliationState.RECONCILED
        or position.last_reconciled_at is None
        or timestamp - position.last_reconciled_at > timedelta(hours=reconciliation_max_age_hours)
    ):
        return PositionAction.RECONCILE_REQUIRED
    if timestamp >= position.time_stop_at and last < position.average_fill:
        return PositionAction.SELL_NOW
    if last >= position.tp2:
        return PositionAction.TAKE_PROFIT
    if last >= position.tp1:
        return PositionAction.TRIM
    return PositionAction.HOLD


def validate_stop_change(position: PositionState, new_stop: float) -> None:
    if position.stop is None:
        raise ValueError("POSITION_STOP_UNDEFINED")
    if new_stop < position.stop:
        raise ValueError("STOP_WIDENING_FORBIDDEN")
    if position.tp1 is None:
        raise ValueError("POSITION_TARGET_UNDEFINED")
    if new_stop >= position.tp1:
        raise ValueError("STOP_ABOVE_TARGET_INVALID")


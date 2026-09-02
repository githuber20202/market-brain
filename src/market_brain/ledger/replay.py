from __future__ import annotations

from dataclasses import asdict

from market_brain.ledger.events import LedgerEvent
from market_brain.ledger.store import (
    EventStore,
    _plan_from_json,
    _position_from_json,
    _reservation_from_json,
    _shadow_trade_from_json,
    _wallet_from_json,
)

_WALLET_PAYLOAD_PATHS = {
    "WALLET_SEEDED": None,
    "WALLET_RECONCILED": None,
    "POSITION_IMPORTED": "wallet",
    "CAPACITY_RESERVED": "wallet",
    "CAPACITY_RELEASED": "wallet",
    "FILL_CONFIRMED": "wallet",
    "STOP_ORDER_CONFIRMED": "wallet",
    "EXIT_CONFIRMED": "wallet",
}
_WALLET_REQUIRED_FIELDS = frozenset({"capital_base", "cash_available"})


def _wallet_snapshot(event: LedgerEvent) -> dict | None:
    if event.event_type not in _WALLET_PAYLOAD_PATHS:
        return None
    path = _WALLET_PAYLOAD_PATHS[event.event_type]
    if path is not None and path not in event.payload:
        return None
    candidate = event.payload if path is None else event.payload.get(path)
    if not isinstance(candidate, dict) or not _WALLET_REQUIRED_FIELDS.issubset(candidate):
        raise ValueError(f"INVALID_WALLET_SNAPSHOT:{event.event_type}")
    return candidate


def rebuild_state(events: list[LedgerEvent]) -> dict:
    wallet = None
    plans = {}
    reservations = {}
    positions = {}
    shadow_trades = {}
    for event in events:
        payload = event.payload
        wallet_snapshot = _wallet_snapshot(event)
        if wallet_snapshot is not None:
            wallet = _wallet_from_json(wallet_snapshot)
        if event.event_type == "PLAN_ISSUED" and payload.get("plan") is not None:
            plan = _plan_from_json(payload["plan"])
            plans[plan.plan_id] = plan
        if payload.get("plan") is not None:
            plan = _plan_from_json(payload["plan"])
            plans[plan.plan_id] = plan
        if payload.get("reservation") is not None:
            reservation = _reservation_from_json(payload["reservation"])
            reservations[reservation.plan_id] = reservation
        if payload.get("reservation_deleted"):
            plan_payload = payload.get("plan") or {}
            reservation_plan_id = plan_payload.get("plan_id", event.aggregate_id)
            reservations.pop(reservation_plan_id, None)
        if payload.get("position") is not None:
            position = _position_from_json(payload["position"])
            positions[position.position_id] = position
        if event.event_type == "POSITION_IMPORTED":
            raw = payload.get("position", payload)
            position = _position_from_json(raw)
            positions[position.position_id] = position
        if event.event_type == "RECONCILIATION_COMPLETED":
            for raw in payload.get("positions", []):
                position = _position_from_json(raw)
                positions[position.position_id] = position
        if payload.get("shadow_trade") is not None:
            trade = _shadow_trade_from_json(payload["shadow_trade"])
            shadow_trades[trade.plan_id] = trade
    return {
        "wallet": wallet,
        "plans": plans,
        "reservations": reservations,
        "positions": positions,
        "shadow_trades": shadow_trades,
    }


def _same(left, right) -> bool:
    if left is None or right is None:
        return left is right
    return asdict(left) == asdict(right)


async def replay_check(store: EventStore) -> list[str]:
    rebuilt = rebuild_state(await store.read_events())
    differences: list[str] = []
    actual_wallet = await store.get_wallet()
    if not _same(rebuilt["wallet"], actual_wallet):
        differences.append("wallet")

    actual_plans = {row.plan_id: row for row in await store.list_plans()}
    for key in sorted(set(rebuilt["plans"]) | set(actual_plans)):
        if not _same(rebuilt["plans"].get(key), actual_plans.get(key)):
            differences.append(f"plan:{key}")

    actual_reservations = {row.plan_id: row for row in await store.list_reservations()}
    for key in sorted(set(rebuilt["reservations"]) | set(actual_reservations)):
        if not _same(rebuilt["reservations"].get(key), actual_reservations.get(key)):
            differences.append(f"reservation:{key}")

    actual_positions = {row.position_id: row for row in await store.list_positions()}
    for key in sorted(set(rebuilt["positions"]) | set(actual_positions)):
        if not _same(rebuilt["positions"].get(key), actual_positions.get(key)):
            differences.append(f"position:{key}")

    actual_shadow = {row.plan_id: row for row in await store.list_shadow_trades()}
    for key in sorted(set(rebuilt["shadow_trades"]) | set(actual_shadow)):
        if not _same(rebuilt["shadow_trades"].get(key), actual_shadow.get(key)):
            differences.append(f"shadow_trade:{key}")
    return differences

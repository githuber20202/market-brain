from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from market_brain.orchestration.universe import EASTERN

LEASE_MINUTES = 25


def market_session_id(now: datetime) -> str:
    return _aware(now).astimezone(EASTERN).date().isoformat()


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return _aware(datetime.fromisoformat(value))
    except ValueError:
        return None


def lease_held_by_other(
    lease: dict[str, Any] | None,
    *,
    now: datetime,
    session_id: str,
    workflow_run_id: str,
) -> bool:
    if not lease or str(lease.get("session_id")) != session_id:
        return False
    expires_at = parse_timestamp(lease.get("expires_at"))
    if expires_at is None or expires_at <= _aware(now):
        return False
    return str(lease.get("workflow_run_id")) != workflow_run_id


def renew_lease(
    state_dir: Path,
    *,
    now: datetime,
    session_id: str,
    workflow_run_id: str,
) -> dict[str, str]:
    timestamp = _aware(now)
    path = state_dir / "lease.json"
    current = read_json(path)
    acquired_at = timestamp
    if (
        current
        and str(current.get("session_id")) == session_id
        and str(current.get("workflow_run_id")) == workflow_run_id
    ):
        acquired_at = parse_timestamp(current.get("acquired_at")) or timestamp
    payload = {
        "session_id": session_id,
        "workflow_run_id": workflow_run_id,
        "acquired_at": acquired_at.isoformat(),
        "expires_at": (timestamp + timedelta(minutes=LEASE_MINUTES)).isoformat(),
    }
    write_json(path, payload)
    return payload


def write_heartbeat(
    state_dir: Path,
    *,
    now: datetime,
    session_id: str,
    workflow_run_id: str,
    head_sha: str,
    phase: str,
    last_scheduled_tick: str | None,
    last_completed_tick: str | None,
    next_due_tick: str | None,
    lease_expires_at: str,
    policy_version: str,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "session_id": session_id,
        "workflow_run_id": workflow_run_id,
        "head_sha": head_sha,
        "phase": phase,
        "last_scheduled_tick": last_scheduled_tick,
        "last_completed_tick": last_completed_tick,
        "next_due_tick": next_due_tick,
        "lease_expires_at": lease_expires_at,
        "policy_version": policy_version,
        "as_of": _aware(now).isoformat(),
    }
    write_json(state_dir / "heartbeat.json", payload)
    return payload


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_handoff(
    state_dir: Path,
    *,
    session_id: str,
    workflow_run_id: str,
    last_completed_tick: str | None,
) -> dict[str, Any]:
    dump_path = state_dir / "market.dump"
    if not dump_path.exists():
        raise RuntimeError("HANDOFF_DUMP_MISSING")
    payload: dict[str, Any] = {
        "from_phase": "a",
        "to_phase": "b",
        "session_id": session_id,
        "workflow_run_id": workflow_run_id,
        "last_completed_tick": last_completed_tick,
        "state_dump_sha256": file_sha256(dump_path),
    }
    write_json(state_dir / "handoff.json", payload)
    return payload


def verify_handoff(state_dir: Path, *, session_id: str) -> dict[str, Any] | None:
    handoff = read_json(state_dir / "handoff.json")
    if handoff is None or str(handoff.get("session_id")) != session_id:
        return None
    required = {
        "from_phase",
        "to_phase",
        "session_id",
        "workflow_run_id",
        "last_completed_tick",
        "state_dump_sha256",
    }
    if set(handoff) != required or handoff.get("from_phase") != "a" or handoff.get(
        "to_phase"
    ) != "b":
        raise RuntimeError("HANDOFF_MISMATCH")
    dump_path = state_dir / "market.dump"
    if not dump_path.exists() or file_sha256(dump_path) != handoff.get("state_dump_sha256"):
        raise RuntimeError("HANDOFF_MISMATCH")
    return handoff


def load_policy_version(repo: Path) -> str:
    payload = read_json(repo / "config" / "POLICY_RELEASE.json")
    value = payload.get("policy_version") if payload else None
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError("POLICY_VERSION_MISSING")
    return value.strip()


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

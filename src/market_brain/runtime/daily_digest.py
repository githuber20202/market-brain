from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

from market_brain.domain.models import AlertRecord
from market_brain.ledger.events import LedgerEvent
from market_brain.ledger.replay import replay_check
from market_brain.orchestration.universe import EASTERN
from market_brain.runtime.shadow import shadow_metrics


class DailyDigest:
    def __init__(self, store) -> None:
        self.store = store

    async def create(
        self,
        *,
        now: datetime | None = None,
        run_id: str | None = None,
    ) -> AlertRecord | None:
        timestamp = _aware(now or datetime.now(UTC))
        session_date = timestamp.astimezone(EASTERN).date()
        digest_run_id = run_id or f"daily_digest:{session_date.isoformat()}"
        status_key = f"daily_digest_run:{session_date.isoformat()}"
        existing = await self.store.get_runtime_status_key(status_key)
        if isinstance(existing, dict) and existing.get("status") == "COMPLETED":
            return None

        all_events = await self.store.read_events()
        events = [
            event
            for event in all_events
            if _event_date(event.occurred_at) == session_date
        ]
        delivered_ids = {
            event.aggregate_id for event in events if event.event_type == "ALERT_DELIVERED"
        }
        failed_ids = {
            event.aggregate_id
            for event in events
            if event.event_type == "ALERT_DELIVERY_FAILED"
        }
        signals_created = sum(event.event_type == "BUY_NOW_EMITTED" for event in events)
        positions = [
            position
            for position in await self.store.list_positions()
            if position.closed_at is None and position.remaining_quantity > 0
        ]
        positions.sort(key=lambda position: (position.symbol, position.position_id))
        runtime = await self.store.get_runtime_status()
        stream = _stream_status(runtime, timestamp)
        differences = await replay_check(self.store)
        shadow_trades = await self.store.list_shadow_trades()
        shadow_today = shadow_metrics(shadow_trades, all_events, session_date=session_date)
        shadow_cumulative = shadow_metrics(shadow_trades, all_events)
        open_positions = [
            {
                "symbol": position.symbol,
                "remaining_quantity": position.remaining_quantity,
                "protection": str(position.protection),
                "stop": position.stop,
                "reconciliation_state": str(position.reconciliation_state),
            }
            for position in positions
        ]
        payload: dict[str, Any] = {
            "run_id": digest_run_id,
            "session_date": session_date.isoformat(),
            "as_of": timestamp.isoformat(),
            "stream": stream,
            "signals_created": signals_created,
            "alerts_delivered": len(delivered_ids),
            "alerts_failed": len(failed_ids),
            "open_positions": open_positions,
            "replay_check": {"ok": not differences, "differences": differences},
            "shadow": {
                "today": shadow_today,
                "cumulative": shadow_cumulative,
            },
            "reconcile_reminder": "RECONCILE_BROKER_HOLDINGS_WITH_POSITION_TWIN",
        }
        payload["text"] = _format_text(payload)
        alert = AlertRecord(kind="DAILY_DIGEST", payload=payload, created_at=timestamp)
        async with self.store.transaction():
            current = await self.store.get_runtime_status_key(status_key)
            if isinstance(current, dict) and current.get("status") == "COMPLETED":
                return None
            await self.store.save_alert(alert)
            await self.store.append(
                LedgerEvent(
                    "DAILY_DIGEST_CREATED",
                    digest_run_id,
                    {"alert_id": alert.alert_id, **payload},
                    occurred_at=timestamp,
                )
            )
            await self.store.set_runtime_status(
                status_key,
                {
                    "status": "COMPLETED",
                    "run_id": digest_run_id,
                    "alert_id": alert.alert_id,
                    "created_at": timestamp.isoformat(),
                },
            )
        return alert


def _stream_status(runtime: dict, timestamp: datetime) -> dict[str, Any]:
    connected = runtime.get("stream_connected") is True
    connected_since = _runtime_datetime(runtime.get("stream_connected_since"))
    last_message_at = _runtime_datetime(runtime.get("stream_last_message_at"))
    uptime_seconds = None
    if connected and connected_since is not None and connected_since <= timestamp:
        uptime_seconds = int((timestamp - connected_since).total_seconds())
    return {
        "connected": connected,
        "connected_since": connected_since.isoformat() if connected_since else None,
        "last_message_at": last_message_at.isoformat() if last_message_at else None,
        "uptime_seconds": uptime_seconds,
    }


def _format_text(payload: dict[str, Any]) -> str:
    stream = payload["stream"]
    stream_state = "CONNECTED" if stream["connected"] else "DISCONNECTED"
    uptime = _duration(stream["uptime_seconds"])
    replay = "PASS" if payload["replay_check"]["ok"] else "FAIL"
    today = payload["shadow"]["today"]
    cumulative = payload["shadow"]["cumulative"]
    position_lines = [
        (
            f"- {row['symbol']} qty={row['remaining_quantity']} "
            f"protection={row['protection']} reconcile={row['reconciliation_state']}"
        )
        for row in payload["open_positions"]
    ]
    if not position_lines:
        position_lines = ["- none"]
    setup_lines = [
        (
            f"- {setup}: trades={row['trades']} hit_rate={row['hit_rate']:.2%} "
            f"expectancy={row['expectancy_r']:.3f}R"
        )
        for setup, row in today["by_setup"].items()
    ]
    if not setup_lines:
        setup_lines = ["- none"]
    return "\n".join(
        [
            f"Market Brain daily digest — {payload['session_date']} ET",
            f"Stream: {stream_state} | uptime={uptime}",
            f"Signals created: {payload['signals_created']}",
            (
                f"Alerts: delivered={payload['alerts_delivered']} "
                f"failed={payload['alerts_failed']}"
            ),
            f"Open positions: {len(payload['open_positions'])}",
            *position_lines,
            f"Replay check: {replay}",
            (
                "Shadow today: "
                f"signals={today['signals']} trades={today['trades']} "
                f"unfinalized={today['unfinalized']} no_trigger={today['no_trigger']} "
                f"hit_rate={today['hit_rate']:.2%} "
                f"expectancy={today['expectancy_r']:.3f}R max_dd={today['max_drawdown_r']:.3f}R"
            ),
            (
                "Shadow cumulative: "
                f"trades={cumulative['trades']} unfinalized={cumulative['unfinalized']} "
                f"hit_rate={cumulative['hit_rate']:.2%} "
                f"expectancy={cumulative['expectancy_r']:.3f}R "
                f"max_dd={cumulative['max_drawdown_r']:.3f}R"
            ),
            "Shadow by setup:",
            *setup_lines,
            "Reminder: reconcile broker holdings with the Position Twin.",
        ]
    )


def _duration(seconds: int | None) -> str:
    if seconds is None:
        return "UNKNOWN"
    hours, remainder = divmod(max(0, seconds), 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _runtime_datetime(value) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return _aware(datetime.fromisoformat(value))
    except ValueError:
        return None


def _event_date(value: datetime) -> date:
    return _aware(value).astimezone(EASTERN).date()


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

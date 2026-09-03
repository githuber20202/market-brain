from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

from market_brain.domain.models import AlertRecord
from market_brain.ledger.events import LedgerEvent
from market_brain.ledger.replay import replay_check
from market_brain.orchestration.universe import EASTERN
from market_brain.runtime.coverage import coverage_for_events, coverage_line
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
        wallet_status = runtime.get("shadow_wallet")
        wallet_mode = (
            "virtual"
            if isinstance(wallet_status, dict)
            and wallet_status.get("source") == "SHADOW_VIRTUAL"
            else "unseeded"
        )
        quality_status = runtime.get("quality_state")
        quality = (
            quality_status
            if isinstance(quality_status, dict)
            else {"status": "QUALITY_MISSING", "rows": 0}
        )
        stream = _stream_status(runtime, timestamp)
        differences = await replay_check(self.store)
        shadow_trades = await self.store.list_shadow_trades()
        shadow_today = shadow_metrics(shadow_trades, all_events, session_date=session_date)
        shadow_cumulative = shadow_metrics(shadow_trades, all_events)
        data_availability = _data_availability(events)
        session_coverage = coverage_for_events(events, session_date)
        plan_rejections = _plan_rejections(events)
        score_histogram = _score_histogram(events)
        shadow_entries = _shadow_entries(events)
        premarket = _premarket_learning(events)
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
            "data_availability": data_availability,
            "session_coverage": session_coverage,
            "workflow_status": "COMPLETED",
            "session_status": session_coverage["session_status"],
            "learning_status": session_coverage["learning_status"],
            "plan_rejections": plan_rejections,
            "score_histogram": score_histogram,
            "shadow_entries": shadow_entries,
            "premarket": premarket,
            "wallet": wallet_mode,
            "quality": quality,
            "reconcile_reminder": "RECONCILE_BROKER_HOLDINGS_WITH_POSITION_TWIN",
        }
        payload["text"] = _format_text(payload)
        alert = AlertRecord(kind="DAILY_DIGEST", payload=payload, created_at=timestamp)
        async with self.store.transaction():
            current = await self.store.get_runtime_status_key(status_key)
            if isinstance(current, dict) and current.get("status") == "COMPLETED":
                return None
            await self.store.save_alert(alert)
            if session_coverage["session_status"] != "COMPLETE":
                await self.store.append(
                    LedgerEvent(
                        "SESSION_INCOMPLETE",
                        f"session:{session_date.isoformat()}",
                        {
                            "session_date": session_date.isoformat(),
                            "workflow_status": "COMPLETED",
                            "session_status": session_coverage["session_status"],
                            "learning_status": session_coverage["learning_status"],
                            "incomplete_slots": session_coverage["incomplete_slots"],
                            "coverage": session_coverage,
                        },
                        occurred_at=timestamp,
                    )
                )
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
    rejection_lines = [
        f"- {reason}: count={count}"
        for reason, count in payload["plan_rejections"].items()
    ]
    if not rejection_lines:
        rejection_lines = ["- none"]
    entry_lines = [
        (
            f"- {row['symbol']}: virtual_entry={row['virtual_entry']:.4f} "
            f"current_price={row['current_price']:.4f} "
            f"gap={row['price_gap_pct']:+.3f}%"
        )
        for row in payload["shadow_entries"]
    ]
    if not entry_lines:
        entry_lines = ["- none"]
    premarket = payload["premarket"]
    outcome = premarket["outcome_review"]
    checkpoint_lines = [
        (
            f"- {checkpoint}: status={row['status']} "
            f"audit={row['audit_rows']}/{row['required']} "
            f"finalists={','.join(row['finalists']) or 'none'}"
        )
        for checkpoint, row in premarket["checkpoints"].items()
    ]
    if not checkpoint_lines:
        checkpoint_lines = ["- none"]
    lines = [
            f"Market Brain daily digest — {payload['session_date']} ET",
            (
                "Statuses: "
                f"workflow_status={payload['workflow_status']} "
                f"session_status={payload['session_status']} "
                f"learning_status={payload['learning_status']}"
            ),
            coverage_line(payload["session_coverage"]),
            f"Stream: {stream_state} | uptime={uptime}",
            f"Signals created: {payload['signals_created']}",
            (
                f"Alerts: delivered={payload['alerts_delivered']} "
                f"failed={payload['alerts_failed']}"
            ),
            f"Open positions: {len(payload['open_positions'])}",
            *position_lines,
            f"Replay check: {replay}",
            f"Wallet: {payload['wallet']}",
            (
                f"Quality: {payload['quality'].get('status')} "
                f"rows={payload['quality'].get('rows', 0)}"
            ),
            (
                "Data availability: "
                f"slots_ok={payload['data_availability']['slots_ok']} "
                f"slots_unavailable={payload['data_availability']['slots_unavailable']} "
                f"slots_missed={payload['data_availability']['slots_missed']}"
            ),
            "Plan rejections:",
            *rejection_lines,
            (
                "Score histogram: "
                f"0-20={payload['score_histogram']['0-20']} "
                f"20-40={payload['score_histogram']['20-40']} "
                f"40-65={payload['score_histogram']['40-65']} "
                f"65+={payload['score_histogram']['65+']}"
            ),
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
            "Shadow delayed entries:",
            *entry_lines,
            (
                "Premarket learning: "
                f"state={premarket['evaluation_state']} "
                f"final_checkpoint={premarket['final_checkpoint'] or 'none'} "
                f"predictions={','.join(premarket['final_predictions']) or 'none'} "
                f"radar_seen={','.join(premarket['radar_seen']) or 'none'} "
                f"confirmed={','.join(premarket['confirmed_after_open']) or 'none'}"
            ),
            (
                "Premarket outcomes: "
                f"state={outcome['data_state']} "
                f"complete={outcome['complete_records']}/{outcome['records']} "
                f"finalist_avg_mfe={_optional_percent(outcome['finalist_average_mfe_percent'])} "
                f"finalist_avg_mae={_optional_percent(outcome['finalist_average_mae_percent'])} "
                f"finalist_avg_eod={_optional_percent(outcome['finalist_average_eod_return_percent'])}"
            ),
            "Premarket checkpoints:",
            *checkpoint_lines,
            "Reminder: reconcile broker holdings with the Position Twin.",
        ]
    if payload["session_status"] == "NEVER_RAN":
        lines.insert(0, "NEVER_RAN")
    return "\n".join(lines)


def _duration(seconds: int | None) -> str:
    if seconds is None:
        return "UNKNOWN"
    hours, remainder = divmod(max(0, seconds), 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _data_availability(events) -> dict[str, int]:
    latest: dict[str, dict] = {}
    for event in events:
        if event.event_type == "RADAR_RUN":
            latest[event.aggregate_id] = event.payload
    return {
        "slots_ok": sum(
            row.get("status") == "COMPLETED" for row in latest.values()
        ),
        "slots_unavailable": sum(
            row.get("status") == "DATA_UNAVAILABLE"
            or (
                row.get("status") == "MISSED"
                and row.get("previous_status") == "DATA_UNAVAILABLE"
            )
            for row in latest.values()
        ),
        "slots_missed": sum(
            row.get("status") == "MISSED" for row in latest.values()
        ),
    }


def _plan_rejections(events) -> dict[str, int]:
    latest: dict[str, dict] = {}
    for event in events:
        if event.event_type == "RADAR_RUN":
            latest[event.aggregate_id] = event.payload
    counts: dict[str, int] = {}
    for payload in latest.values():
        for candidate in payload.get("candidates", []):
            reason = candidate.get("reason")
            if isinstance(reason, str) and reason:
                counts[reason] = counts.get(reason, 0) + 1
    return dict(sorted(counts.items()))


def _shadow_entries(events) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for event in events:
        if event.event_type != "BUY_NOW_EMITTED":
            continue
        context = event.payload.get("activation_context")
        if not isinstance(context, dict) or context.get("activation_basis") != "RETEST_BAR":
            continue
        try:
            output.append(
                {
                    "symbol": str(event.payload["decision"]["symbol"]),
                    "virtual_entry": float(context["virtual_entry"]),
                    "current_price": float(context["current_price"]),
                    "price_gap_pct": float(context["price_gap_pct"]),
                    "retest_bar_ts": str(context["retest_bar_ts"]),
                    "detected_at": str(context["detected_at"]),
                }
            )
        except (KeyError, TypeError, ValueError):
            continue
    return output


def _score_histogram(events) -> dict[str, int]:
    latest: dict[str, dict] = {}
    for event in events:
        if event.event_type == "RADAR_RUN":
            latest[event.aggregate_id] = event.payload
    output = {"0-20": 0, "20-40": 0, "40-65": 0, "65+": 0}
    for payload in latest.values():
        histogram = payload.get("score_histogram", {})
        for bucket in output:
            try:
                output[bucket] += int(histogram.get(bucket, 0))
            except (TypeError, ValueError):
                continue
    return output


def _premarket_learning(events) -> dict[str, Any]:
    order = {"T-30": 0, "T-12": 1, "T-3": 2}
    checkpoints: dict[str, dict[str, Any]] = {}
    radar_symbols: set[str] = set()
    confirmed_symbols: set[str] = set()
    radar_runs = 0
    outcome_review: dict[str, Any] | None = None
    for event in events:
        if event.event_type == "PREMARKET_RUN":
            checkpoint = str(event.payload.get("checkpoint") or "")
            if checkpoint not in order:
                continue
            coverage = event.payload.get("coverage")
            coverage = coverage if isinstance(coverage, dict) else {}
            checkpoints[checkpoint] = {
                "status": str(event.payload.get("status") or "UNKNOWN"),
                "audit_rows": _safe_int(coverage.get("audit_rows")),
                "required": _safe_int(coverage.get("required")),
                "top10": _symbols_from(event.payload.get("top10")),
                "finalists": _symbols_from(event.payload.get("finalists"))[:2],
                "delta_state": str(
                    event.payload.get("delta_state") or "DELTA_UNAVAILABLE"
                ),
            }
        elif event.event_type == "RADAR_RUN":
            radar_runs += 1
            for candidate in event.payload.get("candidates", []):
                if isinstance(candidate, dict):
                    symbol = _symbol(candidate.get("symbol"))
                    if symbol:
                        radar_symbols.add(symbol)
        elif event.event_type == "BUY_NOW_EMITTED":
            decision = event.payload.get("decision")
            if isinstance(decision, dict):
                symbol = _symbol(decision.get("symbol"))
                if symbol:
                    confirmed_symbols.add(symbol)
        elif event.event_type == "PREMARKET_LEARNING_REVIEW":
            summary = event.payload.get("summary")
            summary = summary if isinstance(summary, dict) else {}
            outcome_review = {
                "data_state": str(
                    event.payload.get("data_state") or "LEARNING_DATA_INCOMPLETE"
                ),
                "records": _safe_int(summary.get("records")),
                "complete_records": _safe_int(summary.get("complete_records")),
                "finalist_records": _safe_int(summary.get("finalist_records")),
                "finalist_average_mfe_percent": _safe_float(
                    summary.get("finalist_average_mfe_percent")
                ),
                "finalist_average_mae_percent": _safe_float(
                    summary.get("finalist_average_mae_percent")
                ),
                "finalist_average_eod_return_percent": _safe_float(
                    summary.get("finalist_average_eod_return_percent")
                ),
            }

    sorted_checkpoints = dict(
        sorted(checkpoints.items(), key=lambda item: order[item[0]])
    )
    final_checkpoint = max(checkpoints, key=order.get) if checkpoints else None
    final_predictions = (
        list(checkpoints[final_checkpoint]["finalists"])
        if final_checkpoint is not None
        else []
    )
    predicted = set(final_predictions)
    if not checkpoints:
        evaluation_state = "NO_PREMARKET_DATA"
    elif not final_predictions:
        evaluation_state = "NO_FINAL_PREDICTIONS"
    elif radar_runs == 0:
        evaluation_state = "POSTOPEN_DATA_MISSING"
    else:
        evaluation_state = "MEASURED"
    return {
        "evaluation_state": evaluation_state,
        "final_checkpoint": final_checkpoint,
        "final_predictions": final_predictions,
        "radar_seen": sorted(predicted & radar_symbols),
        "confirmed_after_open": sorted(predicted & confirmed_symbols),
        "not_confirmed_after_open": sorted(predicted - confirmed_symbols),
        "checkpoints": sorted_checkpoints,
        "outcome_review": outcome_review
        or {
            "data_state": "LEARNING_DATA_INCOMPLETE",
            "records": 0,
            "complete_records": 0,
            "finalist_records": 0,
            "finalist_average_mfe_percent": None,
            "finalist_average_mae_percent": None,
            "finalist_average_eod_return_percent": None,
        },
    }


def _symbols_from(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    output: list[str] = []
    for item in value:
        symbol = _symbol(item)
        if symbol and symbol not in output:
            output.append(symbol)
    return output


def _symbol(value: Any) -> str | None:
    symbol = str(value or "").strip().upper()
    return symbol or None


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_percent(value: Any) -> str:
    number = _safe_float(value)
    return f"{number:+.3f}%" if number is not None else "N/A"


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

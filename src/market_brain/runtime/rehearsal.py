from __future__ import annotations

import json
import time as monotonic_time
from collections import Counter
from datetime import UTC, date, datetime, time
from typing import Any

from market_brain.orchestration.universe import EASTERN
from market_brain.runtime.radar_scheduler import (
    DISCOVERY_END,
    DISCOVERY_INTERVAL,
    DISCOVERY_START,
    scheduled_slots,
)
from market_brain.runtime.state import activate_quality_from_state
from market_brain.settings import ROOT


class MutableClock:
    def __init__(self, value: datetime) -> None:
        self.value = _aware(value)

    def set(self, value: datetime) -> None:
        self.value = _aware(value)

    def now(self) -> datetime:
        return self.value


class RehearsalConsoleSink:
    name = "rehearsal_console"
    configured = True

    async def send(self, payload: dict) -> bool:
        print(
            "REHEARSAL_ALERT="
            + json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False)
        )
        return True


def rehearsal_ticks(session_date: date) -> tuple[datetime, ...]:
    current = datetime.combine(session_date, DISCOVERY_START, EASTERN)
    final = datetime.combine(session_date, DISCOVERY_END, EASTERN)
    output: list[datetime] = []
    while current <= final:
        output.append(current)
        current += DISCOVERY_INTERVAL
    return tuple(output)


async def run_rehearsal(
    runtime,
    provider,
    *,
    session_date: date,
    clock: MutableClock,
    publish_issue: bool = False,
) -> dict[str, Any]:
    first_tick = rehearsal_ticks(session_date)[0]
    quality_status = await activate_quality_from_state(
        ROOT,
        runtime.cfg.quality_path,
        runtime.store,
        now=datetime.now(UTC),
    )
    if quality_status.get("status") != "READY":
        raise RuntimeError(str(quality_status.get("status", "QUALITY_STATE_INVALID")))
    runtime.scheduler.validate_startup(now=first_tick)
    assert runtime.scheduler.calendar is not None
    session = runtime.scheduler.calendar.session_for(session_date)
    if session is None:
        raise RuntimeError("REHEARSAL_NO_SESSION")
    if tuple(scheduled_slots(session)) != rehearsal_ticks(session_date):
        raise RuntimeError("REHEARSAL_DISCOVERY_SLOTS_INVALID")

    tick_reports: list[dict[str, Any]] = []
    exceptions = 0
    for tick in rehearsal_ticks(session_date):
        clock.set(tick)
        requests_before = provider.request_count
        started = monotonic_time.monotonic()
        try:
            result = await runtime.run("radar", now=tick.astimezone(UTC))
        except Exception as exc:
            exceptions += 1
            print(
                "REHEARSAL_EXCEPTION="
                + json.dumps(
                    {"tick": tick.isoformat(), "error_type": type(exc).__name__},
                    sort_keys=True,
                )
            )
            raise
        duration = monotonic_time.monotonic() - started
        discovery = [_discovery_report(row) for row in result.get("runs", [])]
        report = {
            "tick": tick.isoformat(),
            "duration_seconds": round(duration, 3),
            "http_requests": provider.request_count - requests_before,
            "http_requests_total": provider.request_count,
            "discovery": discovery,
            "plan_watch": result.get("plan_watch", {}),
            "shadow": await _shadow_rows(runtime.store),
            "expired": result.get("expired", {}),
        }
        tick_reports.append(report)
        print(
            "REHEARSAL_SLOT="
            + json.dumps(report, sort_keys=True, default=str, ensure_ascii=False)
        )

    digest_tick = datetime.combine(session_date, time(16, 20), EASTERN)
    clock.set(digest_tick)
    requests_before = provider.request_count
    started = monotonic_time.monotonic()
    try:
        digest_result = await runtime.run("digest", now=digest_tick.astimezone(UTC))
    except Exception as exc:
        exceptions += 1
        print(
            "REHEARSAL_EXCEPTION="
            + json.dumps(
                {"tick": digest_tick.isoformat(), "error_type": type(exc).__name__},
                sort_keys=True,
            )
        )
        raise
    digest_duration = monotonic_time.monotonic() - started
    alerts = await runtime.store.list_alerts()
    digest_alert = next(
        (
            row
            for row in reversed(alerts)
            if row.kind == "DAILY_DIGEST"
            and row.payload.get("session_date") == session_date.isoformat()
        ),
        None,
    )
    if digest_alert is None:
        raise RuntimeError("REHEARSAL_DIGEST_MISSING")
    digest_text = str(digest_alert.payload.get("text", ""))
    print("REHEARSAL_DIGEST_TEXT_BEGIN")
    print(digest_text)
    print("REHEARSAL_DIGEST_TEXT_END")
    print(
        "REHEARSAL_DIGEST="
        + json.dumps(
            {
                **digest_result,
                "duration_seconds": round(digest_duration, 3),
                "http_requests": provider.request_count - requests_before,
                "http_requests_total": provider.request_count,
            },
            sort_keys=True,
            default=str,
        )
    )

    runtime_status = await runtime.store.get_runtime_status()
    discovery_states = {
        slot.isoformat(): runtime_status.get(
            f"radar_run:radar:{session_date.isoformat()}:{slot.strftime('%H%M')}"
        )
        for slot in scheduled_slots(session)
    }
    incomplete = {
        slot: value
        for slot, value in discovery_states.items()
        if not isinstance(value, dict)
        or value.get("status") not in {"COMPLETED", "DATA_UNAVAILABLE"}
    }
    if incomplete:
        print(
            "REHEARSAL_INCOMPLETE_SLOTS="
            + json.dumps(incomplete, sort_keys=True, default=str)
        )
        raise RuntimeError("REHEARSAL_SLOT_INCOMPLETE")

    events = await runtime.store.read_events()
    radar_events = [row for row in events if row.event_type == "RADAR_RUN"]
    rejection_counts: Counter[str] = Counter()
    score_histogram: Counter[str] = Counter()
    skipped_symbols = 0
    for event in radar_events:
        skipped_symbols += len(event.payload.get("skipped_symbols", []))
        score_histogram.update(event.payload.get("score_histogram", {}))
        for candidate in event.payload.get("candidates", []):
            reason = candidate.get("reason")
            if isinstance(reason, str) and reason:
                rejection_counts[reason] += 1
    trades = await _shadow_rows(runtime.store)
    summary = {
        "session": session_date.isoformat(),
        "status": "CLEAN",
        "ticks": len(tick_reports),
        "discovery_slots": len(radar_events),
        "data_unavailable_slots": sum(
            row.payload.get("status") == "DATA_UNAVAILABLE" for row in radar_events
        ),
        "plans": len(await runtime.store.list_plans()),
        "plan_rejections": dict(sorted(rejection_counts.items())),
        "score_histogram": {
            bucket: score_histogram[bucket]
            for bucket in ("0-20", "20-40", "40-65", "65+")
        },
        "skipped_symbols": skipped_symbols,
        "trigger_hits": sum(row.event_type == "TRIGGER_HIT" for row in events),
        "retest_valid": sum(
            int(report.get("plan_watch", {}).get("retest_valid", 0))
            for report in tick_reports
        ),
        "buy_now": sum(row.event_type == "BUY_NOW_EMITTED" for row in events),
        "activation_rejected": sum(
            row.event_type == "ACTIVATION_REJECTED" for row in events
        ),
        "shadow_trades": trades,
        "http_requests": provider.request_count,
        "exceptions": exceptions,
        "digest": digest_result,
        "quality": quality_status,
    }
    print(
        "REHEARSAL_SUMMARY="
        + json.dumps(summary, sort_keys=True, default=str, ensure_ascii=False)
    )
    print(f"REHEARSAL_EXCEPTIONS={exceptions}")

    if publish_issue:
        issue_text = _issue_summary(summary)
        number = await runtime.issue_sink.send_rehearsal_summary(
            session_date.isoformat(), issue_text
        )
        summary["issue_number"] = number
        print(f"REHEARSAL_ISSUE=PASS number={number} state=closed comments=1")
    return summary


def _discovery_report(payload: dict) -> dict:
    candidates = payload.get("candidates", [])
    rejection_counts: Counter[str] = Counter(
        str(row["reason"])
        for row in candidates
        if isinstance(row.get("reason"), str) and row.get("reason")
    )
    return {
        "scheduled_for": payload.get("scheduled_for"),
        "status": payload.get("status"),
        "candidates": candidates,
        "plans_created": len(payload.get("plan_ids", [])),
        "plan_rejections": dict(sorted(rejection_counts.items())),
        "score_histogram": payload.get("score_histogram", {}),
        "skipped_symbols": payload.get("skipped_symbols", []),
        "error_type": payload.get("error_type"),
    }


async def _shadow_rows(store) -> list[dict]:
    return [
        {
            "trade_id": row.trade_id,
            "plan_id": row.plan_id,
            "symbol": row.symbol,
            "status": str(row.status),
            "virtual_entry": row.fill,
            "realized_r": row.realized_r,
            "opened_at": row.opened_at.isoformat(),
            "closed_at": row.closed_at.isoformat() if row.closed_at else None,
            "exit_legs": row.exit_legs,
        }
        for row in await store.list_shadow_trades()
    ]


def _issue_summary(summary: dict[str, Any]) -> str:
    lines = [
        f"Shadow rehearsal {summary['session']}: {summary['status']}",
        (
            f"ticks={summary['ticks']} discovery_slots={summary['discovery_slots']} "
            f"data_unavailable={summary['data_unavailable_slots']}"
        ),
        (
            f"plans={summary['plans']} trigger_hits={summary['trigger_hits']} "
            f"retest_valid={summary['retest_valid']} buy_now={summary['buy_now']} "
            f"activation_rejected={summary['activation_rejected']}"
        ),
        f"skipped_symbols={summary['skipped_symbols']} http_requests={summary['http_requests']}",
        (
            f"quality={summary['quality']['status']} "
            f"rows={summary['quality']['rows']} as_of={summary['quality'].get('as_of')}"
        ),
        "plan_rejections="
        + json.dumps(summary["plan_rejections"], sort_keys=True, ensure_ascii=False),
        "score_histogram="
        + json.dumps(summary["score_histogram"], sort_keys=True, ensure_ascii=False),
        "shadow_trades="
        + json.dumps(summary["shadow_trades"], sort_keys=True, default=str, ensure_ascii=False),
        "Measurement only; not advice or execution.",
    ]
    return "\n".join(lines)


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from market_brain.ledger.events import LedgerEvent
from market_brain.orchestration.universe import EASTERN, load_market_calendar
from market_brain.providers.base import DataUnavailable

PREMARKET_STAGES = ("T-30", "T-12", "T-3")
FORWARD_MINUTES = (5, 15, 30, 60)


class PremarketLearningReviewer:
    def __init__(
        self,
        *,
        store,
        provider,
        calendar_path: Path,
        state_dir: Path,
    ) -> None:
        self.store = store
        self.provider = provider
        self.calendar_path = calendar_path
        self.state_dir = state_dir

    async def review(self, *, now: datetime) -> dict[str, Any]:
        timestamp = _aware(now)
        session_date = timestamp.astimezone(EASTERN).date()
        status_key = f"premarket_learning_review:{session_date.isoformat()}"
        existing = await self.store.get_runtime_status_key(status_key)
        if isinstance(existing, dict) and existing.get("persisted") is True:
            return {
                "status": "ALREADY_COMPLETED",
                "data_state": existing.get("data_state"),
                "records": existing.get("records", 0),
                "complete_records": existing.get("complete_records", 0),
            }

        calendar = load_market_calendar(
            self.calendar_path,
            required_years={session_date.year, session_date.year + 1},
        )
        session = calendar.session_for(session_date)
        if session is None:
            return {
                "status": "NO_SESSION",
                "data_state": "LEARNING_DATA_INCOMPLETE",
                "records": 0,
                "complete_records": 0,
            }

        events = [
            event
            for event in await self.store.read_events()
            if event.event_type == "PREMARKET_RUN"
            and _aware(event.occurred_at).astimezone(EASTERN).date() == session_date
        ]
        by_stage = {
            str(event.payload.get("checkpoint")): event
            for event in events
            if str(event.payload.get("checkpoint")) in PREMARKET_STAGES
        }
        appearances: list[dict[str, Any]] = []
        for stage in PREMARKET_STAGES:
            event = by_stage.get(stage)
            if event is None:
                continue
            top_rows = event.payload.get("top10_rows")
            for row in top_rows if isinstance(top_rows, list) else []:
                if not isinstance(row, dict):
                    continue
                symbol = str(row.get("symbol") or "").strip().upper()
                metrics = row.get("metrics")
                metrics = metrics if isinstance(metrics, dict) else {}
                reasons = row.get("reason_codes")
                if not symbol:
                    continue
                appearances.append(
                    {
                        "symbol": symbol,
                        "checkpoint": stage,
                        "as_of": str(event.payload.get("as_of") or event.occurred_at.isoformat()),
                        "reference_price": _number(metrics.get("price")),
                        "score": _number(row.get("score")),
                        "status": row.get("status"),
                        "finalist_eligible": row.get("finalist_eligible") is True,
                        "decision_reasons": (
                            [str(value) for value in reasons]
                            if isinstance(reasons, list)
                            else []
                        ),
                    }
                )

        bars_by_symbol: dict[str, list[dict]] = {}
        bar_errors: dict[str, str] = {}
        for symbol in sorted({row["symbol"] for row in appearances}):
            try:
                bars_by_symbol[symbol] = await self._bars(symbol)
            except (DataUnavailable, OSError, RuntimeError, TypeError, ValueError) as exc:
                bar_errors[symbol] = _error_type(exc)

        records = [
            _review_appearance(
                appearance,
                bars_by_symbol.get(appearance["symbol"], []),
                session_close=session.closes_at,
            )
            for appearance in appearances
        ]
        missing_stages = [stage for stage in PREMARKET_STAGES if stage not in by_stage]
        incomplete = [
            f"{row['symbol']}:{row['checkpoint']}"
            for row in records
            if row["data_state"] != "COMPLETE"
        ]
        data_state = (
            "COMPLETE"
            if not missing_stages and not incomplete and records
            else "LEARNING_DATA_INCOMPLETE"
        )
        payload: dict[str, Any] = {
            "schema_version": "market-premarket-learning.v1",
            "session_id": session_date.isoformat(),
            "as_of": timestamp.isoformat(),
            "data_state": data_state,
            "missing_checkpoint_stages": missing_stages,
            "missing_records": incomplete,
            "bar_errors": bar_errors,
            "records": records,
            "summary": _summary(records),
            "weights_change_permitted": False,
            "pattern_change_permitted": False,
            "broker_actions_allowed": False,
            "note": (
                "Outcome measurement only; forward returns use the first available "
                "one-minute bar at or after each checkpoint offset."
            ),
        }
        output_path = _write_review(payload, self.state_dir)
        payload["artifact_path"] = str(output_path)
        async with self.store.transaction():
            current = await self.store.get_runtime_status_key(status_key)
            if isinstance(current, dict) and current.get("persisted") is True:
                return {
                    "status": "ALREADY_COMPLETED",
                    "data_state": current.get("data_state"),
                    "records": current.get("records", 0),
                    "complete_records": current.get("complete_records", 0),
                }
            await self.store.append(
                LedgerEvent(
                    "PREMARKET_LEARNING_REVIEW",
                    f"premarket_learning:{session_date.isoformat()}",
                    payload,
                    occurred_at=timestamp,
                )
            )
            await self.store.set_runtime_status(
                status_key,
                {
                    "persisted": True,
                    "data_state": data_state,
                    "records": len(records),
                    "complete_records": sum(
                        row["data_state"] == "COMPLETE" for row in records
                    ),
                    "artifact_path": str(output_path),
                    "as_of": timestamp.isoformat(),
                },
            )
        return {
            "status": "COMPLETED",
            "data_state": data_state,
            "records": len(records),
            "complete_records": sum(
                row["data_state"] == "COMPLETE" for row in records
            ),
            "artifact_path": str(output_path),
        }

    async def _bars(self, symbol: str) -> list[dict]:
        method = getattr(self.provider, "learning_bars", None)
        if method is None:
            raise RuntimeError("PREMARKET_LEARNING_PROVIDER_UNAVAILABLE")
        rows = await method(symbol)
        if not isinstance(rows, list):
            raise TypeError("PREMARKET_LEARNING_BARS_INVALID")
        return rows


def _review_appearance(
    appearance: dict[str, Any],
    bars: list[dict],
    *,
    session_close: datetime,
) -> dict[str, Any]:
    reference = _number(appearance.get("reference_price"))
    as_of = _parse_time(appearance.get("as_of"))
    close_utc = _aware(session_close)
    usable: list[tuple[datetime, float, float, float]] = []
    if as_of is not None:
        for bar in bars:
            if not isinstance(bar, dict):
                continue
            bar_time = _parse_time(bar.get("t") or bar.get("time"))
            high = _number(bar.get("h", bar.get("high")))
            low = _number(bar.get("l", bar.get("low")))
            close = _number(bar.get("c", bar.get("close")))
            if (
                bar_time is not None
                and high is not None
                and low is not None
                and close is not None
                and as_of <= bar_time < close_utc
            ):
                usable.append((bar_time, high, low, close))
    usable.sort(key=lambda row: row[0])
    result = {
        **appearance,
        "source_id": "YAHOO_PREMARKET_DELAYED",
        "data_state": "LEARNING_DATA_INCOMPLETE",
        "forward_returns_percent": {str(value): None for value in FORWARD_MINUTES},
        "mfe_percent": None,
        "mae_percent": None,
        "eod_return_percent": None,
        "bars_count": len(usable),
        "last_bar_at": usable[-1][0].isoformat() if usable else None,
        "missing": [],
    }
    if reference is None or reference <= 0:
        result["missing"].append("REFERENCE_PRICE_MISSING")
    if as_of is None:
        result["missing"].append("REFERENCE_TIMESTAMP_MISSING")
    if not usable:
        result["missing"].append("OUTCOME_BARS_MISSING")
    if result["missing"]:
        return result

    result["mfe_percent"] = _return_percent(max(row[1] for row in usable), reference)
    result["mae_percent"] = _return_percent(min(row[2] for row in usable), reference)
    for minutes in FORWARD_MINUTES:
        target = as_of + timedelta(minutes=minutes)
        hit = next((row for row in usable if row[0] >= target), None)
        if hit is None:
            result["missing"].append(f"FORWARD_{minutes}M_MISSING")
        else:
            result["forward_returns_percent"][str(minutes)] = _return_percent(
                hit[3], reference
            )
    last_time, _high, _low, last_close = usable[-1]
    if last_time < close_utc - timedelta(minutes=2):
        result["missing"].append("EOD_BAR_MISSING")
    else:
        result["eod_return_percent"] = _return_percent(last_close, reference)
    if not result["missing"]:
        result["data_state"] = "COMPLETE"
    return result


def _summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    complete = [row for row in records if row.get("data_state") == "COMPLETE"]
    finalists = [row for row in complete if row.get("finalist_eligible") is True]
    return {
        "records": len(records),
        "complete_records": len(complete),
        "finalist_records": len(finalists),
        "finalist_average_mfe_percent": _average(finalists, "mfe_percent"),
        "finalist_average_mae_percent": _average(finalists, "mae_percent"),
        "finalist_average_eod_return_percent": _average(
            finalists, "eod_return_percent"
        ),
    }


def _average(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [_number(row.get(key)) for row in rows]
    usable = [value for value in values if value is not None]
    return round(sum(usable) / len(usable), 4) if usable else None


def _write_review(payload: dict[str, Any], state_dir: Path) -> Path:
    directory = state_dir / "premarket" / str(payload["session_id"])
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "learning_review.json"
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    return path


def _return_percent(value: float, reference: float) -> float:
    return round((value / reference - 1.0) * 100.0, 4)


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return _aware(datetime.fromisoformat(value))
    except ValueError:
        return None


def _error_type(exc: BaseException) -> str:
    return exc.error_type if isinstance(exc, DataUnavailable) else type(exc).__name__


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

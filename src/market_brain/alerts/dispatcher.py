from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from typing import Any

import httpx

from market_brain.alerts.sink import AlertSink
from market_brain.domain.models import AlertRecord, utc_now
from market_brain.ledger.events import LedgerEvent
from market_brain.ledger.store import EventStore
from market_brain.settings import settings

BACKOFF_SECONDS = (1.0, 5.0, 30.0, 120.0)
PRIORITY = {"SELL_NOW": 0, "PLACE_STOP_NOW": 1, "STREAM_STALE": 2}
DELIVERY_ERRORS = (httpx.HTTPError, OSError, RuntimeError, ValueError)


def _safe_error(exc: BaseException) -> str:
    name = type(exc).__name__
    if isinstance(exc, httpx.HTTPStatusError):
        return f"{name}:status={exc.response.status_code}"
    return name


def _redact(value: Any, secrets: tuple[str, ...]) -> Any:
    if isinstance(value, str):
        result = value
        for secret in secrets:
            if secret:
                result = result.replace(secret, "[REDACTED]")
        return result
    if isinstance(value, dict):
        return {_redact(key, secrets): _redact(item, secrets) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact(item, secrets) for item in value]
    if isinstance(value, tuple):
        return tuple(_redact(item, secrets) for item in value)
    return value


class AlertDispatcher:
    def __init__(
        self,
        store: EventStore,
        sinks: list[AlertSink],
        *,
        poll_seconds: float = 2.0,
        max_attempts: int = 6,
        redact_values: tuple[str, ...] = (),
        run_mode: str = settings.run_mode,
        data_plan: str = settings.data_plan,
    ) -> None:
        self.store = store
        self.sinks = sinks
        self.poll_seconds = poll_seconds
        self.max_attempts = max_attempts
        self.run_mode = run_mode
        self.data_plan = data_plan
        sink_secrets = tuple(
            token
            for sink in sinks
            if (token := getattr(sink, "bot_token", None))
        )
        self.redact_values = tuple(
            dict.fromkeys(value for value in (*redact_values, *sink_secrets) if value)
        )
        self._stop = asyncio.Event()

    @property
    def active_sinks(self) -> list[AlertSink]:
        return [sink for sink in self.sinks if sink.configured]

    @property
    def active_sink_names(self) -> list[str]:
        return [sink.name for sink in self.active_sinks]

    def _backoff_seconds(self, attempt_number: int) -> float:
        return BACKOFF_SECONDS[min(max(attempt_number - 1, 0), len(BACKOFF_SECONDS) - 1)]

    def _event_payload(self, payload: dict) -> dict:
        return _redact(payload, self.redact_values)

    def _delivery_payload(self, alert: AlertRecord) -> dict:
        payload = dict(_redact(alert.payload, self.redact_values))
        tags = []
        if self.run_mode == "shadow":
            tags.append("[SHADOW]")
        if self.data_plan == "keyless_delayed":
            tags.append("[DELAYED]")
        if tags:
            text = payload.get("text")
            if not isinstance(text, str):
                text = json.dumps(payload, sort_keys=True, default=str)
            for tag in ("[SHADOW]", "[DELAYED]"):
                if text.startswith(tag):
                    text = text.removeprefix(tag).lstrip()
            payload["text"] = f"{''.join(tags)} {text}"
        return payload

    async def deliver(self, alert: AlertRecord, *, now=None) -> bool:
        sinks = self.active_sinks
        if not sinks or alert.delivered_at is not None or alert.attempts >= self.max_attempts:
            return False

        attempt_time = now or utc_now()
        payload = self._delivery_payload(alert)
        try:
            for sink in sinks:
                await sink.send(payload)
            delivered = await self.store.mark_delivered(alert.alert_id)
            if delivered is None:
                return False
            for sink in sinks:
                await self.store.append(
                    LedgerEvent(
                        "ALERT_DELIVERED",
                        alert.alert_id,
                        self._event_payload(
                            {"alert_id": alert.alert_id, "sink": sink.name}
                        ),
                        occurred_at=attempt_time,
                    )
                )
            return True
        except DELIVERY_ERRORS as exc:
            attempt_number = alert.attempts + 1
            next_attempt_at = None
            if attempt_number < self.max_attempts:
                next_attempt_at = attempt_time + timedelta(
                    seconds=self._backoff_seconds(attempt_number)
                )
            failed = await self.store.mark_failed(
                alert.alert_id,
                _safe_error(exc),
                next_attempt_at,
            )
            if failed is None:
                return False
            if failed.attempts >= self.max_attempts:
                await self.store.append(
                    LedgerEvent(
                        "ALERT_DELIVERY_FAILED",
                        alert.alert_id,
                        self._event_payload(
                            {
                                "alert_id": alert.alert_id,
                                "attempts": failed.attempts,
                                "last_error": failed.last_error,
                            }
                        ),
                        occurred_at=attempt_time,
                    )
                )
            return False

    async def dispatch_once(self, *, now=None) -> int:
        current_time = now or utc_now()
        alerts = [
            alert
            for alert in await self.store.list_undelivered()
            if alert.attempts < self.max_attempts
            and (alert.next_attempt_at is None or alert.next_attempt_at <= current_time)
        ]
        alerts.sort(key=lambda alert: (PRIORITY.get(alert.kind, 3), alert.created_at))

        delivered = 0
        for alert in alerts:
            if await self.deliver(alert, now=current_time):
                delivered += 1
        return delivered

    async def run(self) -> None:
        while not self._stop.is_set():
            await self.dispatch_once()
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.poll_seconds)
            except TimeoutError:
                pass

    async def stop(self) -> None:
        self._stop.set()

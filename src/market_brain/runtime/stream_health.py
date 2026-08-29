from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

from market_brain.domain.models import AlertRecord
from market_brain.ledger.events import LedgerEvent
from market_brain.orchestration.universe import EASTERN, NyseMarketCalendar, load_market_calendar
from market_brain.settings import Settings, settings

LOGGER = logging.getLogger(__name__)


class StreamStaleMonitor:
    def __init__(
        self,
        store,
        *,
        cfg: Settings = settings,
        clock=lambda: datetime.now(UTC),
        sleep=asyncio.sleep,
    ) -> None:
        self.store = store
        self.cfg = cfg
        self.clock = clock
        self.sleep = sleep
        self.calendar: NyseMarketCalendar | None = None
        self._stop = asyncio.Event()

    def validate_startup(self, *, now: datetime | None = None) -> None:
        local = _aware(now or self.clock()).astimezone(EASTERN)
        self.calendar = load_market_calendar(
            self.cfg.market_calendar_path,
            required_years={local.year, local.year + 1},
        )

    async def check(self, *, now: datetime | None = None) -> str | None:
        if self.calendar is None:
            raise RuntimeError("STREAM_STALE_MONITOR_NOT_VALIDATED")
        timestamp = _aware(now or self.clock())
        local = timestamp.astimezone(EASTERN)
        session = self.calendar.session_for(local.date())
        if session is None or not session.opens_at <= local < session.closes_at:
            return None
        runtime = await self.store.get_runtime_status()
        last = _runtime_datetime(runtime.get("stream_last_message_at"))
        session_open = session.opens_at.astimezone(UTC)
        reference = max(last, session_open) if last is not None else session_open
        stale = (timestamp - reference).total_seconds() > self.cfg.stream_stale_alert_seconds
        same_session = runtime.get("stream_stale_session_date") == local.date().isoformat()
        was_stale = runtime.get("stream_stale") is True and same_session
        if stale == was_stale:
            return None
        event_type = "STREAM_STALE" if stale else "STREAM_RECOVERED"
        payload = {
            "action": event_type,
            "session_date": local.date().isoformat(),
            "threshold_seconds": self.cfg.stream_stale_alert_seconds,
            "text": (
                f"{event_type}: no market stream data within "
                f"{self.cfg.stream_stale_alert_seconds:g} seconds."
                if stale
                else "STREAM_RECOVERED: market stream data is flowing again."
            ),
        }
        async with self.store.transaction():
            await self.store.set_runtime_status("stream_stale", stale)
            await self.store.set_runtime_status(
                "stream_stale_session_date", local.date().isoformat()
            )
            await self.store.set_runtime_status(
                "stream_stale_changed_at", timestamp.isoformat()
            )
            await self.store.save_alert(AlertRecord(kind=event_type, payload=payload))
            await self.store.append(
                LedgerEvent(event_type, "market_stream", payload, occurred_at=timestamp)
            )
        return event_type

    async def run(self) -> None:
        self._stop.clear()
        while not self._stop.is_set():
            try:
                await self.check()
            except (OSError, RuntimeError, TypeError, ValueError) as exc:
                LOGGER.error("stream_stale_monitor_error type=%s", type(exc).__name__)
            await self.sleep(min(5.0, self.cfg.stream_stale_alert_seconds))

    async def stop(self) -> None:
        self._stop.set()


def _runtime_datetime(value) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return _aware(datetime.fromisoformat(value))
    except ValueError:
        return None


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

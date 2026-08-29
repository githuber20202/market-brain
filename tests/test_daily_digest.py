from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from market_brain.domain.models import (
    PositionState,
    ProtectionState,
    ReconciliationState,
    ShadowTrade,
)
from market_brain.ledger.events import LedgerEvent
from market_brain.ledger.store import InMemoryEventStore
from market_brain.runtime.daily_digest import DailyDigest

EASTERN = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.asyncio
async def test_daily_digest_aggregates_runtime_alerts_positions_and_replay_check():
    store = InMemoryEventStore()
    now = datetime(2026, 8, 28, 16, 15, tzinfo=EASTERN).astimezone(UTC)
    opened = now - timedelta(hours=2)
    position = PositionState(
        position_id="digest-position",
        plan_id="manual-digest",
        symbol="AAPL",
        quantity=2,
        remaining_quantity=2,
        average_fill=100.0,
        stop=98.0,
        tp1=103.0,
        tp2=104.0,
        opened_at=opened,
        time_stop_at=opened + timedelta(minutes=30),
        protection=ProtectionState.PROTECTED,
        broker_stop_price=98.0,
        protected_quantity=2,
        reconciliation_state=ReconciliationState.RECONCILED,
        last_reconciled_at=opened,
        source="MANUAL_IMPORT",
    )
    await store.save_position(position)
    await store.append(
        LedgerEvent("POSITION_IMPORTED", position.position_id, {"position": asdict(position)}, occurred_at=opened)
    )
    await store.append(LedgerEvent("BUY_NOW_EMITTED", "plan-1", {}, occurred_at=now))
    await store.append(LedgerEvent("BUY_NOW_EMITTED", "plan-2", {}, occurred_at=now))
    await store.append(LedgerEvent("ALERT_DELIVERED", "alert-1", {"sink": "telegram"}, occurred_at=now))
    await store.append(LedgerEvent("ALERT_DELIVERED", "alert-1", {"sink": "webhook"}, occurred_at=now))
    await store.append(LedgerEvent("ALERT_DELIVERY_FAILED", "alert-2", {}, occurred_at=now))
    connected_since = now - timedelta(hours=6, minutes=15)
    await store.set_runtime_status("stream_connected", True)
    await store.set_runtime_status("stream_connected_since", connected_since.isoformat())
    await store.set_runtime_status("stream_last_message_at", (now - timedelta(seconds=2)).isoformat())
    await store.set_runtime_status(
        "shadow_wallet",
        {"mode": "virtual", "source": "SHADOW_VIRTUAL"},
    )
    await store.set_runtime_status(
        "quality_state",
        {"status": "QUALITY_STALE", "rows": 58},
    )
    await store.append(
        LedgerEvent(
            "RADAR_RUN",
            "radar:2026-08-28:1020",
            {"status": "MISSED"},
            occurred_at=now,
        )
    )
    await store.append(
        LedgerEvent(
            "RADAR_RUN",
            "radar:2026-08-28:1050",
            {
                "status": "COMPLETED",
                "candidates": [
                    {"symbol": "LHX", "reason": "RISK_TOO_SMALL"},
                    {"symbol": "MS", "reason": "RISK_TOO_SMALL"},
                    {"symbol": "QUIET", "reason": "OPENING_RANGE_TOO_NARROW"},
                    {"symbol": "PASS", "reason": None},
                ],
            },
            occurred_at=now,
        )
    )
    shadow_trade = ShadowTrade(
        trade_id="digest-shadow",
        plan_id="digest-shadow-plan",
        symbol="NVDA",
        setup="CORE_MOMENTUM",
        quantity=2,
        trigger=100.0,
        fill=100.1,
        stop=99.0,
        tp1=102.0,
        tp2=103.0,
        opened_at=opened,
        time_stop_at=opened + timedelta(minutes=30),
    )
    await store.save_shadow_trade(shadow_trade)
    await store.append(
        LedgerEvent(
            "SHADOW_TRADE_OPENED",
            shadow_trade.trade_id,
            {"shadow_trade": asdict(shadow_trade)},
            occurred_at=opened,
        )
    )

    alert = await DailyDigest(store).create(now=now)

    assert alert is not None
    assert alert.kind == "DAILY_DIGEST"
    assert alert.payload["signals_created"] == 2
    assert alert.payload["alerts_delivered"] == 1
    assert alert.payload["alerts_failed"] == 1
    assert alert.payload["stream"]["uptime_seconds"] == 22_500
    assert alert.payload["open_positions"][0]["protection"] == "PROTECTED"
    assert alert.payload["replay_check"] == {"ok": True, "differences": []}
    assert alert.payload["shadow"]["today"]["signals"] == 2
    assert alert.payload["shadow"]["today"]["trades"] == 1
    assert alert.payload["shadow"]["today"]["unfinalized"] == 1
    assert alert.payload["wallet"] == "virtual"
    assert alert.payload["quality"]["status"] == "QUALITY_STALE"
    assert alert.payload["data_availability"]["slots_missed"] == 1
    assert alert.payload["plan_rejections"] == {
        "OPENING_RANGE_TOO_NARROW": 1,
        "RISK_TOO_SMALL": 2,
    }
    assert "Wallet: virtual" in alert.payload["text"]
    assert "Quality: QUALITY_STALE rows=58" in alert.payload["text"]
    assert "- RISK_TOO_SMALL: count=2" in alert.payload["text"]
    assert "Shadow today:" in alert.payload["text"]
    assert "Shadow by setup: {'" not in alert.payload["text"]
    assert (
        "- CORE_MOMENTUM: trades=1 hit_rate=0.00% expectancy=0.000R"
        in alert.payload["text"]
    )
    assert "Reminder: reconcile broker holdings" in alert.payload["text"]
    assert await DailyDigest(store).create(now=now) is None


@pytest.mark.asyncio
async def test_daily_digest_handles_disconnected_stream_and_replay_mismatch():
    store = InMemoryEventStore()
    now = datetime(2026, 8, 28, 16, 15, tzinfo=EASTERN).astimezone(UTC)
    await store.set_runtime_status("stream_connected", False)
    position = PositionState(
        position_id="orphan",
        plan_id="manual-orphan",
        symbol="MSFT",
        quantity=1,
        remaining_quantity=1,
        average_fill=200.0,
        stop=None,
        tp1=None,
        tp2=None,
        opened_at=now,
        time_stop_at=None,
        protection=ProtectionState.UNPROTECTED,
    )
    await store.save_position(position)

    alert = await DailyDigest(store).create(now=now)

    assert alert is not None
    assert alert.payload["stream"]["uptime_seconds"] is None
    assert alert.payload["replay_check"]["ok"] is False
    assert alert.payload["replay_check"]["differences"] == ["position:orphan"]
    assert alert.payload["open_positions"][0]["protection"] == "UNPROTECTED"
    assert "Replay check: FAIL" in alert.payload["text"]


def test_daily_digest_script_is_network_free_and_uses_existing_outbox():
    script = (ROOT / "scripts" / "daily_digest.py").read_text()
    assert "DailyDigest(store).create()" in script
    assert "TelegramSink" not in script
    assert "httpx" not in script

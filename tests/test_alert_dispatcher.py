import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from market_brain.alerts.dispatcher import AlertDispatcher
from market_brain.alerts.sink import TelegramSink, WebhookSink
from market_brain.domain.models import AlertRecord
from market_brain.ledger.store import InMemoryEventStore


@pytest.mark.asyncio
async def test_dispatcher_delivers_and_marks_alert_delivered():
    requests = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        store = InMemoryEventStore()
        alert = AlertRecord(kind="BUY_NOW", payload={"text": "BUY TEST"})
        await store.save_alert(alert)
        dispatcher = AlertDispatcher(
            store,
            [WebhookSink("https://alerts.example.test", client)],
        )

        assert await dispatcher.dispatch_once() == 1

    delivered = await store.get_alert(alert.alert_id)
    assert delivered is not None
    assert delivered.delivered_at is not None
    assert delivered.attempts == 1
    assert delivered.next_attempt_at is None
    assert requests == 1
    events = [event for event in store.events if event.event_type == "ALERT_DELIVERED"]
    assert events[-1].payload["sink"] == "webhook"


@pytest.mark.asyncio
async def test_failed_alert_does_not_block_next_alert_in_same_dispatch():
    requests = 0
    now = datetime.now(UTC)

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(500 if requests == 1 else 200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        store = InMemoryEventStore()
        first = AlertRecord(
            kind="BUY_NOW",
            payload={"text": "FIRST"},
            created_at=now - timedelta(seconds=2),
        )
        second = AlertRecord(
            kind="BUY_NOW",
            payload={"text": "SECOND"},
            created_at=now - timedelta(seconds=1),
        )
        await store.save_alert(first)
        await store.save_alert(second)
        dispatcher = AlertDispatcher(
            store,
            [WebhookSink("https://alerts.example.test", client)],
        )

        assert await dispatcher.dispatch_once(now=now) == 1

    first_saved = await store.get_alert(first.alert_id)
    second_saved = await store.get_alert(second.alert_id)
    assert first_saved is not None and second_saved is not None
    assert first_saved.delivered_at is None
    assert first_saved.attempts == 1
    assert first_saved.next_attempt_at == now + timedelta(seconds=1)
    assert second_saved.delivered_at is not None
    assert requests == 2


@pytest.mark.asyncio
async def test_failed_alert_is_not_retried_before_next_attempt_at():
    requests = 0
    now = datetime.now(UTC)

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(500)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        store = InMemoryEventStore()
        alert = AlertRecord(kind="BUY_NOW", payload={"text": "BUY"})
        await store.save_alert(alert)
        dispatcher = AlertDispatcher(
            store,
            [WebhookSink("https://alerts.example.test", client)],
        )

        assert await dispatcher.dispatch_once(now=now) == 0
        assert await dispatcher.dispatch_once(now=now + timedelta(milliseconds=500)) == 0
        saved = await store.get_alert(alert.alert_id)
        assert saved is not None
        assert saved.attempts == 1
        assert requests == 1

        assert await dispatcher.dispatch_once(now=now + timedelta(seconds=1)) == 0

    retried = await store.get_alert(alert.alert_id)
    assert retried is not None
    assert retried.attempts == 2
    assert requests == 2


@pytest.mark.asyncio
async def test_sell_now_is_delivered_before_older_buy_now():
    order: list[str] = []
    now = datetime.now(UTC)

    async def handler(request: httpx.Request) -> httpx.Response:
        order.append(json.loads(request.content)["text"])
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        store = InMemoryEventStore()
        buy = AlertRecord(
            kind="BUY_NOW",
            payload={"text": "BUY"},
            created_at=now - timedelta(minutes=5),
        )
        sell = AlertRecord(
            kind="SELL_NOW",
            payload={"text": "SELL"},
            created_at=now,
        )
        await store.save_alert(buy)
        await store.save_alert(sell)
        dispatcher = AlertDispatcher(
            store,
            [WebhookSink("https://alerts.example.test", client)],
        )

        assert await dispatcher.dispatch_once(now=now) == 2

    assert order == ["[SHADOW] SELL", "[SHADOW] BUY"]


@pytest.mark.asyncio
async def test_shadow_prefix_is_added_once_and_live_remains_brokerless_label_free():
    seen: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(json.loads(request.content)["text"])
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        shadow_store = InMemoryEventStore()
        live_store = InMemoryEventStore()
        await shadow_store.save_alert(AlertRecord(kind="BUY_NOW", payload={"text": "BUY"}))
        await live_store.save_alert(AlertRecord(kind="BUY_NOW", payload={"text": "BUY"}))
        await AlertDispatcher(
            shadow_store,
            [WebhookSink("https://alerts.example.test", client)],
            run_mode="shadow",
        ).dispatch_once()
        await AlertDispatcher(
            live_store,
            [WebhookSink("https://alerts.example.test", client)],
            run_mode="live",
        ).dispatch_once()

    assert seen == ["[SHADOW] BUY", "BUY"]


@pytest.mark.asyncio
async def test_permanent_failure_stays_undelivered_and_emits_failed_event():
    requests = 0
    now = datetime.now(UTC)

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(500)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        store = InMemoryEventStore()
        alert = AlertRecord(kind="PLACE_STOP_NOW", payload={"text": "PLACE STOP"})
        await store.save_alert(alert)
        dispatcher = AlertDispatcher(
            store,
            [WebhookSink("https://alerts.example.test", client)],
            max_attempts=6,
        )

        current = now
        for _ in range(6):
            await dispatcher.dispatch_once(now=current)
            saved = await store.get_alert(alert.alert_id)
            assert saved is not None
            if saved.next_attempt_at is not None:
                current = saved.next_attempt_at

    failed = await store.get_alert(alert.alert_id)
    assert failed is not None
    assert failed.delivered_at is None
    assert failed.attempts == 6
    assert failed.next_attempt_at is None
    assert requests == 6
    assert [row.alert_id for row in await store.list_undelivered()] == [alert.alert_id]
    events = [
        event for event in store.events if event.event_type == "ALERT_DELIVERY_FAILED"
    ]
    assert len(events) == 1
    assert events[0].payload["attempts"] == 6


@pytest.mark.asyncio
async def test_telegram_failure_never_persists_or_logs_bot_token(caplog):
    token = "secret-token-123"
    seen_body = ""

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal seen_body
        seen_body = request.content.decode()
        return httpx.Response(500)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        store = InMemoryEventStore()
        alert = AlertRecord(
            kind="SELL_NOW",
            payload={"text": f"SELL token={token}"},
        )
        await store.save_alert(alert)
        dispatcher = AlertDispatcher(
            store,
            [TelegramSink(token, "12345", client)],
            max_attempts=1,
        )

        assert await dispatcher.dispatch_once() == 0

    failed = await store.get_alert(alert.alert_id)
    assert failed is not None
    assert failed.last_error == "HTTPStatusError:status=500"
    assert token not in failed.last_error
    assert token not in seen_body
    assert "[REDACTED]" in seen_body
    assert token not in repr(store.events)
    assert token not in caplog.text
    events = [
        event for event in store.events if event.event_type == "ALERT_DELIVERY_FAILED"
    ]
    assert len(events) == 1
    assert token not in json.dumps(events[0].payload, default=str)


@pytest.mark.asyncio
async def test_unconfigured_telegram_makes_no_request_and_raises_no_exception():
    requests = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        raise AssertionError("unconfigured Telegram sink must not send")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        store = InMemoryEventStore()
        alert = AlertRecord(kind="RECONCILE_REQUIRED", payload={"text": "RECONCILE"})
        await store.save_alert(alert)
        sink = TelegramSink(None, None, client)
        dispatcher = AlertDispatcher(store, [sink])

        assert sink.configured is False
        assert await dispatcher.dispatch_once() == 0

    pending = await store.get_alert(alert.alert_id)
    assert pending is not None
    assert pending.attempts == 0
    assert pending.delivered_at is None
    assert requests == 0


@pytest.mark.asyncio
async def test_telegram_sink_uses_bot_api_send_message():
    seen: dict = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["body"] = request.content.decode()
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        sink = TelegramSink("test-token", "12345", client)
        assert await sink.send({"text": "BUY TEST"}) is True

    assert seen["url"].endswith("/bottest-token/sendMessage")
    assert json.loads(seen["body"]) == {"chat_id": "12345", "text": "BUY TEST"}

from __future__ import annotations

import json
from typing import Protocol

import httpx


class AlertSink(Protocol):
    name: str

    @property
    def configured(self) -> bool: ...

    async def send(self, payload: dict) -> bool: ...


class WebhookSink:
    name = "webhook"

    def __init__(
        self,
        url: str | None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.url = url
        self.client = client

    @property
    def configured(self) -> bool:
        return bool(self.url)

    async def send(self, payload: dict) -> bool:
        if not self.configured:
            return False
        owned = self.client is None
        client = self.client or httpx.AsyncClient(timeout=10.0)
        try:
            response = await client.post(self.url or "", json=payload)
            response.raise_for_status()
        finally:
            if owned:
                await client.aclose()
        return True


class TelegramSink:
    name = "telegram"

    def __init__(
        self,
        bot_token: str | None,
        chat_id: str | None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.client = client

    @property
    def configured(self) -> bool:
        return bool(self.bot_token and self.chat_id)

    async def send(self, payload: dict) -> bool:
        if not self.configured:
            return False
        text = payload.get("text")
        if not text:
            text = json.dumps(payload, default=str, ensure_ascii=False, sort_keys=True)
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        owned = self.client is None
        client = self.client or httpx.AsyncClient(timeout=10.0)
        try:
            response = await client.post(
                url,
                json={"chat_id": self.chat_id, "text": text},
            )
            response.raise_for_status()
        finally:
            if owned:
                await client.aclose()
        return True


WebhookAlertSink = WebhookSink


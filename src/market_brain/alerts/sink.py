from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol
from zoneinfo import ZoneInfo

import httpx

EASTERN = ZoneInfo("America/New_York")


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


class GitHubIssueSink:
    name = "github_issue"

    def __init__(
        self,
        token: str | None,
        repository: str | None,
        client: httpx.AsyncClient | None = None,
        *,
        mention: str = "githuber20202",
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.token = token
        self.repository = repository
        self.client = client
        self.mention = mention
        self.clock = clock or (lambda: datetime.now(UTC))
        self._owned_client: httpx.AsyncClient | None = None
        self._issues: dict[str, int] = {}
        self._label_ready = False

    @property
    def configured(self) -> bool:
        return bool(self.token and self.repository)

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self.token}",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    def _client(self) -> httpx.AsyncClient:
        if self.client is not None:
            return self.client
        if self._owned_client is None:
            self._owned_client = httpx.AsyncClient(timeout=10.0)
        return self._owned_client

    async def _ensure_label(self) -> None:
        if self._label_ready:
            return
        client = self._client()
        base = f"https://api.github.com/repos/{self.repository}"
        response = await client.get(f"{base}/labels/shadow", headers=self._headers())
        if response.status_code == 404:
            response = await client.post(
                f"{base}/labels",
                headers=self._headers(),
                json={"name": "shadow", "color": "6f42c1", "description": "Shadow-mode output"},
            )
        response.raise_for_status()
        self._label_ready = True

    async def _daily_issue(self, session_date: str) -> int:
        cached = self._issues.get(session_date)
        if cached is not None:
            return cached
        await self._ensure_label()
        client = self._client()
        base = f"https://api.github.com/repos/{self.repository}"
        title = f"Shadow {session_date}"
        response = await client.get(
            f"{base}/issues",
            headers=self._headers(),
            params={"state": "all", "labels": "shadow", "per_page": 100},
        )
        response.raise_for_status()
        issues = response.json()
        issue_number = next(
            (
                int(row["number"])
                for row in issues
                if "pull_request" not in row and row.get("title") == title
            ),
            None,
        )
        if issue_number is None:
            response = await client.post(
                f"{base}/issues",
                headers=self._headers(),
                json={
                    "title": title,
                    "body": (
                        f"Brokerless shadow-mode alerts for {session_date}. "
                        "Measurement only; not advice or execution."
                    ),
                    "labels": ["shadow"],
                },
            )
            response.raise_for_status()
            issue_number = int(response.json()["number"])
        self._issues[session_date] = issue_number
        return issue_number

    async def send(self, payload: dict) -> bool:
        if not self.configured:
            return False
        session_date = str(
            payload.get("session_date")
            or self.clock().astimezone(EASTERN).date().isoformat()
        )
        issue_number = await self._daily_issue(session_date)
        text = payload.get("text")
        if not isinstance(text, str):
            text = json.dumps(payload, default=str, ensure_ascii=False, sort_keys=True)
        response = await self._client().post(
            f"https://api.github.com/repos/{self.repository}/issues/{issue_number}/comments",
            headers=self._headers(),
            json={"body": f"@{self.mention}\n\n{text}"},
        )
        response.raise_for_status()
        if str(payload.get("run_id", "")).startswith("daily_digest:"):
            response = await self._client().patch(
                f"https://api.github.com/repos/{self.repository}/issues/{issue_number}",
                headers=self._headers(),
                json={"state": "closed"},
            )
            response.raise_for_status()
        return True

    async def send_rehearsal_summary(self, session_date: str, text: str) -> int:
        """Post one supervised rehearsal summary to its own issue and close it."""
        if not self.configured:
            raise RuntimeError("GITHUB_ISSUE_SINK_NOT_CONFIGURED")
        await self._ensure_label()
        client = self._client()
        base = f"https://api.github.com/repos/{self.repository}"
        title = f"Shadow rehearsal {session_date}"
        response = await client.get(
            f"{base}/issues",
            headers=self._headers(),
            params={"state": "all", "labels": "shadow", "per_page": 100},
        )
        response.raise_for_status()
        issue_number = next(
            (
                int(row["number"])
                for row in response.json()
                if "pull_request" not in row and row.get("title") == title
            ),
            None,
        )
        if issue_number is None:
            response = await client.post(
                f"{base}/issues",
                headers=self._headers(),
                json={
                    "title": title,
                    "body": (
                        f"Brokerless production-path rehearsal for {session_date}. "
                        "Measurement only; not advice or execution."
                    ),
                    "labels": ["shadow"],
                },
            )
            response.raise_for_status()
            issue_number = int(response.json()["number"])
        response = await client.post(
            f"{base}/issues/{issue_number}/comments",
            headers=self._headers(),
            json={"body": f"@{self.mention}\n\n{text}"},
        )
        response.raise_for_status()
        response = await client.patch(
            f"{base}/issues/{issue_number}",
            headers=self._headers(),
            json={"state": "closed"},
        )
        response.raise_for_status()
        return issue_number

    async def aclose(self) -> None:
        if self._owned_client is not None:
            await self._owned_client.aclose()
            self._owned_client = None


WebhookAlertSink = WebhookSink

from __future__ import annotations

import argparse
import asyncio
import os
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import asyncpg
import httpx
import nats

from market_brain.alerts.dispatcher import _redact, _safe_error
from market_brain.ledger.replay import replay_check
from market_brain.ledger.store import PostgresEventStore
from market_brain.providers.rate_limit import TokenBucketRateLimiter

SCHEMA_PATH = Path(__file__).resolve().parents[3] / "config" / "schema.sql"
PAPER_API_BASE_URL = "https://paper-api.alpaca.markets"
DATA_API_BASE_URL = "https://data.alpaca.markets"
TELEGRAM_API_BASE_URL = "https://api.telegram.org"
DEPENDENCY_ERRORS = (
    OSError,
    RuntimeError,
    TypeError,
    ValueError,
    TimeoutError,
    asyncpg.PostgresError,
    httpx.HTTPError,
    nats.errors.Error,
)

REQUIRED_ENV = (
    "ALPACA_API_KEY",
    "ALPACA_API_SECRET",
    "DATA_PLAN",
    "DIRECT_ACCOUNT_ACCESS_ALLOWED",
    "EXECUTION_ACTIONS_ALLOWED",
    "HISTORICAL_LAG_MINUTES",
    "NATS_URL",
    "POSTGRES_DSN",
    "POSTGRES_PASSWORD",
    "RUN_MODE",
    "STREAM_STALE_ALERT_SECONDS",
    "STREAM_MAX_SYMBOLS",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
)


@dataclass(frozen=True, slots=True)
class CheckResult:
    status: str
    code: str
    hint: str

    def render(self) -> str:
        return f"[{self.status}] {self.code} — {self.hint}"


class PostgresProbe:
    def __init__(self, dsn: str) -> None:
        self.store = PostgresEventStore(dsn)

    async def connect(self) -> None:
        await self.store.connect()

    async def table_names(self) -> set[str]:
        assert self.store.pool is not None
        async with self.store.pool.acquire() as connection:
            rows = await connection.fetch(
                "SELECT tablename FROM pg_catalog.pg_tables "
                "WHERE schemaname = current_schema()"
            )
        return {str(row["tablename"]) for row in rows}

    async def replay_differences(self) -> list[str]:
        return await replay_check(self.store)

    async def close(self) -> None:
        await self.store.close()


class NatsProbe:
    def __init__(self, url: str) -> None:
        self.url = url
        self.connection = None
        self.jetstream = None

    async def connect(self) -> None:
        self.connection = await nats.connect(
            self.url,
            connect_timeout=5,
            max_reconnect_attempts=0,
        )

    async def connect_jetstream(self) -> None:
        assert self.connection is not None
        self.jetstream = self.connection.jetstream()
        await self.jetstream.account_info()

    async def close(self) -> None:
        if self.connection is not None:
            await self.connection.drain()


class PreflightRunner:
    def __init__(
        self,
        *,
        env: Mapping[str, str] | None = None,
        schema_path: Path = SCHEMA_PATH,
        postgres_factory: Callable[[str], Any] = PostgresProbe,
        nats_factory: Callable[[str], Any] = NatsProbe,
        client: httpx.AsyncClient | None = None,
        limiter: TokenBucketRateLimiter | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.env = dict(os.environ if env is None else env)
        self.schema_path = schema_path
        self.postgres_factory = postgres_factory
        self.nats_factory = nats_factory
        self.client = client
        safe_calls = _positive_int(self.env.get("REST_SAFE_CALLS_PER_MINUTE"), 180)
        self.limiter = limiter or TokenBucketRateLimiter(safe_calls)
        self.now = now or (lambda: datetime.now(UTC))
        self.secrets = tuple(
            value
            for name in ("ALPACA_API_KEY", "ALPACA_API_SECRET", "TELEGRAM_BOT_TOKEN")
            if (value := self.env.get(name, ""))
        )

    async def run(self, *, offline: bool = False) -> list[CheckResult]:
        results = self._environment_checks()
        results.extend(await self._postgres_checks())
        results.extend(await self._nats_checks())
        if offline:
            results.extend(
                CheckResult("SKIP", code, "offline mode; no external request was sent")
                for code in (
                    "ALPACA_IEX_QUOTE",
                    "ALPACA_SIP_BARS",
                    "ALPACA_PAPER_CLOCK",
                    "TELEGRAM_GET_ME",
                    "TELEGRAM_SEND_MESSAGE",
                )
            )
        else:
            results.extend(
                await self._online_checks(
                    prerequisites_ok=not any(result.status == "FAIL" for result in results)
                )
            )
        return results

    def _environment_checks(self) -> list[CheckResult]:
        results = [
            CheckResult(
                "PASS" if name in self.env else "FAIL",
                f"ENV_{name}",
                "PRESENT" if name in self.env else "MISSING",
            )
            for name in REQUIRED_ENV
        ]
        results.extend(
            (
                _exact_policy(
                    self.env,
                    "DATA_PLAN",
                    "free",
                    "DATA_PLAN_FREE",
                    "free data plan is required",
                ),
                _exact_policy(
                    self.env,
                    "EXECUTION_ACTIONS_ALLOWED",
                    "false",
                    "EXECUTION_DISABLED",
                    "automatic execution must remain disabled",
                ),
                _exact_policy(
                    self.env,
                    "DIRECT_ACCOUNT_ACCESS_ALLOWED",
                    "false",
                    "ACCOUNT_ACCESS_DISABLED",
                    "direct account access must remain disabled",
                ),
                _integer_policy(
                    self.env,
                    "HISTORICAL_LAG_MINUTES",
                    "HISTORICAL_LAG_SAFE",
                    lambda value: value >= 16,
                    "historical SIP lag must be at least 16 minutes",
                ),
                _integer_policy(
                    self.env,
                    "STREAM_MAX_SYMBOLS",
                    "STREAM_SYMBOL_CAP_SAFE",
                    lambda value: 0 < value <= 30,
                    "free-plan stream cap must be between 1 and 30",
                ),
                _integer_policy(
                    self.env,
                    "STREAM_STALE_ALERT_SECONDS",
                    "STREAM_STALE_ALERT_SAFE",
                    lambda value: value > 0,
                    "stream stale alert threshold must be positive",
                ),
            )
        )
        return results

    async def _postgres_checks(self) -> list[CheckResult]:
        dsn = self.env.get("POSTGRES_DSN", "")
        if not dsn:
            return [
                CheckResult("FAIL", "POSTGRES_CONNECTION", "POSTGRES_DSN is missing or empty"),
                CheckResult("SKIP", "POSTGRES_SCHEMA", "database connection is unavailable"),
                CheckResult("SKIP", "POSTGRES_REPLAY", "database connection is unavailable"),
            ]
        probe = self.postgres_factory(dsn)
        try:
            await probe.connect()
        except DEPENDENCY_ERRORS as exc:
            return [
                CheckResult("FAIL", "POSTGRES_CONNECTION", self._error_hint(exc)),
                CheckResult("SKIP", "POSTGRES_SCHEMA", "database connection is unavailable"),
                CheckResult("SKIP", "POSTGRES_REPLAY", "database connection is unavailable"),
            ]

        results = [CheckResult("PASS", "POSTGRES_CONNECTION", "database accepted a connection")]
        try:
            expected = _schema_tables(self.schema_path)
            actual = await probe.table_names()
            missing = sorted(expected - actual)
            results.append(
                CheckResult(
                    "FAIL" if missing else "PASS",
                    "POSTGRES_SCHEMA",
                    (
                        "missing tables: " + ",".join(missing)
                        if missing
                        else "all schema.sql tables are present"
                    ),
                )
            )
        except DEPENDENCY_ERRORS as exc:
            results.append(CheckResult("FAIL", "POSTGRES_SCHEMA", self._error_hint(exc)))
        try:
            differences = await probe.replay_differences()
            results.append(
                CheckResult(
                    "FAIL" if differences else "PASS",
                    "POSTGRES_REPLAY",
                    (
                        "materialized state differs from the event ledger"
                        if differences
                        else "event replay matches materialized state"
                    ),
                )
            )
        except DEPENDENCY_ERRORS as exc:
            results.append(CheckResult("FAIL", "POSTGRES_REPLAY", self._error_hint(exc)))
        finally:
            await _quiet_close(probe)
        return results

    async def _nats_checks(self) -> list[CheckResult]:
        url = self.env.get("NATS_URL", "")
        if not url:
            return [
                CheckResult("FAIL", "NATS_CONNECTION", "NATS_URL is missing or empty"),
                CheckResult("SKIP", "NATS_JETSTREAM", "NATS connection is unavailable"),
            ]
        probe = self.nats_factory(url)
        try:
            await probe.connect()
        except DEPENDENCY_ERRORS as exc:
            return [
                CheckResult("FAIL", "NATS_CONNECTION", self._error_hint(exc)),
                CheckResult("SKIP", "NATS_JETSTREAM", "NATS connection is unavailable"),
            ]

        results = [CheckResult("PASS", "NATS_CONNECTION", "NATS accepted a connection")]
        try:
            await probe.connect_jetstream()
            results.append(CheckResult("PASS", "NATS_JETSTREAM", "JetStream account is active"))
        except DEPENDENCY_ERRORS as exc:
            results.extend(
                (CheckResult("FAIL", "NATS_JETSTREAM", self._error_hint(exc)),)
            )
            await _quiet_close(probe)
            return results
        await _quiet_close(probe)
        return results

    async def _online_checks(self, *, prerequisites_ok: bool) -> list[CheckResult]:
        owned = self.client is None
        client = self.client or httpx.AsyncClient(timeout=10.0)
        try:
            results = await self._alpaca_checks(client)
            results.extend(
                await self._telegram_checks(
                    client,
                    allow_send=(
                        prerequisites_ok
                        and not any(result.status == "FAIL" for result in results)
                    ),
                )
            )
            return results
        finally:
            if owned:
                await client.aclose()

    async def _alpaca_checks(self, client: httpx.AsyncClient) -> list[CheckResult]:
        key = self.env.get("ALPACA_API_KEY", "")
        secret = self.env.get("ALPACA_API_SECRET", "")
        if not key or not secret:
            hint = "Alpaca credentials are missing or empty"
            return [
                CheckResult("FAIL", "ALPACA_IEX_QUOTE", hint),
                CheckResult("FAIL", "ALPACA_SIP_BARS", hint),
                CheckResult("FAIL", "ALPACA_PAPER_CLOCK", hint),
            ]
        headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}
        results = [
            await self._data_get(
                client,
                code="ALPACA_IEX_QUOTE",
                path="/v2/stocks/SPY/quotes/latest",
                headers=headers,
                params={"feed": "iex"},
                success_hint="IEX latest quote endpoint returned HTTP 200",
            )
        ]
        now = _aware(self.now())
        lag = _positive_int(self.env.get("HISTORICAL_LAG_MINUTES"), 16)
        end = now - timedelta(minutes=max(16, lag))
        results.append(
            await self._data_get(
                client,
                code="ALPACA_SIP_BARS",
                path="/v2/stocks/SPY/bars",
                headers=headers,
                params={
                    "timeframe": "1Day",
                    "start": (end - timedelta(days=7)).isoformat(),
                    "end": end.isoformat(),
                    "feed": "sip",
                    "adjustment": "raw",
                },
                success_hint="delayed SIP daily bars endpoint returned HTTP 200",
            )
        )
        await self.limiter.acquire()
        try:
            response = await client.get(
                f"{PAPER_API_BASE_URL}/v2/clock",
                headers=headers,
            )
            if response.status_code in {401, 403}:
                results.append(
                    CheckResult(
                        "FAIL",
                        "ALPACA_PAPER_CLOCK",
                        "PAPER_KEYS_REQUIRED: credentials are not valid for Alpaca Paper",
                    )
                )
            else:
                response.raise_for_status()
                results.append(
                    CheckResult(
                        "PASS",
                        "ALPACA_PAPER_CLOCK",
                        "Alpaca Paper clock returned HTTP 200",
                    )
                )
        except DEPENDENCY_ERRORS as exc:
            results.append(CheckResult("FAIL", "ALPACA_PAPER_CLOCK", self._error_hint(exc)))
        return results

    async def _data_get(
        self,
        client: httpx.AsyncClient,
        *,
        code: str,
        path: str,
        headers: dict[str, str],
        params: dict[str, Any],
        success_hint: str,
    ) -> CheckResult:
        await self.limiter.acquire()
        try:
            response = await client.get(
                f"{DATA_API_BASE_URL}{path}",
                headers=headers,
                params=params,
            )
            if response.status_code == 403:
                return CheckResult(
                    "FAIL",
                    code,
                    "SUBSCRIPTION_REQUIRED: Alpaca data access was denied",
                )
            response.raise_for_status()
            return CheckResult("PASS", code, success_hint)
        except DEPENDENCY_ERRORS as exc:
            return CheckResult("FAIL", code, self._error_hint(exc))

    async def _telegram_checks(
        self,
        client: httpx.AsyncClient,
        *,
        allow_send: bool,
    ) -> list[CheckResult]:
        token = self.env.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = self.env.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            hint = "Telegram credentials are missing or empty"
            return [
                CheckResult("FAIL", "TELEGRAM_GET_ME", hint),
                CheckResult("FAIL", "TELEGRAM_SEND_MESSAGE", hint),
            ]
        base = f"{TELEGRAM_API_BASE_URL}/bot{token}"
        results: list[CheckResult] = []
        await self.limiter.acquire()
        try:
            response = await client.get(f"{base}/getMe")
            response.raise_for_status()
            payload = response.json()
            if payload.get("ok") is not True or not isinstance(payload.get("result"), dict):
                raise ValueError("TELEGRAM_RESPONSE_INVALID")
            bot_name = payload["result"].get("username") or payload["result"].get("first_name")
            results.append(
                CheckResult(
                    "PASS",
                    "TELEGRAM_GET_ME",
                    f"authenticated bot {bot_name or 'UNKNOWN'}",
                )
            )
        except DEPENDENCY_ERRORS as exc:
            results.append(CheckResult("FAIL", "TELEGRAM_GET_ME", self._error_hint(exc)))

        if not allow_send or results[-1].status == "FAIL":
            results.append(
                CheckResult(
                    "SKIP",
                    "TELEGRAM_SEND_MESSAGE",
                    "preflight prerequisites failed; no OK message was sent",
                )
            )
            return results

        await self.limiter.acquire()
        timestamp = _aware(self.now()).strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            response = await client.post(
                f"{base}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": f"MARKET BRAIN preflight OK {timestamp}",
                },
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("ok") is not True:
                raise ValueError("TELEGRAM_RESPONSE_INVALID")
            results.append(
                CheckResult(
                    "PASS",
                    "TELEGRAM_SEND_MESSAGE",
                    "fixed preflight message was accepted",
                )
            )
        except DEPENDENCY_ERRORS as exc:
            results.append(CheckResult("FAIL", "TELEGRAM_SEND_MESSAGE", self._error_hint(exc)))
        return results

    def _error_hint(self, exc: BaseException) -> str:
        safe = _safe_error(exc)
        if isinstance(exc, httpx.HTTPStatusError):
            safe = f"HTTPStatusError HTTP {exc.response.status_code}"
        return str(_redact(safe, self.secrets))


def _schema_tables(path: Path) -> set[str]:
    text = path.read_text()
    tables = set(
        re.findall(
            r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([a-zA-Z_][a-zA-Z0-9_]*)",
            text,
            flags=re.IGNORECASE,
        )
    )
    if not tables:
        raise RuntimeError("SCHEMA_TABLES_NOT_FOUND")
    return tables


def _exact_policy(
    env: Mapping[str, str],
    name: str,
    expected: str,
    code: str,
    hint: str,
) -> CheckResult:
    passed = env.get(name, "").strip().lower() == expected
    return CheckResult("PASS" if passed else "FAIL", code, hint)


def _integer_policy(
    env: Mapping[str, str],
    name: str,
    code: str,
    predicate: Callable[[int], bool],
    hint: str,
) -> CheckResult:
    try:
        passed = predicate(int(env.get(name, "")))
    except ValueError:
        passed = False
    return CheckResult("PASS" if passed else "FAIL", code, hint)


def _positive_int(raw: str | None, default: int) -> int:
    try:
        value = int(raw or "")
    except ValueError:
        return default
    return value if value > 0 else default


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


async def _quiet_close(probe) -> None:
    try:
        await probe.close()
    except DEPENDENCY_ERRORS:
        return


async def async_main(*, offline: bool = False) -> int:
    results = await PreflightRunner().run(offline=offline)
    for result in results:
        print(result.render())
    return 1 if any(result.status == "FAIL" for result in results) else 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Market Brain server preflight")
    parser.add_argument(
        "--offline",
        action="store_true",
        help="skip Alpaca and Telegram requests",
    )
    args = parser.parse_args()
    raise SystemExit(asyncio.run(async_main(offline=args.offline)))


if __name__ == "__main__":
    main()

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from market_brain.ops.preflight import (
    REQUIRED_ENV,
    CheckResult,
    PreflightRunner,
    _schema_tables,
)
from market_brain.providers.rate_limit import TokenBucketRateLimiter

ROOT = Path(__file__).resolve().parents[1]
FIXED_NOW = datetime(2026, 8, 29, 20, 0, tzinfo=UTC)


def full_env() -> dict[str, str]:
    return {
        "ALPACA_API_KEY": "alpaca-key-private",
        "ALPACA_API_SECRET": "alpaca-secret-private",
        "DATA_PLAN": "free",
        "DIRECT_ACCOUNT_ACCESS_ALLOWED": "false",
        "EXECUTION_ACTIONS_ALLOWED": "false",
        "HISTORICAL_LAG_MINUTES": "16",
        "NATS_URL": "nats://nats:4222",
        "POSTGRES_DSN": "postgresql://market:private@postgres:5432/market",
        "POSTGRES_PASSWORD": "postgres-private",
        "RUN_MODE": "shadow",
        "REST_SAFE_CALLS_PER_MINUTE": "180",
        "STREAM_MAX_SYMBOLS": "30",
        "STREAM_STALE_ALERT_SECONDS": "120",
        "TELEGRAM_BOT_TOKEN": "telegram-token-private",
        "TELEGRAM_CHAT_ID": "telegram-chat-private",
    }


class FakePostgresProbe:
    def __init__(
        self,
        *,
        tables: set[str] | None = None,
        differences: list[str] | None = None,
        connect_error: BaseException | None = None,
    ) -> None:
        self.tables = tables if tables is not None else _schema_tables(ROOT / "config/schema.sql")
        self.differences = differences or []
        self.connect_error = connect_error
        self.closed = False

    async def connect(self) -> None:
        if self.connect_error is not None:
            raise self.connect_error

    async def table_names(self) -> set[str]:
        return self.tables

    async def replay_differences(self) -> list[str]:
        return self.differences

    async def close(self) -> None:
        self.closed = True


class FakeNatsProbe:
    def __init__(self, *, connect_error: BaseException | None = None) -> None:
        self.connect_error = connect_error
        self.closed = False

    async def connect(self) -> None:
        if self.connect_error is not None:
            raise self.connect_error

    async def connect_jetstream(self) -> None:
        return None

    async def close(self) -> None:
        self.closed = True

    def __getattr__(self, name: str):
        raise AssertionError(f"read-only NATS preflight accessed unexpected method: {name}")


def build_runner(
    *,
    env: dict[str, str] | None = None,
    handler=None,
    postgres: FakePostgresProbe | None = None,
    nats_probe: FakeNatsProbe | None = None,
) -> tuple[PreflightRunner, FakePostgresProbe, FakeNatsProbe, httpx.AsyncClient | None]:
    pg = postgres or FakePostgresProbe()
    nats_instance = nats_probe or FakeNatsProbe()
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler)) if handler else None
    runner = PreflightRunner(
        env=env or full_env(),
        postgres_factory=lambda _dsn: pg,
        nats_factory=lambda _url: nats_instance,
        client=client,
        limiter=TokenBucketRateLimiter(180),
        now=lambda: FIXED_NOW,
    )
    return runner, pg, nats_instance, client


def by_code(results: list[CheckResult]) -> dict[str, CheckResult]:
    return {result.code: result for result in results}


@pytest.mark.asyncio
async def test_offline_preflight_passes_internal_checks_and_sends_no_http():
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        raise AssertionError("offline preflight attempted an external request")

    runner, pg, nats_probe, client = build_runner(handler=handler)
    try:
        results = await runner.run(offline=True)
    finally:
        assert client is not None
        await client.aclose()

    assert not [result for result in results if result.status == "FAIL"]
    assert sum(result.status == "SKIP" for result in results) == 5
    assert requests == []
    assert pg.closed is True
    assert nats_probe.closed is True


@pytest.mark.asyncio
async def test_online_preflight_uses_required_endpoints_and_fixed_telegram_message():
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/getMe"):
            return httpx.Response(200, json={"ok": True, "result": {"username": "shadow_bot"}})
        if request.url.path.endswith("/sendMessage"):
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 1}})
        return httpx.Response(200, json={})

    runner, _pg, _nats, client = build_runner(handler=handler)
    try:
        results = await runner.run()
    finally:
        assert client is not None
        await client.aclose()

    assert not [result for result in results if result.status == "FAIL"]
    urls = [str(request.url) for request in requests]
    assert any("/v2/stocks/SPY/quotes/latest?feed=iex" in url for url in urls)
    bars_request = next(request for request in requests if request.url.path.endswith("/SPY/bars"))
    assert bars_request.url.params["feed"] == "sip"
    assert bars_request.url.params["timeframe"] == "1Day"
    assert datetime.fromisoformat(bars_request.url.params["end"]) <= FIXED_NOW - timedelta(minutes=16)
    assert any(request.url.host == "paper-api.alpaca.markets" for request in requests)
    send = next(request for request in requests if request.url.path.endswith("/sendMessage"))
    payload = json.loads(send.content)
    assert payload == {
        "chat_id": "telegram-chat-private",
        "text": "MARKET BRAIN preflight OK 2026-08-29T20:00:00Z",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [401, 403])
async def test_paper_clock_auth_failure_is_reported_as_paper_keys_required(status_code: int):
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "paper-api.alpaca.markets":
            return httpx.Response(status_code, json={"message": "unauthorized"})
        if request.url.path.endswith("/getMe"):
            return httpx.Response(200, json={"ok": True, "result": {"username": "bot"}})
        return httpx.Response(200, json={"ok": True})

    runner, _pg, _nats, client = build_runner(handler=handler)
    try:
        result = by_code(await runner.run())["ALPACA_PAPER_CLOCK"]
    finally:
        assert client is not None
        await client.aclose()

    assert result.status == "FAIL"
    assert "PAPER_KEYS_REQUIRED" in result.hint


@pytest.mark.asyncio
async def test_http_status_error_includes_only_status_code_and_no_secret():
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/getMe"):
            return httpx.Response(500, text="telegram-token-private must never be printed")
        return httpx.Response(200, json={})

    runner, _pg, _nats, client = build_runner(handler=handler)
    try:
        results = await runner.run()
    finally:
        assert client is not None
        await client.aclose()

    result = by_code(results)["TELEGRAM_GET_ME"]
    rendered = "\n".join(row.render() for row in results)
    assert result.hint == "HTTPStatusError HTTP 500"
    assert "telegram-token-private" not in rendered
    assert "api.telegram.org" not in rendered


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_path", ["/v2/stocks/SPY/quotes/latest", "/v2/stocks/SPY/bars"])
async def test_data_403_is_reported_as_subscription_required(failed_path: str):
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == failed_path:
            return httpx.Response(403, json={"message": "forbidden"})
        if request.url.path.endswith("/getMe"):
            return httpx.Response(200, json={"ok": True, "result": {"username": "bot"}})
        return httpx.Response(200, json={"ok": True})

    runner, _pg, _nats, client = build_runner(handler=handler)
    code = "ALPACA_IEX_QUOTE" if failed_path.endswith("latest") else "ALPACA_SIP_BARS"
    try:
        result = by_code(await runner.run())[code]
    finally:
        assert client is not None
        await client.aclose()

    assert result.status == "FAIL"
    assert "SUBSCRIPTION_REQUIRED" in result.hint


@pytest.mark.asyncio
async def test_http_timeout_is_safe_and_redacts_token_bearing_url():
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/getMe"):
            raise httpx.ReadTimeout(
                f"timeout at {request.url}",
                request=request,
            )
        if request.url.path.endswith("/sendMessage"):
            return httpx.Response(200, json={"ok": True})
        return httpx.Response(200, json={})

    runner, _pg, _nats, client = build_runner(handler=handler)
    try:
        results = await runner.run()
    finally:
        assert client is not None
        await client.aclose()

    result = by_code(results)["TELEGRAM_GET_ME"]
    rendered = "\n".join(row.render() for row in results)
    assert result.status == "FAIL"
    assert result.hint == "ReadTimeout"
    assert "telegram-token-private" not in rendered
    assert "alpaca-secret-private" not in rendered
    assert "telegram-chat-private" not in rendered


@pytest.mark.asyncio
async def test_missing_and_policy_invalid_env_fail_without_printing_values():
    env = full_env()
    del env["ALPACA_API_SECRET"]
    env["HISTORICAL_LAG_MINUTES"] = "15"
    env["STREAM_MAX_SYMBOLS"] = "31"
    runner, _pg, _nats, _client = build_runner(env=env)

    results = await runner.run(offline=True)
    mapped = by_code(results)
    rendered = "\n".join(row.render() for row in results)

    assert mapped["ENV_ALPACA_API_SECRET"].hint == "MISSING"
    assert mapped["HISTORICAL_LAG_SAFE"].status == "FAIL"
    assert mapped["STREAM_SYMBOL_CAP_SAFE"].status == "FAIL"
    assert "15" not in rendered
    assert "31" not in rendered
    assert "alpaca-key-private" not in rendered
    assert set(REQUIRED_ENV) <= {code.removeprefix("ENV_") for code in mapped if code.startswith("ENV_")}


@pytest.mark.asyncio
async def test_postgres_and_nats_connection_failures_are_fail_closed():
    pg = FakePostgresProbe(connect_error=ConnectionError("database private detail"))
    nats_probe = FakeNatsProbe(connect_error=OSError("nats private detail"))
    runner, _pg, _nats, _client = build_runner(postgres=pg, nats_probe=nats_probe)

    mapped = by_code(await runner.run(offline=True))

    assert mapped["POSTGRES_CONNECTION"].status == "FAIL"
    assert mapped["POSTGRES_CONNECTION"].hint == "ConnectionError"
    assert mapped["POSTGRES_SCHEMA"].status == "SKIP"
    assert mapped["NATS_CONNECTION"].status == "FAIL"
    assert mapped["NATS_CONNECTION"].hint == "OSError"
    assert mapped["NATS_JETSTREAM"].status == "SKIP"


@pytest.mark.asyncio
async def test_schema_and_replay_mismatches_fail_independently():
    tables = _schema_tables(ROOT / "config/schema.sql") - {"alerts"}
    pg = FakePostgresProbe(tables=tables, differences=["wallet"])
    runner, _pg, _nats, _client = build_runner(postgres=pg)

    mapped = by_code(await runner.run(offline=True))

    assert mapped["POSTGRES_CONNECTION"].status == "PASS"
    assert mapped["POSTGRES_SCHEMA"].status == "FAIL"
    assert mapped["POSTGRES_SCHEMA"].hint == "missing tables: alerts"
    assert mapped["POSTGRES_REPLAY"].status == "FAIL"


def test_host_script_is_beginner_readable_and_calls_internal_preflight():
    script = (ROOT / "scripts/preflight.sh").read_text()
    assert "set -euo pipefail" in script
    assert "stat -c '%a' .env" in script
    assert "git check-ignore -q .env" in script
    assert "docker info" in script
    assert "docker compose config -q" in script
    assert "ss -H -lntp" in script
    assert "docker compose run --rm api python -m market_brain.ops.preflight" in script
    assert script.count("# ") >= 7
    assert "PREFLIGHT=PASS" in script
    assert "PREFLIGHT=FAIL" in script


def test_compose_smoke_requires_offline_preflight():
    script = (ROOT / "scripts/compose_smoke.sh").read_text()
    assert "python -m market_brain.ops.preflight --offline" in script
    assert 'echo "PREFLIGHT_OFFLINE=PASS"' in script

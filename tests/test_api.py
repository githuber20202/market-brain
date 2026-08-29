import pytest
from fastapi.testclient import TestClient

import market_brain.api.main as api_main
from market_brain.ledger.store import InMemoryEventStore

app = api_main.app


def test_health_and_policy():
    client = TestClient(app)
    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["architecture"] == "BROKERLESS_EVENT_SOURCED"
    policy = client.get("/policy")
    assert policy.status_code == 200
    assert policy.json()["automatic_execution"] is False


def test_screen_contract_rejects_empty_symbols():
    client = TestClient(app)
    response = client.post("/screen", json={"symbols": [], "top_n": 10})
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_health_exposes_stream_stale(monkeypatch):
    store = InMemoryEventStore()
    await store.set_runtime_status("stream_stale", True)
    monkeypatch.setattr(api_main.service, "store", store)

    result = await api_main.health()

    assert result["stream_stale"] is True
    assert result["run_mode"] in {"shadow", "live"}

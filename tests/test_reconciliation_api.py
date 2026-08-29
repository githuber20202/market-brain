from fastapi.testclient import TestClient

import market_brain.api.main as api_main
from market_brain.ledger.store import InMemoryEventStore
from market_brain.orchestration.service import DecisionService
from market_brain.settings import Settings


def make_service() -> DecisionService:
    return DecisionService(InMemoryEventStore(), cfg=Settings())


def test_positions_import_creates_manual_import_and_exposes_reconciliation(monkeypatch):
    service = make_service()
    monkeypatch.setattr(api_main, "service", service)
    with TestClient(api_main.app) as client:
        assert client.post(
            "/wallet/seed", json={"capital_base": 10000, "cash_available": 10000}
        ).status_code == 200
        response = client.post(
            "/positions/import",
            json={
                "symbol": "abc",
                "quantity": 10,
                "average_fill": 100.0,
                "stop_order_price": 95.0,
                "broker_order_ref": "stop-1",
            },
        )
        positions = client.get("/positions")

    assert response.status_code == 200
    body = response.json()
    assert body["source"] == "MANUAL_IMPORT"
    assert body["protection"] == "PROTECTED"
    assert body["reconciliation_state"] == "RECONCILED"
    assert body["last_reconciled_at"] is not None
    assert positions.status_code == 200
    assert positions.json()[0]["reconciliation_state"] == "RECONCILED"


def test_reconcile_exact_match_updates_get_positions(monkeypatch):
    service = make_service()
    monkeypatch.setattr(api_main, "service", service)
    with TestClient(api_main.app) as client:
        client.post(
            "/wallet/seed", json={"capital_base": 10000, "cash_available": 10000}
        )
        imported = client.post(
            "/positions/import",
            json={
                "symbol": "ABC",
                "quantity": 10,
                "average_fill": 100.0,
                "stop_order_price": 95.0,
            },
        ).json()
        response = client.post("/reconcile", json=[{"symbol": "ABC", "quantity": 10}])
        positions = client.get("/positions").json()

    assert response.status_code == 200
    assert response.json()["reconciled_symbols"] == ["ABC"]
    row = next(item for item in positions if item["position_id"] == imported["position_id"])
    assert row["reconciliation_state"] == "RECONCILED"
    assert row["last_reconciled_at"] is not None


def test_reconcile_unknown_holding_does_not_create_twin(monkeypatch):
    service = make_service()
    monkeypatch.setattr(api_main, "service", service)
    with TestClient(api_main.app) as client:
        client.post(
            "/wallet/seed", json={"capital_base": 10000, "cash_available": 10000}
        )
        response = client.post("/reconcile", json=[{"symbol": "XYZ", "quantity": 5}])
        positions = client.get("/positions")

    assert response.status_code == 200
    assert response.json()["unknown_holdings"] == [{"symbol": "XYZ", "quantity": 5}]
    assert positions.json() == []


def test_reconcile_rejects_invalid_quantity(monkeypatch):
    service = make_service()
    monkeypatch.setattr(api_main, "service", service)
    with TestClient(api_main.app) as client:
        response = client.post("/reconcile", json=[{"symbol": "ABC", "quantity": 0}])
    assert response.status_code == 422


from pathlib import Path

import yaml

from market_brain.settings import RISK_ENVELOPE, V4_MANIFEST

ROOT = Path(__file__).resolve().parents[1]


def test_runtime_contains_no_ibkr_reference():
    paths = list((ROOT / "src").rglob("*.py")) + [
        path for path in (ROOT / "config").rglob("*") if path.is_file()
    ]
    matches = [
        str(path.relative_to(ROOT))
        for path in paths
        if "ibkr" in path.read_text(errors="ignore").lower()
    ]
    assert matches == []


def test_execution_and_account_access_are_disabled():
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    for service in ("api", "stream-worker"):
        env = compose["services"][service]["environment"]
        assert env["EXECUTION_ACTIONS_ALLOWED"] == "false"
        assert env["DIRECT_ACCOUNT_ACCESS_ALLOWED"] == "false"


def test_runtime_loaded_contract_is_brokerless_and_manual_execution_only():
    assert V4_MANIFEST["invariants"]["account_connectivity"] == "NONE"
    assert V4_MANIFEST["invariants"]["automatic_execution"] is False
    assert V4_MANIFEST["invariants"]["portfolio_state"] == "USER_CONFIRMED_EVENT_LEDGER"
    assert RISK_ENVELOPE["automatic_execution_allowed"] is False


import json
from pathlib import Path

from market_brain.settings import RISK_ENVELOPE, V4_MANIFEST, Settings

ROOT = Path(__file__).resolve().parents[1]


def test_config_directory_contains_only_runtime_loaded_contract():
    expected = {"00-V4_MANIFEST.json", *V4_MANIFEST["canonical_files"]}
    actual = {path.name for path in (ROOT / "config").iterdir() if path.is_file()}
    assert actual == expected
    assert "05-RUNTIME_CONFIG.json" not in actual


def test_runtime_defaults_match_risk_envelope_without_duplicate_env_defaults(monkeypatch):
    monkeypatch.delenv("MAX_POSITION_NOTIONAL_PCT", raising=False)
    monkeypatch.delenv("MAX_CONCURRENT_POSITIONS", raising=False)
    cfg = Settings(_env_file=None)
    assert cfg.max_position_notional_pct == RISK_ENVELOPE["max_position_notional_pct"]
    assert cfg.max_concurrent_positions == RISK_ENVELOPE["max_concurrent_positions"]
    env_text = (ROOT / ".env.example").read_text()
    assert "MAX_POSITION_NOTIONAL_PCT=" not in env_text
    assert "MAX_CONCURRENT_POSITIONS=" not in env_text


def test_manifest_declares_how_every_remaining_config_is_loaded():
    loading = V4_MANIFEST["runtime_loading"]
    for name in {"00-V4_MANIFEST.json", *V4_MANIFEST["canonical_files"]}:
        assert name in loading
        assert loading[name]


def test_readme_lists_real_modes_api_and_not_removed_planned_endpoints():
    readme = (ROOT / "README.md").read_text()
    for real in (
        "GET /health",
        "GET /policy",
        "POST /plans",
        "POST /fills/confirm",
        "POST /reconcile",
        "POST /positions/{position_id}/exit",
    ):
        assert real in readme
    for stale in (
        "/portfolio/reconcile",
        "/executions/ack-buy",
        "/executions/ack-sell",
        "/trade-intents/evaluate-and-issue",
        "/architecture/invariants",
    ):
        assert stale not in readme
    assert "GitHub Actions keyless" in readme
    assert "SEC EDGAR חסום" in readme
    assert "אינו ייעוץ פיננסי" in readme
    assert "READY" not in readme


def test_release_manifest_cannot_claim_ready_or_old_fixed_test_count():
    release = json.loads((ROOT / "RELEASE_MANIFEST.json").read_text())
    assert release["status"] == "CI_VALIDATED_NOT_LIVE"
    assert "pytest" in release["validation"]["gates"]
    assert "24/24" not in json.dumps(release)
    assert release["risk_envelope"] == "config/02-RISK_ENVELOPE.json"


def test_compose_smoke_excludes_live_stream_worker():
    smoke = (ROOT / "scripts/compose_smoke.sh").read_text()
    assert "up -d --build postgres nats api" in smoke
    assert "stream-worker" not in smoke
    assert "COMPOSE_SMOKE=PASS" in smoke

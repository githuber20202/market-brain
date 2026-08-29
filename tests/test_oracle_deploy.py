from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_official_ci_has_blocking_arm64_build():
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
    assert "docker/setup-qemu-action@v3" in workflow
    assert "docker/setup-buildx-action@v3" in workflow
    assert "docker buildx build --platform linux/arm64" in workflow
    assert "continue-on-error" not in workflow


def test_stream_worker_and_api_share_postgres_in_compose():
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    api = compose["services"]["api"]
    worker = compose["services"]["stream-worker"]
    assert "./reports:/app/reports" in api["volumes"]
    assert worker["environment"]["POSTGRES_DSN"] == (
        "postgresql://market:${POSTGRES_PASSWORD:-market}@postgres:5432/market"
    )
    assert worker["depends_on"]["postgres"]["condition"] == "service_healthy"


def test_backup_is_custom_format_validated_and_retains_fourteen_days():
    script = (ROOT / "scripts" / "backup_postgres.sh").read_text()
    assert "set -Eeuo pipefail" in script
    assert "pg_dump" in script
    assert "--format=custom" in script
    assert "pg_restore --list" in script
    assert "-mtime +13 -delete" in script
    assert "POSTGRES_PASSWORD" not in script


def test_deploy_kit_contains_systemd_timer_and_logrotate_contracts():
    deploy = ROOT / "deploy" / "oracle-free"
    service = (deploy / "market-brain.service").read_text()
    timer = (deploy / "market-brain-backup.timer").read_text()
    logrotate = (deploy / "market-brain.logrotate").read_text()
    assert "docker compose --profile live up --build" in service
    assert "OnCalendar=*-*-* 02:30:00" in timer
    assert "Persistent=true" in timer
    assert "rotate 14" in logrotate
    assert "daily" in logrotate


def test_beginner_guide_covers_required_zero_cost_and_success_checks():
    guide = (ROOT / "deploy" / "oracle-free" / "README.md").read_text()
    for required in (
        "Ubuntu 24.04",
        "aarch64",
        "VM.Standard.A1.Flex",
        "Always Free-eligible",
        "Pay As You Go",
        "Home Region",
        "Destination port: `22`",
        "docker compose --profile live up",
        "market-brain.service",
        "backup_postgres.sh",
        "logrotate",
        "הצלחה:",
    ):
        assert required in guide
    assert guide.count("הצלחה:") >= 20
    assert "אין להעביר את הערכים דרך אדם אחר" in guide

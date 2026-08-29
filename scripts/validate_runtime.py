from __future__ import annotations

import sys
from pathlib import Path

import yaml

from market_brain.settings import settings

ROOT = Path(__file__).resolve().parents[1]
errors: list[str] = []

try:
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    services = compose.get("services", {})
    required = {"api", "stream-worker", "postgres", "nats"}
    missing = required - set(services)
    if missing:
        errors.append(f"COMPOSE_MISSING={sorted(missing)}")
    for service_name in ("api", "stream-worker"):
        environment = services.get(service_name, {}).get("environment", {})
        if str(environment.get("EXECUTION_ACTIONS_ALLOWED", "")).lower() != "false":
            errors.append(f"AUTOMATIC_EXECUTION_NOT_DISABLED={service_name}")
        if str(environment.get("DIRECT_ACCOUNT_ACCESS_ALLOWED", "")).lower() != "false":
            errors.append(f"DIRECT_ACCOUNT_ACCESS_NOT_DISABLED={service_name}")
except Exception as exc:
    errors.append(f"COMPOSE_INVALID={exc}")

if settings.execution_actions_allowed:
    errors.append("AUTOMATIC_EXECUTION_MUST_BE_FALSE")
if settings.direct_account_access_allowed:
    errors.append("DIRECT_ACCOUNT_ACCESS_MUST_BE_FALSE")

for path in list((ROOT / "src").rglob("*.py")) + list((ROOT / "config").rglob("*")):
    if path.is_file() and "ibkr" in path.read_text(errors="ignore").lower():
        errors.append(f"FORBIDDEN_DIRECT_ACCOUNT_PROVIDER_REFERENCE={path.relative_to(ROOT)}")

print("RUNTIME_VALIDATION=PASS" if not errors else "RUNTIME_VALIDATION=FAIL")
for error in errors:
    print(error)
sys.exit(1 if errors else 0)


#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

failures=0

pass() {
  printf '[PASS] %s — %s\n' "$1" "$2"
}

fail() {
  printf '[FAIL] %s — %s\n' "$1" "$2"
  failures=$((failures + 1))
}

# Confirm that the operator created the private runtime environment file.
if [[ -f .env ]]; then
  pass "HOST_ENV_FILE" ".env exists"
else
  fail "HOST_ENV_FILE" "copy .env.example to .env and fill it locally"
fi

# Require owner-only permissions so another local user cannot read credentials.
if [[ -f .env ]] && [[ "$(stat -c '%a' .env)" == "600" ]]; then
  pass "HOST_ENV_PERMISSIONS" ".env permissions are 600"
else
  fail "HOST_ENV_PERMISSIONS" "run: chmod 600 .env"
fi

# Verify that Git refuses to track the credentials file.
if git check-ignore -q .env; then
  pass "HOST_ENV_GIT_IGNORE" ".env is ignored by Git"
else
  fail "HOST_ENV_GIT_IGNORE" ".env must be matched by .gitignore"
fi

# Ask the Docker daemon for information; this proves the service and socket work.
if docker info >/dev/null 2>&1; then
  pass "HOST_DOCKER" "Docker daemon is active"
else
  fail "HOST_DOCKER" "start Docker and verify membership in the docker group"
fi

# Render Compose after applying .env; invalid variables or YAML fail here.
if [[ -f .env ]] && docker compose config -q; then
  pass "HOST_COMPOSE_CONFIG" "Docker Compose configuration is valid"
else
  fail "HOST_COMPOSE_CONFIG" "fix .env or docker-compose.yml before startup"
fi

# Check only Market Brain service ports; they may be closed or bound to loopback.
unsafe_ports=""
if command -v ss >/dev/null 2>&1; then
  while IFS= read -r endpoint; do
    host="${endpoint%:*}"
    port="${endpoint##*:}"
    case "$port" in
      8080|5432|4222|8222)
        case "$host" in
          127.0.0.1|localhost|\[::1\]|::1) ;;
          *) unsafe_ports+=" ${endpoint}" ;;
        esac
        ;;
    esac
  done < <(ss -H -lntp | awk '{print $4}')
  if [[ -z "$unsafe_ports" ]]; then
    pass "HOST_PORT_BINDINGS" "service ports are closed or loopback-only"
  else
    fail "HOST_PORT_BINDINGS" "public service binding detected:${unsafe_ports}"
  fi
else
  fail "HOST_PORT_BINDINGS" "ss is unavailable; install the Ubuntu iproute2 package"
fi

# Run dependency and policy checks inside the same Compose network as the API.
if ((failures == 0)); then
  if docker compose run --rm api python -m market_brain.ops.preflight "$@"; then
    pass "INTERNAL_PREFLIGHT" "all required internal checks completed"
  else
    fail "INTERNAL_PREFLIGHT" "review the failed internal check above"
  fi
else
  fail "INTERNAL_PREFLIGHT" "host prerequisites failed; internal checks were not started"
fi

if ((failures > 0)); then
  printf 'PREFLIGHT=FAIL failures=%d\n' "$failures"
  exit 1
fi

echo "PREFLIGHT=PASS"


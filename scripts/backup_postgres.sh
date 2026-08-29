#!/usr/bin/env bash
set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project_dir="$(cd -- "${script_dir}/.." && pwd)"
backup_dir="${MARKET_BACKUP_DIR:-/var/backups/market-brain}"

install -d -m 0700 "${backup_dir}"
exec 9>"${backup_dir}/.backup.lock"
if ! flock -n 9; then
  echo "BACKUP_SKIPPED=ANOTHER_BACKUP_IS_RUNNING"
  exit 0
fi

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
temporary="$(mktemp "${backup_dir}/.market_${timestamp}.XXXXXX")"
final_path="${backup_dir}/market_${timestamp}.dump"
cleanup() {
  rm -f -- "${temporary}"
}
trap cleanup EXIT

cd "${project_dir}"
docker compose exec -T postgres pg_dump \
  --username=market \
  --dbname=market \
  --format=custom \
  --no-owner \
  --no-privileges >"${temporary}"

test -s "${temporary}"
docker compose exec -T postgres pg_restore --list <"${temporary}" >/dev/null
chmod 0600 "${temporary}"
mv -- "${temporary}" "${final_path}"
find "${backup_dir}" -mindepth 1 -maxdepth 1 -type f \
  -name 'market_*.dump' -mtime +13 -delete

echo "BACKUP_CREATED=${final_path}"


#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
python3 -c 'import sys; assert sys.version_info >= (3, 11), "Python 3.11+ required"'
python3 -B -m unittest discover -s cloud/tests -v
if [[ -d .market-core ]]; then
  python3 -B cloud/market_core.py verify
else
  echo 'Launcher ready. Private algorithm bundle is required before evaluation.'
fi

#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

npm run build
npm run test:compiled
bash scripts/runtime-smoke.sh
bash scripts/memory-smoke.sh

echo "validation ok"

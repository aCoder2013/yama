#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TIME_LIMIT="${TIME_LIMIT:-60}"
CONCURRENCY="${CONCURRENCY:-6}"
OPS_PER_KEY="${OPS_PER_KEY:-32}"

export YAMA_JEPSEN_PORTS_FILE="$ROOT/cluster-ports.edn"

if ! command -v lein >/dev/null 2>&1; then
  echo "Leiningen (lein) is required. Install from https://leiningen.org/"
  exit 1
fi

"$ROOT/scripts/local-cluster.sh" start

cleanup() {
  "$ROOT/scripts/local-cluster.sh" stop
}
trap cleanup EXIT

cd "$ROOT"
lein run test \
  --nodes n1,n2,n3 \
  --local \
  --time-limit "$TIME_LIMIT" \
  --concurrency "$CONCURRENCY" \
  --ops-per-key "$OPS_PER_KEY" \
  --test-count 1

echo "Results in $ROOT/store/latest/"

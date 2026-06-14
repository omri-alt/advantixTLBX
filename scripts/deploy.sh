#!/usr/bin/env bash
# Production deploy on a VPS (DigitalOcean, etc.). Do NOT use ``docker compose watch``.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ ! -f .env ]]; then
  echo "ERROR: Missing .env in $ROOT" >&2
  exit 1
fi
if [[ ! -f secrets/credentials.json ]]; then
  echo "ERROR: Missing secrets/credentials.json (Google service account for Sheets)." >&2
  echo "  mkdir -p secrets && cp /path/to/your-credentials.json secrets/credentials.json" >&2
  exit 1
fi

mkdir -p runtime data secrets

echo "Building image..."
docker compose build

echo "Starting container (detached)..."
docker compose up -d --remove-orphans

echo "Waiting for /health (up to 60s)..."
for _ in $(seq 1 30); do
  if curl -fsS "http://127.0.0.1:5000/health" >/dev/null 2>&1; then
    echo "Deploy OK."
    curl -fsS "http://127.0.0.1:5000/health"
    echo ""
    docker compose ps
    exit 0
  fi
  sleep 2
done

echo "ERROR: Health check failed. Recent logs:" >&2
docker compose logs --tail=80 klblend >&2
exit 1

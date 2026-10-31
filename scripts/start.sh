#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

if ! command -v docker >/dev/null 2>&1; then
  echo 'Docker is required. Install Docker Desktop, then run make start again.' >&2
  exit 1
fi
if ! docker compose version >/dev/null 2>&1; then
  echo 'Docker Compose v2 is required.' >&2
  exit 1
fi
if ! docker info >/dev/null 2>&1; then
  echo 'Docker is not available. Start Docker Desktop, then run make start again.' >&2
  exit 1
fi
if [[ ! -f .env ]]; then
  cp .env.example .env
  echo 'Created .env with local demo defaults.'
fi

docker compose up -d --build
printf '\nWaiting for the dashboard, database, and Airflow scheduler…\n'
for ((attempt=1; attempt<=90; attempt++)); do
  if docker compose exec -T dashboard python -c '
import json, urllib.request
with urllib.request.urlopen("http://localhost:8501/api/runs", timeout=3) as response:
    json.load(response)
with urllib.request.urlopen("http://webserver:8080/health", timeout=3) as response:
    health = json.load(response)
assert health["metadatabase"]["status"] == "healthy"
assert health["scheduler"]["status"] == "healthy"
' >/dev/null 2>&1; then
    printf '\nFilingFlow is ready.\nDashboard: http://localhost:8501\nAirflow:   http://localhost:8080 (admin / admin)\n\nRun a pipeline: make trigger\nStop the app:  make down\n'
    exit 0
  fi
  sleep 2
done
printf '\nServices did not become ready in time. Inspect: docker compose logs --tail 100\n' >&2
exit 1

#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Start Redis if needed: use systemctl when available and not already active, else start redis-server when not reachable
if command -v systemctl >/dev/null 2>&1; then
    if ! systemctl is-active redis-server >/dev/null 2>&1; then
        systemctl start redis-server 2>/dev/null || true
    fi
fi
if ! redis-cli ping >/dev/null 2>&1; then
    redis-server &
    sleep 1
fi

# Start the ingestion services
echo "Starting services..."
docker compose up -d --build

echo ""
echo "Waiting for services to initialize..."
sleep 10
# Ensure postgres user password is set so ingestion can connect (idempotent for existing volumes)
docker exec assignment-postgres psql -U postgres -d ingestion -c "ALTER USER postgres PASSWORD 'postgres';" 2>/dev/null || true
# Monitor progress
echo ""
echo "Monitoring ingestion progress..."
echo "(Press Ctrl+C to stop monitoring)"
echo "=============================================="

while true; do
    COUNT=$(docker exec assignment-postgres psql -U postgres -d ingestion -t -c "SELECT COUNT(*) FROM ingested_events;" 2>/dev/null | tr -d ' ' || echo "0")

    if docker logs assignment-ingestion 2>&1 | grep -q "ingestion complete" 2>/dev/null; then
        echo ""
        echo "=============================================="
        echo "INGESTION COMPLETE!"
        echo "Total events: $COUNT"
        echo "=============================================="
        exit 0
    fi

    echo "[$(date '+%H:%M:%S')] Events ingested: $COUNT"
    sleep 5
done

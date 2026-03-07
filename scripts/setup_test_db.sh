#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

PGUSER="${PGUSER:-postgres}"
PGPASSWORD="${PGPASSWORD:-postgres}"
PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"

export PGUSER PGPASSWORD PGHOST PGPORT

echo "==> Starting PostgreSQL via Docker Compose..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" up -d --wait

echo "==> Waiting for PostgreSQL to be ready..."
until pg_isready -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" > /dev/null 2>&1; do
  sleep 1
done

echo "==> Running test database setup..."
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -f "$PROJECT_DIR/test/setup.sql"

echo "==> Test database setup complete!"
echo ""
echo "Connection details:"
echo "  Host:     $PGHOST"
echo "  Port:     $PGPORT"
echo "  Database: postgleam_test"
echo "  User:     $PGUSER"

#!/usr/bin/env bash
# First-run setup for Superset: metadata DB, admin user, warehouse connection.
set -euo pipefail

superset db upgrade
superset fab create-admin --username admin --firstname Admin --lastname User \
  --email admin@example.com --password admin || true   # already exists on restarts
superset init
# Dashboards read through a read-only role
superset set-database-uri --database_name "Customer 360 Warehouse" \
  --uri "postgresql+psycopg2://bi_reader:bi_reader@postgres:5432/c360" || true

exec /usr/bin/run-server.sh

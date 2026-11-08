#!/usr/bin/env bash
# Check that every source system is reachable before a run. Exit 1 if any is down.
set -uo pipefail

PG_HOST="${PG_HOST:-postgres}"
MONGO_HOST="${MONGO_HOST:-mongo}"
KAFKA_HOST="${KAFKA_HOST:-kafka}"

status=0
check() {  # name host port
  if timeout 3 bash -c "</dev/tcp/$2/$3" 2>/dev/null; then
    printf '  %-10s %-10s ok\n' "$1" "$2:$3"
  else
    printf '  %-10s %-10s UNREACHABLE\n' "$1" "$2:$3"
    status=1
  fi
}

echo "Source systems:"
check postgres "$PG_HOST" 5432
check mongodb "$MONGO_HOST" 27017
check kafka "$KAFKA_HOST" 9092

# Lake disk usage, as an early warning before a volume fills up
if [[ -d "${C360_LAKE_PATH:-./lake}" ]]; then
  echo "Lake size: $(du -sh "${C360_LAKE_PATH:-./lake}" | cut -f1)"
fi
exit $status

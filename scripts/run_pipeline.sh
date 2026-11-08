#!/usr/bin/env bash
# Run one pipeline step (or `all`) the way a scheduler should:
#   * one run at a time per step (file lock)
#   * timestamped log file per day and step
#   * retries with back-off for transient failures (network, a busy database)
#   * NO retry when a data quality gate fails (exit 3): retrying won't fix bad data
#   * the churn model is trained automatically if `score` runs before any model exists
#
# Usage: scripts/run_pipeline.sh <step|all>      e.g.  scripts/run_pipeline.sh silver
# Exit codes: 0 ok · 3 data quality failure · 75 already running · other = step failed
set -Eeuo pipefail

STEP="${1:?usage: run_pipeline.sh <step|all>}"
MAX_RETRIES="${MAX_RETRIES:-2}"
BACKOFF_SECONDS="${BACKOFF_SECONDS:-30}"
LAKE="${C360_LAKE_PATH:-./lake}"
LOG_DIR="${C360_LOG_DIR:-./logs}"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/$(date +%Y%m%d)_${STEP}.log"

log() { echo "$(date '+%F %T') | run_pipeline | $*" | tee -a "$LOG_FILE"; }

exec 9>"/tmp/c360_${STEP}.lock"
if ! flock -n 9; then
  log "another '$STEP' run is in progress, exiting"
  exit 75
fi

if [[ "$STEP" == "score" && ! -f "$LAKE/models/churn_model.joblib" ]]; then
  log "no churn model found, training one first"
  "$0" train
fi

attempt=0
while true; do
  attempt=$((attempt + 1))
  log "starting '$STEP' (attempt $attempt of $((MAX_RETRIES + 1)))"
  set +e
  python -m c360 "$STEP" 2>&1 | tee -a "$LOG_FILE"
  rc=${PIPESTATUS[0]}
  set -e

  if [[ $rc -eq 0 ]]; then
    log "'$STEP' succeeded"
    exit 0
  elif [[ $rc -eq 3 ]]; then
    log "'$STEP' stopped by a data quality gate; not retrying (see $LOG_FILE)"
    exit 3
  elif [[ $attempt -gt $MAX_RETRIES ]]; then
    log "'$STEP' failed with exit code $rc after $attempt attempts"
    exit "$rc"
  fi
  wait_s=$((BACKOFF_SECONDS * attempt))
  log "'$STEP' failed with exit code $rc; retrying in ${wait_s}s"
  sleep "$wait_s"
done

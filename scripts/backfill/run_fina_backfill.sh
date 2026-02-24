#!/usr/bin/env bash
set -euo pipefail

# Batch backfill script for ods_fina_indicator YoY fields and ODS fina refill
# Location: scripts/backfill/run_fina_backfill.sh
# Usage: see README in same folder or run with --help

PROG_NAME=$(basename "$0")
LOG_DIR="logs"
LOG_FILE="$LOG_DIR/backfill_fina.log"

DEFAULT_PYTHON=".venv/bin/python"
DEFAULT_DB_NAME="tushare_stock"

usage() {
  cat <<EOF
Usage: $PROG_NAME [options]

Options:
  --start-year YYYY      Start year (default: 2010)
  --end-year YYYY        End year (default: 2026)
  --batch-years N        Years per batch (default: 5)
  --rate-limit N         TuShare rate limit per minute (default: use config/etl.ini)
  --token TOKEN          TuShare token (or set TUSHARE_TOKEN env)
  --db-user USER         MySQL user (default: root)
  --db-pass PASS         MySQL password (or set MYSQL_PASSWORD env)
  --db-host HOST         MySQL host (default: localhost)
  --db-name NAME         MySQL database (default: tushare_stock)
  --python PY            Python interpreter to run ETL (default: .venv/bin/python)
  --with-balancesheet    Also call balancesheet API (slower, but can backfill total_assets/total_hldr_eqy)
  --full-columns         Disable YoY-only mode and load full fina columns
  --full-ts-codes        Disable "only missing YoY" filter and scan all ts_code
  --active-ts-only       Only process stocks active in the target date range (list_date/delist_date filter)
  --commit-every N       Commit interval by ts_code (default: 100)
  --progress-every N     Progress log interval by ts_code (default: 100)
  --fail-fast            Abort batch immediately on first ts_code error
  --run-downstream       After each batch optionally run DWD/DWS/ADS if DWD dates given
  --dwd-start-date DATE  DWD start trade_date (YYYYMMDD) for downstream run
  --dwd-end-date DATE    DWD end trade_date (YYYYMMDD) for downstream run
  --retries N            Retry count for each batch (default: 3)
  --help                 Show this help

Example:
  $PROG_NAME --start-year 2010 --end-year 2026 --batch-years 5 --token "xxx" --db-user root --db-pass "pwd"
EOF
}

# defaults
START_YEAR=2010
END_YEAR=2026
BATCH_YEARS=5
TOKEN=${TUSHARE_TOKEN:-}
DB_USER=${DB_USER:-root}
DB_PASS=${MYSQL_PASSWORD:-}
DB_HOST=${DB_HOST:-localhost}
DB_NAME=${DB_NAME:-$DEFAULT_DB_NAME}
PYTHON=${PYTHON:-$DEFAULT_PYTHON}
RUN_DOWNSTREAM=0
DWD_START_DATE=""
DWD_END_DATE=""
RETRIES=3
RATE_LIMIT=""
SKIP_BALANCESHEET=1
YOY_ONLY=1
ONLY_MISSING_YOY=1
COMMIT_EVERY=100
PROGRESS_EVERY=100
CONTINUE_ON_ERROR=1
ACTIVE_TS_ONLY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --start-year) START_YEAR="$2"; shift 2;;
    --end-year) END_YEAR="$2"; shift 2;;
    --batch-years) BATCH_YEARS="$2"; shift 2;;
    --rate-limit) RATE_LIMIT="$2"; shift 2;;
    --token) TOKEN="$2"; shift 2;;
    --db-user) DB_USER="$2"; shift 2;;
    --db-pass) DB_PASS="$2"; shift 2;;
    --db-host) DB_HOST="$2"; shift 2;;
    --db-name) DB_NAME="$2"; shift 2;;
    --python) PYTHON="$2"; shift 2;;
    --with-balancesheet) SKIP_BALANCESHEET=0; shift 1;;
    --full-columns) YOY_ONLY=0; shift 1;;
    --full-ts-codes) ONLY_MISSING_YOY=0; shift 1;;
    --active-ts-only) ACTIVE_TS_ONLY=1; shift 1;;
    --commit-every) COMMIT_EVERY="$2"; shift 2;;
    --progress-every) PROGRESS_EVERY="$2"; shift 2;;
    --fail-fast) CONTINUE_ON_ERROR=0; shift 1;;
    --run-downstream) RUN_DOWNSTREAM=1; shift 1;;
    --dwd-start-date) DWD_START_DATE="$2"; shift 2;;
    --dwd-end-date) DWD_END_DATE="$2"; shift 2;;
    --retries) RETRIES="$2"; shift 2;;
    --help) usage; exit 0;;
    *) echo "Unknown option: $1"; usage; exit 2;;
  esac
done

mkdir -p "$LOG_DIR"
exec >> "$LOG_FILE" 2>&1

echo "==== Backfill run started at $(date -u +'%Y-%m-%d %H:%M:%S %Z') ===="
echo "Params: start_year=$START_YEAR end_year=$END_YEAR batch_years=$BATCH_YEARS retries=$RETRIES run_downstream=$RUN_DOWNSTREAM rate_limit=${RATE_LIMIT:-config_default} skip_balancesheet=$SKIP_BALANCESHEET yoy_only=$YOY_ONLY only_missing_yoy=$ONLY_MISSING_YOY active_ts_only=$ACTIVE_TS_ONLY commit_every=$COMMIT_EVERY progress_every=$PROGRESS_EVERY continue_on_error=$CONTINUE_ON_ERROR"

if [[ -z "$TOKEN" ]]; then
  echo "ERROR: TuShare token missing. Pass --token or set TUSHARE_TOKEN env var."; exit 3
fi

if [[ -z "$DB_PASS" ]]; then
  echo "WARNING: MYSQL password not provided via --db-pass or MYSQL_PASSWORD env; mysql may prompt.";
fi

# 1) Ensure DB schema has the YoY columns (compatible with older MySQL)
echo "Ensuring ods_fina_indicator has or_yoy and netprofit_yoy columns..."
# Query information_schema to decide what to alter
OR_EXISTS=$(mysql -u"$DB_USER" -p"$DB_PASS" -h"$DB_HOST" -D"$DB_NAME" -sNe "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='$DB_NAME' AND TABLE_NAME='ods_fina_indicator' AND COLUMN_NAME='or_yoy'" || echo "0")
NP_EXISTS=$(mysql -u"$DB_USER" -p"$DB_PASS" -h"$DB_HOST" -D"$DB_NAME" -sNe "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='$DB_NAME' AND TABLE_NAME='ods_fina_indicator' AND COLUMN_NAME='netprofit_yoy'" || echo "0")

ALTER_PARTS=()
if [[ "$OR_EXISTS" -eq 0 ]]; then
  ALTER_PARTS+=("ADD COLUMN or_yoy DECIMAL(12,6) NULL COMMENT '营业收入同比'")
fi
if [[ "$NP_EXISTS" -eq 0 ]]; then
  ALTER_PARTS+=("ADD COLUMN netprofit_yoy DECIMAL(12,6) NULL COMMENT '净利润同比'")
fi

if [[ ${#ALTER_PARTS[@]} -gt 0 ]]; then
  # join parts with comma
  IFS=','; PARTS_JOINED="${ALTER_PARTS[*]}"; unset IFS
  SQL="ALTER TABLE ods_fina_indicator ${PARTS_JOINED};"
  if ! mysql -u"$DB_USER" -p"$DB_PASS" -h"$DB_HOST" -D"$DB_NAME" -e "$SQL"; then
    echo "Failed to alter table; exiting."; exit 4
  fi
else
  echo "Columns already present, no ALTER needed."
fi

# Helper: run a command with retries
run_with_retries() {
  local cmd="$1"
  local attempts=0
  local max=$RETRIES
  local delay=5
  until [[ $attempts -ge $max ]]; do
    attempts=$((attempts+1))
    echo "Attempt $attempts/$max: $cmd"
    if eval "$cmd"; then
      echo "Command succeeded"
      return 0
    fi
    echo "Command failed; sleeping $delay s before retry..."
    sleep $delay
    delay=$((delay*2))
  done
  echo "Command failed after $max attempts"
  return 1
}

# 2) Batch loop per year range
year=$START_YEAR
while [[ $year -le $END_YEAR ]]; do
  end_batch=$((year + BATCH_YEARS - 1))
  if [[ $end_batch -gt $END_YEAR ]]; then
    end_batch=$END_YEAR
  fi
  echo "Processing years: $year - $end_batch"

  CMD="${PYTHON} scripts/sync/run_fina_yearly.py --start-year ${year} --end-year ${end_batch} --token \"${TOKEN}\""
  if [[ -n "$RATE_LIMIT" ]]; then
    CMD="${CMD} --rate-limit ${RATE_LIMIT}"
  fi
  if [[ "$SKIP_BALANCESHEET" -eq 1 ]]; then
    CMD="${CMD} --skip-balancesheet"
  fi
  if [[ "$YOY_ONLY" -eq 1 ]]; then
    CMD="${CMD} --yoy-only"
  fi
  if [[ "$ONLY_MISSING_YOY" -eq 1 ]]; then
    CMD="${CMD} --only-missing-yoy"
  fi
  if [[ "$ACTIVE_TS_ONLY" -eq 1 ]]; then
    CMD="${CMD} --active-ts-only"
  fi
  CMD="${CMD} --commit-every ${COMMIT_EVERY} --progress-every ${PROGRESS_EVERY}"
  if [[ "$CONTINUE_ON_ERROR" -eq 1 ]]; then
    CMD="${CMD} --continue-on-error"
  fi
  if ! run_with_retries "$CMD"; then
    echo "Batch $year-$end_batch failed permanently, aborting."; exit 5
  fi

  # Optional downstream runs
  if [[ "$RUN_DOWNSTREAM" -eq 1 && -n "$DWD_START_DATE" && -n "$DWD_END_DATE" ]]; then
    echo "Running downstream DWD/DWS/ADS for trade_date range $DWD_START_DATE - $DWD_END_DATE"
    run_with_retries "${PYTHON} scripts/sync/run_dwd.py --start-date ${DWD_START_DATE} --end-date ${DWD_END_DATE}" || { echo "DWD failed"; exit 6; }
    run_with_retries "${PYTHON} scripts/sync/run_dws.py --start-date ${DWD_START_DATE} --end-date ${DWD_END_DATE}" || { echo "DWS failed"; exit 7; }
    run_with_retries "${PYTHON} scripts/sync/run_ads.py --start-date ${DWD_START_DATE} --end-date ${DWD_END_DATE}" || { echo "ADS failed"; exit 8; }
  fi

  # advance
  year=$((end_batch + 1))
done

echo "==== Backfill run completed at $(date -u +'%Y-%m-%d %H:%M:%S %Z') ===="

exit 0

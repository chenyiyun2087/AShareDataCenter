#!/bin/bash

# Configuration
PROJECT_ROOT="/Users/chenyiyun/PycharmProjects/AShareDataCenter"
PYTHON_EXEC="${PROJECT_ROOT}/.venv/bin/python"
LOG_DIR="${PROJECT_ROOT}/logs"
DAILY_LOG="${LOG_DIR}/$(date +%Y%m%d).log"

cd "${PROJECT_ROOT}"

echo "==============================================================================" >> "${DAILY_LOG}"
echo "START 20:00 TASK: $(date '+%Y-%m-%d %H:%M:%S')" >> "${DAILY_LOG}"
echo "==============================================================================" >> "${DAILY_LOG}"

# 1. Run Daily Pipeline (Lenient mode)
echo -e "\n[$(date '+%H:%M:%S')] Phase 1: Running Daily Pipeline (Lenient)..." >> "${DAILY_LOG}"
"${PROJECT_ROOT}/.venv/bin/python" "${PROJECT_ROOT}/scripts/schedule/run_with_retry.py" --retries 3 --delay 300 -- "${PYTHON_EXEC}" "${PROJECT_ROOT}/scripts/sync/run_daily_pipeline.py" --config config/etl.ini --lenient >> "${DAILY_LOG}" 2>&1

# 2. Run Data Integrity Check (Whole Picture)
echo -e "\n[$(date '+%H:%M:%S')] Phase 2: Running Data Integrity Check (Whole Picture)..." >> "${DAILY_LOG}"
"${PYTHON_EXEC}" "${PROJECT_ROOT}/scripts/check/inspect_data_completeness.py" --config config/etl.ini --days 5 >> "${DAILY_LOG}" 2>&1

echo -e "\n==============================================================================" >> "${DAILY_LOG}"
echo "COMPLETE 20:00 TASK: $(date '+%Y-%m-%d %H:%M:%S')" >> "${DAILY_LOG}"
echo "==============================================================================" >> "${DAILY_LOG}"

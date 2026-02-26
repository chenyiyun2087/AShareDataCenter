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

# 1. Run 20:00 enhancement pipeline (dividend + fina + integrity)
echo -e "\n[$(date '+%H:%M:%S')] Phase 1: Running 20:00 Enhancement Pipeline..." >> "${DAILY_LOG}"
"${PROJECT_ROOT}/.venv/bin/python" "${PROJECT_ROOT}/scripts/schedule/run_with_retry.py" --retries 3 --delay 300 -- "${PYTHON_EXEC}" "${PROJECT_ROOT}/scripts/sync/run_2000_pipeline.py" --config config/etl.ini --lenient >> "${DAILY_LOG}" 2>&1

echo -e "\n==============================================================================" >> "${DAILY_LOG}"
echo "COMPLETE 20:00 TASK: $(date '+%Y-%m-%d %H:%M:%S')" >> "${DAILY_LOG}"
echo "==============================================================================" >> "${DAILY_LOG}"

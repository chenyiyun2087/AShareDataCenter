#!/bin/bash

# AShare Data Center - Data Repair Script (Baseline 2020-01-02)
# Usage: ./scripts/tools/repair_data_2020.sh

set -e  # Exit on error

# Configuration
START_DATE=20200102
END_DATE=$(date +%Y%m%d)
PYTHON_EXEC="$HOME/PycharmProjects/AShareDataCenter/.venv/bin/python"

echo "========================================================"
echo "Starting Data Repair (Baseline: $START_DATE - $END_DATE)"
echo "========================================================"

export PYTHONPATH=$PYTHONPATH:$(pwd)/scripts

# 1. ODS Basic (Daily, Weekly, Monthly, Adj Factor, Daily Basic)
echo "[1/4] Running ODS Basic Sync..."
$PYTHON_EXEC scripts/sync/run_ods.py --mode incremental --start-date $START_DATE --end-date $END_DATE --config config/etl.ini

# 2. ODS Features (Margin, Moneyflow, etc.)
# Note: 'cyq_chips' is excluded by default as it is extremely API intensive (1 request per stock per day).
# Only 'cyq_perf' (daily summary) is included.
echo "[2/4] Running ODS Features Sync..."
$PYTHON_EXEC scripts/sync/run_ods_features.py \
    --start-date $START_DATE \
    --end-date $END_DATE \
    --config config/etl.ini \
    --apis "margin,margin_detail,moneyflow,moneyflow_ths,cyq_perf,stk_factor"

# 3. DWD Sync
echo "[3/4] Running DWD Sync..."
$PYTHON_EXEC scripts/sync/run_dwd.py --mode incremental --start-date $START_DATE --end-date $END_DATE --config config/etl.ini

# 4. DWS Sync (Batch Mode)
echo "[4/4] Running DWS Sync (Batch Mode)..."
# This typically takes 10-20 minutes for 1 year of data with the new optimization.
$PYTHON_EXEC scripts/sync/run_dws.py --mode incremental --start-date $START_DATE --end-date $END_DATE --config config/etl.ini

echo "========================================================"
echo "Data Repair Completed Successfully!"
echo "========================================================"

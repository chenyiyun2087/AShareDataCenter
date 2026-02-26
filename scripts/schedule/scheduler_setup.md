# Pipeline Scheduling Setup

To automate your data pipeline, we will use `crontab` on macOS.

## 1. Verify Environment

Ensure the following paths are correct (adjust if your project path differs):
- Project Root: `/Users/chenyiyun/PycharmProjects/AShareDataCenter`
- Python Executable: `/Users/chenyiyun/PycharmProjects/AShareDataCenter/.venv/bin/python`
- Config File: `/Users/chenyiyun/PycharmProjects/AShareDataCenter/config/etl.ini`

## 2. Generate Crontab Commands

Run `crontab -e` in your terminal to edit cron jobs, and append the following lines:

```bash
# ==============================================================================
# AShare Data Center Pipeline Schedule
# ==============================================================================

# 1. 17:00 Afternoon Core Pipeline
# Retry 3 times, delay 5 minutes between retries
0 17 * * 1-5 cd /Users/chenyiyun/PycharmProjects/AShareDataCenter && /Users/chenyiyun/PycharmProjects/AShareDataCenter/.venv/bin/python scripts/schedule/run_with_retry.py --retries 3 --delay 300 -- /Users/chenyiyun/PycharmProjects/AShareDataCenter/.venv/bin/python scripts/sync/run_1700_pipeline.py --config config/etl.ini --lenient >> /Users/chenyiyun/PycharmProjects/AShareDataCenter/logs/cron_1700.log 2>&1

# 2. 20:00 Evening Enhancement Pipeline (dividend + fina + integrity)
0 20 * * 1-5 cd /Users/chenyiyun/PycharmProjects/AShareDataCenter && /Users/chenyiyun/PycharmProjects/AShareDataCenter/.venv/bin/python scripts/schedule/run_with_retry.py --retries 3 --delay 300 -- /Users/chenyiyun/PycharmProjects/AShareDataCenter/.venv/bin/python scripts/sync/run_2000_pipeline.py --config config/etl.ini --lenient >> /Users/chenyiyun/PycharmProjects/AShareDataCenter/logs/cron_2000.log 2>&1

# 3. 08:30 T+1 Morning Completion (Margin-first)
# Reliable execution, ensures all historical components are matched
30 8 * * 1-5 cd /Users/chenyiyun/PycharmProjects/AShareDataCenter && /Users/chenyiyun/PycharmProjects/AShareDataCenter/.venv/bin/python scripts/schedule/run_with_retry.py --retries 1 --delay 60 -- /Users/chenyiyun/PycharmProjects/AShareDataCenter/.venv/bin/python scripts/sync/run_0830_pipeline.py --config config/etl.ini >> /Users/chenyiyun/PycharmProjects/AShareDataCenter/logs/cron_0830.log 2>&1


# 4. 20:30 Index Suite Sync (A-share index + SW industry)
# Add this only after local tests/checks pass
30 20 * * 1-5 cd /Users/chenyiyun/PycharmProjects/AShareDataCenter && /Users/chenyiyun/PycharmProjects/AShareDataCenter/.venv/bin/python scripts/sync/run_index_suite.py --config config/etl.ini --start-date 20100101 --end-date $(date +\%Y\%m\%d) >> /Users/chenyiyun/PycharmProjects/AShareDataCenter/logs/cron_index_suite.log 2>&1

# ==============================================================================
```

## 3. Explanation

- **`1-5`**: Runs Monday to Friday.
- **`run_with_retry.py`**:
    - Wraps the command execution.
    - Sends a macOS desktop notification on success or failure.
    - Retries automatically on failure (e.g., network glitch).
- **`--lenient`**: Used at 17:00 and 20:00 to avoid false failures due to temporary data lag.
- **Logs**: Output is redirected to `logs/cron_*.log` for troubleshooting.

## 4. Test Notification

Run this command to verify you receive notifications:
```bash
/Users/chenyiyun/PycharmProjects/AShareDataCenter/.venv/bin/python scripts/schedule/run_with_retry.py --retries 0 -- echo "Test Notification"
```

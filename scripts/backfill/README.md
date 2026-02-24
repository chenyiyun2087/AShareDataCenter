# Fina Backfill Script

This folder contains `run_fina_backfill.sh` — a helper to backfill `ods_fina_indicator` from TuShare in year batches, ensure schema YoY columns exist, and optionally run downstream DWD/DWS/ADS pipelines.

Quick example:

```bash
cd /path/to/AShareDataCenter
source .venv/bin/activate
export TUSHARE_TOKEN=your_token_here
export MYSQL_PASSWORD=your_mysql_password

# Fast YoY backfill (default behavior): only missing YoY, skip balancesheet, batch commit
scripts/backfill/run_fina_backfill.sh --start-year 2010 --end-year 2026 --batch-years 5 --token "$TUSHARE_TOKEN" --db-user root --db-pass "$MYSQL_PASSWORD"

# Full fina backfill (slower): include balancesheet and full columns
scripts/backfill/run_fina_backfill.sh --start-year 2010 --end-year 2026 --batch-years 5 --token "$TUSHARE_TOKEN" --db-user root --db-pass "$MYSQL_PASSWORD" --with-balancesheet --full-columns --full-ts-codes --fail-fast

# Throughput-oriented full-field mode: keep full columns, but only active stocks in each year
scripts/backfill/run_fina_backfill.sh --start-year 2025 --end-year 2020 --batch-years 1 --token "$TUSHARE_TOKEN" --db-user root --db-pass "$MYSQL_PASSWORD" --with-balancesheet --full-columns --full-ts-codes --active-ts-only --commit-every 100 --progress-every 200

# Run downstream tasks if needed
scripts/backfill/run_fina_backfill.sh --start-year 2010 --end-year 2026 --batch-years 5 --token "$TUSHARE_TOKEN" --db-user root --db-pass "$MYSQL_PASSWORD" --run-downstream --dwd-start-date 20260101 --dwd-end-date 20260228
```

Log file: `logs/backfill_fina.log`.

Notes:
- Script checks table columns via `information_schema` and adds missing YoY columns in a MySQL-compatible way.
- Default mode is optimized for YoY repair (`--yoy-only --only-missing-yoy --skip-balancesheet --commit-every 100 --progress-every 100 --continue-on-error`).
- Use `--with-balancesheet --full-columns --full-ts-codes --fail-fast` if you need strict full reload semantics.

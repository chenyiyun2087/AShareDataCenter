# Ops Scripts

This folder contains operational scripts used for MySQL/pipeline diagnostics and zombie run-log cleanup.

## 1) Check MySQL status and leak symptoms

```bash
.venv/bin/python scripts/ops/check_mysql_status.py --config config/etl.ini
```

Optional:

```bash
.venv/bin/python scripts/ops/check_mysql_status.py --config config/etl.ini --json --fail-on-warn
```

## 2) Check ODS/DWD/DWS/ADS component status

```bash
.venv/bin/python scripts/ops/check_component_status.py --config config/etl.ini
```

Strict mode for financial ann_date tables:

```bash
.venv/bin/python scripts/ops/check_component_status.py --config config/etl.ini --strict-financial-ann-date --fail-on-issues
```

## 3) Cleanup zombie RUNNING rows in `meta_etl_run_log`

Dry-run (default):

```bash
.venv/bin/python scripts/ops/cleanup_meta_etl_run_log_zombies.py --config config/etl.ini --threshold-minutes 120
```

Apply cleanup:

```bash
.venv/bin/python scripts/ops/cleanup_meta_etl_run_log_zombies.py --config config/etl.ini --threshold-minutes 120 --apply
```


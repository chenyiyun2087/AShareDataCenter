# TuShare 日频选股平台数据底座（MySQL）

本仓库实现了日频选股平台一期数据底座的落库设计与任务清单，并提供可直接运行的 TuShare 数据采集脚本。

## 内容概览
- `sql/ddl.sql`：MySQL 8.0 DDL（维表、DWD/DWS/ADS、元数据表）
- `sql/transform.sql`：DWS/ADS 计算示例 SQL
- `docs/etl_tasks.md`：全量初始化与每日增量任务清单（含水位、重试、校验）
- `scripts/tushare_etl.py`：TuShare 日频数据采集程序（全量/增量）

## 设计要点
- `trade_date` 使用 `INT(YYYYMMDD)`，`ts_code` 使用 `CHAR(9)`。
- 日频事实表主键：`(trade_date, ts_code)`，必备索引 `idx_ts_date (ts_code, trade_date)`。
- 采集写入使用 `INSERT ... ON DUPLICATE KEY UPDATE`，保证幂等。
- 财务点时快照遵循 `ann_date <= trade_date` 规则，避免未来函数。

## 使用建议
1. 执行 `sql/ddl.sql` 建库与建表。
2. 安装依赖：`pip install -r requirements.txt`。
3. 配置数据库与 TuShare Token：
   - `TUSHARE_TOKEN`（或脚本参数 `--token`）
   - `MYSQL_HOST`、`MYSQL_PORT`、`MYSQL_USER`、`MYSQL_PASSWORD`、`MYSQL_DB`
4. 全量初始化：
   ```bash
   python scripts/tushare_etl.py --mode full --start-date 20100101
   ```
5. 每日增量：
   ```bash
   python scripts/tushare_etl.py --mode incremental
   ```
6. 财务指标补齐（按公告期窗口）：
   ```bash
   python scripts/tushare_etl.py --mode incremental --fina-start 20200101 --fina-end 20231231
   ```
7. 使用 `sql/transform.sql` 生成 DWS/ADS 数据集。

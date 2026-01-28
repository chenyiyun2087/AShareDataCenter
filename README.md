# TuShare 日频选股平台数据底座（MySQL）

本仓库实现了日频选股平台一期数据底座的落库设计与任务清单。

## 内容概览
- `sql/ddl.sql`：MySQL 8.0 DDL（维表、DWD/DWS/ADS、元数据表）
- `sql/transform.sql`：DWS/ADS 计算示例 SQL
- `docs/etl_tasks.md`：全量初始化与每日增量任务清单（含水位、重试、校验）

## 设计要点
- `trade_date` 使用 `INT(YYYYMMDD)`，`ts_code` 使用 `CHAR(9)`。
- 日频事实表主键：`(trade_date, ts_code)`，必备索引 `idx_ts_date (ts_code, trade_date)`。
- 采集写入使用 `INSERT ... ON DUPLICATE KEY UPDATE`，保证幂等。
- 财务点时快照遵循 `ann_date <= trade_date` 规则，避免未来函数。

## 使用建议
1. 执行 `sql/ddl.sql` 建库与建表。
2. 按 `docs/etl_tasks.md` 进行全量初始化与每日增量采集。
3. 使用 `sql/transform.sql` 生成 DWS/ADS 数据集。

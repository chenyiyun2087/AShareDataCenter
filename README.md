# TuShare 日频选股平台数据底座（MySQL）

本仓库实现了日频选股平台一期数据底座的落库设计与任务清单，并提供可直接运行的 TuShare 数据采集脚本。

## 内容概览
- `sql/ddl.sql`：MySQL 8.0 DDL（维表、DWD/DWS/ADS、元数据表）
- `sql/transform.sql`：DWS/ADS 计算示例 SQL
- `sql/preprocess.sql`：ODS 清洗、缺失值填充与极值处理示例 SQL
- `docs/etl_tasks.md`：全量初始化与每日增量任务清单（含水位、重试、校验）
- `scripts/tushare_etl.py`：按层级调用的 TuShare 日频 ETL（全量/增量）
- `scripts/run_base.py`：base 维表任务
- `scripts/run_ods.py`：ODS 原始入库
- `scripts/run_ods_features.py`：ODS 特征数据入库
- `scripts/run_dwd.py`：DWD 标准明细
- `scripts/run_dws.py`：DWS 主题衍生
- `scripts/run_ads.py`：ADS 服务层
- `scripts/run_daily_pipeline.py`：每日增量全流程（含检查与耗时统计）
- `scripts/check_ods_features.py`：ODS 特征数据完整性检查
- `scripts/check_data_status.py`：全链路数据状态检查（ODS/Financial/Features/DWD/DWS/ADS）
- `scripts/run_fina_yearly.py`：按年补齐财务指标（fina_indicator）
- `scripts/score_stocks.py`：股票评分脚本（单只/批量）
  - 当 ADS 特征表为空时，自动回退到 DWD/DWS 明细表拼接因子。
  - 可使用 `--fallback-latest` 在指定交易日缺失时自动回退到最新可用交易日。
  - 当 DWD/DWS 也缺失时，会使用 ODS 数据拼接简化因子用于评分。
- `scripts/run_web.py`：Web 控制台（手动触发/定时任务/执行日志）

## 设计要点
- `trade_date` 使用 `INT(YYYYMMDD)`，`ts_code` 使用 `CHAR(9)`。
- 日频事实表主键：`(trade_date, ts_code)`，必备索引 `idx_ts_date (ts_code, trade_date)`。
- 采集写入使用 `INSERT ... ON DUPLICATE KEY UPDATE`，保证幂等。
- 财务点时快照遵循 `ann_date <= trade_date` 规则，避免未来函数。

## 使用建议
1. 执行 `sql/ddl.sql` 建库与建表。
2. 安装依赖：`pip install -r requirements.txt`。
3. 配置数据库与 TuShare Token：
   - 环境变量：`TUSHARE_TOKEN`、`MYSQL_HOST`、`MYSQL_PORT`、`MYSQL_USER`、`MYSQL_PASSWORD`、`MYSQL_DB`
   - 或使用本地配置文件 `config/etl.ini`（从 `config/etl.example.ini` 复制）
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
7. 分层独立运行：
   ```bash
   python scripts/run_base.py --mode full --start-date 20100101
   python scripts/run_ods.py --mode incremental
   python scripts/run_dwd.py --mode incremental
   python scripts/run_dws.py --mode incremental
   python scripts/run_ads.py --mode incremental
   ```
8. 每日增量全流程（自动分步校验与耗时统计）：
   ```bash
   python scripts/run_daily_pipeline.py --config config/etl.ini --token $TUSHARE_TOKEN
   # 传入 --debug 查看每步耗时与参数
   python scripts/run_daily_pipeline.py --config config/etl.ini --token $TUSHARE_TOKEN --debug
   ```
9. 数据检查：
   ```bash
   # ODS 特征表完整性
   python scripts/check_ods_features.py --start-date 20260101 --end-date 20260131 --config config/etl.ini

   # 全链路状态（ODS/Financial/Features/DWD/DWS/ADS）
   python scripts/check_data_status.py --config config/etl.ini
   ```
10. Web 控制台：
   ```bash
   # 先构建前端（在 app/ 目录）
   cd app
   npm install
   npm run build
   cd ..

   # 再启动 Flask 服务（会托管 app/dist）
   python scripts/run_web.py --host 0.0.0.0 --port 5000
   ```

## 前端开发联调（可选）

如需使用 Vite 开发服务器联调（前端与后端不同端口），可通过环境变量指定后端地址：

```bash
cd app
export VITE_API_BASE_URL=http://localhost:5000
npm run dev
```

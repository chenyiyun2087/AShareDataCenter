# TuShare 日频选股平台数据底座（MySQL）

本仓库实现了日频选股平台一期数据底座的落库设计与任务清单，并提供可直接运行的 TuShare 数据采集脚本。

## 内容概览
- `sql/ddl.sql`：MySQL 8.0 DDL（维表、DWD/DWS/ADS、元数据表）
- `sql/transform.sql`：DWS/ADS 计算示例 SQL
- `sql/preprocess.sql`：ODS 清洗、缺失值填充与极值处理示例 SQL
- `docs/etl_tasks.md`：全量初始化与每日增量任务清单（含水位、重试、校验）
- `scripts/sync/run_base.py`：base 维表任务
- `scripts/sync/run_ods.py`：ODS 原始入库
- `scripts/sync/run_ods_features.py`：ODS 特征数据入库
- `scripts/sync/run_dwd.py`：DWD 标准明细
- `scripts/sync/run_dws.py`：DWS 主题衍生
- `scripts/sync/run_ads.py`：ADS 服务层
- `scripts/sync/run_1700_pipeline.py`：17:00 核心批次（主链路，不含 dividend/fina 增强）
- `scripts/sync/run_2000_pipeline.py`：20:00 增强批次（dividend + fina + 完整性巡检）
- `scripts/sync/run_0830_pipeline.py`：T+1 08:30 两融补全批次（margin-first）
- `scripts/check/check_ods_features.py`：ODS 特征数据完整性检查
- `scripts/check/check_data_status.py`：全链路数据状态检查（ODS/Financial/Features/DWD/DWS/ADS）
- `scripts/sync/run_fina_yearly.py`：按年补齐财务指标（fina_indicator）
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
4. 全量初始化（推荐按层级独立运行）：
   ```bash
   python scripts/sync/run_base.py --mode full --start-date 20100101
   python scripts/sync/run_ods.py --mode incremental
   python scripts/sync/run_dwd.py --mode incremental
   python scripts/sync/run_dws.py --mode incremental
   python scripts/sync/run_ads.py --mode incremental
   ```
5. 按生产批次执行（推荐）：
   ```bash
   # 17:00 核心批次（主链路）
   python scripts/sync/run_1700_pipeline.py --config config/etl.ini --token $TUSHARE_TOKEN --lenient

   # 20:00 增强批次（dividend + fina + 完整性）
   python scripts/sync/run_2000_pipeline.py --config config/etl.ini --token $TUSHARE_TOKEN --lenient

   # T+1 08:30 两融补全批次
   python scripts/sync/run_0830_pipeline.py --config config/etl.ini --token $TUSHARE_TOKEN
   ```

6. 财务指标补齐（按公告期窗口）：
   ```bash
   python scripts/sync/run_fina_yearly.py --start-year 2020 --end-year 2023
   ```
7. 数据检查：
   ```bash
   # ODS 特征表完整性
   python scripts/check/check_ods_features.py --start-date 20260101 --end-date 20260131 --config config/etl.ini

   # 全链路状态（ODS/Financial/Features/DWD/DWS/ADS）
   python scripts/check/check_data_status.py --config config/etl.ini

   # 历史数据完整性回溯（检查过去 N 天的各层数据行数）
   python scripts/check/inspect_data_completeness.py --end-date 20260213 --days 30 --config config/etl.ini
   ```

8. 指数专题同步（A股核心指数 + 申万行业）：
   ```bash
   python scripts/sync/run_index_suite.py --start-date 20100101 --end-date 20261231 --config config/etl.ini
   python scripts/check/check_index_suite_status.py --start-date 20100101 --end-date 20261231 --config config/etl.ini --fail-on-empty
   ```

9. Web 控制台：
   ```bash
   # 启动 Flask 服务（已集成前端静态资源）
   python scripts/run_web.py --port 5999
   ```
   启动后访问：http://localhost:5999

## 跑批稳定性加固与扩容

新增稳定性工具：

- 失败类型统计：`python scripts/check/batch_failure_stats.py --hours 24`
- 重试与幂等保护：`python scripts/schedule/stability_guard.py --task-name ... --idempotency-key ... -- --command`
- 看板与报警：`python scripts/check/batch_slo_dashboard.py --hours 24`
- 方案说明：`docs/batch_stability_hardening.md`

## Web 界面功能

启动控制台后，可使用以下功能：
- **平台首页**：查看项目文档 (`README.md`)。
- **作业管理 -> 数据跑批**：
    - **数据状态**：分层查看 (ODS/DWD/DWS/ADS) 数据就绪情况。
    - **手动触发**：支持按日期范围触发指定层级的 ETL 任务。
- **作业管理 -> 定时任务**：
    - 管理系统的 Cron 调度任务，支持手动立即执行或删除任务。
- **作业管理 -> 检查脚本**：
    - **数据完整性检查**：按日期范围检查各层核心表的行数与完整性。

## 前端开发联调（可选）

如需使用 Vite 开发服务器联调（前端与后端不同端口），可通过环境变量指定后端地址：

```bash
cd app
export VITE_API_BASE_URL=http://localhost:5000
npm run dev
```

---

## 数据架构

本项目采用分层数仓架构：**ODS → DWD → DWS → ADS**

### 层次总览

| 层 | 用途 | 表数量 |
|----|------|--------|
| **ODS** | 原始数据层，TuShare 接口直接入库 | 14+ |
| **DWD** | 明细数据层，标准化清洗、财务快照 | 8 |
| **DWS** | 主题数据层，评分指标、技术形态 | 12 |
| **ADS** | 应用服务层，最终因子、股票池、综合评分 | 3 |

---

### ODS 原始层

| 主题 | 表名 | 说明 |
|------|------|------|
| **行情** | `ods_daily` | 日频行情 (OHLCV) |
| | `ods_weekly` | 周频行情 |
| | `ods_monthly` | 月频行情 |
| | `ods_index_daily` | 指数日频行情 (沪深300/上证/创业板等) |
| | `ods_index_basic` | 指数基础信息 (名称/发布方/基日基点) |
| | `ods_index_member` | 指数成分股信息 |
| | `ods_index_weight` | 指数成分权重 |
| | `ods_index_tech_factor` | 指数技术因子 (index_dailybasic) |
| | `ods_sw_index_classify` | 申万行业指数分类明细 |
| | `ods_sw_index_daily` | 申万行业指数日频行情 |
| | `ods_daily_basic` | 每日指标 (PE/PB/换手率等) |
| | `ods_adj_factor` | 复权因子 |
| | `ods_stk_factor` | 技术因子 (100+字段, MACD/KDJ/RSI/BOLL等) |
| **财务** | `ods_fina_indicator` | 财务指标 (ROE/毛利率/负债率) |
| **资金** | `ods_moneyflow` | 资金流向 (大/中/小单净流入) |
| | `ods_moneyflow_ths` | 同花顺资金流向 |
| **融资融券** | `ods_margin` | 两融汇总 (交易所级) |
| | `ods_margin_detail` | 两融明细 (个股级) |
| | `ods_margin_target` | 两融标的名单 |
| **筹码** | `ods_cyq_chips` | 筹码分布 |
| | `ods_cyq_perf` | 筹码绩效 (获利比例/成本分位) |
| **原始JSON** | `ods_*_raw` | 原始响应备份 (margin/moneyflow/cyq) |

---

### DWD 明细层

| 主题 | 表名 | 说明 |
|------|------|------|
| **行情标准化** | `dwd_daily` | 行情清洗后 |
| | `dwd_daily_basic` | 指标清洗后 (去极值) |
| | `dwd_adj_factor` | 复权因子 |
| | `dwd_stock_daily_standard` | 前复权行情 (用于技术计算) |
| **财务** | `dwd_fina_indicator` | 财务指标明细 |
| | `dwd_fina_snapshot` | 财务快照 (按交易日展开) |
| **情绪** | `dwd_margin_sentiment` | 融资情绪 (净买入占比/变化率) |
| **筹码** | `dwd_chip_stability` | 筹码稳定性 (集中度/成本偏离) |
| **标签** | `dwd_stock_label_daily` | 股票标签 (ST/次新股/涨跌幅限制/板块) |
---

### DWS 主题层

#### 评分体系 (Claude Score)

| 维度 | 表名 | 满分 |
|------|------|------|
| 动量 | `dws_momentum_score` | 25分 |
| 价值 | `dws_value_score` | 20分 |
| 质量 | `dws_quality_score` | 20分 |
| 技术 | `dws_technical_score` | 15分 |
| 资金 | `dws_capital_score` | 10分 |
| 筹码 | `dws_chip_score` | 10分 |
| **合计** | - | **100分** |

#### Fama-French 评分 (fama_score)

| 维度 | 表名 | 满分 |
|------|------|------|
| 市值 (SMB) | `dws_fama_size_score` | 10分 |
| 动量 (MOM) | `dws_fama_momentum_score` | 22分 |
| 价值 (HML) | `dws_fama_value_score` | 18分 |
| 质量 (RMW+CMA) | `dws_fama_quality_score` | 22分 |
| 技术 | `dws_fama_technical_score` | 10分 |
| 资金 | `dws_fama_capital_score` | 10分 |
| 筹码 | `dws_fama_chip_score` | 8分 |
| **合计** | - | **100分** |

#### 其他主题表

| 表名 | 说明 |
|------|------|
| `dws_price_adj_daily` | 复权收益率 (1/5/20/60日) |
| `dws_fina_pit_daily` | 财务PIT快照 |
| `dws_tech_pattern` | 技术形态 (HMA/RSI/BOLL) |
| `dws_capital_flow` | 资金驱动 (主力净流入) |
| `dws_leverage_sentiment` | 情绪杠杆 (融资强度) |
| **筹码** | `dws_chip_dynamics` | 筹码动态 (集中度变化/获利变化) |
| **流动性** | `dws_liquidity_factor` | 流动性因子 (Amihud/换手率波动) |
| **扩展动量** | `dws_momentum_extended` | 扩展动量 (52周高点/反转/12M-1M) |
| **扩展质量** | `dws_quality_extended` | 扩展质量 (杜邦三因子/ROE趋势) |
| **风险** | `dws_risk_factor` | 风险因子 (下行波动/最大回撤/VaR/Beta) |

---

### ADS 应用层

| 表名 | 说明 |
|------|------|
| `ads_features_stock_daily` | 综合因子服务表 (收益率/估值/质量) |
| `ads_universe_daily` | 每日股票池 (可交易/停牌/过滤标记) |
| `ads_stock_score_daily` | 综合评分表 (技术40%+资金25%+情绪20%+筹码15%) |

---

### 元数据表

| 表名 | 说明 |
|------|------|
| `meta_etl_watermark` | ETL 水位表 (记录各接口最新采集日期) |
| `meta_etl_run_log` | ETL 运行日志 |
| `meta_quality_check_log` | 数据质量校验日志 |

---

### 维表

| 表名 | 说明 |
|------|------|
| `dim_stock` | 股票维度 (代码/名称/行业/上市日期) |
| `dim_trade_cal` | 交易日历 (SSE/SZSE) |

## 执行策略与数据就绪时间

为确保每日增量任务高效、准确，建议参考以下数据就绪时间安排批量执行计划。

### 数据就绪时间表预测 (针对交易日 T)

| 数据类型 | 对应 ODS 表名 | 预计就绪时间 | 备注 |
| :--- | :--- | :--- | :--- |
| **基础行情/指标** | `ods_daily`, `ods_daily_basic` | 15:30 - 17:00 | OHLCV、PE、PB、总市值等 |
| **资金流向** | `ods_moneyflow` | 16:00 - 18:00 | 大单/中单/小单净流入 |
| **筹码分布/表现** | `ods_cyq_chips`, `ods_cyq_perf` | 18:00 - 19:00 | 筹码集中度、获利比例 |
| **股票因子 (Pro)** | `ods_stk_factor` | 18:00 - 20:00 | MACD、RSI 等技术指标 |
| **融资融券 (明细)** | `ods_margin`, `ods_margin_detail` | **T+1 08:30** | 交易所盘后结算较晚，通常需次日获取 |
| **复权因子** | `ods_adj_factor` | **T 盘前 09:15** | 用于当日价格复权计算 |

### 批量任务安排建议 (Batch Schedule Suggestions)

为适配 TuShare 数据发布节奏，流水线分为三个核心阶段执行：

#### 1. 下午同步 (17:00 以后) - 基础行情阶段
同步基础行情、核心特征（不含两融）、A股核心指数与 DWD/DWS/ADS 基础计算。
- **命令**: `python scripts/sync/run_1700_pipeline.py --config config/etl.ini --lenient`
- **说明**: 使用宽容模式 (`--lenient`)，适合收盘后快速产出当日主链路结果。

#### 2. 晚间强化 (20:00 以后) - 增强与完整性阶段
执行增强链路：`dividend` + 财务增量（ODS/DWD fina）+ 数据完整性巡检。
- **命令**: `python scripts/sync/run_2000_pipeline.py --config config/etl.ini --lenient`
- **日志**: 详细任务过程记录在 `logs/cron_2000.log`。
- **说明**: 与 17:00 批次解耦，减少整链路重复运行。

#### 3. 完整同步 (次日 T+1 08:30 以前) - 最终闭环阶段
补齐最晚发布的融资融券数据，并完成下游 DWD/DWS/ADS 的闭环更新。
- **命令**: `python scripts/sync/run_0830_pipeline.py --config config/etl.ini`
- **说明**: 聚焦两融 T+1 补全与下游同步。

### 自动化调度 (Cron)
建议在 `crontab` 中配置以下脚本：
```bash
# 1. 17:00 Afternoon Sync (Basic Data)
0 17 * * 1-5 cd /path/to/project && .venv/bin/python scripts/schedule/run_with_retry.py --retries 3 --delay 300 -- .venv/bin/python scripts/sync/run_1700_pipeline.py --config config/etl.ini --lenient >> logs/cron_1700.log 2>&1

# 2. 20:00 Evening Enhancement (Dividend + Fina + Integrity Check)
0 20 * * 1-5 cd /path/to/project && .venv/bin/python scripts/schedule/run_with_retry.py --retries 3 --delay 300 -- .venv/bin/python scripts/sync/run_2000_pipeline.py --config config/etl.ini --lenient >> logs/cron_2000.log 2>&1

# 3. 08:30 T+1 Morning Completion (Margin-first)
30 8 * * 1-5 cd /path/to/project && .venv/bin/python scripts/schedule/run_with_retry.py --retries 1 --delay 60 -- .venv/bin/python scripts/sync/run_0830_pipeline.py --config config/etl.ini >> logs/cron_0830.log 2>&1
```

### 宽容模式说明 (`--lenient`)
由于 TuShare 各类数据发布时间不一，`--lenient` 参数允许流水线在“今日”部分特征数据缺失时仅记录警告而不中断退出。这适用于需要在收盘后立即查看初步分析结果的场景。
---

## 详细模块分析 (ETL 流程)

以下是各层级脚本的核心逻辑深度解析，按执行依赖顺序排列。

### 1. DWD (Data Warehouse Detail) - 数据清洗与标准化
**入口脚本**: `scripts/sync/run_dwd.py` | **核心实现**: `scripts/etl/dwd/runner.py`

- **日线行情 (`load_dwd_daily`)**: 统一字段命名，提取 `ods_daily` 指标。
- **每日指标 (`load_dwd_daily_basic`)**: 对估值指标（PE/PB/PS等）进行**异常值清洗**（防极值脏数据）。
- **复权因子 (`load_dwd_adj_factor`)**: 标准化存储 TuShare 复权系数。
- **财务指标 (`load_dwd_fina_indicator`)**: 批量加载 `roe`, `netprofit_margin` 等核心财务数据。

### 2. DWS (Data Warehouse Summary) - 衍生指标与复权价格
**入口脚本**: `scripts/sync/run_dws.py` | **核心实现**: `scripts/etl/dws/runner.py`

- **复权价格计算 (`_run_price_adj`)**:
  - 自动计算历史日期的**前复权价**。
  - 计算多周期收益率：`qfq_ret_1`, `5`, `20`, `60` 日。
- **PIT (Point-in-Time) 财务数据 (`_run_fina_pit`)**:
  - **避免未来数据**: 仅关联在该交易日或之前已发布的财报 (`ann_date <= trade_date`)。
  - **每日展开**: 将低频财务指标展开到日维度，方便直接在回测系统中使用。

### 3. ADS (Application Data Store) - 应用服务层
**入口脚本**: `scripts/sync/run_ads.py` | **核心实现**: `scripts/etl/ads/runner.py`

- **股票特征宽表 (`_run_features`)**:
  - **全量拼接**: 将日线、估值、复权收益率、PIT 财务指标按 `trade_date` + `ts_code` 聚合成单条记录。
  - 直接服务于选股模型和机器学习特征工程。
- **选股空间/过滤器 (`_run_universe`)**:
  - 标记 `is_tradable` (是否有成交), `is_listed` (是否上市), `is_suspended` (是否停牌)。
  - 提供回测期间的可交易股票池，防止“幻觉交易”。

---

### 2. 数据状态校验工具

本项目提供了一套完整的检查脚本，用于监控数据新鲜度、流水线进度及数据库健康状况。

#### 全链路数据新鲜度检查 (`check_data_status.py`)
检查 ODS、DWD、DWS、ADS 各层表的最新的 `trade_date`，确认数据是否已同步。
```bash
python scripts/check/check_data_status.py --config config/etl.ini --categories ods,dwd,dws,ads
```

#### 流水线执行进度监控 (`check_pipeline_status.py`)
从元数据表 `meta_etl_run_log` 中实时提取任务状态（RUNNING/SUCCESS/FAILED）。
```bash
python scripts/check/check_pipeline_status.py --config config/etl.ini
```

#### 数据库连接安全检查 (`check_db_connections.py`)
监控 MySQL 当前连接数，确保 `get_mysql_session` 正常工作，无连接泄露风险。
```bash
python scripts/check/check_db_connections.py --config config/etl.ini
```

#### 数据完整性回溯检查 (`inspect_data_completeness.py`)
检查指定日期向前的 ODS/DWD/DWS/ADS 各层数据是否存在（用于发现漏跑或积压）。
```bash
# 检查最近 14 个交易日的数据完整性
python scripts/check/inspect_data_completeness.py --end-date 20260213 --days 14
```

### 3. 历史数据回测补数方案
针对 2010-2019 年的历史缺失数据，提供了批处理补数方案：

- **补数顺序**: ODS (Moneyflow/Factors) -> DWD -> DWS -> ADS。
- **核心工具**: `scripts/backfill/batch_runner.py`。
- **预计耗时**: 补齐 10 年的历史全量数据（含因子）约需 **22 小时**，建议在非交易时段挂机执行。
- **详细计划**:

#### 1. Data Health Assessment

| Layer | Table | Status | Action Required |
| :--- | :--- | :--- | :--- |
| **ODS** | `ods_daily` | ✅ Complete (1990-2026) | None |
| **ODS** | `ods_adj_factor` | ✅ Complete (1990-2026) | None |
| **ODS** | `ods_daily_basic` | ✅ Complete (1990-2026) | None |
| **ODS** | `ods_moneyflow` | ⚠️ Missing 2010-2019 | **Backfill 20100101-20191231** |
| **ODS** | `ods_margin_detail` | ⚠️ Missing 2010-2019 | **Backfill 20100101-20191231** |
| **ODS** | `ods_stk_factor` | ⚠️ Missing 2010-2019 | **Backfill 20100101-20191231** |
| **ODS** | `ods_fina_indicator` | ⚠️ Missing 2010-2019 | **Backfill 20100101-20191231** |
| **DWD** | All Tables | ⚠️ Dependent on ODS | **Backfill 20100101-20191231** |
| **DWS** | All Tables | ❌ Missing Pre-2020 | **Backfill 20100101-20260208** |
| **ADS** | All Tables | ❌ Missing Pre-2020 | **Backfill 20100101-20260208** |

#### 2. Backfill Strategy

**Constraints**:
- **TuShare Rate Limits**:
    - `moneyflow`: High volume (requires chunking).
    - `stk_factor`: High volume.
- **Execution Time**:
    - Processing 16 years of DWS/ADS data is computationally intensive.
    - Recommended batch size: 1 year per batch.

**Execution Phases**:

**Phase 1: ODS Backfill (The Foundation)**
Target: 2010-01-01 to 2019-12-31

1.  **Financial Indicators** (Low volume, high importance)
    ```bash
    python scripts/sync/run_ods.py --fina-start 20100101 --fina-end 20191231
    ```
2.  **Moneyflow & Margin** (High volume, use `run_ods_features.py`)
    ```bash
    python scripts/sync/run_ods_features.py --apis moneyflow,margin_detail,margin,margin_target --start-date 20100101 --end-date 20191231 --skip-existing
    ```
3.  **Stock Factors** (High volume)
    ```bash
    python scripts/sync/run_ods_features.py --apis stk_factor --start-date 20100101 --end-date 20191231 --skip-existing
    ```

**Phase 2: DWD Backfill**
Target: 2010-01-01 to 2019-12-31

```bash
# Example for a single year
python scripts/sync/run_dwd.py --mode incremental --start-date 20100101 --end-date 20101231
```

**Phase 3: DWS & ADS Backfill (The Strategy Layer)**
Target: 2010-01-01 to 2026-02-08 (Present)

```bash
# Example for a single year
python scripts/sync/run_dws.py --mode incremental --start-date 20100101 --end-date 20101231
python scripts/sync/run_ads.py --mode incremental --start-date 20100101 --end-date 20101231
```

#### 3. Estimated Timeline

| Task | Estimated Time (Year/Thread) | Total (10-16 Years) |
| :--- | :--- | :--- |
| ODS Moneyflow | ~30 mins | ~5 hours |
| ODS Stk Factor | ~45 mins | ~7.5 hours |
| DWD Sync | ~10 mins | ~2.5 hours |
| DWS Sync | ~20 mins | ~5 hours |
| ADS Sync | ~5 mins | ~1.5 hours |
| **Total** | | **~21.5 hours** |

#### 4. Batch Execution Script
To automate this year-by-year execution, use the provided batch script with required arguments:
```bash
# [Insert batch script example if available, or leave for future implementation]
```

### 5. 待办任务：历史数据大回溯 (2013-2019)

针对 2013-2019 年的历史数据空缺，请按顺序执行以下补录任务。建议在非交易时段进行：

1. **补录 ODS Moneyflow (2014-2019)**
   ```bash
   python scripts/backfill/backfill_ods_moneyflow.py --start-date 20140101 --end-date 20191231 --config config/etl.ini
   ```

2. **补录 DWS (2013-2019)**
   *说明：已在 `scripts/etl/dws/runner.py` 中将事务隔离级别优化为 `READ COMMITTED`，支持大规模并发写入而不会触发锁表错误。*
   ```bash
   python scripts/sync/run_dws.py --mode incremental --start-date 20130101 --end-date 20191231 --config config/etl.ini
   ```

3. **补录 ADS (2013-2019)**
   ```bash
   python scripts/backfill/backfill_ads_features.py --start-date 20130101 --end-date 20191231 --config config/etl.ini
   ```

### 6. 常见问题排查 (Troubleshooting)

#### Q1: 增量同步水位线跑向未来 (2026-12-31)
**现象**: `check_data_status.py` 显示水位线在未来日期，导致今日数据不更新。
**原因**: 交易日历表 (`dim_trade_cal`) 预填了未来一年的日期。旧版脚本未限制“今天”，导致它空跑未来日期的任务并提交了成功状态。
**解决方案**: 
- **代码层面**: 所有 Runner (`ods/dwd/dws/ads`) 已增加 `today_cap` 逻辑，强行过滤掉大于今天的日期。
- **数据层面**: 如果再次发生，需手动执行 SQL 重置水位线：
  ```sql
  UPDATE meta_etl_watermark SET water_mark = 20260210 WHERE api_name = 'ods_daily'; -- 设为最后实际完成的日期
  ```

#### Q2: MySQL 连接数过高 (Too Many Connections)
**现象**: 数据库报错 `Too many connections`，查询 `SHOW PROCESSLIST` 发现大量 `Sleep` 进程。
**原因**: 外部调度脚本（如 sibling 项目 `Chenyiyun2087/scheduler.py`）未正确复用数据库连接，高频创建新连接池。
**解决方案**:
- **代码层面**: 确保所有 Python 脚本使用全局单例模式 (`Singleton`) 创建 SQLAlchemy Engine，或使用 `runtime.get_mysql_session` 上下文管理器。
- **运维层面**: 杀掉泄露连接的进程，或重启 MySQL 服务。
  ```bash
  # 查找疑似泄露的 Python 进程
  ps aux | grep python | grep -v grep
  ```


#### Q3: 定时任务长期未运行 / 僵死 (Process Stucked / Not Running)
**现象**: `logs/cron_*.log` 长时间无新日志，数据库 `meta_etl_run_log` 中有任务长期处于 `RUNNING` 状态（超过数小时）。
**原因**:
1. **设备休眠/关机**: 个人电脑在预定任务时间（17:00, 20:00, 08:30）处于睡眠或关机状态，导致 cron 任务未触发。
2. **进程僵死**: 任务在执行过程中被强制中断（如断电/重启），导致数据库状态未更新为 `FAILED`，监控脚本误判为仍在运行。
**解决方案**:
1. **清理僵死状态**:
   使用专用脚本将超时的 `RUNNING` 任务标记为 `FAILED`：
   ```bash
   python scripts/check/cleanup_stale_tasks.py
   ```
2. **手动补录数据**:
   按批次手动补跑（推荐），避免整链路重复：
   ```bash
   # 17:00 核心批次
   PYTHONUNBUFFERED=1 nohup python scripts/sync/run_1700_pipeline.py --config config/etl.ini --lenient >> logs/manual_1700.log 2>&1 &

   # 20:00 增强批次
   PYTHONUNBUFFERED=1 nohup python scripts/sync/run_2000_pipeline.py --config config/etl.ini --lenient >> logs/manual_2000.log 2>&1 &

   # 次日 08:30 两融补全批次
   PYTHONUNBUFFERED=1 nohup python scripts/sync/run_0830_pipeline.py --config config/etl.ini >> logs/manual_0830.log 2>&1 &
   ```

## Daily Pipeline Verification Checklist

To ensure the daily data pipeline is running correctly and data is up-to-date, perform the following checks:

### 1. Check Overall Pipeline Status
Monitors the execution logs for the latest run status of each component.
```bash
python scripts/check/check_pipeline_status.py --config config/etl.ini
```
*Expected Output*: Should show `SUCCESS` for `ods_daily`, `ods_dividend`, `ods_fina_indicator`, and other key tasks for the current date.

### 2. Check Data Freshness (Layer by Layer)
Verifies that the latest `trade_date` in each layer matches the expected market date.
```bash
python scripts/check/check_data_status.py --config config/etl.ini
```
*Expected Output*: All layers (ODS, DWD, DWS, ADS) should show the latest trading date.

### 3. Verify Specific Components

#### Financial Data (Assets & Equity)
Confirm that critical financial fields (Total Assets, Shareholder Equity) are being populated (via Balance Sheet fallback).
```bash
# Check for nulls in recent entries (should be empty or very low count)
mysql -u root -p$MYSQL_PASSWORD -e "USE tushare_stock; SELECT count(*) FROM ods_fina_indicator WHERE ann_date >= 20240101 AND total_assets IS NULL;"
```

#### Dividend Data
Verify that the `ods_dividend` table is populated and growing.
```bash
mysql -u root -p$MYSQL_PASSWORD -e "USE tushare_stock; SELECT count(*) FROM ods_dividend;"
```

#### Index Data
Ensure A-share indices and SW industries are up-to-date.
```bash
python scripts/check/check_index_suite_status.py --config config/etl.ini --fail-on-empty
```

### 4. Manual Trigger (Troubleshooting)
If any step fails or is missing, you can manually trigger by batch (recommended):

```bash
# 17:00 core batch
python scripts/sync/run_1700_pipeline.py --config config/etl.ini --lenient --token $TUSHARE_TOKEN

# 20:00 enhancement batch
python scripts/sync/run_2000_pipeline.py --config config/etl.ini --lenient --token $TUSHARE_TOKEN

# 08:30 T+1 margin-first batch
python scripts/sync/run_0830_pipeline.py --config config/etl.ini --token $TUSHARE_TOKEN

# Run specific component (e.g., Dividend Sync)
python scripts/sync/run_ods.py --dividend --only-dividend --config config/etl.ini
```

---

## DWS和ADS数据评分逻辑详解

### 整体架构

项目采用分层数仓架构：**ODS → DWD → DWS → ADS**

- **DWS（Data Warehouse Summary）**：主题数据层，包含6大维度的评分体系，满分100分
- **ADS（Application Data Service）**：应用服务层，综合DWS各维度评分，提供最终的应用层排序

---

## DWS层评分体系（Claude Score）

### 1. 动量评分（25分）- `dws_momentum_score`

基于多时间维度的收益率、资金量能和动量指标的组合，判断股票的上涨动能。

| 指标 | 分值 | 判断标准 |
|------|------|--------|
| 5日收益评分 | 0-3分 | >10% → 3分 \| >5% → 2分 \| >0% → 1分 |
| 20日收益评分 | 0-2分 | >15% → 2分 \| >5% → 1分 |
| 60日收益评分 | 0-3分 | >30% → 3分 \| >10% → 2分 \| >0% → 1分 |
| 量比评分 | 0-4分 | >1.5 → 4分 \| >1.2 → 3分 \| >1.0 → 2分 \| 其他 → 1分 |
| 换手率评分 | 0-4分 | >10% → 4分 \| >5% → 3分 \| >2% → 2分 \| 其他 → 1分 |
| MTM动量指标 | 0-5分 | >1.0 → 5分 \| >0.5 → 4分 \| >0.2 → 3分 \| >0 → 2分 \| >-0.5 → 1分 |
| MTMMA交叉信号 | 0-4分 | 金叉+多头 → 4分 \| 金叉 → 3分 \| 双多头 → 2分 \| 接近零轴 → 1分 |

**实现位置**：[scripts/etl/dws/scoring.py](scripts/etl/dws/scoring.py) - `_run_momentum_score()`

---

### 2. 价值评分（20分）- `dws_value_score`

低估值是价值投资的核心。PE越低、PB越接近破净对应分数越高。

| 指标 | 分值 | 判断标准 |
|------|------|--------|
| PE评分 | 0-7分 | PE < 15 → 7分 \| <25 → 5分 \| <40 → 3分 \| <60 → 1分 |
| PB评分 | 0-7分 | PB < 1.0（破净） → 7分 \| <2.0 → 6分 \| <3.0 → 4分 \| <5.0 → 2分 |
| PS评分 | 0-6分 | PS < 1.0 → 6分 \| <2.0 → 5分 \| <3.0 → 3分 \| <5.0 → 1分 |

**实现位置**：[scripts/etl/dws/scoring.py](scripts/etl/dws/scoring.py) - `_run_value_score()`

---

### 3. 质量评分（20分）- `dws_quality_score`

衡量企业的盈利能力和财务健康度。高ROE、高毛利率、低负债是优质公司的标志。

| 指标 | 分值 | 判断标准 |
|------|------|--------|
| ROE评分 | 0-8分 | 年化ROE > 20% → 8分 \| >15% → 6分 \| >10% → 4分 \| >5% → 2分 |
| 毛利率评分 | 0-6分 | >50% → 6分 \| >30% → 5分 \| >20% → 3分 \| >10% → 1分 |
| 负债率评分 | 0-6分 | 资产负债率 < 30% → 6分 \| <50% → 5分 \| <70% → 3分 \| ≥70% → 1分 |

**实现位置**：[scripts/etl/dws/scoring.py](scripts/etl/dws/scoring.py) - `_run_quality_score()`

---

### 4. 技术评分（15分）- `dws_technical_score`

使用多个技术指标的组合，判断股票的短期技术形态和超买超卖状态。

| 指标 | 分值 | 判断标准 |
|------|------|--------|
| MACD评分 | 0-4分 | MACD>0 & DIF>DEA → 4分 \| MACD>0 → 2分 \| DIF>DEA → 1分 |
| KDJ评分 | 0-3分 | KDJ < 20（超卖） → 3分 \| 40-60 → 2分 \| >80（超买） → 0分 |
| RSI评分 | 0-3分 | RSI < 30（超卖） → 3分 \| 40-60 → 2分 \| >70（超买） → 0分 |
| CCI评分 | 0-3分 | CCI > 100（强势） → 3分 \| 0-100 → 2分 \| <-100（超卖） → 2分 |
| BIAS评分 | 0-2分 | BIAS < -3（超卖） → 2分 \| -1-1（正常） → 1分 \| >5（过热） → 0分 |

**实现位置**：[scripts/etl/dws/scoring.py](scripts/etl/dws/scoring.py) - `_run_technical_score()`

---

### 5. 资金评分（10分）- `dws_capital_score`

主力资金和机构资金的净流入情况，判断股票的实际资金支持力度。

| 指标 | 分值 | 判断标准 |
|------|------|--------|
| 特大单净流（1亿+） | 0-5分 | 净买入 > 1亿 → 5分 \| >5000万 → 4分 \| >1000万 → 2分 \| >0 → 1分 |
| 大单净流（5000万+） | 0-3分 | 净买入 > 5000万 → 3分 \| >2000万 → 2分 \| >0 → 1分 |
| 融资融券评分 | 0-2分 | 融资净买入 > 融资余额 2% → 2分 \| >0.5% → 1.5分 \| >0% → 1分 |

**实现位置**：[scripts/etl/dws/scoring.py](scripts/etl/dws/scoring.py) - `_run_capital_score()`

---

### 6. 筹码评分（10分）- `dws_chip_score`

分析股票的成本分布。深度套牢（获利比低）可能预示反转机会；突破成本具有强支撑。

| 指标 | 分值 | 判断标准 |
|------|------|--------|
| 获利比例评分 | 0-6分 | 获利比 < 10%（深度套牢） → 6分 \| <30% → 5分 \| 40-60% → 3分 \| >90% → 1分 |
| 成本偏离评分 | 0-4分 | 现价 > 成本 10% → 4分 \| >5% → 3分 \| >0% → 2分 |

**实现位置**：[scripts/etl/dws/scoring.py](scripts/etl/dws/scoring.py) - `_run_chip_score()`

---

## ADS层综合评分

### 综合评分表（`ads_stock_score_daily`）

使用**百分位排名（Percentile Rank）** 归一化，对标的进行每日排序：

| 维度 | 权重 | 指标构成 |
|------|------|--------|
| **技术评分** | 40% | HMA斜率 + RSI合理区间 + BOLL波幅 |
| **资金评分** | 25% | 主力净流入比 + 成交量价格关联度 |
| **情绪评分** | 20% | 融资买入强度 + 换手率波动 |
| **筹码评分** | 15% | 获利比例合理度 + 成本突破 |

### 计算方法

```
每个维度先用 PERCENT_RANK() 在同一交易日的所有股票中进行百分位排名（0-100）

然后加权求和：
总分 = 技术评分×0.4 + 资金评分×0.25 + 情绪评分×0.2 + 筹码评分×0.15

最终生成当日排名（RANK() OVER PARTITION BY trade_date）
```

**实现位置**：[scripts/etl/ads/runner.py](scripts/etl/ads/runner.py) - `_run_stock_score()`

---

## 数据流架构图

```
ODS (原始数据)
    └─> DWD (明细数据) 
            ├─> dwd_daily (行情明细)
            ├─> dwd_daily_basic (估值基础)
            ├─> dwd_fina_indicator (财务明细)
            ├─> dwd_margin_sentiment (融资情绪)
            └─> ods_moneyflow (资金流向)
                    │
                    └─> DWS (主题评分层)
                            ├─> dws_price_adj_daily (复权收益)
                            ├─> dws_fina_pit_daily (财务PIT快照)
                            ├─> dws_momentum_score (动量25分)
                            ├─> dws_value_score (价值20分)
                            ├─> dws_quality_score (质量20分)
                            ├─> dws_technical_score (技术15分)
                            ├─> dws_capital_score (资金10分)
                            └─> dws_chip_score (筹码10分)
                                    └─> ADS (应用服务层)
                                            ├─> ads_features_stock_daily (特征表)
                                            ├─> ads_universe_daily (股票池)
                                            └─> ads_stock_score_daily (最终排序)
```

---

## 评分体系特点

1. **多维度组合**：6个维度共100分，覆盖技术面、基本面、资金面、筹码面
2. **动态权重**：ADS层使用百分位排名，每日根据整体市场情况动态调整相对排名
3. **PIT快照**：财务数据采用Point-In-Time快照方式，确保财务数据的时间一致性
4. **归一化处理**：避免市值偏差，使用比率和百分位而非绝对值
5. **反向指标**：某些指标（如获利比低、负债低）体现反转机会，增加策略多样性
6. **实时更新**：支持全量模式和增量模式，每日自动更新评分数据

---

## 运行评分计算

### DWS评分计算

```bash
# 全量初始化 (较耗时)
python scripts/sync/run_dws.py --mode full --start-date 20100101

# 增量更新 (推荐)
python scripts/sync/run_dws.py --mode incremental

# 指定日期范围
python scripts/sync/run_dws.py --mode incremental --start-date 20240101 --end-date 20240331
```

### ADS综合评分

```bash
# 全量初始化
python scripts/sync/run_ads.py --mode full --start-date 20100101

# 增量更新 (推荐)
python scripts/sync/run_ads.py --mode incremental

# 指定日期范围
python scripts/sync/run_ads.py --mode incremental --start-date 20240101 --end-date 20240331
```

---

## 关键实现文件

| 功能 | 文件路径 |
|------|--------|
| DWS评分计算 | [scripts/etl/dws/scoring.py](scripts/etl/dws/scoring.py) |
| DWS主题表计算 | [scripts/etl/dws/runner.py](scripts/etl/dws/runner.py) |
| DWS增强因子 | [scripts/etl/dws/enhanced_factors.py](scripts/etl/dws/enhanced_factors.py) |
| ADS综合评分 | [scripts/etl/ads/runner.py](scripts/etl/ads/runner.py) |
| DWS运行脚本 | [scripts/sync/run_dws.py](scripts/sync/run_dws.py) |
| ADS运行脚本 | [scripts/sync/run_ads.py](scripts/sync/run_ads.py) |
| SQL转换逻辑 | [sql/transform.sql](sql/transform.sql) |
| 表结构定义 | [sql/ddl.sql](sql/ddl.sql) |

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
| | `dwd_daily_basic` | 指标清洗后 |
| | `dwd_adj_factor` | 复权因子 |
| | `dwd_stock_daily_standard` | 前复权行情 |
| **财务** | `dwd_fina_indicator` | 财务指标明细 |
| | `dwd_fina_snapshot` | 财务快照 (按交易日映射) |
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
218: 
219: ---
220: 
221: ## 执行策略与数据就绪时间
222: 
223: 为确保每日增量任务高效、准确，建议参考以下数据就绪时间安排批量执行计划。
224: 
225: ### 数据就绪时间表预测 (针对交易日 T)
226: 
227: | 数据类型 | 对应 ODS 表名 | 预计就绪时间 | 备注 |
| :--- | :--- | :--- | :--- |
| **基础行情/指标** | `ods_daily`, `ods_daily_basic` | 15:30 - 17:00 | OHLCV、PE、PB、总市值等 |
| **资金流向** | `ods_moneyflow` | 16:00 - 18:00 | 大单/中单/小单净流入 |
| **筹码分布/表现** | `ods_cyq_chips`, `ods_cyq_perf` | 18:00 - 19:00 | 筹码集中度、获利比例 |
| **股票因子 (Pro)** | `ods_stk_factor` | 18:00 - 20:00 | MACD、RSI 等技术指标 |
| **融资融券 (明细)** | `ods_margin`, `ods_margin_detail` | **T+1 08:30** | 交易所盘后结算较晚，通常需次日获取 |
| **复权因子** | `ods_adj_factor` | **T 盘前 09:15** | 用于当日价格复权计算 |
228: 
229: ### 批量任务安排建议
230: 
231: #### 1. 下午同步 (17:00 以后)
232: 同步基础行情与 DWD/DWS/ADS 基础计算。
233: ```bash
234: # 使用宽容模式 (--lenient)，会自动忽略尚未发布的特征数据（如两融）
235: python scripts/sync/run_daily_pipeline.py --config config/etl.ini --lenient
236: ```
237: 
238: #### 2. 晚间强化 (20:00 以后)
239: 获取大部分特征数据（资金流、筹码、技术因子）并更新对应的增强因子。
240: ```bash
241: python scripts/sync/run_daily_pipeline.py --config config/etl.ini --lenient
242: ```
243: 
244: #### 3. 完整同步 (T+1 09:00 以前)
245: 补齐最晚发布的融资融券数据，完成全量 ADS 评分更新。
246: ```bash
247: # 不带 --lenient，确保所有检查通过
248: python scripts/sync/run_daily_pipeline.py --config config/etl.ini
249: ```
250: 
251: ### 宽容模式说明 (`--lenient`)
252: 由于 TuShare 各类数据发布时间不一，`--lenient` 参数允许流水线在“今日”部分特征数据缺失时仅记录警告而不中断退出。这适用于需要在收盘后立即查看初步分析结果的场景。

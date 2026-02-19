# TuShare 日频数据底座任务清单（全量/增量）

## 1. 全量初始化（可配置起始日期）

### 1.1 base 维表
- **dim_trade_cal**
  - 拉取 TuShare `trade_cal`，按交易所写入。
  - 幂等写入：`INSERT ... ON DUPLICATE KEY UPDATE`。
- **dim_stock**
  - 拉取 TuShare `stock_basic`，按 ts_code 主键写入。

### 1.2 ODS 原始入库（按交易日拉取）
- **ods_daily**、**ods_daily_basic**、**ods_adj_factor**
  - 通过 `dim_trade_cal` 生成交易日序列。
  - 逐 `trade_date` 拉取全市场数据（减少请求数）。
  - 原始字段尽量保持 TuShare 字段命名以便追溯。

### 1.3 DWD 标准明细
- **dwd_daily**、**dwd_daily_basic**、**dwd_adj_factor**
  - 由 ODS 转换，统一字段类型与主键，保证幂等写入。
- **dwd_fina_indicator**
  - 以公告日/报告期窗口分批拉取。
  - 按 `(ts_code, ann_date, end_date)` 幂等写入。

### 1.4 水位初始化
- 每个接口在 `meta_etl_watermark` 中写入 `water_mark` 为起始日期的前一交易日。
- 状态置为 `SUCCESS`。

## 2. 每日增量同步

### 2.1 通用流程
1. 从 `meta_etl_watermark` 读取每接口最后成功日期。
2. 在 `dim_trade_cal` 中找到下一交易日。
3. 按交易日循环拉取，直到最新可得交易日。
4. 任一日失败：水位不前移，记录错误，状态置 `FAILED`。
5. 每日成功完成：更新水位到最后成功日。

### 2.2 频控与重试
- 全局限流：约 500 次/分钟。
- 网络超时重试：指数退避（例如 2s、4s、8s）。
- 失败记录：写入 `meta_etl_run_log`，并更新 `meta_etl_watermark`。


### 2.3 指数专题增量（新增）
1. 执行 `scripts/sync/run_index_suite.py --start-date <T-5> --end-date <T>` 同步以下数据集：
   - 目标指数：上证指数、深证成指、沪深300、中证500、上证50、科创50、标普中国A股1500、富时中国A50、深证100、创业板指。
   - 数据内容：指数基础信息、成分股、成分权重、指数日线、指数技术因子（`index_dailybasic`）、申万行业分类、申万行业日线。
2. 执行 `scripts/check/check_index_suite_status.py --start-date <T-5> --end-date <T> --fail-on-empty`。
3. 检查通过后纳入调度计划（建议在晚间 20:30 与 ODS 特征任务串行执行）。

## 3. DWS/ADS 产出顺序

1. **dws_price_adj_daily**
   - 依赖：`dwd_daily` + `dwd_adj_factor`
   - 复权基准日固定为最新交易日或配置基准日。
2. **dws_fina_pit_daily**
   - 依赖：`dwd_fina_indicator` + `dim_trade_cal`
   - 规则：`ann_date <= trade_date` 的最新一条财务记录。
3. **ads_features_stock_daily**
   - 依赖：`dws_price_adj_daily` + `dwd_daily_basic` + `dws_fina_pit_daily` + `dim_stock`
4. **ads_universe_daily**
   - 依赖：`dwd_daily` + `dim_stock`

## 4. Web 控制台

- 支持在页面发起分层 ETL 任务（全量/增量）。
- 支持配置 Cron 定时任务。
- 任务执行日志来自 `meta_etl_run_log`，展示开始/结束时间与状态。

## 5. 数据质量校验（日志级）

- 交易日是否缺失（对齐 `dim_trade_cal`）
- 当日写入股票数异常（与历史均值比较）
- 极端值检查（成交额/涨跌幅异常）
- 主键重复检查（理论上不允许）

建议将检查结果写入 `meta_quality_check_log`。

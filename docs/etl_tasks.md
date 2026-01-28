# TuShare 日频数据底座任务清单（全量/增量）

## 1. 全量初始化（可配置起始日期）

### 1.1 维表
- **dim_trade_cal**
  - 拉取 TuShare `trade_cal`，按交易所写入。
  - 幂等写入：`INSERT ... ON DUPLICATE KEY UPDATE`。
- **dim_stock**
  - 拉取 TuShare `stock_basic`，按 ts_code 主键写入。

### 1.2 日频事实表（按交易日拉取）
- **dwd_daily**、**dwd_daily_basic**、**dwd_adj_factor**
  - 通过 `dim_trade_cal` 生成交易日序列。
  - 逐 `trade_date` 拉取全市场数据（减少请求数）。
  - 批量写入（1k~5k 行 / 批），`INSERT ... ON DUPLICATE KEY UPDATE`。

### 1.3 财务指标（按公告期窗口补齐）
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

## 4. 数据质量校验（日志级）

- 交易日是否缺失（对齐 `dim_trade_cal`）
- 当日写入股票数异常（与历史均值比较）
- 极端值检查（成交额/涨跌幅异常）
- 主键重复检查（理论上不允许）

建议将检查结果写入 `meta_quality_check_log`。

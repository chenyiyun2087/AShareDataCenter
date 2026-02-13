# 跑批稳定性加固 + 扩容方案（asharedatacenter）

## 1) 失败类型统计

使用 `scripts/check/batch_failure_stats.py` 对 `meta_etl_run_log` 失败记录进行聚合统计：

- 按任务ID聚合（从错误文本提取 `task_id/run_id/job_id`，缺失则回退为日志ID）
- 按错误码聚合（提取 `ERRxxx/HTTP状态码/数字错误码`）
- 按模块聚合（优先 `module/api/stage`，否则退化为 `api_name` 前缀）
- 输出模块-错误码交叉矩阵 TopN

示例：

```bash
python scripts/check/batch_failure_stats.py --hours 24 --limit 3000
```

## 2) 重试 + 幂等策略

使用 `scripts/schedule/stability_guard.py` 包裹真实任务命令：

- 自动创建 `meta_etl_retry_guard`，维护 `task_name + idempotency_key` 唯一约束
- 如果同一 key 已成功，直接跳过（幂等）
- 支持超时、失败重试、错误尾日志落表
- 适合日跑批：`idempotency_key=task_name:trade_date`

示例：

```bash
python scripts/schedule/stability_guard.py \
  --task-name daily_pipeline \
  --idempotency-key daily_pipeline:20260213 \
  --retries 2 \
  --retry-delay 120 \
  --timeout 5400 \
  -- python scripts/sync/run_daily_pipeline.py --config config/etl.ini
```

> 现有数据写入 `ON DUPLICATE KEY UPDATE` 已具备数据层幂等；此处新增的是“任务执行层”幂等。

## 3) 超时与资源配置基线

在 `config/etl.example.ini` 增加 `[batch]` 建议基线：

- `timeout_sec=5400`：单任务 90 分钟超时
- `retry_times=2`：失败重试 2 次
- `retry_delay_sec=120`：重试间隔 2 分钟
- `concurrency=2`：单机并发建议 2
- `worker_cpu=2`：每 worker 建议 2 vCPU
- `worker_memory_mb=4096`：每 worker 建议 4GB 内存

扩容建议：

- 优先“水平扩容”（增加 worker）而非无限提升单 worker 并发
- 并发提升时同步观察 TuShare 限频、MySQL QPS、锁等待
- 通过 `batch_slo_dashboard.py` 的 P95 与积压指标闭环调参

## 4) 报警与看板

使用 `scripts/check/batch_slo_dashboard.py` 输出并阈值判断：

- 成功率（success/total）
- P95 耗时（秒）
- 积压（`status=RUNNING` 数量）

示例：

```bash
python scripts/check/batch_slo_dashboard.py \
  --hours 24 \
  --success-rate-threshold 99.0 \
  --p95-threshold-sec 1800 \
  --backlog-threshold 3
```

建议接入方式：

1. Crontab 每 5~10 分钟执行一次
2. exit code 非 0 时对接告警系统（企业微信/飞书/Webhook）
3. 看板周期按 24h + 7d 双窗口展示，观察短期抖动与长期趋势

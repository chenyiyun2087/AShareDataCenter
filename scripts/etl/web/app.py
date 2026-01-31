from __future__ import annotations

import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from flask import Flask, redirect, render_template_string, request, url_for

from .. import ads, base, dwd, dws, ods
from ..base.runtime import get_env_config, get_mysql_connection

app = Flask(__name__)

scheduler = BackgroundScheduler()
if not scheduler.running:
    scheduler.start()


def _run_layer(layer: str, mode: str, params: dict) -> None:
    if layer == "base":
        token = params["token"]
        if mode == "full":
            base.run_full(token, params["start_date"], params["rate_limit"])
        else:
            base.run_incremental(token, params["start_date"], params["rate_limit"])
        return
    if layer == "ods":
        token = params["token"]
        if mode == "full":
            ods.run_full(token, params["start_date"], params["rate_limit"])
        else:
            ods.run_incremental(token, params["rate_limit"])
        if params.get("fina_start") and params.get("fina_end"):
            ods.run_fina_incremental(
                token,
                params["fina_start"],
                params["fina_end"],
                params["rate_limit"],
            )
        return
    if layer == "dwd":
        if mode == "full":
            dwd.run_full(params["start_date"])
        else:
            dwd.run_incremental()
        if params.get("fina_start") and params.get("fina_end"):
            dwd.run_fina_incremental(params["fina_start"], params["fina_end"])
        return
    if layer == "dws":
        if mode == "full":
            dws.run_full(params["start_date"])
        else:
            dws.run_incremental()
        return
    if layer == "ads":
        if mode == "full":
            ads.run_full(params["start_date"])
        else:
            ads.run_incremental()
        return
    raise ValueError(f"Unknown layer: {layer}")


def _run_async(layer: str, mode: str, params: dict) -> None:
    thread = threading.Thread(target=_run_layer, args=(layer, mode, params), daemon=True)
    thread.start()


def _schedule_job(layer: str, mode: str, params: dict, cron: dict) -> str:
    job_id = f"{layer}-{mode}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    trigger = CronTrigger(**cron)
    scheduler.add_job(
        _run_layer,
        trigger=trigger,
        args=[layer, mode, params],
        id=job_id,
        replace_existing=False,
    )
    return job_id


def _run_ods_features(params: dict) -> None:
    script_path = Path(__file__).resolve().parents[2] / "run_ods_features.py"
    cmd = [
        sys.executable,
        str(script_path),
        "--start-date",
        str(params["start_date"]),
        "--end-date",
        str(params["end_date"]),
        "--rate-limit",
        str(params["rate_limit"]),
        "--cyq-rate-limit",
        str(params["cyq_rate_limit"]),
        "--apis",
        params["apis"],
    ]
    if params.get("token"):
        cmd.extend(["--token", params["token"]])
    if params.get("config"):
        cmd.extend(["--config", params["config"]])
    subprocess.run(cmd, check=False)


def _run_ods_features_async(params: dict) -> None:
    thread = threading.Thread(target=_run_ods_features, args=(params,), daemon=True)
    thread.start()


def _get_last_ods_run() -> Optional[dict]:
    cfg = get_env_config()
    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT start_at, end_at, status FROM meta_etl_run_log "
                "WHERE api_name=%s ORDER BY id DESC LIMIT 1",
                ("ods",),
            )
            row = cursor.fetchone()
    if not row:
        return None
    start_at, end_at, status = row
    duration = None
    if start_at and end_at:
        duration = (end_at - start_at).total_seconds()
    return {"start_at": start_at, "end_at": end_at, "status": status, "duration": duration}


ODS_TABLES: List[str] = [
    "ods_daily",
    "ods_daily_basic",
    "ods_adj_factor",
    "ods_margin_detail",
    "ods_moneyflow",
    "ods_moneyflow_ths",
    "ods_cyq_chips",
    "ods_stk_factor",
]


def _fetch_ods_rows(
    table: str,
    page: int,
    search_ts_code: Optional[str],
    search_trade_date: Optional[int],
    page_size: int = 50,
) -> Tuple[List[str], List[tuple], int]:
    if table not in ODS_TABLES:
        raise ValueError("Invalid ODS table selection.")
    cfg = get_env_config()
    where_clauses = []
    params: List[object] = []
    if search_ts_code:
        where_clauses.append("ts_code LIKE %s")
        params.append(f"%{search_ts_code}%")
    if search_trade_date:
        where_clauses.append("trade_date = %s")
        params.append(search_trade_date)
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    offset = (page - 1) * page_size
    with get_mysql_connection(cfg) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table} {where_sql}", params)
            total = cursor.fetchone()[0]
            cursor.execute(
                f"SELECT * FROM {table} {where_sql} "
                "ORDER BY trade_date DESC, ts_code DESC "
                "LIMIT %s OFFSET %s",
                [*params, page_size, offset],
            )
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
    return columns, rows, total


@app.route("/", methods=["GET"])
def index():
    today = int(datetime.now().strftime("%Y%m%d"))
    params = {
        "token": request.args.get("token") or None,
        "start_date": int(request.args.get("start_date", 20100101)),
        "fina_start": request.args.get("fina_start"),
        "fina_end": request.args.get("fina_end"),
        "rate_limit": int(request.args.get("rate_limit", 500)),
    }
    if params["fina_start"]:
        params["fina_start"] = int(params["fina_start"])
    if params["fina_end"]:
        params["fina_end"] = int(params["fina_end"])

    cfg_token = params["token"]
    logs: List[dict] = []
    logs_error = None
    try:
        logs = base.list_runs(50)
    except Exception as exc:
        logs_error = str(exc)
    jobs = scheduler.get_jobs()

    ods_status = None
    ods_status_error = None
    try:
        ods_status = _get_last_ods_run()
    except Exception as exc:
        ods_status_error = str(exc)

    ods_table = request.args.get("ods_table", "ods_daily")
    search_ts_code = request.args.get("search_ts_code", "").strip() or None
    search_trade_date = request.args.get("search_trade_date", "").strip()
    search_trade_date_int = int(search_trade_date) if search_trade_date else None
    page = int(request.args.get("page", 1))
    ods_columns: List[str] = []
    ods_rows: List[tuple] = []
    ods_total = 0
    ods_error = None
    try:
        ods_columns, ods_rows, ods_total = _fetch_ods_rows(
            ods_table,
            page,
            search_ts_code,
            search_trade_date_int,
        )
    except Exception as exc:
        ods_error = str(exc)

    return render_template_string(
        _TEMPLATE,
        params=params,
        logs=logs,
        logs_error=logs_error,
        jobs=jobs,
        token=cfg_token,
        ods_status=ods_status,
        ods_status_error=ods_status_error,
        ods_table=ods_table,
        ods_tables=ODS_TABLES,
        ods_columns=ods_columns,
        ods_rows=ods_rows,
        ods_total=ods_total,
        ods_page=page,
        ods_page_size=50,
        ods_error=ods_error,
        search_ts_code=search_ts_code or "",
        search_trade_date=search_trade_date or "",
        today=today,
    )


@app.route("/run", methods=["POST"])
def run_task():
    layer = request.form["layer"]
    mode = request.form["mode"]
    params = {
        "token": request.form.get("token"),
        "start_date": int(request.form.get("start_date", 20100101)),
        "fina_start": request.form.get("fina_start"),
        "fina_end": request.form.get("fina_end"),
        "rate_limit": int(request.form.get("rate_limit", 500)),
    }
    if params["fina_start"]:
        params["fina_start"] = int(params["fina_start"])
    if params["fina_end"]:
        params["fina_end"] = int(params["fina_end"])

    _run_async(layer, mode, params)
    return redirect(url_for("index"))


@app.route("/schedule", methods=["POST"])
def schedule_task():
    layer = request.form["layer"]
    mode = request.form["mode"]
    params = {
        "token": request.form.get("token"),
        "start_date": int(request.form.get("start_date", 20100101)),
        "fina_start": request.form.get("fina_start"),
        "fina_end": request.form.get("fina_end"),
        "rate_limit": int(request.form.get("rate_limit", 500)),
    }
    if params["fina_start"]:
        params["fina_start"] = int(params["fina_start"])
    if params["fina_end"]:
        params["fina_end"] = int(params["fina_end"])

    cron = {
        "minute": request.form.get("cron_minute", "0"),
        "hour": request.form.get("cron_hour", "2"),
    }
    _schedule_job(layer, mode, params, cron)
    return redirect(url_for("index"))


@app.route("/run-ods-features", methods=["POST"])
def run_ods_features():
    params = {
        "token": request.form.get("token"),
        "start_date": int(request.form.get("start_date")),
        "end_date": int(request.form.get("end_date")),
        "rate_limit": int(request.form.get("rate_limit", 500)),
        "cyq_rate_limit": int(request.form.get("cyq_rate_limit", 180)),
        "apis": request.form.get("apis", "stk_factor"),
        "config": request.form.get("config") or None,
    }
    _run_ods_features_async(params)
    return redirect(url_for("index"))


_TEMPLATE = """
<!DOCTYPE html>
<html lang="zh">
<head>
  <meta charset="utf-8" />
  <title>ETL 控制台</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 20px; }
    table { border-collapse: collapse; width: 100%; margin-top: 16px; }
    th, td { border: 1px solid #ddd; padding: 8px; }
    th { background: #f2f2f2; }
    .card { border: 1px solid #ddd; padding: 12px; margin-bottom: 16px; }
    .flex { display: flex; gap: 12px; flex-wrap: wrap; }
    .muted { color: #666; font-size: 12px; }
    .error { color: #b00020; }
    .pagination { display: flex; gap: 8px; align-items: center; margin-top: 12px; }
    .badge { padding: 2px 8px; background: #eef; border-radius: 12px; }
  </style>
</head>
<body>
  <h1>ETL 控制台</h1>
  <div class="card">
    <h2>ODS 执行状态</h2>
    {% if ods_status_error %}
      <div class="error">读取 ODS 状态失败：{{ ods_status_error }}</div>
    {% elif ods_status %}
      <div class="flex">
        <div>状态：<span class="badge">{{ ods_status.status }}</span></div>
        <div>开始时间：{{ ods_status.start_at }}</div>
        <div>结束时间：{{ ods_status.end_at or '执行中' }}</div>
        <div>耗时：{{ ods_status.duration or 0 }} 秒</div>
      </div>
    {% else %}
      <div class="muted">暂无 ODS 运行记录。</div>
    {% endif %}
  </div>
  <div class="card">
    <h2>ODS 技术指标同步</h2>
    <form method="post" action="/run-ods-features">
      <div class="flex">
        <label>Token
          <input type="text" name="token" value="{{ token or '' }}" />
        </label>
        <label>Config
          <input type="text" name="config" placeholder="config/etl.ini" />
        </label>
        <label>Start Date
          <input type="number" name="start_date" value="{{ today }}" />
        </label>
        <label>End Date
          <input type="number" name="end_date" value="{{ today }}" />
        </label>
        <label>Rate Limit
          <input type="number" name="rate_limit" value="{{ params.rate_limit }}" />
        </label>
        <label>Cyq Rate Limit
          <input type="number" name="cyq_rate_limit" value="180" />
        </label>
        <label>APIs
          <input type="text" name="apis" value="stk_factor" />
        </label>
      </div>
      <button type="submit">同步技术指标</button>
    </form>
    <div class="muted">默认仅同步 stk_factor，可按需调整 APIs。</div>
  </div>
  <div class="card">
    <h2>手动触发</h2>
    <form method="post" action="/run">
      <div class="flex">
        <label>层级
          <select name="layer">
            <option value="base">base</option>
            <option value="ods">ods</option>
            <option value="dwd">dwd</option>
            <option value="dws">dws</option>
            <option value="ads">ads</option>
          </select>
        </label>
        <label>模式
          <select name="mode">
            <option value="incremental">incremental</option>
            <option value="full">full</option>
          </select>
        </label>
        <label>Token
          <input type="text" name="token" value="{{ token or '' }}" />
        </label>
        <label>Start Date
          <input type="number" name="start_date" value="{{ params.start_date }}" />
        </label>
        <label>Fina Start
          <input type="number" name="fina_start" value="{{ params.fina_start or '' }}" />
        </label>
        <label>Fina End
          <input type="number" name="fina_end" value="{{ params.fina_end or '' }}" />
        </label>
        <label>Rate Limit
          <input type="number" name="rate_limit" value="{{ params.rate_limit }}" />
        </label>
      </div>
      <button type="submit">运行任务</button>
    </form>
  </div>

  <div class="card">
    <h2>定时任务</h2>
    <form method="post" action="/schedule">
      <div class="flex">
        <label>层级
          <select name="layer">
            <option value="base">base</option>
            <option value="ods">ods</option>
            <option value="dwd">dwd</option>
            <option value="dws">dws</option>
            <option value="ads">ads</option>
          </select>
        </label>
        <label>模式
          <select name="mode">
            <option value="incremental">incremental</option>
            <option value="full">full</option>
          </select>
        </label>
        <label>Cron Hour
          <input type="text" name="cron_hour" value="2" />
        </label>
        <label>Cron Minute
          <input type="text" name="cron_minute" value="0" />
        </label>
        <label>Token
          <input type="text" name="token" value="{{ token or '' }}" />
        </label>
        <label>Start Date
          <input type="number" name="start_date" value="{{ params.start_date }}" />
        </label>
        <label>Fina Start
          <input type="number" name="fina_start" value="{{ params.fina_start or '' }}" />
        </label>
        <label>Fina End
          <input type="number" name="fina_end" value="{{ params.fina_end or '' }}" />
        </label>
        <label>Rate Limit
          <input type="number" name="rate_limit" value="{{ params.rate_limit }}" />
        </label>
      </div>
      <button type="submit">添加定时任务</button>
    </form>
    <h3>已安排任务</h3>
    <table>
      <thead>
        <tr>
          <th>ID</th>
          <th>Next Run</th>
          <th>Trigger</th>
        </tr>
      </thead>
      <tbody>
      {% for job in jobs %}
        <tr>
          <td>{{ job.id }}</td>
          <td>{{ job.next_run_time }}</td>
          <td>{{ job.trigger }}</td>
        </tr>
      {% else %}
        <tr><td colspan="3">暂无任务</td></tr>
      {% endfor %}
      </tbody>
    </table>
  </div>

  <div class="card">
    <h2>最近任务执行情况</h2>
    {% if logs_error %}
      <div class="error">读取日志失败：{{ logs_error }}</div>
    {% endif %}
    <table>
      <thead>
        <tr>
          <th>ID</th>
          <th>API</th>
          <th>类型</th>
          <th>开始时间</th>
          <th>结束时间</th>
          <th>状态</th>
          <th>错误</th>
        </tr>
      </thead>
      <tbody>
      {% for log in logs %}
        <tr>
          <td>{{ log.id }}</td>
          <td>{{ log.api_name }}</td>
          <td>{{ log.run_type }}</td>
          <td>{{ log.start_at }}</td>
          <td>{{ log.end_at }}</td>
          <td>{{ log.status }}</td>
          <td>{{ log.err_msg }}</td>
        </tr>
      {% else %}
        <tr><td colspan="7">暂无记录</td></tr>
      {% endfor %}
      </tbody>
    </table>
  </div>
  <div class="card">
    <h2>ODS 数据浏览</h2>
    <form method="get" action="/">
      <div class="flex">
        <label>表
          <select name="ods_table">
            {% for t in ods_tables %}
              <option value="{{ t }}" {% if t == ods_table %}selected{% endif %}>{{ t }}</option>
            {% endfor %}
          </select>
        </label>
        <label>ts_code
          <input type="text" name="search_ts_code" value="{{ search_ts_code }}" />
        </label>
        <label>trade_date
          <input type="number" name="search_trade_date" value="{{ search_trade_date }}" />
        </label>
        <input type="hidden" name="page" value="1" />
        <button type="submit">搜索</button>
      </div>
    </form>
    {% if ods_error %}
      <div class="error">读取 ODS 数据失败：{{ ods_error }}</div>
    {% else %}
      <table>
        <thead>
          <tr>
            {% for col in ods_columns %}
              <th>{{ col }}</th>
            {% endfor %}
          </tr>
        </thead>
        <tbody>
          {% for row in ods_rows %}
            <tr>
              {% for value in row %}
                <td>{{ value }}</td>
              {% endfor %}
            </tr>
          {% else %}
            <tr><td colspan="{{ ods_columns|length }}">暂无数据</td></tr>
          {% endfor %}
        </tbody>
      </table>
      <div class="pagination">
        <span>共 {{ ods_total }} 条</span>
        {% set total_pages = (ods_total // ods_page_size) + (1 if ods_total % ods_page_size else 0) %}
        <a href="/?ods_table={{ ods_table }}&search_ts_code={{ search_ts_code }}&search_trade_date={{ search_trade_date }}&page={{ ods_page - 1 }}"
           {% if ods_page <= 1 %}class="muted"{% endif %}>上一页</a>
        <span>第 {{ ods_page }} / {{ total_pages }} 页</span>
        <a href="/?ods_table={{ ods_table }}&search_ts_code={{ search_ts_code }}&search_trade_date={{ search_trade_date }}&page={{ ods_page + 1 }}"
           {% if ods_page >= total_pages %}class="muted"{% endif %}>下一页</a>
      </div>
    {% endif %}
  </div>
</body>
</html>
"""

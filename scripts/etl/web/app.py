from __future__ import annotations

import threading
from datetime import datetime
from typing import Callable, Dict, Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from flask import Flask, redirect, render_template_string, request, url_for

from .. import ads, base, dwd, dws, ods

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


@app.route("/", methods=["GET"])
def index():
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
    logs = base.list_runs(50)
    jobs = scheduler.get_jobs()

    return render_template_string(
        _TEMPLATE,
        params=params,
        logs=logs,
        jobs=jobs,
        token=cfg_token,
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
  </style>
</head>
<body>
  <h1>ETL 控制台</h1>
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
</body>
</html>
"""

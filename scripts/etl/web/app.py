from __future__ import annotations

import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from flask import Flask, jsonify, request, send_from_directory

from .. import ads, base, dwd, dws, ods
from ..base.runtime import get_env_config, get_mysql_connection

ROOT_DIR = Path(__file__).resolve().parents[3]
DIST_DIR = ROOT_DIR / "app" / "dist"

app = Flask(__name__, static_folder=str(DIST_DIR), static_url_path="")

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


def _format_datetime(value: Optional[datetime]) -> Optional[str]:
    if not value:
        return None
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _json_error(message: str, status_code: int = 500):
    return jsonify({"error": message}), status_code


def _get_payload() -> dict:
    if request.is_json:
        payload = request.get_json(silent=True)
        return payload if isinstance(payload, dict) else {}
    return request.form.to_dict()


def _serialize_job(job) -> dict:
    next_run = job.next_run_time.isoformat() if job.next_run_time else None
    return {
        "id": job.id,
        "next_run_time": next_run,
        "trigger": str(job.trigger),
    }


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


@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def index(path: str):
    if path and (DIST_DIR / path).is_file():
        return send_from_directory(DIST_DIR, path)
    return send_from_directory(DIST_DIR, "index.html")


@app.route("/api/ods/status", methods=["GET"])
def api_ods_status():
    try:
        ods_status = _get_last_ods_run()
        if not ods_status:
            return jsonify({"data": None})
        return jsonify(
            {
                "data": {
                    "status": ods_status["status"],
                    "start_at": _format_datetime(ods_status["start_at"]),
                    "end_at": _format_datetime(ods_status["end_at"]),
                    "duration": ods_status["duration"],
                }
            }
        )
    except Exception as exc:
        return _json_error(str(exc))


@app.route("/api/ods/features-run", methods=["POST"])
def api_run_ods_features():
    payload = _get_payload()
    try:
        params = {
            "token": payload.get("token"),
            "start_date": int(payload.get("start_date")),
            "end_date": int(payload.get("end_date")),
            "rate_limit": int(payload.get("rate_limit", 500)),
            "cyq_rate_limit": int(payload.get("cyq_rate_limit", 180)),
            "apis": payload.get("apis", "stk_factor"),
            "config": payload.get("config") or None,
        }
        _run_ods_features_async(params)
        return jsonify({"message": "ODS features task scheduled."})
    except Exception as exc:
        return _json_error(str(exc))


@app.route("/api/tasks/run", methods=["POST"])
def api_run_task():
    payload = _get_payload()
    try:
        layer = payload["layer"]
        mode = payload["mode"]
        params = {
            "token": payload.get("token"),
            "start_date": int(payload.get("start_date", 20100101)),
            "fina_start": payload.get("fina_start"),
            "fina_end": payload.get("fina_end"),
            "rate_limit": int(payload.get("rate_limit", 500)),
        }
        if params["fina_start"]:
            params["fina_start"] = int(params["fina_start"])
        if params["fina_end"]:
            params["fina_end"] = int(params["fina_end"])
        _run_async(layer, mode, params)
        return jsonify({"message": "Task triggered."})
    except Exception as exc:
        return _json_error(str(exc))


@app.route("/api/tasks/schedule", methods=["GET", "POST"])
def api_schedule_task():
    if request.method == "GET":
        jobs = scheduler.get_jobs()
        return jsonify({"data": [_serialize_job(job) for job in jobs]})
    payload = _get_payload()
    try:
        layer = payload["layer"]
        mode = payload["mode"]
        params = {
            "token": payload.get("token"),
            "start_date": int(payload.get("start_date", 20100101)),
            "fina_start": payload.get("fina_start"),
            "fina_end": payload.get("fina_end"),
            "rate_limit": int(payload.get("rate_limit", 500)),
        }
        if params["fina_start"]:
            params["fina_start"] = int(params["fina_start"])
        if params["fina_end"]:
            params["fina_end"] = int(params["fina_end"])
        cron = {
            "minute": payload.get("cron_minute", "0"),
            "hour": payload.get("cron_hour", "2"),
        }
        job_id = _schedule_job(layer, mode, params, cron)
        return jsonify({"message": "Scheduled.", "job_id": job_id})
    except Exception as exc:
        return _json_error(str(exc))


@app.route("/api/tasks/schedule/<job_id>", methods=["DELETE"])
def api_delete_schedule(job_id: str):
    try:
        scheduler.remove_job(job_id)
        return jsonify({"message": "Deleted."})
    except Exception as exc:
        return _json_error(str(exc))


@app.route("/api/tasks/history", methods=["GET"])
def api_task_history():
    try:
        limit = int(request.args.get("limit", 50))
        logs = base.list_runs(limit)
        data = []
        for log in logs:
            data.append(
                {
                    "id": log["id"],
                    "api": log["api_name"],
                    "type": log["run_type"],
                    "start_time": _format_datetime(log["start_at"]),
                    "end_time": _format_datetime(log["end_at"]),
                    "status": log["status"],
                    "error": log["err_msg"],
                }
            )
        return jsonify({"data": data})
    except Exception as exc:
        return _json_error(str(exc))


@app.route("/api/ods/tables", methods=["GET"])
def api_ods_tables():
    return jsonify({"data": ODS_TABLES})


@app.route("/api/ods/rows", methods=["GET"])
def api_ods_rows():
    try:
        table = request.args.get("table", "ods_daily")
        page = int(request.args.get("page", 1))
        search_ts_code = request.args.get("search_ts_code") or None
        search_trade_date = request.args.get("search_trade_date") or None
        search_trade_date_int = int(search_trade_date) if search_trade_date else None
        columns, rows, total = _fetch_ods_rows(
            table,
            page,
            search_ts_code,
            search_trade_date_int,
        )
        return jsonify(
            {
                "data": {
                    "columns": columns,
                    "rows": [list(row) for row in rows],
                    "total": total,
                    "page": page,
                    "page_size": 50,
                }
            }
        )
    except Exception as exc:
        return _json_error(str(exc))


from __future__ import annotations

import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from flask import Flask, jsonify, make_response, request, send_from_directory

from .. import ads, base, dwd, dws, ods
from ..base.runtime import get_env_config, get_mysql_session

ROOT_DIR = Path(__file__).resolve().parents[3]
DIST_DIR = ROOT_DIR / "app" / "dist"

app = Flask(__name__)

scheduler = BackgroundScheduler()
if not scheduler.running:
    scheduler.start()


@app.before_request
def handle_preflight():
    if request.method != "OPTIONS" or not request.path.startswith("/api/"):
        return None
    response = make_response("", 204)
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET,POST,DELETE,OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return response


@app.after_request
def add_cors_headers(response):
    if request.path.startswith("/api/"):
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET,POST,DELETE,OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return response


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
            ods.run_incremental(
                token, 
                params.get("start_date"), 
                params.get("end_date"), 
                params["rate_limit"]
            )
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
            dwd.run_incremental(params.get("start_date"), params.get("end_date"))
        if params.get("fina_start") and params.get("fina_end"):
            dwd.run_fina_incremental(params["fina_start"], params["fina_end"])
        return
    if layer == "dws":
        if mode == "full":
            dws.run_full(params["start_date"])
        else:
            dws.run_incremental(params.get("start_date"), params.get("end_date"))
        return
    if layer == "ads":
        if mode == "full":
            ads.run_full(params["start_date"])
        else:
            ads.run_incremental(params.get("start_date"), params.get("end_date"))
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
    with get_mysql_session(cfg) as conn:
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
    # Parse ID to get readable name
    name = job.id
    try:
        parts = job.id.split("-")
        if len(parts) >= 2:
            name = f"{parts[0].upper()} {parts[1].capitalize()}"
    except:
        pass

    return {
        "id": job.id,
        "name": name,
        "next_run_time": next_run,
        "trigger": str(job.trigger),
        "args": job.args if hasattr(job, "args") else [],
    }


@app.route("/api/jobs", methods=["GET"])
def api_list_jobs():
    jobs = scheduler.get_jobs()
    return jsonify({"data": [_serialize_job(job) for job in jobs]})


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
    with get_mysql_session(cfg) as conn:
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


@app.route("/api/readme", methods=["GET"])
def api_readme():
    try:
        return send_from_directory(ROOT_DIR, "README.md")
    except Exception as exc:
        return _json_error(str(exc))


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
        
        # 处理默认触发逻辑
        start_date = payload.get("start_date")
        end_date = payload.get("end_date")
        
        # 如果是 default_trigger，且没有显式指定日期，可以在这里计算默认日期
        if payload.get("default_trigger"):
             # 此处保留为空，让 runner 决定（通常是增量）
             # 或者如果用户意图是"跑今天"，可以显式设置
             pass

        params = {
            "token": payload.get("token"),
            "start_date": int(start_date) if start_date else None,
            "end_date": int(end_date) if end_date else None,
            "fina_start": payload.get("fina_start"),
            "fina_end": payload.get("fina_end"),
            "rate_limit": int(payload.get("rate_limit", 500)),
        }
        if params["fina_start"]:
            params["fina_start"] = int(params["fina_start"])
        if params["fina_end"]:
            params["fina_end"] = int(params["fina_end"])
            
        # 补充默认 start_date for full run if missing
        if mode == "full" and not params["start_date"]:
             params["start_date"] = 20180101

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


# ============== 数据状态 API ==============

@app.route("/api/data/status", methods=["GET"])
def api_data_status():
    """获取各层数据就绪状态"""
    try:
        cfg = get_env_config()
        status = {}
        
        with get_mysql_session(cfg) as conn:
            with conn.cursor() as cursor:
                # ODS层状态
                ods_tables = [
                    ("ods_daily", "ODS日线数据"),
                    ("ods_daily_basic", "ODS每日指标"),
                    ("ods_stk_factor", "ODS技术因子"),
                    ("ods_moneyflow", "ODS资金流向"),
                    ("ods_margin_detail", "ODS融资融券"),
                    ("ods_cyq_perf", "ODS筹码指标"),
                ]
                status["ods"] = []
                for table, name in ods_tables:
                    try:
                        cursor.execute(f"SELECT MAX(trade_date), COUNT(DISTINCT trade_date), COUNT(*) FROM {table}")
                        row = cursor.fetchone()
                        status["ods"].append({
                            "table": table,
                            "name": name,
                            "latest_date": row[0],
                            "date_count": row[1],
                            "row_count": row[2],
                            "ready": row[0] is not None
                        })
                    except Exception:
                         status["ods"].append({
                            "table": table,
                            "name": name,
                            "latest_date": None,
                            "date_count": 0,
                            "row_count": 0,
                            "ready": False
                        })

                # DWD层状态
                dwd_tables = [
                    ("dwd_daily_basic", "DWD日线基础"),
                ]
                status["dwd"] = []
                for table, name in dwd_tables:
                    try:
                        cursor.execute(f"SELECT MAX(trade_date), COUNT(DISTINCT trade_date), COUNT(*) FROM {table}")
                        row = cursor.fetchone()
                        status["dwd"].append({
                            "table": table,
                            "name": name,
                            "latest_date": row[0],
                            "date_count": row[1],
                            "row_count": row[2],
                            "ready": row[0] is not None
                        })
                    except Exception:
                        pass
                
                # DWS层状态 (评分表)
                dws_tables = [
                    ("dws_momentum_score", "动量评分"),
                    ("dws_value_score", "价值评分"),
                    ("dws_quality_score", "质量评分"),
                    ("dws_technical_score", "技术评分"),
                    ("dws_capital_score", "资金评分"),
                    ("dws_chip_score", "筹码评分"),
                ]
                status["dws"] = []
                for table, name in dws_tables:
                    try:
                        cursor.execute(f"SELECT MAX(trade_date), COUNT(DISTINCT trade_date), COUNT(*) FROM {table}")
                        row = cursor.fetchone()
                        status["dws"].append({
                            "table": table,
                            "name": name,
                            "latest_date": row[0],
                            "date_count": row[1],
                            "row_count": row[2],
                            "ready": row[0] is not None
                        })
                    except Exception:
                        pass
                
                # ADS层状态
                ads_tables = [
                    ("ads_features_stock_daily", "ADS特征日表"),
                ]
                status["ads"] = []
                for table, name in ads_tables:
                    try:
                        cursor.execute(f"SELECT MAX(trade_date), COUNT(DISTINCT trade_date), COUNT(*) FROM {table}")
                        row = cursor.fetchone()
                        status["ads"].append({
                            "table": table,
                            "name": name,
                            "latest_date": row[0],
                            "date_count": row[1],
                            "row_count": row[2],
                            "ready": row[0] is not None
                        })
                    except Exception:
                        pass

                # 获取最新交易日
                cursor.execute("SELECT MAX(trade_date) FROM ods_daily")
                row = cursor.fetchone()
                status["latest_trade_date"] = row[0] if row else None
        
        return jsonify({"data": status})
    except Exception as exc:
        return _json_error(str(exc))


# ============== 股票评分 API ==============

@app.route("/api/scores", methods=["GET"])
def api_scores():
    """获取股票评分，支持排序和分页"""
    try:
        trade_date = request.args.get("trade_date")
        sort_by = request.args.get("sort_by", "total_score")
        sort_order = request.args.get("sort_order", "desc")
        page = int(request.args.get("page", 1))
        page_size = int(request.args.get("page_size", 50))
        search_ts_code = request.args.get("ts_code") or None
        score_type = request.args.get("type", "claude")  # claude or fama
        
        cfg = get_env_config()
        
        # 验证排序字段
        valid_sort_fields = [
            "ts_code", "name", "pct_chg",
            "momentum_score", "value_score", "quality_score", 
            "technical_score", "capital_score", "chip_score", 
            "total_score", "size_score"
        ]
        if sort_by not in valid_sort_fields:
            sort_by = "total_score"
        
        sort_direction = "DESC" if sort_order.lower() == "desc" else "ASC"
        
        with get_mysql_session(cfg) as conn:
            with conn.cursor() as cursor:
                # 获取最新交易日
                if not trade_date:
                    cursor.execute("SELECT MAX(trade_date) FROM dws_momentum_score")
                    trade_date = cursor.fetchone()[0]
                else:
                    trade_date = int(trade_date)
                
                if not trade_date:
                    return jsonify({"data": {"rows": [], "total": 0, "trade_date": None}})
                
                # 构建查询 SQL
                if score_type == "fama":
                    base_sql = """
                    SELECT 
                        s.ts_code, ds.name, od.pct_chg,
                        s.size_score,
                        m.momentum_score, v.value_score, q.quality_score,
                        t.technical_score, c.capital_score, ch.chip_score,
                        (COALESCE(s.size_score, 0) + COALESCE(m.momentum_score, 0) + 
                         COALESCE(v.value_score, 0) + COALESCE(q.quality_score, 0) + 
                         COALESCE(t.technical_score, 0) + COALESCE(c.capital_score, 0) + 
                         COALESCE(ch.chip_score, 0)) AS total_score
                    FROM dws_fama_size_score s
                    LEFT JOIN dws_fama_momentum_score m ON s.trade_date = m.trade_date AND s.ts_code = m.ts_code
                    LEFT JOIN dws_fama_value_score v ON s.trade_date = v.trade_date AND s.ts_code = v.ts_code
                    LEFT JOIN dws_fama_quality_score q ON s.trade_date = q.trade_date AND s.ts_code = q.ts_code
                    LEFT JOIN dws_fama_technical_score t ON s.trade_date = t.trade_date AND s.ts_code = t.ts_code
                    LEFT JOIN dws_fama_capital_score c ON s.trade_date = c.trade_date AND s.ts_code = c.ts_code
                    LEFT JOIN dws_fama_chip_score ch ON s.trade_date = ch.trade_date AND s.ts_code = ch.ts_code
                    LEFT JOIN ods_daily od ON s.trade_date = od.trade_date AND s.ts_code = od.ts_code
                    LEFT JOIN dim_stock ds ON s.ts_code = ds.ts_code
                    WHERE s.trade_date = %s
                    """
                else:
                    base_sql = """
                    SELECT 
                        m.ts_code, ds.name, od.pct_chg,
                        NULL AS size_score,
                        m.momentum_score, v.value_score, q.quality_score,
                        t.technical_score, c.capital_score, ch.chip_score,
                        (COALESCE(m.momentum_score, 0) + COALESCE(v.value_score, 0) + 
                         COALESCE(q.quality_score, 0) + COALESCE(t.technical_score, 0) + 
                         COALESCE(c.capital_score, 0) + COALESCE(ch.chip_score, 0)) AS total_score
                    FROM dws_momentum_score m
                    LEFT JOIN dws_value_score v ON m.trade_date = v.trade_date AND m.ts_code = v.ts_code
                    LEFT JOIN dws_quality_score q ON m.trade_date = q.trade_date AND m.ts_code = q.ts_code
                    LEFT JOIN dws_technical_score t ON m.trade_date = t.trade_date AND m.ts_code = t.ts_code
                    LEFT JOIN dws_capital_score c ON m.trade_date = c.trade_date AND m.ts_code = c.ts_code
                    LEFT JOIN dws_chip_score ch ON m.trade_date = ch.trade_date AND m.ts_code = ch.ts_code
                    LEFT JOIN ods_daily od ON m.trade_date = od.trade_date AND m.ts_code = od.ts_code
                    LEFT JOIN dim_stock ds ON m.ts_code = ds.ts_code
                    WHERE m.trade_date = %s
                    """
                
                params = [trade_date]
                
                if search_ts_code:
                    base_sql += " AND m.ts_code LIKE %s"
                    params.append(f"%{search_ts_code}%")
                
                # 计数
                count_sql = f"SELECT COUNT(*) FROM ({base_sql}) AS sub"
                cursor.execute(count_sql, params)
                total = cursor.fetchone()[0]
                
                # 分页查询
                offset = (page - 1) * page_size
                query_sql = f"""
                    {base_sql}
                    ORDER BY {sort_by} {sort_direction}
                    LIMIT %s OFFSET %s
                """
                cursor.execute(query_sql, params + [page_size, offset])
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
        
        return jsonify({
            "data": {
                "columns": columns,
                "rows": [list(row) for row in rows],
                "total": total,
                "page": page,
                "page_size": page_size,
                "trade_date": trade_date,
                "sort_by": sort_by,
                "sort_order": sort_order
            }
        })
    except Exception as exc:
        return _json_error(str(exc))


@app.route("/api/scores/top", methods=["GET"])
def api_scores_top():
    """获取 Top N 股票评分"""
    try:
        trade_date = request.args.get("trade_date")
        top_n = int(request.args.get("top_n", 50))
        score_type = request.args.get("type", "claude")
        
        cfg = get_env_config()
        
        with get_mysql_session(cfg) as conn:
            with conn.cursor() as cursor:
                if not trade_date:
                    cursor.execute("SELECT MAX(trade_date) FROM dws_momentum_score")
                    trade_date = cursor.fetchone()[0]
                else:
                    trade_date = int(trade_date)
                
                if not trade_date:
                    return jsonify({"data": []})
                
                if score_type == "fama":
                    sql = """
                    SELECT 
                        s.ts_code, ds.name, od.pct_chg,
                        s.size_score, m.momentum_score, v.value_score, q.quality_score,
                        t.technical_score, c.capital_score, ch.chip_score,
                        (COALESCE(s.size_score, 0) + COALESCE(m.momentum_score, 0) + 
                         COALESCE(v.value_score, 0) + COALESCE(q.quality_score, 0) + 
                         COALESCE(t.technical_score, 0) + COALESCE(c.capital_score, 0) + 
                         COALESCE(ch.chip_score, 0)) AS total_score
                    FROM dws_fama_size_score s
                    LEFT JOIN dws_fama_momentum_score m ON s.trade_date = m.trade_date AND s.ts_code = m.ts_code
                    LEFT JOIN dws_fama_value_score v ON s.trade_date = v.trade_date AND s.ts_code = v.ts_code
                    LEFT JOIN dws_fama_quality_score q ON s.trade_date = q.trade_date AND s.ts_code = q.ts_code
                    LEFT JOIN dws_fama_technical_score t ON s.trade_date = t.trade_date AND s.ts_code = t.ts_code
                    LEFT JOIN dws_fama_capital_score c ON s.trade_date = c.trade_date AND s.ts_code = c.ts_code
                    LEFT JOIN dws_fama_chip_score ch ON s.trade_date = ch.trade_date AND s.ts_code = ch.ts_code
                    LEFT JOIN ods_daily od ON s.trade_date = od.trade_date AND s.ts_code = od.ts_code
                    LEFT JOIN dim_stock ds ON s.ts_code = ds.ts_code
                    WHERE s.trade_date = %s
                    ORDER BY total_score DESC
                    LIMIT %s
                    """
                else:
                    sql = """
                    SELECT 
                        m.ts_code, ds.name, od.pct_chg,
                        NULL AS size_score,
                        m.momentum_score, v.value_score, q.quality_score,
                        t.technical_score, c.capital_score, ch.chip_score,
                        (COALESCE(m.momentum_score, 0) + COALESCE(v.value_score, 0) + 
                         COALESCE(q.quality_score, 0) + COALESCE(t.technical_score, 0) + 
                         COALESCE(c.capital_score, 0) + COALESCE(ch.chip_score, 0)) AS total_score
                    FROM dws_momentum_score m
                    LEFT JOIN dws_value_score v ON m.trade_date = v.trade_date AND m.ts_code = v.ts_code
                    LEFT JOIN dws_quality_score q ON m.trade_date = q.trade_date AND m.ts_code = q.ts_code
                    LEFT JOIN dws_technical_score t ON m.trade_date = t.trade_date AND m.ts_code = t.ts_code
                    LEFT JOIN dws_capital_score c ON m.trade_date = c.trade_date AND m.ts_code = c.ts_code
                    LEFT JOIN dws_chip_score ch ON m.trade_date = ch.trade_date AND m.ts_code = ch.ts_code
                    LEFT JOIN ods_daily od ON m.trade_date = od.trade_date AND m.ts_code = od.ts_code
                    LEFT JOIN dim_stock ds ON m.ts_code = ds.ts_code
                    WHERE m.trade_date = %s
                    ORDER BY total_score DESC
                    LIMIT %s
                    """
                
                cursor.execute(sql, [trade_date, top_n])
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
        
        # 添加排名
        result = []
        for i, row in enumerate(rows, 1):
            row_dict = dict(zip(columns, row))
            row_dict["rank"] = i
            result.append(row_dict)
        
        return jsonify({
            "data": result,
            "trade_date": trade_date,
            "type": score_type
        })
    except Exception as exc:
        return _json_error(str(exc))


@app.route("/api/scores/compare", methods=["GET"])
def api_scores_compare():
    """对比 Claude 和 Fama 评分系统"""
    try:
        trade_date = request.args.get("trade_date")
        top_n = int(request.args.get("top_n", 50))
        
        cfg = get_env_config()
        
        with get_mysql_session(cfg) as conn:
            with conn.cursor() as cursor:
                if not trade_date:
                    cursor.execute("SELECT MAX(trade_date) FROM dws_momentum_score")
                    trade_date = cursor.fetchone()[0]
                else:
                    trade_date = int(trade_date)
                
                if not trade_date:
                    return jsonify({"data": []})
                
                sql = """
                SELECT 
                    c.ts_code,
                    ds.name,
                    c.claude_score,
                    f.fama_score,
                    f.fama_score - c.claude_score AS score_diff
                FROM (
                    SELECT 
                        m.ts_code,
                        (COALESCE(m.momentum_score, 0) + COALESCE(v.value_score, 0) + 
                         COALESCE(q.quality_score, 0) + COALESCE(t.technical_score, 0) + 
                         COALESCE(c.capital_score, 0) + COALESCE(ch.chip_score, 0)) AS claude_score
                    FROM dws_momentum_score m
                    LEFT JOIN dws_value_score v ON m.trade_date = v.trade_date AND m.ts_code = v.ts_code
                    LEFT JOIN dws_quality_score q ON m.trade_date = q.trade_date AND m.ts_code = q.ts_code
                    LEFT JOIN dws_technical_score t ON m.trade_date = t.trade_date AND m.ts_code = t.ts_code
                    LEFT JOIN dws_capital_score c ON m.trade_date = c.trade_date AND m.ts_code = c.ts_code
                    LEFT JOIN dws_chip_score ch ON m.trade_date = ch.trade_date AND m.ts_code = ch.ts_code
                    WHERE m.trade_date = %s
                ) c
                LEFT JOIN (
                    SELECT 
                        s.ts_code,
                        (COALESCE(s.size_score, 0) + COALESCE(m.momentum_score, 0) + 
                         COALESCE(v.value_score, 0) + COALESCE(q.quality_score, 0) + 
                         COALESCE(t.technical_score, 0) + COALESCE(c.capital_score, 0) + 
                         COALESCE(ch.chip_score, 0)) AS fama_score
                    FROM dws_fama_size_score s
                    LEFT JOIN dws_fama_momentum_score m ON s.trade_date = m.trade_date AND s.ts_code = m.ts_code
                    LEFT JOIN dws_fama_value_score v ON s.trade_date = v.trade_date AND s.ts_code = v.ts_code
                    LEFT JOIN dws_fama_quality_score q ON s.trade_date = q.trade_date AND s.ts_code = q.ts_code
                    LEFT JOIN dws_fama_technical_score t ON s.trade_date = t.trade_date AND s.ts_code = t.ts_code
                    LEFT JOIN dws_fama_capital_score c ON s.trade_date = c.trade_date AND s.ts_code = c.ts_code
                    LEFT JOIN dws_fama_chip_score ch ON s.trade_date = ch.trade_date AND s.ts_code = ch.ts_code
                    WHERE s.trade_date = %s
                ) f ON c.ts_code = f.ts_code
                LEFT JOIN dim_stock ds ON c.ts_code = ds.ts_code
                ORDER BY c.claude_score DESC
                LIMIT %s
                """
                
                cursor.execute(sql, [trade_date, trade_date, top_n])
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
        
        result = [dict(zip(columns, row)) for row in rows]
        
        return jsonify({
            "data": result,
            "trade_date": trade_date
        })
    except Exception as exc:
        return _json_error(str(exc))


@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve(path):
    if path != "" and (DIST_DIR / path).exists():
        return send_from_directory(DIST_DIR, path)
    else:
        return send_from_directory(DIST_DIR, "index.html")

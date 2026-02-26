#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
LOG_FILE="${LOG_DIR}/web_console.log"
PID_FILE="${LOG_DIR}/web_console.pid"
HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-5999}"
CONFIG="${CONFIG:-config/etl.ini}"
PYTHON_BIN="${PYTHON_BIN:-${PROJECT_ROOT}/.venv/bin/python}"

if [[ ! -x "${PYTHON_BIN}" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python)"
  else
    echo "Error: python not found. Please install python3 or set PYTHON_BIN."
    exit 1
  fi
fi

usage() {
  cat <<'EOF'
Usage:
  scripts/run_web_console.sh [start|stop|restart|status] [--host HOST] [--port PORT] [--config PATH]

Examples:
  scripts/run_web_console.sh start
  scripts/run_web_console.sh start --port 6000
  scripts/run_web_console.sh restart --host 127.0.0.1 --config config/etl.ini
EOF
}

is_pid_running() {
  local pid="${1:-}"
  [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null
}

read_pid() {
  if [[ -f "${PID_FILE}" ]]; then
    tr -d '[:space:]' < "${PID_FILE}"
  fi
}

start_service() {
  mkdir -p "${LOG_DIR}"
  local current_pid
  current_pid="$(read_pid || true)"

  if is_pid_running "${current_pid}"; then
    echo "Web console is already running. PID=${current_pid}"
    echo "Log: ${LOG_FILE}"
    return 0
  fi

  if [[ -f "${PID_FILE}" ]]; then
    rm -f "${PID_FILE}"
  fi

  echo "Starting web console..."
  (
    cd "${PROJECT_ROOT}"
    nohup "${PYTHON_BIN}" scripts/run_web.py \
      --host "${HOST}" \
      --port "${PORT}" \
      --config "${CONFIG}" \
      >> "${LOG_FILE}" 2>&1 &
    echo $! > "${PID_FILE}"
  )

  sleep 1
  local new_pid
  new_pid="$(read_pid || true)"
  if is_pid_running "${new_pid}"; then
    echo "Started successfully. PID=${new_pid}"
    echo "URL: http://${HOST}:${PORT}"
    echo "Log: ${LOG_FILE}"
    return 0
  fi

  echo "Failed to start web console. Check log: ${LOG_FILE}"
  tail -n 40 "${LOG_FILE}" 2>/dev/null || true
  return 1
}

stop_service() {
  local current_pid
  current_pid="$(read_pid || true)"
  if ! is_pid_running "${current_pid}"; then
    echo "Web console is not running."
    rm -f "${PID_FILE}"
    return 0
  fi

  echo "Stopping web console. PID=${current_pid}"
  kill -TERM "${current_pid}" 2>/dev/null || true

  for _ in {1..20}; do
    if ! is_pid_running "${current_pid}"; then
      rm -f "${PID_FILE}"
      echo "Stopped."
      return 0
    fi
    sleep 0.5
  done

  echo "Graceful stop timed out, forcing kill..."
  kill -KILL "${current_pid}" 2>/dev/null || true
  rm -f "${PID_FILE}"
  echo "Stopped (forced)."
}

status_service() {
  local current_pid
  current_pid="$(read_pid || true)"
  if is_pid_running "${current_pid}"; then
    echo "Web console is running. PID=${current_pid}"
    echo "Log: ${LOG_FILE}"
  else
    echo "Web console is not running."
  fi
}

ACTION="${1:-status}"
if [[ $# -gt 0 ]]; then
  shift
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      HOST="$2"
      shift 2
      ;;
    --port)
      PORT="$2"
      shift 2
      ;;
    --config)
      CONFIG="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1"
      usage
      exit 2
      ;;
  esac
done

case "${ACTION}" in
  start)
    start_service
    ;;
  stop)
    stop_service
    ;;
  restart)
    stop_service
    start_service
    ;;
  status)
    status_service
    ;;
  *)
    echo "Unknown action: ${ACTION}"
    usage
    exit 2
    ;;
esac

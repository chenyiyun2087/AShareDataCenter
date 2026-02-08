#!/usr/bin/env python3
"""
Flask 管理后台启动脚本

用法:
    python scripts/run_web.py
    python scripts/run_web.py --port 5999
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# 设置默认配置路径
PROJECT_ROOT = Path(__file__).parent.parent
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "etl.ini"

# 添加 scripts 目录到 Python 路径
SCRIPTS_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPTS_DIR))


def main():
    parser = argparse.ArgumentParser(description="启动 Flask 管理后台")
    parser.add_argument("--host", default="0.0.0.0", help="绑定地址 (默认: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=5999, help="端口 (默认: 5999)")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="配置文件路径")
    parser.add_argument("--debug", action="store_true", help="调试模式")
    args = parser.parse_args()
    
    # 设置配置路径环境变量
    os.environ["ETL_CONFIG_PATH"] = args.config
    print(f"📋 使用配置: {args.config}")
    
    from etl.web.app import app
    
    print(f"🚀 启动管理后台: http://{args.host}:{args.port}")
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()

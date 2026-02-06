#!/usr/bin/env python3
import argparse
import logging

from etl.web import app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ETL Web Server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=5500)
    parser.add_argument("--debug", action="store_true")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    args = parse_args()
    logging.info(f"Starting Web Server with args: {args}")
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()

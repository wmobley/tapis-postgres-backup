#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

from config import get_settings


def run_subprocess(args: list[str]) -> int:
    process = subprocess.run(args, check=False)
    return int(process.returncode)


def backup_once(python_bin: str) -> int:
    settings = get_settings()
    return run_subprocess([python_bin, "tapis_postgres_backup.py", "--log-level", settings.TAPIS_POSTGRES_BACKUP_LOG_LEVEL])


def backup_loop(python_bin: str) -> int:
    settings = get_settings()
    if settings.TAPIS_POSTGRES_BACKUP_RUN_IMMEDIATELY:
        rc = backup_once(python_bin)
        if rc != 0:
            logging.error("Initial backup run failed with exit code %s", rc)
    interval = max(60, settings.TAPIS_POSTGRES_BACKUP_INTERVAL_SECONDS)
    while True:
        time.sleep(interval)
        rc = backup_once(python_bin)
        if rc != 0:
            logging.error("Scheduled backup run failed with exit code %s", rc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Container runner for tapis-postgres-backup.")
    parser.add_argument(
        "--mode",
        choices=["backup-once", "backup-loop", "restore"],
        default=None,
        help="Override TAPIS_POSTGRES_BACKUP_MODE.",
    )
    parser.add_argument("restore_args", nargs=argparse.REMAINDER)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    settings = get_settings()
    logging.basicConfig(level=getattr(logging, settings.TAPIS_POSTGRES_BACKUP_LOG_LEVEL.upper(), logging.INFO))
    mode = args.mode or settings.TAPIS_POSTGRES_BACKUP_MODE
    python_bin = os.environ.get("PYTHON_BIN", "python")

    if mode == "backup-once":
        return backup_once(python_bin)
    if mode == "backup-loop":
        return backup_loop(python_bin)
    if mode == "restore":
        restore_args = args.restore_args[1:] if args.restore_args and args.restore_args[0] == "--" else args.restore_args
        return run_subprocess([python_bin, "tapis_postgres_restore.py", *restore_args])
    raise SystemExit(f"Unsupported mode: {mode}")


if __name__ == "__main__":
    raise SystemExit(main())

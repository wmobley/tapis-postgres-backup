#!/usr/bin/env python3
"""Back up Upstream Postgres pods into Tapis Files on Corral."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

TOOLS_ROOT = Path(__file__).resolve().parent
UPSTREAM_DOCKER_PODS_ROOT = TOOLS_ROOT.parent / "upstream-docker-pods"
if str(TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(TOOLS_ROOT))
if str(UPSTREAM_DOCKER_PODS_ROOT) not in sys.path:
    sys.path.insert(0, str(UPSTREAM_DOCKER_PODS_ROOT))

from backup import BackupManager, TapisBackupClient, resolve_tapis_token
from app.core.config import get_settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--token", help="Explicit Tapis access token.")
    parser.add_argument(
        "--base-url",
        default=None,
        help="Tapis API base URL. Defaults to TAPIS_PODS_BASE_URL or TAPIS_BASE_URL.",
    )
    parser.add_argument(
        "--system-id",
        default=None,
        help="Tapis Files system id for backups. Defaults to TAPIS_BACKUP_SYSTEM_ID.",
    )
    parser.add_argument(
        "--root-path",
        default=None,
        help="Remote root path under the Tapis system. Defaults to TAPIS_BACKUP_ROOT_PATH.",
    )
    parser.add_argument(
        "--staging-dir",
        default=None,
        help="Local staging directory for temporary dump files.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))

    settings = get_settings()
    if args.system_id:
        settings.TAPIS_BACKUP_SYSTEM_ID = args.system_id
    if args.root_path:
        settings.TAPIS_BACKUP_ROOT_PATH = args.root_path
    if args.staging_dir:
        settings.TAPIS_BACKUP_STAGING_DIR = args.staging_dir

    token = resolve_tapis_token(explicit_token=args.token, settings=settings)
    client = TapisBackupClient(
        token=token,
        base_url=args.base_url or settings.TAPIS_PODS_BASE_URL or settings.TAPIS_BASE_URL,
        timeout_seconds=settings.TAPIS_BACKUP_TIMEOUT_SECONDS,
    )
    manager = BackupManager(
        client=client,
        settings=settings,
        staging_dir=Path(settings.TAPIS_BACKUP_STAGING_DIR),
    )
    summary = manager.run_backup()
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all(item["success"] for item in summary["results"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())

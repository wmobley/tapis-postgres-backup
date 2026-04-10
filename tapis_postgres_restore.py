#!/usr/bin/env python3
"""Restore a Tapis-backed Postgres dump into an Upstream Postgres pod."""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path

TOOLS_ROOT = Path(__file__).resolve().parent
UPSTREAM_DOCKER_PODS_ROOT = TOOLS_ROOT.parent / "upstream-docker-pods"
if str(TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(TOOLS_ROOT))
if str(UPSTREAM_DOCKER_PODS_ROOT) not in sys.path:
    sys.path.insert(0, str(UPSTREAM_DOCKER_PODS_ROOT))

from backup import RestoreManager, TapisBackupClient, resolve_tapis_token
from app.core.config import get_settings
from app.services.pods_service import PodsService


def build_postgres_payload(
    *,
    pod_id: str,
    volume_id: str,
    db_user: str,
    db_password: str,
) -> dict[str, object]:
    return {
        "pod_id": pod_id,
        "image": "postgis/postgis:17-3.5",
        "description": "postgres for upstream-docker",
        "command": ["docker-entrypoint.sh"],
        "arguments": [
            "-c",
            "ssl=on",
            "-c",
            "ssl_cert_file=/etc/ssl/certs/ssl-cert-snakeoil.pem",
            "-c",
            "ssl_key_file=/etc/ssl/private/ssl-cert-snakeoil.key",
        ],
        "environment_variables": {
            "POSTGRES_USER": db_user,
            "POSTGRES_PASSWORD": db_password,
            "POSTGRES_DB": db_user,
        },
        "status_requested": "ON",
        "volume_mounts": {
            "/var/lib/postgresql/data": {
                "type": "tapisvolume",
                "source_id": volume_id,
                "sub_path": "",
            }
        },
        "time_to_stop_default": -1,
        "networking": {
            "default": {
                "protocol": "postgres",
                "port": 5432,
                "url": f"{pod_id}.pods.tacc.tapis.io",
            }
        },
        "resources": {
            "cpu_request": 250,
            "cpu_limit": 2000,
            "mem_request": 256,
            "mem_limit": 3072,
            "gpus": 0,
        },
    }


def run_command(args: list[str], *, env: dict[str, str]) -> None:
    result = subprocess.run(args, check=False, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or f"{args[0]} failed")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pod-id", required=True, help="Source pod id used to select the backup set.")
    parser.add_argument("--token", help="Explicit Tapis access token.")
    parser.add_argument("--backup-date", help="Backup date in YYYY-MM-DD. Defaults to latest-good.")
    parser.add_argument(
        "--target-pod-id",
        help="Target Postgres pod id. Defaults to the source pod id.",
    )
    parser.add_argument(
        "--target-volume-id",
        help="Target Tapis volume id. Defaults to target pod id with postgres->volume replacement.",
    )
    parser.add_argument(
        "--reuse-existing-pod",
        action="store_true",
        help="Skip pod creation and restore into an already-running target pod.",
    )
    parser.add_argument(
        "--skip-globals",
        action="store_true",
        help="Skip applying pg_dumpall globals.sql before pg_restore.",
    )
    parser.add_argument(
        "--staging-dir",
        default=None,
        help="Local directory for temporary restore downloads.",
    )
    parser.add_argument(
        "--base-url",
        default=None,
        help="Tapis API base URL. Defaults to TAPIS_PODS_BASE_URL or TAPIS_BASE_URL.",
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
    if args.staging_dir:
        settings.TAPIS_BACKUP_STAGING_DIR = args.staging_dir

    token = resolve_tapis_token(explicit_token=args.token, settings=settings)
    client = TapisBackupClient(
        token=token,
        base_url=args.base_url or settings.TAPIS_PODS_BASE_URL or settings.TAPIS_BASE_URL,
        timeout_seconds=settings.TAPIS_BACKUP_TIMEOUT_SECONDS,
    )
    manager = RestoreManager(client=client, settings=settings)

    requested_day = date.fromisoformat(args.backup_date) if args.backup_date else None
    backup_day = manager.resolve_backup_day(pod_id=args.pod_id, requested_day=requested_day)

    target_pod_id = args.target_pod_id or args.pod_id
    target_volume_id = args.target_volume_id or target_pod_id.replace("postgres", "volume")
    Path(settings.TAPIS_BACKUP_STAGING_DIR).mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="restore-", dir=settings.TAPIS_BACKUP_STAGING_DIR) as work_dir_name:
        work_dir = Path(work_dir_name)
        manifest_path, dump_path, globals_path = manager.download_backup_set(
            pod_id=args.pod_id,
            backup_day=backup_day,
            destination=work_dir,
        )
        _ = manifest_path

        source_connection = manager.resolve_pod_connection(pod_id=args.pod_id)
        db_user = source_connection.db_user
        db_password = source_connection.db_password
        db_name = source_connection.db_name

        target_host = f"{target_pod_id}.pods.tacc.tapis.io"
        if not args.reuse_existing_pod:
            pods_service = PodsService(token_override=token)
            pods_service.create_volume(
                volume_id=target_volume_id,
                description=f"Volume for {target_pod_id.removesuffix('postgres')}",
            )
            pods_service.create_pod(
                build_postgres_payload(
                    pod_id=target_pod_id,
                    volume_id=target_volume_id,
                    db_user=db_user,
                    db_password=db_password,
                )
            )

        target_connection = manager.resolve_pod_connection(pod_id=target_pod_id)
        db_user = target_connection.db_user
        db_password = target_connection.db_password
        db_name = target_connection.db_name

        manager.wait_for_database(
            host=target_host,
            port=443,
            user=db_user,
            password=db_password,
            dbname=db_name,
        )

        env = os.environ.copy()
        env.update({"PGPASSWORD": db_password, "PGSSLMODE": "require"})

        if not args.skip_globals:
            run_command(
                [
                    "psql",
                    "--host",
                    target_host,
                    "--port",
                    "443",
                    "--username",
                    db_user,
                    "--dbname",
                    db_name,
                    "--set",
                    "ON_ERROR_STOP=0",
                    "--file",
                    str(globals_path),
                ],
                env=env,
            )

        run_command(
            [
                "pg_restore",
                "--host",
                target_host,
                "--port",
                "443",
                "--username",
                db_user,
                "--dbname",
                db_name,
                "--clean",
                "--if-exists",
                "--no-owner",
                str(dump_path),
            ],
            env=env,
        )

        summary = {
            "restored_from_pod_id": args.pod_id,
            "backup_date": backup_day.isoformat(),
            "target_pod_id": target_pod_id,
            "target_volume_id": target_volume_id,
            "database_url": f"postgresql+psycopg://{db_user}:{db_password}@{target_host}:443/{db_name}?sslmode=require",
            "note": "If the API pod does not already point to this Postgres host, update its DATABASE_URL before restarting it.",
            "restored_at": datetime.now(UTC).isoformat(),
        }
        print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

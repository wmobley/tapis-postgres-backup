#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv


SCRIPT_DIR = Path(__file__).resolve().parent
load_dotenv(SCRIPT_DIR / ".env")


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def request(*, method: str, url: str, token: str, json_body: dict[str, Any] | None = None) -> dict[str, Any]:
    response = requests.request(
        method=method,
        url=url,
        headers={
            "X-Tapis-Token": token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        json=json_body,
        timeout=60,
    )
    if not response.ok:
        raise RuntimeError(response.text.strip() or f"Request failed: {response.status_code}")
    return response.json()


def extract_result(payload: object) -> object:
    if isinstance(payload, dict) and "result" in payload:
        return payload["result"]
    return payload


def actor_payload(*, image: str, name: str, description: str, cron_schedule: str) -> dict[str, Any]:
    return {
        "image": image,
        "name": name,
        "description": description,
        "stateless": True,
        "cron_on": True,
        "cron_schedule": cron_schedule,
        "default_environment": {
            "TAPIS_BASE_URL": require_env("TAPIS_BASE_URL"),
            "TAPIS_TENANT_ID": require_env("TAPIS_TENANT_ID"),
            "TAPIS_SERVICE_USERNAME": require_env("TAPIS_SERVICE_USERNAME"),
            "TAPIS_SERVICE_PASSWORD": require_env("TAPIS_SERVICE_PASSWORD"),
            "TAPIS_BACKUP_SYSTEM_ID": require_env("TAPIS_BACKUP_SYSTEM_ID"),
            "TAPIS_BACKUP_ROOT_PATH": require_env("TAPIS_BACKUP_ROOT_PATH"),
            "TAPIS_BACKUP_RETENTION_DAYS": os.getenv("TAPIS_BACKUP_RETENTION_DAYS", "7"),
            "TAPIS_BACKUP_STAGING_DIR": os.getenv("TAPIS_BACKUP_STAGING_DIR", "/tmp/upstream-postgres-backups"),
            "TAPIS_BACKUP_TIMEOUT_SECONDS": os.getenv("TAPIS_BACKUP_TIMEOUT_SECONDS", "300"),
            "TAPIS_POSTGRES_BACKUP_MODE": "backup-once",
            "TAPIS_POSTGRES_BACKUP_LOG_LEVEL": os.getenv("TAPIS_POSTGRES_BACKUP_LOG_LEVEL", "INFO"),
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create or update a scheduled Tapis Actor for nightly backups.")
    parser.add_argument("--token", default=os.getenv("ACTOR_TEST_TOKEN"), help="Tapis token used to create/update the actor.")
    parser.add_argument("--image", default=os.getenv("ACTOR_TEST_IMAGE"), help="Container image for the actor.")
    parser.add_argument("--base-url", default=os.getenv("ACTOR_TEST_BASE_URL") or os.getenv("TAPIS_BASE_URL"), help="Tapis base URL.")
    parser.add_argument("--actor-id", default=os.getenv("ACTOR_SCHEDULE_ID"), help="Existing actor id to update.")
    parser.add_argument("--name", default=os.getenv("ACTOR_SCHEDULE_NAME", "upstream-postgres-backup"), help="Actor name.")
    parser.add_argument(
        "--description",
        default=os.getenv("ACTOR_SCHEDULE_DESCRIPTION", "Nightly Upstream Postgres backup actor"),
        help="Actor description.",
    )
    parser.add_argument(
        "--cron-schedule",
        default=os.getenv("ACTOR_SCHEDULE_CRON", ""),
        help="Cron schedule in Abaco format: 'yyyy-mm-dd hh + <increment>' (UTC).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    token = (args.token or "").strip()
    image = (args.image or "").strip()
    base_url = (args.base_url or "").rstrip("/")
    cron_schedule = (args.cron_schedule or "").strip()

    if not token:
        raise SystemExit("Missing token. Set ACTOR_TEST_TOKEN or pass --token.")
    if not image:
        raise SystemExit("Missing image. Set ACTOR_TEST_IMAGE or pass --image.")
    if not base_url:
        raise SystemExit("Missing base URL. Set ACTOR_TEST_BASE_URL/TAPIS_BASE_URL or pass --base-url.")
    if not cron_schedule:
        raise SystemExit("Missing cron schedule. Set ACTOR_SCHEDULE_CRON or pass --cron-schedule.")

    payload = actor_payload(
        image=image,
        name=args.name,
        description=args.description,
        cron_schedule=cron_schedule,
    )

    if args.actor_id:
        response = request(
            method="PUT",
            url=f"{base_url}/v3/actors/{args.actor_id}",
            token=token,
            json_body=payload,
        )
    else:
        response = request(
            method="POST",
            url=f"{base_url}/v3/actors",
            token=token,
            json_body=payload,
        )

    result = extract_result(response)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

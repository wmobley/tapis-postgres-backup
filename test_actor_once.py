#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import time
from datetime import UTC, datetime

import requests
from dotenv import load_dotenv


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _extract_result(payload: object) -> object:
    if isinstance(payload, dict) and "result" in payload:
        return payload["result"]
    return payload


def _request(
    *,
    method: str,
    url: str,
    token: str,
    json_body: dict[str, object] | None = None,
) -> dict[str, object]:
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


def _actor_payload(image: str, actor_name: str) -> dict[str, object]:
    return {
        "image": image,
        "name": actor_name,
        "description": os.getenv("ACTOR_TEST_DESCRIPTION", "One-shot actor smoke test for tapis-postgres-backup"),
        "stateless": True,
        "cron_on": False,
        "default_environment": {
            "TAPIS_BASE_URL": _require_env("TAPIS_BASE_URL"),
            "TAPIS_TENANT_ID": _require_env("TAPIS_TENANT_ID"),
            "TAPIS_SERVICE_USERNAME": _require_env("TAPIS_SERVICE_USERNAME"),
            "TAPIS_SERVICE_PASSWORD": _require_env("TAPIS_SERVICE_PASSWORD"),
            "TAPIS_BACKUP_SYSTEM_ID": _require_env("TAPIS_BACKUP_SYSTEM_ID"),
            "TAPIS_BACKUP_ROOT_PATH": _require_env("TAPIS_BACKUP_ROOT_PATH"),
            "TAPIS_BACKUP_RETENTION_DAYS": os.getenv("TAPIS_BACKUP_RETENTION_DAYS", "7"),
            "TAPIS_BACKUP_STAGING_DIR": os.getenv("TAPIS_BACKUP_STAGING_DIR", "/tmp/upstream-postgres-backups"),
            "TAPIS_BACKUP_TIMEOUT_SECONDS": os.getenv("TAPIS_BACKUP_TIMEOUT_SECONDS", "300"),
            "TAPIS_POSTGRES_BACKUP_MODE": "backup-once",
            "TAPIS_POSTGRES_BACKUP_LOG_LEVEL": os.getenv("TAPIS_POSTGRES_BACKUP_LOG_LEVEL", "INFO"),
        },
    }


def wait_for_actor_ready(*, api_base: str, token: str, actor_id: str, timeout_seconds: int) -> dict[str, object]:
    deadline = time.time() + timeout_seconds
    actor_url = f"{api_base}/v3/actors/{actor_id}"
    while time.time() < deadline:
        actor = _extract_result(_request(method="GET", url=actor_url, token=token))
        if isinstance(actor, dict):
            status = str(actor.get("status", "")).upper()
            if status == "READY":
                return actor
            if status in {"ERROR", "FAILED"}:
                raise RuntimeError(f"Actor entered terminal status {status}: {json.dumps(actor, indent=2)}")
        time.sleep(5)
    raise RuntimeError(f"Timed out waiting for actor {actor_id} to become READY")


def wait_for_execution_complete(
    *,
    api_base: str,
    token: str,
    actor_id: str,
    execution_id: str,
    timeout_seconds: int,
) -> dict[str, object]:
    deadline = time.time() + timeout_seconds
    execution_url = f"{api_base}/v3/actors/{actor_id}/executions/{execution_id}"
    while time.time() < deadline:
        execution = _extract_result(_request(method="GET", url=execution_url, token=token))
        if isinstance(execution, dict):
            status = str(execution.get("status", "")).upper()
            if status in {"COMPLETE", "COMPLETED"}:
                return execution
            if status in {"ERROR", "FAILED"}:
                return execution
        time.sleep(5)
    raise RuntimeError(f"Timed out waiting for execution {execution_id}")


def fetch_logs(*, api_base: str, token: str, actor_id: str, execution_id: str) -> str:
    payload = _request(
        method="GET",
        url=f"{api_base}/v3/actors/{actor_id}/executions/{execution_id}/logs",
        token=token,
    )
    result = _extract_result(payload)
    if isinstance(result, dict):
        logs = result.get("logs") or result.get("log") or result
    else:
        logs = result
    return logs if isinstance(logs, str) else json.dumps(logs, indent=2)


def delete_actor(*, api_base: str, token: str, actor_id: str) -> None:
    _request(method="DELETE", url=f"{api_base}/v3/actors/{actor_id}", token=token)


def main() -> int:
    load_dotenv()

    token = _require_env("ACTOR_TEST_TOKEN")
    image = _require_env("ACTOR_TEST_IMAGE")
    api_base = os.getenv("ACTOR_TEST_BASE_URL", _require_env("TAPIS_BASE_URL")).rstrip("/")
    timeout_seconds = int(os.getenv("ACTOR_TEST_TIMEOUT_SECONDS", "600"))
    cleanup = _bool_env("ACTOR_TEST_CLEANUP", True)
    actor_name_prefix = os.getenv("ACTOR_TEST_NAME_PREFIX", "tapis-postgres-backup-smoke")
    actor_name = f"{actor_name_prefix}-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}"

    actor_id: str | None = None
    execution_id: str | None = None
    try:
        create_payload = _actor_payload(image, actor_name)
        actor = _extract_result(
            _request(
                method="POST",
                url=f"{api_base}/v3/actors",
                token=token,
                json_body=create_payload,
            )
        )
        if not isinstance(actor, dict) or not actor.get("id"):
            raise RuntimeError(f"Unexpected actor create response: {json.dumps(actor, indent=2)}")
        actor_id = str(actor["id"])
        print(f"Created actor: {actor_id}")

        wait_for_actor_ready(api_base=api_base, token=token, actor_id=actor_id, timeout_seconds=timeout_seconds)
        print(f"Actor READY: {actor_id}")

        execution_response = _extract_result(
            _request(
                method="POST",
                url=f"{api_base}/v3/actors/{actor_id}/messages",
                token=token,
                json_body={"message": "run backup now"},
            )
        )
        if not isinstance(execution_response, dict) or not execution_response.get("execution_id"):
            raise RuntimeError(f"Unexpected execution response: {json.dumps(execution_response, indent=2)}")
        execution_id = str(execution_response["execution_id"])
        print(f"Execution queued: {execution_id}")

        execution = wait_for_execution_complete(
            api_base=api_base,
            token=token,
            actor_id=actor_id,
            execution_id=execution_id,
            timeout_seconds=timeout_seconds,
        )
        print(json.dumps({"actor_id": actor_id, "execution_id": execution_id, "execution": execution}, indent=2))

        logs = fetch_logs(api_base=api_base, token=token, actor_id=actor_id, execution_id=execution_id)
        print("\n=== Actor Logs ===")
        print(logs)

        status = str(execution.get("status", "")).upper() if isinstance(execution, dict) else ""
        if status not in {"COMPLETE", "COMPLETED"}:
            return 1
        return 0
    finally:
        if cleanup and actor_id:
            try:
                delete_actor(api_base=api_base, token=token, actor_id=actor_id)
                print(f"Deleted actor: {actor_id}")
            except Exception as exc:
                print(f"Warning: failed to delete actor {actor_id}: {exc}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())

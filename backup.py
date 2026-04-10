from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Sequence
from urllib.parse import quote

import psycopg
import requests

from app.core.config import Settings, get_settings
from app.tapis.client import TapisAuthClient

logger = logging.getLogger(__name__)

UPSTREAM_POSTGRES_DESCRIPTION = "postgres for upstream-docker"
POSTGRES_DATA_MOUNT = "/var/lib/postgresql/data"


@dataclass(slots=True)
class PostgresPodTarget:
    pod_id: str
    host: str
    port: int
    db_name: str
    db_user: str
    db_password: str
    volume_id: str | None
    description: str | None = None


@dataclass(slots=True)
class BackupManifest:
    pod: dict[str, Any]
    backup_date: str
    backup_timestamp: str
    remote_directory: str
    files: dict[str, str]
    checksums: dict[str, str]


@dataclass(slots=True)
class BackupResult:
    pod_id: str
    success: bool
    remote_directory: str | None
    error: str | None = None
    uploaded_files: list[str] | None = None


def resolve_tapis_token(
    *,
    explicit_token: str | None = None,
    settings: Settings | None = None,
) -> str:
    settings = settings or get_settings()
    if explicit_token:
        return explicit_token

    service_username = settings.TAPIS_SERVICE_USERNAME or settings.TAS_USER
    service_password = settings.TAPIS_SERVICE_PASSWORD or settings.TAS_SECRET
    if not service_username or not service_password:
        raise RuntimeError("Tapis token or service credentials are required")

    auth_client = TapisAuthClient(
        base_url=settings.TAPIS_BASE_URL,
        tenant_id=settings.TAPIS_TENANT_ID,
    )
    outcome = auth_client.authenticate(service_username, service_password)
    if not outcome.tokens or not outcome.tokens.get("access_token"):
        raise RuntimeError(outcome.error or "Failed to obtain Tapis access token")
    return str(outcome.tokens["access_token"])


def _quote_path(path: str) -> str:
    return quote(path.lstrip("/"), safe="/")


def _extract_result(payload: Any) -> Any:
    if isinstance(payload, dict) and "result" in payload:
        return payload["result"]
    return payload


def _get_mount(volume_mounts: dict[str, Any], mount_path: str) -> dict[str, Any] | None:
    direct = volume_mounts.get(mount_path)
    if isinstance(direct, dict):
        return direct
    for key, value in volume_mounts.items():
        if key.rstrip("/") == mount_path.rstrip("/") and isinstance(value, dict):
            return value
    return None


def discover_upstream_postgres_pods(pods: Iterable[dict[str, Any]]) -> list[PostgresPodTarget]:
    targets: list[PostgresPodTarget] = []
    for pod in pods:
        pod_id = str(pod.get("pod_id") or pod.get("id") or "").strip()
        if not pod_id.endswith("postgres"):
            continue

        description = pod.get("description")
        if description and description != UPSTREAM_POSTGRES_DESCRIPTION:
            continue

        volume_mounts = pod.get("volume_mounts") or {}
        if not isinstance(volume_mounts, dict):
            continue
        data_mount = _get_mount(volume_mounts, POSTGRES_DATA_MOUNT)
        if not data_mount:
            continue

        env = pod.get("environment_variables") or {}
        if not isinstance(env, dict):
            continue
        db_user = str(env.get("POSTGRES_USER") or "").strip()
        db_password = str(env.get("POSTGRES_PASSWORD") or "").strip()
        db_name = str(env.get("POSTGRES_DB") or db_user).strip()
        if not (db_user and db_password and db_name):
            continue

        host = (
            ((pod.get("networking") or {}).get("default") or {}).get("url")
            or f"{pod_id}.pods.tacc.tapis.io"
        )
        expected_volume_id = re.sub(r"postgres$", "volume", pod_id)
        volume_id = data_mount.get("source_id")
        if volume_id and volume_id != expected_volume_id:
            logger.info(
                "Skipping pod %s because volume_id=%s does not match expected %s",
                pod_id,
                volume_id,
                expected_volume_id,
            )
            continue

        targets.append(
            PostgresPodTarget(
                pod_id=pod_id,
                host=str(host),
                port=443,
                db_name=db_name,
                db_user=db_user,
                db_password=db_password,
                volume_id=str(volume_id) if volume_id else None,
                description=str(description) if description else None,
            )
        )
    return sorted(targets, key=lambda item: item.pod_id)


def build_backup_remote_dir(
    *,
    root_path: str,
    pod_id: str,
    backup_day: date,
) -> str:
    path = (
        PurePosixPath(root_path)
        / pod_id
        / f"{backup_day.year:04d}"
        / f"{backup_day.month:02d}"
        / f"{backup_day.day:02d}"
    )
    return str(path)


def build_inventory_remote_path(
    *,
    root_path: str,
    backup_time: datetime,
) -> str:
    return str(
        PurePosixPath(root_path)
        / "_inventory"
        / f"{backup_time.year:04d}"
        / f"{backup_time.month:02d}"
        / f"{backup_time.day:02d}"
        / f"inventory-{backup_time.strftime('%H%M%S')}.json"
    )


def scrub_target_for_manifest(target: PostgresPodTarget) -> dict[str, Any]:
    return {
        "pod_id": target.pod_id,
        "host": target.host,
        "port": target.port,
        "db_name": target.db_name,
        "volume_id": target.volume_id,
        "description": target.description,
    }


def parse_backup_date_from_path(path: str) -> date | None:
    match = re.search(r"/(\d{4})/(\d{2})/(\d{2})/?$", path)
    if not match:
        return None
    year, month, day = (int(part) for part in match.groups())
    return date(year, month, day)


def select_retention_prune_candidates(paths: Sequence[str], *, keep: int) -> list[str]:
    dated_paths: list[tuple[date, str]] = []
    for path in paths:
        backup_day = parse_backup_date_from_path(path)
        if backup_day is not None:
            dated_paths.append((backup_day, path))
    dated_paths.sort(key=lambda item: item[0], reverse=True)
    return [path for _, path in dated_paths[keep:]]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parent_path(path: str) -> str:
    parent = str(PurePosixPath(path).parent)
    return parent if parent != "." else "/"


def _subprocess_env(extra: dict[str, str]) -> dict[str, str]:
    env = os.environ.copy()
    env.update(extra)
    return env


class TapisBackupClient:
    def __init__(
        self,
        *,
        token: str,
        base_url: str,
        timeout_seconds: int = 300,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout_seconds = timeout_seconds
        self.session = session or requests.Session()

    def _headers(self) -> dict[str, str]:
        return {
            "X-Tapis-Token": self.token,
            "Accept": "application/json",
        }

    def _request(
        self,
        *,
        method: str,
        path: str,
        json_body: dict[str, Any] | None = None,
        files: dict[str, Any] | None = None,
        stream: bool = False,
    ) -> requests.Response:
        headers = self._headers()
        if files is None:
            headers["Content-Type"] = "application/json"
        response = self.session.request(
            method=method,
            url=f"{self.base_url}{path}",
            headers=headers,
            json=json_body if files is None else None,
            files=files,
            timeout=self.timeout_seconds,
            stream=stream,
        )
        if not response.ok:
            detail = response.text.strip() or f"Tapis request failed ({response.status_code})"
            raise RuntimeError(detail)
        return response

    def list_pods(self) -> list[dict[str, Any]]:
        response = self._request(method="GET", path="/v3/pods")
        result = _extract_result(response.json())
        if not isinstance(result, list):
            raise RuntimeError("Unexpected pods list response from Tapis")
        return [item for item in result if isinstance(item, dict)]

    def get_pod(self, pod_id: str) -> dict[str, Any]:
        response = self._request(method="GET", path=f"/v3/pods/{pod_id}")
        result = _extract_result(response.json())
        if not isinstance(result, dict):
            raise RuntimeError(f"Unexpected pod response for {pod_id}")
        return result

    def mkdir(self, *, system_id: str, path: str) -> None:
        self._request(
            method="POST",
            path=f"/v3/files/ops/{system_id}",
            json_body={"path": path},
        )

    def list_files(self, *, system_id: str, path: str) -> list[dict[str, Any]]:
        normalized = path if path == "/" else path.strip("/")
        suffix = "/" if not normalized else f"/{_quote_path(normalized)}"
        response = self._request(
            method="GET",
            path=f"/v3/files/ops/{system_id}{suffix}",
        )
        result = _extract_result(response.json())
        if not isinstance(result, list):
            raise RuntimeError(f"Unexpected files list response for {system_id}:{path}")
        return [item for item in result if isinstance(item, dict)]

    def upload_file(self, *, system_id: str, local_path: Path, dest_path: str) -> None:
        dest = dest_path.strip("/")
        with local_path.open("rb") as handle:
            self._request(
                method="POST",
                path=f"/v3/files/ops/{system_id}/{_quote_path(dest)}",
                files={"file": handle},
            )

    def download_file(self, *, system_id: str, path: str, destination: Path) -> Path:
        remote_path = path.strip("/")
        response = self._request(
            method="GET",
            path=f"/v3/files/content/{system_id}/{_quote_path(remote_path)}",
            stream=True,
        )
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)
        return destination

    def delete_path(self, *, system_id: str, path: str) -> None:
        remote_path = path.strip("/")
        self._request(
            method="DELETE",
            path=f"/v3/files/ops/{system_id}/{_quote_path(remote_path)}",
        )


class BackupManager:
    def __init__(
        self,
        *,
        client: TapisBackupClient,
        settings: Settings | None = None,
        staging_dir: Path | None = None,
    ) -> None:
        self.client = client
        self.settings = settings or get_settings()
        self.staging_dir = staging_dir or Path(self.settings.TAPIS_BACKUP_STAGING_DIR)

    def discover_targets(self) -> list[PostgresPodTarget]:
        return discover_upstream_postgres_pods(self.client.list_pods())

    def _run_command(self, args: list[str], *, env: dict[str, str]) -> None:
        result = subprocess.run(
            args,
            check=False,
            capture_output=True,
            text=True,
            env=env,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip() or f"{args[0]} failed")

    def _build_manifest(
        self,
        *,
        target: PostgresPodTarget,
        backup_time: datetime,
        remote_dir: str,
        files: dict[str, str],
        checksums: dict[str, str],
    ) -> BackupManifest:
        return BackupManifest(
            pod=scrub_target_for_manifest(target),
            backup_date=backup_time.date().isoformat(),
            backup_timestamp=backup_time.isoformat(),
            remote_directory=remote_dir,
            files=files,
            checksums=checksums,
        )

    def _list_remote_backup_paths(self, *, pod_id: str) -> list[str]:
        root = PurePosixPath(self.settings.TAPIS_BACKUP_ROOT_PATH) / pod_id
        backup_paths: list[str] = []
        try:
            year_entries = self.client.list_files(system_id=self.settings.TAPIS_BACKUP_SYSTEM_ID, path=str(root))
        except RuntimeError:
            return []
        for year_entry in year_entries:
            if year_entry.get("type") != "dir":
                continue
            year_path = str(root / str(year_entry.get("name")))
            try:
                month_entries = self.client.list_files(
                    system_id=self.settings.TAPIS_BACKUP_SYSTEM_ID,
                    path=year_path,
                )
            except RuntimeError:
                continue
            for month_entry in month_entries:
                if month_entry.get("type") != "dir":
                    continue
                month_path = str(PurePosixPath(year_path) / str(month_entry.get("name")))
                try:
                    day_entries = self.client.list_files(
                        system_id=self.settings.TAPIS_BACKUP_SYSTEM_ID,
                        path=month_path,
                    )
                except RuntimeError:
                    continue
                for day_entry in day_entries:
                    if day_entry.get("type") == "dir":
                        backup_paths.append(str(PurePosixPath(month_path) / str(day_entry.get("name"))))
        return backup_paths

    def _upload_backup_directory(self, *, remote_dir: str, local_dir: Path) -> list[str]:
        self.client.mkdir(system_id=self.settings.TAPIS_BACKUP_SYSTEM_ID, path=remote_dir)
        uploaded: list[str] = []
        for local_path in sorted(local_dir.iterdir()):
            if local_path.is_file():
                remote_path = str(PurePosixPath(remote_dir) / local_path.name)
                self.client.upload_file(
                    system_id=self.settings.TAPIS_BACKUP_SYSTEM_ID,
                    local_path=local_path,
                    dest_path=remote_path,
                )
                uploaded.append(remote_path)
        return uploaded

    def _prune_old_backups(self, *, pod_id: str) -> list[str]:
        existing = self._list_remote_backup_paths(pod_id=pod_id)
        prune_paths = select_retention_prune_candidates(
            existing,
            keep=self.settings.TAPIS_BACKUP_RETENTION_DAYS,
        )
        for path in prune_paths:
            self.client.delete_path(system_id=self.settings.TAPIS_BACKUP_SYSTEM_ID, path=path)
        return prune_paths

    def backup_target(
        self,
        target: PostgresPodTarget,
        *,
        backup_time: datetime | None = None,
    ) -> BackupResult:
        backup_time = backup_time or datetime.now(UTC)
        remote_dir = build_backup_remote_dir(
            root_path=self.settings.TAPIS_BACKUP_ROOT_PATH,
            pod_id=target.pod_id,
            backup_day=backup_time.date(),
        )
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        work_dir = Path(
            tempfile.mkdtemp(
                prefix=f"{target.pod_id}-",
                dir=self.staging_dir,
            )
        )
        env = _subprocess_env({
            "PGPASSWORD": target.db_password,
            "PGSSLMODE": "require",
        })

        dump_path = work_dir / f"{target.pod_id}.dump"
        globals_path = work_dir / f"{target.pod_id}-globals.sql"
        checksums_path = work_dir / "checksums.txt"
        manifest_path = work_dir / "manifest.json"
        try:
            self._run_command(
                [
                    "pg_dump",
                    "--host",
                    target.host,
                    "--port",
                    str(target.port),
                    "--username",
                    target.db_user,
                    "--dbname",
                    target.db_name,
                    "--format=custom",
                    "--clean",
                    "--if-exists",
                    "--no-owner",
                    "--no-privileges",
                    "--file",
                    str(dump_path),
                ],
                env=env,
            )
            self._run_command(
                [
                    "pg_dumpall",
                    "--host",
                    target.host,
                    "--port",
                    str(target.port),
                    "--username",
                    target.db_user,
                    "--globals-only",
                    "--file",
                    str(globals_path),
                ],
                env=env,
            )
            self._run_command(["pg_restore", "--list", str(dump_path)], env=env)

            checksums = {
                dump_path.name: sha256_file(dump_path),
                globals_path.name: sha256_file(globals_path),
            }
            checksums_path.write_text(
                "\n".join(f"{digest}  {name}" for name, digest in checksums.items()) + "\n",
                encoding="utf-8",
            )
            checksums[checksums_path.name] = sha256_file(checksums_path)

            manifest = self._build_manifest(
                target=target,
                backup_time=backup_time,
                remote_dir=remote_dir,
                files={
                    "dump": dump_path.name,
                    "globals": globals_path.name,
                    "checksums": checksums_path.name,
                },
                checksums=checksums,
            )
            manifest_path.write_text(
                json.dumps(asdict(manifest), indent=2, sort_keys=True),
                encoding="utf-8",
            )

            uploaded_files = self._upload_backup_directory(remote_dir=remote_dir, local_dir=work_dir)
            self._prune_old_backups(pod_id=target.pod_id)
            return BackupResult(
                pod_id=target.pod_id,
                success=True,
                remote_directory=remote_dir,
                uploaded_files=uploaded_files,
            )
        except Exception as exc:
            return BackupResult(
                pod_id=target.pod_id,
                success=False,
                remote_directory=None,
                error=str(exc),
            )
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)

    def run_backup(self) -> dict[str, Any]:
        backup_time = datetime.now(UTC)
        targets = self.discover_targets()
        results = [self.backup_target(target, backup_time=backup_time) for target in targets]
        summary = {
            "backup_timestamp": backup_time.isoformat(),
            "system_id": self.settings.TAPIS_BACKUP_SYSTEM_ID,
            "targets": [asdict(target) for target in targets],
            "results": [asdict(result) for result in results],
        }
        inventory_path = build_inventory_remote_path(
            root_path=self.settings.TAPIS_BACKUP_ROOT_PATH,
            backup_time=backup_time,
        )
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        inventory_file = Path(
            tempfile.mkdtemp(prefix="inventory-", dir=self.staging_dir)
        ) / "inventory.json"
        try:
            inventory_file.parent.mkdir(parents=True, exist_ok=True)
            inventory_file.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
            self.client.mkdir(
                system_id=self.settings.TAPIS_BACKUP_SYSTEM_ID,
                path=_parent_path(inventory_path),
            )
            self.client.upload_file(
                system_id=self.settings.TAPIS_BACKUP_SYSTEM_ID,
                local_path=inventory_file,
                dest_path=inventory_path,
            )
        finally:
            shutil.rmtree(inventory_file.parent, ignore_errors=True)
        return summary


class RestoreManager:
    def __init__(
        self,
        *,
        client: TapisBackupClient,
        settings: Settings | None = None,
    ) -> None:
        self.client = client
        self.settings = settings or get_settings()

    def list_backup_dates(self, *, pod_id: str) -> list[date]:
        backup_root = PurePosixPath(self.settings.TAPIS_BACKUP_ROOT_PATH) / pod_id
        backup_paths: list[str] = []
        try:
            year_entries = self.client.list_files(system_id=self.settings.TAPIS_BACKUP_SYSTEM_ID, path=str(backup_root))
        except RuntimeError:
            return []
        for year_entry in year_entries:
            if year_entry.get("type") != "dir":
                continue
            year_path = backup_root / str(year_entry.get("name"))
            for month_entry in self.client.list_files(system_id=self.settings.TAPIS_BACKUP_SYSTEM_ID, path=str(year_path)):
                if month_entry.get("type") != "dir":
                    continue
                month_path = year_path / str(month_entry.get("name"))
                for day_entry in self.client.list_files(system_id=self.settings.TAPIS_BACKUP_SYSTEM_ID, path=str(month_path)):
                    if day_entry.get("type") == "dir":
                        backup_paths.append(str(month_path / str(day_entry.get("name"))))
        days = [parsed for parsed in (parse_backup_date_from_path(path) for path in backup_paths) if parsed is not None]
        return sorted(days, reverse=True)

    def resolve_backup_day(self, *, pod_id: str, requested_day: date | None) -> date:
        if requested_day is not None:
            return requested_day
        available = self.list_backup_dates(pod_id=pod_id)
        if not available:
            raise RuntimeError(f"No backups found for pod {pod_id}")
        return available[0]

    def download_backup_set(self, *, pod_id: str, backup_day: date, destination: Path) -> tuple[Path, Path, Path]:
        remote_dir = build_backup_remote_dir(
            root_path=self.settings.TAPIS_BACKUP_ROOT_PATH,
            pod_id=pod_id,
            backup_day=backup_day,
        )
        manifest_path = self.client.download_file(
            system_id=self.settings.TAPIS_BACKUP_SYSTEM_ID,
            path=f"{remote_dir}/manifest.json",
            destination=destination / "manifest.json",
        )
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        files = manifest.get("files") or {}
        dump_path = self.client.download_file(
            system_id=self.settings.TAPIS_BACKUP_SYSTEM_ID,
            path=f"{remote_dir}/{files['dump']}",
            destination=destination / str(files["dump"]),
        )
        globals_path = self.client.download_file(
            system_id=self.settings.TAPIS_BACKUP_SYSTEM_ID,
            path=f"{remote_dir}/{files['globals']}",
            destination=destination / str(files["globals"]),
        )
        return manifest_path, dump_path, globals_path

    def resolve_pod_connection(self, *, pod_id: str) -> PostgresPodTarget:
        pod = self.client.get_pod(pod_id)
        targets = discover_upstream_postgres_pods([pod])
        if not targets:
            raise RuntimeError(f"Unable to resolve live Postgres connection details for pod {pod_id}")
        return targets[0]

    def wait_for_database(
        self,
        *,
        host: str,
        port: int,
        user: str,
        password: str,
        dbname: str,
        timeout_seconds: int = 180,
    ) -> None:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            try:
                with psycopg.connect(
                    host=host,
                    port=port,
                    user=user,
                    password=password,
                    dbname=dbname,
                    sslmode="require",
                ):
                    return
            except Exception:
                time.sleep(5)
        raise RuntimeError(f"Timed out waiting for Postgres at {host}:{port}")

from __future__ import annotations

import copy
import logging
import re
from typing import Any

import requests

from config import Settings, get_settings

logger = logging.getLogger(__name__)


def sanitize_base(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]", "", value.strip().lower())
    if not cleaned:
        raise ValueError("Base name must contain letters or numbers")
    if not cleaned[0].isalpha():
        cleaned = f"v{cleaned}"
    return cleaned


class PodsService:
    def __init__(self, *, token: str, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.base_url = (self.settings.TAPIS_PODS_BASE_URL or self.settings.TAPIS_BASE_URL).rstrip("/")
        self.token = token

    def _headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "X-Tapis-Token": self.token,
            "Accept": "application/json",
        }

    def _request(self, *, method: str, path: str, json: dict[str, Any] | None = None) -> dict[str, Any]:
        response = requests.request(
            method=method,
            url=f"{self.base_url}{path}",
            headers=self._headers(),
            json=json,
            timeout=self.settings.TAPIS_BACKUP_TIMEOUT_SECONDS,
        )
        if not response.ok:
            raise RuntimeError(response.text or f"Pods API request failed ({response.status_code})")
        return response.json()

    def create_volume(self, *, volume_id: str, description: str) -> dict[str, Any]:
        payload = {"volume_id": volume_id, "description": description}
        try:
            return self._request(method="POST", path="/v3/pods/volumes", json=payload)
        except RuntimeError as exc:
            if "already exists" in str(exc).lower():
                return {"status": "exists", "volume_id": volume_id}
            raise

    def create_pod(self, payload: dict[str, Any]) -> dict[str, Any]:
        sanitized = dict(payload)
        sanitized.pop("pod_template", None)
        try:
            return self._request(method="POST", path="/v3/pods", json=sanitized)
        except RuntimeError as exc:
            message = str(exc)
            compatibility_payload = copy.deepcopy(sanitized)
            volume_mounts = compatibility_payload.get("volume_mounts")
            if (
                isinstance(volume_mounts, dict)
                and "volume_mounts" in message
                and ("mount_path" in message or "requires source_id" in message)
            ):
                normalized_mounts: dict[str, Any] = {}
                for mount_name, mount_cfg in volume_mounts.items():
                    if not isinstance(mount_cfg, dict):
                        normalized_mounts[mount_name] = mount_cfg
                        continue
                    mount_cfg_copy = dict(mount_cfg)
                    mount_path = mount_cfg_copy.get("mount_path")
                    if mount_cfg_copy.get("type") == "tapisvolume" and not mount_cfg_copy.get("source_id"):
                        mount_cfg_copy["source_id"] = mount_name
                    mount_cfg_copy.pop("mount_path", None)
                    mount_key = mount_name
                    if isinstance(mount_path, str) and mount_path.startswith("/"):
                        mount_key = mount_path
                    normalized_mounts[mount_key] = mount_cfg_copy
                compatibility_payload["volume_mounts"] = normalized_mounts
                return self._request(method="POST", path="/v3/pods", json=compatibility_payload)
            raise


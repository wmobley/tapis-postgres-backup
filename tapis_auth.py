from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from tapipy.errors import BaseTapyException  # type: ignore[import-untyped]
from tapipy.tapis import Tapis  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class TapisAuthOutcome:
    tokens: dict[str, Any] | None
    error: str | None = None


@dataclass(slots=True)
class TapisAuthClient:
    base_url: str
    tenant_id: str

    @staticmethod
    def _token_summary(token: Any) -> str:
        if not isinstance(token, str) or not token:
            return "missing"
        return f"len={len(token)} dots={token.count('.')} prefix={token[:12]} suffix={token[-12:]}"

    @classmethod
    def _coerce_token_string(cls, value: Any, *, token_key: str) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            candidate = value.strip()
            if candidate.lower().startswith("bearer "):
                candidate = candidate.split(" ", 1)[1].strip()
            if candidate.startswith('"') and candidate.endswith('"'):
                candidate = candidate[1:-1].strip()
            if candidate.startswith("{") and candidate.endswith("}"):
                try:
                    return cls._coerce_token_string(json.loads(candidate), token_key=token_key)
                except Exception:
                    return candidate or None
            return candidate or None
        if isinstance(value, dict):
            for key in (token_key, "token", "access_token", "refresh_token"):
                if key in value:
                    return cls._coerce_token_string(value.get(key), token_key=token_key)
            return None
        for attr in (token_key, "token", "access_token", "refresh_token"):
            if hasattr(value, attr):
                nested = getattr(value, attr, None)
                if nested is not None and nested is not value:
                    return cls._coerce_token_string(nested, token_key=token_key)
        return None

    def authenticate(self, username: str, password: str) -> TapisAuthOutcome:
        try:
            client = Tapis(
                base_url=self.base_url,
                tenant_id=self.tenant_id,
                username=username,
                password=password,
            )
            client.get_tokens()
        except BaseTapyException as exc:
            logger.info(
                "Tapis authentication failed for user %s: %s (status=%s)",
                username,
                exc,
                getattr(exc, "status_code", None),
            )
            return TapisAuthOutcome(tokens=None, error=getattr(exc, "message", None) or str(exc))

        access_obj = getattr(client, "access_token", None)
        refresh_obj = getattr(client, "refresh_token", None)
        token_payload = getattr(client, "token", None)

        access_token = self._coerce_token_string(getattr(access_obj, "access_token", None), token_key="access_token")
        refresh_token = self._coerce_token_string(getattr(refresh_obj, "refresh_token", None), token_key="refresh_token")
        expires_at = getattr(access_obj, "expires_at", None)

        if token_payload:
            access_token = access_token or self._coerce_token_string(token_payload.get("access_token"), token_key="access_token")
            refresh_token = refresh_token or self._coerce_token_string(token_payload.get("refresh_token"), token_key="refresh_token")
            expires_at = expires_at or token_payload.get("expires_at")
        else:
            logger.info("Tapis authentication succeeded but no token payload returned for %s.", username)

        logger.info(
            "Tapis token extraction summary for %s: access=%s refresh=%s",
            username,
            self._token_summary(access_token),
            self._token_summary(refresh_token),
        )

        if not access_token or access_token.count(".") != 2:
            return TapisAuthOutcome(tokens=None, error="Malformed or missing access_token returned from Tapis")

        if isinstance(expires_at, datetime):
            expires_at_value = int(expires_at.timestamp())
        elif isinstance(expires_at, (int, float)):
            expires_at_value = int(expires_at)
        elif isinstance(expires_at, str):
            try:
                normalized = expires_at.replace("Z", "+00:00") if expires_at.endswith("Z") else expires_at
                expires_at_value = int(datetime.fromisoformat(normalized).timestamp())
            except ValueError:
                expires_at_value = None
        else:
            expires_at_value = None

        return TapisAuthOutcome(
            tokens={
                "access_token": access_token,
                "refresh_token": refresh_token,
                "expires_at": expires_at_value,
            }
        )

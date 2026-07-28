"""Safe, deterministic episode evidence shared by resolver and memory code.

Episode rows are retained in full unless they contain operator e-mail,
credential-like fields, or resolver-owned mutable bookkeeping. Bounds fail
closed instead of silently truncating evidence, so a memory can never claim to
represent an incomplete episode.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from typing import Any


MAX_EPISODE_EVIDENCE_CHARS = 160_000
MAX_EPISODE_STRING_CHARS = 32_000
MAX_EPISODE_COLLECTION_ITEMS = 4_096
MAX_EPISODE_DEPTH = 16

_SENSITIVE_KEY_PARTS = (
    "api_key",
    "apikey",
    "authorization",
    "client_secret",
    "cookie",
    "credential",
    "passwd",
    "password",
    "private_key",
    "service_role",
)
_RESOLVER_BOOKKEEPING_KEYS = {
    "matcher_status",
    "matcher_message",
    "resolver_suggestion",
    "memory_status",
    "memory_resolution_id",
    "memory_commit_sha",
    "memory_message",
    "memory_ingested_at",
    "updated_at",
}


def build_episode_evidence(
    *,
    failure_event: Mapping[str, Any] | None,
    resolution_event: Mapping[str, Any],
) -> dict[str, Any]:
    """Return complete sanitized source rows, or raise when bounds are exceeded."""

    evidence = {
        "failure_event": (
            sanitize_episode_record(failure_event)
            if failure_event is not None
            else None
        ),
        "resolution_event": sanitize_episode_record(resolution_event),
    }
    return validate_episode_evidence(evidence)


def sanitize_episode_record(record: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(record, Mapping):
        raise ValueError("episode record must be a mapping")
    sanitized = _sanitize_value(record, depth=0)
    if not isinstance(sanitized, dict):
        raise ValueError("episode record must normalize to an object")
    return sanitized


def sanitize_episode_value(value: Any) -> Any:
    """Sanitize arbitrary structured evidence using the same episode policy."""

    return _sanitize_value(value, depth=0)


def validate_episode_evidence(value: Any) -> dict[str, Any]:
    """Sanitize and size-check a structured evidence object without truncation."""

    sanitized = _sanitize_value(value, depth=0)
    if not isinstance(sanitized, dict):
        raise ValueError("episode evidence must be an object")
    serialized = json.dumps(
        sanitized,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    if len(serialized) > MAX_EPISODE_EVIDENCE_CHARS:
        raise ValueError("episode evidence exceeds the bounded limit")
    return sanitized


def _sanitize_value(value: Any, *, depth: int) -> Any:
    if depth > MAX_EPISODE_DEPTH:
        raise ValueError("episode evidence exceeds the maximum depth")
    if value is None or isinstance(value, (str, int, bool)):
        if isinstance(value, str) and len(value) > MAX_EPISODE_STRING_CHARS:
            raise ValueError("episode evidence contains oversized text")
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("episode evidence contains a non-finite number")
        return value
    if isinstance(value, Mapping):
        if len(value) > MAX_EPISODE_COLLECTION_ITEMS:
            raise ValueError("episode evidence object has too many fields")
        result: dict[str, Any] = {}
        for key, child in value.items():
            if not isinstance(key, str):
                raise ValueError("episode evidence keys must be text")
            if _is_sensitive_key(key):
                continue
            result[key] = _sanitize_value(child, depth=depth + 1)
        return result
    if isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    ):
        if len(value) > MAX_EPISODE_COLLECTION_ITEMS:
            raise ValueError("episode evidence array has too many items")
        return [
            _sanitize_value(child, depth=depth + 1)
            for child in value
        ]
    raise ValueError("episode evidence must contain only JSON values")


def _is_sensitive_key(key: str) -> bool:
    normalized = key.strip().lower().replace("-", "_")
    return (
        normalized in _RESOLVER_BOOKKEEPING_KEYS
        or normalized == "operator_email"
        or normalized.endswith("_email")
        or normalized == "token"
        or normalized.startswith("token_")
        or normalized.endswith("_token")
        or any(part in normalized for part in _SENSITIVE_KEY_PARTS)
    )

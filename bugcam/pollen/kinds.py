"""Per-kind upload strategies.

Each artifact kind keeps its own treatment beyond where its file lives: content
type, whether it should be skipped (the reintegrated cost bug-fixes), and whether
its local file is deleted after a successful upload. Stateful dedup (e.g. an
unchanged DOT results.json) is handled by the store's key idempotency; these
strategies are pure decisions over a path + caller-supplied metadata.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Mapping

JSON = "application/json"


def _content_type_by_ext(filename: str) -> str:
    lowered = filename.lower()
    if lowered.endswith((".jpg", ".jpeg")):
        return "image/jpeg"
    if lowered.endswith(".png"):
        return "image/png"
    if lowered.endswith(".mp4"):
        return "video/mp4"
    if lowered.endswith(".json"):
        return JSON
    if lowered.endswith((".log", ".txt")):
        return "text/plain"
    if lowered.endswith(".tar"):
        return "application/x-tar"
    return "application/octet-stream"


def _result_is_empty(results_json: Path) -> bool:
    """True when a result has zero tracks and no crop/composite/video media."""
    try:
        payload = json.loads(results_json.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if payload.get("tracks"):
        return False
    results_dir = results_json.parent
    for sub in ("crops", "composites", "videos"):
        media_dir = results_dir / sub
        if media_dir.is_dir() and any(p.is_file() for p in media_dir.rglob("*")):
            return False
    return True


class KindStrategy:
    name = "object"

    def content_type(self, filename: str) -> str:
        return _content_type_by_ext(filename)

    def should_skip(self, path: Path, metadata: Mapping) -> bool:
        return False

    def delete_after_upload(self, metadata: Mapping) -> bool:
        # Delete the local file after a successful upload unless asked to retain
        # it (e.g. DOT day-buckets that keep accumulating).
        return not bool(metadata.get("retain", False))


class ResultKind(KindStrategy):
    name = "result"

    def should_skip(self, path: Path, metadata: Mapping) -> bool:
        return _result_is_empty(Path(path))


class LogKind(KindStrategy):
    name = "log"

    def should_skip(self, path: Path, metadata: Mapping) -> bool:
        # The current day's log is still being appended; ship it after rollover.
        today = metadata.get("today") or datetime.now().strftime("%Y%m%d")
        return today in Path(path).name


class HeartbeatKind(KindStrategy):
    name = "heartbeat"


class EnvironmentKind(KindStrategy):
    name = "environment"


class ArchiveKind(KindStrategy):
    name = "archive"


_GENERIC = KindStrategy()
_REGISTRY: dict[str, KindStrategy] = {
    s.name: s for s in (ResultKind(), LogKind(), HeartbeatKind(), EnvironmentKind(), ArchiveKind())
}


def for_kind(name: str) -> KindStrategy:
    return _REGISTRY.get(name, _GENERIC)

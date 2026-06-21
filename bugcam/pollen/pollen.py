"""Pollen: the device-side upload subsystem.

Owns a SQLite-backed queue, the upload loop, and cleanup. The app enqueues an
artifact (result / heartbeat / log) as it is produced; a background loop uploads
pending items (per-object, or bundled into an archive when batching is on),
marks each success, then deletes the local file and prunes its row.

Lifecycle: ``start()`` runs the loop in a thread; ``flush()`` drains the queue to
empty while still accepting new enqueues; ``stop()`` halts the loop.
"""
from __future__ import annotations

import logging
import threading
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional

from bugcam.pollen import kinds
from bugcam.pollen.archive import Archiver
from bugcam.pollen.store import PollenStore, UploadRow
from bugcam.pollen.transport import DEFAULT_MULTIPART_THRESHOLD, Uploader

logger = logging.getLogger("bugcam.pollen")

ARCHIVE_KIND = "archive"


@dataclass
class PollenConfig:
    db_path: Path
    output_root: Path
    staging_dir: Path
    key_prefix: str = "v1"
    poll_interval: float = 10.0
    multipart_threshold: int = DEFAULT_MULTIPART_THRESHOLD
    batch: bool = False


class Pollen:
    def __init__(
        self,
        config: PollenConfig,
        *,
        presigner: Any = None,
        uploader: Any = None,
        archiver: Optional[Archiver] = None,
        store: Optional[PollenStore] = None,
        clock: Optional[Callable[[], datetime]] = None,
    ) -> None:
        self.config = config
        self.store = store or PollenStore(config.db_path)
        self.uploader = uploader or Uploader(
            presigner, self.store, multipart_threshold=config.multipart_threshold
        )
        self.archiver = archiver
        self._clock = clock or datetime.now
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------ #
    # enqueue
    # ------------------------------------------------------------------ #
    def enqueue(self, path: str | Path, kind: str, metadata: Optional[dict] = None) -> Optional[int]:
        """Queue an artifact for upload. Returns the row id, or None if skipped."""
        path = Path(path)
        metadata = dict(metadata or {})
        if kinds.for_kind(kind).should_skip(path, metadata):
            return None
        s3_key = self._derive_key(path)
        size = path.stat().st_size if path.exists() else None
        return self.store.enqueue(str(path), kind=kind, s3_key=s3_key, metadata=metadata, size=size)

    def _derive_key(self, path: Path) -> str:
        root = self.config.output_root.resolve()
        resolved = Path(path).resolve()
        try:
            rel = resolved.relative_to(root)
        except ValueError:
            rel = Path(resolved.name)
        return f"{self.config.key_prefix}/{rel.as_posix()}"

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #
    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="Pollen", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None

    def flush(self) -> None:
        """Drain the queue to empty, still accepting enqueues from other threads."""
        while self.store.pending_count() > 0 and not self._stop.is_set():
            self._tick()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._tick()
            except Exception:  # never let one bad tick kill the loop
                logger.exception("pollen tick failed")
            self._stop.wait(self.config.poll_interval)

    # ------------------------------------------------------------------ #
    # the work
    # ------------------------------------------------------------------ #
    def _tick(self) -> None:
        pending = self.store.claim_pending()
        if self.config.batch and self.archiver is not None:
            self._upload_batched(pending)
        else:
            for row in pending:
                self._upload_one(row)
        self._cleanup()

    def _upload_one(self, row: UploadRow) -> None:
        try:
            self.store.record_attempt(row.id)
            self.uploader.upload(row)
            self.store.mark_uploaded(row.id)
        except Exception:
            logger.exception("upload failed for %s", row.s3_key)

    def _upload_batched(self, pending: list[UploadRow]) -> None:
        # Archive rows already in flight (e.g. from a previous interrupted tick)
        # upload directly; the rest are bundled per group.
        members = [r for r in pending if r.kind != ARCHIVE_KIND]
        for row in (r for r in pending if r.kind == ARCHIVE_KIND):
            self._upload_one(row)
        if not members:
            return

        groups: dict[str, list[UploadRow]] = defaultdict(list)
        for row in members:
            groups[self._group_of(row)].append(row)

        timestamp = self._clock().strftime("%Y%m%d_%H%M%S")
        for group, items in groups.items():
            artifact = self.archiver.pack(group, items, self.config.staging_dir, timestamp=timestamp)
            if artifact is None:
                continue
            tar_id = self.store.enqueue(str(artifact.path), kind=ARCHIVE_KIND, s3_key=artifact.s3_key)
            if tar_id is None:
                continue  # archive key already queued (same timestamp); try next tick
            try:
                self.store.record_attempt(tar_id)
                self.uploader.upload(self.store.get(tar_id))
                self.store.mark_uploaded(tar_id)
                for item in items:
                    self.store.mark_uploaded(item.id)
            except Exception:
                logger.exception("archive upload failed for %s", artifact.s3_key)

    def _group_of(self, row: UploadRow) -> str:
        # Canonical key is v1/<device>/...; group a batch per device.
        parts = row.s3_key.split("/")
        return parts[1] if len(parts) > 1 else "batch"

    def _cleanup(self) -> None:
        for row in self.store.uploaded_rows():
            if kinds.for_kind(row.kind).delete_after_upload(row.metadata):
                Path(row.local_path).unlink(missing_ok=True)
            self.store.delete(row.id)

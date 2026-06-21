"""Pollen: the device-side upload subsystem.

Owns a SQLite-backed queue, the upload loop, and cleanup. The app enqueues an
artifact (result / heartbeat / log) as it is produced; a background loop uploads
pending items (per-object, or bundled into an archive when batching is on),
marks each success, then deletes the local file and prunes its row.

Lifecycle: ``start()`` runs the loop in a thread; ``flush()`` drains the queue to
empty while still accepting new enqueues; ``stop()`` halts the loop.
"""
from __future__ import annotations

import hashlib
import logging
import threading
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional

from bugcam.pollen import kinds
from bugcam.pollen.archive import Archiver
from bugcam.pollen.presign import RateLimitError
from bugcam.pollen.store import PollenStore, UploadRow
from bugcam.pollen.transport import DEFAULT_MULTIPART_THRESHOLD, DEFAULT_PART_SIZE, Uploader

logger = logging.getLogger("bugcam.pollen")

ARCHIVE_KIND = "archive"
MAX_RETRY_DELAY_SECONDS = 300  # matches the legacy upload loop


# Hash used for the change-detection fingerprint. A single knob so it is a
# one-line swap. md5 is fine for these small files, but the choice is a deliberate
# FUTURE-DISCUSSION item: if profiling shows the per-scan re-hash of retained
# files matters, switch to a cheaper non-cryptographic hash (e.g. blake2b which is
# already in hashlib, or xxhash) -- collision-resistance is not required here,
# only change detection.
FINGERPRINT_HASH = "md5"


def _fingerprint(path: Path) -> Optional[str]:
    """Content signature so any change to the bytes re-uploads, regardless of
    size/mtime. The files this runs on (results.json, crops, logs) are small; the
    large hourly tar is enqueued without going through here."""
    try:
        digest = hashlib.new(FINGERPRINT_HASH)
        with open(path, "rb") as handle:
            for chunk in iter(lambda: handle.read(65536), b""):
                digest.update(chunk)
        return digest.hexdigest()
    except OSError:
        return None


@dataclass
class PollenConfig:
    db_path: Path
    output_root: Path
    staging_dir: Path
    key_prefix: str = "v1"
    poll_interval: float = 10.0
    multipart_threshold: int = DEFAULT_MULTIPART_THRESHOLD
    part_size: int = DEFAULT_PART_SIZE
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
        enqueue_source: Optional[Callable[["Pollen"], None]] = None,
    ) -> None:
        self.config = config
        self.store = store or PollenStore(config.db_path)
        self.uploader = uploader or Uploader(
            presigner, self.store,
            multipart_threshold=config.multipart_threshold,
            part_size=config.part_size,
        )
        self.archiver = archiver
        self._clock = clock or datetime.now
        self._source = enqueue_source  # called each tick to enqueue ready outputs
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
        return self.store.enqueue(
            str(path), kind=kind, s3_key=s3_key, metadata=metadata, size=size,
            fingerprint=_fingerprint(path),
        )

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
        """Drain the queue, still accepting enqueues. Returns when empty or when a
        tick makes no progress (uploads failing) so it never spins forever."""
        while not self._stop.is_set():
            before = self.store.pending_count()
            if before == 0:
                return
            try:
                self._tick()
            except RateLimitError:
                return  # backend throttling; leave the rest durable for next run
            if self.store.pending_count() >= before:
                return  # no progress this pass; don't hang the shutdown

    def _loop(self) -> None:
        consecutive_failures = 0
        while not self._stop.is_set():
            try:
                tick_failures = self._tick()
            except RateLimitError as exc:
                consecutive_failures += 1
                delay = exc.retry_after or min(self.config.poll_interval * 2, MAX_RETRY_DELAY_SECONDS)
                logger.warning("pollen rate limited; backing off %ss", delay)
                self._stop.wait(delay)
                continue
            except Exception:
                consecutive_failures += 1
                delay = min(self.config.poll_interval * (2 ** consecutive_failures), MAX_RETRY_DELAY_SECONDS)
                logger.exception("pollen tick failed; backing off %ss", delay)
                self._stop.wait(delay)
                continue
            if tick_failures:
                consecutive_failures += 1
                delay = min(self.config.poll_interval * (2 ** consecutive_failures), MAX_RETRY_DELAY_SECONDS)
                logger.warning("pollen: %d upload(s) failed; backing off %ss", tick_failures, delay)
                self._stop.wait(delay)
            else:
                consecutive_failures = 0
                self._stop.wait(self.config.poll_interval)

    # ------------------------------------------------------------------ #
    # the work
    # ------------------------------------------------------------------ #
    def _tick(self) -> int:
        """Run one upload pass. Returns the number of failed uploads; raises
        RateLimitError so the loop can honour backend backoff."""
        if self._source is not None:
            try:
                self._source(self)  # enqueue any ready outputs (results, logs, ...)
            except Exception:
                logger.exception("pollen enqueue source failed")
        pending = self.store.claim_pending()
        if self.config.batch and self.archiver is not None:
            failures = self._upload_batched(pending)
        else:
            failures = sum(0 if self._upload_one(row) else 1 for row in pending)
        self._cleanup()
        return failures

    def _upload_one(self, row: UploadRow) -> bool:
        """Upload one row. True on success; False on a per-file error (left pending,
        not deleted). RateLimitError propagates so the whole loop backs off."""
        try:
            self.store.record_attempt(row.id)
            self.uploader.upload(row)
            self.store.mark_uploaded(row.id)
            return True
        except RateLimitError:
            raise
        except Exception:
            logger.exception("upload failed for %s (will retry; file kept)", row.s3_key)
            return False

    def _upload_batched(self, pending: list[UploadRow]) -> int:
        # Archive rows already in flight (e.g. from a previous interrupted tick)
        # upload directly; the rest are bundled per group.
        failures = 0
        for row in (r for r in pending if r.kind == ARCHIVE_KIND):
            if not self._upload_one(row):
                failures += 1
        members = [r for r in pending if r.kind != ARCHIVE_KIND]
        if not members:
            return failures

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
            except RateLimitError:
                raise
            except Exception:
                logger.exception("archive upload failed for %s (will retry)", artifact.s3_key)
                failures += 1
        return failures

    def _group_of(self, row: UploadRow) -> str:
        # Canonical key is v1/<device>/...; group a batch per device.
        parts = row.s3_key.split("/")
        return parts[1] if len(parts) > 1 else "batch"

    def _cleanup(self) -> None:
        for row in self.store.uploaded_rows():
            if kinds.for_kind(row.kind).delete_after_upload(row.metadata):
                Path(row.local_path).unlink(missing_ok=True)
                self.store.delete(row.id)
            else:
                # Retained file (DOT bucket, log): keep a tombstone so a re-scan
                # of the same content is deduped, but new content re-uploads.
                self.store.mark_done(row.id)

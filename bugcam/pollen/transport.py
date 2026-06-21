"""Resumable upload transport.

Small files go up in a single presigned PUT; large files use multipart, with
each completed part persisted to the store as it lands so an interrupted upload
resumes from where it left off rather than restarting. HTTP session is injectable.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from bugcam.pollen import kinds
from bugcam.pollen.store import PollenStore, UploadRow

try:
    import requests
except ImportError:
    requests = None  # type: ignore

DEFAULT_PART_SIZE = 64 * 1024 * 1024          # 64 MiB
DEFAULT_MULTIPART_THRESHOLD = 256 * 1024 * 1024  # 256 MiB


class UploadError(Exception):
    """Raised when an upload fails."""


class Uploader:
    def __init__(
        self,
        presigner: Any,
        store: PollenStore,
        *,
        multipart_threshold: int = DEFAULT_MULTIPART_THRESHOLD,
        part_size: int = DEFAULT_PART_SIZE,
        session: Any = None,
    ) -> None:
        self.presigner = presigner
        self.store = store
        self.multipart_threshold = multipart_threshold
        self.part_size = part_size
        self._session = session or (requests.Session() if requests else None)

    def upload(self, row: UploadRow) -> None:
        path = Path(row.local_path)
        size = path.stat().st_size
        content_type = kinds.for_kind(row.kind).content_type(path.name)
        if size <= self.multipart_threshold:
            self._single(row, path, content_type)
        else:
            self._multipart(row, path, content_type)

    def _put(self, url: str, data: bytes, content_type: str | None = None) -> Any:
        if self._session is None:
            raise UploadError("no HTTP session available")
        headers = {"Content-Type": content_type} if content_type else {}
        resp = self._session.put(url, data=data, headers=headers)
        resp.raise_for_status()
        return resp

    def _single(self, row: UploadRow, path: Path, content_type: str) -> None:
        url = self.presigner.put_url(row.s3_key)
        with open(path, "rb") as handle:
            self._put(url, handle.read(), content_type)

    def _multipart(self, row: UploadRow, path: Path, content_type: str) -> None:
        upload_id = row.upload_id
        if not upload_id:
            upload_id = self.presigner.create_multipart(row.s3_key)
            self.store.mark_uploading(row.id, upload_id=upload_id)

        already_done = {p["part_number"] for p in row.parts}
        part_number = 1
        with open(path, "rb") as handle:
            while True:
                chunk = handle.read(self.part_size)
                if not chunk:
                    break
                if part_number not in already_done:
                    url = self.presigner.part_url(row.s3_key, upload_id, part_number)
                    resp = self._put(url, chunk, content_type)
                    etag = resp.headers.get("ETag") or resp.headers.get("etag")
                    if not etag:
                        raise UploadError(f"missing ETag for part {part_number} of {row.s3_key}")
                    self.store.record_part(row.id, part_number, etag)
                part_number += 1

        parts = self.store.get(row.id).parts
        self.presigner.complete_multipart(row.s3_key, upload_id, parts)

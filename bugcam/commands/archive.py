"""Hourly batched-tar archiving of device output.

Bundles each device's ready output into one tar per run at
``v2/archives/<device_id>/<timestamp>.tar`` so a single upload replaces the
many small per-object PUTs the live v1 path makes today. Additive: this path
only runs when enabled and never touches the v1 upload path.

- FLIK: each finalized ``.done`` chunk dir is already terminal, so it is
  bundled whole (minus sidecars) and deleted locally after a successful ship.
- DOT: the day-bucket grows all day, so each run ships a *delta* -- a
  results.json filtered to tracks new since the last archive plus only those
  tracks' media -- so every tar is self-contained (the backend resolves media
  by path relative to results.json with no cross-object fallback). Archived
  track-ids accumulate in a per-bucket ``.archived`` state; the bucket itself
  is retained (day-bucket cleanup is handled separately).

Tar members mirror the local dir tree (``<device>/<date_time>/...``), which is
the bucket layout minus the ``v1/`` prefix.
"""
from __future__ import annotations

import io
import json
import shutil
import tarfile
from datetime import datetime
from pathlib import Path

from bugcam.s3_upload import (
    COMPLETED_TRACKS_FILENAME,
    DETECTION_META_FILENAME,
    DONE_MARKER_FILENAME,
    EXPECTED_TRACKS_FILENAME,
    RESULTS_FILENAME,
    UPLOADED_STATE_FILENAME,
    upload_file,
)

ARCHIVED_STATE_FILENAME = ".archived"
STAGING_DIRNAME = ".archive_staging"

# Sidecars and state files that must never be shipped inside an archive.
_SIDECAR_NAMES = {
    DONE_MARKER_FILENAME,
    DETECTION_META_FILENAME,
    EXPECTED_TRACKS_FILENAME,
    COMPLETED_TRACKS_FILENAME,
    UPLOADED_STATE_FILENAME,
    ARCHIVED_STATE_FILENAME,
    f"{RESULTS_FILENAME}.tmp",
}


def build_archives(
    output_dir: Path,
    flick_id: str,
    dot_ids: list[str],
    api_url: str,
    api_key: str,
    *,
    now: datetime | None = None,
) -> list[str]:
    """Bundle and ship one tar per device with new content. Returns S3 keys."""
    output_dir = Path(output_dir)
    if not output_dir.exists():
        return []
    timestamp = (now or datetime.now()).strftime("%Y%m%d_%H%M%S")

    shipped: list[str] = []
    shipped += _archive_flik_device(output_dir, flick_id, api_url, api_key, timestamp)
    for dot_id in dot_ids:
        shipped += _archive_dot_device(output_dir, dot_id, api_url, api_key, timestamp)
    return shipped


# --------------------------------------------------------------------------- #
# FLIK: whole finalized dirs, bundled then deleted
# --------------------------------------------------------------------------- #
def _archive_flik_device(output_dir: Path, flick_id: str, api_url: str, api_key: str, timestamp: str) -> list[str]:
    device_dir = output_dir / flick_id
    if not device_dir.is_dir():
        return []

    member_files: list[tuple[str, Path]] = []
    bundled_dirs: list[Path] = []
    for results_dir in _result_dirs(device_dir):
        if not (results_dir / DONE_MARKER_FILENAME).exists():
            continue  # not finished classifying yet; leave for a later run
        if _result_is_empty(results_dir):
            shutil.rmtree(results_dir)  # nothing detected: junk, not a result
            continue
        for path in sorted(results_dir.rglob("*")):
            if path.is_file() and path.name not in _SIDECAR_NAMES:
                member_files.append((path.relative_to(output_dir).as_posix(), path))
        bundled_dirs.append(results_dir)

    if not bundled_dirs:
        return []

    s3_key = f"v2/archives/{flick_id}/{timestamp}.tar"
    _ship_tar(output_dir, s3_key, member_files, [], api_url, api_key)
    for results_dir in bundled_dirs:
        shutil.rmtree(results_dir)
    return [s3_key]


# --------------------------------------------------------------------------- #
# DOT: hourly delta of new tracks, self-contained, state accumulates
# --------------------------------------------------------------------------- #
def _archive_dot_device(output_dir: Path, dot_id: str, api_url: str, api_key: str, timestamp: str) -> list[str]:
    device_dir = output_dir / dot_id
    if not device_dir.is_dir():
        return []

    member_files: list[tuple[str, Path]] = []
    member_data: list[tuple[str, bytes]] = []
    pending_states: list[dict] = []
    for results_dir in _result_dirs(device_dir):
        state = _collect_dot_delta(output_dir, results_dir, member_files, member_data)
        if state is not None:
            pending_states.append(state)

    if not pending_states:
        return []

    s3_key = f"v2/archives/{dot_id}/{timestamp}.tar"
    _ship_tar(output_dir, s3_key, member_files, member_data, api_url, api_key)
    for state in pending_states:
        _save_archived_state(state["results_dir"], {"track_ids": state["track_ids"], "files": state["files"]})
    return [s3_key]


def _collect_dot_delta(
    output_dir: Path,
    results_dir: Path,
    member_files: list[tuple[str, Path]],
    member_data: list[tuple[str, bytes]],
) -> dict | None:
    """Add this bucket's new-since-last-archive content to the tar member lists.

    Returns the state to persist after a successful ship, or None if nothing new.
    """
    state = _load_archived_state(results_dir)
    archived_tracks = set(state.get("track_ids", []))
    archived_files = set(state.get("files", []))

    results = _load_results(results_dir)
    new_tracks = [t for t in results.get("tracks", []) if str(t["track_id"]) not in archived_tracks]

    # Loose media not tied to a track (videos, backgrounds): ship newly arrived.
    new_files: list[Path] = []
    for sub in ("videos",):
        sub_dir = results_dir / sub
        if sub_dir.is_dir():
            for path in sorted(sub_dir.rglob("*")):
                if path.is_file() and path.relative_to(results_dir).as_posix() not in archived_files:
                    new_files.append(path)

    if not new_tracks and not new_files:
        return None

    # Self-contained delta: results.json holds only the new tracks.
    delta = dict(results)
    delta["tracks"] = new_tracks
    arc_results = (results_dir.relative_to(output_dir) / RESULTS_FILENAME).as_posix()
    member_data.append((arc_results, json.dumps(delta).encode("utf-8")))

    for track in new_tracks:
        for path in _dot_track_media(results_dir, str(track["track_id"])):
            member_files.append((path.relative_to(output_dir).as_posix(), path))
    for path in new_files:
        member_files.append((path.relative_to(output_dir).as_posix(), path))

    return {
        "results_dir": results_dir,
        "track_ids": sorted(archived_tracks | {str(t["track_id"]) for t in new_tracks}),
        "files": sorted(archived_files | {p.relative_to(results_dir).as_posix() for p in new_files}),
    }


def _dot_track_media(results_dir: Path, track_id: str) -> list[Path]:
    """Every crop/composite/label file belonging to one DOT track."""
    paths: list[Path] = []
    for crop_dir in sorted((results_dir / "crops").glob(f"{track_id}_*")):
        paths.extend(sorted(p for p in crop_dir.rglob("*") if p.is_file()))
    paths.extend(sorted(p for p in (results_dir / "composites").glob(f"{track_id}_*") if p.is_file()))
    label = results_dir / "labels" / f"{track_id}.json"
    if label.exists():
        paths.append(label)
    return paths


# --------------------------------------------------------------------------- #
# shared helpers
# --------------------------------------------------------------------------- #
def _result_dirs(device_dir: Path) -> list[Path]:
    return sorted(path.parent for path in device_dir.rglob(RESULTS_FILENAME))


def _load_results(results_dir: Path) -> dict:
    return json.loads((results_dir / RESULTS_FILENAME).read_text(encoding="utf-8"))


def _result_has_media(results_dir: Path) -> bool:
    for sub in ("crops", "composites", "videos"):
        media_dir = results_dir / sub
        if media_dir.is_dir() and any(path.is_file() for path in media_dir.rglob("*")):
            return True
    return False


def _result_is_empty(results_dir: Path) -> bool:
    return not _load_results(results_dir).get("tracks") and not _result_has_media(results_dir)


def _load_archived_state(results_dir: Path) -> dict:
    path = results_dir / ARCHIVED_STATE_FILENAME
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return {"track_ids": [], "files": []}


def _save_archived_state(results_dir: Path, state: dict) -> None:
    (results_dir / ARCHIVED_STATE_FILENAME).write_text(json.dumps(state), encoding="utf-8")


def _ship_tar(
    output_dir: Path,
    s3_key: str,
    member_files: list[tuple[str, Path]],
    member_data: list[tuple[str, bytes]],
    api_url: str,
    api_key: str,
) -> None:
    staging = output_dir / STAGING_DIRNAME
    staging.mkdir(parents=True, exist_ok=True)
    tar_path = staging / s3_key.replace("/", "_")
    try:
        with tarfile.open(tar_path, "w") as tar:
            for arcname, path in member_files:
                tar.add(path, arcname=arcname)
            for arcname, data in member_data:
                info = tarfile.TarInfo(name=arcname)
                info.size = len(data)
                tar.addfile(info, io.BytesIO(data))
        upload_file(api_url, api_key, tar_path, s3_key)
    finally:
        tar_path.unlink(missing_ok=True)

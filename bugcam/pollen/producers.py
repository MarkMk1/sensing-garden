"""Enqueue ready on-disk outputs (results, logs) into Pollen.

Telemetry (heartbeats/environment) is enqueued at its produce site; results and
logs are discovered by scanning the output tree, since they are produced deep in
the pipeline. Re-scans are safe: Pollen dedups on s3_key + fingerprint, keeping a
tombstone for retained files (DOT buckets, logs) so identical content is skipped
while changed content (a grown DOT results.json) re-uploads.
"""
from __future__ import annotations

import shutil
from pathlib import Path

from bugcam.pollen.kinds import _result_is_empty

RESULTS_FILENAME = "results.json"
DONE_MARKER = ".done"
SIDECARS = {
    ".done", ".detection.json", ".expected_tracks", ".completed_tracks",
    ".uploaded", ".archived", ".archived-aux",
}
AUX_DIR_NAMES = {"heartbeats", "environment", "logs"}


def enqueue_ready_outputs(pollen, output_dir, flick_id: str, dot_ids: list[str], *, today: str | None = None) -> int:
    """Scan output_dir and enqueue ready results + logs. Returns count enqueued."""
    output_dir = Path(output_dir)
    if not output_dir.exists():
        return 0
    pollen.store.prune_missing()  # bound tombstones whose files are gone
    enqueued = (
        _enqueue_results(pollen, output_dir, flick_id, dot_ids)
        + _enqueue_logs(pollen, output_dir, today=today)
    )
    _sweep_empty_result_dirs(output_dir, flick_id, dot_ids)
    return enqueued


def _sweep_empty_result_dirs(output_dir: Path, flick_id: str, dot_ids: list[str]) -> None:
    """Remove result-unit dirs left empty after their files were uploaded + deleted."""
    for device in {flick_id, *dot_ids}:
        device_dir = output_dir / device
        if not device_dir.is_dir():
            continue
        for child in list(device_dir.iterdir()):
            if child.is_dir() and child.name not in AUX_DIR_NAMES:
                # Sweepable once nothing but sidecars (e.g. a leftover .done) remains.
                if not any(p.is_file() and p.name not in SIDECARS for p in child.rglob("*")):
                    shutil.rmtree(child, ignore_errors=True)


def enqueue_result_dir(pollen, results_dir, flick_id: str, dot_ids: list[str]) -> int:
    """Enqueue one finalized result dir's files. Used by the scanner and by the
    pipeline's produce-site hook when a chunk is finalized."""
    results_dir = Path(results_dir)
    results_json = results_dir / RESULTS_FILENAME
    if not results_json.exists():
        return 0
    device = results_dir.parent.name
    is_flik = device == flick_id
    is_dot = device in dot_ids
    if not (is_flik or is_dot):
        return 0
    if _result_is_empty(results_json):
        if is_flik:
            shutil.rmtree(results_dir, ignore_errors=True)  # nothing detected: junk
        return 0
    count = 0
    for path in sorted(results_dir.rglob("*")):
        if path.is_file() and path.name not in SIDECARS:
            if pollen.enqueue(path, "result", metadata={"retain": is_dot}) is not None:
                count += 1
    return count


def _enqueue_results(pollen, output_dir: Path, flick_id: str, dot_ids: list[str]) -> int:
    count = 0
    for results_json in sorted(output_dir.rglob(RESULTS_FILENAME)):
        results_dir = results_json.parent
        if results_dir.parent.name == flick_id and not (results_dir / DONE_MARKER).exists():
            continue  # FLIK chunk not finalized yet
        count += enqueue_result_dir(pollen, results_dir, flick_id, dot_ids)
    return count


def _enqueue_logs(pollen, output_dir: Path, *, today: str | None = None) -> int:
    count = 0
    for log_dir in sorted(output_dir.glob("*/logs")):
        if not log_dir.is_dir():
            continue
        for path in sorted(p for p in log_dir.iterdir() if p.is_file()):
            if path.name.startswith("."):
                continue
            metadata = {"retain": True}
            if today is not None:
                metadata["today"] = today
            if pollen.enqueue(path, "log", metadata=metadata) is not None:
                count += 1
    return count

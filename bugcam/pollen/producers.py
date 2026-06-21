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


def enqueue_ready_outputs(pollen, output_dir, flick_id: str, dot_ids: list[str], *, today: str | None = None) -> int:
    """Scan output_dir and enqueue ready results + logs. Returns count enqueued."""
    output_dir = Path(output_dir)
    if not output_dir.exists():
        return 0
    pollen.store.prune_missing()  # bound tombstones whose files are gone
    return (
        _enqueue_results(pollen, output_dir, flick_id, dot_ids)
        + _enqueue_logs(pollen, output_dir, today=today)
    )


def _enqueue_results(pollen, output_dir: Path, flick_id: str, dot_ids: list[str]) -> int:
    count = 0
    for results_json in sorted(output_dir.rglob(RESULTS_FILENAME)):
        results_dir = results_json.parent
        device = results_dir.parent.name
        is_flik = device == flick_id
        is_dot = device in dot_ids
        if not (is_flik or is_dot):
            continue
        if is_flik and not (results_dir / DONE_MARKER).exists():
            continue  # FLIK chunk not finalized yet
        if _result_is_empty(results_json):
            if is_flik:
                shutil.rmtree(results_dir, ignore_errors=True)  # nothing detected: junk
            continue
        for path in sorted(results_dir.rglob("*")):
            if path.is_file() and path.name not in SIDECARS:
                if pollen.enqueue(path, "result", metadata={"retain": is_dot}) is not None:
                    count += 1
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

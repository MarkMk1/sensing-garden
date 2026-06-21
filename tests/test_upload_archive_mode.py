"""Archive-mode bypass on the per-object upload path.

When the hourly tar archiver is active it OWNS shipping of results, heartbeats,
environment, and logs. The per-object path must then do nothing but the
manifest -- which stays on the live path because the backend reads it at the
fixed key ``v1/manifest.json`` and it is uploaded only once per run.

TESTS WRITTEN FIRST: ``upload_ready_results`` has no ``archive_mode`` yet.
"""
import json
from pathlib import Path

from bugcam.commands import upload as upload_mod


def _make_flik_result(out: Path, device: str, chunk: str, track_ids: list[str]) -> Path:
    results_dir = out / device / chunk
    results_dir.mkdir(parents=True, exist_ok=True)
    tracks = [{"track_id": t} for t in track_ids]
    (results_dir / "results.json").write_text(
        json.dumps({"tracks": tracks, "summary": {"confirmed_tracks": len(tracks)}}),
        encoding="utf-8",
    )
    for t in track_ids:
        crop = results_dir / "crops" / t
        crop.mkdir(parents=True, exist_ok=True)
        (crop / "frame_000000.jpg").write_bytes(b"x")
    (results_dir / ".done").write_text("", encoding="utf-8")
    return results_dir


def _patch_uploads(mocker):
    return {
        "upload_directory": mocker.patch.object(upload_mod, "upload_directory"),
        "upload_file": mocker.patch.object(upload_mod, "upload_file"),
        "upload_manifest": mocker.patch.object(upload_mod, "upload_manifest"),
    }


class TestArchiveModeBypass:
    def test_manifest_only_when_archiving(self, tmp_path, mocker):
        out = tmp_path / "out"
        results_dir = _make_flik_result(out, "flick1", "chunk_001", ["t1"])
        # a heartbeat + a log waiting on the per-object path
        hb = out / "flick1" / "heartbeats"
        hb.mkdir(parents=True, exist_ok=True)
        (hb / "hb.json").write_text("{}", encoding="utf-8")
        logs = out / "flick1" / "logs"
        logs.mkdir(parents=True, exist_ok=True)
        (logs / "edge26_20200101.log").write_text("old\n", encoding="utf-8")
        patches = _patch_uploads(mocker)

        _, manifest_uploaded = upload_mod.upload_ready_results(
            out, "http://api", "k", "flick1", [],
            delete_after_upload=False, manifest_uploaded=False, archive_mode=True,
        )

        # manifest still ships; nothing else does
        patches["upload_manifest"].assert_called_once()
        patches["upload_directory"].assert_not_called()
        patches["upload_file"].assert_not_called()
        assert manifest_uploaded is True
        assert results_dir.exists()  # archiver owns results; not touched here

    def test_normal_mode_unaffected(self, tmp_path, mocker):
        out = tmp_path / "out"
        _make_flik_result(out, "flick1", "chunk_001", ["t1"])
        patches = _patch_uploads(mocker)

        upload_mod.upload_ready_results(
            out, "http://api", "k", "flick1", [],
            delete_after_upload=False, manifest_uploaded=True, archive_mode=False,
        )

        patches["upload_directory"].assert_called_once()  # default path still ships results

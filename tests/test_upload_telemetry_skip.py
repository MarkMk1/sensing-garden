"""When Pollen owns uploads, the per-object path ships only the manifest.

Pollen handles telemetry (produce-site), results, and logs; the manifest stays on
this path because the backend reads it at a fixed key.
"""
from datetime import datetime, timedelta
from pathlib import Path

from bugcam.commands import upload as upload_mod


def _patch(mocker):
    return {
        "upload_directory": mocker.patch.object(upload_mod, "upload_directory"),
        "upload_file": mocker.patch.object(upload_mod, "upload_file"),
        "upload_manifest": mocker.patch.object(upload_mod, "upload_manifest"),
    }


def _heartbeat(out: Path, device="flick1"):
    d = out / device / "heartbeats"
    d.mkdir(parents=True, exist_ok=True)
    (d / "20260101_000000.json").write_text("{}", encoding="utf-8")


def _completed_log(out: Path, device="flick1"):
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    d = out / device / "logs"
    d.mkdir(parents=True, exist_ok=True)
    (d / f"edge26_{yesterday}.log").write_text("log\n", encoding="utf-8")


def _flik_result(out: Path):
    rd = out / "flick1" / "20260101_120000"
    rd.mkdir(parents=True, exist_ok=True)
    (rd / "results.json").write_text('{"tracks":[{"track_id":"t"}]}', encoding="utf-8")
    (rd / ".done").write_text("", encoding="utf-8")


def test_pollen_owns_uploads_ships_manifest_only(tmp_path, mocker):
    out = tmp_path / "out"
    _heartbeat(out)
    _completed_log(out)
    _flik_result(out)
    patches = _patch(mocker)

    _, manifest_uploaded = upload_mod.upload_ready_results(
        out, "http://api", "k", "flick1", [],
        delete_after_upload=False, manifest_uploaded=False, pollen_owns_uploads=True,
    )

    patches["upload_manifest"].assert_called_once()
    patches["upload_directory"].assert_not_called()  # results -> Pollen
    patches["upload_file"].assert_not_called()        # heartbeats + logs -> Pollen
    assert manifest_uploaded is True


def test_default_path_unchanged(tmp_path, mocker):
    out = tmp_path / "out"
    _heartbeat(out)
    patches = _patch(mocker)

    upload_mod.upload_ready_results(
        out, "http://api", "k", "flick1", [],
        delete_after_upload=False, manifest_uploaded=True,
    )

    uploaded_keys = [c.args[3] for c in patches["upload_file"].call_args_list]
    assert any("/heartbeats/" in key for key in uploaded_keys)

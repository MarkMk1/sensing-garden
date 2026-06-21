"""When Pollen owns telemetry, the per-object path skips heartbeats/environment
but still ships logs, the manifest, and results.
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


def test_skip_telemetry_leaves_heartbeats_but_ships_logs(tmp_path, mocker):
    out = tmp_path / "out"
    _heartbeat(out)
    _completed_log(out)
    patches = _patch(mocker)

    upload_mod.upload_ready_results(
        out, "http://api", "k", "flick1", [],
        delete_after_upload=False, manifest_uploaded=True, skip_telemetry=True,
    )

    uploaded_keys = [c.args[3] for c in patches["upload_file"].call_args_list]
    assert not any("/heartbeats/" in key for key in uploaded_keys)  # left to Pollen
    assert any("/logs/" in key for key in uploaded_keys)            # logs still ship


def test_default_still_ships_heartbeats(tmp_path, mocker):
    out = tmp_path / "out"
    _heartbeat(out)
    patches = _patch(mocker)

    upload_mod.upload_ready_results(
        out, "http://api", "k", "flick1", [],
        delete_after_upload=False, manifest_uploaded=True,
    )

    uploaded_keys = [c.args[3] for c in patches["upload_file"].call_args_list]
    assert any("/heartbeats/" in key for key in uploaded_keys)

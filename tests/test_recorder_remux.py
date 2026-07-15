"""Remux behavior: timing/timeout reporting, ionice scheduling, and the
fallback when ffmpeg can't produce a valid MP4.

A failed or timed-out remux must not rename the (still raw H.264) temp file
to a .mp4 path -- that mislabels an elementary stream as a container format
most consumers can't read. It should also report its wall-clock duration and
whether it timed out, so the heartbeat can surface remux health without
guessing from log scraping.
"""
import subprocess
from unittest.mock import MagicMock

import pytest

import bugcam.edge26.capture.recorder as recorder_module
from bugcam.edge26.capture.recorder import VideoRecorder


def make_recorder(tmp_path, **overrides) -> VideoRecorder:
    kwargs = dict(
        output_dir=str(tmp_path),
        fps=10,
        chunk_duration=0,
        resolution=(640, 480),
        device_id="test",
        use_picamera=True,
    )
    kwargs.update(overrides)
    return VideoRecorder(**kwargs)


class TestRemuxCompleteCallback:
    def test_successful_remux_reports_duration_and_not_timed_out(self, tmp_path, monkeypatch):
        events = []
        rec = make_recorder(tmp_path, on_remux_complete=lambda d, t: events.append((d, t)))
        src = tmp_path / "chunk.h264"
        dst = tmp_path / "chunk.mp4"
        src.write_bytes(b"data")

        monkeypatch.setattr(
            recorder_module.subprocess, "run",
            lambda *a, **k: MagicMock(returncode=0, stderr=""),
        )
        monkeypatch.setattr(recorder_module.shutil, "which", lambda name: None)

        assert rec._remux_chunk(src, dst) is True
        assert len(events) == 1
        duration, timed_out = events[0]
        assert duration >= 0
        assert timed_out is False

    def test_nonzero_exit_reports_duration_and_not_timed_out(self, tmp_path, monkeypatch):
        events = []
        rec = make_recorder(tmp_path, on_remux_complete=lambda d, t: events.append((d, t)))
        src = tmp_path / "chunk.h264"
        src.write_bytes(b"data")

        monkeypatch.setattr(
            recorder_module.subprocess, "run",
            lambda *a, **k: MagicMock(returncode=1, stderr="bad input"),
        )
        monkeypatch.setattr(recorder_module.shutil, "which", lambda name: None)

        assert rec._remux_chunk(src, tmp_path / "chunk.mp4") is False
        assert events == [(pytest.approx(events[0][0]), False)]

    def test_timeout_reports_timed_out_true(self, tmp_path, monkeypatch):
        events = []
        rec = make_recorder(tmp_path, on_remux_complete=lambda d, t: events.append((d, t)))
        src = tmp_path / "chunk.h264"
        src.write_bytes(b"data")

        def raise_timeout(*a, **k):
            raise subprocess.TimeoutExpired(cmd=["ffmpeg"], timeout=30)

        monkeypatch.setattr(recorder_module.subprocess, "run", raise_timeout)
        monkeypatch.setattr(recorder_module.shutil, "which", lambda name: None)

        assert rec._remux_chunk(src, tmp_path / "chunk.mp4") is False
        assert len(events) == 1
        assert events[0][1] is True

    def test_missing_ffmpeg_does_not_report_a_remux_attempt(self, tmp_path, monkeypatch):
        events = []
        rec = make_recorder(tmp_path, on_remux_complete=lambda d, t: events.append((d, t)))
        src = tmp_path / "chunk.h264"
        src.write_bytes(b"data")

        def raise_not_found(*a, **k):
            raise FileNotFoundError("ffmpeg")

        monkeypatch.setattr(recorder_module.subprocess, "run", raise_not_found)
        monkeypatch.setattr(recorder_module.shutil, "which", lambda name: None)

        assert rec._remux_chunk(src, tmp_path / "chunk.mp4") is False
        assert events == []

    def test_missing_callback_is_a_noop(self, tmp_path, monkeypatch):
        rec = make_recorder(tmp_path)  # on_remux_complete not supplied
        src = tmp_path / "chunk.h264"
        src.write_bytes(b"data")

        monkeypatch.setattr(
            recorder_module.subprocess, "run",
            lambda *a, **k: MagicMock(returncode=0, stderr=""),
        )
        monkeypatch.setattr(recorder_module.shutil, "which", lambda name: None)

        assert rec._remux_chunk(src, tmp_path / "chunk.mp4") is True

    def test_callback_failure_does_not_break_remux(self, tmp_path, monkeypatch):
        def boom(duration, timed_out):
            raise RuntimeError("callback exploded")

        rec = make_recorder(tmp_path, on_remux_complete=boom)
        src = tmp_path / "chunk.h264"
        src.write_bytes(b"data")

        monkeypatch.setattr(
            recorder_module.subprocess, "run",
            lambda *a, **k: MagicMock(returncode=0, stderr=""),
        )
        monkeypatch.setattr(recorder_module.shutil, "which", lambda name: None)

        assert rec._remux_chunk(src, tmp_path / "chunk.mp4") is True


class TestIonicePrefix:
    def test_prefixes_command_when_ionice_available(self, tmp_path, monkeypatch):
        rec = make_recorder(tmp_path)
        src = tmp_path / "chunk.h264"
        src.write_bytes(b"data")
        captured = {}

        def fake_run(cmd, **kwargs):
            captured["cmd"] = cmd
            return MagicMock(returncode=0, stderr="")

        monkeypatch.setattr(recorder_module.subprocess, "run", fake_run)
        monkeypatch.setattr(recorder_module.shutil, "which", lambda name: "/usr/bin/ionice" if name == "ionice" else None)

        rec._remux_chunk(src, tmp_path / "chunk.mp4")

        assert captured["cmd"][:3] == ["ionice", "-c2", "-n0"]
        assert "ffmpeg" in captured["cmd"]

    def test_no_prefix_when_ionice_unavailable(self, tmp_path, monkeypatch):
        rec = make_recorder(tmp_path)
        src = tmp_path / "chunk.h264"
        src.write_bytes(b"data")
        captured = {}

        def fake_run(cmd, **kwargs):
            captured["cmd"] = cmd
            return MagicMock(returncode=0, stderr="")

        monkeypatch.setattr(recorder_module.subprocess, "run", fake_run)
        monkeypatch.setattr(recorder_module.shutil, "which", lambda name: None)

        rec._remux_chunk(src, tmp_path / "chunk.mp4")

        assert captured["cmd"][0] == "ffmpeg"


class TestFailedRemuxFallback:
    """_record_chunk_hardware's fallback when ffmpeg is installed but the
    remux itself fails or times out."""

    def install_camera_writing(self, rec, data=b"raw h264 bytes"):
        cam = MagicMock()

        def fake_start_recording(encoder, path, quality=None):
            from pathlib import Path
            Path(path).write_bytes(data)

        cam.start_recording.side_effect = fake_start_recording
        rec._init_camera = lambda: None
        rec.camera = cam
        rec.encoder = MagicMock()
        rec.encoder_quality = None
        return cam

    def test_failed_remux_keeps_h264_extension_and_data(self, tmp_path, monkeypatch):
        rec = make_recorder(tmp_path)
        self.install_camera_writing(rec, data=b"raw h264 bytes")
        rec._check_ffmpeg_available = lambda: True
        rec._remux_chunk = lambda src, dst: False  # simulates timeout/failure

        chunk_path = rec._record_chunk_hardware()

        assert chunk_path is not None
        assert chunk_path.suffix == ".h264"
        assert chunk_path.read_bytes() == b"raw h264 bytes"
        assert list(tmp_path.glob("*.mp4")) == []

    def test_successful_remux_still_produces_mp4(self, tmp_path, monkeypatch):
        rec = make_recorder(tmp_path)
        self.install_camera_writing(rec, data=b"raw h264 bytes")
        rec._check_ffmpeg_available = lambda: True

        def fake_remux(src, dst):
            dst.write_bytes(b"remuxed mp4 bytes")
            src.unlink(missing_ok=True)
            return True

        rec._remux_chunk = fake_remux

        chunk_path = rec._record_chunk_hardware()

        assert chunk_path.suffix == ".mp4"
        assert chunk_path.read_bytes() == b"remuxed mp4 bytes"

    def test_no_ffmpeg_still_saves_as_mp4_unchanged(self, tmp_path, monkeypatch):
        # Distinct, unchanged case: ffmpeg missing entirely is a deployment
        # condition, not the transient failure this fix targets.
        rec = make_recorder(tmp_path)
        self.install_camera_writing(rec, data=b"raw h264 bytes")
        rec._check_ffmpeg_available = lambda: False

        chunk_path = rec._record_chunk_hardware()

        assert chunk_path.suffix == ".mp4"
        assert chunk_path.read_bytes() == b"raw h264 bytes"

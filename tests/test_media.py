"""Tests for shared media/system probe helpers in bugcam.media."""
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

from bugcam.media import (
    check_camera_available,
    check_disk_space,
    check_ffmpeg_available,
    remux_to_mp4,
)


def test_check_ffmpeg_available_true() -> None:
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0
        assert check_ffmpeg_available() is True


def test_check_ffmpeg_available_false() -> None:
    with patch("subprocess.run", side_effect=FileNotFoundError()):
        assert check_ffmpeg_available() is False


def test_remux_to_mp4_success(tmp_path: Path) -> None:
    src = tmp_path / "in.h264"
    src.write_bytes(b"fake")
    dst = tmp_path / "out.mp4"

    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0
        assert remux_to_mp4(src, dst) is True
        args = mock_run.call_args[0][0]
        assert args[0] == "ffmpeg"
        assert "-c" in args and "copy" in args
        assert str(src) in args and str(dst) in args
        assert "-r" not in args


def test_remux_to_mp4_passes_fps_and_timeout(tmp_path: Path) -> None:
    src = tmp_path / "in.h264"
    src.write_bytes(b"fake")
    dst = tmp_path / "out.mp4"

    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0
        assert remux_to_mp4(src, dst, fps=30, timeout=30) is True
        args = mock_run.call_args[0][0]
        assert "-r" in args and "30" in args
        assert mock_run.call_args.kwargs.get("timeout") == 30


def test_remux_to_mp4_failure_returns_false(tmp_path: Path) -> None:
    src = tmp_path / "in.h264"
    src.write_bytes(b"fake")
    dst = tmp_path / "out.mp4"

    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 1
        mock_run.return_value.stderr = "boom"
        assert remux_to_mp4(src, dst) is False


def test_check_disk_space_sufficient(tmp_path: Path) -> None:
    with patch("shutil.disk_usage") as mock_usage:
        mock_usage.return_value = MagicMock(free=500 * 1024 * 1024)
        has_space, free_bytes = check_disk_space(tmp_path, 300 * 1024 * 1024)
        assert has_space is True
        assert free_bytes == 500 * 1024 * 1024


def test_check_disk_space_insufficient(tmp_path: Path) -> None:
    with patch("shutil.disk_usage") as mock_usage:
        mock_usage.return_value = MagicMock(free=100 * 1024 * 1024)
        has_space, free_bytes = check_disk_space(tmp_path, 300 * 1024 * 1024)
        assert has_space is False
        assert free_bytes == 100 * 1024 * 1024


def test_check_disk_space_unreadable_assumes_ok(tmp_path: Path) -> None:
    with patch("shutil.disk_usage", side_effect=OSError("nope")):
        has_space, free_bytes = check_disk_space(tmp_path, 300 * 1024 * 1024)
        assert has_space is True
        assert free_bytes == -1


def test_check_camera_available_ok() -> None:
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0
        ok, detail = check_camera_available()
        assert ok is True
        assert detail == "Accessible"


def test_check_camera_available_numpy_incompatibility() -> None:
    with patch("subprocess.run") as mock_run:
        mock_run.return_value.returncode = 1
        mock_run.return_value.stderr = b"... dtype size changed ..."
        ok, detail = check_camera_available()
        assert ok is False
        assert detail == "NumPy incompatibility"


def test_check_camera_available_timeout() -> None:
    with patch(
        "subprocess.run",
        side_effect=subprocess.TimeoutExpired(cmd="python3", timeout=5),
    ):
        ok, detail = check_camera_available(timeout=5)
        assert ok is False
        assert detail == "Timeout"

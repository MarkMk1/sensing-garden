"""Shared camera/ffmpeg/disk probes used by commands and the edge26 recorder."""
from __future__ import annotations

import logging
import shutil
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)

# The probe runs in the system interpreter: picamera2 is an OS package, not a
# venv dependency, and instantiating Picamera2 is the only reliable check that
# the camera stack (driver + libcamera + bindings) actually works.
_CAMERA_PROBE = ["/usr/bin/python3", "-c", "from picamera2 import Picamera2; Picamera2()"]


def check_ffmpeg_available(timeout: float = 5) -> bool:
    try:
        result = subprocess.run(["ffmpeg", "-version"], capture_output=True, timeout=timeout)
        return result.returncode == 0
    except Exception:
        return False


def remux_to_mp4(src: Path, dst: Path, *, fps: int | None = None, timeout: float | None = None) -> bool:
    """Rewrap a video into an MP4 container with ``ffmpeg -c copy``.

    Returns True on success. The source file is left in place; callers own
    any delete/replace semantics.
    """
    command = ["ffmpeg", "-y", "-loglevel", "error", "-i", str(src), "-c", "copy"]
    if fps is not None:
        command += ["-r", str(fps)]
    command.append(str(dst))
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    except Exception as exc:
        logger.error(f"Remux error: {exc}")
        return False
    if result.returncode != 0:
        logger.error(f"Remux failed: {result.stderr}")
        return False
    return True


def check_disk_space(directory: Path, min_free_bytes: int) -> tuple[bool, int]:
    """Whether ``directory`` has at least ``min_free_bytes`` free.

    Returns (has_space, free_bytes); free_bytes is -1 when the filesystem
    cannot be queried, in which case space is assumed sufficient.
    """
    try:
        free_bytes = shutil.disk_usage(directory).free
        return free_bytes >= min_free_bytes, free_bytes
    except Exception:
        return True, -1


def check_camera_available(timeout: float = 10) -> tuple[bool, str]:
    """Probe the Pi camera stack. Returns (ok, detail) for status displays."""
    try:
        result = subprocess.run(_CAMERA_PROBE, capture_output=True, timeout=timeout)
        if result.returncode == 0:
            return True, "Accessible"
        stderr = result.stderr.decode()
        if "dtype size changed" in stderr or "binary incompatibility" in stderr:
            return False, "NumPy incompatibility"
        return False, "Not accessible"
    except subprocess.TimeoutExpired:
        return False, "Timeout"
    except Exception as e:
        return False, str(e)[:50]

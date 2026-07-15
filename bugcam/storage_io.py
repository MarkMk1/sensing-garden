"""Best-effort storage I/O health for the heartbeat.

Cumulative counters from /proc/diskstats for the block device backing a given
path, plus its mount options. Values are cumulative since boot, not windowed
-- the backend diffs between timestamped heartbeats to get rate/utilization
over an interval, the same way it already must for the pipeline metrics'
total_seconds/total_count. Any read failure (non-Linux, path not backed by a
resolvable device, exotic layout) degrades to None: storage telemetry must
never break heartbeat delivery.
"""
from __future__ import annotations

from pathlib import Path

PROC_MOUNTS = Path("/proc/mounts")
PROC_DISKSTATS = Path("/proc/diskstats")

_DISKSTATS_FIELDS = (
    "read_ios", "read_merges", "read_sectors", "read_ms",
    "write_ios", "write_merges", "write_sectors", "write_ms",
    "ios_in_progress", "io_ms", "weighted_io_ms",
)


def _find_mount(path: Path) -> dict[str, str] | None:
    """Mount entry (device, options) whose filesystem backs ``path``.

    Matches by st_dev rather than path-string prefix, so it's correct for
    any subdirectory of the mount, and picks the last matching /proc/mounts
    line (later entries reflect more specific/recent mounts for a device).
    """
    try:
        target_dev = path.stat().st_dev
    except OSError:
        return None
    try:
        lines = PROC_MOUNTS.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None

    match = None
    for line in lines:
        parts = line.split()
        if len(parts) < 4:
            continue
        device, mount_point, _fstype, options = parts[:4]
        try:
            if Path(mount_point).stat().st_dev != target_dev:
                continue
        except OSError:
            continue
        match = {"device": device, "options": options}
    return match


def _read_diskstats_line(device_name: str) -> dict[str, int] | None:
    try:
        lines = PROC_DISKSTATS.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    for line in lines:
        fields = line.split()
        if len(fields) < 14 or fields[2] != device_name:
            continue
        values = [int(v) for v in fields[3:14]]
        return dict(zip(_DISKSTATS_FIELDS, values))
    return None


def read_storage_io_stats(path: Path) -> dict[str, object] | None:
    """Cumulative I/O counters for the device backing ``path``, or None if
    unavailable. Never raises."""
    mount = _find_mount(path)
    if mount is None:
        return None
    device_name = mount["device"].removeprefix("/dev/")
    stats = _read_diskstats_line(device_name)
    if stats is None:
        return None
    return {
        "device": mount["device"],
        "mount_options": mount["options"],
        **stats,
    }

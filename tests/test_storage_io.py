"""Storage I/O health for the heartbeat: cumulative /proc/diskstats counters
for the block device backing a given path. Best-effort -- unresolvable input
(no matching mount, no matching diskstats line, unreadable /proc files) must
degrade to None, never raise, since this feeds heartbeat delivery."""
from pathlib import Path

import bugcam.storage_io as storage_io


def _write_mounts(tmp_path: Path, lines: list[str]) -> Path:
    mounts = tmp_path / "mounts"
    mounts.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return mounts


def _write_diskstats(tmp_path: Path, lines: list[str]) -> Path:
    diskstats = tmp_path / "diskstats"
    diskstats.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return diskstats


def _diskstats_line(major: int, minor: int, name: str, *, read_ios=10, read_merges=1,
                     read_sectors=100, read_ms=5, write_ios=20, write_merges=2,
                     write_sectors=200, write_ms=15, ios_in_progress=0, io_ms=25,
                     weighted_io_ms=40) -> str:
    return (
        f"{major:4d} {minor:4d} {name} {read_ios} {read_merges} {read_sectors} "
        f"{read_ms} {write_ios} {write_merges} {write_sectors} {write_ms} "
        f"{ios_in_progress} {io_ms} {weighted_io_ms}"
    )


def test_reads_cumulative_counters_for_matching_device(tmp_path, monkeypatch):
    target = tmp_path / "media" / "KINGSTON"
    target.mkdir(parents=True)
    mounts = _write_mounts(tmp_path, [
        f"/dev/sda1 {target} ext4 rw,relatime 0 0",
    ])
    diskstats = _write_diskstats(tmp_path, [
        _diskstats_line(8, 1, "sda1", read_ios=42, write_ios=99, io_ms=1234, weighted_io_ms=5678),
    ])
    monkeypatch.setattr(storage_io, "PROC_MOUNTS", mounts)
    monkeypatch.setattr(storage_io, "PROC_DISKSTATS", diskstats)

    stats = storage_io.read_storage_io_stats(target)

    assert stats["device"] == "/dev/sda1"
    assert stats["mount_options"] == "rw,relatime"
    assert stats["read_ios"] == 42
    assert stats["write_ios"] == 99
    assert stats["io_ms"] == 1234
    assert stats["weighted_io_ms"] == 5678


def test_matches_via_parent_directory_not_just_exact_path(tmp_path, monkeypatch):
    mount_point = tmp_path / "media" / "KINGSTON"
    mount_point.mkdir(parents=True)
    nested = mount_point / "bugcam" / "incoming"
    nested.mkdir(parents=True)
    mounts = _write_mounts(tmp_path, [
        f"/dev/sda1 {mount_point} ext4 rw,relatime 0 0",
    ])
    diskstats = _write_diskstats(tmp_path, [_diskstats_line(8, 1, "sda1")])
    monkeypatch.setattr(storage_io, "PROC_MOUNTS", mounts)
    monkeypatch.setattr(storage_io, "PROC_DISKSTATS", diskstats)

    stats = storage_io.read_storage_io_stats(nested)

    assert stats["device"] == "/dev/sda1"


def test_no_matching_mount_returns_none(tmp_path, monkeypatch):
    # A /proc/mounts entry whose mount point no longer exists on disk (e.g. a
    # stale/unmounted entry) can't be resolved to a device -- must be skipped,
    # not mistaken for a match via some fallback.
    target = tmp_path / "media" / "KINGSTON"
    target.mkdir(parents=True)
    mounts = _write_mounts(tmp_path, [
        f"/dev/sda1 {tmp_path / 'nonexistent-mount-point'} ext4 rw,relatime 0 0",
    ])
    diskstats = _write_diskstats(tmp_path, [_diskstats_line(8, 1, "sda1")])
    monkeypatch.setattr(storage_io, "PROC_MOUNTS", mounts)
    monkeypatch.setattr(storage_io, "PROC_DISKSTATS", diskstats)

    assert storage_io.read_storage_io_stats(target) is None


def test_mount_found_but_no_matching_diskstats_line_returns_none(tmp_path, monkeypatch):
    target = tmp_path / "media" / "KINGSTON"
    target.mkdir(parents=True)
    mounts = _write_mounts(tmp_path, [
        f"/dev/sda1 {target} ext4 rw,relatime 0 0",
    ])
    diskstats = _write_diskstats(tmp_path, [_diskstats_line(8, 2, "sda2")])
    monkeypatch.setattr(storage_io, "PROC_MOUNTS", mounts)
    monkeypatch.setattr(storage_io, "PROC_DISKSTATS", diskstats)

    assert storage_io.read_storage_io_stats(target) is None


def test_missing_proc_files_returns_none_instead_of_raising(tmp_path, monkeypatch):
    monkeypatch.setattr(storage_io, "PROC_MOUNTS", tmp_path / "no-such-mounts-file")
    monkeypatch.setattr(storage_io, "PROC_DISKSTATS", tmp_path / "no-such-diskstats-file")

    assert storage_io.read_storage_io_stats(tmp_path) is None


def test_nonexistent_target_path_returns_none(tmp_path, monkeypatch):
    mounts = _write_mounts(tmp_path, [f"/dev/sda1 {tmp_path} ext4 rw,relatime 0 0"])
    diskstats = _write_diskstats(tmp_path, [_diskstats_line(8, 1, "sda1")])
    monkeypatch.setattr(storage_io, "PROC_MOUNTS", mounts)
    monkeypatch.setattr(storage_io, "PROC_DISKSTATS", diskstats)

    assert storage_io.read_storage_io_stats(tmp_path / "does" / "not" / "exist") is None

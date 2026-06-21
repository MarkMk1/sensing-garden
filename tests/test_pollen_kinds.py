"""Per-kind strategies: heartbeats/logs/results each keep their own treatment.

This is where the earlier cost bug-fixes are reintegrated as policy:
- an empty result (zero tracks, no media) is junk and is skipped,
- the current day's log is still being appended, so it ships only after rollover.
"""
import json
from datetime import datetime, timedelta
from pathlib import Path

from bugcam.pollen import kinds


def _write_results(results_dir: Path, tracks: list) -> Path:
    results_dir.mkdir(parents=True, exist_ok=True)
    path = results_dir / "results.json"
    path.write_text(json.dumps({"tracks": tracks}), encoding="utf-8")
    return path


class TestContentType:
    def test_by_extension(self):
        strat = kinds.for_kind("result")
        assert strat.content_type("results.json") == "application/json"
        assert strat.content_type("frame_000000.jpg") == "image/jpeg"
        assert strat.content_type("clip.mp4") == "video/mp4"
        assert strat.content_type("edge26_20260101.log") == "text/plain"
        assert kinds.for_kind("archive").content_type("x.tar") == "application/x-tar"
        assert strat.content_type("mystery.bin") == "application/octet-stream"


class TestResultKind:
    def test_empty_result_is_skipped(self, tmp_path):
        results = _write_results(tmp_path / "flick1" / "chunk", [])
        assert kinds.for_kind("result").should_skip(results, {}) is True

    def test_result_with_tracks_not_skipped(self, tmp_path):
        results = _write_results(tmp_path / "flick1" / "chunk", [{"track_id": "t1"}])
        assert kinds.for_kind("result").should_skip(results, {}) is False

    def test_zero_track_result_with_media_not_skipped(self, tmp_path):
        results_dir = tmp_path / "flick1" / "chunk"
        results = _write_results(results_dir, [])
        (results_dir / "videos").mkdir()
        (results_dir / "videos" / "clip.mp4").write_bytes(b"video")
        assert kinds.for_kind("result").should_skip(results, {}) is False


class TestLogKind:
    def test_active_day_log_skipped(self, tmp_path):
        today = datetime.now().strftime("%Y%m%d")
        path = tmp_path / f"edge26_{today}.log"
        path.write_text("active\n", encoding="utf-8")
        assert kinds.for_kind("log").should_skip(path, {}) is True

    def test_rolled_over_log_not_skipped(self, tmp_path):
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        path = tmp_path / f"edge26_{yesterday}.log"
        path.write_text("done\n", encoding="utf-8")
        assert kinds.for_kind("log").should_skip(path, {}) is False

    def test_today_can_be_injected(self, tmp_path):
        path = tmp_path / "edge26_20200101.log"
        path.write_text("x\n", encoding="utf-8")
        assert kinds.for_kind("log").should_skip(path, {"today": "20200101"}) is True


class TestCleanupPolicy:
    def test_delete_after_upload_by_default(self):
        assert kinds.for_kind("result").delete_after_upload({}) is True
        assert kinds.for_kind("heartbeat").delete_after_upload({}) is True

    def test_retain_metadata_keeps_file(self):
        # DOT day-buckets are retained, not deleted, after upload.
        assert kinds.for_kind("result").delete_after_upload({"retain": True}) is False


class TestRegistry:
    def test_known_kinds_resolve(self):
        for name in ("result", "log", "heartbeat", "environment", "archive"):
            assert kinds.for_kind(name).name == name

    def test_unknown_kind_falls_back_to_generic(self):
        strat = kinds.for_kind("something-new")
        assert strat.should_skip(Path("/x"), {}) is False
        assert strat.delete_after_upload({}) is True

"""The Pollen orchestrator: enqueue -> upload -> mark -> delete+prune, plus
lifecycle (start/stop), flush (drain while still accepting), and batched mode.

Transport is faked so nothing touches the network.
"""
import time
from datetime import datetime
from pathlib import Path

from bugcam.pollen.archive import TarArchiver
from bugcam.pollen.pollen import Pollen, PollenConfig


class FakeUploader:
    def __init__(self, fail_keys=()):
        self.uploaded = []
        self.fail_keys = set(fail_keys)

    def upload(self, row):
        if row.s3_key in self.fail_keys:
            raise RuntimeError("boom")
        self.uploaded.append(row.s3_key)


def _config(tmp_path, **kw):
    cfg = PollenConfig(
        db_path=tmp_path / "pollen.db",
        output_root=tmp_path / "out",
        staging_dir=tmp_path / "staging",
        poll_interval=0.01,
        **kw,
    )
    cfg.output_root.mkdir(parents=True, exist_ok=True)
    return cfg


def _write(root: Path, rel: str, content: bytes = b"x") -> Path:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def _pollen(cfg, uploader=None, **kw):
    return Pollen(cfg, uploader=uploader or FakeUploader(), **kw)


class TestEnqueue:
    def test_derives_key_from_output_root(self, tmp_path):
        cfg = _config(tmp_path)
        pol = _pollen(cfg)
        path = _write(cfg.output_root, "flick1/heartbeats/h.json", b"{}")
        rid = pol.enqueue(path, "heartbeat")
        assert pol.store.get(rid).s3_key == "v1/flick1/heartbeats/h.json"

    def test_empty_result_is_not_enqueued(self, tmp_path):
        cfg = _config(tmp_path)
        pol = _pollen(cfg)
        results = _write(cfg.output_root, "flick1/c/results.json", b'{"tracks":[]}')
        assert pol.enqueue(results, "result") is None
        assert pol.store.pending_count() == 0

    def test_active_log_not_enqueued(self, tmp_path):
        cfg = _config(tmp_path)
        pol = _pollen(cfg)
        today = datetime.now().strftime("%Y%m%d")
        log = _write(cfg.output_root, f"flick1/logs/edge26_{today}.log", b"active\n")
        assert pol.enqueue(log, "log") is None


class TestTick:
    def test_uploads_then_deletes_and_prunes(self, tmp_path):
        cfg = _config(tmp_path)
        up = FakeUploader()
        pol = _pollen(cfg, up)
        path = _write(cfg.output_root, "flick1/c/results.json", b'{"tracks":[{"track_id":"t"}]}')
        rid = pol.enqueue(path, "result")

        pol._tick()

        assert up.uploaded == ["v1/flick1/c/results.json"]
        assert not path.exists()
        assert pol.store.get(rid) is None

    def test_retain_keeps_file_but_prunes_row(self, tmp_path):
        cfg = _config(tmp_path)
        pol = _pollen(cfg)
        path = _write(cfg.output_root, "dot1/20260204/results.json", b'{"tracks":[{"track_id":"t"}]}')
        rid = pol.enqueue(path, "result", metadata={"retain": True})

        pol._tick()

        assert path.exists()  # DOT day-bucket retained
        assert pol.store.get(rid) is None

    def test_failed_upload_leaves_row_pending(self, tmp_path):
        cfg = _config(tmp_path)
        up = FakeUploader(fail_keys={"v1/flick1/c/results.json"})
        pol = _pollen(cfg, up)
        path = _write(cfg.output_root, "flick1/c/results.json", b'{"tracks":[{"track_id":"t"}]}')
        rid = pol.enqueue(path, "result")

        pol._tick()

        assert up.uploaded == []
        assert path.exists()
        row = pol.store.get(rid)
        assert row is not None and row.attempts >= 1


class TestFlush:
    def test_flush_drains_queue(self, tmp_path):
        cfg = _config(tmp_path)
        up = FakeUploader()
        pol = _pollen(cfg, up)
        for i in range(5):
            pol.enqueue(_write(cfg.output_root, f"flick1/c{i}/results.json", b'{"tracks":[{"track_id":"t"}]}'), "result")

        pol.flush()

        assert len(up.uploaded) == 5
        assert pol.store.pending_count() == 0


class TestLifecycle:
    def test_start_processes_then_stop(self, tmp_path):
        cfg = _config(tmp_path)
        up = FakeUploader()
        pol = _pollen(cfg, up)
        pol.start()
        try:
            pol.enqueue(_write(cfg.output_root, "flick1/c/results.json", b'{"tracks":[{"track_id":"t"}]}'), "result")
            deadline = time.time() + 3.0
            while not up.uploaded and time.time() < deadline:
                time.sleep(0.01)
        finally:
            pol.stop()
        assert up.uploaded == ["v1/flick1/c/results.json"]


class TestBatched:
    def test_batched_packs_uploads_and_cleans_members(self, tmp_path):
        cfg = _config(tmp_path, batch=True)
        up = FakeUploader()
        pol = _pollen(cfg, up, archiver=TarArchiver(), clock=lambda: datetime(2026, 2, 4, 13, 0, 0))
        a = _write(cfg.output_root, "flick1/c/results.json", b'{"tracks":[{"track_id":"t"}]}')
        b = _write(cfg.output_root, "flick1/c/crops/t/frame_000000.jpg", b"img")
        pol.enqueue(a, "result")
        pol.enqueue(b, "result")

        pol._tick()

        assert up.uploaded == ["v2/archives/flick1/20260204_130000.tar"]
        assert not a.exists() and not b.exists()  # members shipped in the tar, cleaned up
        assert pol.store.pending_count() == 0

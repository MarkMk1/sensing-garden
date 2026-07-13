"""Heartbeat transport decoupling (SPEC-fleet-monitoring item 1): heartbeats ship
flat and first (never inside archive tars), only the newest per device survives,
ship passes run between batch polls, and the run loop separates snapshot cadence
from upload cadence."""
import threading
import time
from pathlib import Path

from bugcam.commands import run as run_module
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


class TestFlatShipping:
    def test_batched_tick_ships_heartbeats_first_and_flat(self, tmp_path):
        cfg = _config(tmp_path, batch=True)
        up = FakeUploader()
        pol = Pollen(cfg, uploader=up, archiver=TarArchiver())
        result = _write(cfg.output_root, "flick1/c/results.json", b'{"tracks":[{"track_id":"t"}]}')
        beat = _write(cfg.output_root, "flick1/heartbeats/h1.json", b"{}")
        pol.enqueue_set([result], device="flick1", kind="result")
        pol.enqueue_set([beat], device="flick1", kind="heartbeat")

        pol._tick()

        assert up.uploaded[0] == "v1/flick1/heartbeats/h1.json"  # first, before the tar
        assert any(key.startswith("v2/archives/") for key in up.uploaded[1:])
        assert not any("heartbeats" in key for key in up.uploaded if key.startswith("v2/"))

    def test_supersede_keeps_only_newest_per_device(self, tmp_path):
        cfg = _config(tmp_path, batch=True)
        up = FakeUploader()
        pol = Pollen(cfg, uploader=up, archiver=TarArchiver())
        old_ids = []
        for name in ("h1.json", "h2.json", "h3.json"):
            path = _write(cfg.output_root, f"flick1/heartbeats/{name}", b"{}")
            old_ids.extend(pol.enqueue_set([path], device="flick1", kind="heartbeat"))
        other = _write(cfg.output_root, "flick2/heartbeats/h1.json", b"{}")
        pol.enqueue_set([other], device="flick2", kind="heartbeat")
        stale_staged = [pol.store.get(rid).staging_path for rid in old_ids[:2]]

        pol._ship_pending_heartbeats()

        assert "v1/flick1/heartbeats/h3.json" in up.uploaded
        assert "v1/flick2/heartbeats/h1.json" in up.uploaded
        assert "v1/flick1/heartbeats/h1.json" not in up.uploaded
        assert "v1/flick1/heartbeats/h2.json" not in up.uploaded
        assert pol.store.get(old_ids[0]) is None  # superseded rows dropped
        assert pol.store.get(old_ids[1]) is None
        assert not any(Path(p).exists() for p in stale_staged)  # staged copies swept

    def test_ship_pass_runs_between_polls(self, tmp_path):
        cfg = _config(tmp_path, batch=True, heartbeat_ship_interval=0.02)
        up = FakeUploader()
        pol = Pollen(cfg, uploader=up, archiver=TarArchiver())
        beat = _write(cfg.output_root, "flick1/heartbeats/h1.json", b"{}")
        pol.enqueue_set([beat], device="flick1", kind="heartbeat")

        pol._wait_shipping_heartbeats(0.1)  # inter-tick wait window

        assert "v1/flick1/heartbeats/h1.json" in up.uploaded

    def test_ship_pass_disabled_when_interval_zero(self, tmp_path):
        cfg = _config(tmp_path, batch=True, heartbeat_ship_interval=0)
        up = FakeUploader()
        pol = Pollen(cfg, uploader=up, archiver=TarArchiver())
        beat = _write(cfg.output_root, "flick1/heartbeats/h1.json", b"{}")
        pol.enqueue_set([beat], device="flick1", kind="heartbeat")

        pol._wait_shipping_heartbeats(0.05)

        assert up.uploaded == []


class RecordingPollen:
    def __init__(self):
        self.enqueued: list[Path] = []

    def enqueue_set(self, files, *, device, kind):
        assert kind == "heartbeat"
        self.enqueued.extend(Path(f) for f in files)
        return [1]


class TestRunLoopDecoupling:
    def test_uploads_less_often_than_snapshots(self, tmp_path, monkeypatch):
        counter = {"n": 0}

        def fake_snapshot(output_dir, flick_id, input_dir, dot_ids):
            counter["n"] += 1
            path = tmp_path / f"hb_{counter['n']:04d}.json"
            path.write_text("{}", encoding="utf-8")
            return path

        monkeypatch.setattr(run_module, "write_heartbeat_snapshot", fake_snapshot)
        pollen = RecordingPollen()
        stop = threading.Event()
        thread = threading.Thread(
            target=run_module._heartbeat_loop,
            args=("flick1", tmp_path, tmp_path, [], stop, pollen, 0.02, 0.08),
        )
        thread.start()
        time.sleep(0.3)
        stop.set()
        thread.join(timeout=2)

        assert counter["n"] >= 3  # snapshots kept their own cadence
        assert 1 <= len(pollen.enqueued) < counter["n"]  # uploads decoupled and rarer
        # Enqueued snapshots and superseded ones are unlinked; at most the held
        # latest remains on disk.
        remaining = list(tmp_path.glob("hb_*.json"))
        assert len(remaining) <= 1

    def test_resolve_upload_interval_cli_wins(self, monkeypatch):
        monkeypatch.setattr(run_module, "load_config", lambda: {"heartbeat_upload_interval": 99})
        assert run_module._resolve_heartbeat_upload_interval(5) == 5.0

    def test_resolve_upload_interval_from_config(self, monkeypatch):
        monkeypatch.setattr(run_module, "load_config", lambda: {"heartbeat_upload_interval": 15})
        assert run_module._resolve_heartbeat_upload_interval(None) == 15.0

    def test_resolve_upload_interval_default(self, monkeypatch):
        monkeypatch.setattr(run_module, "load_config", lambda: {})
        assert run_module._resolve_heartbeat_upload_interval(None) == float(
            run_module.HEARTBEAT_UPLOAD_INTERVAL_SECONDS
        )

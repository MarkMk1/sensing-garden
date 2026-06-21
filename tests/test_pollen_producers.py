"""The result/log enqueue scanner that bridges the output tree into Pollen."""
from datetime import datetime, timedelta
from pathlib import Path

from bugcam.pollen.pollen import Pollen, PollenConfig
from bugcam.pollen.producers import enqueue_ready_outputs


class FakeUploader:
    def __init__(self):
        self.uploaded = []

    def upload(self, row):
        self.uploaded.append(row.s3_key)


def _pollen(tmp_path):
    cfg = PollenConfig(
        db_path=tmp_path / "pollen.db",
        output_root=tmp_path / "out",
        staging_dir=tmp_path / "staging",
        poll_interval=0.01,
    )
    cfg.output_root.mkdir(parents=True, exist_ok=True)
    return Pollen(cfg, uploader=FakeUploader()), cfg.output_root


def _result_dir(out: Path, device: str, name: str, *, tracks=("t1",), done=False) -> Path:
    rd = out / device / name
    rd.mkdir(parents=True, exist_ok=True)
    import json
    (rd / "results.json").write_text(json.dumps({"tracks": [{"track_id": t} for t in tracks]}), encoding="utf-8")
    for t in tracks:
        crop = rd / "crops" / t
        crop.mkdir(parents=True, exist_ok=True)
        (crop / "frame_000000.jpg").write_bytes(b"img")
    if done:
        (rd / ".done").write_text("", encoding="utf-8")
    return rd


class TestResults:
    def test_flik_done_dir_enqueued(self, tmp_path):
        pol, out = _pollen(tmp_path)
        _result_dir(out, "flick1", "20260204_120000", done=True)
        n = enqueue_ready_outputs(pol, out, "flick1", [])
        assert n == 2  # results.json + one crop
        keys = {r.s3_key for r in pol.store.claim_pending()}
        assert "v1/flick1/20260204_120000/results.json" in keys
        assert "v1/flick1/20260204_120000/crops/t1/frame_000000.jpg" in keys

    def test_flik_without_done_skipped(self, tmp_path):
        pol, out = _pollen(tmp_path)
        _result_dir(out, "flick1", "20260204_120000", done=False)
        assert enqueue_ready_outputs(pol, out, "flick1", []) == 0

    def test_empty_flik_done_dir_deleted(self, tmp_path):
        pol, out = _pollen(tmp_path)
        rd = _result_dir(out, "flick1", "20260204_120000", tracks=(), done=True)  # zero tracks, no media
        assert enqueue_ready_outputs(pol, out, "flick1", []) == 0
        assert not rd.exists()

    def test_dot_dir_enqueued_with_retain(self, tmp_path):
        pol, out = _pollen(tmp_path)
        _result_dir(out, "dot1", "20260204")
        enqueue_ready_outputs(pol, out, "flick1", ["dot1"])
        rows = pol.store.claim_pending()
        assert rows and all(r.metadata.get("retain") is True for r in rows)

    def test_rescan_dedups(self, tmp_path):
        pol, out = _pollen(tmp_path)
        _result_dir(out, "dot1", "20260204")
        first = enqueue_ready_outputs(pol, out, "flick1", ["dot1"])
        second = enqueue_ready_outputs(pol, out, "flick1", ["dot1"])
        assert first > 0 and second == 0  # nothing new on the re-scan

    def test_changed_dot_results_reenqueues_after_upload(self, tmp_path):
        pol, out = _pollen(tmp_path)
        rd = _result_dir(out, "dot1", "20260204", tracks=("t1",))
        enqueue_ready_outputs(pol, out, "flick1", ["dot1"])
        pol._tick()  # upload -> tombstone (retained)
        assert enqueue_ready_outputs(pol, out, "flick1", ["dot1"]) == 0  # unchanged

        # a new track grows results.json -> changed fingerprint -> re-enqueues
        import json
        (rd / "results.json").write_text(json.dumps({"tracks": [{"track_id": "t1"}, {"track_id": "t2"}]}), encoding="utf-8")
        assert enqueue_ready_outputs(pol, out, "flick1", ["dot1"]) >= 1
        assert any(r.s3_key.endswith("results.json") for r in pol.store.claim_pending())


class TestEmptyDirSweep:
    def test_emptied_flik_dir_is_swept(self, tmp_path):
        pol, out = _pollen(tmp_path)
        rd = _result_dir(out, "flick1", "20260204_120000", done=True)
        enqueue_ready_outputs(pol, out, "flick1", [])
        pol._tick()  # uploads + deletes the FLIK files, leaving an empty shell

        enqueue_ready_outputs(pol, out, "flick1", [])  # next scan sweeps it
        assert not rd.exists()

    def test_telemetry_dirs_not_swept_when_empty(self, tmp_path):
        pol, out = _pollen(tmp_path)
        (out / "flick1" / "heartbeats").mkdir(parents=True)
        (out / "flick1" / "logs").mkdir(parents=True)
        enqueue_ready_outputs(pol, out, "flick1", [])
        assert (out / "flick1" / "heartbeats").exists()
        assert (out / "flick1" / "logs").exists()

    def test_retained_dot_dir_not_swept(self, tmp_path):
        pol, out = _pollen(tmp_path)
        rd = _result_dir(out, "dot1", "20260204")
        enqueue_ready_outputs(pol, out, "flick1", ["dot1"])
        pol._tick()  # DOT files retained -> dir still has files
        enqueue_ready_outputs(pol, out, "flick1", ["dot1"])
        assert rd.exists()


class TestLogs:
    def test_completed_log_enqueued_today_skipped(self, tmp_path):
        pol, out = _pollen(tmp_path)
        today = datetime.now().strftime("%Y%m%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        logs = out / "flick1" / "logs"
        logs.mkdir(parents=True)
        (logs / f"edge26_{yesterday}.log").write_text("done\n", encoding="utf-8")
        (logs / f"edge26_{today}.log").write_text("active\n", encoding="utf-8")

        enqueue_ready_outputs(pol, out, "flick1", [])

        keys = [r.s3_key for r in pol.store.claim_pending()]
        assert any(yesterday in k for k in keys)
        assert not any(today in k for k in keys)

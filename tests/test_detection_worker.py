"""Tests for the edge26 detection worker loop and its extracted helpers.

These pin the orchestration behaviour of ``Pipeline._detection_worker`` so it can
be refactored safely as part of the upload-batching work:

- DOT directories are drained on every tick (after a video AND while idle), via a
  single shared helper (no copy-pasted loop in the ``try`` and ``except Empty``).
- The stale-directory sweep runs from a ``finally`` clause, so it advances once per
  *tick* (including idle seconds) rather than only when a FLIK video arrives.
- The completion check that terminates the worker is a single readable predicate.

Detection/classification themselves are never run here: every per-item method is a
spy, so we test loop control flow only — no camera, no Hailo, no S3.
"""
import queue
import threading

from bugcam.edge26.main import Pipeline


# --------------------------------------------------------------------------- #
# harness
# --------------------------------------------------------------------------- #
def _bare_pipeline(tmp_path, mocker, *, dot_ids=()):
    """A Pipeline with __init__ bypassed and only the attributes the worker uses."""
    p = Pipeline.__new__(Pipeline)
    p.config = {}
    p.flick_id = "edge26"
    p.dot_ids = list(dot_ids)
    p.input_storage = tmp_path / "input"
    p.input_storage.mkdir(parents=True, exist_ok=True)
    p.results_dir = tmp_path / "results"
    p.results_dir.mkdir(parents=True, exist_ok=True)
    p.video_queue = queue.Queue()
    p.stop_event = threading.Event()
    p.recording_stopped = threading.Event()
    p.classification_queue = mocker.Mock()
    p.classification_queue.count.return_value = 0
    p._sweep_counter = 0
    p._sweep_interval = 30
    return p


def _make_dot_dir(input_storage, dot_id, date="20260101", ready_track=None):
    """Create input_storage/<dot_id>_<date>/, optionally with a done.txt-ready track."""
    d = input_storage / f"{dot_id}_{date}"
    (d / "crops").mkdir(parents=True, exist_ok=True)
    if ready_track:
        td = d / "crops" / f"{ready_track}_120000"
        td.mkdir(parents=True, exist_ok=True)
        (td / "frame_000000.jpg").write_bytes(b"x")
        (td / "done.txt").write_text("", encoding="utf-8")
    return d


def _spy_item_methods(p, mocker):
    """Replace every per-item processing method with a spy."""
    return {
        "video": mocker.patch.object(p, "_process_video_detection"),
        "media": mocker.patch.object(p, "_process_dot_media"),
        "detect": mocker.patch.object(p, "_process_dot_directory_detection"),
        "sweep": mocker.patch.object(p, "_sweep_stale_directories"),
    }


def _run_worker(p, timeout=6.0):
    """Run the worker to completion in a thread; fail if it does not terminate."""
    t = threading.Thread(target=p._detection_worker, daemon=True)
    t.start()
    t.join(timeout)
    still_running = t.is_alive()
    p.stop_event.set()  # release any lingering loop before asserting
    assert not still_running, "detection worker did not terminate"


# --------------------------------------------------------------------------- #
# _drain_dot_directories — the de-duplicated interleave helper
# --------------------------------------------------------------------------- #
class TestDrainDotDirectories:
    def test_processes_every_dot_directory_once(self, tmp_path, mocker):
        p = _bare_pipeline(tmp_path, mocker, dot_ids=["dot1", "dot2"])
        _make_dot_dir(p.input_storage, "dot1")
        _make_dot_dir(p.input_storage, "dot2")
        spies = _spy_item_methods(p, mocker)

        p._drain_dot_directories()

        assert spies["media"].call_count == 2
        assert spies["detect"].call_count == 2
        drained = {call.args[0].name for call in spies["media"].call_args_list}
        assert drained == {"dot1_20260101", "dot2_20260101"}

    def test_stops_early_on_stop_event(self, tmp_path, mocker):
        p = _bare_pipeline(tmp_path, mocker, dot_ids=["dot1", "dot2"])
        _make_dot_dir(p.input_storage, "dot1")
        _make_dot_dir(p.input_storage, "dot2")
        spies = _spy_item_methods(p, mocker)
        p.stop_event.set()

        p._drain_dot_directories()

        assert spies["media"].call_count == 0

    def test_noop_without_dot_ids(self, tmp_path, mocker):
        p = _bare_pipeline(tmp_path, mocker, dot_ids=[])
        _make_dot_dir(p.input_storage, "dot1")  # present on disk but not configured
        spies = _spy_item_methods(p, mocker)

        p._drain_dot_directories()

        assert spies["media"].call_count == 0
        assert spies["detect"].call_count == 0


# --------------------------------------------------------------------------- #
# _processing_complete — the worker's termination predicate
# --------------------------------------------------------------------------- #
class TestProcessingComplete:
    def test_false_while_recording(self, tmp_path, mocker):
        p = _bare_pipeline(tmp_path, mocker, dot_ids=["dot1"])
        # recording_stopped not set
        assert p._processing_complete() is False

    def test_false_with_videos_queued(self, tmp_path, mocker):
        p = _bare_pipeline(tmp_path, mocker, dot_ids=["dot1"])
        p.recording_stopped.set()
        p.video_queue.put(tmp_path / "edge26_20260101_120000.mp4")
        assert p._processing_complete() is False

    def test_false_with_ready_dot_tracks(self, tmp_path, mocker):
        p = _bare_pipeline(tmp_path, mocker, dot_ids=["dot1"])
        p.recording_stopped.set()
        _make_dot_dir(p.input_storage, "dot1", ready_track="t1")
        assert p._processing_complete() is False

    def test_false_with_classification_pending(self, tmp_path, mocker):
        p = _bare_pipeline(tmp_path, mocker, dot_ids=["dot1"])
        p.recording_stopped.set()
        p.classification_queue.count.return_value = 1
        assert p._processing_complete() is False

    def test_true_when_fully_drained(self, tmp_path, mocker):
        p = _bare_pipeline(tmp_path, mocker, dot_ids=["dot1"])
        p.recording_stopped.set()
        _make_dot_dir(p.input_storage, "dot1")  # dir exists but no ready tracks
        p.classification_queue.count.return_value = 0
        assert p._processing_complete() is True


# --------------------------------------------------------------------------- #
# _detection_worker — the loop itself (threaded, stopped via spy side-effects)
# --------------------------------------------------------------------------- #
class TestDetectionWorkerLoop:
    def test_processes_queued_video(self, tmp_path, mocker):
        p = _bare_pipeline(tmp_path, mocker)
        mocker.patch.object(p, "_find_existing_items", return_value=[])
        spies = _spy_item_methods(p, mocker)
        video = tmp_path / "edge26_20260101_120000.mp4"
        p.video_queue.put(video)
        spies["video"].side_effect = lambda *_: p.stop_event.set()

        _run_worker(p)

        spies["video"].assert_called_once_with(video)

    def test_drains_dot_while_idle(self, tmp_path, mocker):
        # No video is ever queued: the DOT dir must be drained from the idle path.
        p = _bare_pipeline(tmp_path, mocker, dot_ids=["dot1"])
        mocker.patch.object(p, "_find_existing_items", return_value=[])
        _make_dot_dir(p.input_storage, "dot1")
        spies = _spy_item_methods(p, mocker)
        spies["media"].side_effect = lambda *_: p.stop_event.set()

        _run_worker(p)

        assert spies["media"].called
        assert spies["video"].call_count == 0

    def test_terminates_when_pipeline_drained(self, tmp_path, mocker):
        # recording stopped + nothing queued + no ready tracks + nothing classifying
        p = _bare_pipeline(tmp_path, mocker)
        mocker.patch.object(p, "_find_existing_items", return_value=[])
        _spy_item_methods(p, mocker)
        p.recording_stopped.set()

        _run_worker(p)  # asserts the worker exits on its own

    def test_sweep_fires_on_interval(self, tmp_path, mocker):
        p = _bare_pipeline(tmp_path, mocker)
        p._sweep_interval = 3
        mocker.patch.object(p, "_find_existing_items", return_value=[])
        spies = _spy_item_methods(p, mocker)
        for i in range(3):
            p.video_queue.put(tmp_path / f"edge26_2026010{i}_120000.mp4")

        calls = {"n": 0}

        def _count_then_maybe_stop(*_):
            calls["n"] += 1
            if calls["n"] >= 3:
                p.stop_event.set()

        spies["video"].side_effect = _count_then_maybe_stop

        _run_worker(p)

        # Three ticks, interval 3 -> exactly one sweep.
        assert spies["sweep"].call_count == 1

    def test_sweep_fires_during_idle(self, tmp_path, mocker):
        # Intentional behaviour change: the sweep advances on idle ticks too, not
        # only when a FLIK video is processed. Fails before the finally-clause move.
        p = _bare_pipeline(tmp_path, mocker)
        p._sweep_interval = 2
        mocker.patch.object(p, "_find_existing_items", return_value=[])
        spies = _spy_item_methods(p, mocker)
        # recording_stopped left unset so the worker does not self-terminate;
        # the sweep itself stops the loop once it finally fires.
        spies["sweep"].side_effect = lambda *_: p.stop_event.set()

        _run_worker(p)

        assert spies["sweep"].called
        assert spies["video"].call_count == 0

    def test_exception_does_not_kill_worker(self, tmp_path, mocker):
        # A failure processing one video must not tear down the producer thread.
        p = _bare_pipeline(tmp_path, mocker)
        mocker.patch.object(p, "_find_existing_items", return_value=[])
        spies = _spy_item_methods(p, mocker)
        p.video_queue.put(tmp_path / "edge26_20260101_120000.mp4")
        p.video_queue.put(tmp_path / "edge26_20260101_120100.mp4")

        calls = {"n": 0}

        def _raise_then_succeed(*_):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("boom")
            p.stop_event.set()

        spies["video"].side_effect = _raise_then_succeed

        _run_worker(p)

        # Second video was processed despite the first raising.
        assert spies["video"].call_count == 2

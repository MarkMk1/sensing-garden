"""Tests for hourly batched-tar archiving (upload-batching workstream).

The archiver bundles a device's ready output into one tar per hourly run at
``v2/archives/<device_id>/<timestamp>.tar`` so that one upload replaces the
thousands of small per-object PUTs the live v1 path makes today.

Two unit types, both self-contained so the backend can ingest each tar in
isolation (it resolves media by path relative to results.json, with no
cross-object fallback):

- FLIK: each ``.done`` chunk dir is already terminal -> bundled whole (minus
  sidecars), then deleted locally after a successful ship.
- DOT: the day-bucket grows all day, so each run ships a *delta* -- a
  results.json filtered to tracks new since the last archive plus only those
  tracks' media. Archived track-ids accumulate in a per-bucket ``.archived``
  state. No new tracks -> no tar. The day-bucket is not deleted here (that is
  separate cleanup).

Tar members mirror the local dir tree (``<device>/<date_time>/...``), which is
the bucket layout minus the ``v1/`` prefix. Shipping goes through
``archive.upload_file`` (the multipart-capable client), patched here so no
network or AWS is touched.

TESTS WRITTEN FIRST: the ``bugcam.commands.archive`` module does not exist yet,
so this whole file is RED until the archiver lands.
"""
import json
import tarfile
from datetime import datetime
from pathlib import Path

from bugcam.commands import archive


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
NOW = datetime(2026, 2, 4, 13, 0, 0)  # fixed run time -> deterministic tar names


def _write_results(results_dir: Path, tracks: list[dict]) -> None:
    results_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "source_device": results_dir.parent.name,
        "date": results_dir.name[:8],
        "summary": {"confirmed_tracks": len(tracks)},
        "tracks": tracks,
    }
    (results_dir / "results.json").write_text(json.dumps(payload), encoding="utf-8")


def _track(track_id: str, ts: str) -> dict:
    return {"track_id": track_id, "timestamp": ts,
            "final_prediction": {"family": "f", "genus": "g", "species": "s"}}


def _add_track_media(results_dir: Path, track_id: str, ts: str) -> None:
    crop = results_dir / "crops" / f"{track_id}_{ts}"
    crop.mkdir(parents=True, exist_ok=True)
    (crop / "frame_000000.jpg").write_bytes(b"crop")
    composites = results_dir / "composites"
    composites.mkdir(parents=True, exist_ok=True)
    (composites / f"{track_id}_{ts}.jpg").write_bytes(b"composite")
    labels = results_dir / "labels"
    labels.mkdir(parents=True, exist_ok=True)
    (labels / f"{track_id}.json").write_text("{}", encoding="utf-8")


def _make_flik_dir(out: Path, flick_id: str, chunk: str, track_ids: list[str], *, done: bool = True) -> Path:
    results_dir = out / flick_id / chunk
    _write_results(results_dir, [_track(t, chunk.split("_")[-1]) for t in track_ids])
    for t in track_ids:
        crop = results_dir / "crops" / t
        crop.mkdir(parents=True, exist_ok=True)
        (crop / "frame_000000.jpg").write_bytes(b"crop")
    if done:
        (results_dir / ".done").write_text("classified=1\nexpected=1\n", encoding="utf-8")
    return results_dir


def _make_dot_bucket(out: Path, dot_id: str, day: str, tracks: list[tuple[str, str]]) -> Path:
    results_dir = out / dot_id / day
    _write_results(results_dir, [_track(t, ts) for t, ts in tracks])
    for t, ts in tracks:
        _add_track_media(results_dir, t, ts)
    return results_dir


def _capture_uploads(mocker):
    """Patch archive.upload_file; record each tar's S3 key and member names."""
    captured: list[dict] = []

    def _fake_upload(api_url, api_key, local_path, s3_key):
        with tarfile.open(local_path) as tf:
            members = sorted(m.name for m in tf.getmembers() if m.isfile())
        captured.append({"s3_key": s3_key, "members": members})

    mocker.patch.object(archive, "upload_file", side_effect=_fake_upload)
    return captured


def _run(out: Path, flick_id="flick1", dot_ids=("dot1",), now=NOW):
    return archive.build_archives(out, flick_id, list(dot_ids), "http://api", "k", now=now)


# --------------------------------------------------------------------------- #
# FLIK: whole finalized dirs, bundled then deleted
# --------------------------------------------------------------------------- #
class TestFlikArchive:
    def test_done_dir_bundled_and_deleted(self, tmp_path, mocker):
        out = tmp_path / "out"
        results_dir = _make_flik_dir(out, "flick1", "20260204_120000", ["t1"])
        captured = _capture_uploads(mocker)

        _run(out)

        assert len(captured) == 1
        assert captured[0]["s3_key"] == "v2/archives/flick1/20260204_130000.tar"
        assert "flick1/20260204_120000/results.json" in captured[0]["members"]
        assert "flick1/20260204_120000/crops/t1/frame_000000.jpg" in captured[0]["members"]
        # shipped -> removed locally
        assert not results_dir.exists()

    def test_unfinished_dir_skipped(self, tmp_path, mocker):
        out = tmp_path / "out"
        results_dir = _make_flik_dir(out, "flick1", "20260204_120000", ["t1"], done=False)
        captured = _capture_uploads(mocker)

        _run(out)

        assert captured == []
        assert results_dir.exists()  # left for a later run

    def test_empty_done_dir_not_shipped(self, tmp_path, mocker):
        out = tmp_path / "out"
        _make_flik_dir(out, "flick1", "20260204_120000", [])  # zero tracks, no media
        captured = _capture_uploads(mocker)

        _run(out)

        assert captured == []

    def test_sidecars_excluded_from_tar(self, tmp_path, mocker):
        out = tmp_path / "out"
        results_dir = _make_flik_dir(out, "flick1", "20260204_120000", ["t1"])
        (results_dir / ".detection.json").write_text("{}", encoding="utf-8")
        (results_dir / ".expected_tracks").write_text("1", encoding="utf-8")
        captured = _capture_uploads(mocker)

        _run(out)

        members = captured[0]["members"]
        assert not any(name.endswith(".done") for name in members)
        assert not any(name.endswith(".detection.json") for name in members)
        assert not any(name.endswith(".expected_tracks") for name in members)


# --------------------------------------------------------------------------- #
# DOT: hourly delta of new tracks, self-contained, state accumulates
# --------------------------------------------------------------------------- #
class TestDotDeltaArchive:
    def test_first_run_ships_all_current_tracks(self, tmp_path, mocker):
        out = tmp_path / "out"
        _make_dot_bucket(out, "dot1", "20260204", [("t1", "120100")])
        captured = _capture_uploads(mocker)

        _run(out)

        assert len(captured) == 1
        assert captured[0]["s3_key"] == "v2/archives/dot1/20260204_130000.tar"
        assert "dot1/20260204/results.json" in captured[0]["members"]
        assert "dot1/20260204/crops/t1_120100/frame_000000.jpg" in captured[0]["members"]

    def test_delta_results_json_holds_only_new_tracks(self, tmp_path, mocker):
        out = tmp_path / "out"
        results_dir = _make_dot_bucket(out, "dot1", "20260204", [("t1", "120100")])
        mocker.patch.object(archive, "upload_file")  # seed run: no-op ship
        _run(out)  # archives t1

        # a new track is classified into the same growing day-bucket
        _write_results(results_dir, [_track("t1", "120100"), _track("t2", "120500")])
        _add_track_media(results_dir, "t2", "120500")
        captured = _capture_uploads(mocker)

        _run(out, now=datetime(2026, 2, 4, 14, 0, 0))

        assert len(captured) == 1
        # the delta tar carries t2 only, and it is self-contained
        members = captured[0]["members"]
        assert "dot1/20260204/crops/t2_120500/frame_000000.jpg" in members
        assert "dot1/20260204/crops/t1_120100/frame_000000.jpg" not in members

    def test_delta_results_json_payload_filtered(self, tmp_path, mocker):
        out = tmp_path / "out"
        results_dir = _make_dot_bucket(out, "dot1", "20260204", [("t1", "120100")])
        mocker.patch.object(archive, "upload_file")  # seed run: no-op ship
        _run(out)
        _write_results(results_dir, [_track("t1", "120100"), _track("t2", "120500")])
        _add_track_media(results_dir, "t2", "120500")

        captured_payload = {}

        def _grab(api_url, api_key, local_path, s3_key):
            with tarfile.open(local_path) as tf:
                member = tf.extractfile("dot1/20260204/results.json")
                captured_payload["data"] = json.load(member)

        mocker.patch.object(archive, "upload_file", side_effect=_grab)
        _run(out, now=datetime(2026, 2, 4, 14, 0, 0))

        track_ids = [t["track_id"] for t in captured_payload["data"]["tracks"]]
        assert track_ids == ["t2"]

    def test_no_new_tracks_ships_nothing(self, tmp_path, mocker):
        out = tmp_path / "out"
        _make_dot_bucket(out, "dot1", "20260204", [("t1", "120100")])
        mocker.patch.object(archive, "upload_file")  # seed run: no-op ship
        _run(out)
        captured = _capture_uploads(mocker)

        _run(out, now=datetime(2026, 2, 4, 14, 0, 0))  # nothing changed

        assert captured == []

    def test_day_bucket_retained_after_archive(self, tmp_path, mocker):
        out = tmp_path / "out"
        results_dir = _make_dot_bucket(out, "dot1", "20260204", [("t1", "120100")])
        _capture_uploads(mocker)

        _run(out)

        # day-bucket still growing -> not deleted; archived state recorded
        assert results_dir.exists()
        assert (results_dir / ".archived").exists()

    def test_archived_state_excluded_from_tar(self, tmp_path, mocker):
        out = tmp_path / "out"
        _make_dot_bucket(out, "dot1", "20260204", [("t1", "120100")])
        captured = _capture_uploads(mocker)

        _run(out)

        assert not any(name.endswith(".archived") for name in captured[0]["members"])


# --------------------------------------------------------------------------- #
# self-containment invariant (the backend's hard requirement)
# --------------------------------------------------------------------------- #
class TestSelfContainment:
    def test_every_referenced_track_has_media_in_tar(self, tmp_path, mocker):
        out = tmp_path / "out"
        _make_dot_bucket(out, "dot1", "20260204", [("t1", "120100"), ("t2", "120500")])

        captured_members = {}

        def _grab(api_url, api_key, local_path, s3_key):
            with tarfile.open(local_path) as tf:
                names = set(tf.getnames())
                member = tf.extractfile("dot1/20260204/results.json")
                captured_members["names"] = names
                captured_members["tracks"] = json.load(member)["tracks"]

        mocker.patch.object(archive, "upload_file", side_effect=_grab)
        _run(out)

        # every track in the shipped results.json must have its crop dir present
        for track in captured_members["tracks"]:
            tid, ts = track["track_id"], track["timestamp"]
            crop = f"dot1/20260204/crops/{tid}_{ts}/frame_000000.jpg"
            assert crop in captured_members["names"], f"missing media for {tid}"

    def test_no_tar_when_device_has_nothing(self, tmp_path, mocker):
        out = tmp_path / "out"
        out.mkdir()
        captured = _capture_uploads(mocker)

        _run(out)

        assert captured == []

"""Fingerprint-aware enqueue, the 'done' tombstone, and prune_missing.

These power the results/logs migration: a retained file (DOT bucket, log) leaves
a tombstone so a re-scan of identical content is deduped, while changed content
(a grown DOT results.json) re-activates for upload.
"""
from pathlib import Path

from bugcam.pollen.store import PollenStore, UploadStatus


def _store(tmp_path: Path) -> PollenStore:
    return PollenStore(tmp_path / "pollen.db")


class TestFingerprintEnqueue:
    def test_same_fingerprint_is_deduped(self, tmp_path):
        store = _store(tmp_path)
        first = store.enqueue("/p", "result", "v1/a/results.json", fingerprint="fp1")
        again = store.enqueue("/p", "result", "v1/a/results.json", fingerprint="fp1")
        assert first is not None and again is None

    def test_changed_fingerprint_reactivates(self, tmp_path):
        store = _store(tmp_path)
        rid = store.enqueue("/p", "result", "v1/a/results.json", fingerprint="fp1")
        store.mark_uploaded(rid)
        store.mark_done(rid)  # tombstone

        again = store.enqueue("/p", "result", "v1/a/results.json", fingerprint="fp2")
        assert again == rid
        row = store.get(rid)
        assert row.status == UploadStatus.PENDING
        assert row.fingerprint == "fp2"

    def test_reactivation_resets_multipart_state(self, tmp_path):
        store = _store(tmp_path)
        rid = store.enqueue("/p", "archive", "v2/archives/d/x.tar", fingerprint="fp1")
        store.mark_uploading(rid, upload_id="UP-1")
        store.record_part(rid, 1, "e1")
        store.mark_uploaded(rid)
        store.mark_done(rid)

        store.enqueue("/p", "archive", "v2/archives/d/x.tar", fingerprint="fp2")
        row = store.get(rid)
        assert row.upload_id is None and row.parts == []


class TestTombstone:
    def test_done_excluded_from_claim_and_count(self, tmp_path):
        store = _store(tmp_path)
        rid = store.enqueue("/p", "log", "v1/a/logs/x.log", fingerprint="fp")
        store.mark_uploaded(rid)
        store.mark_done(rid)
        assert store.claim_pending() == []
        assert store.pending_count() == 0
        assert store.get(rid).status == UploadStatus.DONE


class TestPruneMissing:
    def test_prunes_tombstones_whose_file_is_gone(self, tmp_path):
        store = _store(tmp_path)
        present = tmp_path / "present.json"
        present.write_text("{}", encoding="utf-8")

        keep = store.enqueue(str(present), "result", "v1/keep/results.json", fingerprint="a")
        gone = store.enqueue("/does/not/exist.json", "result", "v1/gone/results.json", fingerprint="b")
        for rid in (keep, gone):
            store.mark_uploaded(rid)
            store.mark_done(rid)

        pruned = store.prune_missing()

        assert pruned == 1
        assert store.get(gone) is None      # file gone -> tombstone dropped
        assert store.get(keep) is not None  # file present -> tombstone kept

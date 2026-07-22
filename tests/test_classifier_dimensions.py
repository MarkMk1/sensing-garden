"""Model/labels dimension validation.

model.hef and labels.txt are two independently-versioned files that only
happen to sit in the same bundle directory -- nothing upstream (bundle
resolution, provenance hashing, publish verification) checks they're
actually compatible. Without a load-time check, a mismatch surfaces as an
IndexError deep inside _best_within, on whichever track happens to hit a
class near the tail of the list.
"""
from __future__ import annotations

import pytest

from bugcam.edge26.processing import classifier as clf


class _FakeOutputInfo:
    def __init__(self, n: int) -> None:
        self.shape = (1, n)  # code reads shape[-1]


class _FakeHef:
    def __init__(self, output_dims: list[int]) -> None:
        self._infos = [_FakeOutputInfo(n) for n in output_dims]

    def get_output_vstream_infos(self):
        return self._infos


def _classifier(tmp_path, species, output_dims, monkeypatch, taxonomy=None):
    labels_path = tmp_path / "labels.txt"
    labels_path.write_text("\n".join(species) + "\n", encoding="utf-8")
    instance = clf.HailoClassifier({"model": str(tmp_path / "model.hef"), "labels": str(labels_path)})
    instance._hef = _FakeHef(output_dims)
    if taxonomy is None:
        # One family/genus per species by default -- keeps single-head-model
        # tests focused on the species count alone.
        taxonomy = {
            1: [f"family_{i}" for i in range(len(species))],
            2: {f"genus_{i}": f"family_{i}" for i in range(len(species))},
            3: {s: f"genus_{i}" for i, s in enumerate(species)},
        }
    monkeypatch.setattr(clf, "get_taxonomy", lambda species_list, cache_path=None: taxonomy)
    return instance


class TestSpeciesDimension:
    def test_matching_count_loads_cleanly(self, tmp_path, monkeypatch):
        instance = _classifier(tmp_path, ["a", "b", "c"], output_dims=[3], monkeypatch=monkeypatch)
        instance._load_labels()
        assert instance.species_list == ["a", "b", "c"]

    def test_mismatch_raises_with_both_counts_in_message(self, tmp_path, monkeypatch):
        instance = _classifier(tmp_path, ["a", "b", "c", "d"], output_dims=[3], monkeypatch=monkeypatch)
        with pytest.raises(ValueError, match=r"species: 4 labels vs\. model outputs 3"):
            instance._load_labels()

    def test_more_model_classes_than_labels_also_raises(self, tmp_path, monkeypatch):
        instance = _classifier(tmp_path, ["a", "b"], output_dims=[5], monkeypatch=monkeypatch)
        with pytest.raises(ValueError, match=r"species: 2 labels vs\. model outputs 5"):
            instance._load_labels()


class TestThreeHeadModel:
    def test_matching_family_genus_species_loads_cleanly(self, tmp_path, monkeypatch):
        taxonomy = {1: ["F1", "F2"], 2: {"G1": "F1", "G2": "F2"}, 3: {"a": "G1", "b": "G2"}}
        instance = _classifier(tmp_path, ["a", "b"], output_dims=[2, 2, 2], monkeypatch=monkeypatch, taxonomy=taxonomy)
        instance._load_labels()
        assert instance.family_list == ["F1", "F2"]
        assert instance.genus_list == ["G1", "G2"]

    def test_genus_mismatch_raises_even_when_species_matches(self, tmp_path, monkeypatch):
        # Species count matches the model (2 == 2), but the taxonomy the
        # runtime resolved via GBIF only has 1 distinct genus where the model
        # expects 2 -- exactly the kind of mismatch a species-only check
        # would miss entirely.
        taxonomy = {1: ["F1"], 2: {"G1": "F1"}, 3: {"a": "G1", "b": "G1"}}
        instance = _classifier(tmp_path, ["a", "b"], output_dims=[2, 2, 2], monkeypatch=monkeypatch, taxonomy=taxonomy)
        with pytest.raises(ValueError, match=r"genus: 1 labels vs\. model outputs 2"):
            instance._load_labels()

    def test_error_reports_every_mismatched_head_not_just_the_first(self, tmp_path, monkeypatch):
        taxonomy = {1: ["F1"], 2: {"G1": "F1"}, 3: {"a": "G1", "b": "G1", "c": "G1"}}
        instance = _classifier(tmp_path, ["a", "b", "c"], output_dims=[2, 2, 2], monkeypatch=monkeypatch, taxonomy=taxonomy)
        with pytest.raises(ValueError) as exc_info:
            instance._load_labels()
        message = str(exc_info.value)
        assert "family: 1 labels vs. model outputs 2" in message
        assert "genus: 1 labels vs. model outputs 2" in message
        assert "species: 3 labels vs. model outputs 2" in message


class TestFallbackPathNeverMismatches:
    def test_labels_derived_straight_from_model_shape_always_matches(self, tmp_path, monkeypatch):
        """The fallback path (no labels.txt) builds placeholder labels from the
        model's own output shape, so it's self-consistent by construction --
        this just documents that _load_labels_fallback needs no validation."""
        instance = clf.HailoClassifier({"model": str(tmp_path / "model.hef")})  # no "labels" key
        instance._hef = _FakeHef([2, 2, 3])
        instance._load_labels()
        assert len(instance.family_list) == 2
        assert len(instance.genus_list) == 2
        assert len(instance.species_list) == 3

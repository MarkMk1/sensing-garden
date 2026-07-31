"""Tests for shared command-layer helpers in bugcam.commands.common."""
import pytest
import typer

from bugcam.commands.common import parse_resolution_option


def test_parse_resolution_option_valid() -> None:
    assert parse_resolution_option("1920x1080") == (1920, 1080)


def test_parse_resolution_option_invalid_raises_bad_parameter() -> None:
    with pytest.raises(typer.BadParameter):
        parse_resolution_option("not-a-resolution")


def test_record_and_run_share_resolution_parser() -> None:
    from bugcam.commands import record, run

    assert record.parse_resolution_option is parse_resolution_option
    assert run.parse_resolution_option is parse_resolution_option


def test_heartbeat_uses_shared_flick_id_resolution(tmp_path, monkeypatch) -> None:
    """The standalone heartbeat command resolves the device id via
    resolve_flick_id (CLI > config > default) like every other command."""
    from bugcam.commands import heartbeat

    monkeypatch.setattr(heartbeat, "resolve_flick_id", lambda value: value or "flick-default")
    written = {}

    def fake_write(*, output_dir, flick_id, input_dir, dot_ids, timezone_name=None):
        written["flick_id"] = flick_id
        written["dot_ids"] = dot_ids
        path = tmp_path / "hb.json"
        path.write_text("{}", encoding="utf-8")
        return path

    monkeypatch.setattr(heartbeat, "write_heartbeat_snapshot", fake_write)
    monkeypatch.setattr(heartbeat, "load_config", lambda: {})

    heartbeat.heartbeat(flick_id=None, dot_ids="d1,d2", input_dir=tmp_path, output_dir=tmp_path)

    assert written["flick_id"] == "flick-default"
    assert written["dot_ids"] == ["d1", "d2"]

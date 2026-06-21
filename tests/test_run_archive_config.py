"""Archive settings resolve from the config file, with CLI taking precedence.

Mirrors how api_url/flick_id/dot_ids resolve: an explicit CLI flag wins;
otherwise the value comes from ~/.config/bugcam/config.json; otherwise default.
"""
from bugcam.commands import run


class TestResolveArchiveSettings:
    def test_config_used_when_cli_unset(self, mocker):
        mocker.patch.object(run, "load_config", return_value={"archive": True, "archive_interval": 1800})
        assert run._resolve_archive_settings(None, None) == (True, 1800)

    def test_cli_true_overrides_config_false(self, mocker):
        mocker.patch.object(run, "load_config", return_value={"archive": False})
        assert run._resolve_archive_settings(True, None) == (True, 3600)

    def test_cli_false_overrides_config_true(self, mocker):
        mocker.patch.object(run, "load_config", return_value={"archive": True})
        assert run._resolve_archive_settings(False, None) == (False, 3600)

    def test_default_off_when_neither(self, mocker):
        mocker.patch.object(run, "load_config", return_value={})
        assert run._resolve_archive_settings(None, None) == (False, 3600)

    def test_cli_interval_overrides_config(self, mocker):
        mocker.patch.object(run, "load_config", return_value={"archive_interval": 1800})
        assert run._resolve_archive_settings(True, 900) == (True, 900)

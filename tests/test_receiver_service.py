"""Tests for the shared receiver startup logic in bugcam.receiver.service."""
import threading
from unittest.mock import MagicMock, patch

from bugcam.receiver import service


def test_finalization_loop_checks_pending_until_stopped() -> None:
    tracker = MagicMock()
    tracker.CHECK_INTERVAL = 0.01
    stop_event = threading.Event()

    calls = {"count": 0}

    def check_pending() -> None:
        calls["count"] += 1
        if calls["count"] >= 3:
            stop_event.set()

    tracker.check_pending.side_effect = check_pending
    with patch.object(service.PendingTrackTracker, "CHECK_INTERVAL", 0.01):
        service.finalization_loop(tracker, stop_event)
    assert calls["count"] >= 3


def test_finalization_loop_survives_check_errors() -> None:
    tracker = MagicMock()
    stop_event = threading.Event()

    calls = {"count": 0}

    def check_pending() -> None:
        calls["count"] += 1
        if calls["count"] >= 2:
            stop_event.set()
        raise RuntimeError("boom")

    tracker.check_pending.side_effect = check_pending
    with patch.object(service.PendingTrackTracker, "CHECK_INTERVAL", 0.01):
        service.finalization_loop(tracker, stop_event)
    assert calls["count"] >= 2


def test_run_receiver_recovers_tracks_and_runs_app() -> None:
    tracker = MagicMock()
    flask_app = MagicMock()
    flask_app.config = {"TRACKER": tracker}

    with patch.object(service, "create_app", return_value=flask_app) as mock_create:
        service.run_receiver("127.0.0.1", 5555)

    mock_create.assert_called_once_with(config={"host": "127.0.0.1", "port": 5555})
    tracker.recover_orphaned_tracks.assert_called_once()
    flask_app.run.assert_called_once_with(
        host="127.0.0.1", port=5555, threaded=True, debug=False
    )


def test_run_receiver_without_tracker_still_runs_app() -> None:
    flask_app = MagicMock()
    flask_app.config = {}

    with patch.object(service, "create_app", return_value=flask_app):
        service.run_receiver("0.0.0.0", 8080, debug=True)

    flask_app.run.assert_called_once_with(
        host="0.0.0.0", port=8080, threaded=True, debug=True
    )


def test_commands_delegate_to_shared_service() -> None:
    """Both the standalone receive command and bugcam run use the one service."""
    from bugcam.commands import receive, run

    assert receive.run_receiver is service.run_receiver
    assert run.run_receiver is service.run_receiver

"""Shared DOT receiver startup: app creation, orphan recovery, finalization."""
from __future__ import annotations

import logging
import threading

from bugcam.receiver import create_app
from bugcam.receiver.tracker import PendingTrackTracker

logger = logging.getLogger(__name__)


def finalization_loop(tracker: PendingTrackTracker, stop_event: threading.Event) -> None:
    """Background loop that checks for idle tracks to finalize."""
    while not stop_event.is_set():
        try:
            tracker.check_pending()
        except Exception as e:
            logger.error(f"Finalization loop error: {e}")
        stop_event.wait(PendingTrackTracker.CHECK_INTERVAL)


def run_receiver(host: str, port: int, *, debug: bool = False) -> None:
    """Run the Flask receiver server; blocks until the server exits.

    Recovers orphaned tracks and runs the finalization loop in a daemon
    thread alongside the server.
    """
    flask_app = create_app(config={"host": host, "port": port})
    tracker = flask_app.config.get("TRACKER")

    finalization_thread = None
    finalization_stop = threading.Event()
    if tracker:
        logger.info("Scanning for orphaned tracks...")
        tracker.recover_orphaned_tracks()

        finalization_thread = threading.Thread(
            target=finalization_loop,
            args=(tracker, finalization_stop),
            daemon=True,
        )
        finalization_thread.start()
        logger.info("Track finalization thread started")

    logger.info(f"Receiver starting on {host}:{port}")
    flask_app.run(host=host, port=port, threaded=True, debug=debug)

    if finalization_thread:
        finalization_stop.set()
        finalization_thread.join(timeout=5)

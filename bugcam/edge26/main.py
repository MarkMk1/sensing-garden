import json
import logging
import multiprocessing as mp
import queue
import sys
import threading
import time
from pathlib import Path
from datetime import datetime, timezone

import cv2

from bugcam.edge26.capture import VideoRecorder
from bugcam.edge26.detector import Detector
from bugcam.edge26.processing import VideoProcessor, HailoClassifier
from bugcam.edge26.output import ResultsWriter
from bugcam.edge26.queue import ClassificationQueue, QueueEntry
from bugcam.log_shipping import DailyLogHandler, ship_existing_logs


def setup_logging(log_dir: Path, *, on_log_complete=None) -> None:
    """Configure logging to console and a daily-rotating file.

    When ``on_log_complete`` is given, the log mechanism owns shipping: a completed
    (rolled-over) file is pushed to it, and any non-today logs left by a prior run are
    shipped now. The upload subsystem never scans for logs. When it is ``None``
    (uploads disabled), nothing ships logs -- they accumulate on disk locally."""
    log_dir.mkdir(parents=True, exist_ok=True)

    #TODO I think this is handling logging for the application broadly,
    # so it should be declared outside of edge26
    file_handler = DailyLogHandler(log_dir)
    file_handler.on_complete = on_log_complete

    # Format
    fmt = "%(asctime)s | %(levelname)-8s | %(message)s"
    datefmt = "%H:%M:%S"

    # Root logger
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        datefmt=datefmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            file_handler,
        ]
    )

    if on_log_complete is not None:
        ship_existing_logs(on_log_complete, log_dir)

    # Reduce noise from libraries
    logging.getLogger("PIL").setLevel(logging.WARNING)
    logging.getLogger("hailo_platform").setLevel(logging.WARNING)


logger = logging.getLogger("edge26")


class Pipeline:
    """
    Main pipeline orchestrating capture and processing.

    Architecture:
        - Detector child: Runs BugSpot detection/tracking (maintains tracker
          state) in a thread, or standalone in a dedicated subprocess
        - Classification thread: Runs Hailo classification (shared resource)
        - Classification queue: Disk-based FIFO queue for both FLIK and DOT tracks
    """

    def __init__(
        self,
        config: dict,
        *,
        on_result_ready=None,
        on_video_ready=None,
    ):
        self.config = config
        self._on_result_ready = on_result_ready
        self._on_video_ready = on_video_ready

        # --- Pipeline mode (resolved early; queue/event types depend on it) ---
        pipeline_config = config.get("pipeline", {})
        self.enable_recording = pipeline_config.get("enable_recording", True)
        self.enable_processing = pipeline_config.get("enable_processing", True)
        self.enable_classification = pipeline_config.get("enable_classification", True)
        self.continuous_tracking = pipeline_config.get("continuous_tracking", False)

        # Run the GIL-heavy detection loop in its own subprocess so it cannot
        # starve the recorder threads (dropped-frame fix). The subprocess runs
        # a standalone Detector; the parent keeps recording + classification.
        self.detection_in_subprocess = pipeline_config.get("detection_in_subprocess", False)

        # Coordination primitives. In subprocess mode the recorder (parent) and
        # detection loop (child) live in different processes, so the queue and
        # events must be multiprocessing-backed and shared between them.
        if self.detection_in_subprocess:
            self._mp_ctx = mp.get_context("spawn")
            self.video_queue = self._mp_ctx.JoinableQueue()
            self.stop_event = self._mp_ctx.Event()
            self.recording_stopped = self._mp_ctx.Event()
        else:
            self._mp_ctx = None
            self.video_queue = queue.Queue()
            self.stop_event = threading.Event()
            self.recording_stopped = threading.Event()

        self.recorder_thread = None
        self.detection_thread = None
        self.detection_process = None
        self.classification_thread = None

        # Device config
        device_config = config.get("device", {})
        self.flick_id = device_config.get("flick_id", "edge26")
        self.dot_ids = device_config.get("dot_ids", [])
        self.input_storage = Path(config["paths"]["input_storage"])

        # Output paths
        self.results_dir = Path(config["output"]["results_dir"])

        # Pending queue for classification
        pending_dir = Path(config["paths"].get("pending_dir",
                         Path(config["paths"]["input_storage"]).parent / "pending"))
        self.classification_queue = ClassificationQueue(pending_dir)

        # Initialize components based on mode
        self.recorder = self._init_recorder() if self.enable_recording else None
        self.processor = VideoProcessor(config) if self.enable_processing else None
        self.writer = ResultsWriter(config["output"]) if self.enable_processing else None

        # Detector child. In-process it shares this pipeline's processor,
        # writer and classification queue; in subprocess mode the detection
        # subprocess builds its own Detector instead (see
        # _detection_subprocess_entry), so there is none to hold here.
        if self.enable_processing and not self.detection_in_subprocess:
            self.detector = Detector(
                config,
                video_queue=self.video_queue,
                stop_event=self.stop_event,
                recording_stopped=self.recording_stopped,
                processor=self.processor,
                writer=self.writer,
                classification_queue=self.classification_queue,
                on_result_ready=self._notify_result_ready,
            )
        else:
            self.detector = None

        # Eagerly initialize classifier for the classification thread
        if self.enable_classification and self.processor:
            self.processor._classifier = HailoClassifier(self.processor.classification_config)
            logger.info("Hailo classifier initialized")

        logger.info("=" * 60)
        logger.info("EDGE26 PIPELINE INITIALIZED")
        logger.info("=" * 60)

        # Mode info
        mode = "RECORD + PROCESS" if (self.enable_recording and self.enable_processing) else \
               "RECORD ONLY" if self.enable_recording else \
               "PROCESS ONLY" if self.enable_processing else "NONE"
        logger.info(f"Mode:          {mode}")
        logger.info(f"Device:        {self.flick_id}")
        logger.info(f"Input storage: {config['paths']['input_storage']}")
        logger.info(f"Pending dir:   {pending_dir}")
        if self.enable_processing:
            logger.info(f"Results dir:   {config['output']['results_dir']}")
            classify = pipeline_config.get("enable_classification", True)
            logger.info(f"Classification: {'enabled' if classify else 'disabled (detection only)'}")
            cont_track = pipeline_config.get("continuous_tracking", True)
            logger.info(f"Tracking:      {'continuous (across videos)' if cont_track else 'per-video (reset each)'}")
        if self.dot_ids:
            logger.info(f"DOT devices:   {', '.join(self.dot_ids)}")
        if self.enable_recording:
            rec_mode = pipeline_config.get("recording_mode", "continuous")
            logger.info(f"Chunk duration: {config['capture']['chunk_duration_seconds']}s")
            logger.info(f"Recording mode: {rec_mode}"
                       + (f" (every {pipeline_config.get('recording_interval_minutes', 5)} min)"
                          if rec_mode == "interval" else ""))

    def _init_recorder(self) -> VideoRecorder:
        """Initialize video recorder from config."""
        paths = self.config["paths"]
        capture = self.config["capture"]
        pipeline_cfg = self.config.get("pipeline", {})

        return VideoRecorder(
            output_dir=paths["input_storage"],
            fps=capture["fps"],
            chunk_duration=capture["chunk_duration_seconds"],
            resolution=tuple(capture.get("resolution", [1080, 1080])),
            device_id=self.flick_id,
            video_queue=self.video_queue,
            camera_index=capture["camera_index"],
            use_picamera=capture["use_picamera"],
            recording_mode=pipeline_cfg.get("recording_mode", "continuous"),
            interval_minutes=pipeline_cfg.get("recording_interval_minutes", 5),
            bitrate=capture.get("bitrate", 20_000_000),
        )

    def _notify_result_ready(self, output_dir: Path) -> None:
        """Tell the upload owner (Pollen) a result dir is finalized, if wired."""
        if self._on_result_ready is not None:
            try:
                self._on_result_ready(output_dir)
            except Exception:
                logger.error("result-ready callback failed", exc_info=True)

    def _notify_video_ready(self, video_path: Path, device: str) -> bool:
        """Tell the upload owner a DOT video is ready. DOT videos are not tied to a
        track, so they ship as their own unit keyed under the device/day. Returns True
        if an upload owner took it (staged it), so the producer can drop its copy."""
        if self._on_video_ready is None:
            return False
        try:
            self._on_video_ready(video_path, device)
            return True
        except Exception:
            logger.error("video-ready callback failed", exc_info=True)
            return False

    def _publish_dot_video(self, entry: QueueEntry) -> None:
        """Ship a queued DOT video to the upload owner, then drop the local copy. Runs in
        the main-process classification worker (which holds Pollen); detection only
        enqueues the task. Raises on failure so the queue retries rather than lose it."""
        video_path = Path(entry.track_dir)
        if not video_path.exists():
            return  # already shipped/cleaned: idempotent
        if not self._notify_video_ready(video_path, entry.source_device):
            raise RuntimeError(f"video enqueue failed (no upload owner?): {video_path.name}")
        video_path.unlink()
        logger.info("DOT video staged for upload: %s (dropped local copy)", video_path.name)

    @staticmethod
    def _deduplicate_track_id(track_id: str, results: dict) -> str:
        """If track_id already exists in results, append a suffix to make it unique."""
        existing_ids = {t.get("track_id") for t in results.get("tracks", [])}
        if track_id not in existing_ids:
            return track_id
        n = 1
        while f"{track_id}_{n}" in existing_ids:
            n += 1
        deduped = f"{track_id}_{n}"
        logger.warning(f"Track {track_id} already in results, saving as {deduped}")
        return deduped

    def _load_existing_results(self, results_path: Path) -> dict:
        """Load existing results.json for incremental updates, or create a fresh structure."""
        if results_path.exists():
            try:
                with open(results_path) as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.warning(f"Corrupt results.json, starting fresh: {results_path}")
            except Exception as e:
                logger.error(f"Cannot read results.json ({e}), starting fresh: {results_path}")
        return {
            "source_device": None,
            "processing_timestamp": None,
            "summary": {
                "total_detections": 0,
                "total_tracks": 0,
                "confirmed_tracks": 0,
                "unconfirmed_tracks": 0,
            },
            "tracks": [],
        }

    def _save_last_recording_marker(self) -> None:
        """Write the .last_recording marker when recording stops.

        An in-process Detector picks up the boundary immediately; a detection
        subprocess reads the marker file at its next startup."""
        if not (self.continuous_tracking and self.recorder
                and self.recorder.last_chunk_path):
            return

        filename = self.recorder.last_chunk_path.name
        (self.input_storage / ".last_recording").write_text(filename)
        if self.detector is not None:
            self.detector.mark_reset_after(filename)
        logger.info(f"Marked last recording: {filename}")

    # ------------------------------------------------------------------
    # Classification Thread - Runs Hailo classification
    # ------------------------------------------------------------------

    def _classification_worker(self) -> None:
        """
        Worker that processes classification queue (FIFO).

        Classifies tracks from both FLIK and DOT sources.
        """
        logger.info("Classification worker started")

        # Recover any pending from crash
        self.classification_queue.recover()

        while not self.stop_event.is_set():
            result = self.classification_queue.get_next()

            if result is None:
                time.sleep(0.5)
                continue

            filepath, entry = result

            try:
                if entry.entry_type == "flik":
                    self._classify_flik_track(entry)
                elif entry.entry_type == "video":
                    self._publish_dot_video(entry)
                else:
                    self._classify_dot_track(entry)

                self.classification_queue.remove(filepath)

            except Exception as e:
                logger.error(f"Classification failed for {filepath.name}: {e}", exc_info=True)
                should_retry = self.classification_queue.mark_failed(filepath, entry, str(e))
                if should_retry:
                    time.sleep(1.0)
                else:
                    # Permanently failed — still count as completed for .done check
                    self._check_classification_complete(Path(entry.output_dir))

        logger.info("Classification worker stopped")

    def _classify_flik_track(self, entry: QueueEntry) -> None:
        """Classify a FLIK track from queue entry."""
        track_dir = Path(entry.track_dir)
        output_dir = Path(entry.output_dir)

        if not track_dir.exists():
            logger.warning(f"Track directory not found: {track_dir}")
            self._check_classification_complete(output_dir)
            return

        logger.info(f"CLASSIFY FLIK: {entry.track_id} ({entry.num_crops} crops)")

        # Load crops
        crop_files = sorted(track_dir.glob("frame_*.jpg"))
        if not crop_files:
            logger.warning(f"No crops found in {track_dir}")
            self._check_classification_complete(output_dir)
            return

        # Ensure classifier is initialized
        if self.processor._classifier is None:
            self.processor._classifier = HailoClassifier(self.processor.classification_config)

        # Classify
        classifications = []
        frames = []

        for crop_path in crop_files:
            crop = cv2.imread(str(crop_path))
            if crop is None:
                continue

            frame_num = int(crop_path.stem.split("_")[1])
            classification = self.processor._classifier.classify(crop)
            classifications.append(classification)

            frames.append({
                "frame_number": frame_num,
                "prediction": {
                    "family": classification.family,
                    "genus": classification.genus,
                    "species": classification.species,
                    "family_confidence": classification.family_confidence,
                    "genus_confidence": classification.genus_confidence,
                    "species_confidence": classification.species_confidence,
                }
            })

        if not classifications:
            self._check_classification_complete(output_dir)
            return

        # Hierarchical aggregation
        final_pred = self.processor._classifier.hierarchical_aggregate(classifications)
        if not final_pred:
            self._check_classification_complete(output_dir)
            return

        logger.info(f"  {final_pred['family']} / {final_pred['genus']} / {final_pred['species']} "
                   f"({final_pred['species_confidence']:.1%})")

        # Load existing results
        results_path = output_dir / "results.json"
        results = self._load_existing_results(results_path)

        # Load detection metadata to enrich results
        detection_meta = self._load_detection_meta(output_dir)

        # Deduplicate track_id if this is a retry after crash
        track_id = self._deduplicate_track_id(entry.track_id, results)

        # Enrich results with detection metadata (first track writes top-level fields)
        if detection_meta and not results.get("video_file"):
            results["video_file"] = detection_meta.get("video_file")
            results["video_timestamp"] = detection_meta.get("video_timestamp")
            results["model_id"] = detection_meta.get("model_id")
            if detection_meta.get("video_info"):
                results["video_info"] = detection_meta["video_info"]
            results["date"] = detection_meta.get("date", entry.date)
        results["source_device"] = entry.source_device
        results["processing_timestamp"] = datetime.now(timezone.utc).isoformat()

        # Build per-track frame data, enriched with detection metadata
        track_frames = frames
        track_meta = detection_meta.get("tracks", {}).get(entry.track_id, {}) if detection_meta else {}
        frame_dets = detection_meta.get("frame_detections", {}).get(entry.track_id, []) if detection_meta else []

        if detection_meta and not track_meta and not frame_dets:
            logger.warning(f"Track {entry.track_id} not found in detection metadata, enrichment skipped")

        if frame_dets or track_meta:
            frame_det_map = {fd["frame_number"]: fd for fd in frame_dets if fd.get("frame_number") is not None}
            enriched_frames = []
            for f in frames:
                fd = frame_det_map.get(f.get("frame_number"))
                enriched = dict(f)
                if fd:
                    if fd.get("timestamp_seconds") is not None:
                        enriched["timestamp_seconds"] = fd["timestamp_seconds"]
                    if fd.get("bbox") is not None:
                        enriched["bbox"] = fd["bbox"]
                enriched_frames.append(enriched)
            track_frames = enriched_frames

        # Update results
        track_result = {
            "track_id": track_id,
            "timestamp": entry.time,
            "final_prediction": final_pred,
            "num_detections": len(track_frames),
            "frames": track_frames,
        }
        if track_meta.get("first_seen_seconds") is not None:
            track_result["first_seen_seconds"] = track_meta["first_seen_seconds"]
        if track_meta.get("last_seen_seconds") is not None:
            track_result["last_seen_seconds"] = track_meta["last_seen_seconds"]
        if track_meta.get("duration_seconds") is not None:
            track_result["duration_seconds"] = track_meta["duration_seconds"]
        if track_meta.get("topology_metrics") is not None:
            track_result["topology_metrics"] = track_meta["topology_metrics"]

        results["tracks"].append(track_result)

        # Update summary: total counts from detection metadata, confirmed from actual classified tracks
        if detection_meta and "summary" in detection_meta:
            results["summary"]["total_detections"] = detection_meta["summary"].get("total_detections", 0)
            results["summary"]["total_tracks"] = detection_meta["summary"].get("total_tracks", 0)
            results["summary"]["unconfirmed_tracks"] = detection_meta["summary"].get("unconfirmed_tracks", 0)
        else:
            results["summary"]["total_detections"] = sum(t.get("num_detections", 0) for t in results["tracks"])
            results["summary"]["total_tracks"] = len(results["tracks"])
        results["summary"]["confirmed_tracks"] = len(results["tracks"])

        # Write results
        self.writer.write_results(results=results, output_dir=output_dir)

        # Check if all tracks for this output directory are done
        self._check_classification_complete(output_dir)

    def _classify_dot_track(self, entry: QueueEntry) -> None:
        """Classify a DOT track from queue entry."""
        track_dir = Path(entry.track_dir)
        output_dir = Path(entry.output_dir)

        if not track_dir.exists():
            logger.warning(f"Track directory not found: {track_dir}")
            self._check_classification_complete(output_dir)
            return

        logger.info(f"CLASSIFY DOT: {entry.track_id} ({entry.num_crops} crops)")

        # Classify using existing method
        track_result = self.processor.classify_dot_track(
            track_dir, entry.track_id, entry.time
        )

        if not track_result:
            self._check_classification_complete(output_dir)
            return

        final = track_result.get("final_prediction", {})
        logger.info(f"  {final.get('family', 'N/A')} / {final.get('genus', 'N/A')} / "
                   f"{final.get('species', 'N/A')} ({final.get('species_confidence', 0):.1%})")

        # Create composite if background available
        if entry.background_path:
            background_path = Path(entry.background_path)
            labels_path = Path(entry.labels_path) if entry.labels_path else None
            composite_dir = output_dir / "composites"
            composite_dir.mkdir(parents=True, exist_ok=True)

            track_dir_name = f"{entry.track_id}_{entry.time}" if entry.time else entry.track_id
            composite_path = composite_dir / f"{track_dir_name}.jpg"

            try:
                if labels_path and labels_path.exists():
                    self.processor.create_dot_composite(
                        track_dir, background_path, labels_path, composite_path
                    )
                    logger.debug("  Composite saved")
            except Exception as e:
                logger.warning(f"  Could not create composite: {e}")

        # Load existing results
        results_path = output_dir / "results.json"
        results = self._load_existing_results(results_path)

        # Deduplicate track_id if this is a retry after crash
        track_id = self._deduplicate_track_id(track_result["track_id"], results)
        track_result["track_id"] = track_id

        # Update results
        results["tracks"].append(track_result)
        results["source_device"] = entry.source_device
        results["date"] = entry.date
        results["processing_timestamp"] = datetime.now(timezone.utc).isoformat()

        # Update summary
        results["summary"]["total_tracks"] = len(results["tracks"])
        results["summary"]["confirmed_tracks"] = len(results["tracks"])
        results["summary"]["total_detections"] = sum(t.get("num_detections", 0) for t in results["tracks"])

        # Write results
        self.writer.write_results(results=results, output_dir=output_dir)

        # Check if all tracks for this output directory are done
        self._check_classification_complete(output_dir)

    @staticmethod
    def _load_detection_meta(output_dir: Path) -> dict:
        """Load detection metadata sidecar if available."""
        meta_path = output_dir / ".detection.json"
        if meta_path.exists():
            try:
                return json.loads(meta_path.read_text())
            except (json.JSONDecodeError, OSError) as e:
                logger.warning(f"Could not read detection metadata: {e}")
        return {}

    def _check_classification_complete(self, output_dir: Path) -> None:
        """
        Increment completed count and check if all tracks for this dir are done.

        When detection enqueues tracks, it writes .expected_tracks with the count.
        Each call to this method increments .completed_tracks. When
        completed >= expected, writes .done to signal the upload thread.
        Also called on graceful failures (missing crops, empty dirs) and
        permanent queue failures to ensure .done is always written eventually.
        """
        expected_path = output_dir / ".expected_tracks"
        if not expected_path.exists():
            return

        try:
            expected = int(expected_path.read_text().strip())
        except (ValueError, OSError):
            return

        # Atomically increment completed count
        completed_path = output_dir / ".completed_tracks"
        try:
            completed = int(completed_path.read_text().strip()) + 1
        except (ValueError, OSError):
            completed = 1
        completed_path.write_text(str(completed))

        if completed >= expected:
            done_path = output_dir / ".done"
            done_path.write_text(f"classified={completed}\nexpected={expected}\n")
            logger.info(f"Classification complete: {completed}/{expected} tracks in {output_dir.name}")
            expected_path.unlink(missing_ok=True)
            completed_path.unlink(missing_ok=True)
            detection_meta_path = output_dir / ".detection.json"
            detection_meta_path.unlink(missing_ok=True)
            self._notify_result_ready(output_dir)

    # ------------------------------------------------------------------
    # Pipeline Control
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the pipeline."""
        logger.info("=" * 60)
        logger.info("STARTING PIPELINE")
        logger.info("=" * 60)

        # Start recorder (if enabled)
        if self.enable_recording and self.recorder:
            self.recorder_thread = threading.Thread(
                target=self.recorder.start,
                daemon=True,
                name="Recorder"
            )
            self.recorder_thread.start()
            logger.info("Recorder thread started")
        else:
            self.recording_stopped.set()  # No recording

        # Start the detection worker — the Detector child runs in an
        # in-process thread, or standalone in a dedicated subprocess
        # (separate interpreter/GIL) when detection_in_subprocess is
        # enabled, so detection can't starve the recorder threads.
        if self.enable_processing and self.processor:
            if self.detection_in_subprocess:
                self.detection_process = self._mp_ctx.Process(
                    target=_detection_subprocess_entry,
                    args=(self.config, self.video_queue,
                          self.stop_event, self.recording_stopped),
                    name="DetectionProcess",
                    daemon=False,
                )
                self.detection_process.start()
                logger.info(f"Detection subprocess started (pid={self.detection_process.pid})")
            else:
                self.detection_thread = threading.Thread(
                    target=self.detector.run,
                    daemon=False,
                    name="Detection"
                )
                self.detection_thread.start()
                logger.info("Detection thread started")

            # Start classification worker
            if self.enable_classification:
                self.classification_thread = threading.Thread(
                    target=self._classification_worker,
                    daemon=False,
                    name="Classification"
                )
                self.classification_thread.start()
                logger.info("Classification thread started")

        if self.enable_recording and self.enable_processing:
            logger.info("Pipeline running - Ctrl+C to stop recording (processing continues)")
        elif self.enable_recording:
            logger.info("Recording - Ctrl+C to stop")
        else:
            logger.info("Processing existing videos...")

    def stop_recording(self) -> None:
        """Stop recording only, processing continues."""
        if not self.recording_stopped.is_set():
            logger.info("=" * 60)
            logger.info("STOPPING RECORDING")
            logger.info("=" * 60)

            if self.recorder:
                self.recorder.stop()
            if self.recorder_thread:
                self.recorder_thread.join(timeout=10.0)

            # Mark the last recorded video so tracker resets after it
            self._save_last_recording_marker()

            self.recording_stopped.set()
            logger.info("Recording stopped - processing remaining videos...")

            remaining = self.video_queue.qsize()
            if remaining > 0:
                logger.info(f"Videos in queue: {remaining}")

            pending = self.classification_queue.count()
            if pending > 0:
                logger.info(f"Pending classifications: {pending}")

    def stop(self) -> None:
        """Stop the pipeline gracefully."""
        logger.info("=" * 60)
        logger.info("STOPPING PIPELINE")
        logger.info("=" * 60)

        # Stop recorder first
        self.stop_recording()

        # Stop threads / detection subprocess
        self.stop_event.set()

        if self.detection_process:
            self.detection_process.join(timeout=30.0)
            if self.detection_process.is_alive():
                logger.warning("Detection subprocess did not exit in time; terminating")
                self.detection_process.terminate()
                self.detection_process.join(timeout=5.0)
            logger.info("Detection subprocess stopped")

        if self.detection_thread:
            self.detection_thread.join(timeout=30.0)
            logger.info("Detection thread stopped")

        if self.classification_thread:
            self.classification_thread.join(timeout=30.0)
            logger.info("Classification thread stopped")

        logger.info("Pipeline stopped cleanly")

    def wait(self) -> None:
        """Wait for pipeline (blocks until stopped)."""
        # Wait for recorder to finish (if running)
        if self.recorder_thread:
            self.recorder_thread.join()

        # Wait for detection subprocess to finish (if running)
        if self.detection_process:
            self.detection_process.join()

        # Wait for detection thread to finish (if running)
        if self.detection_thread:
            self.detection_thread.join()

        # Wait for classification thread to finish (if running)
        if self.classification_thread:
            self.classification_thread.join()


def _detection_subprocess_entry(config, video_queue, stop_event, recording_stopped):
    """Spawned-subprocess entrypoint for the detection loop.

    Runs in its own interpreter (own GIL) so the GIL-heavy detection work cannot
    starve the recorder threads in the parent process. Builds a standalone
    ``Detector`` that shares the recorder's video queue and stop/recording
    events, then runs its worker loop. All outputs (crops, composites, and the
    disk-based classification queue) are written to disk exactly as in the
    in-process path, so detection results are unchanged.
    """
    try:
        setup_logging(Path(config["paths"]["logs_dir"]))
    except Exception:
        logging.basicConfig(level=logging.INFO)
    logger.info("Detection subprocess starting")
    detector = Detector(
        config,
        video_queue=video_queue,
        stop_event=stop_event,
        recording_stopped=recording_stopped,
    )
    try:
        detector.run()
    finally:
        logger.info("Detection subprocess exiting")

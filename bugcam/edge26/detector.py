import json
import logging
import queue
import shutil
from datetime import datetime, timezone
from pathlib import Path

from bugcam.edge26.processing import VideoProcessor
from bugcam.edge26.output import ResultsWriter
from bugcam.edge26.queue import ClassificationQueue

logger = logging.getLogger("edge26")


class Detector:
    """
    Detection/tracking stage of the pipeline.

    Consumes recorded videos from the video queue, runs BugSpot
    detection/tracking (maintaining tracker state), polls for DOT
    directories, and enqueues tracks on the disk-based classification
    queue for the Pipeline's classification worker.

    Owned by ``Pipeline`` as a child when detection runs in-process, or
    constructed standalone inside the detection subprocess. The processor,
    writer and classification queue can be injected (to share the
    Pipeline's instances in-process) or are built from config when run
    standalone.
    """

    def __init__(
        self,
        config: dict,
        *,
        video_queue,
        stop_event,
        recording_stopped,
        processor=None,
        writer=None,
        classification_queue=None,
        on_result_ready=None,
    ):
        self.config = config
        self.video_queue = video_queue
        self.stop_event = stop_event
        self.recording_stopped = recording_stopped
        self._on_result_ready = on_result_ready

        pipeline_config = config.get("pipeline", {})
        self.continuous_tracking = pipeline_config.get("continuous_tracking", False)

        # Device config
        device_config = config.get("device", {})
        self.flick_id = device_config.get("flick_id", "edge26")
        self.dot_ids = device_config.get("dot_ids", [])
        self.input_storage = Path(config["paths"]["input_storage"])

        # Output paths
        self.results_dir = Path(config["output"]["results_dir"])

        # Pending queue for classification (shared with the Pipeline's
        # classification worker; disk-based, so a separate instance over the
        # same directory is equivalent when running in a subprocess)
        if classification_queue is None:
            pending_dir = Path(config["paths"].get("pending_dir",
                             Path(config["paths"]["input_storage"]).parent / "pending"))
            classification_queue = ClassificationQueue(pending_dir)
        self.classification_queue = classification_queue

        self.processor = processor if processor is not None else VideoProcessor(config)
        self.writer = writer if writer is not None else ResultsWriter(config["output"])

        # --- Video sampling (save 1 video per N to output) ---
        self._video_batch_count = 0
        self._video_sample_saved = False
        self._video_sample_interval = pipeline_config.get("video_sample_interval", 10)

        # --- Tracker reset signals (continuous_tracking mode) ---
        self._sweep_counter = 0
        self._sweep_interval = 30
        # 1. Day-change: reset when the date in the filename changes
        self._last_video_date: str = ""
        # 2. Recording-stop: reset after the last recorded video is processed
        #    Persisted via .last_recording marker file so it survives restarts.
        self._reset_after_video: str = ""
        self._pending_tracker_reset = False
        if self.continuous_tracking:
            self._load_last_recording_marker()

    def _notify_result_ready(self, output_dir: Path) -> None:
        """Tell the upload owner (Pollen) a result dir is finalized, if wired."""
        if self._on_result_ready is not None:
            try:
                self._on_result_ready(output_dir)
            except Exception:
                logger.error("result-ready callback failed", exc_info=True)

    def _is_flick_video(self, path: Path) -> bool:
        """Check if a path is a FLICK video (matches flick_id prefix)."""
        return (path.is_file()
                and path.suffix == ".mp4"
                and path.name.startswith(f"{self.flick_id}_"))

    def _is_dot_directory(self, path: Path) -> bool:
        """Check if a path is a DOT device directory (matches a dot_id prefix)."""
        if not path.is_dir():
            return False
        return any(path.name.startswith(f"{dot_id}_") for dot_id in self.dot_ids)

    def _find_existing_items(self) -> list:
        """
        Find existing videos and DOT directories in input_storage.

        Returns a sorted list of (path, type) tuples where type is
        "video" or "dot". Only items matching configured device IDs
        are included. Sorted by name gives chronological order since
        filenames and directory names both contain timestamps.
        """
        if not self.input_storage.exists():
            return []

        items = []
        for entry in sorted(self.input_storage.iterdir()):
            if self._is_flick_video(entry):
                items.append((entry, "video"))
            elif self.dot_ids and self._is_dot_directory(entry):
                items.append((entry, "dot"))

        if items:
            n_videos = sum(1 for _, t in items if t == "video")
            n_dots = sum(1 for _, t in items if t == "dot")
            parts = []
            if n_videos:
                parts.append(f"{n_videos} video(s)")
            if n_dots:
                parts.append(f"{n_dots} DOT dir(s)")
            logger.info(f"Found {', '.join(parts)} to process")

        return items

    def _find_dot_directories(self) -> list:
        """Find unprocessed DOT directories in input_storage."""
        if not self.input_storage.exists() or not self.dot_ids:
            return []

        return [d for d in sorted(self.input_storage.iterdir())
                if self._is_dot_directory(d)]

    def _parse_dot_dir_name(self, dir_name: str):
        """
        Parse a DOT directory name into (dot_id, date_str).

        Directory name format: {dot_id}_{YYYYMMDD}
        Returns (dot_id, "YYYYMMDD") or (None, None).
        """
        for dot_id in self.dot_ids:
            if dir_name.startswith(f"{dot_id}_"):
                date_str = dir_name[len(dot_id) + 1:]
                return dot_id, date_str
        return None, None

    def _compute_output_dir(self, device_id: str, date_time: str) -> Path:
        """Compute the output directory for a device and timestamp."""
        return self.results_dir / device_id / date_time

    def _find_ready_dot_tracks(self, dot_dir: Path) -> list:
        """Find tracks within a DOT directory that have a done.txt signal."""
        crops_dir = dot_dir / "crops"
        if not crops_dir.exists():
            return []
        return [d for d in sorted(crops_dir.iterdir())
                if d.is_dir() and (d / "done.txt").exists()]

    def _find_latest_background(self, dot_dir: Path):
        """Find the most recent background image in a DOT directory."""
        backgrounds = sorted(dot_dir.glob("*_background.jpg"))
        if backgrounds:
            return backgrounds[-1]
        fallback = dot_dir / "current_background.jpg"
        return fallback if fallback.exists() else None

    def _process_dot_media(self, dot_dir: Path) -> None:
        """Copy videos and backgrounds to output regardless of track readiness.

        Ensures media files reach S3 quickly even when no insect tracks
        have been detected yet. Called on every detection worker poll.
        """
        try:
            dot_id, date_str = self._parse_dot_dir_name(dot_dir.name)
            if not dot_id:
                return

            output_dir = self._compute_output_dir(dot_id, date_str)
            output_dir.mkdir(parents=True, exist_ok=True)
            copied_something = False

            videos_dir = dot_dir / "videos"
            if videos_dir.exists():
                dst_videos = output_dir / "videos"
                dst_videos.mkdir(parents=True, exist_ok=True)
                for vid in sorted(videos_dir.iterdir()):
                    if vid.is_file() and vid.suffix == ".mp4":
                        dst = dst_videos / vid.name
                        if not dst.exists():
                            shutil.copy2(vid, dst)
                            logger.info(f"  Video copied: {vid.name}")
                            copied_something = True
                            # Detection may run in a subprocess with no upload callback,
                            # so hand the video to the main-process classification worker
                            # (which owns Pollen) via the disk queue, same path as tracks.
                            self.classification_queue.enqueue(
                                entry_type="video",
                                source_device=dot_id,
                                date=date_str,
                                track_id=dst.stem,
                                track_dir=dst,
                                output_dir=dst.parent,
                            )
                        vid.unlink()

            background = self._find_latest_background(dot_dir)
            if background:
                dst_background = output_dir / background.name
                if not dst_background.exists():
                    shutil.copy2(background, dst_background)
                    logger.info(f"  Background copied: {background.name}")

            if copied_something:
                logger.info(f"MEDIA: Copied new files from {dot_dir.name} to {output_dir.name}")

        except Exception as e:
            logger.error(f"Failed to process media from {dot_dir.name}: {e}", exc_info=True)

    # ------------------------------------------------------------------
    # Last-recording marker (persists across restarts)
    # ------------------------------------------------------------------

    @property
    def _marker_path(self) -> Path:
        return self.input_storage / ".last_recording"

    def _load_last_recording_marker(self) -> None:
        """Read the .last_recording marker on startup."""
        if not self._marker_path.exists():
            return

        marker_video = self._marker_path.read_text().strip()
        if not marker_video:
            self._marker_path.unlink(missing_ok=True)
            return

        if (self.input_storage / marker_video).exists():
            # Video still waiting to be processed
            self._reset_after_video = marker_video
            logger.info(f"Previous session marker: will reset tracker after {marker_video}")
        else:
            # Already processed (deleted) — reset before next video
            self._pending_tracker_reset = True
            self._marker_path.unlink(missing_ok=True)
            logger.info(f"Previous session ended ({marker_video} already processed), "
                       f"tracker will reset on next video")

    def mark_reset_after(self, filename: str) -> None:
        """Reset the tracker after ``filename`` is processed (recording stopped)."""
        self._reset_after_video = filename

    def _clear_last_recording_marker(self) -> None:
        """Delete the marker after the boundary video is processed."""
        self._marker_path.unlink(missing_ok=True)
        self._reset_after_video = ""

    # ------------------------------------------------------------------
    # Detection worker - Runs BugSpot detection/tracking
    # ------------------------------------------------------------------

    def run(self) -> None:
        """
        Worker loop that runs detection/tracking for videos and queues DOT tracks.

        Maintains continuous tracker state for FLIK videos.
        Queues both FLIK and DOT tracks for classification.
        """
        logger.info("Detection worker started")

        # Process existing items in chronological order
        for path, item_type in self._find_existing_items():
            if self.stop_event.is_set():
                break
            if item_type == "video":
                self._process_video_detection(path)
            else:
                self._process_dot_media(path)
                self._process_dot_directory_detection(path)

        # Process new videos from queue + poll for DOT directories
        while not self.stop_event.is_set():
            try:
                video_path = self.video_queue.get(timeout=1.0)
                self._process_video_detection(video_path)
                self.video_queue.task_done()

                # Check for DOT directories after each video (interleaved processing)
                for dot_dir in self._find_dot_directories():
                    if self.stop_event.is_set():
                        break
                    self._process_dot_media(dot_dir)
                    self._process_dot_directory_detection(dot_dir)

                # Periodically sweep stale output directories
                self._sweep_counter += 1
                if self._sweep_counter >= self._sweep_interval:
                    self._sweep_stale_directories()
                    self._sweep_counter = 0

            except queue.Empty:
                # Check for new DOT directories while waiting
                for dot_dir in self._find_dot_directories():
                    if self.stop_event.is_set():
                        break
                    self._process_dot_media(dot_dir)
                    self._process_dot_directory_detection(dot_dir)

                # If recording stopped, check if we're done
                if self.recording_stopped.is_set():
                    remaining = self.video_queue.qsize()
                    has_ready_tracks = any(
                        self._find_ready_dot_tracks(d)
                        for d in self._find_dot_directories()
                    )
                    pending_count = self.classification_queue.count()
                    if remaining == 0 and not has_ready_tracks and pending_count == 0:
                        logger.info("Queue empty - processing complete")
                        break
                continue
            except Exception as e:
                logger.error(f"Detection error: {e}", exc_info=True)

        logger.info("Detection worker stopped")

    def _process_video_detection(self, video_path: Path) -> None:
        """
        Process a FLIK video: detection/tracking only, queue crops for classification.

        Maintains tracker state for continuous tracking across videos.
        """
        if not video_path.exists():
            logger.warning(f"Video not found: {video_path}")
            return

        logger.info("-" * 50)
        logger.info(f"DETECTION: {video_path.name}")
        logger.info("-" * 50)

        try:
            # Compute output directory: results_dir/flick_id/date_time/
            date_time = video_path.stem[len(self.flick_id) + 1:]
            output_dir = self._compute_output_dir(self.flick_id, date_time)
            output_dir.mkdir(parents=True, exist_ok=True)

            # --- Pre-process tracker resets (continuous_tracking only) ---
            if self.continuous_tracking:
                # Pending reset from a previous session whose marker video
                # was already processed before we started
                if self._pending_tracker_reset:
                    logger.info("Resetting tracker (previous recording session ended)")
                    self.processor.reset_tracker()
                    self._pending_tracker_reset = False

                # Day-change detection
                video_date = date_time[:8]  # YYYYMMDD
                if self._last_video_date and video_date != self._last_video_date:
                    logger.info(f"Day changed ({self._last_video_date} → {video_date}), resetting tracker")
                    self.processor.reset_tracker()
                self._last_video_date = video_date

            # Run BugSpot detection/tracking (Phases 1-4)
            result = self.processor._pipeline.process_video(
                str(video_path),
                extract_crops=True,
                render_composites=self.processor.output_config.get("save_composites", True),
                save_crops_dir=str(output_dir / "crops"),
                save_composites_dir=str(output_dir / "composites") if self.processor.output_config.get("save_composites", True) else None,
            )

            logger.info(f"  BugSpot: {len(result.confirmed_tracks)} confirmed / "
                       f"{len(result.track_paths)} total tracks")

            # Save crops and queue for classification
            confirmed_count = 0
            for track_id, track in result.confirmed_tracks.items():
                # BugSpot saves crops using first 8 chars of track UUID
                # track_id format: {uuid}_{timestamp} -> use first 8 chars for directory
                base_track_id = track_id.split('-')[0]
                track_dir = output_dir / "crops" / base_track_id

                if not track_dir.exists():
                    logger.warning(f"Track directory not found: {track_dir}")
                    continue

                # Extract timestamp from video filename
                track_timestamp = date_time.split('_')[-1] if '_' in date_time else None

                # Queue for classification
                self.classification_queue.enqueue(
                    entry_type="flik",
                    source_device=self.flick_id,
                    date=date_time[:8],  # YYYYMMDD
                    time=track_timestamp,
                    track_id=track_id,
                    track_dir=track_dir,
                    output_dir=output_dir,
                    num_crops=len(track.crops),
                )
                confirmed_count += 1

            # Sample video: save 1 per N to output (0 = disabled)
            if self._video_sample_interval > 0:
                self._video_batch_count += 1
                is_last_in_batch = self._video_batch_count >= self._video_sample_interval

                if not self._video_sample_saved and (confirmed_count > 0 or is_last_in_batch):
                    shutil.copy2(video_path, output_dir / "video.mp4")
                    self._video_sample_saved = True
                    reason = "detections" if confirmed_count > 0 else "fallback"
                    logger.info(f"  Sample video saved ({reason})")

                if is_last_in_batch:
                    self._video_batch_count = 0
                    self._video_sample_saved = False

            # Clear detections but KEEP tracker state (continuous tracking)
            self.processor.clear_video_detections()

            # Delete processed video
            self._delete_video(video_path)

            # Recording-stop boundary: reset tracker after the last
            # video from the previous recording session
            if self._reset_after_video and video_path.name == self._reset_after_video:
                logger.info("Last recorded video processed, resetting tracker")
                self.processor.reset_tracker()
                self._clear_last_recording_marker()

            logger.info(f"QUEUED: {confirmed_count} tracks for classification")

            # Save detection metadata for classification thread to merge into results
            if confirmed_count > 0:
                # Backend parses video_timestamp as ISO-8601; date_time is the
                # compact YYYYMMDD_HHMMSS_micros video stem (matches processor.py).
                date_str, time_str = date_time.split('_')[:2]
                video_timestamp_iso = datetime.strptime(
                    f"{date_str}_{time_str}", "%Y%m%d_%H%M%S"
                ).isoformat()
                detection_meta = {
                    "source_device": self.flick_id,
                    "date": date_time[:8],
                    "video_file": video_path.name,
                    "video_timestamp": video_timestamp_iso,
                    "model_id": self.config.get("model", {}).get("model_id"),
                    "video_info": {
                        "fps": result.video_info.get("fps"),
                        "total_frames": result.video_info.get("total_frames"),
                        "duration_seconds": result.video_info.get("duration"),
                    } if hasattr(result, "video_info") and result.video_info else None,
                    "summary": {
                        "total_detections": len(result.all_detections) if hasattr(result, "all_detections") else 0,
                        "total_tracks": len(result.track_paths) if hasattr(result, "track_paths") else 0,
                        "confirmed_tracks": len(result.confirmed_tracks),
                        "unconfirmed_tracks": (len(result.track_paths) - len(result.confirmed_tracks)) if hasattr(result, "track_paths") else 0,
                    },
                    "tracks": {
                        tid: {
                            "num_detections": track.num_detections if hasattr(track, "num_detections") else None,
                            "first_seen_seconds": track.first_frame_time if hasattr(track, "first_frame_time") else None,
                            "last_seen_seconds": track.last_frame_time if hasattr(track, "last_frame_time") else None,
                            "duration_seconds": track.duration if hasattr(track, "duration") else None,
                            "topology_metrics": track.topology_metrics if hasattr(track, "topology_metrics") else None,
                        }
                        for tid, track in result.confirmed_tracks.items()
                    },
                    "frame_detections": {
                        track_id: [
                            {
                                "frame_number": det.get("frame_number"),
                                "timestamp_seconds": det.get("frame_time_seconds"),
                                "bbox": det.get("bbox"),
                            }
                            for det in result.all_detections
                            if det.get("track_id") == track_id
                        ]
                        for track_id in result.confirmed_tracks
                    } if hasattr(result, "all_detections") else {},
                }
                meta_path = output_dir / ".detection.json"
                meta_path.write_text(json.dumps(detection_meta, indent=2, default=str))

                # Write expected track count for completeness check
                (output_dir / ".expected_tracks").write_text(str(confirmed_count))
                logger.info(f"  Detection metadata saved ({confirmed_count} tracks)")
            else:
                # No confirmed tracks — write empty results and mark done so
                # the upload thread can discover and clean up this directory
                empty_results = {
                    "source_device": self.flick_id,
                    "date": date_time[:8],
                    "processing_timestamp": datetime.now(timezone.utc).isoformat(),
                    "summary": {
                        "total_detections": 0,
                        "total_tracks": 0,
                        "confirmed_tracks": 0,
                        "unconfirmed_tracks": 0,
                    },
                    "tracks": [],
                }
                self.writer.write_results(results=empty_results, output_dir=output_dir)
                (output_dir / ".done").write_text("classified=0\nexpected=0\n")
                logger.info("  No confirmed tracks, marked directory done")
                self._notify_result_ready(output_dir)

        except Exception as e:
            logger.error(f"Failed to process {video_path.name}: {e}", exc_info=True)

    def _process_dot_directory_detection(self, dot_dir: Path) -> None:
        """
        Process DOT directory: copy crops/labels, queue for classification.

        Does NOT touch the tracker - DOT processing is independent.
        """
        try:
            dot_id, date_str = self._parse_dot_dir_name(dot_dir.name)
            if not dot_id:
                logger.warning(f"Could not parse DOT directory: {dot_dir.name}")
                return

            ready_tracks = self._find_ready_dot_tracks(dot_dir)
            if not ready_tracks:
                return

            logger.info("-" * 50)
            logger.info(f"DOT DETECTION: {dot_dir.name} ({len(ready_tracks)} track(s) ready)")
            logger.info("-" * 50)

            # Videos are handled separately by _process_dot_media (a standalone unit
            # under <dot>/<YYYYMMDD>/videos/); detection only owns the tracks.
            background = self._find_latest_background(dot_dir)

            # Each ready track becomes its own terminal result dir,
            # <dot>/<YYYYMMDD>/<track_id>_<HHMMSS>/: one results.json + .done,
            # uploaded once and deleted (no day-bucket accumulation).
            queued_count = 0
            for track_dir in ready_tracks:
                if self.stop_event.is_set():
                    break

                track_dir_name = track_dir.name
                track_id = track_dir_name.rsplit("_", 1)[0]
                track_timestamp = track_dir_name.rsplit("_", 1)[-1] if "_" in track_dir_name else None

                track_output_dir = self._compute_output_dir(dot_id, f"{date_str}/{track_dir_name}")
                track_output_dir.mkdir(parents=True, exist_ok=True)

                # Background lives in the track dir so the composite step has it after
                # the incoming DOT dir is cleaned up; the dir is terminal so it's local.
                track_background = None
                if background:
                    track_background = track_output_dir / background.name
                    shutil.copy2(background, track_background)

                # Copy crops to output
                dst_crops = track_output_dir / "crops" / track_dir_name
                dst_crops.mkdir(parents=True, exist_ok=True)

                crop_count = 0
                for f in track_dir.iterdir():
                    if f.name != "done.txt" and f.is_file():
                        shutil.copy2(f, dst_crops / f.name)
                        crop_count += 1

                # Copy label file to output
                label_src = dot_dir / "labels" / f"{track_id}.json"
                dst_labels = track_output_dir / "labels"
                dst_labels.mkdir(parents=True, exist_ok=True)
                if label_src.exists():
                    shutil.copy2(label_src, dst_labels / f"{track_id}.json")

                # Queue for classification. track_id stays bare: the backend
                # reconstructs crop/composite keys as {track_id}_{timestamp} and keys
                # the tracks table on (device_id, timestamp), not track_id.
                self.classification_queue.enqueue(
                    entry_type="dot",
                    source_device=dot_id,
                    date=date_str,
                    time=track_timestamp,
                    track_id=track_id,
                    track_dir=dst_crops,
                    output_dir=track_output_dir,
                    labels_path=dst_labels / f"{track_id}.json" if label_src.exists() else None,
                    background_path=track_background,
                    num_crops=crop_count,
                )
                # One track per dir: complete-on-single, so .done fires immediately.
                (track_output_dir / ".expected_tracks").write_text("1")
                queued_count += 1

                # Delete processed track from input
                shutil.rmtree(track_dir)
                logger.info(
                    "DOT track -> %s/%s/%s (%d crops, bare id=%s)",
                    dot_id, date_str, track_dir_name, crop_count, track_id,
                )

            logger.info(f"QUEUED: {queued_count} DOT tracks for classification")

            # Clean up DOT directory if empty after processing
            try:
                remaining = list(dot_dir.iterdir())
                if not remaining:
                    dot_dir.rmdir()
                    logger.info(f"Removed empty DOT directory: {dot_dir.name}")
            except OSError:
                pass

        except Exception as e:
            logger.error(f"Failed to process DOT {dot_dir.name}: {e}", exc_info=True)

    def _delete_video(self, video_path: Path) -> None:
        """Delete processed video."""
        try:
            video_path.unlink()
            logger.debug(f"Deleted: {video_path.name}")
        except Exception as e:
            logger.error(f"Could not delete {video_path.name}: {e}")

    def _sweep_stale_directories(self) -> None:
        """Clean up FLIK output directories that are stuck without .done markers.

        Handles two cases:
        1. Directories with results.json but no .done and no pending classification
           entries — likely a crash left them incomplete. If older than 30 minutes,
           write .done so the upload thread can pick them up.
        2. Empty directories with no results.json and no .done — created by detection
           but never populated. Remove them if older than 10 minutes.
        """
        stale_threshold_seconds = 30 * 60
        empty_threshold_seconds = 10 * 60

        try:
            for device_dir in self.results_dir.iterdir():
                if not device_dir.is_dir():
                    continue
                for output_dir in device_dir.iterdir():
                    if not output_dir.is_dir():
                        continue

                    done_path = output_dir / ".done"
                    if done_path.exists():
                        continue

                    results_path = output_dir / "results.json"
                    expected_path = output_dir / ".expected_tracks"

                    # Case 1: Has results.json but not marked done
                    if results_path.exists() and not expected_path.exists():
                        # No pending classification — mark done
                        age_seconds = (datetime.now().timestamp() - output_dir.stat().st_mtime)
                        if age_seconds > stale_threshold_seconds:
                            done_path.write_text("swept=stale\n")
                            logger.info(f"Swept stale directory: {output_dir.name} (no .expected_tracks, marked done)")

                    elif results_path.exists() and expected_path.exists():
                        # Has expected tracks but not all completed
                        # Check if all tracks are already classified
                        try:
                            expected = int(expected_path.read_text().strip())
                        except (ValueError, OSError):
                            expected = 0
                        completed_path = output_dir / ".completed_tracks"
                        try:
                            completed = int(completed_path.read_text().strip())
                        except (ValueError, OSError):
                            completed = 0

                        age_seconds = (datetime.now().timestamp() - output_dir.stat().st_mtime)
                        if age_seconds > stale_threshold_seconds and completed >= expected:
                            done_path.write_text(f"swept=stale\ncompleted={completed}\nexpected={expected}\n")
                            logger.info(f"Swept stale directory: {output_dir.name} (all completed but no .done)")

                    # Case 2: Empty directory (no results.json, no classification activity)
                    elif not results_path.exists() and not expected_path.exists():
                        sidecar_names = {".done", ".detection.json", ".expected_tracks", ".completed_tracks", "results.json.tmp"}
                        has_content = False
                        for f in output_dir.rglob("*"):
                            if f.is_file() and f.name not in sidecar_names:
                                has_content = True
                                break
                        if not has_content:
                            age_seconds = (datetime.now().timestamp() - output_dir.stat().st_mtime)
                            if age_seconds > empty_threshold_seconds:
                                shutil.rmtree(output_dir)
                                logger.info(f"Removed empty stale directory: {output_dir.name}")
        except Exception as e:
            logger.warning(f"Error during stale directory sweep: {e}")

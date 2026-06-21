"""Wiring helpers that build a Pollen instance from bugcam runtime settings.

Keeps construction details (state-dir paths, presigner, archiver selection) out
of the app entrypoint so producers just call ``pollen.enqueue(...)``.
"""
from __future__ import annotations

from pathlib import Path

from bugcam.config import get_state_dir
from bugcam.pollen.archive import TarArchiver
from bugcam.pollen.pollen import Pollen, PollenConfig
from bugcam.pollen.presign import Presigner


def build_pollen(
    output_dir: Path,
    api_url: str,
    api_key: str,
    *,
    batch: bool = False,
    poll_interval: float = 10.0,
    state_dir: Path | None = None,
) -> Pollen:
    """Construct a Pollen owning uploads out of ``output_dir``."""
    base = (state_dir or get_state_dir()) / "pollen"
    config = PollenConfig(
        db_path=base / "pollen.db",
        output_root=Path(output_dir),
        staging_dir=base / "staging",
        poll_interval=poll_interval,
        batch=batch,
    )
    presigner = Presigner(api_url, api_key)
    archiver = TarArchiver() if batch else None
    return Pollen(config, presigner=presigner, archiver=archiver)

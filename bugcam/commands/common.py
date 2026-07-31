"""Helpers shared across bugcam command modules."""
from __future__ import annotations

import typer

from bugcam.processing import parse_capture_resolution


def parse_resolution_option(value: str) -> tuple[int, int]:
    """Parse a WxH CLI resolution value, raising BadParameter on bad input."""
    try:
        return parse_capture_resolution(value)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

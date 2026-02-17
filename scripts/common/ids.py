"""Run identifier helpers."""

from __future__ import annotations

from datetime import datetime, timezone


def generate_run_id() -> str:
    now = datetime.now(tz=timezone.utc)
    # UUIDv7-like sortable id without external dependency.
    return now.strftime("run-%Y%m%dT%H%M%S%fZ")

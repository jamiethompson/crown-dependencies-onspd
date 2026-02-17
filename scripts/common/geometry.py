"""Geometry helpers."""

from __future__ import annotations

from typing import Any


def extract_point_from_geometry(geometry: dict[str, Any] | None) -> tuple[float | None, float | None]:
    if not geometry:
        return None, None
    x = geometry.get("x")
    y = geometry.get("y")
    if x is None or y is None:
        return None, None
    return float(y), float(x)

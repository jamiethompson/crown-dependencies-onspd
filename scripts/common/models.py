"""Data models used across the pipeline."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class RawRecord:
    territory: str
    source_name: str
    source_class: str
    source_record_id: str | None
    raw_postcode: str | None
    raw_lat: float | None
    raw_lon: float | None
    raw_geometry: dict[str, Any] | None
    source_wkid: int | None
    extract_date: str
    run_id: str
    raw_payload_ref: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

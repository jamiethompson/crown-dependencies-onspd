"""Canonical export stage placeholder."""

from __future__ import annotations

from pathlib import Path

from scripts.common.fs import write_csv


CANONICAL_HEADERS = [
    "territory",
    "postcode",
    "normalised_postcode",
    "source_list",
    "source_count",
    "has_coordinates",
    "lat",
    "lon",
    "coordinate_source",
    "confidence_score",
    "first_seen",
    "last_seen",
    "notes",
]


def write_empty_canonical(territory_code: str, territory_config: dict, data_dir: Path) -> Path:
    output_name = territory_config["output"]["canonical_filename"]
    out_path = data_dir / "out" / output_name
    write_csv(out_path, CANONICAL_HEADERS, [])
    return out_path

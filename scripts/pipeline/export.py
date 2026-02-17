"""Canonical CSV export."""

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


def _serialize_row(row: dict) -> dict:
    out = {}
    for key in CANONICAL_HEADERS:
        value = row.get(key)
        if value is None:
            out[key] = ""
        elif isinstance(value, bool):
            out[key] = "true" if value else "false"
        else:
            out[key] = value
    return out


def write_canonical_csv(territory_config: dict, data_dir: Path, rows: list[dict]) -> Path:
    output_name = territory_config["output"]["canonical_filename"]
    out_path = data_dir / "out" / output_name
    sorted_rows = sorted(rows, key=lambda row: row["normalised_postcode"])
    serialized_rows = [_serialize_row(row) for row in sorted_rows]
    write_csv(out_path, CANONICAL_HEADERS, serialized_rows)
    return out_path

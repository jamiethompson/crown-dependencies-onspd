"""Validation stage placeholder."""

from __future__ import annotations

from pathlib import Path

from scripts.common.fs import write_json


def run_validate(territory_code: str, data_dir: Path, run_id: str, run_date: str) -> Path:
    report_path = data_dir / "out" / "reports" / f"{territory_code.lower()}_report.json"
    payload = {
        "territory": territory_code,
        "run_id": run_id,
        "run_date": run_date,
        "counts": {
            "raw_rows": 0,
            "valid_postcodes": 0,
            "unique_postcodes": 0,
            "with_coordinates": 0,
            "without_coordinates": 0,
            "invalid_postcodes": 0,
        },
        "sources": {"authoritative": 0, "digimap": 0, "osm": 0},
        "quality": {"bbox_outliers": 0, "duplicate_keys": 0},
        "confidence_buckets": {"0_24": 0, "25_49": 0, "50_74": 0, "75_100": 0},
        "onspd_fill": [],
        "warnings": [],
        "errors": [],
    }
    write_json(report_path, payload)
    return report_path

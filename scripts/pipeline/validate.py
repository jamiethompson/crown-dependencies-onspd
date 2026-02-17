"""Validation stage and territory quality report generation."""

from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path

from scripts.common.constants import TERRITORY_SLUG_BY_CODE
from scripts.common.errors import ContractError, StageError
from scripts.common.fs import read_json, write_json


def _read_csv_rows(path: Path) -> tuple[list[str], list[dict]]:
    if not path.exists():
        raise StageError(f"Missing CSV input: {path}")
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader.fieldnames or []), list(reader)


def _load_raw_rows(data_dir: Path, territory_code: str) -> list[dict]:
    territory = territory_code.lower()
    paths = [
        data_dir / "raw" / "arcgis" / f"{territory}_arcgis.json",
        data_dir / "raw" / "osm" / "overpass" / f"{territory}_overpass.json",
        data_dir / "raw" / "osm" / "geofabrik" / f"{territory}_geofabrik.json",
    ]
    rows: list[dict] = []
    for path in paths:
        if not path.exists():
            continue
        payload = read_json(path)
        rows.extend(payload.get("rows", []))
    return rows


def _confidence_buckets(rows: list[dict]) -> dict[str, int]:
    buckets = {"0_24": 0, "25_49": 0, "50_74": 0, "75_100": 0}
    for row in rows:
        try:
            score = int(row.get("confidence_score") or 0)
        except ValueError:
            score = 0

        if score <= 24:
            buckets["0_24"] += 1
        elif score <= 49:
            buckets["25_49"] += 1
        elif score <= 74:
            buckets["50_74"] += 1
        else:
            buckets["75_100"] += 1
    return buckets


def _compute_fill_rates(header: list[str], rows: list[dict]) -> list[dict]:
    total = len(rows)
    stats = []
    for column in header:
        filled = sum(1 for row in rows if row.get(column, "") not in ("", None))
        null = total - filled
        fill_percent = 0.0 if total == 0 else round((filled / total) * 100, 2)
        stats.append({"column": column, "filled": filled, "null": null, "fill_percent": fill_percent})
    return stats


def run_validate(
    territory_code: str,
    territory_config: dict,
    onspd_columns: dict,
    data_dir: Path,
    run_id: str,
    run_date: str,
) -> Path:
    canonical_path = data_dir / "out" / territory_config["output"]["canonical_filename"]
    onspd_path = data_dir / "out" / territory_config["output"]["onspd_filename"]
    intermediate_path = data_dir / "intermediate" / f"{territory_code.lower()}_canonical.json"

    canonical_header, canonical_rows = _read_csv_rows(canonical_path)
    onspd_header, onspd_rows = _read_csv_rows(onspd_path)

    intermediate = read_json(intermediate_path) if intermediate_path.exists() else {}
    raw_rows = _load_raw_rows(data_dir, territory_code)

    normalised_values = [row.get("normalised_postcode") for row in canonical_rows if row.get("normalised_postcode")]
    duplicates = sum(count - 1 for count in Counter(normalised_values).values() if count > 1)

    with_coordinates = sum(1 for row in canonical_rows if (row.get("has_coordinates", "").lower() == "true"))
    without_coordinates = len(canonical_rows) - with_coordinates

    invalid_by_source = intermediate.get("invalid_postcodes", {})
    invalid_count = sum(int(v) for v in invalid_by_source.values())

    source_counts = {"authoritative": 0, "digimap": 0, "osm": 0}
    for row in raw_rows:
        source_class = row.get("source_class", "other")
        if source_class in source_counts:
            source_counts[source_class] += 1

    bbox_outliers = 0
    for row in canonical_rows:
        notes = row.get("notes") or ""
        if "COORDINATE_OUTLIER" in notes:
            bbox_outliers += 1

    expected_onspd_header = [column["name"] for column in onspd_columns.get("columns", [])]
    warnings: list[str] = []
    errors: list[str] = []

    if onspd_header != expected_onspd_header:
        errors.append("ONSPD_HEADER_ORDER_MISMATCH")

    if duplicates > 0:
        warnings.append("DUPLICATE_NORMALISED_POSTCODES_PRESENT")

    if errors:
        raise ContractError(";".join(errors))

    report_payload = {
        "territory": territory_code,
        "run_id": run_id,
        "run_date": run_date,
        "counts": {
            "raw_rows": int(intermediate.get("raw_row_count", len(raw_rows))),
            "valid_postcodes": int(intermediate.get("valid_postcodes", 0)),
            "unique_postcodes": len(canonical_rows),
            "with_coordinates": with_coordinates,
            "without_coordinates": without_coordinates,
            "invalid_postcodes": invalid_count,
        },
        "sources": source_counts,
        "quality": {
            "bbox_outliers": bbox_outliers,
            "duplicate_keys": duplicates,
            "coordinate_coverage_percent": 0.0
            if len(canonical_rows) == 0
            else round((with_coordinates / len(canonical_rows)) * 100, 2),
        },
        "confidence_buckets": _confidence_buckets(canonical_rows),
        "onspd_fill": _compute_fill_rates(onspd_header, onspd_rows),
        "warnings": warnings,
        "errors": errors,
        "diagnostics": {
            "invalid_postcodes_by_source": invalid_by_source,
            "canonical_header": canonical_header,
            "onspd_header": onspd_header,
        },
    }

    slug = TERRITORY_SLUG_BY_CODE.get(territory_code, territory_code.lower())
    report_path = data_dir / "out" / "reports" / f"{slug}_report.json"
    write_json(report_path, report_payload)
    return report_path

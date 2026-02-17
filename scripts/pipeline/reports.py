"""Run report aggregation."""

from __future__ import annotations

from pathlib import Path

from scripts.common.constants import TERRITORY_SLUG_BY_CODE
from scripts.common.fs import read_json, write_json


def write_run_summary(data_dir: Path, run_id: str, run_date: str, territories: list[str]) -> Path:
    territory_reports = {}
    totals = {
        "raw_rows": 0,
        "unique_postcodes": 0,
        "with_coordinates": 0,
        "without_coordinates": 0,
        "invalid_postcodes": 0,
    }
    warning_count = 0
    error_count = 0

    for territory_code in territories:
        slug = TERRITORY_SLUG_BY_CODE.get(territory_code, territory_code.lower())
        report_path = data_dir / "out" / "reports" / f"{slug}_report.json"
        if not report_path.exists():
            territory_reports[territory_code] = {"status": "missing_report"}
            error_count += 1
            continue

        report = read_json(report_path)
        territory_reports[territory_code] = {
            "counts": report.get("counts", {}),
            "warnings": report.get("warnings", []),
            "errors": report.get("errors", []),
        }

        counts = report.get("counts", {})
        totals["raw_rows"] += int(counts.get("raw_rows", 0))
        totals["unique_postcodes"] += int(counts.get("unique_postcodes", 0))
        totals["with_coordinates"] += int(counts.get("with_coordinates", 0))
        totals["without_coordinates"] += int(counts.get("without_coordinates", 0))
        totals["invalid_postcodes"] += int(counts.get("invalid_postcodes", 0))

        warning_count += len(report.get("warnings", []))
        error_count += len(report.get("errors", []))

    status = "success"
    if error_count > 0:
        status = "error"
    elif warning_count > 0:
        status = "partial"

    summary_path = data_dir / "out" / "reports" / "run_summary.json"
    payload = {
        "run_id": run_id,
        "run_date": run_date,
        "status": status,
        "territories": territories,
        "totals": totals,
        "warning_count": warning_count,
        "error_count": error_count,
        "territory_reports": territory_reports,
    }
    write_json(summary_path, payload)
    return summary_path

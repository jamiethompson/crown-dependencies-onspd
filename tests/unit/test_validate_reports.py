import csv
from pathlib import Path

import pytest

from scripts.common.errors import ContractError
from scripts.common.fs import write_json
from scripts.pipeline.reports import write_run_summary
from scripts.pipeline.validate import run_validate


def _write_csv(path: Path, header: list[str], rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_validate_generates_territory_report(tmp_path: Path):
    canonical_path = tmp_path / "out" / "jersey.csv"
    onspd_path = tmp_path / "out" / "jersey_onspd.csv"
    intermediate_path = tmp_path / "intermediate" / "je_canonical.json"
    raw_arcgis_path = tmp_path / "raw" / "arcgis" / "je_arcgis.json"

    _write_csv(
        canonical_path,
        [
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
        ],
        [
            {
                "territory": "JE",
                "postcode": "JE2 3AB",
                "normalised_postcode": "JE2 3AB",
                "source_list": "auth_source",
                "source_count": "1",
                "has_coordinates": "true",
                "lat": "49.2",
                "lon": "-2.1",
                "coordinate_source": "authoritative",
                "confidence_score": "80",
                "first_seen": "2026-01-01",
                "last_seen": "2026-02-17",
                "notes": "",
            }
        ],
    )
    _write_csv(
        onspd_path,
        ["pcd", "pcd2", "lat", "long", "ctry"],
        [{"pcd": "JE2 3AB", "pcd2": "JE23AB", "lat": "49.2", "long": "-2.1", "ctry": "JE"}],
    )

    write_json(intermediate_path, {"raw_row_count": 1, "valid_postcodes": 1, "invalid_postcodes": {"auth_source": 0}})
    write_json(raw_arcgis_path, {"rows": [{"source_class": "authoritative"}]})

    territory_config = {"output": {"canonical_filename": "jersey.csv", "onspd_filename": "jersey_onspd.csv"}}
    onspd_columns = {
        "columns": [
            {"name": "pcd"},
            {"name": "pcd2"},
            {"name": "lat"},
            {"name": "long"},
            {"name": "ctry"},
        ]
    }

    report_path = run_validate("JE", territory_config, onspd_columns, tmp_path, run_id="run-1", run_date="2026-02-17")

    assert report_path.exists()
    report = report_path.read_text(encoding="utf-8")
    assert '"unique_postcodes": 1' in report
    assert '"75_100": 1' in report


def test_validate_raises_on_onspd_header_mismatch(tmp_path: Path):
    _write_csv(
        tmp_path / "out" / "jersey.csv",
        [
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
        ],
        [],
    )
    _write_csv(tmp_path / "out" / "jersey_onspd.csv", ["wrong"], [])

    territory_config = {"output": {"canonical_filename": "jersey.csv", "onspd_filename": "jersey_onspd.csv"}}
    onspd_columns = {"columns": [{"name": "pcd"}]}

    with pytest.raises(ContractError):
        run_validate("JE", territory_config, onspd_columns, tmp_path, run_id="run-1", run_date="2026-02-17")


def test_run_summary_aggregates_reports(tmp_path: Path):
    write_json(
        tmp_path / "out" / "reports" / "jersey_report.json",
        {
            "counts": {
                "raw_rows": 2,
                "unique_postcodes": 1,
                "with_coordinates": 1,
                "without_coordinates": 0,
                "invalid_postcodes": 0,
            },
            "warnings": [],
            "errors": [],
        },
    )
    write_json(
        tmp_path / "out" / "reports" / "guernsey_report.json",
        {
            "counts": {
                "raw_rows": 3,
                "unique_postcodes": 2,
                "with_coordinates": 1,
                "without_coordinates": 1,
                "invalid_postcodes": 1,
            },
            "warnings": ["warn"],
            "errors": [],
        },
    )

    summary_path = write_run_summary(tmp_path, run_id="run-2", run_date="2026-02-17", territories=["JE", "GY"])
    payload = summary_path.read_text(encoding="utf-8")

    assert '"status": "partial"' in payload
    assert '"raw_rows": 5' in payload

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.cli import parse_args, run_command
from scripts.common.fs import write_json


@pytest.mark.regression
def test_fixture_pipeline_snapshot_outputs_are_stable(tmp_path: Path):
    data_dir = tmp_path / "data"

    write_json(
        data_dir / "raw" / "arcgis" / "je_arcgis.json",
        {
            "rows": [
                {
                    "territory": "JE",
                    "source_name": "jersey_gov_arcgis",
                    "source_class": "authoritative",
                    "source_record_id": "1",
                    "raw_postcode": "je23ab",
                    "raw_lat": 49.2,
                    "raw_lon": -2.1,
                    "source_wkid": 4326,
                }
            ]
        },
    )
    write_json(
        data_dir / "raw" / "osm" / "overpass" / "je_overpass.json",
        {
            "rows": [
                {
                    "territory": "JE",
                    "source_name": "osm_overpass",
                    "source_class": "osm",
                    "source_record_id": "2",
                    "raw_postcode": "JE2 3AB",
                    "raw_lat": 49.21,
                    "raw_lon": -2.11,
                    "source_wkid": 4326,
                },
                {
                    "territory": "JE",
                    "source_name": "osm_overpass",
                    "source_class": "osm",
                    "source_record_id": "3",
                    "raw_postcode": "JE1 1AA",
                    "raw_lat": 49.25,
                    "raw_lon": -2.15,
                    "source_wkid": 4326,
                },
            ]
        },
    )
    write_json(data_dir / "raw" / "osm" / "geofabrik" / "je_geofabrik.json", {"rows": []})

    merge_args = parse_args(
        [
            "merge",
            "--territory",
            "JE",
            "--config-dir",
            "config",
            "--data-dir",
            str(data_dir),
            "--run-date",
            "2026-02-17",
            "--run-id",
            "run-fixture",
        ]
    )
    map_args = parse_args(
        [
            "map-onspd",
            "--territory",
            "JE",
            "--config-dir",
            "config",
            "--data-dir",
            str(data_dir),
            "--run-date",
            "2026-02-17",
            "--run-id",
            "run-fixture",
        ]
    )
    validate_args = parse_args(
        [
            "validate",
            "--territory",
            "JE",
            "--config-dir",
            "config",
            "--data-dir",
            str(data_dir),
            "--run-date",
            "2026-02-17",
            "--run-id",
            "run-fixture",
        ]
    )

    assert run_command(merge_args) == 0
    assert run_command(map_args) == 0
    assert run_command(validate_args) == 0

    canonical_actual = (data_dir / "out" / "jersey.csv").read_text(encoding="utf-8")
    canonical_expected = Path("tests/fixtures/expected/jersey_fixture_canonical.csv").read_text(encoding="utf-8")
    assert canonical_actual == canonical_expected

    onspd_actual = (data_dir / "out" / "jersey_onspd.csv").read_text(encoding="utf-8")
    onspd_expected = Path("tests/fixtures/expected/jersey_fixture_onspd.csv").read_text(encoding="utf-8")
    assert onspd_actual == onspd_expected

    report = (data_dir / "out" / "reports" / "jersey_report.json").read_text(encoding="utf-8")
    assert '"duplicate_keys": 0' in report
    assert '"75_100": 1' in report

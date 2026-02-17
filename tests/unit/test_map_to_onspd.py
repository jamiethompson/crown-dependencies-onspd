import csv
from pathlib import Path

from scripts.pipeline.map_to_onspd import run_map_onspd


def test_map_to_onspd_writes_contract_header_and_rows(tmp_path: Path):
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    canonical_path = out_dir / "jersey.csv"
    with canonical_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["normalised_postcode", "lat", "lon", "territory"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "normalised_postcode": "JE2 3AB",
                "lat": "49.2",
                "lon": "-2.1",
                "territory": "JE",
            }
        )

    territory_config = {
        "output": {
            "canonical_filename": "jersey.csv",
            "onspd_filename": "jersey_onspd.csv",
        }
    }
    onspd_columns = {
        "columns": [
            {"name": "pcd", "source_mapping": "normalised_postcode"},
            {"name": "pcd2", "source_mapping": "normalised_postcode_no_space"},
            {"name": "lat", "source_mapping": "lat"},
            {"name": "long", "source_mapping": "lon"},
            {"name": "ctry", "source_mapping": "country_code_or_blank"},
        ]
    }

    result = run_map_onspd("JE", territory_config, onspd_columns, tmp_path)

    assert result["rows"] == 1
    with (out_dir / "jersey_onspd.csv").open("r", encoding="utf-8", newline="") as f:
        reader = list(csv.reader(f))
    assert reader[0] == ["pcd", "pcd2", "lat", "long", "ctry"]
    assert reader[1][0] == "JE2 3AB"
    assert reader[1][1] == "JE23AB"

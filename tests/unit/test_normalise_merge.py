from pathlib import Path

from scripts.common.fs import write_json
from scripts.pipeline.normalise_merge import run_normalise_merge


def test_normalise_merge_dedupes_and_applies_source_priority(tmp_path: Path):
    base = tmp_path
    write_json(
        base / "raw" / "arcgis" / "je_arcgis.json",
        {
            "rows": [
                {
                    "territory": "JE",
                    "source_name": "auth_source",
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
        base / "raw" / "osm" / "overpass" / "je_overpass.json",
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
                    "raw_postcode": "bad",
                    "raw_lat": 49.0,
                    "raw_lon": -2.0,
                    "source_wkid": 4326,
                },
            ]
        },
    )
    write_json(base / "raw" / "osm" / "geofabrik" / "je_geofabrik.json", {"rows": []})

    territory_config = {
        "source_priority": ["auth_source", "osm_overpass"],
        "validation": {"bbox_wgs84": {"min_lat": 49.0, "max_lat": 50.0, "min_lon": -3.0, "max_lon": -1.0}},
        "crs": {"default_epsg": 4326, "authoritative_epsg_hint_by_source": {}},
        "scoring_profile": "default",
    }
    scoring_rules = {
        "profiles": {
            "default": {
                "rules": [
                    {"id": "authoritative_presence", "when": "has_source(authoritative)", "add": 50},
                    {"id": "osm_presence", "when": "has_source(osm)", "add": 10},
                    {"id": "authoritative_coords", "when": "coord_source(authoritative)", "add": 15},
                ],
                "clamp": {"min": 0, "max": 100},
            }
        }
    }

    merged = run_normalise_merge("JE", territory_config, scoring_rules, base, run_id="run-1")

    assert merged["unique_postcodes"] == 1
    row = merged["rows"][0]
    assert row["postcode"] == "je23ab"
    assert row["source_list"] == "auth_source;osm_overpass"
    assert row["confidence_score"] == 75
    assert merged["invalid_postcodes"]["osm_overpass"] == 1

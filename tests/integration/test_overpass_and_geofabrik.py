from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.harvest.geofabrik_parse import run_geofabrik_parse
from scripts.harvest.overpass_harvest import build_overpass_query, run_overpass_harvest


class FakeHttpClient:
    def __init__(self, payload: dict):
        self.payload = payload

    def post_form_json(self, *_args, **_kwargs):
        return self.payload

    def close(self):
        return None


@pytest.mark.integration
def test_overpass_harvest_ingests_elements(tmp_path: Path):
    payload = json.loads(Path("tests/fixtures/harvest/overpass_payload.json").read_text(encoding="utf-8"))
    territory_config = {
        "overpass": {
            "enabled": True,
            "endpoint": "https://overpass-api.de/api/interpreter",
            "timeout_seconds": 180,
            "area_strategy": "bbox",
            "bbox": [49.15, -2.3, 49.31, -1.95],
        }
    }

    result = run_overpass_harvest(
        "JE",
        territory_config,
        tmp_path,
        run_id="run-3",
        run_date="2026-02-17",
        http_client=FakeHttpClient(payload),
    )

    assert result["row_count"] == 2
    assert result["rows"][1]["raw_lat"] == 49.25


@pytest.mark.integration
def test_build_overpass_query_relation():
    query = build_overpass_query(
        {
            "timeout_seconds": 120,
            "area_strategy": "relation",
            "relation_id": 123,
        }
    )
    assert "area(3600000123)" in query


@pytest.mark.integration
def test_geofabrik_parse_ingests_json_fixture(tmp_path: Path):
    fixture_path = tmp_path / "geofabrik_payload.json"
    fixture_path.write_text(Path("tests/fixtures/harvest/geofabrik_payload.json").read_text(encoding="utf-8"), encoding="utf-8")

    territory_config = {
        "geofabrik": {
            "enabled": True,
            "pbf_path": str(fixture_path),
            "download_url": "",
        }
    }

    result = run_geofabrik_parse("JE", territory_config, tmp_path, run_id="run-4", run_date="2026-02-17")

    assert result["row_count"] == 1
    assert result["rows"][0]["raw_postcode"] == "JE1 1AA"

from __future__ import annotations

import json
from pathlib import Path

import pytest
import requests

from scripts.harvest import geofabrik_parse
from scripts.harvest.geofabrik_parse import run_geofabrik_parse
from scripts.harvest.overpass_harvest import build_overpass_query, run_overpass_harvest


class FakeHttpClient:
    def __init__(self, payload: dict):
        self.payload = payload

    def post_form_json(self, *_args, **_kwargs):
        return self.payload

    def close(self):
        return None


class FakeDownloadResponse:
    def __init__(self, body: bytes):
        self.body = body

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size: int = 0):
        del chunk_size
        yield self.body


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
def test_overpass_harvest_reads_multiple_tags_and_dedupes(tmp_path: Path):
    payload = {
        "elements": [
            {
                "type": "node",
                "id": 101,
                "lat": 49.21,
                "lon": -2.11,
                "tags": {"postal_code": "JE4 5EF"},
            },
            {
                "type": "node",
                "id": 101,
                "lat": 49.21,
                "lon": -2.11,
                "tags": {"postal_code": "JE4 5EF"},
            },
            {
                "type": "way",
                "id": 102,
                "center": {"lat": 49.22, "lon": -2.12},
                "tags": {"addr:postal_code": "JE5 6GH"},
            },
        ]
    }
    territory_config = {
        "overpass": {
            "enabled": True,
            "endpoint": "https://overpass-api.de/api/interpreter",
            "timeout_seconds": 180,
            "area_strategy": "bbox",
            "bbox": [49.15, -2.3, 49.31, -1.95],
            "postcode_tags": ["postal_code", "addr:postal_code", "addr:postcode"],
        }
    }

    result = run_overpass_harvest(
        "JE",
        territory_config,
        tmp_path,
        run_id="run-3b",
        run_date="2026-02-17",
        http_client=FakeHttpClient(payload),
    )

    assert result["row_count"] == 2
    assert {row["raw_postcode"] for row in result["rows"]} == {"JE4 5EF", "JE5 6GH"}


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
def test_build_overpass_query_uses_all_configured_tags():
    query = build_overpass_query(
        {
            "timeout_seconds": 120,
            "area_strategy": "bbox",
            "bbox": [49.15, -2.3, 49.31, -1.95],
            "postcode_tags": ["addr:postcode", "postal_code"],
        }
    )
    assert 'nwr["addr:postcode"](49.15,-2.3,49.31,-1.95);' in query
    assert 'nwr["postal_code"](49.15,-2.3,49.31,-1.95);' in query


@pytest.mark.integration
def test_build_overpass_query_accepts_scalar_tag_config():
    query = build_overpass_query(
        {
            "timeout_seconds": 120,
            "area_strategy": "bbox",
            "bbox": [49.15, -2.3, 49.31, -1.95],
            "postcode_tags": "postal_code",
        }
    )
    assert 'nwr["postal_code"](49.15,-2.3,49.31,-1.95);' in query


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


@pytest.mark.integration
def test_geofabrik_parse_ingests_geojson_feature_collection(tmp_path: Path):
    fixture_path = tmp_path / "im_extract.geojson"
    fixture_path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"postcode": "IM1 2AU"},
                        "geometry": {"type": "Point", "coordinates": [-4.4755, 54.1505]},
                    },
                    {
                        "type": "Feature",
                        "properties": {"addr:postcode": "IM2 3CD"},
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [-4.49, 54.16],
                                    [-4.48, 54.16],
                                    [-4.48, 54.17],
                                    [-4.49, 54.17],
                                    [-4.49, 54.16],
                                ]
                            ],
                        },
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    territory_config = {
        "fields": {"postcode_candidates": ["postcode", "addr:postcode"]},
        "geofabrik": {
            "enabled": True,
            "pbf_path": str(fixture_path),
            "download_url": "",
        },
    }

    result = run_geofabrik_parse("IM", territory_config, tmp_path, run_id="run-7", run_date="2026-02-17")

    assert result["row_count"] == 2
    assert {row["raw_postcode"] for row in result["rows"]} == {"IM1 2AU", "IM2 3CD"}
    assert any(row["raw_lat"] is not None and row["raw_lon"] is not None for row in result["rows"])


@pytest.mark.integration
def test_geofabrik_parse_downloads_and_ingests_geojson(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    fixture_bytes = Path("tests/fixtures/harvest/geofabrik_payload.json").read_bytes()

    def fake_get(*_args, **_kwargs):
        return FakeDownloadResponse(fixture_bytes)

    monkeypatch.setattr(geofabrik_parse.requests, "get", fake_get)
    territory_config = {
        "geofabrik": {
            "enabled": True,
            "input_path": "",
            "download_url": "https://download.example.com/im_extract.geojson",
        }
    }

    result = run_geofabrik_parse("IM", territory_config, tmp_path, run_id="run-8", run_date="2026-02-17")

    assert result["row_count"] == 1
    assert result["rows"][0]["raw_postcode"] == "JE1 1AA"
    assert result["input_path"].endswith("im_extract.geojson")
    assert Path(result["input_path"]).exists()


@pytest.mark.integration
def test_geofabrik_parse_uses_converted_geojson_for_pbf(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    pbf_path = tmp_path / "im_extract.osm.pbf"
    pbf_path.write_bytes(b"not-a-real-pbf")

    def fake_convert(_input_path: Path, output_geojson: Path, _postcode_candidates: list[str]):
        output_geojson.write_text(
            json.dumps(
                {
                    "elements": [
                        {
                            "type": "node",
                            "id": 701,
                            "lat": 54.15,
                            "lon": -4.48,
                            "tags": {"addr:postcode": "IM1 2AU"},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        return None

    monkeypatch.setattr(geofabrik_parse, "_convert_pbf_to_geojson", fake_convert)
    territory_config = {
        "geofabrik": {
            "enabled": True,
            "input_path": str(pbf_path),
            "download_url": "",
        }
    }

    result = run_geofabrik_parse("IM", territory_config, tmp_path, run_id="run-9", run_date="2026-02-17")

    assert result["row_count"] == 1
    assert result["rows"][0]["raw_postcode"] == "IM1 2AU"
    assert result["warnings"] == []


@pytest.mark.integration
def test_geofabrik_parse_emits_warning_when_download_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    def fail_get(*_args, **_kwargs):
        raise requests.RequestException("down")

    monkeypatch.setattr(geofabrik_parse.requests, "get", fail_get)
    territory_config = {
        "geofabrik": {
            "enabled": True,
            "input_path": "",
            "download_url": "https://download.example.com/im_extract.geojson",
        }
    }

    result = run_geofabrik_parse("IM", territory_config, tmp_path, run_id="run-10", run_date="2026-02-17")

    assert result["row_count"] == 0
    assert "GEOFABRIK_DOWNLOAD_FAILED" in result["warnings"]


@pytest.mark.integration
def test_geofabrik_parse_emits_warning_when_pbf_conversion_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    pbf_path = tmp_path / "im_extract.osm.pbf"
    pbf_path.write_bytes(b"not-a-real-pbf")

    monkeypatch.setattr(geofabrik_parse, "_convert_pbf_to_geojson", lambda *_args, **_kwargs: "GEOFABRIK_PBF_CONVERSION_FAILED")
    territory_config = {
        "geofabrik": {
            "enabled": True,
            "input_path": str(pbf_path),
            "download_url": "",
        }
    }

    result = run_geofabrik_parse("IM", territory_config, tmp_path, run_id="run-11", run_date="2026-02-17")

    assert result["row_count"] == 0
    assert "GEOFABRIK_PBF_CONVERSION_FAILED" in result["warnings"]


@pytest.mark.integration
def test_convert_pbf_warns_when_osmium_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(geofabrik_parse.shutil, "which", lambda _name: None)
    warning = geofabrik_parse._convert_pbf_to_geojson(
        tmp_path / "x.osm.pbf",
        tmp_path / "x.geojson",
        list(geofabrik_parse.DEFAULT_POSTCODE_KEYS),
    )
    assert warning == "GEOFABRIK_OSMIUM_MISSING"


@pytest.mark.integration
def test_convert_pbf_uses_configured_postcode_candidates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    pbf_path = tmp_path / "x.osm.pbf"
    pbf_path.write_bytes(b"pbf")
    out_path = tmp_path / "x.geojson"
    captured: list[list[str]] = []

    monkeypatch.setattr(geofabrik_parse.shutil, "which", lambda _name: "/usr/bin/osmium")

    def fake_run(args, **_kwargs):
        if args[1] == "tags-filter":
            captured.append(args)
            filtered_index = args.index("-o") + 1
            Path(args[filtered_index]).write_bytes(b"filtered")
        elif args[1] == "export":
            export_index = args.index("-o") + 1
            Path(args[export_index]).write_text(json.dumps({"elements": []}), encoding="utf-8")
        return None

    monkeypatch.setattr(geofabrik_parse.subprocess, "run", fake_run)
    warning = geofabrik_parse._convert_pbf_to_geojson(
        pbf_path,
        out_path,
        ["postcode", "POSTCODE", "addr:postcode", "postal_code"],
    )

    assert warning is None
    assert len(captured) == 1
    filter_args = captured[0]
    assert "nwr/addr:postcode" in filter_args
    assert "nwr/postal_code" in filter_args

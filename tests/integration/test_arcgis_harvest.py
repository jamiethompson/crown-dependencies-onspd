from __future__ import annotations

from pathlib import Path

import pytest

from scripts.harvest.arcgis_harvest import run_arcgis_harvest


class FakeHttpClient:
    def __init__(self):
        self.calls: list[tuple[str, dict]] = []

    def get_json(self, url: str, **kwargs):
        self.calls.append((url, kwargs))
        params = kwargs.get("params") or {}

        if params.get("returnIdsOnly") == "true":
            return {"objectIdFieldName": "OBJECTID", "objectIds": [1, 2]}

        if params.get("outSR") == "4326":
            return {"error": {"code": 400, "message": "Unsupported outSR"}}

        return {
            "features": [
                {
                    "attributes": {"OBJECTID": 1, "postcode": "JE2 3AB"},
                    "geometry": {"x": -2.1, "y": 49.2, "spatialReference": {"wkid": 4326}},
                },
                {
                    "attributes": {"OBJECTID": 2, "postcode": "JE3 4CD"},
                    "geometry": {"x": -2.2, "y": 49.3, "spatialReference": {"wkid": 4326}},
                },
            ]
        }

    def close(self):
        return None


@pytest.mark.integration
def test_arcgis_harvest_ids_chunk_and_fallback_without_outsr(tmp_path: Path):
    territory_config = {
        "arcgis": {
            "enabled": True,
            "services": [
                {
                    "name": "jersey_gov_arcgis",
                    "service_url": "https://example.je/arcgis/rest/services/Postcodes/MapServer",
                    "layer_ids": [0],
                    "query_where": "1=1",
                    "id_chunk_size": 500,
                    "out_fields": "*",
                    "source_label": "authoritative",
                }
            ],
        },
        "fields": {
            "postcode_candidates": ["postcode"],
            "lat_candidates": ["lat"],
            "lon_candidates": ["lon"],
        },
    }

    result = run_arcgis_harvest(
        "JE",
        territory_config,
        tmp_path,
        run_id="run-2",
        run_date="2026-02-17",
        http_client=FakeHttpClient(),
    )

    assert result["row_count"] == 2
    assert result["rows"][0]["raw_postcode"] == "JE2 3AB"
    assert result["rows"][0]["source_wkid"] == 4326
    assert (tmp_path / "raw" / "arcgis" / "je_arcgis.json").exists()


@pytest.mark.integration
def test_arcgis_harvest_uses_geometry_when_latlon_fields_are_malformed(tmp_path: Path):
    class FakeMalformedClient:
        def get_json(self, _url: str, **kwargs):
            params = kwargs.get("params") or {}
            if params.get("returnIdsOnly") == "true":
                return {"objectIdFieldName": "OBJECTID", "objectIds": [1]}
            return {
                "features": [
                    {
                        "attributes": {"OBJECTID": 1, "postcode": "JE2 3AB", "lat": "not-a-number", "lon": "x"},
                        "geometry": {"x": -2.11, "y": 49.22, "spatialReference": {"wkid": 4326}},
                    }
                ]
            }

        def close(self):
            return None

    territory_config = {
        "arcgis": {
            "enabled": True,
            "services": [
                {
                    "name": "jersey_gov_arcgis",
                    "service_url": "https://example.je/arcgis/rest/services/Postcodes/MapServer",
                    "layer_ids": [0],
                    "query_where": "1=1",
                    "id_chunk_size": 500,
                    "out_fields": "*",
                    "source_label": "authoritative",
                }
            ],
        },
        "fields": {
            "postcode_candidates": ["postcode"],
            "lat_candidates": ["lat"],
            "lon_candidates": ["lon"],
        },
    }

    result = run_arcgis_harvest(
        "JE",
        territory_config,
        tmp_path,
        run_id="run-2b",
        run_date="2026-02-17",
        http_client=FakeMalformedClient(),
    )

    assert result["rows"][0]["raw_lat"] == 49.22
    assert result["rows"][0]["raw_lon"] == -2.11


@pytest.mark.integration
def test_arcgis_harvest_respects_return_geometry_false(tmp_path: Path):
    class FakeNoGeometryClient:
        def __init__(self):
            self.chunk_params = []

        def get_json(self, _url: str, **kwargs):
            params = kwargs.get("params") or {}
            if params.get("returnIdsOnly") == "true":
                return {"objectIdFieldName": "OBJECTID", "objectIds": [1]}
            self.chunk_params.append(params)
            return {
                "features": [
                    {
                        "attributes": {"OBJECTID": 1, "postcode": "IM1 1AA"},
                    }
                ]
            }

        def close(self):
            return None

    fake = FakeNoGeometryClient()
    territory_config = {
        "arcgis": {
            "enabled": True,
            "services": [
                {
                    "name": "iom_landregistry_public",
                    "service_url": "https://utility.arcgis.com/usrsvcs/servers/x/rest/services/y/MapServer",
                    "layer_ids": [4],
                    "query_where": "1=1",
                    "id_chunk_size": 500,
                    "out_fields": "OBJECTID,Postcode",
                    "source_label": "authoritative",
                    "return_geometry": False,
                }
            ],
        },
        "fields": {
            "postcode_candidates": ["postcode"],
            "lat_candidates": ["lat"],
            "lon_candidates": ["lon"],
        },
    }

    result = run_arcgis_harvest(
        "IM",
        territory_config,
        tmp_path,
        run_id="run-2c",
        run_date="2026-02-17",
        http_client=fake,
    )

    assert result["row_count"] == 1
    assert fake.chunk_params
    assert fake.chunk_params[0]["returnGeometry"] == "false"
    assert result["rows"][0]["raw_geometry"] is None

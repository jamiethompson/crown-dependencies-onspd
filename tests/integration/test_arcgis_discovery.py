from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.discovery.arcgis_discover import run_discovery


class FakeHttpClient:
    def __init__(self, responses: list[dict]):
        self.responses = responses
        self.calls: list[tuple[str, dict]] = []

    def get_json(self, url: str, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses.pop(0)

    def close(self):
        return None


@pytest.mark.integration
def test_arcgis_discovery_fetches_service_and_layer_metadata(tmp_path: Path):
    service_meta = json.loads(Path("tests/fixtures/discovery/arcgis_service_meta.json").read_text(encoding="utf-8"))
    layer_meta = json.loads(Path("tests/fixtures/discovery/arcgis_layer_meta.json").read_text(encoding="utf-8"))
    client = FakeHttpClient([service_meta, layer_meta])

    territory_config = {
        "arcgis": {
            "enabled": True,
            "services": [
                {
                    "name": "jersey_gov_arcgis",
                    "service_url": "https://example.je/arcgis/rest/services/Postcodes/MapServer",
                }
            ],
        }
    }

    result = run_discovery("JE", territory_config, tmp_path, "run-1", http_client=client)

    assert result["services"][0]["name"] == "jersey_gov_arcgis"
    assert result["services"][0]["layers"][0]["metadata"]["name"] == "Postcodes"
    assert (tmp_path / "raw" / "discovery" / "je_discovery.json").exists()

from pathlib import Path

import pytest

from scripts.common.config_loader import load_all_configs, resolve_territories
from scripts.common.errors import ConfigError


def test_load_all_configs_from_repo_config_dir():
    bundle = load_all_configs(Path("config"))
    assert set(bundle.territories) == {"JE", "GY", "IM"}
    assert bundle.onspd_columns["columns"]
    assert "default" in bundle.scoring_rules["profiles"]


def test_resolve_territories():
    assert resolve_territories("all") == ["JE", "GY", "IM"]
    assert resolve_territories("JE") == ["JE"]


def test_load_all_configs_applies_overlay_values(tmp_path: Path):
    base = tmp_path / "base"
    overlay = tmp_path / "overlay"
    base.mkdir()
    overlay.mkdir()

    (base / "jersey.yml").write_text(
        """territory:
  code: JE
  name: Jersey
source_priority: ["a"]
validation:
  bbox_wgs84:
    min_lat: 1
    max_lat: 2
    min_lon: 3
    max_lon: 4
arcgis:
  enabled: false
  services: []
overpass:
  enabled: false
  endpoint: "https://example.test"
  timeout_seconds: 10
  area_strategy: bbox
  bbox: [1, 2, 3, 4]
geofabrik:
  enabled: false
fields:
  postcode_candidates: [postcode]
  lat_candidates: [lat]
  lon_candidates: [lon]
crs: {}
scoring_profile: default
output:
  canonical_filename: jersey.csv
  onspd_filename: jersey_onspd.csv
""",
        encoding="utf-8",
    )
    (base / "guernsey.yml").write_text((base / "jersey.yml").read_text(encoding="utf-8").replace("JE", "GY"), encoding="utf-8")
    (base / "isle_of_man.yml").write_text((base / "jersey.yml").read_text(encoding="utf-8").replace("JE", "IM"), encoding="utf-8")
    (base / "onspd_columns.yml").write_text(
        """version: "1"
null_policy: blank
columns:
  - name: pcd
    type: string
    nullable: false
    source_mapping: normalised_postcode
""",
        encoding="utf-8",
    )
    (base / "scoring_rules.yml").write_text(
        """profiles:
  default:
    rules: []
    clamp:
      min: 0
      max: 100
""",
        encoding="utf-8",
    )

    (overlay / "isle_of_man.yml").write_text(
        """arcgis:
  enabled: true
overpass:
  enabled: true
""",
        encoding="utf-8",
    )

    bundle = load_all_configs(base, overlay_config_dir=overlay)

    assert bundle.territories["IM"]["arcgis"]["enabled"] is True
    assert bundle.territories["IM"]["overpass"]["enabled"] is True
    assert bundle.territories["JE"]["arcgis"]["enabled"] is False


def test_load_all_configs_ignores_empty_overlay_file(tmp_path: Path):
    base = tmp_path / "base"
    overlay = tmp_path / "overlay"
    base.mkdir()
    overlay.mkdir()

    jersey_yaml = """territory:
  code: JE
  name: Jersey
source_priority: ["a"]
validation:
  bbox_wgs84:
    min_lat: 1
    max_lat: 2
    min_lon: 3
    max_lon: 4
arcgis:
  enabled: false
  services: []
overpass:
  enabled: false
  endpoint: "https://example.test"
  timeout_seconds: 10
  area_strategy: bbox
  bbox: [1, 2, 3, 4]
geofabrik:
  enabled: false
fields:
  postcode_candidates: [postcode]
  lat_candidates: [lat]
  lon_candidates: [lon]
crs: {}
scoring_profile: default
output:
  canonical_filename: jersey.csv
  onspd_filename: jersey_onspd.csv
"""
    for filename, code in [("jersey.yml", "JE"), ("guernsey.yml", "GY"), ("isle_of_man.yml", "IM")]:
        (base / filename).write_text(jersey_yaml.replace("JE", code), encoding="utf-8")
    (base / "onspd_columns.yml").write_text(
        """version: "1"
null_policy: blank
columns:
  - name: pcd
    type: string
    nullable: false
    source_mapping: normalised_postcode
""",
        encoding="utf-8",
    )
    (base / "scoring_rules.yml").write_text(
        """profiles:
  default:
    rules: []
    clamp:
      min: 0
      max: 100
""",
        encoding="utf-8",
    )

    (overlay / "isle_of_man.yml").write_text("", encoding="utf-8")
    bundle = load_all_configs(base, overlay_config_dir=overlay)
    assert bundle.territories["IM"]["arcgis"]["enabled"] is False


def test_load_all_configs_rejects_non_mapping_overlay(tmp_path: Path):
    base = tmp_path / "base"
    overlay = tmp_path / "overlay"
    base.mkdir()
    overlay.mkdir()

    minimal = """territory:
  code: JE
  name: Jersey
source_priority: ["a"]
validation:
  bbox_wgs84:
    min_lat: 1
    max_lat: 2
    min_lon: 3
    max_lon: 4
arcgis:
  enabled: false
  services: []
overpass:
  enabled: false
  endpoint: "https://example.test"
  timeout_seconds: 10
  area_strategy: bbox
  bbox: [1, 2, 3, 4]
geofabrik:
  enabled: false
fields:
  postcode_candidates: [postcode]
  lat_candidates: [lat]
  lon_candidates: [lon]
crs: {}
scoring_profile: default
output:
  canonical_filename: jersey.csv
  onspd_filename: jersey_onspd.csv
"""
    for filename, code in [("jersey.yml", "JE"), ("guernsey.yml", "GY"), ("isle_of_man.yml", "IM")]:
        (base / filename).write_text(minimal.replace("JE", code), encoding="utf-8")
    (base / "onspd_columns.yml").write_text(
        """version: "1"
null_policy: blank
columns:
  - name: pcd
    type: string
    nullable: false
    source_mapping: normalised_postcode
""",
        encoding="utf-8",
    )
    (base / "scoring_rules.yml").write_text(
        """profiles:
  default:
    rules: []
    clamp:
      min: 0
      max: 100
""",
        encoding="utf-8",
    )

    (overlay / "isle_of_man.yml").write_text("- not\n- a\n- mapping\n", encoding="utf-8")
    with pytest.raises(ConfigError):
        load_all_configs(base, overlay_config_dir=overlay)

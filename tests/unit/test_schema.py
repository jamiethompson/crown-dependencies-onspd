import pytest

from scripts.common.errors import ConfigError
from scripts.common.schema import validate_onspd_columns_config, validate_territory_config


BASE_TERRITORY = {
    "territory": {"code": "JE", "name": "Jersey"},
    "source_priority": ["a"],
    "validation": {"bbox_wgs84": {"min_lat": 1, "max_lat": 2, "min_lon": 3, "max_lon": 4}},
    "arcgis": {"enabled": True, "services": []},
    "overpass": {"enabled": True, "endpoint": "x", "timeout_seconds": 10, "area_strategy": "bbox"},
    "geofabrik": {"enabled": True},
    "fields": {"postcode_candidates": [], "lat_candidates": [], "lon_candidates": []},
    "crs": {},
    "scoring_profile": "default",
    "output": {"canonical_filename": "a.csv", "onspd_filename": "b.csv"},
}


def test_validate_territory_config_accepts_valid_shape():
    validated = validate_territory_config(dict(BASE_TERRITORY))
    assert validated["territory"]["code"] == "JE"


def test_validate_territory_config_rejects_unknown_key_by_default():
    bad = dict(BASE_TERRITORY)
    bad["unexpected"] = True
    with pytest.raises(ConfigError):
        validate_territory_config(bad)


def test_validate_territory_config_allows_unknown_when_enabled():
    okay = dict(BASE_TERRITORY)
    okay["extra"] = 1
    validate_territory_config(okay, allow_unknown=True)


def test_validate_onspd_columns_rejects_duplicate_names():
    cfg = {
        "version": "1",
        "null_policy": "blank",
        "columns": [
            {"name": "pcd", "type": "string", "nullable": False, "source_mapping": "x"},
            {"name": "pcd", "type": "string", "nullable": True, "source_mapping": "y"},
        ],
    }
    with pytest.raises(ConfigError):
        validate_onspd_columns_config(cfg)

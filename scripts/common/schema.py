"""Minimal strict schemas for YAML config validation."""

from __future__ import annotations

from dataclasses import dataclass

from scripts.common.errors import ConfigError


@dataclass(frozen=True)
class SchemaOptions:
    allow_unknown: bool = False


def _assert_required_keys(obj: dict, required: set[str], ctx: str) -> None:
    missing = required - set(obj)
    if missing:
        missing_str = ", ".join(sorted(missing))
        raise ConfigError(f"Missing keys in {ctx}: {missing_str}")


def _assert_no_unknown_keys(obj: dict, known: set[str], ctx: str, allow_unknown: bool) -> None:
    if allow_unknown:
        return
    unknown = set(obj) - known
    if unknown:
        unknown_str = ", ".join(sorted(unknown))
        raise ConfigError(f"Unknown keys in {ctx}: {unknown_str}")


def validate_territory_config(cfg: dict, *, allow_unknown: bool = False) -> dict:
    top_required = {
        "territory",
        "source_priority",
        "validation",
        "arcgis",
        "overpass",
        "geofabrik",
        "fields",
        "crs",
        "scoring_profile",
        "output",
    }
    top_known = top_required
    _assert_required_keys(cfg, top_required, "territory config")
    _assert_no_unknown_keys(cfg, top_known, "territory config", allow_unknown)

    _assert_required_keys(cfg["territory"], {"code", "name"}, "territory")
    _assert_required_keys(cfg["validation"], {"bbox_wgs84"}, "validation")
    _assert_required_keys(
        cfg["validation"]["bbox_wgs84"],
        {"min_lat", "max_lat", "min_lon", "max_lon"},
        "validation.bbox_wgs84",
    )
    _assert_required_keys(cfg["arcgis"], {"enabled", "services"}, "arcgis")
    _assert_required_keys(
        cfg["overpass"],
        {"enabled", "endpoint", "timeout_seconds", "area_strategy"},
        "overpass",
    )
    _assert_required_keys(cfg["geofabrik"], {"enabled"}, "geofabrik")
    _assert_required_keys(
        cfg["fields"],
        {"postcode_candidates", "lat_candidates", "lon_candidates"},
        "fields",
    )
    _assert_required_keys(cfg["crs"], set(), "crs")
    _assert_required_keys(cfg["output"], {"canonical_filename", "onspd_filename"}, "output")

    return cfg


def validate_onspd_columns_config(cfg: dict) -> dict:
    _assert_required_keys(cfg, {"version", "null_policy", "columns"}, "onspd_columns")
    if not isinstance(cfg["columns"], list) or not cfg["columns"]:
        raise ConfigError("onspd_columns.columns must be a non-empty list")

    names: list[str] = []
    for idx, col in enumerate(cfg["columns"]):
        _assert_required_keys(col, {"name", "type", "nullable", "source_mapping"}, f"columns[{idx}]")
        names.append(col["name"])

    dupes = {name for name in names if names.count(name) > 1}
    if dupes:
        raise ConfigError(f"Duplicate ONSPD columns: {', '.join(sorted(dupes))}")

    return cfg


def validate_scoring_config(cfg: dict) -> dict:
    _assert_required_keys(cfg, {"profiles"}, "scoring_rules")
    if not isinstance(cfg["profiles"], dict) or not cfg["profiles"]:
        raise ConfigError("scoring_rules.profiles must be a non-empty mapping")
    return cfg

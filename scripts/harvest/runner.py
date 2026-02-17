"""Harvest orchestration with fail-soft semantics."""

from __future__ import annotations

from pathlib import Path

from scripts.common.errors import StageError
from scripts.harvest.arcgis_harvest import run_arcgis_harvest
from scripts.harvest.geofabrik_parse import run_geofabrik_parse
from scripts.harvest.overpass_harvest import run_overpass_harvest


def run_harvest_for_territory(
    territory_code: str,
    territory_config: dict,
    data_dir: Path,
    run_id: str,
    run_date: str,
) -> dict:
    failures: list[str] = []
    results: dict[str, dict] = {}

    enabled_sources = []
    if territory_config["arcgis"]["enabled"]:
        enabled_sources.append("arcgis")
    if territory_config["overpass"]["enabled"]:
        enabled_sources.append("overpass")
    if territory_config["geofabrik"]["enabled"]:
        enabled_sources.append("geofabrik")

    try:
        results["arcgis"] = run_arcgis_harvest(
            territory_code,
            territory_config,
            data_dir,
            run_id,
            run_date,
        )
    except Exception:
        failures.append("arcgis")

    try:
        results["overpass"] = run_overpass_harvest(
            territory_code,
            territory_config,
            data_dir,
            run_id,
            run_date,
        )
    except Exception:
        failures.append("overpass")

    try:
        results["geofabrik"] = run_geofabrik_parse(
            territory_code,
            territory_config,
            data_dir,
            run_id,
            run_date,
        )
    except Exception:
        failures.append("geofabrik")

    if enabled_sources and len(failures) >= len(enabled_sources):
        raise StageError(f"All enabled sources failed for territory {territory_code}")

    return {
        "territory": territory_code,
        "run_id": run_id,
        "results": results,
        "failed_sources": failures,
    }

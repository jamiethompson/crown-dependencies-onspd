"""ArcGIS harvest stage entrypoint."""

from __future__ import annotations

from pathlib import Path

from scripts.common.fs import ensure_dir, write_json


def run_arcgis_harvest(territory_code: str, territory_config: dict, data_dir: Path, run_id: str) -> dict:
    out_dir = data_dir / "raw" / "arcgis"
    ensure_dir(out_dir)
    payload = {
        "territory": territory_code,
        "run_id": run_id,
        "source": "arcgis",
        "enabled": territory_config["arcgis"]["enabled"],
        "rows": [],
    }
    write_json(out_dir / f"{territory_code.lower()}_arcgis.json", payload)
    return payload

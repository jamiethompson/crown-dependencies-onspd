"""Geofabrik parser stage entrypoint."""

from __future__ import annotations

from pathlib import Path

from scripts.common.fs import ensure_dir, write_json


def run_geofabrik_parse(territory_code: str, territory_config: dict, data_dir: Path, run_id: str) -> dict:
    out_dir = data_dir / "raw" / "osm" / "geofabrik"
    ensure_dir(out_dir)
    payload = {
        "territory": territory_code,
        "run_id": run_id,
        "source": "geofabrik",
        "enabled": territory_config["geofabrik"]["enabled"],
        "rows": [],
    }
    write_json(out_dir / f"{territory_code.lower()}_geofabrik.json", payload)
    return payload

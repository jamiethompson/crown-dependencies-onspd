"""Overpass harvest stage entrypoint."""

from __future__ import annotations

from pathlib import Path

from scripts.common.fs import ensure_dir, write_json


def run_overpass_harvest(territory_code: str, territory_config: dict, data_dir: Path, run_id: str) -> dict:
    out_dir = data_dir / "raw" / "osm" / "overpass"
    ensure_dir(out_dir)
    payload = {
        "territory": territory_code,
        "run_id": run_id,
        "source": "overpass",
        "enabled": territory_config["overpass"]["enabled"],
        "rows": [],
    }
    write_json(out_dir / f"{territory_code.lower()}_overpass.json", payload)
    return payload

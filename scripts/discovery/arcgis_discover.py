"""ArcGIS discovery stage entrypoint."""

from __future__ import annotations

from pathlib import Path

from scripts.common.fs import ensure_dir, write_json


def run_discovery(territory_code: str, territory_config: dict, data_dir: Path, run_id: str) -> dict:
    out_dir = data_dir / "raw" / "discovery"
    ensure_dir(out_dir)
    payload = {
        "territory": territory_code,
        "run_id": run_id,
        "services": territory_config["arcgis"]["services"],
        "status": "stub_discovery",
    }
    write_json(out_dir / f"{territory_code.lower()}_discovery.json", payload)
    return payload

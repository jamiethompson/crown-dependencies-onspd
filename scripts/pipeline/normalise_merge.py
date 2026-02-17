"""Normalise and merge stage placeholder."""

from __future__ import annotations

from pathlib import Path

from scripts.common.fs import ensure_dir, write_json


def run_normalise_merge(territory_code: str, data_dir: Path, run_id: str) -> dict:
    out_dir = data_dir / "intermediate"
    ensure_dir(out_dir)
    payload = {
        "territory": territory_code,
        "run_id": run_id,
        "status": "stub_merge",
        "rows": [],
    }
    write_json(out_dir / f"{territory_code.lower()}_canonical.json", payload)
    return payload

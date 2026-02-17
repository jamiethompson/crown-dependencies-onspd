"""ONSPD mapping stage placeholder."""

from __future__ import annotations

from pathlib import Path

from scripts.common.fs import write_csv


def run_map_onspd(territory_code: str, territory_config: dict, onspd_columns: dict, data_dir: Path) -> Path:
    output_name = territory_config["output"]["onspd_filename"]
    out_path = data_dir / "out" / output_name
    headers = [column["name"] for column in onspd_columns["columns"]]
    write_csv(out_path, headers, [])
    return out_path

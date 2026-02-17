"""Run report aggregation."""

from __future__ import annotations

from pathlib import Path

from scripts.common.fs import write_json


def write_run_summary(data_dir: Path, run_id: str, run_date: str, territories: list[str]) -> Path:
    summary_path = data_dir / "out" / "reports" / "run_summary.json"
    payload = {
        "run_id": run_id,
        "run_date": run_date,
        "territories": territories,
        "status": "success",
    }
    write_json(summary_path, payload)
    return summary_path

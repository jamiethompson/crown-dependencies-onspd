"""Temporal tracking for first_seen / last_seen fields."""

from __future__ import annotations

import csv
from pathlib import Path

from scripts.common.fs import ensure_dir, read_json, write_json


def _load_previous_from_csv(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        key = row.get("normalised_postcode")
        if not key:
            continue
        out[key] = {
            "first_seen": row.get("first_seen", ""),
            "last_seen": row.get("last_seen", ""),
        }
    return out


def _load_previous_from_state(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    payload = read_json(path)
    return payload.get("postcodes", {})


def apply_temporal_tracking(
    rows: list[dict],
    *,
    territory_code: str,
    canonical_output_path: Path,
    state_path: Path,
    run_date: str,
) -> tuple[list[dict], dict]:
    previous = _load_previous_from_csv(canonical_output_path)
    if not previous:
        previous = _load_previous_from_state(state_path)

    current_keys = {row["normalised_postcode"] for row in rows}
    previous_keys = set(previous)

    for row in rows:
        key = row["normalised_postcode"]
        prior = previous.get(key)
        if prior and prior.get("first_seen"):
            row["first_seen"] = prior["first_seen"]
        else:
            row["first_seen"] = run_date
        row["last_seen"] = run_date

    ensure_dir(state_path.parent)
    state_payload = {
        "territory": territory_code,
        "postcodes": {
            row["normalised_postcode"]: {
                "first_seen": row["first_seen"],
                "last_seen": row["last_seen"],
            }
            for row in rows
        },
    }
    write_json(state_path, state_payload)

    stats = {
        "disappeared_count": len(previous_keys - current_keys),
    }
    return rows, stats

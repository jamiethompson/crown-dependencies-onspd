"""Map canonical rows to strict ONSPD-compatible CSV contract."""

from __future__ import annotations

import csv
from pathlib import Path

from scripts.common.errors import ContractError
from scripts.common.fs import write_csv


def _read_canonical_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _value_for_mapping(mapping: str, canonical_row: dict, territory_code: str) -> str:
    if mapping == "normalised_postcode":
        return canonical_row.get("normalised_postcode", "")
    if mapping == "normalised_postcode_no_space":
        return canonical_row.get("normalised_postcode", "").replace(" ", "")
    if mapping == "country_code_or_blank":
        return territory_code
    if mapping in canonical_row:
        return canonical_row.get(mapping, "")
    if mapping == "blank":
        return ""
    raise ContractError(f"Missing mapped column definition for source_mapping={mapping}")


def _verify_header(path: Path, expected_header: list[str]) -> None:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        actual = next(reader)
    if actual != expected_header:
        raise ContractError(f"ONSPD header/order mismatch: {actual} != {expected_header}")


def _compute_fill_rates(rows: list[dict], header: list[str]) -> list[dict]:
    stats = []
    total = len(rows)
    for column in header:
        filled_count = sum(1 for row in rows if row.get(column, "") not in ("", None))
        null_count = total - filled_count
        fill_percent = 0.0 if total == 0 else round((filled_count / total) * 100, 2)
        stats.append(
            {
                "column": column,
                "filled": filled_count,
                "null": null_count,
                "fill_percent": fill_percent,
            }
        )
    return stats


def run_map_onspd(
    territory_code: str,
    territory_config: dict,
    onspd_columns: dict,
    data_dir: Path,
) -> dict:
    columns = onspd_columns.get("columns", [])
    header = [column["name"] for column in columns]

    if len(header) != len(set(header)):
        raise ContractError("Duplicate header names in onspd_columns config")

    canonical_path = data_dir / "out" / territory_config["output"]["canonical_filename"]
    canonical_rows = _read_canonical_rows(canonical_path)

    out_rows: list[dict] = []
    for canonical_row in canonical_rows:
        out_row = {}
        for column in columns:
            name = column["name"]
            mapping = column["source_mapping"]
            value = _value_for_mapping(mapping, canonical_row, territory_code)
            out_row[name] = "" if value is None else value
        out_rows.append(out_row)

    out_path = data_dir / "out" / territory_config["output"]["onspd_filename"]
    write_csv(out_path, header, out_rows)
    _verify_header(out_path, header)

    return {
        "path": str(out_path),
        "rows": len(out_rows),
        "fill_rates": _compute_fill_rates(out_rows, header),
        "header": header,
    }

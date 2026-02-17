"""Geofabrik parser stage implementation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from scripts.common.fs import ensure_dir, write_json
from scripts.common.models import RawRecord


def _iter_elements_from_json(path: Path) -> Iterable[dict]:
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if isinstance(payload, dict) and "elements" in payload:
        yield from payload["elements"]
    elif isinstance(payload, list):
        yield from payload
    else:
        raise ValueError(f"Unsupported Geofabrik JSON payload shape at {path}")


def run_geofabrik_parse(
    territory_code: str,
    territory_config: dict,
    data_dir: Path,
    run_id: str,
    run_date: str,
) -> dict:
    out_dir = data_dir / "raw" / "osm" / "geofabrik"
    ensure_dir(out_dir)

    geofabrik_cfg = territory_config["geofabrik"]
    if not geofabrik_cfg["enabled"]:
        payload = {
            "territory": territory_code,
            "run_id": run_id,
            "source": "geofabrik",
            "enabled": False,
            "rows": [],
            "warnings": [],
        }
        write_json(out_dir / f"{territory_code.lower()}_geofabrik.json", payload)
        return payload

    pbf_path = (geofabrik_cfg.get("pbf_path") or "").strip()
    warnings: list[str] = []
    rows: list[RawRecord] = []

    if not pbf_path:
        warnings.append("GEOFABRIK_INPUT_PATH_MISSING")
    else:
        input_path = Path(pbf_path)
        if not input_path.exists():
            warnings.append("GEOFABRIK_INPUT_NOT_FOUND")
        elif input_path.suffix.lower() not in {".json", ".geojson"}:
            warnings.append("GEOFABRIK_PARSE_REQUIRES_PRECONVERTED_JSON")
        else:
            for element in _iter_elements_from_json(input_path):
                tags = element.get("tags") or {}
                raw_postcode = tags.get("addr:postcode")
                if raw_postcode in (None, ""):
                    continue

                lat = element.get("lat")
                lon = element.get("lon")
                center = element.get("center") or {}
                if lat is None:
                    lat = center.get("lat")
                if lon is None:
                    lon = center.get("lon")

                rows.append(
                    RawRecord(
                        territory=territory_code,
                        source_name="osm_geofabrik",
                        source_class="osm",
                        source_record_id=f"{element.get('type')}/{element.get('id')}",
                        raw_postcode=str(raw_postcode),
                        raw_lat=float(lat) if lat is not None else None,
                        raw_lon=float(lon) if lon is not None else None,
                        raw_geometry=None,
                        source_wkid=4326,
                        extract_date=run_date,
                        run_id=run_id,
                        raw_payload_ref=f"raw/osm/geofabrik/{territory_code.lower()}_geofabrik.json",
                    )
                )

    payload = {
        "territory": territory_code,
        "run_id": run_id,
        "source": "geofabrik",
        "enabled": True,
        "row_count": len(rows),
        "rows": [row.to_dict() for row in rows],
        "warnings": warnings,
    }
    write_json(out_dir / f"{territory_code.lower()}_geofabrik.json", payload)
    return payload

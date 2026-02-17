"""Geofabrik parser stage implementation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Iterator

from scripts.common.fs import ensure_dir, write_json
from scripts.common.models import RawRecord


DEFAULT_POSTCODE_KEYS = (
    "addr:postcode",
    "postcode",
    "POSTCODE",
    "Postcode",
    "PostCode",
    "post_code",
    "postal_code",
    "postalcode",
)


def _iter_elements_from_json(path: Path) -> Iterable[dict]:
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if isinstance(payload, dict) and "elements" in payload:
        yield from payload["elements"]
    elif isinstance(payload, dict) and payload.get("type") == "FeatureCollection":
        yield from payload.get("features", [])
    elif isinstance(payload, list):
        yield from payload
    else:
        raise ValueError(f"Unsupported Geofabrik JSON payload shape at {path}")


def _lookup_first(mapping: dict, candidates: list[str]) -> object | None:
    for key in candidates:
        if key in mapping and mapping[key] not in (None, ""):
            return mapping[key]
    return None


def _iter_geojson_points(coordinates: object) -> Iterator[tuple[float, float]]:
    if not isinstance(coordinates, list):
        return
    if len(coordinates) >= 2 and all(isinstance(v, (int, float)) for v in coordinates[:2]):
        yield float(coordinates[0]), float(coordinates[1])
        return
    for item in coordinates:
        yield from _iter_geojson_points(item)


def _geojson_lat_lon(geometry: dict | None) -> tuple[float | None, float | None]:
    if not geometry:
        return None, None
    coordinates = geometry.get("coordinates")
    points = list(_iter_geojson_points(coordinates))
    if not points:
        return None, None
    lon = sum(point[0] for point in points) / len(points)
    lat = sum(point[1] for point in points) / len(points)
    return lat, lon


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
    postcode_candidates = list(
        dict.fromkeys((territory_config.get("fields", {}).get("postcode_candidates") or []) + list(DEFAULT_POSTCODE_KEYS))
    )

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
                properties = element.get("properties") or {}
                raw_postcode = _lookup_first(tags, postcode_candidates) or _lookup_first(properties, postcode_candidates)
                if raw_postcode in (None, ""):
                    continue

                lat = element.get("lat")
                lon = element.get("lon")
                center = element.get("center") or {}
                if lat is None:
                    lat = center.get("lat")
                if lon is None:
                    lon = center.get("lon")
                if lat is None or lon is None:
                    geojson_lat, geojson_lon = _geojson_lat_lon(element.get("geometry"))
                    if lat is None:
                        lat = geojson_lat
                    if lon is None:
                        lon = geojson_lon

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

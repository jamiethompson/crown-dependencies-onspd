"""Coordinate extraction, transformation, and precedence resolution."""

from __future__ import annotations

from typing import Any

from pyproj import CRS, Transformer

SOURCE_CLASS_PRECEDENCE = {
    "authoritative": 3,
    "digimap": 2,
    "osm": 1,
    "other": 0,
}


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _valid_lat_lon(lat: float | None, lon: float | None) -> bool:
    if lat is None or lon is None:
        return False
    return -90 <= lat <= 90 and -180 <= lon <= 180


def _within_bbox(lat: float, lon: float, bbox: dict) -> bool:
    return (
        bbox["min_lat"] <= lat <= bbox["max_lat"]
        and bbox["min_lon"] <= lon <= bbox["max_lon"]
    )


def _transform_to_wgs84(lat: float, lon: float, source_epsg: int) -> tuple[float, float] | None:
    if source_epsg == 4326:
        return lat, lon
    try:
        transformer = Transformer.from_crs(CRS.from_epsg(source_epsg), CRS.from_epsg(4326), always_xy=True)
        transformed_lon, transformed_lat = transformer.transform(lon, lat)
        return transformed_lat, transformed_lon
    except Exception:
        return None


def resolve_best_coordinate(records: list[dict], territory_config: dict) -> dict:
    bbox = territory_config["validation"]["bbox_wgs84"]
    default_epsg = territory_config.get("crs", {}).get("default_epsg")
    hint_epsg = territory_config.get("crs", {}).get("authoritative_epsg_hint_by_source", {})
    source_priority = {name: idx for idx, name in enumerate(territory_config.get("source_priority", []))}

    candidates: list[dict] = []
    unknown_crs = False
    had_outlier = False

    for record in records:
        raw_lat = _safe_float(record.get("raw_lat"))
        raw_lon = _safe_float(record.get("raw_lon"))
        if raw_lat is None or raw_lon is None:
            continue

        wkid = record.get("source_wkid")
        if wkid is None:
            wkid = hint_epsg.get(record.get("source_name"), default_epsg)

        if wkid is None:
            unknown_crs = True
            continue

        transformed = _transform_to_wgs84(raw_lat, raw_lon, int(wkid))
        if transformed is None:
            unknown_crs = True
            continue
        lat, lon = transformed

        if not _valid_lat_lon(lat, lon):
            continue

        if not _within_bbox(lat, lon, bbox):
            had_outlier = True
            continue

        candidates.append(
            {
                "lat": lat,
                "lon": lon,
                "source_class": record.get("source_class", "other"),
                "source_name": record.get("source_name", ""),
                "source_record_id": record.get("source_record_id") or "",
            }
        )

    if not candidates:
        notes = []
        if had_outlier:
            notes.append("COORDINATE_OUTLIER")
        if unknown_crs:
            notes.append("COORDINATE_CRS_UNKNOWN")
        return {
            "has_coordinates": False,
            "lat": None,
            "lon": None,
            "coordinate_source": None,
            "notes": notes,
        }

    def _sort_key(candidate: dict):
        precedence = SOURCE_CLASS_PRECEDENCE.get(candidate["source_class"], 0)
        source_index = source_priority.get(candidate["source_name"], 9999)
        return (-precedence, source_index, candidate["source_name"], candidate["source_record_id"])

    chosen = sorted(candidates, key=_sort_key)[0]

    return {
        "has_coordinates": True,
        "lat": chosen["lat"],
        "lon": chosen["lon"],
        "coordinate_source": chosen["source_class"],
        "notes": [],
    }

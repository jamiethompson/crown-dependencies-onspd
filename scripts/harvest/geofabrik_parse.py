"""Geofabrik parser stage implementation."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import Iterable, Iterator
from urllib.parse import urlparse

import requests

from scripts.common.constants import USER_AGENT
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


def _download_filename(download_url: str, territory_code: str, run_date: str) -> str:
    parsed = urlparse(download_url)
    basename = Path(parsed.path).name
    if basename:
        return basename
    return f"{territory_code.lower()}_{run_date}.osm.pbf"


def _download_input(download_url: str, target_path: Path) -> None:
    ensure_dir(target_path.parent)
    response = requests.get(
        download_url,
        headers={"User-Agent": USER_AGENT, "Accept": "*/*"},
        timeout=(20, 120),
        stream=True,
    )
    response.raise_for_status()
    with target_path.open("wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 128):
            if chunk:
                f.write(chunk)


def _pbf_filter_tags(postcode_candidates: list[str]) -> list[str]:
    tags = [f"nwr/{tag}" for tag in postcode_candidates if ":" in tag or "_" in tag]
    return tags or [f"nwr/{tag}" for tag in DEFAULT_POSTCODE_KEYS]


def _convert_pbf_to_geojson(input_path: Path, output_geojson: Path, postcode_candidates: list[str]) -> str | None:
    osmium_path = shutil.which("osmium")
    if osmium_path is None:
        return "GEOFABRIK_OSMIUM_MISSING"

    filtered_pbf = output_geojson.with_suffix(".filtered.osm.pbf")
    try:
        subprocess.run(
            [
                osmium_path,
                "tags-filter",
                str(input_path),
                *_pbf_filter_tags(postcode_candidates),
                "-o",
                str(filtered_pbf),
                "-O",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        subprocess.run(
            [
                osmium_path,
                "export",
                str(filtered_pbf),
                "-o",
                str(output_geojson),
                "-f",
                "geojson",
                "-O",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError:
        return "GEOFABRIK_PBF_CONVERSION_FAILED"
    finally:
        if filtered_pbf.exists():
            filtered_pbf.unlink()

    return None


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

    configured_input_path = (geofabrik_cfg.get("input_path") or geofabrik_cfg.get("pbf_path") or "").strip()
    download_url = (geofabrik_cfg.get("download_url") or "").strip()
    warnings: list[str] = []
    rows: list[RawRecord] = []
    postcode_candidates = list(
        dict.fromkeys((territory_config.get("fields", {}).get("postcode_candidates") or []) + list(DEFAULT_POSTCODE_KEYS))
    )
    input_path: Path | None = None

    if configured_input_path:
        input_path = Path(configured_input_path)
    elif download_url:
        filename = _download_filename(download_url, territory_code, run_date)
        downloaded_path = out_dir / filename
        try:
            _download_input(download_url, downloaded_path)
            input_path = downloaded_path
        except requests.RequestException:
            warnings.append("GEOFABRIK_DOWNLOAD_FAILED")
    else:
        warnings.append("GEOFABRIK_INPUT_PATH_MISSING")

    if input_path is not None:
        if not input_path.exists():
            warnings.append("GEOFABRIK_INPUT_NOT_FOUND")
        else:
            parse_path = input_path
            suffix = input_path.suffix.lower()
            if suffix == ".pbf":
                parse_path = out_dir / f"{territory_code.lower()}_{run_id}_geofabrik_export.geojson"
                conversion_warning = _convert_pbf_to_geojson(input_path, parse_path, postcode_candidates)
                if conversion_warning:
                    warnings.append(conversion_warning)
                    parse_path = None
            elif suffix not in {".json", ".geojson"}:
                warnings.append("GEOFABRIK_UNSUPPORTED_INPUT_FORMAT")
                parse_path = None

            if parse_path is not None:
                if not parse_path.exists():
                    warnings.append("GEOFABRIK_INPUT_NOT_FOUND")
                else:
                    for element in _iter_elements_from_json(parse_path):
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
        "input_path": str(input_path) if input_path is not None else None,
        "row_count": len(rows),
        "rows": [row.to_dict() for row in rows],
        "warnings": warnings,
    }
    write_json(out_dir / f"{territory_code.lower()}_geofabrik.json", payload)
    return payload

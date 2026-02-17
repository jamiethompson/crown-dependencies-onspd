"""ArcGIS harvest stage implementation."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

from scripts.common.arcgis_hosts import resolve_arcgis_service_url
from scripts.common.fs import ensure_dir, write_json
from scripts.common.http import HttpClient, HttpRequestError, TimeoutConfig
from scripts.common.models import RawRecord


def _layer_url(service_url: str, layer_id: int) -> str:
    parsed = urlparse(service_url)
    suffix = parsed.path.rstrip("/").split("/")[-1]
    if suffix.isdigit():
        return service_url.rstrip("/")
    return f"{service_url.rstrip('/')}/{layer_id}"


def _lookup_first(attributes: dict, candidates: list[str]) -> object | None:
    for key in candidates:
        if key in attributes and attributes[key] not in (None, ""):
            return attributes[key]
    return None


def _source_class(source_label: str) -> str:
    if source_label in {"authoritative", "digimap", "osm"}:
        return source_label
    return "other"


def _parse_wkid(geometry: dict | None) -> int | None:
    if not geometry:
        return None
    spatial_ref = geometry.get("spatialReference") or {}
    wkid = spatial_ref.get("latestWkid") or spatial_ref.get("wkid")
    if wkid is None:
        return None
    try:
        return int(wkid)
    except (TypeError, ValueError):
        return None


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _fetch_ids(client: HttpClient, layer_url: str, where: str) -> tuple[str | None, list[int]]:
    payload = client.get_json(
        f"{layer_url}/query",
        source_type="arcgis",
        params={"where": where, "returnIdsOnly": "true", "f": "json"},
        timeout=TimeoutConfig(connect=20, read=120),
    )
    if "error" in payload:
        raise HttpRequestError(f"ArcGIS ID query failed for {layer_url}: {payload['error']}")

    object_id_field = payload.get("objectIdFieldName")
    object_ids = payload.get("objectIds") or []
    return object_id_field, [int(v) for v in object_ids]


def _fetch_chunk(client: HttpClient, layer_url: str, object_ids: list[int], out_fields: str) -> dict:
    params = {
        "objectIds": ",".join(str(i) for i in object_ids),
        "outFields": out_fields,
        "returnGeometry": "true",
        "outSR": "4326",
        "f": "json",
    }
    if hasattr(client, "post_form_json"):
        payload = client.post_form_json(
            f"{layer_url}/query",
            source_type="arcgis",
            data=params,
            timeout=TimeoutConfig(connect=20, read=120),
        )
    else:
        payload = client.get_json(
            f"{layer_url}/query",
            source_type="arcgis",
            params=params,
            timeout=TimeoutConfig(connect=20, read=120),
        )

    if "error" in payload:
        # Some endpoints reject outSR. Retry once without outSR.
        params.pop("outSR", None)
        if hasattr(client, "post_form_json"):
            payload = client.post_form_json(
                f"{layer_url}/query",
                source_type="arcgis",
                data=params,
                timeout=TimeoutConfig(connect=20, read=120),
            )
        else:
            payload = client.get_json(
                f"{layer_url}/query",
                source_type="arcgis",
                params=params,
                timeout=TimeoutConfig(connect=20, read=120),
            )

    if "error" in payload:
        raise HttpRequestError(f"ArcGIS chunk query failed for {layer_url}: {payload['error']}")

    return payload


def _chunked(values: list[int], size: int):
    for i in range(0, len(values), size):
        yield values[i : i + size]


def run_arcgis_harvest(
    territory_code: str,
    territory_config: dict,
    data_dir: Path,
    run_id: str,
    run_date: str,
    http_client: HttpClient | None = None,
) -> dict:
    out_dir = data_dir / "raw" / "arcgis"
    ensure_dir(out_dir)

    if not territory_config["arcgis"]["enabled"]:
        payload = {
            "territory": territory_code,
            "run_id": run_id,
            "source": "arcgis",
            "enabled": False,
            "rows": [],
        }
        write_json(out_dir / f"{territory_code.lower()}_arcgis.json", payload)
        return payload

    postcode_candidates = territory_config["fields"]["postcode_candidates"]
    lat_candidates = territory_config["fields"]["lat_candidates"]
    lon_candidates = territory_config["fields"]["lon_candidates"]

    rows: list[RawRecord] = []

    owns_client = http_client is None
    client = http_client or HttpClient()
    try:
        for service in territory_config["arcgis"]["services"]:
            resolved = resolve_arcgis_service_url(service["service_url"].rstrip("/"), client)
            service_url = resolved.resolved_url.rstrip("/")
            source_name = service["name"]
            source_label = service.get("source_label", "other")
            layer_ids = service.get("layer_ids") or [0]
            where = service.get("query_where", "1=1")
            id_chunk_size = int(service.get("id_chunk_size", 500))
            out_fields = service.get("out_fields", "*")

            for layer_id in layer_ids:
                layer_url = _layer_url(service_url, int(layer_id))
                object_id_field_name, object_ids = _fetch_ids(client, layer_url, where)
                if not object_ids:
                    continue

                for chunk_ids in _chunked(object_ids, id_chunk_size):
                    chunk_payload = _fetch_chunk(client, layer_url, chunk_ids, out_fields)
                    features = chunk_payload.get("features") or []

                    for feature in features:
                        attributes = feature.get("attributes") or {}
                        geometry = feature.get("geometry") or None
                        source_id = None
                        if object_id_field_name and object_id_field_name in attributes:
                            source_id = str(attributes[object_id_field_name])
                        raw_postcode = _lookup_first(attributes, postcode_candidates)
                        raw_lat = _safe_float(_lookup_first(attributes, lat_candidates))
                        raw_lon = _safe_float(_lookup_first(attributes, lon_candidates))

                        if (raw_lat is None or raw_lon is None) and geometry:
                            geom_x = geometry.get("x")
                            geom_y = geometry.get("y")
                            if geom_x is not None and geom_y is not None:
                                raw_lat = _safe_float(geom_y)
                                raw_lon = _safe_float(geom_x)

                        record = RawRecord(
                            territory=territory_code,
                            source_name=source_name,
                            source_class=_source_class(source_label),
                            source_record_id=source_id,
                            raw_postcode=str(raw_postcode) if raw_postcode is not None else None,
                            raw_lat=raw_lat,
                            raw_lon=raw_lon,
                            raw_geometry=geometry,
                            source_wkid=_parse_wkid(geometry),
                            extract_date=run_date,
                            run_id=run_id,
                            raw_payload_ref=f"raw/arcgis/{territory_code.lower()}_arcgis.json",
                        )
                        rows.append(record)
    finally:
        if owns_client:
            client.close()

    payload = {
        "territory": territory_code,
        "run_id": run_id,
        "source": "arcgis",
        "enabled": True,
        "row_count": len(rows),
        "rows": [row.to_dict() for row in rows],
    }
    write_json(out_dir / f"{territory_code.lower()}_arcgis.json", payload)
    return payload

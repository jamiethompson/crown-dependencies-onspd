"""Overpass harvest stage implementation."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

from scripts.common.fs import ensure_dir, write_json
from scripts.common.http import HttpClient, TimeoutConfig
from scripts.common.models import RawRecord

DEFAULT_POSTCODE_TAGS = ("addr:postcode", "postal_code", "contact:postcode", "addr:postal_code")


def _postcode_tags(overpass_config: dict) -> list[str]:
    configured = overpass_config.get("postcode_tags")
    if not configured:
        return list(DEFAULT_POSTCODE_TAGS)
    values: Iterable[object]
    if isinstance(configured, str):
        values = [configured]
    elif isinstance(configured, Iterable):
        values = configured
    else:
        values = [configured]
    tags = [str(tag).strip() for tag in values if str(tag).strip()]
    return tags or list(DEFAULT_POSTCODE_TAGS)


def _tag_filters(tags: Iterable[str], area_clause: str) -> str:
    return "".join(f'  nwr["{tag}"]({area_clause});\n' for tag in tags)


def build_overpass_query(overpass_config: dict) -> str:
    strategy = overpass_config["area_strategy"]
    timeout = int(overpass_config.get("timeout_seconds", 180))
    postcode_tags = _postcode_tags(overpass_config)

    if strategy == "bbox":
        min_lat, min_lon, max_lat, max_lon = overpass_config["bbox"]
        area_clause = f"{min_lat},{min_lon},{max_lat},{max_lon}"
        return (
            f"[out:json][timeout:{timeout}];\n"
            "(\n"
            f"{_tag_filters(postcode_tags, area_clause)}"
            ");\n"
            "out center tags;"
        )

    if strategy == "relation":
        relation_id = int(overpass_config["relation_id"])
        relation_area_id = relation_id if relation_id >= 3600000000 else relation_id + 3600000000
        return (
            f"[out:json][timeout:{timeout}];\n"
            f"area({relation_area_id})->.searchArea;\n"
            "(\n"
            f"{_tag_filters(postcode_tags, 'area.searchArea')}"
            ");\n"
            "out center tags;"
        )

    if strategy == "polygon":
        polygon = overpass_config.get("polygon")
        if not polygon:
            raise ValueError("overpass.polygon is required for area_strategy=polygon")
        area_clause = f'poly:"{polygon}"'
        return (
            f"[out:json][timeout:{timeout}];\n"
            "(\n"
            f"{_tag_filters(postcode_tags, area_clause)}"
            ");\n"
            "out center tags;"
        )

    raise ValueError(f"Unsupported overpass area strategy: {strategy}")


def run_overpass_harvest(
    territory_code: str,
    territory_config: dict,
    data_dir: Path,
    run_id: str,
    run_date: str,
    http_client: HttpClient | None = None,
) -> dict:
    out_dir = data_dir / "raw" / "osm" / "overpass"
    ensure_dir(out_dir)

    if not territory_config["overpass"]["enabled"]:
        payload = {
            "territory": territory_code,
            "run_id": run_id,
            "source": "overpass",
            "enabled": False,
            "rows": [],
        }
        write_json(out_dir / f"{territory_code.lower()}_overpass.json", payload)
        return payload

    overpass_cfg = territory_config["overpass"]
    query = build_overpass_query(overpass_cfg)
    postcode_tags = _postcode_tags(overpass_cfg)

    owns_client = http_client is None
    client = http_client or HttpClient()
    try:
        payload = client.post_form_json(
            overpass_cfg["endpoint"],
            source_type="overpass",
            data={"data": query},
            timeout=TimeoutConfig(connect=20, read=180),
            post_heavy_sleep=(2.0, 5.0),
        )
    finally:
        if owns_client:
            client.close()

    rows: list[RawRecord] = []
    seen_ids: set[str] = set()
    for element in payload.get("elements", []):
        tags = element.get("tags") or {}
        raw_postcode = None
        for tag in postcode_tags:
            candidate = tags.get(tag)
            if candidate not in (None, ""):
                raw_postcode = candidate
                break
        if raw_postcode in (None, ""):
            continue

        source_record_id = f"{element.get('type')}/{element.get('id')}"
        dedupe_key = f"{source_record_id}|{raw_postcode}"
        if dedupe_key in seen_ids:
            continue
        seen_ids.add(dedupe_key)

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
                source_name="osm_overpass",
                source_class="osm",
                source_record_id=source_record_id,
                raw_postcode=str(raw_postcode),
                raw_lat=float(lat) if lat is not None else None,
                raw_lon=float(lon) if lon is not None else None,
                raw_geometry=None,
                source_wkid=4326,
                extract_date=run_date,
                run_id=run_id,
                raw_payload_ref=f"raw/osm/overpass/{territory_code.lower()}_overpass.json",
            )
        )

    out_payload = {
        "territory": territory_code,
        "run_id": run_id,
        "source": "overpass",
        "enabled": True,
        "row_count": len(rows),
        "rows": [row.to_dict() for row in rows],
    }
    write_json(out_dir / f"{territory_code.lower()}_overpass.json", out_payload)
    return out_payload

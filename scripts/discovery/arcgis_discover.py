"""ArcGIS discovery stage entrypoint."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

from scripts.common.arcgis_hosts import resolve_arcgis_service_url
from scripts.common.fs import ensure_dir, write_json
from scripts.common.http import HttpClient, TimeoutConfig


def _service_metadata_url(service_url: str) -> str:
    return service_url.rstrip("/")


def _layer_url(service_url: str, layer_id: int) -> str:
    parsed = urlparse(service_url)
    path = parsed.path.rstrip("/")
    suffix = path.split("/")[-1]
    if suffix.isdigit():
        return service_url.rstrip("/")
    return f"{service_url.rstrip('/')}/{layer_id}"


def run_discovery(
    territory_code: str,
    territory_config: dict,
    data_dir: Path,
    run_id: str,
    http_client: HttpClient | None = None,
) -> dict:
    out_dir = data_dir / "raw" / "discovery"
    ensure_dir(out_dir)

    if not territory_config["arcgis"]["enabled"]:
        payload = {
            "territory": territory_code,
            "run_id": run_id,
            "services": [],
            "enabled": False,
        }
        write_json(out_dir / f"{territory_code.lower()}_discovery.json", payload)
        return payload

    services_payload: list[dict] = []
    owns_client = http_client is None
    client = http_client or HttpClient()
    try:
        for service in territory_config["arcgis"]["services"]:
            service_url = _service_metadata_url(service["service_url"])
            resolved = resolve_arcgis_service_url(service_url, client)
            service_url = _service_metadata_url(resolved.resolved_url)
            service_meta = client.get_json(
                service_url,
                source_type="arcgis",
                params={"f": "pjson"},
                timeout=TimeoutConfig(connect=20, read=120),
            )

            configured_layer_ids = service.get("layer_ids")
            if configured_layer_ids:
                layer_ids = configured_layer_ids
            else:
                layer_ids = [layer["id"] for layer in service_meta.get("layers", [])]

            layers: list[dict] = []
            for layer_id in layer_ids:
                layer_meta = client.get_json(
                    _layer_url(service_url, int(layer_id)),
                    source_type="arcgis",
                    params={"f": "pjson"},
                    timeout=TimeoutConfig(connect=20, read=120),
                )
                layers.append({"layer_id": int(layer_id), "metadata": layer_meta})

            services_payload.append(
                {
                    "name": service["name"],
                    "service_url": service_url,
                    "original_service_url": resolved.original_url,
                    "host_fallback_used": resolved.fallback_used,
                    "attempted_hosts": resolved.attempted_hosts,
                    "service_metadata": service_meta,
                    "layers": layers,
                }
            )
    finally:
        if owns_client:
            client.close()

    payload = {
        "territory": territory_code,
        "run_id": run_id,
        "services": services_payload,
    }
    write_json(out_dir / f"{territory_code.lower()}_discovery.json", payload)
    return payload

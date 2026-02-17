"""ArcGIS Online host resolution helpers.

Some ArcGIS org services are hosted on services, services1..services9 subdomains.
This module resolves host variants deterministically and caches the winner.
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import ParseResult, urlparse, urlunparse

from scripts.common.http import HttpClient, HttpRequestError, TimeoutConfig

_ARCGIS_HOST_CACHE: dict[str, str] = {}


@dataclass(frozen=True)
class ResolvedArcGisUrl:
    original_url: str
    resolved_url: str
    fallback_used: bool
    attempted_hosts: list[str]


def _is_arcgis_services_host(host: str) -> bool:
    return host in {
        "services.arcgis.com",
        "services1.arcgis.com",
        "services2.arcgis.com",
        "services3.arcgis.com",
        "services4.arcgis.com",
        "services5.arcgis.com",
        "services6.arcgis.com",
        "services7.arcgis.com",
        "services8.arcgis.com",
        "services9.arcgis.com",
    }


def _service_cache_key(parsed: ParseResult) -> str:
    # Key by service path after domain so org/service stick together.
    return parsed.path


def _replace_host(parsed: ParseResult, host: str) -> str:
    return urlunparse(parsed._replace(netloc=host))


def _is_invalid_url_payload(payload: dict) -> bool:
    error = payload.get("error")
    if not isinstance(error, dict):
        return False
    message = str(error.get("message", ""))
    details = " ".join(str(x) for x in (error.get("details") or []))
    text = f"{message} {details}".lower()
    return "invalid url" in text


def _probe_url(client: HttpClient, url: str) -> dict:
    try:
        return client.get_json(
            url,
            source_type="arcgis",
            params={"f": "pjson"},
            timeout=TimeoutConfig(connect=20, read=120),
        )
    except HttpRequestError:
        return {"error": {"message": "probe_http_error"}}


def resolve_arcgis_service_url(service_url: str, http_client: HttpClient) -> ResolvedArcGisUrl:
    parsed = urlparse(service_url.rstrip("/"))
    if not _is_arcgis_services_host(parsed.netloc):
        return ResolvedArcGisUrl(
            original_url=service_url,
            resolved_url=service_url,
            fallback_used=False,
            attempted_hosts=[parsed.netloc],
        )

    cache_key = _service_cache_key(parsed)
    if cache_key in _ARCGIS_HOST_CACHE:
        cached_host = _ARCGIS_HOST_CACHE[cache_key]
        resolved = _replace_host(parsed, cached_host)
        return ResolvedArcGisUrl(
            original_url=service_url,
            resolved_url=resolved,
            fallback_used=(cached_host != parsed.netloc),
            attempted_hosts=[cached_host],
        )

    hosts = [
        parsed.netloc,
        "services.arcgis.com",
        "services1.arcgis.com",
        "services2.arcgis.com",
        "services3.arcgis.com",
        "services4.arcgis.com",
        "services5.arcgis.com",
        "services6.arcgis.com",
        "services7.arcgis.com",
        "services8.arcgis.com",
        "services9.arcgis.com",
    ]

    attempted: list[str] = []
    for host in dict.fromkeys(hosts):
        attempted.append(host)
        candidate = _replace_host(parsed, host)
        payload = _probe_url(http_client, candidate)
        if _is_invalid_url_payload(payload):
            continue

        _ARCGIS_HOST_CACHE[cache_key] = host
        return ResolvedArcGisUrl(
            original_url=service_url,
            resolved_url=candidate,
            fallback_used=(host != parsed.netloc),
            attempted_hosts=attempted,
        )

    # No viable fallback discovered; keep original.
    return ResolvedArcGisUrl(
        original_url=service_url,
        resolved_url=service_url,
        fallback_used=False,
        attempted_hosts=attempted,
    )

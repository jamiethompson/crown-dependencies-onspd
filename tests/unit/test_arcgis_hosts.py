from __future__ import annotations

from scripts.common.arcgis_hosts import resolve_arcgis_service_url


class FakeHttpClient:
    def __init__(self):
        self.calls: list[str] = []

    def get_json(self, url: str, **_kwargs):
        self.calls.append(url)
        if "services.arcgis.com" in url or "services1.arcgis.com" in url or "services2.arcgis.com" in url:
            return {"error": {"message": "Invalid URL", "details": ["Invalid URL"]}}
        return {"currentVersion": 11.5, "layers": []}


def test_resolve_arcgis_service_url_falls_back_to_working_host():
    client = FakeHttpClient()
    original = "https://services.arcgis.com/dBlcVWoJgxxpCttN/arcgis/rest/services/Foo/FeatureServer"

    resolved = resolve_arcgis_service_url(original, client)

    assert resolved.resolved_url.startswith("https://services3.arcgis.com/")
    assert resolved.fallback_used is True
    assert "services3.arcgis.com" in resolved.attempted_hosts


def test_resolve_arcgis_service_url_leaves_non_arcgis_hosts_unchanged():
    client = FakeHttpClient()
    original = "https://ppmaps.gov.im/manngispubserver/rest/services/CorporateDynamicServices/PPDEFA/MapServer"

    resolved = resolve_arcgis_service_url(original, client)

    assert resolved.resolved_url == original
    assert resolved.fallback_used is False
    assert resolved.attempted_hosts == ["ppmaps.gov.im"]
    assert client.calls == []

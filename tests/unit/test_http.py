from __future__ import annotations

import pytest

from scripts.common.http import HttpClient, HttpRequestError, RetryConfig, RetryableHttpError


class FakeResponse:
    def __init__(self, status_code: int, payload=None, raises_json: bool = False):
        self.status_code = status_code
        self._payload = payload
        self._raises_json = raises_json

    def json(self):
        if self._raises_json:
            raise ValueError("bad json")
        return self._payload


def test_http_get_json_success(monkeypatch):
    client = HttpClient(retry=RetryConfig(max_attempts=1))

    monkeypatch.setattr(client.session, "request", lambda **_kwargs: FakeResponse(200, {"ok": True}))
    payload = client.get_json("https://example.com", source_type="arcgis")

    assert payload == {"ok": True}


def test_http_retryable_status_raises_retryable_error(monkeypatch):
    client = HttpClient(retry=RetryConfig(max_attempts=1))
    monkeypatch.setattr(client.session, "request", lambda **_kwargs: FakeResponse(503, {"x": 1}))

    with pytest.raises(RetryableHttpError):
        client.get_json("https://example.com", source_type="arcgis")


def test_http_invalid_json_raises(monkeypatch):
    client = HttpClient(retry=RetryConfig(max_attempts=1))
    monkeypatch.setattr(client.session, "request", lambda **_kwargs: FakeResponse(200, raises_json=True))

    with pytest.raises(HttpRequestError):
        client.get_json("https://example.com", source_type="arcgis")

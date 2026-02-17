"""HTTP client with retries, timeouts, and host-aware rate limiting."""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass
from types import TracebackType
from typing import Any
from urllib.parse import urlparse

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from scripts.common.constants import USER_AGENT
from scripts.common.errors import StageError

RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}


@dataclass(frozen=True)
class TimeoutConfig:
    connect: float = 20.0
    read: float = 120.0


@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 5
    multiplier: float = 1.0
    max_wait: float = 30.0


class HttpRequestError(StageError):
    error_code = "HTTP_ERROR"


class RetryableHttpError(HttpRequestError):
    pass


class TokenBucket:
    def __init__(self, rate_per_sec: float, capacity: float | None = None) -> None:
        self.rate_per_sec = rate_per_sec
        self.capacity = capacity if capacity is not None else rate_per_sec
        self.tokens = self.capacity
        self.updated_at = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self, tokens: float = 1.0) -> None:
        while True:
            with self.lock:
                now = time.monotonic()
                elapsed = now - self.updated_at
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate_per_sec)
                self.updated_at = now
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return
                deficit = tokens - self.tokens
                wait_for = max(deficit / self.rate_per_sec, 0.01)
            time.sleep(wait_for)


class HostRateLimiter:
    def __init__(self, default_rate_per_sec: float) -> None:
        self.default_rate_per_sec = default_rate_per_sec
        self.buckets: dict[str, TokenBucket] = {}
        self.lock = threading.Lock()

    def acquire(self, host: str, tokens: float = 1.0) -> None:
        with self.lock:
            bucket = self.buckets.get(host)
            if bucket is None:
                bucket = TokenBucket(rate_per_sec=self.default_rate_per_sec)
                self.buckets[host] = bucket
        bucket.acquire(tokens=tokens)


class HttpClient:
    def __init__(
        self,
        *,
        timeout: TimeoutConfig | None = None,
        retry: RetryConfig | None = None,
    ) -> None:
        self.timeout = timeout or TimeoutConfig()
        self.retry = retry or RetryConfig()
        self.session = requests.Session()
        self.arcgis_limiter = HostRateLimiter(default_rate_per_sec=5.0)
        self.overpass_limiter = HostRateLimiter(default_rate_per_sec=1.0)

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> "HttpClient":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def _host(self, url: str) -> str:
        return urlparse(url).netloc

    def _apply_rate_limit(self, url: str, source_type: str) -> None:
        host = self._host(url)
        if source_type == "arcgis":
            self.arcgis_limiter.acquire(host)
        elif source_type == "overpass":
            self.overpass_limiter.acquire(host)

    def _headers(self, headers: dict[str, str] | None) -> dict[str, str]:
        out = {"User-Agent": USER_AGENT, "Accept": "application/json"}
        if headers:
            out.update(headers)
        return out

    def _raise_for_status_or_retry(self, response: requests.Response) -> None:
        status = response.status_code
        if status in RETRYABLE_STATUS_CODES:
            raise RetryableHttpError(f"Retryable HTTP status: {status}")
        if status >= 400:
            raise HttpRequestError(f"HTTP status: {status}")

    def _request_json(
        self,
        method: str,
        url: str,
        *,
        source_type: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: TimeoutConfig | None = None,
        post_heavy_sleep: tuple[float, float] | None = None,
    ) -> dict[str, Any]:
        req_timeout = timeout or self.timeout
        self._apply_rate_limit(url, source_type)

        response = self.session.request(
            method=method,
            url=url,
            params=params,
            data=data,
            headers=self._headers(headers),
            timeout=(req_timeout.connect, req_timeout.read),
        )
        self._raise_for_status_or_retry(response)

        try:
            payload = response.json()
        except ValueError as exc:
            raise HttpRequestError(f"Invalid JSON payload from {url}") from exc

        if post_heavy_sleep is not None:
            low, high = post_heavy_sleep
            time.sleep(random.uniform(low, high))

        return payload

    def request_json(
        self,
        method: str,
        url: str,
        *,
        source_type: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: TimeoutConfig | None = None,
        post_heavy_sleep: tuple[float, float] | None = None,
    ) -> dict[str, Any]:
        @retry(
            stop=stop_after_attempt(self.retry.max_attempts),
            wait=wait_exponential_jitter(
                initial=self.retry.multiplier,
                max=self.retry.max_wait,
                jitter=1.0,
            ),
            retry=retry_if_exception_type(RetryableHttpError),
            reraise=True,
        )
        def _wrapped() -> dict[str, Any]:
            return self._request_json(
                method,
                url,
                source_type=source_type,
                params=params,
                data=data,
                headers=headers,
                timeout=timeout,
                post_heavy_sleep=post_heavy_sleep,
            )

        return _wrapped()

    def get_json(
        self,
        url: str,
        *,
        source_type: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: TimeoutConfig | None = None,
    ) -> dict[str, Any]:
        return self.request_json(
            "GET",
            url,
            source_type=source_type,
            params=params,
            headers=headers,
            timeout=timeout,
        )

    def post_form_json(
        self,
        url: str,
        *,
        source_type: str,
        data: dict[str, Any],
        headers: dict[str, str] | None = None,
        timeout: TimeoutConfig | None = None,
        post_heavy_sleep: tuple[float, float] | None = None,
    ) -> dict[str, Any]:
        merged = {"Content-Type": "application/x-www-form-urlencoded"}
        if headers:
            merged.update(headers)
        return self.request_json(
            "POST",
            url,
            source_type=source_type,
            data=data,
            headers=merged,
            timeout=timeout,
            post_heavy_sleep=post_heavy_sleep,
        )

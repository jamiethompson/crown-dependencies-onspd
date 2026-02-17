"""HTTP primitives with retries/rate limiting (full implementation in later stages)."""

from __future__ import annotations

from dataclasses import dataclass

import requests


RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}


@dataclass(frozen=True)
class TimeoutConfig:
    connect: float = 20.0
    read: float = 120.0


def basic_session() -> requests.Session:
    return requests.Session()

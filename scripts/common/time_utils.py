"""UTC-focused helpers for deterministic run metadata."""

from __future__ import annotations

from datetime import date, datetime, timezone


def utc_today_iso() -> str:
    return datetime.now(tz=timezone.utc).date().isoformat()


def parse_run_date(value: str | None) -> str:
    if not value:
        return utc_today_iso()
    parsed = date.fromisoformat(value)
    return parsed.isoformat()


def utc_timestamp_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat(timespec="milliseconds")

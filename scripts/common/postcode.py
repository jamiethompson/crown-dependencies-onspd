"""UK unit postcode normalisation and validation."""

from __future__ import annotations

import re

UK_UNIT_POSTCODE_RE = re.compile(r"^([A-Z]{1,2}\d[A-Z\d]?)\s(\d[A-Z]{2})$")
_EMBEDDED_POSTCODE_RE = re.compile(r"\b([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})\b")

_PUNCTUATION_RE = re.compile(r"[\.,;:'\"`_\-/\\()\[\]{}|~!?@#$%^&*+=]")
_WHITESPACE_RE = re.compile(r"\s+")


def is_valid_uk_unit_postcode(value: str) -> bool:
    return bool(UK_UNIT_POSTCODE_RE.match(value))


def normalise_postcode(raw: str | None) -> str | None:
    if raw is None:
        return None

    cleaned = raw.strip()
    if not cleaned:
        return None

    cleaned = cleaned.upper()
    # Some source fields contain full addresses with postcode embedded.
    if len(cleaned) > 8:
        embedded = _EMBEDDED_POSTCODE_RE.search(cleaned)
        if embedded:
            cleaned = embedded.group(1)
    cleaned = _PUNCTUATION_RE.sub("", cleaned)
    cleaned = _WHITESPACE_RE.sub("", cleaned)

    if len(cleaned) < 5 or len(cleaned) > 8:
        return None

    cleaned = f"{cleaned[:-3]} {cleaned[-3:]}"

    if not is_valid_uk_unit_postcode(cleaned):
        return None

    return cleaned

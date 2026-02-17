"""JSON logging with stable schema."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from scripts.common.constants import JSON_LOG_FIELDS
from scripts.common.fs import ensure_dir
from scripts.common.time_utils import utc_timestamp_iso


class JsonLineFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": utc_timestamp_iso(),
            "run_id": getattr(record, "run_id", None),
            "stage": getattr(record, "stage", None),
            "territory": getattr(record, "territory", None),
            "source": getattr(record, "source", None),
            "event": getattr(record, "event", None),
            "status": getattr(record, "status", None),
            "attempt": getattr(record, "attempt", None),
            "duration_ms": getattr(record, "duration_ms", None),
            "rows_in": getattr(record, "rows_in", None),
            "rows_out": getattr(record, "rows_out", None),
            "error_code": getattr(record, "error_code", None),
            "message": record.getMessage(),
        }
        for field in JSON_LOG_FIELDS:
            payload.setdefault(field, None)
        return json.dumps(payload, ensure_ascii=False)


def build_logger(run_id: str, data_dir: Path, level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(f"crown_postcodes.{run_id}")
    logger.setLevel(level.upper())
    logger.handlers.clear()

    stream = logging.StreamHandler()
    stream.setFormatter(JsonLineFormatter())
    logger.addHandler(stream)

    log_path = data_dir / "run_meta" / f"{run_id}.log.jsonl"
    ensure_dir(log_path.parent)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(JsonLineFormatter())
    logger.addHandler(file_handler)

    return logger


def log_event(logger: logging.Logger, message: str, **event_fields: Any) -> None:
    logger.info(message, extra=event_fields)

"""Application constants."""

USER_AGENT = "crown-postcodes/1.2 (+research; contact: configured-email)"
SUPPORTED_TERRITORIES = ("JE", "GY", "IM")
STAGES = (
    "discover",
    "harvest",
    "merge",
    "map-onspd",
    "validate",
)
EXIT_SUCCESS = 0
EXIT_PARTIAL = 10
EXIT_HARD_FAIL = 20
JSON_LOG_FIELDS = (
    "timestamp",
    "run_id",
    "stage",
    "territory",
    "source",
    "event",
    "status",
    "attempt",
    "duration_ms",
    "rows_in",
    "rows_out",
    "error_code",
    "message",
)

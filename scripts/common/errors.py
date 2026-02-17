"""Domain errors and failure typing."""


class PipelineError(Exception):
    """Base class for pipeline failures."""

    error_code = "PIPELINE_ERROR"


class ConfigError(PipelineError):
    """Raised for invalid or missing configuration."""

    error_code = "CONFIG_ERROR"


class ContractError(PipelineError):
    """Raised when strict output contracts are broken."""

    error_code = "CONTRACT_ERROR"


class StageError(PipelineError):
    """Raised for stage failures that should halt in strict mode."""

    error_code = "STAGE_ERROR"

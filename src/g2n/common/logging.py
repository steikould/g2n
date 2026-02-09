"""Structured JSON logging with correlation IDs for SOX auditability."""

from __future__ import annotations

import uuid

import structlog


def configure_logging(*, json_format: bool = True) -> None:
    """Configure structlog for the application.

    Args:
        json_format: If True, output JSON lines. If False, use console renderer.
    """
    processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if json_format:
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a logger bound with the given name."""
    return structlog.get_logger(name)


def new_correlation_id() -> str:
    """Generate a new correlation ID for request/pipeline tracing."""
    return str(uuid.uuid4())

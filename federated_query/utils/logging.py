"""Logging configuration with structured logging support."""

import logging
import sys
import json
from typing import Optional, Dict, Any
from datetime import datetime


class StructuredFormatter(logging.Formatter):
    """Structured JSON formatter for logs."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON.

        Args:
            record: Log record to format

        Returns:
            JSON-formatted log string
        """
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Add extra fields
        if hasattr(record, "extra_fields"):
            log_data.update(record.extra_fields)

        return json.dumps(log_data)


class StandardFormatter(logging.Formatter):
    """Standard human-readable formatter."""

    def __init__(self):
        super().__init__(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )


def setup_logging(
    level: str = "INFO",
    structured: bool = False,
    log_file: Optional[str] = None,
) -> None:
    """Set up logging configuration.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        structured: Use structured JSON logging if True
        log_file: Optional log file path
    """
    # Get logging level
    log_level = getattr(logging, level.upper(), logging.INFO)

    # Choose formatter
    if structured:
        formatter = StructuredFormatter()
    else:
        formatter = StandardFormatter()

    # Configure handlers
    handlers = []

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)
    handlers.append(console_handler)

    # File handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        file_handler.setLevel(log_level)
        handlers.append(file_handler)

    # Configure root logger
    logging.basicConfig(
        level=log_level,
        handlers=handlers,
        force=True,  # Override any existing configuration
    )

    # Set level for third-party loggers to WARNING to reduce noise
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("psycopg2").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Logger instance
    """
    return logging.getLogger(name)


class LoggerAdapter(logging.LoggerAdapter):
    """Logger adapter for adding contextual information."""

    def process(self, msg: str, kwargs: Dict[str, Any]) -> tuple:
        """Process log message with extra context.

        Args:
            msg: Log message
            kwargs: Keyword arguments

        Returns:
            Tuple of (message, kwargs)
        """
        if "extra" not in kwargs:
            kwargs["extra"] = {}

        # Add contextual fields
        kwargs["extra"]["extra_fields"] = self.extra

        return msg, kwargs


def get_contextual_logger(name: str, context: Dict[str, Any]) -> LoggerAdapter:
    """Get a logger with contextual information.

    Args:
        name: Logger name
        context: Context dictionary to include in all logs

    Returns:
        Logger adapter with context

    Example:
        >>> logger = get_contextual_logger(__name__, {"query_id": "123"})
        >>> logger.info("Query executed")  # Will include query_id in log
    """
    logger = get_logger(name)
    return LoggerAdapter(logger, context)

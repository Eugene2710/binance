import logging
import sys


def setup_logger(
    name: str, level: int = logging.INFO, format_string: str | None = None
) -> logging.Logger:
    """
    Setup a logger with StreamHandler and Formatter.

    Args:
        name: Logger name
        level: Logging level (default: INFO)
        format_string: Custom format string (optional)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger

    logger.setLevel(level)

    # Create StreamHandler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    # Create Formatter
    if format_string is None:
        format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    formatter = logging.Formatter(format_string)
    handler.setFormatter(formatter)

    # Add handler to logger
    logger.addHandler(handler)

    return logger

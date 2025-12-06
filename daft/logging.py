from __future__ import annotations

import logging
import typing

from daft import refresh_logger

VALID_LEVELS: set[str] = {
    "DEBUG",
    "INFO",
    "WARNING",
    "WARN",
    "ERROR",
}

LOG_LEVEL_MAP = {
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "WARN": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
}


def setup_debug_logger() -> None:
    import warnings

    warnings.warn(
        "setup_debug_logger() is deprecated. Use setup_logger('debug') instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return setup_logger("debug")


def setup_logger(
    level: str = "debug",
    exclude_prefix: typing.Iterable[str] | None = None,
    daft_only: bool = True,
) -> None:
    """Setup Daft logger with a specific log level, optional prefix filtering, and Rust sync.

    Args:
        level (str, optional): The log level to use. Valid options are `DEBUG`, `INFO`, `WARNING`, `ERROR`. Defaults to `DEBUG`.
        exclude_prefix (typing.Iterable[str] | None, optional): A list of prefixes to exclude from logging. Defaults to None.
        daft_only (bool, optional): Whether to only log messages from the Daft module. Defaults to True.

    Raises:
        ValueError: If the log level is not valid.
    """
    if not level or level.upper() not in VALID_LEVELS:
        raise ValueError(f"Invalid log level '{level}'. Valid options: {VALID_LEVELS}")
    level = level.upper()

    if level == "WARN":
        level = "WARNING"

    logging.basicConfig(level=level)

    root_logger: logging.Logger = logging.getLogger()

    numeric_level: int = LOG_LEVEL_MAP[level]
    root_logger.setLevel(numeric_level)

    for handler in root_logger.handlers:
        handler.filters.clear()

    if daft_only:
        for handler in root_logger.handlers:
            handler.addFilter(lambda record: record.name.startswith("daft"))

    if exclude_prefix:
        for prefix in exclude_prefix:
            for handler in root_logger.handlers:
                handler.addFilter(lambda r, p=prefix: not r.name.startswith(p))  # type: ignore

    refresh_logger()

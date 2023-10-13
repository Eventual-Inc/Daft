from __future__ import annotations

import logging


class FilterIncludeOnlyDaftRustLogs(logging.Filter):
    """Custom logging filter to include logs originating from Daft Rust codebase"""

    def filter(self, record):
        return record.name.startswith("daft_") and record.filename.endswith(".rs")


class FilterExcludeDaftRustLogs(logging.Filter):
    """Custom logging filter to exclude logs originating from Daft Rust codebase"""

    def filter(self, record):
        return not (record.name.startswith("daft_") and record.filename.endswith(".rs"))


class ReemitRustLogsHandler(logging.Handler):
    """Custom logging handler that intercepts logs originating from Daft Rust codebase
    and re-emits them using a "daft-rs" logger instead.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.addFilter(FilterIncludeOnlyDaftRustLogs())

    def emit(self, record: logging.LogRecord) -> None:
        # Set name to be prefixed by daft-rs
        # 1. This does not trigger infinite recursion on FilterIncludeOnlyDaftRustLogs
        # 2. This gives us a common prefix to grep daft-rs.* logs
        record.name = f"daft-rs.{record.name}"

        # Re-emits Daft Rust logs by forcing them to be re-emitted from the appropriate logger
        logger = logging.getLogger(record.name)
        logger.handle(record)


def setup_logger() -> None:
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        handler.addFilter(FilterExcludeDaftRustLogs())
    root_logger.addHandler(ReemitRustLogsHandler())

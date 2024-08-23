from __future__ import annotations

import logging

from daft import refresh_logger


def setup_debug_logger(
    exclude_prefix: list[str] = [],
    daft_only: bool = True,
):
    logging.basicConfig(level="DEBUG")
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    if daft_only:
        for handler in root_logger.handlers:
            handler.addFilter(lambda record: record.name.startswith("daft"))

    if exclude_prefix:
        for prefix in exclude_prefix:
            for handler in root_logger.handlers:
                handler.addFilter(lambda record: not record.name.startswith(prefix))

    refresh_logger()

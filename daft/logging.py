from __future__ import annotations

import logging


def setup_debug_logger(
    exclude_prefix: list[str] = [],
    daft_only: bool = True,
):
    logging.basicConfig(level="DEBUG")
    root_logger = logging.getLogger()

    if daft_only:
        for handler in root_logger.handlers:
            handler.addFilter(lambda record: record.name.startswith("daft"))

    if exclude_prefix:
        for prefix in exclude_prefix:
            for handler in root_logger.handlers:
                handler.addFilter(lambda record: not record.name.startswith(prefix))

from __future__ import annotations

import sys


def setup_logger() -> None:
    from loguru import logger
    from loguru._defaults import env

    logger.remove()
    LOGURU_LEVEL = env("LOGURU_LEVEL", str, "INFO")
    logger.add(sys.stderr, level=LOGURU_LEVEL)

from __future__ import annotations

import sys


def setup_logger() -> None:
    import inspect
    import logging

    from loguru import logger
    from loguru._defaults import env

    logger.remove()
    LOGURU_LEVEL = env("LOGURU_LEVEL", str, "INFO")
    logger.add(sys.stderr, level=LOGURU_LEVEL)

    class InterceptHandler(logging.Handler):
        def filter(self, record: logging.LogRecord) -> bool:
            parent = super().filter(record)
            return parent or record.pathname.startswith("src/")

        def emit(self, record: logging.LogRecord) -> None:
            # Get corresponding Loguru level if it exists.
            level: str | int
            try:
                level = logger.level(record.levelname).name
            except ValueError:
                level = record.levelno

            # Find caller from where originated the logged message.
            frame, depth = inspect.currentframe(), 0
            while frame and (depth == 0 or frame.f_code.co_filename == logging.__file__):
                frame = frame.f_back
                depth += 1

            logger.opt(depth=depth - 1, exception=record.exc_info).log(level, record.getMessage())

    logging.getLogger().setLevel(logger.level(LOGURU_LEVEL).no)
    logging.getLogger().addHandler(InterceptHandler())

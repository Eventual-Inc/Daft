from __future__ import annotations

import logging

import pytest


class CaptureHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def new_root_logger() -> tuple[logging.Logger, CaptureHandler]:
    """Create a fresh root logger with isolated handler."""
    root = logging.getLogger()
    for h in root.handlers[:]:
        root.removeHandler(h)
    capture = CaptureHandler()
    root.addHandler(capture)
    return root, capture


def test_logger_initialization() -> None:
    import daft

    rust_level = daft.daft.get_max_log_level()

    assert rust_level == "WARN"


def test_invalid_level_raises() -> None:
    from daft.logging import setup_logger_level

    with pytest.raises(ValueError):
        setup_logger_level(level="INVALID")


def test_debug_logger() -> None:
    import daft
    from daft.logging import setup_debug_logger

    setup_debug_logger()
    rust_level = daft.daft.get_max_log_level()
    assert rust_level == "DEBUG"


def test_info_logger() -> None:
    import daft
    from daft.logging import setup_info_logger

    setup_info_logger()
    rust_level = daft.daft.get_max_log_level()
    assert rust_level == "INFO"


def test_warn_logger() -> None:
    import daft
    from daft.logging import setup_warn_logger

    setup_warn_logger()
    rust_level = daft.daft.get_max_log_level()
    assert rust_level == "WARN"


def test_error_logger() -> None:
    import daft
    from daft.logging import setup_error_logger

    setup_error_logger()
    rust_level = daft.daft.get_max_log_level()
    assert rust_level == "ERROR"


def test_daft_only_filters_non_daft_logs() -> None:
    from daft.logging import setup_logger_level

    _, capture = new_root_logger()

    setup_logger_level(level="DEBUG", exclude_prefix=["daft"], daft_only=False)

    logging.getLogger("daft.core").debug("hello daft")
    logging.getLogger("other.module").debug("nope")

    assert len(capture.records) == 1
    assert capture.records[0].name == "other.module"


def test_daft_contains_only_daft_logs() -> None:
    from daft.logging import setup_logger_level

    _, capture = new_root_logger()

    setup_logger_level(level="DEBUG", exclude_prefix=[], daft_only=True)

    logging.getLogger("daft.core").debug("hello daft")
    logging.getLogger("other.module").debug("nope")

    assert len(capture.records) == 1
    assert capture.records[0].name == "daft.core"


def test_exclude_prefix_filters_daft_logs() -> None:
    from daft.logging import setup_logger_level

    _, capture = new_root_logger()

    setup_logger_level(level="DEBUG", exclude_prefix=["daft"], daft_only=False)

    logging.getLogger("daft.core").debug("should be filtered")
    logging.getLogger("spark.executor").debug("ok")

    names = [r.name for r in capture.records]
    assert "daft.core" not in names
    assert "spark.executor" in names


def test_daft_only_and_exclude_prefix() -> None:
    from daft.logging import setup_logger_level

    _, capture = new_root_logger()

    setup_logger_level(level="DEBUG", exclude_prefix=["daft"], daft_only=False)

    logging.getLogger("daft.debug.cache").debug("filtered out by exclude_prefix")
    logging.getLogger("daft.core").debug("passes")
    logging.getLogger("spark").debug("filtered by daft_only")

    names = [r.name for r in capture.records]

    assert "daft.debug.cache" not in names
    assert "daft.core" not in names
    assert "spark" in names


@pytest.mark.parametrize(
    "level, expected",
    [
        (logging.DEBUG, "DEBUG"),
        (logging.INFO, "INFO"),
        (logging.WARNING, "WARN"),
        (logging.ERROR, "ERROR"),
    ],
)
def test_refresh_logger(level, expected):
    import logging

    import daft

    logging.getLogger().setLevel(level)
    daft.daft.refresh_logger()

    rust_level = daft.daft.get_max_log_level()
    assert rust_level == expected

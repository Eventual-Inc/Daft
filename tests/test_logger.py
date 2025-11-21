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


def test_daft_contains_only_daft_logs() -> None:
    from daft.logging import setup_logger_level

    _, capture = new_root_logger()

    setup_logger_level(level="DEBUG", exclude_prefix=[], daft_only=True)

    logging.getLogger("daft.core").debug("pass")
    logging.getLogger("other.module").debug("excluded")

    assert len(capture.records) == 1
    assert capture.records[0].name == "daft.core"


def test_exclude_prefix_filters_daft_logs() -> None:
    from daft.logging import setup_logger_level

    _, capture = new_root_logger()

    setup_logger_level(level="DEBUG", exclude_prefix=["daft"], daft_only=False)

    logging.getLogger("daft.core").debug("excluded")
    logging.getLogger("spark.executor").debug("pass")

    names = [r.name for r in capture.records]
    assert "daft.core" not in names
    assert "spark.executor" in names


def test_daft_only_and_exclude_prefix() -> None:
    from daft.logging import setup_logger_level

    _, capture = new_root_logger()

    setup_logger_level(level="DEBUG", exclude_prefix=["daft"], daft_only=False)

    logging.getLogger("daft.debug.cache").debug("excluded")
    logging.getLogger("daft.core").debug("excluded")
    logging.getLogger("spark").debug("pass")

    names = [r.name for r in capture.records]

    assert "daft.debug.cache" not in names
    assert "daft.core" not in names
    assert "spark" in names


def test_exclude_prefix_multiple_prefixes() -> None:
    from daft.logging import setup_logger_level

    _, capture = new_root_logger()

    setup_logger_level(
        level="DEBUG",
        exclude_prefix=["daft", "spark"],
        daft_only=False,
    )

    logging.getLogger("daft.core").debug("excluded")
    logging.getLogger("spark.executor").debug("excluded")
    logging.getLogger("tensorflow").debug("should pass")

    names = [r.name for r in capture.records]

    assert "daft.core" not in names
    assert "spark.executor" not in names
    assert "tensorflow" in names


def test_daft_only_false_does_not_filter_other_modules() -> None:
    from daft.logging import setup_logger_level

    _, capture = new_root_logger()

    setup_logger_level(level="DEBUG", daft_only=False)

    logging.getLogger("daft.core").debug("ok")
    logging.getLogger("spark").debug("ok")
    logging.getLogger("pytorch").debug("ok")

    names = [r.name for r in capture.records]

    assert "daft.core" in names
    assert "spark" in names
    assert "pytorch" in names


def test_daft_only_and_exclude_prefix_multiple():
    # daft_only=True → only logs beginning with "daft" are allowed
    # exclude_prefix=["daft.debug", "spark"] → further excludes some daft and non-daft logs
    #
    # Expected:
    #   - daft.debug.* excluded by exclude_prefix
    #   - daft.core.* passes
    #   - spark.* excluded by both rules
    #   - other modules also excluded by daft_only

    from daft.logging import setup_logger_level

    _, capture = new_root_logger()

    setup_logger_level(level="DEBUG", daft_only=True, exclude_prefix=["daft.debug", "spark"])

    logging.getLogger("daft.debug.cache").debug("excluded")
    logging.getLogger("daft.core").debug("allowed")
    logging.getLogger("spark.executor").debug("excluded")
    logging.getLogger("tensorflow").debug("excluded")

    names = [r.name for r in capture.records]

    assert "daft.debug.cache" not in names
    assert "spark.executor" not in names
    assert "tensorflow" not in names

    # Only this one should pass
    assert names == ["daft.core"]


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

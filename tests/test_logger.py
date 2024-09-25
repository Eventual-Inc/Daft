import logging

import pytest


def test_logger_initialization():
    import daft

    rust_level = daft.daft.get_max_log_level()

    assert rust_level == "WARN"


def test_debug_logger():
    import daft
    from daft.logging import setup_debug_logger

    setup_debug_logger()
    rust_level = daft.daft.get_max_log_level()
    assert rust_level == "DEBUG"


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

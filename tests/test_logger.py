import logging

import pytest


def test_logger_initialization(caplog):
    import daft

    daft.daft.test_logging()

    assert len(caplog.records) == 2
    assert caplog.records[0].levelname == "WARNING"
    assert caplog.records[1].levelname == "ERROR"


@pytest.mark.parametrize(
    "level, expected",
    [
        (logging.DEBUG, 4),
        (logging.INFO, 3),
        (logging.WARNING, 2),
        (logging.ERROR, 1),
    ],
)
def test_refresh_logger(level, expected, caplog):
    import daft

    caplog.set_level(level)
    daft.refresh_logger()

    daft.daft.test_logging()
    assert len(caplog.records) == expected

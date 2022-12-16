from __future__ import annotations

from typing import TYPE_CHECKING

from daft.daft import version as _version
from daft.dataframe import DataFrame
from daft.expressions import col, lit
from daft.logging import setup_logger
from daft.udf import polars_udf, udf

__all__ = ["DataFrame", "col", "lit", "udf", "polars_udf"]

setup_logger()


def get_version() -> str:
    return _version()


__version__ = get_version()

if TYPE_CHECKING:

    class daft:
        pass

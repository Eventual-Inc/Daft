from __future__ import annotations

from typing import TYPE_CHECKING

from daft.dataframe import DataFrame
from daft.expressions import col, lit
from daft.logging import setup_logger
from daft.udf import polars_udf, udf

__all__ = ["DataFrame", "col", "lit", "udf", "polars_udf"]

setup_logger()

if TYPE_CHECKING:

    class daft:
        pass

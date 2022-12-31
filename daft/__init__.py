from __future__ import annotations

from typing import TYPE_CHECKING

from daft.logging import setup_logger

###
# Setup logging
###


setup_logger()

###
# Get version number from Rust .so
###

from daft.daft import version as _version


def get_version() -> str:
    return _version()


__version__ = get_version()

if TYPE_CHECKING:

    class daft:
        pass


###
# Initialize analytics
###

from daft.analytics import init_analytics

analytics_client = init_analytics(__version__)
if analytics_client is not None:
    analytics_client.track_import(__version__)

###
# Daft top-level imports
###

from daft.dataframe import DataFrame
from daft.expressions import col, lit
from daft.udf import polars_udf, udf

__all__ = ["DataFrame", "col", "lit", "udf", "polars_udf"]

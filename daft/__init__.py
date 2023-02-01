from __future__ import annotations

import os
from typing import TYPE_CHECKING

from daft.logging import setup_logger

###
# Setup logging
###


setup_logger()

###
# Get build constants from Rust .so
###

from daft.daft import build_type as _build_type
from daft.daft import version as _version


def get_version() -> str:
    return _version()


def get_build_type() -> str:
    return _build_type()


__version__ = get_version()

if TYPE_CHECKING:
    # Placeholder for type checking for Rust module
    class daft:
        pass


###
# Initialize analytics
###

from daft.analytics import init_analytics

dev_build = get_build_type() == "dev"
user_opted_out = os.getenv("DAFT_ANALYTICS_ENABLED") == "0"
if not dev_build and not user_opted_out:
    analytics_client = init_analytics(get_version(), get_build_type())
    analytics_client.track_import()

###
# Daft top-level imports
###

from daft.dataframe import DataFrame
from daft.expressions import col, lit
from daft.udf import polars_udf, udf

__all__ = ["DataFrame", "col", "lit", "udf", "polars_udf"]

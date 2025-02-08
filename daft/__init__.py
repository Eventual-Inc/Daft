from __future__ import annotations

import os

###
# Set up code coverage for when running code coverage with ray
###
if "COV_CORE_SOURCE" in os.environ:
    try:
        from pytest_cov.embed import init

        init()
    except Exception as exc:
        import sys

        sys.stderr.write(
            "pytest-cov: Failed to setup subprocess coverage. " "Environ: {!r} " "Exception: {!r}\n".format(
                {k: v for k, v in os.environ.items() if k.startswith("COV_CORE")}, exc
            )
        )

###
# Get build constants from Rust .so
###

from daft.daft import build_type as _build_type
from daft.daft import version as _version
from daft.daft import refresh_logger as _refresh_logger


def get_version() -> str:
    return _version()


def get_build_type() -> str:
    return _build_type()


def refresh_logger() -> None:
    """Refreshes Daft's internal rust logging to the current python log level."""
    _refresh_logger()


__version__ = get_version()

###
# Initialize analytics
###

from daft.analytics import init_analytics

user_opted_out = os.getenv("DAFT_ANALYTICS_ENABLED") == "0"
analytics_client = init_analytics(get_version(), get_build_type(), user_opted_out)
analytics_client.track_import()

###
# Scarf Analytics
###

from daft.scarf_telemetry import scarf_analytics

scarf_opt_out = os.getenv("SCARF_NO_ANALYTICS") == "true" or os.getenv("DO_NOT_TRACK") == "true"


# what happens if user sets runner manually after import? call function again after setting variable
def get_runner() -> str:
    runner = os.getenv("DAFT_RUNNER", "native").lower()
    if runner not in ("py", "ray", "native"):
        return "native"
    return runner


try:
    scarf_analytics(scarf_opt_out, get_build_type(), get_version(), get_runner())
except Exception:
    pass

###
# Daft top-level imports
###

from daft.catalog import (
    Identifier,
    read_table,
    register_table,
)
from daft.context import set_execution_config, set_planning_config, execution_config_ctx, planning_config_ctx
from daft.convert import (
    from_arrow,
    from_dask_dataframe,
    from_pandas,
    from_pydict,
    from_pylist,
    from_ray_dataset,
)
from daft.daft import ImageFormat, ImageMode, ResourceRequest
from daft.dataframe import DataFrame
from daft.logical.schema import Schema
from daft.datatype import DataType, TimeUnit
from daft.expressions import Expression, col, list_, lit, interval, struct, coalesce
from daft.io import (
    DataCatalogTable,
    DataCatalogType,
    from_glob_path,
    read_csv,
    read_deltalake,
    read_hudi,
    read_iceberg,
    read_json,
    read_parquet,
    read_sql,
    read_lance,
)
from daft.series import Series
from daft.sql import sql, sql_expr
from daft.udf import udf
from daft.viz import register_viz_hook

to_struct = Expression.to_struct

__all__ = [
    "DataCatalogTable",
    "DataCatalogType",
    "DataFrame",
    "DataType",
    "Expression",
    "Identifier",
    "ImageFormat",
    "ImageMode",
    "ResourceRequest",
    "Schema",
    "Series",
    "TimeUnit",
    "coalesce",
    "col",
    "execution_config_ctx",
    "from_arrow",
    "from_dask_dataframe",
    "from_glob_path",
    "from_pandas",
    "from_pydict",
    "from_pylist",
    "from_ray_dataset",
    "interval",
    "list_",
    "lit",
    "planning_config_ctx",
    "read_csv",
    "read_deltalake",
    "read_hudi",
    "read_iceberg",
    "read_json",
    "read_lance",
    "read_parquet",
    "read_sql",
    "read_table",
    "refresh_logger",
    "register_table",
    "register_viz_hook",
    "set_execution_config",
    "set_planning_config",
    "sql",
    "sql_expr",
    "struct",
    "to_struct",
    "udf",
]

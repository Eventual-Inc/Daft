from __future__ import annotations

import os

from daft.scarf_telemetry import track_import_on_scarf

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

track_import_on_scarf()

###
# Daft top-level imports
###

from daft.catalog import (
    Catalog,
    Identifier,
    Table,
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
from daft.schema import Schema
from daft.datatype import DataType, TimeUnit
from daft.expressions import Expression, col, element, list_, lit, interval, struct, coalesce
from daft.io import (
    DataCatalogTable,
    DataCatalogType,
    from_glob_path,
    _range as range,
    read_csv,
    read_deltalake,
    read_hudi,
    read_iceberg,
    read_json,
    read_parquet,
    read_sql,
    read_lance,
    read_warc,
)
from daft.series import Series
from daft.session import (
    Session,
    attach,
    attach_catalog,
    attach_table,
    create_namespace,
    create_namespace_if_not_exists,
    create_table,
    create_table_if_not_exists,
    create_temp_table,
    current_catalog,
    current_namespace,
    current_session,
    detach_catalog,
    detach_table,
    drop_namespace,
    drop_table,
    get_catalog,
    get_table,
    has_catalog,
    has_namespace,
    has_table,
    list_catalogs,
    list_tables,
    read_table,
    set_catalog,
    set_namespace,
    set_session,
    write_table,
    attach_function,
    detach_function,
)
from daft.sql import sql, sql_expr
from daft.udf import udf, _DaftFunc as func
from daft.viz import register_viz_hook
from daft.window import Window

to_struct = Expression.to_struct

__all__ = [
    "Catalog",
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
    "Session",
    "Table",
    "TimeUnit",
    "Window",
    "attach",
    "attach_catalog",
    "attach_function",
    "attach_table",
    "coalesce",
    "col",
    "create_namespace",
    "create_namespace_if_not_exists",
    "create_table",
    "create_table_if_not_exists",
    "create_temp_table",
    "current_catalog",
    "current_namespace",
    "current_session",
    "detach_catalog",
    "detach_function",
    "detach_table",
    "drop_namespace",
    "drop_table",
    "element",
    "execution_config_ctx",
    "from_arrow",
    "from_dask_dataframe",
    "from_glob_path",
    "from_pandas",
    "from_pydict",
    "from_pylist",
    "from_ray_dataset",
    "func",
    "get_catalog",
    "get_table",
    "has_catalog",
    "has_namespace",
    "has_table",
    "interval",
    "list_",
    "list_catalogs",
    "list_tables",
    "lit",
    "planning_config_ctx",
    "range",
    "read_csv",
    "read_deltalake",
    "read_hudi",
    "read_iceberg",
    "read_json",
    "read_lance",
    "read_parquet",
    "read_sql",
    "read_table",
    "read_warc",
    "refresh_logger",
    "register_viz_hook",
    "set_catalog",
    "set_execution_config",
    "set_namespace",
    "set_planning_config",
    "set_session",
    "sql",
    "sql_expr",
    "struct",
    "to_struct",
    "udf",
    "write_table",
]

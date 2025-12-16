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
            "pytest-cov: Failed to setup subprocess coverage. Environ: {!r} Exception: {!r}\n".format(
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
from daft.context import (
    set_execution_config,
    set_planning_config,
    execution_config_ctx,
    planning_config_ctx,
)
from daft.convert import (
    from_arrow,
    from_dask_dataframe,
    from_pandas,
    from_pydict,
    from_pylist,
    from_ray_dataset,
)
from daft.daft import ImageFormat, ImageMode, ImageProperty, ResourceRequest
from daft.dataframe import DataFrame
from daft.schema import Schema
from daft.datatype import DataType, TimeUnit, MediaType
from daft.expressions import Expression, col, element, lit, interval
from daft.series import Series
from daft.session import (
    Session,
    attach,
    attach_catalog,
    attach_provider,
    attach_function,
    attach_table,
    create_namespace,
    create_namespace_if_not_exists,
    create_table,
    create_table_if_not_exists,
    create_temp_table,
    current_catalog,
    current_model,
    current_namespace,
    current_provider,
    current_session,
    detach_catalog,
    detach_function,
    detach_provider,
    detach_table,
    drop_namespace,
    drop_table,
    get_catalog,
    get_provider,
    get_table,
    has_catalog,
    has_namespace,
    has_provider,
    has_table,
    list_catalogs,
    list_tables,
    read_table,
    session,
    set_catalog,
    set_model,
    set_namespace,
    set_provider,
    set_session,
    write_table,
)
from daft.udf import udf, func, cls, method, metrics
from daft.io import (
    DataCatalogTable,
    DataCatalogType,
    IOConfig,
    from_glob_path,
    _range as range,
    read_lance,
    read_csv,
    read_deltalake,
    read_hudi,
    read_iceberg,
    read_json,
    read_parquet,
    read_sql,
    read_video_frames,
    read_warc,
    read_huggingface,
    read_mcap,
)
from daft.runners import get_or_create_runner, get_or_infer_runner_type, set_runner_native, set_runner_ray
from daft.sql import sql, sql_expr
from daft.viz import register_viz_hook
from daft.window import Window
from daft.file import File, VideoFile, AudioFile

import daft.context as context
import daft.io as io
import daft.runners as runners
import daft.datasets as datasets
import daft.functions as functions
import daft.gravitino as gravitino

__all__ = [
    "AudioFile",
    "Catalog",
    "DataCatalogTable",
    "DataCatalogType",
    "DataFrame",
    "DataType",
    "Expression",
    "File",
    "IOConfig",
    "Identifier",
    "ImageFormat",
    "ImageMode",
    "ImageProperty",
    "MediaType",
    "ResourceRequest",
    "Schema",
    "Series",
    "Session",
    "Table",
    "TimeUnit",
    "VideoFile",
    "Window",
    "attach",
    "attach_catalog",
    "attach_function",
    "attach_provider",
    "attach_table",
    "cls",
    "col",
    "context",
    "create_namespace",
    "create_namespace_if_not_exists",
    "create_table",
    "create_table_if_not_exists",
    "create_temp_table",
    "current_catalog",
    "current_model",
    "current_namespace",
    "current_provider",
    "current_session",
    "datasets",
    "detach_catalog",
    "detach_function",
    "detach_provider",
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
    "functions",
    "get_catalog",
    "get_or_create_runner",
    "get_or_infer_runner_type",
    "get_provider",
    "get_table",
    "gravitino",
    "has_catalog",
    "has_namespace",
    "has_provider",
    "has_table",
    "interval",
    "io",
    "list_catalogs",
    "list_tables",
    "lit",
    "method",
    "metrics",
    "planning_config_ctx",
    "range",
    "read_csv",
    "read_deltalake",
    "read_hudi",
    "read_huggingface",
    "read_iceberg",
    "read_json",
    "read_lance",
    "read_mcap",
    "read_parquet",
    "read_sql",
    "read_table",
    "read_video_frames",
    "read_warc",
    "refresh_logger",
    "register_viz_hook",
    "runners",
    "session",
    "set_catalog",
    "set_execution_config",
    "set_model",
    "set_namespace",
    "set_planning_config",
    "set_provider",
    "set_runner_native",
    "set_runner_ray",
    "set_session",
    "sql",
    "sql_expr",
    "udf",
    "write_table",
]

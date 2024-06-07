from __future__ import annotations

from typing import Generator, TypeVar

import pyarrow as pa
import pytest

pyiceberg = pytest.importorskip("pyiceberg")

PYARROW_LE_8_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) < (8, 0, 0)
pytestmark = pytest.mark.skipif(PYARROW_LE_8_0_0, reason="iceberg writes only supported if pyarrow >= 8.0.0")


import tenacity
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.table import Table

T = TypeVar("T")

YieldFixture = Generator[T, None, None]

local_tables_names = [
    "test_all_types",
    "test_limit",
    "test_null_nan",
    "test_null_nan_rewritten",
    "test_partitioned_by_bucket",
    "test_partitioned_by_days",
    "test_partitioned_by_hours",
    "test_partitioned_by_identity",
    "test_partitioned_by_months",
    "test_partitioned_by_truncate",
    "test_partitioned_by_years",
    "test_positional_mor_deletes",
    "test_positional_mor_double_deletes",
    "test_table_sanitized_character",
    "test_table_version",
    "test_uuid_and_fixed_unpartitioned",
]


cloud_tables_names = [
    "azure.test",
    # TODO(Kevin): Add more tables from more cloud providers
]


@tenacity.retry(
    stop=tenacity.stop_after_delay(60),
    retry=tenacity.retry_if_exception_type(pyiceberg.exceptions.NoSuchTableError),
    wait=tenacity.wait_fixed(5),
    reraise=True,
)
def _load_table(catalog, name) -> Table:
    return catalog.load_table(f"default.{name}")


@pytest.fixture(scope="session")
def local_iceberg_catalog() -> Catalog:
    cat = load_catalog(
        "local",
        **{
            "type": "rest",
            "uri": "http://localhost:8181",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        },
    )
    # ensure all tables are available
    for name in local_tables_names:
        _load_table(cat, name)

    return cat


@pytest.fixture(scope="session", params=local_tables_names)
def local_iceberg_tables(request, local_iceberg_catalog) -> Table:
    NAMESPACE = "default"
    table_name = request.param
    return local_iceberg_catalog.load_table(f"{NAMESPACE}.{table_name}")


@pytest.fixture(scope="session")
def cloud_iceberg_catalog() -> Catalog:
    return load_catalog(
        "default",
        **{
            "uri": "sqlite:///tests/assets/pyiceberg_catalog.db",
            "adlfs.account-name": "dafttestdata",
        },
    )


@pytest.fixture(scope="session", params=cloud_tables_names)
def cloud_iceberg_table(request, cloud_iceberg_catalog) -> Table:
    return cloud_iceberg_catalog.load_table(request.param)

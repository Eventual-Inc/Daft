from __future__ import annotations

from typing import Generator, Iterator, TypeVar

import pyarrow as pa
import pytest

import daft
import daft.catalog

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


@pytest.fixture(scope="session")
def local_iceberg_catalog() -> Iterator[tuple[str, Catalog]]:
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

    catalog_name = "_local_iceberg_catalog"
    daft.catalog.register_python_catalog(cat, name=catalog_name)
    yield catalog_name, cat
    daft.catalog.unregister_catalog(catalog_name=catalog_name)


@pytest.fixture(scope="session")
def azure_iceberg_catalog() -> Iterator[tuple[str, Catalog]]:
    cat = load_catalog(
        "default",
        **{
            "uri": "sqlite:///tests/assets/pyiceberg_catalog.db",
            "adlfs.account-name": "dafttestdata",
        },
    )

    catalog_name = "_azure_iceberg_catalog"
    daft.catalog.register_python_catalog(cat, name=catalog_name)
    yield catalog_name, cat
    daft.catalog.unregister_catalog(catalog_name=catalog_name)


@tenacity.retry(
    stop=tenacity.stop_after_delay(60),
    retry=tenacity.retry_if_exception_type(pyiceberg.exceptions.NoSuchTableError),
    wait=tenacity.wait_fixed(5),
    reraise=True,
)
def _load_table(catalog, name) -> Table:
    return catalog.load_table(f"default.{name}")


@pytest.fixture(scope="function", params=local_tables_names)
def local_iceberg_tables(request, local_iceberg_catalog) -> Iterator[str]:
    NAMESPACE = "default"
    table_name = request.param
    yield f"{NAMESPACE}.{table_name}"


@pytest.fixture(scope="function", params=cloud_tables_names)
def azure_iceberg_table(request, azure_iceberg_catalog) -> Iterator[str]:
    yield request.param

from __future__ import annotations

import pytest

pyiceberg = pytest.importorskip("pyiceberg")

import daft
from tests.conftest import assert_df_equals


@pytest.mark.integration
def test_daft_iceberg_cloud_table_load(azure_iceberg_table, azure_iceberg_catalog):
    catalog_name, pyiceberg_catalog = azure_iceberg_catalog
    df = daft.read_table(f"{catalog_name}.{azure_iceberg_table}")
    daft_pandas = df.to_pandas()
    iceberg_pandas = pyiceberg_catalog.load_table(azure_iceberg_table).scan().to_arrow().to_pandas()
    assert_df_equals(daft_pandas, iceberg_pandas, sort_key=[])

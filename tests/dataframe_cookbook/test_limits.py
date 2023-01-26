from __future__ import annotations

from tests.conftest import assert_df_equals
from tests.dataframe_cookbook.conftest import (
    parametrize_service_requests_csv_repartition,
)


@parametrize_service_requests_csv_repartition
def test_simple_limit(daft_df, service_requests_csv_pd_df, repartition_nparts):
    daft_df = daft_df.repartition(repartition_nparts)
    daft_df = daft_df.limit(5)
    daft_pd_df = daft_df.to_pandas()

    assert_df_equals(daft_pd_df, service_requests_csv_pd_df.head(5))


@parametrize_service_requests_csv_repartition
def test_simple_limit_tail(daft_df, service_requests_csv_pd_df, repartition_nparts):
    daft_df = daft_df.repartition(repartition_nparts)
    daft_df = daft_df.limit(5, tail=True)
    daft_pd_df = daft_df.to_pandas()

    assert_df_equals(daft_pd_df, service_requests_csv_pd_df.tail(5))

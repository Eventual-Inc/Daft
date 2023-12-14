from __future__ import annotations

from itertools import product

import pytest

import daft
from daft.table import MicroPartition

PRED_PUSHDOWN_FILES = [
    "s3://daft-public-data/test_fixtures/parquet-dev/sampled-tpch-with-stats.parquet",
    "tests/assets/parquet-data/sampled-tpch-with-stats.parquet",
]


@pytest.mark.integration()
@pytest.mark.parametrize(
    "path, pred, limit",
    product(
        PRED_PUSHDOWN_FILES,
        [daft.col("L_ORDERKEY") == 7, daft.col("L_ORDERKEY") == 10000, daft.lit(True)],
        [None, 1, 1000],
    ),
)
def test_parquet_filter_pushdowns(path, pred, limit, aws_public_s3_config):
    with_pushdown = MicroPartition.read_parquet(path, predicate=pred, num_rows=limit, io_config=aws_public_s3_config)
    after = MicroPartition.read_parquet(path, io_config=aws_public_s3_config).filter([pred])
    if limit is not None:
        after = after.head(limit)
    assert with_pushdown.to_arrow() == after.to_arrow()


@pytest.mark.integration()
@pytest.mark.parametrize(
    "path, pred",
    product(PRED_PUSHDOWN_FILES, [daft.col("L_ORDERKEY") == 7, daft.col("L_ORDERKEY") == 10000, daft.lit(True)]),
)
def test_parquet_filter_pushdowns_disjoint_predicate(path, pred, aws_public_s3_config):
    with_pushdown = MicroPartition.read_parquet(
        path, predicate=pred, columns=["L_QUANTITY"], io_config=aws_public_s3_config
    )
    after = (
        MicroPartition.read_parquet(path, io_config=aws_public_s3_config)
        .filter([pred])
        .eval_expression_list([daft.col("L_QUANTITY")])
    )
    assert with_pushdown.to_arrow() == after.to_arrow()


@pytest.mark.integration()
@pytest.mark.parametrize(
    "path, pred",
    product(
        ["tests/assets/parquet-data/mvp.parquet", "s3://daft-public-data/test_fixtures/parquet-dev/mvp.parquet"],
        [daft.col("a") == 7, daft.col("a") == 10000, daft.lit(True)],
    ),
)
def test_parquet_filter_pushdowns_disjoint_predicate_no_stats(path, pred, aws_public_s3_config):
    with_pushdown = MicroPartition.read_parquet(path, predicate=pred, columns=["b"], io_config=aws_public_s3_config)
    after = (
        MicroPartition.read_parquet(path, io_config=aws_public_s3_config)
        .filter([pred])
        .eval_expression_list([daft.col("b")])
    )
    assert with_pushdown.to_arrow() == after.to_arrow()

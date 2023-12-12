from __future__ import annotations

from itertools import product

import pytest

import daft
from daft.daft import CsvConvertOptions
from daft.table import MicroPartition

PRED_PUSHDOWN_FILES = [
    "s3://daft-public-data/test_fixtures/csv-dev/sampled-tpch.csv",
    "tests/assets/sampled-tpch.csv",
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
def test_csv_filter_pushdowns(path, pred, limit, aws_public_s3_config):
    with_pushdown = MicroPartition.read_csv(
        path,
        io_config=aws_public_s3_config,
        convert_options=CsvConvertOptions(limit=limit, predicate=pred._expr),
        parse_options=None,
        read_options=None,
    )
    after = MicroPartition.read_csv(
        path, io_config=aws_public_s3_config, convert_options=None, parse_options=None, read_options=None
    ).filter([pred])
    if limit is not None:
        after = after.head(limit)
    assert with_pushdown.to_arrow() == after.to_arrow()


@pytest.mark.integration()
@pytest.mark.parametrize(
    "path, pred",
    product(PRED_PUSHDOWN_FILES, [daft.col("L_ORDERKEY") == 7, daft.col("L_ORDERKEY") == 10000, daft.lit(True)]),
)
def test_csv_filter_pushdowns_disjoint_predicate(path, pred, aws_public_s3_config):
    with_pushdown = MicroPartition.read_csv(
        path,
        convert_options=CsvConvertOptions(predicate=pred._expr, include_columns=["L_QUANTITY"]),
        parse_options=None,
        read_options=None,
        io_config=aws_public_s3_config,
    )
    after = (
        MicroPartition.read_csv(
            path, io_config=aws_public_s3_config, convert_options=None, parse_options=None, read_options=None
        )
        .filter([pred])
        .eval_expression_list([daft.col("L_QUANTITY")])
    )
    assert with_pushdown.to_arrow() == after.to_arrow()

from __future__ import annotations

from itertools import product

import pytest

import daft
from daft.daft import JsonConvertOptions
from daft.table import MicroPartition

PRED_PUSHDOWN_FILES = [
    "s3://daft-public-data/test_fixtures/json-dev/sampled-tpch.jsonl",
    "tests/assets/sampled-tpch.jsonl",
]


@pytest.mark.integration()
@pytest.mark.parametrize(
    "path, pred, limit",
    product(
        PRED_PUSHDOWN_FILES,
        [daft.col("L_ORDERKEY") == 7, daft.col("L_ORDERKEY") == 10000, daft.lit(True), None],
        [None, 1, 1000],
    ),
)
def test_json_filter_pushdowns(path, pred, limit, aws_public_s3_config):
    with_pushdown = MicroPartition.read_json(
        path,
        io_config=aws_public_s3_config,
        convert_options=JsonConvertOptions(limit=limit, predicate=pred._expr if pred is not None else None),
        parse_options=None,
        read_options=None,
    )
    after = MicroPartition.read_json(
        path, io_config=aws_public_s3_config, convert_options=None, parse_options=None, read_options=None
    )
    if pred is not None:
        after = after.filter([pred])
    if limit is not None:
        after = after.head(limit)
    assert with_pushdown.to_arrow() == after.to_arrow()

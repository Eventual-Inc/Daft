from __future__ import annotations

from typing import Any

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from daft.expressions import col
from daft.types import ExpressionType
from daft.udf import polars_udf, udf
from tests.conftest import assert_df_column_type, assert_df_equals
from tests.dataframe_cookbook.conftest import (
    parametrize_service_requests_csv_repartition,
)


class MyObj:
    def __init__(self, x: int):
        self._x = x

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, MyObj):
            return False
        return self._x == other._x


###
# Test different return containers (list vs numpy vs polars vs pandas vs arrow)
###


@udf(return_type=int)
def multiply_np(x, num=2, container="numpy"):
    arr = x * num
    if container == "numpy":
        return arr
    elif container == "list":
        return arr.tolist()
    elif container == "arrow":
        return pa.array(arr)
    elif container == "arrow_chunked":
        return pa.chunked_array([pa.array(arr)])
    elif container == "pandas":
        return pd.Series(arr)
    elif container == "polars":
        return pl.Series(arr)
    raise NotImplementedError(container)


@polars_udf(return_type=int)
def multiply_polars(x, num=2, container="numpy"):
    arr = x * num
    if container == "numpy":
        return arr.to_numpy()
    elif container == "list":
        return arr.to_list()
    elif container == "arrow":
        return arr.to_arrow()
    elif container == "arrow_chunked":
        return pa.chunked_array([arr.to_arrow()])
    elif container == "pandas":
        return arr.to_pandas()
    elif container == "polars":
        return arr
    raise NotImplementedError(container)


@parametrize_service_requests_csv_repartition
@pytest.mark.parametrize("multiply_kwarg", [multiply_np, multiply_polars])
@pytest.mark.parametrize(
    "return_container",
    ["numpy", "list", "arrow", "arrow_chunked", "pandas", "polars"],
)
def test_single_return_udf(daft_df, service_requests_csv_pd_df, repartition_nparts, multiply_kwarg, return_container):
    daft_df = daft_df.repartition(repartition_nparts).with_column(
        "unique_key_multiply_2", multiply_kwarg(col("Unique Key"), container=return_container)
    )
    service_requests_csv_pd_df["unique_key_multiply_2"] = service_requests_csv_pd_df["Unique Key"] * 2
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df)

    # Assert that the data after UDF runs is an ArrowDataBlock with type int64
    daft_df.collect()
    assert_df_column_type(
        daft_df._result,
        "unique_key_multiply_2",
        int,
    )


###
# Stateful UDFs
###


class MyModel:
    def predict(self, data):
        return data


@udf(return_type=int)
class RunModelNumpy:
    def __init__(self) -> None:
        self._model = MyModel()

    def __call__(self, x):
        return self._model.predict(x)


@polars_udf(return_type=int)
class RunModelPolars:
    def __init__(self) -> None:
        self._model = MyModel()

    def __call__(self, x):
        return self._model.predict(x)


@parametrize_service_requests_csv_repartition
@pytest.mark.parametrize("RunModel", [RunModelNumpy, RunModelPolars])
def test_dependency_injection_udf(daft_df, service_requests_csv_pd_df, repartition_nparts, RunModel):
    daft_df = daft_df.repartition(repartition_nparts).with_column("model_results", RunModel(col("Unique Key")))
    service_requests_csv_pd_df["model_results"] = service_requests_csv_pd_df["Unique Key"]
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df)

    # Assert that the data after UDF runs is an ArrowDataBlock with type int64
    daft_df.collect()
    assert_df_column_type(
        daft_df._result,
        "model_results",
        int,
    )


###
# .apply UDFs
###


def test_apply_type_inference(daft_df):
    def to_string(data: Any) -> str:
        return str(data)

    daft_df = daft_df.with_column("string_key", col("Unique Key").apply(to_string))

    daft_df.collect()
    assert_df_column_type(
        daft_df._result,
        "string_key",
        str,
    )


@parametrize_service_requests_csv_repartition
def test_apply_udf(daft_df, service_requests_csv_pd_df, repartition_nparts):
    daft_df = daft_df.repartition(repartition_nparts).with_column(
        "string_key", col("Unique Key").apply(lambda key: str(key))
    )
    service_requests_csv_pd_df["string_key"] = service_requests_csv_pd_df["Unique Key"].apply(str)
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df)
    daft_df.collect()
    assert_df_column_type(
        daft_df._result,
        "string_key",
        object,  # no return_type specified for .apply, column has PY[object] type
    )

    # Running .str expressions will fail on PyObj columns
    assert daft_df.schema()["string_key"].daft_type == ExpressionType.python_object()
    # TODO(jay): This should fail during column resolving instead of at runtime
    daft_df_fail = daft_df.with_column("string_key_starts_with_1", col("string_key").str.startswith("1"))
    with pytest.raises(AssertionError):
        daft_df_fail.to_pandas()

    # However, if we specify the return type then the blocks will be casted correctly to string types
    daft_df_pass = daft_df.with_column("string_key", col("string_key").apply(lambda x: x, return_type=str)).with_column(
        "string_key_starts_with_1", col("string_key").str.startswith("1")
    )
    service_requests_csv_pd_df["string_key_starts_with_1"] = service_requests_csv_pd_df["string_key"].str.startswith(
        "1"
    )
    daft_pd_df_pass = daft_df_pass.to_pandas()
    assert_df_equals(daft_pd_df_pass, service_requests_csv_pd_df)

    # Assert that the data after UDF runs is an ArrowDataBlock with type string and bool_
    assert daft_df_pass.schema()["string_key"].daft_type == ExpressionType.string()
    assert daft_df_pass.schema()["string_key_starts_with_1"].daft_type == ExpressionType.logical()
    daft_df_pass.collect()
    assert_df_column_type(
        daft_df_pass._result,
        "string_key",
        str,
    )
    assert_df_column_type(
        daft_df_pass._result,
        "string_key_starts_with_1",
        bool,
    )

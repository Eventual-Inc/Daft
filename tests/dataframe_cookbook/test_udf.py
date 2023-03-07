# TODO: [RUST-INT][UDF] Enable after adding support for UDFs

# from __future__ import annotations

from __future__ import annotations

# from typing import Any
# import numpy as np
# import pytest

# from daft import DataFrame
# from daft.datatype import DataType
# from daft.expressions import col
# from daft.udf import udf
# from tests.conftest import assert_df_equals
# from tests.dataframe_cookbook.conftest import (
#     parametrize_service_requests_csv_repartition,
# )


# class MyObj:
#     def __init__(self, x: int):
#         self._x = x

#     def __eq__(self, other: Any) -> bool:
#         if not isinstance(other, MyObj):
#             return False
#         return self._x == other._x


# ###
# # Test different return containers (list vs numpy vs polars vs pandas vs arrow)
# ###


# @udf(return_dtype=int, input_columns={"x": np.ndarray})
# def multiply_np(x: np.ndarray, num=2):
#     return x * num


# @parametrize_service_requests_csv_repartition
# def test_single_return_udf(daft_df: DataFrame, service_requests_csv_pd_df, repartition_nparts):
#     daft_df = daft_df.repartition(repartition_nparts).with_column(
#         "unique_key_multiply_2", multiply_np(col("Unique Key"))
#     )
#     service_requests_csv_pd_df["unique_key_multiply_2"] = service_requests_csv_pd_df["Unique Key"] * 2
#     daft_pd_df = daft_df.to_pandas()
#     assert_df_equals(daft_pd_df, service_requests_csv_pd_df)


# ###
# # Stateful UDFs
# ###


# class MyModel:
#     def predict(self, data):
#         return data


# @udf(return_dtype=int, input_columns={"x": np.ndarray})
# class RunModelNumpy:
#     def __init__(self) -> None:
#         self._model = MyModel()

#     def __call__(self, x: np.ndarray):
#         return self._model.predict(x)


# @parametrize_service_requests_csv_repartition
# def test_dependency_injection_udf(daft_df, service_requests_csv_pd_df, repartition_nparts):
#     daft_df = daft_df.repartition(repartition_nparts).with_column("model_results", RunModelNumpy(col("Unique Key")))
#     service_requests_csv_pd_df["model_results"] = service_requests_csv_pd_df["Unique Key"]
#     daft_pd_df = daft_df.to_pandas()
#     assert_df_equals(daft_pd_df, service_requests_csv_pd_df)


# ###
# # .apply UDFs
# ###


# def test_apply_type_inference(daft_df):
#     def to_string(data: Any) -> str:
#         return str(data)

#     daft_df = daft_df.with_column("string_key", col("Unique Key").apply(to_string))
#     daft_df.collect()


# @parametrize_service_requests_csv_repartition
# def test_apply_udf(daft_df, service_requests_csv_pd_df, repartition_nparts):
#     daft_df = daft_df.repartition(repartition_nparts).with_column(
#         "string_key", col("Unique Key").apply(lambda key: str(key))
#     )
#     service_requests_csv_pd_df["string_key"] = service_requests_csv_pd_df["Unique Key"].apply(str)
#     daft_pd_df = daft_df.to_pandas()
#     assert_df_equals(daft_pd_df, service_requests_csv_pd_df)

#     # Running .str expressions will fail on PyObj columns
#     assert daft_df.schema()["string_key"].dtype == DataType.python_object()
#     # TODO(jay): This should fail during column resolving instead of at runtime
#     daft_df_fail = daft_df.with_column("string_key_starts_with_1", col("string_key").str.startswith("1"))
#     with pytest.raises(AssertionError):
#         daft_df_fail.to_pandas()

#     # However, if we specify the return type then the blocks will be casted correctly to string types
#     daft_df_pass = daft_df.with_column(
#         "string_key", col("string_key").apply(lambda x: x, return_dtype=str)
#     ).with_column("string_key_starts_with_1", col("string_key").str.startswith("1"))
#     service_requests_csv_pd_df["string_key_starts_with_1"] = service_requests_csv_pd_df["string_key"].str.startswith(
#         "1"
#     )
#     daft_pd_df_pass = daft_df_pass.to_pandas()
#     assert_df_equals(daft_pd_df_pass, service_requests_csv_pd_df)

#     assert daft_df_pass.schema()["string_key"].dtype == DataType.string()
#     assert daft_df_pass.schema()["string_key_starts_with_1"].dtype == DataType.bool()
#     daft_df_pass.collect()

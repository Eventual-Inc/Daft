from typing import Any

from daft.expressions import col, udf
from tests.conftest import assert_df_equals
from tests.dataframe_cookbook.conftest import (
    parametrize_service_requests_csv_daft_df,
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
# Simple UDFs with one or two return types
###


@udf(return_type=int)
def multiply_kwarg(x, num=2):
    return x * num


@udf(return_type=int)
def multiply(x, num):
    return x * num


@parametrize_service_requests_csv_daft_df
@parametrize_service_requests_csv_repartition
def test_single_return_udf(daft_df, service_requests_csv_pd_df, repartition_nparts):
    daft_df = daft_df.repartition(repartition_nparts).with_column(
        "unique_key_identity", multiply_kwarg(col("Unique Key"))
    )
    service_requests_csv_pd_df["unique_key_identity"] = service_requests_csv_pd_df["Unique Key"] * 2
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df)


@parametrize_service_requests_csv_daft_df
@parametrize_service_requests_csv_repartition
def test_udf_args(daft_df, service_requests_csv_pd_df, repartition_nparts):
    daft_df = daft_df.repartition(repartition_nparts).with_column("unique_key_identity", multiply(col("Unique Key"), 2))
    service_requests_csv_pd_df["unique_key_identity"] = service_requests_csv_pd_df["Unique Key"] * 2
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df)


@parametrize_service_requests_csv_daft_df
@parametrize_service_requests_csv_repartition
def test_udf_kwargs(daft_df, service_requests_csv_pd_df, repartition_nparts):
    daft_df = daft_df.repartition(repartition_nparts).with_column(
        "unique_key_identity", multiply_kwarg(col("Unique Key"), num=2)
    )
    service_requests_csv_pd_df["unique_key_identity"] = service_requests_csv_pd_df["Unique Key"] * 2
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df)


# ###
# # Stateful UDF using dependency injection
# ###


# class MyModel:
#     def predict(data: pd.Series):
#         return data


# def my_initializations():
#     return MyModel()


# @udf(return_type=int)
# def run_model(x: pd.Series, my_model: MyModel = daft.Depends(my_initializations)) -> pd.Series:
#     return my_model.predict(x)


# @pytest.mark.tdd
# @parametrize_service_requests_csv_daft_df
# @parametrize_service_requests_csv_repartition
# def test_dependency_injection_udf(daft_df, service_requests_csv_pd_df, repartition_nparts):
#     daft_df = daft_df.repartition(repartition_nparts).with_column("model_results", run_model(col("Unique Key")))
#     service_requests_csv_pd_df["model_results"] = service_requests_csv_pd_df["Unique Key"]
#     daft_pd_df = daft_df.to_pandas()
#     assert_df_equals(daft_pd_df, service_requests_csv_pd_df)

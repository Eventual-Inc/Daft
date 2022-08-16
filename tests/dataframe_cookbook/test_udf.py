from daft.expressions import col, udf
from tests.conftest import assert_df_equals
from tests.dataframe_cookbook.conftest import (
    parametrize_service_requests_csv_daft_df,
    parametrize_service_requests_csv_repartition,
)

###
# Simple UDFs with one or two return types
###


@udf(return_type=int)
def multiply_two(x):
    return x * 2


@udf(return_type=[int, str])
def int_and_str(x):
    return (x, x.astype(str))


@parametrize_service_requests_csv_daft_df
@parametrize_service_requests_csv_repartition
def test_single_return_udf(daft_df, service_requests_csv_pd_df, repartition_nparts):
    daft_df = daft_df.repartition(repartition_nparts).with_column(
        "unique_key_identity", multiply_two(col("Unique Key"))
    )
    service_requests_csv_pd_df["unique_key_identity"] = service_requests_csv_pd_df["Unique Key"] * 2
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df)


@parametrize_service_requests_csv_daft_df
@parametrize_service_requests_csv_repartition
def test_multi_return_udf(daft_df, service_requests_csv_pd_df, repartition_nparts):
    int_col, str_col = int_and_str(col("Unique Key"))
    daft_df = (
        daft_df.repartition(repartition_nparts)
        .with_column("unique_key_identity", int_col)
        .with_column("unique_key_identity_str", str_col)
    )
    service_requests_csv_pd_df["unique_key_identity"] = service_requests_csv_pd_df["Unique Key"]
    service_requests_csv_pd_df["unique_key_identity_str"] = service_requests_csv_pd_df["Unique Key"].astype(str)
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

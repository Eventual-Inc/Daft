import pandas as pd
import pytest

from daft.dataframe import DataFrame
from daft.expressions import col
from tests.conftest import assert_df_equals

SCHEMA = {
    "part": [
        "P_PARTKEY",
        "P_NAME",
        "P_MFGR",
        "P_BRAND",
        "P_TYPE",
        "P_SIZE",
        "P_CONTAINER",
        "P_RETAILPRICE",
        "P_COMMENT",
    ],
    "supplier": [
        "S_SUPPKEY",
        "S_NAME",
        "S_ADDRESS",
        "S_NATIONKEY",
        "S_PHONE",
        "S_ACCTBAL",
        "S_COMMENT",
    ],
    "partsupp": [
        "PS_PARTKEY",
        "PS_SUPPKEY",
        "PS_AVAILQTY",
        "PS_SUPPLYCOST",
        "PS_COMMENT",
    ],
    "customer": [
        "C_CUSTKEY",
        "C_NAME",
        "C_ADDRESS",
        "C_NATIONKEY",
        "C_PHONE",
        "C_ACCTBAL",
        "C_MKTSEGMENT",
        "C_COMMENT",
    ],
    "orders": [
        "O_ORDERKEY",
        "O_CUSTKEY",
        "O_ORDERSTATUS",
        "O_TOTALPRICE",
        "O_ORDERDATE",
        "O_ORDERPRIORITY",
        "O_CLERK",
        "O_SHIPPRIORITY",
        "O_COMMENT",
    ],
    "lineitem": [
        "L_ORDERKEY",
        "L_PARTKEY",
        "L_SUPPKEY",
        "L_LINENUMBER",
        "L_QUANTITY",
        "L_EXTENDEDPRICE",
        "L_DISCOUNT",
        "L_TAX",
        "L_RETURNFLAG",
        "L_LINESTATUS",
        "L_SHIPDATE",
        "L_COMMITDATE",
        "L_RECEIPTDATE",
        "L_SHIPINSTRUCT",
        "L_SHIPMODE",
        "L_COMMENT",
    ],
    "nation": [
        "N_NATIONKEY",
        "N_NAME",
        "N_REGIONKEY",
        "N_COMMENT",
    ],
    "region": [
        "R_REGIONKEY",
        "R_NAME",
        "R_COMMENT",
    ],
}


@pytest.fixture(scope="function")
def lineitem():
    return DataFrame.from_csv(
        "data/tpch/lineitem.tbl", headers=False, column_names=SCHEMA["lineitem"] + [""], delimiter="|"
    )


@pytest.mark.tpch
def test_tpch_q1(lineitem):
    import pdb

    pdb.set_trace()
    discounted_price = col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT"))
    taxed_discounted_price = discounted_price * (1 + col("L_TAX"))
    daft_df = (
        lineitem.where(col("L_SHIPDATE") <= "1998-09-02")
        .groupby(col("L_RETURNFLAG"), col("L_LINESTATUS"))
        .agg(
            col("L_QUANTITY").agg.sum().alias("sum_qty"),
            col("L_EXTENDEDPRICE").agg.sum().alias("sum_base_price"),
            discounted_price.agg.sum().alias("sum_disc_price"),
            taxed_discounted_price.agg.sum().alias("sum_charge"),
            col("L_QUANTITY").agg.mean().alias("avg_qty"),
            col("L_EXTENDEDPRICE").agg.mean().alias("avg_price"),
            col("L_DISCOUNT").agg.mean().alias("avg_disc"),
            col("L_QUANTITY").agg.count().alias("count_order"),
        )
        .sort(col("L_RETURNFLAG"), col("L_LINESTATUS"))
    )
    answer = pd.read_csv("data/tpch/answers/q1.out", delimiter="|")
    answer.columns = [
        "L_RETURNFLAG",
        "L_LINESTATUS",
        "sum_qty",
        "sum_base_price",
        "sum_disc_price",
        "sum_charge",
        "avg_qty",
        "avg_price",
        "avg_disc",
        "count_order",
    ]
    assert_df_equals(daft_df, answer)

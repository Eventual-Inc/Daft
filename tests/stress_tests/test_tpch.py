import datetime
import os
import shlex
import subprocess

import pandas as pd
import pyarrow as pa
import pytest

from daft.dataframe import DataFrame
from daft.expressions import col

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
def gen_tpch():
    script = "scripts/tpch-gen.sh"
    if not os.path.exists("data/tpch"):
        subprocess.check_output(shlex.split(f"{script}"))


@pytest.fixture(scope="function")
def lineitem(gen_tpch):
    return DataFrame.from_csv(
        "data/tpch/lineitem.tbl", has_headers=False, column_names=SCHEMA["lineitem"] + [""], delimiter="|"
    )


def test_tpch_q1(lineitem, tmp_path):
    discounted_price = col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT"))
    taxed_discounted_price = discounted_price * (1 + col("L_TAX"))
    daft_df = (
        lineitem.where(col("L_SHIPDATE") <= pa.scalar(datetime.date(1998, 9, 2)))
        .groupby(col("L_RETURNFLAG"), col("L_LINESTATUS"))
        .agg(
            [
                (col("L_QUANTITY").alias("sum_qty"), "sum"),
                (col("L_EXTENDEDPRICE").alias("sum_base_price"), "sum"),
                (discounted_price.alias("sum_disc_price"), "sum"),
                (taxed_discounted_price.alias("sum_charge"), "sum"),
                (col("L_QUANTITY").alias("avg_qty"), "mean"),
                (col("L_EXTENDEDPRICE").alias("avg_price"), "mean"),
                (col("L_DISCOUNT").alias("avg_disc"), "mean"),
                (col("L_QUANTITY").alias("count_order"), "count"),
            ]
        )
        .sort(col("L_RETURNFLAG"))
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
    daft_pd_df = daft_df.to_pandas()
    daft_pd_df = daft_pd_df.sort_values(by=["L_RETURNFLAG", "L_LINESTATUS"])  # WE don't have multicolumn sort
    csv_out = f"{tmp_path}/q1.out"
    daft_pd_df.to_csv(csv_out, sep="|", line_terminator="|\n", index=False)

    assert run_tpch_checker(1, csv_out)


def run_tpch_checker(q_num: int, result_file: str) -> bool:
    script = "./cmpq.pl"
    answer = f"../answers/q{q_num}.out"

    output = subprocess.check_output(
        shlex.split(f"{script} {q_num} {result_file} {answer}"), cwd="data/tpch/check_answers"
    )
    return output.decode() == f"Query {q_num} 0 unacceptable missmatches\n"

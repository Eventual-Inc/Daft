import datetime
import os
import shlex
import subprocess

import pandas as pd
import pyarrow as pa
import pytest

from daft.dataframe import DataFrame
from daft.expressions import col, udf

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
def tables(gen_tpch):
    return {
        tbl_name: DataFrame.from_csv(
            f"data/tpch/{tbl_name}.tbl", has_headers=False, column_names=SCHEMA[tbl_name] + [""], delimiter="|"
        )
        for tbl_name in SCHEMA
    }


@pytest.mark.parametrize("num_partitions", [None, 3])
def test_tpch_q1(tables, tmp_path, num_partitions):
    if num_partitions is not None:
        for key in tables:
            tables[key] = tables[key].repartition(num_partitions)

    lineitem = tables["lineitem"]
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


@pytest.mark.parametrize("num_partitions", [None, 3])
def test_tpch_q2(tables, tmp_path, num_partitions):
    if num_partitions is not None:
        for key in tables:
            tables[key] = tables[key].repartition(num_partitions)

    @udf(return_type=bool)
    def ends_with(column, suffix):
        return column.str.endswith(suffix)

    region = tables["region"]
    nation = tables["nation"]
    supplier = tables["supplier"]
    partsupp = tables["partsupp"]
    part = tables["part"]

    europe = (
        region.where(col("R_NAME") == "EUROPE")
        .join(nation, left_on=col("R_REGIONKEY"), right_on=col("N_REGIONKEY"))
        .join(supplier, left_on=col("N_NATIONKEY"), right_on=col("S_NATIONKEY"))
        .join(partsupp, left_on=col("S_SUPPKEY"), right_on=col("PS_SUPPKEY"))
    )

    brass = part.where((col("P_SIZE") == 15) and (ends_with(col("p_type"), "BRASS"))).join(
        europe,
        left_on=col("P_PARTKEY"),
        right_on=col("PS_PARTKEY"),
    )

    min_cost = brass.groupby(col("PS_PARTKEY")).agg(
        [
            (col("PS_SUPPLYCOST").alias("min"), "min"),
        ]
    )

    daft_df = (
        brass.join(min_cost, on=col("PS_PARTKEY"))
        .where(col("PS_SUPPLYCOST") == col("min"))
        .select(
            col("S_ACCTBAL"),
            col("S_NAME"),
            col("N_NAME"),
            col("P_PARTKEY"),
            col("P_MFGR"),
            col("S_ADDRESS"),
            col("S_PHONE"),
            col("S_COMMENT"),
        )
    )

    # Multicol sorts not implemented yet
    daft_pd_df = daft_df.to_pandas()
    daft_pd_df = daft_pd_df.sort_values(
        by=["S_ACCTBAL", "N_NAME", "S_NAME", "P_PARTKEY"], ascending=[False, True, True, True]
    )
    daft_pd_df = daft_pd_df.head(100)
    csv_out = f"{tmp_path}/q2.out"
    daft_pd_df.to_csv(csv_out, sep="|", line_terminator="|\n", index=False)

    assert run_tpch_checker(2, csv_out)


def run_tpch_checker(q_num: int, result_file: str) -> bool:
    script = "./cmpq.pl"
    answer = f"../answers/q{q_num}.out"

    output = subprocess.check_output(
        shlex.split(f"{script} {q_num} {result_file} {answer}"), cwd="data/tpch/check_answers"
    )
    return output.decode() == f"Query {q_num} 0 unacceptable missmatches\n"

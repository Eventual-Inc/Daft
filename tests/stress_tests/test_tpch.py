import datetime
import os
import pathlib
import shlex
import sqlite3
import subprocess
from typing import Optional

import pandas as pd
import pytest

from daft.dataframe import DataFrame
from daft.expressions import col, udf
from tests.conftest import assert_df_equals

# If running in github, we use smaller-scale data
SQLITE_DB_FILE_PATH = "data/tpch-sqlite/TPC-H.db"

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


@pytest.fixture(scope="function", autouse=True)
def gen_tpch():
    script = "scripts/tpch-gen.sh"
    if not os.path.exists("data/tpch-sqlite"):
        # If running in CI, use a scale factor of 0.2
        # Otherwise, check for TPCH_SCALE_FACTOR env variable or default to 1
        scale_factor = float(os.getenv("TPCH_SCALE_FACTOR", "1"))
        scale_factor = 0.2 if os.getenv("CI") else scale_factor
        subprocess.check_output(shlex.split(f"{script} {scale_factor}"))


def get_df(tbl_name: str, num_partitions: Optional[int] = None):
    df = DataFrame.from_csv(
        f"data/tpch-sqlite/tpch-dbgen/{tbl_name}.tbl",
        has_headers=False,
        column_names=SCHEMA[tbl_name] + [""],
        delimiter="|",
    )
    df = df.exclude("")
    if num_partitions is not None:
        df = df.repartition(num_partitions)
    return df


def get_data_size_gb():
    # Check the size of the sqlite db
    return os.path.getsize(SQLITE_DB_FILE_PATH) / (1024**3)


def check_answer(daft_pd_df: pd.DataFrame, tpch_question: int, tmp_path: str):
    # If comparing data smaller than 1GB, we fall back onto using SQLite for checking correctness
    if get_data_size_gb() < 1.0:
        query = pathlib.Path(f"tests/assets/tpch-sqlite-queries/{tpch_question}.sql").read_text()
        conn = sqlite3.connect(SQLITE_DB_FILE_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
        cursor = conn.cursor()
        res = cursor.execute(query)
        sqlite_results = res.fetchall()
        sqlite_pd_results = pd.DataFrame.from_records(sqlite_results, columns=daft_pd_df.columns)
        assert_df_equals(daft_pd_df, sqlite_pd_results, assert_ordering=True)
    else:
        csv_out = f"{tmp_path}/q{tpch_question}.out"
        daft_pd_df.to_csv(csv_out, sep="|", line_terminator="|\n", index=False)
        assert run_tpch_checker(tpch_question, csv_out)


@pytest.mark.parametrize("num_partitions", [None, 3])
def test_tpch_q1(tmp_path, num_partitions, ray_cluster):
    lineitem = get_df("lineitem", num_partitions=num_partitions)
    discounted_price = col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT"))
    taxed_discounted_price = discounted_price * (1 + col("L_TAX"))
    daft_df = (
        lineitem.where(col("L_SHIPDATE") <= datetime.date(1998, 9, 2))
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

    daft_pd_df = daft_df.to_pandas()
    daft_pd_df = daft_pd_df.sort_values(by=["L_RETURNFLAG", "L_LINESTATUS"])  # WE don't have multicolumn sort
    check_answer(daft_pd_df, 1, tmp_path)


@pytest.mark.parametrize("num_partitions", [None, 3])
def test_tpch_q2(tmp_path, num_partitions, ray_cluster):
    @udf(return_type=bool)
    def ends_with(column, suffix):
        return column.str.endswith(suffix)

    region = get_df("region", num_partitions=num_partitions)
    nation = get_df("nation", num_partitions=num_partitions)
    supplier = get_df("supplier", num_partitions=num_partitions)
    partsupp = get_df("partsupp", num_partitions=num_partitions)
    part = get_df("part", num_partitions=num_partitions)

    europe = (
        region.where(col("R_NAME") == "EUROPE")
        .join(nation, left_on=col("R_REGIONKEY"), right_on=col("N_REGIONKEY"))
        .join(supplier, left_on=col("N_NATIONKEY"), right_on=col("S_NATIONKEY"))
        .join(partsupp, left_on=col("S_SUPPKEY"), right_on=col("PS_SUPPKEY"))
    )

    brass = part.where((col("P_SIZE") == 15) & (ends_with(col("P_TYPE"), "BRASS"))).join(
        europe,
        left_on=col("P_PARTKEY"),
        right_on=col("PS_PARTKEY"),
    )
    min_cost = brass.groupby(col("P_PARTKEY")).agg(
        [
            (col("PS_SUPPLYCOST").alias("min"), "min"),
        ]
    )

    daft_df = (
        brass.join(min_cost, on=col("P_PARTKEY"))
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
    check_answer(daft_pd_df, 2, tmp_path)


@pytest.mark.parametrize("num_partitions", [None, 3])
def test_tpch_q3(tmp_path, num_partitions, ray_cluster):
    def decrease(x, y):
        return x * (1 - y)

    customer = get_df("customer", num_partitions=num_partitions).where(col("C_MKTSEGMENT") == "BUILDING")
    orders = get_df("orders", num_partitions=num_partitions).where(col("O_ORDERDATE") < datetime.date(1995, 3, 15))
    lineitem = get_df("lineitem", num_partitions=num_partitions).where(col("L_SHIPDATE") > datetime.date(1995, 3, 15))

    daft_df = (
        customer.join(orders, left_on=col("C_CUSTKEY"), right_on=col("O_CUSTKEY"))
        .select(col("O_ORDERKEY"), col("O_ORDERDATE"), col("O_SHIPPRIORITY"))
        .join(lineitem, left_on=col("O_ORDERKEY"), right_on=col("L_ORDERKEY"))
        .select(
            col("O_ORDERKEY"),
            decrease(col("L_EXTENDEDPRICE"), col("L_DISCOUNT")).alias("volume"),
            col("O_ORDERDATE"),
            col("O_SHIPPRIORITY"),
        )
        .groupby(col("O_ORDERKEY"), col("O_ORDERDATE"), col("O_SHIPPRIORITY"))
        .agg([(col("volume").alias("revenue"), "sum")])
    )

    # Multicol sorts not implemented yet
    daft_pd_df = daft_df.to_pandas()
    daft_pd_df = daft_pd_df.sort_values(by=["revenue", "O_ORDERDATE"], ascending=[False, True])
    daft_pd_df = daft_pd_df.head(10)
    daft_pd_df = daft_pd_df[["O_ORDERKEY", "revenue", "O_ORDERDATE", "O_SHIPPRIORITY"]]
    check_answer(daft_pd_df, 3, tmp_path)


@pytest.mark.parametrize("num_partitions", [None, 3])
def test_tpch_q4(tmp_path, num_partitions):
    orders = get_df("orders", num_partitions=num_partitions).where(
        (col("O_ORDERDATE") >= datetime.date(1993, 7, 1)) & (col("O_ORDERDATE") < datetime.date(1993, 10, 1))
    )

    lineitems = (
        get_df("lineitem", num_partitions=num_partitions)
        .where(col("L_COMMITDATE") < col("L_RECEIPTDATE"))
        .select(col("L_ORDERKEY"))
        .distinct()
    )

    daft_df = (
        lineitems.join(orders, left_on=col("L_ORDERKEY"), right_on=col("O_ORDERKEY"))
        .groupby(col("O_ORDERPRIORITY"))
        .agg([(col("L_ORDERKEY").alias("order_count"), "count")])
        .sort(col("O_ORDERPRIORITY"))
    )

    daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 4, tmp_path)


@pytest.mark.parametrize("num_partitions", [None, 3])
def test_tpch_q5(tmp_path, num_partitions, ray_cluster):
    orders = get_df("orders", num_partitions=num_partitions).where(
        (col("O_ORDERDATE") >= datetime.date(1994, 1, 1)) & (col("O_ORDERDATE") < datetime.date(1995, 1, 1))
    )
    region = get_df("region", num_partitions=num_partitions).where(col("R_NAME") == "ASIA")
    nation = get_df("nation", num_partitions=num_partitions)
    supplier = get_df("supplier", num_partitions=num_partitions)
    lineitem = get_df("lineitem", num_partitions=num_partitions)
    customer = get_df("customer", num_partitions=num_partitions)

    daft_df = (
        region.join(nation, left_on=col("R_REGIONKEY"), right_on=col("N_REGIONKEY"))
        .join(supplier, left_on=col("N_NATIONKEY"), right_on=col("S_NATIONKEY"))
        .join(lineitem, left_on=col("S_SUPPKEY"), right_on=col("L_SUPPKEY"))
        .select(col("N_NAME"), col("L_EXTENDEDPRICE"), col("L_DISCOUNT"), col("L_ORDERKEY"), col("N_NATIONKEY"))
        .join(orders, left_on=col("L_ORDERKEY"), right_on=col("O_ORDERKEY"))
        # Multiple && join conditions expressed as a JOIN and a WHERE instead
        .join(customer, left_on=col("O_CUSTKEY"), right_on=col("C_CUSTKEY"))
        .where(col("N_NATIONKEY") == col("C_NATIONKEY"))
        .select(col("N_NAME"), (col("L_EXTENDEDPRICE") * (1 - col("L_DISCOUNT"))).alias("value"))
        .groupby(col("N_NAME"))
        .agg([(col("value").alias("revenue"), "sum")])
        .sort(col("revenue"), desc=True)
    )

    daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 5, tmp_path)


@pytest.mark.parametrize("num_partitions", [None, 3])
def test_tpch_q6(tmp_path, num_partitions, ray_cluster):
    lineitem = get_df("lineitem", num_partitions=num_partitions)
    daft_df = lineitem.where(
        (col("L_SHIPDATE") >= datetime.date(1994, 1, 1))
        & (col("L_SHIPDATE") < datetime.date(1995, 1, 1))
        & (col("L_DISCOUNT") >= 0.05)
        & (col("L_DISCOUNT") <= 0.07)
        & (col("L_QUANTITY") < 24)
    ).sum(col("L_EXTENDEDPRICE") * col("L_DISCOUNT"))

    daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 6, tmp_path)


@pytest.mark.parametrize("num_partitions", [None, 3])
def test_tpch_q7(tmp_path, num_partitions):
    def decrease(x, y):
        return x * (1 - y)

    @udf(return_type=int)
    def get_year(d):
        # TODO: should not need to coerce here after proper type conversions
        return pd.to_datetime(d).dt.year

    lineitem = get_df("lineitem", num_partitions=num_partitions).where(
        (col("L_SHIPDATE") >= datetime.date(1995, 1, 1)) & (col("L_SHIPDATE") <= datetime.date(1996, 12, 31))
    )
    nation = get_df("nation", num_partitions=num_partitions).where(
        (col("N_NAME") == "FRANCE") | (col("N_NAME") == "GERMANY")
    )
    supplier = get_df("supplier", num_partitions=num_partitions)
    customer = get_df("customer", num_partitions=num_partitions)
    orders = get_df("orders", num_partitions=num_partitions)

    supNation = (
        nation.join(supplier, left_on=col("N_NATIONKEY"), right_on=col("S_NATIONKEY"))
        .join(lineitem, left_on=col("S_SUPPKEY"), right_on=col("L_SUPPKEY"))
        .select(
            col("N_NAME").alias("supp_nation"),
            col("L_ORDERKEY"),
            col("L_EXTENDEDPRICE"),
            col("L_DISCOUNT"),
            col("L_SHIPDATE"),
        )
    )

    daft_df = (
        nation.join(customer, left_on=col("N_NATIONKEY"), right_on=col("C_NATIONKEY"))
        .join(orders, left_on=col("C_CUSTKEY"), right_on=col("O_CUSTKEY"))
        .select(col("N_NAME").alias("cust_nation"), col("O_ORDERKEY"))
        .join(supNation, left_on=col("O_ORDERKEY"), right_on=col("L_ORDERKEY"))
        .where(
            ((col("supp_nation") == "FRANCE") & (col("cust_nation") == "GERMANY"))
            | ((col("supp_nation") == "GERMANY") & (col("cust_nation") == "FRANCE"))
        )
        .select(
            col("supp_nation"),
            col("cust_nation"),
            get_year(col("L_SHIPDATE")).alias("l_year"),
            decrease(col("L_EXTENDEDPRICE"), col("L_DISCOUNT")).alias("volume"),
        )
        .groupby(col("supp_nation"), col("cust_nation"), col("l_year"))
        .agg([(col("volume").alias("revenue"), "sum")])
    )

    # Multicol sorts not implemented yet
    daft_pd_df = daft_df.to_pandas()
    daft_pd_df = daft_pd_df.sort_values(by=["supp_nation", "cust_nation", "l_year"])
    check_answer(daft_pd_df, 7, tmp_path)


@pytest.mark.parametrize("num_partitions", [None, 3])
def test_tpch_q8(tmp_path, num_partitions, ray_cluster):
    lineitem = get_df("lineitem", num_partitions=num_partitions)

    def decrease(x, y):
        return x * (1 - y)

    @udf(return_type=int)
    def get_year(d):
        # TODO: should not need to coerce here after proper type conversions
        return pd.to_datetime(d).dt.year

    @udf(return_type=float)
    def is_brazil(nation, y):
        return y.where(nation == "BRAZIL", 0.0)

    region = get_df("region", num_partitions=num_partitions).where(col("R_NAME") == "AMERICA")
    orders = get_df("orders", num_partitions=num_partitions).where(
        (col("O_ORDERDATE") <= datetime.date(1996, 12, 31)) & (col("O_ORDERDATE") >= datetime.date(1995, 1, 1))
    )
    part = get_df("part", num_partitions=num_partitions).where(col("P_TYPE") == "ECONOMY ANODIZED STEEL")
    nation = get_df("nation", num_partitions=num_partitions)
    supplier = get_df("supplier", num_partitions=num_partitions)
    lineitem = get_df("lineitem", num_partitions=num_partitions)
    customer = get_df("customer", num_partitions=num_partitions)

    nat = nation.join(supplier, left_on=col("N_NATIONKEY"), right_on=col("S_NATIONKEY"))

    line = (
        lineitem.select(
            col("L_PARTKEY"),
            col("L_SUPPKEY"),
            col("L_ORDERKEY"),
            decrease(col("L_EXTENDEDPRICE"), col("L_DISCOUNT")).alias("volume"),
        )
        .join(part, left_on=col("L_PARTKEY"), right_on=col("P_PARTKEY"))
        .join(nat, left_on=col("L_SUPPKEY"), right_on=col("S_SUPPKEY"))
    )

    daft_df = (
        nation.join(region, left_on=col("N_REGIONKEY"), right_on=col("R_REGIONKEY"))
        .select(col("N_NATIONKEY"))
        .join(customer, left_on=col("N_NATIONKEY"), right_on=col("C_NATIONKEY"))
        .select(col("C_CUSTKEY"))
        .join(orders, left_on=col("C_CUSTKEY"), right_on=col("O_CUSTKEY"))
        .select(col("O_ORDERKEY"), col("O_ORDERDATE"))
        .join(line, left_on=col("O_ORDERKEY"), right_on=col("L_ORDERKEY"))
        .select(
            get_year(col("O_ORDERDATE")).alias("o_year"),
            col("volume"),
            is_brazil(col("N_NAME"), col("volume")).alias("case_volume"),
        )
        .groupby(col("o_year"))
        .agg([(col("case_volume").alias("case_volume_sum"), "sum"), (col("volume").alias("volume_sum"), "sum")])
        .select(col("o_year"), col("case_volume_sum") / col("volume_sum"))
        .sort(col("o_year"))
    )

    daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 8, tmp_path)


@pytest.mark.parametrize("num_partitions", [None, 3])
def test_tpch_q9(tmp_path, num_partitions, ray_cluster):
    lineitem = get_df("lineitem", num_partitions=num_partitions)
    part = get_df("part", num_partitions=num_partitions)
    nation = get_df("nation", num_partitions=num_partitions)
    supplier = get_df("supplier", num_partitions=num_partitions)
    partsupp = get_df("partsupp", num_partitions=num_partitions)
    orders = get_df("orders", num_partitions=num_partitions)

    @udf(return_type=bool)
    def contains_green(s):
        return s.str.contains("green")

    @udf(return_type=int)
    def get_year(d):
        # TODO: should not need to coerce here after proper type conversions
        return pd.to_datetime(d).dt.year

    def expr(x, y, v, w):
        return x * (1 - y) - (v * w)

    linepart = part.where(contains_green(col("P_NAME"))).join(
        lineitem, left_on=col("P_PARTKEY"), right_on=col("L_PARTKEY")
    )
    natsup = nation.join(supplier, left_on=col("N_NATIONKEY"), right_on=col("S_NATIONKEY"))

    daft_df = (
        linepart.join(natsup, left_on=col("L_SUPPKEY"), right_on=col("S_SUPPKEY"))
        .join(partsupp, left_on=col("L_SUPPKEY"), right_on=col("PS_SUPPKEY"))
        .where(col("P_PARTKEY") == col("PS_PARTKEY"))
        .join(orders, left_on=col("L_ORDERKEY"), right_on=col("O_ORDERKEY"))
        .select(
            col("N_NAME"),
            get_year(col("O_ORDERDATE")).alias("o_year"),
            expr(col("L_EXTENDEDPRICE"), col("L_DISCOUNT"), col("PS_SUPPLYCOST"), col("L_QUANTITY")).alias("amount"),
        )
        .groupby(col("N_NAME"), col("o_year"))
        .agg([(col("amount"), "sum")])
    )

    daft_pd_df = daft_df.to_pandas()
    daft_pd_df = daft_pd_df.sort_values(by=["N_NAME", "o_year"], ascending=[True, False])
    check_answer(daft_pd_df, 9, tmp_path)


@pytest.mark.parametrize("num_partitions", [None, 3])
def test_tpch_q10(tmp_path, num_partitions, ray_cluster):
    def decrease(x, y):
        return x * (1 - y)

    lineitem = get_df("lineitem", num_partitions=num_partitions).where(col("L_RETURNFLAG") == "R")
    orders = get_df("orders", num_partitions=num_partitions)
    nation = get_df("nation", num_partitions=num_partitions)
    customer = get_df("customer", num_partitions=num_partitions)

    daft_df = (
        orders.where(
            (col("O_ORDERDATE") < datetime.date(1994, 1, 1)) & (col("O_ORDERDATE") >= datetime.date(1993, 10, 1))
        )
        .join(customer, left_on=col("O_CUSTKEY"), right_on=col("C_CUSTKEY"))
        .join(nation, left_on=col("C_NATIONKEY"), right_on=col("N_NATIONKEY"))
        .join(lineitem, left_on=col("O_ORDERKEY"), right_on=col("L_ORDERKEY"))
        .select(
            col("O_CUSTKEY"),
            col("C_NAME"),
            decrease(col("L_EXTENDEDPRICE"), col("L_DISCOUNT")).alias("volume"),
            col("C_ACCTBAL"),
            col("N_NAME"),
            col("C_ADDRESS"),
            col("C_PHONE"),
            col("C_COMMENT"),
        )
        .groupby(
            col("O_CUSTKEY"),
            col("C_NAME"),
            col("C_ACCTBAL"),
            col("C_PHONE"),
            col("N_NAME"),
            col("C_ADDRESS"),
            col("C_COMMENT"),
        )
        .agg([(col("volume").alias("revenue"), "sum")])
        .sort(col("revenue"), desc=True)
        .select(
            col("O_CUSTKEY"),
            col("C_NAME"),
            col("revenue"),
            col("C_ACCTBAL"),
            col("N_NAME"),
            col("C_ADDRESS"),
            col("C_PHONE"),
            col("C_COMMENT"),
        )
        .limit(20)
    )

    daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 10, tmp_path)


def run_tpch_checker(q_num: int, result_file: str) -> bool:
    script = "./cmpq.pl"
    answer = f"../answers/q{q_num}.out"

    output = subprocess.check_output(
        shlex.split(f"{script} {q_num} {result_file} {answer}"), cwd="data/tpch-sqlite/tpch-dbgen/check_answers"
    )
    return output.decode() == f"Query {q_num} 0 unacceptable missmatches\n"

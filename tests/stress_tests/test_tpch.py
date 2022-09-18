import datetime
import math
import os
import pathlib
import shlex
import sqlite3
import subprocess

import pandas as pd
import pytest
from fsspec.implementations.local import LocalFileSystem
from loguru import logger
from sentry_sdk import start_transaction

from daft.config import get_daft_settings
from daft.dataframe import DataFrame
from daft.expressions import col
from tests.conftest import assert_df_equals

# If running in github, we use smaller-scale data
TPCH_DBGEN_DIR = "data/tpch-dbgen"
TPCH_SQLITE_TABLE_CREATION_SQL = [
    """
        CREATE TABLE NATION (
        N_NATIONKEY INTEGER PRIMARY KEY NOT NULL,
        N_NAME      TEXT NOT NULL,
        N_REGIONKEY INTEGER NOT NULL,
        N_COMMENT   TEXT,
        FOREIGN KEY (N_REGIONKEY) REFERENCES REGION(R_REGIONKEY)
        );
    """,
    """
        CREATE TABLE REGION (
        R_REGIONKEY INTEGER PRIMARY KEY NOT NULL,
        R_NAME      TEXT NOT NULL,
        R_COMMENT   TEXT
        );
    """,
    """
        CREATE TABLE PART (
        P_PARTKEY     INTEGER PRIMARY KEY NOT NULL,
        P_NAME        TEXT NOT NULL,
        P_MFGR        TEXT NOT NULL,
        P_BRAND       TEXT NOT NULL,
        P_TYPE        TEXT NOT NULL,
        P_SIZE        INTEGER NOT NULL,
        P_CONTAINER   TEXT NOT NULL,
        P_RETAILPRICE INTEGER NOT NULL,
        P_COMMENT     TEXT NOT NULL
        );
    """,
    """
        CREATE TABLE SUPPLIER (
        S_SUPPKEY   INTEGER PRIMARY KEY NOT NULL,
        S_NAME      TEXT NOT NULL,
        S_ADDRESS   TEXT NOT NULL,
        S_NATIONKEY INTEGER NOT NULL,
        S_PHONE     TEXT NOT NULL,
        S_ACCTBAL   INTEGER NOT NULL,
        S_COMMENT   TEXT NOT NULL,
        FOREIGN KEY (S_NATIONKEY) REFERENCES NATION(N_NATIONKEY)
        );
    """,
    """
        CREATE TABLE PARTSUPP (
        PS_PARTKEY    INTEGER NOT NULL,
        PS_SUPPKEY    INTEGER NOT NULL,
        PS_AVAILQTY   INTEGER NOT NULL,
        PS_SUPPLYCOST INTEGER NOT NULL,
        PS_COMMENT    TEXT NOT NULL,
        PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY),
        FOREIGN KEY (PS_SUPPKEY) REFERENCES SUPPLIER(S_SUPPKEY),
        FOREIGN KEY (PS_PARTKEY) REFERENCES PART(P_PARTKEY)
        );
    """,
    """
        CREATE TABLE CUSTOMER (
        C_CUSTKEY    INTEGER PRIMARY KEY NOT NULL,
        C_NAME       TEXT NOT NULL,
        C_ADDRESS    TEXT NOT NULL,
        C_NATIONKEY  INTEGER NOT NULL,
        C_PHONE      TEXT NOT NULL,
        C_ACCTBAL    INTEGER   NOT NULL,
        C_MKTSEGMENT TEXT NOT NULL,
        C_COMMENT    TEXT NOT NULL,
        FOREIGN KEY (C_NATIONKEY) REFERENCES NATION(N_NATIONKEY)
        );
    """,
    """
        CREATE TABLE ORDERS (
        O_ORDERKEY      INTEGER PRIMARY KEY NOT NULL,
        O_CUSTKEY       INTEGER NOT NULL,
        O_ORDERSTATUS   TEXT NOT NULL,
        O_TOTALPRICE    INTEGER NOT NULL,
        O_ORDERDATE     DATE NOT NULL,
        O_ORDERPRIORITY TEXT NOT NULL,
        O_CLERK         TEXT NOT NULL,
        O_SHIPPRIORITY  INTEGER NOT NULL,
        O_COMMENT       TEXT NOT NULL,
        FOREIGN KEY (O_CUSTKEY) REFERENCES CUSTOMER(C_CUSTKEY)
        );
    """,
    """
        CREATE TABLE LINEITEM (
        L_ORDERKEY      INTEGER NOT NULL,
        L_PARTKEY       INTEGER NOT NULL,
        L_SUPPKEY       INTEGER NOT NULL,
        L_LINENUMBER    INTEGER NOT NULL,
        L_QUANTITY      INTEGER NOT NULL,
        L_EXTENDEDPRICE INTEGER NOT NULL,
        L_DISCOUNT      INTEGER NOT NULL,
        L_TAX           INTEGER NOT NULL,
        L_RETURNFLAG    TEXT NOT NULL,
        L_LINESTATUS    TEXT NOT NULL,
        L_SHIPDATE      DATE NOT NULL,
        L_COMMITDATE    DATE NOT NULL,
        L_RECEIPTDATE   DATE NOT NULL,
        L_SHIPINSTRUCT  TEXT NOT NULL,
        L_SHIPMODE      TEXT NOT NULL,
        L_COMMENT       TEXT NOT NULL,
        PRIMARY KEY (L_ORDERKEY, L_LINENUMBER),
        FOREIGN KEY (L_ORDERKEY) REFERENCES ORDERS(O_ORDERKEY),
        FOREIGN KEY (L_PARTKEY, L_SUPPKEY) REFERENCES PARTSUPP(PS_PARTKEY, PS_SUPPKEY)
        );
    """,
]

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
def gen_tpch(request):

    benchmark_mode = request.config.getoption("--tpch_benchmark")

    scale_factor = float(os.getenv("SCALE_FACTOR", "1"))
    scale_factor = 0.2 if os.getenv("CI") else scale_factor

    if benchmark_mode:
        scale_factor = 10.0

    SF_TPCH_DIR = os.path.join(TPCH_DBGEN_DIR, ("%.1f" % scale_factor).replace(".", "_"))

    SQLITE_DB_FILE_PATH = os.path.join(SF_TPCH_DIR, "TPC-H.db")

    def import_table(table, table_path):
        if not os.path.isfile(table_path):
            raise FileNotFoundError(f"Did not find {table_path}")
        subprocess.check_output(shlex.split(f"sed -e 's/|$//' -i.bak {table_path}"))
        subprocess.check_output(
            shlex.split(f"sqlite3 {SQLITE_DB_FILE_PATH} -cmd '.mode csv' '.separator |' '.import {table_path} {table}'")
        )

    if not os.path.exists(SF_TPCH_DIR):
        # If running in CI, use a scale factor of 0.2
        # Otherwise, check for SCALE_FACTOR env variable or default to 1
        logger.info("Cloning tpch dbgen repo")
        subprocess.check_output(shlex.split(f"git clone https://github.com/electrum/tpch-dbgen {SF_TPCH_DIR}"))
        subprocess.check_output("make", cwd=SF_TPCH_DIR)

        num_parts = math.ceil(scale_factor)
        logger.info(f"Generating {num_parts} for tpch")
        if num_parts == 1:
            subprocess.check_output(shlex.split(f"./dbgen -v -f -s {scale_factor}"), cwd=SF_TPCH_DIR)
        else:

            for part_idx in range(1, num_parts + 1):
                logger.info(f"Generating partition: {part_idx}")
                subprocess.check_output(
                    shlex.split(f"./dbgen -v -f -s {scale_factor} -S {part_idx} -C {num_parts}"), cwd=SF_TPCH_DIR
                )

        subprocess.check_output(shlex.split("chmod -R u+rwx ."), cwd=SF_TPCH_DIR)

    SQLITE_DB_FILE_PATH = os.path.join(SF_TPCH_DIR, "TPC-H.db")
    if not benchmark_mode and not os.path.exists(SQLITE_DB_FILE_PATH):
        con = sqlite3.connect(SQLITE_DB_FILE_PATH)
        cur = con.cursor()
        for creation_sql in TPCH_SQLITE_TABLE_CREATION_SQL:
            cur.execute(creation_sql)

        static_tables = ["nation", "region"]
        partitioned_tables = ["customer", "lineitem", "orders", "partsupp", "part", "supplier"]
        single_partition_tables = static_tables + partitioned_tables if num_parts == 1 else static_tables
        multi_partition_tables = [] if num_parts == 1 else partitioned_tables

        for table in single_partition_tables:
            import_table(table, f"{SF_TPCH_DIR}/{table}.tbl")

        for table in multi_partition_tables:
            for part_idx in range(1, num_parts + 1):
                import_table(table, f"{SF_TPCH_DIR}/{table}.tbl.{part_idx}")

        # Remove all backup files as they cause problems downstream when searching for files via wilcards
        local_fs = LocalFileSystem()
        backup_files = local_fs.expand_path(f"{SF_TPCH_DIR}/*.bak")
        for backup_file in backup_files:
            os.remove(backup_file)

    PARQUET_FILE_PATH = os.path.join(SF_TPCH_DIR, "parquet")

    if benchmark_mode and not os.path.exists(PARQUET_FILE_PATH):
        for tab_name in SCHEMA.keys():
            logger.info(f"Generating Parquet Files for {tab_name}")
            df = DataFrame.from_csv(
                os.path.join(SF_TPCH_DIR, f"{tab_name}.tbl*"),
                has_headers=False,
                column_names=SCHEMA[tab_name] + [""],
                delimiter="|",
            ).exclude("")
            df = df.write_parquet(os.path.join(PARQUET_FILE_PATH, tab_name))
            df.collect()


@pytest.fixture(scope="module")
def get_df(request):
    def _get_df(tbl_name: str):
        benchmark_mode = request.config.getoption("--tpch_benchmark")
        local_fs = LocalFileSystem()
        scale_factor = float(os.getenv("SCALE_FACTOR", "1"))
        scale_factor = 0.2 if os.getenv("CI") else scale_factor

        if benchmark_mode:
            scale_factor = 10.0
        SF_TPCH_DIR = os.path.join(TPCH_DBGEN_DIR, ("%.1f" % scale_factor).replace(".", "_"))

        if benchmark_mode:
            logger.debug(f"using parquet files for {tbl_name}")
            df = DataFrame.from_parquet(os.path.join(SF_TPCH_DIR, "parquet", tbl_name, "*.parquet"))
        else:
            logger.debug(f"using csv files for {tbl_name}")

            # Used chunked files if found
            nonchunked_filepath = f"{SF_TPCH_DIR}/{tbl_name}.tbl"
            chunked_filepath = nonchunked_filepath + ".*"
            try:
                local_fs.expand_path(chunked_filepath)
                fp = chunked_filepath
            except FileNotFoundError:
                fp = nonchunked_filepath

            df = DataFrame.from_csv(
                fp,
                has_headers=False,
                column_names=SCHEMA[tbl_name],
                delimiter="|",
            )
        return df

    return _get_df


@pytest.fixture(scope="module")
def check_answer(request):
    benchmark_mode = request.config.getoption("--tpch_benchmark")
    scale_factor = float(os.getenv("SCALE_FACTOR", "1"))
    scale_factor = 0.2 if os.getenv("CI") else scale_factor

    if benchmark_mode:
        scale_factor = 10.0
    SF_TPCH_DIR = os.path.join(TPCH_DBGEN_DIR, ("%.1f" % scale_factor).replace(".", "_"))
    SQLITE_DB_FILE_PATH = os.path.join(SF_TPCH_DIR, "TPC-H.db")

    def _check_answer(daft_pd_df: pd.DataFrame, tpch_question: int, tmp_path: str):
        if benchmark_mode:
            return
        query = pathlib.Path(f"tests/assets/tpch-sqlite-queries/{tpch_question}.sql").read_text()
        conn = sqlite3.connect(SQLITE_DB_FILE_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
        cursor = conn.cursor()
        res = cursor.execute(query)
        sqlite_results = res.fetchall()
        sqlite_pd_results = pd.DataFrame.from_records(sqlite_results, columns=daft_pd_df.columns)
        assert_df_equals(daft_pd_df, sqlite_pd_results, assert_ordering=True)

    return _check_answer


def test_tpch_q1(tmp_path, check_answer, get_df):
    lineitem = get_df("lineitem")
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
    with start_transaction(op="task", name=f"tpch_q1:runner={get_daft_settings().runner_config.name.upper()}"):
        daft_pd_df = daft_df.to_pandas()
    daft_pd_df = daft_pd_df.sort_values(by=["L_RETURNFLAG", "L_LINESTATUS"])  # WE don't have multicolumn sort
    check_answer(daft_pd_df, 1, tmp_path)


def test_tpch_q2(tmp_path, check_answer, get_df):

    region = get_df("region")
    nation = get_df("nation")
    supplier = get_df("supplier")
    partsupp = get_df("partsupp")
    part = get_df("part")

    europe = (
        region.where(col("R_NAME") == "EUROPE")
        .join(nation, left_on=col("R_REGIONKEY"), right_on=col("N_REGIONKEY"))
        .join(supplier, left_on=col("N_NATIONKEY"), right_on=col("S_NATIONKEY"))
        .join(partsupp, left_on=col("S_SUPPKEY"), right_on=col("PS_SUPPKEY"))
    )

    brass = part.where((col("P_SIZE") == 15) & col("P_TYPE").str.endswith("BRASS")).join(
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
    with start_transaction(op="task", name=f"tpch_q2:runner={get_daft_settings().runner_config.name.upper()}"):
        daft_pd_df = daft_df.to_pandas()
    daft_pd_df = daft_pd_df.sort_values(
        by=["S_ACCTBAL", "N_NAME", "S_NAME", "P_PARTKEY"], ascending=[False, True, True, True]
    )
    daft_pd_df = daft_pd_df.head(100)
    check_answer(daft_pd_df, 2, tmp_path)


def test_tpch_q3(tmp_path, check_answer, get_df):
    def decrease(x, y):
        return x * (1 - y)

    customer = get_df("customer").where(col("C_MKTSEGMENT") == "BUILDING")
    orders = get_df("orders").where(col("O_ORDERDATE") < datetime.date(1995, 3, 15))
    lineitem = get_df("lineitem").where(col("L_SHIPDATE") > datetime.date(1995, 3, 15))

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
    with start_transaction(op="task", name=f"tpch_q3:runner={get_daft_settings().runner_config.name.upper()}"):
        daft_pd_df = daft_df.to_pandas()
    daft_pd_df = daft_pd_df.sort_values(by=["revenue", "O_ORDERDATE"], ascending=[False, True])
    daft_pd_df = daft_pd_df.head(10)
    daft_pd_df = daft_pd_df[["O_ORDERKEY", "revenue", "O_ORDERDATE", "O_SHIPPRIORITY"]]
    check_answer(daft_pd_df, 3, tmp_path)


def test_tpch_q4(tmp_path, check_answer, get_df):
    orders = get_df("orders").where(
        (col("O_ORDERDATE") >= datetime.date(1993, 7, 1)) & (col("O_ORDERDATE") < datetime.date(1993, 10, 1))
    )

    lineitems = (
        get_df("lineitem").where(col("L_COMMITDATE") < col("L_RECEIPTDATE")).select(col("L_ORDERKEY")).distinct()
    )

    daft_df = (
        lineitems.join(orders, left_on=col("L_ORDERKEY"), right_on=col("O_ORDERKEY"))
        .groupby(col("O_ORDERPRIORITY"))
        .agg([(col("L_ORDERKEY").alias("order_count"), "count")])
        .sort(col("O_ORDERPRIORITY"))
    )

    with start_transaction(op="task", name=f"tpch_q4:runner={get_daft_settings().runner_config.name.upper()}"):
        daft_pd_df = daft_df.to_pandas()

    check_answer(daft_pd_df, 4, tmp_path)


def test_tpch_q5(tmp_path, check_answer, get_df):
    orders = get_df("orders").where(
        (col("O_ORDERDATE") >= datetime.date(1994, 1, 1)) & (col("O_ORDERDATE") < datetime.date(1995, 1, 1))
    )
    region = get_df("region").where(col("R_NAME") == "ASIA")
    nation = get_df("nation")
    supplier = get_df("supplier")
    lineitem = get_df("lineitem")
    customer = get_df("customer")

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

    with start_transaction(op="task", name=f"tpch_q5:runner={get_daft_settings().runner_config.name.upper()}"):
        daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 5, tmp_path)


def test_tpch_q6(tmp_path, check_answer, get_df):
    lineitem = get_df("lineitem")
    daft_df = lineitem.where(
        (col("L_SHIPDATE") >= datetime.date(1994, 1, 1))
        & (col("L_SHIPDATE") < datetime.date(1995, 1, 1))
        & (col("L_DISCOUNT") >= 0.05)
        & (col("L_DISCOUNT") <= 0.07)
        & (col("L_QUANTITY") < 24)
    ).sum(col("L_EXTENDEDPRICE") * col("L_DISCOUNT"))

    with start_transaction(op="task", name=f"tpch_q6:runner={get_daft_settings().runner_config.name.upper()}"):
        daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 6, tmp_path)


def test_tpch_q7(tmp_path, check_answer, get_df):
    def decrease(x, y):
        return x * (1 - y)

    lineitem = get_df("lineitem").where(
        (col("L_SHIPDATE") >= datetime.date(1995, 1, 1)) & (col("L_SHIPDATE") <= datetime.date(1996, 12, 31))
    )
    nation = get_df("nation").where((col("N_NAME") == "FRANCE") | (col("N_NAME") == "GERMANY"))
    supplier = get_df("supplier")
    customer = get_df("customer")
    orders = get_df("orders")

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
            col("L_SHIPDATE").dt.year().alias("l_year"),
            decrease(col("L_EXTENDEDPRICE"), col("L_DISCOUNT")).alias("volume"),
        )
        .groupby(col("supp_nation"), col("cust_nation"), col("l_year"))
        .agg([(col("volume").alias("revenue"), "sum")])
    )

    # Multicol sorts not implemented yet
    with start_transaction(op="task", name=f"tpch_q7:runner={get_daft_settings().runner_config.name.upper()}"):
        daft_pd_df = daft_df.to_pandas()
    daft_pd_df = daft_pd_df.sort_values(by=["supp_nation", "cust_nation", "l_year"])
    check_answer(daft_pd_df, 7, tmp_path)


def test_tpch_q8(tmp_path, check_answer, get_df):
    def decrease(x, y):
        return x * (1 - y)

    region = get_df("region").where(col("R_NAME") == "AMERICA")
    orders = get_df("orders").where(
        (col("O_ORDERDATE") <= datetime.date(1996, 12, 31)) & (col("O_ORDERDATE") >= datetime.date(1995, 1, 1))
    )
    part = get_df("part").where(col("P_TYPE") == "ECONOMY ANODIZED STEEL")
    nation = get_df("nation")
    supplier = get_df("supplier")
    lineitem = get_df("lineitem")
    customer = get_df("customer")

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
            col("O_ORDERDATE").dt.year().alias("o_year"),
            col("volume"),
            (col("N_NAME") == "BRAZIL").if_else(col("volume"), 0.0).alias("case_volume"),
        )
        .groupby(col("o_year"))
        .agg([(col("case_volume").alias("case_volume_sum"), "sum"), (col("volume").alias("volume_sum"), "sum")])
        .select(col("o_year"), col("case_volume_sum") / col("volume_sum"))
        .sort(col("o_year"))
    )

    with start_transaction(op="task", name=f"tpch_q8:runner={get_daft_settings().runner_config.name.upper()}"):
        daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 8, tmp_path)


def test_tpch_q9(tmp_path, check_answer, get_df):
    lineitem = get_df("lineitem")
    part = get_df("part")
    nation = get_df("nation")
    supplier = get_df("supplier")
    partsupp = get_df("partsupp")
    orders = get_df("orders")

    def expr(x, y, v, w):
        return x * (1 - y) - (v * w)

    linepart = part.where(col("P_NAME").str.contains("green")).join(
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
            col("O_ORDERDATE").dt.year().alias("o_year"),
            expr(col("L_EXTENDEDPRICE"), col("L_DISCOUNT"), col("PS_SUPPLYCOST"), col("L_QUANTITY")).alias("amount"),
        )
        .groupby(col("N_NAME"), col("o_year"))
        .agg([(col("amount"), "sum")])
    )

    with start_transaction(op="task", name=f"tpch_q9:runner={get_daft_settings().runner_config.name.upper()}"):
        daft_pd_df = daft_df.to_pandas()
    daft_pd_df = daft_pd_df.sort_values(by=["N_NAME", "o_year"], ascending=[True, False])
    check_answer(daft_pd_df, 9, tmp_path)


def test_tpch_q10(tmp_path, check_answer, get_df):
    def decrease(x, y):
        return x * (1 - y)

    lineitem = get_df("lineitem").where(col("L_RETURNFLAG") == "R")
    orders = get_df("orders")
    nation = get_df("nation")
    customer = get_df("customer")

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

    with start_transaction(op="task", name=f"tpch_q10:runner={get_daft_settings().runner_config.name.upper()}"):
        daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 10, tmp_path)


def run_tpch_checker(q_num: int, result_file: str) -> bool:
    script = "./cmpq.pl"
    answer = f"../answers/q{q_num}.out"

    output = subprocess.check_output(
        shlex.split(f"{script} {q_num} {result_file} {answer}"), cwd="data/tpch-sqlite/tpch-dbgen/check_answers"
    )
    return output.decode() == f"Query {q_num} 0 unacceptable missmatches\n"

"""Get a Daft DataFrame corresponding to a TPC-H question.

You may also run this file directly as such: `python answers_sql.py <path to TPC-H parquet data dir> <question number>`
"""

from __future__ import annotations

import os
import sys

import daft
from daft import col
from daft.io import IOConfig, S3Config
from daft.sql import SQLCatalog

TABLE_NAMES = [
    "part",
    "supplier",
    "partsupp",
    "customer",
    "orders",
    "lineitem",
    "nation",
    "region",
]


def lowercase_column_names(df):
    return df.select(*[col(name).alias(name.lower()) for name in df.column_names])


def q21(get_df) -> daft.DataFrame:
    supplier = get_df("supplier")
    nation = get_df("nation")
    lineitem = get_df("lineitem")
    orders = get_df("orders")

    res_1 = (
        lineitem.select("L_SUPPKEY", "L_ORDERKEY")
        # distinct not needed? either way we should stop using dataframe answers soon
        # .distinct()
        .groupby("L_ORDERKEY")
        .agg(col("L_SUPPKEY").count().alias("nunique_col"))
        .where(col("nunique_col") > 1)
        .join(lineitem.where(col("L_RECEIPTDATE") > col("L_COMMITDATE")), on="L_ORDERKEY")
    )

    daft_df = (
        res_1.select("L_SUPPKEY", "L_ORDERKEY")
        .groupby("L_ORDERKEY")
        .agg(col("L_SUPPKEY").count().alias("nunique_col"))
        .join(res_1, on="L_ORDERKEY")
        .join(supplier, left_on="L_SUPPKEY", right_on="S_SUPPKEY")
        .join(nation, left_on="S_NATIONKEY", right_on="N_NATIONKEY")
        .join(orders, left_on="L_ORDERKEY", right_on="O_ORDERKEY")
        .where((col("nunique_col") == 1) & (col("N_NAME") == "SAUDI ARABIA") & (col("O_ORDERSTATUS") == "F"))
        .groupby("S_NAME")
        .agg(col("O_ORDERKEY").count().alias("numwait"))
        .sort(["numwait", "S_NAME"], desc=[True, False])
        .limit(100)
    )

    return daft_df


def get_answer(q: int, get_df) -> daft.DataFrame:
    assert 1 <= q <= 22, f"TPC-H has 22 questions, received q={q}"

    if q == 21:
        # TODO: remove this once we support q21
        return q21(get_df)
    else:
        catalog = SQLCatalog({tbl: lowercase_column_names(get_df(tbl)) for tbl in TABLE_NAMES})

        module_dir = os.path.dirname(os.path.abspath(__file__))
        query_file_path = os.path.join(module_dir, f"queries/{q:02}.sql")

        with open(query_file_path) as query_file:
            query = query_file.read()
        return daft.sql(query, catalog=catalog)


def main(parquet_path, q):
    s3_config_from_env = S3Config.from_env()
    io_config = IOConfig(s3=s3_config_from_env)

    def get_df(name):
        return daft.read_parquet(f"{parquet_path}{name}/*", io_config=io_config)

    daft_df = get_answer(q, get_df)

    daft_df.collect()


if __name__ == "__main__":
    parquet_path = sys.argv[1]
    q = int(sys.argv[2])
    main(parquet_path, q)

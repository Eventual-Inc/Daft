"""Get a Daft DataFrame corresponding to a TPC-H question.

You may also run this file directly as such: `python answers_sql.py <path to TPC-H parquet data dir> <question number>`
"""

import os
import sys

import daft
from daft.sql import SQLCatalog

from . import answers

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
    return df.select(*[daft.col(name).alias(name.lower()) for name in df.column_names])


def get_answer(q: int, get_df) -> daft.DataFrame:
    assert 1 <= q <= 22, f"TPC-H has 22 questions, received q={q}"

    if q == 21:
        # TODO: remove this once we support q21
        return answers.q21(get_df)
    else:
        catalog = SQLCatalog({tbl: lowercase_column_names(get_df(tbl)) for tbl in TABLE_NAMES})

        module_dir = os.path.dirname(os.path.abspath(__file__))
        query_file_path = os.path.join(module_dir, f"queries/{q:02}.sql")

        with open(query_file_path) as query_file:
            query = query_file.read()
        return daft.sql(query, catalog=catalog)


def main(parquet_path, q):
    def get_df(name):
        return daft.read_parquet(f"{parquet_path}{name}/*")

    daft_df = get_answer(q, get_df)

    daft_df.collect()


if __name__ == "__main__":
    parquet_path = sys.argv[1]
    q = int(sys.argv[2])
    main(parquet_path, q)

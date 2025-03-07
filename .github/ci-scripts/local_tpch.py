"""Run TPC-H benchmarks with native runner on local Parquet data and upload results to Google sheets.

Expects tables as Parquet files in "/tmp/tpch-data/"
"""

import os
import time
from datetime import datetime, timezone

import gspread

import daft
import daft.context
from benchmarking.tpch import answers
from daft.sql import SQLCatalog


def get_df(name):
    return daft.read_parquet(f"/tmp/tpch-data/{name}/*")


def run_benchmark():
    table_names = [
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

    catalog = SQLCatalog({tbl: lowercase_column_names(get_df(tbl)) for tbl in table_names})

    results = {}

    for q in range(1, 23):
        print(f"Running TPC-H Q{q}... ", end="", flush=True)
        if q == 21:
            # TODO: remove this once we support q21
            daft_df = answers.q21(get_df)
        else:
            with open(f"benchmarking/tpch/queries/{q:02}.sql") as query_file:
                query = query_file.read()
            daft_df = daft.sql(query, catalog=catalog)

        start = time.perf_counter()
        daft_df.collect()
        end = time.perf_counter()

        results[q] = end - start

        print(f"done in {results[q]:.2f}s")

    return results


def get_run_metadata():
    return {
        "started at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"),
        "daft version": daft.__version__,
        "github ref": os.getenv("GITHUB_REF"),
        "github sha": os.getenv("GITHUB_SHA"),
    }


def upload_to_google_sheets(data):
    gc = gspread.service_account()

    sh = gc.open_by_url(
        "https://docs.google.com/spreadsheets/d/1d6pXsIsBkjjM93GYtoiF83WXvJXR4vFgFQdmG05u8eE/edit?gid=0#gid=0"
    )
    ws = sh.worksheet("Local TPC-H")
    ws.append_row(data)


def main():
    daft.context.set_runner_native()

    metadata = get_run_metadata()

    results = run_benchmark()

    data_dict = {**metadata, **results}

    print("Results:")
    print(data_dict)

    upload_to_google_sheets(list(data_dict.values()))


if __name__ == "__main__":
    main()

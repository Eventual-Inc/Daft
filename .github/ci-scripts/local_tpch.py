"""Run TPC-H benchmarks with native runner on local Parquet data and upload results to Google sheets.

Expects tables as Parquet files in "/tmp/tpch-data/"
"""

from __future__ import annotations

import time

import daft
import daft.context
from benchmarking.tpch.answers_sql import get_answer
from tools.ci_bench_utils import get_run_metadata, upload_to_google_sheets


def get_df(name):
    return daft.read_parquet(f"/tmp/tpch-data/{name}/*")


def run_benchmark():
    results = {}

    for q in range(1, 23):
        print(f"Running TPC-H Q{q}... ", end="", flush=True)

        daft_df = get_answer(q, get_df)

        start = time.perf_counter()
        daft_df.collect()
        end = time.perf_counter()

        results[q] = end - start

        print(f"done in {results[q]:.2f}s")

    return results


def main():
    daft.context.set_runner_native()

    metadata = get_run_metadata()

    results = run_benchmark()

    data_dict = {**metadata, **results}

    print("Results:")
    print(data_dict)

    upload_to_google_sheets("Local TPC-H", list(data_dict.values()))


if __name__ == "__main__":
    main()

import argparse
import contextlib
import math
import os
import time
from typing import Callable

from loguru import logger
from sentry_sdk import start_transaction

from benchmarking.tpch import answers, data_generation
from daft import DataFrame
from daft.context import get_context


@contextlib.contextmanager
def timer(q: int):
    logger.info(f"Running benchmarks for TPC-H q{q}")
    start = time.time()
    yield
    logger.info(f"Finished benchmarks for q{q}: {time.time() - start}s")


def get_df_with_parquet_folder(parquet_folder: str) -> Callable[[str], DataFrame]:
    def _get_df(table_name: str) -> DataFrame:
        return DataFrame.from_parquet(os.path.join(parquet_folder, table_name, "*.parquet"))

    return _get_df


def run_all_benchmarks(parquet_folder):
    get_df = get_df_with_parquet_folder(parquet_folder)

    for i in range(1, 11):
        answer = getattr(answers, f"q{i}")
        daft_df = answer(get_df)
        with start_transaction(op="task", name=f"tpch_q{i}:runner={get_context().runner_config.name.upper()}"), timer(
            i
        ):
            daft_df.to_pandas()


def generate_parquet_data(tpch_gen_folder: str, scale_factor: float) -> str:
    """Generates Parquet data and returns the path to the folder

    Args:
        tpch_gen_folder (str): Path to the folder containing the TPCH dbgen tool and generated data
        scale_factor (float): Scale factor to run on in GB

    Returns:
        str: Path to folder containing Parquet files
    """
    num_parts = math.ceil(scale_factor)
    csv_folder = data_generation.gen_csv_files(basedir=tpch_gen_folder, scale_factor=scale_factor, num_parts=num_parts)
    return data_generation.gen_parquet(csv_folder)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tpch_gen_folder",
        default="data/tpch-dbgen",
        help="Path to the folder containing the TPCH dbgen tool and generated data",
    )
    parser.add_argument("--scale_factor", default=10.0, help="Scale factor to run on in GB", type=float)
    args = parser.parse_args()
    parquet_folder = generate_parquet_data(args.tpch_gen_folder, args.scale_factor)
    run_all_benchmarks(parquet_folder)

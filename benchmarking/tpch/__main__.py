import argparse
import contextlib
import csv
import math
import os
import platform
import socket
import subprocess
import time
from datetime import datetime, timezone
from typing import Callable, Optional, Set

from loguru import logger

from benchmarking.tpch import answers, data_generation
from daft import DataFrame
from daft.context import get_context


class MetricsBuilder:

    HEADERS = [
        "finished_at",
        "benchmark_name",
        "runner",
        "walltime[s]",
        "commit_hash",
        "commit_time",
        "release_tag",
        "env",
        "success",
        "python_version",
        "github_runner_os",
        "github_runner_arch",
        "github_workflow",
        "github_run_id",
        "github_run_attempt",
    ]

    def __init__(self, runner: str):
        self._metrics = {header: [] for header in MetricsBuilder.HEADERS}
        self._runner = runner
        self._env = "github_actions" if os.getenv("GITHUB_ACTIONS") else socket.gethostname()

        self._github_metadata = {
            "github_runner_os": os.getenv("RUNNER_OS"),
            "github_runner_arch": os.getenv("RUNNER_ARCH"),
            "github_workflow": os.getenv("GITHUB_WORKFLOW"),
            "github_run_id": os.getenv("GITHUB_RUN_ID"),
            "github_run_attempt": os.getenv("GITHUB_RUN_ATTEMPT"),
        }

        self._commit_hash = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode("utf-8").strip()
        self._commit_time = (
            subprocess.check_output(["git", "show", "-s", "--format=%cI", "HEAD"]).decode("utf-8").strip()
        )
        self._release_tag = subprocess.check_output(["git", "tag", "--points-at", "HEAD"]).decode("utf-8").strip()

    def _add_metric(self, benchmark_name: str, walltime_s: float, success: bool):
        data = {
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "benchmark_name": benchmark_name,
            "runner": self._runner,
            "walltime[s]": walltime_s,
            "commit_hash": self._commit_hash,
            "commit_time": self._commit_time,
            "release_tag": self._release_tag,
            "env": self._env,
            "success": success,
            "python_version": ".".join(platform.python_version_tuple()),
            **self._github_metadata,
        }
        for header in data:
            self._metrics[header].append(data[header])

    @contextlib.contextmanager
    def collect_metrics(self, qnum: int):
        logger.info(f"Running benchmarks for TPC-H q{qnum}")
        success = True
        start = time.time()
        yield
        walltime_s = time.time() - start
        logger.info(f"Finished benchmarks for q{qnum}: {walltime_s}s")

        self._add_metric(
            f"tpch_q{qnum}",
            walltime_s,
            success,
        )

    def dump_csv(self, csv_output_location: str, output_csv_headers: bool):
        if len(self._metrics) == 0:
            logger.warning("No metrics to upload!")

        with open(csv_output_location, "w", newline="") as csvfile:
            writer = csv.writer(csvfile, delimiter=",")
            if output_csv_headers:
                writer.writerow(MetricsBuilder.HEADERS)
            for i in range(len(self._metrics[MetricsBuilder.HEADERS[0]])):
                writer.writerow([self._metrics[header][i] for header in MetricsBuilder.HEADERS])


def get_df_with_parquet_folder(parquet_folder: str) -> Callable[[str], DataFrame]:
    def _get_df(table_name: str) -> DataFrame:
        return DataFrame.from_parquet(os.path.join(parquet_folder, table_name, "*.parquet"))

    return _get_df


def run_all_benchmarks(
    parquet_folder: str, skip_questions: Set[int], csv_output_location: Optional[str], output_csv_headers: bool
):
    get_df = get_df_with_parquet_folder(parquet_folder)

    daft_context = get_context()
    metrics_builder = MetricsBuilder(daft_context.runner_config.name)

    for i in range(1, 11):

        if i in skip_questions:
            logger.warning(f"Skipping TPC-H q{i}")
            continue

        answer = getattr(answers, f"q{i}")
        daft_df = answer(get_df)

        with metrics_builder.collect_metrics(i):
            daft_df.collect()

    if csv_output_location:
        logger.info(f"Writing CSV to: {csv_output_location}")
        metrics_builder.dump_csv(csv_output_location, output_csv_headers)
    else:
        logger.info(f"No CSV location specified, skipping CSV write")


def generate_parquet_data(tpch_gen_folder: str, scale_factor: float, num_parts: int) -> str:
    """Generates Parquet data and returns the path to the folder

    Args:
        tpch_gen_folder (str): Path to the folder containing the TPCH dbgen tool and generated data
        scale_factor (float): Scale factor to run on in GB

    Returns:
        str: Path to folder containing Parquet files
    """
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
    parser.add_argument(
        "--num_parts", default=None, help="Number of parts to generate (defaults to 1 part per GB)", type=int
    )
    parser.add_argument("--skip_questions", action="append", default=[], help="Questions to skip", type=int)
    parser.add_argument("--output_csv", default=None, type=str, help="Location to output CSV file")
    parser.add_argument("--output_csv_headers", action="store_true", help="Whether to output headers for the CSV file")
    args = parser.parse_args()
    num_parts = math.ceil(args.scale_factor) if args.num_parts is None else args.num_parts

    # Generate Parquet data, or skip if data is cached on disk
    parquet_folder = generate_parquet_data(args.tpch_gen_folder, args.scale_factor, num_parts)

    run_all_benchmarks(
        parquet_folder,
        skip_questions=set(args.skip_questions),
        csv_output_location=args.output_csv,
        output_csv_headers=args.output_csv_headers,
    )

from __future__ import annotations

import argparse
import contextlib
import csv
import logging
import math
import os
import pathlib
import platform
import socket
import subprocess
import warnings
from datetime import datetime, timezone
from typing import Any, Callable

import ray

import daft
from benchmarking.tpch import answers, data_generation
from daft import DataFrame
from daft.context import get_context
from daft.runners.profiler import profiler

logger = logging.getLogger(__name__)

ALL_TABLES = [
    "part",
    "supplier",
    "partsupp",
    "customer",
    "orders",
    "lineitem",
    "nation",
    "region",
]


class MetricsBuilder:
    NUM_TPCH_QUESTIONS = 22

    HEADERS = [
        "started_at",
        "runner",
        "commit_hash",
        "commit_time",
        "release_tag",
        "env",
        "python_version",
        "github_runner_os",
        "github_runner_arch",
        "github_workflow",
        "github_run_id",
        "github_run_attempt",
        "github_ref",
        *[f"tpch_q{i}" for i in range(1, NUM_TPCH_QUESTIONS + 1)],
        "worker_count",
        "worker_instance_name",
    ]

    def __init__(self, runner: str):
        self._runner = runner
        self._env = "github_actions" if os.getenv("GITHUB_ACTIONS") else socket.gethostname()
        self._commit_hash = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode("utf-8").strip()
        self._commit_time = (
            subprocess.check_output(["git", "show", "-s", "--format=%cI", "HEAD"]).decode("utf-8").strip()
        )
        self._release_tag = subprocess.check_output(["git", "tag", "--points-at", "HEAD"]).decode("utf-8").strip()

        self._metrics: dict[str, Any] = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "runner": runner,
            "commit_hash": self._commit_hash,
            "commit_time": self._commit_time,
            "release_tag": self._release_tag,
            "env": self._env,
            "python_version": ".".join(platform.python_version_tuple()),
            "github_runner_os": os.getenv("RUNNER_OS"),
            "github_runner_arch": os.getenv("RUNNER_ARCH"),
            "github_workflow": os.getenv("GITHUB_WORKFLOW"),
            "github_run_id": os.getenv("GITHUB_RUN_ID"),
            "github_run_attempt": os.getenv("GITHUB_RUN_ATTEMPT"),
            "github_ref": os.getenv("GITHUB_REF"),
            "worker_count": os.getenv("WORKER_COUNT"),
            "worker_instance_name": os.getenv("WORKER_INSTANCE_NAME"),
        }

    @contextlib.contextmanager
    def collect_metrics(self, qnum: int):
        logger.info("Running benchmarks for TPC-H q %s", qnum)
        start = datetime.now()
        profile_filename = (
            f"tpch_q{qnum}_{self._runner}_{datetime.replace(start, microsecond=0).isoformat()}_viztracer.json"
        )
        with profiler(profile_filename):
            yield
        walltime_s = (datetime.now() - start).total_seconds()
        logger.info("Finished benchmarks for q%s: %ss", qnum, walltime_s)
        self._metrics[f"tpch_q{qnum}"] = walltime_s

        if str(os.getenv("RAY_PROFILING")) == str(1) and self._runner == "ray":
            profile_filename = (
                f"tpch_q{qnum}_{self._runner}_{datetime.replace(start, microsecond=0).isoformat()}_raytimeline.json"
            )
            ray.timeline(profile_filename)

    def dump_csv(self, csv_output_location: str):
        if len(self._metrics) == 0:
            logger.warning("No metrics to upload!")

        with open(csv_output_location, "w", newline="") as csvfile:
            writer = csv.writer(csvfile, delimiter=",")
            writer.writerow(MetricsBuilder.HEADERS)
            writer.writerow([self._metrics.get(header, "") for header in MetricsBuilder.HEADERS])


def get_df_with_parquet_folder(parquet_folder: str) -> Callable[[str], DataFrame]:
    def _get_df(table_name: str) -> DataFrame:
        return daft.read_parquet(os.path.join(parquet_folder, table_name, "*.parquet"))

    return _get_df


def run_all_benchmarks(
    parquet_folder: str,
    skip_questions: set[int],
    csv_output_location: str | None,
    ray_job_dashboard_url: str | None = None,
    requirements: str | None = None,
):
    get_df = get_df_with_parquet_folder(parquet_folder)

    daft_context = get_context()
    metrics_builder = MetricsBuilder(daft_context.runner_config.name)

    for i in range(1, 23):
        if i in skip_questions:
            logger.warning("Skipping TPC-H q%s", i)
            continue

        # Run as a Ray Job if dashboard URL is provided
        if ray_job_dashboard_url is not None:
            from benchmarking.tpch import ray_job_runner

            working_dir = pathlib.Path(os.path.dirname(__file__))
            entrypoint = working_dir / "ray_job_runner.py"
            job_params = ray_job_runner.ray_job_params(
                parquet_folder_path=parquet_folder,
                tpch_qnum=i,
                working_dir=working_dir,
                entrypoint=entrypoint,
                runtime_env=get_ray_runtime_env(requirements),
            )

            # Run once as a warmup step
            ray_job_runner.run_on_ray(
                ray_job_dashboard_url,
                {**job_params, "submission_id": job_params["submission_id"] + "-warmup"},
            )

            # Run second time to collect metrics
            with metrics_builder.collect_metrics(i):
                ray_job_runner.run_on_ray(
                    ray_job_dashboard_url,
                    job_params,
                )

        # Run locally (potentially on a local Ray cluster)
        else:
            answer = getattr(answers, f"q{i}")
            daft_df = answer(get_df)

            with metrics_builder.collect_metrics(i):
                daft_df.collect()

    if csv_output_location:
        logger.info("Writing CSV to: %s", csv_output_location)
        metrics_builder.dump_csv(csv_output_location)
    else:
        logger.info("No CSV location specified, skipping CSV write")


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


def get_daft_version() -> str:
    return daft.get_version()


def get_ray_runtime_env(requirements: str | None) -> dict:
    runtime_env = {
        "py_modules": [daft],
        "eager_install": True,
        "env_vars": {"DAFT_PROGRESS_BAR": "0"},
    }
    if requirements:
        runtime_env.update({"pip": requirements})
    return runtime_env


def warmup_environment(requirements: str | None, parquet_folder: str):
    """Performs necessary setup of Daft on the current benchmarking environment"""
    ctx = daft.context.get_context()

    if ctx.runner_config.name == "ray":
        runtime_env = get_ray_runtime_env(requirements)

        ray.init(
            address=ctx.runner_config.address,
            runtime_env=runtime_env,
        )

        logger.info("Warming up Ray cluster with a function...")

        # NOTE: installation of runtime_env is supposed to be eager but it seems to be happening async.
        # Here we farm out some work on Ray to warm up all the workers by downloading data.
        # Warm up n := num_cpus workers.
        @ray.remote(num_cpus=1, scheduling_strategy="SPREAD")
        def warm_up_function():
            import time

            time.sleep(1)
            return get_daft_version()

        num_workers_to_warm = int(ray.cluster_resources()["CPU"])
        tasks = [warm_up_function.remote() for _ in range(num_workers_to_warm)]
        assert ray.get(tasks) == [get_daft_version() for _ in range(num_workers_to_warm)]
        del tasks

        logger.info("Ray cluster warmed up")

    get_df = get_df_with_parquet_folder(parquet_folder)
    for table in ALL_TABLES:
        df = get_df(table)
        logger.info(
            "Warming up local execution environment by loading table %s and counting rows: %s",
            table,
            df.count(df.columns[0]).to_pandas(),
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tpch_gen_folder",
        default="data/tpch-dbgen",
        help="Path to the folder containing the TPCH dbgen tool and generated data",
    )
    parser.add_argument(
        "--parquet_file_cache",
        default=None,
        help="Path to root folder (local or in S3) containing cached Parquet files",
    )
    parser.add_argument("--scale_factor", default=10.0, help="Scale factor to run on in GB", type=float)
    parser.add_argument(
        "--num_parts", default=None, help="Number of parts to generate (defaults to 1 part per GB)", type=int
    )
    parser.add_argument("--skip_questions", type=str, default=None, help="Comma-separated list of questions to skip")
    parser.add_argument("--output_csv", default=None, type=str, help="Location to output CSV file")
    parser.add_argument(
        "--output_csv_headers",
        action="store_true",
        help="DEPRECATED: We always output CSV headers regardless of this flag",
    )
    parser.add_argument(
        "--requirements",
        default=None,
        help="Path to pip-style requirements.txt file to bootstrap environment on remote Ray cluster",
    )
    parser.add_argument(
        "--skip_warmup",
        action="store_true",
        help="Skip warming up data before benchmark",
    )
    parser.add_argument(
        "--ray_job_dashboard_url",
        default=None,
        help="Ray Dashboard URL to submit jobs instead of using Ray client, most useful when running on a remote cluster",
    )

    args = parser.parse_args()
    if args.output_csv_headers:
        warnings.warn("Detected --output_csv_headers flag, but this flag is deprecated - CSVs always output headers")

    num_parts = math.ceil(args.scale_factor) if args.num_parts is None else args.num_parts

    # Generate Parquet data, or skip if data is cached on disk
    parquet_folder: str
    if args.parquet_file_cache is not None:
        parquet_folder = (
            os.path.join(args.parquet_file_cache, str(args.scale_factor).replace(".", "_"), str(num_parts), "parquet")
            + "/"
        )
    else:
        parquet_folder = generate_parquet_data(args.tpch_gen_folder, args.scale_factor, num_parts)

    if args.skip_warmup:
        warnings.warn("Detected --skip_warmup flag, skipping warm up task")
    else:
        warmup_environment(args.requirements, parquet_folder)

    run_all_benchmarks(
        parquet_folder,
        skip_questions={int(s) for s in args.skip_questions.split(",")} if args.skip_questions is not None else set(),
        csv_output_location=args.output_csv,
        ray_job_dashboard_url=args.ray_job_dashboard_url,
        requirements=args.requirements,
    )

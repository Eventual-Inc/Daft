import argparse
import logging
import typing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import ray

import daft

from ..tpch import __main__ as tpch
from ..tpch import ray_job_runner
from . import datagen, helpers

logger = logging.getLogger(__name__)

SQL_QUERIES_PATH = Path("benchmarking") / "tpcds" / "queries"

Results = tuple[dict[int, float], dict[int, str]]


@dataclass
class ParsedArgs:
    tpcds_gen_folder: Path
    scale_factor: float
    questions: str
    ray_address: Optional[str]


@dataclass
class RunArgs:
    scaled_tpcds_gen_folder: Path
    query_indices: list[int]
    ray_address: Optional[str]


def run_query_on_ray(
    run_args: RunArgs,
) -> Results:
    successes = {}
    failures = {}
    ray.init()

    for query_index in run_args.query_indices:
        working_dir = Path("benchmarking") / "tpcds"
        ray_entrypoint_script = "ray_entrypoint.py"
        try:
            start = datetime.now()
            ray_job_runner.run_on_ray(
                run_args.ray_address,
                {
                    "entrypoint": f"python {ray_entrypoint_script} --tpcds-gen-folder 'data/0.01' --question {query_index}",
                    "runtime_env": {
                        "working_dir": working_dir,
                    },
                },
            )
            end = datetime.now()
            duration = end - start
            successes[query_index] = duration
        except RuntimeError as rte:
            failures[query_index] = str(rte)

    return successes, failures


def run_query_on_local(
    run_args: RunArgs,
) -> Results:
    successes = {}
    failures = {}
    catalog = helpers.generate_catalog(run_args.scaled_tpcds_gen_folder)

    for query_index in run_args.query_indices:
        query_file = SQL_QUERIES_PATH / f"{query_index:02}.sql"
        with open(query_file) as f:
            query = f.read()

        start = datetime.now()

        try:
            daft.sql(query, catalog=catalog).collect()
            end = datetime.now()
            duration = end - start
            successes[query_index] = duration
        except Exception as e:
            failures[query_index] = str(e)

    return successes, failures


def run_benchmarks(
    run_args: RunArgs,
) -> Results:
    logger.info(
        "Running the following questions: %s",
        run_args.query_indices,
    )

    runner = tpch.get_daft_benchmark_runner_name()

    logger.info(
        "Running on the following runner: %s",
        runner,
    )

    if runner == "ray":
        return run_query_on_ray(run_args)
    elif runner == "py" or runner == "native":
        return run_query_on_local(run_args)
    else:
        typing.assert_never(runner)


def main(args: ParsedArgs):
    scaled_tpcds_gen_folder = args.tpcds_gen_folder / str(args.scale_factor)
    datagen.gen_tpcds(scaled_tpcds_gen_folder, args.scale_factor)
    query_indices = helpers.parse_questions_str(args.questions)
    _successes, failures = run_benchmarks(
        RunArgs(
            scaled_tpcds_gen_folder=scaled_tpcds_gen_folder,
            query_indices=query_indices,
            ray_address=args.ray_address,
        )
    )
    print(failures)


if __name__ == "__main__":
    logging.basicConfig(level="INFO")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tpcds-gen-folder",
        default="benchmarking/tpcds/data",
        type=Path,
        help="Path to the folder containing the TPC-DS dsdgen tool and generated data",
    )
    parser.add_argument("--scale-factor", default=0.01, type=float, help="Scale factor to run on in GB")
    parser.add_argument("--questions", default="*", type=str, help="The questions to run")
    parser.add_argument("--ray-address", type=str, help="The address of the head node of the ray cluster")
    args = parser.parse_args()

    tpcds_gen_folder: Path = args.tpcds_gen_folder
    assert args.scale_factor > 0

    main(
        ParsedArgs(
            tpcds_gen_folder=tpcds_gen_folder,
            scale_factor=args.scale_factor,
            questions=args.questions,
            ray_address=args.ray_address,
        )
    )

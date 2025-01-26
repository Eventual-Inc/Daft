import argparse
import logging
import typing
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import ray

import daft

from ..tpch import __main__ as tpch
from ..tpch import ray_job_runner
from . import datagen, helpers

logger = logging.getLogger(__name__)

SQL_QUERIES_PATH = Path(__file__).parent / "queries"


@dataclass
class ParsedArgs:
    tpcds_gen_folder: Path
    scale_factor: float
    questions: str
    ray_address: Optional[str]
    dry_run: bool


@dataclass
class RunArgs:
    scaled_tpcds_gen_folder: Path
    query_indices: list[int]
    ray_address: Optional[str]
    dry_run: bool


@dataclass
class Result:
    index: int
    duration: Optional[timedelta]
    error_msg: Optional[str]

    def __repr__(self) -> str:
        if self.duration and self.error_msg:
            typing.assert_never("Both duration and error_msg are not None")
        elif self.duration:
            return f"(Q{self.index} SUCCESS - duration: {self.duration})"
        elif self.error_msg:
            return f"(Q{self.index} FAILURE - error msg: {self.error_msg})"
        else:
            typing.assert_never("Both duration and error_msg are None")


def run_query_on_ray(
    run_args: RunArgs,
) -> list[Result]:
    ray.init(address=run_args.ray_address if run_args.ray_address else None)
    results = []

    for query_index in run_args.query_indices:
        working_dir = Path("benchmarking") / "tpcds"
        ray_entrypoint_script = "ray_entrypoint.py"
        duration = None
        error_msg = None
        try:
            start = datetime.now()
            ray_job_runner.run_on_ray(
                run_args.ray_address,
                {
                    "entrypoint": f"python {ray_entrypoint_script} --tpcds-gen-folder 'data/0.01' --question {query_index} {'--dry-run' if run_args.dry_run else ''}",
                    "runtime_env": {
                        "working_dir": working_dir,
                    },
                },
            )
            end = datetime.now()
            duration = end - start
        except Exception as e:
            error_msg = str(e)

        results.append(Result(index=query_index, duration=duration, error_msg=error_msg))

    return results


def run_query_on_local(
    run_args: RunArgs,
) -> list[Result]:
    catalog = helpers.generate_catalog(run_args.scaled_tpcds_gen_folder)
    results = []

    for query_index in run_args.query_indices:
        query_file = SQL_QUERIES_PATH / f"{query_index:02}.sql"
        with open(query_file) as f:
            query = f.read()

        start = datetime.now()

        duration = None
        error_msg = None
        try:
            daft.sql(query, catalog=catalog).explain(show_all=True)
            if not run_args.dry_run:
                daft.sql(query, catalog=catalog).collect()

            end = datetime.now()
            duration = end - start
        except Exception as e:
            error_msg = str(e)

        results.append(Result(index=query_index, duration=duration, error_msg=error_msg))

    return results


def run_benchmarks(
    run_args: RunArgs,
) -> list[Result]:
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
    results = run_benchmarks(
        RunArgs(
            scaled_tpcds_gen_folder=scaled_tpcds_gen_folder,
            query_indices=query_indices,
            ray_address=args.ray_address,
            dry_run=args.dry_run,
        )
    )

    # TODO(ronnie): improve visualization of results; simply printing them to console is not the best way...
    print(f"{results=}")


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
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Whether to run in dry-run mode; if true, only the plan will be printed, but no query will be executed",
    )
    args = parser.parse_args()

    tpcds_gen_folder: Path = args.tpcds_gen_folder
    assert args.scale_factor > 0

    main(
        ParsedArgs(
            tpcds_gen_folder=tpcds_gen_folder,
            scale_factor=args.scale_factor,
            questions=args.questions,
            ray_address=args.ray_address,
            dry_run=args.dry_run,
        )
    )

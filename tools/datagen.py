# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "PyGithub",
#   "boto3",
#   "duckdb",
#   "getdaft",
# ]
# ///


import logging
import os
from argparse import ArgumentParser
from pathlib import Path
from typing import Optional

import duckdb
import git_utils

import daft

logger = logging.getLogger(__name__)


def generate_local_tpcds_data(
    scale_factor: int,
    output_dir: Path,
):
    final_dir = output_dir / str(scale_factor)
    if final_dir.exists():
        if not final_dir.is_dir():
            raise ValueError(f"The path {final_dir} already exists, but it's not a directory")
        logger.warning(
            "The directory '%s' already exists; doing nothing",
            final_dir,
        )
        return

    final_dir.mkdir(parents=True, exist_ok=True)
    db = duckdb.connect(database=final_dir / "tpcds.db")
    db.sql(f"call dsdgen(sf = {scale_factor})")
    for item in db.sql("show tables").fetchall():
        tbl = item[0]
        parquet_file = final_dir / f"{tbl}.parquet"
        print(f"Exporting {tbl} to {parquet_file}")
        db.sql(f"COPY {tbl} TO '{parquet_file}'")

        daft.read_parquet(str(parquet_file)).write_parquet(final_dir / tbl)
        os.remove(parquet_file)


def generate_remote_tpcds_data(
    scale_factor: int,
):
    branch_name, _ = git_utils.get_name_and_commit_hash(None)

    workflow = git_utils.repo.get_workflow("datagen.yaml")
    git_utils.dispatch(
        workflow=workflow,
        branch_name=branch_name,
        inputs={
            "bench_type": "tpcds",
            "scale_factor": str(scale_factor),
        },
    )


def main(
    bench_type: str,
    scale_factor: int,
    num_partitions: int,
    output_dir: Optional[Path],
    remote: bool,
):
    if output_dir and remote:
        raise ValueError("Can't specify both `--output-dir` and `--remote`")

    if remote:
        if bench_type == "tpcds":
            generate_remote_tpcds_data(scale_factor)
        elif bench_type == "tpch":
            # todo!
            ...
    else:
        output_dir = output_dir or Path("data")

        if bench_type == "tpcds":
            generate_local_tpcds_data(scale_factor, output_dir)
        elif bench_type == "tpch":
            # todo!
            ...


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "bench_type", choices=["tpcds", "tpch"], type=str, help="Type of benchmark to generate data for"
    )
    parser.add_argument("--scale-factor", type=int, required=True, help="Size of data to generate (in GB)")
    parser.add_argument("--num-partitions", type=int, required=False, help="Number of partitions to create")
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=False,
        help="Which output directory to store the data in (relative to $CWD)",
    )
    parser.add_argument(
        "--remote",
        action="store_true",
        required=False,
        help="Whether to generate this data remotely (in S3)",
    )

    args = parser.parse_args()

    main(
        args.bench_type,
        args.scale_factor,
        args.num_partitions,
        args.output_dir,
        args.remote,
    )

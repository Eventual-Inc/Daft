# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "PyGithub",
#   "boto3",
#   "duckdb",
# ]
# ///


import logging
import typing
from argparse import ArgumentParser
from pathlib import Path
from typing import Optional

import duckdb

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


def generate_local_tpch_data(
    scale_factor: int,
    num_partitions: int,
    output_dir: Path,
): ...


def main(
    bench: str,
    scale_factor: int,
    num_partitions: int,
    output_dir: Optional[Path],
    remote: bool,
):
    if output_dir and remote:
        raise ValueError("Can't specify both `--output-dir` and `--remote`")

    if remote:
        # todo!
        ...

    output_dir = output_dir or Path("data")

    if bench == "tpcds":
        generate_local_tpcds_data(scale_factor, output_dir)
    elif bench == "tpch":
        generate_local_tpch_data(scale_factor, num_partitions, output_dir)

    else:
        typing.assert_never()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("bench", choices=["tpcds", "tpch"], type=str, help="Type of benchmark to generate data for")
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
        args.bench,
        args.scale_factor,
        args.num_partitions,
        args.output_dir,
        args.remote,
    )

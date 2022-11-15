"""This script provides a pipelined data generation implementation of data_generation.py

Note that after running this script, data will no longer be coherent/exist locally. This is used for generating large amounts
of benchmarking data that lands directly in AWS S3, but for local benchmarking/testing use-cases use data_generation.py instead.

Example call:
    DAFT_RUNNER=ray poetry run python benchmarking/tpch/pipelined_data_generation.py \
        --num-parts 32 \
        --scale-factor 1 \
        --aws-s3-sync-location s3://jay-test/tpch-pipelined-dbgen \
        --parallelism=8
"""

from __future__ import annotations

import argparse
import glob
import os
import pathlib
import shlex
import shutil
import subprocess
from multiprocessing import Pool

from loguru import logger

from benchmarking.tpch.data_generation import gen_parquet

STATIC_TABLES = ["nation", "region"]


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx : min(ndx + n, l)]


def gen_csv(part_idx: int, cachedir: str, scale_factor: float, num_parts: int):
    subprocess.check_output(
        shlex.split(f"./dbgen -v -f -s {scale_factor} -S {part_idx} -C {num_parts}"),
        cwd=str(cachedir),
    )


def pipelined_data_generation(
    scratch_dir: str, scale_factor: float, num_parts: int, aws_s3_sync_location: str, parallelism: int = 4
):
    assert num_parts > 1, "script should only be used if num_parts > 1"

    cachedir = pathlib.Path(scratch_dir) / ("%.1f" % scale_factor).replace(".", "_") / str(num_parts)

    if not cachedir.exists():
        logger.info("Cloning tpch dbgen repo")
        subprocess.check_output(shlex.split(f"git clone https://github.com/electrum/tpch-dbgen {str(cachedir)}"))
        subprocess.check_output("make", cwd=str(cachedir))

    for i, part_indices in enumerate(batch(range(1, num_parts + 1), n=parallelism)):
        logger.info(f"Partition {part_indices}: Generating CSV files")
        with Pool(parallelism) as process_pool:
            process_pool.starmap(gen_csv, [(part_idx, cachedir, scale_factor, num_parts) for part_idx in part_indices])

        # Postprocessing: remove trailing delimiter, change permissions of files
        logger.info(f"Partition {part_indices}: Post-processing CSV files")
        subprocess.check_output(shlex.split("chmod -R u+rwx ."), cwd=str(cachedir))
        csv_files = glob.glob(f"{cachedir}/*.tbl*")
        for csv_file in csv_files:
            subprocess.check_output(shlex.split(f"sed -e 's/|$//' -i.bak {csv_file}"))
        backup_files = glob.glob(f"{cachedir}/*.bak")
        for backup_file in backup_files:
            os.remove(backup_file)

        logger.info(f"Partition {part_indices}: Generating Parquet")
        generated_parquet_folder = gen_parquet(cachedir)

        logger.info(f"Partition {part_indices}: Syncing to AWS S3")
        # Exclude static tables except for first iteration
        exclude_static_tables = "" if i == 0 else " ".join([f'--exclude "*/{tbl}/*"' for tbl in STATIC_TABLES])
        subprocess.check_output(
            shlex.split(
                f'aws s3 sync {scratch_dir} {aws_s3_sync_location} --exclude "*" --include "*.parquet" {exclude_static_tables}'
            )
        )

        logger.info(f"Partition {part_indices}: Cleaning up files")
        shutil.rmtree(generated_parquet_folder)
        for table_file in glob.glob(f"{cachedir}/*.tbl*"):
            os.remove(table_file)

        logger.info(f"Partition {part_indices}: Completed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tpch_gen_folder",
        default="data/tpch-dbgen",
        help="Path to the folder containing the TPCH dbgen tool and generated data",
    )
    parser.add_argument("--scale-factor", default=10.0, help="Scale factor to run on in GB", type=float)
    parser.add_argument(
        "--num-parts", default=32, help="Number of parts to generate (defaults to 1 part per GB)", type=int
    )
    parser.add_argument(
        "--aws-s3-sync-location",
        default="s3://eventual-dev-benchmarking-fixtures/uncompressed/tpch-dbgen/",
        help="Where to sync files to in AWS S3",
    )
    parser.add_argument(
        "--parallelism",
        default=4,
        type=int,
        help="Number of partitions to generate per pipeline window",
    )
    args = parser.parse_args()
    pipelined_data_generation(
        args.tpch_gen_folder, args.scale_factor, args.num_parts, args.aws_s3_sync_location, parallelism=args.parallelism
    )

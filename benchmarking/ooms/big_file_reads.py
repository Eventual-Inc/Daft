# /// script
# dependencies = []
# ///

import argparse
import time

import daft


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--enable-optimized-splits", action="store_true", default=False, help="Whether to enable the new splits"
    )
    parser.add_argument("--dry-run", action="store_true", default=False)
    return parser.parse_args()


if __name__ == "__main__":
    daft.context.set_runner_ray()

    args = get_args()

    if args.enable_optimized_splits:
        daft.set_execution_config(enable_aggressive_scantask_splitting=True)

    df = daft.read_parquet(
        [
            "s3://daft-public-data/test_fixtures/parquet/large-fake-data.parquet",
        ]
        + [
            "s3://daft-public-data/test_fixtures/parquet/small-fake-data.parquet",
        ]
        * 10
    )
    df = df.with_column("name", df["name"] + "a")  # trigger data materialization by adding a projection

    start = time.time()
    df.explain(True)
    print(f"Explain took: {time.time() - start}s")

    start = time.time()
    df.show()
    print(f"Show took: {time.time() - start}s")

    if not args.dry_run:
        start = time.time()
        df.collect()
        print(f"Collect took: {time.time() - start}s")

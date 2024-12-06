import argparse
import logging
from datetime import datetime
from pathlib import Path

import daft
from daft.sql import SQLCatalog

from . import datagen


def generate_tpcds_catalog(tpcds_gen_folder: Path):
    if not tpcds_gen_folder.exists():
        raise RuntimeError
    return SQLCatalog(
        tables={
            file.stem: daft.read_parquet(path=str(file))
            for file in tpcds_gen_folder.iterdir()
            if file.is_file() and file.suffix == ".parquet"
        }
    )


def run_query(query_index: int, catalog: SQLCatalog) -> str:
    sql_queries_path = Path("benchmarking") / "tpcds" / "queries"
    query_file = sql_queries_path / f"{str(query_index).zfill(2)}.sql"
    with open(query_file) as f:
        query = f.read()

    start = datetime.now()

    try:
        daft.sql(query, catalog=catalog).collect()
        success = True
        error_message = None
    except Exception as e:
        success = False
        error_message = str(e)

    end = datetime.now()
    duration = end - start
    return duration, success, error_message if error_message else None


def run_benchmarks(basedir: Path):
    successes = {}
    failures = {}

    catalog = generate_tpcds_catalog(basedir)
    for query_index in range(1, 100):
        duration, was_successful, error_msg = run_query(query_index, catalog)
        if was_successful:
            successes[query_index] = duration
        else:
            failures[query_index] = (duration, error_msg)

    return successes, failures


def main():
    logging.basicConfig(level="INFO")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tpcds-gen-folder",
        default="data/tpcds-dbgen",
        type=Path,
        help="Path to the folder containing the TPC-DS dsdgen tool and generated data",
    )
    parser.add_argument("--scale-factor", default=0.01, type=float, help="Scale factor to run on in GB")
    args = parser.parse_args()

    tpcds_gen_folder = Path(args.tpcds_gen_folder)
    assert tpcds_gen_folder.exists()
    assert args.scale_factor > 0

    basedir = tpcds_gen_folder / str(args.scale_factor)
    datagen.gen_tpcds(basedir=basedir, scale_factor=args.scale_factor)
    run_benchmarks(basedir)


if __name__ == "__main__":
    main()

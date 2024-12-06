import argparse
import logging
from datetime import datetime
from pathlib import Path

import daft
from daft.sql import SQLCatalog

from . import datagen

logger = logging.getLogger(__name__)


def parse_questions_str(questions: str) -> list[int]:
    if questions == "*":
        return list(range(1, 100))

    nums = []
    for split in filter(lambda str: str, questions.split(",")):
        try:
            num = int(split)
            nums.append(num)
        except ValueError:
            ints = split.split("-")
            assert (
                len(ints) == 2
            ), f"A range must include two numbers split by a dash (i.e., '-'); instead got '{split}'"
            [lower, upper] = ints
            try:
                lower = int(lower)
                upper = int(upper)
                nums.extend(range(lower, upper + 1))
            except ValueError:
                raise ValueError(f"Invalid range: {split}")

    return nums


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


def run_benchmarks(basedir: Path, questions: str):
    successes = {}
    failures = {}

    query_indices = parse_questions_str(questions)
    logger.info(
        "Running the following questions: %s",
        query_indices,
    )

    catalog = generate_tpcds_catalog(basedir)
    for query_index in query_indices:
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
    parser.add_argument("--questions", default="*", type=str, help="The questions to run")
    args = parser.parse_args()

    tpcds_gen_folder = Path(args.tpcds_gen_folder)
    assert tpcds_gen_folder.exists()
    assert args.scale_factor > 0

    basedir = tpcds_gen_folder / str(args.scale_factor)
    datagen.gen_tpcds(basedir=basedir, scale_factor=args.scale_factor)
    run_benchmarks(basedir, args.questions)


if __name__ == "__main__":
    main()

import argparse
from pathlib import Path

import helpers

import daft


def run(
    parquet_folder: Path,
    question: int,
    dry_run: bool,
):
    catalog = helpers.generate_catalog(parquet_folder)
    query_file = Path(__file__).parent / "queries" / f"{question:02}.sql"
    with open(query_file) as f:
        query = f.read()

    daft.sql(query, catalog=catalog).explain(show_all=True)
    if not dry_run:
        daft.sql(query, catalog=catalog).collect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tpcds-gen-folder",
        required=True,
        type=Path,
        help="Path to the TPC-DS data generation folder",
    )
    parser.add_argument(
        "--question",
        required=True,
        type=int,
        help="The TPC-DS question index to run",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Whether or not to run the query in dry-run mode; if true, only the plan will be printed out",
    )
    args = parser.parse_args()

    tpcds_gen_folder: Path = args.tpcds_gen_folder
    assert tpcds_gen_folder.exists()
    assert args.question in range(1, 100)

    run(args.tpcds_gen_folder, args.question, args.dry_run)

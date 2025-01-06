import argparse
from pathlib import Path

import daft
from daft.sql.sql import SQLCatalog

TABLE_NAMES = [
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
]


def register_catalog(scale_factor: int) -> SQLCatalog:
    return SQLCatalog(
        tables={
            table: daft.read_parquet(
                f"s3://eventual-dev-benchmarking-fixtures/uncompressed/tpcds-dbgen/{scale_factor}/{table}.parquet"
            )
            for table in TABLE_NAMES
        }
    )


def run(
    question: int,
    dry_run: bool,
    scale_factor: int,
):
    catalog = register_catalog(scale_factor)
    query_file = Path(__file__).parent / "queries" / f"{question:02}.sql"
    with open(query_file) as f:
        query = f.read()

    daft.sql(query, catalog=catalog).explain(show_all=True)
    if not dry_run:
        daft.sql(query, catalog=catalog).collect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--question",
        type=int,
        help="The TPC-DS question index to run",
        required=True,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Whether or not to run the query in dry-run mode; if true, only the plan will be printed out",
    )
    parser.add_argument(
        "--scale-factor",
        type=int,
        help="Which scale factor to run this data at",
        required=False,
        default=2,
    )
    args = parser.parse_args()

    assert args.question in range(1, 100)

    run(question=args.question, dry_run=args.dry_run, scale_factor=args.scale_factor)

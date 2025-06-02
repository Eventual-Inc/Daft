from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import daft

if TYPE_CHECKING:
    from daft.dataframe.dataframe import DataFrame

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


def create_bindings(scale_factor: int) -> dict[str, DataFrame]:
    return {
        table: daft.read_parquet(
            f"s3://eventual-dev-benchmarking-fixtures/uncompressed/tpcds-dbgen/{scale_factor}/{table}"
        )
        for table in TABLE_NAMES
    }


def run(
    question: int,
    dry_run: bool,
    scale_factor: int,
):
    query_file = Path(__file__).parent / "queries" / f"{question:02}.sql"
    with open(query_file) as f:
        query_string = f.read()

    info_path = Path("/tmp") / "ray" / "session_latest" / "logs" / "info"
    info_path.mkdir(parents=True, exist_ok=True)
    query = daft.sql(query_string, **create_bindings(scale_factor))

    explain_delta = None
    with open(info_path / f"plan-{question}.txt", "w") as f:
        explain_start = datetime.now()
        query.explain(show_all=True, file=f, format="mermaid")
        explain_end = datetime.now()
        explain_delta = explain_end - explain_start

    execute_delta = None
    if not dry_run:
        execute_start = datetime.now()
        query.collect()
        execute_end = datetime.now()
        execute_delta = execute_end - execute_start

    with open(info_path / f"stats-{question}.txt", "w") as f:
        stats = json.dumps(
            {
                "question": question,
                "scale-factor": scale_factor,
                "planning-time": str(explain_delta),
                "execution-time": str(execute_delta),
            }
        )
        f.write(stats)


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

import argparse
import logging
import typing
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import duckdb
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import ray

import daft
from benchmarking.tpcds.datagen import gen_tpcds
from benchmarking.tpcds.helpers import convert_all_tpcds_decimals

from ..tpch import __main__ as tpch
from ..tpch import ray_job_runner
from . import helpers

logger = logging.getLogger(__name__)

SQL_QUERIES_PATH = Path(__file__).parent / "queries"


def print_table_schemas(data_dir: Path):
    """Print available tables and their columns using pyarrow."""
    logger.info("Available tables and their columns:")
    for parquet_file in data_dir.glob("*.parquet"):
        table_name = parquet_file.stem
        logger.info("\nTable: %s", table_name)
        schema = pq.read_schema(parquet_file)
        for field in schema:
            logger.info("  - %s: %s", field.name, field.type)


@dataclass
class ParsedArgs:
    tpcds_gen_folder: Path
    scale_factor: float
    questions: str
    ray_address: Optional[str]
    dry_run: bool
    convert_decimals: bool
    validate: bool
    print_schema: bool


@dataclass
class RunArgs:
    scaled_tpcds_gen_folder: Path
    query_indices: list[int]
    ray_address: Optional[str]
    dry_run: bool
    validate: bool
    print_schema: bool


@dataclass
class Result:
    index: int
    duration: Optional[timedelta]
    error_msg: Optional[str]
    is_correct: Optional[bool] = None
    validation_error: Optional[str] = None

    def __repr__(self) -> str:
        if self.duration and self.error_msg:
            typing.assert_never("Both duration and error_msg are not None")
        elif self.duration:
            validation_str = ""
            if self.is_correct is not None:
                validation_str = f" - Correct: {self.is_correct}"
                if not self.is_correct and self.validation_error:
                    validation_str += f" - Validation Error: {self.validation_error}"
            return f"(Q{self.index} SUCCESS - duration: {self.duration}{validation_str})"
        elif self.error_msg:
            return f"(Q{self.index} FAILURE - error msg: {self.error_msg})"
        else:
            typing.assert_never("Both duration and error_msg are None")


def setup_duckdb_catalog(data_dir: Path) -> duckdb.DuckDBPyConnection:
    """Setup DuckDB connection with the same data files."""
    conn = duckdb.connect(database=":memory:")

    for parquet_file in data_dir.glob("*.parquet"):
        table_name = parquet_file.stem
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_file}')")

    return conn


def validate_query_results(daft_df: pd.DataFrame, query: str, data_dir: Path) -> tuple[bool, Optional[str]]:
    """Compare Daft results with DuckDB results for validation."""
    try:
        conn = setup_duckdb_catalog(data_dir)

        duckdb_result = conn.execute(query).fetchdf()

        if len(daft_df) != len(duckdb_result):
            return False, f"Row count mismatch: Daft={len(daft_df)}, DuckDB={len(duckdb_result)}"

        daft_df.columns = [col.lower() for col in daft_df.columns]
        duckdb_result.columns = [col.lower() for col in duckdb_result.columns]

        if set(daft_df.columns) != set(duckdb_result.columns):
            return False, f"Column mismatch: Daft={daft_df.columns}, DuckDB={duckdb_result.columns}"

        daft_df = daft_df.sort_values(by=list(daft_df.columns)).reset_index(drop=True)
        duckdb_result = duckdb_result.sort_values(by=list(duckdb_result.columns)).reset_index(drop=True)

        for col in daft_df.columns:
            if pd.api.types.is_numeric_dtype(daft_df[col]) and pd.api.types.is_numeric_dtype(duckdb_result[col]):
                if not np.allclose(
                    daft_df[col].fillna(0).to_numpy(),
                    duckdb_result[col].fillna(0).to_numpy(),
                    rtol=1e-5,
                    atol=1e-8,
                    equal_nan=True,
                ):
                    return False, f"Numeric values differ in column {col}"
            else:
                if not daft_df[col].equals(duckdb_result[col]):
                    return False, f"Values differ in column {col}"

        return True, None

    except Exception as e:
        return False, f"Validation error: {e!s}"


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
        is_correct = None
        validation_error = None
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

        results.append(
            Result(
                index=query_index,
                duration=duration,
                error_msg=error_msg,
                is_correct=is_correct,
                validation_error=validation_error,
            )
        )

    return results


def run_query_on_local(
    run_args: RunArgs,
) -> list[Result]:
    if run_args.print_schema:
        print_table_schemas(run_args.scaled_tpcds_gen_folder)

    catalog = helpers.generate_catalog(run_args.scaled_tpcds_gen_folder)
    results = []

    for query_index in run_args.query_indices:
        query_file = SQL_QUERIES_PATH / f"{query_index:02}.sql"
        with open(query_file) as f:
            query = f.read()

        start = datetime.now()

        duration = None
        error_msg = None
        is_correct = None
        validation_error = None

        try:
            daft.sql(query, catalog=catalog).explain(show_all=True)
            if not run_args.dry_run:
                daft_results = daft.sql(query, catalog=catalog).collect()

                daft_df = daft_results.to_pandas()

                if run_args.validate and not run_args.dry_run:
                    is_correct, validation_error = validate_query_results(
                        daft_df, query, run_args.scaled_tpcds_gen_folder
                    )
                    if is_correct:
                        logger.info("Query %s results validated successfully", query_index)
                    else:
                        logger.warning("Query %s validation failed: %s", query_index, validation_error)

            end = datetime.now()
            duration = end - start
        except Exception as e:
            error_msg = str(e)

        results.append(
            Result(
                index=query_index,
                duration=duration,
                error_msg=error_msg,
                is_correct=is_correct,
                validation_error=validation_error,
            )
        )

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
    gen_tpcds(scaled_tpcds_gen_folder, args.scale_factor)
    query_indices = helpers.parse_questions_str(args.questions)
    if args.convert_decimals:
        convert_all_tpcds_decimals(scaled_tpcds_gen_folder)
    results = run_benchmarks(
        RunArgs(
            scaled_tpcds_gen_folder=scaled_tpcds_gen_folder,
            query_indices=query_indices,
            ray_address=args.ray_address,
            dry_run=args.dry_run,
            validate=args.validate,
            print_schema=args.print_schema,
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
    parser.add_argument(
        "--print-schema",
        action="store_false",
        help="Print the schema of the tables in the TPC-DS dataset",
    )
    parser.add_argument(
        "--convert-decimals",
        action="store_true",
        help="Convert decimal columns to float64 before running queries",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate query results against DuckDB reference implementation",
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
            convert_decimals=args.convert_decimals,
            validate=args.validate,
            print_schema=args.print_schema,
        )
    )

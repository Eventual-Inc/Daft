from __future__ import annotations

import argparse
import logging
import typing
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import ray

import daft
from benchmarking.tpcds.datagen import gen_tpcds
from benchmarking.tpcds.helpers import parse_questions_str

from ..tpch import __main__ as tpch
from ..tpch import ray_job_runner
from . import helpers

logger = logging.getLogger(__name__)

SQL_QUERIES_PATH = Path(__file__).parent / "queries"


@dataclass
class ParsedArgs:
    tpcds_gen_folder: Path
    scale_factor: float
    questions: str
    ray_address: str | None
    dry_run: bool
    validate: bool
    cast_decimals: bool
    save_csv_on_failure: bool


@dataclass
class RunArgs:
    scaled_tpcds_gen_folder: Path
    query_indices: list[int]
    ray_address: str | None
    dry_run: bool
    validate: bool
    save_csv_on_failure: bool


@dataclass
class Result:
    index: int
    duration: timedelta | None
    error_msg: str | None
    is_correct: bool | None = None
    validation_error: str | None = None

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
    """Setup DuckDB connection using the existing database file."""
    return duckdb.connect(str(data_dir / "tpcds.db"))


def _is_string(tp: pa.DataType) -> bool:
    return pa.types.is_string(tp) or pa.types.is_large_string(tp)


def _is_numeric(tp: pa.DataType) -> bool:
    return pa.types.is_floating(tp) or pa.types.is_decimal(tp) or pa.types.is_integer(tp)


def _types_compatible(t1: pa.DataType, t2: pa.DataType) -> bool:
    """Decide whether two Arrow data types should be considered the same."""
    if t1.equals(t2):
        return True

    if _is_string(t1) and _is_string(t2):
        return True

    if _is_numeric(t1) and _is_numeric(t2):
        return True

    return False


def _arrays_equal(
    lhs: pa.ChunkedArray,
    rhs: pa.ChunkedArray,
    *,
    rtol: float,
    atol: float,
) -> bool:
    """Return True if two ChunkedArrays are equal within tolerance."""
    lhs = lhs.combine_chunks()
    rhs = rhs.combine_chunks()

    if pc.any(pc.xor(pc.is_null(lhs), pc.is_null(rhs))).as_py():
        return False

    t1, t2 = lhs.type, rhs.type

    if _is_numeric(t1) and _is_numeric(t2):
        lhs_f = pc.cast(lhs, pa.float64(), safe=False)
        rhs_f = pc.cast(rhs, pa.float64(), safe=False)

        mask = pc.invert(pc.is_null(lhs_f))
        lhs_np = lhs_f.filter(mask).to_numpy()
        rhs_np = rhs_f.filter(mask).to_numpy()

        return np.allclose(lhs_np, rhs_np, rtol=rtol, atol=atol)

    if _is_string(t1) and _is_string(t2):
        lhs_casted = pc.cast(lhs, pa.large_string(), safe=False)
        rhs_casted = pc.cast(rhs, pa.large_string(), safe=False)
        return lhs_casted.equals(rhs_casted)

    return lhs.equals(rhs)


def validate_query_results(
    daft_results,
    query: str,
    data_dir: Path,
    *,
    rtol: float = 1e-3,
    atol: float = 1e-3,
    save_csv_on_failure: bool = False,
    query_index: int | None = None,
) -> tuple[bool, str | None]:
    """Compare Daft and DuckDB results."""
    try:
        conn = setup_duckdb_catalog(data_dir)
        daft_tbl = daft_results.to_arrow()
        duck_tbl = conn.execute(query).arrow()

        daft_csv_name = f"daft_q{query_index}_results.csv" if query_index is not None else "daft_results.csv"
        duckdb_csv_name = f"duckdb_q{query_index}_results.csv" if query_index is not None else "duckdb_results.csv"

        csv_saved_message = f" Results saved to {daft_csv_name} and {duckdb_csv_name}." if save_csv_on_failure else ""

        if daft_tbl.num_rows != duck_tbl.num_rows:
            if save_csv_on_failure:
                daft_tbl.to_pandas().to_csv(daft_csv_name, index=False)
                duck_tbl.to_pandas().to_csv(duckdb_csv_name, index=False)
            return (
                False,
                f"Row counts differ: Daft has {daft_tbl.num_rows} rows, DuckDB has {duck_tbl.num_rows} rows.{csv_saved_message}",
            )

        if daft_tbl.schema.names != duck_tbl.schema.names:
            if save_csv_on_failure:
                daft_tbl.to_pandas().to_csv(daft_csv_name, index=False)
                duck_tbl.to_pandas().to_csv(duckdb_csv_name, index=False)
            return (
                False,
                f"Column name order differs.\\nDaft column names: {daft_tbl.schema.names}\\nDuckDB column names: {duck_tbl.schema.names}.{csv_saved_message}",
            )

        for field_daft, field_duck in zip(daft_tbl.schema, duck_tbl.schema):
            name = field_daft.name
            type_daft = field_daft.type
            type_duck = field_duck.type

            if not _types_compatible(type_daft, type_duck):
                if save_csv_on_failure:
                    daft_tbl.to_pandas().to_csv(daft_csv_name, index=False)
                    duck_tbl.to_pandas().to_csv(duckdb_csv_name, index=False)
                return (
                    False,
                    f"Incompatible types for column '{name}': Daft type is {type_daft}, DuckDB type is {type_duck}.{csv_saved_message}",
                )

            lhs_arr = daft_tbl.column(name)
            rhs_arr = duck_tbl.column(name)

            if not _arrays_equal(lhs_arr, rhs_arr, rtol=rtol, atol=atol):
                if save_csv_on_failure:
                    daft_tbl.to_pandas().to_csv(daft_csv_name, index=False)
                    duck_tbl.to_pandas().to_csv(duckdb_csv_name, index=False)
                if _is_string(type_daft) and _is_string(type_duck):
                    return (
                        False,
                        f"Column '{name}' (types: Daft={type_daft}, DuckDB={type_duck}) differs.{csv_saved_message}",
                    )
                else:
                    return (
                        False,
                        f"Column '{name}' (types: Daft={type_daft}, DuckDB={type_duck}) differs beyond tolerance "
                        f"(rtol={rtol}, atol={atol}).{csv_saved_message}",
                    )

        return True, None

    except Exception as e:
        return False, f"Validation error during comparison process: {e!s}"


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

                if run_args.validate and not run_args.dry_run:
                    is_correct, validation_error = validate_query_results(
                        daft_results,
                        query,
                        run_args.scaled_tpcds_gen_folder,
                        save_csv_on_failure=run_args.save_csv_on_failure,
                        query_index=query_index,
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
    elif runner == "native":
        return run_query_on_local(run_args)
    else:
        typing.assert_never(runner)


def main(args: ParsedArgs):
    scaled_tpcds_gen_folder = args.tpcds_gen_folder / str(args.scale_factor)
    gen_tpcds(scaled_tpcds_gen_folder, args.scale_factor, cast_decimal=args.cast_decimals)
    query_indices = parse_questions_str(args.questions)
    results = run_benchmarks(
        RunArgs(
            scaled_tpcds_gen_folder=scaled_tpcds_gen_folder,
            query_indices=query_indices,
            ray_address=args.ray_address,
            dry_run=args.dry_run,
            validate=args.validate,
            save_csv_on_failure=args.save_csv_on_failure,
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
        "--cast-decimals",
        action="store_true",
        help="Cast decimal columns to float64 before running queries",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate query results against DuckDB reference implementation",
    )
    parser.add_argument(
        "--save-csv-on-failure",
        action="store_true",
        dest="save_csv_on_failure",
        help="Save Daft and DuckDB results to CSV if validation fails (default: False)",
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
            validate=args.validate,
            cast_decimals=args.cast_decimals,
            save_csv_on_failure=args.save_csv_on_failure,
        )
    )

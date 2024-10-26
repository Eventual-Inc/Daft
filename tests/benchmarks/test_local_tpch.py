from __future__ import annotations

import os
import sys

import pytest
from fsspec.implementations.local import LocalFileSystem

import daft
from benchmarking.tpch import answers, data_generation

if sys.platform == "win32":
    pytest.skip(allow_module_level=True)

import itertools

import daft.context
from tests.assets import TPCH_DBGEN_DIR
from tests.integration.conftest import *  # noqa: F403

IS_CI = True if os.getenv("CI") else False

SCALE_FACTOR = 0.2
ENGINES = ["native"] if IS_CI else ["native", "python"]
NUM_PARTS = [1] if IS_CI else [1, 2]
SOURCE_TYPES = ["in-memory"] if IS_CI else ["parquet", "in-memory"]


@pytest.fixture(scope="session", params=NUM_PARTS)
def gen_tpch(request):
    # Parametrize the number of parts for each file so that we run tests on single-partition files and multi-partition files
    num_parts = request.param

    csv_files_location = data_generation.gen_csv_files(TPCH_DBGEN_DIR, num_parts, SCALE_FACTOR)

    # Disable native executor to generate parquet files, remove once native executor supports writing parquet files
    with daft.context.execution_config_ctx(enable_native_executor=False):
        parquet_files_location = data_generation.gen_parquet(csv_files_location)

    in_memory_tables = {}
    for tbl_name in data_generation.SCHEMA.keys():
        arrow_table = daft.read_parquet(f"{parquet_files_location}/{tbl_name}/*").to_arrow()
        in_memory_tables[tbl_name] = daft.from_arrow(arrow_table)

    sqlite_path = data_generation.gen_sqlite_db(
        csv_filepath=csv_files_location,
        num_parts=num_parts,
    )

    return (
        csv_files_location,
        parquet_files_location,
        in_memory_tables,
        num_parts,
    ), sqlite_path


@pytest.fixture(scope="module", params=SOURCE_TYPES)  # TODO: Enable CSV after improving the CSV reader
def get_df(gen_tpch, request):
    (csv_files_location, parquet_files_location, in_memory_tables, num_parts), _ = gen_tpch
    source_type = request.param

    def _get_df(tbl_name: str):
        if source_type == "csv":
            local_fs = LocalFileSystem()
            nonchunked_filepath = f"{csv_files_location}/{tbl_name}.tbl"
            chunked_filepath = nonchunked_filepath + ".*"
            try:
                local_fs.expand_path(chunked_filepath)
                fp = chunked_filepath
            except FileNotFoundError:
                fp = nonchunked_filepath

            df = daft.read_csv(
                fp,
                has_headers=False,
                delimiter="|",
            )
            df = df.select(
                *[
                    daft.col(autoname).alias(colname)
                    for autoname, colname in zip(df.column_names, data_generation.SCHEMA[tbl_name])
                ]
            )
        elif source_type == "parquet":
            fp = f"{parquet_files_location}/{tbl_name}/*"
            df = daft.read_parquet(fp)
        elif source_type == "in-memory":
            df = in_memory_tables[tbl_name]

        return df

    return _get_df, num_parts


TPCH_QUESTIONS = list(range(1, 11))


@pytest.mark.skipif(
    daft.context.get_context().runner_config.name not in {"py"},
    reason="requires PyRunner to be in use",
)
@pytest.mark.benchmark(group="tpch")
@pytest.mark.parametrize("engine, q", itertools.product(ENGINES, TPCH_QUESTIONS))
def test_tpch(tmp_path, check_answer, get_df, benchmark_with_memray, engine, q):
    get_df, num_parts = get_df

    def f():
        if engine == "native":
            ctx = daft.context.execution_config_ctx(enable_native_executor=True)
        elif engine == "python":
            ctx = daft.context.execution_config_ctx(enable_native_executor=False)
        else:
            raise ValueError(f"{engine} unsupported")

        with ctx:
            question = getattr(answers, f"q{q}")
            daft_df = question(get_df)
            return daft_df.to_arrow()

    benchmark_group = f"q{q}-parts-{num_parts}"
    daft_pd_df = benchmark_with_memray(f, benchmark_group).to_pandas()
    check_answer(daft_pd_df, q, tmp_path)

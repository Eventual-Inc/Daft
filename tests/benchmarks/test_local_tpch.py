from __future__ import annotations

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

# Hardcode scale factor to 200M for local testing
SCALE_FACTOR = 0.2


@pytest.fixture(scope="session", params=[1, 2])
def gen_tpch(request):
    # Parametrize the number of parts for each file so that we run tests on single-partition files and multi-partition files
    num_parts = request.param

    csv_files_location = data_generation.gen_csv_files(TPCH_DBGEN_DIR, num_parts, SCALE_FACTOR)

    sqlite_path = data_generation.gen_sqlite_db(
        csv_filepath=csv_files_location,
        num_parts=num_parts,
    )

    return (csv_files_location, num_parts), sqlite_path


@pytest.fixture(scope="module")
def get_df(gen_tpch):
    (csv_files_location, num_parts), _ = gen_tpch

    def _get_df(tbl_name: str):
        # TODO (jay): Perhaps we should use Parquet here instead similar to benchmarking and get rid of this CSV parsing stuff?
        local_fs = LocalFileSystem()
        # Used chunked files if found
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
        return df

    return _get_df, num_parts


TPCH_QUESTIONS = list(range(1, 11))


@pytest.mark.parametrize("engine, q", itertools.product(["native", "python"], TPCH_QUESTIONS))
def test_tpch(tmp_path, check_answer, get_df, benchmark, engine, q):
    if engine == "native":
        daft.context.set_execution_config(enable_native_executor=True)
    elif engine == "python":
        daft.context.set_execution_config(enable_native_executor=False)
    else:
        raise ValueError(f"{engine} unsupported")

    get_df, num_parts = get_df
    benchmark.group = f"q{q}-parts-{num_parts}"

    def f():
        question = getattr(answers, f"q{q}")
        daft_df = question(get_df)
        return daft_df.to_arrow()

    daft_pd_df = benchmark(f).to_pandas()
    check_answer(daft_pd_df, q, tmp_path)

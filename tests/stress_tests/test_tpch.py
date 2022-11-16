from __future__ import annotations

import math
import os
import pathlib
import sqlite3

import pandas as pd
import pytest
from fsspec.implementations.local import LocalFileSystem
from sentry_sdk import start_transaction

from benchmarking.tpch import answers, data_generation
from daft.context import get_context
from daft.dataframe import DataFrame
from tests.assets.assets import TPCH_DBGEN_DIR, TPCH_QUERIES
from tests.conftest import assert_df_equals

# If running in github, we use smaller-scale data


@pytest.fixture(scope="session", autouse=True)
def scale_factor():
    scale_factor = float(os.getenv("SCALE_FACTOR", "1"))
    scale_factor = 0.2 if os.getenv("CI") else scale_factor
    return scale_factor


@pytest.fixture(scope="session", autouse=True)
def gen_tpch(scale_factor):
    num_parts = math.ceil(scale_factor)

    csv_files_location = data_generation.gen_csv_files(TPCH_DBGEN_DIR, num_parts, scale_factor)

    sqlite_path = data_generation.gen_sqlite_db(
        csv_filepath=csv_files_location,
        num_parts=num_parts,
    )

    return csv_files_location, sqlite_path


@pytest.fixture(scope="module")
def get_df(gen_tpch):
    csv_files_location, _ = gen_tpch

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

        df = DataFrame.read_csv(
            fp,
            has_headers=False,
            column_names=data_generation.SCHEMA[tbl_name],
            delimiter="|",
        )
        return df

    return _get_df


@pytest.fixture(scope="module")
def check_answer(gen_tpch):
    _, sqlite_db_file_path = gen_tpch

    def _check_answer(daft_pd_df: pd.DataFrame, tpch_question: int, tmp_path: str):
        query = pathlib.Path(f"{TPCH_QUERIES}/{tpch_question}.sql").read_text()
        conn = sqlite3.connect(sqlite_db_file_path, detect_types=sqlite3.PARSE_DECLTYPES)
        cursor = conn.cursor()
        res = cursor.execute(query)
        sqlite_results = res.fetchall()
        sqlite_pd_results = pd.DataFrame.from_records(sqlite_results, columns=daft_pd_df.columns)
        assert_df_equals(daft_pd_df, sqlite_pd_results, assert_ordering=True)

    return _check_answer


def test_tpch_q1(tmp_path, check_answer, get_df):
    daft_df = answers.q1(get_df)
    with start_transaction(op="task", name=f"tpch_q1:runner={get_context().runner_config.name.upper()}"):
        daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 1, tmp_path)


def test_tpch_q2(tmp_path, check_answer, get_df):
    daft_df = answers.q2(get_df)
    with start_transaction(op="task", name=f"tpch_q2:runner={get_context().runner_config.name.upper()}"):
        daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 2, tmp_path)


def test_tpch_q3(tmp_path, check_answer, get_df):
    daft_df = answers.q3(get_df)
    with start_transaction(op="task", name=f"tpch_q3:runner={get_context().runner_config.name.upper()}"):
        daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 3, tmp_path)


def test_tpch_q4(tmp_path, check_answer, get_df):
    daft_df = answers.q4(get_df)

    with start_transaction(op="task", name=f"tpch_q4:runner={get_context().runner_config.name.upper()}"):
        daft_pd_df = daft_df.to_pandas()

    check_answer(daft_pd_df, 4, tmp_path)


def test_tpch_q5(tmp_path, check_answer, get_df):
    daft_df = answers.q5(get_df)

    with start_transaction(op="task", name=f"tpch_q5:runner={get_context().runner_config.name.upper()}"):
        daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 5, tmp_path)


def test_tpch_q6(tmp_path, check_answer, get_df):
    daft_df = answers.q6(get_df)

    with start_transaction(op="task", name=f"tpch_q6:runner={get_context().runner_config.name.upper()}"):
        daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 6, tmp_path)


def test_tpch_q7(tmp_path, check_answer, get_df):
    daft_df = answers.q7(get_df)

    with start_transaction(op="task", name=f"tpch_q7:runner={get_context().runner_config.name.upper()}"):
        daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 7, tmp_path)


def test_tpch_q8(tmp_path, check_answer, get_df):
    daft_df = answers.q8(get_df)

    with start_transaction(op="task", name=f"tpch_q8:runner={get_context().runner_config.name.upper()}"):
        daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 8, tmp_path)


def test_tpch_q9(tmp_path, check_answer, get_df):
    daft_df = answers.q9(get_df)

    with start_transaction(op="task", name=f"tpch_q9:runner={get_context().runner_config.name.upper()}"):
        daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 9, tmp_path)


def test_tpch_q10(tmp_path, check_answer, get_df):
    daft_df = answers.q10(get_df)

    with start_transaction(op="task", name=f"tpch_q10:runner={get_context().runner_config.name.upper()}"):
        daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 10, tmp_path)

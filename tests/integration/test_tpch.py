from __future__ import annotations

import sys

import pytest
from fsspec.implementations.local import LocalFileSystem

import daft
from benchmarking.tpch import answers, data_generation

if sys.platform == "win32":
    pytest.skip(allow_module_level=True)


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

    return _get_df


def test_tpch_q1(tmp_path, check_answer, get_df):
    daft_df = answers.q1(get_df)
    daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 1, tmp_path)


def test_tpch_q2(tmp_path, check_answer, get_df):
    daft_df = answers.q2(get_df)
    daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 2, tmp_path)


def test_tpch_q3(tmp_path, check_answer, get_df):
    daft_df = answers.q3(get_df)
    daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 3, tmp_path)


def test_tpch_q4(tmp_path, check_answer, get_df):
    daft_df = answers.q4(get_df)
    daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 4, tmp_path)


def test_tpch_q5(tmp_path, check_answer, get_df):
    daft_df = answers.q5(get_df)
    daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 5, tmp_path)


def test_tpch_q6(tmp_path, check_answer, get_df):
    daft_df = answers.q6(get_df)
    daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 6, tmp_path)


def test_tpch_q7(tmp_path, check_answer, get_df):
    daft_df = answers.q7(get_df)
    daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 7, tmp_path)


def test_tpch_q8(tmp_path, check_answer, get_df):
    daft_df = answers.q8(get_df)
    daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 8, tmp_path)


def test_tpch_q9(tmp_path, check_answer, get_df):
    daft_df = answers.q9(get_df)
    daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 9, tmp_path)


def test_tpch_q10(tmp_path, check_answer, get_df):
    daft_df = answers.q10(get_df)
    daft_pd_df = daft_df.to_pandas()
    check_answer(daft_pd_df, 10, tmp_path)

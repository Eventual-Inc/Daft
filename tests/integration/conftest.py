from __future__ import annotations

import pathlib
import sqlite3

import pandas as pd
import pytest

from benchmarking.tpch import data_generation
from tests.assets import TPCH_DBGEN_DIR, TPCH_QUERIES
from tests.conftest import assert_df_equals

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

    return csv_files_location, sqlite_path


@pytest.fixture(scope="module")
def check_answer(gen_tpch):
    _, sqlite_db_file_path = gen_tpch

    def _check_answer(daft_pd_df: pd.DataFrame, tpch_question: int, tmp_path: str):
        query = pathlib.Path(f"{TPCH_QUERIES}/{tpch_question}.sql").read_text()
        conn = sqlite3.connect(sqlite_db_file_path, detect_types=sqlite3.PARSE_DECLTYPES)
        cursor = conn.cursor()
        queries = query.split(";")
        for q in queries:
            if not q.isspace():
                res = cursor.execute(q)
        sqlite_results = res.fetchall()
        sqlite_pd_results = pd.DataFrame.from_records(sqlite_results, columns=daft_pd_df.columns)
        assert_df_equals(
            daft_pd_df,
            sqlite_pd_results,
            assert_ordering=True,
            # We lose fine-grained dtype information from the sqlite3 API
            check_dtype=False,
        )

    return _check_answer

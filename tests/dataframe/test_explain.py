from __future__ import annotations

import io

import lance
import pytest

import daft
from daft.dependencies import pa
from tests.conftest import get_tests_daft_runner_name
from tests.utils import clean_explain_output


@pytest.fixture
def input_df(tmp_path):
    lance.write_dataset(pa.Table.from_pydict({"id": [id for id in range(16)]}), uri=tmp_path)
    return daft.read_lance(uri=str(tmp_path))


@pytest.mark.skipif(
    condition=get_tests_daft_runner_name() == "native",
    reason="The physical plan displayed in Native and Ray mode is inconsistent.",
)
def test_explain_with_empty_scantask(input_df):
    string_io = io.StringIO()
    input_df.explain(True, file=string_io)
    expected = """

    * ScanTaskSource:
    |   Num Scan Tasks = 1
    |   Estimated Scan Bytes = 0
    |   Schema: {id#Int64}
    |   Scan Tasks: [
    |   {daft.io.lance.lance_scan:_lancedb_table_factory_function}
    |   ]

    """
    assert clean_explain_output(string_io.getvalue().split("== Physical Plan ==")[-1]) == clean_explain_output(expected)

    string_io = io.StringIO()
    input_df.limit(0).explain(True, file=string_io)
    expected = """

* ScanTaskSource:
|   Num Scan Tasks = 0
|   Estimated Scan Bytes = 0
|   Pushdowns: {limit: 0}
|   Schema: {id#Int64}
|   Scan Tasks: [
|   ]

"""
    assert clean_explain_output(string_io.getvalue().split("== Physical Plan ==")[-1]) == clean_explain_output(expected)

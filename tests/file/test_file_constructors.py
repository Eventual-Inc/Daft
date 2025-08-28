from __future__ import annotations

from pathlib import Path

import pytest

import daft
from tests.conftest import get_tests_daft_runner_name


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_df_construct_from_file(tmp_path: Path):
    temp_file = tmp_path / "test_file.txt"
    temp_file.write_text("hello world from file")
    path_file = daft.File(str(temp_file.absolute()))
    mem_file = daft.File(b"hello world from bytes")
    df = daft.from_pydict({"file": [path_file, mem_file]})
    data = df.to_pydict()["file"]
    expected_data_0 = path_file.read()
    actual_data_0 = data[0].read()
    assert expected_data_0 == actual_data_0
    expected_data_1 = mem_file.read()
    actual_data_1 = data[1].read()
    assert expected_data_1 == actual_data_1

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
    df = daft.from_pydict({"file": [path_file]})
    data = df.to_pydict()["file"]
    with path_file.open() as f:
        expected_data_0 = f.read()
    with data[0].open() as f:
        actual_data_0 = f.read()
    assert expected_data_0 == actual_data_0

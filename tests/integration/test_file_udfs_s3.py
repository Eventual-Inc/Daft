from __future__ import annotations

import pytest

import daft
from daft.functions import file


@pytest.mark.integration
def test_file_read_from_s3():
    df = daft.from_glob_path("s3://daft-public-data/tutorials/pdfs/**/*.pdf").limit(5)

    @daft.func
    def n_bytes(file: daft.File) -> int:
        # This is the most inefficient way to do this. but its just for testing so :shrug:
        return len(file.read())

    actual = df.select(file(df["path"])).select(n_bytes(df["path"])).to_pydict()
    expected = df.select(df["path"].url.download().binary.length()).to_pydict()
    assert actual == expected

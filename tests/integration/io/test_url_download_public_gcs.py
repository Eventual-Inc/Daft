from __future__ import annotations

import pytest

import daft
from tests.utils import sort_pydict


@pytest.mark.integration
def test_url_download_gcs_public_special_characters():
    df = daft.from_glob_path("gs://daft-public-data-gs/test_naming/**")
    df = df.with_column("data", df["path"].url.download())

    assert sort_pydict(df.to_pydict(), "path", ascending=True) == {
        "path": [
            "gs://daft-public-data-gs/test_naming/test. .txt",
            "gs://daft-public-data-gs/test_naming/test.%.txt",
            "gs://daft-public-data-gs/test_naming/test.-.txt",
            "gs://daft-public-data-gs/test_naming/test.=.txt",
            "gs://daft-public-data-gs/test_naming/test.?.txt",
        ],
        "size": [5, 5, 5, 5, 5],
        "num_rows": [None, None, None, None, None],
        "data": [b"test\n", b"test\n", b"test\n", b"test\n", b"test\n"],
    }

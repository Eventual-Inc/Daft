from __future__ import annotations

import pytest

import daft


@pytest.mark.integration()
def test_url_download_gcs_public_special_characters(small_images_s3_paths):
    df = daft.from_glob_path("gs://daft-public-data-gs/test_naming/**")
    df = df.with_column("data", df["path"].url.download())

    assert df.to_pydict() == {
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

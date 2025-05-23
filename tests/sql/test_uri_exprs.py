from __future__ import annotations

import os
import tempfile

import daft
from daft import col, lit


def test_url_download():
    df = daft.from_pydict({"one": [1]})  # just have a single row, doesn't matter what it is
    url = "https://daft-public-data.s3.us-west-2.amazonaws.com/test_fixtures/small_images/rickroll0.jpg"

    # download one
    df_actual = daft.sql(f"SELECT url_download('{url}') as downloaded FROM df").collect().to_pydict()
    df_expect = df.select(lit(url).url.download().alias("downloaded")).collect().to_pydict()

    assert df_actual == df_expect


def test_url_download_multi():
    df = daft.from_pydict(
        {
            "urls": [
                "https://daft-public-data.s3.us-west-2.amazonaws.com/test_fixtures/small_images/rickroll0.jpg",
                "https://daft-public-data.s3.us-west-2.amazonaws.com/test_fixtures/small_images/rickroll1.jpg",
            ]
        }
    )

    actual = (
        daft.sql(
            """
        SELECT
            url_download(urls) as downloaded,
            url_download(urls, max_connections=>1) as downloaded_single_conn,
            url_download(urls, on_error=>'null') as downloaded_ignore_errors
        FROM df
        """
        )
        .collect()
        .to_pydict()
    )

    expected = (
        df.select(
            col("urls").url.download().alias("downloaded"),
            col("urls").url.download(max_connections=1).alias("downloaded_single_conn"),
            col("urls").url.download(on_error="null").alias("downloaded_ignore_errors"),
        )
        .collect()
        .to_pydict()
    )

    assert actual == expected


def test_url_upload():
    with tempfile.TemporaryDirectory() as tmp_dir:
        df = daft.from_pydict(
            {
                "data": [b"test1", b"test2"],
                "paths": [
                    os.path.join(tmp_dir, "test1.txt"),
                    os.path.join(tmp_dir, "test2.txt"),
                ],
            }
        )

        actual = (
            daft.sql(
                """
            SELECT
                url_upload(data, paths) as uploaded,
                url_upload(data, paths, max_connections=>1) as uploaded_single_conn,
                url_upload(data, paths, on_error=>'null') as uploaded_ignore_errors
            FROM df
            """
            )
            .collect()
            .to_pydict()
        )

        expected = (
            df.select(
                col("data").url.upload(daft.col("paths")).alias("uploaded"),
                col("data").url.upload(daft.col("paths"), max_connections=1).alias("uploaded_single_conn"),
                col("data").url.upload(daft.col("paths"), on_error="null").alias("uploaded_ignore_errors"),
            )
            .collect()
            .to_pydict()
        )

        assert actual == expected

        # Verify files were created
        assert os.path.exists(os.path.join(tmp_dir, "test1.txt"))
        assert os.path.exists(os.path.join(tmp_dir, "test2.txt"))

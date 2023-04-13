from __future__ import annotations

import pathlib
import uuid

import pandas as pd
import pytest

from daft import DataFrame
from daft.expressions import col
from tests.conftest import assert_df_equals


@pytest.fixture(scope="function")
def files(tmpdir) -> list[str]:
    filepaths = [pathlib.Path(tmpdir) / str(uuid.uuid4()) for _ in range(10)]
    for fp in filepaths:
        fp.write_bytes(fp.name.encode("utf-8"))
    return filepaths


def test_download(files):
    df = DataFrame.from_pydict({"filenames": [str(f) for f in files]})
    df = df.with_column("bytes", col("filenames").url.download())
    pd_df = pd.DataFrame.from_dict({"filenames": [str(f) for f in files]})
    pd_df["bytes"] = pd.Series([pathlib.Path(fn).read_bytes() for fn in files])
    assert_df_equals(df.to_pandas(), pd_df, sort_key="filenames")


def test_download_with_none(files):
    data = {"id": list(range(len(files) * 2)), "filenames": [str(f) for f in files] + [None for _ in range(len(files))]}
    df = DataFrame.from_pydict(data)
    df = df.with_column("bytes", col("filenames").url.download())
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["bytes"] = pd.Series([pathlib.Path(fn).read_bytes() if fn is not None else None for fn in files])
    assert_df_equals(df.to_pandas(), pd_df, sort_key="id")


def test_download_with_broken_urls(files):
    data = {
        "id": list(range(len(files) * 2)),
        "filenames": [str(f) for f in files] + [str(uuid.uuid4()) for _ in range(len(files))],
    }
    df = DataFrame.from_pydict(data)
    df = df.with_column("bytes", col("filenames").url.download())
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["bytes"] = pd.Series([pathlib.Path(fn).read_bytes() if pathlib.Path(fn).exists() else None for fn in files])
    assert_df_equals(df.to_pandas(), pd_df, sort_key="id")


def test_download_with_duplicate_urls(files):
    data = {
        "id": list(range(len(files) * 2)),
        "filenames": [str(f) for f in files] * 2,
    }
    df = DataFrame.from_pydict(data)
    df = df.with_column("bytes", col("filenames").url.download())
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["bytes"] = pd.Series(
        [pathlib.Path(fn).read_bytes() if pathlib.Path(fn).exists() else None for fn in files * 2]
    )
    assert_df_equals(df.to_pandas(), pd_df, sort_key="id")

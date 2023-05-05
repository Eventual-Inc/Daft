from __future__ import annotations

import pathlib
import uuid

import pandas as pd
import pytest

import daft
from daft.expressions import col
from tests.conftest import assert_df_equals


def _get_filename():
    name = str(uuid.uuid4())

    # Inject colons into the name
    name += ":foo:bar"

    return name


@pytest.fixture(scope="function")
def files(tmpdir) -> list[str]:
    filepaths = [pathlib.Path(tmpdir) / _get_filename() for _ in range(10)]
    for fp in filepaths:
        fp.write_bytes(fp.name.encode("utf-8"))
    return filepaths


def test_download(files):
    df = daft.from_pydict({"filenames": [str(f) for f in files]})
    df = df.with_column("bytes", col("filenames").url.download())
    pd_df = pd.DataFrame.from_dict({"filenames": [str(f) for f in files]})
    pd_df["bytes"] = pd.Series([pathlib.Path(fn).read_bytes() for fn in files])
    assert_df_equals(df.to_pandas(), pd_df, sort_key="filenames")


def test_download_with_none(files):
    data = {"id": list(range(len(files) * 2)), "filenames": [str(f) for f in files] + [None for _ in range(len(files))]}
    df = daft.from_pydict(data)
    df = df.with_column("bytes", col("filenames").url.download())
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["bytes"] = pd.Series([pathlib.Path(fn).read_bytes() if fn is not None else None for fn in files])
    assert_df_equals(df.to_pandas(), pd_df, sort_key="id")


def test_download_with_broken_urls(files):
    data = {
        "id": list(range(len(files) * 2)),
        "filenames": [str(f) for f in files] + [str(uuid.uuid4()) for _ in range(len(files))],
    }
    df = daft.from_pydict(data)
    df = df.with_column("bytes", col("filenames").url.download(on_error="null"))
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["bytes"] = pd.Series([pathlib.Path(fn).read_bytes() if pathlib.Path(fn).exists() else None for fn in files])
    assert_df_equals(df.to_pandas(), pd_df, sort_key="id")


def test_download_with_broken_urls_reraise_errors(files):
    data = {
        "id": list(range(len(files) * 2)),
        "filenames": [str(f) for f in files] + [str(uuid.uuid4()) for _ in range(len(files))],
    }
    df = daft.from_pydict(data)
    df = df.with_column("bytes", col("filenames").url.download(on_error="raise"))

    with pytest.raises(FileNotFoundError):
        df.collect()


def test_download_with_duplicate_urls(files):
    data = {
        "id": list(range(len(files) * 2)),
        "filenames": [str(f) for f in files] * 2,
    }
    df = daft.from_pydict(data)
    df = df.with_column("bytes", col("filenames").url.download())
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["bytes"] = pd.Series(
        [pathlib.Path(fn).read_bytes() if pathlib.Path(fn).exists() else None for fn in files * 2]
    )
    assert_df_equals(df.to_pandas(), pd_df, sort_key="id")

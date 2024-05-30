from __future__ import annotations

import pathlib
import sys
import uuid

import pandas as pd
import pytest

import daft
from daft.expressions import col
from tests.conftest import assert_df_equals


def _get_filename():
    name = str(uuid.uuid4())

    # Inject colons into the name if not windows
    if sys.platform != "win32":
        name += ":foo:bar"

    return name


@pytest.fixture(scope="function")
def files(tmpdir) -> list[str]:
    filepaths = [pathlib.Path(tmpdir) / _get_filename() for _ in range(10)]
    for fp in filepaths:
        fp.write_bytes(fp.name.encode("utf-8"))
    return filepaths


@pytest.mark.parametrize("use_native_downloader", [False, True])
def test_download(files, use_native_downloader):
    # Run it twice to ensure runtime works
    for _ in range(2):
        df = daft.from_pydict({"filenames": [str(f) for f in files]})
        df = df.with_column("bytes", col("filenames").url.download(use_native_downloader=use_native_downloader))
        pd_df = pd.DataFrame.from_dict({"filenames": [str(f) for f in files]})
        pd_df["bytes"] = pd.Series([pathlib.Path(fn).read_bytes() for fn in files])
        assert_df_equals(df.to_pandas(), pd_df, sort_key="filenames")


@pytest.mark.parametrize("use_native_downloader", [False, True])
def test_download_with_none(files, use_native_downloader):
    data = {"id": list(range(len(files) * 2)), "filenames": [str(f) for f in files] + [None for _ in range(len(files))]}
    # Run it twice to ensure runtime works
    for _ in range(2):
        df = daft.from_pydict(data)
        df = df.with_column("bytes", col("filenames").url.download(use_native_downloader=use_native_downloader))
        pd_df = pd.DataFrame.from_dict(data)
        pd_df["bytes"] = pd.Series([pathlib.Path(fn).read_bytes() if fn is not None else None for fn in files])
        assert_df_equals(df.to_pandas(), pd_df, sort_key="id")


@pytest.mark.parametrize("use_native_downloader", [False, True])
def test_download_with_missing_urls(files, use_native_downloader):
    data = {
        "id": list(range(len(files) * 2)),
        "filenames": [str(f) for f in files] + [str(uuid.uuid4()) for _ in range(len(files))],
    }
    # Run it twice to ensure runtime works
    for _ in range(2):
        df = daft.from_pydict(data)
        df = df.with_column(
            "bytes", col("filenames").url.download(on_error="null", use_native_downloader=use_native_downloader)
        )
        pd_df = pd.DataFrame.from_dict(data)
        pd_df["bytes"] = pd.Series(
            [pathlib.Path(fn).read_bytes() if pathlib.Path(fn).exists() else None for fn in files]
        )
        assert_df_equals(df.to_pandas(), pd_df, sort_key="id")


@pytest.mark.parametrize("use_native_downloader", [False, True])
def test_download_with_missing_urls_reraise_errors(files, use_native_downloader):
    data = {
        "id": list(range(len(files) * 2)),
        "filenames": [str(f) for f in files] + [str(uuid.uuid4()) for _ in range(len(files))],
    }
    # Run it twice to ensure runtime works
    for _ in range(2):
        df = daft.from_pydict(data)
        df = df.with_column(
            "bytes", col("filenames").url.download(on_error="raise", use_native_downloader=use_native_downloader)
        )
        # TODO: Change to a FileNotFound Error

        if not use_native_downloader:
            with pytest.raises(RuntimeError) as exc_info:
                df.collect()

            # Ray's wrapping of the exception loses information about the `.cause`, but preserves it in the string error message
            if daft.context.get_context().runner_config.name == "ray":
                assert "FileNotFoundError" in str(exc_info.value)
            else:
                assert isinstance(exc_info.value.__cause__, FileNotFoundError)
        else:
            with pytest.raises(FileNotFoundError):
                df.collect()


@pytest.mark.parametrize("use_native_downloader", [False, True])
def test_download_with_duplicate_urls(files, use_native_downloader):
    data = {
        "id": list(range(len(files) * 2)),
        "filenames": [str(f) for f in files] * 2,
    }
    # Run it twice to ensure runtime works
    for _ in range(2):
        df = daft.from_pydict(data)
        df = df.with_column("bytes", col("filenames").url.download(use_native_downloader=use_native_downloader))
        pd_df = pd.DataFrame.from_dict(data)
        pd_df["bytes"] = pd.Series(
            [pathlib.Path(fn).read_bytes() if pathlib.Path(fn).exists() else None for fn in files * 2]
        )
        assert_df_equals(df.to_pandas(), pd_df, sort_key="id")

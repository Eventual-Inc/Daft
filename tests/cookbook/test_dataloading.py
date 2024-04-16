from __future__ import annotations

import os
import pathlib

import pandas as pd

import daft
from tests.conftest import assert_df_equals
from tests.cookbook.assets import COOKBOOK_DATA_CSV


def test_load(daft_df, service_requests_csv_pd_df, repartition_nparts):
    """Loading data from a CSV or Parquet works"""
    pd_slice = service_requests_csv_pd_df
    daft_slice = daft_df.repartition(repartition_nparts)
    daft_pd_df = daft_slice.to_pandas()
    assert_df_equals(daft_pd_df, pd_slice)


def test_load_csv_no_headers(tmp_path: pathlib.Path):
    """Generate a default set of headers `f0, f1, ... f{n}` when loading a CSV that has no headers"""
    csv = tmp_path / "headerless_iris.csv"
    csv.write_text("\n".join(pathlib.Path(COOKBOOK_DATA_CSV).read_text().split("\n")[1:]))
    daft_df = daft.read_csv(str(csv), has_headers=False)
    pd_df = pd.read_csv(csv, header=None, keep_default_na=False)
    pd_df.columns = [f"column_{i}" for i in range(1, 53)]
    daft_pd_df = daft_df.to_pandas()
    assert list(daft_pd_df.columns) == list(pd_df.columns)


def test_load_csv_tab_delimited(tmp_path: pathlib.Path):
    """Generate a default set of headers `col_0, col_1, ... col_{n}` when loading a CSV that has no headers"""
    csv = tmp_path / "headerless_iris.csv"
    csv.write_text(pathlib.Path(COOKBOOK_DATA_CSV).read_text().replace(",", "\t"))
    daft_df = daft.read_csv(str(csv), delimiter="\t")
    pd_df = pd.read_csv(csv, delimiter="\t")
    daft_pd_df = daft_df.to_pandas()
    assert list(daft_pd_df.columns) == list(pd_df.columns)


def test_load_json(tmp_path: pathlib.Path):
    """Generate a default set of headers `col_0, col_1, ... col_{n}` when loading a JSON file"""
    json_file = tmp_path / "iris.json"
    pd_df = pd.read_csv(COOKBOOK_DATA_CSV)
    pd_df.to_json(json_file, lines=True, orient="records")
    daft_df = daft.read_json(str(json_file))
    daft_pd_df = daft_df.to_pandas()
    assert list(daft_pd_df.columns) == list(pd_df.columns)


def test_load_pydict():
    data = {"foo": [1, 2, 3], "bar": [1.0, 2.0, 3.0], "baz": ["a", "b", "c"]}
    daft_df = daft.from_pydict(data)
    pd_df = pd.DataFrame(data)
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, pd_df, sort_key="foo")


def test_load_pylist():
    data = [
        {"foo": 1, "bar": 1.0, "baz": "a"},
        {"foo": 2, "bar": 2.0, "baz": "b"},
        {"foo": 3, "bar": 3.0, "baz": "c"},
    ]
    daft_df = daft.from_pylist(data)
    pd_df = pd.DataFrame.from_records(data)
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, pd_df, sort_key="foo")


def test_glob_files(tmpdir):
    filepaths = []
    for i in range(10):
        filepath = pathlib.Path(tmpdir) / f"file_{i}.foo"
        filepath.write_text("a" * i)
        filepaths.append(filepath)
        bar_filepath = pathlib.Path(tmpdir) / f"file_{i}.bar"
        bar_filepath.write_text("b" * i)

    daft_df = daft.from_glob_path(os.path.join(tmpdir, "*.foo"))
    daft_pd_df = daft_df.to_pandas()

    pd_df = pd.DataFrame.from_records(
        {"path": "file://" + str(path.as_posix()), "size": size, "num_rows": None}
        for path, size in zip(filepaths, list(range(10)))
    )
    pd_df = pd_df[~pd_df["path"].str.endswith(".bar")]
    pd_df = pd_df.astype({"num_rows": float})
    assert_df_equals(daft_pd_df, pd_df, sort_key="path")


def test_glob_files_single_file(tmpdir):
    filepath = pathlib.Path(tmpdir) / "file.foo"
    filepath.write_text("b" * 10)
    daft_df = daft.from_glob_path(os.path.join(tmpdir, "file.foo"))
    daft_pd_df = daft_df.to_pandas()
    pd_df = pd.DataFrame.from_records([{"path": "file://" + str(filepath), "size": 10, "num_rows": None}])
    pd_df = pd_df.astype({"num_rows": float})
    assert_df_equals(daft_pd_df, pd_df, sort_key="path")


def test_glob_files_directory(tmpdir):
    extra_empty_dir = pathlib.Path(tmpdir) / "bar"
    extra_empty_dir.mkdir()
    filepaths = []
    for i in range(10):
        for ext in ("foo", "bar"):
            filepath = pathlib.Path(tmpdir) / f"file_{i}.{ext}"
            filepath.write_text("a" * i)
            filepaths.append(filepath)

    daft_df = daft.from_glob_path(str(tmpdir))
    daft_pd_df = daft_df.to_pandas()

    listing_records = [
        {"path": "file://" + str(path.as_posix()), "size": size, "num_rows": None}
        for path, size in zip(filepaths, [i for i in range(10) for _ in range(2)])
    ]

    listing_records = listing_records
    pd_df = pd.DataFrame.from_records(listing_records)
    pd_df = pd_df.astype({"num_rows": float})
    assert_df_equals(daft_pd_df, pd_df, sort_key="path")


def test_glob_files_recursive(tmpdir):
    nested_dir_path = pathlib.Path(tmpdir) / "bar"
    nested_dir_path.mkdir()
    paths = []
    for i in range(10):
        for prefix in [pathlib.Path(tmpdir), pathlib.Path(tmpdir) / "bar"]:
            filepath = prefix / f"file_{i}.foo"
            filepath.write_text("a" * i)
            paths.append(filepath)

    daft_df = daft.from_glob_path(os.path.join(tmpdir, "**"))
    daft_pd_df = daft_df.to_pandas()
    listing_records = [
        {"path": "file://" + str(path.as_posix()), "size": size, "num_rows": None}
        for path, size in zip(paths, [i for i in range(10) for _ in range(2)])
    ]

    pd_df = pd.DataFrame.from_records(listing_records)
    pd_df = pd_df.astype({"num_rows": float})

    assert_df_equals(daft_pd_df, pd_df, sort_key="path")

from __future__ import annotations

import pathlib

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from daft.dataframe import DataFrame
from daft.expressions import col
from daft.filesystem import get_filesystem_from_path
from daft.types import ExpressionType, PythonExpressionType
from tests.assets.assets import (
    IRIS_CSV,
    SERVICE_REQUESTS_PARQUET,
    SERVICE_REQUESTS_PARQUET_FOLDER,
)
from tests.conftest import assert_df_equals
from tests.dataframe_cookbook.conftest import (
    COLUMNS,
    parametrize_service_requests_csv_repartition,
)


class MyObj:
    pass


class MyObj2:
    pass


@parametrize_service_requests_csv_repartition
def test_load(daft_df, service_requests_csv_pd_df, repartition_nparts):
    """Loading data from a CSV or Parquet works"""
    pd_slice = service_requests_csv_pd_df
    daft_slice = daft_df.repartition(repartition_nparts)
    daft_pd_df = daft_slice.to_pandas()
    assert_df_equals(daft_pd_df, pd_slice)


@pytest.mark.parametrize("parquet_path", [SERVICE_REQUESTS_PARQUET, SERVICE_REQUESTS_PARQUET_FOLDER])
@parametrize_service_requests_csv_repartition
def test_load_parquet(parquet_path, service_requests_csv_pd_df, repartition_nparts):
    """Loading data from a CSV or Parquet works"""
    daft_df = DataFrame.read_parquet(parquet_path).select(*[col(c) for c in COLUMNS])
    pd_slice = service_requests_csv_pd_df
    daft_slice = daft_df.repartition(repartition_nparts)
    daft_pd_df = daft_slice.to_pandas()
    assert_df_equals(daft_pd_df, pd_slice)


def test_load_csv_no_headers(tmp_path: pathlib.Path):
    """Generate a default set of headers `f0, f1, ... f{n}` when loading a CSV that has no headers"""
    csv = tmp_path / "headerless_iris.csv"
    csv.write_text("\n".join(pathlib.Path(IRIS_CSV).read_text().split("\n")[1:]))
    daft_df = DataFrame.read_csv(str(csv), has_headers=False)
    pd_df = pd.read_csv(csv, header=None)
    pd_df.columns = [f"f{i}" for i in range(5)]
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, pd_df, assert_ordering=True)


def test_load_csv_tab_delimited(tmp_path: pathlib.Path):
    """Generate a default set of headers `col_0, col_1, ... col_{n}` when loading a CSV that has no headers"""
    csv = tmp_path / "headerless_iris.csv"
    csv.write_text(pathlib.Path(IRIS_CSV).read_text().replace(",", "\t"))
    daft_df = DataFrame.read_csv(str(csv), delimiter="\t")
    pd_df = pd.read_csv(csv, delimiter="\t")
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, pd_df, assert_ordering=True)


def test_load_json(tmp_path: pathlib.Path):
    """Generate a default set of headers `col_0, col_1, ... col_{n}` when loading a JSON file"""
    json_file = tmp_path / "iris.json"
    pd_df = pd.read_csv(IRIS_CSV)

    # Test that nested types like lists and dicts get loaded as Python objects
    pd_df["dicts"] = pd.Series([{"foo": i} for i in range(len(pd_df))])
    pd_df["lists"] = pd.Series([[1 for _ in range(i)] for i in range(len(pd_df))])

    pd_df.to_json(json_file, lines=True, orient="records")
    daft_df = DataFrame.read_json(str(json_file))

    assert daft_df.schema()["dicts"].dtype == PythonExpressionType(dict)
    assert daft_df.schema()["lists"].dtype == PythonExpressionType(list)

    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, pd_df, assert_ordering=True)


def test_load_pydict():
    data = {
        "arrow_int": [None, 2, 3],
        "arrow_float": [None, 1.0, 3.0],
        "arrow_str": [None, "b", "c"],
        "arrow_struct": [None, {"foo": 1}, {"bar": 1}],
        "arrow_nulls": [None, None, None],
        "arrow_arr_int_all_null": pa.array([None, None, None], type=pa.int64()),
        "arrow_arr_structs": pa.array([None, {"foo": 1}, {"bar": 1}]),
        "py_objs": [None, MyObj(), MyObj()],
        "heterogenous_py_objs": [None, MyObj(), MyObj2()],
        "numpy_arrays": [np.array([1]), np.array([2]), np.array([3])],
    }
    daft_df = DataFrame.from_pydict(data)

    daft_df.collect()
    collected_data = daft_df.to_pydict()

    expected = {
        "arrow_int": ExpressionType.integer(),
        "arrow_float": ExpressionType.float(),
        "arrow_str": ExpressionType.string(),
        "arrow_struct": PythonExpressionType(dict),
        "arrow_nulls": ExpressionType.null(),
        "arrow_arr_int_all_null": ExpressionType.integer(),
        "arrow_arr_structs": PythonExpressionType(dict),
        "py_objs": PythonExpressionType(MyObj),
        "heterogenous_py_objs": ExpressionType.python_object(),
        "numpy_arrays": PythonExpressionType(np.ndarray),
    }

    assert collected_data.keys() == data.keys() == expected.keys()
    for colname, expected_schema_type in expected.items():
        assert daft_df.schema()[colname].dtype == expected_schema_type
        expected_container_type = list if ExpressionType.is_py(expected_schema_type) else pa.ChunkedArray
        assert isinstance(collected_data[colname], expected_container_type)


def test_load_pylist():
    data = [
        {"foo": 1, "bar": 1.0, "baz": "a"},
        {"foo": 2, "bar": 2.0, "baz": "b"},
        {"foo": 3, "bar": 3.0, "baz": "c"},
    ]
    daft_df = DataFrame.from_pylist(data)
    pd_df = pd.DataFrame.from_records(data)
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, pd_df, sort_key="foo")


def test_load_files(tmpdir):
    for i in range(10):
        filepath = pathlib.Path(tmpdir) / f"file_{i}.foo"
        filepath.write_text("a" * i)
        filepath = pathlib.Path(tmpdir) / f"file_{i}.bar"
        filepath.write_text("b" * i)

    daft_df = DataFrame.from_files(f"{tmpdir}/*.foo")
    daft_pd_df = daft_df.to_pandas()
    pd_df = pd.DataFrame.from_records(get_filesystem_from_path(str(tmpdir)).ls(str(tmpdir), detail=True))
    pd_df = pd_df[~pd_df["name"].str.endswith(".bar")]
    assert_df_equals(daft_pd_df, pd_df, sort_key="name")


def test_glob_files(tmpdir):
    filepaths = []
    for i in range(10):
        filepath = pathlib.Path(tmpdir) / f"file_{i}.foo"
        filepath.write_text("a" * i)
        filepaths.append(filepath)
        bar_filepath = pathlib.Path(tmpdir) / f"file_{i}.bar"
        bar_filepath.write_text("b" * i)

    daft_df = DataFrame.from_glob_path(f"{tmpdir}/*.foo")
    daft_pd_df = daft_df.to_pandas()
    pd_df = pd.DataFrame.from_records(
        {"path": str(path), "size": size, "type": "file"} for path, size in zip(filepaths, list(range(10)))
    )
    pd_df = pd_df[~pd_df["path"].str.endswith(".bar")]
    assert_df_equals(daft_pd_df, pd_df, sort_key="path")


def test_glob_files_single_file(tmpdir):
    filepath = pathlib.Path(tmpdir) / f"file.foo"
    filepath.write_text("b" * 10)
    daft_df = DataFrame.from_glob_path(f"{tmpdir}/file.foo")
    daft_pd_df = daft_df.to_pandas()
    pd_df = pd.DataFrame.from_records([{"path": str(filepath), "size": 10, "type": "file"}])
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

    daft_df = DataFrame.from_glob_path(str(tmpdir))
    daft_pd_df = daft_df.to_pandas()

    listing_records = [
        {"path": str(path), "size": size, "type": "file"}
        for path, size in zip(filepaths, [i for i in range(10) for _ in range(2)])
    ]
    listing_records = listing_records + [
        {"path": str(extra_empty_dir), "size": extra_empty_dir.stat().st_size, "type": "directory"}
    ]
    pd_df = pd.DataFrame.from_records(listing_records)

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

    daft_df = DataFrame.from_glob_path(f"{tmpdir}/**")
    daft_pd_df = daft_df.to_pandas()

    listing_records = [
        {"path": str(path), "size": size, "type": "file"}
        for path, size in zip(paths, [i for i in range(10) for _ in range(2)])
    ]
    listing_records = listing_records + [
        {"path": str(nested_dir_path), "size": nested_dir_path.stat().st_size, "type": "directory"}
    ]
    pd_df = pd.DataFrame.from_records(listing_records)

    assert_df_equals(daft_pd_df, pd_df, sort_key="path")

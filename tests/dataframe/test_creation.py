from __future__ import annotations

import contextlib
import csv
import decimal
import json
import os
import tempfile
import uuid
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as papq
import pytest
from fsspec.implementations.local import LocalFileSystem
from ray.data.extensions import ArrowTensorArray, TensorArray

import daft
from daft.api_annotations import APITypeError
from daft.context import get_context
from daft.dataframe import DataFrame
from daft.datatype import DataType
from daft.utils import pyarrow_supports_fixed_shape_tensor
from tests.conftest import *

ARROW_VERSION = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric())


class MyObj:
    pass


class MyObj2:
    pass


class MyObjWithValue:
    def __init__(self, value: int):
        self.value = value

    def __eq__(self, other: MyObjWithValue):
        return isinstance(other, MyObjWithValue) and self.value == other.value


COL_NAMES = [
    "sepal_length",
    "sepal_width",
    "petal_length",
    "petal_width",
    "variety",
]


@pytest.mark.parametrize("read_method", ["read_csv", "read_json", "read_parquet"])
def test_load_missing(read_method):
    """Loading data from a missing filepath"""
    with pytest.raises(FileNotFoundError):
        getattr(daft, read_method)(str(uuid.uuid4()))


@pytest.mark.parametrize("data", [{"foo": [1, 2, 3]}, [{"foo": i} for i in range(3)], "foo"])
def test_error_thrown_create_dataframe_constructor(data) -> None:
    with pytest.raises(ValueError):
        DataFrame(data)


def test_wrong_input_type():
    with pytest.raises(APITypeError):
        daft.from_pydict("invalid input")


def test_create_dataframe_empty_list() -> None:
    with pytest.raises(ValueError):
        daft.read_parquet([])
    with pytest.raises(ValueError):
        daft.read_csv([])
    with pytest.raises(ValueError):
        daft.read_json([])


###
# List tests
###


def test_create_dataframe_list(valid_data: list[dict[str, float]]) -> None:
    df = daft.from_pylist(valid_data)
    assert set(df.column_names) == set(COL_NAMES)


def test_create_dataframe_list_empty() -> None:
    df = daft.from_pylist([])
    assert df.column_names == []


def test_create_dataframe_list_ragged_keys() -> None:
    df = daft.from_pylist(
        [
            {"foo": 1},
            {"foo": 2, "bar": 1},
            {"foo": 3, "bar": 2, "baz": 1},
        ]
    )
    assert df.to_pydict() == {
        "foo": [1, 2, 3],
        "bar": [None, 1, 2],
        "baz": [None, None, 1],
    }


def test_create_dataframe_list_empty_dicts() -> None:
    df = daft.from_pylist([{}, {}, {}])
    assert df.column_names == []


def test_create_dataframe_list_non_dicts() -> None:
    with pytest.raises(ValueError) as e:
        daft.from_pylist([1, 2, 3])
    assert "Expected list of dictionaries of {column_name: value}" in str(e.value)


###
# Dict tests
###


def test_create_dataframe_pydict(valid_data: list[dict[str, float]]) -> None:
    pydict = {k: [item[k] for item in valid_data] for k in valid_data[0].keys()}
    df = daft.from_pydict(pydict)
    assert set(df.column_names) == set(COL_NAMES)


def test_create_dataframe_empty_pydict() -> None:
    df = daft.from_pydict({})
    assert df.column_names == []


def test_create_dataframe_pydict_ragged_col_lens() -> None:
    with pytest.raises(ValueError) as e:
        daft.from_pydict({"foo": [1, 2], "bar": [1, 2, 3]})
    assert "Expected all columns to be of the same length" in str(e.value)


def test_create_dataframe_pydict_bad_columns() -> None:
    with pytest.raises(ValueError) as e:
        daft.from_pydict({"foo": "somestring"})
    assert "Creating a Series from data of type" in str(e.value)


###
# Arrow tests
###


@pytest.mark.parametrize("multiple", [False, True])
def test_create_dataframe_arrow(valid_data: list[dict[str, float]], multiple) -> None:
    t = pa.Table.from_pydict({k: [valid_data[i][k] for i in range(len(valid_data))] for k in valid_data[0].keys()})
    if multiple:
        t = [t, t, t]
    df = daft.from_arrow(t)
    if multiple:
        t = pa.concat_tables(t)
    assert set(df.column_names) == set(t.column_names)
    casted_field = t.schema.field("variety").with_type(pa.large_string())
    expected = t.cast(t.schema.set(t.schema.get_field_index("variety"), casted_field))
    # Check roundtrip.
    assert df.to_arrow() == expected


def test_create_dataframe_arrow_tensor_ray(valid_data: list[dict[str, float]]) -> None:
    pydict = {k: [item[k] for item in valid_data] for k in valid_data[0].keys()}
    shape = (2, 2)
    arr = np.ones((len(valid_data),) + shape)
    ata = ArrowTensorArray.from_numpy(arr)
    pydict["tensor"] = ata
    t = pa.Table.from_pydict(pydict)
    df = daft.from_arrow(t)
    assert set(df.column_names) == set(t.column_names)
    # Tensor type should be inferred.
    expected_tensor_dtype = DataType.tensor(DataType.float64(), shape)
    assert df.schema()["tensor"].dtype == expected_tensor_dtype
    casted_variety = t.schema.field("variety").with_type(pa.large_string())
    schema = t.schema.set(t.schema.get_field_index("variety"), casted_variety)
    expected = t.cast(schema)
    # Check roundtrip.
    assert df.to_arrow(True) == expected


@pytest.mark.skipif(
    not pyarrow_supports_fixed_shape_tensor(),
    reason=f"Arrow version {ARROW_VERSION} doesn't support the canonical tensor extension type.",
)
def test_create_dataframe_arrow_tensor_canonical(valid_data: list[dict[str, float]]) -> None:
    pydict = {k: [item[k] for item in valid_data] for k in valid_data[0].keys()}
    shape = (2, 2)
    dtype = pa.fixed_shape_tensor(pa.int64(), shape)
    storage = pa.array([list(range(4 * i, 4 * (i + 1))) for i in range(len(valid_data))], pa.list_(pa.int64(), 4))
    fst = pa.ExtensionArray.from_storage(dtype, storage)
    pydict["tensor"] = fst
    t = pa.Table.from_pydict(pydict)
    df = daft.from_arrow(t)
    assert set(df.column_names) == set(t.column_names)
    assert df.schema()["tensor"].dtype == DataType.tensor(DataType.int64(), shape)
    casted_field = t.schema.field("variety").with_type(pa.large_string())
    expected = t.cast(t.schema.set(t.schema.get_field_index("variety"), casted_field))
    # Check roundtrip.
    assert df.to_arrow() == expected


def test_create_dataframe_arrow_extension_type(valid_data: list[dict[str, float]], uuid_ext_type: UuidType) -> None:
    pydict = {k: [item[k] for item in valid_data] for k in valid_data[0].keys()}
    storage = pa.array([f"{i}".encode() for i in range(len(valid_data))])
    pydict["obj"] = pa.ExtensionArray.from_storage(uuid_ext_type, storage)
    t = pa.Table.from_pydict(pydict)
    df = daft.from_arrow(t)
    assert set(df.column_names) == set(t.column_names)
    assert df.schema()["obj"].dtype == DataType.extension(
        uuid_ext_type.NAME, DataType.from_arrow_type(uuid_ext_type.storage_type), ""
    )
    casted_field = t.schema.field("variety").with_type(pa.large_string())
    expected = t.cast(t.schema.set(t.schema.get_field_index("variety"), casted_field))
    # Check roundtrip.
    assert df.to_arrow() == expected


class PyExtType(pa.PyExtensionType):
    def __init__(self):
        pa.PyExtensionType.__init__(self, pa.binary())

    def __reduce__(self):
        return PyExtType, ()


def test_create_dataframe_arrow_py_ext_type_raises(valid_data: list[dict[str, float]]) -> None:
    pydict = {k: [item[k] for item in valid_data] for k in valid_data[0].keys()}
    uuid_type = PyExtType()
    storage_array = pa.array([f"foo-{i}".encode() for i in range(len(valid_data))], pa.binary())
    arr = pa.ExtensionArray.from_storage(uuid_type, storage_array)
    pydict["obj"] = arr
    t = pa.Table.from_pydict(pydict)
    with pytest.raises(ValueError):
        daft.from_arrow(t)


def test_create_dataframe_arrow_unsupported_dtype(valid_data: list[dict[str, float]]) -> None:
    pydict = {k: [item[k] for item in valid_data] for k in valid_data[0].keys()}
    pydict["obj"] = [
        decimal.Decimal("12456789012345678901234567890123456789012345678901234567890") for _ in range(len(valid_data))
    ]
    t = pa.Table.from_pydict(pydict)
    df = daft.from_arrow(t)
    assert set(df.column_names) == set(t.column_names)
    # Type not natively supported, so should have Python object dtype.
    assert df.schema()["obj"].dtype == DataType.python()
    casted_field = t.schema.field("variety").with_type(pa.large_string())
    expected = t.cast(t.schema.set(t.schema.get_field_index("variety"), casted_field))
    # Check roundtrip.
    assert df.to_arrow() == expected


###
# Pandas tests
###


@pytest.mark.parametrize("multiple", [False, True])
def test_create_dataframe_pandas(valid_data: list[dict[str, float]], multiple) -> None:
    pd_df = pd.DataFrame(valid_data)
    if multiple:
        pd_df = [pd_df, pd_df, pd_df]
    df = daft.from_pandas(pd_df)
    if multiple:
        pd_df = pd.concat(pd_df).reset_index(drop=True)
    assert set(df.column_names) == set(pd_df.columns)
    # Check roundtrip.
    pd.testing.assert_frame_equal(df.to_pandas(), pd_df)


def test_create_dataframe_pandas_py_object(valid_data: list[dict[str, float]]) -> None:
    pydict = {k: [item[k] for item in valid_data] for k in valid_data[0].keys()}
    pydict["obj"] = [MyObjWithValue(i) for i in range(len(valid_data))]
    pd_df = pd.DataFrame(pydict)
    df = daft.from_pandas(pd_df)
    # Type not natively supported, so should have Python object dtype.
    assert df.schema()["obj"].dtype == DataType.python()
    assert set(df.column_names) == set(pd_df.columns)
    # Check roundtrip.
    pd.testing.assert_frame_equal(df.to_pandas(), pd_df)


def test_create_dataframe_pandas_tensor(valid_data: list[dict[str, float]]) -> None:
    pydict = {k: [item[k] for item in valid_data] for k in valid_data[0].keys()}
    shape = (2, 2)
    pydict["tensor"] = TensorArray(np.ones((len(valid_data),) + shape))
    pd_df = pd.DataFrame(pydict)
    df = daft.from_pandas(pd_df)
    assert df.schema()["tensor"].dtype == DataType.tensor(DataType.float64(), shape)
    assert set(df.column_names) == set(pd_df.columns)
    # Check roundtrip.
    pd.testing.assert_frame_equal(df.to_pandas(cast_tensors_to_ray_tensor_dtype=True), pd_df)


@pytest.mark.parametrize(
    ["data", "expected_dtype"],
    [
        pytest.param([None, 2, 3], DataType.int64(), id="arrow_int"),
        pytest.param([None, 1.0, 3.0], DataType.float64(), id="arrow_float"),
        pytest.param([None, 1.0, 3], DataType.float64(), id="arrow_mixed_numbers"),
        pytest.param([None, "b", "c"], DataType.string(), id="arrow_str"),
        pytest.param([None, None, None], DataType.null(), id="arrow_nulls"),
        pytest.param(np.array([1, 2, 3], dtype=np.int64), DataType.int64(), id="np_int"),
        pytest.param(np.array([None, "foo", "bar"], dtype=np.object_), DataType.string(), id="np_string"),
        pytest.param(pa.array([1, 2, 3]), DataType.int64(), id="pa_int"),
        pytest.param(pa.chunked_array([pa.array([1, 2, 3])]), DataType.int64(), id="pa_int_chunked"),
        pytest.param([None, MyObj(), MyObj()], DataType.python(), id="py_objs"),
        pytest.param([None, MyObj(), MyObj2()], DataType.python(), id="heterogenous_py_objs"),
        pytest.param(np.array([None, MyObj(), MyObj()], dtype=np.object_), DataType.python(), id="np_object"),
        pytest.param(
            [None, {"foo": 1}, {"bar": 1}],
            DataType.struct({"bar": DataType.int64(), "foo": DataType.int64()}),
            id="arrow_struct",
        ),
        pytest.param(
            [np.array([1], dtype=np.int64), np.array([2], dtype=np.int64), np.array([3], dtype=np.int64)],
            DataType.list(DataType.int64()),
            id="numpy_1d_arrays",
        ),
        pytest.param(pa.array([[1, 2, 3], [1, 2], [1]]), DataType.list(DataType.int64()), id="pa_nested"),
        pytest.param(
            pa.chunked_array([pa.array([[1, 2, 3], [1, 2], [1]])]),
            DataType.list(DataType.int64()),
            id="pa_nested_chunked",
        ),
        pytest.param(np.ones((3, 3)), DataType.list(DataType.float64()), id="np_nested_1d"),
        pytest.param(np.ones((3, 3, 3)), DataType.tensor(DataType.float64()), id="np_nested_nd"),
    ],
)
def test_load_pydict_types(data, expected_dtype):
    data_dict = {"x": data}
    daft_df = daft.from_pydict(data_dict)
    daft_df.collect()
    collected_data = daft_df.to_pydict()

    assert list(collected_data.keys()) == ["x"]
    assert daft_df.schema()["x"].dtype == expected_dtype


###
# CSV tests
###
@contextlib.contextmanager
def create_temp_filename() -> str:
    with tempfile.TemporaryDirectory() as dir:
        yield os.path.join(dir, "tempfile")


@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_create_dataframe_csv(valid_data: list[dict[str, float]], use_native_downloader) -> None:
    with create_temp_filename() as fname:
        with open(fname, "w") as f:
            header = list(valid_data[0].keys())
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows([[item[col] for col in header] for item in valid_data])
            f.flush()

        df = daft.read_csv(fname, use_native_downloader=use_native_downloader)
        assert df.column_names == COL_NAMES

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == COL_NAMES
        assert len(pd_df) == len(valid_data)


@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_create_dataframe_multiple_csvs(valid_data: list[dict[str, float]], use_native_downloader) -> None:
    with create_temp_filename() as f1name, create_temp_filename() as f2name:
        with open(f1name, "w") as f1, open(f2name, "w") as f2:
            for f in (f1, f2):
                header = list(valid_data[0].keys())
                writer = csv.writer(f)
                writer.writerow(header)
                writer.writerows([[item[col] for col in header] for item in valid_data])
                f.flush()

        df = daft.read_csv([f1name, f2name], use_native_downloader=use_native_downloader)
        assert df.column_names == COL_NAMES

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == COL_NAMES
        assert len(pd_df) == (len(valid_data) * 2)


@pytest.mark.skipif(
    get_context().runner_config.name not in {"py"},
    reason="requires PyRunner to be in use",
)
def test_create_dataframe_csv_custom_fs(valid_data: list[dict[str, float]]) -> None:
    with create_temp_filename() as fname:
        with open(fname, "w") as f:
            header = list(valid_data[0].keys())
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows([[item[col] for col in header] for item in valid_data])
            f.flush()

        # Mark that this filesystem instance shouldn't be automatically reused by fsspec; without this,
        # fsspec would cache this instance and reuse it for Daft's default construction of filesystems,
        # which would make this test pass without the passed filesystem being used.
        fs = LocalFileSystem(skip_instance_cache=True)
        mock_cache = MagicMock(return_value=None)
        with patch.object(fs, "info", wraps=fs.info) as mock_info, patch.object(
            fs, "open", wraps=fs.open
        ) as mock_open, patch("daft.filesystem._get_fs_from_cache", mock_cache):
            df = daft.read_csv(fname, fs=fs)
            # Check that info() is called on the passed filesystem.
            mock_info.assert_called()

            assert df.column_names == COL_NAMES
            pd_df = df.to_pandas()

            # Check that open() is called on the passed filesystem.
            mock_open.assert_called()

        assert list(pd_df.columns) == COL_NAMES
        assert len(pd_df) == len(valid_data)


@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_create_dataframe_csv_generate_headers(valid_data: list[dict[str, float]], use_native_downloader) -> None:
    with create_temp_filename() as fname:
        with open(fname, "w") as f:
            header = list(valid_data[0].keys())
            writer = csv.writer(f)
            writer.writerows([[item[col] for col in header] for item in valid_data])
            f.flush()

        cnames = [f"column_{i}" for i in range(1, 6)] if use_native_downloader else [f"f{i}" for i in range(5)]
        df = daft.read_csv(fname, has_headers=False, use_native_downloader=use_native_downloader)
        assert df.column_names == cnames

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == cnames
        assert len(pd_df) == len(valid_data)


@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_create_dataframe_csv_column_projection(valid_data: list[dict[str, float]], use_native_downloader) -> None:
    with create_temp_filename() as fname:
        with open(fname, "w") as f:
            header = list(valid_data[0].keys())
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows([[item[col] for col in header] for item in valid_data])
            f.flush()

        col_subset = COL_NAMES[:3]

        df = daft.read_csv(fname)
        df = df.select(*col_subset)
        assert df.column_names == col_subset

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == col_subset
        assert len(pd_df) == len(valid_data)


@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_create_dataframe_csv_custom_delimiter(valid_data: list[dict[str, float]], use_native_downloader) -> None:
    with create_temp_filename() as fname:
        with open(fname, "w") as f:
            header = list(valid_data[0].keys())
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(header)
            writer.writerows([[item[col] for col in header] for item in valid_data])
            f.flush()

        df = daft.read_csv(fname, delimiter="\t", use_native_downloader=use_native_downloader)
        assert df.column_names == COL_NAMES

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == COL_NAMES
        assert len(pd_df) == len(valid_data)


@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_create_dataframe_csv_specify_schema(valid_data: list[dict[str, float]], use_native_downloader) -> None:
    with create_temp_filename() as fname:
        with open(fname, "w") as f:
            header = list(valid_data[0].keys())
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(header)
            writer.writerows([[item[col] for col in header] for item in valid_data])
            f.flush()

        df = daft.read_csv(
            fname,
            delimiter="\t",
            schema_hints={
                "sepal_length": DataType.float32(),
                "sepal_width": DataType.float32(),
                "petal_length": DataType.float32(),
                "petal_width": DataType.float32(),
                "variety": DataType.string(),
            },
            use_native_downloader=use_native_downloader,
        )
        assert df.column_names == COL_NAMES

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == COL_NAMES
        assert len(pd_df) == len(valid_data)


@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_create_dataframe_csv_specify_schema_no_headers(
    valid_data: list[dict[str, float]], use_native_downloader
) -> None:
    with create_temp_filename() as fname:
        with open(fname, "w") as f:
            header = list(valid_data[0].keys())
            writer = csv.writer(f, delimiter="\t")
            writer.writerows([[item[col] for col in header] for item in valid_data])
            f.flush()

        df = daft.read_csv(
            fname,
            delimiter="\t",
            schema_hints={
                "sepal_length": DataType.float64(),
                "sepal_width": DataType.float64(),
                "petal_length": DataType.float64(),
                "petal_width": DataType.float64(),
                "variety": DataType.string(),
            },
            has_headers=False,
            use_native_downloader=use_native_downloader,
        )
        assert df.column_names == COL_NAMES

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == COL_NAMES
        assert len(pd_df) == len(valid_data)


###
# JSON tests
###


def test_create_dataframe_json(valid_data: list[dict[str, float]]) -> None:
    with create_temp_filename() as fname:
        with open(fname, "w") as f:
            for data in valid_data:
                f.write(json.dumps(data))
                f.write("\n")
            f.flush()

        df = daft.read_json(fname)
        assert df.column_names == COL_NAMES

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == COL_NAMES
        assert len(pd_df) == len(valid_data)


def test_create_dataframe_multiple_jsons(valid_data: list[dict[str, float]]) -> None:
    with create_temp_filename() as f1name, create_temp_filename() as f2name:
        with open(f1name, "w") as f1, open(f2name, "w") as f2:
            for f in (f1, f2):
                for data in valid_data:
                    f.write(json.dumps(data))
                    f.write("\n")
                f.flush()

        df = daft.read_json([f1name, f2name])
        assert df.column_names == COL_NAMES

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == COL_NAMES
        assert len(pd_df) == (len(valid_data) * 2)


@pytest.mark.skipif(
    get_context().runner_config.name not in {"py"},
    reason="requires PyRunner to be in use",
)
def test_create_dataframe_json_custom_fs(valid_data: list[dict[str, float]]) -> None:
    with create_temp_filename() as fname:
        with open(fname, "w") as f:
            for data in valid_data:
                f.write(json.dumps(data))
                f.write("\n")
            f.flush()

        # Mark that this filesystem instance shouldn't be automatically reused by fsspec; without this,
        # fsspec would cache this instance and reuse it for Daft's default construction of filesystems,
        # which would make this test pass without the passed filesystem being used.
        fs = LocalFileSystem(skip_instance_cache=True)
        mock_cache = MagicMock(return_value=None)
        with patch.object(fs, "info", wraps=fs.info) as mock_info, patch.object(
            fs, "open", wraps=fs.open
        ) as mock_open, patch("daft.filesystem._get_fs_from_cache", mock_cache):
            df = daft.read_json(fname, fs=fs)

            # Check that info() is called on the passed filesystem.
            mock_info.assert_called()

            assert df.column_names == COL_NAMES
            pd_df = df.to_pandas()

            # Check that open() is called on the passed filesystem.
            mock_open.assert_called()

        assert list(pd_df.columns) == COL_NAMES
        assert len(pd_df) == len(valid_data)


def test_create_dataframe_json_column_projection(valid_data: list[dict[str, float]]) -> None:
    with create_temp_filename() as fname:
        with open(fname, "w") as f:
            for data in valid_data:
                f.write(json.dumps(data))
                f.write("\n")
            f.flush()

        col_subset = COL_NAMES[:3]

        df = daft.read_json(fname)
        df = df.select(*col_subset)
        assert df.column_names == col_subset

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == col_subset
        assert len(pd_df) == len(valid_data)


def test_create_dataframe_json_https() -> None:
    df = daft.read_json("https://github.com/Eventual-Inc/mnist-json/raw/master/mnist_handwritten_test.json.gz")
    df.collect()
    assert set(df.column_names) == {"label", "image"}
    assert len(df) == 10000


def test_create_dataframe_json_specify_schema(valid_data: list[dict[str, float]]) -> None:
    with create_temp_filename() as fname:
        with open(fname, "w") as f:
            for data in valid_data:
                f.write(json.dumps(data))
                f.write("\n")
            f.flush()

        df = daft.read_json(
            fname,
            schema_hints={
                "sepal_length": DataType.float32(),
                "sepal_width": DataType.float32(),
                "petal_length": DataType.float32(),
                "petal_width": DataType.float32(),
                "variety": DataType.string(),
            },
        )
        assert df.column_names == COL_NAMES

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == COL_NAMES
        assert len(pd_df) == len(valid_data)


###
# Parquet tests
###


@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_create_dataframe_parquet(valid_data: list[dict[str, float]], use_native_downloader) -> None:
    with create_temp_filename() as fname:
        with open(fname, "w") as f:
            table = pa.Table.from_pydict({col: [d[col] for d in valid_data] for col in COL_NAMES})
            papq.write_table(table, f.name)
            f.flush()

        df = daft.read_parquet(fname, use_native_downloader=use_native_downloader)
        assert df.column_names == COL_NAMES

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == COL_NAMES
        assert len(pd_df) == len(valid_data)


@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_create_dataframe_parquet_with_filter(valid_data: list[dict[str, float]], use_native_downloader) -> None:
    with create_temp_filename() as fname:
        with open(fname, "w") as f:
            table = pa.Table.from_pydict({col: [d[col] for d in valid_data] for col in COL_NAMES})
            papq.write_table(table, f.name)
            f.flush()

        df = daft.read_parquet(fname, use_native_downloader=use_native_downloader)
        assert df.column_names == COL_NAMES

        df = df.where(daft.col("sepal_length") > 4.8)

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == COL_NAMES
        assert len(pd_df) == len(valid_data) - 1


@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_create_dataframe_multiple_parquets(valid_data: list[dict[str, float]], use_native_downloader) -> None:
    with create_temp_filename() as f1name, create_temp_filename() as f2name:
        with open(f1name, "w") as f1, open(f2name, "w") as f2:
            for f in (f1, f2):
                table = pa.Table.from_pydict({col: [d[col] for d in valid_data] for col in COL_NAMES})
                papq.write_table(table, f.name)
                f.flush()

        df = daft.read_parquet([f1name, f2name], use_native_downloader=use_native_downloader)
        assert df.column_names == COL_NAMES

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == COL_NAMES
        assert len(pd_df) == (len(valid_data) * 2)


@pytest.mark.skipif(
    get_context().runner_config.name not in {"py"},
    reason="requires PyRunner to be in use",
)
def test_create_dataframe_parquet_custom_fs(valid_data: list[dict[str, float]]) -> None:
    with create_temp_filename() as fname:
        with open(fname, "w") as f:
            table = pa.Table.from_pydict({col: [d[col] for d in valid_data] for col in COL_NAMES})
            papq.write_table(table, f.name)
            f.flush()

        # Mark that this filesystem instance shouldn't be automatically reused by fsspec; without this,
        # fsspec would cache this instance and reuse it for Daft's default construction of filesystems,
        # which would make this test pass without the passed filesystem being used.
        fs = LocalFileSystem(skip_instance_cache=True)
        mock_cache = MagicMock(return_value=None)
        with patch.object(fs, "info", wraps=fs.info) as mock_info, patch.object(
            fs, "open", wraps=fs.open
        ) as mock_open, patch("daft.filesystem._get_fs_from_cache", mock_cache):
            df = daft.read_parquet(fname, fs=fs)

            # Check that info() is called on the passed filesystem.
            mock_info.assert_called()

            assert df.column_names == COL_NAMES
            pd_df = df.to_pandas()

            # Check that open() is called on the passed filesystem.
            mock_open.assert_called()

        assert list(pd_df.columns) == COL_NAMES
        assert len(pd_df) == len(valid_data)


@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_create_dataframe_parquet_column_projection(valid_data: list[dict[str, float]], use_native_downloader) -> None:
    with create_temp_filename() as fname:
        with open(fname, "w") as f:
            table = pa.Table.from_pydict({col: [d[col] for d in valid_data] for col in COL_NAMES})
            papq.write_table(table, fname)
            f.flush()

        col_subset = COL_NAMES[:3]

        df = daft.read_parquet(fname, use_native_downloader=use_native_downloader)
        df = df.select(*col_subset)
        assert df.column_names == col_subset

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == col_subset
        assert len(pd_df) == len(valid_data)


@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_create_dataframe_parquet_specify_schema(valid_data: list[dict[str, float]], use_native_downloader) -> None:
    with create_temp_filename() as fname:
        with open(fname, "w") as f:
            table = pa.Table.from_pydict({col: [d[col] for d in valid_data] for col in COL_NAMES})
            papq.write_table(table, fname)
            f.flush()

        df = daft.read_parquet(
            fname,
            schema_hints={
                "sepal_length": DataType.float64(),
                "sepal_width": DataType.float64(),
                "petal_length": DataType.float64(),
                "petal_width": DataType.float64(),
                "variety": DataType.string(),
            },
            use_native_downloader=use_native_downloader,
        )
        assert df.column_names == COL_NAMES

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == COL_NAMES
        assert len(pd_df) == len(valid_data)

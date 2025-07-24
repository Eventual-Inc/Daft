from __future__ import annotations

import datetime
import decimal
from functools import partial
from pathlib import Path
from typing import Callable, Literal

import numpy as np
import pyarrow as pa
import pytest

import daft
from daft import DataType, Series, TimeUnit
from tests.utils import random_numerical_embedding

FMT = Literal["parquet", "lance", "json", "csv"]

PYARROW_GE_8_0_0: bool = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) >= (8, 0, 0)

PYARROW_GE_11_0_0: bool = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) >= (11, 0, 0)


@pytest.mark.parametrize("fmt", ["parquet", "lance", "json", "csv"])
@pytest.mark.parametrize(
    ["data", "pa_type", "expected_dtype"],
    [
        # TODO [mg]: CSV export erases the 2 lists and drops the None (null)
        # E         foo: [[1,2,null],[1,2,null]] | after: pyarrow.Table
        # E         foo: int64
        # E         ----
        # E         foo: [[1,2,1,2]]
        ([1, 2, None], pa.int64(), DataType.int64()),
        # TODO [mg]: CSV export erases the 2 lists and drops the None (null)
        # E       AssertionError: (Arrow) before: pyarrow.Table
        # E         foo: large_string
        # E         ----
        # E         foo: [["a","b",null],["a","b",null]] | after: pyarrow.Table
        # E         foo: large_string
        # E         ----
        # E         foo: [["a","b","a","b"]]
        # E       assert pyarrow.Table\nfoo: large_string\n----\nfoo: [["a","b",null],["a","b",null]] == pyarrow.Table\nfoo: large_string\n----\nfoo: [["a","b","a","b"]]
        # E         Full diff:
        # E           pyarrow.Table
        # E           foo: large_string
        # E           ----
        # E         - foo: [["a","b","a","b"],
        # E         + foo: [["a","b",null],["a","b",null],
        (["a", "b", None], pa.large_string(), DataType.string()),
        ([True, False, None], pa.bool_(), DataType.bool()),
        ([b"a", b"b", None], pa.large_binary(), DataType.binary()),
        ([None, None, None], pa.null(), DataType.null()),
        # TODO [mg]: JSON write-read messes this up:
        # AssertionError: (Schema) after[foo]: Float64 | expected: Decimal(precision=16, scale=8)
        ([decimal.Decimal("1.23"), decimal.Decimal("1.24"), None], pa.decimal128(16, 8), DataType.decimal128(16, 8)),
        ([datetime.date(1994, 1, 1), datetime.date(1995, 1, 1), None], pa.date32(), DataType.date()),
        # TODO [mg]: JSON write-read messes this up:
        # AssertionError: (Schema) after[foo]: Timestamp(Seconds, None) | expected: Timestamp(Milliseconds, None)
        (
            [datetime.time(12, 1, 22, 4), datetime.time(13, 8, 45, 34), None],
            pa.time64("us"),
            DataType.time(TimeUnit.us()),
        ),
        # TODO [mg]: JSON write-read messes this up:
        # AssertionError: (Schema) after[foo]: Time(Microseconds) | expected: Time(Nanoseconds)
        (
            [datetime.time(12, 1, 22, 4), datetime.time(13, 8, 45, 34), None],
            pa.time64("ns"),
            DataType.time(TimeUnit.ns()),
        ),
        # TODO [mg]: JSON write-read messes this up:
        # AssertionError: (Schema) after[foo]: Timestamp(Seconds, None) | expected: Timestamp(Milliseconds, None)
        (
            [datetime.datetime(1994, 1, 1), datetime.datetime(1995, 1, 1), None],
            pa.timestamp("ms"),
            DataType.timestamp(TimeUnit.ms()),
        ),
        ([datetime.date(1994, 1, 1), datetime.date(1995, 1, 1), None], pa.date64(), DataType.timestamp(TimeUnit.ms())),
        # TODO [mg]: JSON write-read messes this up:
        # AssertionError: (Schema) after[foo]: Timestamp(Seconds, None) | expected: Timestamp(Milliseconds, None)
        (
            [datetime.timedelta(days=1), datetime.timedelta(days=2), None],
            pa.duration("ms"),
            DataType.duration(TimeUnit.ms()),
        ),
        ([[1, 2, 3], [], None], pa.large_list(pa.int64()), DataType.list(DataType.int64())),
        # TODO [mg]: Crashes when parsing fixed size lists
        # Parquet **and** Lance changes the schema (visible on read) from FixedSizeList[Int64; 3] into List[Int64]
        #   => Looks like Daft is interpreting this list as variable size.
        #      Maybe because the existing machinery doesn't look at list_size ?
        #      If that was unspecified, then list[int64] would be right. But since
        #      that is here, it should get the fixed length type and reject writing
        #      if the length of everything isn't the exact `list_size`.
        #   => Maybe the `None` is tripping it up? It should infer this as an optional / nullable type!
        #          -> No, removing this last None still results in same failure.
        (
            [[1, 2, 3], [4, 5, 6], None],
            pa.list_(pa.int64(), list_size=3),
            DataType.fixed_size_list(DataType.int64(), 3),
        ),
        #
        # TODO [mg]: Lance messes up the schema on read:
        #   before: [{'foo': {'bar': 1}}, {'foo': {'bar': None}}, {'foo': None}, {'foo': {'bar': 1}}, {'foo': {'bar': None}}, {'foo': None}] {'foo': None}, {'foo': {'bar': 1}}, {'foo': {'bar': None}}, {'foo': None}]
        #   after:  [{'foo': {'bar': 1}}, {'foo': {'bar': None}}, {'foo': {'bar': None}}, {'foo': {'bar': 1}}, {'foo': {'bar': None}}, {'foo': {'bar': None}}]
        #                                                                 ^^
        #   => Looks like the before is wrong!
        ([{"bar": 1}, {"bar": None}, None], pa.struct({"bar": pa.int64()}), DataType.struct({"bar": DataType.int64()})),
        #
        # TODO[mg]: Lance is unable to write data in this schema!
        # OSError: LanceError(Schema): Unsupported data type: Map(Field { name: "entries", data_type: Struct([Field { name: "key", data_type: LargeUtf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), /Users/runner/work/lance/lance/rust/lance-core/src/datatypes.rs:171:31
        # TODO[mg]: JSON cannot handle this either! It gets coerced into a struct type!
        # AssertionError: (Schema) after[foo]: Struct[a: Int64, b: Int64] | expected: Map[Utf8: Int64]
        (
            [[("a", 1), ("b", 2)], [], None],
            pa.map_(pa.large_string(), pa.int64()),
            DataType.map(DataType.string(), DataType.int64()),
        ),
        (
            [datetime.time(1, 2, 3, 4), datetime.time(5, 6, 7, 8), None],
            pa.time64("us"),
            DataType.time(TimeUnit.us()),
        ),
    ],
)
def test_roundtrip_simple_arrow_types(tmp_path: Path, fmt: FMT, data: list, pa_type, expected_dtype: DataType):
    if fmt == "parquet" and not PYARROW_GE_8_0_0:
        pytest.skip("PyArrow writing to Parquet does not have good coverage for all types for versions <8.0.0")
    if fmt == "csv" and not PYARROW_GE_11_0_0:
        pytest.skip("PyArrow writing to CSV does not have good coverage for all types for versions <11.0.0.")
    if fmt == "csv":
        pytest.skip("BUG -- FIXME: csv read-write doesn't work for simple arrow types without special 'id' column")
    if fmt == "json":
        if isinstance(data, (datetime.datetime, datetime.time)) or expected_dtype == DataType.binary():
            pytest.skip(
                "BUG -- FIXME: daft.exceptions.DaftCoreException: Not Yet Implemented: JSON writes are not supported "
                "with extension, timezone with timestamp, binary, or duration data types"
            )
        if pa_type != pa.time64("us") and expected_dtype in [
            DataType.decimal128(16, 8),
            DataType.duration(TimeUnit.ms()),
            DataType.duration(TimeUnit.us()),
            DataType.duration(TimeUnit.ns()),
            DataType.timestamp(TimeUnit.ms()),
            DataType.timestamp(TimeUnit.us()),
            DataType.timestamp(TimeUnit.ns()),
            DataType.time(TimeUnit.us()),
            DataType.time(TimeUnit.ns()),
            DataType.map(DataType.string(), DataType.int64()),
        ]:
            pytest.skip(f"BUG -- FIXME: JSON write-read cycle doesn't handle {expected_dtype=} values properly.")
    if data == [[1, 2, 3], [4, 5, 6], None]:
        pytest.skip(
            f"BUG -- FIXME: Daft cannot handle this {data=} as {expected_dtype=} -> it "
            f"coerces into a list[int64] despite the PyArrow list_size being specified as 3."
        )
    if fmt == "lance":
        if data == [{"bar": 1}, {"bar": None}, None] or data == [[("a", 1), ("b", 2)], [], None]:
            pytest.skip(f"BUG -- FIXME: Lance cannot handle this test case: {data=} {pa_type=} {expected_dtype=}")
    #
    before = daft.from_arrow(pa.table({"foo": pa.array(data, type=pa_type)}))
    before = before.concat(before).collect()
    getattr(before, f"write_{fmt}")(str(tmp_path))
    after = getattr(daft, f"read_{fmt}")(str(tmp_path)).collect()
    assert (
        before.schema()["foo"].dtype == expected_dtype
    ), f'(Schema) before[foo]: {before.schema()["foo"].dtype} | expected: {expected_dtype} '
    assert (
        after.schema()["foo"].dtype == expected_dtype
    ), f'(Schema) after[foo]: {after.schema()["foo"].dtype} | expected: {expected_dtype} '
    assert before.to_arrow() == after.to_arrow(), f"(Arrow) before: {before.to_arrow()} | after: {after.to_arrow()}"


@pytest.mark.parametrize("fmt", ["parquet", "lance", "csv", "json"])
@pytest.mark.parametrize(
    ["data", "pa_type", "expected_dtype"],
    [
        (
            [datetime.datetime(1994, 1, 1), datetime.datetime(1995, 1, 1), None],
            pa.timestamp("ms", None),
            DataType.timestamp(TimeUnit.ms(), None),
        ),
        # TODO [mg] fix this in follow-up.
        # NOTE: doesn't work with Lance:
        # thread 'lance_background_thread' panicked at /Users/runner/work/lance/lance/rust/lance-core/src/datatypes/field.rs:164:42:
        # called `Result::unwrap()` on an `Err` value: Schema { message: "Unsupported timestamp type: timestamp:ms:+00:00", location: Location { file: "/Users/runner/work/lance/lance/rust/lance-core/src/datatypes.rs", line: 326, column: 39 } }
        # note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
        #
        # thread '<unnamed>' panicked at /Users/runner/work/lance/lance/rust/lance-datafusion/src/utils.rs:57:10:
        # called `Result::unwrap()` on an `Err` value: JoinError::Panic(Id(369), "called `Result::unwrap()` on an `Err` value: Schema { message: \"Unsupported timestamp type: timestamp:ms:+00:00\", location: Location { file: \"/Users/runner/work/lance/lance/rust/lance-core/src/datatypes.rs\", line: 326, column: 39 } }", ...)
        #                                                                 ----------------------------------------------------------------------------------------------------------------- Captured log call -----------------------------------------------------------------------------------------------------------------
        # ERROR    daft_local_execution:lib.rs:318 Error when running pipeline node DataSink
        (
            [datetime.datetime(1994, 1, 1), datetime.datetime(1995, 1, 1), None],
            pa.timestamp("ms", "+00:00"),
            DataType.timestamp(TimeUnit.ms(), "+00:00"),
        ),
        (
            [datetime.datetime(1994, 1, 1), datetime.datetime(1995, 1, 1), None],
            pa.timestamp("ms", "UTC"),
            DataType.timestamp(TimeUnit.ms(), "UTC"),
        ),
        # TODO [mg] fix this in follow-up.
        # NOTE: doesn't work with Lance:
        (
            [datetime.datetime(1994, 1, 1), datetime.datetime(1995, 1, 1), None],
            pa.timestamp("ms", "+08:00"),
            DataType.timestamp(TimeUnit.ms(), "+08:00"),
        ),
    ],
)
def test_roundtrip_temporal_arrow_types(
    tmp_path: Path, fmt: FMT, data: list[datetime.datetime], pa_type, expected_dtype: DataType
):
    if fmt == "csv":
        pytest.skip("BUG -- FIXME: CSV read-write doesn't work for temporal types!")
    if fmt == "json":
        pytest.skip("BUG -- FIXME: JSON read-write doesn't work for temporal types!")
    # TODO [mg] fix this in follow-up.
    if fmt == "lance" and (pa_type == pa.timestamp("ms", "+08:00") or pa_type == pa.timestamp("ms", "+00:00")):
        pytest.skip(f"BUG -- FIXME: Lance cannot handle this timestamp: {pa_type}")

    before = daft.from_arrow(pa.table({"foo": pa.array(data, type=pa_type)}))
    before = before.concat(before)
    getattr(before, f"write_{fmt}")(str(tmp_path))
    after = getattr(daft, f"read_{fmt}")(str(tmp_path)).collect()
    assert before.schema()["foo"].dtype == expected_dtype
    assert after.schema()["foo"].dtype == expected_dtype
    assert before.to_arrow() == after.to_arrow()


@pytest.mark.parametrize("fmt", ["parquet", "lance"])
def test_roundtrip_tensor_types(tmp_path: Path, fmt: FMT):
    if fmt == "lance":
        # Error is on the .to_arrow() assert. Before vs. after differs in their "is_valid".
        # E         - tensor_col: [  -- is_valid: all not null
        # E         + tensor_col: [  -- is_valid:  [true,false,false]
        pytest.skip("BUG -- FIXME: Lance cannot handle tensor types!")

    # Define the expected data type for the tensor column
    expected_tensor_dtype = DataType.tensor(DataType.int64())

    # Create sample tensor data with some null values
    tensor_data = [np.array([[1, 2], [3, 4]]), None, None]

    # Create a Daft DataFrame with the tensor data
    df_original = daft.from_pydict({"tensor_col": Series.from_pylist(tensor_data)})

    # Double the size of the DataFrame to ensure we test with more data
    df_original = df_original.concat(df_original).collect()

    assert df_original.schema()["tensor_col"].dtype == expected_tensor_dtype

    # Write the DataFrame to a Parquet file
    getattr(df_original, f"write_{fmt}")(str(tmp_path))

    # Read the Parquet file back into a new DataFrame
    df_roundtrip = getattr(daft, f"read_{fmt}")(str(tmp_path)).collect()

    # Verify that the data type is preserved after the roundtrip
    assert df_roundtrip.schema()["tensor_col"].dtype == expected_tensor_dtype

    # Ensure the data content is identical after the roundtrip
    assert df_original.to_arrow() == df_roundtrip.to_arrow()


@pytest.mark.parametrize("fmt", ["parquet", "lance"])
@pytest.mark.parametrize("fixed_shape", [True, False])
def test_roundtrip_sparse_tensor_types(tmp_path, fmt: FMT, fixed_shape: bool):
    if fixed_shape:
        expected_dtype = DataType.sparse_tensor(DataType.int64(), (2, 2))
        data = [np.array([[0, 0], [1, 0]]), None, np.array([[0, 0], [0, 0]]), np.array([[0, 1], [0, 0]])]
    else:
        expected_dtype = DataType.sparse_tensor(DataType.int64())
        data = [np.array([[0, 0], [1, 0]]), None, np.array([[0, 0]]), np.array([[0, 1, 0], [0, 0, 1]])]
    if fmt == "lance":
        # Error is on the .to_arrow() assert. Before vs. after differs in their "is_valid".
        # E         - tensor_col: [  -- is_valid: all not null
        # E         + tensor_col: [  -- is_valid:  [true,false,false]
        pytest.skip("BUG -- FIXME: Lance cannot handle sparse tensor w/ and w/o fixed shape!")
    before = daft.from_pydict({"foo": Series.from_pylist(data)})
    before = before.with_column("foo", before["foo"].cast(expected_dtype))
    before = before.concat(before).collect()
    getattr(before, f"write_{fmt}")(str(tmp_path))
    after = getattr(daft, f"read_{fmt}")(str(tmp_path)).collect()
    assert before.schema()["foo"].dtype == expected_dtype
    assert after.schema()["foo"].dtype == expected_dtype
    assert before.to_arrow() == after.to_arrow()


def _make_check_embeddings(
    test_df: daft.DataFrame, dtype, *, embeddings_col: str = "e"
) -> Callable[[daft.DataFrame], None]:
    """Verify validity of the test dataframe & produce a function that checks another dataframe for equality."""
    test_df = test_df.collect()
    if test_df.count_rows() == 0:
        raise ValueError("Test DataFrame cannot be empty!")

    if embeddings_col not in test_df:
        raise ValueError(f"Test DataFrame doesn't have an embeddings column={embeddings_col}")

    test_rows = list(x[embeddings_col] for x in test_df.iter_rows())
    for i, t in enumerate(test_rows):
        assert isinstance(t, np.ndarray), f"Row {i} is not a numpy array, it is: {type(t)}: {t}"
        assert t.dtype == dtype, f"Row {i} array doesn't have {dtype=}, instead={t.dtype}"

    def _check_embeddings(loaded_df: daft.DataFrame) -> None:
        loaded_df = loaded_df.collect()
        if loaded_df.count_rows() != test_df.count_rows():
            raise ValueError(
                f"Expecting {test_df.count_rows()} rows but got a " f"DataFrame with {loaded_df.count_rows()}"
            )

        l_rows = list(x[embeddings_col] for x in loaded_df.iter_rows())
        for i, (t, l) in enumerate(zip(test_rows, l_rows)):  # noqa: E741
            assert isinstance(l, np.ndarray), f"Row {i} expected a numpy array when loading, got a {type(l)}: {l}"
            assert l.dtype == t.dtype, f"Row {i} has wrong dtype. Expected={t.dtype} vs. found={l.dtype}"
            assert (t == l).all(), f"Row {i} failed equality check: test_df={t} vs. loaded={l}"

    return _check_embeddings


@pytest.mark.parametrize("fmt", ["parquet", "lance"])
@pytest.mark.parametrize(
    ["dtype", "size"],
    [
        # (np.float16, 64), -- Arrow doesn't support f16
        (np.float32, 1024),
        (np.float64, 512),
        (np.int8, 2048),
        (np.int16, 512),
        (np.int32, 256),
        (np.int64, 128),
        (np.uint8, 2048),
        (np.uint16, 512),
        (np.uint32, 256),
        (np.uint64, 128),
        # (np.bool_, 512), -- Arrow only accepts numeric types
        # (np.complex64, 32), (np.complex128, 16), - Arrow doesn't support complex numbers
    ],
)
def test_roundtrip_embedding(tmp_path: Path, fmt: FMT, dtype: np.dtype, size: int) -> None:
    # make some embeddings of the specified data type and dimensionality
    # with uniformly at random distributed values
    make_array = partial(random_numerical_embedding, np.random.default_rng(), dtype, size)
    test_df = (
        daft.from_pydict({"e": [make_array() for _ in range(50)]})
        .with_column("e", daft.col("e").cast(DataType.embedding(DataType.from_numpy_dtype(dtype), size)))
        .collect()
    )

    # make a checking function for the loaded dataframe & verify our original dataframe
    check = _make_check_embeddings(test_df, dtype)

    # write the embeddings-containing dataframe to disk using the specified format
    getattr(test_df, f"write_{fmt}")(str(tmp_path)).collect()

    # read that same dataframe
    loaded_df = getattr(daft, f"read_{fmt}")(str(tmp_path)).collect()

    # check that the values in the embedding column exactly equal each other
    check(loaded_df)

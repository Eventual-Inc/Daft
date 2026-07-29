from __future__ import annotations

import pytest

import daft
from daft import DataType, Schema


def test_cast_to_schema_changes_dtypes() -> None:
    df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
    target = Schema.from_pydict({"a": DataType.float64(), "b": DataType.string()})

    result = df.cast_to_schema(target)

    assert result.schema() == target
    assert result.to_pydict() == {"a": [1.0, 2.0, 3.0], "b": ["4", "5", "6"]}


def test_cast_to_schema_drops_extra_columns() -> None:
    df = daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"], "extra": [7, 8, 9]})
    target = Schema.from_pydict({"a": DataType.int64(), "b": DataType.string()})

    result = df.cast_to_schema(target)

    assert result.column_names == ["a", "b"]


def test_cast_to_schema_reorders_columns() -> None:
    df = daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    target = Schema.from_pydict({"b": DataType.string(), "a": DataType.int64()})

    result = df.cast_to_schema(target)

    assert result.column_names == ["b", "a"]
    assert result.schema() == target


def test_cast_to_schema_missing_column_raises_by_default() -> None:
    df = daft.from_pydict({"a": [1, 2, 3]})
    target = Schema.from_pydict({"a": DataType.int64(), "absent": DataType.string()})

    with pytest.raises(ValueError, match="absent"):
        df.cast_to_schema(target)


def test_cast_to_schema_missing_column_raises_lists_all_absent() -> None:
    df = daft.from_pydict({"a": [1, 2, 3]})
    target = Schema.from_pydict({"a": DataType.int64(), "x": DataType.string(), "y": DataType.bool()})

    with pytest.raises(ValueError) as exc_info:
        df.cast_to_schema(target)

    message = str(exc_info.value)
    assert "x" in message
    assert "y" in message


def test_cast_to_schema_missing_null_fills_typed_nulls() -> None:
    df = daft.from_pydict({"a": [1, 2, 3]})
    target = Schema.from_pydict({"a": DataType.int64(), "absent": DataType.string()})

    result = df.cast_to_schema(target, missing="null")

    assert result.schema() == target
    assert result.to_pydict() == {"a": [1, 2, 3], "absent": [None, None, None]}


def test_cast_to_schema_missing_null_preserves_dtype() -> None:
    df = daft.from_pydict({"a": [1, 2, 3]})
    target = Schema.from_pydict({"a": DataType.int64(), "absent": DataType.float64()})

    result = df.cast_to_schema(target, missing="null")

    assert result.schema()["absent"].dtype == DataType.float64()


def test_cast_to_schema_all_columns_missing_with_null() -> None:
    df = daft.from_pydict({"a": [1, 2, 3]})
    target = Schema.from_pydict({"x": DataType.int64(), "y": DataType.string()})

    result = df.cast_to_schema(target, missing="null")

    assert result.schema() == target
    assert result.to_pydict() == {"x": [None, None, None], "y": [None, None, None]}


def test_cast_to_schema_identity_is_noop() -> None:
    df = daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    result = df.cast_to_schema(df.schema())

    assert result.schema() == df.schema()
    assert result.to_pydict() == df.to_pydict()


def test_cast_to_schema_preserves_row_count() -> None:
    df = daft.from_pydict({"a": list(range(100))})
    target = Schema.from_pydict({"a": DataType.float64()})

    result = df.cast_to_schema(target)

    assert result.count_rows() == 100


def test_cast_to_schema_on_empty_dataframe() -> None:
    df = daft.from_pydict({"a": []})
    target = Schema.from_pydict({"a": DataType.float64()})

    result = df.cast_to_schema(target)

    assert result.schema() == target
    assert result.to_pydict() == {"a": []}


def test_cast_to_schema_nested_types() -> None:
    df = daft.from_pydict({"a": [[1, 2], [3], []]})
    target = Schema.from_pydict({"a": DataType.list(DataType.float64())})

    result = df.cast_to_schema(target)

    assert result.schema() == target
    assert result.to_pydict() == {"a": [[1.0, 2.0], [3.0], []]}


def test_cast_to_schema_rejects_non_schema() -> None:
    df = daft.from_pydict({"a": [1, 2, 3]})

    with pytest.raises(TypeError):
        df.cast_to_schema({"a": DataType.int64()})


def test_cast_to_schema_rejects_pyarrow_schema() -> None:
    pa = pytest.importorskip("pyarrow")
    df = daft.from_pydict({"a": [1, 2, 3]})

    with pytest.raises(TypeError):
        df.cast_to_schema(pa.schema([pa.field("a", pa.int64())]))


def test_cast_to_schema_rejects_invalid_missing_value() -> None:
    df = daft.from_pydict({"a": [1, 2, 3]})
    target = Schema.from_pydict({"a": DataType.int64()})

    with pytest.raises(TypeError):
        df.cast_to_schema(target, missing="bogus")


def test_cast_to_schema_missing_must_be_keyword_only() -> None:
    df = daft.from_pydict({"a": [1, 2, 3]})
    target = Schema.from_pydict({"a": DataType.int64()})

    with pytest.raises(TypeError):
        df.cast_to_schema(target, "null")


def test_cast_to_schema_is_lazy() -> None:
    """The method should build a plan without materializing."""
    df = daft.from_pydict({"a": [1, 2, 3]})
    target = Schema.from_pydict({"a": DataType.float64()})

    result = df.cast_to_schema(target)

    assert result._result_cache is None


def test_cast_to_schema_works_across_partitions(make_df) -> None:
    df = make_df({"a": [1, 2, 3, 4], "b": ["w", "x", "y", "z"]}, repartition=2)
    target = Schema.from_pydict({"a": DataType.float64(), "b": DataType.string()})

    result = df.cast_to_schema(target).sort("a")

    assert result.schema() == target
    assert result.to_pydict() == {"a": [1.0, 2.0, 3.0, 4.0], "b": ["w", "x", "y", "z"]}

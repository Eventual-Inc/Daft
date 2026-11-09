from __future__ import annotations

import datetime

import pytest
import sqlalchemy

import daft
from daft.io._sql import SQLDataSink

pytest.importorskip("adbc_driver_sqlite")


@pytest.fixture()
def sqlite_url(tmp_path):
    return f"sqlite:///{tmp_path / 'write_test.db'}"


def _read(table_name, url):
    return daft.read_sql(f"SELECT * FROM {table_name}", url).sort("id").to_pydict()


def test_write_sql_adbc_basic(sqlite_url):
    data = {"id": [1, 2, 3], "name": ["A", "B", "C"], "score": [1.5, 2.5, None]}
    daft.from_pydict(data).write_sql("users", sqlite_url, backend="adbc")
    assert _read("users", sqlite_url) == data


def test_write_sql_adbc_roundtrip_with_adbc_read(sqlite_url):
    data = {"id": list(range(10)), "name": [f"name_{i}" for i in range(10)]}
    daft.from_pydict(data).write_sql("users", sqlite_url, backend="adbc")
    df = daft.read_sql("SELECT * FROM users", sqlite_url, backend="adbc")
    assert df.sort("id").to_pydict() == data


@pytest.mark.parametrize("write_mode", ["append", "overwrite", "fail"])
def test_write_sql_adbc_write_modes(sqlite_url, write_mode):
    daft.from_pydict({"id": [1], "name": ["A"]}).write_sql("users", sqlite_url, backend="adbc")
    new_df = daft.from_pydict({"id": [2], "name": ["B"]})

    if write_mode == "fail":
        with pytest.raises(ValueError, match="Table .* already exists"):
            new_df.write_sql("users", sqlite_url, write_mode=write_mode, backend="adbc")
        assert _read("users", sqlite_url) == {"id": [1], "name": ["A"]}
    elif write_mode == "append":
        new_df.write_sql("users", sqlite_url, write_mode=write_mode, backend="adbc")
        assert _read("users", sqlite_url) == {"id": [1, 2], "name": ["A", "B"]}
    else:
        new_df.write_sql("users", sqlite_url, write_mode=write_mode, backend="adbc")
        assert _read("users", sqlite_url) == {"id": [2], "name": ["B"]}


def test_write_sql_adbc_fail_mode_creates_missing_table(sqlite_url):
    daft.from_pydict({"id": [1]}).write_sql("users", sqlite_url, write_mode="fail", backend="adbc")
    assert _read("users", sqlite_url) == {"id": [1]}


def test_write_sql_adbc_overwrite_replaces_schema(sqlite_url):
    daft.from_pydict({"id": [1], "name": ["A"]}).write_sql("users", sqlite_url, backend="adbc")
    daft.from_pydict({"id": [2], "other": [3.0]}).write_sql("users", sqlite_url, write_mode="overwrite", backend="adbc")
    assert _read("users", sqlite_url) == {"id": [2], "other": [3.0]}


def test_write_sql_adbc_appends_to_table_created_by_sqlalchemy(sqlite_url):
    daft.from_pydict({"id": [1], "name": ["A"]}).write_sql("users", sqlite_url)
    daft.from_pydict({"id": [2], "name": ["B"]}).write_sql("users", sqlite_url, backend="adbc")
    assert _read("users", sqlite_url) == {"id": [1, 2], "name": ["A", "B"]}


def test_write_sql_adbc_many_partitions(sqlite_url):
    num_rows = 2000
    chunks = [range(start, start + 500) for start in range(0, num_rows, 500)]
    df = daft.from_pydict({"id": list(chunks[0]), "val": [f"val_{i}" for i in chunks[0]]})
    for chunk in chunks[1:]:
        df = df.concat(daft.from_pydict({"id": list(chunk), "val": [f"val_{i}" for i in chunk]}))
    df.write_sql("users", sqlite_url, backend="adbc")
    result = _read("users", sqlite_url)
    assert result["id"] == list(range(num_rows))
    assert result["val"] == [f"val_{i}" for i in range(num_rows)]


def test_write_sql_adbc_empty(sqlite_url):
    df = daft.from_pydict({"id": [], "name": []})
    df = df.select(df["id"].cast(daft.DataType.int64()), df["name"].cast(daft.DataType.string()))

    metrics = df.write_sql("users", sqlite_url, backend="adbc").to_pydict()

    assert metrics["total_written_rows"] == [0]
    read_df = daft.read_sql("SELECT * FROM users", sqlite_url).collect()
    assert len(read_df) == 0
    assert read_df.column_names == ["id", "name"]


def test_write_sql_adbc_returns_metrics(sqlite_url):
    df = daft.from_pydict({"id": [1, 2, 3], "name": ["A", "B", "C"]})
    metrics_df = df.write_sql("users", sqlite_url, backend="adbc")
    assert metrics_df.column_names == ["total_written_rows", "total_written_bytes"]
    metrics = metrics_df.to_pydict()
    assert metrics["total_written_rows"] == [3]
    assert metrics["total_written_bytes"][0] > 0


def test_write_sql_adbc_temporal_types(sqlite_url):
    data = {
        "id": [1, 2],
        "day": [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)],
        "created_at": [datetime.datetime(2024, 1, 1, 12, 30), datetime.datetime(2024, 1, 2, 8, 0)],
    }
    daft.from_pydict(data).write_sql("events", sqlite_url, backend="adbc")
    schema = {"id": daft.DataType.int64(), "day": daft.DataType.date(), "created_at": daft.DataType.timestamp("us")}
    df = daft.read_sql("SELECT * FROM events", sqlite_url, schema=schema)
    assert df.sort("id").to_pydict() == data


@pytest.mark.parametrize(
    ("non_primitive_handling", "expected"),
    [
        (None, ["[1, 2]", "[3]"]),
        ("str", ["[1, 2]", "[3]"]),
        ("bytes", [b"[1, 2]", b"[3]"]),
    ],
)
def test_write_sql_adbc_non_primitive_columns(sqlite_url, non_primitive_handling, expected):
    df = daft.from_pydict({"id": [1, 2], "list_col": [[1, 2], [3]]})
    if non_primitive_handling is None:
        with pytest.warns(UserWarning, match="Detected non-primitive columns"):
            df.write_sql("users", sqlite_url, backend="adbc")
    else:
        df.write_sql("users", sqlite_url, non_primitive_handling=non_primitive_handling, backend="adbc")
    assert _read("users", sqlite_url) == {"id": [1, 2], "list_col": expected}


def test_write_sql_adbc_python_objects(sqlite_url):
    class Point:
        def __init__(self, x):
            self.x = x

        def __str__(self):
            return f"Point({self.x})"

    df = daft.from_pydict({"id": [1, 2], "obj": [Point(1), None]})
    df.write_sql("points", sqlite_url, non_primitive_handling="str", backend="adbc")
    assert _read("points", sqlite_url) == {"id": [1, 2], "obj": ["Point(1)", None]}


def test_write_sql_adbc_non_primitive_error(sqlite_url):
    df = daft.from_pydict({"id": [1], "list_col": [[1, 2]]})
    with pytest.raises(ValueError, match="Cannot write non-primitive columns list_col"):
        df.write_sql("users", sqlite_url, non_primitive_handling="error", backend="adbc")


def test_write_sql_adbc_rejects_column_types(sqlite_url):
    df = daft.from_pydict({"id": [1]})
    with pytest.raises(ValueError, match="column_types"):
        df.write_sql("users", sqlite_url, column_types={"id": sqlalchemy.Integer()}, backend="adbc")


def test_write_sql_adbc_rejects_connection_factory(sqlite_url):
    def create_conn():
        return sqlalchemy.create_engine(sqlite_url).connect()

    df = daft.from_pydict({"id": [1]})
    with pytest.raises(ValueError, match="requires a database URL"):
        df.write_sql("users", create_conn, backend="adbc")


def test_write_sql_adbc_unsupported_dialect():
    df = daft.from_pydict({"id": [1]})
    with pytest.raises(ValueError, match="does not support the 'trino' dialect"):
        df.write_sql("users", "trino://user@localhost:8080/memory/default", backend="adbc")


def test_sql_data_sink_rejects_connectorx(sqlite_url):
    df = daft.from_pydict({"id": [1]})
    with pytest.raises(ValueError, match="can only read"):
        SQLDataSink(
            table_name="users",
            conn=sqlite_url,
            write_mode="append",
            column_types=None,
            df_schema=df.schema(),
            backend="connectorx",
        )

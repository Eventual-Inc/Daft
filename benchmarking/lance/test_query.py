from __future__ import annotations

import duckdb
import lance
import pytest

import daft

PATH = "/tmp/test.lance"

# Consolidated test configuration with built-in IDs
TEST_CASES = [
    pytest.param("count", "COUNT(1)", id="count"),
    pytest.param("sum_struct", "SUM(image.height)", id="sum_struct"),
    pytest.param("sum_int", "SUM(size)", id="sum_int"),
]


def test_duck_not_support_lance_struct():
    """Verify DuckDB's limitations with Lance struct types."""
    conn = duckdb.connect()
    arrow_table = lance.dataset(uri=PATH).to_table()
    conn.register("arrow_table", arrow_table)
    res = conn.execute("DESCRIBE arrow_table").fetchall()

    assert any(col[0] == "image" and col[1] == "INVALID" for col in res), "image type should be INVALID"

    with pytest.raises(duckdb.BinderException) as excinfo:
        conn.execute("SELECT SUM(image.height) FROM arrow_table").fetchall()

    assert "Cannot extract field 'height' from expression \"image\" because it is not a struct" in str(excinfo.value)


@pytest.mark.parametrize("agg_name, _", [case.values[:2] for case in TEST_CASES])
def test_daft_dataframe_read_lance(benchmark, agg_name, _):
    """Benchmark Daft aggregations on Lance data."""

    def operation():
        df = daft.read_lance(PATH)
        if agg_name == "count":
            return df.count_rows()
        elif agg_name == "sum_struct":
            return df.sum(df["image"].struct.get("height")).collect()
        elif agg_name == "sum_int":
            return df.sum(df["size"]).collect()
        else:
            return df.select("image").collect()

    result = benchmark.pedantic(operation, rounds=5, warmup_rounds=1)

    if agg_name == "count":
        assert result == 1000
    elif agg_name == "sum_struct":
        result_data = result.to_pydict()
        print(f"daft height: { result_data['height'][0]}")
        assert result_data["height"][0] > 0
    elif agg_name == "sum_int":
        result_data = result.to_pydict()
        print(f"daft size: { result_data['size'][0]}")
        assert result_data["size"][0] > 0


@pytest.mark.parametrize("agg_name, _", [case.values[:2] for case in TEST_CASES])
def test_daft_sql_read_lance(benchmark, agg_name, _):
    """Benchmark Daft aggregations on Lance data."""

    def operation():
        df = daft.read_lance(PATH)
        if agg_name == "count":
            return df.count_rows()
        elif agg_name == "sum_struct":
            df = daft.sql("SELECT sum(image.height) FROM df")
            return df.collect()
        elif agg_name == "sum_int":
            df = daft.sql("SELECT sum(size) FROM df")
            return df.collect()
        else:
            df = daft.sql("SELECT image FROM df")
            return df.collect()

    result = benchmark.pedantic(operation, rounds=5, warmup_rounds=1)

    if agg_name == "count":
        assert result == 1000
    elif agg_name == "sum_struct":
        result_data = result.to_pydict()
        print(f"daft height: { result_data['height'][0]}")
        assert result_data["height"][0] > 0
    elif agg_name == "sum_int":
        result_data = result.to_pydict()
        print(f"daft size: { result_data['size'][0]}")
        assert result_data["size"][0] > 0

@pytest.mark.parametrize("agg_name, _", [case.values[:2] for case in TEST_CASES])
def test_lance_native_read(benchmark, agg_name, _):
    """Benchmark Lance native query execution."""

    def read_fn():
        df = lance.dataset(PATH)
        if agg_name == "count":
            return df.count_rows()
        elif agg_name == "sum_struct":
            query = df.sql("SELECT sum(image.height) FROM dataset").build()
            return query.to_batch_records()
        elif agg_name == "sum_int":
            query = df.sql("SELECT sum(size) FROM dataset").build()
            return query.to_batch_records()
        else:
            return df["image"]

    result = benchmark(read_fn)

    if agg_name == "count":
        assert result == 1000
    elif agg_name == "sum_struct":
        height = result[0].to_pydict()["sum(dataset.image[height])"][0]
        print(f"native height: {height }")
        assert height > 0
    elif agg_name == "sum_int":
        size = result[0].to_pydict()["sum(dataset.size)"][0]
        print(f"native size: {size} ")
        assert size > 0
    else:
        assert len(result) == 1000


@pytest.mark.parametrize("agg_name, agg_expr", TEST_CASES)
def test_duckdb_read_lance(benchmark, agg_name, agg_expr):
    """Benchmark DuckDB aggregations on Lance data."""
    conn = duckdb.connect()
    arrow_table = lance.dataset(uri=PATH).to_table()

    conn.register("arrow_table", arrow_table)

    def read_fn():
        if agg_name == "sum_struct":
            return None
        return conn.execute(f"SELECT {agg_expr} FROM arrow_table").fetchall()

    result = benchmark.pedantic(read_fn, rounds=5, warmup_rounds=1)

    if agg_name == "count":
        assert result[0][0] == 1000
    elif agg_name == "sum_struct":
        print("not support")
    elif agg_name == "sum_int":
        assert result[0][0] > 0
    else:
        assert len(result) == 1000

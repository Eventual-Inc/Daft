"""Tests for DataFrameSource.get_dataframe unfold."""

from __future__ import annotations

import io
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import daft
from daft import DataType, col
from daft.io.source import DataFrameSource
from daft.schema import Schema

SCHEMA = Schema.from_pydict({"id": DataType.int64(), "x": DataType.int64(), "y": DataType.string()})
FULL = {"id": [1, 2, 3, 4, 5], "x": [1, 2, 3, 4, 5], "y": list("abcde")}
X_GT_3 = {"id": [4, 5], "x": [4, 5], "y": ["d", "e"]}


def _parquet(path: Path, table: pa.Table) -> str:
    pq.write_table(table, path)
    return str(path)


class _Unfold(DataFrameSource):
    def __init__(self, name: str = "Fake", schema: Schema = SCHEMA) -> None:
        self._name = name
        self._schema = schema

    @property
    def name(self) -> str:
        return self._name

    @property
    def schema(self) -> Schema:
        return self._schema


class Fake(_Unfold):
    def __init__(self, files: dict[str, str]) -> None:
        super().__init__()
        self.files = files
        self.seen: list = []
        self.used: list[list[str]] = []

    def get_dataframe(self, pushdowns):
        self.seen.append(pushdowns)
        left = [self.files["high"]] if pushdowns.filters is not None else [self.files["low"], self.files["high"]]
        self.used.append(left)
        return daft.read_parquet(left).join(daft.read_parquet(self.files["right"]), on="id")


@pytest.fixture
def files(tmp_path: Path) -> dict[str, str]:
    return {
        "low": _parquet(tmp_path / "low.parquet", pa.table({"id": [1, 2], "x": [1, 2]})),
        "high": _parquet(tmp_path / "high.parquet", pa.table({"id": [3, 4, 5], "x": [3, 4, 5]})),
        "right": _parquet(tmp_path / "right.parquet", pa.table({"id": [1, 2, 3, 4, 5], "y": list("abcde")})),
    }


def _explain(df, *, show_all: bool = False) -> str:
    buf = io.StringIO()
    df.explain(show_all=show_all, file=buf)
    return buf.getvalue()


def test_collect_twice(files: dict[str, str]):
    df = Fake(files).read()
    assert df.sort("id").to_pydict() == FULL
    assert df.sort("id").to_pydict() == FULL


def test_filter_prunes_file_and_still_applies(files: dict[str, str]):
    src = Fake(files)
    assert src.read().where(col("x") > 3).sort("id").to_pydict() == X_GT_3
    assert src.seen[-1].filters is not None
    assert files["low"] not in src.used[-1]
    assert src.read().where(col("x") > 3).select("y").sort("y").to_pydict() == {"y": ["d", "e"]}


def test_explain_limit_count_select(files: dict[str, str]):
    df = Fake(files).read()
    assert "DataSource: Fake" in _explain(df)
    assert "Join" not in _explain(df)
    assert "Join" in _explain(df, show_all=True)

    limited = df.limit(2)
    assert len(limited.to_pydict()["id"]) == 2
    plan = _explain(limited, show_all=True)
    assert -1 != plan.find("Limit") < plan.find("Join")

    assert df.count_rows() == 5
    assert df.count().to_pydict() == {"count": [5]}
    assert Fake(files).read().select("y").sort("y").to_pydict() == {"y": list("abcde")}


def test_errors(files: dict[str, str]):
    class Boom(_Unfold):
        def get_dataframe(self, pushdowns):
            raise RuntimeError("boom from get_dataframe")

    class Missing(_Unfold):
        def get_dataframe(self, pushdowns):
            return daft.read_parquet(files["high"]).select("id", "x")

    with pytest.raises(Exception, match="boom from get_dataframe"):
        Boom().read().collect()
    with pytest.raises(ValueError, match="missing column 'y'"):
        Missing("MissingCol").read().collect()


def test_self_read_raises():
    class Loop(_Unfold):
        def get_dataframe(self, pushdowns):
            return self.read()

    with pytest.raises(Exception, match="get_tasks"):
        Loop().read().collect()


def test_nested_dataframe_source(files: dict[str, str]):
    inner_schema = Schema.from_pydict({"id": DataType.int64(), "y": DataType.string()})

    class Inner(_Unfold):
        def __init__(self, path: str) -> None:
            super().__init__("Inner", inner_schema)
            self.path = path
            self.seen: list = []

        def get_dataframe(self, pushdowns):
            self.seen.append(pushdowns)
            return daft.read_parquet(self.path)

    class Outer(_Unfold):
        def __init__(self, inner: Inner, left: list[str]) -> None:
            super().__init__("Outer")
            self.inner = inner
            self.left = left

        def get_dataframe(self, pushdowns):
            return daft.read_parquet(self.left).join(self.inner.read(), on="id")

    inner = Inner(files["right"])
    outer = Outer(inner, [files["low"], files["high"]])
    assert outer.read().sort("id").to_pydict() == FULL
    assert inner.seen


def test_wrong_dtype():
    class Wrong(_Unfold):
        def get_dataframe(self, pushdowns):
            return daft.from_pydict({"id": [1], "x": ["a"], "y": ["b"]})

    with pytest.raises(ValueError, match="column 'x' has type"):
        Wrong().read().collect()

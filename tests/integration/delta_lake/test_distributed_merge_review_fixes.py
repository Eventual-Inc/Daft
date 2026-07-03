"""Regression tests for the 2026-07-03 review of distributed_merge_deltalake.

Covers, in order:
  - Parser: silent lit(True)/lit(str) fallbacks, "IS"-substring guard,
    lowercase IS NULL, quote-blind splitting, arrow_cast fractional seconds,
    case-sensitive alias matching (Task 1).
  - NULL clause predicates silently deleting rows (Task 2).
  - First-match-wins clause ordering: DELETE vs UPDATE order, per-column
    clause blending (Task 3).
  - Partition scoping: cartesian-product over-rewrite, temporal-partitioned
    crash, microsecond partition values (Task 4).
  - Concurrent-commit detection (Task 5).
  - Feature gating: CHECK constraints, Change Data Feed (Task 6).
  - Metrics: hardcoded zeros, num_source_rows overcount (Task 7).
  - except_cols on *_all clauses (Task 8).
"""

from __future__ import annotations

import datetime
import json
import os

import pytest

import daft
from daft.io.delta_lake._deltalake import (
    _parse_merge_predicate,
    _parse_predicate,
    _resolve_expr,
)


def _eval(expr, data):
    """Evaluate an expression against a small in-memory frame."""
    return daft.from_pydict(data).select(expr.alias("out")).to_pydict()["out"]


def _write_base(tmp_path, data):
    path = str(tmp_path / "table")
    daft.from_pydict(data).write_deltalake(path)
    return path


def _read_sorted(path: str, sort_col: str = "id") -> dict:
    return daft.read_deltalake(path).sort(sort_col).to_pydict()


# ---------------------------------------------------------------------------
# Task 1 — predicate/expression parsing
# ---------------------------------------------------------------------------


class TestParser:
    def test_comparison_operators_parse(self):
        # previously silently lit(True)
        pred = _parse_predicate("target.qty > 100", "source", "target", ["id"], set())
        assert _eval(pred, {"qty": [50, 150]}) == [False, True]

    def test_is_substring_column_names(self):
        # previously lit(True) because "IS_active"/"hIStory"/"ParIS" contain "IS"
        pred = _parse_predicate("target.is_active = true", "source", "target", ["id"], set())
        assert _eval(pred, {"is_active": [True, False]}) == [True, False]
        pred2 = _parse_predicate(
            "source.history != target.history", "source", "target", ["id"], {"history"}
        )
        assert _eval(
            pred2, {"history": ["a", "b"], "history.__src__": ["a", "c"]}
        ) == [False, True]

    def test_lowercase_is_not_null(self):
        pred = _parse_predicate("source.hash is not null", "source", "target", ["id"], set())
        assert _eval(pred, {"hash": ["x", None]}) == [True, False]

    def test_string_literal_containing_operators(self):
        pred = _parse_predicate("target.tag = 'a AND b'", "source", "target", ["id"], set())
        assert _eval(pred, {"tag": ["a AND b", "z"]}) == [True, False]
        pred2 = _parse_predicate("source.label = 'x != y'", "source", "target", ["id"], set())
        assert _eval(pred2, {"label": ["x != y", "nope"]}) == [True, False]

    def test_arithmetic_update_value(self):
        val = _resolve_expr("target.n + 1", "source", "target", ["id"], set())
        assert _eval(val, {"n": [1, 41]}) == [2, 42]

    def test_numeric_literal_is_numeric(self):
        val = _resolve_expr("1", "source", "target", ["id"], set())
        assert _eval(val, {"x": [0]}) == [1]

    def test_arrow_cast_fractional_seconds(self):
        val = _resolve_expr(
            "arrow_cast('2020-01-01 12:00:00.500000', 'Timestamp(Microsecond, None)')",
            "source",
            "target",
            ["id"],
            set(),
        )
        out = _eval(val, {"x": [0]})
        assert out == [datetime.datetime(2020, 1, 1, 12, 0, 0, 500000)]

    def test_garbage_predicate_raises(self):
        with pytest.raises(ValueError):
            _parse_predicate("target.qty >>>= ??", "source", "target", ["id"], set())

    def test_merge_predicate_quote_aware_and_case_insensitive(self):
        keys, residual = _parse_merge_predicate(
            "TARGET.id = SOURCE.id AND target.tag = 'a AND b'", "source", "target"
        )
        assert keys == ["id"]
        assert residual == ["target.tag = 'a AND b'"]


# ---------------------------------------------------------------------------
# Task 2 — NULL clause predicates must be treated as not-satisfied
# ---------------------------------------------------------------------------


class TestNullPredicates:
    def test_null_delete_predicate_keeps_row(self, tmp_path):
        # Row 3 legitimately fires the delete so the write pass actually runs
        # (a no-op merge skips the commit and would mask the bug).
        path = _write_base(tmp_path, {"id": [1, 2, 3], "flag": ["x", "same", "y"]})
        source = daft.from_pydict({"id": [1, 2, 3], "flag": [None, "same", "DIFFERENT"]})
        result = (
            daft.distributed_merge_deltalake(
                table=path, source=source, predicate="target.id = source.id"
            )
            .when_matched_delete(predicate="source.flag != target.flag")
            .execute()
        ).to_pydict()
        rows = _read_sorted(path)
        # NULL != 'x' is NULL -> clause does NOT fire -> row 1 kept
        assert rows["id"] == [1, 2]
        assert result["num_target_rows_deleted"][0] == 1


# ---------------------------------------------------------------------------
# Task 3 — first-match-wins clause ordering (per ROW, across clause kinds)
# ---------------------------------------------------------------------------


class TestClauseOrdering:
    def test_update_before_delete_wins(self, tmp_path):
        path = _write_base(tmp_path, {"id": [1], "v": ["old"]})
        source = daft.from_pydict({"id": [1], "v": ["new"]})
        (
            daft.distributed_merge_deltalake(
                table=path, source=source, predicate="target.id = source.id"
            )
            .when_matched_update(updates={"v": "source.v"})
            .when_matched_delete()
            .execute()
        )
        rows = daft.read_deltalake(path).to_pydict()
        assert rows["id"] == [1] and rows["v"] == ["new"]  # updated, NOT deleted

    def test_delete_before_update_wins(self, tmp_path):
        path = _write_base(tmp_path, {"id": [1], "v": ["old"]})
        source = daft.from_pydict({"id": [1], "v": ["new"]})
        (
            daft.distributed_merge_deltalake(
                table=path, source=source, predicate="target.id = source.id"
            )
            .when_matched_delete()
            .when_matched_update(updates={"v": "source.v"})
            .execute()
        )
        assert daft.read_deltalake(path).count_rows() == 0

    def test_only_first_matching_update_clause_applies(self, tmp_path):
        path = _write_base(
            tmp_path, {"id": [1], "a": ["a0"], "b": ["b0"], "x": [1], "y": [2]}
        )
        source = daft.from_pydict({"id": [1], "a": ["a1"], "b": ["b1"], "x": [1], "y": [2]})
        (
            daft.distributed_merge_deltalake(
                table=path, source=source, predicate="target.id = source.id"
            )
            .when_matched_update(updates={"a": "source.a"}, predicate="source.x = 1")
            .when_matched_update(updates={"b": "source.b"}, predicate="source.y = 2")
            .execute()
        )
        rows = daft.read_deltalake(path).to_pydict()
        assert rows["a"] == ["a1"]
        assert rows["b"] == ["b0"]  # second clause must NOT also fire

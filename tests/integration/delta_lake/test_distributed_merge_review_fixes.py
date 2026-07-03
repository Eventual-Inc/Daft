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

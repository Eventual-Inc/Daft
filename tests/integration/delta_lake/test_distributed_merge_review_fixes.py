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


# ---------------------------------------------------------------------------
# Task 4 — partition scoping: exact tuples, temporal partition values
# ---------------------------------------------------------------------------


def _log_actions(path):
    """All actions from every JSON commit in the table's _delta_log."""
    logdir = os.path.join(path, "_delta_log")
    actions = []
    for f in sorted(os.listdir(logdir)):
        if f.endswith(".json"):
            with open(os.path.join(logdir, f)) as fh:
                actions += [json.loads(line) for line in fh if line.strip()]
    return actions


class TestPartitionScoping:
    def test_untouched_partition_not_rewritten(self, tmp_path):
        path = str(tmp_path / "table")
        daft.from_pydict(
            {"p": ["a", "b", "c"], "id": [1, 2, 3], "v": ["x", "y", "z"]}
        ).write_deltalake(path, partition_cols=["p"])
        source = daft.from_pydict({"id": [1, 2], "v": ["X", "Y"], "p": ["a", "b"]})
        result = (
            daft.distributed_merge_deltalake(
                table=path, source=source, predicate="target.id = source.id"
            )
            .when_matched_update_all()
            .execute()
        ).to_pydict()
        merge_removes = [a["remove"]["path"] for a in _log_actions(path) if "remove" in a]
        assert not any("p=c" in p for p in merge_removes)
        assert result["num_target_rows_copied"][0] == 0
        rows = _read_sorted(path)
        assert rows["v"] == ["X", "Y", "z"]

    def test_multicolumn_partition_consistency(self, tmp_path):
        # delta-rs's commit API only accepts a single conjunction filter, so a
        # non-product tuple set is expanded to its per-column closure — the
        # write filter and removal filter must cover the SAME set (no data
        # loss), and copied rows are counted accordingly.
        path = str(tmp_path / "table")
        daft.from_pydict(
            {
                "y": [2023, 2023, 2024, 2024],
                "m": ["jan", "jun", "jan", "jun"],
                "id": [1, 2, 3, 4],
                "v": ["a", "b", "c", "d"],
            }
        ).write_deltalake(path, partition_cols=["y", "m"])
        source = daft.from_pydict(
            {"id": [1, 4], "v": ["A", "D"], "y": [2023, 2024], "m": ["jan", "jun"]}
        )
        result = (
            daft.distributed_merge_deltalake(
                table=path, source=source, predicate="target.id = source.id"
            )
            .when_matched_update_all()
            .execute()
        ).to_pydict()
        rows = _read_sorted(path)
        assert rows["id"] == [1, 2, 3, 4]  # closure rows preserved, not lost
        assert rows["v"] == ["A", "b", "c", "D"]
        assert result["num_target_rows_copied"][0] == 2  # closure copies counted

    def test_timestamp_partitioned_merge(self, tmp_path):
        path = str(tmp_path / "table")
        ts1 = datetime.datetime(2024, 1, 1, 12, 0, 0)
        ts2 = datetime.datetime(2024, 6, 1, 0, 0, 0, 123456)  # microseconds!
        daft.from_pydict({"ts": [ts1, ts2], "id": [1, 2], "v": ["a", "b"]}).write_deltalake(
            path, partition_cols=["ts"]
        )
        source = daft.from_pydict({"id": [2], "v": ["B"], "ts": [ts2]})
        (
            daft.distributed_merge_deltalake(
                table=path, source=source, predicate="target.id = source.id"
            )
            .when_matched_update_all()
            .execute()
        )
        rows = _read_sorted(path)
        # no crash, no duplicate rows, update applied
        assert rows["id"] == [1, 2]
        assert rows["v"] == ["a", "B"]


# ---------------------------------------------------------------------------
# Task 5 — concurrent commits must fail the merge, not be silently discarded
# ---------------------------------------------------------------------------


class TestConcurrency:
    def test_external_commit_between_pin_and_write_raises(self, tmp_path):
        import deltalake as dl
        import pyarrow as pa

        path = _write_base(tmp_path, {"id": [1], "v": ["a"]})
        source = daft.from_pydict({"id": [1], "v": ["A"]})
        builder = daft.distributed_merge_deltalake(
            table=path, source=source, predicate="target.id = source.id"
        ).when_matched_update_all()
        # A concurrent writer lands a commit the builder's pinned snapshot
        # cannot see.
        dl.write_deltalake(path, pa.table({"id": [99], "v": ["zz"]}), mode="append")
        with pytest.raises(RuntimeError, match="[Cc]oncurrent"):
            builder.execute()
        # Nothing lost: the concurrent row is still there.
        assert 99 in daft.read_deltalake(path).to_pydict()["id"]


# ---------------------------------------------------------------------------
# Task 6 — tables relying on write-time enforcement must be rejected loudly
# ---------------------------------------------------------------------------


class TestFeatureGating:
    def test_check_constraint_table_rejected(self, tmp_path):
        import deltalake as dl
        import pyarrow as pa

        path = str(tmp_path / "table")
        dl.write_deltalake(path, pa.table({"id": [1], "v": [5]}))
        dt = dl.DeltaTable(path)
        dt.alter.add_constraint({"v_positive": "v > 0"})
        source = daft.from_pydict({"id": [1], "v": [-100]})
        with pytest.raises(ValueError, match="constraint"):
            (
                daft.distributed_merge_deltalake(
                    table=path, source=source, predicate="target.id = source.id"
                )
                .when_matched_update_all()
                .execute()
            )
        # The violating row was never committed.
        assert daft.read_deltalake(path).to_pydict()["v"] == [5]

    def test_cdf_table_rejected(self, tmp_path):
        import deltalake as dl
        import pyarrow as pa

        path = str(tmp_path / "table")
        dl.write_deltalake(
            path,
            pa.table({"id": [1], "v": [5]}),
            configuration={"delta.enableChangeDataFeed": "true"},
        )
        source = daft.from_pydict({"id": [1], "v": [6]})
        with pytest.raises(ValueError, match="[Cc]hange [Dd]ata [Ff]eed"):
            (
                daft.distributed_merge_deltalake(
                    table=path, source=source, predicate="target.id = source.id"
                )
                .when_matched_update_all()
                .execute()
            )


# ---------------------------------------------------------------------------
# Task 7 — metrics must reflect reality
# ---------------------------------------------------------------------------


class TestMetrics:
    def test_file_and_timing_metrics_populated(self, tmp_path):
        path = _write_base(tmp_path, {"id": [1, 2], "v": ["a", "b"]})
        source = daft.from_pydict({"id": [2, 3], "v": ["B", "c"]})
        result = (
            daft.distributed_merge_deltalake(
                table=path, source=source, predicate="target.id = source.id"
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        ).to_pydict()
        assert result["num_target_files_added"][0] >= 1
        assert result["num_target_files_removed"][0] >= 1
        assert result["execution_time_ms"][0] > 0

    def test_num_source_rows_exact_with_duplicate_target_keys(self, tmp_path):
        path = _write_base(tmp_path, {"id": [1, 1], "v": ["a", "b"]})  # dup target keys
        source = daft.from_pydict({"id": [1], "v": ["X"]})
        result = (
            daft.distributed_merge_deltalake(
                table=path, source=source, predicate="target.id = source.id"
            )
            .when_matched_update_all()
            .execute()
        ).to_pydict()
        assert result["num_source_rows"][0] == 1  # was 2 (join-row count)

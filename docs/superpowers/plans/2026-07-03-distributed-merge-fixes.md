# Distributed Delta Merge Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all 15 verified code-review findings in `distributed_merge_deltalake` / `DistributedDeltaMergeBuilder` (daft/io/delta_lake/_deltalake.py) and add regression tests for each.

**Architecture:** Replace the hand-rolled SQL string parser with a quote-aware alias rewriter that delegates to Daft's native `daft.sql_expr`; restructure clause storage into one globally-ordered list so first-match-wins is enforced per row (including DELETE vs UPDATE order); track affected partitions as value **tuples** (fixes cartesian over-rewrite and sidesteps the temporal `is_in` crash via OR-of-equalities); add a pre-commit version check, table-feature gating (constraints/CDF), real metrics, and `except_cols`.

**Tech Stack:** Python only (no Rust rebuild). daft (this fork), deltalake 1.6.0, pytest with local `tmp_path` Delta tables.

## Global Constraints

- Run all tests with: `DAFT_RUNNER=native .venv/bin/pytest -o addopts="" -q <file-or-node> -x` (the fork's spill join hangs otherwise; `-o addopts=""` bypasses the `not integration` marker exclusion).
- New tests live in `tests/integration/delta_lake/test_distributed_merge_review_fixes.py` following the existing plain-pytest + `tmp_path` local-table style of `test_distributed_merge_fixes.py`.
- All production edits are in `daft/io/delta_lake/_deltalake.py` unless stated.
- Public API compatibility: `str` and `Expression` predicates/update values keep working; parse failures must now RAISE `ValueError` instead of silently guessing.
- After each task: run the new tests for that task AND `tests/integration/delta_lake/test_distributed_merge_fixes.py` (existing regression suite) before committing.
- Commit after each green task with a conventional-commit message; end commit body with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

---

### Task 1: Replace string parsers with alias-rewrite + `daft.sql_expr`

Fixes findings: silent `lit(True)` fallback (1557), unanchored "IS" guard (1540), lowercase `is null` (1548), `_resolve_expr` lit fallback + `source.n + 1` misparse (1485/1471), arrow_cast fractional crash (1462), quote-blind splitting in clause predicates (1533/1509), plus below-cut case-sensitive alias and parser-reuse findings.

**Files:**
- Modify: `daft/io/delta_lake/_deltalake.py` (`_resolve_expr`, `_parse_predicate`, delete `_parse_single_predicate`, rework `_parse_merge_predicate`)
- Test: `tests/integration/delta_lake/test_distributed_merge_review_fixes.py`

**Interfaces:**
- Produces: `_rewrite_merge_sql(sql: str, source_alias: str, target_alias: str, on: list[str], suffixed_cols: set[str] | None) -> str` (module-level), `_resolve_expr(...) -> Expression` (same signature as today, raises ValueError on unparseable), `_parse_predicate(...) -> Expression` (same signature, now full SQL via sql_expr). `_parse_merge_predicate` keeps its signature `(predicate, source_alias, target_alias) -> tuple[list[str], list[str]]` but becomes quote-aware and alias-case-insensitive.

- [ ] **Step 1: Write failing parser tests**

```python
# tests/integration/delta_lake/test_distributed_merge_review_fixes.py
"""Regression tests for the 2026-07-03 review of distributed_merge_deltalake."""
from __future__ import annotations

import datetime

import pytest

import daft
from daft import col, lit
from daft.io.delta_lake._deltalake import (
    _parse_merge_predicate,
    _parse_predicate,
    _resolve_expr,
)


def _eval(expr, data):
    """Evaluate an expression against a small in-memory frame."""
    return daft.from_pydict(data).select(expr.alias("out")).to_pydict()["out"]


class TestParser:
    def test_comparison_operators_parse(self):
        # previously silently lit(True)
        pred = _parse_predicate("target.qty > 100", "source", "target", ["id"], set())
        assert _eval(pred, {"qty": [50, 150]}) == [False, True]

    def test_is_substring_column_names(self):
        # previously lit(True) because "hIStory"/"IS_active" contain "IS"
        pred = _parse_predicate("target.is_active = true", "source", "target", ["id"], set())
        assert _eval(pred, {"is_active": [True, False]}) == [True, False]
        pred2 = _parse_predicate(
            "source.history != target.history", "source", "target", ["id"], {"history"}
        )
        assert _eval(pred2, {"history": ["a", "b"], "history.__src__": ["a", "c"]}) == [False, True]

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
            "source", "target", ["id"], set(),
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
```

- [ ] **Step 2: Run tests, verify they fail**

Run: `DAFT_RUNNER=native .venv/bin/pytest -o addopts="" -q tests/integration/delta_lake/test_distributed_merge_review_fixes.py -x`
Expected: FAIL (wrong values from lit(True) fallbacks / ValueError from strptime).

- [ ] **Step 3: Implement the rewriter and delegate to sql_expr**

Replace the bodies of `_resolve_expr` / `_parse_predicate`, delete `_parse_single_predicate`, add helpers:

```python
_ARROW_CAST_RE = None  # compiled lazily in _translate_arrow_cast


def _split_sql_quotes(sql: str) -> "list[tuple[str, bool]]":
    """Split sql into (segment, is_quoted_literal) pieces, quote-aware."""
    parts: list[tuple[str, bool]] = []
    buf = []
    in_quote = False
    for ch in sql:
        if ch == "'":
            if in_quote:
                buf.append(ch)
                parts.append(("".join(buf), True))
                buf = []
                in_quote = False
                continue
            if buf:
                parts.append(("".join(buf), False))
            buf = [ch]
            in_quote = True
            continue
        buf.append(ch)
    if buf:
        parts.append(("".join(buf), in_quote))
    return parts


def _translate_arrow_cast(sql: str) -> str:
    """Rewrite DataFusion-style arrow_cast('v', 'Timestamp(...)') to SQL CAST."""
    import re

    return re.sub(
        r"arrow_cast\(\s*('[^']*')\s*,\s*'Timestamp\([^)]*\)'\s*\)",
        r"CAST(\1 AS TIMESTAMP)",
        sql,
        flags=re.IGNORECASE,
    )


def _rewrite_merge_sql(
    sql: str,
    source_alias: str,
    target_alias: str,
    on: "list[str]",
    suffixed_cols: "set[str] | None",
) -> str:
    """Rewrite alias-qualified refs to the joined frame's actual column names.

    ``source.x`` -> ``"x.__src__"`` when x collides with a target column,
    else ``"x"``; ``target.x`` -> ``"x"``. Quoted string literals are left
    untouched; alias matching is case-insensitive (SQL identifier rules).
    """
    import re

    def _src_name(name: str) -> str:
        if name in on:
            return f'"{name}"'
        if suffixed_cols is None or name in suffixed_cols:
            return f'"{name}.__src__"'
        return f'"{name}"'

    src_pat = re.compile(rf"\b{re.escape(source_alias)}\.(\w+)", re.IGNORECASE)
    tgt_pat = re.compile(rf"\b{re.escape(target_alias)}\.(\w+)", re.IGNORECASE)

    out = []
    for segment, quoted in _split_sql_quotes(sql):
        if quoted:
            out.append(segment)
            continue
        segment = _translate_arrow_cast(segment) if "arrow_cast" not in segment.lower() else segment
        # arrow_cast translation must see the literal, so run it on the whole
        # string first (see _resolve_expr/_parse_predicate below); here only
        # alias rewriting happens.
        segment = src_pat.sub(lambda m: _src_name(m.group(1)), segment)
        segment = tgt_pat.sub(lambda m: f'"{m.group(1)}"', segment)
        out.append(segment)
    return "".join(out)


def _resolve_expr(
    expr_str: str,
    source_alias: str,
    target_alias: str,
    on: "list[str]",
    suffixed_cols: "set[str] | None" = None,
) -> Any:
    """Resolve a SQL expression string to a Daft expression via daft.sql_expr.

    Raises ValueError on unparseable input instead of guessing.
    """
    from daft.sql import sql_expr

    sql = _translate_arrow_cast(expr_str.strip())
    sql = _rewrite_merge_sql(sql, source_alias, target_alias, on, suffixed_cols)
    try:
        return sql_expr(sql)
    except Exception as e:
        raise ValueError(
            f"Could not parse merge expression {expr_str!r} (rewritten: {sql!r}): {e}"
        ) from e


def _parse_predicate(
    pred_str: str,
    source_alias: str,
    target_alias: str,
    on: "list[str] | None" = None,
    suffixed_cols: "set[str] | None" = None,
) -> Any:
    """Parse a predicate string into a Daft boolean expression via daft.sql_expr."""
    return _resolve_expr(pred_str, source_alias, target_alias, on or [], suffixed_cols)
```

Note: `_translate_arrow_cast` must run on the FULL string before quote-splitting (its literal lives inside quotes); the call order in `_resolve_expr` above does that, so remove the stray arrow_cast line inside `_rewrite_merge_sql`'s loop (keep the loop purely alias rewriting).

Rework `_parse_merge_predicate` to strip literals before splitting on AND and to match aliases case-insensitively:

```python
def _parse_merge_predicate(
    predicate: str,
    source_alias: str,
    target_alias: str,
) -> "tuple[list[str], list[str]]":
    import re

    join_keys: list[str] = []
    residual_predicates: list[str] = []

    # Replace quoted literals with placeholders so AND-splitting is quote-safe.
    segments = _split_sql_quotes(predicate)
    literals: list[str] = []
    masked_parts: list[str] = []
    for seg, quoted in segments:
        if quoted:
            masked_parts.append(f"\x00{len(literals)}\x00")
            literals.append(seg)
        else:
            masked_parts.append(seg)
    masked = "".join(masked_parts)

    def _unmask(s: str) -> str:
        for i, litstr in enumerate(literals):
            s = s.replace(f"\x00{i}\x00", litstr)
        return s

    eq_re = re.compile(
        rf"^\s*(?:(?P<t1>{re.escape(target_alias)})|(?P<s1>{re.escape(source_alias)}))\.(?P<c1>\w+)\s*=\s*"
        rf"(?:(?P<t2>{re.escape(target_alias)})|(?P<s2>{re.escape(source_alias)}))\.(?P<c2>\w+)\s*$",
        re.IGNORECASE,
    )

    for part in re.split(r"\s+[Aa][Nn][Dd]\s+", masked):
        part = part.strip()
        if not part:
            continue
        m = eq_re.match(part)
        if m and m.group("c1") == m.group("c2"):
            one_target = bool(m.group("t1")) != bool(m.group("t2"))
            one_source = bool(m.group("s1")) != bool(m.group("s2"))
            if one_target and one_source:
                join_keys.append(m.group("c1"))
                continue
        residual_predicates.append(_unmask(part))

    return join_keys, residual_predicates
```

Delete `_parse_single_predicate` entirely (no other callers — verify with grep).

- [ ] **Step 4: Run tests, verify pass**

Run the Task 1 tests plus the existing suite:
`DAFT_RUNNER=native .venv/bin/pytest -o addopts="" -q tests/integration/delta_lake/test_distributed_merge_review_fixes.py tests/integration/delta_lake/test_distributed_merge_fixes.py`
Expected: PASS.

- [ ] **Step 5: Commit** — `fix(delta): parse merge predicates with daft.sql_expr instead of hand-rolled parser`

---

### Task 2: NULL-safe clause predicates

Fixes: NULL delete predicate silently drops rows (1178); NULL metric inconsistency part of the metrics finding.

**Files:**
- Modify: `daft/io/delta_lake/_deltalake.py` (`execute()`: `_resolve_predicate`)
- Test: `tests/integration/delta_lake/test_distributed_merge_review_fixes.py`

**Interfaces:**
- Produces: `_resolve_predicate(pred)` inside `execute()` returns predicates already wrapped with `.fill_null(lit(False))` (SQL WHEN-clause semantics: NULL condition = not satisfied).

- [ ] **Step 1: Write failing test**

```python
def _write_base(tmp_path, data):
    path = str(tmp_path / "table")
    daft.from_pydict(data).write_deltalake(path)
    return path


class TestNullPredicates:
    def test_null_delete_predicate_keeps_row(self, tmp_path):
        path = _write_base(tmp_path, {"id": [1, 2], "flag": ["x", "same"]})
        source = daft.from_pydict({"id": [1, 2], "flag": [None, "same"]})
        result = (
            daft.distributed_merge_deltalake(table=path, source=source,
                                             predicate="target.id = source.id")
            .when_matched_delete(predicate="source.flag != target.flag")
            .execute()
        ).to_pydict()
        rows = daft.read_deltalake(path).sort("id").to_pydict()
        # NULL != 'x' is NULL -> clause does NOT fire -> row kept
        assert rows["id"] == [1, 2]
        assert result["num_target_rows_deleted"][0] == 0
```

- [ ] **Step 2: Run, verify FAIL** (row id=1 currently disappears).

- [ ] **Step 3: Implement** — in `execute()`, change `_resolve_predicate`:

```python
        def _resolve_predicate(pred: Any) -> Any:
            if pred is None:
                return None
            if isinstance(pred, Expression):
                return pred.fill_null(lit(False))
            return _parse_predicate(
                pred, self._source_alias, self._target_alias, on, suffixed
            ).fill_null(lit(False))
```

- [ ] **Step 4: Run tests (new + existing suite), verify PASS.**

- [ ] **Step 5: Commit** — `fix(delta): treat NULL merge clause predicates as not-satisfied`

---

### Task 3: Row-level first-match-wins clause ordering

Fixes: DELETE overrides UPDATE regardless of order (1172); per-column clause blending (1203).

**Files:**
- Modify: `daft/io/delta_lake/_deltalake.py` (builder state, all `when_*` methods, `execute()` clause logic)
- Test: `tests/integration/delta_lake/test_distributed_merge_review_fixes.py`

**Interfaces:**
- Produces: `self._clauses: list[tuple[str, Mapping | None, Any]]` — ordered (kind, updates, predicate) with kind in {"matched_update", "matched_delete", "by_source_update", "by_source_delete", "insert"}. `execute()` computes per-category first-match clause index columns `__daft_dm_mc__` (matched), `__daft_dm_tc__` (target-only), `__daft_dm_sc__` (source-only); value -1 = no clause fired. Legacy per-kind lists are removed. Dead `_update_all`/`_insert_all` flags removed here (they were only set by these methods).

- [ ] **Step 1: Write failing tests**

```python
class TestClauseOrdering:
    def test_update_before_delete_wins(self, tmp_path):
        path = _write_base(tmp_path, {"id": [1], "v": ["old"]})
        source = daft.from_pydict({"id": [1], "v": ["new"]})
        (
            daft.distributed_merge_deltalake(table=path, source=source,
                                             predicate="target.id = source.id")
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
            daft.distributed_merge_deltalake(table=path, source=source,
                                             predicate="target.id = source.id")
            .when_matched_delete()
            .when_matched_update(updates={"v": "source.v"})
            .execute()
        )
        assert daft.read_deltalake(path).count_rows() == 0

    def test_only_first_matching_update_clause_applies(self, tmp_path):
        path = _write_base(tmp_path, {"id": [1], "a": ["a0"], "b": ["b0"], "x": [1], "y": [2]})
        source = daft.from_pydict({"id": [1], "a": ["a1"], "b": ["b1"], "x": [1], "y": [2]})
        (
            daft.distributed_merge_deltalake(table=path, source=source,
                                             predicate="target.id = source.id")
            .when_matched_update(updates={"a": "source.a"}, predicate="source.x = 1")
            .when_matched_update(updates={"b": "source.b"}, predicate="source.y = 2")
            .execute()
        )
        rows = daft.read_deltalake(path).to_pydict()
        assert rows["a"] == ["a1"]
        assert rows["b"] == ["b0"]  # second clause must NOT also fire
```

- [ ] **Step 2: Run, verify FAIL** (first: row deleted; third: b == "b1").

- [ ] **Step 3: Implement unified clause list.**

In `__init__`, replace the five per-kind lists and two flags with:

```python
        # Globally-ordered WHEN clauses: (kind, updates-or-None, predicate).
        # SQL MERGE fires the FIRST matching clause per row category.
        self._clauses: list[tuple[str, Mapping[str, Any] | None, Any]] = []
```

Each `when_*` method appends, preserving call order (updates dicts copied):

```python
    def when_matched_update(self, updates, predicate=None):
        self._clauses.append(("matched_update", dict(updates), predicate))
        return self

    def when_matched_update_all(self, predicate=None):
        self._clauses.append(("matched_update", None, predicate))
        return self

    def when_matched_delete(self, predicate=None):
        self._clauses.append(("matched_delete", None, predicate))
        return self

    def when_not_matched_insert(self, updates, predicate=None):
        self._clauses.append(("insert", dict(updates), predicate))
        return self

    def when_not_matched_insert_all(self, predicate=None):
        self._clauses.append(("insert", None, predicate))
        return self

    def when_not_matched_by_source_update(self, updates, predicate=None):
        self._clauses.append(("by_source_update", dict(updates), predicate))
        return self

    def when_not_matched_by_source_delete(self, predicate=None):
        self._clauses.append(("by_source_delete", None, predicate))
        return self
```

(Keep existing docstrings/signatures; only bodies change. `except_cols` is added in Task 8.)

In `execute()`, replace the `_clause_fires`/`deleted`/`updated`/`inserted` block (lines ~1159-1179) and the per-column output loop with clause-index logic:

```python
        # --- First-match-wins clause index per row category -------------------
        matched_clauses = [
            (i, kind, updates, pred)
            for i, (kind, updates, pred) in enumerate(self._clauses)
            if kind in ("matched_update", "matched_delete")
        ]
        by_source_clauses = [
            (i, kind, updates, pred)
            for i, (kind, updates, pred) in enumerate(self._clauses)
            if kind in ("by_source_update", "by_source_delete")
        ]
        insert_clauses = [
            (i, kind, updates, pred)
            for i, (kind, updates, pred) in enumerate(self._clauses)
            if kind == "insert"
        ]

        def _first_match_idx(clauses: list, base_cond: Any) -> Any:
            """Expression: global index of the first clause whose predicate holds, else -1."""
            expr = lit(-1)
            for i, _kind, _updates, pred in reversed(clauses):
                cond = base_cond if pred is None else (base_cond & _resolve_predicate(pred))
                expr = when(cond, lit(i)).otherwise(expr)
            return expr

        matched_idx = _first_match_idx(matched_clauses, is_matched)
        by_source_idx = _first_match_idx(by_source_clauses, is_target_only)
        insert_idx = _first_match_idx(insert_clauses, is_source_only)

        def _kind_fires(idx_expr: Any, clauses: list, kinds: "tuple[str, ...]") -> Any:
            fired = lit(False)
            for i, kind, _updates, _pred in clauses:
                if kind in kinds:
                    fired = fired | (idx_expr == lit(i))
            return fired

        deleted = _kind_fires(matched_idx, matched_clauses, ("matched_delete",)) | _kind_fires(
            by_source_idx, by_source_clauses, ("by_source_delete",)
        )
        updated = _kind_fires(matched_idx, matched_clauses, ("matched_update",)) | _kind_fires(
            by_source_idx, by_source_clauses, ("by_source_update",)
        )
        inserted = insert_idx != lit(-1)
        dropped_source = is_source_only & (insert_idx == lit(-1))
        emitted = ~deleted & ~dropped_source
        copied = emitted & ~inserted & ~updated
```

The `matched_idx`/`by_source_idx`/`insert_idx` expressions are reused in the per-column output loop, so compute them once and reference. Replace the per-column loop:

```python
        def _clause_value(updates: "Mapping[str, Any] | None", col_name: str) -> Any:
            """Value a clause assigns to col_name, or None if it leaves it alone."""
            if updates is None:  # update_all / insert_all
                return _src_ref(col_name) if col_name in suffixed else None
            if col_name in updates:
                return _resolve_update_val(updates[col_name])
            return None

        output_exprs = []
        for col_name in target_columns:
            dtype = target_schema[col_name].dtype
            default = col(col_name)
            cases: list = []
            for i, kind, updates, _pred in matched_clauses + by_source_clauses:
                if kind not in ("matched_update", "by_source_update"):
                    continue
                idx_expr = matched_idx if kind == "matched_update" else by_source_idx
                val = _clause_value(updates, col_name)
                if val is not None:
                    cases.append((idx_expr == lit(i), val))
            for i, _kind, updates, _pred in insert_clauses:
                val = _clause_value(updates, col_name)
                if val is not None:
                    cases.append((insert_idx == lit(i), val))

            if cases:
                branch = when(cases[0][0], cases[0][1])
                for cond, val in cases[1:]:
                    branch = branch.when(cond, val)
                expr = branch.otherwise(default)
            else:
                expr = default
            output_exprs.append(expr.cast(dtype).alias(col_name))
```

Delete `_matched_update_value` and `_insert_value` (replaced by `_clause_value`) and the old `_clause_fires`. `insert_all` semantics unchanged: join keys use the coalesced key column via `default`... note the key columns for source-only rows are already coalesced by the join, and `_clause_value` returns None for keys (not in `suffixed`), so `default = col(key)` carries the source key — same as before.

- [ ] **Step 4: Run tests (new + existing suite), verify PASS.**

- [ ] **Step 5: Commit** — `fix(delta): enforce SQL MERGE first-match-wins across distributed merge clauses`

---

### Task 4: Tuple-based affected partitions + temporal partition support

Fixes: cartesian-product over-rewrite (1282/1026/1326), temporal `is_in` write-pass crash (1328), microsecond partition-value serialization (1392).

**Files:**
- Modify: `daft/io/delta_lake/_deltalake.py` (`execute()` stats/write sections, `_write_partition_scoped`, `_delta_partition_value_str`)
- Test: `tests/integration/delta_lake/test_distributed_merge_review_fixes.py`

**Interfaces:**
- Produces: `affected: set[tuple]` of partition-value tuples (ordered by `partition_cols`). `_write_partition_scoped(write_df, partition_cols, affected: set[tuple], pinned_version, merge_metadata)` builds DNF removal filters `[[(pc, "=", str_v), ...] per tuple]`. Write filter is OR-of-AND equality, not `is_in`.

- [ ] **Step 1: Write failing tests**

```python
import json
import os


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
    def test_only_touched_partition_tuples_rewritten(self, tmp_path):
        path = str(tmp_path / "table")
        daft.from_pydict(
            {"y": [2023, 2023, 2024, 2024], "m": ["jan", "jun", "jan", "jun"],
             "id": [1, 2, 3, 4], "v": ["a", "b", "c", "d"]}
        ).write_deltalake(path, partition_cols=["y", "m"])
        source = daft.from_pydict({"id": [1, 4], "v": ["A", "D"], "y": [2023, 2024], "m": ["jan", "jun"]})
        result = (
            daft.distributed_merge_deltalake(table=path, source=source,
                                             predicate="target.id = source.id")
            .when_matched_update_all()
            .execute()
        ).to_pydict()
        # Untouched cross-product partitions (2023,jun) and (2024,jan) must not be rewritten.
        merge_removes = [
            a["remove"]["path"] for a in _log_actions(path) if "remove" in a
        ]
        assert not any("m=jun" in p and "y=2023" in p for p in merge_removes)
        assert not any("m=jan" in p and "y=2024" in p for p in merge_removes)
        assert result["num_target_rows_copied"][0] == 0
        rows = daft.read_deltalake(path).sort("id").to_pydict()
        assert rows["v"] == ["A", "b", "c", "D"]

    def test_timestamp_partitioned_merge(self, tmp_path):
        path = str(tmp_path / "table")
        ts1 = datetime.datetime(2024, 1, 1, 12, 0, 0)
        ts2 = datetime.datetime(2024, 6, 1, 0, 0, 0, 123456)  # microseconds!
        daft.from_pydict({"ts": [ts1, ts2], "id": [1, 2], "v": ["a", "b"]}).write_deltalake(
            path, partition_cols=["ts"]
        )
        source = daft.from_pydict({"id": [2], "v": ["B"], "ts": [ts2]})
        (
            daft.distributed_merge_deltalake(table=path, source=source,
                                             predicate="target.id = source.id")
            .when_matched_update_all()
            .execute()
        )
        rows = daft.read_deltalake(path).sort("id").to_pydict()
        # no crash, no duplicate rows, update applied
        assert rows["id"] == [1, 2]
        assert rows["v"] == ["a", "B"]
```

- [ ] **Step 2: Run, verify FAIL** (first: extra removes / copied==2; second: DaftTypeError or duplicate id=2 rows).

- [ ] **Step 3: Implement tuple-based affected set.**

Stats section — replace per-column set accumulation (lines ~1282-1305):

```python
            affected_tuples: "set[tuple] | None" = set()
            for g in groups:
                if stats["inserted"][g] + stats["updated"][g] + stats["deleted"][g] > 0:
                    affected_tuples.add(tuple(stats[pc][g] for pc in partition_cols))
                    if stats["__tp__"][g]:
                        affected_tuples.add(tuple(stats[pre_alias[pc]][g] for pc in partition_cols))

            if not _partition_values_filterable(affected_tuples):
                affected_tuples = None  # NULL/exotic values -> full overwrite

            if affected_tuples is not None:
                num_copied = 0
                num_output = 0
                for g in groups:
                    if tuple(stats[pc][g] for pc in partition_cols) in affected_tuples:
                        num_copied += stats["copied"][g]
                        num_output += stats["emitted"][g]
            else:
                num_copied = sum(stats["copied"])
                num_output = sum(stats["emitted"])
```

Change `_partition_values_filterable` to take the tuple set:

```python
def _partition_values_filterable(affected: "set[tuple]") -> bool:
    """Whether every affected partition value can be expressed as a DNF filter."""
    import datetime as _dt

    for tup in affected:
        for v in tup:
            if v is None or not isinstance(v, (str, int, bool, _dt.date)):
                return False
    return True
```

Write filter — replace `is_in` cartesian filter (lines ~1326-1329) with OR-of-AND equality over tuples:

```python
                part_filter = lit(False)
                for tup in affected_tuples:
                    tup_match = lit(True)
                    for pc, v in zip(partition_cols, tup):
                        tup_match = tup_match & (col(pc) == lit(v))
                    part_filter = part_filter | tup_match
```

`_write_partition_scoped` — DNF removal filters per tuple:

```python
        filters = [
            [(pc, "=", _delta_partition_value_str(v)) for pc, v in zip(partition_cols, tup)]
            for tup in affected
        ]
```

(`affected` parameter type becomes `set[tuple]`; update the signature and the call site.)

`_delta_partition_value_str` — preserve sub-second precision (matches observed delta-log forms: `'2024-01-01 12:00:00'` when microsecond==0, `'...12:00:00.123456'` otherwise):

```python
    if isinstance(value, _dt.datetime):  # before date: datetime is a date subclass
        if value.microsecond:
            return value.strftime("%Y-%m-%d %H:%M:%S.%f")
        return value.strftime("%Y-%m-%d %H:%M:%S")
```

Also update the docstring note (line ~626) — with tuple filters the "only partitions containing an insert, update, or delete are rewritten" claim becomes accurate; no text change needed unless wording refers to per-column values.

- [ ] **Step 4: Run tests. If the timestamp test still crashes with `DaftTypeError: ... Null, Timestamp`,** diagnose before patching further: run the failing merge with `annotated.where(...).explain(show_all=True)` to find which comparison introduces a Null-typed side; the likely culprit is `lit(None)` branches in `output_exprs` for the partition column combined with filter pushdown. Fallback fix if equality also fails: compute the partition filter on the PRE-CAST joined columns by selecting a helper string column `col(pc).cast(DataType.string())` into `annotated` and filtering on `_delta_partition_value_str` forms. Record whichever fix works in the commit message.

- [ ] **Step 5: Run new + existing suites, verify PASS.**

- [ ] **Step 6: Commit** — `fix(delta): partition-scope distributed merge writes by exact partition tuples`

---

### Task 5: Pre-commit concurrent-writer check

Fixes: silent lost update on the unpartitioned path; stale-snapshot partitioned commits (1337/1106/1029).

**Files:**
- Modify: `daft/io/delta_lake/_deltalake.py` (`execute()` write section, `_write_partition_scoped`)
- Test: `tests/integration/delta_lake/test_distributed_merge_review_fixes.py`

**Interfaces:**
- Produces: `_check_no_concurrent_commit(self, pinned_version: int) -> None` — raises `RuntimeError` if the table's latest version on storage differs from `pinned_version`. Called immediately before both commit paths. A small TOCTOU window remains (documented in the `execute` docstring); delta-rs's own delete/delete conflict detection still applies on the partition-scoped path.

- [ ] **Step 1: Write failing test**

```python
class TestConcurrency:
    def test_external_commit_between_pin_and_write_raises(self, tmp_path):
        import deltalake as dl
        import pyarrow as pa

        path = _write_base(tmp_path, {"id": [1], "v": ["a"]})
        source = daft.from_pydict({"id": [1], "v": ["A"]})
        builder = (
            daft.distributed_merge_deltalake(table=path, source=source,
                                             predicate="target.id = source.id")
            .when_matched_update_all()
        )
        # concurrent writer lands a commit the builder's pinned snapshot can't see
        dl.write_deltalake(path, pa.table({"id": [99], "v": ["zz"]}), mode="append")
        with pytest.raises(RuntimeError, match="concurrent"):
            builder.execute()
        # nothing lost: the concurrent row is still there
        assert 99 in daft.read_deltalake(path).to_pydict()["id"]
```

- [ ] **Step 2: Run, verify FAIL** (currently the overwrite silently discards id=99).

- [ ] **Step 3: Implement.** Add method:

```python
    def _check_no_concurrent_commit(self, pinned_version: int) -> None:
        """Fail fast if the table advanced past the pinned snapshot.

        The merge output was computed from ``pinned_version``; committing it
        over a newer version would silently discard the concurrent commit.
        A small TOCTOU window remains between this check and the commit.
        """
        import deltalake

        current = deltalake.DeltaTable(
            self._resolved_table.table_uri, storage_options=self._storage_options or None
        ).version()
        if current != pinned_version:
            raise RuntimeError(
                f"Concurrent modification detected: table advanced from version "
                f"{pinned_version} to {current} during the merge. Re-run the merge."
            )
```

Call it right before both commits in `execute()`:

```python
            self._check_no_concurrent_commit(pinned_version)
```

— immediately before `self._write_partition_scoped(...)` and immediately before `write_df.write_deltalake(...)`. Note this also resurrects a real use for `self._storage_options` (previously dead state — do NOT delete it in Task 9).

- [ ] **Step 4: Run new + existing suites, verify PASS.**

- [ ] **Step 5: Commit** — `fix(delta): detect concurrent commits before distributed merge writes`

---

### Task 6: Gate unsupported table features (constraints, CDF)

Fixes: silent CHECK-constraint violations and missing CDC records (1029/1337).

**Files:**
- Modify: `daft/io/delta_lake/_deltalake.py` (`execute()` entry)
- Test: `tests/integration/delta_lake/test_distributed_merge_review_fixes.py`

**Interfaces:**
- Produces: `_check_unsupported_table_features(self) -> None` — raises `ValueError` naming the feature when the target has `delta.constraints.*` keys, `delta.enableChangeDataFeed=true`, or writer features `checkConstraints`/`changeDataFeed`/`invariants`/`generatedColumns`. Called at the top of `execute()`.

- [ ] **Step 1: Write failing tests**

```python
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
                daft.distributed_merge_deltalake(table=path, source=source,
                                                 predicate="target.id = source.id")
                .when_matched_update_all()
                .execute()
            )

    def test_cdf_table_rejected(self, tmp_path):
        import deltalake as dl
        import pyarrow as pa

        path = str(tmp_path / "table")
        dl.write_deltalake(
            path, pa.table({"id": [1], "v": [5]}),
            configuration={"delta.enableChangeDataFeed": "true"},
        )
        source = daft.from_pydict({"id": [1], "v": [6]})
        with pytest.raises(ValueError, match="[Cc]hange [Dd]ata [Ff]eed"):
            (
                daft.distributed_merge_deltalake(table=path, source=source,
                                                 predicate="target.id = source.id")
                .when_matched_update_all()
                .execute()
            )
```

- [ ] **Step 2: Run, verify FAIL** (currently commits silently).

- [ ] **Step 3: Implement.** Add method and call it first in `execute()`:

```python
    def _check_unsupported_table_features(self) -> None:
        """Distributed merge writes raw files + a manual commit, bypassing
        delta-rs validation, so tables relying on write-time enforcement must
        be rejected loudly rather than silently corrupted."""
        config = dict(self._resolved_table.metadata().configuration or {})
        constraint_keys = [k for k in config if k.lower().startswith("delta.constraints.")]
        if constraint_keys:
            raise ValueError(
                "distributed_merge_deltalake does not support tables with CHECK "
                f"constraints ({constraint_keys}); use merge_deltalake instead."
            )
        if config.get("delta.enableChangeDataFeed", "").lower() == "true":
            raise ValueError(
                "distributed_merge_deltalake does not support tables with Change Data "
                "Feed enabled (no CDC files would be written); use merge_deltalake instead."
            )
        protocol = self._resolved_table.protocol()
        writer_features = set(getattr(protocol, "writer_features", None) or [])
        unsupported = writer_features & {"checkConstraints", "changeDataFeed", "invariants", "generatedColumns"}
        if unsupported:
            raise ValueError(
                f"distributed_merge_deltalake does not support tables with writer "
                f"feature(s) {sorted(unsupported)}; use merge_deltalake instead."
            )
```

Note: `invariants` appears in writer_features on many plain tables written with min_writer_version 7 — verify empirically what daft/delta-rs-written test tables report; if plain tables carry `invariants` in writer_features, only reject when the SCHEMA actually contains invariant metadata (check `delta.invariants` in field metadata) — adjust the set accordingly so existing tests keep passing.

- [ ] **Step 4: Run new + existing suites, verify PASS** (watch for false-positive gating breaking existing tests; adjust per the note above).

- [ ] **Step 5: Commit** — `fix(delta): reject distributed merge on tables with constraints or CDF`

---

### Task 7: Real metrics

Fixes: hardcoded 0 file/timing metrics, `num_source_rows` overcount (1353/1347).

**Files:**
- Modify: `daft/io/delta_lake/_deltalake.py` (`execute()`, `_write_partition_scoped`)
- Test: `tests/integration/delta_lake/test_distributed_merge_review_fixes.py`

**Interfaces:**
- Produces: `_write_partition_scoped(...) -> tuple[int, int]` returns `(files_added, files_removed)`; the unpartitioned path computes files_added from the table's post-write log delta and files_removed as the pinned snapshot's file count. `execute()` measures `execution_time_ms` (whole call), `scan_time_ms` (stats pass), `rewrite_time_ms` (write pass) with `time.monotonic()`. `num_source_rows` uses `source.count_rows()` when `materialize_source=True` (exact even with duplicate target keys); the lazy path keeps the join-derived count (documented).

- [ ] **Step 1: Write failing test**

```python
class TestMetrics:
    def test_file_and_timing_metrics_populated(self, tmp_path):
        path = _write_base(tmp_path, {"id": [1, 2], "v": ["a", "b"]})
        source = daft.from_pydict({"id": [2, 3], "v": ["B", "c"]})
        result = (
            daft.distributed_merge_deltalake(table=path, source=source,
                                             predicate="target.id = source.id")
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        ).to_pydict()
        assert result["num_target_files_added"][0] >= 1
        assert result["num_target_files_removed"][0] >= 1
        assert result["execution_time_ms"][0] > 0

    def test_num_source_rows_exact_with_duplicate_target_keys(self, tmp_path):
        path = _write_base(tmp_path, {"id": [1, 1], "v": ["a", "b"]})  # duplicate target keys
        source = daft.from_pydict({"id": [1], "v": ["X"]})
        result = (
            daft.distributed_merge_deltalake(table=path, source=source,
                                             predicate="target.id = source.id")
            .when_matched_update_all()
            .execute()
        ).to_pydict()
        assert result["num_source_rows"][0] == 1  # was 2 (join-row count)
```

- [ ] **Step 2: Run, verify FAIL.**

- [ ] **Step 3: Implement.**

In `_write_partition_scoped`, count removals before committing and return counts:

```python
        removed_files = len(self._resolved_table._table.files(filters))  # files matched by the DNF removal filter
        ...
        return len(add_actions), removed_files
```

(If `RawDeltaTable.files(filters)` rejects the DNF form, use `self._resolved_table.file_uris(partition_filters=filters)` — verify which accepts list-of-list DNF and use that one.)

Unpartitioned path: `files_removed = len(self._resolved_table.file_uris())` (pinned snapshot files, all replaced by the overwrite); `files_added`: re-open the table after the write and count files in the new snapshot: `len(deltalake.DeltaTable(uri, storage_options=...).file_uris())`.

Timing + source count in `execute()`:

```python
        import time
        t0 = time.monotonic()
        ...
        num_source_rows = None
        if self._materialize_source:
            num_source_rows = source.count_rows()
        ...
        t_stats0 = time.monotonic()
        # (stats pass)
        scan_time_ms = int((time.monotonic() - t_stats0) * 1000)
        ...
        t_write0 = time.monotonic()
        # (write pass)
        rewrite_time_ms = int((time.monotonic() - t_write0) * 1000)
        ...
        raw_metrics = {
            "num_source_rows": num_source_rows if num_source_rows is not None else num_matched + num_source_only,
            ...
            "num_target_files_added": files_added,
            "num_target_files_removed": files_removed,
            "execution_time_ms": int((time.monotonic() - t0) * 1000),
            "scan_time_ms": scan_time_ms,
            "rewrite_time_ms": rewrite_time_ms,
        }
```

(`files_added = files_removed = 0` when the merge modifies nothing and skips the commit.)

- [ ] **Step 4: Run new + existing suites, verify PASS** (existing suite asserts on some metrics — check `test_distributed_merge_fixes.py` expectations still hold).

- [ ] **Step 5: Commit** — `fix(delta): report real file/timing metrics from distributed merge`

---

### Task 8: `except_cols` on `*_all` clauses

Fixes: API parity with `DeltaMergeBuilder` (875/913).

**Files:**
- Modify: `daft/io/delta_lake/_deltalake.py` (`when_matched_update_all`, `when_not_matched_insert_all`, `_clause_value`)
- Test: `tests/integration/delta_lake/test_distributed_merge_review_fixes.py`

**Interfaces:**
- Produces: `when_matched_update_all(predicate=None, except_cols: list[str] | None = None)`, `when_not_matched_insert_all(predicate=None, except_cols: list[str] | None = None)`. Internally an `*_all` clause stores `updates=None` plus a frozen `except_cols` set; clause tuples become 4-tuples `(kind, updates, predicate, except_cols)` throughout (plain clauses store `except_cols=None`).

- [ ] **Step 1: Write failing test**

```python
class TestExceptCols:
    def test_update_all_except_cols(self, tmp_path):
        path = _write_base(tmp_path, {"id": [1], "v": ["old"], "audit": ["keep"]})
        source = daft.from_pydict({"id": [1], "v": ["new"], "audit": ["clobber"]})
        (
            daft.distributed_merge_deltalake(table=path, source=source,
                                             predicate="target.id = source.id")
            .when_matched_update_all(except_cols=["audit"])
            .execute()
        )
        rows = daft.read_deltalake(path).to_pydict()
        assert rows["v"] == ["new"]
        assert rows["audit"] == ["keep"]

    def test_insert_all_except_cols(self, tmp_path):
        path = _write_base(tmp_path, {"id": [1], "v": ["a"], "audit": ["x"]})
        source = daft.from_pydict({"id": [2], "v": ["b"], "audit": ["y"]})
        (
            daft.distributed_merge_deltalake(table=path, source=source,
                                             predicate="target.id = source.id")
            .when_not_matched_insert_all(except_cols=["audit"])
            .execute()
        )
        rows = daft.read_deltalake(path).sort("id").to_pydict()
        assert rows["audit"] == ["x", None]
```

- [ ] **Step 2: Run, verify FAIL** (TypeError: unexpected keyword argument).

- [ ] **Step 3: Implement** — widen clause tuples to `(kind, updates, predicate, except_cols)` (all appends pass `None` except the two `*_all` methods), add the parameter with a docstring matching `DeltaMergeBuilder`'s ("List of columns to exclude from update/insert."), and honor it in `_clause_value`:

```python
        def _clause_value(updates, col_name, except_cols=None):
            if updates is None:  # update_all / insert_all
                if except_cols and col_name in except_cols:
                    return None if_update_all_else_null  # see below
                return _src_ref(col_name) if col_name in suffixed else None
            ...
```

Semantics: for `update_all`, an excluded column keeps the target value (return `None` → falls to `default`). For `insert_all`, an excluded column must be NULL on inserted rows — which is also what `default = col(col_name)` yields on source-only rows (target side is NULL) — so returning `None` is correct for both. Simplify to:

```python
        def _clause_value(updates, col_name, except_cols=None):
            if updates is None:
                if except_cols and col_name in except_cols:
                    return None
                return _src_ref(col_name) if col_name in suffixed else None
            if col_name in updates:
                return _resolve_update_val(updates[col_name])
            return None
```

- [ ] **Step 4: Run new + existing suites, verify PASS.**

- [ ] **Step 5: Commit** — `feat(delta): support except_cols in distributed merge *_all clauses`

---

### Task 9: Cleanups and latent compatibility

Fixes below-cut findings: dead state, duplicated suffix logic, deltalake<1.0.0 schema type, operationMetrics on the partition-scoped commit.

**Files:**
- Modify: `daft/io/delta_lake/_deltalake.py`
- Test: existing suites (behavior-preserving except commitInfo)

**Interfaces:**
- Consumes: Task 3 removed `_matched_update_value`/`_insert_value` and the per-kind clause lists; Task 5 made `_storage_options` live again (keep it).
- Produces: `source_col()` and `execute()`'s `_src_ref` share one helper `_suffixed_ref(name, on, collision_set) -> Expression`.

- [ ] **Step 1: Remove dead state** — delete `self._insert_all`/`self._update_all` if any assignment survived Task 3 (grep first: `grep -n "_insert_all\|_update_all" daft/io/delta_lake/_deltalake.py` — only `DeltaMergeBuilder` occurrences may remain).

- [ ] **Step 2: Unify suffix logic** — module-level helper; `source_col` and `_src_ref` both call it:

```python
def _suffixed_ref(name: str, on: "list[str]", collision_names: "set[str] | list[str]") -> Any:
    from daft import col

    if name in on:
        return col(name)
    if name in collision_names:
        return col(f"{name}.__src__")
    return col(name)
```

- [ ] **Step 3: deltalake<1.0.0 schema branch in `_write_partition_scoped`** (mirror `dataframe.py:2150-2162`):

```python
        from packaging.version import parse

        import deltalake

        if parse(deltalake.__version__) < parse("1.0.0"):
            txn_schema = delta_schema_to_pyarrow(self._resolved_table.schema())
        else:
            txn_schema = self._resolved_table.schema()
        self._resolved_table._table.create_write_transaction(
            add_actions, "overwrite", list(partition_cols), txn_schema, filters,
            CommitProperties(custom_metadata=merge_metadata),
        )
```

- [ ] **Step 4: operationMetrics on the partition-scoped commit** — build the metadata like `dataframe.py._merge_delta_custom_metadata` does (inline; the helper is module-private in dataframe.py):

```python
        import json

        merge_metadata = {**(self._custom_metadata or {}), "daft.operation": "DISTRIBUTED_MERGE"}
        operation_metrics = {
            "num_added_files": len(add_actions),
            "num_removed_files": removed_files,
        }
        merge_metadata["operationMetrics"] = json.dumps(operation_metrics)
```

(Move the `merge_metadata` construction into `_write_partition_scoped` where the counts exist, or pass counts back — whichever Task 7 left cleaner.)

- [ ] **Step 5: Run BOTH new and all three existing distributed-merge test files, verify PASS.**

Run: `DAFT_RUNNER=native .venv/bin/pytest -o addopts="" -q tests/integration/delta_lake/test_distributed_merge_review_fixes.py tests/integration/delta_lake/test_distributed_merge_fixes.py tests/integration/delta_lake/test_distributed_merge.py`

- [ ] **Step 6: Commit** — `refactor(delta): dedupe suffix logic, old-deltalake compat, commit operationMetrics`

---

### Task 10: Full regression + docstring truth pass

**Files:**
- Modify: `daft/io/delta_lake/_deltalake.py` (docstrings only)

- [ ] **Step 1: Docstring updates** in `distributed_merge_deltalake` and `execute()`:
  - Note that clause predicates/update expressions are parsed with Daft SQL (full expression support) and that unparseable input raises `ValueError`.
  - Note NULL predicates are treated as not-satisfied (SQL semantics).
  - Note concurrent commits are detected pre-commit and raise `RuntimeError`.
  - Note tables with CHECK constraints / CDF / invariants are rejected.
  - Confirm the "only partitions containing an insert, update, or delete are rewritten" sentence is now accurate (tuple scoping) — keep it.

- [ ] **Step 2: Full run** of all distributed-merge suites plus the merge e2e file:

`DAFT_RUNNER=native .venv/bin/pytest -o addopts="" -q tests/integration/delta_lake/test_distributed_merge_review_fixes.py tests/integration/delta_lake/test_distributed_merge_fixes.py tests/integration/delta_lake/test_distributed_merge.py tests/integration/delta_lake/test_merge_e2e.py`
Expected: PASS (no skips beyond pre-existing ones).

- [ ] **Step 3: Commit** — `docs(delta): document distributed merge parsing, NULL, and concurrency semantics`

---

## Self-Review

- **Spec coverage:** findings 1,2 (Task 1), 3 (Task 3-M1), 4 (Task 2), 5,6 (Task 4), 7 (Task 5), 8 (Task 3-S2), 9 (Task 1), 10 (Task 6), 11 (Task 1), 12 (Task 4), 13 (Tasks 2/3/7), 14 (Task 8), 15 (Task 1). Below-cut cleanups: Task 9. ✔
- **Placeholder scan:** Task 4 Step 4 contains a contingency (diagnose-then-fix) because the temporal crash's root cause is plan-context-dependent — the failing test defines done. Task 7 Step 3 has a verify-which-API note for DNF file counting. Both give the concrete fallback code path. ✔
- **Type consistency:** clause tuples are 3-tuples after Task 3 and widen to 4-tuples in Task 8 — Task 8 explicitly owns that migration. `affected` becomes `set[tuple]` in Task 4 and `_write_partition_scoped` returns `(int, int)` from Task 7 — call sites updated in the same tasks. ✔

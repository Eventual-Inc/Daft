"""Tests for dialect-aware percentile AST rewriting in SQLConnection."""

from __future__ import annotations

import pytest

# Module-level rewrite functions — these are pure functions with no
# daft.daft dependency, so they can be tested in isolation.
from daft.sql.sql_connection import (
    _adapt_projection_for_dialect,
    _rewrite_percentile_to_clickhouse,
    _rewrite_percentile_to_tsql,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_percentile_node(pct: float, col: str = "col", alias: str | None = None):
    """Build a dialect-neutral WithinGroup(PercentileDisc(pct), Order(col))."""
    import sqlglot.expressions as exp

    node = exp.WithinGroup(
        this=exp.PercentileDisc(this=exp.Literal.number(pct)),
        expression=exp.Order(expressions=[exp.Ordered(this=exp.Column(this=col))]),
    )
    if alias is not None:
        node = node.as_(alias)
    return node


def _sql_for_dialect(expr, dialect: str) -> str:
    return expr.sql(dialect=dialect)


# ---------------------------------------------------------------------------
# _rewrite_percentile_to_clickhouse — unit tests
# ---------------------------------------------------------------------------


class TestRewritePercentileToClickHouse:
    def test_basic_rewrite(self):
        node = _make_percentile_node(0.5, "col", "bound_0")
        result = node.transform(_rewrite_percentile_to_clickhouse)
        sql = _sql_for_dialect(result, "clickhouse")
        assert "quantileExactLow(0.5)(col) AS bound_0" in sql
        assert "PERCENTILE_DISC" not in sql

    def test_multiple_percentiles(self):
        for pct in (0.0, 0.1, 0.5, 1.0):
            node = _make_percentile_node(pct, "x", f"b_{pct}")
            result = node.transform(_rewrite_percentile_to_clickhouse)
            sql = _sql_for_dialect(result, "clickhouse")
            assert f"quantileExactLow({pct!r})" in sql or f"quantileExactLow({pct})" in sql

    def test_alias_preserved(self):
        """The parent Alias wrapper must survive the rewrite."""
        node = _make_percentile_node(0.2, "col", "my_bound")
        result = node.transform(_rewrite_percentile_to_clickhouse)
        import sqlglot.expressions as exp

        assert isinstance(result, exp.Alias)
        assert result.alias == "my_bound"

    def test_non_within_group_passes_through(self):
        import sqlglot.expressions as exp

        node = exp.Literal.number(42)
        result = node.transform(_rewrite_percentile_to_clickhouse)
        assert isinstance(result, exp.Literal)

    def test_multi_column_order_by_raises(self):
        import sqlglot.expressions as exp

        node = exp.WithinGroup(
            this=exp.PercentileDisc(this=exp.Literal.number(0.5)),
            expression=exp.Order(
                expressions=[
                    exp.Ordered(this=exp.Column(this="a")),
                    exp.Ordered(this=exp.Column(this="b")),
                ]
            ),
        )
        with pytest.raises(ValueError, match="Expected exactly one ORDER BY"):
            node.transform(_rewrite_percentile_to_clickhouse)

    def test_desc_order_raises(self):
        import sqlglot.expressions as exp

        node = exp.WithinGroup(
            this=exp.PercentileDisc(this=exp.Literal.number(0.5)),
            expression=exp.Order(
                expressions=[
                    exp.Ordered(this=exp.Column(this="col"), desc=True),
                ]
            ),
        )
        with pytest.raises(ValueError, match="DESC ordering"):
            node.transform(_rewrite_percentile_to_clickhouse)


# ---------------------------------------------------------------------------
# _rewrite_percentile_to_tsql — unit tests
# ---------------------------------------------------------------------------


class TestRewritePercentileToTSQL:
    def test_adds_over_clause(self):
        node = _make_percentile_node(0.5, "col", "bound_0")
        result = node.transform(_rewrite_percentile_to_tsql)
        sql = _sql_for_dialect(result, "tsql")
        assert "OVER ()" in sql
        assert "PERCENTILE_DISC" in sql

    def test_non_within_group_passes_through(self):
        import sqlglot.expressions as exp

        node = exp.Literal.number(42)
        result = node.transform(_rewrite_percentile_to_tsql)
        assert isinstance(result, exp.Literal)


# ---------------------------------------------------------------------------
# _adapt_projection_for_dialect — integration
# ---------------------------------------------------------------------------


class TestAdaptProjectionForDialect:
    def test_clickhouse_dialect_converts_all(self):
        projection = [
            _make_percentile_node(0.0, "col", "bound_0"),
            _make_percentile_node(0.5, "col", "bound_1"),
            _make_percentile_node(1.0, "col", "bound_2"),
        ]
        adapted = _adapt_projection_for_dialect(projection, "clickhouse")
        for p in adapted:
            sql = _sql_for_dialect(p, "clickhouse")
            assert "quantileExactLow" in sql, f"Expected quantileExactLow in: {sql}"
            assert "PERCENTILE_DISC" not in sql, f"PERCENTILE_DISC should not appear: {sql}"

    def test_tsql_dialect_adds_over(self):
        projection = [
            _make_percentile_node(0.5, "col", "bound_0"),
        ]
        adapted = _adapt_projection_for_dialect(projection, "tsql")
        sql = _sql_for_dialect(adapted[0], "tsql")
        assert "OVER ()" in sql

    def test_postgres_passes_through(self):
        projection = [
            _make_percentile_node(0.5, "col", "bound_0"),
        ]
        adapted = _adapt_projection_for_dialect(projection, "postgres")
        sql = _sql_for_dialect(adapted[0], "postgres")
        assert "PERCENTILE_DISC" in sql
        assert "OVER ()" not in sql
        assert "quantileExactLow" not in sql

    def test_string_projection_passes_through(self):
        projection = ["COUNT(*)", _make_percentile_node(0.5, "col", "b0")]
        adapted = _adapt_projection_for_dialect(projection, "clickhouse")
        assert adapted[0] == "COUNT(*)"
        sql = _sql_for_dialect(adapted[1], "clickhouse")
        assert "quantileExactLow" in sql


# ---------------------------------------------------------------------------
# construct_sql_query — end-to-end dialect tests
# ---------------------------------------------------------------------------


class TestConstructSQLQueryDialect:
    """Test SQLConnection.construct_sql_query end-to-end with real SQLGlot."""

    @pytest.mark.parametrize(
        "dialect,expect_contains,expect_not_contains",
        [
            pytest.param("clickhousedb", "quantileExactLow", "PERCENTILE_DISC", id="clickhouse"),
            pytest.param("mssql", "OVER ()", "quantileExactLow", id="mssql"),
            pytest.param("postgresql", "PERCENTILE_DISC", "quantileExactLow", id="postgresql"),
        ],
    )
    def test_dialect_specific_output(self, dialect, expect_contains, expect_not_contains):
        from daft.sql.sql_connection import SQLConnection

        projection = [
            _make_percentile_node(0.5, "col", "bound_0"),
        ]
        conn = SQLConnection("sqlite://", driver="", dialect=dialect, url="sqlite://")
        sql = conn.construct_sql_query("SELECT 1 AS col", projection=projection, limit=1)
        assert expect_contains in sql, f"Expected '{expect_contains}' in: {sql}"
        assert expect_not_contains not in sql, f"'{expect_not_contains}' should not appear in: {sql}"
        assert "bound_0" in sql

    def test_string_projection_unchanged(self):
        from daft.sql.sql_connection import SQLConnection

        conn = SQLConnection("sqlite://", driver="", dialect="clickhousedb", url="sqlite://")
        sql = conn.construct_sql_query("SELECT 1 AS a", projection=["COUNT(*)"], limit=1)
        assert "COUNT(*)" in sql
        assert "quantileExactLow" not in sql

    def test_mixed_projection(self):
        from daft.sql.sql_connection import SQLConnection

        projection = [
            "COUNT(*)",
            _make_percentile_node(0.5, "col", "median"),
        ]
        conn = SQLConnection("sqlite://", driver="", dialect="clickhousedb", url="sqlite://")
        sql = conn.construct_sql_query("SELECT 1 AS col", projection=projection, limit=1)
        assert "COUNT(*)" in sql
        assert "quantileExactLow(0.5)(col) AS median" in sql


# ---------------------------------------------------------------------------
# ClickHouse integration tests (require Docker — manual or CI with service)
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(
    "not config.getoption('--clickhouse', False)",
    reason="ClickHouse integration tests require a running ClickHouse instance",
)
class TestClickHouseIntegration:
    """End-to-end tests against a real ClickHouse instance.

    Run with: pytest --clickhouse tests/sql/test_clickhouse_percentile.py
    """

    @pytest.fixture(scope="class")
    def ch_conn(self):
        from daft.sql.sql_connection import SQLConnection

        return SQLConnection(
            "clickhouse://default:@localhost:8123/default",
            driver="",
            dialect="clickhousedb",
            url="clickhouse://default:@localhost:8123/default",
        )

    def test_percentile_syntax_executable(self, ch_conn):
        """QuantileExactLow SQL must execute without syntax errors."""
        projection = [
            _make_percentile_node(0.0, "number", "b0"),
            _make_percentile_node(0.5, "number", "b1"),
            _make_percentile_node(1.0, "number", "b2"),
        ]
        sql = ch_conn.construct_sql_query(
            "SELECT number FROM numbers(10)",
            projection=projection,
            limit=1,
        )
        result = ch_conn.execute_sql_query(sql)
        assert result.num_rows == 1
        assert result.num_columns == 3

    def test_min_max_correct(self, ch_conn):
        """quantileExactLow(0) and quantileExactLow(1) must return actual min/max."""
        projection = [
            _make_percentile_node(0.0, "number", "min"),
            _make_percentile_node(1.0, "number", "max"),
        ]
        sql = ch_conn.construct_sql_query(
            "SELECT number FROM numbers(100)",
            projection=projection,
            limit=1,
        )
        result = ch_conn.execute_sql_query(sql)
        pydict = result.to_pydict()
        assert pydict["min"][0] == 0
        assert pydict["max"][0] == 99

    def test_percentile_01_and_05(self, ch_conn):
        """Verify quantileExactLow at 0.1 and 0.5 levels."""
        projection = [
            _make_percentile_node(0.0, "number", "b0"),
            _make_percentile_node(0.1, "number", "b1"),
            _make_percentile_node(0.5, "number", "b2"),
            _make_percentile_node(1.0, "number", "b3"),
        ]
        sql = ch_conn.construct_sql_query(
            "SELECT number FROM numbers(10)",
            projection=projection,
            limit=1,
        )
        result = ch_conn.execute_sql_query(sql)
        pydict = result.to_pydict()
        assert pydict["b0"][0] == 0
        assert pydict["b1"][0] == 1  # quantileExactLow(0.1) on 0..9 = 1
        assert pydict["b2"][0] == 4  # quantileExactLow(0.5) on 0..9 = 4
        assert pydict["b3"][0] == 9

    def test_duplicate_values(self, ch_conn):
        """QuantileExactLow with repeated values must still produce valid bounds."""
        projection = [
            _make_percentile_node(0.0, "x", "b0"),
            _make_percentile_node(0.5, "x", "b1"),
            _make_percentile_node(1.0, "x", "b2"),
        ]
        sql = ch_conn.construct_sql_query(
            "SELECT * FROM ("
            "SELECT 1 AS x UNION ALL SELECT 1 UNION ALL SELECT 5 "
            "UNION ALL SELECT 5 UNION ALL SELECT 5 UNION ALL SELECT 5"
            ")",
            projection=projection,
            limit=1,
        )
        result = ch_conn.execute_sql_query(sql)
        pydict = result.to_pydict()
        assert pydict["b0"][0] == 1
        assert pydict["b2"][0] == 5
        # bounds must be non-decreasing
        assert pydict["b0"][0] <= pydict["b1"][0] <= pydict["b2"][0]

    def test_bounds_non_decreasing(self, ch_conn):
        """All partition bounds must be non-decreasing."""
        N = 5
        percentiles = [i / N for i in range(N + 1)]
        projection = [_make_percentile_node(pct, "number", f"b{i}") for i, pct in enumerate(percentiles)]
        sql = ch_conn.construct_sql_query(
            "SELECT number FROM numbers(97)",
            projection=projection,
            limit=1,
        )
        result = ch_conn.execute_sql_query(sql)
        pydict = result.to_pydict()
        bounds = [pydict[f"b{i}"][0] for i in range(N + 1)]
        for i in range(len(bounds) - 1):
            assert bounds[i] <= bounds[i + 1], f"Bounds not non-decreasing: {bounds}"

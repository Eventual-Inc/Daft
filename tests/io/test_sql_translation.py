"""Tests for expression SQL translation (Expr.to_sql / Literal.display_sql)."""

from __future__ import annotations

import datetime
import uuid
from decimal import Decimal

from daft.expressions import col, lit


class TestLiteralSqlTranslation:
    """Test Literal.display_sql for various types."""

    def test_int_types(self):
        assert lit(42)._to_sql() == "42"
        assert lit(-1)._to_sql() == "-1"
        assert lit(0)._to_sql() == "0"

    def test_float_types(self):
        assert lit(3.14)._to_sql() == "3.14"
        assert lit(-0.5)._to_sql() == "-0.5"

    def test_boolean(self):
        assert lit(True)._to_sql() == "true"
        assert lit(False)._to_sql() == "false"

    def test_null(self):
        assert lit(None)._to_sql() == "NULL"

    def test_date(self):
        assert lit(datetime.date(2024, 1, 15))._to_sql() is not None

    def test_decimal(self):
        """B6: Decimal literals should produce valid SQL."""
        result = lit(Decimal("123.45"))._to_sql()
        assert result is not None
        assert "123.45" in result

    def test_decimal_integer(self):
        result = lit(Decimal(100))._to_sql()
        assert result is not None
        assert "100" in result

    def test_decimal_scientific_notation_does_not_panic(self):
        """Decimal with scientific notation should not panic.

        Python Decimal('1.5E+3') normalizes to exponent=2, so Daft
        sets scale=0 (via the from_pyobj normalizer in lit/python.rs).
        This test verifies
        the conversion doesn't panic, even though it doesn't exercise
        the negative-scale guard (that path is only reachable from
        Arrow data with negative-scale Decimal128 schema).
        """
        result = lit(Decimal("1.5E+3"))._to_sql()
        assert result is not None
        assert isinstance(result, str)

    def test_uuid_python_lit_returns_none(self):
        """Python uuid.UUID does not map to Lit::Uuid in from_pyobj.

        Python uuid.UUID objects fall through the generic branch of
        Literal::from_pyobj (lit/python.rs) and become a Python-typed
        literal, not Lit::Uuid. The Lit::Uuid display_sql arm added in
        lit/mod.rs serves the Arrow→Lit path (get_lit.rs) for UUID
        columns read from Parquet/Iceberg files, not Python lit().
        """
        val = uuid.UUID("12345678-1234-5678-1234-567812345678")
        result = lit(val)._to_sql()
        assert result is None  # current limitation — Python UUID → Lit::Uuid not wired


class TestIsInSqlTranslation:
    """Test Expr.is_in SQL generation."""

    def test_is_in_integers(self):
        """B2: is_in should generate col IN (v1, v2, v3)."""
        expr = col("x").is_in([lit(1), lit(2), lit(3)])
        sql = expr._to_sql()
        assert sql is not None
        assert "IN" in sql
        assert "1" in sql
        assert "2" in sql
        assert "3" in sql

    def test_is_in_strings(self):
        expr = col("name").is_in([lit("alice"), lit("bob")])
        sql = expr._to_sql()
        assert sql is not None
        assert "IN" in sql
        assert "'alice'" in sql
        assert "'bob'" in sql

    def test_is_in_single_item(self):
        expr = col("x").is_in([lit(42)])
        sql = expr._to_sql()
        assert sql is not None
        assert "IN" in sql
        assert "42" in sql

    def test_is_in_empty(self):
        """Empty is_in([]) should emit constant-false, not invalid IN ()."""
        expr = col("x").is_in([])
        sql = expr._to_sql()
        assert sql is not None
        assert sql == "(1 = 0)"
        assert "IN ()" not in sql  # must not generate invalid SQL


class TestBinaryOpSqlTranslation:
    """Test BinaryOp SQL generation (existing functionality)."""

    def test_eq(self):
        assert (col("x") == lit(1))._to_sql() == '"x" = 1'

    def test_neq(self):
        assert (col("x") != lit(1))._to_sql() == '"x" != 1'

    def test_lt(self):
        assert (col("x") < lit(1))._to_sql() == '"x" < 1'

    def test_lte(self):
        assert (col("x") <= lit(1))._to_sql() == '"x" <= 1'

    def test_gt(self):
        assert (col("x") > lit(1))._to_sql() == '"x" > 1'

    def test_gte(self):
        assert (col("x") >= lit(1))._to_sql() == '"x" >= 1'

    def test_and(self):
        expr = (col("x") > lit(1)) & (col("y") < lit(10))
        sql = expr._to_sql()
        assert sql is not None
        assert "AND" in sql

    def test_or(self):
        expr = (col("x") > lit(1)) | (col("y") < lit(10))
        sql = expr._to_sql()
        assert sql is not None
        assert "OR" in sql


class TestIdentifierQuoting:
    """Verify column identifiers are properly quoted for SQL safety."""

    def test_space_in_name(self):
        sql = (col("full name") == lit(1))._to_sql()
        assert sql is not None
        assert '"full name"' in sql

    def test_embedded_double_quote(self):
        sql = (col('x"y') == lit(1))._to_sql()
        assert sql is not None
        # SQL-standard escaping: embedded " becomes ""
        assert '"x""y"' in sql

    def test_hyphen_in_name(self):
        sql = (col("my-col") == lit(1))._to_sql()
        assert sql is not None
        assert '"my-col"' in sql

    def test_reserved_word_column(self):
        sql = (col("SELECT") == lit(1))._to_sql()
        assert sql is not None
        assert '"SELECT"' in sql

    def test_special_names_survive_construct_sql_query(self):
        """Quoted identifiers must survive sqlglot parse→render round-trip."""
        from daft.sql.sql_connection import SQLConnection

        conn = SQLConnection.from_url("mysql://u:p@h/db")
        # "full name" should become `full name` in MySQL dialect
        predicate = (col("full name") == lit(1))._to_sql()
        assert predicate is not None
        query = conn.construct_sql_query("SELECT * FROM t", predicate=predicate)
        # sqlglot renders MySQL identifiers with backticks
        assert "full name" in query


class TestUnarySqlTranslation:
    """Test unary expression SQL generation."""

    def test_not(self):
        expr = ~(col("x") == lit(1))
        sql = expr._to_sql()
        assert sql is not None
        assert "NOT" in sql

    def test_is_null(self):
        expr = col("x").is_null()
        sql = expr._to_sql()
        assert sql is not None
        assert "IS NULL" in sql

    def test_not_null(self):
        expr = col("x").not_null()
        sql = expr._to_sql()
        assert sql is not None
        assert "IS NOT NULL" in sql

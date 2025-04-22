from __future__ import annotations

from typing import TYPE_CHECKING

from daft.daft import Pushdowns as PyPushdowns
from daft.expressions import col, lit
from daft.io.pushdowns import Expr, Reference, Term
from daft.logical.schema import Schema

if TYPE_CHECKING:
    from daft.expressions import Expression


def _term(expr: Expression, schema: Schema | None = None) -> Term:
    return PyPushdowns._to_term(expr._expr, schema._schema if schema else None)


def test_iceberg_predicates():
    """Test we can translate each one of the Iceberg predicates.

    References:
        - https://iceberg.apache.org/javadoc/1.0.0/index.html?org/apache/iceberg/expressions/Expressions.html
        - https://py.iceberg.apache.org/reference/pyiceberg/expressions/
    """
    assert _term(~col("a")) == Expr("not", Reference("a"))

    assert _term(lit(True) & lit(False)) == Expr("and", True, False)
    assert _term(lit(True) | lit(False)) == Expr("or", True, False)

    assert _term(lit(1) == lit(2)) == Expr("=", 1, 2)
    assert _term(lit(1) != lit(2)) == Expr("!=", 1, 2)
    assert _term(lit(1) < lit(2)) == Expr("<", 1, 2)
    assert _term(lit(1) <= lit(2)) == Expr("<=", 1, 2)
    assert _term(lit(1) > lit(2)) == Expr(">", 1, 2)
    assert _term(lit(1) >= lit(2)) == Expr(">=", 1, 2)

    assert _term(col("a").is_null()) == Expr("is_null", Reference("a"))
    assert _term(col("a").not_null()) == Expr("not_null", Reference("a"))

    assert _term(col("a").float.is_nan()) == Expr("is_nan", Reference("a"))
    assert _term(col("a").float.not_nan()) == Expr("not_nan", Reference("a"))

    items = [1, 2, 3]
    assert _term(col("a").is_in(items)) == Expr("is_in", Reference("a"), Expr("list", 1, 2, 3))
    assert _term(~(col("a").is_in(items))) == Expr("not", Expr("is_in", Reference("a"), Expr("list", 1, 2, 3)))

    assert _term(col("a").str.startswith("xyz")) == Expr("startswith", Reference("a"), "xyz")
    assert _term(~(col("a").str.startswith("xyz"))) == Expr("not", Expr("startswith", Reference("a"), "xyz"))


def test_iceberg_transforms():
    """Test we can translate each one of the Iceberg partition expressions.

    References:
        - https://iceberg.apache.org/javadoc/1.0.0/index.html?org/apache/iceberg/transforms/Transforms.html
        - https://py.iceberg.apache.org/reference/pyiceberg/transforms/
    """
    assert _term(col("a").partitioning.years()) == Expr("years", Reference("a"))
    assert _term(col("a").partitioning.months()) == Expr("months", Reference("a"))
    assert _term(col("a").partitioning.days()) == Expr("days", Reference("a"))
    assert _term(col("a").partitioning.hours()) == Expr("hours", Reference("a"))

    # need args from functions
    assert _term(col("a").partitioning.iceberg_bucket(16)) == Expr("iceberg_bucket", Reference("a"), 16)
    assert _term(col("a").partitioning.iceberg_truncate(10)) == Expr("iceberg_truncate", Reference("a"), 10)

from __future__ import annotations

import pytest

from daft import Expression, col, lit
from daft.dependencies import pa, pc


def assert_eq(actual: Expression, expect):
    """I don't know of a better way, but this can be switched out at a later time."""
    assert str(actual.to_arrow_expr()) == str(expect)


def test_col():
    assert_eq(col("a"), pc.field("a"))


def test_and():
    expr = lit(True) & lit(False)

    assert_eq(expr, "(true and false)")


def test_or():
    expr = lit(True) | lit(False)

    assert_eq(expr, "(true or false)")


def test_not():
    expr = ~lit(True)

    assert_eq(expr, "invert(true)")


def test_equal():
    expr = col("a") == col("b")

    assert_eq(expr, "(a == b)")


def test_not_equal():
    expr = col("a") != col("b")

    assert_eq(expr, "(a != b)")


def test_less_than():
    expr = col("a") < col("b")

    assert_eq(expr, "(a < b)")


def test_less_than_or_equal():
    expr = col("a") <= col("b")

    assert_eq(expr, "(a <= b)")


def test_greater_than():
    expr = col("a") > col("b")

    assert_eq(expr, "(a > b)")


def test_greater_than_or_equal():
    expr = col("a") >= col("b")

    assert_eq(expr, "(a >= b)")


def test_between():
    expr = col("a").between(col("b"), col("c"))

    assert_eq(expr, "((b <= a) and (a <= c))")


def test_is_in():
    expr = col("a").is_in([1, 2, 3])

    assert_eq(expr, pc.field("a").isin([1, 2, 3]))


def test_is_null():
    expr = col("a").is_null()

    assert_eq(expr, pc.field("a").is_null())


def test_not_null():
    expr = col("a").not_null()

    assert_eq(expr, ~pc.field("a").is_null())


def test_alias():
    expr = col("a").alias("b")

    assert_eq(expr, "a")


def test_cast():
    expr = col("a").cast("int64")

    assert_eq(expr, pc.field("a").cast(pa.int64()))


def test_lit():
    expr = lit(42)

    assert_eq(expr, "42")


@pytest.mark.parametrize(
    ["daft_expr", "arrow_expr"],
    [
        (lit(1).bitwise_and(lit(2)), "(1 and 2)"),
        (lit(1).bitwise_or(lit(2)), "(1 or 2)"),
        (lit(1) << lit(2), "shift_left(1, 2)"),
        (lit(1) >> lit(2), "shift_right(1, 2)"),
    ],
)
def test_bitwise_functions(daft_expr: Expression, arrow_expr: str):
    assert_eq(daft_expr, arrow_expr)


@pytest.mark.parametrize(
    ["daft_expr", "arrow_expr"],
    [
        (lit(1) + lit(2), "add(1, 2)"),
        (lit(1) - lit(2), "subtract(1, 2)"),
        (lit(1) * lit(2), "multiply(1, 2)"),
        (lit(1).ceil(), "ceil(1)"),
        (lit(1).floor(), "floor(1)"),
        (lit(1).sign(), "sign(1)"),
        (lit(1).signum(), "sign(1)"),
        (lit(1).negative(), "negate(1)"),
        (lit(1).round(2), pc.round(pc.scalar(1), 2)),
        (lit(1).sqrt(), "sqrt(1)"),
        (lit(1).sin(), "sin(1)"),
        (lit(1).cos(), "cos(1)"),
        (lit(1).tan(), "tan(1)"),
        (lit(1).arcsin(), "asin(1)"),
        (lit(1).arccos(), "acos(1)"),
        (lit(1).arctan(), "atan(1)"),
        (lit(1).arctan2(lit(2)), "atan2(1, 2)"),
        (lit(1).log2(), "log2(1)"),
        (lit(1).log10(), "log10(1)"),
        (lit(1).log(5), "logb(1, 5)"),
        (lit(1).ln(), "ln(1)"),
        (lit(1).log1p(), "log1p(1)"),
        # not available in older versions
        # (lit(1).exp(), "exp(1)"),
        # (lit(1).expm1(), "expm1(1)"),
        # not supported
        # (lit(1).cbrt(), "cbrt(1)"),
        # (lit(1).csc(), "csc(1)"),
        # (lit(1).sec(), "sec(1)"),
        # (lit(1).cot(), "cot(1)"),
        # (lit(1).sinh(), "sinh(1)"),
        # (lit(1).tanh(), "tanh(1)"),
        # (lit(1).arctanh(), "atanh(1)"),
        # (lit(1).arccosh(), "acosh(1)"),
        # (lit(1).arcsinh(), "asinh(1)"),
        # (lit(1).radians(), "radians(1)"),
        # (lit(1).degrees(), "degrees(1)"),
    ],
)
def test_math_functions(daft_expr: Expression, arrow_expr: str):
    assert_eq(daft_expr, arrow_expr)


@pytest.mark.parametrize(
    ["daft_expr", "arrow_expr"],
    [
        (lit("hello").str.capitalize(), 'utf8_capitalize("hello")'),
        (lit("hello").str.concat(lit("world")), 'add("hello", "world")'),
        # containment tests
        (lit("hello").str.count_matches(lit("l")), pc.count_substring(pc.scalar("hello"), "l")),
        (lit("hello").str.contains(lit("ll")), pc.match_substring(pc.scalar("hello"), "ll")),
        (lit("hello").str.endswith(lit("o")), pc.ends_with(pc.scalar("hello"), "o")),
        (lit("hello").str.startswith(lit("h")), pc.starts_with(pc.scalar("hello"), "h")),
        # TODO
        # (lit("hello").str.extract(lit("h(.*)o")), "extract('hello', 'h(.*)o')"),
        # (lit("hello").str.extract_all(lit("l")), "extract_all('hello', 'l')"),
        # (lit("hello").str.find(lit("l")), "find('hello', 'l')"),
        # (lit("hello").str.ilike(lit("HELLO")), "ilike('hello', 'HELLO')"),
        # (lit("hello").str.left(lit(3)), "left('hello', 3)"),
        # (lit("hello").str.length(), "utf8_length('hello')"),
        # (lit("hello").str.length_bytes(), "length_bytes('hello')"),
        # (lit("hello").str.like(lit("h%")), "like('hello', 'h%')"),
        # (lit("hello").str.lower(), "lower('hello')"),
        # (lit("hello").str.lpad(lit(10), lit(" ")), "lpad('hello', 10, ' ')"),
        # (lit("hello").str.lstrip(), "lstrip('hello')"),
        # (lit("hello").str.match(lit("h.*o")), "match('hello', 'h.*o')"),
        # (lit("hello").str.repeat(lit(3)), "repeat('hello', 3)"),
        # (lit("hello").str.replace(lit("l"), lit("L")), "replace('hello', 'l', 'L')"),
        # (lit("hello").str.reverse(), "reverse('hello')"),
        # (lit("hello").str.right(lit(3)), "right('hello', 3)"),
        # (lit("hello").str.rpad(lit(10), lit(" ")), "rpad('hello', 10, ' ')"),
        # (lit("hello").str.rstrip(), "rstrip('hello')"),
        # (lit("hello").str.split(lit("l")), "split('hello', 'l')"),
        # (lit("hello").str.substr(lit(1), lit(3)), "substr('hello', 1, 3)"),
        # (lit("hello").str.to_date(format="xxx"), "strftime('hello', 'xxx')"),
        # (lit("hello").str.to_datetime(format="xxx"), "strftime('hello', 'xxx')"),
        # (lit("hello").str.upper(), "utf8_upper('hello')"),
        # not supported
        # (lit("hello").str.normalize(lit("NFC")), "normalize('hello', 'NFC')"),
    ],
)
def test_string_functions(daft_expr: Expression, arrow_expr: str):
    assert_eq(daft_expr, arrow_expr)

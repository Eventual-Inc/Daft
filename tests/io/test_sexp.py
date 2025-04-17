from __future__ import annotations

import pytest

from typing import TYPE_CHECKING

from daft.io.sexp import Atom, Sexp, Expr
from daft.expressions import col, lit

if TYPE_CHECKING:
    from daft.expressions import Expression

def test_atom_repr():
    # str
    assert repr(Atom("hello")) == '"hello"'
    assert repr(Atom("🤠🤠")) == '"🤠🤠"'
    # int
    assert repr(Atom(1)) == "1"
    assert repr(Atom(0)) == "0"
    assert repr(Atom(-1)) == "-1"
    # float
    assert repr(Atom(2.0)) == "2.00"
    assert repr(Atom(3.14159)) == "3.14"
    # bool
    assert repr(Atom(True)) == "true"
    assert repr(Atom(False)) == "false"
    # nil/null/none
    assert repr(Atom(None)) == "nil"


def test_expr_repr():
    # zero arguments
    assert repr(Expr("f")) == "(f)"
    # positional arguments
    assert repr(Expr("f", 42)) == "(f 42)"
    assert repr(Expr("f", "hello")) == '(f "hello")'
    assert repr(Expr("f", 1, 2.5, "test")) == '(f 1 2.50 "test")'
    # named arguments
    assert repr(Expr("f", value=True)) == "(f value::true)"
    assert repr(Expr("f", x=10, y=20.5, label="data")) == '(f x::10 y::20.50 label::"data")'
    # mixed arguments
    assert repr(Expr("f", 100, False, name="example", flag=True)) == '(f 100 false name::"example" flag::true)'
    # nested expressions
    assert repr(Expr("f", Expr("g", 5))) == "(f (g 5))"
    # atom objects
    assert repr(Expr("f", Atom(42), keyword=Atom("value"))) == '(f 42 keyword::"value")'


def _sexp(expr: Expression) -> Sexp:
    return expr._expr.to_sexp()

def test_pyexpr_lit():
    # null/nil/none
    assert _sexp(lit(None)) == Atom(None)
    # bool
    assert _sexp(lit(True)) == Atom(True)
    assert _sexp(lit(False)) == Atom(False)
    # int
    assert _sexp(lit(0)) == Atom(0)
    assert _sexp(lit(1)) == Atom(1)
    assert _sexp(lit(-1)) == Atom(-1)
    # float
    assert _sexp(lit(0.0)) == Atom(0)
    assert _sexp(lit(1.0)) == Atom(1.0)
    assert _sexp(lit(-1.0)) == Atom(-1.0)
    # string
    assert _sexp(lit(("hello"))) == Atom("hello")
    assert _sexp(lit(("🤠🤠"))) == Atom("🤠🤠")

@pytest.mark.skip("need rust side fix")
def test_pyexpr_col():
    assert _sexp(col("abc")) == Expr("col", "abc")

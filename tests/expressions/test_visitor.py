from decimal import Decimal
from typing import Any

from daft.expressions import (
    Expression,
    ExpressionVisitor,
    col,
    lit,
)

class Visitor(ExpressionVisitor[None, str]):

    def visit_col(self, name: str, ctx: None) -> str:
        raise NotImplementedError

    def visit_lit(self, value: Any, ctx: None) -> str:
        raise NotImplementedError



def test_inspect():
    visitor = Visitor()
    visitor._visit("cast")

# class LispyVisitor(ExpressionVisitor[None, str]):
#     """LispyVisitor is an example visitor implementation for printing s-expressions.

#     This can be implemented *much* more concisely directly in the __str__ .. but
#     this is an exercise and tutorial to show off visitor usage and patterns. We
#     use a 'str' return type and there is currently no scoped context. An example
#     of scoped context here might be an indentation level for pretty-printing.
#     """

#     ###
#     # visitor variants
#     ###

#     def visit_reference(self, reference: Reference, context: None) -> str:
#         """References just use their unquoted path."""
#         return reference.get_path()

#     def visit_literal(self, literal: Literal, context: None) -> str:
#         """Literals uses the underlying value's python representation."""
#         return self._to_str(literal.get_value())

#     # def visit_procedure(self, term: Expr, context: None) -> str:
#     #     """Expr is represented as procs, so (proc args...)."""
#     #     proc = term.proc
#     #     args = ""
#     #     for arg in term.args:
#     #         args += " "  # no join, since we may add indentation
#     #         args += self._arg(arg, context)
#     #     return f"({proc}{args})" if args else f"({proc})"

#     @singledispatchmethod
#     def _to_str(self, val: Any | None) -> str:
#         """Lisp value representation variants, could use str(lit) as the default."""
#         raise NotImplementedError(f"No _lit override for type {type(val)}.")

#     @_to_str.register
#     def _(self, val: None) -> str:
#         """Lisp uses nil, but sicp scheme prefers () :shrug: .. using null."""
#         return "null"

#     @_to_str.register
#     def _(self, val: bool) -> str:
#         return "true" if val else "false"

#     @_to_str.register
#     def _(self, val: int) -> str:
#         return str(val)

#     @_to_str.register
#     def _(self, val: float) -> str:
#         """Use two decimal places since precision doesn't actually matter here."""
#         return f"{val:.2f}"

#     @_to_str.register
#     def _(self, val: Decimal) -> str:
#         return str(val)

#     @_to_str.register
#     def _(self, val: str) -> str:
#         """Lisp uses double-quotes for string literals."""
#         return f'"{val}"'

#     @_to_str.register
#     def _(self, val: dict) -> str:
#         return str(val)


# def lisp(expr: Expression) -> str:
#     return LispyVisitor().visit(expr, None)


# def test_lispy_literals():
#     # str
#     assert lisp(lit("hello")) == '"hello"'
#     assert lisp(lit("🤠🤠")) == '"🤠🤠"'

#     # int
#     assert lisp(lit(1)) == "1"
#     assert lisp(lit(0)) == "0"
#     assert lisp(lit(-1)) == "-1"

#     # float
#     assert lisp(lit(2.0)) == "2.00"
#     assert lisp(lit(3.14159)) == "3.14"

#     # decimal
#     assert lisp(lit(Decimal("1.23"))) == "1.23"
#     assert lisp(lit(Decimal("-42.5"))) == "-42.5"
#     assert lisp(lit(Decimal("0.0001"))) == "0.0001"

#     # bool
#     assert lisp(lit(True)) == "true"
#     assert lisp(lit(False)) == "false"

#     # nil/null/none
#     assert lisp(lit(None)) == "null"

#     # struct
#     print(lisp(lit({"a": True, "b": 1})))


# def test_lispy_references():
#     assert lisp(col("a")) == "a"
#     assert lisp(col("abc")) == "abc"


# def test_lispy_procedures():
#     pass

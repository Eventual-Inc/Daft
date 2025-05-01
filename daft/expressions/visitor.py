from __future__ import annotations

from abc import ABC, abstractmethod
from functools import singledispatchmethod
from typing import Generic, TypeVar, Union

from .expressions import Expression, Literal, Reference

Value = Union[str, int, float, bool, None]
R = TypeVar("R")
C = TypeVar("C")


class ExpressionVisitor(ABC, Generic[C, R]):
    """ExpressionVisitor uses the @singledispatchmethod for a class-based visitor.

    Note that we are not using the typical "accept" method on an term variant
    for dispatching because the @singledispatchmethod handles this for us.
    There is no need to add accept methods to each variant which simplifies
    both the expression tree and the visitor implementations.

    The type `R` represents the return type, and `C` represents the context.
    The context parameter is useful when passing state which is scoped when
    performing a visitor traversal (fold). For non-scoped state, you can just
    add normal instance properties.
    """

    @singledispatchmethod
    def visit(self, expr: Expression, context: C) -> R:
        raise NotImplementedError(f"No visit method for type {type(expr)}")

    @visit.register
    def _(self, expr: Reference, context: C) -> R:
        return self.visit_reference(expr, context)

    @visit.register
    def _(self, expr: Literal, context: C) -> R:
        return self.visit_literal(expr, context)

    # @visit.register
    # def _(self, expr: Procedure, context: C) -> R:
    #     return self.visit_expr(term, context)

    @abstractmethod
    def visit_reference(self, reference: Reference, context: C) -> R: ...

    @abstractmethod
    def visit_literal(self, literal: Literal, context: C) -> R: ...

    # @abstractmethod
    # def visit_procedure(self, procedure: Procedure, context: C) -> R: ...

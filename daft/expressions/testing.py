from __future__ import annotations

from typing import TYPE_CHECKING

from daft.daft import eq as _eq

if TYPE_CHECKING:
    from daft.expressions import Expression


def expr_structurally_equal(e1: Expression, e2: Expression) -> bool:
    """Returns a boolean indicating whether two Expressions are structurally equal:
    1. Expressions' local parameters are value-wise equal
    2. Expressions have the same number of children Expressions
    3. (Recursive) Expressions' childrens are structurally equal to each other as well
    """
    return _eq(e1._expr, e2._expr)

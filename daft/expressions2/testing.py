from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daft.expressions2 import Expression


def expr_structurally_equal(e1: Expression, e2: Expression) -> bool:
    """Returns a boolean indicating whether two Expressions are structurally equal:
    1. Expressions' local parameters are value-wise equal
    2. Expressions have the same number of children Expressions
    3. (Recursive) Expressions' childrens are structurally equal to each other as well
    """
    raise NotImplementedError("[RUST-INT] Call into Rust code utility for checking 2 expressions' structural equality")

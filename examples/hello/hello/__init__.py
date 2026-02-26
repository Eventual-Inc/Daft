from __future__ import annotations

from typing import TYPE_CHECKING

import daft

if TYPE_CHECKING:
    from daft.expressions import Expression


def greet(name: Expression) -> Expression:
    """Greet someone by name."""
    return daft.get_function("greet", name)

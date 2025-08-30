from __future__ import annotations

import sys
from collections.abc import Callable, Generator, Iterator
from typing import TYPE_CHECKING, Any, Generic, TypeVar, get_args, get_origin, get_type_hints, overload

from daft.daft import row_wise_udf
from daft.datatype import DataType

from ._internal import check_fn_serializable, get_expr_args, get_unique_function_name

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec
else:
    from typing import ParamSpec

from daft.expressions import Expression

if TYPE_CHECKING:
    from daft.datatype import DataTypeLike


P = ParamSpec("P")
T = TypeVar("T")


class GeneratorUdf(Generic[P, T]):
    """A user-defined Daft generator function, created by calling `daft.func` on a generator function.

    Unlike row-wise functions which return one value per input row, generator functions may yield multiple values per row.
    Each value is placed in its own row, with the other output columns broadcast to match the number of generated values.
    If no values are yielded for an input, a null value is inserted.
    """

    def __init__(self, fn: Callable[P, Iterator[T]], return_dtype: DataTypeLike | None):
        self._inner = fn
        self.name = get_unique_function_name(fn)

        # attempt to extract return type from an Iterator or Generator type hint
        if return_dtype is None:
            type_hints = get_type_hints(fn)
            if "return" not in type_hints:
                raise ValueError(
                    "`@daft.func` requires either a return type hint or the `return_dtype` argument to be specified."
                )

            iterator_type = type_hints["return"]
            origin = get_origin(iterator_type)
            args = get_args(iterator_type)

            if origin not in (Iterator, Generator):
                raise TypeError(
                    f"The return type hint of a Daft generator function must be an iterator or generator, found: {iterator_type}"
                )

            return_dtype = args[0]
        self.return_dtype = DataType._infer_type(return_dtype)

    @overload
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Iterator[T]: ...
    @overload
    def __call__(self, *args: Expression, **kwargs: Expression) -> Expression: ...
    @overload
    def __call__(self, *args: Any, **kwargs: Any) -> Expression | Iterator[T]: ...

    def __call__(self, *args: Any, **kwargs: Any) -> Expression | Iterator[T]:
        expr_args = get_expr_args(args, kwargs)

        # evaluate the function eagerly if there are no expression arguments
        if len(expr_args) == 0:
            return self._inner(*args, **kwargs)

        check_fn_serializable(self._inner, "@daft.func")

        # temporary workaround before we implement actual generator UDFs: convert it into a list-type row-wise UDF + explode
        def inner_rowwise(*args: P.args, **kwargs: P.kwargs) -> list[T]:
            return list(self._inner(*args, **kwargs))

        return_dtype_rowwise = DataType.list(self.return_dtype)

        return Expression._from_pyexpr(
            row_wise_udf(self.name, inner_rowwise, return_dtype_rowwise._dtype, (args, kwargs), expr_args)
        ).explode()

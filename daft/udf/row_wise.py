from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, get_type_hints

from daft.datatype import DataType
from daft.expressions import Expression
from daft.series import Series

if TYPE_CHECKING:
    from daft.daft import PyDataType, PySeries


class RowWiseUdf:
    def __init__(self, fn: Callable[..., Any], return_dtype: DataType | None):
        self._inner = fn

        module_name = getattr(fn, "__module__")
        qual_name: str = getattr(fn, "__qualname__")
        if module_name:
            self.name = f"{module_name}.{qual_name}"
        else:
            self.name = qual_name

        type_hints = get_type_hints(fn)
        if return_dtype is None:
            if "return" in type_hints:
                self.return_dtype = DataType._infer_type(type_hints["return"])
            else:
                raise ValueError("return_dtype is required when function has no return annotation")
        else:
            self.return_dtype = return_dtype

    def __call__(self, *args: Any, **kwargs: Any) -> Expression | Any:
        children_exprs = []
        for arg in args:
            if isinstance(arg, Expression):
                children_exprs.append(arg)
        for arg in kwargs.values():
            if isinstance(arg, Expression):
                children_exprs.append(arg)

        if len(children_exprs) == 0:
            # all inputs are Python literals, evaluate immediately
            return self._inner(*args, **kwargs)
        else:
            return Expression._row_wise_udf(self.name, self._inner, self.return_dtype, (args, kwargs), children_exprs)


def call_func_with_evaluated_exprs(
    fn: Callable[..., Any],
    return_dtype: PyDataType,
    original_args: tuple[tuple[Any, ...], dict[str, Any]],
    evaluted_args: list[Any],
) -> PySeries:
    args, kwargs = original_args

    new_args = [evaluted_args.pop(0) if isinstance(arg, Expression) else arg for arg in args]
    new_kwargs = {key: (evaluted_args.pop(0) if isinstance(arg, Expression) else arg) for key, arg in kwargs.items()}

    output = fn(*new_args, **new_kwargs)
    dtype = DataType._from_pydatatype(return_dtype)
    return Series.from_pylist([output], dtype=dtype)._series

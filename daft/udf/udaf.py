from __future__ import annotations

import inspect
import random
from typing import TYPE_CHECKING, Any

from daft.daft import udaf_expr
from daft.datatype import DataType, DataTypeLike
from daft.expressions.expressions import Expression
from daft.udf.udf_v2 import check_serializable

if TYPE_CHECKING:
    from collections.abc import Callable


def udaf(
    cls: type | None = None,
    *,
    return_dtype: DataTypeLike,
    state: DataTypeLike | dict[str, DataTypeLike],
) -> type | Callable[[type], type]:
    """Decorator to create a user-defined aggregate function (UDAF) from a class.

    The class must define three methods:
    - ``agg_block(*inputs) -> value | dict``: Map stage — receives input columns as Series, returns partial state
    - ``combine(states) -> value | dict``: Combine stage — merges partial states (must be commutative/associative)
    - ``finalize(state) -> value``: Reduce stage — produces the final output from merged state

    Args:
        return_dtype: The output data type of the aggregate function.
        state: The intermediate state type(s). Either a single DataType for simple accumulators,
            or a dict of ``{name: DataType}`` for multi-field state.

    Examples:
        Single-state UDAF:

        >>> import daft
        >>> from daft import DataType, Series
        >>> @daft.udaf(return_dtype=DataType.float64(), state=DataType.float64())
        ... class MySum:
        ...     def agg_block(self, values: Series) -> float:
        ...         return float(values.sum())
        ...     def combine(self, states: Series) -> float:
        ...         return float(states.sum())
        ...     def finalize(self, state: float) -> float:
        ...         return state

        Multi-state UDAF:

        >>> @daft.udaf(
        ...     return_dtype=DataType.float64(),
        ...     state={"sum": DataType.float64(), "count": DataType.int64()},
        ... )
        ... class MyMean:
        ...     def agg_block(self, values: Series) -> dict:
        ...         return {"sum": float(values.sum()), "count": int(values.count())}
        ...     def combine(self, states: dict) -> dict:
        ...         return {"sum": float(states["sum"].sum()), "count": int(states["count"].sum())}
        ...     def finalize(self, state: dict) -> float:
        ...         return state["sum"] / state["count"]

        Usage:

        >>> my_sum = MySum()
        >>> # df.groupby("category").agg(my_sum(df["value"]))
    """

    resolved_return_dtype = DataType._infer(return_dtype)

    if isinstance(state, dict):
        state_field_names = list(state.keys())
        state_field_dtypes = [DataType._infer(v) for v in state.values()]
    else:
        state_field_names = ["_state"]
        state_field_dtypes = [DataType._infer(state)]

    def _wrap(klass: type) -> type:
        for method_name in ("agg_block", "combine", "finalize"):
            attr = inspect.getattr_static(klass, method_name, None)
            if attr is None or not inspect.isfunction(attr):
                raise ValueError(
                    f"UDAF class `{klass.__name__}` must define a `{method_name}` method."
                )

        module_name = getattr(klass, "__module__", "")
        qual_name = getattr(klass, "__qualname__", klass.__name__)
        func_name = f"{module_name}.{qual_name}" if module_name else qual_name

        class WrappedUDAF:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                self._daft_cls_factory = klass
                self._daft_init_args: tuple[tuple[Any, ...], dict[str, Any]] = (args, kwargs)
                self._daft_func_name = func_name
                self._daft_func_id = f"{func_name}-{random.randint(0, 1_000_000)}"
                self._daft_call_seq = 0

            def __call__(self, *args: Any, **kwargs: Any) -> Expression:
                expr_args = []
                for arg in args:
                    if isinstance(arg, Expression):
                        expr_args.append(arg._expr)
                for arg in kwargs.values():
                    if isinstance(arg, Expression):
                        expr_args.append(arg._expr)

                if len(expr_args) == 0:
                    raise ValueError(
                        "UDAF must be called with at least one Expression argument. "
                        "Use it in a groupby().agg() context, e.g.: df.groupby('col').agg(my_udaf(df['value']))"
                    )

                call_id = f"{self._daft_func_id}-{self._daft_call_seq}"
                self._daft_call_seq += 1

                check_serializable(
                    self._daft_cls_factory,
                    "UDAF class must be serializable. Ensure it can be pickled.",
                )

                result = udaf_expr(
                    call_id,
                    self._daft_func_name,
                    self._daft_cls_factory,
                    self._daft_init_args,
                    (args, kwargs),
                    resolved_return_dtype._dtype,
                    state_field_names,
                    [dt._dtype for dt in state_field_dtypes],
                    expr_args,
                )
                return Expression._from_pyexpr(result)

            def __repr__(self) -> str:
                return f"AggFunc({func_name})"

        WrappedUDAF.__name__ = klass.__name__
        WrappedUDAF.__qualname__ = klass.__qualname__
        WrappedUDAF.__module__ = klass.__module__
        return WrappedUDAF

    if cls is not None:
        return _wrap(cls)
    return _wrap

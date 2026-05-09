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

    The execution pipeline follows three stages:

    .. code-block:: text

        Aggregation:   aggregate(inputs)  -> partial state
        Combination:   combine(states)    -> merged state   (associative & commutative)
        Finalization:  finalize(state)    -> final output

    The class must define exactly three methods:

    - ``aggregate(*inputs)``: Consume all rows of the input columns for one
      group and produce the initial partial state.  Receives one :class:`Series`
      per input column.  Returns either a single scalar (single-state mode) or
      a ``dict[str, scalar]`` (multi-state mode).

    - ``combine(states)``: Merge multiple partial states into one.  Must be
      **associative and commutative** — the framework does not guarantee the
      order in which partial states arrive.  In single-state mode receives a
      :class:`Series` of scalars; in multi-state mode receives a
      ``dict[str, Series]``.

    - ``finalize(state)``: Produce the final output value from the fully-merged
      state.  Called exactly once per group.  In single-state mode receives a
      single Python scalar; in multi-state mode receives a
      ``dict[str, scalar]``.

    State is typed: the ``state`` parameter declares one data type per state
    component. The framework carries state between stages using these types,
    which lets Arrow and the query planner reason about intermediate results.

    Args:
        return_dtype: The output data type of the aggregate function.
        state: The intermediate state type(s). Either a single DataType for
            simple accumulators, or a dict of ``{name: DataType}`` for
            multi-field state.

    Examples:
        Single-state UDAF:

        >>> import daft
        >>> from daft import DataType, Series
        >>> @daft.udaf(return_dtype=DataType.float64(), state=DataType.float64())
        ... class MySum:
        ...     def aggregate(self, values: Series) -> float:
        ...         return float(values.sum())
        ...
        ...     def combine(self, states: Series) -> float:
        ...         return float(states.sum())
        ...
        ...     def finalize(self, state: float) -> float:
        ...         return state

        Multi-state UDAF:

        >>> @daft.udaf(
        ...     return_dtype=DataType.float64(),
        ...     state={"sum": DataType.float64(), "count": DataType.int64()},
        ... )
        ... class MyMean:
        ...     def aggregate(self, values: Series) -> dict:
        ...         return {"sum": float(values.sum()), "count": int(values.count())}
        ...
        ...     def combine(self, states: dict) -> dict:
        ...         return {"sum": float(states["sum"].sum()), "count": int(states["count"].sum())}
        ...
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
        for method_name in ("aggregate", "combine", "finalize"):
            attr = inspect.getattr_static(klass, method_name, None)
            if attr is None or not inspect.isfunction(attr):
                raise ValueError(f"UDAF class `{klass.__name__}` must define a `{method_name}` method.")

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
                check_serializable(
                    self._daft_cls_factory,
                    "UDAF class must be serializable. Ensure it can be pickled.",
                )

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

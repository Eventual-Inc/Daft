from __future__ import annotations

import functools
import inspect
import uuid
from abc import ABC, abstractmethod
from collections.abc import Coroutine, Generator, Iterator
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    ParamSpec,
    TypeVar,
    get_args,
    get_origin,
    get_type_hints,
    overload,
)

if TYPE_CHECKING:
    from typing import Concatenate, Literal

from daft.daft import batch_udf, row_wise_udf
from daft.datatype import DataType, DataTypeLike
from daft.expressions.expressions import Expression

# TODO(cory): use a dataclass to hold all of these attributes
RETURN_DTYPE_ATTR = "_daft_return_dtype"
UNNEST_ATTR = "_daft_unnest"
USE_PROCESS_ATTR = "_daft_use_process"
BATCH_ATTR = "_daft_batch_method"
BATCH_SIZE_ATTR = "_daft_batch_size"
MAX_RETRIES_ATTR = "_daft_max_retries"
ON_ERROR_ATTR = "_daft_on_error"

P = ParamSpec("P")
T = TypeVar("T")
C = TypeVar("C")


def check_serializable(obj: Any, error_msg: str) -> None:
    from daft import pickle

    try:
        pickle.dumps(obj)
    except Exception as e:
        raise ValueError(error_msg) from e


@dataclass
class Func(Generic[P, T, C]):
    _cls: ClsBase[C]
    _method: Callable[Concatenate[C, P], T]
    is_generator: bool
    is_async: bool
    is_batch: bool
    batch_size: int | None
    unnest: bool
    gpus: int
    use_process: bool | None
    max_concurrency: int | None
    max_retries: int | None
    on_error: str | None
    return_dtype: DataType
    input_dtypes: dict[str, DataType]
    name: str = field(init=False)

    @classmethod
    def _from_func(
        cls,
        fn: Callable[P, T],
        return_dtype: DataTypeLike | None,
        unnest: bool,
        use_process: bool | None,
        is_batch: bool,
        batch_size: int | None,
        max_retries: int | None,
        on_error: Literal["raise", "log", "ignore"] | None = None,
    ) -> Func[P, T, None]:
        # create a class instance with no setup method
        class NoopCls(ClsBase[None]):
            def _daft_get_instance(self) -> None:
                return None

        # wrap the function in a function that takes in an additional class instance argument
        # since the cls instance is not used in the function, we can just discard it
        @functools.wraps(fn)
        def method(_self: None, *args: P.args, **kwargs: P.kwargs) -> T:
            return fn(*args, **kwargs)

        is_generator = inspect.isgeneratorfunction(fn)
        is_async = inspect.iscoroutinefunction(fn)

        return_dtype = cls._get_return_dtype(fn, return_dtype, is_generator, is_batch)
        input_dtypes = cls._get_input_dtypes(fn)

        return Func(
            NoopCls(),
            method,  # type: ignore[arg-type]
            is_generator,
            is_async,
            is_batch,
            batch_size,
            unnest,
            0,
            use_process,
            None,
            max_retries,
            on_error,
            return_dtype,
            input_dtypes,
        )

    @classmethod
    def _from_method(
        cls,
        cls_: ClsBase[C],
        method: Callable[Concatenate[C, P], T],
        gpus: int,
        use_process: bool | None,
        max_concurrency: int | None,
        max_retries: int | None,
        on_error: Literal["raise", "log", "ignore"] | None = None,
    ) -> Func[P, T, C]:
        is_generator = inspect.isgeneratorfunction(method)
        is_async = inspect.iscoroutinefunction(method)

        unnest = getattr(method, UNNEST_ATTR, False)
        is_batch = getattr(method, BATCH_ATTR, False)
        batch_size = getattr(method, BATCH_SIZE_ATTR, None)
        return_dtype = getattr(method, RETURN_DTYPE_ATTR, None)
        return_dtype = cls._get_return_dtype(method, return_dtype, is_generator, is_batch)
        input_dtypes = cls._get_input_dtypes(method)
        return cls(
            cls_,
            method,
            is_generator,
            is_async,
            is_batch,
            batch_size,
            unnest,
            gpus,
            use_process,
            max_concurrency,
            max_retries,
            on_error,
            return_dtype,
            input_dtypes,
        )

    def __post_init__(self) -> None:
        """Post-init checks and setup."""
        functools.update_wrapper(self, self._method)
        self.name = self._derive_function_name()

        if self.unnest and not self.return_dtype.is_struct():
            raise ValueError(
                f"Expected Daft function `return_dtype` to be `DataType.struct(..)` when `unnest=True`, instead found: {self.return_dtype}"
            )

        if not self.is_batch and self.batch_size is not None:
            raise ValueError("Non-batch Daft functions cannot have a batch size.")

        if self.is_async and self.is_generator:
            raise ValueError("Daft functions do not yet support both async and generator functions.")

    def _derive_function_name(self) -> str:
        """Compute a unique name for the function using its module and qualified name."""
        module_name = getattr(self, "__module__")
        qual_name: str = getattr(self, "__qualname__")
        if module_name:
            return f"{module_name}.{qual_name}-{uuid.uuid4()}"
        else:
            return f"{qual_name}-{uuid.uuid4()}"

    @staticmethod
    def _get_input_dtypes(fn: Callable[..., Any]) -> dict[str, DataType]:
        type_hints = get_type_hints(fn)
        sig = inspect.signature(fn)
        input_dtypes = {}
        for param_name in sig.parameters:
            if param_name == "self":
                continue
            if param_name not in type_hints:
                input_dtypes[param_name] = DataType.python()
            else:
                input_dtypes[param_name] = DataType._infer(type_hints[param_name])
        return input_dtypes

    @staticmethod
    def _get_return_dtype(
        fn: Callable[..., Any], return_dtype: DataTypeLike | None, is_generator: bool, is_batch: bool
    ) -> DataType:
        if return_dtype is None:
            if is_batch:
                raise ValueError(
                    "Daft batch functions require a return type to be explicitly specified using the `return_dtype` argument."
                )

            type_hints = get_type_hints(fn)
            if "return" not in type_hints:
                raise ValueError(
                    "Daft functions require either a return type hint or the `return_dtype` argument to be specified."
                )
            return_dtype = type_hints["return"]

            if is_generator:
                origin = get_origin(return_dtype)
                args = get_args(return_dtype)
                if origin not in (Iterator, Generator):
                    raise TypeError(
                        f"The return type hint of a Daft generator function must be an iterator or generator, found: {return_dtype}"
                    )
                return_dtype = args[0]
        return DataType._infer(return_dtype)  # type: ignore[arg-type]

    @overload
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T: ...
    @overload
    def __call__(self, *args: Expression, **kwargs: Expression) -> Expression: ...
    @overload
    def __call__(self, *args: Any, **kwargs: Any) -> Expression | T: ...

    def __call__(self, *args: Any, **kwargs: Any) -> Expression | T:
        expr_args = []
        input_dtypes = []

        tmp_dtypes_iter = iter(self.input_dtypes.values())

        for arg in args:
            if isinstance(arg, Expression):
                expr_args.append(arg._expr)
                input_dtypes.append(next(tmp_dtypes_iter)._dtype)

        for key, arg in kwargs.items():
            if isinstance(arg, Expression):
                expr_args.append(arg._expr)
                input_dtypes.append(self.input_dtypes[key]._dtype)

        # evaluate the function eagerly if there are no expression arguments
        if len(expr_args) == 0:
            bound_method = self._cls._daft_bind_method(self._method)
            return bound_method(*args, **kwargs)

        check_serializable(
            self._method,
            "Daft functions must be serializable. If your function accesses a non-serializable global or nonlocal variable to avoid reinitialization, use `@daft.cls` with a setup method instead.",
        )
        check_serializable(
            self._cls,
            "Daft classes must be serializable. If your class accesses a non-serializable global or nonlocal variable, initialize it in the setup method instead.",
        )

        # TODO: implement generator UDFs on the engine side
        if self.is_generator:

            def method(s: C, *args: P.args, **kwargs: P.kwargs) -> list[Any]:
                return list(self._method(s, *args, **kwargs))  # type: ignore[call-overload]

            expr = Expression._from_pyexpr(
                row_wise_udf(
                    self.name,
                    self._cls,
                    method,  # type: ignore[arg-type]
                    self.is_async,
                    DataType.list(self.return_dtype)._dtype,
                    self.gpus,
                    self.use_process,
                    self.max_concurrency,
                    self.max_retries,
                    self.on_error,
                    (args, kwargs),
                    expr_args,
                    input_dtypes,
                )
            ).explode()
        elif self.is_batch:
            expr = Expression._from_pyexpr(
                batch_udf(
                    self.name,
                    self._cls,
                    self._method,
                    self.is_async,
                    self.return_dtype._dtype,
                    self.gpus,
                    self.use_process,
                    self.max_concurrency,
                    self.batch_size,
                    self.max_retries,
                    self.on_error,
                    (args, kwargs),
                    expr_args,
                )
            )
        else:
            expr = Expression._from_pyexpr(
                row_wise_udf(
                    self.name,
                    self._cls,
                    self._method,
                    self.is_async,
                    self.return_dtype._dtype,
                    self.gpus,
                    self.use_process,
                    self.max_concurrency,
                    self.max_retries,
                    self.on_error,
                    (args, kwargs),
                    expr_args,
                    input_dtypes,
                )
            )

        if self.unnest:
            expr = expr.unnest()

        return expr


def mark_cls_method(
    method: Callable[P, T],
    return_dtype: DataTypeLike | None,
    unnest: bool,
    is_batch: bool,
    batch_size: int | None,
    max_retries: int | None = None,
    on_error: Literal["raise", "log", "ignore"] | None = None,
) -> Callable[P, T]:
    """Mark a Daft class method as a Daft method, along with decorator arguments."""
    setattr(method, RETURN_DTYPE_ATTR, return_dtype)
    setattr(method, UNNEST_ATTR, unnest)
    setattr(method, BATCH_ATTR, is_batch)
    setattr(method, BATCH_SIZE_ATTR, batch_size)
    setattr(method, MAX_RETRIES_ATTR, max_retries)
    setattr(method, ON_ERROR_ATTR, on_error)
    return method


class ClsBase(ABC, Generic[C]):
    @abstractmethod
    def _daft_get_instance(self) -> C: ...

    def _daft_bind_method(self, method: Callable[Concatenate[C, P], T]) -> Callable[P, T]:
        """Bind a method to the local instance of the Daft class."""
        local_instance = self._daft_get_instance()

        def bound_method(*args: P.args, **kwargs: P.kwargs) -> T:
            return method(local_instance, *args, **kwargs)

        return bound_method

    def _daft_bind_coroutine_method(
        self, method: Callable[Concatenate[C, P], Coroutine[Any, Any, T]]
    ) -> Callable[P, Coroutine[Any, Any, T]]:
        """Bind a method to the local instance of the Daft class."""
        local_instance = self._daft_get_instance()

        async def bound_coroutine(*args: P.args, **kwargs: P.kwargs) -> T:
            return await method(local_instance, *args, **kwargs)

        return bound_coroutine


def wrap_cls(
    cls: type,
    gpus: int,
    use_process: bool | None,
    max_concurrency: int | None,
    max_retries: int | None,
    on_error: Literal["raise", "log", "ignore"] | None = None,
) -> type:
    class Cls(ClsBase[cls]):  # type: ignore[valid-type]
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self._daft_setup_args = (args, kwargs)
            self._daft_local_instance = None

        def __getstate__(self) -> dict[str, Any]:
            """Custom pickle state that excludes the local instance."""
            state = self.__dict__.copy()
            del state["_daft_local_instance"]
            return state

        def __setstate__(self, state: dict[str, Any]) -> None:
            """Restore state after unpickling, excluding the local instance."""
            self.__dict__.update(state)
            self._daft_local_instance = None

        def __getattr__(self, name: str) -> Func[Any, Any, cls]:  # type: ignore[valid-type]
            attr = inspect.getattr_static(cls, name)

            if not inspect.isfunction(attr) or isinstance(attr, (classmethod, staticmethod)):
                raise AttributeError("Can only access methods on a Daft class instance.")

            return Func._from_method(self, attr, gpus, use_process, max_concurrency, max_retries, on_error)

        def __call__(self, *args: Any, **kwargs: Any) -> Any:
            return self.__getattr__("__call__")(*args, **kwargs)

        def _daft_get_instance(self) -> cls:  # type: ignore[valid-type]
            """Get the local instance of the Daft class. If it is not already created, create it and call the setup method."""
            if self._daft_local_instance is None:
                args, kwargs = self._daft_setup_args
                self._daft_local_instance = cls(*args, **kwargs)

            return self._daft_local_instance

    return Cls

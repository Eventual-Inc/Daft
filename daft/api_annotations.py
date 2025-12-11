from __future__ import annotations

import functools
import inspect
from collections.abc import Callable as CallableABC
from typing import (
    Any,
    Callable,
    ForwardRef,
    Literal,
    ParamSpec,
    TypeVar,
    Union,
    get_args,
    get_origin,
)

from daft.errors import UDFException

T = TypeVar("T")
P = ParamSpec("P")


def DataframePublicAPI(func: Callable[P, T]) -> Callable[P, T]:
    """A decorator to mark a function as part of the Daft DataFrame's public API."""

    @functools.wraps(func)
    def _wrap(*args: P.args, **kwargs: P.kwargs) -> T:
        __tracebackhide__ = True
        type_check_function(func, *args, **kwargs)
        try:
            return func(*args, **kwargs)
        except UDFException as e:
            e = e.with_traceback(None)
            raise
        except Exception as e:
            e = e.with_traceback(e.__traceback__.tb_next if e.__traceback__ else None)
            raise  # If we `raise e`, it will add a new frame right here

    return _wrap


def PublicAPI(func: Callable[P, T]) -> Callable[P, T]:
    """A decorator to mark a function as part of the Daft public API."""

    @functools.wraps(func)
    def _wrap(*args: P.args, **kwargs: P.kwargs) -> T:
        __tracebackhide__ = True
        type_check_function(func, *args, **kwargs)
        try:
            return func(*args, **kwargs)
        except UDFException as e:
            e = e.with_traceback(None)
            raise
        except Exception as e:
            e = e.with_traceback(e.__traceback__.tb_next if e.__traceback__ else None)
            raise  # If we `raise e`, it will add a new frame right here

    return _wrap


class APITypeError(TypeError):
    pass


def type_check_function(func: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
    signature = inspect.signature(func)
    arguments = signature.bind(*args, **kwargs).arguments
    type_hints = func.__annotations__

    def isinstance_helper(value: Any, T: Any) -> bool:
        """Like builtins.isinstance, but also accepts typing.* types."""
        if T is Any:
            return True

        # T is an unresolved annotation.
        # We cannot typecheck these, so just treat them as Any.
        if isinstance(T, (str, ForwardRef)):
            return True

        origin_T = get_origin(T)

        # Handle Callable types
        if (origin_T is CallableABC or T is CallableABC) and isinstance(CallableABC, type):
            return isinstance(value, CallableABC)

        # Handle generic types that are subclasses of Callable
        if (
            origin_T is not None
            and hasattr(origin_T, "__mro__")
            and CallableABC in getattr(origin_T, "__mro__", [])
            and isinstance(CallableABC, type)
        ):
            return isinstance(value, CallableABC)

        # T is a builtin primitive type, like `int`
        if origin_T is None:
            return isinstance(value, T)

        # T is a `typing.Union`
        if origin_T is Union:
            union_types = get_args(T)
            return any(isinstance_helper(value, union_type) for union_type in union_types)

        # T is a generic type, like `typing.List` or builtin container like `list`
        if isinstance(origin_T, type):
            return isinstance(value, origin_T)

        # T is a `typing.Literal`
        if origin_T is Literal:
            literal_values = get_args(T)
            return value in literal_values

        raise NotImplementedError(
            f"Unexpected error: Type checking is not implemented for type {T}. Sorry! Please file an issue."
        )

    for name, value in arguments.items():
        if name not in type_hints:
            continue

        param_kind = signature.parameters[name].kind
        param_type = type_hints[name]

        # Non-variadic arguments.
        if param_kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            if not isinstance_helper(value, param_type):
                raise APITypeError(
                    f"{func.__qualname__} received wrong input type.\n"
                    f"Required:\n\t{name} = <{param_type}>\n"
                    f"Given:\n\t{name} = <{type(value).__name__}>"
                )

        elif param_kind == inspect.Parameter.VAR_POSITIONAL:
            for i, item in enumerate(value):
                if not isinstance_helper(item, param_type):
                    raise APITypeError(
                        f"{func.__qualname__} received wrong input type.\n"
                        f"Required:\n\t{name} = <{param_type}>, ...\n"
                        f"Given:\n\t{name} = <{type(item).__name__}> (in position {i})"
                    )

        elif param_kind == inspect.Parameter.VAR_KEYWORD:
            for key, item in value.items():
                if not isinstance_helper(item, param_type):
                    raise APITypeError(
                        f"{func.__qualname__} received wrong input type.\n"
                        f"Required:\n\t{key} = <{param_type}>\n"
                        f"Given:\n\t{key} = <{type(item).__name__}>"
                    )

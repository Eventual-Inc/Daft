from __future__ import annotations

import functools
import inspect
import sys
from typing import Any, Callable, ForwardRef, TypeVar, Union

if sys.version_info < (3, 8):
    from typing_extensions import get_args, get_origin
else:
    from typing import get_args, get_origin

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec
else:
    from typing import ParamSpec

from daft.analytics import time_df_method, time_func

T = TypeVar("T")
P = ParamSpec("P")


def DataframePublicAPI(func: Callable[P, T]) -> Callable[P, T]:
    """A decorator to mark a function as part of the Daft DataFrame's public API."""

    @functools.wraps(func)
    def _wrap(*args: P.args, **kwargs: P.kwargs) -> T:
        type_check_function(func, *args, **kwargs)
        timed_method = time_df_method(func)
        return timed_method(*args, **kwargs)

    return _wrap


def PublicAPI(func: Callable[P, T]) -> Callable[P, T]:
    """A decorator to mark a function as part of the Daft public API."""

    @functools.wraps(func)
    def _wrap(*args: P.args, **kwargs: P.kwargs) -> T:
        type_check_function(func, *args, **kwargs)
        timed_func = time_func(func)
        return timed_func(*args, **kwargs)

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

        # T is a simple type, like `int`
        if isinstance(T, type):
            return isinstance(value, T)

        # T is a generic type, like `typing.List`
        origin_T = get_origin(T)
        if isinstance(origin_T, type):
            return isinstance(value, origin_T)

        # T is a higher order type, like `typing.Union`
        if origin_T is Union:
            union_types = get_args(T)
            return any(isinstance_helper(value, union_type) for union_type in union_types)

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

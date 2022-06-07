from __future__ import annotations
from ast import Return

import dataclasses
import functools

from daft.datarepo.query.definitions import QueryColumn

from typing import Any, Optional, Type, Tuple, Dict, Generic, TypeVar, Protocol, get_type_hints, cast


ReturnType = TypeVar("ReturnType", covariant=True)


class UserFunction(Protocol[ReturnType]):
    """Arbitrary user-defined function of a given return type"""

    def __call__(self, *args: Any, **kwargs: Any) -> ReturnType:
        ...


class QueryFunction(Protocol[ReturnType]):
    """A function that can be called on QueryColumns, returning a QueryExpression"""

    def __call__(self, *args: QueryColumn, **kwargs: QueryColumn) -> QueryExpression[ReturnType]:
        ...


@dataclasses.dataclass(frozen=True)
class QueryExpression(Generic[ReturnType]):
    """A declaration of a function and the columns to use as input to the function"""

    func: UserFunction[ReturnType]
    return_type: Type[ReturnType]
    args: Tuple[QueryColumn, ...]
    kwargs: Dict[str, QueryColumn]


def func(
    user_func: UserFunction[ReturnType], return_type: Optional[Type[ReturnType]] = None
) -> QueryFunction[ReturnType]:
    """A decorator to convert a user's Python function into a QueryFunction. QueryFunctions
    can be called on QueryColumns to produce QueryExpressions, which can then be used in a Datarepo
    query to declare the values of new columns.

    Usage:

        @func
        def f(x: int) -> int:
            return x * 2

        query = query.with_column("foo_times_two", f("foo"))

    Args:
        user_func (Callable): user-provided function
        return_type (Optional[Type], optional): the return type of the function, if not declared through type annotations

    Returns:
        QueryFunction: function that can be called on columns to return a QueryExpression
    """
    parsed_return_type: Type[ReturnType]
    if return_type is None:
        type_hints = get_type_hints(user_func)
        if "return" not in type_hints:
            raise ValueError(
                f"Function {user_func} is not type-annotated with a return type and no return_type provided"
            )
        parsed_return_type = type_hints["return"]
    else:
        parsed_return_type = return_type

    def query_function(*args: QueryColumn, **kwargs: QueryColumn) -> QueryExpression[ReturnType]:
        return QueryExpression(func=user_func, return_type=parsed_return_type, args=args, kwargs=kwargs)

    return query_function

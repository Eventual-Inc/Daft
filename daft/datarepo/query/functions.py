from __future__ import annotations

import dataclasses

from daft.datarepo.query.definitions import QueryColumn

from typing import (
    Union,
    Callable,
    Any,
    Optional,
    Type,
    Tuple,
    Dict,
    Generic,
    TypeVar,
    Protocol,
    get_type_hints,
    cast,
    overload,
)


ReturnType = TypeVar("ReturnType", covariant=True)

UserFunction = Callable


class QueryFunction(Protocol[ReturnType]):
    """A function that can be called on QueryColumns, returning a QueryExpression"""

    def __call__(self, *args: QueryColumn, **kwargs: QueryColumn) -> QueryExpression[ReturnType]:
        ...


@dataclasses.dataclass(frozen=True)
class QueryExpression(Generic[ReturnType]):
    """A declaration of a function and the columns to use as input to the function"""

    func: UserFunction
    return_type: Type[ReturnType]
    args: Tuple[QueryColumn, ...]
    kwargs: Dict[str, QueryColumn]
    batch_size: Optional[int]


@overload
def func(
    userfunc: None = None, *, return_type: Optional[Type[ReturnType]] = None
) -> Callable[[UserFunction], QueryFunction[ReturnType]]:
    ...


@overload
def func(userfunc: UserFunction, *, return_type: Optional[Type[ReturnType]] = None) -> QueryFunction[ReturnType]:
    ...


def func(
    userfunc: Optional[UserFunction] = None, *, return_type: Optional[Type[ReturnType]] = None
) -> Union[Callable[[UserFunction], QueryFunction[ReturnType]], QueryFunction[ReturnType]]:
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

    def _func(user_func: UserFunction) -> QueryFunction[ReturnType]:
        parsed_return_type = return_type if return_type is not None else _get_return_type(user_func)

        def query_function(*args: QueryColumn, **kwargs: QueryColumn) -> QueryExpression[ReturnType]:
            return QueryExpression(
                func=user_func, return_type=parsed_return_type, args=args, kwargs=kwargs, batch_size=None
            )

        return query_function

    # This lets us either call this function as @func, @func(return_type=Foo) or f = func(f, return_type=Foo)
    if userfunc is not None:
        return _func(userfunc)
    return _func


def batch_func(
    *args: UserFunction,
    return_type: Optional[Type[ReturnType]] = None,
    batch_size: int = 8,
):
    """Batch variant of @func, which decorates functions that receive a batches of data as lists instead of single items

    Usage:

        @batch_func
        def f(data: List[int]) -> List[int]:
            return [x * 2 for x in data]

        query = query.with_column("foo_times_two", f("foo"))

    Args:
        user_func (Callable): user-provided function
        return_type (Optional[Type], optional): the return type of the function, if not declared through type annotations


    Returns:
        QueryFunction: function that can be called on columns to return a QueryExpression
    """

    def _batch_func(user_func: UserFunction) -> QueryFunction[ReturnType]:
        parsed_return_type = return_type if return_type is not None else _get_return_type(user_func)

        def query_function(*args: QueryColumn, **kwargs: QueryColumn) -> QueryExpression[ReturnType]:
            return QueryExpression(
                func=user_func, return_type=parsed_return_type, args=args, kwargs=kwargs, batch_size=batch_size
            )

        return query_function

    # This lets us either call this function as @batch_func,
    # @batch_func(batch_size=N, return_type=Foo) or f = batch_func(f, batch_size=N, return_type=Foo)
    arg0_is_wrapped_func = len(args) == 1 and callable(args[0])
    if arg0_is_wrapped_func:
        return _batch_func(args[0])
    return _batch_func


def _get_return_type(user_func: UserFunction) -> Type[ReturnType]:
    """Gets the return type from a UserFunction, which could be either a Python function
    or callable class"""
    type_hints = get_type_hints(user_func.__call__) if isinstance(user_func, type) else get_type_hints(user_func)
    if "return" not in type_hints:
        raise ValueError(f"Function {user_func} is not type-annotated with a return type and no return_type provided")
    return cast(Type[ReturnType], type_hints["return"])

import asyncio
import functools
import json
from typing import Any, Awaitable, Callable, List, Optional, TypeVar, overload

import fastapi
import tenacity
from kubernetes_asyncio import client

T = TypeVar("T")
FuncT = Callable[..., Awaitable[T]]
FuncOptT = Callable[..., Awaitable[Optional[T]]]


def _should_retry(e: Exception) -> bool:
    # Retry on transient timeouts
    if isinstance(e, (TimeoutError, asyncio.TimeoutError)):
        return True

    if not isinstance(e, client.rest.ApiException):
        return False

    # Too many requests
    if e.status == 429:
        return True

    if e.status == 500:
        try:
            body_dict = json.loads(e.body)
        except json.decoder.JSONDecodeError:
            return False

        # ServerTimeout
        if body_dict["reason"] == "ServerTimeout":
            return True

        return False

    return e.status > 500


@overload
def k8s_retryable() -> Callable[[FuncT], FuncT]:
    pass


@overload
def k8s_retryable(ignore: List[int]) -> Callable[[FuncT], FuncOptT]:
    pass


def k8s_retryable(ignore: Optional[List[int]] = None) -> Callable[[FuncT], FuncOptT]:
    def decorator(func: FuncT) -> FuncOptT:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Optional[T]:
            try:
                # TODO: tune retry policy
                retry_decorator = tenacity.retry(
                    stop=tenacity.stop_after_attempt(10),
                    wait=tenacity.wait_exponential(multiplier=1, min=1, max=10) + tenacity.wait_random(0, 2),
                    retry=tenacity.retry_if_exception(_should_retry),
                    reraise=True,
                )

                # `func` maybe an asyncio coroutine function or a callable
                # that returns an asyncio coroutine. In order to seamlessly
                # handle both these cases, we wrap it as follows:
                @retry_decorator
                async def _inner() -> Optional[T]:
                    return await func(*args, **kwargs)

                return await _inner()

            except client.rest.ApiException as e:
                if ignore is None or e.status not in ignore:
                    raise
                return None

        return wrapper

    return decorator


def passthrough_status_code(response: fastapi.Response) -> Callable[[FuncT], FuncT]:
    def decorator(func: FuncT) -> FuncOptT:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            try:

                async def _inner() -> T:
                    return await func(*args, **kwargs)

                return await _inner()
            except client.rest.ApiException as e:
                response.status_code = e.status
                raise e

        return wrapper

    return decorator

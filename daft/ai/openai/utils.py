from __future__ import annotations

from typing import TYPE_CHECKING, Callable, TypeVar

from daft.ai.utils import raise_retry_after

if TYPE_CHECKING:
    from collections.abc import Awaitable

T = TypeVar("T")


async def execute_openai_call(
    coro_factory: Callable[[], Awaitable[T]],
) -> T:
    """Run an OpenAI async call, surfacing Retry-After hints."""
    from openai import APIStatusError

    try:
        return await coro_factory()
    except APIStatusError as exc:
        if exc.status_code in (429, 503):
            raise_retry_after(exc.response, exc)
        raise
    except Exception:
        raise

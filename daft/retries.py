from __future__ import annotations

import functools
import random
import time
from typing import Any, Callable, TypeVar

T = TypeVar("T")


def retry_with_backoff(
    max_retries: int = 3,
    jitter_ms: int = 1000,
    max_backoff_ms: int = 20000,
    should_retry: Callable[[Exception], bool] | None = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """A decorator that retries a function using the 'Full Jitter' strategy.

    Args:
        max_retries: The total number of attempts to make (including the first call).
        jitter_ms: The base interval in milliseconds used to calculate the
            exponential upper bound (2^attempt * jitter_ms).
        max_backoff_ms: The maximum allowed sleep time in milliseconds.
        should_retry: A callback function that accepts an Exception and returns
            True if a retry should be attempted. If None, all Exceptions are retried.
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            retries = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    # Logic: If no callback provided, retry everything.
                    # If callback provided, only retry if it returns True.
                    retry = should_retry(e) if should_retry else True

                    if not retry or retries >= max_retries - 1:
                        raise e

                    upper_bound = (2**retries) * jitter_ms
                    jitter = random.randint(0, upper_bound)
                    backoff = min(max_backoff_ms, jitter)

                    sleep_seconds = backoff / 1000.0

                    time.sleep(sleep_seconds)
                    retries += 1

        return wrapper

    return decorator

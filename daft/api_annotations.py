from __future__ import annotations

import functools
from typing import Any, Callable

from daft.analytics import time_df_method


def DataframePublicAPI(func: Callable[..., Any]) -> Callable[..., Any]:
    """A decorator to mark a function as part of the Daft DataFrame's public API."""

    @functools.wraps(func)
    def _wrap(*args, **kwargs):
        timed_method = time_df_method(func)
        return timed_method(*args, **kwargs)

    return _wrap

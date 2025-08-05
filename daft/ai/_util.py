from __future__ import annotations

import functools
import weakref


def weak_once():
    """Memoize a singleton as a weak reference."""
    def wrapper(func):
        @functools.lru_cache(1)
        def _func(_self, *args, **kwargs):
            return func(_self(), *args, **kwargs)
        @functools.wraps(func)
        def inner(self, *args, **kwargs):
            return _func(weakref.ref(self), *args, **kwargs)
        return inner
    return wrapper

from __future__ import annotations


def is_running_from_ipython():
    try:
        from IPython import get_ipython

        return get_ipython() is not None
    except ImportError:
        return False

"""
Taken from: https://github.com/cloudpipe/cloudpickle/blob/master/cloudpickle/compat.py
"""

from __future__ import annotations

import sys

if sys.version_info < (3, 8):
    try:
        import pickle5 as pickle  # noqa: F401
        from pickle5 import Pickler  # noqa: F401
    except ImportError:
        import pickle  # noqa: F401

        # Use the Python pickler for old CPython versions
        from pickle import _Pickler as Pickler  # noqa: F401
else:
    import pickle  # noqa: F401

    # Pickler will the C implementation in CPython and the Python
    # implementation in PyPy
    from pickle import Pickler  # noqa: F401

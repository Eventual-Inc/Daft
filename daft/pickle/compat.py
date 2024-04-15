"""
Taken from: https://github.com/cloudpipe/cloudpickle/blob/master/cloudpickle/compat.py
"""

from __future__ import annotations

import sys

if sys.version_info < (3, 8):
    try:
        import pickle5 as pickle
        from pickle5 import Pickler
    except ImportError:
        import pickle

        # Use the Python pickler for old CPython versions
        from pickle import _Pickler as Pickler
else:
    import pickle  # noqa: F401

    # Pickler will the C implementation in CPython and the Python
    # implementation in PyPy
    from pickle import Pickler  # noqa: F401

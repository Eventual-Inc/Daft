"""
Taken from: https://github.com/cloudpipe/cloudpickle/blob/master/cloudpickle/compat.py
"""

from __future__ import annotations

import pickle  # noqa: F401

# Pickler will the C implementation in CPython and the Python
# implementation in PyPy
from pickle import Pickler  # noqa: F401

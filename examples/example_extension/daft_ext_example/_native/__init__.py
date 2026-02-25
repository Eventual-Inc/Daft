__all__ = ["ffi", "lib"]

import os
from .ffi import ffi

lib = ffi.dlopen(os.path.join(os.path.dirname(__file__), "lib_native.dylib"))
del os

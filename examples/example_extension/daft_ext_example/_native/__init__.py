"""Loads the compiled native extension with RTLD_GLOBAL symbol visibility."""

import ctypes
import os
import sys


def _load() -> None:
    ext = ".dylib" if sys.platform == "darwin" else ".so"
    # maturin places the lib here at build time
    pkg_dir = os.path.dirname(os.path.dirname(__file__))
    lib_path = os.path.join(pkg_dir, f"libdaft_ext_example_native{ext}")
    if not os.path.exists(lib_path):
        raise RuntimeError(
            f"Native library not found at {lib_path}.\nRun 'maturin develop' from the extension directory first."
        )
    ctypes.CDLL(lib_path, mode=ctypes.RTLD_GLOBAL)


_load()

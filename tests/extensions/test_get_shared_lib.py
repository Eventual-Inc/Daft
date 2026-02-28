from __future__ import annotations

import types
from pathlib import Path
from unittest.mock import patch

import pytest

from daft.session import _get_shared_lib, _get_shared_lib_extension


def test_get_shared_lib_extension():
    import platform

    ext = _get_shared_lib_extension()
    if platform.system() == "Windows":
        assert ext == ".dll"
    else:
        assert ext == ".so"


def test_get_shared_lib_no_file_attr():
    mod = types.ModuleType("fake")
    # Ensure __file__ is absent
    if hasattr(mod, "__file__"):
        del mod.__file__
    with pytest.raises(ValueError, match="no __file__ attribute"):
        _get_shared_lib(mod)


def test_get_shared_lib_no_library(tmp_path: Path):
    mod = types.ModuleType("fake")
    mod.__file__ = str(tmp_path / "__init__.py")
    with pytest.raises(ValueError, match="No native library"):
        _get_shared_lib(mod)


def test_get_shared_lib_multiple_libraries(tmp_path: Path):
    mod = types.ModuleType("fake")
    mod.__file__ = str(tmp_path / "__init__.py")
    # Create two .so files
    (tmp_path / "liba.so").touch()
    (tmp_path / "libb.so").touch()
    with pytest.raises(ValueError, match="Multiple native libraries"):
        _get_shared_lib(mod)


def test_get_shared_lib_single_library(tmp_path: Path):
    mod = types.ModuleType("fake")
    mod.__file__ = str(tmp_path / "__init__.py")
    lib = tmp_path / "libfake.so"
    lib.touch()
    with patch("daft.session._get_shared_lib_extension", return_value=".so"):
        result = _get_shared_lib(mod)
    assert result == str(lib)

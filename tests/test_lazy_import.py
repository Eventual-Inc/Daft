from __future__ import annotations

import pickle
import sys

import pytest

from daft.lazy_import import LazyImport


def test_lazy_import_valid_module():
    """Test that a valid module can be lazily imported and attributes accessed."""
    # Use sys as a test module since it's always available
    lazy_sys = LazyImport("sys")

    # Module should not be loaded yet
    assert lazy_sys._module is None

    # Accessing an attribute should load the module
    version = lazy_sys.version
    assert version == sys.version

    # Module should now be cached
    assert lazy_sys._module is not None


def test_lazy_import_invalid_module_raises_import_error():
    """Test that accessing an attribute on a non-existent module raises ImportError."""
    lazy_fake = LazyImport("nonexistent_fake_module_12345")

    with pytest.raises(ImportError) as exc_info:
        lazy_fake.some_attribute

    # Check that error message mentions lazy loading
    error_msg = str(exc_info.value)
    assert "lazily-loaded" in error_msg
    assert "nonexistent_fake_module_12345" in error_msg


def test_module_available_returns_true_for_valid_module():
    """Test that module_available() returns True for valid modules."""
    lazy_sys = LazyImport("sys")
    assert lazy_sys.module_available() is True


def test_module_available_returns_false_for_invalid_module():
    """Test that module_available() returns False for invalid modules."""
    lazy_fake = LazyImport("nonexistent_fake_module_12345")
    assert lazy_fake.module_available() is False


def test_access_submodule():
    """Test that submodules can be accessed dynamically."""
    lazy_os = LazyImport("os")

    path = lazy_os.path

    # We should be able to access attributes on it
    assert hasattr(path, "join")


def test_lazy_import_submodule():
    """Test that submodules can be lazy-imported."""
    lazy_path = LazyImport("os.path")

    # We should be able to access attributes on it
    assert hasattr(lazy_path, "join")


def test_lazy_import_nonexistent_attribute_raises_attribute_error():
    """Test that accessing a non-existent attribute raises AttributeError."""
    lazy_sys = LazyImport("sys")

    with pytest.raises(AttributeError):
        lazy_sys.nonexistent_attribute_xyz_12345


def test_lazy_import_object_attributes_accessible():
    """Test that LazyImport object's own attributes are accessible."""
    lazy_sys = LazyImport("sys")

    # Should be able to access LazyImport's own attributes
    assert lazy_sys._module_name == "sys"
    assert lazy_sys._module is None  # Not loaded yet


def test_lazy_import_caches_module():
    """Test that the module is only imported once and cached."""
    lazy_sys = LazyImport("sys")

    # First access loads the module
    _ = lazy_sys.version
    first_module = lazy_sys._module

    # Second access uses cached module
    _ = lazy_sys.platform
    second_module = lazy_sys._module

    # Should be the same object
    assert first_module is second_module


def test_lazy_import_pickle_roundtrip():
    """Test that LazyImport can be pickled and unpickled."""
    lazy_sys = LazyImport("sys")

    # Access an attribute to load the module
    _ = lazy_sys.version
    assert lazy_sys._module is not None

    # Pickle and unpickle
    pickled = pickle.dumps(lazy_sys)
    unpickled = pickle.loads(pickled)

    # The unpickled instance should have the module name but not the loaded module
    assert unpickled._module_name == "sys"
    assert unpickled._module is None  # Module is not serialized

    # But we should still be able to use it
    assert unpickled.module_available()
    version = unpickled.version
    assert version == sys.version


def test_lazy_import_nested_submodule():
    """Test accessing deeply nested submodules."""
    # Use xml.etree.ElementTree as it's in stdlib
    lazy_xml = LazyImport("xml")

    etree = lazy_xml.etree

    # Can access further
    assert hasattr(etree, "ElementTree")


def test_lazy_import_does_not_import_eagerly():
    """Test that creating a LazyImport doesn't actually import the module."""
    # Remove json from sys.modules if it exists
    if "json" in sys.modules:
        del sys.modules["json"]

    # Create lazy import
    lazy_json = LazyImport("json")

    # json should not be in sys.modules yet
    assert "json" not in sys.modules

    # Now access an attribute
    _ = lazy_json.dumps

    # Now json should be in sys.modules
    assert "json" in sys.modules

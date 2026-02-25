from __future__ import annotations

# Import the extension module so it's available for loading
import pytest

import daft
from daft import col
from daft.session import Session


def test_load_and_call_via_sql():
    """Load the example extension and call increment() via SQL."""
    import daft_ext_example

    sess = Session()
    sess.load_extension(daft_ext_example)

    sess.create_temp_table("T", daft.from_pydict({"x": [1, 2, 3]}))
    df = sess.sql("SELECT increment(x) FROM T")
    assert df is not None

    result = df.to_pydict()
    assert result["result"] == [2, 3, 4]


def test_extension_function_isolation():
    """Only the session that loads the extension can call its functions."""
    import daft_ext_example

    sess_a = Session()
    sess_b = Session()

    sess_a.load_extension(daft_ext_example)
    sess_a.create_temp_table("T", daft.from_pydict({"x": [1, 2, 3]}))

    # Session A can call increment
    df = sess_a.sql("SELECT increment(x) FROM T")
    assert df is not None
    assert df.to_pydict()["result"] == [2, 3, 4]

    # Session B cannot — increment is not registered
    sess_b.create_temp_table("T", daft.from_pydict({"x": [1, 2, 3]}))
    with pytest.raises(Exception):
        sess_b.sql("SELECT increment(x) FROM T")


def test_load_nonexistent_extension():
    """Loading a nonexistent path raises an error."""
    sess = Session()
    with pytest.raises(Exception):
        sess.load_extension("/nonexistent/path/libfoo.so")


def test_call_via_dataframe_api():
    """Load extension and call increment() via DataFrame API."""
    import daft_ext_example

    sess = Session()
    sess.load_extension(daft_ext_example)
    df = daft.from_pydict({"x": [1, 2, 3]})
    result_expr = sess.get_function("increment", col("x"))
    result = df.select(result_expr).collect().to_pydict()
    assert next(iter(result.values())) == [2, 3, 4]


def test_load_by_module_object():
    """Load extension by passing a module object."""
    import daft_ext_example

    sess = Session()
    sess.load_extension(daft_ext_example)  # module object
    sess.create_temp_table("T", daft.from_pydict({"x": [1, 2, 3]}))
    assert sess.sql("SELECT increment(x) FROM T").to_pydict()["result"] == [2, 3, 4]


def test_load_by_package_name():
    """Load extension by importing a package and passing the module."""
    import daft_ext_example

    sess = Session()
    sess.load_extension(daft_ext_example)
    sess.create_temp_table("T", daft.from_pydict({"x": [1, 2, 3]}))
    assert sess.sql("SELECT increment(x) FROM T").to_pydict()["result"] == [2, 3, 4]


def test_python_wrapper_function():
    """Full UX: import function, load extension, use in DataFrame."""
    import daft_ext_example
    from daft_ext_example import increment

    sess = Session()
    sess.load_extension(daft_ext_example)
    with sess:  # set as current session so increment() resolves
        df = daft.from_pydict({"x": [1, 2, 3]})
        result = df.select(increment(col("x"))).collect().to_pydict()
        assert next(iter(result.values())) == [2, 3, 4]


def test_error_without_load():
    """Calling extension function without load_extension gives clear error."""
    from daft_ext_example import increment

    sess = Session()
    with sess:
        df = daft.from_pydict({"x": [1, 2, 3]})
        with pytest.raises(Exception, match="not found"):
            df.select(increment(col("x"))).collect()

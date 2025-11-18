from __future__ import annotations

import pytest

import daft
import daft.exceptions
from daft import col


def test_to_struct_empty_structs():
    df = daft.from_pydict({"a": [1, 2, 3]})

    with pytest.raises(daft.exceptions.DaftCoreException, match="Cannot call struct with no inputs"):
        df.select(daft.functions.to_struct()).collect()


# there was a bug with pushdowns onto to_struct previously
def test_to_struct_pushdown():
    df = daft.from_pydict(
        {
            "a": [1, 2, 3, 4, None, 6, None],
            "b": ["a", "b", "c", "", "e", None, None],
        }
    )
    df = df.select(daft.functions.to_struct(col("a"), col("b")))
    df = df.select(daft.functions.get(col("struct"), "a"))
    assert df.to_pydict() == {"a": [1, 2, 3, 4, None, 6, None]}

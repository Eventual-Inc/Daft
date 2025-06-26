from __future__ import annotations

import daft
from daft.expressions import col
from daft.recordbatch import MicroPartition


def test_to_struct():
    df = MicroPartition.from_pydict(
        {
            "a": [1, 2, 3, 4, None, 6, None],
            "b": ["a", "b", "c", "", "e", None, None],
        }
    )
    res = df.eval_expression_list([daft.struct(col("a"), col("b"))]).to_pydict()
    assert res["struct"] == [
        {"a": 1, "b": "a"},
        {"a": 2, "b": "b"},
        {"a": 3, "b": "c"},
        {"a": 4, "b": ""},
        {"a": None, "b": "e"},
        {"a": 6, "b": None},
        {"a": None, "b": None},
    ]


def test_to_struct_subexpr():
    df = MicroPartition.from_pydict(
        {
            "a": [1, 2, 3, 4, None, 6, None],
            "b": ["a", "b", "c", "", "e", None, None],
        }
    )
    res = df.eval_expression_list([daft.struct(col("a") * 2, col("b").str.upper())]).to_pydict()
    assert res["struct"] == [
        {"a": 2, "b": "A"},
        {"a": 4, "b": "B"},
        {"a": 6, "b": "C"},
        {"a": 8, "b": ""},
        {"a": None, "b": "E"},
        {"a": 12, "b": None},
        {"a": None, "b": None},
    ]


def test_to_struct_strs():
    df = MicroPartition.from_pydict(
        {
            "a": [1, 2, 3, 4, None, 6, None],
            "b": ["a", "b", "c", "", "e", None, None],
        }
    )
    res = df.eval_expression_list([daft.struct("a", "b")]).to_pydict()
    assert res["struct"] == [
        {"a": 1, "b": "a"},
        {"a": 2, "b": "b"},
        {"a": 3, "b": "c"},
        {"a": 4, "b": ""},
        {"a": None, "b": "e"},
        {"a": 6, "b": None},
        {"a": None, "b": None},
    ]

from __future__ import annotations

import daft
from daft import col


def test_row_wise_udf_loaded_from_external_module():
    from tests.udf.my_funcs import my_add_func

    df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
    actual = df.select(my_add_func(col("a"), col("b"))).to_pydict()

    expected = {"a": [5, 7, 9]}

    assert actual == expected


def test_row_wise_udf_loaded_from_external_module_with_literal():
    from tests.udf.my_funcs import my_add_func

    df = daft.from_pydict({"a": [1, 2, 3]})
    actual = df.select(my_add_func(col("a"), 10)).to_pydict()

    expected = {"a": [11, 12, 13]}

    assert actual == expected


def test_cls_udf_loaded_from_external_module():
    from tests.udf.my_funcs import MockModelPredictor

    predictor = MockModelPredictor("bert-base")
    df = daft.from_pydict({"text": ["hello", "world", "daft"]})
    actual = df.select(predictor(col("text"))).to_pydict()

    expected = {
        "text": ["model bert-base predict hello", "model bert-base predict world", "model bert-base predict daft"]
    }

    assert actual == expected

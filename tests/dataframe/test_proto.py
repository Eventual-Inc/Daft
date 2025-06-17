from __future__ import annotations

import daft


def test_proto():
    df = daft.from_pydict(
        {
            "a": [True, True, False],
            "b": [1, 2, 3],
            "c": ["ABC", "DEF", "GHI"],
        }
    )
    df = df.collect().show()

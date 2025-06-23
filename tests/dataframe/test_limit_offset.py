from __future__ import annotations

from random import randint

import daft


# FIXME by zhenchao 2025-06-23 16:12:17
def test_limit_offset() -> None:
    df = daft.from_pydict(
        {
            "id": [i for i in range(1024)],
            "name": [f"name_{i}" for i in range(1024)],
            "age": [randint(1, 100) for _ in range(1024)],
        }
    )

    df = df.select("name", "age").limit(-1)
    # df = df.collect()
    df.explain(show_all=True)
    # print(df)
    # df.show(n=1024, format="grid")


def test_offset_limit() -> None:
    df = daft.from_pydict(
        {
            "id": [i for i in range(1024)],
            "name": [f"name_{i}" for i in range(1024)],
            "age": [randint(1, 100) for _ in range(1024)],
        }
    )

    df = df.select("name", "age").offset(7).limit(10)
    # df = df.collect()
    # print(df)
    # df.explain(show_all=True)
    df.show(n=1024, format="grid")

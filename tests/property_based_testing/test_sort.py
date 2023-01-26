from __future__ import annotations

import datetime
from typing import Any

from hypothesis import given
from hypothesis.strategies import (
    binary,
    booleans,
    composite,
    data,
    dates,
    floats,
    integers,
    lists,
    none,
    one_of,
    sampled_from,
    text,
)


class UserObject:
    def __init__(self, x: str, y: int):
        self.x = x
        self.y = y

    def __repr__(self):
        return f"UserObject(x={self.x}, y={self.y})"


@composite
def user_object(draw) -> UserObject:
    return UserObject(x=draw(text()), y=draw(integers()))


element_strategy = sampled_from(
    [
        (text(), str),
        (integers(), int),
        (floats(), float),
        (booleans(), bool),
        (binary(), bytes),
        (dates(), datetime.date),
        (user_object(), UserObject),
    ]
)
num_rows = integers(min_value=0, max_value=128)


@composite
def column(draw, length: int = 64) -> tuple[list[Any], type]:
    drawn_element_strategy, dtype = draw(element_strategy)
    col_data = draw(lists(one_of(none(), drawn_element_strategy), min_size=length, max_size=length))
    return col_data, dtype


@composite
def dataframe(draw):
    df_len = data.draw(num_rows)
    num_cols = data.draw(integers(min_value=0, max_value=8))
    cols = [draw(column(length=df_len), label=f"Column {i}") for i in range(num_cols)]


@given(data())
def test_sort(data):
    data.draw(num_rows)

    sorted_df = df.sort()
    sorted_df.to_pydict()

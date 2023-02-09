from __future__ import annotations

from typing import Any

import pytest

from daft import DataFrame


def test_filter_missing_column(valid_data: list[dict[str, Any]]) -> None:
    df = DataFrame.from_pylist(valid_data)
    with pytest.raises(ValueError):
        df.select("sepal_width").where(df["petal_length"] > 4.8)

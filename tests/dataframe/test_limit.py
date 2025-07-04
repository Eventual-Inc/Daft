from __future__ import annotations

import pytest

import daft


def test_limit():
    df = daft.from_pydict({"id": [i for i in range(1024)], "name": [f"user_{i}" for i in range(1024)]})

    df = df.select("name").limit(1)
    pdf = df.to_pandas()

    assert len(pdf) == 1, f"Expected 1 rows, got {len(pdf)}"
    assert pdf["name"].iloc[0] == "user_0", f"Expected 'user_0', got {pdf['name'].iloc[0]}"


def test_negative_limit():
    df = daft.from_pydict({"id": [i for i in range(1024)], "name": [f"user_{i}" for i in range(1024)]})

    with pytest.raises(ValueError) as excinfo:
        df.select("name").limit(-1)

    assert "LIMIT <n> must be greater than or equal to 0, instead got: -1" in str(excinfo.value)

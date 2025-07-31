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


def test_multiple_limits():
    df = daft.range(1000, partitions=100)
    df = df.filter(daft.col("id") > 100).limit(900)
    df = df.filter(daft.col("id") > 200).limit(800)
    df = df.filter(daft.col("id") > 300).limit(700)
    df = df.filter(daft.col("id") > 400).limit(600)
    df = df.filter(daft.col("id") > 500).limit(500)
    df = df.filter(daft.col("id") > 600).limit(400)
    df = df.filter(daft.col("id") > 700).limit(300)
    df = df.filter(daft.col("id") > 800).limit(200)
    df = df.filter(daft.col("id") > 900).limit(100)

    df = df.to_pydict()
    assert df["id"] == [i for i in range(901, 1000)]

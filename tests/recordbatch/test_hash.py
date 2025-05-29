from __future__ import annotations

import daft
from daft import col


def test_table_expr_hash():
    df = daft.from_pydict(
        {
            "utf8": ["foo", "bar"],
            "int": [1, None],
        }
    )
    expected = {
        "utf8": [12352915711150947722, 15304296276065178466],
        "int": [3439722301264460078, 3244421341483603138],
    }
    result = df.select(col("utf8").hash(), col("int").hash())
    assert result.to_pydict() == expected


def test_table_expr_hash_with_seed():
    df = daft.from_pydict(
        {
            "utf8": ["foo", "bar"],
            "int": [1, None],
        }
    )
    expected = {
        "utf8": [15221504070560512414, 2671805001252040144],
        "int": [16405722695416140795, 3244421341483603138],
    }
    result = df.select(col("utf8").hash(seed=42), col("int").hash(seed=42))
    assert result.to_pydict() == expected


def test_table_expr_hash_with_seed_array():
    df = daft.from_pydict(
        {
            "utf8": ["foo", "bar"],
            "seed": [1, 1000],
        }
    )
    expected = {"utf8": [6076897603942036120, 15438169081903732554]}
    result = df.select(col("utf8").hash(seed=col("seed")))
    assert result.to_pydict() == expected


def test_table_expr_struct_hash():
    df = daft.from_pydict({"s": [{"a": 1, "b": 2}, {"a": 3, "b": 4}, {"a": 1, "b": 2}, {"a": 1, "b": 4}]})
    res = df.select(col("s").hash()).to_pydict()["s"]
    assert res[0] == res[2]
    assert res[0] != res[1] and res[1] != res[3] and res[0] != res[3]

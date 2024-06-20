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

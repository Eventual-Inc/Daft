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


def test_table_expr_hash_with_different_algorithms():
    """Test hash function with different algorithms in DataFrame context."""
    df = daft.from_pydict(
        {
            "utf8": ["foo", "bar", "baz"],
            "int": [1, 2, 3],
        }
    )

    # Test default (xxhash)
    result_default = df.select(col("utf8").hash(), col("int").hash())
    assert len(result_default.to_pydict()["utf8"]) == 3
    assert len(result_default.to_pydict()["int"]) == 3

    # Test explicit xxhash
    result_xxhash = df.select(col("utf8").hash(hash_function="xxhash"), col("int").hash(hash_function="xxhash"))
    assert len(result_xxhash.to_pydict()["utf8"]) == 3
    assert len(result_xxhash.to_pydict()["int"]) == 3

    # Test murmurhash3
    result_murmur = df.select(
        col("utf8").hash(hash_function="murmurhash3"), col("int").hash(hash_function="murmurhash3")
    )
    assert len(result_murmur.to_pydict()["utf8"]) == 3
    assert len(result_murmur.to_pydict()["int"]) == 3

    # Test sha1
    result_sha1 = df.select(col("utf8").hash(hash_function="sha1"), col("int").hash(hash_function="sha1"))
    assert len(result_sha1.to_pydict()["utf8"]) == 3
    assert len(result_sha1.to_pydict()["int"]) == 3

    # Verify different algorithms produce different results
    assert result_default.to_pydict() == result_xxhash.to_pydict()  # Same algorithm
    assert result_default.to_pydict() != result_murmur.to_pydict()  # Different algorithms
    assert result_default.to_pydict() != result_sha1.to_pydict()  # Different algorithms


def test_table_expr_hash_with_seed_and_algorithms():
    """Test hash function with seed and different algorithms in DataFrame context."""
    df = daft.from_pydict(
        {
            "utf8": ["foo", "bar", "baz"],
            "int": [1, 2, 3],
        }
    )

    # Test with seed and different algorithms
    result_xxhash_seeded = df.select(col("utf8").hash(seed=42, hash_function="xxhash"))
    result_murmur_seeded = df.select(col("utf8").hash(seed=42, hash_function="murmurhash3"))
    result_sha1_seeded = df.select(col("utf8").hash(seed=42, hash_function="sha1"))

    assert len(result_xxhash_seeded.to_pydict()["utf8"]) == 3
    assert len(result_murmur_seeded.to_pydict()["utf8"]) == 3
    assert len(result_sha1_seeded.to_pydict()["utf8"]) == 3

    # Verify seeded hashes are different from unseeded
    result_xxhash_unseeded = df.select(col("utf8").hash(hash_function="xxhash"))
    assert result_xxhash_seeded.to_pydict() != result_xxhash_unseeded.to_pydict()


def test_table_expr_hash_backward_compatibility():
    """Test that existing hash() calls work without specifying algorithm in DataFrame context."""
    df = daft.from_pydict(
        {
            "utf8": ["foo", "bar"],
            "int": [1, 2],
        }
    )

    # Test old-style call (no hash_function parameter)
    result_old = df.select(col("utf8").hash(), col("int").hash())

    # Test new-style call with default algorithm
    result_new = df.select(col("utf8").hash(hash_function="xxhash"), col("int").hash(hash_function="xxhash"))

    # Both should produce the same result
    assert result_old.to_pydict() == result_new.to_pydict()


def test_table_expr_hash_mixed_algorithms():
    """Test using different hash algorithms for different columns."""
    df = daft.from_pydict(
        {
            "utf8": ["foo", "bar", "baz"],
            "int": [1, 2, 3],
        }
    )

    # Use different algorithms for different columns
    result = df.select(col("utf8").hash(hash_function="xxhash"), col("int").hash(hash_function="murmurhash3"))

    assert len(result.to_pydict()["utf8"]) == 3
    assert len(result.to_pydict()["int"]) == 3

    # Both columns should have valid hash values
    assert all(h is not None for h in result.to_pydict()["utf8"])
    assert all(h is not None for h in result.to_pydict()["int"])

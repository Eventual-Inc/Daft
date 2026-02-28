from __future__ import annotations

import math

import pytest

import daft
from daft import col
from daft.session import Session


@pytest.fixture
def sess():
    """Session with dvector extension loaded."""
    import dvector

    s = Session()
    s.load_extension(dvector)
    return s


# ---------------------------------------------------------------------------
# L2 distance
# ---------------------------------------------------------------------------


def test_l2_fixed_float32(sess):
    """L2 distance on FixedSizeList<Float32> columns."""
    from dvector import l2_distance

    df = daft.from_pydict(
        {
            "a": [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]],
            "b": [[0.0, 1.0, 0.0], [0.0, 0.0, 1.0]],
        }
    )
    with sess:
        result = df.select(l2_distance(col("a"), col("b"))).collect().to_pydict()
    values = next(iter(result.values()))
    assert len(values) == 2
    assert abs(values[0] - math.sqrt(2.0)) < 1e-6
    assert abs(values[1] - math.sqrt(2.0)) < 1e-6


def test_l2_with_null(sess):
    """Null rows propagate as null output."""
    from dvector import l2_distance

    df = daft.from_pydict(
        {
            "a": [[1.0, 0.0], None],
            "b": [[0.0, 1.0], [1.0, 0.0]],
        }
    )
    with sess:
        result = df.select(l2_distance(col("a"), col("b"))).collect().to_pydict()
    values = next(iter(result.values()))
    assert abs(values[0] - math.sqrt(2.0)) < 1e-6
    assert values[1] is None


def test_l2_with_broadcast(sess):
    """L2 distance with a constant second column (simulates literal usage)."""
    from dvector import l2_distance

    df = daft.from_pydict(
        {
            "emb": [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]],
            "target": [[0.0, 0.0, 1.0], [0.0, 0.0, 1.0]],
        }
    )
    with sess:
        result = df.select(l2_distance(col("emb"), col("target"))).collect().to_pydict()
    values = next(iter(result.values()))
    assert abs(values[0] - math.sqrt(2.0)) < 1e-6
    assert abs(values[1] - math.sqrt(2.0)) < 1e-6


# ---------------------------------------------------------------------------
# Inner product
# ---------------------------------------------------------------------------


def test_inner_product(sess):
    """Negative inner product per pgvector convention."""
    from dvector import inner_product

    df = daft.from_pydict(
        {
            "a": [[1.0, 2.0, 3.0]],
            "b": [[4.0, 5.0, 6.0]],
        }
    )
    with sess:
        result = df.select(inner_product(col("a"), col("b"))).collect().to_pydict()
    values = next(iter(result.values()))
    # 1*4 + 2*5 + 3*6 = 32, negated = -32
    assert abs(values[0] - (-32.0)) < 1e-6


# ---------------------------------------------------------------------------
# Cosine distance
# ---------------------------------------------------------------------------


def test_cosine_distance(sess):
    """Cosine distance between identical vectors should be ~0."""
    from dvector import cosine_distance

    df = daft.from_pydict(
        {
            "a": [[1.0, 0.0], [1.0, 1.0]],
            "b": [[1.0, 0.0], [0.0, 1.0]],
        }
    )
    with sess:
        result = df.select(cosine_distance(col("a"), col("b"))).collect().to_pydict()
    values = next(iter(result.values()))
    assert abs(values[0] - 0.0) < 1e-6  # identical
    # [1,1] vs [0,1]: cosine_sim = 1/sqrt(2), distance = 1 - 1/sqrt(2)
    assert abs(values[1] - (1.0 - 1.0 / math.sqrt(2.0))) < 1e-6


def test_cosine_zero_norm(sess):
    """Cosine distance with zero-norm vector returns null, not error."""
    from dvector import cosine_distance

    df = daft.from_pydict(
        {
            "a": [[0.0, 0.0]],
            "b": [[1.0, 0.0]],
        }
    )
    with sess:
        result = df.select(cosine_distance(col("a"), col("b"))).collect().to_pydict()
    values = next(iter(result.values()))
    assert values[0] is None


# ---------------------------------------------------------------------------
# L1 distance
# ---------------------------------------------------------------------------


def test_l1_distance(sess):
    """L1 (Manhattan) distance."""
    from dvector import l1_distance

    df = daft.from_pydict(
        {
            "a": [[1.0, 2.0, 3.0]],
            "b": [[4.0, 6.0, 3.0]],
        }
    )
    with sess:
        result = df.select(l1_distance(col("a"), col("b"))).collect().to_pydict()
    values = next(iter(result.values()))
    # |1-4| + |2-6| + |3-3| = 3 + 4 + 0 = 7
    assert abs(values[0] - 7.0) < 1e-6


# ---------------------------------------------------------------------------
# Hamming distance
# ---------------------------------------------------------------------------


def test_hamming_distance(sess):
    """Hamming distance on boolean vectors."""
    from dvector import hamming_distance

    df = daft.from_pydict(
        {
            "a": [[True, False, True, True]],
            "b": [[True, True, False, True]],
        }
    )
    with sess:
        result = df.select(hamming_distance(col("a"), col("b"))).collect().to_pydict()
    values = next(iter(result.values()))
    # positions 1,2 differ => 2
    assert values[0] == 2


# ---------------------------------------------------------------------------
# Jaccard distance
# ---------------------------------------------------------------------------


def test_jaccard_distance(sess):
    """Jaccard distance on boolean vectors."""
    from dvector import jaccard_distance

    df = daft.from_pydict(
        {
            "a": [[True, True, False, False]],
            "b": [[True, False, True, False]],
        }
    )
    with sess:
        result = df.select(jaccard_distance(col("a"), col("b"))).collect().to_pydict()
    values = next(iter(result.values()))
    # union=3 (positions 0,1,2), intersection=1 (position 0)
    # jaccard = 1 - 1/3 = 2/3
    assert abs(values[0] - (2.0 / 3.0)) < 1e-6


def test_jaccard_all_false(sess):
    """Jaccard distance with all-false vectors returns null."""
    from dvector import jaccard_distance

    df = daft.from_pydict(
        {
            "a": [[False, False]],
            "b": [[False, False]],
        }
    )
    with sess:
        result = df.select(jaccard_distance(col("a"), col("b"))).collect().to_pydict()
    values = next(iter(result.values()))
    assert values[0] is None


# ---------------------------------------------------------------------------
# Session isolation
# ---------------------------------------------------------------------------


def test_function_not_available_without_extension():
    """Functions should not be available without loading the extension."""
    from dvector import l2_distance

    sess = Session()
    with sess:
        df = daft.from_pydict({"a": [[1.0, 0.0]], "b": [[0.0, 1.0]]})
        with pytest.raises(Exception, match="not found"):
            df.select(l2_distance(col("a"), col("b"))).collect()

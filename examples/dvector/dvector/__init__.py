from __future__ import annotations

from typing import TYPE_CHECKING

import daft

if TYPE_CHECKING:
    from daft.expressions import Expression


def l2_distance(a: Expression, b: Expression) -> Expression:
    """L2 (Euclidean) distance between two vectors."""
    return daft.get_function("dvector_l2_distance", a, b)


def inner_product(a: Expression, b: Expression) -> Expression:
    """Negative inner product between two vectors."""
    return daft.get_function("dvector_inner_product", a, b)


def cosine_distance(a: Expression, b: Expression) -> Expression:
    """Cosine distance (1 - cosine_similarity)."""
    return daft.get_function("dvector_cosine_distance", a, b)


def l1_distance(a: Expression, b: Expression) -> Expression:
    """L1 (Manhattan) distance between two vectors."""
    return daft.get_function("dvector_l1_distance", a, b)


def hamming_distance(a: Expression, b: Expression) -> Expression:
    """Hamming distance between two boolean vectors."""
    return daft.get_function("dvector_hamming_distance", a, b)


def jaccard_distance(a: Expression, b: Expression) -> Expression:
    """Jaccard distance between two boolean vectors."""
    return daft.get_function("dvector_jaccard_distance", a, b)

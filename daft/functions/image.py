from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from daft.expressions import Expression, lit

if TYPE_CHECKING:
    from daft.io import IOConfig


def image_hash(
    expr: Expression,
    algorithm: Literal["average", "perceptual", "difference", "wavelet", "crop_resistant"] = "average",
) -> Expression:
    """Computes the hash of an image using the specified algorithm.

    This is the functional equivalent of `col("image").image.hash(...)`.
    """
    return expr._eval_expressions("image_hash", algorithm=lit(algorithm))



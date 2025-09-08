"""Image Functions."""

from __future__ import annotations

from daft.datatype import DataType
from daft.expressions import Expression


def resize(expr: Expression, w: int, h: int) -> Expression:
    """Resize image into the provided width and height.

    Args:
        expr: Expression to resize.
        w: Desired width of the resized image.
        h: Desired height of the resized image.

    Returns:
        Expression: An Image expression representing an image column of the resized images.
    """
    return Expression._call_builtin_scalar_fn("image_resize", expr, w=w, h=h)


def crop(expr: Expression, bbox: tuple[int, int, int, int] | Expression) -> Expression:
    """Crops images with the provided bounding box.

    Args:
        expr: Expression to crop.
        bbox (tuple[float, float, float, float] | Expression): Either a tuple of (x, y, width, height)
            parameters for cropping, or a List Expression where each element is a length 4 List
            which represents the bounding box for the crop

    Returns:
        Expression: An Image expression representing the cropped image
    """
    if not isinstance(bbox, Expression):
        if len(bbox) != 4 or not all([isinstance(x, int) for x in bbox]):
            raise ValueError(f"Expected `bbox` to be either a tuple of 4 ints or an Expression but received: {bbox}")
        bbox = Expression._to_expression(bbox).cast(DataType.fixed_size_list(DataType.uint64(), 4))
    return Expression._call_builtin_scalar_fn("image_crop", expr, bbox)

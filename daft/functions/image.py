"""Image Functions."""

from __future__ import annotations

from typing import Literal

from daft.daft import ImageFormat, ImageMode
from daft.datatype import DataType
from daft.expressions import Expression, lit


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
        bbox (tuple[int, int, int, int] | Expression): Either a tuple of (x, y, width, height)
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


def encode_image(expr: Expression, image_format: str | ImageFormat) -> Expression:
    """Encode an image column as the provided image file format, returning a binary column of encoded bytes.

    Args:
        expr: The image expression to encode.
        image_format: The image file format into which the images will be encoded.

    Returns:
        Expression: A Binary expression representing a binary column of encoded image bytes.
    """
    if isinstance(image_format, str):
        image_format = ImageFormat.from_format_string(image_format.upper())
    if not isinstance(image_format, ImageFormat):
        raise ValueError(f"image_format must be a string or ImageFormat variant, but got: {image_format}")
    return Expression._call_builtin_scalar_fn("image_encode", expr, image_format=image_format)


def decode_image(
    expr: Expression,
    on_error: Literal["raise", "null"] = "raise",
    mode: str | ImageMode | None = None,
) -> Expression:
    """Decodes the binary data in this column into images.

    This can only be applied to binary columns that contain encoded images (e.g. PNG, JPEG, etc.)

    Args:
        expr: The binary expression to decode.
        on_error: Whether to raise when encountering an error, or log a warning and return a null
        mode: What mode to convert the images into before storing it in the column. This may prevent
            errors relating to unsupported types.

    Returns:
        Expression: An Image expression representing an image column.
    """
    return Expression._call_builtin_scalar_fn("image_decode", expr, on_error=on_error, mode=mode)


def convert_image(expr: Expression, mode: str | ImageMode) -> Expression:
    """Convert an image expression to the specified mode."""
    if isinstance(mode, str):
        mode = ImageMode.from_mode_string(mode.upper())
    if not isinstance(mode, ImageMode):
        raise ValueError(f"mode must be a string or ImageMode variant, but got: {mode}")
    return Expression._call_builtin_scalar_fn("to_mode", expr, mode=mode)


def image_hash(
    expr: Expression,
    algorithm: Literal["average", "perceptual", "difference", "wavelet", "crop_resistant"] = "average",
) -> Expression:
    """Computes the hash of an image using the specified algorithm.

    Args:
        expr: Expression to compute hash for.
        algorithm: The hashing algorithm to use. Options are:
            - "average": Average hash (default)
            - "perceptual": Perceptual hash
            - "difference": Difference hash
            - "wavelet": Wavelet hash
            - "crop_resistant": Crop-resistant hash

    Returns:
        Expression: A Utf8 expression representing the hash of the image.
    """
    return Expression._call_builtin_scalar_fn("image_hash", expr, algorithm=lit(algorithm))

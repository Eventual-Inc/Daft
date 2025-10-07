"""Image Functions."""

from __future__ import annotations

from typing import Literal

from daft.daft import ImageFormat, ImageMode, ImageProperty
from daft.datatype import DataType
from daft.expressions import Expression


def resize(image: Expression, w: int, h: int) -> Expression:
    """Resize image into the provided width and height.

    Args:
        expr: Expression to resize.
        w: Desired width of the resized image.
        h: Desired height of the resized image.

    Returns:
        Expression: An Image expression representing an image column of the resized images.
    """
    return Expression._call_builtin_scalar_fn("image_resize", image, w=w, h=h)


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


def image_attribute(
    expr: Expression, name: Literal["width", "height", "channel", "mode"] | ImageProperty
) -> Expression:
    """Get a property of the image, such as 'width', 'height', 'channel', or 'mode'.

    Args:
        expr: The image expression to retrieve the property from.
        name: The name of the property to retrieve.

    Returns:
        Expression: An Expression representing the requested property.
    """
    if isinstance(name, str):
        name = ImageProperty.from_property_string(name)
    return Expression._call_builtin_scalar_fn("image_attribute", expr, name)


def image_width(expr: Expression) -> Expression:
    """Gets the width of an image in pixels.

    Example:
        >>> import daft
        >>> from daft.functions import image_width
        >>> # Create a dataframe with an image column
        >>> df = ...  # doctest: +SKIP
        >>> df = df.with_column("width", image_width(df["images"]))  # doctest: +SKIP
    """
    return image_attribute(expr, "width")


def image_height(expr: Expression) -> Expression:
    """Gets the height of an image in pixels.

    Example:
        >>> import daft
        >>> from daft.functions import image_height
        >>> # Create a dataframe with an image column
        >>> df = ...  # doctest: +SKIP
        >>> df = df.with_column("height", image_height(df["images"]))  # doctest: +SKIP
    """
    return image_attribute(expr, "height")


def image_channel(expr: Expression) -> Expression:
    """Gets the number of channels in an image.

    Example:
        >>> import daft
        >>> from daft.functions import image_channel
        >>> # Create a dataframe with an image column
        >>> df = ...  # doctest: +SKIP
        >>> df = df.with_column("channel", image_channel(df["images"]))  # doctest: +SKIP
    """
    return image_attribute(expr, "channel")


def image_mode(expr: Expression) -> Expression:
    """Gets the mode of an image as a string.

    Example:
        >>> import daft
        >>> from daft.functions import image_mode
        >>> # Create a dataframe with an image column
        >>> df = ...  # doctest: +SKIP
        >>> df = df.with_column("mode", image_mode(df["images"]))  # doctest: +SKIP
    """
    return image_attribute(expr, "mode")

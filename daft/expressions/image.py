from __future__ import annotations

from typing_extensions import Literal

from daft import DataType, Expression, ImageFormat
from daft.expressions.expressions import ExpressionNamespace


class ExpressionImageNamespace(ExpressionNamespace):
    """Expression operations for image columns."""

    def decode(self, on_error: Literal["raise"] | Literal["null"] = "raise") -> Expression:
        """
        Decodes the binary data in this column into images.

        This can only be applied to binary columns that contain encoded images (e.g. PNG, JPEG, etc.)

        Args:
            on_error: Whether to raise when encountering an error, or log a warning and return a null

        Returns:
            Expression: An Image expression represnting an image column.
        """
        raise_on_error = False
        if on_error == "raise":
            raise_on_error = True
        elif on_error == "null":
            raise_on_error = False
        else:
            raise NotImplementedError(f"Unimplemented on_error option: {on_error}.")

        return Expression._from_pyexpr(self._expr.image_decode(raise_error_on_failure=raise_on_error))

    def encode(self, image_format: str | ImageFormat) -> Expression:
        """
        Encode an image column as the provided image file format, returning a binary column
        of encoded bytes.

        Args:
            image_format: The image file format into which the images will be encoded.

        Returns:
            Expression: A Binary expression representing a binary column of encoded image bytes.
        """
        if isinstance(image_format, str):
            image_format = ImageFormat.from_format_string(image_format.upper())
        if not isinstance(image_format, ImageFormat):
            raise ValueError(f"image_format must be a string or ImageFormat variant, but got: {image_format}")
        return Expression._from_pyexpr(self._expr.image_encode(image_format))

    def resize(self, w: int, h: int) -> Expression:
        """
        Resize image into the provided width and height.

        Args:
            w: Desired width of the resized image.
            h: Desired height of the resized image.

        Returns:
            Expression: An Image expression representing an image column of the resized images.
        """
        if not isinstance(w, int):
            raise TypeError(f"expected int for w but got {type(w)}")
        if not isinstance(h, int):
            raise TypeError(f"expected int for h but got {type(h)}")
        return Expression._from_pyexpr(self._expr.image_resize(w, h))

    def crop(self, bbox: tuple[int, int, int, int] | Expression) -> Expression:
        """
        Crops images with the provided bounding box

        Args:
            bbox (tuple[float, float, float, float] | Expression): Either a tuple of (x, y, width, height)
                parameters for cropping, or a List Expression where each element is a length 4 List
                which represents the bounding box for the crop

        Returns:
            Expression: An Image expression representing the cropped image
        """
        if not isinstance(bbox, Expression):
            if len(bbox) != 4 or not all([isinstance(x, int) for x in bbox]):
                raise ValueError(
                    f"Expected `bbox` to be either a tuple of 4 ints or an Expression but received: {bbox}"
                )
            bbox = Expression._to_expression(bbox).cast(DataType.fixed_size_list(DataType.uint64(), 4))
        assert isinstance(bbox, Expression)
        return Expression._from_pyexpr(self._expr.image_crop(bbox._expr))

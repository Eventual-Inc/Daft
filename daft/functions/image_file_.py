"""Image File Functions."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from daft.expressions import Expression


def image_file_metadata(
    file_expr: Expression,
) -> Expression:
    """Extract image metadata (width, height, format, mode) from a File column.

    Args:
        file_expr (File Expression): The file expression to extract metadata from.

    Returns:
        Expression (Struct Expression): A struct containing:
            - width: uint32 - Image width in pixels
            - height: uint32 - Image height in pixels
            - format: str - Image format (e.g. "png", "jpeg")
            - mode: str - Image mode (e.g. "RGB", "RGBA", "L")
    """
    from daft.expressions import Expression

    return Expression._call_builtin_scalar_fn("image_file_metadata", file_expr)


def decode_image_file(
    file_expr: Expression,
    mode: str | None = None,
    on_error: Literal["raise", "null"] = "raise",
) -> Expression:
    """Decode image files from a File column into an Image column.

    Args:
        file_expr (File Expression): The file expression to decode.
        mode (str | None, default=None): Target image mode (e.g. "RGB", "RGBA").
            If None, the mode is inferred from the image data.
        on_error (str, default="raise"): Error handling strategy.
            "raise" raises on decode failure, "null" returns null.

    Returns:
        Expression (Image Expression): The decoded image.
    """
    from daft.daft import ImageMode
    from daft.expressions import Expression

    image_mode = ImageMode.from_mode_string(mode.upper()) if isinstance(mode, str) else mode

    return Expression._call_builtin_scalar_fn("decode_image_file", file_expr, mode=image_mode, on_error=on_error)

"""Image File Functions."""

from __future__ import annotations

from typing import TYPE_CHECKING

import daft
from daft.udf.udf_v2 import Func

if TYPE_CHECKING:
    import PIL

    from daft import Expression
    from daft.file.typing import ImageMetadata


def get_metadata_impl(
    file: daft.ImageFile,
) -> ImageMetadata:
    return file.metadata()


image_file_metadata_fn = Func._from_func(
    get_metadata_impl,
    return_dtype=daft.DataType.struct(
        {
            "width": daft.DataType.int64(),
            "height": daft.DataType.int64(),
            "format": daft.DataType.string(),
            "mode": daft.DataType.string(),
        }
    ),
    unnest=False,
    use_process=None,
    is_batch=False,
    batch_size=None,
    max_retries=None,
    on_error=None,
    name_override="image_file_metadata",
)


def image_file_metadata(
    file_expr: Expression,
) -> Expression:
    """Get metadata for an image file.

    Reads only the file header to extract dimensions, format, and mode
    without decoding pixel data.

    Args:
        file_expr (ImageFile Expression): The image file to get metadata for.

    Returns:
        Expression (Struct Expression): A struct containing:
            - width: int - Image width in pixels
            - height: int - Image height in pixels
            - format: str - Image format (e.g. "PNG", "JPEG")
            - mode: str - Image mode (e.g. "RGB", "RGBA", "L")
    """
    from daft.dependencies import pil_image

    if not pil_image.module_available():
        raise ImportError(
            "The 'pillow' module is required to get image metadata. Please install it with: pip install 'daft[image]'"
        )

    return image_file_metadata_fn(file_expr)


def decode_image_file_impl(
    file: daft.ImageFile,
) -> PIL.Image.Image:
    return file.decode()


decode_image_file_fn = Func._from_func(
    decode_image_file_impl,
    return_dtype=daft.DataType.image(),
    unnest=False,
    use_process=None,
    is_batch=False,
    batch_size=None,
    max_retries=None,
    on_error=None,
    name_override="decode_image_file",
)


def decode_image_file(
    file_expr: Expression,
) -> Expression:
    """Decode an image file into an Image column.

    Downloads and decodes the image file referenced by the File(Image) column
    into the Daft Image data type.

    Args:
        file_expr (ImageFile Expression): The image file to decode.

    Returns:
        Expression (Image Expression): The decoded image.
    """
    from daft.dependencies import pil_image

    if not pil_image.module_available():
        raise ImportError(
            "The 'pillow' module is required to decode image files. Please install it with: pip install 'daft[image]'"
        )

    return decode_image_file_fn(file_expr)

from __future__ import annotations

from typing import TYPE_CHECKING

from daft.datatype import MediaType
from daft.dependencies import pil_image
from daft.file import File
from daft.file.typing import ImageMetadata

if TYPE_CHECKING:
    import PIL

    from daft.daft import PyFileReference
    from daft.io import IOConfig


class ImageFile(File):
    """An image-specific file interface that provides image operations."""

    @staticmethod
    def _from_file_reference(reference: PyFileReference) -> ImageFile:
        instance = ImageFile.__new__(ImageFile)
        instance._inner = reference
        return instance

    def __init__(self, url: str, io_config: IOConfig | None = None) -> None:
        if not pil_image.module_available():
            raise ImportError(
                "The 'pillow' module is required to create image files. "
                "Please install it with: pip install 'daft[image]'"
            )
        super().__init__(url, io_config, MediaType.image())

    def __post_init__(self) -> None:
        if not self.is_image():
            raise ValueError(f"File {self} is not an image file")

    def metadata(self) -> ImageMetadata:
        """Extract basic image metadata from file headers.

        PIL's Image.open() is lazy -- it reads only the file header to
        determine dimensions, format, and mode without decoding pixel data.

        Returns:
            ImageMetadata: Image metadata containing width, height, format, mode.
        """
        with self.open() as f:
            img = pil_image.open(f)
            return ImageMetadata(
                width=img.width,
                height=img.height,
                format=img.format,
                mode=img.mode,
            )

    def decode(self, mode: str | None = None) -> PIL.Image.Image:
        """Decode the image file into a PIL Image.

        Args:
            mode: Optional image mode to convert to (e.g. "RGB", "RGBA", "L").

        Returns:
            PIL.Image.Image: The decoded image.
        """
        with self.open() as f:
            img = pil_image.open(f)
            img.load()
            if mode is not None and img.mode != mode:
                img = img.convert(mode)
            return img

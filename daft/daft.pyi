from enum import Enum
from typing import Any

class ImageMode(Enum):
    """
    Supported image modes for Daft's image type.
    """

    #: 8-bit grayscale
    L: int

    #: 8-bit grayscale + alpha
    LA: int

    #: 8-bit RGB
    RGB: int

    #: 8-bit RGB + alpha
    RGBA: int

    #: 16-bit grayscale
    L16: int

    #: 16-bit grayscale + alpha
    LA16: int

    #: 16-bit RGB
    RGB16: int

    #: 16-bit RGB + alpha
    RGBA16: int

    #: 32-bit floating RGB
    RGB32F: int

    #: 32-bit floating RGB + alpha
    RGBA32F: int

    @staticmethod
    def from_mode_string(mode: str) -> ImageMode:
        """
        Create an ImageMode from its string representation.

        Args:
            mode: String representation of the mode. This is the same as the enum
                attribute name, e.g. ``ImageMode.from_mode_string("RGB")`` would
                return ``ImageMode.RGB``.
        """
        ...

class ImageFormat(Enum):
    """
    Supported image formats for Daft's image I/O.
    """

    PNG: int

    JPEG: int

    TIFF: int

    GIF: int

    BMP: int

    @staticmethod
    def from_format_string(mode: str) -> ImageFormat:
        """
        Create an ImageFormat from its string representation.
        """
        ...

def __getattr__(name) -> Any: ...

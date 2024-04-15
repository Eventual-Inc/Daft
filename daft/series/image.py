from __future__ import annotations

from typing_extensions import Literal

from daft import ImageFormat, Series
from daft.series.series import SeriesNamespace


class SeriesImageNamespace(SeriesNamespace):
    def decode(self, on_error: Literal["raise"] | Literal["null"] = "raise") -> Series:
        raise_on_error = False
        if on_error == "raise":
            raise_on_error = True
        elif on_error == "null":
            raise_on_error = False
        else:
            raise NotImplementedError(f"Unimplemented on_error option: {on_error}.")
        return Series._from_pyseries(self._series.image_decode(raise_error_on_failure=raise_on_error))

    def encode(self, image_format: str | ImageFormat) -> Series:
        if isinstance(image_format, str):
            image_format = ImageFormat.from_format_string(image_format.upper())
        if not isinstance(image_format, ImageFormat):
            raise ValueError(f"image_format must be a string or ImageFormat variant, but got: {image_format}")
        return Series._from_pyseries(self._series.image_encode(image_format))

    def resize(self, w: int, h: int) -> Series:
        if not isinstance(w, int):
            raise TypeError(f"expected int for w but got {type(w)}")
        if not isinstance(h, int):
            raise TypeError(f"expected int for h but got {type(h)}")

        return Series._from_pyseries(self._series.image_resize(w, h))

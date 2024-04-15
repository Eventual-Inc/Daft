from __future__ import annotations

from daft import Series
from daft.series.series import SeriesNamespace


class SeriesFloatNamespace(SeriesNamespace):
    def is_nan(self) -> Series:
        return Series._from_pyseries(self._series.is_nan())

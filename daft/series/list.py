from __future__ import annotations

from daft import Series
from daft.daft import CountMode
from daft.series.series import SeriesNamespace


class SeriesListNamespace(SeriesNamespace):
    def lengths(self) -> Series:
        return Series._from_pyseries(self._series.list_count(CountMode.All))

    def get(self, idx: Series, default: Series) -> Series:
        return Series._from_pyseries(self._series.list_get(idx._series, default._series))

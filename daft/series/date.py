from __future__ import annotations

from daft import Series
from daft.series.series import SeriesNamespace


class SeriesDateNamespace(SeriesNamespace):
    def date(self) -> Series:
        return Series._from_pyseries(self._series.dt_date())

    def day(self) -> Series:
        return Series._from_pyseries(self._series.dt_day())

    def hour(self) -> Series:
        return Series._from_pyseries(self._series.dt_hour())

    def month(self) -> Series:
        return Series._from_pyseries(self._series.dt_month())

    def year(self) -> Series:
        return Series._from_pyseries(self._series.dt_year())

    def day_of_week(self) -> Series:
        return Series._from_pyseries(self._series.dt_day_of_week())

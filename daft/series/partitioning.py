from __future__ import annotations

from daft import Series
from daft.series.series import SeriesNamespace


class SeriesPartitioningNamespace(SeriesNamespace):
    def days(self) -> Series:
        return Series._from_pyseries(self._series.partitioning_days())

    def hours(self) -> Series:
        return Series._from_pyseries(self._series.partitioning_hours())

    def months(self) -> Series:
        return Series._from_pyseries(self._series.partitioning_months())

    def years(self) -> Series:
        return Series._from_pyseries(self._series.partitioning_years())

    def iceberg_bucket(self, n: int) -> Series:
        return Series._from_pyseries(self._series.partitioning_iceberg_bucket(n))

    def iceberg_truncate(self, w: int) -> Series:
        return Series._from_pyseries(self._series.partitioning_iceberg_truncate(w))

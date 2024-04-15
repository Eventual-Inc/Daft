from __future__ import annotations

from daft import Series
from daft.series.series import SeriesNamespace


class SeriesStringNamespace(SeriesNamespace):
    def endswith(self, suffix: Series) -> Series:
        if not isinstance(suffix, Series):
            raise ValueError(f"expected another Series but got {type(suffix)}")
        assert self._series is not None and suffix._series is not None
        return Series._from_pyseries(self._series.utf8_endswith(suffix._series))

    def startswith(self, prefix: Series) -> Series:
        if not isinstance(prefix, Series):
            raise ValueError(f"expected another Series but got {type(prefix)}")
        assert self._series is not None and prefix._series is not None
        return Series._from_pyseries(self._series.utf8_startswith(prefix._series))

    def contains(self, pattern: Series) -> Series:
        if not isinstance(pattern, Series):
            raise ValueError(f"expected another Series but got {type(pattern)}")
        assert self._series is not None and pattern._series is not None
        return Series._from_pyseries(self._series.utf8_contains(pattern._series))

    def match(self, pattern: Series) -> Series:
        if not isinstance(pattern, Series):
            raise ValueError(f"expected another Series but got {type(pattern)}")
        assert self._series is not None and pattern._series is not None
        return Series._from_pyseries(self._series.utf8_match(pattern._series))

    def split(self, pattern: Series, regex: bool = False) -> Series:
        if not isinstance(pattern, Series):
            raise ValueError(f"expected another Series but got {type(pattern)}")
        assert self._series is not None and pattern._series is not None
        return Series._from_pyseries(self._series.utf8_split(pattern._series, regex))

    def concat(self, other: Series) -> Series:
        if not isinstance(other, Series):
            raise ValueError(f"expected another Series but got {type(other)}")
        assert self._series is not None and other._series is not None
        return Series._from_pyseries(self._series) + other

    def extract(self, pattern: Series, index: int = 0) -> Series:
        if not isinstance(pattern, Series):
            raise ValueError(f"expected another Series but got {type(pattern)}")
        assert self._series is not None and pattern._series is not None
        return Series._from_pyseries(self._series.utf8_extract(pattern._series, index))

    def extract_all(self, pattern: Series, index: int = 0) -> Series:
        if not isinstance(pattern, Series):
            raise ValueError(f"expected another Series but got {type(pattern)}")
        assert self._series is not None and pattern._series is not None
        return Series._from_pyseries(self._series.utf8_extract_all(pattern._series, index))

    def replace(self, pattern: Series, replacement: Series, regex: bool = False) -> Series:
        if not isinstance(pattern, Series):
            raise ValueError(f"expected another Series but got {type(pattern)}")
        if not isinstance(replacement, Series):
            raise ValueError(f"expected another Series but got {type(replacement)}")
        assert self._series is not None and pattern._series is not None
        return Series._from_pyseries(self._series.utf8_replace(pattern._series, replacement._series, regex))

    def length(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.utf8_length())

    def lower(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.utf8_lower())

    def upper(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.utf8_upper())

    def lstrip(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.utf8_lstrip())

    def rstrip(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.utf8_rstrip())

    def reverse(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.utf8_reverse())

    def capitalize(self) -> Series:
        assert self._series is not None
        return Series._from_pyseries(self._series.utf8_capitalize())

    def left(self, nchars: Series) -> Series:
        if not isinstance(nchars, Series):
            raise ValueError(f"expected another Series but got {type(nchars)}")
        assert self._series is not None and nchars._series is not None
        return Series._from_pyseries(self._series.utf8_left(nchars._series))

    def right(self, nchars: Series) -> Series:
        if not isinstance(nchars, Series):
            raise ValueError(f"expected another Series but got {type(nchars)}")
        assert self._series is not None and nchars._series is not None
        return Series._from_pyseries(self._series.utf8_right(nchars._series))

    def find(self, substr: Series) -> Series:
        if not isinstance(substr, Series):
            raise ValueError(f"expected another Series but got {type(substr)}")
        assert self._series is not None and substr._series is not None
        return Series._from_pyseries(self._series.utf8_find(substr._series))

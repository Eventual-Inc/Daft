from __future__ import annotations

from daft.datatype import DataType
from daft.series import Series


def test_series_date_year_operation() -> None:
    from datetime import date

    def date_maker(y):
        if y is None:
            return None
        return date(y, 1, 1)

    input = [1, 1969, 2023 + 5, 2023 + 4, 2023 + 1, None, 2023 + 2, None]

    days = list(map(date_maker, input))
    s = Series.from_pylist(days)
    years = s.dt_year()

    assert years.datatype() == DataType.int32()

    assert input == years.to_pylist()

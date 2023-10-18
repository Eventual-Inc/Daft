from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest

from daft import col, lit
from daft.table import MicroPartition, Table


@pytest.mark.parametrize("TableCls", [Table, MicroPartition])
def test_table_filter_all_pass(TableCls) -> None:
    pa_table = pa.Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    daft_table = TableCls.from_arrow(pa_table)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]

    exprs = [col("a") < col("b"), col("a") < 5]
    new_table = daft_table.filter(exprs)
    assert len(new_table) == 4
    assert new_table.column_names() == ["a", "b"]
    result = new_table.to_pydict()
    assert result["a"] == [1, 2, 3, 4]
    assert result["b"] == [5, 6, 7, 8]

    exprs = [lit(True), lit(True)]
    new_table = daft_table.filter(exprs)
    assert len(new_table) == 4
    assert new_table.column_names() == ["a", "b"]
    result = new_table.to_pydict()
    assert result["a"] == [1, 2, 3, 4]
    assert result["b"] == [5, 6, 7, 8]


@pytest.mark.parametrize("TableCls", [Table, MicroPartition])
def test_table_filter_some_pass(TableCls) -> None:
    pa_table = pa.Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    daft_table = TableCls.from_arrow(pa_table)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]

    exprs = [((col("a") * 4) < col("b")) | (col("b") == 8)]
    new_table = daft_table.filter(exprs)
    assert len(new_table) == 2
    assert new_table.column_names() == ["a", "b"]
    result = new_table.to_pydict()
    assert result["a"] == [1, 4]
    assert result["b"] == [5, 8]

    exprs = [(col("b") / col("a")) >= 3]
    new_table = daft_table.filter(exprs)
    assert len(new_table) == 2
    assert new_table.column_names() == ["a", "b"]
    result = new_table.to_pydict()
    assert result["a"] == [1, 2]
    assert result["b"] == [5, 6]


@pytest.mark.parametrize("TableCls", [Table, MicroPartition])
def test_table_filter_none_pass(TableCls) -> None:
    pa_table = pa.Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    daft_table = TableCls.from_arrow(pa_table)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]

    exprs = [col("a") < col("b"), col("a") > 5]
    new_table = daft_table.filter(exprs)
    assert len(new_table) == 0
    assert new_table.column_names() == ["a", "b"]
    result = new_table.to_pydict()
    assert result["a"] == []
    assert result["b"] == []

    exprs = [col("a") < col("b"), lit(False)]
    new_table = daft_table.filter(exprs)
    assert len(new_table) == 0
    assert new_table.column_names() == ["a", "b"]
    result = new_table.to_pydict()
    assert result["a"] == []
    assert result["b"] == []


@pytest.mark.parametrize("TableCls", [Table, MicroPartition])
def test_table_filter_bad_expression(TableCls) -> None:
    pa_table = pa.Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    daft_table = TableCls.from_arrow(pa_table)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]

    exprs = [col("a") + 1]

    with pytest.raises(ValueError, match="Boolean Series"):
        daft_table.filter(exprs)


@pytest.mark.parametrize("TableCls", [Table, MicroPartition])
def test_table_filter_with_dates(TableCls) -> None:
    from datetime import date

    def date_maker(d):
        if d is None:
            return None
        return date(2023, 1, d)

    days = list(map(date_maker, [5, 4, 1, None, 2, None]))
    pa_table = pa.Table.from_pydict({"days": days, "enum": [0, 1, 2, 3, 4, 5]})
    daft_table = TableCls.from_arrow(pa_table)
    assert len(daft_table) == 6
    assert daft_table.column_names() == ["days", "enum"]

    exprs = [(col("days") > date(2023, 1, 2)) & (col("enum") > 0)]
    new_table = daft_table.filter(exprs)
    assert len(new_table) == 1
    assert new_table.column_names() == ["days", "enum"]
    result = new_table.to_pydict()
    assert result["days"] == [date(2023, 1, 4)]
    assert result["enum"] == [1]


@pytest.mark.parametrize("TableCls", [Table, MicroPartition])
def test_table_filter_with_date_days(TableCls) -> None:
    from datetime import date

    def date_maker(d):
        if d is None:
            return None
        return date(2023, 1, d)

    days = list(map(date_maker, [3, 28, None, 9, 18, None]))
    pa_table = pa.Table.from_pydict({"days": days, "enum": [0, 1, 2, 3, 4, 5]})
    daft_table = TableCls.from_arrow(pa_table)
    assert len(daft_table) == 6
    assert daft_table.column_names() == ["days", "enum"]

    exprs = [col("days").dt.day() > 15]
    new_table = daft_table.filter(exprs)
    assert len(new_table) == 2
    assert new_table.column_names() == ["days", "enum"]
    result = new_table.to_pydict()
    assert result["days"] == [date(2023, 1, 28), date(2023, 1, 18)]
    assert result["enum"] == [1, 4]


@pytest.mark.parametrize("TableCls", [Table, MicroPartition])
def test_table_filter_with_date_months(TableCls) -> None:
    from datetime import date

    def date_maker(m):
        if m is None:
            return None
        return date(2023, m, 1)

    days = list(map(date_maker, [2, 6, None, 4, 11, None]))
    pa_table = pa.Table.from_pydict({"days": days, "enum": [0, 1, 2, 3, 4, 5]})
    daft_table = TableCls.from_arrow(pa_table)
    assert len(daft_table) == 6
    assert daft_table.column_names() == ["days", "enum"]

    exprs = [col("days").dt.month() > 5]
    new_table = daft_table.filter(exprs)
    assert len(new_table) == 2
    assert new_table.column_names() == ["days", "enum"]
    result = new_table.to_pydict()
    assert result["days"] == [date(2023, 6, 1), date(2023, 11, 1)]
    assert result["enum"] == [1, 4]


@pytest.mark.parametrize("TableCls", [Table, MicroPartition])
def test_table_filter_with_date_years(TableCls) -> None:
    from datetime import date

    def date_maker(y):
        if y is None:
            return None
        return date(y, 1, 1)

    days = list(map(date_maker, [5, 4000, 1, None, 2022, None]))
    pa_table = pa.Table.from_pydict({"days": days, "enum": [0, 1, 2, 3, 4, 5]})
    daft_table = TableCls.from_arrow(pa_table)
    assert len(daft_table) == 6
    assert daft_table.column_names() == ["days", "enum"]

    exprs = [col("days").dt.year() > 2000]
    new_table = daft_table.filter(exprs)
    assert len(new_table) == 2
    assert new_table.column_names() == ["days", "enum"]
    result = new_table.to_pydict()
    assert result["days"] == [date(4000, 1, 1), date(2022, 1, 1)]
    assert result["enum"] == [1, 4]


@pytest.mark.parametrize("TableCls", [Table, MicroPartition])
def test_table_filter_with_date_days_of_week(TableCls) -> None:
    from datetime import date

    def date_maker(d):
        if d is None:
            return None
        return date(2023, 4, d)

    # 04/03/2023 is a Monday.
    days = list(map(date_maker, [8, 5, None, 15, 12, None]))
    pa_table = pa.Table.from_pydict({"days": days, "enum": [0, 1, 2, 3, 4, 5]})
    daft_table = TableCls.from_arrow(pa_table)
    assert len(daft_table) == 6
    assert daft_table.column_names() == ["days", "enum"]

    exprs = [col("days").dt.day_of_week() == 2]
    new_table = daft_table.filter(exprs)
    assert len(new_table) == 2
    assert new_table.column_names() == ["days", "enum"]
    result = new_table.to_pydict()
    assert result["days"] == [date(2023, 4, 5), date(2023, 4, 12)]
    assert result["enum"] == [1, 4]


@pytest.mark.parametrize("TableCls", [Table, MicroPartition])
def test_table_float_is_nan(TableCls) -> None:
    table = TableCls.from_pydict({"a": [1.0, np.nan, 3.0, None, float("nan")]})
    result_table = table.eval_expression_list([col("a").float.is_nan()])
    # Note that null entries are _not_ treated as float NaNs.
    assert result_table.to_pydict() == {"a": [False, True, False, None, True]}


@pytest.mark.parametrize("TableCls", [Table, MicroPartition])
def test_table_if_else(TableCls) -> None:
    table = TableCls.from_arrow(
        pa.Table.from_pydict({"ones": [1, 1, 1], "zeros": [0, 0, 0], "pred": [True, False, None]})
    )
    result_table = table.eval_expression_list([col("pred").if_else(col("ones"), col("zeros"))])
    assert result_table.to_pydict() == {"ones": [1, 0, None]}

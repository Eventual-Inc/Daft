from __future__ import annotations

import copy

import pyarrow as pa
import pytest

from daft.datatype import DataType
from daft.expressions import ExpressionsProjection, col
from daft.logical.schema import Schema
from daft.table import MicroPartition

DATA = {
    "int": ([1, 2, None], DataType.int64()),
    "float": ([1.0, 2.0, None], DataType.float64()),
    "string": (["a", "b", None], DataType.string()),
    "bool": ([True, True, None], DataType.bool()),
}

TABLE = MicroPartition.from_pydict({k: data for k, (data, _) in DATA.items()})
EXPECTED_TYPES = {k: t for k, (_, t) in DATA.items()}

from tests.utils import ANSI_ESCAPE, TD_STYLE, TH_STYLE


def test_schema_len():
    schema = TABLE.schema()
    assert len(schema) == len(DATA)


def test_schema_column_names():
    schema = TABLE.schema()
    assert schema.column_names() == list(DATA.keys())


def test_schema_field_types():
    schema = TABLE.schema()
    for key in EXPECTED_TYPES:
        assert schema[key].name == key
        assert schema[key].dtype == EXPECTED_TYPES[key]


def test_schema_iter():
    schema = TABLE.schema()
    for expected_name, field in zip(EXPECTED_TYPES, schema):
        assert field.name == expected_name
        assert field.dtype == EXPECTED_TYPES[expected_name]


def test_schema_eq():
    t1, t2 = (
        MicroPartition.from_pydict({k: data for k, (data, _) in DATA.items()}),
        MicroPartition.from_pydict({k: data for k, (data, _) in DATA.items()}),
    )
    s1, s2 = t1.schema(), t2.schema()
    assert s1 == s2

    t_empty = MicroPartition.empty()
    assert s1 != t_empty.schema()


def test_schema_to_name_set():
    schema = TABLE.schema()
    assert schema.to_name_set() == set(DATA.keys())


def test_truncated_repr():
    schema = TABLE.schema()
    out_repr = schema._truncated_table_string()
    without_escape = ANSI_ESCAPE.sub("", out_repr)
    assert (
        without_escape.replace("\r", "")
        == """╭───────┬─────────┬────────┬─────────╮
│ int   ┆ float   ┆ string ┆ bool    │
│ ---   ┆ ---     ┆ ---    ┆ ---     │
│ Int64 ┆ Float64 ┆ Utf8   ┆ Boolean │
╰───────┴─────────┴────────┴─────────╯
"""
    )


def test_truncated_repr_html():
    schema = TABLE.schema()
    out_repr = schema._truncated_table_html()
    assert (
        out_repr
        == f"""<table class="dataframe">
<thead><tr><th {TH_STYLE}>int<br />Int64</th><th {TH_STYLE}>float<br />Float64</th><th {TH_STYLE}>string<br />Utf8</th><th {TH_STYLE}>bool<br />Boolean</th></tr></thead>
</table>"""
    )


def test_repr():
    schema = TABLE.schema()
    out_repr = repr(schema)
    without_escape = ANSI_ESCAPE.sub("", out_repr)
    assert (
        without_escape.replace("\r", "")
        == """╭─────────────┬─────────╮
│ Column Name ┆ Type    │
╞═════════════╪═════════╡
│ int         ┆ Int64   │
├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ float       ┆ Float64 │
├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ string      ┆ Utf8    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ bool        ┆ Boolean │
╰─────────────┴─────────╯
"""
    )


def test_repr_html():
    schema = TABLE.schema()
    out_repr = schema._repr_html_()
    print(out_repr)
    assert (
        out_repr
        == f"""<table class="dataframe">
<thead><tr><th {TH_STYLE}>Column Name</th><th {TH_STYLE}>Type</th></tr></thead>
<tbody>
<tr><td {TD_STYLE}>int</td><td {TD_STYLE}>Int64</td></tr>
<tr><td {TD_STYLE}>float</td><td {TD_STYLE}>Float64</td></tr>
<tr><td {TD_STYLE}>string</td><td {TD_STYLE}>Utf8</td></tr>
<tr><td {TD_STYLE}>bool</td><td {TD_STYLE}>Boolean</td></tr>
</tbody>
</table>"""
    )


def test_to_col_expr():
    schema = TABLE.schema()
    schema_col_exprs = ExpressionsProjection.from_schema(schema)
    expected_col_exprs = [col(n) for n in schema.column_names()]

    assert len(schema_col_exprs) == len(expected_col_exprs)
    for sce, ece in zip(schema_col_exprs, expected_col_exprs):
        assert sce.name() == ece.name()


def test_union():
    schema = TABLE.schema()
    with pytest.raises(ValueError):
        schema.union(schema)

    new_data = {f"{k}_": d for k, (d, _) in DATA.items()}
    new_table = MicroPartition.from_pydict(new_data)
    unioned_schema = schema.union(new_table.schema())

    assert unioned_schema.column_names() == list(DATA.keys()) + list(new_data.keys())
    assert list(unioned_schema) == list(schema) + list(new_table.schema())


def test_from_field_name_and_types():
    schema = Schema._from_field_name_and_types([("foo", DataType.int16())])
    assert schema["foo"].name == "foo"
    assert schema["foo"].dtype == DataType.int16()


def test_from_empty_field_name_and_types():
    schema = Schema._from_field_name_and_types([])
    assert len(schema) == 0


def test_field_pickling():
    schema = Schema._from_field_name_and_types([("foo", DataType.int16())])
    f1 = schema["foo"]
    f1_copy = copy.deepcopy(f1)
    assert f1_copy.dtype == f1.dtype
    assert f1_copy.name == f1.name
    assert f1_copy == f1


def test_schema_pickling():
    t1, t2 = (
        MicroPartition.from_pydict({k: data for k, (data, _) in DATA.items()}),
        MicroPartition.from_pydict({k: data for k, (data, _) in DATA.items()}),
    )

    s1, s2 = t1.schema(), t2.schema()

    s1 = copy.deepcopy(s1)

    assert s1 == s2

    t_empty = MicroPartition.empty()
    assert s1 != t_empty.schema()
    t_empty_schema_copy = copy.deepcopy(t_empty.schema())
    assert t_empty.schema() == t_empty_schema_copy


def test_schema_pyarrow_roundtrip():
    pa_schema = pa.schema(
        {
            "int": pa.int64(),
            "str": pa.string(),
            "list": pa.list_(pa.int64()),
            "map": pa.map_(pa.string(), pa.int64()),
        }
    )

    expected_daft_schema = Schema._from_field_name_and_types(
        [
            ("int", DataType.int64()),
            ("str", DataType.string()),
            ("list", DataType.list(DataType.int64())),
            ("map", DataType.map(DataType.string(), DataType.int64())),
        ]
    )

    assert Schema.from_pyarrow_schema(pa_schema) == expected_daft_schema

    roundtrip_pa_schema = pa.schema(
        {
            "int": pa.int64(),
            "str": pa.large_string(),
            "list": pa.large_list(pa.int64()),
            "map": pa.map_(pa.large_string(), pa.int64()),
        }
    )

    assert Schema.from_pyarrow_schema(pa_schema).to_pyarrow_schema() == roundtrip_pa_schema

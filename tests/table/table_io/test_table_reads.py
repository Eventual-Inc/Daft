# from __future__ import annotations

# import copy
# from typing import Callable

from __future__ import annotations

# from daft.datatype import DataType
# from daft.logical.schema import Schema
# from daft.runners.partitioning import TableParseCSVOptions, TableReadOptions
# from daft.table import table_io
# from tests.table.table_io.conftest import InputType
# ###
# # JSON
# ###

# JSON_DTYPES_HINTS = {
#     # TODO(jaychia): [SCHEMA] fix nested type casting logic
#     # "dates": DataType.date(),
#     "strings": DataType.string(),
#     "integers": DataType.int8(),
#     "floats": DataType.float32(),
#     "bools": DataType.bool(),
#     # TODO(jaychia): [SCHEMA] fix nested type casting logic
#     # "var_sized_arrays": DataType.list("item", DataType.int32()),
#     # "fixed_sized_arrays": DataType.fixed_size_list("item", DataType.int32(), 4),
#     # "structs": DataType.struct({"foo": DataType.int32()}),
# }
# JSON_SCHEMA = Schema._from_field_name_and_types(list(JSON_DTYPES_HINTS.items()))


# def test_json_reads(json_input, json_expected_data):
#     table = table_io.read_json_with_schema(json_input, JSON_SCHEMA)
#     d = table.to_pydict()
#     assert d == json_expected_data


# def test_json_reads_limit_rows(json_input, json_expected_data):
#     row_limit = 3
#     table = table_io.read_json_with_schema(
#         json_input, JSON_SCHEMA, read_options=TableReadOptions(num_rows=row_limit)
#     )
#     d = table.to_pydict()
#     assert d == {k: v[:row_limit] for k, v in json_expected_data.items()}


# def test_json_reads_pruned_columns(json_input, json_expected_data):
#     included_columns = ["strings", "integers"]
#     table = table_io.read_json_with_schema(
#         json_input, JSON_SCHEMA, read_options=TableReadOptions(column_names=included_columns)
#     )
#     d = table.to_pydict()
#     assert d == {k: v for k, v in json_expected_data.items() if k in included_columns}


# ###
# # Parquet
# ###


# PARQUET_DTYPES_HINTS = {
#     "dates": DataType.date(),
#     "strings": DataType.string(),
#     "integers": DataType.int8(),
#     "floats": DataType.float32(),
#     "bools": DataType.bool(),
#     # TODO(jaychia): [SCHEMA] fix nested type casting logic
#     # "var_sized_arrays": DataType.list("item", DataType.int32()),
#     # "fixed_sized_arrays": DataType.fixed_size_list("item", DataType.int32(), 4),
#     # "structs": DataType.struct({"foo": DataType.int32()}),
# }
# PARQUET_SCHEMA = Schema._from_field_name_and_types(list(PARQUET_DTYPES_HINTS.items()))


# def test_parquet_reads(parquet_input, parquet_expected_data):
#     table = table_io.read_parquet_with_schema(parquet_input, PARQUET_SCHEMA)
#     d = table.to_pydict()
#     assert d == parquet_expected_data


# def test_parquet_reads_limit_rows(parquet_input, parquet_expected_data):
#     row_limit = 3
#     table = table_io.read_parquet_with_schema(
#         parquet_input, PARQUET_SCHEMA, read_options=TableReadOptions(num_rows=row_limit)
#     )
#     d = table.to_pydict()
#     assert d == {k: v[:row_limit] for k, v in parquet_expected_data.items()}


# def test_parquet_reads_no_rows(parquet_input, parquet_expected_data):
#     row_limit = 0
#     table = table_io.read_parquet_with_schema(
#         parquet_input, PARQUET_SCHEMA, read_options=TableReadOptions(num_rows=row_limit)
#     )
#     d = table.to_pydict()
#     assert d == {k: [] for k, _ in parquet_expected_data.items()}


# def test_parquet_reads_pruned_columns(parquet_input, parquet_expected_data):
#     included_columns = ["strings", "integers"]
#     table = table_io.read_parquet_with_schema(
#         parquet_input, PARQUET_SCHEMA, read_options=TableReadOptions(column_names=included_columns)
#     )
#     d = table.to_pydict()
#     assert d == {k: v for k, v in parquet_expected_data.items() if k in included_columns}


# ## Pickling


# def test_table_pickling(parquet_input: str):
#     table = table_io.read_parquet_with_schema(parquet_input, PARQUET_SCHEMA)
#     copied_table = copy.deepcopy(table)

#     assert table.column_names() == copied_table.column_names()

#     for name in table.column_names():
#         assert table.get_column(name).datatype() == copied_table.get_column(name).datatype()
#         assert table.get_column(name).to_pylist() == copied_table.get_column(name).to_pylist()

from __future__ import annotations

import contextlib
import copy
import csv
import datetime
import json
import pathlib
import sys
from typing import Callable, Union

import pyarrow as pa
import pytest
from pyarrow import parquet as papq

if sys.version_info < (3, 8):
    from typing_extensions import Literal
else:
    from typing import Literal

from daft.datatype import DataType
from daft.logical.schema import Schema
from daft.runners.partitioning import (
    vPartitionParseCSVOptions,
    vPartitionReadOptions,
    vPartitionSchemaInferenceOptions,
)
from daft.table import table_io

InputType = Union[Literal["file"], Literal["filepath"], Literal["pathlib.Path"]]
TEST_INPUT_TYPES = ["file", "path", "pathlib.Path"]

TEST_DATA_LEN = 16
TEST_DATA = {
    "dates": [datetime.date(2020, 1, 1) + datetime.timedelta(days=i) for i in range(TEST_DATA_LEN)],
    "strings": [f"foo_{i}" for i in range(TEST_DATA_LEN)],
    "integers": [i for i in range(TEST_DATA_LEN)],
    "floats": [float(i) for i in range(TEST_DATA_LEN)],
    "bools": [True for i in range(TEST_DATA_LEN)],
    "var_sized_arrays": [[i for _ in range(i)] for i in range(TEST_DATA_LEN)],
    "fixed_sized_arrays": [[i for _ in range(4)] for i in range(TEST_DATA_LEN)],
    "structs": [{"foo": i} for i in range(TEST_DATA_LEN)],
}


@contextlib.contextmanager
def _resolve_parametrized_input_type(input_type: InputType, path: str) -> table_io.FileInput:
    if input_type == "file":
        with open(path, "rb") as f:
            yield f
    elif input_type == "path":
        yield path
    elif input_type == "pathlib.Path":
        yield pathlib.Path(path)
    else:
        raise NotImplementedError(f"input_type={input_type}")


###
# JSON
###


JSON_EXPECTED_DATA = {
    # NOTE: PyArrow JSON parser parses dates as timestamps, so this fails for us at the moment
    # as we still lack timestamp type support.
    # "dates": [datetime.datetime(d.year, d.month, d.day) for d in TEST_DATA["dates"]],
    "strings": TEST_DATA["strings"],
    "integers": TEST_DATA["integers"],
    "floats": TEST_DATA["floats"],
    "bools": TEST_DATA["bools"],
    "var_sized_arrays": TEST_DATA["var_sized_arrays"],
    "fixed_sized_arrays": TEST_DATA["fixed_sized_arrays"],
    "structs": TEST_DATA["structs"],
}


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.date):
            return obj.isoformat()
        else:
            return super().default(obj)


@pytest.fixture(scope="function", params=[(it,) for it in TEST_INPUT_TYPES])
def json_input(request, tmpdir: str) -> str:
    (input_type,) = request.param

    # NOTE: PyArrow JSON parser parses dates as timestamps, so this fails for us at the moment
    # as we still lack timestamp type support.
    skip_columns = {"dates"}

    path = str(tmpdir + f"/data.json")
    with open(path, "wb") as f:
        for row in range(TEST_DATA_LEN):
            row_data = {cname: TEST_DATA[cname][row] for cname in TEST_DATA if cname not in skip_columns}
            f.write(json.dumps(row_data, default=CustomJSONEncoder().default).encode("utf-8"))
            f.write(b"\n")

    with _resolve_parametrized_input_type(input_type, path) as table_io_input:
        yield table_io_input


def test_json_reads(json_input):
    table = table_io.read_json(json_input)
    d = table.to_pydict()
    assert d == JSON_EXPECTED_DATA


def test_json_reads_limit_rows(json_input: str):
    row_limit = 3
    table = table_io.read_json(json_input, read_options=vPartitionReadOptions(num_rows=row_limit))
    d = table.to_pydict()
    assert d == {k: v[:row_limit] for k, v in JSON_EXPECTED_DATA.items()}


def test_json_reads_pruned_columns(json_input: str):
    included_columns = ["strings", "integers"]
    table = table_io.read_json(json_input, read_options=vPartitionReadOptions(column_names=included_columns))
    d = table.to_pydict()
    assert d == {k: v for k, v in PARQUET_EXPECTED_DATA.items() if k in included_columns}


###
# Parquet
###


PARQUET_EXPECTED_DATA = {
    "dates": TEST_DATA["dates"],
    "strings": TEST_DATA["strings"],
    "integers": TEST_DATA["integers"],
    "floats": TEST_DATA["floats"],
    "bools": TEST_DATA["bools"],
    "var_sized_arrays": TEST_DATA["var_sized_arrays"],
    "fixed_sized_arrays": TEST_DATA["fixed_sized_arrays"],
    "structs": TEST_DATA["structs"],
}


@pytest.fixture(scope="function")
def parquet_input(tmpdir: str) -> str:
    path = str(tmpdir + f"/data.parquet")
    papq.write_table(pa.Table.from_pydict(TEST_DATA), path)
    yield path


@pytest.mark.parametrize(["input_type"], [(ip,) for ip in TEST_INPUT_TYPES])
def test_parquet_reads(parquet_input: str, input_type: InputType):
    with _resolve_parametrized_input_type(input_type, parquet_input) as table_io_input:
        table = table_io.read_parquet(table_io_input)
        d = table.to_pydict()
        assert d == PARQUET_EXPECTED_DATA


def test_parquet_reads_limit_rows(parquet_input: str):
    row_limit = 3
    table = table_io.read_parquet(parquet_input, read_options=vPartitionReadOptions(num_rows=row_limit))
    d = table.to_pydict()
    assert d == {k: v[:row_limit] for k, v in PARQUET_EXPECTED_DATA.items()}


def test_parquet_reads_no_rows(parquet_input: str):
    row_limit = 0
    table = table_io.read_parquet(parquet_input, read_options=vPartitionReadOptions(num_rows=row_limit))
    d = table.to_pydict()
    assert d == {k: [] for k, _ in PARQUET_EXPECTED_DATA.items()}


def test_parquet_reads_pruned_columns(parquet_input: str):
    included_columns = ["strings", "integers"]
    table = table_io.read_parquet(parquet_input, read_options=vPartitionReadOptions(column_names=included_columns))
    d = table.to_pydict()
    assert d == {k: v for k, v in PARQUET_EXPECTED_DATA.items() if k in included_columns}


###
# CSV
###


CSV_EXPECTED_DATA = {
    "dates": TEST_DATA["dates"],
    "strings": TEST_DATA["strings"],
    "integers": TEST_DATA["integers"],
    "floats": TEST_DATA["floats"],
    "bools": TEST_DATA["bools"],
    "var_sized_arrays": [str(l) for l in TEST_DATA["var_sized_arrays"]],
    "fixed_sized_arrays": [str(l) for l in TEST_DATA["fixed_sized_arrays"]],
    "structs": [str(s) for s in TEST_DATA["structs"]],
}
CSV_DTYPES_HINTS = {
    "dates": DataType.date(),
    "strings": DataType.string(),
    "integers": DataType.int8(),
    "floats": DataType.float32(),
    "bools": DataType.bool(),
    "var_sized_arrays": DataType.string(),
    "fixed_sized_arrays": DataType.string(),
    "structs": DataType.string(),
}


@pytest.fixture(scope="function", params=[(it,) for it in TEST_INPUT_TYPES])
def generate_csv_input(request, tmpdir: str) -> Callable[[vPartitionParseCSVOptions], str]:
    (input_type,) = request.param

    @contextlib.contextmanager
    def _generate(csv_options) -> InputType:
        path = str(tmpdir + f"/data.csv")
        headers = [cname for cname in TEST_DATA]
        with open(path, "w") as f:
            writer = csv.writer(f, delimiter=csv_options.delimiter)
            if csv_options.header_index is not None:
                for _ in range(csv_options.header_index):
                    writer.writerow(["extra-data-before-header" for _ in TEST_DATA])
                writer.writerow(headers)
            writer.writerows([[TEST_DATA[cname][row] for cname in headers] for row in range(TEST_DATA_LEN)])

        with _resolve_parametrized_input_type(input_type, path) as table_io_input:
            yield table_io_input

    yield _generate


def test_csv_reads_infer_schema(generate_csv_input: Callable[[vPartitionParseCSVOptions], InputType]):
    with generate_csv_input(vPartitionParseCSVOptions()) as table_io_input:
        table = table_io.read_csv_infer_schema(table_io_input)
        d = table.to_pydict()
        assert d == CSV_EXPECTED_DATA


def test_csv_reads_provided_schema(generate_csv_input: Callable[[vPartitionParseCSVOptions], InputType]):
    schema = Schema._from_field_name_and_types(list(CSV_DTYPES_HINTS.items()))
    with generate_csv_input(vPartitionParseCSVOptions()) as table_io_input:
        table = table_io.read_csv_with_schema(table_io_input, schema=schema)
        d = table.to_pydict()
        assert d == CSV_EXPECTED_DATA
        assert table.schema() == schema


def test_csv_reads_provided_schema_differing_headers(
    generate_csv_input: Callable[[vPartitionParseCSVOptions], InputType]
):
    # Reverse the ordering of the schema, but this should be resolved against the headers available on the CSV
    schema = Schema._from_field_name_and_types(list(reversed(CSV_DTYPES_HINTS.items())))
    with generate_csv_input(vPartitionParseCSVOptions()) as table_io_input:
        table = table_io.read_csv_with_schema(table_io_input, schema=schema)
        d = table.to_pydict()
        assert d == CSV_EXPECTED_DATA
        assert table.schema() == schema


def test_csv_reads_limit_rows_infer_schema(generate_csv_input: Callable[[vPartitionParseCSVOptions], InputType]):
    row_limit = 3
    with generate_csv_input(vPartitionParseCSVOptions()) as table_io_input:
        table = table_io.read_csv_infer_schema(table_io_input, read_options=vPartitionReadOptions(num_rows=row_limit))
        d = table.to_pydict()
        assert d == {k: v[:row_limit] for k, v in CSV_EXPECTED_DATA.items()}


def test_csv_reads_limit_rows_provided_schema(generate_csv_input: Callable[[vPartitionParseCSVOptions], InputType]):
    schema = Schema._from_field_name_and_types(list(CSV_DTYPES_HINTS.items()))
    row_limit = 3
    with generate_csv_input(vPartitionParseCSVOptions()) as table_io_input:
        table = table_io.read_csv_with_schema(
            table_io_input, schema, read_options=vPartitionReadOptions(num_rows=row_limit)
        )
        d = table.to_pydict()
        assert d == {k: v[:row_limit] for k, v in CSV_EXPECTED_DATA.items()}
        assert table.schema() == schema


def test_csv_reads_pruned_columns_infer_schema(generate_csv_input: Callable[[vPartitionParseCSVOptions], InputType]):
    included_columns = ["strings", "integers"]
    with generate_csv_input(vPartitionParseCSVOptions()) as table_io_input:
        table = table_io.read_csv_infer_schema(
            table_io_input, read_options=vPartitionReadOptions(column_names=included_columns)
        )
        d = table.to_pydict()
        assert d == {k: v for k, v in CSV_EXPECTED_DATA.items() if k in included_columns}


def test_csv_reads_pruned_columns_provided_schema(generate_csv_input: Callable[[vPartitionParseCSVOptions], InputType]):
    schema = Schema._from_field_name_and_types(list(CSV_DTYPES_HINTS.items()))
    included_columns = ["strings", "integers"]
    with generate_csv_input(vPartitionParseCSVOptions()) as table_io_input:
        table = table_io.read_csv_with_schema(
            table_io_input, schema, read_options=vPartitionReadOptions(column_names=included_columns)
        )
        d = table.to_pydict()
        assert d == {k: v for k, v in CSV_EXPECTED_DATA.items() if k in included_columns}
        assert table.schema() == Schema._from_field_name_and_types([(k, schema[k].dtype) for k in included_columns])


@pytest.mark.parametrize(
    ["csv_options", "schema_options"],
    [
        # Default options - headers present with default delimiter (",")
        (vPartitionParseCSVOptions(), vPartitionSchemaInferenceOptions()),
        # No headers, but inference_column_names is provided
        (
            vPartitionParseCSVOptions(header_index=None),
            vPartitionSchemaInferenceOptions(inference_column_names=list(TEST_DATA.keys())),
        ),
        # Has headers, but provide inference_column_names so we should skip the first row of headers
        (
            vPartitionParseCSVOptions(header_index=0),
            vPartitionSchemaInferenceOptions(inference_column_names=list(TEST_DATA.keys())),
        ),
        # Custom delimiter
        (vPartitionParseCSVOptions(delimiter="|"), vPartitionSchemaInferenceOptions()),
    ],
)
def test_csv_reads_custom_options_infer_schema(
    generate_csv_input: Callable[[vPartitionParseCSVOptions], InputType],
    csv_options: vPartitionParseCSVOptions,
    schema_options: vPartitionSchemaInferenceOptions,
):
    with generate_csv_input(csv_options) as table_io_input:
        table = table_io.read_csv_infer_schema(
            table_io_input, override_column_names=schema_options.inference_column_names, csv_options=csv_options
        )
        d = table.to_pydict()
        assert d == CSV_EXPECTED_DATA


## Pickling


@pytest.mark.parametrize(["input_type"], [(ip,) for ip in TEST_INPUT_TYPES])
def test_table_pickling(parquet_input: str, input_type: InputType):
    with _resolve_parametrized_input_type(input_type, parquet_input) as table_io_input:
        table = table_io.read_parquet(table_io_input)
        copied_table = copy.deepcopy(table)

        assert table.column_names() == copied_table.column_names()

        for name in table.column_names():
            assert table.get_column(name).datatype() == copied_table.get_column(name).datatype()
            assert table.get_column(name).to_pylist() == copied_table.get_column(name).to_pylist()

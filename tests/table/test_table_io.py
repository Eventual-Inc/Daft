from __future__ import annotations

import contextlib
import csv
import dataclasses
import datetime
import json
import pathlib
from typing import Literal

import pyarrow as pa
import pytest
from pyarrow import parquet as papq

from daft.runners.partitioning import (
    vPartitionParseCSVOptions,
    vPartitionSchemaInferenceOptions,
)
from daft.table import table_io

TEST_DATA_LEN = 16
TEST_DATA = {
    # TODO: [RUST-INT] conversions back to dates is currently bad. We can should add these tests into a Hypothesis
    # encode/decode test: https://hypothesis.works/articles/encode-decode-invariant/ to test across all our different datatypes
    "dates": [datetime.date(2020, 1, 1) + datetime.timedelta(days=i) for i in range(TEST_DATA_LEN)],
    "strings": [f"foo_{i}" for i in range(TEST_DATA_LEN)],
    "integers": [i for i in range(TEST_DATA_LEN)],
    "floats": [float(i) for i in range(TEST_DATA_LEN)],
    "bools": [True for i in range(TEST_DATA_LEN)],
    "structs": [{"foo": i} for i in range(TEST_DATA_LEN)],
    "fixed_sized_arrays": [[i for _ in range(4)] for i in range(TEST_DATA_LEN)],
    "var_sized_arrays": [[i for _ in range(i)] for i in range(TEST_DATA_LEN)],
}

PARQUET_EXPECTED_DATA = {
    "dates": TEST_DATA["dates"],
    "strings": TEST_DATA["strings"],
    "integers": TEST_DATA["integers"],
    "floats": TEST_DATA["floats"],
    "bools": TEST_DATA["bools"],
    "structs": TEST_DATA["structs"],
    "fixed_sized_arrays": TEST_DATA["fixed_sized_arrays"],
    "var_sized_arrays": TEST_DATA["var_sized_arrays"],
}

JSON_EXPECTED_DATA = {
    # NOTE: JSON decoder parses isoformat dates as datetimes - seems incorrect.
    "dates": [datetime.datetime(d.year, d.month, d.day) for d in TEST_DATA["dates"]],
    "strings": TEST_DATA["strings"],
    "integers": TEST_DATA["integers"],
    "floats": TEST_DATA["floats"],
    "bools": TEST_DATA["bools"],
    "structs": TEST_DATA["structs"],
    "fixed_sized_arrays": TEST_DATA["fixed_sized_arrays"],
    "var_sized_arrays": TEST_DATA["var_sized_arrays"],
}

CSV_EXPECTED_DATA = {
    "dates": TEST_DATA["dates"],
    "strings": TEST_DATA["strings"],
    "integers": TEST_DATA["integers"],
    "floats": TEST_DATA["floats"],
    "bools": TEST_DATA["bools"],
    "structs": [str(s) for s in TEST_DATA["structs"]],
    "fixed_sized_arrays": [str(l) for l in TEST_DATA["fixed_sized_arrays"]],
    "var_sized_arrays": [str(l) for l in TEST_DATA["var_sized_arrays"]],
}


InputType = Literal["file"] | Literal["filepath"] | Literal["pathlib.Path"]


@dataclasses.dataclass(frozen=True)
class TableIOTestParams:
    input_type: InputType


TEST_INPUT_TYPES = ["file", "path", "pathlib.Path"]


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.date):
            return obj.isoformat()
        else:
            return super().default(obj)


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


@pytest.fixture(scope="function")
def json_input(tmpdir: str) -> str:
    path = tmpdir + f"/data.json"
    with open(path, "wb") as f:
        for row in range(TEST_DATA_LEN):
            row_data = {cname: TEST_DATA[cname][row] for cname in TEST_DATA}
            f.write(json.dumps(row_data, default=CustomJSONEncoder().default).encode("utf-8"))
            f.write(b"\n")
    yield path


@pytest.fixture(
    scope="function",
    params=[
        (vPartitionParseCSVOptions(), vPartitionSchemaInferenceOptions()),
        (
            vPartitionParseCSVOptions(has_headers=False),
            vPartitionSchemaInferenceOptions(inference_column_names=list(TEST_DATA.keys())),
        ),
        (
            vPartitionParseCSVOptions(has_headers=True),
            vPartitionSchemaInferenceOptions(inference_column_names=list(TEST_DATA.keys())),
        ),
        (vPartitionParseCSVOptions(delimiter="|"), vPartitionSchemaInferenceOptions()),
    ],
)
def csv_input(request, tmpdir: str) -> tuple[str, vPartitionParseCSVOptions, vPartitionSchemaInferenceOptions]:
    csv_options, schema_inference_options = request.param

    path = tmpdir + f"/data.csv"
    headers = [cname for cname in TEST_DATA]
    with open(path, "w") as f:
        writer = csv.writer(f, delimiter=csv_options.delimiter)
        if csv_options.has_headers:
            writer.writerow(headers)
        writer.writerows([[TEST_DATA[cname][row] for cname in headers] for row in range(TEST_DATA_LEN)])

    yield path, csv_options, schema_inference_options


@pytest.fixture(scope="function")
def parquet_input(tmpdir: str) -> str:
    path = tmpdir + f"/data.parquet"
    papq.write_table(pa.Table.from_pydict(TEST_DATA), path)
    yield path


@pytest.mark.parametrize(["input_type"], [(ip,) for ip in TEST_INPUT_TYPES])
def test_json_reads(json_input: str, input_type: InputType):
    with _resolve_parametrized_input_type(input_type, json_input) as table_io_input:
        table = table_io.read_json(table_io_input)
        d = table.to_pydict()
        assert d == JSON_EXPECTED_DATA


@pytest.mark.parametrize(["input_type"], [(ip,) for ip in TEST_INPUT_TYPES])
def test_csv_reads(csv_input: tuple[str, vPartitionParseCSVOptions], input_type: InputType):
    csv_input_path, csv_options, schema_inference_options = csv_input
    with _resolve_parametrized_input_type(input_type, csv_input_path) as table_io_input:
        table = table_io.read_csv(table_io_input, csv_options=csv_options, schema_options=schema_inference_options)
        d = table.to_pydict()
        assert d == CSV_EXPECTED_DATA


@pytest.mark.parametrize(["input_type"], [(ip,) for ip in TEST_INPUT_TYPES])
def test_parquet_reads(parquet_input: str, input_type: InputType):
    with _resolve_parametrized_input_type(input_type, parquet_input) as table_io_input:
        table = table_io.read_parquet(table_io_input)
        d = table.to_pydict()
        assert d == PARQUET_EXPECTED_DATA

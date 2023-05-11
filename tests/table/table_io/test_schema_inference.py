from __future__ import annotations

from typing import Callable

import pytest

from daft.datatype import DataType
from daft.logical.schema import Schema
from daft.runners.partitioning import (
    vPartitionParseCSVOptions,
    vPartitionSchemaInferenceOptions,
)
from daft.table import table_io
from tests.table.table_io.conftest import TEST_DATA, InputType

CSV_EXPECTED_SCHEMA = Schema._from_field_name_and_types(
    list(
        {
            "dates": DataType.date(),
            "strings": DataType.string(),
            "integers": DataType.int64(),
            "floats": DataType.float64(),
            "bools": DataType.bool(),
            "var_sized_arrays": DataType.string(),
            "fixed_sized_arrays": DataType.string(),
            "structs": DataType.string(),
        }.items()
    )
)


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
        schema = table_io.infer_schema_csv(
            table_io_input, override_column_names=schema_options.inference_column_names, csv_options=csv_options
        )
        assert schema == CSV_EXPECTED_SCHEMA

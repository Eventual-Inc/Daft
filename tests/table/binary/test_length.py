from __future__ import annotations

import pytest

from daft.expressions import col
from daft.table import MicroPartition


@pytest.mark.parametrize(
    "input_data,expected_lengths",
    [
        # Basic ASCII strings
        (
            [b"Hello", b"World!", b"", b"Test"],
            [5, 6, 0, 4],
        ),
        # Special binary sequences
        (
            [
                b"\x00\x01\x02",  # Null and control characters
                b"\xff\xfe\xfd",  # High-value bytes
                b"\x7f\x80\x81",  # Around ASCII boundary
                b"Hello\x00World",  # Embedded null
                b"\xe2\x98\x83",  # UTF-8 encoded snowman (☃)
                b"\xf0\x9f\x98\x89",  # UTF-8 encoded winking face (😉)
            ],
            [3, 3, 3, 11, 3, 4],
        ),
        # Nulls and empty strings
        (
            [b"Hello", None, b"", b"\x00", None, b"Test", b""],
            [5, None, 0, 1, None, 4, 0],
        ),
    ],
)
def test_binary_length(input_data: list[bytes | None], expected_lengths: list[int | None]) -> None:
    table = MicroPartition.from_pydict({"col": input_data})
    result = table.eval_expression_list([col("col").binary.length()])
    assert result.to_pydict() == {"col": expected_lengths}

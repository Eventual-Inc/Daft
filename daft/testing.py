from __future__ import annotations

"""
Testing utilities for Daft DataFrames and Schemas.

These helpers make it easier to write unit tests that verify DataFrame
contents and structure without boilerplate.
"""

from typing import TYPE_CHECKING, Optional, Sequence

if TYPE_CHECKING:
    from daft.dataframe import DataFrame
    from daft.schema import Schema


def assert_schema_equal(
    actual: "Schema",
    expected: "Schema",
    check_field_order: bool = True,
) -> None:

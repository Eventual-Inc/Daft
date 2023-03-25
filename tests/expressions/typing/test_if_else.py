from __future__ import annotations

from daft.datatype import DataType
from daft.expressions import col
from daft.series import Series
from daft.table import Table
from tests.expressions.typing.conftest import (
    assert_typing_resolve_vs_runtime_behavior,
    is_numeric,
)


def test_if_else(binary_data_fixture):
    lhs, rhs = binary_data_fixture

    # TODO: [RUST-INT] If/Else has not implemented all these types yet, enable when ready
    kernel_not_implemented = not is_numeric(lhs.datatype()) and not lhs.datatype() == DataType.string()

    assert_typing_resolve_vs_runtime_behavior(
        Table.from_pydict(
            {
                "predicate": Series.from_pylist([True, False, None]),
                **{s.name(): s for s in binary_data_fixture},
            }
        ),
        col("predicate").if_else(col(lhs.name()), col(rhs.name())),
        lambda tbl: tbl.get_column("predicate").if_else(tbl.get_column(lhs.name()), tbl.get_column(rhs.name())),
        (DataType.supertype(lhs.datatype(), rhs.datatype()) is not None) and (not kernel_not_implemented),
    )

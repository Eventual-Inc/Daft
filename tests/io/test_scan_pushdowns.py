from __future__ import annotations

from collections.abc import Iterator
from typing import Callable, Iterable

import pyarrow as pa
import pyarrow.compute as pa_compute
import pyarrow.dataset as pa_dataset
import pytest

from daft import col, context
from daft.daft import PartitionField, PyRecordBatch, ScanOperatorHandle, ScanTask
from daft.daft import Pushdowns as PyPushdowns
from daft.dataframe import DataFrame
from daft.io.pushdowns import Expr, Literal, Reference, Term, TermVisitor
from daft.io.scan import ScanOperator, ScanPushdowns
from daft.logical.builder import LogicalPlanBuilder
from daft.logical.schema import DataType, Schema
from daft.recordbatch.recordbatch import RecordBatch

pytestmark = pytest.mark.skipif(
    context.get_context().get_or_create_runner().name == "ray",
    reason="Skipping on ray since we are using an in-memory dataset.",
)

###
# Custom Scan Implementation
###


class TestScanOperator(ScanOperator):
    """TestScanOperator applies pushdowns to a pyarrow in-memory DataSet."""

    _ds: pa_dataset.Dataset
    _schema: Schema

    @classmethod
    def from_patable(cls, table: pa.Table) -> TestScanOperator:
        s = cls.__new__(cls)
        s._ds = pa_dataset.InMemoryDataset(table)
        s._schema = Schema._from_field_name_and_types(
            [(f.name, DataType.from_arrow_type(f.type)) for f in table.schema]
        )
        return s

    def schema(self) -> Schema:
        return self._schema

    def name(self) -> str:
        return "TestScanOperator"

    def display_name(self) -> str:
        return "test_scan"

    def partitioning_keys(self) -> list[PartitionField]:
        return []

    def can_absorb_filter(self) -> bool:
        return True

    def can_absorb_limit(self) -> bool:
        """The `can_absorb_limit` toggles limit pushdowns.

        If this is true, then the scan operator MUST apply the limit.

        Daft will include always include a limit in the pushdown if possible,
        regardless of this value. This allows scans to apply a local limit
        to each of its tasks, but daft will still handle the global limit.

        In this example, I am unable to apply a global limit to the scan
        without materializing the datasource in `to_scan_tasks` which is
        not ideal .. so instead I will pass the limit to each ScanTask and
        apply locally, then daft will handle the global limit for correctness.
        """
        return False

    def can_absorb_select(self) -> bool:
        return True

    def multiline_display(self) -> list[str]:
        raise NotImplementedError

    def to_scan_tasks(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
        pd = ScanPushdowns._from_pypushdowns(pushdowns, self._schema)

        # print("PUSHDOWNS")
        # print("---------")
        # print(pd)

        # LIMIT PUSHDOWN
        #
        #   We must apply the limit locally (task level).
        #   Therefore it's an argument of our custom scan task.
        #
        limit: int | None = pd.limit

        # FILTER PUSHDOWN
        #
        #   Here's we must convert a daft PyExpr into a pyarrow expression.
        #   What do we do if we are unable to translate it?
        #   More on this later.
        #
        predicate: pa_compute.Expression = _translate_term(pd.predicate) if pd.predicate else None

        # PROJECTION PUSHDOWN
        #
        #   This one is easy since pyarrow can accept a list of strings
        #   as well as indexes into the schema. We can't push down projections
        #   of arbitrary expressions because we don't have any pre-computed
        #   columns. On one hand, we could walk the terms, but we actually
        #   already have columns as strings via the PyPushdowns rust wrapper.
        #   So we grab that directly and don't need to use terms.
        #
        #   option (1) using existing column names
        #
        # projections: list[str] | None = pushdowns.columns
        #
        #   option (2) extract columns for arbitrary expressions
        #
        if pd.projections:
            schema = _translate_projections(self._schema, pd.projections)
            projections = schema.column_names()
        else:
            schema = self._schema
            projections = None

        # RETURN SCAN TASKS
        #
        #   Map each of the pyarrow Dataset Fragments to a TestScanTask.
        #   We pushdown the limit here. Note that, if the data has a partition
        #   expression or some parquet stats apply, we could even push a
        #   predicate into get_fragments, but this is just an example.
        #
        for task in [TestScanTask(frag, predicate, projections, limit) for frag in self._ds.get_fragments()]:
            yield ScanTask.python_factory_func_scan_task(
                module=_apply_task.__module__,
                func_name=_apply_task.__name__,
                func_args=(task,),
                schema=schema._schema,  # <-- USE UPDATED SCHEMA !!
                num_rows=None,
                size_bytes=None,
                pushdowns=None,
                stats=None,
            )

    def to_dataframe(self) -> DataFrame:
        """Create a DataFrame backed by this ScanOperator."""
        handle = ScanOperatorHandle.from_python_scan_operator(self)
        builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
        return DataFrame(builder)


class TestScanTask:
    """The TestScanTask lets us hold some state, in this case we need the pushdowns."""

    _fragment: pa_dataset.Fragment
    _predicate: pa_compute.Expression
    _projections: list[str] | None
    _limit: int | None

    def __init__(
        self,
        fragment: pa_dataset.Fragment,
        predicate: pa_compute.Expression | None = None,
        projections: list[str] | None = None,
        limit: int | None = None,
    ) -> None:
        self._fragment = fragment
        self._predicate = predicate
        self._projections = projections
        self._limit = limit

    def read(self) -> Iterable[RecordBatch]:
        if self._limit is not None:
            pa_table = self._fragment.head(
                num_rows=self._limit,
                columns=self._projections,
                filter=self._predicate,
            )
        else:
            pa_table = self._fragment.to_table(
                columns=self._projections,
                filter=self._predicate,
            )

        # print("TestScanTask")
        # print("---------")
        # print(pa_table)

        yield RecordBatch.from_arrow(pa_table)


def _apply_task(task: TestScanTask) -> Iterator[PyRecordBatch]:
    """The task instance has been pickled then sent to this stateless method."""
    for batch in task.read():
        yield batch._table


###
# Pushdown Term Translation
###


def _translate_term(term: Term) -> pa_compute.Expression:
    return Translator().visit(term, None)


def _translate_projections(schema: Schema, projections: list[Term]) -> Schema:
    # lookup by index
    curr_fields = list(schema)
    next_fields = []
    # we could lookup by name too, but let's make use of our bound references.

    for proj in projections:
        if isinstance(proj, Reference):
            # option (1) using field name (path)
            # next_fields.append(schema[proj.path])
            # option (2) using bound field index
            next_fields.append(curr_fields[proj.index])
        else:
            raise ValueError("pyarrow does not support expressions in pushdown projections.")
    return Schema._from_fields(next_fields)


class Translator(TermVisitor[None, pa_compute.Expression]):
    """Translator does a tree fold into pa_compute.Expression domain.

    Think of a visitor like a closure (it's a class after all) where instance
    members are the closed state. Then we can recurse with scoped args (context)
    while accumulating return values (folding) which translates a tree from
    one domain (Term) into a tree of another (pyarrow Expression).
    """

    # PROCEDURE MAPPINGS
    #
    #   This maps daft procedures to pyarrow.compute expression functions.
    #   Note the type annotation. You can use this same technique, and to
    #   be honest there could be a generic Translator class that accepts
    #   mappings and allows overloading.
    #
    #   Because we have setup the Terms to be modeled like s-expressions,
    #   we are effectively making an interpreter. We will use a table
    #   of callables just like https://norvig.com/lispy.html but we don't
    #   introduce anything new into the env (_PROCEDURES).
    #
    _PROCEDURES: dict[str, Callable[..., pa_compute.Expression]] = {
        # comparison predicates
        "=": pa_compute.equal,
        "!=": pa_compute.not_equal,
        "<": pa_compute.less,
        ">": pa_compute.greater,
        "<=": pa_compute.less_equal,
        ">=": pa_compute.greater_equal,
        # logical predicates
        "and": pa_compute.and_,
        "or": pa_compute.or_,
        "not": pa_compute.negate,
        # you could (should) add more..
    }

    def visit_reference(self, term: Reference, context: None) -> pa_compute.Expression:
        """Convert the Reference to a pyarrow.compute.Field which only stores a field name."""
        return pa_compute.field(term.path)

    def visit_literal(self, term: Literal, context: None) -> pa_compute.Expression:
        """Convert the Literal to a pyarrow.compute.Scalar without check the type.

        From the pyarrow scalar docs:
        > value : bool, int, float or string
        > Python value of the scalar. Note that only a subset of types are currently supported.

        Coincidentally, this is what term is currently limited to.
        """
        return pa_compute.scalar(term.value)

    def visit_expr(self, term: Expr, context: None) -> pa_compute.Expression:
        """Here's where the fun begins, this requires knowledge of both systems' expression support."""
        proc = term.proc
        args = [self.visit(arg.term, None) for arg in term.args]
        if proc not in self._PROCEDURES:
            raise ValueError(f"pyarrow.compute does not support procedure '{proc}'.")
        return self._PROCEDURES[proc](*args)


###
# Testing
###


@pytest.fixture(scope="session")
def scan():
    a_values = []
    b_values = []
    c_values = []
    for i in range(1, 21):
        a_values.append(i % 2 == 0)  # true if even
        b_values.append(i)
        c_values.append(f"str_{i}")
    table = pa.table(
        [
            pa.array(a_values, type=pa.bool_()),
            pa.array(b_values, type=pa.int64()),
            pa.array(c_values, type=pa.string()),
        ],
        names=["a", "b", "c"],
    )
    yield TestScanOperator.from_patable(table)


def test_schema(scan: TestScanOperator):
    schema = scan.schema()
    assert len(schema) == 3
    assert schema["a"].dtype == DataType.bool()
    assert schema["b"].dtype == DataType.int64()
    assert schema["c"].dtype == DataType.string()


def test_scan_sanity(scan: TestScanOperator):
    df = scan.to_dataframe()
    assert df.count_rows() == 20


def test_limit_pushdown(scan: TestScanOperator):
    df = scan.to_dataframe().limit(2)
    assert df.to_pydict() == {"a": [False, True], "b": [1, 2], "c": ["str_1", "str_2"]}


def test_predicate_pushdown(scan: TestScanOperator):
    df = scan.to_dataframe().where(col("b") < 4)
    assert df.to_pydict() == {"a": [False, True, False], "b": [1, 2, 3], "c": ["str_1", "str_2", "str_3"]}


def test_projection_pushdown(scan: TestScanOperator):
    df1 = scan.to_dataframe().select("a", "b").to_pydict()
    df2 = scan.to_dataframe().select("b", "c").to_pydict()
    assert len(df1) == 2
    assert len(df2) == 2
    assert df1["b"] == df2["b"]


def test_combined_pushdowns(scan: TestScanOperator):
    df = scan.to_dataframe().where(col("b") > 15).select("a", "c").limit(3)
    assert df.to_pydict() == {"a": [True, False, True], "c": ["str_16", "str_17", "str_18"]}

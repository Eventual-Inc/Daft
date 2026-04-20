"""Tests for custom DataSource column pushdown with joins.

Regression test for https://github.com/Eventual-Inc/Daft/issues/6500

When ``can_absorb_select()`` returns ``False`` (the default for custom
DataSource implementations), the optimizer may push down an incomplete
column set that omits columns required by downstream operators such as
joins and aggregations.  This caused a HashJoin schema assertion panic.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator

import pyarrow as pa

from daft import col
from daft.io.pushdowns import Pushdowns
from daft.io.source import DataSource, DataSourceTask
from daft.recordbatch import MicroPartition
from daft.schema import Schema

# ---------------------------------------------------------------------------
# Minimal in-memory DataSource (can_absorb_select=False by default)
# ---------------------------------------------------------------------------


class _InMemoryTask(DataSourceTask):
    def __init__(self, table: pa.Table, columns: list[str] | None) -> None:
        self._table = table
        self._columns = columns

    @property
    def schema(self) -> Schema:
        if self._columns:
            projected = pa.schema([self._table.schema.field(c) for c in self._columns])
            return Schema.from_pyarrow_schema(projected)
        return Schema.from_pyarrow_schema(self._table.schema)

    def get_micro_partitions(self) -> Iterator[MicroPartition]:
        t = self._table
        if self._columns:
            t = t.select(self._columns)
        yield MicroPartition.from_arrow(t)


class InMemorySource(DataSource):
    """A DataSource that applies column pushdowns (filtering to table-owned columns)."""

    def __init__(self, name: str, table: pa.Table) -> None:
        self._name = name
        self._table = table

    @property
    def name(self) -> str:
        return self._name

    @property
    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(self._table.schema)

    async def get_tasks(self, pushdowns) -> AsyncIterator[_InMemoryTask]:
        columns = None
        if isinstance(pushdowns, Pushdowns) and pushdowns.columns:
            table_col_set = set(self._table.schema.names)
            columns = [c for c in pushdowns.columns if c in table_col_set] or None
        yield _InMemoryTask(self._table, columns)


# ---------------------------------------------------------------------------
# Test data
# ---------------------------------------------------------------------------

_ORDERS = pa.table(
    {
        "order_id": list(range(1, 1001)),
        "customer_id": [i % 100 + 1 for i in range(1000)],
        "amount": [float(i) for i in range(1000)],
    }
)

_CUSTOMERS = pa.table(
    {
        "customer_id": list(range(1, 101)),
        "name": [f"c_{i}" for i in range(1, 101)],
        "tier": ["gold" if i % 2 == 0 else "silver" for i in range(1, 101)],
    }
)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_join_select_does_not_panic():
    """Join + select should not cause a HashJoin schema assertion failure."""
    df_orders = InMemorySource("orders", _ORDERS).read()
    df_customers = InMemorySource("customers", _CUSTOMERS).read()

    result = (
        df_orders.join(df_customers, on="customer_id", how="inner")
        .select("order_id", "name", "amount")
        .sort("order_id")
        .limit(5)
        .to_pydict()
    )
    assert len(result["order_id"]) == 5
    assert set(result.keys()) == {"order_id", "name", "amount"}


def test_join_groupby_agg_does_not_panic():
    """Join + groupby + agg should not cause a HashJoin schema assertion failure."""
    df_orders = InMemorySource("orders", _ORDERS).read()
    df_customers = InMemorySource("customers", _CUSTOMERS).read()

    result = (
        df_orders.join(df_customers, on="customer_id", how="inner")
        .groupby("tier")
        .agg(
            col("amount").sum().alias("total"),
            col("order_id").count().alias("cnt"),
        )
        .sort("total", desc=True)
        .to_pydict()
    )
    assert len(result["tier"]) == 2
    assert set(result["tier"]) == {"gold", "silver"}
    assert all(v > 0 for v in result["total"])


def test_multi_join_agg_does_not_panic():
    """Three-way join + aggregation mirrors the cross-backend notebook scenario."""
    products = pa.table(
        {
            "product": ["A", "B"],
            "category": ["Widgets", "Gadgets"],
            "cost_price": [15.0, 45.0],
        }
    )

    orders_with_product = pa.table(
        {
            "order_id": list(range(1, 1001)),
            "customer_id": [i % 100 + 1 for i in range(1000)],
            "product": ["A" if i % 2 == 0 else "B" for i in range(1000)],
            "amount": [float(i) for i in range(1000)],
        }
    )

    df_orders = InMemorySource("orders", orders_with_product).read()
    df_customers = InMemorySource("customers", _CUSTOMERS).read()
    df_products = InMemorySource("products", products).read()

    df_enriched = df_orders.join(df_products, on="product", how="inner").select(
        "order_id",
        "customer_id",
        "product",
        "category",
        "amount",
        "cost_price",
    )

    df_full = df_enriched.join(df_customers, on="customer_id", how="inner").select(
        "order_id",
        "customer_id",
        "product",
        "category",
        "amount",
        "cost_price",
        "name",
        "tier",
    )

    result = (
        df_full.with_column("margin", col("amount") - col("cost_price"))
        .groupby("category")
        .agg(
            col("margin").sum().alias("total_margin"),
            col("order_id").count().alias("order_count"),
        )
        .sort("total_margin", desc=True)
        .to_pydict()
    )

    assert len(result["category"]) == 2
    assert set(result["category"]) == {"Widgets", "Gadgets"}
    assert all(v > 0 for v in result["order_count"])
